package flowcontrol

import (
	"context"
	"fmt"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
)

var _ pipeline.Gate = (*TierPriorityAdmissionGate)(nil)

type TierPriorityAdmissionGate struct {
	saturationGate pipeline.Gate
	tierLabel      string
}

func NewTierPriorityAdmissionGate(saturationGate pipeline.Gate, tierLabel string) *TierPriorityAdmissionGate {
	if tierLabel == "" {
		tierLabel = "tier"
	}
	return &TierPriorityAdmissionGate{
		saturationGate: saturationGate,
		tierLabel:      tierLabel,
	}
}

func (g *TierPriorityAdmissionGate) Budget(ctx context.Context) float64 {
	return g.saturationGate.Budget(ctx)
}

func (g *TierPriorityAdmissionGate) Apply(ctx context.Context, msg *api.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	satRes, err := g.saturationGate.Apply(ctx, msg, releases)
	if err != nil {
		return pipeline.Verdict{}, fmt.Errorf("saturation gate failed: %w", err)
	}

	isSaturated := satRes.Action == pipeline.ActionRefuse

	if !isSaturated {
		return pipeline.Continue(), nil
	}

	classification := msg.GetClassification()
	if classification == api.ClassificationReserved {
		return pipeline.Wait(), nil
	}

	tier := ""
	if msg.Labels != nil {
		tier = msg.Labels[g.tierLabel]
	}

	if tier == string(api.TierInteractive) && classification == api.ClassificationOverflow {
		result := &api.ResultMessage{
			ID:      msg.PublicRequest.ReqID(),
			Payload: `{"error": "Too Many Requests", "code": 429}`,
			Routing: msg.InternalRouting,
		}
		return pipeline.Drop(result), nil
	}

	return pipeline.Refuse(), nil
}
