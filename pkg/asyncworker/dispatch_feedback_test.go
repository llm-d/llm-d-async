package asyncworker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
)

func TestSendRequest_responseHeadersReturned(t *testing.T) {
	h := make(http.Header)
	h.Set(asyncapi.ViewQueueDurationHeader, "125")
	client := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     h,
		}, nil
	}))
	resp, err := client.SendRequest(context.Background(), "http://localhost/v1/completions", nil, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := resp.Header.Get(asyncapi.ViewQueueDurationHeader); got != "125" {
		t.Errorf("view header = %q, want %q", got, "125")
	}
}

func TestDispatchFeedback_outcomeClassification(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		outcome pipeline.DispatchOutcome
	}{
		{"success", nil, pipeline.OutcomeAccepted},
		{
			"saturated by reason",
			&asyncapi.ClientError{StatusCode: 503, DroppedReason: asyncapi.DroppedReasonSaturated},
			pipeline.OutcomeRejectedCapacity,
		},
		{
			"bare 429 counts as capacity",
			&asyncapi.ClientError{StatusCode: 429},
			pipeline.OutcomeRejectedCapacity,
		},
		{
			"ttl expired",
			&asyncapi.ClientError{StatusCode: 503, DroppedReason: asyncapi.DroppedReasonTTLExpired},
			pipeline.OutcomeTTLExpired,
		},
		{
			"evicted",
			&asyncapi.ClientError{StatusCode: 429, DroppedReason: "evicted-priority"},
			pipeline.OutcomeEvicted,
		},
		{
			"non-capacity reason",
			&asyncapi.ClientError{StatusCode: 503, DroppedReason: "rejected-shutting-down"},
			pipeline.OutcomeRejectedOther,
		},
		{
			"bare server error",
			&asyncapi.ClientError{StatusCode: 500},
			pipeline.OutcomeError,
		},
		{
			"non-client error",
			errors.New("boom"),
			pipeline.OutcomeError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fb := dispatchFeedback(nil, nil, tt.err)
			if fb.Outcome != tt.outcome {
				t.Errorf("Outcome = %v, want %v", fb.Outcome, tt.outcome)
			}
		})
	}
}

func TestDispatchFeedback_advisoryViewsParsed(t *testing.T) {
	h := make(http.Header)
	h.Set(asyncapi.ViewQueueDurationHeader, "250")
	h.Set(asyncapi.ViewBandHeadroomHeader, "12")
	resp := &asyncapi.InferenceResponse{StatusCode: 200, Header: h}

	fb := dispatchFeedback(nil, resp, nil)
	if fb.QueueDuration != 250*time.Millisecond {
		t.Errorf("QueueDuration = %v, want 250ms", fb.QueueDuration)
	}
	if !fb.HasBandHeadroom || fb.BandHeadroom != 12 {
		t.Errorf("BandHeadroom = %v (has=%v), want 12", fb.BandHeadroom, fb.HasBandHeadroom)
	}

	// Absent or malformed headers mean no signal.
	fb = dispatchFeedback(nil, &asyncapi.InferenceResponse{StatusCode: 200}, nil)
	if fb.QueueDuration != 0 {
		t.Errorf("QueueDuration = %v, want 0 (unset)", fb.QueueDuration)
	}
	if fb.HasBandHeadroom {
		t.Errorf("HasBandHeadroom = true, want no signal")
	}
	bad := make(http.Header)
	bad.Set(asyncapi.ViewQueueDurationHeader, "not-a-number")
	bad.Set(asyncapi.ViewBandHeadroomHeader, "-5")
	fb = dispatchFeedback(nil, &asyncapi.InferenceResponse{StatusCode: 200, Header: bad}, nil)
	if fb.QueueDuration != 0 || fb.HasBandHeadroom {
		t.Errorf("malformed views should be ignored, got %+v", fb)
	}
}
