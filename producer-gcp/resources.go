package producergcp

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type subscriptionSpec struct {
	id                 string
	topicID            string
	filter             string
	enableExactlyOnce  bool
	ackDeadlineSeconds int32
	retry              bool
	deadLetterTopicID  string
	neverExpire        bool
}

func (p *Producer) ensureResources(ctx context.Context) error {
	requestSub := subscriptionSpec{
		id:                 p.cfg.RequestSubscriptionID,
		topicID:            p.cfg.RequestTopicID,
		enableExactlyOnce:  true,
		ackDeadlineSeconds: requestAckDeadlineSeconds,
		retry:              true,
		deadLetterTopicID:  p.cfg.DeadLetterTopicID,
		neverExpire:        true,
	}
	// An existing request subscription is checked before the DLQ topic and
	// per-route subscription are created. A mismatch must not leave those behind.
	existingRequest, err := p.validateExistingSubscription(ctx, requestSub)
	if err != nil {
		return err
	}

	for _, topicID := range []string{p.cfg.RequestTopicID, p.cfg.DeadLetterTopicID, p.cfg.ResultTopicID} {
		if err := p.ensureTopic(ctx, topicID); err != nil {
			return err
		}
	}

	dlqSub := subscriptionSpec{
		id:                 p.cfg.DeadLetterSubscriptionID,
		topicID:            p.cfg.DeadLetterTopicID,
		filter:             resultRouteFilter(p.cfg.ResultRoute),
		ackDeadlineSeconds: resultAckDeadlineSeconds,
		neverExpire:        true,
	}
	if err := p.ensureSubscription(ctx, dlqSub, nil); err != nil {
		return err
	}
	if err := p.ensureSubscription(ctx, requestSub, existingRequest); err != nil {
		return err
	}

	resultSub := subscriptionSpec{
		id:                 p.cfg.ResultSubscriptionID,
		topicID:            p.cfg.ResultTopicID,
		filter:             resultRouteFilter(p.cfg.ResultRoute),
		ackDeadlineSeconds: resultAckDeadlineSeconds,
		neverExpire:        true,
	}
	return p.ensureSubscription(ctx, resultSub, nil)
}

func (p *Producer) ensureTopic(ctx context.Context, topicID string) error {
	name := topicResource(p.cfg.ProjectID, topicID)
	_, err := p.client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: name})
	if err == nil || status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return fmt.Errorf("create topic %q: %w", topicID, err)
}

func (p *Producer) ensureSubscription(ctx context.Context, spec subscriptionSpec, existing *pubsubpb.Subscription) error {
	if existing != nil {
		p.noteMissingDeadLetter(spec, existing)
		return nil
	}

	name := subscriptionResource(p.cfg.ProjectID, spec.id)
	wantTopic := topicResource(p.cfg.ProjectID, spec.topicID)
	_, err := p.client.SubscriptionAdminClient.CreateSubscription(ctx, spec.toProto(p.cfg.ProjectID, name, wantTopic, p.cfg.MaxDeliveryAttempts))
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create subscription %q: %w", spec.id, err)
	}

	got, getErr := p.client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
		Subscription: name,
	})
	if getErr != nil {
		return fmt.Errorf("get existing subscription %q: %w", spec.id, getErr)
	}
	if err := p.subscriptionMismatch(got, spec); err != nil {
		return err
	}
	p.noteMissingDeadLetter(spec, got)
	return nil
}

func (p *Producer) noteMissingDeadLetter(spec subscriptionSpec, existing *pubsubpb.Subscription) {
	if spec.deadLetterTopicID != "" && existing.GetDeadLetterPolicy().GetDeadLetterTopic() == "" {
		p.logger.Info("subscription has no dead-letter policy; leaving it unchanged",
			"subscription", spec.id,
			"wantDeadLetterTopic", topicResource(p.cfg.ProjectID, spec.deadLetterTopicID))
	}
}

// validateExistingSubscription checks a subscription that is already present.
// NotFound is success: ensureSubscription creates it after DLQ resources exist.
// The returned proto is passed into ensureSubscription so an existing
// subscription is not fetched again.
func (p *Producer) validateExistingSubscription(ctx context.Context, spec subscriptionSpec) (*pubsubpb.Subscription, error) {
	existing, err := p.client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
		Subscription: subscriptionResource(p.cfg.ProjectID, spec.id),
	})
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get existing subscription %q: %w", spec.id, err)
	}
	if err := p.subscriptionMismatch(existing, spec); err != nil {
		return nil, err
	}
	return existing, nil
}

func (p *Producer) subscriptionMismatch(existing *pubsubpb.Subscription, spec subscriptionSpec) error {
	wantTopic := topicResource(p.cfg.ProjectID, spec.topicID)
	if existing.GetTopic() != wantTopic {
		return fmt.Errorf("subscription %q exists but is attached to topic %q, want %q", spec.id, existing.GetTopic(), wantTopic)
	}
	if spec.filter != "" && existing.GetFilter() != spec.filter {
		return fmt.Errorf("subscription %q exists but has filter %q, want %q", spec.id, existing.GetFilter(), spec.filter)
	}
	if spec.deadLetterTopicID != "" {
		gotDLQ := existing.GetDeadLetterPolicy().GetDeadLetterTopic()
		wantDLQ := topicResource(p.cfg.ProjectID, spec.deadLetterTopicID)
		// A missing policy is left unchanged. A policy that points somewhere else is not.
		if gotDLQ != "" && gotDLQ != wantDLQ {
			return fmt.Errorf("subscription %q exists but dead-letters to %q, want %q", spec.id, gotDLQ, wantDLQ)
		}
	}
	return nil
}

func (spec subscriptionSpec) toProto(project, name, topic string, deliveryAttempts int32) *pubsubpb.Subscription {
	sub := &pubsubpb.Subscription{
		Name:                      name,
		Topic:                     topic,
		AckDeadlineSeconds:        spec.ackDeadlineSeconds,
		Filter:                    spec.filter,
		EnableExactlyOnceDelivery: spec.enableExactlyOnce,
	}
	if spec.retry {
		sub.RetryPolicy = &pubsubpb.RetryPolicy{
			MinimumBackoff: durationpb.New(minRetryBackoff),
			MaximumBackoff: durationpb.New(maxRetryBackoff),
		}
	}
	if spec.deadLetterTopicID != "" {
		sub.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     topicResource(project, spec.deadLetterTopicID),
			MaxDeliveryAttempts: deliveryAttempts,
		}
	}
	if spec.neverExpire {
		sub.ExpirationPolicy = &pubsubpb.ExpirationPolicy{}
	}
	return sub
}
