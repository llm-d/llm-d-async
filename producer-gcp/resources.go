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
	expiration         *durationpb.Duration
}

func (p *Producer) ensureResources(ctx context.Context) error {
	for _, topicID := range []string{p.requestTopicID, p.deadLetterTopicID, p.resultTopicID} {
		if err := p.ensureTopic(ctx, topicID); err != nil {
			return err
		}
	}

	dlqSub := subscriptionSpec{
		id:                 p.deadLetterSubscriptionID,
		topicID:            p.deadLetterTopicID,
		ackDeadlineSeconds: resultAckDeadlineSeconds,
		neverExpire:        true,
	}
	if err := p.ensureSubscription(ctx, dlqSub); err != nil {
		return err
	}

	requestSub := subscriptionSpec{
		id:                 p.requestSubscriptionID,
		topicID:            p.requestTopicID,
		enableExactlyOnce:  true,
		ackDeadlineSeconds: requestAckDeadlineSeconds,
		retry:              true,
		deadLetterTopicID:  p.deadLetterTopicID,
		neverExpire:        true,
	}
	if err := p.ensureSubscription(ctx, requestSub); err != nil {
		return err
	}

	resultSub := subscriptionSpec{
		id:                 p.resultSubscriptionID,
		topicID:            p.resultTopicID,
		filter:             resultRouteFilter(p.resultRoute),
		ackDeadlineSeconds: resultAckDeadlineSeconds,
		expiration:         durationpb.New(defaultResultSubscriptionExpiry),
	}
	return p.ensureSubscription(ctx, resultSub)
}

func (p *Producer) ensureTopic(ctx context.Context, topicID string) error {
	name := topicResource(p.projectID, topicID)
	_, err := p.client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: name})
	if err == nil || status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return fmt.Errorf("create topic %q: %w", topicID, err)
}

func (p *Producer) ensureSubscription(ctx context.Context, spec subscriptionSpec) error {
	name := subscriptionResource(p.projectID, spec.id)
	wantTopic := topicResource(p.projectID, spec.topicID)
	_, err := p.client.SubscriptionAdminClient.CreateSubscription(ctx, spec.toProto(p.projectID, name, wantTopic))
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create subscription %q: %w", spec.id, err)
	}

	existing, getErr := p.client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
		Subscription: name,
	})
	if getErr != nil {
		return fmt.Errorf("get existing subscription %q: %w", spec.id, getErr)
	}
	if existing.GetTopic() != wantTopic {
		return fmt.Errorf("subscription %q exists but is attached to topic %q, want %q", spec.id, existing.GetTopic(), wantTopic)
	}
	if spec.filter != "" && existing.GetFilter() != spec.filter {
		return fmt.Errorf("subscription %q exists but has filter %q, want %q", spec.id, existing.GetFilter(), spec.filter)
	}
	return nil
}

func (spec subscriptionSpec) toProto(project, name, topic string) *pubsubpb.Subscription {
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
			MaxDeliveryAttempts: maxDeliveryAttempts,
		}
	}
	if spec.neverExpire {
		sub.ExpirationPolicy = &pubsubpb.ExpirationPolicy{}
	} else if spec.expiration != nil {
		sub.ExpirationPolicy = &pubsubpb.ExpirationPolicy{Ttl: spec.expiration}
	}
	return sub
}
