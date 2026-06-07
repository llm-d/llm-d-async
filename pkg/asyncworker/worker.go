package asyncworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"
	uotel "github.com/llm-d-incubation/llm-d-async/internal/otel"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const (
	baseDelaySeconds = 2
	maxDelaySeconds  = 60
)

func Worker(ctx context.Context, characteristics pipeline.Characteristics, client asyncapi.InferenceClient, requestChannel chan pipeline.EmbelishedRequestMessage,
	retryChannel chan pipeline.RetryMessage, resultChannel chan asyncapi.ResultMessage, requestTimeout time.Duration) {

	logger := log.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			logger.V(logutil.DEFAULT).Info("Worker finishing, draining request channel.")
			for {
				select {
				case msg, ok := <-requestChannel:
					if !ok {
						return
					}
					if msg.InternalRequest == nil || msg.PublicRequest == nil {
						continue
					}
					select {
					case retryChannel <- pipeline.RetryMessage{
						EmbelishedRequestMessage: msg,
						BackoffDurationSeconds:   0,
					}:
					default:
						logger.V(logutil.DEFAULT).Error(nil, "retry channel full, dropping message on shutdown", "id", msg.PublicRequest.ReqID())
					}
				default:
					return
				}
			}
		case msg := <-requestChannel:
			if msg.InternalRequest == nil || msg.PublicRequest == nil {
				continue
			}
			queueID := msg.QueueID
			queueName := msg.RequestQueueName
			if msg.RetryCount == 0 {
				// Only count first attempt as a new request.
				metrics.RecordAsyncReq(queueID, queueName)
			}
			payloadBytes := validateAndMarshal(ctx, resultChannel, msg)
			if payloadBytes == nil {
				continue
			}

			// Using a function object for easy boundaries for 'return' and 'defer'!
			sendInferenceRequest := func() {
				// Restore parent trace context from request metadata (producer injects W3C trace context).
				reqCtx := ctx
				if md := msg.PublicRequest.ReqMetadata(); len(md) > 0 {
					reqCtx = otel.GetTextMapPropagator().Extract(reqCtx, propagation.MapCarrier(md))
				}

				spanAttrs := []attribute.KeyValue{
					attribute.String(uotel.AttrRequestID, msg.PublicRequest.ReqID()),
					attribute.Int(uotel.AttrRetryCount, msg.RetryCount),
				}
				if queueID != "" {
					spanAttrs = append(spanAttrs, attribute.String(uotel.AttrQueueID, queueID))
				}
				if queueName != "" {
					spanAttrs = append(spanAttrs, attribute.String(uotel.AttrQueueName, queueName))
				}
				reqCtx, span := uotel.StartSpan(reqCtx, "process-request",
					trace.WithAttributes(spanAttrs...),
				)
				defer span.End()

				// Create a per-request context bounded by both the message deadline
				// and the configured request timeout, whichever comes first.
				reqDeadline := time.Now().Add(requestTimeout)
				if dline := msg.PublicRequest.ReqDeadline(); dline > 0 {
					if msgDeadline := time.Unix(dline, 0); msgDeadline.Before(reqDeadline) {
						reqDeadline = msgDeadline
					}
				}
				reqCtx, cancel := context.WithDeadline(reqCtx, reqDeadline)
				defer cancel()

				logger.V(logutil.DEBUG).Info("Sending inference request", "url", msg.RequestURL)
				responseBody, err := client.SendRequest(reqCtx, msg.RequestURL, msg.HttpHeaders, payloadBytes)

				if err == nil {
					// Success - got a valid response
					metrics.RecordSuccessfulReq(queueID, queueName)
					select {
					case resultChannel <- asyncapi.ResultMessage{
						ID:       msg.PublicRequest.ReqID(),
						Payload:  string(responseBody),
						Routing:  msg.InternalRouting,
						Metadata: msg.PublicRequest.ReqMetadata(),
					}:
					case <-ctx.Done():
					}
					return
				}

				// Shutdown: parent context cancelled and the error is context-related
				// (not a completed HTTP response like 4xx). Re-enqueue directly —
				// retryMessage's select would take ctx.Done() immediately.
				if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					_, bgSpan := uotel.DetachedContext(reqCtx, "re-enqueue")
					bgSpan.SetAttributes(attribute.String(uotel.AttrRequestID, msg.PublicRequest.ReqID()))
					defer bgSpan.End()
					retryChannel <- pipeline.RetryMessage{
						EmbelishedRequestMessage: msg,
						BackoffDurationSeconds:   0,
					}
					return
				}

				// Check if error implements InferenceError
				var inferenceErr asyncapi.InferenceError
				if !errors.As(err, &inferenceErr) || inferenceErr.Category().Fatal() {
					// Unknown error type or fatal error - fail immediately
					span.RecordError(err)
					span.SetStatus(codes.Error, "inference request failed")
					span.SetAttributes(attribute.String(uotel.AttrErrorCategory, inferenceErrorCategory(err)))
					metrics.RecordFailedReq(queueID, queueName)
					select {
					case resultChannel <- CreateErrorResultMessage(msg.PublicRequest, msg.InternalRouting, fmt.Sprintf("Failed to send request to inference: %s", err.Error())):
					case <-ctx.Done():
					}
					return
				}

				// Retryable error - check if it's due to rate limiting
				if inferenceErr.Category().Sheddable() {
					metrics.RecordSheddedReq(queueID, queueName)
				}
				span.SetAttributes(attribute.String(uotel.AttrErrorCategory, string(inferenceErr.Category())))
				// Pass server-specified Retry-After duration if available.
				var retryAfter time.Duration
				var clientErr *asyncapi.ClientError
				if errors.As(err, &clientErr) {
					retryAfter = clientErr.RetryAfter
				}
				retryMessage(ctx, msg, retryChannel, resultChannel, retryAfter)
			}
			sendInferenceRequest()
		}
	}
}

// parsing and validating payload. On failure puts an error msg on the result-channel and returns nil
func validateAndMarshal(ctx context.Context, resultChannel chan asyncapi.ResultMessage, msg pipeline.EmbelishedRequestMessage) []byte {
	if msg.PublicRequest == nil {
		return nil
	}
	queueID := msg.QueueID
	queueName := msg.RequestQueueName
	r := msg.PublicRequest
	deadline := r.ReqDeadline()
	if deadline <= 0 {
		metrics.RecordFailedReq(queueID, queueName)
		select {
		case resultChannel <- CreateErrorResultMessage(r, msg.InternalRouting, "Failed: deadline is missing or invalid (Unix seconds)."):
		case <-ctx.Done():
		}
		return nil
	}

	if deadline < time.Now().Unix() {
		metrics.RecordExceededDeadlineReq(queueID, queueName)
		select {
		case resultChannel <- CreateDeadlineExceededResultMessage(r, msg.InternalRouting):
		case <-ctx.Done():
		}
		return nil
	}

	payloadBytes, err := json.Marshal(r.ReqPayload())
	if err != nil {
		metrics.RecordFailedReq(queueID, queueName)
		select {
		case resultChannel <- CreateErrorResultMessage(r, msg.InternalRouting, fmt.Sprintf("Failed to marshal message's payload: %s", err.Error())):
		case <-ctx.Done():
		}
		return nil
	}
	return payloadBytes
}

// If it is not after deadline, just publish again.
func retryMessage(ctx context.Context, msg pipeline.EmbelishedRequestMessage, retryChannel chan pipeline.RetryMessage, resultChannel chan asyncapi.ResultMessage, retryAfter time.Duration) {
	if msg.PublicRequest == nil {
		return
	}
	queueID := msg.QueueID
	queueName := msg.RequestQueueName
	deadline := msg.PublicRequest.ReqDeadline()
	secondsToDeadline := deadline - time.Now().Unix()
	if secondsToDeadline <= 0 {
		metrics.RecordExceededDeadlineReq(queueID, queueName)
		select {
		case resultChannel <- CreateDeadlineExceededResultMessage(msg.PublicRequest, msg.InternalRouting):
		case <-ctx.Done():
		}
		return
	}

	finalDuration := expBackoffDuration(msg.RetryCount+1, int(secondsToDeadline))
	// Honor server-specified Retry-After when it exceeds the computed backoff,
	// but never schedule a retry beyond the message deadline.
	if retryAfterSec := retryAfter.Seconds(); retryAfterSec > finalDuration {
		finalDuration = retryAfterSec
	}

	if finalDuration >= float64(secondsToDeadline) {
		metrics.RecordExceededDeadlineReq(queueID, queueName)
		select {
		case resultChannel <- CreateDeadlineExceededResultMessage(msg.PublicRequest, msg.InternalRouting):
		case <-ctx.Done():
		}
		return
	}

	msg.RetryCount++
	metrics.RecordRetry(queueID, queueName)
	select {
	case retryChannel <- pipeline.RetryMessage{
		EmbelishedRequestMessage: msg,
		BackoffDurationSeconds:   finalDuration,
	}:
	case <-ctx.Done():
	}
}

// CreateErrorResultMessage builds a ResultMessage using the public request identity;
// metadata is read directly from req.ReqMetadata().
func CreateErrorResultMessage(req asyncapi.Request, routing asyncapi.InternalRouting, errMsg string) asyncapi.ResultMessage {
	errorPayload := map[string]string{"error": errMsg}
	payloadBytes, err := json.Marshal(errorPayload)
	if err != nil {
		payloadBytes = []byte(`{"error": "internal error"}`)
	}
	return asyncapi.ResultMessage{
		ID:       req.ReqID(),
		Payload:  string(payloadBytes),
		Routing:  routing,
		Metadata: req.ReqMetadata(),
	}
}

func CreateDeadlineExceededResultMessage(req asyncapi.Request, routing asyncapi.InternalRouting) asyncapi.ResultMessage {
	return CreateErrorResultMessage(req, routing, "deadline exceeded")
}

func inferenceErrorCategory(err error) string {
	var inferenceErr asyncapi.InferenceError
	if errors.As(err, &inferenceErr) {
		return string(inferenceErr.Category())
	}
	return string(asyncapi.ErrCategoryUnknown)
}

// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
func expBackoffDuration(retryCount int, secondsToDeadline int) float64 {
	if secondsToDeadline <= 0 {
		return 0
	}

	capLevel := math.Min(float64(maxDelaySeconds), float64(secondsToDeadline))

	// exponential growth with cap
	backoff := float64(baseDelaySeconds) * math.Pow(2, float64(retryCount))
	temp := math.Min(capLevel, backoff)

	if temp <= 0 {
		return 0
	}

	// equal jitter: [temp/2, temp)
	half := temp / 2
	return half + rand.Float64()*half // #nosec G404 -- non-security jitter, crypto/rand unnecessary
}
