package asyncworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const (
	baseDelaySeconds = 2
	maxDelaySeconds  = 60

	maxErrorResponseBodyBytes = 4096
)

func Worker(ctx context.Context, characteristics pipeline.Characteristics, client asyncapi.InferenceClient, requestChannel chan pipeline.EmbelishedRequestMessage,
	retryChannel chan pipeline.RetryMessage, resultChannel chan asyncapi.ResultMessage, requestTimeout time.Duration) {

	logger := log.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			logger.V(logutil.DEFAULT).Info("Worker finishing.")
			return
		case msg := <-requestChannel:
			func() {
				// Fire any Releases accumulated on the message (by gates
				// upstream or by the merge policy) on any terminal path
				// out of this message's processing — success, fatal error,
				// retry, validation failure, or ctx cancellation.
				defer msg.FireReleases()

				if msg.InternalRequest == nil || msg.PublicRequest == nil {
					return
				}
				if msg.RetryCount == 0 {
					// Only count first attempt as a new request.
					metrics.AsyncReqs.Inc()
				}
				payloadBytes := validateAndMarshal(ctx, resultChannel, msg)
				if payloadBytes == nil {
					return
				}

				// Run the per-subscription DispatchGate.Budget at the worker.
				// Legacy path: gates that implement the DispatchGate interface
				// use the Budget/Acquire pattern.
				if msg.Gate != nil {
					if attrGate, ok := msg.Gate.(pipeline.AttributeGate); ok {
						var metadata map[string]string
						if msg.PublicRequest != nil {
							metadata = msg.PublicRequest.ReqMetadata()
						}
						allowed, release, err := attrGate.Acquire(ctx, metadata)
						if err != nil {
							retryMessageWithReason(ctx, msg, retryChannel, resultChannel, time.Second, "gate_error")
							return
						}
						if !allowed {
							retryMessageWithReason(ctx, msg, retryChannel, resultChannel, 30*time.Second, "gate_refused")
							return
						}
						if release != nil {
							msg.AttachRelease(release)
						}
					}
				}

				// Using a function object for easy boundaries for 'return' and 'defer'!
				sendInferenceRequest := func() {
					// Create a per-request context bounded by both the message deadline
					// and the configured request timeout, whichever comes first.
					reqDeadline := time.Now().Add(requestTimeout)
					if dline := msg.PublicRequest.ReqDeadline(); dline > 0 {
						if msgDeadline := time.Unix(dline, 0); msgDeadline.Before(reqDeadline) {
							reqDeadline = msgDeadline
						}
					}
					reqCtx, cancel := context.WithDeadline(ctx, reqDeadline)
					defer cancel()

					logger.V(logutil.DEBUG).Info("Sending inference request", "url", msg.RequestURL)
					responseBody, err := client.SendRequest(reqCtx, msg.RequestURL, msg.HttpHeaders, payloadBytes)

					if err == nil {
						// Success - got a valid response
						metrics.SuccessfulReqs.Inc()
						select {
						case resultChannel <- asyncapi.ResultMessage{
							ID:       msg.PublicRequest.ReqID(),
							Payload:  string(responseBody),
							Routing:  msg.InternalRouting,
							Metadata: msg.PublicRequest.ReqMetadata(),
							Labels:   msg.Labels,
						}:
						case <-ctx.Done():
						}
						return
					}

					// Check if error implements InferenceError
					var inferenceErr asyncapi.InferenceError
					errorCategory := asyncapi.ErrCategoryUnknown
					isInferenceErr := errors.As(err, &inferenceErr)
					if isInferenceErr {
						errorCategory = inferenceErr.Category()
					}
					if !isInferenceErr || errorCategory.Fatal() {
						// Unknown error type or fatal error - fail immediately
						logger.V(logutil.DEFAULT).Error(err, "Inference request failed",
							"id", msg.PublicRequest.ReqID(),
							"category", string(errorCategory),
							"retryCount", msg.RetryCount)
						metrics.FailedReqs.Inc()
						select {
						case resultChannel <- CreateErrorResultMessage(msg.PublicRequest, msg.InternalRouting, inferenceErrorMessage(err, responseBody)):
						case <-ctx.Done():
						}
						return
					}

					// Retryable error - check if it's due to rate limiting
					retryReason := ""
					if inferenceErr.Category().Sheddable() {
						metrics.SheddedRequests.Inc()
						retryReason = "rate_limit_429"
					}
					// Pass server-specified Retry-After duration if available.
					var retryAfter time.Duration
					var clientErr *asyncapi.ClientError
					if errors.As(err, &clientErr) {
						retryAfter = clientErr.RetryAfter
					}
					if !errorCategory.Sheddable() {
						logger.V(logutil.DEFAULT).Info("Inference request will retry",
							"id", msg.PublicRequest.ReqID(),
							"category", string(errorCategory),
							"retryCount", msg.RetryCount,
							"retryAfter", retryAfter.String(),
							"error", err.Error())
					}
					retryMessageWithReason(ctx, msg, retryChannel, resultChannel, retryAfter, retryReason)
				}
				sendInferenceRequest()
			}()
		}
	}
}

// parsing and validating payload. On failure puts an error msg on the result-channel and returns nil
func validateAndMarshal(ctx context.Context, resultChannel chan asyncapi.ResultMessage, msg pipeline.EmbelishedRequestMessage) []byte {
	if msg.PublicRequest == nil {
		return nil
	}
	r := msg.PublicRequest
	deadline := r.ReqDeadline()
	if deadline <= 0 {
		metrics.FailedReqs.Inc()
		select {
		case resultChannel <- CreateErrorResultMessage(r, msg.InternalRouting, "Failed: deadline is missing or invalid (Unix seconds)."):
		case <-ctx.Done():
		}
		return nil
	}

	if deadline < time.Now().Unix() {
		metrics.ExceededDeadlineReqs.Inc()
		select {
		case resultChannel <- CreateDeadlineExceededResultMessage(r, msg.InternalRouting):
		case <-ctx.Done():
		}
		return nil
	}

	payloadBytes, err := json.Marshal(payloadWithModelNameOverride(r.ReqPayload(), msg.ModelNameOverride))
	if err != nil {
		metrics.FailedReqs.Inc()
		select {
		case resultChannel <- CreateErrorResultMessage(r, msg.InternalRouting, fmt.Sprintf("Failed to marshal message's payload: %s", err.Error())):
		case <-ctx.Done():
		}
		return nil
	}
	return payloadBytes
}

func payloadWithModelNameOverride(payload map[string]any, modelNameOverride string) map[string]any {
	if modelNameOverride == "" {
		return payload
	}
	rewrittenPayload := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		rewrittenPayload[key] = value
	}
	rewrittenPayload["model"] = modelNameOverride
	return rewrittenPayload
}

// If it is not after deadline, just publish again.
func retryMessage(ctx context.Context, msg pipeline.EmbelishedRequestMessage, retryChannel chan pipeline.RetryMessage, resultChannel chan asyncapi.ResultMessage, retryAfter time.Duration) {
	retryMessageWithReason(ctx, msg, retryChannel, resultChannel, retryAfter, "")
}

func retryMessageWithReason(ctx context.Context, msg pipeline.EmbelishedRequestMessage, retryChannel chan pipeline.RetryMessage, resultChannel chan asyncapi.ResultMessage, retryAfter time.Duration, retryReason string) {
	if msg.PublicRequest == nil {
		return
	}
	deadline := msg.PublicRequest.ReqDeadline()
	secondsToDeadline := deadline - time.Now().Unix()
	if secondsToDeadline <= 0 {
		metrics.ExceededDeadlineReqs.Inc()
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
		metrics.ExceededDeadlineReqs.Inc()
		select {
		case resultChannel <- CreateDeadlineExceededResultMessage(msg.PublicRequest, msg.InternalRouting):
		case <-ctx.Done():
		}
		return
	}

	msg.RetryCount++
	metrics.Retries.Inc()
	select {
	case retryChannel <- pipeline.RetryMessage{
		EmbelishedRequestMessage: msg,
		BackoffDurationSeconds:   finalDuration,
		RetryReason:              retryReason,
	}:
	case <-ctx.Done():
	}
}

// inferenceErrorMessage builds the error message for an inference error.
func inferenceErrorMessage(err error, responseBody []byte) string {
	message := fmt.Sprintf("Failed to send request to inference: %s", err.Error())
	if len(responseBody) == 0 {
		return message
	}
	body := responseBody
	if len(body) > maxErrorResponseBodyBytes {
		body = body[:maxErrorResponseBodyBytes]
		return fmt.Sprintf("%s; response body: %s...<truncated>", message, string(body))
	}
	return fmt.Sprintf("%s; response body: %s", message, string(body))
}

// CreateErrorResultMessage builds a ResultMessage using the public request identity;
// metadata is read directly from req.ReqMetadata().
func CreateErrorResultMessage(req asyncapi.Request, routing asyncapi.InternalRouting, errMsg string) asyncapi.ResultMessage {
	errorPayload := map[string]any{
		"error":       errMsg,
		"retry_count": routing.RetryCount,
	}
	payloadBytes, err := json.Marshal(errorPayload)
	if err != nil {
		payloadBytes = []byte(`{"error": "internal error"}`)
	}
	metadata := make(map[string]string, len(req.ReqMetadata())+1)
	for k, v := range req.ReqMetadata() {
		metadata[k] = v
	}
	metadata["retry_count"] = strconv.Itoa(routing.RetryCount)
	return asyncapi.ResultMessage{
		ID:       req.ReqID(),
		Payload:  string(payloadBytes),
		Routing:  routing,
		Metadata: metadata,
		Labels:   routing.Labels,
	}
}

func CreateDeadlineExceededResultMessage(req asyncapi.Request, routing asyncapi.InternalRouting) asyncapi.ResultMessage {
	return CreateErrorResultMessage(req, routing, "deadline exceeded")
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
