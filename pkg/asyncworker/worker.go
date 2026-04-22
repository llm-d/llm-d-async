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
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const (
	baseDelaySeconds = 2
	maxDelaySeconds  = 60
)

func Worker(ctx context.Context, characteristics asyncapi.Characteristics, client asyncapi.InferenceClient, requestChannel chan asyncapi.EmbelishedRequestMessage,
	retryChannel chan asyncapi.RetryMessage, resultChannel chan asyncapi.ResultMessage, requestTimeout time.Duration) {

	logger := log.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			logger.V(logutil.DEFAULT).Info("Worker finishing.")
			return
		case msg := <-requestChannel:
			if msg.RetryCount == 0 {
				// Only count first attempt as a new request.
				metrics.AsyncReqs.Inc()
			}
			payloadBytes := validateAndMarshal(ctx, resultChannel, msg.RequestMessage)
			if payloadBytes == nil {
				continue
			}

			// Using a function object for easy boundaries for 'return' and 'defer'!
			sendInferenceRequest := func() {
				// Create a per-request context bounded by both the message deadline
				// and the configured request timeout, whichever comes first.
				reqDeadline := time.Now().Add(requestTimeout)
				if deadline, err := strconv.ParseInt(msg.DeadlineUnixSec, 10, 64); err == nil {
					if msgDeadline := time.Unix(deadline, 0); msgDeadline.Before(reqDeadline) {
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
						Id:       msg.Id,
						Payload:  string(responseBody),
						Metadata: msg.Metadata,
					}:
					case <-ctx.Done():
					}
					return
				}

				// Check if error implements InferenceError
				var inferenceErr asyncapi.InferenceError
				if !errors.As(err, &inferenceErr) || inferenceErr.Category().Fatal() {
					// Unknown error type or fatal error - fail immediately
					metrics.FailedReqs.Inc()
					select {
					case resultChannel <- CreateErrorResultMessage(msg.RequestMessage, fmt.Sprintf("Failed to send request to inference: %s", err.Error())):
					case <-ctx.Done():
					}
					return
				}

				// Retryable error - check if it's due to rate limiting
				if inferenceErr.Category().Sheddable() {
					metrics.SheddedRequests.Inc()
				}
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
func validateAndMarshal(ctx context.Context, resultChannel chan asyncapi.ResultMessage, msg asyncapi.RequestMessage) []byte {
	deadline, err := strconv.ParseInt(msg.DeadlineUnixSec, 10, 64)
	if err != nil {
		metrics.FailedReqs.Inc()
		select {
		case resultChannel <- CreateErrorResultMessage(msg, "Failed to parse deadline, should be in Unix seconds."):
		case <-ctx.Done():
		}
		return nil
	}

	if deadline < time.Now().Unix() {
		metrics.ExceededDeadlineReqs.Inc()
		select {
		case resultChannel <- CreateDeadlineExceededResultMessage(msg):
		case <-ctx.Done():
		}
		return nil
	}

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		metrics.FailedReqs.Inc()
		select {
		case resultChannel <- CreateErrorResultMessage(msg, fmt.Sprintf("Failed to marshal message's payload: %s", err.Error())):
		case <-ctx.Done():
		}
		return nil
	}
	return payloadBytes
}

// If it is not after deadline, just publish again.
func retryMessage(ctx context.Context, msg asyncapi.EmbelishedRequestMessage, retryChannel chan asyncapi.RetryMessage, resultChannel chan asyncapi.ResultMessage, retryAfter time.Duration) {
	deadline, err := strconv.ParseInt(msg.DeadlineUnixSec, 10, 64)
	if err != nil { // Can't really happen because this was already parsed in the past. But we don't care to have this branch.
		select {
		case resultChannel <- CreateErrorResultMessage(msg.RequestMessage, "Failed to parse deadline. Should be in Unix time"):
		case <-ctx.Done():
		}
		return
	}
	secondsToDeadline := deadline - time.Now().Unix()
	if secondsToDeadline <= 0 {
		metrics.ExceededDeadlineReqs.Inc()
		select {
		case resultChannel <- CreateDeadlineExceededResultMessage(msg.RequestMessage):
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
		case resultChannel <- CreateDeadlineExceededResultMessage(msg.RequestMessage):
		case <-ctx.Done():
		}
		return
	}

	msg.RetryCount++
	metrics.Retries.Inc()
	select {
	case retryChannel <- asyncapi.RetryMessage{
		EmbelishedRequestMessage: msg,
		BackoffDurationSeconds:   finalDuration,
	}:
	case <-ctx.Done():
	}
}
func CreateErrorResultMessage(msg asyncapi.RequestMessage, errMsg string) asyncapi.ResultMessage {
	errorPayload := map[string]string{"error": errMsg}
	payloadBytes, err := json.Marshal(errorPayload)
	if err != nil {
		// Fallback to a simple error message if marshaling fails
		payloadBytes = []byte(`{"error": "internal error"}`)
	}
	return asyncapi.ResultMessage{
		Id:       msg.Id,
		Payload:  string(payloadBytes),
		Metadata: msg.Metadata,
	}
}

func CreateDeadlineExceededResultMessage(msg asyncapi.RequestMessage) asyncapi.ResultMessage {
	return CreateErrorResultMessage(msg, "deadline exceeded")
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
	return half + rand.Float64()*half
}
