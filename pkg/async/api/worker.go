package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var baseDelaySeconds = 2

func Worker(ctx context.Context, characteristics Characteristics, client InferenceClient, requestChannel chan EmbelishedRequestMessage,
	retryChannel chan RetryMessage, resultChannel chan ResultMessage, requestTimeout time.Duration) {

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
			payloadBytes := validateAndMarshall(resultChannel, msg.RequestMessage)
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

				logger.V(logutil.DEBUG).Info("Sending inference request: " + msg.RequestURL)
				responseBody, err := client.SendRequest(reqCtx, msg.RequestURL, msg.HttpHeaders, payloadBytes)

				if err == nil {
					// Success - got a valid response
					metrics.SuccessfulReqs.Inc()
					resultChannel <- ResultMessage{
						Id:       msg.Id,
						Payload:  string(responseBody),
						Metadata: msg.Metadata,
					}
					return
				}

				// Check if error implements InferenceError
				var inferenceErr InferenceError
				if !errors.As(err, &inferenceErr) || inferenceErr.Category().Fatal() {
					// Unknown error type or fatal error - fail immediately
					metrics.FailedReqs.Inc()
					resultChannel <- CreateErrorResultMessage(msg.RequestMessage, fmt.Sprintf("Failed to send request to inference: %s", err.Error()))
					return
				}

				// Retryable error - check if it's due to rate limiting
				if inferenceErr.Category().Sheddable() {
					metrics.SheddedRequests.Inc()
				}
				retryMessage(msg, retryChannel, resultChannel)
			}
			sendInferenceRequest()
		}
	}
}

// parsing and validating payload. On failure puts an error msg on the result-channel and returns nil
func validateAndMarshall(resultChannel chan ResultMessage, msg RequestMessage) []byte {
	deadline, err := strconv.ParseInt(msg.DeadlineUnixSec, 10, 64)
	if err != nil {
		metrics.FailedReqs.Inc()
		resultChannel <- CreateErrorResultMessage(msg, "Failed to parse deadline, should be in Unix seconds.")
		return nil
	}

	if deadline < time.Now().Unix() {
		metrics.ExceededDeadlineReqs.Inc()
		resultChannel <- CreateDeadlineExceededResultMessage(msg)
		return nil
	}

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		metrics.FailedReqs.Inc()
		resultChannel <- CreateErrorResultMessage(msg, fmt.Sprintf("Failed to marshal message's payload: %s", err.Error()))
		return nil
	}
	return payloadBytes
}

// If it is not after deadline, just publish again.
func retryMessage(msg EmbelishedRequestMessage, retryChannel chan RetryMessage, resultChannel chan ResultMessage) {
	deadline, err := strconv.ParseInt(msg.DeadlineUnixSec, 10, 64)
	if err != nil { // Can't really happen because this was already parsed in the past. But we don't care to have this branch.
		resultChannel <- CreateErrorResultMessage(msg.RequestMessage, "Failed to parse deadline. Should be in Unix time")
		return
	}
	secondsToDeadline := deadline - time.Now().Unix()
	if secondsToDeadline < 0 {
		metrics.ExceededDeadlineReqs.Inc()
		resultChannel <- CreateDeadlineExceededResultMessage(msg.RequestMessage)
	} else {
		msg.RetryCount++
		finalDuration := expBackoffDuration(msg.RetryCount, int(secondsToDeadline))
		metrics.Retries.Inc()
		retryChannel <- RetryMessage{
			EmbelishedRequestMessage: msg,
			BackoffDurationSeconds:   finalDuration,
		}

	}

}
func CreateErrorResultMessage(msg RequestMessage, errMsg string) ResultMessage {
	errorPayload := map[string]string{"error": errMsg}
	payloadBytes, err := json.Marshal(errorPayload)
	if err != nil {
		// Fallback to a simple error message if marshaling fails
		payloadBytes = []byte(`{"error": "internal error"}`)
	}
	return ResultMessage{
		Id:       msg.Id,
		Payload:  string(payloadBytes),
		Metadata: msg.Metadata,
	}
}

func CreateDeadlineExceededResultMessage(msg RequestMessage) ResultMessage {
	return CreateErrorResultMessage(msg, "deadline exceeded")
}

func expBackoffDuration(retryCount int, secondsToDeadline int) float64 {
	backoffDurationSeconds := math.Min(
		float64(baseDelaySeconds)*(math.Pow(2, float64(retryCount))),
		float64(secondsToDeadline))

	jitter := rand.Float64() - 0.5
	finalDuration := backoffDurationSeconds + jitter
	if finalDuration < 0 {
		finalDuration = 0
	}
	return finalDuration
}
