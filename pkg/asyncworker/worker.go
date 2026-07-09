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
	"github.com/llm-d-incubation/llm-d-async/pkg/asyncworker/transform"
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

func Worker(consumeCtx, requestCtx context.Context, characteristics pipeline.Characteristics, client asyncapi.InferenceClient, requestChannel chan pipeline.EmbelishedRequestMessage,
	retryChannel chan pipeline.RetryMessage, resultChannel chan asyncapi.ResultMessage, requestTimeout time.Duration, transforms *transform.Chain) {
	WorkerWithGate(consumeCtx, requestCtx, characteristics, client, requestChannel, retryChannel, resultChannel, requestTimeout, transforms, nil)
}

func WorkerWithGate(consumeCtx, requestCtx context.Context, characteristics pipeline.Characteristics, client asyncapi.InferenceClient, requestChannel chan pipeline.EmbelishedRequestMessage,
	retryChannel chan pipeline.RetryMessage, resultChannel chan asyncapi.ResultMessage, requestTimeout time.Duration, transforms *transform.Chain, poolGate pipeline.Gate) {

	logger := log.FromContext(requestCtx)
	for {
		select {
		case <-consumeCtx.Done():
			logger.V(logutil.DEFAULT).Info("Worker finishing, draining request channel.")
			idle := time.NewTimer(100 * time.Millisecond)
			defer idle.Stop()
			for {
				select {
				case msg, ok := <-requestChannel:
					if !ok {
						return
					}
					if msg.InternalRequest == nil || msg.PublicRequest == nil {
						continue
					}
					metrics.DecQueueDepth(msg.QueueID, msg.RequestQueueName, msg.WorkerPoolID)
					retryMsg := pipeline.RetryMessage{
						EmbelishedRequestMessage: msg,
						BackoffDurationSeconds:   0,
					}
					// Safe to block: retryWorker is guaranteed to outlive Workers
					// (impl.Shutdown runs after wg.Wait in cmd/main.go).
					retryChannel <- retryMsg
					if !idle.Stop() {
						<-idle.C
					}
					idle.Reset(100 * time.Millisecond)
				case <-idle.C:
					return
				}
			}
		case msg := <-requestChannel:
			if msg.InternalRequest == nil || msg.PublicRequest == nil {
				continue
			}
			queueID := msg.QueueID
			queueName := msg.RequestQueueName
			metrics.DecQueueDepth(queueID, queueName, msg.WorkerPoolID)
			if !msg.IngestionTime.IsZero() {
				metrics.RecordQueueResidenceTime(float64(time.Since(msg.IngestionTime).Milliseconds()), queueID, queueName, msg.WorkerPoolID)
			}

			processMessage := func() {
				if msg.RetryCount == 0 {
					metrics.RecordAsyncReq(queueID, queueName, msg.WorkerPoolID)
				}

				payloadBytes := validateAndMarshal(requestCtx, resultChannel, msg, transforms)
				if payloadBytes == nil {
					return
				}

				var poolReleases []pipeline.GateReleaseFunc
				defer func() {
					pipeline.ReleaseGateReleases(poolReleases)
				}()

				if poolGate != nil {
					reqDeadline := time.Now().Add(requestTimeout)
					if dline := msg.PublicRequest.ReqDeadline(); dline > 0 {
						if msgDeadline := time.Unix(dline, 0); msgDeadline.Before(reqDeadline) {
							reqDeadline = msgDeadline
						}
					}
					gateCtx, cancelGate := context.WithDeadline(requestCtx, reqDeadline)
					defer cancelGate()

					var verdict pipeline.Verdict
					var err error
					for {
						verdict, err = poolGate.Apply(gateCtx, msg.InternalRequest, &poolReleases)
						if err != nil {
							if errors.Is(err, context.DeadlineExceeded) || gateCtx.Err() != nil {
								if requestCtx.Err() != nil && !errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
									retryChannel <- pipeline.RetryMessage{
										EmbelishedRequestMessage: msg,
										BackoffDurationSeconds:   0,
									}
									return
								}
								metrics.RecordExceededDeadlineReq(queueID, queueName, msg.WorkerPoolID)
								select {
								case resultChannel <- asyncapi.NewDeadlineExceededResult(msg.PublicRequest, msg.InternalRouting):
								case <-requestCtx.Done():
								}
								return
							}
							select {
							case resultChannel <- asyncapi.NewErrorResult(msg.PublicRequest, msg.InternalRouting, asyncapi.ErrCodeGateError, fmt.Sprintf("Pool gating error: %s", err.Error())):
							case <-requestCtx.Done():
							}
							return
						}

						if verdict.Action == pipeline.ActionContinue {
							break
						}

						if verdict.Action == pipeline.ActionDrop {
							var resultMsg asyncapi.ResultMessage
							if verdict.Result != nil {
								resultMsg = *verdict.Result
							} else {
								resultMsg = asyncapi.NewGateDroppedResult(msg.PublicRequest, msg.InternalRouting)
							}
							select {
							case resultChannel <- resultMsg:
							case <-requestCtx.Done():
							}
							return
						}

						if verdict.Action == pipeline.ActionRefuse {
							select {
							case retryChannel <- pipeline.RetryMessage{
								EmbelishedRequestMessage: msg,
								BackoffDurationSeconds:   0,
							}:
							case <-requestCtx.Done():
							}
							return
						}

						// ActionWait: park/wait and retry in-memory
						if verdict.Action == pipeline.ActionWait {
							select {
							case <-gateCtx.Done():
								if requestCtx.Err() != nil && !errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
									retryChannel <- pipeline.RetryMessage{
										EmbelishedRequestMessage: msg,
										BackoffDurationSeconds:   0,
									}
									return
								}
								metrics.RecordExceededDeadlineReq(queueID, queueName, msg.WorkerPoolID)
								select {
								case resultChannel <- asyncapi.NewDeadlineExceededResult(msg.PublicRequest, msg.InternalRouting):
								case <-requestCtx.Done():
								}
								return
							case <-time.After(50 * time.Millisecond):
								// poll again
							}
						}
					}
				}

				metrics.IncInflight(queueID, queueName, msg.WorkerPoolID)
				defer metrics.DecInflight(queueID, queueName, msg.WorkerPoolID)

				sendInferenceRequest := func() {
					reqCtx := requestCtx
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

					reqDeadline := time.Now().Add(requestTimeout)
					if dline := msg.PublicRequest.ReqDeadline(); dline > 0 {
						if msgDeadline := time.Unix(dline, 0); msgDeadline.Before(reqDeadline) {
							reqDeadline = msgDeadline
						}
					}
					reqCtx, cancel := context.WithDeadline(reqCtx, reqDeadline)
					defer cancel()

					// Apply the request body-transform chain. If a transform handles
					// the message we send its body and Content-Type; otherwise the
					// default JSON payload is sent unchanged. A transform error is
					// fatal/non-retryable.
					sendPayload := payloadBytes
					sendHeaders := msg.HttpHeaders
					if body, contentType, handled, terr := transforms.Apply(payloadBytes, msg.PublicRequest.ReqMetadata()); terr != nil {
						span.RecordError(terr)
						span.SetStatus(codes.Error, "request transform failed")
						span.SetAttributes(attribute.String(uotel.AttrErrorCategory, string(asyncapi.ErrCategoryInvalidReq)))
						metrics.RecordFailedReq(queueID, queueName, msg.WorkerPoolID)
						select {
						case resultChannel <- asyncapi.NewErrorResult(msg.PublicRequest, msg.InternalRouting, asyncapi.ErrCodeInvalidRequest, fmt.Sprintf("Failed to transform request body: %s", terr.Error())):
						case <-requestCtx.Done():
						}
						return
					} else if handled {
						sendPayload = body
						sendHeaders = headersWithContentType(msg.HttpHeaders, contentType)
					}

					logger.V(logutil.DEBUG).Info("Sending inference request", "url", msg.RequestURL)
					inferenceStart := time.Now()
					resp, err := client.SendRequest(reqCtx, msg.RequestURL, sendHeaders, sendPayload)
					metrics.RecordInferenceLatency(float64(time.Since(inferenceStart).Milliseconds()), queueID, queueName, msg.WorkerPoolID)

					if err == nil {
						metrics.RecordSuccessfulReq(queueID, queueName, msg.WorkerPoolID)
						select {
						case resultChannel <- asyncapi.NewHTTPResult(msg.PublicRequest, msg.InternalRouting, resp.StatusCode, resp.Body):
						case <-requestCtx.Done():
						}
						return
					}

					if requestCtx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
						_, bgSpan := uotel.DetachedContext(reqCtx, "re-enqueue")
						bgSpan.SetAttributes(attribute.String(uotel.AttrRequestID, msg.PublicRequest.ReqID()))
						defer bgSpan.End()
						retryChannel <- pipeline.RetryMessage{
							EmbelishedRequestMessage: msg,
							BackoffDurationSeconds:   0,
						}
						return
					}

					var inferenceErr asyncapi.InferenceError
					if !errors.As(err, &inferenceErr) || inferenceErr.Category().Fatal() {
						span.RecordError(err)
						span.SetStatus(codes.Error, "inference request failed")
						span.SetAttributes(attribute.String(uotel.AttrErrorCategory, inferenceErrorCategory(err)))
						metrics.RecordFailedReq(queueID, queueName, msg.WorkerPoolID)

						var resultMsg asyncapi.ResultMessage
						if resp != nil && resp.StatusCode > 0 {
							resultMsg = asyncapi.NewHTTPResult(msg.PublicRequest, msg.InternalRouting, resp.StatusCode, resp.Body)
						} else {
							resultMsg = asyncapi.NewErrorResult(msg.PublicRequest, msg.InternalRouting,
								asyncapi.ErrCodeInferenceError,
								fmt.Sprintf("Failed to send request to inference: %s", err.Error()))
						}
						select {
						case resultChannel <- resultMsg:
						case <-requestCtx.Done():
						}
						return
					}

					if inferenceErr.Category().Sheddable() {
						metrics.RecordSheddedReq(queueID, queueName, msg.WorkerPoolID)
					}
					span.SetAttributes(attribute.String(uotel.AttrErrorCategory, string(inferenceErr.Category())))
					var retryAfter time.Duration
					var clientErr *asyncapi.ClientError
					if errors.As(err, &clientErr) {
						retryAfter = clientErr.RetryAfter
					}
					var lastResp asyncapi.InferenceResponse
					if resp != nil {
						lastResp = *resp
					}
					retryMessage(requestCtx, msg, retryChannel, resultChannel, retryAfter, lastResp)
				}
				sendInferenceRequest()
			}
			processMessage()
		}
	}
}

// parsing and validating payload. On failure puts an error msg on the result-channel and returns nil
func validateAndMarshal(ctx context.Context, resultChannel chan asyncapi.ResultMessage, msg pipeline.EmbelishedRequestMessage, transforms *transform.Chain) []byte {
	if msg.PublicRequest == nil {
		return nil
	}
	queueID := msg.QueueID
	queueName := msg.RequestQueueName
	r := msg.PublicRequest
	deadline := r.ReqDeadline()
	if deadline <= 0 {
		metrics.RecordFailedReq(queueID, queueName, msg.WorkerPoolID)
		select {
		case resultChannel <- asyncapi.NewErrorResult(r, msg.InternalRouting, asyncapi.ErrCodeInvalidRequest, "Failed: deadline is missing or invalid (Unix seconds)."):
		case <-ctx.Done():
		}
		return nil
	}

	if deadline < time.Now().Unix() {
		metrics.RecordExceededDeadlineReq(queueID, queueName, msg.WorkerPoolID)
		select {
		case resultChannel <- asyncapi.NewDeadlineExceededResult(r, msg.InternalRouting):
		case <-ctx.Done():
		}
		return nil
	}

	payloadBytes, err := json.Marshal(r.ReqPayload())
	if err != nil {
		metrics.RecordFailedReq(queueID, queueName, msg.WorkerPoolID)
		select {
		case resultChannel <- asyncapi.NewErrorResult(r, msg.InternalRouting, asyncapi.ErrCodeInvalidRequest, fmt.Sprintf("Failed to marshal message's payload: %s", err.Error())):
		case <-ctx.Done():
		}
		return nil
	}

	// Pre-dispatch transform validation (e.g. signed object URL expiry). A
	// validation failure is fatal/non-retryable, so we surface an error result
	// rather than re-enqueue. A nil chain validates successfully.
	if err := transforms.Validate(payloadBytes, r.ReqMetadata(), deadline); err != nil {
		metrics.RecordFailedReq(queueID, queueName, msg.WorkerPoolID)
		select {
		case resultChannel <- asyncapi.NewErrorResult(r, msg.InternalRouting, asyncapi.ErrCodeInvalidRequest, fmt.Sprintf("Failed to validate request for transform: %s", err.Error())):
		case <-ctx.Done():
		}
		return nil
	}

	return payloadBytes
}

// retryMessage re-enqueues the request for another attempt, or emits a final
// result if the deadline cannot accommodate another retry.
//
// lastResp carries the most recent HTTP response (if any) so that when retries
// are exhausted the actual HTTP status/body is surfaced rather than a generic
// "deadline exceeded" non-HTTP error.
func retryMessage(ctx context.Context, msg pipeline.EmbelishedRequestMessage, retryChannel chan pipeline.RetryMessage, resultChannel chan asyncapi.ResultMessage, retryAfter time.Duration, lastResp asyncapi.InferenceResponse) {
	if msg.PublicRequest == nil {
		return
	}
	queueID := msg.QueueID
	queueName := msg.RequestQueueName
	deadline := msg.PublicRequest.ReqDeadline()
	secondsToDeadline := deadline - time.Now().Unix()
	if secondsToDeadline <= 0 {
		metrics.RecordExceededDeadlineReq(queueID, queueName, msg.WorkerPoolID)
		select {
		case resultChannel <- deadlineExhaustedResult(msg, lastResp):
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
		metrics.RecordExceededDeadlineReq(queueID, queueName, msg.WorkerPoolID)
		select {
		case resultChannel <- deadlineExhaustedResult(msg, lastResp):
		case <-ctx.Done():
		}
		return
	}

	msg.RetryCount++
	metrics.RecordRetry(queueID, queueName, msg.WorkerPoolID)
	select {
	case retryChannel <- pipeline.RetryMessage{
		EmbelishedRequestMessage: msg,
		BackoffDurationSeconds:   finalDuration,
	}:
	case <-ctx.Done():
	}
}

// deadlineExhaustedResult returns the appropriate result message when retries
// are exhausted. If we have a last HTTP response, surface it (preserving the
// actual status/body). Otherwise emit a non-HTTP deadline-exceeded error.
func deadlineExhaustedResult(msg pipeline.EmbelishedRequestMessage, lastResp asyncapi.InferenceResponse) asyncapi.ResultMessage {
	if lastResp.StatusCode > 0 {
		return asyncapi.NewHTTPResult(msg.PublicRequest, msg.InternalRouting, lastResp.StatusCode, lastResp.Body)
	}
	return asyncapi.NewDeadlineExceededResult(msg.PublicRequest, msg.InternalRouting)
}

// headersWithContentType returns a copy of headers with Content-Type set to
// contentType. The original map is not mutated, so the per-message headers stay
// intact across retries.
func headersWithContentType(headers map[string]string, contentType string) map[string]string {
	out := make(map[string]string, len(headers)+1)
	for k, v := range headers {
		out[k] = v
	}
	out["Content-Type"] = contentType
	return out
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
