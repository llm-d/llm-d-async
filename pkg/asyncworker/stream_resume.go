package asyncworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"time"

	"github.com/go-logr/logr"
	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/api/continuation"
	uotel "github.com/llm-d/llm-d-async/internal/otel"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const (
	shutdownCheckpointFlushTimeout = 2 * time.Second
	evictionDefaultMinBackoffSec   = 0.25
	evictionDefaultJitterSec       = 0.50
)

type checkpointStoreKey struct{}

// WithCheckpointStore attaches a lease-fenced CheckpointStore to the worker context.
func WithCheckpointStore(ctx context.Context, store pipeline.CheckpointStore) context.Context {
	if store == nil {
		return ctx
	}
	return context.WithValue(ctx, checkpointStoreKey{}, store)
}

func checkpointStoreFromContext(ctx context.Context) pipeline.CheckpointStore {
	if ctx == nil {
		return nil
	}
	store, _ := ctx.Value(checkpointStoreKey{}).(pipeline.CheckpointStore)
	return store
}

// shouldUseStreamingResume checks whether msg and client qualify for streaming
// SSE dispatch and partial-generation continuation. Ineligible requests (such
// as n > 1, non-OpenAI endpoints, or body-transformed multipart payloads) fall
// back to standard non-streaming SendRequest.
func shouldUseStreamingResume(
	msg pipeline.EmbelishedRequestMessage,
	client asyncapi.InferenceClient,
	payloadBytes []byte,
	transformHandled bool,
) (asyncapi.StreamingInferenceClient, continuation.Endpoint, bool) {
	if !msg.Resume.IsEnabled() || transformHandled {
		return nil, "", false
	}
	streamClient, ok := client.(asyncapi.StreamingInferenceClient)
	if !ok {
		return nil, "", false
	}
	endpoint, ok := continuation.DetectEndpoint(msg.RequestURL)
	if !ok {
		return nil, "", false
	}
	if err := continuation.CheckEligibility(endpoint, payloadBytes); err != nil {
		return nil, "", false
	}
	return streamClient, endpoint, true
}

func executeStreamingInference(
	requestCtx context.Context,
	reqCtx context.Context,
	logger logr.Logger,
	span trace.Span,
	streamClient asyncapi.StreamingInferenceClient,
	endpoint continuation.Endpoint,
	msg pipeline.EmbelishedRequestMessage,
	sendHeaders map[string]string,
	originalPayload []byte,
	retryChannel chan pipeline.RetryMessage,
	resultChannel chan asyncapi.ResultMessage,
) {
	queueID := msg.QueueID
	queueName := msg.RequestQueueName
	poolID := msg.WorkerPoolID
	reqID := msg.PublicRequest.ReqID()
	reqToken := msg.RequestToken
	cfg := msg.Resume.Effective()
	contOpts := continuation.Options{
		MaxCheckpointTokens: cfg.MaxCheckpointTokens,
		TokenExact:          cfg.IsTokenExact(),
	}

	store := checkpointStoreFromContext(requestCtx)
	var priorState *continuation.State
	resumeCount := 0
	priorRecomputed := 0
	loadedInterruptReason := ""

	if store != nil {
		ckpt, loadErr := store.Load(reqCtx, queueName, reqID, reqToken)
		if loadErr != nil {
			logger.V(logutil.DEFAULT).Error(loadErr, "Failed to load continuation checkpoint, starting fresh", "id", reqID)
		} else if ckpt != nil {
			resumeCount = ckpt.ResumeCount
			priorRecomputed = ckpt.RecomputedPromptTokens
			loadedInterruptReason = ckpt.LastInterruptReason
			if len(ckpt.State) > 0 {
				var loaded continuation.State
				if uerr := json.Unmarshal(ckpt.State, &loaded); uerr != nil {
					logger.V(logutil.DEFAULT).Error(uerr, "Failed to unmarshal continuation checkpoint, starting fresh", "id", reqID)
				} else if loaded.HasProgress() {
					if loaded.Endpoint == "" {
						loaded.Endpoint = endpoint
					}
					if ckpt.ResumeCount >= cfg.MaxResumes {
						errMsg := fmt.Sprintf("Failed to resume interrupted request: max_resumes (%d) exhausted", cfg.MaxResumes)
						span.SetStatus(codes.Error, errMsg)
						metrics.RecordFailedReq(queueID, queueName, poolID)
						select {
						case resultChannel <- asyncapi.NewErrorResult(msg.PublicRequest, msg.InternalRouting, asyncapi.ErrCodeInferenceError, errMsg):
						case <-requestCtx.Done():
						}
						return
					}
					priorState = &loaded
				}
			}
		}
	}

	var attemptPayload []byte
	resumedThisAttempt := false
	if priorState != nil {
		reason := loadedInterruptReason
		if reason == "" {
			// Empty LastInterruptReason indicates the previous worker died mid-stream
			// after writing a periodic checkpoint and its lease was reclaimed.
			reason = pipeline.ResumeReasonRedelivery
		}
		contBody, budgetExhausted, buildErr := continuation.BuildContinuationRequest(originalPayload, priorState, contOpts)
		if buildErr != nil {
			logger.V(logutil.DEFAULT).Error(buildErr, "Continuation build failed, restarting fresh attempt", "id", reqID)
			priorState = nil
		} else if budgetExhausted {
			resumeCount++
			metrics.RecordResume(reason, int64(priorState.Usage.GeneratedTokens), queueID, queueName, poolID)
			continuation.MarkBudgetExhausted(priorState)
			assembled, aerr := continuation.AssembleResponse(priorState)
			if aerr == nil {
				metrics.RecordSuccessfulReq(queueID, queueName, poolID)
				if input, output, ok := ParseUsage(assembled, msg.RequestURL); ok {
					metrics.RecordTokens(input, output, queueID, queueName, poolID)
				}
				select {
				case resultChannel <- asyncapi.NewHTTPResult(msg.PublicRequest, msg.InternalRouting, 200, assembled):
				case <-requestCtx.Done():
				}
				return
			}
			logger.V(logutil.DEFAULT).Error(aerr, "Failed to assemble budget-exhausted response, restarting fresh", "id", reqID)
			priorState = nil
		} else {
			resumeCount++
			resumedThisAttempt = true
			metrics.RecordResume(reason, int64(priorState.Usage.GeneratedTokens), queueID, queueName, poolID)
			attemptPayload = contBody
		}
	}

	if len(attemptPayload) == 0 {
		prepBody, prepErr := continuation.PrepareStreamRequest(endpoint, originalPayload, contOpts)
		if prepErr != nil {
			span.RecordError(prepErr)
			span.SetStatus(codes.Error, "failed to prepare streaming request")
			metrics.RecordFailedReq(queueID, queueName, poolID)
			select {
			case resultChannel <- asyncapi.NewErrorResult(msg.PublicRequest, msg.InternalRouting, asyncapi.ErrCodeInvalidRequest, fmt.Sprintf("Failed to prepare streaming request: %s", prepErr.Error())):
			case <-requestCtx.Done():
			}
			return
		}
		attemptPayload = prepBody
	}

	acc := continuation.NewAccumulator(endpoint, priorState, contOpts)
	fenced := false
	lastSavedDelta := 0
	lastSavedAt := time.Now()

	persistCheckpoint := func(saveCtx context.Context, snap *continuation.State, interruptReason string) {
		if store == nil || fenced || snap == nil || !snap.HasProgress() {
			return
		}
		stateBytes, merr := json.Marshal(snap)
		if merr != nil {
			metrics.RecordCheckpointWrite(pipeline.CheckpointWriteError, 0, queueID, queueName, poolID)
			return
		}
		rec := pipeline.CheckpointRecord{
			Endpoint:               string(endpoint),
			State:                  stateBytes,
			ResumeCount:            resumeCount,
			GeneratedTokens:        snap.Usage.GeneratedTokens,
			RecomputedPromptTokens: snap.Usage.RecomputedPromptTokens,
			LastInterruptReason:    interruptReason,
			UpdatedAtUnixMs:        time.Now().UnixMilli(),
		}
		recBytes, _ := json.Marshal(rec)
		writeRes, serr := store.Save(saveCtx, queueName, reqID, reqToken, rec)
		if serr != nil {
			metrics.RecordCheckpointWrite(pipeline.CheckpointWriteError, 0, queueID, queueName, poolID)
			logger.V(logutil.DEBUG).Error(serr, "Checkpoint write failed (continuing stream)", "id", reqID)
			return
		}
		if writeRes == pipeline.CheckpointFenced {
			fenced = true
			metrics.RecordCheckpointWrite(pipeline.CheckpointWriteFenced, 0, queueID, queueName, poolID)
			return
		}
		metrics.RecordCheckpointWrite(pipeline.CheckpointWriteOK, len(recBytes), queueID, queueName, poolID)
	}

	logger.V(logutil.DEBUG).Info("Sending streaming inference request", "url", msg.RequestURL, "resumed", resumedThisAttempt, "resumeCount", resumeCount)
	metrics.RecordDispatchedReq(queueID, queueName, poolID)
	inferenceStart := time.Now()
	sendCtx, stopCancellationWatch := watchInflightCancellation(reqCtx, logger, msg)

	resp, err := streamClient.SendStreamRequest(
		sendCtx,
		msg.RequestURL,
		sendHeaders,
		attemptPayload,
		func(_ int, _ map[string]string, body io.Reader) ([]byte, error) {
			streamErr := acc.ConsumeStream(body, func(_ continuation.SSEEvent) error {
				if cErr := sendCtx.Err(); cErr != nil {
					return fmt.Errorf("stream context ended: %w", cErr)
				}
				deltaTokens := acc.AttemptDeltaTokens()
				now := time.Now()
				tokenDue := cfg.CheckpointIntervalTokens > 0 && (deltaTokens-lastSavedDelta) >= cfg.CheckpointIntervalTokens
				timeDue := cfg.CheckpointIntervalMs > 0 && deltaTokens > lastSavedDelta && now.Sub(lastSavedAt) >= time.Duration(cfg.CheckpointIntervalMs)*time.Millisecond
				if !fenced && (tokenDue || timeDue) {
					snap := acc.CheckpointSnapshot()
					persistCheckpoint(sendCtx, snap, "")
					lastSavedDelta = deltaTokens
					lastSavedAt = now
				}
				return nil
			})
			if streamErr != nil {
				return nil, fmt.Errorf("consume sse stream: %w", streamErr)
			}
			assembled, aerr := continuation.AssembleResponse(acc.FinalState())
			if aerr != nil {
				return nil, fmt.Errorf("assemble streaming response: %w", aerr)
			}
			return assembled, nil
		},
	)
	cancelledInFlight := stopCancellationWatch()
	metrics.RecordInferenceLatency(float64(time.Since(inferenceStart).Milliseconds()), queueID, queueName, poolID)

	if err == nil {
		finalState := acc.FinalState()
		if resumedThisAttempt {
			deltaRecomputed := int64(finalState.Usage.RecomputedPromptTokens - priorRecomputed)
			metrics.RecordResumeRecomputedPromptTokens(deltaRecomputed, queueID, queueName, poolID)
		}
		metrics.RecordSuccessfulReq(queueID, queueName, poolID)
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if input, output, ok := ParseUsage(resp.Body, msg.RequestURL); ok {
				metrics.RecordTokens(input, output, queueID, queueName, poolID)
			}
		}
		select {
		case resultChannel <- asyncapi.NewHTTPResult(msg.PublicRequest, msg.InternalRouting, resp.StatusCode, resp.Body):
		case <-requestCtx.Done():
		}
		return
	}

	snap := acc.CheckpointSnapshot()
	if resumedThisAttempt && snap != nil {
		deltaRecomputed := int64(snap.Usage.RecomputedPromptTokens - priorRecomputed)
		metrics.RecordResumeRecomputedPromptTokens(deltaRecomputed, queueID, queueName, poolID)
	}

	// 1. Graceful worker drain/shutdown: flush checkpoint on a short detached
	// context and immediately release the claim back to the origin queue. Any
	// stream error observed after requestCtx (drainCtx) is cancelled — including
	// ErrIncompleteStream when net/http closes the socket — must requeue cleanly.
	if requestCtx.Err() != nil && !cancelledInFlight {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), shutdownCheckpointFlushTimeout)
		persistCheckpoint(flushCtx, snap, pipeline.ResumeReasonShutdown)
		flushCancel()

		_, bgSpan := uotel.DetachedContext(reqCtx, "re-enqueue")
		bgSpan.SetAttributes(
			attribute.String(uotel.AttrRequestID, reqID),
			attribute.String(uotel.LegacyAttrRequestID, reqID),
		)
		defer bgSpan.End()
		retryChannel <- pipeline.RetryMessage{
			EmbelishedRequestMessage: msg,
			BackoffDurationSeconds:   0,
			ImmediateRequeue:         true,
		}
		return
	}

	// 2. Producer in-flight cancellation (#474): ackResult atomically drops the
	// claim and deletes any persisted checkpoint.
	if cancelledInFlight {
		select {
		case resultChannel <- asyncapi.NewCancelledResult(msg.PublicRequest, msg.InternalRouting):
		case <-requestCtx.Done():
		}
		return
	}

	// 3. Per-request deadline / timeout expired.
	if errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
		metrics.RecordExceededDeadlineReq(queueID, queueName, poolID)
		select {
		case resultChannel <- asyncapi.NewDeadlineExceededResult(msg.PublicRequest, msg.InternalRouting):
		case <-requestCtx.Done():
		}
		return
	}

	var clientErr *asyncapi.ClientError
	errors.As(err, &clientErr)

	// 4. Flow-control eviction (HTTP 429 with x-llm-d-request-dropped-reason: evicted*):
	// flush checkpoint with reason="eviction" and requeue without incrementing RetryCount.
	if clientErr != nil && clientErr.IsEvicted() {
		if snap != nil && snap.HasProgress() && resumeCount >= cfg.MaxResumes {
			errMsg := fmt.Sprintf("Failed to resume evicted request: max_resumes (%d) exhausted", cfg.MaxResumes)
			span.RecordError(err)
			span.SetStatus(codes.Error, errMsg)
			metrics.RecordFailedReq(queueID, queueName, poolID)
			select {
			case resultChannel <- asyncapi.NewErrorResult(msg.PublicRequest, msg.InternalRouting, asyncapi.ErrCodeInferenceError, errMsg):
			case <-requestCtx.Done():
			}
			return
		}
		persistCheckpoint(reqCtx, snap, pipeline.ResumeReasonEviction)
		metrics.RecordSheddedReq(queueID, queueName, poolID)
		span.SetAttributes(
			attribute.String(uotel.AttrErrorCategory, string(clientErr.Category())),
			attribute.String(uotel.LegacyAttrErrorCategory, string(clientErr.Category())),
		)
		var lastResp asyncapi.InferenceResponse
		if resp != nil {
			lastResp = *resp
		}
		requeueEvictedMessage(requestCtx, msg, retryChannel, resultChannel, clientErr.RetryAfter, lastResp)
		return
	}

	var inferenceErr asyncapi.InferenceError
	if !errors.As(err, &inferenceErr) || inferenceErr.Category().Fatal() {
		span.RecordError(err)
		span.SetStatus(codes.Error, "inference request failed")
		span.SetAttributes(
			attribute.String(uotel.AttrErrorCategory, inferenceErrorCategory(err)),
			attribute.String(uotel.LegacyAttrErrorCategory, inferenceErrorCategory(err)),
		)
		metrics.RecordFailedReq(queueID, queueName, poolID)

		var resultMsg asyncapi.ResultMessage
		if resp != nil && resp.StatusCode > 0 && len(resp.Body) > 0 {
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

	// 5. Retryable 5xx, pre-dispatch 429 rate-limit, or mid-stream server drop:
	// enforce max_resumes when partial progress exists, otherwise flush checkpoint
	// and schedule retry.
	if snap != nil && snap.HasProgress() {
		if resumeCount >= cfg.MaxResumes {
			errMsg := fmt.Sprintf("Failed to resume interrupted request: max_resumes (%d) exhausted (%s)", cfg.MaxResumes, err.Error())
			span.RecordError(err)
			span.SetStatus(codes.Error, errMsg)
			metrics.RecordFailedReq(queueID, queueName, poolID)
			select {
			case resultChannel <- asyncapi.NewErrorResult(msg.PublicRequest, msg.InternalRouting, asyncapi.ErrCodeInferenceError, errMsg):
			case <-requestCtx.Done():
			}
			return
		}
		persistCheckpoint(reqCtx, snap, pipeline.ResumeReasonError)
	}

	if inferenceErr.Category().Sheddable() {
		metrics.RecordSheddedReq(queueID, queueName, poolID)
	}
	span.SetAttributes(
		attribute.String(uotel.AttrErrorCategory, string(inferenceErr.Category())),
		attribute.String(uotel.LegacyAttrErrorCategory, string(inferenceErr.Category())),
	)
	var retryAfter time.Duration
	if clientErr != nil {
		retryAfter = clientErr.RetryAfter
	}
	var lastResp asyncapi.InferenceResponse
	if resp != nil {
		lastResp = *resp
	}
	retryMessage(requestCtx, msg, retryChannel, resultChannel, retryAfter, lastResp)
}

// requeueEvictedMessage returns a flow-control-evicted request to retryChannel
// without incrementing RetryCount or recording a failure/retry penalty, so the
// request re-enters its origin queue at its original QueueScore once queue/pool
// gates reopen.
func requeueEvictedMessage(
	ctx context.Context,
	msg pipeline.EmbelishedRequestMessage,
	retryChannel chan pipeline.RetryMessage,
	resultChannel chan asyncapi.ResultMessage,
	retryAfter time.Duration,
	lastResp asyncapi.InferenceResponse,
) {
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

	backoff := evictionDefaultMinBackoffSec + rand.Float64()*evictionDefaultJitterSec // #nosec G404 -- non-security jitter
	if retryAfterSec := retryAfter.Seconds(); retryAfterSec > 0 {
		clamped := math.Min(retryAfterSec, float64(secondsToDeadline)/2)
		backoff = clamped * (1 + rand.Float64()/4) // #nosec G404 -- non-security jitter
	}
	maxAllowed := math.Max(0, float64(secondsToDeadline)*0.5)
	if backoff > maxAllowed {
		backoff = maxAllowed
	}

	select {
	case retryChannel <- pipeline.RetryMessage{
		EmbelishedRequestMessage: msg,
		BackoffDurationSeconds:   backoff,
	}:
	case <-ctx.Done():
	}
}
