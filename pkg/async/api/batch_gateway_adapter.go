package api

import (
	"context"
	"encoding/json"

	batchhttp "github.com/llm-d-incubation/batch-gateway/pkg/clients/http"
	batchinference "github.com/llm-d-incubation/batch-gateway/pkg/clients/inference"
)

// BatchGatewayAdapter adapts batch-gateway's InferenceClient to llm-d-async's InferenceClient interface.
type BatchGatewayAdapter struct {
	client batchinference.InferenceClient
}

// NewBatchGatewayAdapter creates a new adapter that wraps batch-gateway's InferenceClient.
func NewBatchGatewayAdapter(client batchinference.InferenceClient) *BatchGatewayAdapter {
	return &BatchGatewayAdapter{client: client}
}

// SendRequest implements the llm-d-async InferenceClient interface by delegating to batch-gateway's Generate method.
func (a *BatchGatewayAdapter) SendRequest(ctx context.Context, url string, headers map[string]string, payload []byte) ([]byte, error) {
	// Parse the payload as JSON to extract params
	var params map[string]interface{}
	if err := json.Unmarshal(payload, &params); err != nil {
		return nil, &ClientError{
			ErrorCategory: ErrCategoryInvalidReq,
			Message:       "failed to parse request payload",
			RawError:      err,
		}
	}

	// Create the batch-gateway request
	req := &batchinference.GenerateRequest{
		RequestID: "", // Could be extracted from context or metadata if needed
		Endpoint:  url,
		Params:    params,
		Headers:   headers,
	}

	// Execute the request using batch-gateway's client
	resp, clientErr := a.client.Generate(ctx, req)
	if clientErr != nil {
		// Map batch-gateway ClientError to llm-d-async ClientError
		return mapBatchGatewayError(resp, clientErr)
	}

	return resp.Response, nil
}

// mapBatchGatewayError converts batch-gateway's ClientError to llm-d-async's ClientError.
func mapBatchGatewayError(resp *batchinference.GenerateResponse, batchErr *batchinference.ClientError) ([]byte, error) {
	var responseBody []byte
	if resp != nil {
		responseBody = resp.Response
	}

	// Map batch-gateway error categories to our error categories
	var category ErrorCategory
	switch batchErr.Category {
	case batchhttp.ErrCategoryRateLimit:
		category = ErrCategoryRateLimit
	case batchhttp.ErrCategoryServer:
		category = ErrCategoryServer
	case batchhttp.ErrCategoryInvalidReq:
		category = ErrCategoryInvalidReq
	case batchhttp.ErrCategoryAuth, batchhttp.ErrCategoryParse, batchhttp.ErrCategoryUnknown:
		// Treat auth, parse, and unknown errors as network/fatal errors
		category = ErrCategoryNetwork
	default:
		category = ErrCategoryNetwork
	}

	return responseBody, &ClientError{
		ErrorCategory: category,
		Message:       batchErr.Message,
		RawError:      batchErr.RawError,
	}
}
