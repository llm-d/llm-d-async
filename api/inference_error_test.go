package api

import (
	"errors"
	"testing"
)

func TestClientError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  *ClientError
		want string
	}{
		{
			name: "message only",
			err:  &ClientError{ErrorCategory: ErrCategoryRateLimit, Message: "rate limited: status code 429"},
			want: "RATE_LIMIT: rate limited: status code 429",
		},
		{
			name: "with dropped reason",
			err: &ClientError{
				ErrorCategory: ErrCategoryRateLimit,
				Message:       "rate limited: status code 429",
				DroppedReason: "rejected-saturated",
			},
			want: "RATE_LIMIT: rate limited: status code 429 (dropped: rejected-saturated)",
		},
		{
			name: "with dropped reason and cause",
			err: &ClientError{
				ErrorCategory: ErrCategoryServer,
				Message:       "failed to read response",
				RawError:      errors.New("unexpected EOF"),
				DroppedReason: "evicted-queue-pressure",
			},
			want: "SERVER_ERROR: failed to read response (dropped: evicted-queue-pressure) (caused by: unexpected EOF)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClientError_IsEvicted(t *testing.T) {
	tests := []struct {
		name string
		err  *ClientError
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "exact evicted 429", err: &ClientError{StatusCode: 429, DroppedReason: "evicted"}, want: true},
		{name: "evicted-queue-pressure 429", err: &ClientError{StatusCode: 429, DroppedReason: "evicted-queue-pressure"}, want: true},
		{name: "evicted-priority uppercase 429", err: &ClientError{StatusCode: 429, DroppedReason: "EVICTED-PRIORITY"}, want: true},
		{name: "rejected-saturated 429", err: &ClientError{StatusCode: 429, DroppedReason: "rejected-saturated"}, want: false},
		{name: "plain 429 without reason", err: &ClientError{StatusCode: 429}, want: false},
		{name: "503 even with evicted reason", err: &ClientError{StatusCode: 503, DroppedReason: "evicted"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.IsEvicted(); got != tt.want {
				t.Errorf("IsEvicted() = %v, want %v", got, tt.want)
			}
		})
	}
}
