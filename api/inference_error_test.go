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
