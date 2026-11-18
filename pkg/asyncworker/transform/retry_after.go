package transform

import (
	"net/http"
	"strconv"
	"time"
)

// ParseRetryAfter parses a Retry-After header value, which can be either
// a number of seconds (e.g. "120") or an HTTP-date. It returns false when the
// value is empty or invalid.
func ParseRetryAfter(value string) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second, true
	}
	if deadline, err := http.ParseTime(value); err == nil {
		delay := time.Until(deadline)
		if delay < 0 {
			delay = 0
		}
		return delay, true
	}
	return 0, false
}
