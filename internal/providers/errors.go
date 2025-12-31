package providers

import (
	"errors"
	"fmt"
	"time"
)

// RateLimitError captures rate limit responses from upstream providers.
type RateLimitError struct {
	Provider   string
	StatusCode int
	RetryAfter time.Duration
	Remaining  string
	Message    string
}

func (e *RateLimitError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = "provider rate limited"
	}
	if e.StatusCode > 0 {
		return fmt.Sprintf("%s (status=%d)", msg, e.StatusCode)
	}
	return msg
}

// AsRateLimitError attempts to unwrap an error into a RateLimitError.
func AsRateLimitError(err error) (*RateLimitError, bool) {
	var rlErr *RateLimitError
	if errors.As(err, &rlErr) {
		return rlErr, true
	}
	return nil, false
}
