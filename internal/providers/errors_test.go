package providers

import "testing"

func TestRateLimitErrorString(t *testing.T) {
	err := &RateLimitError{
		Provider:   "p",
		StatusCode: 429,
		Message:    "rate limited",
	}
	if got := err.Error(); got == "" || got == "rate limited" {
		t.Fatalf("expected status in error string, got %q", got)
	}

	rl, ok := AsRateLimitError(err)
	if !ok || rl == nil {
		t.Fatalf("expected to unwrap rate limit error")
	}

	noStatus := &RateLimitError{}
	if got := noStatus.Error(); got == "" {
		t.Fatalf("expected fallback message")
	}
}
