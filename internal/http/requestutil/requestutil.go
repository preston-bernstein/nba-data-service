package requestutil

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

var requestIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
var useFallback atomic.Bool

// SanitizeRequestID validates the incoming request ID header and generates a new one when invalid.
func SanitizeRequestID(incoming string) string {
	if incoming != "" && requestIDPattern.MatchString(incoming) {
		return incoming
	}
	return NewRequestID()
}

// NewRequestID generates a random request ID with a time-based fallback.
func NewRequestID() string {
	var b [8]byte
	if !useFallback.Load() {
		if _, err := rand.Read(b[:]); err == nil {
			return hex.EncodeToString(b[:])
		}
	}
	return hex.EncodeToString([]byte(time.Now().Format("20060102150405.000000000")))
}

// ClientIP extracts the client IP from X-Forwarded-For or RemoteAddr.
func ClientIP(r *http.Request) string {
	if r == nil {
		return ""
	}
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		parts := strings.Split(forwarded, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
		return forwarded
	}
	return r.RemoteAddr
}
