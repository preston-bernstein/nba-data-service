package timeutil

import "time"

// DateLayout defines the canonical date format (YYYY-MM-DD).
const DateLayout = "2006-01-02"

// ParseDate parses a YYYY-MM-DD date string.
func ParseDate(value string) (time.Time, error) {
	return time.Parse(DateLayout, value)
}

// FormatDate formats a time as YYYY-MM-DD in its current location.
func FormatDate(t time.Time) string {
	return t.Format(DateLayout)
}
