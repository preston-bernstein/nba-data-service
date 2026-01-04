package testutil

import "time"

// NowAt returns a clock function fixed at the provided time.
func NowAt(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// MustParseRFC3339 parses an RFC3339 timestamp or panics; intended for tests.
func MustParseRFC3339(v string) time.Time {
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		panic(err)
	}
	return t
}
