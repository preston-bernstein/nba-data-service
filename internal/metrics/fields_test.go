package metrics

import "testing"

func TestMetricFieldKeysAreStable(t *testing.T) {
	if AttrMethod == "" || AttrPath == "" || AttrStatus == "" || AttrProvider == "" {
		t.Fatalf("expected metric attribute keys to be non-empty")
	}
}
