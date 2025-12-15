package metrics

import "testing"

func TestNewRecorderNotNil(t *testing.T) {
	if NewRecorder() == nil {
		t.Fatal("expected recorder to be non-nil")
	}
}
