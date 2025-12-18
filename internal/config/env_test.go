package config

import "testing"

func TestBoolEnvOrDefault(t *testing.T) {
	t.Setenv("BOOL_TEST", "")
	if got := boolEnvOrDefault("BOOL_TEST", true); !got {
		t.Fatalf("expected default true when unset")
	}

	cases := []struct {
		val      string
		expected bool
	}{
		{"true", true},
		{"TRUE", true},
		{"1", true},
		{"yes", true},
		{"false", false},
		{"FALSE", false},
		{"0", false},
		{"no", false},
		{"maybe", true}, // falls back to default on unknown
	}

	for _, tc := range cases {
		t.Setenv("BOOL_TEST", tc.val)
		if got := boolEnvOrDefault("BOOL_TEST", true); got != tc.expected {
			t.Fatalf("expected %v for %s, got %v", tc.expected, tc.val, got)
		}
	}
}
