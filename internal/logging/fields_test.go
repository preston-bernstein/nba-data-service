package logging

import (
	"log/slog"
	"testing"
)

func TestWithCommonAppendsServiceAndVersion(t *testing.T) {
	attrs := WithCommon(nil, "svc", "v1")
	if len(attrs) != 2 {
		t.Fatalf("expected 2 attrs, got %d", len(attrs))
	}
	if attrs[0].Key != FieldService || attrs[0].Value.String() != "svc" {
		t.Fatalf("expected service attr, got %+v", attrs[0])
	}
	if attrs[1].Key != FieldVersion || attrs[1].Value.String() != "v1" {
		t.Fatalf("expected version attr, got %+v", attrs[1])
	}
}

func TestWithCommonSkipsEmpty(t *testing.T) {
	attrs := WithCommon([]slog.Attr{{Key: "existing", Value: slog.StringValue("x")}}, "", "")
	if len(attrs) != 1 || attrs[0].Key != "existing" {
		t.Fatalf("expected original attrs preserved, got %+v", attrs)
	}
}
