package snapshots

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadManifestReturnsDefaultOnDecodeError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(path, []byte("{bad json"), 0o644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	m, err := readManifest(path, 5)
	if err == nil {
		t.Fatalf("expected decode error")
	}
	if m.Retention.GamesDays != 5 {
		t.Fatalf("expected retention fallback to provided, got %d", m.Retention.GamesDays)
	}
}

func TestWriteManifestFailsWhenPathMissing(t *testing.T) {
	if err := writeManifest(filepath.Join("does-not-exist", "missing"), defaultManifest(3)); err == nil {
		t.Fatalf("expected error when base path missing")
	}
}

func TestWriteManifestSuccess(t *testing.T) {
	dir := t.TempDir()
	m := defaultManifest(4)
	if err := writeManifest(dir, m); err != nil {
		t.Fatalf("expected manifest to be written, got %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("expected manifest file, got %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("expected manifest content")
	}
}
