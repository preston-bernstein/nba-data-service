package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// readJSONFile decodes the JSON file at path into v.
func readJSONFile(path string, v any) error {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return err //nolint:wrapcheck // error text is logged and shown to callers as is; wrapping changes it
	}
	defer func() {
		_ = f.Close()
	}()

	if err := json.NewDecoder(f).Decode(v); err != nil {
		return err //nolint:wrapcheck // error text is logged and shown to callers as is; wrapping changes it
	}
	return nil
}

// encodeJSON renders v as indented JSON, the format of every snapshot file.
func encodeJSON(v any) ([]byte, error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err //nolint:wrapcheck // error text is logged and shown to callers as is; wrapping changes it
	}
	return data, nil
}

// writeFileAtomic writes data next to path and renames it into place, so readers never see a partial file.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil { //nolint:gosec // snapshot files keep their current mode for other readers; a tighter mode changes access
		return err //nolint:wrapcheck // error text is logged and shown to callers as is; wrapping changes it
	}
	return os.Rename(tmp, path) //nolint:wrapcheck // error text is logged and shown to callers as is; wrapping changes it
}
