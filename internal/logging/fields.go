package logging

import "log/slog"

// Common structured log field keys to keep logs searchable/consistent.
const (
	FieldService    = "service"
	FieldVersion    = "version"
	FieldProvider   = "provider"
	FieldRequestID  = "request_id"
	FieldPath       = "path"
	FieldMethod     = "method"
	FieldStatusCode = "status_code"
	FieldDate       = "date"
	FieldCount      = "count"
	FieldDurationMS = "duration_ms"
)

// WithCommon appends service/version fields when provided.
func WithCommon(attrs []slog.Attr, service, version string) []slog.Attr {
	if service != "" {
		attrs = append(attrs, slog.String(FieldService, service))
	}
	if version != "" {
		attrs = append(attrs, slog.String(FieldVersion, version))
	}
	return attrs
}
