package logging

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
