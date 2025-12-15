package metrics

// Recorder is a placeholder for metrics instrumentation.
// Extend with Prometheus or OpenTelemetry as needed.
type Recorder struct{}

func NewRecorder() *Recorder {
	return &Recorder{}
}
