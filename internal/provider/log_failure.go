package provider

// LogReadFailure carries a fixed diagnostic, never provider-controlled text.
type LogReadFailure struct {
	Reason string
}

func (e LogReadFailure) Valid() bool {
	switch e.Reason {
	case "auth_read_failed", "kernel_read_failed", "stream_timeout",
		"stream_http_401", "stream_http_403", "stream_http_404", "stream_http_429",
		"stream_http_4xx", "stream_http_5xx", "stream_redirect", "stream_http_other",
		"stream_format_invalid", "replay_unavailable", "read_unavailable":
		return true
	default:
		return false
	}
}

func (e LogReadFailure) Error() string {
	if !e.Valid() {
		return "log read unavailable"
	}
	return "log read unavailable: " + e.Reason
}
