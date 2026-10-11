package provider

import "testing"

func TestLogReadFailureDiscardsUntrustedReasons(t *testing.T) {
	for _, reason := range []string{"", "private-provider-canary", "stream_timeout\nprivate-provider-canary"} {
		failure := LogReadFailure{Reason: reason}
		if failure.Valid() || failure.Error() != "log read unavailable" {
			t.Fatal("untrusted diagnostic escaped")
		}
	}
	if failure := (LogReadFailure{Reason: "stream_timeout"}); !failure.Valid() || failure.Error() != "log read unavailable: stream_timeout" {
		t.Fatal(failure)
	}
}
