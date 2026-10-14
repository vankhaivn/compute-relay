package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestOpaqueIDValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "simple", value: "job_01"},
		{name: "allowed separators", value: "job:01.alpha-beta_gamma"},
		{name: "single character", value: "a"},
		{name: "empty", value: "", wantErr: true},
		{name: "leading separator", value: "_job", wantErr: true},
		{name: "space", value: "job 01", wantErr: true},
		{name: "slash", value: "job/01", wantErr: true},
		{name: "unicode", value: "jób", wantErr: true},
		{name: "too long", value: strings.Repeat("a", MaxOpaqueIDLength+1), wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			id, err := ParseJobID(test.value)
			if test.wantErr {
				if err == nil {
					t.Fatalf("ParseJobID(%q) succeeded, want error", test.value)
				}
				var invalid InvalidIDError
				if !errors.As(err, &invalid) {
					t.Fatalf("error = %T, want InvalidIDError", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseJobID(%q) error = %v", test.value, err)
			}
			if id.String() != test.value || !id.Valid() {
				t.Fatalf("parsed ID = %q valid=%v, want %q valid", id, id.Valid(), test.value)
			}
		})
	}
}

func TestOpaqueIDTypesUseSameSafeEncoding(t *testing.T) {
	t.Parallel()

	const value = "opaque_01"
	checks := []struct {
		name  string
		parse func(string) error
	}{
		{"runtime installation", func(raw string) error { _, err := ParseRuntimeInstallationID(raw); return err }},
		{"workspace", func(raw string) error { _, err := ParseWorkspaceID(raw); return err }},
		{"attempt", func(raw string) error { _, err := ParseAttemptID(raw); return err }},
		{"operation", func(raw string) error { _, err := ParseOperationID(raw); return err }},
		{"event", func(raw string) error { _, err := ParseEventID(raw); return err }},
		{"object", func(raw string) error { _, err := ParseObjectID(raw); return err }},
		{"artifact", func(raw string) error { _, err := ParseArtifactID(raw); return err }},
		{"provider instance", func(raw string) error { _, err := ParseProviderInstanceID(raw); return err }},
		{"submission intent", func(raw string) error { _, err := ParseSubmissionIntentID(raw); return err }},
		{"provider resource", func(raw string) error { _, err := ParseProviderResourceID(raw); return err }},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			t.Parallel()
			if err := check.parse(value); err != nil {
				t.Fatalf("valid ID rejected: %v", err)
			}
			if err := check.parse("bad/value"); err == nil {
				t.Fatal("unsafe ID accepted")
			}
		})
	}
}

func TestOpaqueIDJSONIsAString(t *testing.T) {
	t.Parallel()

	encoded, err := json.Marshal(JobID("job_01"))
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if string(encoded) != `"job_01"` {
		t.Fatalf("encoded ID = %s, want JSON string", encoded)
	}
}

func TestSHA256DigestValidation(t *testing.T) {
	t.Parallel()

	valid := strings.Repeat("a", 64)
	digest, err := ParseSHA256Digest(valid)
	if err != nil {
		t.Fatalf("ParseSHA256Digest() error = %v", err)
	}
	if digest.String() != valid || !digest.Valid() {
		t.Fatalf("digest = %q valid=%v", digest, digest.Valid())
	}

	for _, invalid := range []string{
		strings.Repeat("a", 63),
		strings.Repeat("A", 64),
		strings.Repeat("g", 64),
	} {
		if _, err := ParseSHA256Digest(invalid); err == nil {
			t.Fatalf("invalid digest %q accepted", invalid)
		}
	}
}
