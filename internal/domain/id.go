package domain

import (
	"fmt"
	"strings"
)

// MaxOpaqueIDLength is the maximum encoded length accepted by the core identity types.
// Public schemas may choose stricter prefixes while preserving these values as opaque.
const MaxOpaqueIDLength = 128

// InvalidIDError reports a malformed opaque identifier without echoing its value.
type InvalidIDError struct {
	Kind   string
	Reason string
}

func (e InvalidIDError) Error() string {
	return fmt.Sprintf("invalid %s ID: %s", e.Kind, e.Reason)
}

// SHA256Digest is a canonical lowercase SHA-256 hexadecimal digest.
type SHA256Digest string

// ParseSHA256Digest validates a canonical SHA-256 digest.
func ParseSHA256Digest(raw string) (SHA256Digest, error) {
	if len(raw) != 64 {
		return "", fmt.Errorf("invalid SHA-256 digest: must contain 64 lowercase hexadecimal characters")
	}
	if raw != strings.ToLower(raw) {
		return "", fmt.Errorf("invalid SHA-256 digest: must use lowercase hexadecimal characters")
	}
	for index := 0; index < len(raw); index++ {
		character := raw[index]
		if !isASCIIDigit(character) && (character < 'a' || character > 'f') {
			return "", fmt.Errorf("invalid SHA-256 digest: contains a non-hexadecimal character")
		}
	}
	return SHA256Digest(raw), nil
}

func (digest SHA256Digest) String() string { return string(digest) }

func (digest SHA256Digest) Valid() bool {
	_, err := ParseSHA256Digest(string(digest))
	return err == nil
}

// RuntimeInstallationID identifies one local runtime installation.
type RuntimeInstallationID string

// WorkspaceID identifies one application namespace owned by an operator.
type WorkspaceID string

// JobID identifies one immutable requested unit of work.
type JobID string

// AttemptID identifies one explicit compute execution attempt.
type AttemptID string

// OperationID identifies one durable control operation.
type OperationID string

// EventID identifies one durable event.
type EventID string

// ObjectID identifies one immutable local input or code object.
type ObjectID string

// ArtifactID identifies one verified artifact.
type ArtifactID string

// ProviderInstanceID identifies one configured provider instance without exposing its type.
type ProviderInstanceID string

// SubmissionIntentID identifies one write-ahead remote submission intent.
type SubmissionIntentID string

// ProviderResourceID identifies one connector-owned remote resource ledger entry.
type ProviderResourceID string

func ParseRuntimeInstallationID(raw string) (RuntimeInstallationID, error) {
	return parseOpaqueID[RuntimeInstallationID]("runtime installation", raw)
}

func ParseWorkspaceID(raw string) (WorkspaceID, error) {
	return parseOpaqueID[WorkspaceID]("workspace", raw)
}

func ParseJobID(raw string) (JobID, error) {
	return parseOpaqueID[JobID]("job", raw)
}

func ParseAttemptID(raw string) (AttemptID, error) {
	return parseOpaqueID[AttemptID]("attempt", raw)
}

func ParseOperationID(raw string) (OperationID, error) {
	return parseOpaqueID[OperationID]("operation", raw)
}

func ParseEventID(raw string) (EventID, error) {
	return parseOpaqueID[EventID]("event", raw)
}

func ParseObjectID(raw string) (ObjectID, error) {
	return parseOpaqueID[ObjectID]("object", raw)
}

func ParseArtifactID(raw string) (ArtifactID, error) {
	return parseOpaqueID[ArtifactID]("artifact", raw)
}

func ParseProviderInstanceID(raw string) (ProviderInstanceID, error) {
	return parseOpaqueID[ProviderInstanceID]("provider instance", raw)
}

func ParseSubmissionIntentID(raw string) (SubmissionIntentID, error) {
	return parseOpaqueID[SubmissionIntentID]("submission intent", raw)
}

func ParseProviderResourceID(raw string) (ProviderResourceID, error) {
	return parseOpaqueID[ProviderResourceID]("provider resource", raw)
}

func (id RuntimeInstallationID) String() string { return string(id) }
func (id WorkspaceID) String() string           { return string(id) }
func (id JobID) String() string                 { return string(id) }
func (id AttemptID) String() string             { return string(id) }
func (id OperationID) String() string           { return string(id) }
func (id EventID) String() string               { return string(id) }
func (id ObjectID) String() string              { return string(id) }
func (id ArtifactID) String() string            { return string(id) }
func (id ProviderInstanceID) String() string    { return string(id) }
func (id SubmissionIntentID) String() string    { return string(id) }
func (id ProviderResourceID) String() string    { return string(id) }

func (id RuntimeInstallationID) Valid() bool { return validOpaqueID(string(id)) }
func (id WorkspaceID) Valid() bool           { return validOpaqueID(string(id)) }
func (id JobID) Valid() bool                 { return validOpaqueID(string(id)) }
func (id AttemptID) Valid() bool             { return validOpaqueID(string(id)) }
func (id OperationID) Valid() bool           { return validOpaqueID(string(id)) }
func (id EventID) Valid() bool               { return validOpaqueID(string(id)) }
func (id ObjectID) Valid() bool              { return validOpaqueID(string(id)) }
func (id ArtifactID) Valid() bool            { return validOpaqueID(string(id)) }
func (id ProviderInstanceID) Valid() bool    { return validOpaqueID(string(id)) }
func (id SubmissionIntentID) Valid() bool    { return validOpaqueID(string(id)) }
func (id ProviderResourceID) Valid() bool    { return validOpaqueID(string(id)) }

func parseOpaqueID[T ~string](kind, raw string) (T, error) {
	if err := validateOpaqueID(kind, raw); err != nil {
		return "", err
	}
	return T(raw), nil
}

func validateOpaqueID(kind, raw string) error {
	if raw == "" {
		return InvalidIDError{Kind: kind, Reason: "must not be empty"}
	}
	if len(raw) > MaxOpaqueIDLength {
		return InvalidIDError{Kind: kind, Reason: fmt.Sprintf("must not exceed %d bytes", MaxOpaqueIDLength)}
	}
	for index := 0; index < len(raw); index++ {
		character := raw[index]
		if index == 0 {
			if !isASCIILetter(character) && !isASCIIDigit(character) {
				return InvalidIDError{Kind: kind, Reason: "must start with an ASCII letter or digit"}
			}
			continue
		}
		if !isASCIILetter(character) && !isASCIIDigit(character) && character != '-' && character != '_' && character != '.' && character != ':' {
			return InvalidIDError{Kind: kind, Reason: "contains a non-opaque-safe character"}
		}
	}
	return nil
}

func validOpaqueID(raw string) bool {
	return validateOpaqueID("opaque", raw) == nil
}

func isASCIILetter(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
}

func isASCIIDigit(character byte) bool {
	return character >= '0' && character <= '9'
}
