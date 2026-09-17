// Package artifactwire defines the public local-artifact view, not provider output
// metadata. Paths are labels only and must never select a client destination.
package artifactwire

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

const MaxFiles = 10004
const MaxPage = 100
const MaxBytes int64 = 4 << 30
const MaxJSON = 1 << 20
const VerifiedTrailer = "X-Compute-Relay-Verified"

var ErrInvalid = errors.New("invalid published artifact evidence")
var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type Target struct {
	WorkspaceID string `json:"workspace_id"`
	JobID       string `json:"job_id"`
	AttemptID   string `json:"attempt_id"`
}

func (t Target) Valid() bool {
	return idPattern.MatchString(t.WorkspaceID) && idPattern.MatchString(t.JobID) && idPattern.MatchString(t.AttemptID)
}

type File struct {
	ID     string `json:"artifact_id"`
	Path   string `json:"path"`
	Role   string `json:"role"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func (f File) Valid() bool {
	if !idPattern.MatchString(f.ID) || !strings.HasPrefix(f.ID, "art_") || !digestPattern.MatchString(f.SHA256) || f.Bytes < 0 || f.Bytes > MaxBytes || len(f.Path) > 1024 || path.Clean(f.Path) != f.Path || strings.Contains(f.Path, "\\") {
		return false
	}
	for _, c := range f.Path {
		if c < 32 || c > 126 {
			return false
		}
	}
	switch f.Role {
	case "manifest":
		return f.Path == "control/execution-result.json" && f.Bytes <= 1<<20
	case "log":
		return (f.Path == "control/stdout.log" || f.Path == "control/stderr.log") && f.Bytes <= 20<<20
	case "provenance":
		return f.Path == "control/environment.json" && f.Bytes <= 1<<20
	case "output":
		return strings.HasPrefix(f.Path, "outputs/") && len(f.Path) > len("outputs/")
	}
	return false
}

type Metadata struct {
	Target
	ResultPhase string    `json:"result_phase"`
	VerifiedAt  time.Time `json:"verified_at"`
	Artifact    File      `json:"artifact"`
}

type Page struct {
	Target
	ResultPhase    string    `json:"result_phase"`
	VerifiedAt     time.Time `json:"verified_at"`
	SnapshotSHA256 string    `json:"snapshot_sha256"`
	Total          int       `json:"total_artifacts"`
	Artifacts      []File    `json:"artifacts"`
	NextCursor     string    `json:"next_cursor"`
}

// Snapshot binds pagination to an entire immutable published view and target.
// It is neither authorization nor proof of current on-disk byte availability.
func Snapshot(t Target, phase string, at time.Time, files []File) string {
	raw, _ := json.Marshal(struct {
		Target
		Phase string
		At    time.Time
		Files []File
	}{t, phase, at, files})
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func Cursor(digest string, offset int) string { return "ar1_" + digest + "_" + strconv.Itoa(offset) }
func Offset(cursor, digest string, total int) (int, error) {
	if !digestPattern.MatchString(digest) || total < 1 || total > MaxFiles {
		return 0, ErrInvalid
	}
	if cursor == "" {
		return 0, nil
	}
	prefix := "ar1_" + digest + "_"
	if !strings.HasPrefix(cursor, prefix) || len(cursor) > 80 {
		return 0, ErrInvalid
	}
	n, err := strconv.Atoi(strings.TrimPrefix(cursor, prefix))
	if err != nil || n < 1 || n >= total || Cursor(digest, n) != cursor {
		return 0, ErrInvalid
	}
	return n, nil
}

func stringValue(m map[string]json.RawMessage, name string) string {
	var s string
	_ = json.Unmarshal(m[name], &s)
	return s
}
func integer(m map[string]json.RawMessage, name string) (int64, error) {
	var n *int64
	if json.Unmarshal(m[name], &n) != nil || n == nil {
		return 0, ErrInvalid
	}
	return *n, nil
}
func contextFields(raw []byte, t Target) (map[string]json.RawMessage, string, time.Time, error) {
	m, err := jsonwire.Object(raw, MaxJSON)
	phase := stringValue(m, "result_phase")
	at, dateErr := time.Parse(time.RFC3339Nano, stringValue(m, "verified_at"))
	if err != nil || !t.Valid() || stringValue(m, "workspace_id") != t.WorkspaceID || stringValue(m, "job_id") != t.JobID || stringValue(m, "attempt_id") != t.AttemptID || phase == "" || len(phase) > 64 || dateErr != nil || at.IsZero() {
		return nil, "", time.Time{}, ErrInvalid
	}
	return m, phase, at, nil
}
func decodeFile(raw []byte) (File, error) {
	m, err := jsonwire.Object(raw, MaxJSON)
	n, numberErr := integer(m, "bytes")
	f := File{stringValue(m, "artifact_id"), stringValue(m, "path"), stringValue(m, "role"), n, stringValue(m, "sha256")}
	if err != nil || numberErr != nil || !f.Valid() {
		return File{}, ErrInvalid
	}
	return f, nil
}

// Decoders read exact lowercase keys rather than encoding/json's case-insensitive
// struct matching. Compatible unknown fields are ignored, not echoed or executed.
func DecodeMetadata(raw []byte, t Target, id string) (Metadata, error) {
	m, phase, at, err := contextFields(raw, t)
	f, fileErr := decodeFile(m["artifact"])
	if err != nil || fileErr != nil || f.ID != id {
		return Metadata{}, ErrInvalid
	}
	return Metadata{t, phase, at, f}, nil
}
func DecodePage(raw []byte, t Target, cursor string, limit int) (Page, error) {
	m, phase, at, err := contextFields(raw, t)
	total, numberErr := integer(m, "total_artifacts")
	digest := stringValue(m, "snapshot_sha256")
	var entries []json.RawMessage
	if err != nil || numberErr != nil || total < 1 || total > MaxFiles || limit < 1 || limit > MaxPage || json.Unmarshal(m["artifacts"], &entries) != nil || entries == nil {
		return Page{}, ErrInvalid
	}
	offset, err := Offset(cursor, digest, int(total))
	if err != nil || len(entries) != min(limit, int(total)-offset) {
		return Page{}, ErrInvalid
	}
	p := Page{Target: t, ResultPhase: phase, VerifiedAt: at, SnapshotSHA256: digest, Total: int(total), Artifacts: make([]File, 0, len(entries))}
	seen := map[string]bool{}
	previous := ""
	for _, rawFile := range entries {
		f, err := decodeFile(rawFile)
		if err != nil || seen[f.ID] || f.Path <= previous {
			return Page{}, ErrInvalid
		}
		seen[f.ID], previous = true, f.Path
		p.Artifacts = append(p.Artifacts, f)
	}
	// Missing/null is not an explicit end-of-pagination acknowledgement.
	var next *string
	if json.Unmarshal(m["next_cursor"], &next) != nil || next == nil {
		return Page{}, ErrInvalid
	}
	if offset+len(entries) < int(total) {
		p.NextCursor = Cursor(digest, offset+len(entries))
	}
	if *next != p.NextCursor {
		return Page{}, ErrInvalid
	}
	return p, nil
}
