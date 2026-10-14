package api

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
)

const maxArtifactFiles = 10004
const maxArtifactPage = 100
const maxArtifactBytes int64 = 4 << 30
const verifiedArtifactTrailer = "X-Compute-Relay-Verified"

var errArtifactEvidence = errors.New("invalid published artifact evidence")
var artifactIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var artifactDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type artifactTarget struct {
	WorkspaceID string `json:"workspace_id"`
	JobID       string `json:"job_id"`
	AttemptID   string `json:"attempt_id"`
}

func (t artifactTarget) Valid() bool {
	return artifactIDPattern.MatchString(t.WorkspaceID) && artifactIDPattern.MatchString(t.JobID) && artifactIDPattern.MatchString(t.AttemptID)
}

type artifactFile struct {
	ID     string `json:"artifact_id"`
	Path   string `json:"path"`
	Role   string `json:"role"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func (f artifactFile) Valid() bool {
	if !artifactIDPattern.MatchString(f.ID) || !strings.HasPrefix(f.ID, "art_") || !artifactDigestPattern.MatchString(f.SHA256) || f.Bytes < 0 || f.Bytes > maxArtifactBytes || len(f.Path) > 1024 || path.Clean(f.Path) != f.Path || strings.Contains(f.Path, "\\") {
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

type artifactMetadata struct {
	artifactTarget
	ResultPhase string       `json:"result_phase"`
	VerifiedAt  time.Time    `json:"verified_at"`
	Artifact    artifactFile `json:"artifact"`
}

type artifactPage struct {
	artifactTarget
	ResultPhase    string         `json:"result_phase"`
	VerifiedAt     time.Time      `json:"verified_at"`
	SnapshotSHA256 string         `json:"snapshot_sha256"`
	Total          int            `json:"total_artifacts"`
	Artifacts      []artifactFile `json:"artifacts"`
	NextCursor     string         `json:"next_cursor"`
}

// artifactSnapshot binds pagination to an entire immutable published view and target.
// It is neither authorization nor proof of current on-disk byte availability.
func artifactSnapshot(t artifactTarget, phase string, at time.Time, files []artifactFile) string {
	raw, _ := json.Marshal(struct {
		artifactTarget
		Phase string
		At    time.Time
		Files []artifactFile
	}{t, phase, at, files})
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func artifactCursor(digest string, offset int) string {
	return "ar1_" + digest + "_" + strconv.Itoa(offset)
}
func artifactOffset(cursor, digest string, total int) (int, error) {
	if !artifactDigestPattern.MatchString(digest) || total < 1 || total > maxArtifactFiles {
		return 0, errArtifactEvidence
	}
	if cursor == "" {
		return 0, nil
	}
	prefix := "ar1_" + digest + "_"
	if !strings.HasPrefix(cursor, prefix) || len(cursor) > 80 {
		return 0, errArtifactEvidence
	}
	n, err := strconv.Atoi(strings.TrimPrefix(cursor, prefix))
	if err != nil || n < 1 || n >= total || artifactCursor(digest, n) != cursor {
		return 0, errArtifactEvidence
	}
	return n, nil
}
