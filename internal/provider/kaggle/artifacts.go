package kaggle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

const artifactManifest = "control/execution-result.json"
const artifactCatalogBytes = 8 << 20
const artifactFileLimit = 10004

var ErrArtifactCursor = errors.New("invalid or expired artifact catalog cursor; restart listing")
var ErrArtifactTransfer = errors.New("artifact verification failed; retain the original attempt and result pin")

// ArtifactPolicy bounds this read-only component; it does not alter a frozen job.
type ArtifactPolicy struct {
	MaxFiles int
	MaxBytes int64
	Timeout  time.Duration
}

func DefaultArtifactPolicy() ArtifactPolicy {
	return ArtifactPolicy{artifactFileLimit, 4 << 30, 10 * time.Minute}
}
func (p ArtifactPolicy) valid() bool {
	return p.MaxFiles >= 1 && p.MaxFiles <= artifactFileLimit && p.MaxBytes > 0 && p.MaxBytes <= 4<<30 && p.Timeout >= time.Second && p.Timeout <= 30*time.Minute
}

type artifactEntry struct {
	Path   string              `json:"path"`
	Bytes  int64               `json:"bytes"`
	SHA256 domain.SHA256Digest `json:"sha256"`
}
type artifactDeclaration struct {
	Path     string `json:"path"`
	Kind     string `json:"kind"`
	Required bool   `json:"required"`
	MaxBytes int64  `json:"max_bytes"`
}
type artifactIdentity struct {
	JobID     domain.JobID        `json:"job_id"`
	AttemptID domain.AttemptID    `json:"attempt_id"`
	Nonce     string              `json:"attempt_nonce"`
	Bundle    domain.SHA256Digest `json:"bundle_sha256"`
	Inputs    domain.SHA256Digest `json:"input_manifest_sha256"`
}
type artifactRequest struct {
	Protocol  int                   `json:"protocol"`
	Execution executionRequest      `json:"execution"`
	Identity  artifactIdentity      `json:"identity"`
	Outputs   []artifactDeclaration `json:"outputs"`
	MaxFiles  int                   `json:"max_files"`
	MaxBytes  int64                 `json:"max_bytes"`
	Target    *artifactEntry        `json:"target"`
}
type artifactInvocation func(context.Context, Config, string, []byte, artifactRequest, io.Writer) ([]byte, error)

// ArtifactReader is one attempt's read-only output port. It never calls Submit,
// Prepare, Cancel or Cleanup. Only M3 can pin/publish verified local artifacts.
// One complete bounded catalog is held in memory for pagination; a fresh empty
// cursor explicitly rediscovers it. Recovery uses M3's durable pin, not this cache.
type ArtifactReader struct {
	executor      *Executor
	policy        ArtifactPolicy
	request       artifactRequest
	slot          chan struct{}
	run           artifactInvocation
	catalog       []artifactEntry
	catalogRef    provider.RemoteReference
	catalogDigest domain.SHA256Digest
}

func NewArtifactReader(e *Executor, policy ArtifactPolicy) (*ArtifactReader, error) {
	if e == nil || !policy.valid() {
		return nil, ErrConfig
	}
	parsed, err := admission.Parse(e.plan.Job.Specification)
	if err != nil {
		return nil, ErrConfig
	}
	outputs := parsed.Spec().Outputs
	if len(outputs) == 0 || len(outputs) > 64 {
		return nil, ErrConfig
	}
	r := artifactRequest{Protocol: 1, Execution: e.request, MaxFiles: policy.MaxFiles, MaxBytes: policy.MaxBytes, Outputs: make([]artifactDeclaration, 0, len(outputs))}
	r.Execution.Source = ""
	id := e.plan.Job.Identity
	r.Identity = artifactIdentity{id.JobID, id.AttemptID, id.Nonce, id.BundleSHA256, id.InputManifestSHA256}
	for _, o := range outputs {
		kind := o.Kind
		if kind == "" {
			kind = "file"
		}
		if !artifactSafePath(o.Path) {
			return nil, ErrConfig
		}
		r.Outputs = append(r.Outputs, artifactDeclaration{o.Path, kind, o.Required, o.MaxBytes})
	}
	return &ArtifactReader{executor: e, policy: policy, request: r, slot: make(chan struct{}, 1), run: runArtifacts}, nil
}
func artifactSafePath(path string) bool {
	if !provider.SafeArtifactPath(path) || len(path) > 512 || strings.ContainsAny(path, "%") {
		return false
	}
	for _, c := range path {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}
func (a *ArtifactReader) allowed(path string) bool {
	if !artifactSafePath(path) {
		return false
	}
	if _, ok := artifactControlLimit(path); ok {
		return true
	}
	rel, ok := strings.CutPrefix(path, "outputs/")
	if !ok {
		return false
	}
	for _, d := range a.request.Outputs {
		if d.Kind == "file" && rel == d.Path || d.Kind == "directory" && strings.HasPrefix(rel, d.Path+"/") {
			return true
		}
	}
	return false
}
func artifactControlLimit(path string) (int64, bool) {
	switch path {
	case artifactManifest, "control/environment.json":
		return 1 << 20, true
	case "control/stdout.log", "control/stderr.log":
		return 20 << 20, true
	default:
		return 0, false
	}
}
func (a *ArtifactReader) enter(parent context.Context) (context.Context, context.CancelFunc, error) {
	if a == nil {
		return nil, nil, ErrConfig
	}
	ctx, cancel := context.WithTimeout(parent, a.policy.Timeout)
	select {
	case a.slot <- struct{}{}:
		return ctx, func() { <-a.slot; cancel() }, nil
	case <-ctx.Done():
		cancel()
		return nil, nil, ctx.Err()
	}
}

type artifactCursor struct {
	Reference domain.SHA256Digest `json:"reference"`
	Catalog   domain.SHA256Digest `json:"catalog"`
	Offset    int                 `json:"offset"`
}

func artifactReferenceDigest(ref provider.RemoteReference) domain.SHA256Digest {
	raw, _ := json.Marshal(ref)
	return provider.Digest(raw)
}

func (a *ArtifactReader) ListArtifacts(parent context.Context, ref provider.RemoteReference, page provider.PageRequest) (provider.ArtifactPage, error) {
	var none provider.ArtifactPage
	if a == nil || page.Validate() != nil {
		return none, ErrArtifactCursor
	}
	if err := a.executor.checkOperationalReference(ref); err != nil {
		return none, err
	}
	ctx, leave, err := a.enter(parent)
	if err != nil {
		return none, err
	}
	defer leave()
	if ctx.Err() != nil {
		return none, ctx.Err()
	}
	offset := 0
	if page.Cursor == "" {
		a.catalog = nil
		a.catalogDigest = ""
		raw, err := a.call(ctx, "catalog", ref, nil, nil)
		if err != nil {
			return none, err
		}
		files, err := decodeArtifactCatalog(raw)
		if err != nil {
			return none, err
		}
		var total int64
		seen := map[string]bool{}
		hasManifest := false
		if len(files) > a.policy.MaxFiles {
			return none, ErrArtifactTransfer
		}
		sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
		for _, f := range files {
			if !a.allowed(f.Path) || f.Bytes < 0 || !f.SHA256.Valid() || f.Bytes > a.policy.MaxBytes-total {
				return none, ErrArtifactTransfer
			}
			if limit, ok := artifactControlLimit(f.Path); ok && f.Bytes > limit {
				return none, ErrArtifactTransfer
			}
			key := strings.ToLower(f.Path)
			parts := strings.Split(key, "/")
			if seen[key] {
				return none, ErrArtifactTransfer
			}
			for i := 1; i < len(parts); i++ {
				if seen[strings.Join(parts[:i], "/")] {
					return none, ErrArtifactTransfer
				}
			}
			seen[key] = true
			total += f.Bytes
			hasManifest = hasManifest || f.Path == artifactManifest
		}
		// Check prefixes independently of original mixed-case ordering.
		for key := range seen {
			parts := strings.Split(key, "/")
			for i := 1; i < len(parts); i++ {
				if seen[strings.Join(parts[:i], "/")] {
					return none, ErrArtifactTransfer
				}
			}
		}
		if !hasManifest {
			return none, ErrArtifactTransfer
		}
		encoded, _ := json.Marshal(files)
		a.catalog, a.catalogRef, a.catalogDigest = files, ref, provider.Digest(encoded)
	} else {
		raw, err := base64.RawURLEncoding.Strict().DecodeString(page.Cursor)
		var cursor artifactCursor
		if err != nil || base64.RawURLEncoding.EncodeToString(raw) != page.Cursor || closedObject(raw, &cursor) != nil || cursor.Reference != artifactReferenceDigest(ref) || cursor.Catalog != a.catalogDigest || !cursor.Catalog.Valid() || a.catalogRef != ref || cursor.Offset <= 0 || cursor.Offset >= len(a.catalog) {
			return none, ErrArtifactCursor
		}
		offset = cursor.Offset
	}
	end := min(offset+page.Limit, len(a.catalog))
	result := provider.ArtifactPage{Artifacts: make([]provider.Artifact, 0, end-offset)}
	for _, f := range a.catalog[offset:end] {
		result.Artifacts = append(result.Artifacts, provider.Artifact{Remote: ref, Path: f.Path, Bytes: f.Bytes, SHA256: f.SHA256})
	}
	if end < len(a.catalog) {
		raw, _ := json.Marshal(artifactCursor{artifactReferenceDigest(ref), a.catalogDigest, end})
		result.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
	}
	return result, nil
}

// Destination MUST be temporary/unpublished. A full payload followed by failed
// EOF/Close, digest, final identity check or process exit is still an error. M3's
// transfer pipe sends successful EOF only after this entire method succeeds.
func (a *ArtifactReader) FetchArtifact(parent context.Context, ref provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64) (provider.TransferResult, error) {
	var none provider.TransferResult
	if a == nil || dst == nil || file.Validate(ref) != nil || !a.allowed(file.Path) || limit < file.Bytes || file.Bytes > a.policy.MaxBytes {
		return none, ErrArtifactTransfer
	}
	if n, ok := artifactControlLimit(file.Path); ok && file.Bytes > n {
		return none, ErrArtifactTransfer
	}
	if err := a.executor.checkOperationalReference(ref); err != nil {
		return none, err
	}
	ctx, leave, err := a.enter(parent)
	if err != nil {
		return none, err
	}
	defer leave()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	checked := &artifactWriter{ctx: ctx, dst: dst, limit: file.Bytes, hash: sha256.New(), cancel: cancel}
	_, err = a.call(ctx, "fetch", ref, &artifactEntry{file.Path, file.Bytes, file.SHA256}, checked)
	if err != nil || checked.failed || checked.n != file.Bytes || hex.EncodeToString(checked.hash.Sum(nil)) != string(file.SHA256) {
		return none, ErrArtifactTransfer
	}
	return provider.TransferResult{Bytes: file.Bytes, SHA256: file.SHA256}, nil
}
func (a *ArtifactReader) call(ctx context.Context, mode string, ref provider.RemoteReference, target *artifactEntry, dst io.Writer) (raw []byte, err error) {
	defer func() {
		if recover() != nil {
			raw = nil
			err = ErrProcess
		}
	}()
	local, err := a.executor.stager.local(ctx, a.executor.stager.config, Local, nil)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil || !local.valid(Local) || local.Local != "ready" {
		return nil, ErrProcess
	}
	request := a.request
	request.Outputs = append([]artifactDeclaration(nil), request.Outputs...)
	request.Target = target
	var recorded kernelReference
	if closedObject([]byte(ref.Resource), &recorded) != nil {
		return nil, ErrExecutionIdentity
	}
	request.Execution.KernelID = recorded.KernelID
	called, violated := false, false
	err = a.executor.stager.credentials.WithCredential(ctx, a.executor.stager.config.CredentialRef, func(token []byte) error {
		if called {
			violated = true
			return ErrProtocol
		}
		called = true
		if len(token) == 0 || len(token) > credentials.MaxBytes {
			return ErrConfig
		}
		for _, b := range token {
			if b < 33 || b > 126 {
				return ErrConfig
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var inner error
		raw, inner = a.run(ctx, a.executor.stager.config, mode, token, request, dst)
		return inner
	})
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil || !called || violated {
		return nil, ErrProcess
	}
	if mode == "fetch" && len(raw) != 0 {
		return nil, ErrProtocol
	}
	return raw, nil
}

func decodeArtifactCatalog(raw []byte) ([]artifactEntry, error) {
	if len(raw) > artifactCatalogBytes || !utf8.Valid(raw) {
		return nil, ErrProtocol
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, ErrProtocol
	}
	seen := map[string]bool{}
	var files []json.RawMessage
	for d.More() {
		t, err := d.Token()
		key, ok := t.(string)
		if err != nil || !ok || seen[key] {
			return nil, ErrProtocol
		}
		seen[key] = true
		switch key {
		case "protocol":
			var n int
			if d.Decode(&n) != nil || n != 1 {
				return nil, ErrProtocol
			}
		case "files":
			if d.Decode(&files) != nil {
				return nil, ErrProtocol
			}
		default:
			return nil, ErrProtocol
		}
	}
	token, err = d.Token()
	if err != nil || token != json.Delim('}') || d.Decode(new(any)) != io.EOF || len(seen) != 2 || len(files) < 1 || len(files) > artifactFileLimit {
		return nil, ErrProtocol
	}
	result := make([]artifactEntry, len(files))
	for i, f := range files {
		if closedObject(f, &result[i]) != nil {
			return nil, ErrProtocol
		}
	}
	return result, nil
}

type artifactWriter struct {
	ctx      context.Context
	dst      io.Writer
	limit, n int64
	hash     hash.Hash
	failed   bool
	cancel   context.CancelFunc
}

func (w *artifactWriter) Write(p []byte) (n int, err error) {
	defer func() {
		if recover() != nil {
			n = 0
			err = ErrArtifactTransfer
		}
		if err != nil {
			w.failed = true
			w.cancel()
		}
	}()
	if w.failed || w.ctx.Err() != nil || int64(len(p)) > w.limit-w.n {
		return 0, ErrArtifactTransfer
	}
	n, err = w.dst.Write(p)
	if n < 0 || n > len(p) {
		return 0, ErrArtifactTransfer
	}
	w.n += int64(n)
	_, _ = w.hash.Write(p[:n])
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}
