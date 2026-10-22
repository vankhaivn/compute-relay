package kaggle

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/runnerplan"
)

const artifactManifest = "control/execution-result.json"
const artifactCatalogBytes = 8 << 20
const artifactFileLimit = provider.MaxArtifactFiles

var ErrArtifactTransfer = errors.New("artifact verification failed; retain the original attempt and result pin")

// artifactFailure exposes only a closed stage reason. Raw provider/OS text is
// intentionally excluded even from the wrapped cause.
func artifactFailure(reason string) domain.Problem {
	if !domain.ValidCollectionReason(reason) {
		reason = "artifact_read_failed"
	}
	return provider.Problem(domain.CodeArtifactCollectionFailed, domain.FailureStageResults,
		"artifact collection did not verify; retain the original attempt").
		WithDetail(domain.ReasonDetail, reason).WithCause(ErrArtifactTransfer)
}

func boundedArtifactFailure(err error) error {
	var problem domain.Problem
	if errors.As(err, &problem) && problem.Code == domain.CodeArtifactCollectionFailed &&
		domain.ValidCollectionReason(problem.PublicDetails()[domain.ReasonDetail]) {
		return artifactFailure(problem.PublicDetails()[domain.ReasonDetail])
	}
	return artifactFailure("artifact_read_failed")
}

func artifactContextFailure(err error) error {
	reason := "artifact_runtime_stopped"
	if errors.Is(err, context.DeadlineExceeded) {
		reason = "artifact_call_timeout"
	}
	return artifactFailure(reason).WithCause(errors.Join(ErrArtifactTransfer, err))
}

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
type artifactRequest struct {
	Protocol  int                   `json:"protocol"`
	Execution executionRequest      `json:"execution"`
	Identity  provider.Identity     `json:"identity"`
	Outputs   []artifactDeclaration `json:"outputs"`
	MaxFiles  int                   `json:"max_files"`
	MaxBytes  int64                 `json:"max_bytes"`
	Target    *artifactEntry        `json:"target"`
}
type artifactInvocation func(context.Context, Config, string, []byte, artifactRequest, io.Writer) ([]byte, error)

// ArtifactReader is one attempt's read-only output port. It never calls Submit,
// Prepare, Cancel or Cleanup. Only M3 can pin/publish verified local artifacts.
// Every listing returns a complete bounded catalog. Recovery uses the durable result pin.
type ArtifactReader struct {
	executor *Executor
	policy   ArtifactPolicy
	request  artifactRequest
	slot     chan struct{}
	run      artifactInvocation
	runBatch artifactBatchInvocation
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
	r.Identity = e.plan.Job.Identity
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
	return &ArtifactReader{executor: e, policy: policy, request: r, slot: make(chan struct{}, 1), run: runArtifacts, runBatch: runArtifactBatch}, nil
}
func artifactSafePath(path string) bool {
	if !provider.SafeArtifactPath(path) || len(path) > 512 || strings.ContainsAny(path, `%<>"|?*`) {
		return false
	}
	for _, c := range path {
		if c < 32 || c > 126 {
			return false
		}
	}
	for _, part := range strings.Split(path, "/") {
		if len(part) > 255 || strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
			return false
		}
		stem := strings.ToUpper(strings.SplitN(part, ".", 2)[0])
		if stem == "CON" || stem == "PRN" || stem == "AUX" || stem == "NUL" ||
			len(stem) == 4 && (strings.HasPrefix(stem, "COM") || strings.HasPrefix(stem, "LPT")) && stem[3] >= '0' && stem[3] <= '9' {
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
	return a.enterFor(parent, a.policy.Timeout)
}
func (a *ArtifactReader) enterFor(parent context.Context, budget time.Duration) (context.Context, context.CancelFunc, error) {
	if a == nil {
		return nil, nil, ErrConfig
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	select {
	case a.slot <- struct{}{}:
		return ctx, func() { <-a.slot; cancel() }, nil
	case <-ctx.Done():
		cancel()
		return nil, nil, ctx.Err()
	}
}

func (a *ArtifactReader) ListArtifacts(parent context.Context, ref provider.RemoteReference) ([]provider.Artifact, error) {
	var none []provider.Artifact
	if a == nil {
		return none, ErrConfig
	}
	if err := a.executor.checkOperationalReference(ref); err != nil {
		return none, artifactFailure("artifact_remote_identity_mismatch")
	}
	ctx, leave, err := a.enter(parent)
	if err != nil {
		return none, artifactContextFailure(err)
	}
	defer leave()
	raw, err := a.call(ctx, "catalog", ref, nil, nil)
	if err != nil {
		return none, err
	}
	files, err := decodeArtifactCatalog(raw)
	if err != nil {
		return none, artifactFailure("artifact_catalog_invalid")
	}
	var total int64
	seen := map[string]bool{}
	hasManifest := false
	if len(files) > a.policy.MaxFiles {
		return none, artifactFailure("artifact_file_limit")
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	for _, f := range files {
		if !a.allowed(f.Path) || f.Bytes < 0 || !f.SHA256.Valid() {
			return none, artifactFailure("artifact_catalog_invalid")
		}
		if f.Bytes > a.policy.MaxBytes-total {
			return none, artifactFailure("artifact_byte_limit")
		}
		if limit, ok := artifactControlLimit(f.Path); ok && f.Bytes > limit {
			return none, artifactFailure("artifact_byte_limit")
		}
		key := strings.ToLower(f.Path)
		parts := strings.Split(key, "/")
		if seen[key] {
			return none, artifactFailure("artifact_catalog_invalid")
		}
		for i := 1; i < len(parts); i++ {
			if seen[strings.Join(parts[:i], "/")] {
				return none, artifactFailure("artifact_catalog_invalid")
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
				return none, artifactFailure("artifact_catalog_invalid")
			}
		}
	}
	if !hasManifest {
		return none, artifactFailure("artifact_catalog_invalid")
	}

	result := make([]provider.Artifact, 0, len(files))
	for _, f := range files {
		result = append(result, provider.Artifact{Remote: ref, Path: f.Path, Bytes: f.Bytes, SHA256: f.SHA256})
	}
	return result, nil
}

// Destination MUST be temporary/unpublished. A full payload followed by failed
// EOF/Close, digest or final identity check is still an error. M3's
// transfer pipe sends successful EOF only after this entire method succeeds.
func (a *ArtifactReader) Fetch(parent context.Context, ref provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	var none provider.TransferResult
	if a == nil || dst == nil || file.Validate(ref) != nil || !a.allowed(file.Path) || limit < file.Bytes || file.Bytes > a.policy.MaxBytes {
		return none, artifactFailure("artifact_request_invalid")
	}
	if n, ok := artifactControlLimit(file.Path); ok && file.Bytes > n {
		return none, artifactFailure("artifact_byte_limit")
	}
	if err := a.executor.checkOperationalReference(ref); err != nil {
		return none, artifactFailure("artifact_remote_identity_mismatch")
	}
	ctx, leave, err := a.enter(parent)
	if err != nil {
		return none, artifactContextFailure(err)
	}
	defer leave()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var observer *artifactProgressWriter
	if progress != nil {
		observer = &artifactProgressWriter{dst: dst, progress: progress}
		dst = observer
	}
	var manifest bytes.Buffer
	if file.Path == artifactManifest {
		dst = io.MultiWriter(dst, &manifest)
	}
	checked := &artifactWriter{ctx: ctx, dst: dst, limit: file.Bytes, hash: sha256.New(), cancel: cancel}
	_, err = a.call(ctx, "fetch", ref, &artifactEntry{file.Path, file.Bytes, file.SHA256}, checked)
	if observer != nil && observer.err != nil {
		if provider.IsLocalStateError(err) {
			return none, errors.Join(err, observer.err)
		}
		return none, observer.err
	}
	if provider.IsLocalStateError(err) {
		return none, err
	}
	if checked.failed {
		return none, artifactFailure("artifact_write_failed")
	}
	if err != nil {
		return none, err
	}
	if checked.n != file.Bytes || hex.EncodeToString(checked.hash.Sum(nil)) != string(file.SHA256) {
		return none, artifactFailure("artifact_digest_mismatch")
	}
	if file.Path == artifactManifest {
		if _, err := runnerplan.ParseResult(ref.Identity, manifest.Bytes()); err != nil {
			return none, artifactFailure("artifact_manifest_invalid")
		}
	}
	return provider.TransferResult{Bytes: file.Bytes, SHA256: file.SHA256}, nil
}

type artifactProgressWriter struct {
	dst      io.Writer
	progress func(int64) error
	n        int64
	err      error
}

func (w *artifactProgressWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.dst.Write(p)
	if n < 0 || n > len(p) {
		return 0, ErrArtifactTransfer
	}
	w.n += int64(n)
	if err == nil && n == len(p) {
		w.err = w.progress(w.n)
		if w.err != nil {
			return n, w.err
		}
	}
	return n, err
}
func (a *ArtifactReader) call(ctx context.Context, mode string, ref provider.RemoteReference, target *artifactEntry, dst io.Writer) (raw []byte, err error) {
	defer func() {
		if recover() != nil {
			raw = nil
			err = artifactFailure("artifact_read_failed")
		}
	}()
	request := a.request
	request.Outputs = append([]artifactDeclaration(nil), request.Outputs...)
	request.Target = target
	var recorded kernelReference
	if closedObject([]byte(ref.Resource), &recorded) != nil {
		return nil, artifactFailure("artifact_remote_identity_mismatch")
	}
	request.Execution.KernelID = recorded.KernelID
	called, violated := false, false
	err = a.executor.stager.credentials.WithCredential(ctx, a.executor.stager.config.CredentialRef, func(token []byte) error {
		if called {
			violated = true
			return ErrProtocol
		}
		called = true
		if !validArtifactToken(token) {
			return artifactFailure("artifact_credential_unavailable")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var inner error
		raw, inner = a.run(ctx, a.executor.stager.config, mode, token, request, dst)
		return inner
	})
	if provider.IsLocalStateError(err) {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, artifactContextFailure(ctx.Err())
	}
	if !called {
		return nil, artifactFailure("artifact_credential_unavailable")
	}
	if violated {
		return nil, artifactFailure("artifact_request_invalid")
	}
	if err != nil {
		return nil, boundedArtifactFailure(err)
	}
	if mode == "fetch" && len(raw) != 0 {
		return nil, artifactFailure("artifact_catalog_invalid")
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
