package dispatch

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/httpsinput"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/safefs"
)

type BlobStore interface {
	Put(context.Context, domain.ObjectMetadata, io.Reader) (domain.ObjectMetadata, error)
	Open(context.Context, domain.WorkspaceID, domain.ObjectID) (io.ReadCloser, error)
}

// Production must use the existing SSRF-protected httpsinput.Client. A nil fetcher
// disables direct URL preparation; it never weakens the public-address policy.
type HTTPSFetcher interface {
	Fetch(context.Context, httpsinput.Request, func(context.Context, int64, io.Reader) error) error
}

func (e *Engine) freeze(ctx context.Context, s *session) (provider.InputSnapshot, error) {
	work := s.work()
	spec := work.Job.Request.Spec()
	workspace := work.Handle.Claim.WorkspaceID
	refs := map[string]domain.ObjectMetadata{}
	for _, ref := range work.Job.Objects {
		refs[ref.Role] = ref.Object
	}
	var total int64
	for i, in := range spec.Inputs {
		role := "input:" + strconv.Itoa(i)
		if m, ok := refs[role]; ok {
			if m.Bytes > work.Job.Profile.MaxInputBytes-total {
				return provider.InputSnapshot{}, problem(domain.CodeInputTooLarge, false)
			}
			total += m.Bytes
			continue
		}
		if in.Source.Kind != "https" {
			return provider.InputSnapshot{}, problem(domain.CodeInputNotFound, false)
		}
		if e.fetcher == nil {
			return provider.InputSnapshot{}, problem(domain.CodeConfigurationInvalid, false)
		}
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return provider.InputSnapshot{}, ErrInvalid
		}
		meta := domain.ObjectMetadata{ID: domain.ObjectID("obj_" + hex.EncodeToString(random[:])), WorkspaceID: workspace, Bytes: -1, SHA256: in.Source.ExpectedSHA256}
		called := false
		remaining := work.Job.Profile.MaxInputBytes - total
		err := e.fetcher.Fetch(ctx, httpsinput.Request{URL: in.Source.URL, SHA256: string(in.Source.ExpectedSHA256)}, func(inner context.Context, length int64, r io.Reader) error {
			if called || length < -1 {
				return ErrInvalid
			}
			called = true
			if length > min(int64(2<<30), remaining) {
				return problem(domain.CodeInputTooLarge, false)
			}
			meta.Bytes = length
			// A limit reader that reports an error instead of manufacturing successful EOF.
			bounded := &limitedInput{r: r, left: min(int64(2<<30), remaining)}
			result, err := e.blobs.Put(inner, meta, bounded)
			if err != nil {
				return err
			}
			if !result.Valid() || result.ID != meta.ID || result.WorkspaceID != workspace || result.Bytes > remaining || result.Bytes > 2<<30 || length >= 0 && result.Bytes != length || meta.SHA256 != "" && meta.SHA256 != result.SHA256 {
				return ErrInvalid
			}
			meta = result
			return s.freeze(i, result)
		})
		if err != nil || !called {
			if err == nil {
				err = ErrInvalid
			}
			return provider.InputSnapshot{}, inputProblem(err)
		}
		refs[role] = meta
		total += meta.Bytes
	}
	snapshot, err := Snapshot(work.Job, refs)
	if err != nil {
		return snapshot, err
	}
	bundle, err := e.blobs.Open(ctx, workspace, snapshot.Bundle.ID)
	if err != nil {
		return snapshot, problem(domain.CodeInputNotFound, false)
	}
	limits := packaging.DefaultLimits()
	limits.MaxCompressedBytes = work.Job.Profile.MaxBundleBytes
	report, inspectErr := packaging.Inspect(ctx, bundle, limits)
	closeErr := bundle.Close()
	if inspectErr != nil || closeErr != nil || report.Bytes != snapshot.Bundle.Bytes || domain.SHA256Digest(report.SHA256) != snapshot.Bundle.SHA256 {
		return snapshot, problem(domain.CodeInputDigestMismatch, false)
	}
	if err = runnerCompatible(spec, report.Manifest); err != nil {
		return snapshot, err
	}
	for _, in := range snapshot.Inputs {
		r, err := e.blobs.Open(ctx, workspace, in.Object.ID)
		if err != nil {
			return snapshot, problem(domain.CodeInputNotFound, false)
		}
		h := sha256.New()
		n, copyErr := io.CopyBuffer(h, &contextInput{ctx: ctx, r: io.LimitReader(r, in.Object.Bytes+1)}, make([]byte, 64<<10))
		closeErr := r.Close()
		if copyErr != nil || closeErr != nil {
			return snapshot, problem(domain.CodeInputFetchFailed, false)
		}
		if n != in.Object.Bytes || hex.EncodeToString(h.Sum(nil)) != string(in.Object.SHA256) {
			return snapshot, problem(domain.CodeInputDigestMismatch, false)
		}
	}
	return snapshot, nil
}

// Snapshot checks the complete durable role map. SQL callers additionally compare this
// with owned job_objects; an application cannot manufacture an inputs.ready assertion.
func Snapshot(record admission.Record, refs map[string]domain.ObjectMetadata) (provider.InputSnapshot, error) {
	spec := record.Request.Spec()
	w := record.Job.WorkspaceID
	snapshot := provider.InputSnapshot{Bundle: refs["bundle"], Inputs: []provider.FrozenInput{}}
	if snapshot.Bundle.ID != spec.Bundle.ObjectID || snapshot.Bundle.Bytes > record.Profile.MaxBundleBytes {
		return snapshot, ErrInvalid
	}
	var total int64
	if len(refs) != 1+len(spec.Inputs) {
		return snapshot, ErrInvalid
	}
	for i, in := range spec.Inputs {
		m, ok := refs["input:"+strconv.Itoa(i)]
		if !ok || m.Bytes > record.Profile.MaxInputBytes-total {
			return snapshot, ErrInvalid
		}
		if in.Source.Kind == "object" && m.ID != in.Source.ObjectID || in.Source.Kind == "https" && in.Source.ExpectedSHA256 != "" && m.SHA256 != in.Source.ExpectedSHA256 {
			return snapshot, ErrInvalid
		}
		snapshot.Inputs = append(snapshot.Inputs, provider.FrozenInput{Name: in.Name, Target: in.Target, Object: m})
		total += m.Bytes
	}
	if snapshot.Validate(w) != nil {
		return snapshot, ErrInvalid
	}
	return snapshot, nil
}
func ResolveJob(work Work, snapshot provider.InputSnapshot) (provider.ResolvedJob, domain.OperationID, error) {
	var job provider.ResolvedJob
	if snapshot.Validate(work.Job.Job.WorkspaceID) != nil || !work.InstallationID.Valid() {
		return job, "", ErrInvalid
	}
	spec := work.Job.Request.Spec()
	// Preserve exact numeric/string/optional-field semantics with RawMessage, changing
	// only source records into the already frozen objects. Source URLs never reach adapters.
	var raw map[string]json.RawMessage
	if json.Unmarshal(work.Job.Request.Canonical(), &raw) != nil {
		return job, "", ErrInvalid
	}
	var inputs []map[string]json.RawMessage
	if json.Unmarshal(raw["inputs"], &inputs) != nil || len(inputs) != len(snapshot.Inputs) {
		return job, "", ErrInvalid
	}
	for i := range inputs {
		source, _ := json.Marshal(admission.Source{Kind: "object", ObjectID: snapshot.Inputs[i].Object.ID})
		inputs[i]["source"] = source
	}
	raw["inputs"], _ = json.Marshal(inputs)
	encoded, err := json.Marshal(raw)
	if err != nil {
		return job, "", ErrInvalid
	}
	resolved, err := admission.Parse(encoded)
	if err != nil {
		return job, "", err
	}
	seed, _ := json.Marshal([]string{string(work.InstallationID), string(work.Job.Job.WorkspaceID), string(work.Job.Job.ID), string(work.Job.Attempt.ID)})
	key := string(provider.Digest(seed))
	id := provider.Identity{InstallationID: work.InstallationID, WorkspaceID: work.Job.Job.WorkspaceID, JobID: work.Job.Job.ID, AttemptID: work.Job.Attempt.ID, InstanceID: work.Job.Profile.Binding.ProviderInstanceID, IntentID: domain.SubmissionIntentID("si_" + key), ResourceKey: "cr-" + key[:40], Nonce: work.Job.AttemptNonce, BundleSHA256: snapshot.Bundle.SHA256, InputManifestSHA256: snapshot.InputDigest()}
	required := []domain.CapabilityName{domain.CapabilityBatchExecution, domain.CapabilityPrivateInputStaging, domain.CapabilityExecutionTimeout, domain.CapabilityStrongIdentity, domain.CapabilityRemoteNetworkControl}
	if spec.Execution.Kind == "python" {
		required = append(required, domain.CapabilityPython)
	} else {
		required = append(required, domain.CapabilityShell)
	}
	if spec.Resources.Accelerator == "gpu" {
		required = append(required, domain.CapabilityGPU)
	}
	sort.Slice(required, func(i, j int) bool { return required[i] < required[j] })
	copy := snapshot.Clone()
	job = provider.ResolvedJob{Identity: id, Binding: work.Job.Profile.Binding, Specification: resolved.Canonical(), SpecificationSHA256: provider.Digest(resolved.Canonical()), Required: required, WallSeconds: spec.Timeouts.RemoteWallSeconds, Inputs: &copy}
	return job, domain.OperationID("prep_" + key), job.Validate()
}
func ValidatedPlan(expected provider.ResolvedJob, p provider.Plan, d provider.Descriptor) error {
	a, _ := json.Marshal(expected)
	b, _ := json.Marshal(p.Job)
	if string(a) != string(b) || p.Job.Validate() != nil || d.InstanceID != expected.Identity.InstanceID {
		return ErrInvalid
	}
	seen := map[domain.CapabilityName]bool{}
	for _, c := range p.VerifyAfterStart {
		if c != domain.CapabilityGPU || seen[c] {
			return ErrInvalid
		}
		seen[c] = true
	}
	gpuRequired := false
	for _, c := range expected.Required {
		if c == domain.CapabilityGPU {
			gpuRequired = true
		}
		support := d.Support(c)
		if !support.Valid() || support == domain.CapabilitySupportUnsupported || support != domain.CapabilitySupportSupported && !(c == domain.CapabilityGPU && seen[c]) {
			return problem(domain.CodeUnsupportedCapability, false)
		}
	}
	if seen[domain.CapabilityGPU] && !gpuRequired {
		return ErrInvalid
	}
	return nil
}
func runnerCompatible(spec admission.Spec, m packaging.Manifest) error {
	fail := func() error { return problem(domain.CodeResourceRequirementUnsatisfied, false) }
	if len(spec.Execution.Command) == 0 {
		return fail()
	}
	for _, arg := range spec.Execution.Command {
		if strings.ContainsRune(arg, 0) {
			return fail()
		}
	}
	cmd := spec.Execution.Command[0]
	if spec.Execution.Kind == "python" && cmd != "python" && cmd != "python3" || spec.Execution.Kind == "shell" && cmd != "bash" && cmd != "sh" {
		return fail()
	}
	validPath := func(p string) bool {
		if p == "." || len(p) > 240 || len(strings.Split(p, "/")) > 32 || !safefs.ValidPath(p) {
			return false
		}
		for _, part := range strings.Split(p, "/") {
			switch strings.ToLower(part) {
			case ".compute-relay", ".git", ".ssh", ".env":
				return false
			}
		}
		return true
	}
	wd := spec.Execution.WorkingDirectory
	if wd == "" {
		wd = "."
	}
	if wd != "." && !validPath(wd) {
		return fail()
	}
	exists := wd == "."
	reqExists := spec.Execution.Dependencies.PythonRequirements == ""
	for _, f := range m.Files {
		if strings.HasPrefix(f.Path, wd+"/") {
			exists = true
		}
		if f.Path == spec.Execution.Dependencies.PythonRequirements {
			reqExists = true
		}
	}
	if !exists || !reqExists {
		return fail()
	}
	inputPaths := []string{}
	outputPaths := []string{}
	for _, in := range spec.Inputs {
		inputPaths = append(inputPaths, in.Target)
		if !validPath(in.Target) {
			return fail()
		}
	}
	for _, out := range spec.Outputs {
		outputPaths = append(outputPaths, out.Path)
		if !validPath(out.Path) {
			return fail()
		}
	}
	if !disjointRunnerPaths(inputPaths) || !disjointRunnerPaths(outputPaths) {
		return fail()
	}
	for k, v := range spec.Execution.Environment {
		u := strings.ToUpper(k)
		for _, prefix := range []string{"CC_", "PYTHON", "PIP_", "LD_", "DYLD_", "CUDA_", "NVIDIA_", "AWS_", "KAGGLE_"} {
			if strings.HasPrefix(u, prefix) {
				return fail()
			}
		}
		for _, part := range []string{"TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "API_KEY", "PROXY"} {
			if strings.Contains(u, part) {
				return fail()
			}
		}
		switch u {
		case "PATH", "HOME", "TMPDIR", "VIRTUAL_ENV", "BASH_ENV", "ENV", "SHELLOPTS", "BASHOPTS":
			return fail()
		}
		if strings.ContainsRune(v, 0) {
			return fail()
		}
	}
	return nil
}
func inputProblem(err error) error {
	var p *domain.Problem
	if errors.As(err, &p) && p != nil {
		return p
	}
	return problem(domain.CodeInputFetchFailed, false)
}

type limitedInput struct {
	r    io.Reader
	left int64
}

func (r *limitedInput) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.left == 0 {
		var b [1]byte
		n, err := r.r.Read(b[:])
		if n > 0 {
			return 0, problem(domain.CodeInputTooLarge, false)
		}
		return 0, err
	}
	if int64(len(p)) > r.left {
		p = p[:r.left]
	}
	n, err := r.r.Read(p)
	r.left -= int64(n)
	return n, err
}

type contextInput struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextInput) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// Match the runner's case/implicit-directory rules before any provider mutation.
func disjointRunnerPaths(paths []string) bool {
	names, leaves := map[string]string{}, map[string]bool{}
	for _, path := range paths {
		parts := strings.Split(path, "/")
		for n := 1; n <= len(parts); n++ {
			prefix := strings.Join(parts[:n], "/")
			key := strings.ToLower(prefix)
			prior, exists := names[key]
			if leaves[key] || exists && prior != prefix {
				return false
			}
			if n == len(parts) {
				if exists {
					return false
				}
				leaves[key] = true
			}
			names[key] = prefix
		}
	}
	return true
}
