package sshhost

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/runnerplan"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
	runnerassets "github.com/vankhaivn/compute-relay/runner"
)

// ProviderType is the managed connection and descriptor type of this adapter.
const ProviderType = "ssh"

const (
	Version          = "ssh-docker/1"
	containerPrefix  = "compute-relay-"
	labelPrefix      = "io.compute-relay."
	workRoot         = "/relay/work"
	stagedMarker     = "/relay/staged.json"
	cancelMarker     = "/relay/cancel-requested.json"
	nobody           = 65534
	pidsLimit        = 4096
	timeoutGrace     = 60
	timeoutExit      = 124
	maxControlBytes  = 20 << 20
	maxManifestBytes = 1 << 20
	maxLogLines      = 1 << 24
	maxLogLineBytes  = 16 << 10
	maxLogPageBytes  = 256 << 10
)

var (
	ErrBinding  = errors.New("SSH host binding does not match the frozen connection")
	ErrIdentity = errors.New("remote container does not match the recorded attempt")
	ErrNotDone  = errors.New("remote container has not finished")
	ErrArtifact = errors.New("remote artifact does not match its listing")

	errStagingStopped = errors.New("remote copy ended before staging completed")

	resourceKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,100}$`)
	containerID = regexp.MustCompile(`^[0-9a-f]{64}$`)

	// controlFiles are the runner control files collection may select.
	controlFiles = []string{"environment.json", "execution-result.json", "stderr.log", "stdout.log"}
)

// entryPoint mirrors runner/python/run.py; only the relay_runner modules are embedded.
const entryPoint = `"""Explicit entry point for one remote attempt or non-executing manifest validation."""
from relay_runner.main import main

if __name__ == "__main__":
    raise SystemExit(main())
`

// StagingBlobs supplies frozen input bytes by workspace-scoped object identity.
type StagingBlobs interface {
	Open(context.Context, domain.WorkspaceID, domain.ObjectID) (io.ReadCloser, error)
}

// Config is one verified connection revision. Account is the canonical host identity
// that pins the host key; Image is the verified standard image reference.
type Config struct {
	InstanceID        domain.ProviderInstanceID
	Account           string
	CredentialRef     ports.CredentialRef
	AllowInternet     bool
	MaxWallSeconds    int64
	Image             string
	MaxProcesses      int
	Architecture      string
	ImageRecipeSHA256 string
	CPUs              string
	MemoryBytes       int64
	MachineID         string
	EngineID          string
}

type session interface {
	executor
	Close() error
}

// Adapter runs one attempt per container on an operator-owned Docker host. Every call
// dials with the vault credential and the pinned host key; no connection is cached.
type Adapter struct {
	config  Config
	binding provider.BindingSnapshot
	creds   ports.CredentialResolver
	blobs   StagingBlobs
	clock   ports.Clock
	dial    func(context.Context, Target, Credential) (session, error)
}

var (
	_ provider.Provider = (*Adapter)(nil)
)

func NewAdapter(config Config, binding provider.BindingSnapshot, creds ports.CredentialResolver, blobs StagingBlobs, clock ports.Clock) (*Adapter, error) {
	if !binding.Valid() || binding.Binding.ProviderInstanceID != config.InstanceID || binding.CredentialRef != string(config.CredentialRef) ||
		creds == nil || blobs == nil || clock == nil || config.MaxWallSeconds < 1 || config.MaxWallSeconds > 86400 || config.MaxProcesses < 0 || config.MaxProcesses > 65536 {
		return nil, ErrRequest
	}
	if config.MachineID != "" && !domain.SHA256Digest(config.MachineID).Valid() ||
		config.EngineID != "" && (!domain.SHA256Digest(config.EngineID).Valid() || config.MachineID == "") {
		return nil, ErrRequest
	}
	if config.ImageRecipeSHA256 == "" {
		if !ValidImage(config.Image) || config.Architecture != "" || config.CPUs != "" || config.MemoryBytes != 0 {
			return nil, ErrRequest
		}
	} else if !ValidImageID(config.Image) || !domain.SHA256Digest(config.ImageRecipeSHA256).Valid() || !SupportedArchitecture(config.Architecture) || !ValidResourceLimits(config.CPUs, config.MemoryBytes) {
		return nil, ErrRequest
	}
	if config.MaxProcesses == 0 {
		config.MaxProcesses = pidsLimit
	}
	if _, err := HostKeyOf(config.Account); err != nil {
		return nil, ErrRequest
	}
	return &Adapter{config: config, binding: binding, creds: creds, blobs: blobs, clock: clock,
		dial: func(ctx context.Context, t Target, c Credential) (session, error) { return Dial(ctx, t, c) }}, nil
}

// withDocker resolves the vault entry, pins it to the frozen account, dials and finds
// the Docker CLI. The credential never outlives the callback.
func (a *Adapter) withDocker(ctx context.Context, use func(docker, Endpoint) error) error {
	return a.creds.WithCredential(ctx, a.config.CredentialRef, func(raw []byte) error {
		endpoint, err := ParseEndpoint(raw)
		if err != nil {
			return ErrBinding
		}
		if endpoint, err = endpoint.Pin(a.config.Account); err != nil {
			return err
		}
		conn, err := a.dial(ctx, endpoint.Target, endpoint.Credential)
		if err != nil {
			return err
		}
		defer conn.Close()
		binary, err := findDocker(ctx, conn, endpoint.DockerPath)
		if err != nil {
			return err
		}
		d := docker{exec: conn, binary: binary}
		if a.config.MachineID != "" {
			d, err = d.localResource(ctx)
			if err != nil {
				return err
			}
			if err = d.verifyResource(ctx, a.config.MachineID, a.config.EngineID); err != nil {
				return err
			}
		}
		return use(d, endpoint)
	})
}

func (a *Adapter) Describe() provider.Descriptor {
	d := provider.Descriptor{Type: ProviderType, InstanceID: a.config.InstanceID, Version: Version}
	for _, name := range []domain.CapabilityName{
		domain.CapabilityBatchExecution, domain.CapabilityPython, domain.CapabilityShell, domain.CapabilityGPU,
		domain.CapabilityPrivateInputStaging, domain.CapabilityRemoteCancellation, domain.CapabilityRemoteCleanup, domain.CapabilityLogsWhileRunning,
		domain.CapabilityLogsAfterCompletion, domain.CapabilityQuotaReporting, domain.CapabilityExecutionTimeout,
		domain.CapabilityRemoteNetworkControl, domain.CapabilityStrongIdentity, domain.CapabilityCustomContainer,
		domain.CapabilityRetainedSessions, domain.CapabilityAbsoluteDeadline, domain.CapabilitySeparateStart, domain.CapabilityEphemeralSecrets,
	} {
		c := domain.CapabilityStatus{Name: name, Support: domain.CapabilitySupportSupported, Evidence: domain.EvidenceImplementedOffline,
			ClientVersion: Version, Reason: "operator-owned Docker host reached over SSH with a pinned host key"}
		switch name {
		case domain.CapabilityGPU:
			c.Support, c.Reason = domain.CapabilitySupportUnsupported, "this adapter runs CPU containers only"
		case domain.CapabilityQuotaReporting:
			c.Support, c.Reason = domain.CapabilitySupportUnsupported, "an operator-owned host has no provider quota"
		case domain.CapabilityCustomContainer:
			c.Support, c.Reason = domain.CapabilitySupportUnsupported, "jobs run in the Relay standard image"
		case domain.CapabilityRetainedSessions:
			c.Support, c.Reason = domain.CapabilitySupportUnsupported, "not implemented by this finite batch adapter"
		case domain.CapabilityPrivateInputStaging:
			c.Reason = "inputs are copied only into the attempt's own container"
		case domain.CapabilityExecutionTimeout:
			c.Reason = "the runner budget and a host-side timeout bound the container without Relay connectivity"
		case domain.CapabilityEphemeralSecrets:
			c.Reason = "one-shot attempt-scoped material delivered through stdin to private tmpfs after explicit start"
		case domain.CapabilitySeparateStart:
			c.Reason = "verified stopped-container preparation requires separate finite start authority"
		case domain.CapabilityAbsoluteDeadline:
			c.Reason = "UTC and Linux suspend-aware deadlines independently bound payload groups and finalization"
		case domain.CapabilityRemoteNetworkControl:
			c.Reason = "containers have no network unless the frozen policy allows internet"
		case domain.CapabilityRemoteCancellation:
			c.Reason = "Docker stops the container and reports its exit"
		case domain.CapabilityRemoteCleanup:
			c.Reason = "explicit cleanup of the original stopped container with recorded engine identity; host files, volumes and images are preserved"
		case domain.CapabilityStrongIdentity:
			c.Reason = "container name, ID and labels bind the exact attempt"
		}
		d.Capabilities = append(d.Capabilities, c)
	}
	return d
}

// Verify proves the frozen binding and a reachable Docker daemon on the pinned host.
func (a *Adapter) Verify(ctx context.Context, expected provider.BindingSnapshot) error {
	if a == nil || expected != a.binding {
		return ErrBinding
	}
	return a.withDocker(ctx, func(d docker, _ Endpoint) error { return d.ping(ctx) })
}

func (a *Adapter) Validate(ctx context.Context, job provider.ResolvedJob) (provider.Plan, error) {
	if err := ctx.Err(); err != nil {
		return provider.Plan{}, err
	}
	if a == nil || job.Validate() != nil || job.Inputs == nil || job.Identity.InstanceID != a.config.InstanceID ||
		job.Binding != a.binding.Binding || !resourceKey.MatchString(job.Identity.ResourceKey) {
		return provider.Plan{}, ErrBinding
	}
	plan := provider.Plan{Job: job.Clone()}
	_, spec, err := runnerplan.Build(plan)
	if err != nil {
		return provider.Plan{}, ErrRequest
	}
	if spec.Resources.Accelerator != "cpu" || job.WallSeconds > a.config.MaxWallSeconds ||
		spec.Network.RemoteInternet == "required" && !a.config.AllowInternet {
		return provider.Plan{}, provider.Problem(domain.CodeResourceRequirementUnsatisfied, domain.FailureStageValidation,
			"job requirements exceed the SSH host connection policy")
	}
	descriptor := a.Describe()
	for _, name := range job.Required {
		if descriptor.Support(name) != domain.CapabilitySupportSupported {
			return provider.Plan{}, provider.Problem(domain.CodeUnsupportedCapability, domain.FailureStageValidation,
				"required capability is unsupported by this SSH host provider")
		}
	}
	return plan, nil
}

func (a *Adapter) validPlan(ctx context.Context, plan provider.Plan) error {
	again, err := a.Validate(ctx, plan.Job)
	if err != nil || again.Digest() != plan.Digest() {
		return ErrBinding
	}
	return nil
}

func containerName(id provider.Identity) string { return containerPrefix + id.ResourceKey }

func labels(plan provider.Plan, operation domain.OperationID) map[string]string {
	id := plan.Job.Identity
	result := map[string]string{labelPrefix + "installation": string(id.InstallationID), labelPrefix + "instance": string(id.InstanceID),
		labelPrefix + "attempt": string(id.AttemptID), labelPrefix + "preparation": string(operation), labelPrefix + "plan": string(plan.Digest()),
		labelPrefix + "workspace": string(id.WorkspaceID), labelPrefix + "job": string(id.JobID), labelPrefix + "nonce": id.Nonce}
	if req, err := admission.Parse(plan.Job.Specification); err == nil {
		if req.Spec().Timeouts.ExecutionDeadline != "" {
			result[labelPrefix+"execution-deadline"] = req.Spec().Timeouts.ExecutionDeadline
		}
		if req.Spec().SeparateStart() {
			result[labelPrefix+"execution-control"] = "prepare_then_start"
		}
	}
	return result
}

// owned checks that a container carries this attempt's identity labels.
func owned(info containerInfo, id provider.Identity) bool {
	l := info.Config.Labels
	return containerID.MatchString(info.ID) && l[labelPrefix+"installation"] == string(id.InstallationID) &&
		l[labelPrefix+"instance"] == string(id.InstanceID) && l[labelPrefix+"attempt"] == string(id.AttemptID) && l[labelPrefix+"preparation"] != ""
}

func exited(info containerInfo) bool {
	return !info.State.Running && (info.State.Status == "exited" || info.State.Status == "dead")
}

type containerRef struct {
	Container string `json:"container"`
	ID        string `json:"id"`
	EngineID  string `json:"engine_id,omitempty"`
}

func encodeRef(name, id string, engine ...string) string {
	ref := containerRef{Container: name, ID: id}
	if len(engine) == 1 {
		ref.EngineID = engine[0]
	}
	raw, _ := json.Marshal(ref)
	return string(raw)
}

func decodeRef(resource string, id provider.Identity) (containerRef, error) {
	var ref containerRef
	decoder := json.NewDecoder(strings.NewReader(resource))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&ref) != nil || decoder.Decode(new(any)) != io.EOF || ref.Container != containerName(id) || !containerID.MatchString(ref.ID) || ref.EngineID != "" && !engineIdentity.MatchString(ref.EngineID) {
		return containerRef{}, ErrIdentity
	}
	return ref, nil
}

type marker struct {
	Preparation  domain.OperationID  `json:"preparation_id"`
	Plan         domain.SHA256Digest `json:"plan_sha256"`
	Container    string              `json:"container_id"`
	EngineID     string              `json:"engine_id,omitempty"`
	Verification domain.SHA256Digest `json:"verification_sha256,omitempty"`
	Manifest     domain.SHA256Digest `json:"manifest_sha256,omitempty"`
}

// Prepare creates the attempt's stopped container, copies the runner, manifest, bundle
// and inputs into it and then writes a completion marker. A failure before the marker
// removes the container this call created; nothing in it has run.
func (a *Adapter) Prepare(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.Prepared, error) {
	if err := a.validPlan(ctx, plan); err != nil {
		return provider.Prepared{}, err
	}
	manifest, spec, err := runnerplan.Build(plan)
	if err != nil {
		return provider.Prepared{}, ErrRequest
	}
	if spec.Expired(a.clock.Now()) {
		return provider.Prepared{}, admission.ErrExpired
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return provider.Prepared{}, ErrRequest
	}
	modules, err := runnerassets.Sources()
	if err != nil {
		return provider.Prepared{}, ErrRequest
	}
	var prepared provider.Prepared
	err = a.withDocker(ctx, func(d docker, endpoint Endpoint) error {
		engine, err := d.engineID(ctx)
		if err != nil {
			return err
		}
		// The digest-pinned image is pulled once per host; the job network policy
		// governs the container, not the engine's registry access.
		image, found, err := d.inspectImage(ctx, a.config.Image)
		if err == nil && !found && a.config.ImageRecipeSHA256 == "" && !spec.SeparateStart() {
			if err = d.pull(ctx, a.config.Image); err == nil {
				image, found, err = d.inspectImage(ctx, a.config.Image)
			}
		}
		if err != nil || !found || image.OS != "linux" {
			return ErrRemoteDocker
		}
		platform := ""
		if a.config.ImageRecipeSHA256 != "" {
			os, arch, err := d.platform(ctx)
			if err != nil || os != "linux" || arch != a.config.Architecture || image.Architecture != arch || image.ID != a.config.Image || image.Config.Labels[imageRecipeLabel] != a.config.ImageRecipeSHA256 {
				return ErrIdentity
			}
			endpoint.CPUs, endpoint.MemoryBytes = a.config.CPUs, a.config.MemoryBytes
			platform = "linux/" + a.config.Architecture
		}
		name := containerName(plan.Job.Identity)
		if _, exists, err := d.inspect(ctx, name); err != nil || exists {
			return ErrIdentity
		}
		mode := "disabled"
		if spec.Network.RemoteInternet == "required" {
			mode = "required"
		}
		if spec.Expired(a.clock.Now()) {
			return admission.ErrExpired
		}
		command := []string{"timeout", "-k", "30", strconv.FormatInt(plan.Job.WallSeconds+timeoutGrace, 10),
			"python3", "-E", "-s", "/relay/runner/run.py", "--manifest", "/relay/manifest.json", "--execute",
			"--staging-root", "/relay/staging", "--work-root", workRoot, "--network-mode", mode}
		if spec.Timeouts.ExecutionDeadline != "" || spec.SeparateStart() {
			// The trusted supervisor is PID 1, so its final exit also tears down any
			// remaining descendants in Docker's private PID namespace.
			command = append([]string(nil), command[4:]...)
			command[6] = "--supervise"
		}
		if spec.SeparateStart() {
			command = append(command, "--start-control", startControlPath)
		}
		ownedLabels := labels(plan, operation)
		ownedLabels[labelPrefix+"engine"] = engine
		id, err := d.create(ctx, containerSpec{Name: name, Image: image.ID, Platform: platform, Labels: ownedLabels, Internet: mode == "required",
			SecretTmpfs: len(spec.WorkloadSecrets) > 0,
			CPUs:        endpoint.CPUs, MemoryBytes: endpoint.MemoryBytes, PIDs: a.config.MaxProcesses,
			Command: command})
		if err != nil {
			return err
		}
		proof := marker{Preparation: operation, Plan: plan.Digest(), Container: id, EngineID: engine}
		if err = a.stage(ctx, d, id, plan, modules, manifestJSON); err == nil && spec.SeparateStart() {
			var info containerInfo
			var exists bool
			info, exists, err = d.inspect(ctx, name)
			if err == nil && (!exists || info.ID != id || info.Started() || info.Image != image.ID) {
				err = ErrIdentity
			}
			if err == nil {
				proof.Verification, err = a.verifyPreparedBytes(ctx, d, info, plan, modules, manifestJSON)
				proof.Manifest = provider.Digest(manifestJSON)
			}
		}
		if err == nil {
			staged, _ := json.Marshal(proof)
			err = d.copyIn(ctx, id, singleFile(strings.TrimPrefix(stagedMarker, "/"), staged))
		}
		if err != nil {
			_, _ = d.simple(context.WithoutCancel(ctx), "container", "rm", id)
			return err
		}
		prepared = provider.Prepared{Identity: plan.Job.Identity, PreparationID: operation, PlanSHA256: plan.Digest(),
			Resource: encodeRef(name, id, engine), Ready: true, Private: true, VerificationSHA256: proof.Verification, ManifestSHA256: proof.Manifest}
		return nil
	})
	if err != nil {
		return provider.Prepared{}, err
	}
	return prepared, nil
}

// stage streams one tar archive into the stopped container. Both the remote copy and
// the local writer, which verifies every frozen object, must succeed.
func (a *Adapter) stage(ctx context.Context, d docker, container string, plan provider.Plan, modules map[string]string, manifest []byte) error {
	reader, writer := io.Pipe()
	written := make(chan error, 1)
	go func() {
		err := a.writeStaging(ctx, writer, plan, modules, manifest)
		_ = writer.CloseWithError(err)
		written <- err
	}()
	err := d.copyIn(ctx, container, reader)
	_ = reader.CloseWithError(errStagingStopped)
	// A local verification failure is the cause of any remote copy error it produced.
	switch werr := <-written; {
	case werr != nil && !errors.Is(werr, errStagingStopped):
		return werr
	case err != nil:
		return err
	case werr != nil:
		return ErrArtifact
	}
	return nil
}

type tarEntry struct {
	name string
	kind byte
	uid  int
	data []byte
}

func (a *Adapter) writeStaging(ctx context.Context, w io.Writer, plan provider.Plan, modules map[string]string, manifest []byte) error {
	now := a.clock.Now().UTC().Truncate(time.Second)
	tw := tar.NewWriter(w)
	header := func(name string, kind byte, uid int, size int64) error {
		mode := int64(0o644)
		if kind == tar.TypeDir {
			mode = 0o755
		}
		return tw.WriteHeader(&tar.Header{Typeflag: kind, Name: name, Mode: mode, Uid: uid, Gid: uid, Size: size, ModTime: now, Format: tar.FormatPAX})
	}
	// The runner stays root-owned and read-only to the job user; staging belongs to it.
	entries := []tarEntry{{name: "relay/", kind: tar.TypeDir, uid: nobody}, {name: "relay/runner/", kind: tar.TypeDir},
		{name: "relay/runner/relay_runner/", kind: tar.TypeDir}, {name: "relay/runner/run.py", kind: tar.TypeReg, data: []byte(entryPoint)}}
	for _, module := range slices.Sorted(maps.Keys(modules)) {
		entries = append(entries, tarEntry{name: "relay/runner/relay_runner/" + module, kind: tar.TypeReg, data: []byte(modules[module])})
	}
	entries = append(entries, tarEntry{name: "relay/manifest.json", kind: tar.TypeReg, uid: nobody, data: manifest},
		tarEntry{name: "relay/staging/", kind: tar.TypeDir, uid: nobody})
	for _, e := range entries {
		if err := header(e.name, e.kind, e.uid, int64(len(e.data))); err != nil {
			return err
		}
		if _, err := tw.Write(e.data); err != nil {
			return err
		}
	}
	type staged struct {
		path   string
		object domain.ObjectMetadata
	}
	objects := []staged{{runnerplan.BundlePath, plan.Job.Inputs.Bundle}}
	for i, in := range plan.Job.Inputs.Inputs {
		objects = append(objects, staged{runnerplan.InputPath(i), in.Object})
	}
	var total, done int64
	for _, o := range objects {
		total += o.object.Bytes
	}
	for _, o := range objects {
		if err := header("relay/staging/"+o.path, tar.TypeReg, nobody, o.object.Bytes); err != nil {
			return err
		}
		if err := a.copyObject(ctx, tw, o.object, func(n int64) { provider.ReportPreparationProgress(ctx, done+n, total) }); err != nil {
			return err
		}
		done += o.object.Bytes
	}
	return tw.Close()
}

// copyObject writes exactly the frozen object's bytes and fails on any length or
// digest difference before the archive can be completed.
func (a *Adapter) copyObject(ctx context.Context, w io.Writer, object domain.ObjectMetadata, progress func(int64)) error {
	blob, err := a.blobs.Open(ctx, object.WorkspaceID, object.ID)
	if err != nil {
		return err
	}
	defer blob.Close()
	digest := sha256.New()
	copied, err := io.Copy(&progressWriter{w: io.MultiWriter(w, digest), report: progress}, io.LimitReader(blob, object.Bytes))
	if err != nil {
		return err
	}
	extra, _ := blob.Read(make([]byte, 1))
	if copied != object.Bytes || extra != 0 || domain.SHA256Digest(hex.EncodeToString(digest.Sum(nil))) != object.SHA256 {
		return ErrArtifact
	}
	progress(copied)
	return nil
}

// progressWriter reports at most once per 8 MiB of staged bytes.
type progressWriter struct {
	w      io.Writer
	n      int64
	next   int64
	report func(int64)
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	p.n += int64(n)
	if p.n >= p.next {
		p.report(p.n)
		p.next = p.n + 8<<20
	}
	return n, err
}

func singleFile(name string, data []byte) io.Reader {
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	_ = tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o644, Uid: nobody, Gid: nobody, Size: int64(len(data)), Format: tar.FormatPAX})
	_, _ = tw.Write(data)
	_ = tw.Close()
	return &b
}

// ReconcilePrepare is read-only: only a labelled container whose completion
// marker matches is found; a container without one is not ready.
func (a *Adapter) ReconcilePrepare(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.PreparationObservation, error) {
	if err := a.validPlan(ctx, plan); err != nil {
		return provider.PreparationObservation{}, err
	}
	req, _ := admission.Parse(plan.Job.Specification)
	if req.Spec().SeparateStart() {
		return a.reconcileControlledPreparation(ctx, plan, operation)
	}
	observation := provider.PreparationObservation{Status: provider.ReconciliationNotFound}
	err := a.withDocker(ctx, func(d docker, _ Endpoint) error {
		name := containerName(plan.Job.Identity)
		info, found, err := d.inspect(ctx, name)
		if err != nil || !found {
			return err
		}
		if !owned(info, plan.Job.Identity) || info.Config.Labels[labelPrefix+"preparation"] != string(operation) ||
			info.Config.Labels[labelPrefix+"plan"] != string(plan.Digest()) {
			return ErrIdentity
		}
		raw, found, err := readFile(ctx, d, info.ID, stagedMarker, 4096)
		var staged marker
		if err != nil || !found || json.Unmarshal(raw, &staged) != nil {
			return err
		}
		engine := info.Config.Labels[labelPrefix+"engine"]
		if err := d.verifyEngine(ctx, engine); err != nil {
			return err
		}
		if staged != (marker{Preparation: operation, Plan: plan.Digest(), Container: info.ID, EngineID: engine}) {
			return nil
		}
		observation = provider.PreparationObservation{Status: provider.ReconciliationFound, Prepared: &provider.Prepared{
			Identity: plan.Job.Identity, PreparationID: operation, PlanSHA256: plan.Digest(), Resource: encodeRef(name, info.ID, engine), Ready: true, Private: true}}
		return nil
	})
	if err != nil {
		return provider.PreparationObservation{}, err
	}
	return observation, nil
}

func rejected(reason string) provider.SubmissionOutcome {
	p := provider.Problem(domain.CodeProviderRejected, domain.FailureStageSubmission, "container start stopped before execution; inspect the host and staging").
		WithDetail(domain.ReasonDetail, reason)
	return provider.SubmissionOutcome{Status: provider.SubmissionRejected, Problem: &p}
}

func unknownSubmission() provider.SubmissionOutcome {
	p := provider.Problem(domain.CodeProviderSubmissionUnknown, domain.FailureStageSubmission, "container start unresolved; reconcile the original identity without restarting")
	p.ComputeMayHaveStarted = true
	p.SafeOperationRetry = false
	p.RecommendedAction = domain.RecommendedActionReconcile
	return provider.SubmissionOutcome{Status: provider.SubmissionUnknown, Problem: &p}
}

func (a *Adapter) submit(ctx context.Context, prepared provider.Prepared, start *provider.StartAuthorization, material *workloadsecret.Material) provider.SubmissionOutcome {
	if a == nil || prepared.Identity.Validate() != nil || !prepared.PlanSHA256.Valid() || !prepared.PreparationID.Valid() || !prepared.Ready || !prepared.Private || prepared.Identity.InstanceID != a.config.InstanceID {
		return rejected("preparation_mismatch")
	}
	ref, err := decodeRef(prepared.Resource, prepared.Identity)
	if err != nil || ref.EngineID == "" {
		return rejected("preparation_mismatch")
	}
	if ctx.Err() != nil {
		return rejected("call_timeout")
	}
	outcome, sent := rejected("host_unavailable"), false
	err = a.withDocker(ctx, func(d docker, _ Endpoint) error {
		if err := d.verifyEngine(ctx, ref.EngineID); err != nil {
			return err
		}
		info, found, err := d.inspect(ctx, ref.Container)
		switch {
		case err != nil:
			return err
		case !found:
			outcome = rejected("container_missing")
			return nil
		case info.ID != ref.ID || !owned(info, prepared.Identity) || info.Config.Labels[labelPrefix+"preparation"] != string(prepared.PreparationID):
			outcome = rejected("preparation_mismatch")
			return nil
		case info.Started():
			// An earlier start reached Docker; only reconciliation may claim it.
			outcome = unknownSubmission()
			return nil
		}
		labels := info.Config.Labels
		if labels[labelPrefix+"engine"] != ref.EngineID || labels[labelPrefix+"workspace"] != string(prepared.Identity.WorkspaceID) || labels[labelPrefix+"job"] != string(prepared.Identity.JobID) || labels[labelPrefix+"nonce"] != prepared.Identity.Nonce || labels[labelPrefix+"plan"] != string(prepared.PlanSHA256) {
			outcome = rejected("preparation_mismatch")
			return nil
		}
		controlled := info.Config.Labels[labelPrefix+"execution-control"] == "prepare_then_start"
		if controlled != (start != nil) {
			outcome = rejected("start_authorization_required")
			return nil
		}
		rawDeadline := info.Config.Labels[labelPrefix+"execution-deadline"]
		if start != nil {
			if err = a.checkControlledStart(ctx, d, info, prepared, *start); err != nil {
				outcome = rejected("readiness_invalidated")
				if errors.Is(err, errStartClaimed) {
					outcome = unknownSubmission()
				}
				return nil
			}
			rawDeadline = start.ExecutionDeadline
			before, _ := time.Parse(time.RFC3339, start.StartBefore)
			if !a.clock.Now().Before(before) {
				outcome = startExpired(*start)
				return nil
			}
		}
		if raw := rawDeadline; raw != "" {
			deadline, err := time.Parse(time.RFC3339, raw)
			if err != nil || deadline.UTC().Format(time.RFC3339) != raw || info.Config.Labels[labelPrefix+"plan"] != string(prepared.PlanSHA256) {
				outcome = rejected("preparation_mismatch")
				return nil
			}
			if !a.clock.Now().Before(deadline) {
				outcome = deadlineRejected()
				return nil
			}
			before := a.clock.Now()
			remote, err := d.systemTime(ctx)
			after := a.clock.Now()
			if err != nil || after.Before(before) || after.Sub(before) > 5*time.Second || remote.Before(after.Add(-5*time.Second)) || remote.After(before.Add(5*time.Second)) {
				outcome = rejected("clock_skew_unverified")
				return nil
			}
			if !after.Before(deadline) || !remote.Before(deadline) {
				outcome = deadlineRejected()
				return nil
			}
			if start != nil {
				before, _ := time.Parse(time.RFC3339, start.StartBefore)
				if !after.Before(before) || !remote.Before(before) {
					outcome = startExpired(*start)
					return nil
				}
			}
		}
		sent = true
		if start != nil {
			raw, _ := json.Marshal(startControl{Version: "compute-relay/start/v1", JobID: prepared.Identity.JobID,
				AttemptID: prepared.Identity.AttemptID, AttemptNonce: prepared.Identity.Nonce, ManifestSHA256: prepared.ManifestSHA256, StartAuthorization: *start})
			if err = d.copyIn(ctx, ref.ID, singleFile(strings.TrimPrefix(startControlPath, "/"), raw)); err != nil {
				return err
			}
		}
		result, err := d.simple(ctx, "container", "start", ref.ID)
		if err != nil || result.ExitCode != 0 {
			return ErrRemoteDocker
		}
		if material != nil {
			if err = deliverSecrets(ctx, d, ref.ID, prepared, *material); err != nil {
				return err
			}
		}
		outcome = provider.SubmissionOutcome{Status: provider.SubmissionAccepted,
			Remote: &provider.RemoteReference{Identity: prepared.Identity, Resource: prepared.Resource, Version: ref.ID}}
		return nil
	})
	if err != nil && sent {
		return unknownSubmission()
	}
	return outcome
}

func deadlineRejected() provider.SubmissionOutcome {
	p := provider.Problem(domain.CodeRemoteTimeout, domain.FailureStageExecution, "absolute execution deadline expired before container start").WithDetail(domain.ReasonDetail, "absolute_deadline_expired")
	return provider.SubmissionOutcome{Status: provider.SubmissionRejected, Problem: &p}
}

// Reconcile finds the container only when Docker reports it was started.
func (a *Adapter) Reconcile(ctx context.Context, id provider.Identity) (provider.Reconciliation, error) {
	result := provider.Reconciliation{Status: provider.ReconciliationNotFound}
	err := a.withDocker(ctx, func(d docker, _ Endpoint) error {
		name := containerName(id)
		info, found, err := d.inspect(ctx, name)
		if err != nil || !found {
			return err
		}
		if !owned(info, id) {
			return ErrIdentity
		}
		if info.Started() {
			engine := info.Config.Labels[labelPrefix+"engine"]
			if err := d.verifyEngine(ctx, engine); err != nil {
				return err
			}
			result = provider.Reconciliation{Status: provider.ReconciliationFound,
				Remote: &provider.RemoteReference{Identity: id, Resource: encodeRef(name, info.ID, engine), Version: info.ID}}
		}
		return nil
	})
	if err != nil {
		return provider.Reconciliation{}, err
	}
	return result, nil
}

// inspectRemote loads the container behind an exact remote reference.
func inspectRemote(ctx context.Context, d docker, remote provider.RemoteReference) (containerInfo, error) {
	ref, err := decodeRef(remote.Resource, remote.Identity)
	if err != nil || remote.Version != ref.ID {
		return containerInfo{}, ErrIdentity
	}
	if err := d.verifyEngine(ctx, ref.EngineID); err != nil {
		return containerInfo{}, err
	}
	info, found, err := d.inspect(ctx, ref.Container)
	if err != nil {
		return containerInfo{}, err
	}
	if !found || info.ID != ref.ID || !owned(info, remote.Identity) {
		return containerInfo{}, ErrIdentity
	}
	return info, nil
}

// Observe maps Docker's own container state. A nonzero exit after a cancellation
// request is cancelled; exit 124 is the host-side timeout.
func (a *Adapter) Observe(ctx context.Context, remote provider.RemoteReference) (provider.Observation, error) {
	var observation provider.Observation
	err := a.withDocker(ctx, func(d docker, _ Endpoint) error {
		info, err := inspectRemote(ctx, d, remote)
		if err != nil {
			return err
		}
		observation = provider.Observation{Remote: remote, RawState: fmt.Sprintf("docker:%s:%d", info.State.Status, info.State.ExitCode),
			ReleaseEvidence: domain.ReleaseEvidenceUnknown, ObservedAt: a.clock.Now().UTC()}
		switch {
		case info.State.Status == "created":
			observation.Execution, observation.RemoteActivity = domain.ExecutionQueued, domain.RemoteActivityPossible
		case info.State.Running:
			observation.Execution, observation.RemoteActivity = domain.ExecutionRunning, domain.RemoteActivityActive
		case !exited(info):
			return ErrRemoteDocker
		default:
			observation.RemoteActivity, observation.ReleaseEvidence = domain.RemoteActivityInactive, domain.ReleaseEvidenceProviderReported
			switch {
			case info.State.ExitCode == 0 && !info.State.OOMKilled:
				observation.Execution = domain.ExecutionSucceeded
			case info.State.ExitCode == timeoutExit:
				observation.Execution = domain.ExecutionTimedOut
			default:
				observation.Execution = domain.ExecutionFailed
				_, cancelled, err := readFile(ctx, d, info.ID, cancelMarker, 4096)
				if err != nil {
					return err
				}
				if cancelled {
					observation.Execution = domain.ExecutionCancelled
				}
			}
		}
		return observation.Validate(remote)
	})
	if err != nil {
		return provider.Observation{}, err
	}
	return observation, nil
}

// Cancel records the request inside the container, stops it and confirms termination
// only from Docker's own state.
func (a *Adapter) Cancel(ctx context.Context, remote provider.RemoteReference, operation domain.OperationID) (provider.CancellationOutcome, error) {
	if err := ctx.Err(); err != nil {
		return provider.CancellationOutcome{}, err
	}
	if !operation.Valid() {
		return provider.CancellationOutcome{}, ErrRequest
	}
	if a == nil || remote.Validate() != nil || remote.Identity.InstanceID != a.config.InstanceID {
		return provider.CancellationOutcome{}, ErrIdentity
	}
	var outcome provider.CancellationOutcome
	err := a.withDocker(ctx, func(d docker, _ Endpoint) error {
		info, err := inspectRemote(ctx, d, remote)
		if err != nil {
			return err
		}
		if exited(info) {
			outcome = provider.CancellationOutcome{Status: domain.CancellationTooLate}
			return nil
		}
		request, _ := json.Marshal(map[string]string{"operation_id": string(operation)})
		if err = d.copyIn(ctx, info.ID, singleFile(strings.TrimPrefix(cancelMarker, "/"), request)); err != nil {
			return err
		}
		result, err := d.simple(ctx, "container", "stop", "--time", "30", info.ID)
		if err != nil {
			return err
		}
		if result.ExitCode != 0 {
			return ErrRemoteDocker
		}
		if info, err = inspectRemote(ctx, d, remote); err != nil {
			return err
		}
		outcome = provider.CancellationOutcome{Status: domain.CancellationAccepted}
		if info.Started() && exited(info) {
			outcome = provider.CancellationOutcome{Status: domain.CancellationConfirmed, TerminationConfirmed: true}
		}
		return nil
	})
	if err != nil {
		return provider.CancellationOutcome{}, err
	}
	return outcome, nil
}

// readTar streams one container path as a tar archive and hands each regular file to
// sink in order. Links, devices and files over limit fail the read.
func readTar(ctx context.Context, d docker, container, source string, limit int64, sink func(name string, r io.Reader) error) (bool, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader, writer := io.Pipe()
	type copied struct {
		found bool
		err   error
	}
	done := make(chan copied, 1)
	go func() {
		found, err := d.copyOut(ctx, container, source, writer)
		_ = writer.CloseWithError(err)
		done <- copied{found, err}
	}()
	parseErr := parseTar(reader, limit, sink)
	if parseErr != nil {
		cancel()
	}
	_ = reader.CloseWithError(ErrArtifact)
	result := <-done
	if parseErr != nil {
		return false, parseErr
	}
	return result.found, result.err
}

func parseTar(r io.Reader, limit int64, sink func(string, io.Reader) error) error {
	tr := tar.NewReader(r)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			_, err = io.Copy(io.Discard, r)
			return err
		}
		if err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg:
		default:
			return ErrArtifact
		}
		if header.Size < 0 || header.Size > limit {
			return ErrArtifact
		}
		if err = sink(path.Clean(header.Name), tr); err != nil {
			return err
		}
	}
}

// readFile copies one small regular file out of a container.
func readFile(ctx context.Context, d docker, container, source string, limit int64) ([]byte, bool, error) {
	var data []byte
	files := 0
	found, err := readTar(ctx, d, container, source, limit, func(name string, r io.Reader) error {
		if files++; files > 1 || name != path.Base(source) {
			return ErrArtifact
		}
		var err error
		data, err = io.ReadAll(r)
		return err
	})
	if err != nil || !found {
		return nil, false, err
	}
	if files != 1 {
		return nil, false, ErrArtifact
	}
	return data, true, nil
}

// catalog lists the runner control files with measured digests and the declared
// outputs recorded in the runner's result manifest. Collection verifies the outputs.
func (a *Adapter) catalog(ctx context.Context, d docker, remote provider.RemoteReference) ([]provider.Artifact, error) {
	info, err := inspectRemote(ctx, d, remote)
	if err != nil {
		return nil, err
	}
	if !exited(info) {
		return nil, ErrNotDone
	}
	var artifacts []provider.Artifact
	var manifest []byte
	_, err = readTar(ctx, d, info.ID, workRoot+"/control", maxControlBytes, func(name string, r io.Reader) error {
		base, ok := strings.CutPrefix(name, "control/")
		if !ok || !slices.Contains(controlFiles, base) {
			_, err := io.Copy(io.Discard, r)
			return err
		}
		digest := sha256.New()
		var kept bytes.Buffer
		sink := io.Writer(digest)
		if base == "execution-result.json" {
			sink = io.MultiWriter(digest, &limitedWriter{w: &kept, limit: maxManifestBytes})
		}
		n, err := io.Copy(sink, r)
		if err != nil {
			return err
		}
		if base == "execution-result.json" {
			manifest = kept.Bytes()
		}
		artifacts = append(artifacts, provider.Artifact{Remote: remote, Path: name, Bytes: n, SHA256: domain.SHA256Digest(hex.EncodeToString(digest.Sum(nil)))})
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Collection reports a missing or invalid manifest; never list its untrusted outputs.
	if result, err := runnerplan.ParseResult(remote.Identity, manifest); err == nil {
		for _, out := range result.Artifacts {
			artifacts = append(artifacts, provider.Artifact{Remote: remote, Path: "outputs/" + out.Path, Bytes: out.Bytes, SHA256: out.SHA256})
		}
	}
	slices.SortFunc(artifacts, func(x, y provider.Artifact) int { return strings.Compare(x.Path, y.Path) })
	var total int64
	seen := map[string]bool{}
	if len(artifacts) > provider.MaxArtifactFiles {
		return nil, ErrArtifact
	}
	for _, artifact := range artifacts {
		if artifact.Validate(remote) != nil || artifact.Bytes > provider.MaxArtifactBytes-total {
			return nil, ErrArtifact
		}
		key := strings.ToLower(artifact.Path)
		if seen[key] {
			return nil, ErrArtifact
		}
		seen[key] = true
		total += artifact.Bytes
	}
	for key := range seen {
		parts := strings.Split(key, "/")
		for i := 1; i < len(parts); i++ {
			if seen[strings.Join(parts[:i], "/")] {
				return nil, ErrArtifact
			}
		}
	}
	return artifacts, nil
}

func (a *Adapter) ListArtifacts(ctx context.Context, remote provider.RemoteReference) ([]provider.Artifact, error) {
	var result []provider.Artifact
	err := a.withDocker(ctx, func(d docker, _ Endpoint) error {
		var err error
		result, err = a.catalog(ctx, d, remote)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Fetch copies one listed file out of the stopped container. Bytes reach w
// only within the listed length; the digest is verified before success.
func (a *Adapter) Fetch(ctx context.Context, remote provider.RemoteReference, artifact provider.Artifact, w io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	if w == nil || artifact.Validate(remote) != nil || artifact.Bytes > limit || artifact.Bytes > provider.MaxArtifactBytes ||
		!strings.HasPrefix(artifact.Path, "outputs/") && !strings.HasPrefix(artifact.Path, "control/") {
		return provider.TransferResult{}, ErrArtifact
	}
	w = provider.WithTransferProgress(w, progress)
	var result provider.TransferResult
	err := a.withDocker(ctx, func(d docker, _ Endpoint) error {
		info, err := inspectRemote(ctx, d, remote)
		if err != nil {
			return err
		}
		if !exited(info) {
			return ErrNotDone
		}
		files := 0
		found, err := readTar(ctx, d, info.ID, workRoot+"/"+artifact.Path, artifact.Bytes, func(name string, r io.Reader) error {
			if files++; files > 1 || name != path.Base(artifact.Path) {
				return ErrArtifact
			}
			digest := sha256.New()
			n, err := io.Copy(io.MultiWriter(w, digest), r)
			result = provider.TransferResult{Bytes: n, SHA256: domain.SHA256Digest(hex.EncodeToString(digest.Sum(nil)))}
			return err
		})
		if err != nil {
			return err
		}
		if !found || files != 1 || result.Bytes != artifact.Bytes || result.SHA256 != artifact.SHA256 {
			return ErrArtifact
		}
		return nil
	})
	if err != nil {
		return provider.TransferResult{}, err
	}
	return result, nil
}

type logCursor struct {
	Reference domain.SHA256Digest `json:"reference"`
	Line      int                 `json:"line"`
}

// Logs pages the container's stdout stream, which carries the runner's mirrored
// payload output. Standard error is collected as the control/stderr.log artifact.
func (a *Adapter) Logs(ctx context.Context, remote provider.RemoteReference, page provider.PageRequest) (provider.LogPage, error) {
	if err := ctx.Err(); err != nil {
		return provider.LogPage{}, err
	}
	if a == nil || remote.Validate() != nil || remote.Identity.InstanceID != a.config.InstanceID {
		return provider.LogPage{}, ErrIdentity
	}
	if page.Validate() != nil {
		return provider.LogPage{}, provider.ErrLogCursorInvalid
	}
	identity, _ := json.Marshal(remote)
	reference := provider.Digest(identity)
	var cursor logCursor
	if page.Cursor != "" {
		raw, err := base64.RawURLEncoding.Strict().DecodeString(page.Cursor)
		if err != nil || json.Unmarshal(raw, &cursor) != nil || cursor.Reference != reference || cursor.Line < 0 || cursor.Line > maxLogLines {
			return provider.LogPage{}, provider.ErrLogCursorInvalid
		}
	}
	var result provider.LogPage
	err := a.withDocker(ctx, func(d docker, _ Endpoint) error {
		info, err := inspectRemote(ctx, d, remote)
		if err != nil {
			return err
		}
		lines := &lineCollector{skip: cursor.Line, limit: page.Limit}
		found, err := d.logs(ctx, info.ID, lines)
		if err != nil {
			return err
		}
		if !found {
			return ErrIdentity
		}
		if lines.seen < cursor.Line {
			return provider.ErrLogCursorReset
		}
		result = provider.LogPage{Source: "provider", Availability: "after_completion", Lines: lines.lines, Truncated: lines.truncated}
		if !exited(info) {
			result.Availability = "live"
		}
		if lines.more || result.Availability == "live" {
			raw, _ := json.Marshal(logCursor{Reference: reference, Line: cursor.Line + len(lines.lines)})
			result.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
		}
		return result.Validate(page)
	})
	if err != nil {
		return provider.LogPage{}, err
	}
	return result, nil
}

// lineCollector keeps one bounded page of complete lines after skipping consumed
// lines. A trailing line without a newline is still being written and waits.
type lineCollector struct {
	skip, limit, seen, bytes int
	partial                  []byte
	lines                    []string
	more, truncated, clipped bool
}

func (c *lineCollector) Write(p []byte) (int, error) {
	for _, b := range p {
		if b == '\n' {
			c.line()
			continue
		}
		if len(c.partial) < maxLogLineBytes {
			c.partial = append(c.partial, b)
		} else {
			c.clipped = true
		}
	}
	return len(p), nil
}

func (c *lineCollector) line() {
	line, clipped := c.partial, c.clipped
	c.partial, c.clipped = c.partial[:0], false
	if c.seen++; c.seen <= c.skip {
		return
	}
	if c.more || len(c.lines) >= c.limit {
		c.more = true
		return
	}
	text := strings.ToValidUTF8(string(line), "�")
	for len(text) > maxLogLineBytes {
		_, size := utf8.DecodeLastRuneInString(text)
		text, clipped = text[:len(text)-size], true
	}
	if c.bytes+len(text) > maxLogPageBytes {
		c.more = true
		return
	}
	c.lines = append(c.lines, text)
	c.bytes += len(text)
	c.truncated = c.truncated || clipped
}

func (a *Adapter) cleanup(ctx context.Context, request provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
	if request.Mode != provider.CleanupDryRun && request.Mode != provider.CleanupApply || !request.LedgerID.Valid() || !request.CreationOperationID.Valid() {
		return provider.CleanupOutcome{}, ErrRequest
	}
	id := request.Remote.Identity
	if request.Remote.Validate() != nil || !resourceKey.MatchString(id.ResourceKey) || id.InstanceID != a.config.InstanceID {
		return provider.CleanupOutcome{}, ErrIdentity
	}
	ref, err := decodeRef(request.Remote.Resource, id)
	if err != nil || request.Remote.Version != ref.ID && request.Remote.Version != "prepared" {
		return provider.CleanupOutcome{}, ErrIdentity
	}
	if ref.EngineID == "" {
		return provider.CleanupOutcome{}, provider.ErrCleanupUnsupported
	}
	var outcome provider.CleanupOutcome
	err = a.withDocker(ctx, func(d docker, _ Endpoint) error {
		// A name may be reused with copied labels. Only the ID from the original
		// creation receipt can be inspected or passed to Docker removal.
		if err := d.verifyEngine(ctx, ref.EngineID); err != nil {
			return err
		}
		info, found, err := d.inspect(ctx, ref.ID)
		if err != nil {
			return err
		}
		if !found {
			outcome.AlreadyAbsent = true
			return nil
		}
		if info.ID != ref.ID || info.Name != "/"+ref.Container || !owned(info, id) || info.Config.Labels[labelPrefix+"preparation"] != string(request.CreationOperationID) {
			return ErrIdentity
		}
		if info.Config.Labels[labelPrefix+"plan"] != string(request.PlanSHA256) {
			return ErrIdentity
		}
		if ref.EngineID != "" {
			l := info.Config.Labels
			if l[labelPrefix+"engine"] != ref.EngineID || l[labelPrefix+"workspace"] != string(id.WorkspaceID) || l[labelPrefix+"job"] != string(id.JobID) || l[labelPrefix+"nonce"] != id.Nonce {
				return ErrIdentity
			}
		}
		if info.State.Running || !exited(info) && info.State.Status != "created" {
			return ErrNotDone
		}
		if request.RecoverOnly {
			return provider.Problem(domain.CodeProviderStateUnknown, domain.FailureStageObservation, "the original deletion outcome remains unresolved")
		}
		if request.Mode == provider.CleanupDryRun {
			outcome.WouldDelete = true
			return nil
		}
		result, err := d.simple(ctx, "container", "rm", info.ID)
		switch {
		case err != nil:
			return err
		case result.ExitCode == 0:
			outcome.Deleted = true
		case missing(result):
			outcome.AlreadyAbsent = true
		default:
			return ErrRemoteDocker
		}
		return nil
	})
	if err != nil {
		return provider.CleanupOutcome{}, err
	}
	return outcome, nil
}
