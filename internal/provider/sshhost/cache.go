package sshhost

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostcache"
	"github.com/vankhaivn/compute-relay/internal/provider"
	runnerassets "github.com/vankhaivn/compute-relay/runner"
)

const cacheWriterBootstrap = "import base64,sys;source=base64.b64decode(sys.argv.pop(1));exec(source)"

const cacheWriterSource = `import hashlib,os,signal,sys
signal.alarm(7200)
size=int(sys.argv[1]); expected=sys.argv[2]
fd=os.open('/cache/.writing',os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o444)
h=hashlib.sha256(); remaining=size
try:
 while remaining:
  block=sys.stdin.buffer.read(min(remaining,1048576))
  if not block: raise ValueError('short input')
  offset=0
  while offset<len(block):
   written=os.write(fd,block[offset:])
   if written<=0: raise OSError('short write')
   offset+=written
  h.update(block); remaining-=len(block)
 if sys.stdin.buffer.read(1) or h.hexdigest()!=expected: raise ValueError('input changed')
 os.fsync(fd)
finally: os.close(fd)
os.link('/cache/.writing','/cache/blob',follow_symlinks=False)
os.unlink('/cache/.writing')
dirfd=os.open('/cache',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
try: os.fsync(dirfd)
finally: os.close(dirfd)
`

const maxCacheMarkerBytes = 64 << 10

type cacheBinding struct {
	Key        hostcache.Key        `json:"key"`
	Generation hostcache.Generation `json:"generation"`
	Path       string               `json:"path"`
}

func cacheAttempt(id provider.Identity) hostcache.Attempt {
	return hostcache.Attempt{WorkspaceID: id.WorkspaceID, JobID: id.JobID, AttemptID: id.AttemptID}
}

func cacheRelativePath(key hostcache.Key) string {
	return "cache/" + string(key.SHA256) + "-" + strconv.FormatInt(key.Bytes, 10) + "/blob"
}

func (b cacheBinding) mountTarget() string {
	return "/staging-cache/" + strings.TrimSuffix(b.Path, "/blob")
}

func cacheObjects(plan provider.Plan) map[struct {
	sha   domain.SHA256Digest
	bytes int64
}]domain.ObjectMetadata {
	result := map[struct {
		sha   domain.SHA256Digest
		bytes int64
	}]domain.ObjectMetadata{}
	objects := []domain.ObjectMetadata{plan.Job.Inputs.Bundle}
	for _, input := range plan.Job.Inputs.Inputs {
		objects = append(objects, input.Object)
	}
	for _, object := range objects {
		result[struct {
			sha   domain.SHA256Digest
			bytes int64
		}{object.SHA256, object.Bytes}] = object
	}
	return result
}

// prepareCache asks the durable pin ledger before sending any input bytes. Only a
// newly claimed generation's one-shot writer may receive the frozen object.
func (a *Adapter) prepareCache(ctx context.Context, d docker, plan provider.Plan) ([]cacheBinding, error) {
	if a.cache == nil || a.config.MachineID == "" || a.config.EngineID == "" {
		return nil, nil
	}
	owner := cacheAttempt(plan.Job.Identity)
	pins, err := a.cache.Pins(ctx, owner)
	if err != nil {
		return nil, err
	}
	if len(pins) == 0 {
		return nil, nil
	} // A pre-cache admission keeps legacy staging.
	objects := cacheObjects(plan)
	byObject := make(map[struct {
		sha   domain.SHA256Digest
		bytes int64
	}]hostcache.Key, len(pins))
	for _, key := range pins {
		objectKey := struct {
			sha   domain.SHA256Digest
			bytes int64
		}{key.SHA256, key.Bytes}
		if key.InstallationID != plan.Job.Identity.InstallationID || string(key.MachineID) != a.config.MachineID || string(key.EngineID) != a.config.EngineID || objects[objectKey].SHA256 == "" {
			return nil, ErrIdentity
		}
		byObject[objectKey] = key
	}
	if len(byObject) != len(objects) {
		return nil, ErrIdentity
	}
	keys := slices.Collect(maps.Values(byObject))
	slices.SortFunc(keys, func(x, y hostcache.Key) int {
		if x.SHA256 < y.SHA256 {
			return -1
		}
		if x.SHA256 > y.SHA256 {
			return 1
		}
		if x.Bytes < y.Bytes {
			return -1
		}
		if x.Bytes > y.Bytes {
			return 1
		}
		return 0
	})
	bindings := make([]cacheBinding, 0, len(keys))
	var total, done int64
	ready := map[hostcache.Key]hostcache.Generation{}
	for _, key := range keys {
		generation, err := a.waitForPeerGeneration(ctx, key)
		if err != nil {
			return nil, err
		}
		if generation == nil {
			total += key.Bytes
			continue
		}
		if generation.State == hostcache.Claimed && generation.WriterPhase != hostcache.WriterFinished {
			return nil, hostcache.ErrUnknown
		}
		observation, err := a.observeVolume(ctx, d, *generation)
		if err != nil {
			return nil, err
		}
		if observation.Kind != hostcache.ObservedComplete {
			if err := a.cache.Retire(ctx, observation); err != nil {
				return nil, err
			}
			total += key.Bytes
			continue
		}
		if generation.State != hostcache.Verified {
			if err := a.cache.Verify(ctx, observation); err != nil {
				return nil, err
			}
			generation, err = a.cache.Active(ctx, key)
			if err != nil || generation == nil {
				return nil, ErrIdentity
			}
		}
		ready[key] = *generation
	}
	for _, key := range keys {
		g, hit := ready[key]
		if !hit {
			object := objects[struct {
				sha   domain.SHA256Digest
				bytes int64
			}{key.SHA256, key.Bytes}]
			var transferred int64
			g, transferred, err = a.resolveGeneration(ctx, d, owner, key, object, func(n int64) { provider.ReportPreparationProgress(ctx, done+n, total) })
			if err != nil {
				return nil, err
			}
			done += transferred
		}
		bindings = append(bindings, cacheBinding{Key: key, Generation: g, Path: cacheRelativePath(key)})
	}
	return bindings, nil
}

// A second legitimate attempt waits for the first writer's durable terminal
// state. A begun writer with a lost acknowledgement remains unknown: neither
// elapsed time nor a temporarily absent helper grants a second write.
func (a *Adapter) waitForPeerGeneration(ctx context.Context, key hostcache.Key) (*hostcache.Generation, error) {
	deadline := time.NewTimer(30 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		generation, err := a.cache.Active(ctx, key)
		if err != nil || generation == nil || generation.State == hostcache.Verified || generation.WriterPhase == hostcache.WriterFinished {
			return generation, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, hostcache.ErrUnknown
		case <-ticker.C:
		}
	}
}

type volumeInfo struct {
	Name    string            `json:"Name"`
	Driver  string            `json:"Driver"`
	Scope   string            `json:"Scope"`
	Labels  map[string]string `json:"Labels"`
	Options map[string]string `json:"Options"`
}

func (d docker) inspectVolume(ctx context.Context, name string) (volumeInfo, bool, error) {
	var out bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 1 << 16}, "volume", "inspect", name)
	if err != nil {
		return volumeInfo{}, false, err
	}
	if missing(result) || result.ExitCode != 0 && strings.Contains(string(result.Stderr), "No such volume") {
		return volumeInfo{}, false, nil
	}
	var found []volumeInfo
	if result.ExitCode != 0 || json.Unmarshal(out.Bytes(), &found) != nil || len(found) != 1 || found[0].Name != name {
		return volumeInfo{}, false, ErrRemoteDocker
	}
	return found[0], true, nil
}

// Docker's volume inspect accepts names without ownership filtering. Restrict
// discovery to the exact generation's full immutable label set first; a
// foreign same-name volume is never inspected or passed to a helper.
func (d docker) inspectOwnedCacheVolume(ctx context.Context, g hostcache.Generation) (volumeInfo, bool, error) {
	args := []string{"volume", "ls"}
	for _, key := range slices.Sorted(maps.Keys(g.Labels())) {
		args = append(args, "--filter", "label="+key+"="+g.Labels()[key])
	}
	args = append(args, "--format", "{{.Name}}")
	var out bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 4096}, args...)
	if err != nil {
		return volumeInfo{}, false, err
	}
	if result.ExitCode != 0 {
		return volumeInfo{}, false, ErrRemoteDocker
	}
	listed := strings.TrimSpace(out.String())
	if listed == "" {
		return volumeInfo{}, false, nil
	}
	if listed != g.VolumeName {
		return volumeInfo{}, false, ErrIdentity
	}
	return d.inspectVolume(ctx, g.VolumeName)
}

func validCacheVolume(v volumeInfo, g hostcache.Generation) bool {
	if v.Name != g.VolumeName || v.Driver != "local" || v.Scope != "local" || len(v.Options) != 0 || len(v.Labels) != len(g.Labels()) {
		return false
	}
	for key, value := range g.Labels() {
		if v.Labels[key] != value {
			return false
		}
	}
	return true
}

func (d docker) createVolume(ctx context.Context, g hostcache.Generation) error {
	args := []string{"volume", "create", "--driver", "local", "--name", g.VolumeName}
	for _, key := range slices.Sorted(maps.Keys(g.Labels())) {
		args = append(args, "--label", key+"="+g.Labels()[key])
	}
	var out bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 4096}, args...)
	if err != nil {
		return err
	}
	if result.ExitCode != 0 || strings.TrimSpace(out.String()) != g.VolumeName {
		return ErrRemoteDocker
	}
	return nil
}

func cacheHelperLabels(g hostcache.Generation, kind string) []string {
	labels := g.Labels()
	labels[labelPrefix+"kind"] = kind
	args := make([]string, 0, 2*len(labels))
	for _, key := range slices.Sorted(maps.Keys(labels)) {
		args = append(args, "--label", key+"="+labels[key])
	}
	return args
}

func (a *Adapter) writeVolume(ctx context.Context, d docker, image string, g hostcache.Generation, object domain.ObjectMetadata, progress func(int64)) (bool, error) {
	reader, writer := io.Pipe()
	written := make(chan error, 1)
	go func() {
		err := a.copyObject(ctx, writer, object, progress)
		_ = writer.CloseWithError(err)
		written <- err
	}()
	name := "relay-cache-writer-" + strings.TrimPrefix(g.ID, "hcgen_")
	args := []string{"container", "run", "--rm", "-i", "--pull=never", "--name", name,
		"--user", "0:0", "--network", "none", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		"--read-only", "--pids-limit", "32", "--memory", "134217728", "--memory-swap", "134217728", "--cpus", "1",
		"--mount", "type=volume,source=" + g.VolumeName + ",target=/cache,volume-nocopy"}
	args = append(args, cacheHelperLabels(g, "cache-writer")...)
	args = append(args, "--entrypoint", "python3", image, "-B", "-E", "-s", "-c", cacheWriterBootstrap, base64.StdEncoding.EncodeToString([]byte(cacheWriterSource)), strconv.FormatInt(g.Key.Bytes, 10), string(g.Key.SHA256))
	result, err := d.run(ctx, reader, io.Discard, args...)
	_ = reader.CloseWithError(errStagingStopped)
	werr := <-written
	if err != nil {
		return false, err
	}
	if werr != nil && !errors.Is(werr, errStagingStopped) {
		return true, werr
	}
	if result.ExitCode != 0 || werr != nil {
		return true, ErrArtifact
	}
	return true, nil
}

func (a *Adapter) observeVolume(ctx context.Context, d docker, g hostcache.Generation) (hostcache.Observation, error) {
	o := hostcache.Observation{GenerationID: g.ID, VolumeName: g.VolumeName, OperationID: g.OperationID, Key: g.Key, WriterInactive: true}
	v, found, err := d.inspectOwnedCacheVolume(ctx, g)
	if err != nil {
		return o, err
	}
	if !found {
		o.Kind = hostcache.ObservedMissing
		return o, nil
	}
	if !validCacheVolume(v, g) {
		o.Kind = hostcache.ObservedCorrupt
		return o, nil
	}
	o.Labels = v.Labels
	image, err := a.standardVerifierImage(ctx, d)
	if err != nil {
		return o, err
	}
	var output verifierOutput
	args := []string{"container", "run", "--rm", "--pull=never", "--user", "65534:65534", "--network", "none",
		"--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--read-only", "--pids-limit", "32",
		"--memory", "134217728", "--memory-swap", "134217728", "--cpus", "1",
		"--mount", "type=volume,source=" + g.VolumeName + ",target=/cache,readonly,volume-nocopy"}
	args = append(args, cacheHelperLabels(g, "verification")...)
	args = append(args, "--entrypoint", "python3", image, "-B", "-E", "-s", "-c", hostVerifierBootstrap,
		base64.StdEncoding.EncodeToString([]byte(hostVerifierSource)), "volume", strconv.FormatInt(g.Key.Bytes, 10))
	result, err := d.run(ctx, nil, &output, args...)
	if err != nil {
		return o, err
	}
	if result.ExitCode != 0 || output.overLimit {
		o.Kind = hostcache.ObservedCorrupt
		return o, nil
	}
	var inventory verifierInventory
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&inventory) != nil || decoder.Decode(new(any)) != io.EOF || len(inventory.Files) != 1 || inventory.Marker != nil {
		o.Kind = hostcache.ObservedCorrupt
		return o, nil
	}
	file := inventory.Files[0]
	if file.Bytes != g.Key.Bytes || file.SHA256 != string(g.Key.SHA256) {
		o.Kind = hostcache.ObservedCorrupt
		return o, nil
	}
	o.Kind, o.ContentBytes, o.ContentSHA256 = hostcache.ObservedComplete, file.Bytes, domain.SHA256Digest(file.SHA256)
	return o, nil
}

func (a *Adapter) resolveGeneration(ctx context.Context, d docker, owner hostcache.Attempt, key hostcache.Key, object domain.ObjectMetadata, progress func(int64)) (hostcache.Generation, int64, error) {
	for attempt := 0; attempt < 2; attempt++ {
		g, err := a.cache.Active(ctx, key)
		if err != nil {
			return hostcache.Generation{}, 0, err
		}
		created := false
		if g == nil {
			fresh, won, err := a.cache.Claim(ctx, owner, key)
			if err != nil {
				return hostcache.Generation{}, 0, err
			}
			g, created = &fresh, won
		}
		if g.State == hostcache.Verified || g.WriterPhase == hostcache.WriterFinished {
			o, err := a.observeVolume(ctx, d, *g)
			if err != nil {
				return hostcache.Generation{}, 0, err
			}
			if o.Kind == hostcache.ObservedComplete {
				if g.State != hostcache.Verified {
					if err := a.cache.Verify(ctx, o); err != nil {
						return hostcache.Generation{}, 0, err
					}
					g, err = a.cache.Active(ctx, key)
					if err != nil || g == nil {
						return hostcache.Generation{}, 0, ErrIdentity
					}
				}
				return *g, 0, nil
			}
			if err := a.cache.Retire(ctx, o); err != nil {
				return hostcache.Generation{}, 0, err
			}
			continue
		}
		if !created {
			if _, err := a.waitForPeerGeneration(ctx, key); err != nil {
				return hostcache.Generation{}, 0, err
			}
			continue
		}
		if err := a.cache.BeginWrite(ctx, owner, g.ID); err != nil {
			return hostcache.Generation{}, 0, err
		}
		if _, exists, err := d.inspectOwnedCacheVolume(ctx, *g); err != nil {
			return hostcache.Generation{}, 0, err
		} else if exists {
			return hostcache.Generation{}, 0, ErrIdentity
		}
		if err := d.createVolume(ctx, *g); err != nil {
			return hostcache.Generation{}, 0, err
		}
		v, exists, err := d.inspectOwnedCacheVolume(ctx, *g)
		if err != nil {
			return hostcache.Generation{}, 0, err
		}
		if !exists || !validCacheVolume(v, *g) {
			return hostcache.Generation{}, 0, ErrIdentity
		}
		image, err := a.standardVerifierImage(ctx, d)
		if err != nil {
			return hostcache.Generation{}, 0, err
		}
		terminated, writeErr := a.writeVolume(ctx, d, image, *g, object, progress)
		if !terminated {
			return hostcache.Generation{}, 0, writeErr
		}
		if err := a.cache.FinishWrite(ctx, owner, g.ID); err != nil {
			return hostcache.Generation{}, 0, err
		}
		o, err := a.observeVolume(ctx, d, *g)
		if err != nil {
			return hostcache.Generation{}, 0, err
		}
		if writeErr != nil || o.Kind != hostcache.ObservedComplete {
			if o.Kind == hostcache.ObservedComplete {
				return hostcache.Generation{}, 0, writeErr
			}
			if err := a.cache.Retire(ctx, o); err != nil {
				return hostcache.Generation{}, 0, err
			}
			if writeErr != nil {
				return hostcache.Generation{}, 0, writeErr
			}
			return hostcache.Generation{}, 0, ErrArtifact
		}
		if err := a.cache.Verify(ctx, o); err != nil {
			return hostcache.Generation{}, 0, err
		}
		g, err = a.cache.Active(ctx, key)
		if err != nil || g == nil {
			return hostcache.Generation{}, 0, ErrIdentity
		}
		return *g, key.Bytes, nil
	}
	return hostcache.Generation{}, 0, fmt.Errorf("cache generation remained unavailable: %w", ErrIdentity)
}

func cacheMounts(bindings []cacheBinding) []dockerMountRequest {
	mounts := make([]dockerMountRequest, 0, len(bindings))
	for _, binding := range bindings {
		mount := dockerMountRequest{Type: "volume", Source: binding.Generation.VolumeName, Target: binding.mountTarget(), ReadOnly: true}
		mount.VolumeOptions.NoCopy = true
		mounts = append(mounts, mount)
	}
	return mounts
}

// verifyCacheMounts validates Docker's requested and effective mount records,
// followed by the original generation labels and read-only volume content.
func (a *Adapter) verifyCacheMounts(ctx context.Context, d docker, info containerInfo, identity provider.Identity, bindings []cacheBinding) error {
	if len(info.HostConfig.Mounts) != len(bindings) || len(info.Mounts) != len(bindings) {
		return ErrIdentity
	}
	requested := map[string]dockerMountRequest{}
	effective := map[string]dockerMount{}
	for _, mount := range info.HostConfig.Mounts {
		if _, duplicate := requested[mount.Target]; duplicate {
			return ErrIdentity
		}
		requested[mount.Target] = mount
	}
	for _, mount := range info.Mounts {
		if _, duplicate := effective[mount.Destination]; duplicate {
			return ErrIdentity
		}
		effective[mount.Destination] = mount
	}
	for _, binding := range bindings {
		if binding.Key.InstallationID != identity.InstallationID || binding.Path != cacheRelativePath(binding.Key) || binding.Generation.Key != binding.Key || binding.Generation.State != hostcache.Verified {
			return ErrIdentity
		}
		target := binding.mountTarget()
		r, rok := requested[target]
		e, eok := effective[target]
		if !rok || !eok || r.Type != "volume" || r.Source != binding.Generation.VolumeName || !r.ReadOnly || !r.VolumeOptions.NoCopy || e.Type != "volume" || e.Name != binding.Generation.VolumeName || e.RW {
			return ErrIdentity
		}
		if a.cache != nil {
			active, err := a.cache.Active(ctx, binding.Key)
			if err != nil {
				return err
			}
			if active == nil || active.ID != binding.Generation.ID || active.OperationID != binding.Generation.OperationID {
				return ErrIdentity
			}
		}
		o, err := a.observeVolume(ctx, d, binding.Generation)
		if err != nil {
			return err
		}
		if o.Kind != hostcache.ObservedComplete {
			return ErrIdentity
		}
		if a.cache != nil {
			if err := a.cache.RecordMount(ctx, hostcache.MountReceipt{Attempt: cacheAttempt(identity), Generation: binding.Generation, ContainerID: info.ID, ReadOnly: true, Labels: o.Labels}); err != nil {
				return err
			}
		}
	}
	return nil
}

func cacheManifest(manifest map[string]any, plan provider.Plan, bindings []cacheBinding) error {
	if len(bindings) == 0 {
		return nil
	}
	lookup := map[struct {
		sha   domain.SHA256Digest
		bytes int64
	}]string{}
	for _, b := range bindings {
		lookup[struct {
			sha   domain.SHA256Digest
			bytes int64
		}{b.Key.SHA256, b.Key.Bytes}] = b.Path
	}
	bundle := plan.Job.Inputs.Bundle
	path, ok := lookup[struct {
		sha   domain.SHA256Digest
		bytes int64
	}{bundle.SHA256, bundle.Bytes}]
	if !ok {
		return ErrIdentity
	}
	manifest["bundle"].(map[string]any)["path"] = path
	inputs := manifest["inputs"].([]map[string]any)
	for i, input := range plan.Job.Inputs.Inputs {
		path, ok = lookup[struct {
			sha   domain.SHA256Digest
			bytes int64
		}{input.Object.SHA256, input.Object.Bytes}]
		if !ok {
			return ErrIdentity
		}
		inputs[i]["path"] = path
	}
	return nil
}

// activeCacheBindings reconstructs the exact currently verified generations
// without claiming or writing anything. A replaced generation cannot validate
// a stopped container that still mounts the earlier volume.
func (a *Adapter) activeCacheBindings(ctx context.Context, identity provider.Identity, files []stagedFile) ([]cacheBinding, error) {
	if a.cache == nil {
		return nil, ErrIdentity
	}
	pins, err := a.cache.Pins(ctx, cacheAttempt(identity))
	if err != nil {
		return nil, err
	}
	wanted := map[struct {
		sha   domain.SHA256Digest
		bytes int64
	}]bool{}
	for _, file := range files {
		wanted[struct {
			sha   domain.SHA256Digest
			bytes int64
		}{file.SHA256, file.Bytes}] = true
	}
	if len(pins) != len(wanted) {
		return nil, ErrIdentity
	}
	bindings := make([]cacheBinding, 0, len(pins))
	for _, key := range pins {
		if key.InstallationID != identity.InstallationID || string(key.MachineID) != a.config.MachineID || string(key.EngineID) != a.config.EngineID || !wanted[struct {
			sha   domain.SHA256Digest
			bytes int64
		}{key.SHA256, key.Bytes}] {
			return nil, ErrIdentity
		}
		generation, err := a.cache.Active(ctx, key)
		if err != nil {
			return nil, err
		}
		if generation == nil || generation.State != hostcache.Verified {
			return nil, ErrIdentity
		}
		bindings = append(bindings, cacheBinding{Key: key, Generation: *generation, Path: cacheRelativePath(key)})
	}
	return bindings, nil
}

func (a *Adapter) activeCachePlan(ctx context.Context, plan provider.Plan) ([]cacheBinding, error) {
	files := []stagedFile{{Bytes: plan.Job.Inputs.Bundle.Bytes, SHA256: plan.Job.Inputs.Bundle.SHA256}}
	for _, input := range plan.Job.Inputs.Inputs {
		files = append(files, stagedFile{Bytes: input.Object.Bytes, SHA256: input.Object.SHA256})
	}
	return a.activeCacheBindings(ctx, plan.Job.Identity, files)
}

type cacheManifestFile struct {
	Path   string              `json:"path"`
	Bytes  int64               `json:"bytes"`
	SHA256 domain.SHA256Digest `json:"sha256"`
}

// verifyCachedReadiness hashes the control archive on the host and verifies
// every exact mounted generation independently through the fixed RO verifier.
func (a *Adapter) verifyCachedReadiness(ctx context.Context, d docker, info containerInfo, identity provider.Identity, planDigest domain.SHA256Digest, rawManifest []byte, manifestDigest, verificationDigest domain.SHA256Digest) error {
	if provider.Digest(rawManifest) != manifestDigest || info.Config.Labels[labelPrefix+"cache"] != "volumes/v1" {
		return ErrIdentity
	}
	var manifest struct {
		Bundle  cacheManifestFile   `json:"bundle"`
		Inputs  []cacheManifestFile `json:"inputs"`
		Network struct {
			RemoteInternet string `json:"remote_internet"`
		} `json:"network"`
		WorkloadSecrets []json.RawMessage `json:"workload_secrets"`
	}
	if json.Unmarshal(rawManifest, &manifest) != nil || !manifest.Bundle.SHA256.Valid() || manifest.Bundle.Bytes < 0 || len(manifest.Inputs) > 64 || !validStoppedPolicy(info, manifest.Network.RemoteInternet == "required", len(manifest.WorkloadSecrets) > 0) {
		return ErrIdentity
	}
	objects := []stagedFile{{Bytes: manifest.Bundle.Bytes, SHA256: manifest.Bundle.SHA256}}
	for _, input := range manifest.Inputs {
		if !input.SHA256.Valid() || input.Bytes < 0 {
			return ErrIdentity
		}
		objects = append(objects, stagedFile{Bytes: input.Bytes, SHA256: input.SHA256})
	}
	bindings, err := a.activeCacheBindings(ctx, identity, objects)
	if err != nil || len(bindings) == 0 {
		return ErrIdentity
	}
	paths := map[struct {
		sha   domain.SHA256Digest
		bytes int64
	}]string{}
	for _, binding := range bindings {
		paths[struct {
			sha   domain.SHA256Digest
			bytes int64
		}{binding.Key.SHA256, binding.Key.Bytes}] = binding.Path
	}
	for _, file := range append([]cacheManifestFile{manifest.Bundle}, manifest.Inputs...) {
		if file.Path != paths[struct {
			sha   domain.SHA256Digest
			bytes int64
		}{file.SHA256, file.Bytes}] {
			return ErrIdentity
		}
	}
	if err := a.verifyCacheMounts(ctx, d, info, identity, bindings); err != nil {
		return err
	}
	modules, err := runnerassets.Sources()
	if err != nil {
		return err
	}
	files := map[string]stagedFile{"manifest.json": {Bytes: int64(len(rawManifest)), SHA256: provider.Digest(rawManifest)}, "runner/run.py": {Bytes: int64(len(entryPoint)), SHA256: provider.Digest([]byte(entryPoint))}}
	for name, source := range modules {
		files["runner/relay_runner/"+name] = stagedFile{Bytes: int64(len(source)), SHA256: provider.Digest([]byte(source))}
	}
	if !preparationHashMatches(info, planDigest, files, verificationDigest) {
		return ErrIdentity
	}
	return a.verifyStagedOnHost(ctx, d, info, identity, files)
}

func (a *Adapter) checkCachedStart(ctx context.Context, d docker, info containerInfo, prepared provider.Prepared) error {
	if !prepared.VerificationSHA256.Valid() || !prepared.ManifestSHA256.Valid() || info.Config.Labels[labelPrefix+"plan"] != string(prepared.PlanSHA256) {
		return ErrIdentity
	}
	raw, found, err := readFile(ctx, d, info.ID, stagedMarker, maxCacheMarkerBytes)
	if err != nil || !found {
		return ErrIdentity
	}
	var proof marker
	ref, err := decodeRef(prepared.Resource, prepared.Identity)
	if err != nil || json.Unmarshal(raw, &proof) != nil || proof != (marker{Preparation: prepared.PreparationID, Plan: prepared.PlanSHA256, Container: info.ID, EngineID: ref.EngineID, Verification: prepared.VerificationSHA256, Manifest: prepared.ManifestSHA256, Cache: true, ApplicationImage: info.Config.Labels[labelPrefix+"application-image"]}) {
		return ErrIdentity
	}
	manifest, found, err := readFile(ctx, d, info.ID, "/relay/manifest.json", maxManifestBytes)
	if err != nil || !found {
		return ErrIdentity
	}
	return a.verifyCachedReadiness(ctx, d, info, prepared.Identity, prepared.PlanSHA256, manifest, prepared.ManifestSHA256, prepared.VerificationSHA256)
}
