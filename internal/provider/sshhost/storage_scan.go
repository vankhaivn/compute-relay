package sshhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

const storageLabel = labelPrefix + "installation"

type storageScanBudget struct {
	remaining int64
}

func (b *storageScanBudget) command(ctx context.Context, d docker, args ...string) ([]byte, error) {
	if b.remaining <= 0 {
		return nil, errResponseLimit
	}
	var out bytes.Buffer
	writer := &limitedWriter{w: &out, limit: b.remaining}
	result, err := d.run(ctx, nil, writer, args...)
	b.remaining -= writer.n
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil || result.ExitCode != 0 {
		return nil, failure(errors.Join(ErrRemoteDocker, err), dockerStep(args))
	}
	return out.Bytes(), nil
}

// StorageScanReason names, as a closed reason, the step a storage scan failed at.
func StorageScanReason(err error) string {
	if reason := failureReason(err, ""); reason != "" {
		return reason
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "remote_scan_timeout"
	case errors.Is(err, errResponseLimit):
		return "remote_response_limit"
	case errors.Is(err, ErrRemoteDocker):
		return "remote_docker_failed"
	case errors.Is(err, ErrHostKeyMismatch):
		return "host_key_mismatch"
	case errors.Is(err, ErrCredentialReject):
		return "credential_rejected"
	case errors.Is(err, ErrUnavailable):
		return "host_unavailable"
	case errors.Is(err, hoststorage.ErrUnverified):
		return "storage_identity_unverified"
	}
	return "remote_observation_failed"
}

func storageLines(raw []byte, max int) ([]string, error) {
	if len(raw) > hoststorage.ScanMaxBytes {
		return nil, errResponseLimit
	}
	lines := []string{}
	for _, row := range bytes.Split(raw, []byte{'\n'}) {
		if len(row) == 0 {
			continue
		}
		if len(row) > 255 || len(lines) >= max {
			return nil, errResponseLimit
		}
		lines = append(lines, string(row))
	}
	return lines, nil
}

func storageIDs(ctx context.Context, d docker, budget *storageScanBudget, kind, filter string) ([]string, error) {
	args := []string{kind, "ls"}
	if kind == "container" {
		args = append(args, "-a", "--no-trunc")
	} else if kind == "image" {
		args = append(args, "-a", "--no-trunc")
	}
	args = append(args, "--filter", "label="+filter, "--format")
	if kind == "volume" {
		args = append(args, "{{.Name}}")
	} else {
		args = append(args, "{{.ID}}")
	}
	raw, err := budget.command(ctx, d, args...)
	if err != nil {
		return nil, err
	}
	return storageLines(raw, hoststorage.ScanMaxObjects)
}

func uniqueStorageIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func validStorageLabels(kind hoststorage.Kind, labels map[string]string, scope hoststorage.Scope, nativeEngine string) bool {
	if labels[storageLabel] != string(scope.InstallationID) {
		return false
	}
	switch kind {
	case hoststorage.Container:
		if labels[labelPrefix+"kind"] == "verification" {
			if labels[labelPrefix+"verification-schema"] == cacheBatchSchema {
				return labels[labelPrefix+"machine"] == string(scope.MachineID) && labels[labelPrefix+"engine"] == string(scope.EngineID) && domain.AttemptID(labels[labelPrefix+"attempt"]).Valid() && domain.OperationID(labels[labelPrefix+"operation"]).Valid()
			}
			if labels[labelPrefix+"machine"] != "" || labels[labelPrefix+"engine"] != "" {
				// Cache verifiers inherit their generation's hashed scope and
				// immutable content labels rather than an attempt identity.
				bytes, err := strconv.ParseInt(labels[labelPrefix+"bytes"], 10, 64)
				return labels[labelPrefix+"machine"] == string(scope.MachineID) && labels[labelPrefix+"engine"] == string(scope.EngineID) &&
					domain.SHA256Digest(labels[labelPrefix+"sha256"]).Valid() && err == nil && bytes >= 0 &&
					strconv.FormatInt(bytes, 10) == labels[labelPrefix+"bytes"] && labels[labelPrefix+"generation"] != "" &&
					domain.OperationID(labels[labelPrefix+"operation"]).Valid()
			}
			// Older one-shot verifier helpers predate machine/engine labels.
			// The scan already pinned this Docker engine and their immutable
			// installation, attempt and operation labels bound their origin.
			return labels[labelPrefix+"machine"] == "" && labels[labelPrefix+"engine"] == "" &&
				domain.AttemptID(labels[labelPrefix+"attempt"]).Valid() &&
				domain.OperationID(labels[labelPrefix+"operation"]).Valid()
		}
		if labels[labelPrefix+"kind"] == "cache-writer" {
			return labels[labelPrefix+"machine"] == string(scope.MachineID) && labels[labelPrefix+"engine"] == string(scope.EngineID)
		}
		return labels[labelPrefix+"engine"] == nativeEngine &&
			(labels[labelPrefix+"machine"] == "" || labels[labelPrefix+"machine"] == string(scope.MachineID))
	case hoststorage.Volume:
		return labels[labelPrefix+"machine"] == string(scope.MachineID) && labels[labelPrefix+"engine"] == string(scope.EngineID)
	case hoststorage.Image:
		return labels[labelPrefix+"image-machine"] == string(scope.MachineID) && labels[labelPrefix+"image-engine"] == string(scope.EngineID)
	}
	return false
}

type storageContainerInspect struct {
	ID     string `json:"Id"`
	SizeRw *int64 `json:"SizeRw"`
	State  struct {
		Running bool `json:"Running"`
	} `json:"State"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	Mounts []struct {
		Type string `json:"Type"`
		Name string `json:"Name"`
	} `json:"Mounts"`
}

type storageImageInspect struct {
	ID     string `json:"Id"`
	Size   *int64 `json:"Size"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	Tags    []string `json:"RepoTags"`
	Digests []string `json:"RepoDigests"`
}

// storageInspectBatch bounds one inspect command's arguments. A scan's remote round
// trips grow with batches, not objects, so hundreds of cache volumes fit its timeout.
const storageInspectBatch = 64

func storageInspectObject(ctx context.Context, d docker, budget *storageScanBudget, scope hoststorage.Scope, nativeEngine string, kind hoststorage.Kind, id string) (hoststorage.Object, error) {
	objects, err := storageInspectObjects(ctx, d, budget, scope, nativeEngine, kind, []string{id})
	if err != nil {
		return hoststorage.Object{Kind: kind, ID: id}, err
	}
	return objects[0], nil
}

// storageInspectObjects inspects distinct objects of one kind, one Docker command per batch.
func storageInspectObjects(ctx context.Context, d docker, budget *storageScanBudget, scope hoststorage.Scope, nativeEngine string, kind hoststorage.Kind, ids []string) ([]hoststorage.Object, error) {
	var command []string
	switch kind {
	case hoststorage.Container:
		command = []string{"container", "inspect", "--size"}
	case hoststorage.Volume:
		command = []string{"volume", "inspect"}
	case hoststorage.Image:
		command = []string{"image", "inspect"}
	default:
		return nil, hoststorage.ErrInvalid
	}
	objects := make([]hoststorage.Object, 0, len(ids))
	for start := 0; start < len(ids); start += storageInspectBatch {
		batch := ids[start:min(start+storageInspectBatch, len(ids))]
		for _, id := range batch {
			if !(hoststorage.Object{Kind: kind, ID: id}).Valid() {
				return nil, hoststorage.ErrUnverified
			}
		}
		raw, err := budget.command(ctx, d, append(append([]string(nil), command...), batch...)...)
		if err != nil {
			return nil, err
		}
		inspected, err := decodeStorageObjects(kind, raw)
		if err != nil {
			return nil, err
		}
		// Every requested object must be answered exactly once.
		byID := make(map[string]hoststorage.Object, len(inspected))
		for _, o := range inspected {
			if _, dup := byID[o.ID]; dup {
				return nil, failure(ErrRemoteDocker, "storage_inspect_incomplete")
			}
			byID[o.ID] = o
		}
		if len(byID) != len(batch) {
			return nil, failure(ErrRemoteDocker, "storage_inspect_incomplete")
		}
		for _, id := range batch {
			o, ok := byID[id]
			if !ok {
				return nil, failure(ErrRemoteDocker, "storage_inspect_incomplete")
			}
			if o.Labels[storageLabel] != string(scope.InstallationID) || !o.Valid() {
				return nil, hoststorage.ErrUnverified
			}
			// A machine/engine mismatch stays visible to accounting but is never a
			// deletion authority. Its size uncertainty closes the write gate.
			if !validStorageLabels(kind, o.Labels, scope, nativeEngine) {
				o.SizeKnown = false
			}
			objects = append(objects, o)
		}
	}
	return objects, nil
}

func decodeStorageObjects(kind hoststorage.Kind, raw []byte) ([]hoststorage.Object, error) {
	var objects []hoststorage.Object
	switch kind {
	case hoststorage.Container:
		var inspected []storageContainerInspect
		if json.Unmarshal(raw, &inspected) != nil {
			return nil, failure(ErrRemoteDocker, "storage_inspect_invalid")
		}
		for _, c := range inspected {
			o := hoststorage.Object{Kind: kind, ID: c.ID, Labels: c.Config.Labels, Running: c.State.Running, References: int64(len(c.Mounts))}
			if c.SizeRw != nil && *c.SizeRw >= 0 {
				o.SizeKnown, o.SizeBytes = true, *c.SizeRw
			}
			objects = append(objects, o)
		}
	case hoststorage.Volume:
		var inspected []volumeInfo
		if json.Unmarshal(raw, &inspected) != nil {
			return nil, failure(ErrRemoteDocker, "storage_inspect_invalid")
		}
		for _, v := range inspected {
			o := hoststorage.Object{Kind: kind, ID: v.Name, Labels: v.Labels}
			// Only the immutable declared cache reservation is available from a
			// fixed label. Docker volume inspect does not report disk usage.
			if v.Labels[labelPrefix+"kind"] == "cache" {
				size, err := strconv.ParseInt(v.Labels[labelPrefix+"bytes"], 10, 64)
				if err == nil && size >= 0 {
					o.SizeKnown, o.SizeBytes = true, size
				}
			}
			objects = append(objects, o)
		}
	case hoststorage.Image:
		var inspected []storageImageInspect
		if json.Unmarshal(raw, &inspected) != nil {
			return nil, failure(ErrRemoteDocker, "storage_inspect_invalid")
		}
		for _, i := range inspected {
			o := hoststorage.Object{Kind: kind, ID: i.ID, Labels: i.Config.Labels, References: int64(len(i.Tags) + len(i.Digests))}
			if i.Size != nil && *i.Size >= 0 {
				o.SizeKnown, o.SizeBytes = true, *i.Size
			}
			objects = append(objects, o)
		}
	}
	return objects, nil
}

func scanDockerStorage(ctx context.Context, d docker, scope hoststorage.Scope) (hoststorage.Scan, error) {
	if !scope.Valid() {
		return hoststorage.Scan{}, hoststorage.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, hoststorage.ScanTimeout)
	defer cancel()
	budget := &storageScanBudget{remaining: hoststorage.ScanMaxBytes}
	result := hoststorage.Scan{Scope: scope, Objects: []hoststorage.Object{}}
	nativeEngine, err := d.engineID(ctx)
	if err != nil {
		return hoststorage.Scan{}, err
	}
	containers, err := storageContainers(ctx, d, budget, scope, nativeEngine)
	if err != nil {
		return hoststorage.Scan{}, err
	}
	allContainers, err := storageIDs(ctx, d, budget, "container", storageLabel)
	if err != nil {
		return hoststorage.Scan{}, err
	}
	allContainers = uniqueStorageIDs(allContainers)
	if len(allContainers) < len(containers) {
		return hoststorage.Scan{}, failure(ErrRemoteDocker, "storage_inventory_inconsistent")
	}
	listed := make(map[string]bool, len(allContainers))
	for _, id := range allContainers {
		listed[id] = true
	}
	for _, object := range containers {
		if !listed[object.ID] {
			return hoststorage.Scan{}, failure(ErrRemoteDocker, "storage_inventory_inconsistent")
		}
	}
	result.Objects = append(result.Objects, containers...)
	result.Foreign.Containers = int64(len(allContainers) - len(containers))
	for _, class := range []struct {
		docker  string
		kind    hoststorage.Kind
		foreign *int64
	}{
		{"volume", hoststorage.Volume, &result.Foreign.Volumes},
		{"image", hoststorage.Image, &result.Foreign.Images},
	} {
		own, err := storageIDs(ctx, d, budget, class.docker, storageLabel+"="+string(scope.InstallationID))
		if err != nil {
			return hoststorage.Scan{}, err
		}
		own = uniqueStorageIDs(own)
		all, err := storageIDs(ctx, d, budget, class.docker, storageLabel)
		if err != nil {
			return hoststorage.Scan{}, err
		}
		all = uniqueStorageIDs(all)
		// The second list is only counted. Foreign IDs and labels are never
		// inspected, returned, logged, or persisted.
		if len(all) < len(own) {
			return hoststorage.Scan{}, failure(ErrRemoteDocker, "storage_inventory_inconsistent")
		}
		*class.foreign = int64(len(all) - len(own))
		objects, err := storageInspectObjects(ctx, d, budget, scope, nativeEngine, class.kind, own)
		if err != nil {
			return hoststorage.Scan{}, err
		}
		result.Objects = append(result.Objects, objects...)
	}
	result.ObservedAt = time.Now().UTC()
	if !result.Valid() {
		return hoststorage.Scan{}, hoststorage.ErrInvalid
	}
	return result, nil
}

// ScanStorage observes one verified Docker engine through the frozen SSH
// binding. The adapter's withDocker check pins both machine and engine before
// any list or inspect command runs.
func (a *Adapter) ScanStorage(ctx context.Context, scope hoststorage.Scope) (hoststorage.Scan, error) {
	if a == nil || !scope.Valid() || a.config.MachineID != string(scope.MachineID) || a.config.EngineID != string(scope.EngineID) {
		return hoststorage.Scan{}, failure(hoststorage.ErrUnverified, "storage_binding_mismatch")
	}
	var scan hoststorage.Scan
	err := a.withDocker(ctx, func(d docker, _ Endpoint) error {
		var err error
		scan, err = scanDockerStorage(ctx, d, scope)
		return err
	})
	return scan, err
}
