package sshhost

import (
	"bytes"
	"context"
	"encoding/json"
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
	if err != nil || result.ExitCode != 0 {
		return nil, ErrRemoteDocker
	}
	return out.Bytes(), nil
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

func storageInspectObject(ctx context.Context, d docker, budget *storageScanBudget, scope hoststorage.Scope, nativeEngine string, kind hoststorage.Kind, id string) (hoststorage.Object, error) {
	o := hoststorage.Object{Kind: kind, ID: id}
	if !o.Valid() {
		return o, hoststorage.ErrUnverified
	}
	switch kind {
	case hoststorage.Container:
		raw, err := budget.command(ctx, d, "container", "inspect", "--size", id)
		if err != nil {
			return o, err
		}
		var inspected []storageContainerInspect
		if json.Unmarshal(raw, &inspected) != nil || len(inspected) != 1 || inspected[0].ID != id {
			return o, ErrRemoteDocker
		}
		o.Labels = inspected[0].Config.Labels
		o.Running = inspected[0].State.Running
		if inspected[0].SizeRw != nil && *inspected[0].SizeRw >= 0 {
			o.SizeKnown, o.SizeBytes = true, *inspected[0].SizeRw
		}
		o.References = int64(len(inspected[0].Mounts))
	case hoststorage.Volume:
		raw, err := budget.command(ctx, d, "volume", "inspect", id)
		if err != nil {
			return o, err
		}
		var inspected []volumeInfo
		if json.Unmarshal(raw, &inspected) != nil || len(inspected) != 1 || inspected[0].Name != id {
			return o, ErrRemoteDocker
		}
		o.Labels = inspected[0].Labels
		// Only the immutable declared cache reservation is available from a
		// fixed label. Docker volume inspect does not report disk usage.
		if inspected[0].Labels[labelPrefix+"kind"] == "cache" {
			size, err := strconv.ParseInt(inspected[0].Labels[labelPrefix+"bytes"], 10, 64)
			if err == nil && size >= 0 {
				o.SizeKnown, o.SizeBytes = true, size
			}
		}
	case hoststorage.Image:
		raw, err := budget.command(ctx, d, "image", "inspect", id)
		if err != nil {
			return o, err
		}
		var inspected []storageImageInspect
		if json.Unmarshal(raw, &inspected) != nil || len(inspected) != 1 || inspected[0].ID != id {
			return o, ErrRemoteDocker
		}
		o.Labels = inspected[0].Config.Labels
		if inspected[0].Size != nil && *inspected[0].Size >= 0 {
			o.SizeKnown, o.SizeBytes = true, *inspected[0].Size
		}
		o.References = int64(len(inspected[0].Tags) + len(inspected[0].Digests))
	default:
		return o, hoststorage.ErrInvalid
	}
	if o.Labels[storageLabel] != string(scope.InstallationID) || !o.Valid() {
		return o, hoststorage.ErrUnverified
	}
	// A machine/engine mismatch stays visible to accounting but is never a
	// deletion authority. Its size uncertainty closes the write gate.
	if !validStorageLabels(kind, o.Labels, scope, nativeEngine) {
		o.SizeKnown = false
	}
	return o, nil
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
		return hoststorage.Scan{}, ErrRemoteDocker
	}
	listed := make(map[string]bool, len(allContainers))
	for _, id := range allContainers {
		listed[id] = true
	}
	for _, object := range containers {
		if !listed[object.ID] {
			return hoststorage.Scan{}, ErrRemoteDocker
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
			return hoststorage.Scan{}, ErrRemoteDocker
		}
		*class.foreign = int64(len(all) - len(own))
		for _, id := range own {
			object, err := storageInspectObject(ctx, d, budget, scope, nativeEngine, class.kind, id)
			if err != nil {
				return hoststorage.Scan{}, err
			}
			result.Objects = append(result.Objects, object)
		}
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
		return hoststorage.Scan{}, hoststorage.ErrUnverified
	}
	var scan hoststorage.Scan
	err := a.withDocker(ctx, func(d docker, _ Endpoint) error {
		var err error
		scan, err = scanDockerStorage(ctx, d, scope)
		return err
	})
	return scan, err
}
