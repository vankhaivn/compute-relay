package sshhost

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostcache"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

const cacheVerifyBatch = 64
const cacheBatchSchema = "cache-batch/v1"

// cacheVolumeInventory reads only this frozen scope. A complete labelled listing
// is required before absence or an immutable generation can be trusted.
func cacheVolumeInventory(ctx context.Context, d docker, scope hostcache.Key) (map[string]volumeInfo, error) {
	filters, _ := json.Marshal(map[string][]string{"label": {
		labelPrefix + "installation=" + string(scope.InstallationID), labelPrefix + "machine=" + string(scope.MachineID),
		labelPrefix + "engine=" + string(scope.EngineID), labelPrefix + "kind=cache",
	}})
	request, err := http.NewRequest(http.MethodGet, "http://docker/volumes?"+url.Values{"filters": {string(filters)}}.Encode(), nil)
	if err != nil {
		return nil, failure(err, "cache_inventory_request_invalid")
	}
	request.Close = true
	budget := storageScanBudget{remaining: hoststorage.ScanMaxBytes}
	raw, err := budget.readDockerJSON(ctx, d, request)
	if err != nil {
		return nil, failure(err, "cache_inventory_read_failed")
	}
	var envelope struct {
		Volumes  json.RawMessage
		Warnings []string
	}
	if json.Unmarshal(raw, &envelope) != nil || len(envelope.Volumes) == 0 || len(envelope.Warnings) != 0 {
		return nil, failure(ErrRemoteDocker, "cache_inventory_incomplete")
	}
	var volumes []volumeInfo
	if json.Unmarshal(envelope.Volumes, &volumes) != nil || len(volumes) > hoststorage.ScanMaxObjects {
		return nil, failure(ErrRemoteDocker, "cache_inventory_invalid")
	}
	result := make(map[string]volumeInfo, len(volumes))
	for _, volume := range volumes {
		labels := volume.Labels
		if volume.Name == "" || labels[labelPrefix+"installation"] != string(scope.InstallationID) || labels[labelPrefix+"machine"] != string(scope.MachineID) || labels[labelPrefix+"engine"] != string(scope.EngineID) || labels[labelPrefix+"kind"] != "cache" {
			return nil, failure(ErrIdentity, "cache_inventory_identity_unverified")
		}
		if _, duplicate := result[volume.Name]; duplicate {
			return nil, failure(ErrRemoteDocker, "cache_inventory_duplicate")
		}
		result[volume.Name] = volume
	}
	return result, nil
}

type cacheBatchFile struct {
	Index  int    `json:"index"`
	Status string `json:"status"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type cacheBatchInventory struct {
	Files []cacheBatchFile `json:"files"`
}

// observeVolumes never writes or replaces a generation. Only complete metadata
// and per-volume content evidence can authorize a terminal cache observation.
func (a *Adapter) observeVolumes(ctx context.Context, d docker, generations []hostcache.Generation) ([]hostcache.Observation, error) {
	if len(generations) == 0 {
		return nil, nil
	}
	if len(generations) > 257 {
		return nil, failure(ErrRequest, "cache_generation_limit")
	}
	first := generations[0]
	seen := make(map[string]bool, len(generations))
	observations := make([]hostcache.Observation, len(generations))
	for i, g := range generations {
		if !g.Valid() || g.Key.InstallationID != first.Key.InstallationID || g.Key.MachineID != first.Key.MachineID || g.Key.EngineID != first.Key.EngineID || seen[g.VolumeName] {
			return nil, failure(ErrIdentity, "cache_batch_identity_unverified")
		}
		seen[g.VolumeName] = true
		observations[i] = hostcache.Observation{GenerationID: g.ID, VolumeName: g.VolumeName, OperationID: g.OperationID, Key: g.Key, WriterInactive: true}
	}
	volumes, err := cacheVolumeInventory(ctx, d, first.Key)
	if err != nil {
		return nil, err
	}
	eligible := make([]int, 0, len(generations))
	for i, g := range generations {
		v, found := volumes[g.VolumeName]
		if !found || !exactStorageLabels(v.Labels, g.Labels()) {
			observations[i].Kind = hostcache.ObservedMissing
			continue
		}
		if !validCacheVolume(v, g) {
			observations[i].Kind = hostcache.ObservedCorrupt
			continue
		}
		observations[i].Labels = v.Labels
		eligible = append(eligible, i)
	}
	if len(eligible) == 0 {
		return observations, nil
	}
	image, err := a.standardVerifierImage(ctx, d)
	if err != nil {
		return nil, err
	}
	for start := 0; start < len(eligible); start += cacheVerifyBatch {
		indices := eligible[start:min(start+cacheVerifyBatch, len(eligible))]
		args := []string{"container", "run", "--rm", "--pull=never", "--user", "65534:65534", "--network", "none", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--read-only", "--pids-limit", "32", "--memory", "134217728", "--memory-swap", "134217728", "--cpus", "1"}
		sizes := make([]int64, len(indices))
		for j, i := range indices {
			g := generations[i]
			sizes[j] = g.Key.Bytes
			args = append(args, "--mount", "type=volume,source="+g.VolumeName+",target=/cache/"+strconv.Itoa(j)+",readonly,volume-nocopy")
		}
		args = append(args, "--label", labelPrefix+"kind=verification", "--label", labelPrefix+"installation="+string(first.Key.InstallationID), "--label", labelPrefix+"machine="+string(first.Key.MachineID), "--label", labelPrefix+"engine="+string(first.Key.EngineID), "--label", labelPrefix+"verification-schema="+cacheBatchSchema, "--label", labelPrefix+"attempt="+string(first.Owner.AttemptID), "--label", labelPrefix+"operation="+string(first.OperationID))
		encoded, _ := json.Marshal(sizes)
		args = append(args, "--entrypoint", "python3", image, "-B", "-E", "-s", "-c", hostVerifierBootstrap, base64.StdEncoding.EncodeToString([]byte(hostVerifierSource)), "volumes", base64.StdEncoding.EncodeToString(encoded))
		var output verifierOutput
		result, err := d.run(ctx, nil, &output, args...)
		if err != nil {
			return nil, failure(err, "cache_batch_verifier_failed")
		}
		if result.ExitCode != 0 || output.overLimit {
			return nil, failure(hostcache.ErrUnknown, "cache_batch_verifier_failed")
		}
		var inventory cacheBatchInventory
		decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&inventory) != nil || decoder.Decode(new(any)) != io.EOF || len(inventory.Files) != len(indices) {
			return nil, failure(hostcache.ErrUnknown, "cache_batch_response_invalid")
		}
		for j, file := range inventory.Files {
			i := indices[j]
			g := generations[i]
			if file.Index != j || file.Status != "complete" && file.Status != "corrupt" {
				return nil, failure(hostcache.ErrUnknown, "cache_batch_response_invalid")
			}
			o := &observations[i]
			switch file.Status {
			case "corrupt":
				if file.Bytes != 0 || file.SHA256 != "" {
					return nil, failure(hostcache.ErrUnknown, "cache_batch_response_invalid")
				}
				o.Kind = hostcache.ObservedCorrupt
			case "complete":
				if file.Bytes < 0 || !domain.SHA256Digest(file.SHA256).Valid() {
					return nil, failure(hostcache.ErrUnknown, "cache_batch_response_invalid")
				}
				o.Kind = hostcache.ObservedCorrupt
				if file.Bytes == g.Key.Bytes && file.SHA256 == string(g.Key.SHA256) {
					o.Kind = hostcache.ObservedComplete
					o.ContentBytes = file.Bytes
					o.ContentSHA256 = domain.SHA256Digest(file.SHA256)
				}
			}
		}
	}
	return observations, nil
}
