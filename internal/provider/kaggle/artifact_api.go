package kaggle

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
	"github.com/vankhaivn/compute-relay/internal/provider/runnerplan"
)

func runArtifacts(ctx context.Context, _ Config, mode string, token []byte, request artifactRequest, dst io.Writer) ([]byte, error) {
	client, err := nativeClient(token)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	return artifacts(ctx, client, mode, request, dst)
}

func artifacts(ctx context.Context, client *kaggleapi.Client, mode string, request artifactRequest, dst io.Writer) ([]byte, error) {
	if mode != "catalog" && mode != "fetch" || mode == "catalog" && request.Target != nil || mode == "fetch" && (request.Target == nil || dst == nil) {
		return nil, ErrArtifactTransfer
	}
	r := request.Execution
	resource := kaggleapi.Resource{Owner: r.Owner, Slug: r.Slug}
	matched, err := accountMatches(ctx, client, r.Owner)
	if err != nil || !matched {
		return nil, ErrArtifactTransfer
	}
	check := func() error {
		kernel, err := client.GetKernel(ctx, resource)
		if err != nil || !matchingKernel(kernel, r, r.KernelID) {
			return ErrArtifactTransfer
		}
		return nil
	}
	terminal := func() error {
		state, err := client.KernelSessionStatus(ctx, resource)
		if err != nil || state != kaggleapi.KernelComplete && state != kaggleapi.KernelError {
			return ErrArtifactTransfer
		}
		return nil
	}
	download := func(path string, limit int64, sink io.Writer) (artifactEntry, error) {
		receipt, err := client.DownloadKernelOutput(ctx, resource, "relay-result/"+path, limit, sink)
		if err != nil {
			return artifactEntry{}, err
		}
		return artifactEntry{path, receipt.Bytes, domainDigest(receipt.SHA256[:])}, nil
	}
	if check() != nil || terminal() != nil {
		return nil, ErrArtifactTransfer
	}
	var manifest bytes.Buffer
	manifestFile, err := download(artifactManifest, runnerplan.MaxManifestBytes, &manifest)
	if err != nil {
		return nil, ErrArtifactTransfer
	}
	selected, err := selectArtifacts(manifest.Bytes(), request)
	if err != nil {
		return nil, err
	}
	if mode == "catalog" {
		var total int64
		for _, file := range selected {
			total += file.Bytes
		}
		for _, path := range []string{"control/stdout.log", "control/stderr.log", "control/environment.json"} {
			cap, _ := artifactControlLimit(path)
			file, err := download(path, min(cap, request.MaxBytes-total), io.Discard)
			if err != nil {
				var apiErr *kaggleapi.Error
				if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
					continue
				}
				return nil, ErrArtifactTransfer
			}
			if len(selected) >= request.MaxFiles {
				return nil, ErrArtifactTransfer
			}
			total += file.Bytes
			selected = append(selected, file)
		}
	} else {
		target := *request.Target
		if target.Path == artifactManifest {
			if manifestFile != target {
				return nil, ErrArtifactTransfer
			}
			if n, err := dst.Write(manifest.Bytes()); err != nil || n != manifest.Len() {
				return nil, ErrArtifactTransfer
			}
		} else {
			_, control := artifactControlLimit(target.Path)
			found := control
			for _, file := range selected {
				found = found || file == target
			}
			if !found {
				return nil, ErrArtifactTransfer
			}
			actual, err := download(target.Path, target.Bytes, dst)
			if err != nil || actual != target {
				return nil, ErrArtifactTransfer
			}
		}
	}
	// Complete payload bytes do not bypass final terminal and identity checks.
	if terminal() != nil || check() != nil {
		return nil, ErrArtifactTransfer
	}
	if mode == "fetch" {
		return nil, nil
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Path < selected[j].Path })
	return json.Marshal(struct {
		Protocol int             `json:"protocol"`
		Files    []artifactEntry `json:"files"`
	}{1, selected})
}

func domainDigest(raw []byte) domain.SHA256Digest {
	return domain.SHA256Digest(hex.EncodeToString(raw))
}

func selectArtifacts(raw []byte, request artifactRequest) ([]artifactEntry, error) {
	manifest, err := runnerplan.ParseResult(request.Identity, raw)
	if err != nil || len(manifest.Artifacts) >= request.MaxFiles || int64(len(raw)) > request.MaxBytes {
		return nil, ErrArtifactTransfer
	}
	selected := []artifactEntry{{artifactManifest, int64(len(raw)), provider.Digest(raw)}}
	counts, sizes := make([]int, len(request.Outputs)), make([]int64, len(request.Outputs))
	total := int64(len(raw))
	for _, file := range manifest.Artifacts {
		path := "outputs/" + file.Path
		if !artifactSafePath(path) || file.Bytes < 0 || file.Bytes > request.MaxBytes-total {
			return nil, ErrArtifactTransfer
		}
		match := -1
		for i, output := range request.Outputs {
			if output.Kind == "file" && file.Path == output.Path || output.Kind == "directory" && strings.HasPrefix(file.Path, output.Path+"/") {
				match = i
				break
			}
		}
		if match < 0 {
			return nil, ErrArtifactTransfer
		}
		counts[match]++
		sizes[match] += file.Bytes
		if limit := request.Outputs[match].MaxBytes; limit != 0 && sizes[match] > limit {
			return nil, ErrArtifactTransfer
		}
		total += file.Bytes
		selected = append(selected, artifactEntry{path, file.Bytes, file.SHA256})
	}
	if manifest.Phase == "completed" {
		for i, output := range request.Outputs {
			if output.Required && counts[i] == 0 {
				return nil, ErrArtifactTransfer
			}
		}
	}
	return selected, nil
}
