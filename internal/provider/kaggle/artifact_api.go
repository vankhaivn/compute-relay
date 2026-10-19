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

func runArtifacts(ctx context.Context, config Config, mode string, token []byte, request artifactRequest, dst io.Writer) ([]byte, error) {
	client, err := nativeClient(token, config)
	if err != nil {
		return nil, artifactFailure("artifact_credential_unavailable")
	}
	defer client.Close()
	return artifacts(ctx, client, mode, request, dst)
}

func artifacts(ctx context.Context, client *kaggleapi.Client, mode string, request artifactRequest, dst io.Writer) ([]byte, error) {
	if mode != "catalog" && mode != "fetch" || mode == "catalog" && request.Target != nil || mode == "fetch" && (request.Target == nil || dst == nil) {
		return nil, artifactFailure("artifact_request_invalid")
	}
	r := request.Execution
	resource := kaggleapi.Resource{Owner: r.Owner, Slug: r.Slug}
	account, err := client.IntrospectToken(ctx)
	if err != nil {
		return nil, artifactFailure("artifact_account_read_failed")
	}
	if !account.Active {
		return nil, artifactFailure("artifact_credential_unavailable")
	}
	if account.Username != r.Owner {
		return nil, artifactFailure("artifact_account_mismatch")
	}
	check := func() error {
		kernel, err := client.GetKernel(ctx, resource)
		if err != nil {
			return artifactFailure("artifact_remote_read_failed")
		}
		if !matchingKernel(kernel, r, r.KernelID) {
			return artifactFailure("artifact_remote_identity_mismatch")
		}
		return nil
	}
	terminal := func() error {
		state, err := client.KernelSessionStatus(ctx, resource)
		if err != nil {
			return artifactFailure("artifact_status_read_failed")
		}
		if state != kaggleapi.KernelComplete && state != kaggleapi.KernelError {
			return artifactFailure("artifact_not_terminal")
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
	if err := check(); err != nil {
		return nil, err
	}
	if err := terminal(); err != nil {
		return nil, err
	}
	var manifest bytes.Buffer
	manifestFile, err := download(artifactManifest, runnerplan.MaxManifestBytes, &manifest)
	if err != nil {
		return nil, artifactDownloadFailure("artifact_manifest_download_failed", err)
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
		for _, control := range []struct{ path, reason string }{
			{"control/stdout.log", "artifact_stdout_download_failed"},
			{"control/stderr.log", "artifact_stderr_download_failed"},
			{"control/environment.json", "artifact_environment_download_failed"},
		} {
			path := control.path
			cap, _ := artifactControlLimit(path)
			file, err := download(path, min(cap, request.MaxBytes-total), io.Discard)
			if err != nil {
				var apiErr *kaggleapi.Error
				if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
					continue
				}
				return nil, artifactDownloadFailure(control.reason, err)
			}
			if len(selected) >= request.MaxFiles {
				return nil, artifactFailure("artifact_file_limit")
			}
			total += file.Bytes
			selected = append(selected, file)
		}
	} else {
		target := *request.Target
		if target.Path == artifactManifest {
			if manifestFile != target {
				return nil, artifactFailure("artifact_target_mismatch")
			}
			if n, err := dst.Write(manifest.Bytes()); err != nil || n != manifest.Len() {
				return nil, artifactFailure("artifact_write_failed")
			}
		} else {
			_, control := artifactControlLimit(target.Path)
			found := control
			for _, file := range selected {
				found = found || file == target
			}
			if !found {
				return nil, artifactFailure("artifact_target_mismatch")
			}
			actual, err := download(target.Path, target.Bytes, dst)
			if err != nil {
				return nil, artifactDownloadFailure("artifact_download_failed", err)
			}
			if actual != target {
				return nil, artifactFailure("artifact_digest_mismatch")
			}
		}
	}
	// Complete payload bytes do not bypass final terminal and identity checks.
	if err := terminal(); err != nil {
		return nil, err
	}
	if err := check(); err != nil {
		return nil, err
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
	if err != nil {
		return nil, artifactFailure("artifact_manifest_invalid")
	}
	if len(manifest.Artifacts) >= request.MaxFiles {
		return nil, artifactFailure("artifact_file_limit")
	}
	if int64(len(raw)) > request.MaxBytes {
		return nil, artifactFailure("artifact_byte_limit")
	}
	selected := []artifactEntry{{artifactManifest, int64(len(raw)), provider.Digest(raw)}}
	counts, sizes := make([]int, len(request.Outputs)), make([]int64, len(request.Outputs))
	total := int64(len(raw))
	for _, file := range manifest.Artifacts {
		path := "outputs/" + file.Path
		if !artifactSafePath(path) || file.Bytes < 0 {
			return nil, artifactFailure("artifact_path_invalid")
		}
		if file.Bytes > request.MaxBytes-total {
			return nil, artifactFailure("artifact_byte_limit")
		}
		match := -1
		for i, output := range request.Outputs {
			if output.Kind == "file" && file.Path == output.Path || output.Kind == "directory" && strings.HasPrefix(file.Path, output.Path+"/") {
				match = i
				break
			}
		}
		if match < 0 {
			return nil, artifactFailure("artifact_output_undeclared")
		}
		counts[match]++
		sizes[match] += file.Bytes
		if limit := request.Outputs[match].MaxBytes; limit != 0 && sizes[match] > limit {
			return nil, artifactFailure("artifact_byte_limit")
		}
		total += file.Bytes
		selected = append(selected, artifactEntry{path, file.Bytes, file.SHA256})
	}
	if manifest.Phase == "completed" {
		for i, output := range request.Outputs {
			if output.Required && counts[i] == 0 {
				return nil, artifactFailure("artifact_output_missing")
			}
		}
	}
	return selected, nil
}

func artifactDownloadFailure(reason string, err error) error {
	if errors.Is(err, kaggleapi.ErrLimit) {
		return artifactFailure("artifact_byte_limit")
	}
	return artifactFailure(reason)
}
