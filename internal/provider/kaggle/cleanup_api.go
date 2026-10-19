package kaggle

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

func runCleanup(parent context.Context, config Config, mode string, token []byte, request cleanupRequest) (cleanupResponse, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Minute)
	defer cancel()
	client, err := nativeClient(token, config)
	if err != nil {
		return cleanupResponse{}, err
	}
	defer client.Close()
	return cleanup(ctx, client, mode, request), nil
}

func cleanup(ctx context.Context, client *kaggleapi.Client, mode string, request cleanupRequest) cleanupResponse {
	result := func(status string) cleanupResponse { return cleanupResponse{1, status} }
	if mode != "preview" && mode != "apply" && mode != "recover" ||
		request.Kind != "kernel" && request.Kind != "dataset" || !positiveDecimal(request.ID) || !request.SHA256.Valid() {
		return result("unknown")
	}
	matched, err := accountMatches(ctx, client, request.Owner)
	if err != nil {
		return result("unknown")
	}
	if !matched {
		return result("identity_mismatch")
	}
	resource := kaggleapi.Resource{Owner: request.Owner, Slug: request.Slug}
	read := func() (bool, bool, error) {
		if request.Kind == "kernel" {
			kernel, err := client.GetKernel(ctx, resource)
			if err != nil {
				return false, false, err
			}
			m, blob := kernel.Metadata, kernel.Blob
			match := m != nil && string(m.ID) == request.ID && m.Reference == request.Owner+"/"+request.Slug && m.Slug == request.Slug &&
				m.Version != nil && *m.Version == 1 && m.Private != nil && *m.Private && blob != nil && blob.Source != nil &&
				provider.Digest([]byte(*blob.Source)) == request.SHA256
			return true, match, nil
		}
		meta, err := client.GetDataset(ctx, resource)
		if err != nil {
			return false, false, err
		}
		match := string(meta.ID) == request.ID && meta.Reference == request.Owner+"/"+request.Slug && meta.Version != nil &&
			*meta.Version == 1 && meta.Private != nil && *meta.Private && meta.Description == stagingDescription(request.SHA256)
		return true, match, nil
	}
	check := func() string {
		_, matched, err := read()
		if err != nil {
			var apiErr *kaggleapi.Error
			if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
				return "absent"
			}
			return "unknown"
		}
		if !matched {
			return "identity_mismatch"
		}
		return ""
	}
	if status := check(); status != "" {
		return result(status)
	}
	if request.Kind == "kernel" {
		state, err := client.KernelSessionStatus(ctx, resource)
		if err != nil || state != kaggleapi.KernelComplete && state != kaggleapi.KernelError {
			return result("active")
		}
	} else {
		receipt, err := client.DownloadDatasetFile(ctx, resource, "relay-stage.bin", maxMarkerBytes, io.Discard)
		if err != nil {
			return result("unknown")
		}
		if domainDigest(receipt.SHA256[:]) != request.SHA256 {
			return result("identity_mismatch")
		}
	}
	if status := check(); status != "" {
		return result(status)
	}
	if mode == "preview" {
		return result("would_delete")
	}
	if mode == "recover" {
		return result("unknown")
	}
	if request.Kind == "kernel" {
		err = client.DeleteKernel(ctx, resource)
	} else {
		err = client.DeleteDataset(ctx, resource)
	}
	if err != nil {
		return result("unknown")
	}
	_, _, err = read()
	if err != nil {
		var apiErr *kaggleapi.Error
		// An acknowledged synchronous deletion followed by 403 keeps its received
		// completion. The same 403 without that acknowledgement remains unknown.
		if errors.As(err, &apiErr) && (apiErr.StatusCode == 404 || apiErr.StatusCode == 403) {
			return result("deleted")
		}
	}
	return result("unknown")
}
