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

var cleanupReasons = map[string]bool{
	"cleanup_request_invalid": true, "cleanup_account_read_failed": true,
	"cleanup_account_mismatch": true, "cleanup_resource_read_failed": true,
	"cleanup_resource_identity_mismatch": true, "cleanup_status_read_failed": true,
	"cleanup_execution_active": true, "cleanup_marker_download_failed": true,
	"cleanup_marker_mismatch": true, "cleanup_deletion_unconfirmed": true,
	"cleanup_delete_failed": true, "cleanup_delete_readback_failed": true,
	"cleanup_resource_still_visible": true,
}

func cleanup(ctx context.Context, client *kaggleapi.Client, mode string, request cleanupRequest) cleanupResponse {
	result := func(status string) cleanupResponse { return cleanupResponse{Protocol: 1, Status: status} }
	failure := func(status, reason string) cleanupResponse {
		return cleanupResponse{Protocol: 1, Status: status, Reason: reason}
	}
	if mode != "preview" && mode != "apply" && mode != "recover" ||
		request.Kind != "kernel" && request.Kind != "dataset" || !positiveDecimal(request.ID) || !request.SHA256.Valid() {
		return failure("unknown", "cleanup_request_invalid")
	}
	matched, err := accountMatches(ctx, client, request.Owner)
	if err != nil {
		return failure("unknown", "cleanup_account_read_failed")
	}
	if !matched {
		return failure("identity_mismatch", "cleanup_account_mismatch")
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
	readReason := ""
	check := func() string {
		_, matched, err := read()
		if err != nil {
			var apiErr *kaggleapi.Error
			if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
				return "absent"
			}
			readReason = "cleanup_resource_read_failed"
			return "unknown"
		}
		if !matched {
			readReason = "cleanup_resource_identity_mismatch"
			return "identity_mismatch"
		}
		return ""
	}
	if status := check(); status != "" {
		return failure(status, readReason)
	}
	if request.Kind == "kernel" {
		state, err := client.KernelSessionStatus(ctx, resource)
		if err != nil {
			return failure("active", "cleanup_status_read_failed")
		}
		if state != kaggleapi.KernelComplete && state != kaggleapi.KernelError {
			return failure("active", "cleanup_execution_active")
		}
	} else {
		receipt, err := client.DownloadDatasetFile(ctx, resource, "relay-stage.bin", maxMarkerBytes, io.Discard)
		if err != nil {
			return failure("unknown", "cleanup_marker_download_failed")
		}
		if domainDigest(receipt.SHA256[:]) != request.SHA256 {
			return failure("identity_mismatch", "cleanup_marker_mismatch")
		}
	}
	if status := check(); status != "" {
		return failure(status, readReason)
	}
	if mode == "preview" {
		return result("would_delete")
	}
	if mode == "recover" {
		return failure("unknown", "cleanup_deletion_unconfirmed")
	}
	if request.Kind == "kernel" {
		err = client.DeleteKernel(ctx, resource)
	} else {
		err = client.DeleteDataset(ctx, resource)
	}
	if err != nil {
		return failure("unknown", "cleanup_delete_failed")
	}
	_, _, err = read()
	if err != nil {
		var apiErr *kaggleapi.Error
		// An acknowledged synchronous deletion followed by 403 keeps its received
		// completion. The same 403 without that acknowledgement remains unknown.
		if errors.As(err, &apiErr) && (apiErr.StatusCode == 404 || apiErr.StatusCode == 403) {
			return result("deleted")
		}
		return failure("unknown", "cleanup_delete_readback_failed")
	}
	return failure("unknown", "cleanup_resource_still_visible")
}
