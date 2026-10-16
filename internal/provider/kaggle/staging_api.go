package kaggle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

func runStaging(ctx context.Context, _ Config, mode string, token []byte, plan stagingPlan, blobs StagingBlobs) (stagingResponse, error) {
	client, err := nativeClient(token)
	if err != nil {
		return stagingResponse{}, err
	}
	defer client.Close()
	return stage(ctx, client, mode, plan, blobs)
}

func stagingDescription(digest domain.SHA256Digest) string {
	return "Compute Relay staging " + string(digest) + ". Existing licenses remain applicable."
}

func stagingLicense(value string) string {
	if value == "other" {
		return "Other (specified in description)"
	}
	if value == "unknown" {
		return "Unknown"
	}
	return value
}

// stage creates or observes one attempt's staging. "observe" downloads and rehashes the
// attempt dataset; "recheck" compares the same metadata without downloading. Shared
// datasets are always checked by metadata; the remote runner rehashes every input.
func stage(ctx context.Context, client *kaggleapi.Client, mode string, plan stagingPlan, blobs StagingBlobs) (stagingResponse, error) {
	empty := func(status string) stagingResponse { return stagingResponse{Protocol: 1, Status: status} }
	if mode != "create" && mode != "observe" && mode != "recheck" {
		return stagingResponse{}, ErrMode
	}
	r := plan.request
	resource := kaggleapi.Resource{Owner: r.Owner, Slug: r.Slug}
	matched, err := accountMatches(ctx, client, r.Owner)
	if err != nil {
		return empty("unknown"), nil
	}
	if !matched {
		return empty("invalid"), nil
	}
	meta, err := client.GetDataset(ctx, resource)
	if err != nil && !kaggleapi.NameAbsent(err) {
		return empty("unknown"), nil
	}
	if err == nil {
		return observeStaging(ctx, client, plan, meta, mode == "observe" || mode == "create"), nil
	}
	if mode != "create" {
		return empty("not_found"), nil
	}
	// Shared datasets come first, so an attempt dataset exists only after every shared
	// input it names was created. A missing one is created once under this preparation.
	var missing []sharedInput
	for _, entry := range plan.shared {
		switch state := observeShared(ctx, client, entry); state {
		case "not_found":
			missing = append(missing, entry)
		case "ready", "pending":
		default:
			return empty(state), nil
		}
	}
	progress := &uploadProgress{}
	for _, object := range plan.objects {
		progress.total += object.Bytes
	}
	for _, entry := range missing {
		progress.total += entry.object.Bytes
	}
	provider.ReportPreparationProgress(ctx, 0, progress.total)
	for _, entry := range missing {
		if status := createStaging(ctx, client, entry.request, entry.marker, []domain.ObjectMetadata{entry.object}, blobs, progress); status != "" {
			return empty(status), nil
		}
	}
	if status := createStaging(ctx, client, r, plan.marker, plan.objects, blobs, progress); status != "" {
		return empty(status), nil
	}
	meta, err = client.GetDataset(ctx, resource)
	if err != nil {
		var apiErr *kaggleapi.Error
		if errors.As(err, &apiErr) && apiErr.StatusCode == 403 && kaggleapi.NameAbsent(err) {
			return empty("pending"), nil
		}
		return empty("unknown"), nil
	}
	return observeStaging(ctx, client, plan, meta, true), nil
}

type uploadProgress struct {
	sent, total int64
}

// createStaging uploads a marker and its objects (same order as Files[1:]) and creates
// one private dataset. It returns a non-empty status when creation did not complete.
func createStaging(ctx context.Context, client *kaggleapi.Client, r stagingRequest, marker []byte, objects []domain.ObjectMetadata, blobs StagingBlobs, progress *uploadProgress) string {
	files := make([]*kaggleapi.UploadedBlob, 0, len(r.Files))
	for index, file := range r.Files {
		ticket, err := client.StartBlobUpload(ctx, file.Name, file.Bytes)
		if err != nil {
			return "unknown"
		}
		var source io.ReadCloser
		if index == 0 {
			source = io.NopCloser(bytes.NewReader(marker))
		} else {
			object := objects[index-1]
			source, err = blobs.Open(ctx, object.WorkspaceID, object.ID)
			if err != nil {
				return "unknown"
			}
			source = &stagingProgressReader{ReadCloser: source, ctx: ctx, sent: &progress.sent, total: progress.total, remaining: file.Bytes}
		}
		digest, err := hex.DecodeString(string(file.SHA256))
		if err != nil || len(digest) != sha256.Size {
			_ = source.Close()
			return "invalid"
		}
		uploaded, err := client.UploadBlob(ctx, ticket, source, [sha256.Size]byte(digest))
		if err != nil {
			return "unknown"
		}
		files = append(files, uploaded)
	}
	// UploadedBlob exists only after complete bytes, matching SHA256, exact EOF
	// and successful source Close. No create can follow an uncertain local source.
	_, err := client.CreateDataset(ctx, kaggleapi.CreateDatasetRequest{Resource: kaggleapi.Resource{Owner: r.Owner, Slug: r.Slug},
		License: r.License, Description: stagingDescription(r.MarkerSHA256), Files: files})
	if err != nil {
		return "unknown"
	}
	return ""
}

// observeShared checks one shared dataset's identity, privacy, readiness and exact
// file list without downloading it.
func observeShared(ctx context.Context, client *kaggleapi.Client, entry sharedInput) string {
	r := entry.request
	resource := kaggleapi.Resource{Owner: r.Owner, Slug: r.Slug}
	meta, err := client.GetDataset(ctx, resource)
	if err != nil {
		if kaggleapi.NameAbsent(err) {
			return "not_found"
		}
		return "unknown"
	}
	if !matchingStaging(meta, r) || meta.Private == nil || !*meta.Private {
		return "invalid"
	}
	switch state, err := client.DatasetStatus(ctx, resource); {
	case err != nil:
		return "unknown"
	case pendingDataset(state):
		return "pending"
	case state != kaggleapi.DatasetReady:
		return "unknown"
	}
	if listed, err := listedExactly(ctx, client, resource, r.Files); err != nil {
		return "unknown"
	} else if !listed {
		return "invalid"
	}
	return "ready"
}

func pendingDataset(state kaggleapi.DatasetState) bool {
	switch state {
	case "NOT_YET_PERSISTED", "BLOBS_RECEIVED", "BLOBS_DECOMPRESSED", "BLOBS_COPIED_TO_SDS", "INDIVIDUAL_BLOBS_COMPRESSED":
		return true
	}
	return false
}

// listedExactly reports whether a dataset lists exactly the expected names and sizes.
func listedExactly(ctx context.Context, client *kaggleapi.Client, resource kaggleapi.Resource, expected []stagingFile) (bool, error) {
	var total int64
	pins := make(map[string]int64, len(expected))
	for _, file := range expected {
		pins[file.Name] = file.Bytes
		total += file.Bytes
	}
	files, err := client.ListDatasetFiles(ctx, resource, len(expected), total)
	if err != nil {
		return false, err
	}
	if len(files) != len(pins) {
		return false, nil
	}
	for _, file := range files {
		if size, ok := pins[file.Name]; !ok || size != file.Bytes {
			return false, nil
		}
	}
	return true, nil
}

type stagingProgressReader struct {
	io.ReadCloser
	ctx       context.Context
	sent      *int64
	total     int64
	remaining int64
}

func (r *stagingProgressReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.ReadCloser.Read(data)
	if n > 0 {
		count := min(int64(n), r.remaining)
		r.remaining -= count
		if count > 0 {
			*r.sent += count
			provider.ReportPreparationProgress(r.ctx, *r.sent, r.total)
		}
	}
	return n, err
}

func matchingStaging(meta kaggleapi.Dataset, r stagingRequest) bool {
	return positiveDecimal(string(meta.ID)) && meta.Reference == r.Owner+"/"+r.Slug && meta.Version != nil && *meta.Version == 1 &&
		meta.Description == stagingDescription(r.MarkerSHA256) && meta.License == stagingLicense(r.License)
}

func observeStaging(ctx context.Context, client *kaggleapi.Client, plan stagingPlan, meta kaggleapi.Dataset, verifyBytes bool) stagingResponse {
	empty := func(status string) stagingResponse { return stagingResponse{Protocol: 1, Status: status} }
	r := plan.request
	resource := kaggleapi.Resource{Owner: r.Owner, Slug: r.Slug}
	if !matchingStaging(meta, r) {
		return empty("invalid")
	}
	result := stagingResponse{Protocol: 1, Status: "pending", DatasetID: string(meta.ID), Owner: r.Owner,
		Slug: r.Slug, Version: 1, MarkerSHA256: r.MarkerSHA256, Private: meta.Private != nil && *meta.Private}
	if !result.Private {
		return result
	}
	for _, entry := range plan.shared {
		switch state := observeShared(ctx, client, entry); state {
		case "ready":
		case "pending":
			return result
		case "not_found":
			return empty("invalid")
		default:
			return empty(state)
		}
	}
	state, err := client.DatasetStatus(ctx, resource)
	if err != nil {
		return empty("unknown")
	}
	switch {
	case pendingDataset(state):
		return result
	case state != kaggleapi.DatasetReady:
		return empty("unknown")
	}
	if listed, err := listedExactly(ctx, client, resource, r.Files); err != nil {
		return empty("unknown")
	} else if !listed {
		return empty("invalid")
	}
	var total int64
	for _, file := range r.Files {
		total += file.Bytes
	}
	if verifyBytes {
		for _, file := range r.Files {
			receipt, err := client.DownloadDatasetFile(ctx, resource, file.Name, file.Bytes, io.Discard)
			if err != nil {
				return empty("unknown")
			}
			if receipt.Bytes != file.Bytes || hex.EncodeToString(receipt.SHA256[:]) != string(file.SHA256) {
				return empty("invalid")
			}
		}
		final, err := client.GetDataset(ctx, resource)
		if err != nil {
			return empty("unknown")
		}
		finalState, err := client.DatasetStatus(ctx, resource)
		if err != nil {
			return empty("unknown")
		}
		if finalState != kaggleapi.DatasetReady || !matchingStaging(final, r) || final.ID != meta.ID ||
			final.Private == nil || !*final.Private || final.LastUpdated != meta.LastUpdated {
			return empty("invalid")
		}
	}
	result.Status, result.VerifiedFiles, result.VerifiedBytes = "ready", len(r.Files), total
	return result
}
