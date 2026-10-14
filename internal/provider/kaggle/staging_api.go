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

func stage(ctx context.Context, client *kaggleapi.Client, mode string, plan stagingPlan, blobs StagingBlobs) (stagingResponse, error) {
	empty := func(status string) stagingResponse { return stagingResponse{Protocol: 1, Status: status} }
	if mode != "create" && mode != "observe" {
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
		return observeStaging(ctx, client, plan, meta), nil
	}
	if mode == "observe" {
		return empty("not_found"), nil
	}
	files := make([]*kaggleapi.UploadedBlob, 0, len(r.Files))
	var sent, total int64
	for _, object := range plan.objects {
		total += object.Bytes
	}
	provider.ReportPreparationProgress(ctx, 0, total)
	for index, file := range r.Files {
		ticket, err := client.StartBlobUpload(ctx, file.Name, file.Bytes)
		if err != nil {
			return empty("unknown"), nil
		}
		var source io.ReadCloser
		if index == 0 {
			source = io.NopCloser(bytes.NewReader(plan.marker))
		} else {
			object := plan.objects[index-1]
			source, err = blobs.Open(ctx, object.WorkspaceID, object.ID)
			if err != nil {
				return empty("unknown"), nil
			}
			source = &stagingProgressReader{ReadCloser: source, ctx: ctx, sent: &sent, total: total, remaining: file.Bytes}
		}
		digest, err := hex.DecodeString(string(file.SHA256))
		if err != nil || len(digest) != sha256.Size {
			_ = source.Close()
			return empty("invalid"), nil
		}
		uploaded, err := client.UploadBlob(ctx, ticket, source, [sha256.Size]byte(digest))
		if err != nil {
			return empty("unknown"), nil
		}
		files = append(files, uploaded)
	}
	// UploadedBlob exists only after complete bytes, matching SHA256, exact EOF
	// and successful source Close. No create can follow an uncertain local source.
	_, err = client.CreateDataset(ctx, kaggleapi.CreateDatasetRequest{Resource: resource, License: r.License,
		Description: stagingDescription(r.MarkerSHA256), Files: files})
	if err != nil {
		return empty("unknown"), nil
	}
	meta, err = client.GetDataset(ctx, resource)
	if err != nil {
		var apiErr *kaggleapi.Error
		if errors.As(err, &apiErr) && apiErr.StatusCode == 403 && kaggleapi.NameAbsent(err) {
			return empty("pending"), nil
		}
		return empty("unknown"), nil
	}
	return observeStaging(ctx, client, plan, meta), nil
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

func observeStaging(ctx context.Context, client *kaggleapi.Client, plan stagingPlan, meta kaggleapi.Dataset) stagingResponse {
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
	state, err := client.DatasetStatus(ctx, resource)
	if err != nil {
		return empty("unknown")
	}
	switch state {
	case "NOT_YET_PERSISTED", "BLOBS_RECEIVED", "BLOBS_DECOMPRESSED", "BLOBS_COPIED_TO_SDS", "INDIVIDUAL_BLOBS_COMPRESSED":
		return result
	case kaggleapi.DatasetReady:
	default:
		return empty("unknown")
	}
	var total int64
	expected := make(map[string]stagingFile, len(r.Files))
	for _, file := range r.Files {
		expected[file.Name] = file
		total += file.Bytes
	}
	files, err := client.ListDatasetFiles(ctx, resource, len(r.Files), total)
	if err != nil {
		return empty("unknown")
	}
	if len(files) != len(expected) {
		return empty("invalid")
	}
	for _, file := range files {
		pin, ok := expected[file.Name]
		if !ok || pin.Bytes != file.Bytes {
			return empty("invalid")
		}
	}
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
	result.Status, result.VerifiedFiles, result.VerifiedBytes = "ready", len(files), total
	return result
}
