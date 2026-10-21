package kaggle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

func runStaging(ctx context.Context, config Config, mode string, token []byte, plan stagingPlan, blobs StagingBlobs) (stagingResponse, error) {
	client, err := nativeClient(token, config)
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

// stage creates or observes one attempt's staging. Every mode checks metadata only: each
// upload was SHA-checked against its frozen digest, the marker description pins this
// preparation, and the remote runner rehashes every input before use. "create" and
// "observe" also confirm the dataset did not change while it was read.
func stage(ctx context.Context, client *kaggleapi.Client, mode string, plan stagingPlan, blobs StagingBlobs) (stagingResponse, error) {
	empty := func(status string) stagingResponse { return stagingResponse{Protocol: 1, Status: status} }
	failed := func(status, reason string) stagingResponse {
		return stagingResponse{Protocol: 1, Status: status, Reason: reason}
	}
	if mode != "create" && mode != "observe" && mode != "recheck" && mode != "cleanup_observe" {
		return stagingResponse{}, ErrMode
	}
	r := plan.request
	resource := kaggleapi.Resource{Owner: r.Owner, Slug: r.Slug}
	matched, err := accountMatches(ctx, client, r.Owner)
	if err != nil {
		return failed("unknown", "account_check_failed"+failureSuffix(err)), nil
	}
	if !matched {
		return empty("invalid"), nil
	}
	meta, err := client.GetDataset(ctx, resource)
	if err != nil && !kaggleapi.NameAbsent(err) {
		return failed("unknown", "staging_lookup_failed"+failureSuffix(err)), nil
	}
	if err == nil {
		return observeStaging(ctx, client, plan, meta, mode == "observe" || mode == "create", mode == "cleanup_observe"), nil
	}
	if mode != "create" {
		// Missing-name 403 is useful before creation, but proves no absence for cleanup.
		if mode == "cleanup_observe" {
			var failure *kaggleapi.Error
			if !errors.As(err, &failure) || failure.StatusCode != 404 {
				return empty("unknown"), nil
			}
		}
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
			return failed(state, "shared_"+state), nil
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
		files, status, reason := uploadStaging(ctx, client, "shared", entry.request, entry.marker, []domain.ObjectMetadata{entry.object}, blobs, progress)
		if status != "" {
			return failed(status, reason), nil
		}
		// A lost acknowledgement is resolved by observing the exact shared dataset; it
		// is never created again.
		if err := createDataset(ctx, client, entry.request, files); err != nil && !sharedCreated(ctx, client, entry) {
			return failed("unknown", "shared_create_failed"+failureSuffix(err)), nil
		}
	}
	files, status, reason := uploadStaging(ctx, client, "staging", r, plan.marker, plan.objects, blobs, progress)
	if status != "" {
		return failed(status, reason), nil
	}
	if err := createDataset(ctx, client, r, files); err != nil {
		return failed("pending", "staging_create_failed"+failureSuffix(err)), nil
	}
	meta, err = client.GetDataset(ctx, resource)
	if err != nil {
		return failed("pending", "staging_lookup_failed"+failureSuffix(err)), nil
	}
	return observeStaging(ctx, client, plan, meta, true, false), nil
}

type uploadProgress struct {
	sent, total int64
}

// stagingUploadTries bounds sends of one file. A failed upload creates no dataset and
// its blob is never referenced, so the file is sent again with a new ticket.
const stagingUploadTries = 5

var stagingUploadRetryDelay = 15 * time.Second

// uploadStaging uploads a marker and its objects (same order as Files[1:]). It returns
// a non-empty status and closed reason when an upload did not complete.
func uploadStaging(ctx context.Context, client *kaggleapi.Client, kind string, r stagingRequest, marker []byte, objects []domain.ObjectMetadata, blobs StagingBlobs, progress *uploadProgress) ([]*kaggleapi.UploadedBlob, string, string) {
	files := make([]*kaggleapi.UploadedBlob, 0, len(r.Files))
	for index, file := range r.Files {
		digest, err := hex.DecodeString(string(file.SHA256))
		if err != nil || len(digest) != sha256.Size {
			return nil, "invalid", kind + "_file_invalid"
		}
		open := func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(marker)), nil }
		if index > 0 {
			object := objects[index-1]
			open = func() (io.ReadCloser, error) {
				source, err := blobs.Open(ctx, object.WorkspaceID, object.ID)
				if err != nil {
					return nil, err
				}
				return &stagingProgressReader{ReadCloser: source, ctx: ctx, sent: &progress.sent, total: progress.total, remaining: file.Bytes}, nil
			}
		}
		sent := progress.sent
		var uploaded *kaggleapi.UploadedBlob
		reason := ""
		for try := 0; try < stagingUploadTries && uploaded == nil && ctx.Err() == nil; try++ {
			if try > 0 {
				timer := time.NewTimer(stagingUploadRetryDelay << (try - 1))
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, "unknown", kind + "_upload_failed" + failureSuffix(ctx.Err())
				case <-timer.C:
				}
			}
			progress.sent = sent
			ticket, err := client.StartBlobUpload(ctx, file.Name, file.Bytes)
			if err != nil {
				reason = kind + "_upload_start_failed" + failureSuffix(err)
				if !retryableStagingUpload(err) {
					break
				}
				continue
			}
			source, err := open()
			if err != nil {
				return nil, "unknown", kind + "_source_unavailable"
			}
			// UploadedBlob exists only after complete bytes, matching SHA256, exact EOF
			// and successful source Close. No create can follow an uncertain local source.
			if uploaded, err = client.UploadBlob(ctx, ticket, source, [sha256.Size]byte(digest)); err != nil {
				reason = kind + "_upload_failed" + failureSuffix(err)
				if !retryableStagingUpload(err) {
					break
				}
			}
		}
		if uploaded == nil {
			if reason == "" {
				reason = kind + "_upload_failed" + failureSuffix(ctx.Err())
			}
			return nil, "unknown", reason
		}
		files = append(files, uploaded)
	}
	return files, "", ""
}

// createDataset creates one private dataset from uploaded files.
func createDataset(ctx context.Context, client *kaggleapi.Client, r stagingRequest, files []*kaggleapi.UploadedBlob) error {
	_, err := client.CreateDataset(ctx, kaggleapi.CreateDatasetRequest{Resource: kaggleapi.Resource{Owner: r.Owner, Slug: r.Slug},
		License: r.License, Description: stagingDescription(r.MarkerSHA256), Files: files})
	return err
}

// sharedCreated observes, within the visibility grace, whether a shared dataset whose
// create was not acknowledged exists with its exact identity.
func sharedCreated(ctx context.Context, client *kaggleapi.Client, entry sharedInput) bool {
	deadline := time.Now().Add(stagingVisibilityGrace)
	for {
		switch observeShared(ctx, client, entry) {
		case "ready", "pending":
			return true
		case "invalid":
			return false
		}
		if !time.Now().Before(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(stagingVisibilityPoll):
		}
	}
}

// failureSuffix adds the provider HTTP status or a timeout to a closed reason; it
// never includes provider, URL or OS text.
func failureSuffix(err error) string {
	var apiErr *kaggleapi.Error
	switch {
	case errors.As(err, &apiErr) && apiErr.StatusCode >= 100 && apiErr.StatusCode <= 599:
		return "_http_" + strconv.Itoa(apiErr.StatusCode)
	case errors.Is(err, context.DeadlineExceeded):
		return "_timeout"
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

// observeStaging names the step that left a result unknown, so a failed create read-back
// reports it.
func observeStaging(ctx context.Context, client *kaggleapi.Client, plan stagingPlan, meta kaggleapi.Dataset, confirmStable, cleanupOnly bool) stagingResponse {
	empty := func(status string) stagingResponse { return stagingResponse{Protocol: 1, Status: status} }
	unknown := func(reason string) stagingResponse {
		return stagingResponse{Protocol: 1, Status: "unknown", Reason: reason}
	}
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
	if cleanupOnly {
		// Cleanup needs ownership, not executable readiness. Shared datasets may
		// already be absent; they are independent resources and never cleanup targets.
		final, err := client.GetDataset(ctx, resource)
		if err != nil {
			return unknown("staging_readback_failed" + failureSuffix(err))
		}
		if !matchingStaging(final, r) || final.ID != meta.ID || final.Private == nil || !*final.Private || final.LastUpdated != meta.LastUpdated {
			return empty("invalid")
		}
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
			return stagingResponse{Protocol: 1, Status: state, Reason: "shared_" + state}
		}
	}
	state, err := client.DatasetStatus(ctx, resource)
	if err != nil {
		return unknown("staging_status_failed" + failureSuffix(err))
	}
	switch {
	case pendingDataset(state):
		return result
	case state != kaggleapi.DatasetReady:
		return unknown("staging_state_unknown")
	}
	if listed, err := listedExactly(ctx, client, resource, r.Files); err != nil {
		return unknown("staging_list_failed" + failureSuffix(err))
	} else if !listed {
		return empty("invalid")
	}
	var total int64
	for _, file := range r.Files {
		total += file.Bytes
	}
	if confirmStable {
		final, err := client.GetDataset(ctx, resource)
		if err != nil {
			return unknown("staging_readback_failed" + failureSuffix(err))
		}
		finalState, err := client.DatasetStatus(ctx, resource)
		if err != nil {
			return unknown("staging_readback_failed" + failureSuffix(err))
		}
		if finalState != kaggleapi.DatasetReady || !matchingStaging(final, r) || final.ID != meta.ID ||
			final.Private == nil || !*final.Private || final.LastUpdated != meta.LastUpdated {
			return empty("invalid")
		}
	}
	result.Status, result.VerifiedFiles, result.VerifiedBytes = "ready", len(r.Files), total
	return result
}

func retryableStagingUpload(err error) bool {
	var apiErr *kaggleapi.Error
	return errors.Is(err, kaggleapi.ErrTransport) || errors.Is(err, kaggleapi.ErrReceipt) || errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &apiErr) && (apiErr.StatusCode == 429 || apiErr.StatusCode >= 500 && apiErr.StatusCode <= 599)
}
