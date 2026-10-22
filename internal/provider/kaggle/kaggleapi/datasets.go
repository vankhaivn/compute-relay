package kaggleapi

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"
)

// Staging holds a marker of at most 2 MiB, the bundle and up to 256 inputs.
const MaxStagingBytes int64 = (4 << 30) + (100 << 20) + (2 << 20)
const MaxStagingFiles = 258

const datasetFilePageSize = 200

// Bound metadata work even when a provider returns undersized pages.
const maxDatasetFilePages = 16

// Resource names are not numeric identity. Get responses must still be compared
// to the immutable ID/version/privacy evidence saved for the original attempt.
type Resource struct {
	Owner string
	Slug  string
}

var resourceSlug = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,99}$`)

func (r Resource) valid() bool {
	return accountName.MatchString(r.Owner) && resourceSlug.MatchString(r.Slug)
}

func (r Resource) reference() string { return r.Owner + "/" + r.Slug }

func datasetQuery(r Resource) any {
	return struct {
		Owner string `json:"ownerSlug"`
		Slug  string `json:"datasetSlug"`
	}{r.Owner, r.Slug}
}

type Dataset struct {
	ID          ID     `json:"id"`
	Reference   string `json:"ref"`
	Version     *int   `json:"currentVersionNumber"`
	Private     *bool  `json:"isPrivate"`
	Description string `json:"description"`
	License     string `json:"licenseName"`
	LastUpdated string `json:"lastUpdated"`
}

func (c *Client) GetDataset(ctx context.Context, resource Resource) (Dataset, error) {
	op := datasetService + "GetDataset"
	if !resource.valid() {
		return Dataset{}, failure(op, ErrConfig, 0, false)
	}
	object, err := c.rpc(ctx, op, datasetQuery(resource), maxMetadataBytes)
	if err != nil {
		return Dataset{}, err
	}
	var result Dataset
	if decodeObject(object, &result) != nil || result.ID == "" || result.Reference == "" {
		return Dataset{}, failure(op, ErrResponse, 0, true)
	}
	return result, nil
}

type DatasetState string

const (
	DatasetUnknown DatasetState = "UNKNOWN"
	DatasetReady   DatasetState = "READY"
)

func (c *Client) DatasetStatus(ctx context.Context, resource Resource) (DatasetState, error) {
	op := datasetService + "GetDatasetStatus"
	if !resource.valid() {
		return DatasetUnknown, failure(op, ErrConfig, 0, false)
	}
	object, err := c.rpc(ctx, op, datasetQuery(resource), maxAccountBytes)
	if err != nil {
		return DatasetUnknown, err
	}
	state := enumValue(object["status"], []string{"NOT_YET_PERSISTED", "BLOBS_RECEIVED", "BLOBS_DECOMPRESSED",
		"BLOBS_COPIED_TO_SDS", "INDIVIDUAL_BLOBS_COMPRESSED", "READY", "FAILED", "DELETED", "REPROCESSING"})
	return DatasetState(state), nil
}

type DatasetFile struct {
	Name  string `json:"name"`
	Bytes int64  `json:"totalBytes"`
}

func (f *DatasetFile) UnmarshalJSON(raw []byte) error {
	type plain DatasetFile
	var result plain
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &result) != nil || json.Unmarshal(raw, &fields) != nil ||
		bytes.Equal(fields["totalBytes"], []byte("null")) {
		return ErrResponse
	}
	*f = DatasetFile(result)
	return nil
}

// ListDatasetFiles returns the complete version-1 staging list, never a partial
// page. Count, aggregate bytes, duplicate names and repeating cursors are bounded.
func (c *Client) ListDatasetFiles(parent context.Context, resource Resource, maxFiles int, maxBytes int64) ([]DatasetFile, error) {
	op := datasetService + "ListDatasetFiles"
	if c == nil || c.state == nil || !resource.valid() || maxFiles < 1 || maxFiles > MaxStagingFiles || maxBytes < 0 || maxBytes > MaxStagingBytes {
		return nil, failure(op, ErrConfig, 0, false)
	}
	ctx, cancel := context.WithTimeout(parent, c.state.transfer)
	defer cancel()
	files := make([]DatasetFile, 0)
	seen, cursors := make(map[string]bool), make(map[string]bool)
	cursor := ""
	var total int64
	for page := 0; page < maxDatasetFilePages; page++ {
		query := struct {
			Owner   string `json:"ownerSlug"`
			Slug    string `json:"datasetSlug"`
			Version int    `json:"datasetVersionNumber"`
			Size    int    `json:"pageSize"`
			Cursor  string `json:"pageToken,omitempty"`
		}{resource.Owner, resource.Slug, 1, datasetFilePageSize, cursor}
		object, err := c.rpc(ctx, op, query, maxMetadataBytes)
		if err != nil {
			return nil, err
		}
		var response struct {
			Files  []DatasetFile   `json:"datasetFiles"`
			Error  json.RawMessage `json:"errorMessage"`
			Cursor string          `json:"nextPageToken"`
		}
		if decodeObject(object, &response) != nil || len(response.Files) > datasetFilePageSize || !emptyError(response.Error) {
			return nil, failure(op, ErrResponse, 0, true)
		}
		if raw, exists := object["datasetFiles"]; !exists || bytes.Equal(raw, []byte("null")) {
			return nil, failure(op, ErrResponse, 0, true)
		}
		for _, file := range response.Files {
			if !safeFilePath(file.Name) || seen[file.Name] || file.Bytes < 0 || file.Bytes > maxBytes-total || len(files) >= maxFiles {
				return nil, failure(op, ErrResponse, 0, true)
			}
			seen[file.Name], total = true, total+file.Bytes
			files = append(files, file)
		}
		if response.Cursor == "" {
			return files, nil
		}
		if len(response.Files) == 0 || len(files) >= maxFiles || len(response.Cursor) > 2048 || cursors[response.Cursor] {
			return nil, failure(op, ErrResponse, 0, true)
		}
		cursors[response.Cursor], cursor = true, response.Cursor
	}
	return nil, failure(op, ErrLimit, 0, true)
}

type CreateDatasetRequest struct {
	Resource    Resource
	License     string
	Description string
	Files       []*UploadedBlob
}

// CreateDataset always creates private staging and has no update/version path.
// Its returned reference is acknowledgement data, not readiness evidence.
type DatasetReceipt struct {
	Reference string
}

func (c *Client) CreateDataset(ctx context.Context, request CreateDatasetRequest) (DatasetReceipt, error) {
	op := datasetService + "CreateDataset"
	if c == nil || c.state == nil || !request.Resource.valid() || len(request.Description) > 64<<10 ||
		request.License != "other" && request.License != "unknown" && request.License != "copyright-authors" ||
		len(request.Files) < 2 || len(request.Files) > MaxStagingFiles {
		return DatasetReceipt{}, failure(op, ErrConfig, 0, false)
	}
	files := make([]struct {
		Token string `json:"token"`
	}, 0, len(request.Files))
	seen := make(map[string]bool)
	var total int64
	for _, file := range request.Files {
		if file == nil || file.client != c || len(file.token) == 0 || seen[file.name] || file.size > MaxStagingBytes-total {
			return DatasetReceipt{}, failure(op, ErrConfig, 0, false)
		}
		seen[file.name], total = true, total+file.size
		files = append(files, struct {
			Token string `json:"token"`
		}{string(file.token)})
	}
	query := struct {
		Owner       string `json:"ownerSlug"`
		Slug        string `json:"slug"`
		Title       string `json:"title"`
		License     string `json:"licenseName"`
		Description string `json:"description"`
		Private     bool   `json:"isPrivate"`
		Files       any    `json:"files"`
	}{request.Resource.Owner, request.Resource.Slug, request.Resource.Slug, request.License, request.Description, true, files}
	object, err := c.rpc(ctx, op, query, maxAccountBytes)
	if err != nil {
		return DatasetReceipt{}, err
	}
	var receipt struct {
		Reference   string          `json:"ref"`
		Error       json.RawMessage `json:"error"`
		InvalidTags json.RawMessage `json:"invalidTags"`
	}
	if decodeObject(object, &receipt) != nil || !emptyError(receipt.Error) || !emptyList(receipt.InvalidTags) ||
		receipt.Reference != "" && receipt.Reference != request.Resource.reference() {
		return DatasetReceipt{}, failure(op, ErrReceipt, 0, true)
	}
	return DatasetReceipt{receipt.Reference}, nil
}

// DeleteDataset issues one name-based delete. Caller owns exact ID/version/private
// checks and the durable mutation permit. A failed call must not be repeated.
func (c *Client) DeleteDataset(ctx context.Context, resource Resource) error {
	op := datasetService + "DeleteDataset"
	if !resource.valid() {
		return failure(op, ErrConfig, 0, false)
	}
	object, err := c.rpc(ctx, op, datasetQuery(resource), maxAccountBytes)
	if err != nil {
		return err
	}
	if len(object) > 1 || !emptyError(object["error"]) {
		return failure(op, ErrReceipt, 0, true)
	}
	for key := range object {
		if key != "error" {
			return failure(op, ErrReceipt, 0, true)
		}
	}
	return nil
}

func emptyError(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(raw, []byte("null")) || bytes.Equal(raw, []byte(`""`))
}

func emptyList(raw json.RawMessage) bool {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return true
	}
	var items []json.RawMessage
	return json.Unmarshal(raw, &items) == nil && items != nil && len(items) == 0
}

// Keep the bridge's portable path restrictions at the data-plane boundary.
func safeFilePath(path string) bool {
	if len(path) < 1 || len(path) > 512 || strings.ContainsAny(path, `\:%<>"|?*`) {
		return false
	}
	for _, b := range []byte(path) {
		if b < 32 || b > 126 {
			return false
		}
	}
	for _, part := range strings.Split(path, "/") {
		if part == "" || len(part) > 255 || part == "." || part == ".." || strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
			return false
		}
		stem := strings.ToUpper(strings.SplitN(part, ".", 2)[0])
		if stem == "CON" || stem == "PRN" || stem == "AUX" || stem == "NUL" ||
			len(stem) == 4 && (strings.HasPrefix(stem, "COM") || strings.HasPrefix(stem, "LPT")) && stem[3] >= '0' && stem[3] <= '9' {
			return false
		}
	}
	return true
}

func enumValue(raw json.RawMessage, names []string) string {
	var index int
	if len(raw) > 0 && raw[0] >= '0' && raw[0] <= '9' && json.Unmarshal(raw, &index) == nil && index >= 0 && index < len(names) {
		return names[index]
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		text = strings.ToUpper(text)
		for _, name := range names {
			if text == name {
				return name
			}
		}
	}
	return "UNKNOWN"
}
