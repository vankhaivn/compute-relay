package kaggleapi

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

func kernelQuery(resource Resource) any {
	return struct {
		Owner string `json:"userName"`
		Slug  string `json:"kernelSlug"`
	}{resource.Owner, resource.Slug}
}

// Presence remains observable for flags/version required by exact identity
// checks. Empty repeated source fields may be absent or null, as in ProtoJSON.
type KernelMetadata struct {
	ID                 ID       `json:"id"`
	Reference          string   `json:"ref"`
	Slug               string   `json:"slug"`
	Version            *int     `json:"currentVersionNumber"`
	Language           string   `json:"language"`
	Type               string   `json:"kernelType"`
	Private            *bool    `json:"isPrivate"`
	GPU                *bool    `json:"enableGpu"`
	TPU                *bool    `json:"enableTpu"`
	Internet           *bool    `json:"enableInternet"`
	MachineShape       string   `json:"machineShape"`
	DatasetSources     []string `json:"datasetDataSources"`
	KernelSources      []string `json:"kernelDataSources"`
	CompetitionSources []string `json:"competitionDataSources"`
	ModelSources       []string `json:"modelDataSources"`
}

type KernelBlob struct {
	Source *string `json:"source"`
}

type Kernel struct {
	Metadata *KernelMetadata `json:"metadata"`
	Blob     *KernelBlob     `json:"blob"`
}

func (c *Client) GetKernel(ctx context.Context, resource Resource) (Kernel, error) {
	op := kernelService + "GetKernel"
	if !resource.valid() {
		return Kernel{}, failure(op, ErrConfig, 0, false)
	}
	object, err := c.rpc(ctx, op, kernelQuery(resource), maxMetadataBytes)
	if err != nil {
		return Kernel{}, err
	}
	var result Kernel
	if decodeObject(object, &result) != nil || result.Metadata == nil || result.Metadata.ID == "" || result.Blob == nil || result.Blob.Source == nil {
		return Kernel{}, failure(op, ErrResponse, 0, true)
	}
	return result, nil
}

// OwnedKernelPresent scans the authenticated owner's unfiltered private/public
// inventory. Only exhausting every page establishes absence; a partial listing,
// malformed identity or repeated cursor is an error, never an empty inventory.
func (c *Client) OwnedKernelPresent(ctx context.Context, resource Resource) (bool, error) {
	op := kernelService + "ListKernels"
	if !resource.valid() {
		return false, failure(op, ErrConfig, 0, false)
	}
	seen := make(map[string]bool)
	cursors := make(map[string]bool)
	cursor := ""
	for page := 0; page < 100; page++ {
		query := struct {
			Group      string `json:"group"`
			User       string `json:"user"`
			Language   string `json:"language"`
			KernelType string `json:"kernelType"`
			OutputType string `json:"outputType"`
			SortBy     string `json:"sortBy"`
			PageSize   int    `json:"pageSize"`
			PageToken  string `json:"pageToken,omitempty"`
		}{"PROFILE", resource.Owner, "all", "all", "all", "DATE_CREATED", 100, cursor}
		object, err := c.rpc(ctx, op, query, maxMetadataBytes)
		if err != nil {
			return false, err
		}
		var result struct {
			Kernels []struct {
				Reference string `json:"ref"`
			} `json:"kernels"`
			NextPageToken string `json:"nextPageToken"`
		}
		if decodeObject(object, &result) != nil || len(result.Kernels) > 100 || len(result.NextPageToken) > 4096 {
			return false, failure(op, ErrResponse, 0, true)
		}
		if raw, exists := object["kernels"]; !exists || bytes.Equal(raw, []byte("null")) {
			return false, failure(op, ErrResponse, 0, true)
		}
		for _, kernel := range result.Kernels {
			owner, slug, ok := strings.Cut(kernel.Reference, "/")
			if !ok || owner != resource.Owner || !(Resource{Owner: owner, Slug: slug}).valid() || seen[kernel.Reference] {
				return false, failure(op, ErrResponse, 0, true)
			}
			if kernel.Reference == resource.reference() {
				return true, nil
			}
			seen[kernel.Reference] = true
		}
		if result.NextPageToken == "" {
			return false, nil
		}
		if cursors[result.NextPageToken] || len(result.Kernels) == 0 {
			return false, failure(op, ErrResponse, 0, true)
		}
		cursors[result.NextPageToken] = true
		cursor = result.NextPageToken
	}
	return false, failure(op, ErrLimit, 0, true)
}

type SaveKernelRequest struct {
	Resource     Resource
	Source       string
	Datasets     []Resource
	WallSeconds  int
	Internet     bool
	GPU          bool
	MachineShape string
}

type KernelReceipt struct {
	ID        ID
	Reference string
	Version   int
}

// SaveKernel has no ID/update path. It sends an explicit finite private Python
// script with the owner's datasets and no TPU, competition, model or kernel sources.
// The caller must perform exact absent/identity checks and consume its durable
// one-shot permit first. A receipt never proves execution or resource readiness.
// GPU saves first recheck the free-only billing flag, as the pinned bridge does.
// An unavailable/unverified quota produces ErrQuota with Sent=false for the save.
func (c *Client) SaveKernel(ctx context.Context, request SaveKernelRequest) (KernelReceipt, error) {
	op := kernelService + "SaveKernel"
	if c == nil || c.state == nil || !request.Resource.valid() || !ownedDatasets(request.Datasets, request.Resource.Owner) ||
		request.Source == "" || len(request.Source) > 2<<20 || !utf8.ValidString(request.Source) ||
		request.WallSeconds < 1 || request.WallSeconds > 86400 ||
		request.GPU && request.MachineShape != "NvidiaTeslaT4" && request.MachineShape != "NvidiaTeslaP100" ||
		!request.GPU && request.MachineShape != "" {
		return KernelReceipt{}, failure(op, ErrConfig, 0, false)
	}
	token := c.credential()
	containsCredential := len(token) > 0 && strings.Contains(request.Source, string(token))
	clear(token)
	if containsCredential {
		return KernelReceipt{}, failure(op, ErrConfig, 0, false)
	}
	if request.GPU {
		quota, err := c.AcceleratorQuota(ctx)
		if err != nil || !quota.FreeOnly {
			return KernelReceipt{}, failure(op, ErrQuota, 0, false)
		}
	}
	query := struct {
		Slug      string   `json:"slug"`
		Title     string   `json:"newTitle"`
		Text      string   `json:"text"`
		Language  string   `json:"language"`
		Type      string   `json:"kernelType"`
		Private   bool     `json:"isPrivate"`
		GPU       bool     `json:"enableGpu"`
		TPU       bool     `json:"enableTpu"`
		Internet  bool     `json:"enableInternet"`
		Datasets  []string `json:"datasetDataSources"`
		Timeout   int      `json:"sessionTimeoutSeconds"`
		Execution string   `json:"kernelExecutionType"`
		Shape     string   `json:"machineShape,omitempty"`
	}{request.Resource.reference(), request.Resource.Slug, request.Source, "python", "script", true,
		request.GPU, false, request.Internet, datasetReferences(request.Datasets), request.WallSeconds, "SAVE_AND_RUN_ALL", request.MachineShape}
	object, err := c.rpc(ctx, op, query, maxAccountBytes)
	if err != nil {
		return KernelReceipt{}, err
	}
	var receipt struct {
		ID        ID              `json:"kernelId"`
		Reference string          `json:"ref"`
		Version   int             `json:"versionNumber"`
		Error     json.RawMessage `json:"error"`
	}
	if decodeObject(object, &receipt) != nil || receipt.ID == "" || receipt.Reference != request.Resource.reference() ||
		receipt.Version != 1 || !emptyError(receipt.Error) {
		return KernelReceipt{}, failure(op, ErrReceipt, 0, true)
	}
	for _, name := range []string{"invalidTags", "invalidDatasetSources", "invalidKernelSources", "invalidCompetitionSources", "invalidModelSources"} {
		if !emptyList(object[name]) {
			return KernelReceipt{}, failure(op, ErrReceipt, 0, true)
		}
	}
	return KernelReceipt{receipt.ID, receipt.Reference, receipt.Version}, nil
}

type KernelState string

const (
	KernelUnknown            KernelState = "UNKNOWN"
	KernelQueued             KernelState = "QUEUED"
	KernelRunning            KernelState = "RUNNING"
	KernelComplete           KernelState = "COMPLETE"
	KernelError              KernelState = "ERROR"
	KernelCancelRequested    KernelState = "CANCEL_REQUESTED"
	KernelCancelAcknowledged KernelState = "CANCEL_ACKNOWLEDGED"
	KernelNewScript          KernelState = "NEW_SCRIPT"
)

// KernelSessionStatus omits versionLabel because the live API rejects it.
// Version identity must be checked separately against GetKernel before/after.
func (c *Client) KernelSessionStatus(ctx context.Context, resource Resource) (KernelState, error) {
	op := kernelService + "GetKernelSessionStatus"
	if !resource.valid() {
		return KernelUnknown, failure(op, ErrConfig, 0, false)
	}
	object, err := c.rpc(ctx, op, kernelQuery(resource), maxAccountBytes)
	if err != nil {
		return KernelUnknown, err
	}
	return KernelState(enumValue(object["status"], []string{"QUEUED", "RUNNING", "COMPLETE", "ERROR", "CANCEL_REQUESTED", "CANCEL_ACKNOWLEDGED", "NEW_SCRIPT"})), nil
}

// DeleteKernel sends once and accepts only the pinned synchronous empty response.
// Caller must first verify original ID, source, version, privacy and terminal state.
func (c *Client) DeleteKernel(ctx context.Context, resource Resource) error {
	op := kernelService + "DeleteKernel"
	if !resource.valid() {
		return failure(op, ErrConfig, 0, false)
	}
	object, err := c.rpc(ctx, op, kernelQuery(resource), maxAccountBytes)
	if err != nil {
		return err
	}
	if len(object) > 1 || !emptyError(object["errorMessage"]) {
		return failure(op, ErrReceipt, 0, true)
	}
	for key := range object {
		if key != "errorMessage" {
			return failure(op, ErrReceipt, 0, true)
		}
	}
	return nil
}

// maxKernelDatasets bounds the attempt dataset plus its shared input datasets.
const maxKernelDatasets = 8

func ownedDatasets(datasets []Resource, owner string) bool {
	if len(datasets) == 0 || len(datasets) > maxKernelDatasets {
		return false
	}
	seen := make(map[Resource]bool, len(datasets))
	for _, dataset := range datasets {
		if !dataset.valid() || dataset.Owner != owner || seen[dataset] {
			return false
		}
		seen[dataset] = true
	}
	return true
}

func datasetReferences(datasets []Resource) []string {
	references := make([]string, 0, len(datasets))
	for _, dataset := range datasets {
		references = append(references, dataset.reference())
	}
	return references
}
