package kaggleapi

import (
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

type SaveKernelRequest struct {
	Resource     Resource
	Source       string
	Dataset      Resource
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
// script with one dataset and no TPU, competition, model or kernel sources.
// The caller must perform exact absent/identity checks and consume its durable
// one-shot permit first. A receipt never proves execution or resource readiness.
// GPU saves first recheck the free-only billing flag, as the pinned bridge does.
// An unavailable/unverified quota produces ErrQuota with Sent=false for the save.
func (c *Client) SaveKernel(ctx context.Context, request SaveKernelRequest) (KernelReceipt, error) {
	op := kernelService + "SaveKernel"
	if c == nil || c.state == nil || !request.Resource.valid() || !request.Dataset.valid() || request.Dataset.Owner != request.Resource.Owner ||
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
		request.GPU, false, request.Internet, []string{request.Dataset.reference()}, request.WallSeconds, "SAVE_AND_RUN_ALL", request.MachineShape}
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
