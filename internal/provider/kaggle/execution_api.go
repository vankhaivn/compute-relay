package kaggle

import (
	"context"
	"errors"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

func runExecution(ctx context.Context, config Config, mode string, token []byte, request executionRequest) (executionResponse, error) {
	client, err := nativeClient(token, config)
	if err != nil {
		return executionResponse{}, err
	}
	defer client.Close()
	return execute(ctx, client, mode, request), nil
}

func matchingKernel(kernel kaggleapi.Kernel, request executionRequest, expectedID string) bool {
	m, blob := kernel.Metadata, kernel.Blob
	if m == nil || blob == nil || blob.Source == nil || !positiveDecimal(string(m.ID)) || expectedID != "" && string(m.ID) != expectedID ||
		m.Reference != request.Owner+"/"+request.Slug || m.Slug != request.Slug || m.Version == nil || *m.Version != 1 ||
		m.Language != "python" || m.Type != "script" || m.Private == nil || !*m.Private ||
		m.GPU == nil || *m.GPU != request.GPU || m.TPU == nil || *m.TPU || m.Internet == nil || *m.Internet != request.Internet ||
		!sameDatasets(m.DatasetSources, request.Datasets) ||
		len(m.KernelSources) != 0 || len(m.CompetitionSources) != 0 || len(m.ModelSources) != 0 ||
		request.GPU && m.MachineShape != request.MachineShape {
		return false
	}
	return provider.Digest([]byte(*blob.Source)) == request.SourceSHA256
}

// sameDatasets compares mounted dataset references as a set; Kaggle does not promise order.
func sameDatasets(mounted, expected []string) bool {
	if len(expected) == 0 || len(mounted) != len(expected) {
		return false
	}
	want := make(map[string]bool, len(expected))
	for _, name := range expected {
		want[name] = true
	}
	for _, name := range mounted {
		if !want[name] {
			return false
		}
		delete(want, name)
	}
	return len(want) == 0
}

func execute(ctx context.Context, client *kaggleapi.Client, mode string, request executionRequest) executionResponse {
	empty := func(status string) executionResponse { return executionResponse{Protocol: 1, Status: status} }
	reject := func(reason string) executionResponse {
		if mode != "submit" {
			return executionResponse{Protocol: 1, Status: "unknown", Reason: reason}
		}
		return executionResponse{Protocol: 1, Status: "rejected", Reason: reason}
	}
	// rejectRead refuses after a failed read; before SaveKernel it also keeps the read's cause.
	rejectRead := func(err error, fallback string) executionResponse {
		r := reject(executionReadReason(err, fallback, mode))
		if mode == "submit" {
			r.Cause = executionReadReason(err, "", "observe")
		}
		return r
	}
	if mode != "submit" && mode != "observe" && mode != "reconcile" {
		return empty("unknown")
	}
	account, err := client.IntrospectToken(ctx)
	if err != nil {
		return rejectRead(err, "auth_read_failed")
	}
	if !account.Active {
		return reject("token_inactive")
	}
	if account.Username != request.Owner {
		return reject("account_mismatch")
	}
	resource := kaggleapi.Resource{Owner: request.Owner, Slug: request.Slug}
	kernel, err := client.GetKernel(ctx, resource)
	id := request.KernelID
	if err != nil {
		if mode == "reconcile" {
			present, listErr := client.OwnedKernelPresent(ctx, resource)
			if listErr != nil {
				return rejectRead(listErr, "kernel_read_failed")
			}
			if !present {
				return empty("absent")
			}
			// The inventory found the exact name. A private-name lookup can lag;
			// keep observing it, never turn a failed identity read into absence.
			kernel, err = client.GetKernel(ctx, resource)
			if err != nil {
				return rejectRead(err, "kernel_read_failed")
			}
		}
	}
	if err != nil {
		if !kaggleapi.NameAbsent(err) {
			return rejectRead(err, "kernel_read_failed")
		}
		if mode != "submit" {
			return executionResponse{Protocol: 1, Status: "not_found", Reason: executionReadReason(err, "kernel_read_failed", mode)}
		}
		datasets := make([]kaggleapi.Resource, 0, len(request.Datasets))
		for _, dataset := range request.Datasets {
			owner, slug, ok := strings.Cut(dataset, "/")
			if !ok {
				return reject("helper_failed")
			}
			datasets = append(datasets, kaggleapi.Resource{Owner: owner, Slug: slug})
		}
		receipt, err := client.SaveKernel(ctx, kaggleapi.SaveKernelRequest{Resource: resource, Source: request.Source,
			Datasets: datasets, WallSeconds: int(request.WallSeconds),
			Internet: request.Internet, GPU: request.GPU, MachineShape: request.MachineShape})
		if err != nil {
			var apiErr *kaggleapi.Error
			if errors.As(err, &apiErr) && !apiErr.Sent {
				if errors.Is(err, kaggleapi.ErrQuota) {
					return reject("gpu_quota_unverified")
				}
				return reject("helper_failed")
			}
			return submissionFailure(err)
		}
		id = string(receipt.ID)
		// Submission needs new identity proof; an existing resource already has
		// the pre-status proof from the first GetKernel above.
		kernel, err = client.GetKernel(ctx, resource)
		if err != nil {
			return submissionFailure(err)
		}
	}
	if !matchingKernel(kernel, request, id) {
		return empty("invalid")
	}
	id = string(kernel.Metadata.ID)
	state, err := client.KernelSessionStatus(ctx, resource)
	stateReason := ""
	if err != nil {
		state = kaggleapi.KernelUnknown
		stateReason = "execution_status_read_failed"
	}
	kernel, err = client.GetKernel(ctx, resource)
	if err != nil {
		if mode == "submit" {
			return submissionFailure(err)
		}
		return rejectRead(err, "kernel_read_failed")
	}
	if !matchingKernel(kernel, request, id) {
		return empty("invalid")
	}
	return executionResponse{Protocol: 1, Status: "found", KernelID: id, Reference: resource.Owner + "/" + resource.Slug,
		Version: 1, SourceSHA256: request.SourceSHA256, RawState: string(state), StateReason: stateReason}
}

// A name miss is not a historical non-acceptance receipt. In particular Kaggle
// uses a permission-denied response for some missing/private names. Keep that
// distinction visible without exposing provider response bodies.
func executionReadReason(err error, fallback, mode string) string {
	if mode == "submit" {
		return fallback
	}
	var apiErr *kaggleapi.Error
	if errors.As(err, &apiErr) {
		switch apiErr.StatusCode {
		case 401:
			return "lookup_authentication_failed"
		case 403:
			return "lookup_access_denied"
		case 404:
			if fallback == "kernel_read_failed" {
				return "kernel_not_visible"
			}
		case 429:
			return "lookup_rate_limited"
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "lookup_timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "lookup_interrupted"
	}
	if errors.Is(err, kaggleapi.ErrResponse) || errors.Is(err, kaggleapi.ErrLimit) {
		return "lookup_response_invalid"
	}
	return fallback
}

func submissionFailure(err error) executionResponse {
	r := executionResponse{Protocol: 1, Status: "unknown", ErrorType: "unavailable", FailureOperation: "unknown"}
	var apiErr *kaggleapi.Error
	if errors.As(err, &apiErr) {
		r.HTTPStatus = apiErr.StatusCode
		switch apiErr.Operation {
		case "kernels.KernelsApiService/SaveKernel":
			r.FailureOperation = "save_kernel"
		case "kernels.KernelsApiService/GetKernel":
			r.FailureOperation = "verify_kernel"
		}
		if validSubmissionErrorType(apiErr.ProviderType) {
			r.ErrorType = apiErr.ProviderType
		} else if errors.Is(err, kaggleapi.ErrHTTP) {
			r.ErrorType = "http_error"
		}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		r.ErrorType = "timeout"
	case errors.Is(err, context.Canceled):
		r.ErrorType = "interrupted"
	case errors.Is(err, kaggleapi.ErrTransport):
		r.ErrorType = "transport"
	case errors.Is(err, kaggleapi.ErrResponse), errors.Is(err, kaggleapi.ErrReceipt), errors.Is(err, kaggleapi.ErrLimit):
		r.ErrorType = "invalid_response"
	}
	return r
}

func validSubmissionErrorType(value string) bool {
	switch value {
	case "unavailable", "http_error", "timeout", "interrupted", "transport", "invalid_response",
		"PERMISSION_DENIED", "UNAUTHENTICATED", "RESOURCE_EXHAUSTED", "UNAVAILABLE", "DEADLINE_EXCEEDED", "INVALID_ARGUMENT", "INTERNAL", "NOT_FOUND", "ALREADY_EXISTS":
		return true
	}
	return false
}
