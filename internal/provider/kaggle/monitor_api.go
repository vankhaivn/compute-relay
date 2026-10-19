package kaggle

import (
	"context"
	"encoding/base64"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

func runMonitor(ctx context.Context, config Config, mode string, token []byte, request monitorRequest) (monitorResponse, error) {
	client, err := nativeClient(token, config)
	if err != nil {
		return monitorResponse{}, err
	}
	defer client.Close()
	return monitor(ctx, client, mode, request), nil
}

func monitor(ctx context.Context, client *kaggleapi.Client, mode string, request monitorRequest) monitorResponse {
	empty := func(status, reason string) monitorResponse {
		return monitorResponse{Protocol: 1, Status: status, Reason: reason}
	}
	matched, err := accountMatches(ctx, client, request.Owner)
	if err != nil || !matched {
		reason := "read_unavailable"
		if mode == "logs" {
			reason = "auth_read_failed"
		}
		return empty("unavailable", reason)
	}
	if mode == "quota" {
		return quotaResponse(ctx, client)
	}
	if mode != "logs" || request.Execution == nil {
		return empty("unavailable", "read_unavailable")
	}
	r := request.Execution
	if r.Owner != request.Owner {
		return empty("invalid", "identity_mismatch")
	}
	resource := kaggleapi.Resource{Owner: r.Owner, Slug: r.Slug}
	check := func() monitorResponse {
		kernel, err := client.GetKernel(ctx, resource)
		if err != nil {
			return empty("unavailable", "kernel_read_failed")
		}
		if !matchingKernel(kernel, *r, r.KernelID) {
			return empty("invalid", "identity_mismatch")
		}
		return monitorResponse{}
	}
	if response := check(); response.Protocol != 0 {
		return response
	}
	state, err := client.KernelSessionStatus(ctx, resource)
	if err != nil {
		state = kaggleapi.KernelUnknown
	}
	replay, err := client.ReadLogs(ctx, resource, state, kaggleapi.LogRequest{Offset: int64(request.LogOffset), Prefix: request.LogPrefix, LineLimit: request.LogLimit})
	if err != nil {
		if errors.Is(err, kaggleapi.ErrLogChanged) {
			return empty("reset", "log_changed")
		}
		reason := "read_unavailable"
		if errors.Is(err, kaggleapi.ErrResponse) || errors.Is(err, kaggleapi.ErrLimit) {
			reason = "stream_format_invalid"
		} else if errors.Is(err, context.DeadlineExceeded) {
			reason = "stream_timeout"
		}
		return empty("unavailable", reason)
	}
	if response := check(); response.Protocol != 0 {
		return response
	}
	return monitorResponse{Protocol: 1, Status: "logs", Reason: "none", TextB64: base64.StdEncoding.EncodeToString(replay.Text),
		Availability: replay.Availability, Truncated: replay.Truncated, Replay: true, Offset: int(replay.Offset), Prefix: replay.Prefix}
}
