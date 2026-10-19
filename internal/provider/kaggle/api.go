package kaggle

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

// Every credential resolution owns one short-lived native client. Its private
// credential copy is erased before the resolver releases the original bytes.
func nativeClient(token []byte, config Config) (*kaggleapi.Client, error) {
	// A 2 GiB input needs almost 29 minutes at the staging policy's 10 Mbit/s
	// reference rate. The enclosing operation still owns the complete budget.
	return kaggleapi.New(token, kaggleapi.Options{TransferTimeout: 30 * time.Minute, Pacing: config.Pacing, AccountScope: config.AccountName})
}

func accountMatches(ctx context.Context, client *kaggleapi.Client, owner string) (bool, error) {
	account, err := client.IntrospectToken(ctx)
	return account.Active && account.Username == owner, err
}

func quotaResponse(ctx context.Context, client *kaggleapi.Client) monitorResponse {
	result := monitorResponse{Protocol: 1, Status: "unavailable", Reason: "read_unavailable"}
	quota, err := client.AcceleratorQuota(ctx)
	if err != nil {
		return result
	}
	if !quota.Known {
		result.Status, result.Reason = "unknown", quota.Reason
		return result
	}
	result.Status, result.Reason = "known", "none"
	result.LimitNS = strconv.FormatInt(int64(quota.Limit), 10)
	result.UsedNS = strconv.FormatInt(int64(quota.Used), 10)
	result.ReservedNS = strconv.FormatInt(int64(quota.Reserved), 10)
	return result
}

func runDiscovery(ctx context.Context, token []byte, pacing *kaggleapi.RequestPacer) (discoveryResponse, error) {
	client, err := nativeClient(token, Config{Pacing: pacing})
	if err != nil {
		return discoveryResponse{}, err
	}
	defer client.Close()
	return discover(ctx, client), nil
}

func discover(ctx context.Context, client *kaggleapi.Client) discoveryResponse {
	result := discoveryResponse{Protocol: 1, Status: "unavailable"}
	account, err := client.IntrospectToken(ctx)
	if err != nil {
		var apiErr *kaggleapi.Error
		if errors.As(err, &apiErr) && apiErr.StatusCode == 401 {
			result.Status = "credential_rejected"
		} else if errors.Is(err, kaggleapi.ErrResponse) {
			result.Status = "invalid_response"
		}
		return result
	}
	if !account.Active {
		result.Status = "credential_rejected"
		return result
	}
	quota := quotaResponse(ctx, client)
	result.Status, result.Account = "verified", account.Username
	result.QuotaStatus, result.QuotaReason = quota.Status, quota.Reason
	result.LimitNS, result.UsedNS, result.ReservedNS = quota.LimitNS, quota.UsedNS, quota.ReservedNS
	return result
}

func runPreflight(ctx context.Context, config Config, mode Mode, token []byte, checkQuota bool) (Report, error) {
	if mode == Local {
		return baseline(Local), nil
	}
	if mode != ReadOnly {
		return Report{}, ErrMode
	}
	client, err := nativeClient(token, config)
	if err != nil {
		return Report{}, err
	}
	defer client.Close()
	return preflight(ctx, client, config.AccountName, checkQuota), nil
}

func preflight(ctx context.Context, client *kaggleapi.Client, owner string, checkQuota bool) Report {
	r := baseline(ReadOnly)
	account, err := client.IntrospectToken(ctx)
	if err != nil {
		r.Authentication, r.Problem = "unavailable", "provider_unavailable"
		var apiErr *kaggleapi.Error
		if errors.As(err, &apiErr) {
			if apiErr.StatusCode == 401 {
				r.Authentication, r.Problem = "failed", "credential_rejected"
			} else if apiErr.StatusCode == 403 {
				r.Problem = "access_denied"
			} else if errors.Is(err, kaggleapi.ErrResponse) {
				r.Problem = "invalid_response"
			}
		}
		return r
	}
	if !account.Active {
		r.Authentication, r.Problem = "failed", "credential_rejected"
		return r
	}
	r.Authentication = "verified"
	if account.Username != owner {
		r.AccountBinding, r.Problem = "mismatch", "account_mismatch"
		return r
	}
	r.AccountBinding = "matched"
	if !checkQuota {
		return r
	}
	quota := quotaResponse(ctx, client)
	if quota.Status == "unavailable" {
		r.Quota, r.Problem = "unavailable", "quota_unavailable"
	} else if quota.Status == "known" {
		r.Quota = "available"
	} else {
		r.Quota = "unknown"
	}
	return r
}
