package kaggle

import (
	"context"
	"errors"
	"net/http"
	"path"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

func TestRuntimeObservationReadCountsRetainIdentityChecks(t *testing.T) {
	for _, logs := range []bool{false, true} {
		p, plan, saved, _, f := nativeRuntime(t, false)
		ctx := context.Background()
		prepared, err := p.Prepare(ctx, plan, "prep")
		if err != nil {
			t.Fatal(err)
		}
		*saved = prepared
		out := p.Start(ctx, prepared, nil, nil)
		if out.Status != provider.SubmissionAccepted {
			t.Fatal(out)
		}
		f.calls = nil
		if err := p.Verify(ctx, p.binding); err != nil {
			t.Fatal(err)
		}
		want := 5
		if logs {
			want = 6
			page, err := p.Logs(ctx, *out.Remote, provider.PageRequest{Limit: 1})
			if err != nil || len(page.Lines) != 1 {
				t.Fatal(page, err)
			}
		} else if _, err := p.Observe(ctx, *out.Remote); err != nil {
			t.Fatal(err)
		}
		if len(f.calls) != want || f.count("IntrospectToken") != 2 || f.count("GetKernel") != 2 ||
			f.count("GetKernelSessionStatus") != 1 || f.count("GetAcceleratorQuotaStatistics") != 0 {
			t.Fatal("observation repeated quota/metadata or dropped identity boundaries", logs, f.calls)
		}
	}
}

func TestRuntimeVerificationDoesNotCacheAccountOrGPUQuota(t *testing.T) {
	p, plan, _, _, f := nativeRuntime(t, true)
	ctx := context.Background()
	if err := p.Verify(ctx, p.binding); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Validate(ctx, plan.Job); err != nil {
		t.Fatal(err)
	}
	f.quota = `{}`
	if _, err := p.Validate(ctx, plan.Job); err == nil || f.count("GetAcceleratorQuotaStatistics") != 2 {
		t.Fatal("GPU validation reused old quota", err, f.calls)
	}
	f.owner = "replacement_user"
	if err := p.Verify(ctx, p.binding); err == nil {
		t.Fatal("verification reused old account identity")
	}
}

func TestRuntimeUnreadableExecutionStateRetainsUnknownAndReason(t *testing.T) {
	p, plan, saved, _, f := nativeRuntime(t, false)
	ctx := context.Background()
	prepared, err := p.Prepare(ctx, plan, "prep")
	if err != nil {
		t.Fatal(err)
	}
	*saved = prepared
	submitted := p.Start(ctx, prepared, nil, nil)
	if submitted.Status != provider.SubmissionAccepted {
		t.Fatal(submitted)
	}
	c, err := p.attempt(ctx, plan.Job.Identity)
	if err != nil {
		t.Fatal(err)
	}
	c.executor.run = func(ctx context.Context, _ Config, mode string, token []byte, request executionRequest) (executionResponse, error) {
		client, err := kaggleapi.New(token, kaggleapi.Options{APITransport: apiRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			if path.Base(r.URL.Path) == "GetKernelSessionStatus" {
				return jsonResponse(403, `{"private":"SYNTHETIC_TOKEN"}`), nil
			}
			return f.api(r)
		})})
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		return execute(ctx, client, mode, request), nil
	}
	seen, err := p.Observe(ctx, *submitted.Remote)
	assertStepReason(t, err, "execution_status_read_failed")
	var problem domain.Problem
	if !errors.As(err, &problem) || problem.Code != domain.CodeProviderStateUnknown || seen.RemoteActivity == domain.RemoteActivityInactive || seen.Execution == domain.ExecutionFailed {
		t.Fatal("unreadable status acquired terminal evidence", seen, err)
	}
	if retry := p.Start(ctx, prepared, nil, nil); retry.Status != provider.SubmissionUnknown || f.count("SaveKernel") != 1 {
		t.Fatal("failed observation permitted resubmission", retry)
	}
}
