package kaggle

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestMonitorProtoJSONQuotaDefaultReachesGoWithoutInventingAllowance(t *testing.T) {
	const complete = `{"gpuQuota":{"totalTimeAllowed":"108000s","timeUsed":"0s","timeReserved":"0s"}}`
	for _, tc := range []struct {
		name      string
		wire      string
		status    string
		remaining float64
		eligible  bool
		wantError bool
	}{
		{"omitted-false", complete, "known", 108000, true, false},
		{"explicit-false", strings.Replace(complete, `"totalTimeAllowed"`, `"isPayToScaleEnabled":false,"totalTimeAllowed"`, 1), "known", 108000, true, false},
		{"paid", strings.Replace(complete, `"totalTimeAllowed"`, `"isPayToScaleEnabled":true,"totalTimeAllowed"`, 1), "unknown", 0, false, false},
		{"null-flag", strings.Replace(complete, `"totalTimeAllowed"`, `"isPayToScaleEnabled":null,"totalTimeAllowed"`, 1), "unknown", 0, false, false},
		{"numeric-false", strings.Replace(complete, `"totalTimeAllowed"`, `"isPayToScaleEnabled":0,"totalTimeAllowed"`, 1), "unknown", 0, false, false},
		{"missing-reservation", strings.Replace(complete, `,"timeReserved":"0s"`, "", 1), "unknown", 0, false, false},
		{"null-reservation", strings.Replace(complete, `"timeReserved":"0s"`, `"timeReserved":null`, 1), "unknown", 0, false, false},
		{"missing-quota", `{}`, "unknown", 0, false, false},
		{"empty-quota", `{"gpuQuota":{}}`, "unknown", 0, false, false},
		{"zero", strings.Replace(complete, "108000s", "0s", 1), "known", 0, false, false},
		{"fully-reserved", strings.Replace(complete, `"timeReserved":"0s"`, `"timeReserved":"108000s"`, 1), "known", 0, false, false},
		{"subsecond-reservation", `{"gpuQuota":{"totalTimeAllowed":"120s","timeUsed":"0s","timeReserved":"0.000000001s"}}`, "known", 119, false, false},
		{"invalid-duration", strings.Replace(complete, "108000s", "-1s", 1), "unavailable", 0, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := monitorFixture(t)
			m.run = func(ctx context.Context, _ Config, mode string, token []byte, request monitorRequest) (monitorResponse, error) {
				client, err := kaggleapi.New(token, kaggleapi.Options{APITransport: apiRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if strings.HasSuffix(r.URL.Path, "IntrospectToken") {
						return jsonResponse(200, `{"active":true,"username":"`+m.config.AccountName+`"}`), nil
					}
					return jsonResponse(200, tc.wire), nil
				})})
				if err != nil {
					return monitorResponse{}, err
				}
				defer client.Close()
				return monitor(ctx, client, mode, request), nil
			}
			quota, err := m.ReadQuota(context.Background())
			if (err != nil) != tc.wantError || string(quota.Status) != tc.status || quota.Validate() != nil || quota.ResetAt != nil {
				t.Fatal("invalid mapped quota", quota, err)
			}
			if quota.Status == provider.QuotaKnown {
				if quota.Remaining == nil || *quota.Remaining != tc.remaining || quota.Unit != "seconds" || quota.Precision != "lower_bound" {
					t.Fatal("incorrect conservative allowance", quota)
				}
			} else if quota.Remaining != nil || quota.Limit != nil || quota.Used != nil {
				t.Fatal("unknown evidence became numeric capacity", quota)
			}
			reason, _ := (scheduler.Quota{Observation: &quota}).Decide("gpu", 120, true, QuotaFreshFor, m.clock.Now())
			if (reason == scheduler.Eligible) != tc.eligible {
				t.Fatal("incorrect 120-second quota decision", reason)
			}
		})
	}
}
