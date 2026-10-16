package kaggleapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestStrictJSONAndExactIDs(t *testing.T) {
	for _, text := range []string{`{"a":1,"a":2}`, `{"x":{"a":1,"a":2}}`, `{"x":[{"a":1,"a":2}]}`,
		`{"a":1} {}`, `{"a":NaN}`, `{"a":"\ud800"}`, `{"a":"\udc00"}`, `{"a":"\ud800\u0000"}`,
		`{"a":"` + string([]byte{0xff}) + `"}`, strings.Repeat(`{"a":`, 66) + `0` + strings.Repeat(`}`, 66)} {
		if _, err := strictObject([]byte(text)); !errors.Is(err, ErrResponse) {
			t.Fatalf("invalid JSON accepted: %q", text)
		}
	}
	for _, text := range []string{`[]`, `null`, `true`, `"object"`} {
		if _, err := strictObject([]byte(text)); !errors.Is(err, ErrResponse) {
			t.Fatal("non-object accepted")
		}
	}
	for _, text := range []string{`{"a":"\ud83d\ude00"}`, `{"a":"\\ud800"}`, `{"future":[null,false,0],"text":"Tiếng Việt"}`} {
		if _, err := strictObject([]byte(text)); err != nil {
			t.Fatal(err)
		}
	}
	for _, text := range []string{`42`, `"42"`, `9223372036854775807`, `"9223372036854775807"`} {
		var id ID
		requireOK(t, json.Unmarshal([]byte(text), &id))
		if id == "" {
			t.Fatal("lost numeric identity")
		}
	}
	for _, text := range []string{`0`, `null`, `true`, `42.0`, `1e2`, `-1`, `"01"`, `"+1"`, `9223372036854775808`, `" 42"`} {
		var id ID
		if json.Unmarshal([]byte(text), &id) == nil {
			t.Fatalf("invalid ID accepted: %s", text)
		}
	}
}

func TestMetadataBoundsAndContentValidation(t *testing.T) {
	for _, mode := range []string{"oversize", "declared-oversize", "short", "type", "encoding", "duplicate", "array", "embedded-error", "invalid-active"} {
		t.Run(mode, func(t *testing.T) {
			c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
				r := response(200, "application/json", `{"active":true,"username":"fixture_user"}`)
				switch mode {
				case "oversize":
					r.Body = io.NopCloser(strings.NewReader(strings.Repeat(" ", maxAccountBytes+1)))
				case "declared-oversize":
					r.Header.Set("Content-Length", "65537")
					r.Body = &unreadBody{t}
				case "short":
					r.Header.Set("Content-Length", "1000")
				case "type":
					r.Header.Set("Content-Type", "text/html")
					r.Body = &unreadBody{t}
				case "encoding":
					r.Header.Set("Content-Encoding", "gzip")
					r.Body = &unreadBody{t}
				case "duplicate":
					r.Body = io.NopCloser(strings.NewReader(`{"active":false,"active":true}`))
				case "array":
					r.Body = io.NopCloser(strings.NewReader(`[]`))
				case "embedded-error":
					r.Body = io.NopCloser(strings.NewReader(`{"code":401,"message":"SYNTHETIC_TOKEN"}`))
				case "invalid-active":
					r.Body = io.NopCloser(strings.NewReader(`{"active":null,"username":"fixture_user"}`))
				}
				return r, nil
			}, nil)
			_, err := c.IntrospectToken(context.Background())
			if err == nil || strings.Contains(err.Error(), fixtureToken) {
				t.Fatal("unsafe or successful invalid response")
			}
		})
	}
}

func TestNarrowMissingNameClassification(t *testing.T) {
	for _, resource := range []string{"dataset", "kernel"} {
		for _, mode := range []string{"404", "exact403", "other-permission", "generic403", "embedded404", "oversized403", "duplicate403"} {
			t.Run(resource+"/"+mode, func(t *testing.T) {
				permission := "datasets.get"
				if resource == "kernel" {
					permission = "kernels.get"
				}
				status := 403
				body := `{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Permission '` + permission + `' was denied"}}`
				switch mode {
				case "404":
					status = 404
				case "other-permission":
					body = strings.ReplaceAll(body, permission, "different.get")
				case "generic403":
					body = `{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Permission denied"}}`
				case "embedded404":
					status = 200
					body = `{"code":404,"message":"SYNTHETIC_TOKEN"}`
				case "oversized403":
					body += strings.Repeat(" ", maxErrorBytes)
				case "duplicate403":
					body = `{"error":{},"error":{"code":403,"status":"PERMISSION_DENIED","message":"Permission '` + permission + `' was denied"}}`
				}
				c := newFixtureClient(t, func(*http.Request) (*http.Response, error) { return response(status, "application/json", body), nil }, nil)
				var err error
				if resource == "dataset" {
					_, err = c.GetDataset(context.Background(), fixtureDataset)
				} else {
					_, err = c.GetKernel(context.Background(), fixtureKernel)
				}
				if NameAbsent(err) != (mode == "404" || mode == "exact403") {
					t.Fatalf("wrong absence decision: %v", err)
				}
			})
		}
	}
}

func TestQuotaPreservesUnknownAndNanosecondBounds(t *testing.T) {
	for _, text := range []string{"0s", "0.000000001s", "1.5s", "19.999999999s", "31622400s"} {
		if _, ok := duration(text); !ok {
			t.Fatal(text)
		}
	}
	for _, text := range []string{"", "-1s", "+1s", "01s", "1e3s", "1.s", "1.1234567890s", "1s\n", "1ss", "31622400.000000001s"} {
		if _, ok := duration(text); ok {
			t.Fatal(text)
		}
	}
	for _, flag := range []string{"absent", "false", "true", "null", `"false"`, "0", "{}"} {
		for _, missing := range []string{"none", "totalTimeAllowed", "timeUsed", "timeReserved"} {
			t.Run(flag+"/"+missing, func(t *testing.T) {
				quota := map[string]json.RawMessage{"totalTimeAllowed": json.RawMessage(`"19.999999999s"`), "timeUsed": json.RawMessage(`"2.000000001s"`), "timeReserved": json.RawMessage(`"3.000000001s"`)}
				if flag != "absent" {
					quota["isPayToScaleEnabled"] = json.RawMessage(flag)
				}
				if missing != "none" {
					delete(quota, missing)
				}
				body, err := json.Marshal(map[string]any{"gpuQuota": quota})
				requireOK(t, err)
				c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
					return response(200, "application/json", string(body)), nil
				}, nil)
				result, err := c.AcceleratorQuota(context.Background())
				requireOK(t, err)
				free := flag == "absent" || flag == "false"
				known := free && missing == "none"
				if result.Known != known || result.FreeOnly != free {
					t.Fatal(result)
				}
				if !known && (result.Limit != 0 || result.Used != 0 || result.Reserved != 0) {
					t.Fatal("unknown quota fabricated durations")
				}
			})
		}
	}
	for _, raw := range []string{`{}`, `{"gpuQuota":null}`, `{"gpuQuota":{}}`} {
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) { return response(200, "application/json", raw), nil }, nil)
		quota, err := c.AcceleratorQuota(context.Background())
		requireOK(t, err)
		if quota.Known || quota.Reason != "missing_quota" {
			t.Fatal(quota)
		}
	}
	for _, raw := range []string{`{"gpuQuota":[]}`, `{"gpuQuota":{"totalTimeAllowed":"-1s","timeUsed":"0s","timeReserved":"0s"}}`} {
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) { return response(200, "application/json", raw), nil }, nil)
		if _, err := c.AcceleratorQuota(context.Background()); !errors.Is(err, ErrResponse) {
			t.Fatal(err)
		}
	}
}

func TestStatusMissingOrUnknownNeverInheritsQueued(t *testing.T) {
	for _, raw := range []string{`{}`, `{"status":null}`, `{"status":false}`, `{"status":"FUTURE_STATE"}`, `{"status":999}`, `{"status":0.0}`} {
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) { return response(200, "application/json", raw), nil }, nil)
		state, err := c.KernelSessionStatus(context.Background(), fixtureKernel)
		requireOK(t, err)
		if state != KernelUnknown {
			t.Fatalf("invented state from %s: %s", raw, state)
		}
		dataset, err := c.DatasetStatus(context.Background(), fixtureDataset)
		requireOK(t, err)
		if dataset != DatasetUnknown {
			t.Fatalf("invented dataset state from %s: %s", raw, dataset)
		}
	}
	for _, raw := range []string{`{"status":2}`, `{"status":"complete"}`} {
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) { return response(200, "application/json", raw), nil }, nil)
		state, err := c.KernelSessionStatus(context.Background(), fixtureKernel)
		requireOK(t, err)
		if state != KernelComplete {
			t.Fatal(state)
		}
	}
}

func TestFullDatasetListingRejectsPartialOrInconsistentPages(t *testing.T) {
	for _, mode := range []string{"duplicate", "cycle", "never-ending", "count", "page-size", "negative", "null-size", "bytes", "path", "provider-error"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
				calls++
				body := `{"datasetFiles":[{"name":"code.bin","totalBytes":1}]} `
				switch mode {
				case "duplicate":
					body = `{"datasetFiles":[{"name":"code.bin"},{"name":"code.bin"}]}`
				case "cycle":
					body = `{"datasetFiles":[],"nextPageToken":"same"}`
				case "never-ending":
					body = `{"datasetFiles":[],"nextPageToken":"` + strings.Repeat("a", calls) + `"}`
				case "count":
					body = `{"datasetFiles":[{"name":"a"},{"name":"b"},{"name":"c"}]}`
				case "page-size":
					body = `{"datasetFiles":[` + strings.TrimSuffix(strings.Repeat(`{"name":"a"},`, 21), ",") + `]}`
				case "negative":
					body = `{"datasetFiles":[{"name":"code.bin","totalBytes":-1}]}`
				case "null-size":
					body = `{"datasetFiles":[{"name":"code.bin","totalBytes":null}]}`
				case "bytes":
					body = `{"datasetFiles":[{"name":"code.bin","totalBytes":4}]}`
				case "path":
					body = `{"datasetFiles":[{"name":"../code.bin"}]}`
				case "provider-error":
					body = `{"errorMessage":"SYNTHETIC_TOKEN"}`
				}
				return response(200, "application/json", body), nil
			}, nil)
			files, err := c.ListDatasetFiles(context.Background(), fixtureDataset, 2, 3)
			if err == nil || files != nil || calls > 3 || strings.Contains(err.Error(), fixtureToken) {
				t.Fatalf("partial/unbounded list: %v (%d calls)", err, calls)
			}
		})
	}
}

func TestRequestAndParentDeadlines(t *testing.T) {
	calls := 0
	c, err := New([]byte(fixtureToken), Options{RequestTimeout: 5 * time.Millisecond, APITransport: fixtureTransport(func(request *http.Request) (*http.Response, error) {
		calls++
		<-request.Context().Done()
		return nil, errors.New(fixtureToken)
	})})
	requireOK(t, err)
	defer c.Close()
	_, err = c.IntrospectToken(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.IntrospectToken(ctx)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal("cancelled request reached transport")
	}
	var failure *Error
	if !errors.As(err, &failure) || failure.Sent {
		t.Fatal("pre-send cancellation became ambiguous")
	}
}

func TestPreserveMetadataFlagPresence(t *testing.T) {
	for _, value := range []string{"absent", "null", "false", "true"} {
		flag := ""
		if value != "absent" {
			flag = `,"enableGpu":` + value
		}
		body := `{"metadata":{"id":42,"ref":"fixture_user/fixture-run"` + flag + `},"blob":{"source":"fixture"}}`
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) { return response(200, "application/json", body), nil }, nil)
		kernel, err := c.GetKernel(context.Background(), fixtureKernel)
		requireOK(t, err)
		if (kernel.Metadata.GPU == nil) != (value == "absent" || value == "null") {
			t.Fatal("presence lost")
		}
	}
	for _, value := range []string{`"false"`, "0", "{}"} {
		body := `{"metadata":{"id":42,"enableGpu":` + value + `},"blob":{"source":"fixture"}}`
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) { return response(200, "application/json", body), nil }, nil)
		if _, err := c.GetKernel(context.Background(), fixtureKernel); !errors.Is(err, ErrResponse) {
			t.Fatal(err)
		}
	}
}

func TestDeleteCompletionRequiresPinnedResponse(t *testing.T) {
	for _, kind := range []string{"kernel", "dataset"} {
		field := "errorMessage"
		if kind == "dataset" {
			field = "error"
		}
		for _, test := range []struct {
			status int
			body   string
			ok     bool
		}{
			{200, `{}`, true}, {200, `{"` + field + `":null}`, true}, {200, `{"` + field + `":""}`, true},
			{200, `{"` + field + `":"SYNTHETIC_TOKEN"}`, false}, {200, `{"unexpected":null}`, false},
			{200, `{"` + field + `":null,"unexpected":null}`, false}, {202, `{}`, false}, {204, `{}`, false},
		} {
			c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
				return response(test.status, "application/json", test.body), nil
			}, nil)
			var err error
			if kind == "kernel" {
				err = c.DeleteKernel(context.Background(), fixtureKernel)
			} else {
				err = c.DeleteDataset(context.Background(), fixtureDataset)
			}
			if (err == nil) != test.ok {
				t.Fatalf("wrong %s completion for HTTP %d %s: %v", kind, test.status, test.body, err)
			}
		}
	}
}

func TestGPUSaveRequiresImmediateFreeOnlyQuota(t *testing.T) {
	for _, mode := range []string{"omitted-flag", "false", "paid", "null", "missing", "case-alias", "http", "lost-save"} {
		t.Run(mode, func(t *testing.T) {
			quotaReads, saves := 0, 0
			c := newFixtureClient(t, func(request *http.Request) (*http.Response, error) {
				if request.URL.String() == apiBase+quotaRPC {
					quotaReads++
					body := `{"gpuQuota":{"totalTimeAllowed":"108000s","timeUsed":"0s","timeReserved":"0s"}}`
					switch mode {
					case "false":
						body = `{"gpuQuota":{"isPayToScaleEnabled":false}}`
					case "paid":
						body = `{"gpuQuota":{"isPayToScaleEnabled":true}}`
					case "null":
						body = `{"gpuQuota":{"isPayToScaleEnabled":null}}`
					case "missing":
						body = `{}`
					case "case-alias":
						body = `{"gpuQuota":{"IsPayToScaleEnabled":true}}`
					case "http":
						return response(503, "application/json", fixtureToken), nil
					}
					return response(200, "application/json", body), nil
				}
				saves++
				if request.URL.String() != apiBase+kernelService+"SaveKernel" || quotaReads != 1 {
					t.Fatal("GPU save bypassed immediate quota")
				}
				var body map[string]json.RawMessage
				requireOK(t, json.NewDecoder(request.Body).Decode(&body))
				if string(body["enableGpu"]) != "true" || string(body["machineShape"]) != `"NvidiaTeslaT4"` || string(body["enableTpu"]) != "false" {
					t.Fatal("wrong accelerator flags")
				}
				if mode == "lost-save" {
					return nil, errors.New(fixtureToken)
				}
				return response(200, "application/json", `{"ref":"fixture_user/fixture-run","versionNumber":1,"kernelId":42}`), nil
			}, nil)
			_, err := c.SaveKernel(context.Background(), SaveKernelRequest{Resource: fixtureKernel, Datasets: []Resource{fixtureDataset},
				Source: "print('fixture')\n", GPU: true, MachineShape: "NvidiaTeslaT4", WallSeconds: 60})
			allowed := mode == "omitted-flag" || mode == "false" || mode == "lost-save"
			if quotaReads != 1 || saves != map[bool]int{true: 1, false: 0}[allowed] {
				t.Fatalf("wrong bounded calls: quota=%d save=%d", quotaReads, saves)
			}
			if !allowed {
				var failure *Error
				if !errors.Is(err, ErrQuota) || !errors.As(err, &failure) || failure.Sent {
					t.Fatal("unverified quota reached save")
				}
			} else if mode == "lost-save" {
				var failure *Error
				if !errors.As(err, &failure) || !failure.Sent {
					t.Fatal("lost save acknowledgement became retryable")
				}
			} else {
				requireOK(t, err)
			}
		})
	}
}

func TestKnownWireFieldsRequireExactCase(t *testing.T) {
	for _, raw := range []string{`{"Active":true,"username":"fixture_user"}`, `{"active":false,"Active":true,"username":"fixture_user"}`, `{"active":true,"Username":"fixture_user"}`} {
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) { return response(200, "application/json", raw), nil }, nil)
		if _, err := c.IntrospectToken(context.Background()); !errors.Is(err, ErrResponse) {
			t.Fatal("case alias became account evidence")
		}
	}
	for _, metadata := range []string{`{"id":42,"IsPrivate":true}`, `{"id":42,"isPrivate":false,"IsPrivate":true}`, `{"ID":42}`} {
		body := `{"metadata":` + metadata + `,"blob":{"source":"fixture"}}`
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) { return response(200, "application/json", body), nil }, nil)
		if _, err := c.GetKernel(context.Background(), fixtureKernel); !errors.Is(err, ErrResponse) {
			t.Fatal("case alias became kernel evidence")
		}
	}
	c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
		return response(403, "application/json", `{"error":{"Code":403,"status":"PERMISSION_DENIED","message":"Permission 'kernels.get' was denied"}}`), nil
	}, nil)
	_, err := c.GetKernel(context.Background(), fixtureKernel)
	if NameAbsent(err) {
		t.Fatal("case alias became absence evidence")
	}
	c = newFixtureClient(t, func(*http.Request) (*http.Response, error) {
		return response(200, "application/json", `{"datasetFiles":[{"name":"code.bin","TotalBytes":1}]}`), nil
	}, nil)
	if _, err := c.ListDatasetFiles(context.Background(), fixtureDataset, 2, 3); !errors.Is(err, ErrResponse) {
		t.Fatal("case alias became file size evidence")
	}
	c = newFixtureClient(t, func(*http.Request) (*http.Response, error) {
		return response(200, "application/json", `{"active":true,"username":"fixture_user","futureField":{"anything":true}}`), nil
	}, nil)
	_, err = c.IntrospectToken(context.Background())
	requireOK(t, err)
}
