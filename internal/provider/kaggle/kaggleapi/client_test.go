package kaggleapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

const fixtureToken = "SYNTHETIC_TOKEN"

var fixtureDataset = Resource{"fixture_user", "fixture-stage"}
var fixtureKernel = Resource{"fixture_user", "fixture-run"}

type fixtureTransport func(*http.Request) (*http.Response, error)

func (f fixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func response(status int, contentType, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {contentType}},
		Body: io.NopCloser(strings.NewReader(body)), ContentLength: -1}
}

func newFixtureClient(t *testing.T, api, storage fixtureTransport) *Client {
	t.Helper()
	if storage == nil {
		storage = func(*http.Request) (*http.Response, error) {
			t.Fatal("unexpected storage request")
			return nil, nil
		}
	}
	c, err := New([]byte(fixtureToken), Options{APITransport: api, StorageTransport: storage})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func requireOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPinnedProtocolFixtures(t *testing.T) {
	data, err := os.ReadFile("testdata/protocol.json")
	requireOK(t, err)
	var fixture struct {
		Provenance string
		Calls      []struct {
			Operation string
			Request   json.RawMessage
			Response  json.RawMessage
			Status    int
			Location  string
			Body      string
		}
	}
	requireOK(t, json.Unmarshal(data, &fixture))
	if !strings.Contains(fixture.Provenance, "not live captures") {
		t.Fatal("fixture provenance missing")
	}
	index, storageCalls := 0, 0
	seen := make(map[string]bool)
	api := fixtureTransport(func(request *http.Request) (*http.Response, error) {
		if index >= len(fixture.Calls) {
			t.Fatal("unexpected extra RPC")
		}
		call := fixture.Calls[index]
		index++
		seen[call.Operation] = true
		if request.URL.String() != apiBase+call.Operation || request.Method != http.MethodPost ||
			request.Header.Get("Authorization") != "Bearer "+fixtureToken || request.Header.Get("Content-Type") != "application/json" ||
			request.Header.Get("Cookie") != "" || request.GetBody != nil {
			t.Fatalf("unexpected RPC transport for %s", call.Operation)
		}
		actual, err := io.ReadAll(request.Body)
		requireOK(t, err)
		var got, want any
		requireOK(t, json.Unmarshal(actual, &got))
		requireOK(t, json.Unmarshal(call.Request, &want))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("wire request mismatch for %s\ngot: %s\nwant: %s", call.Operation, actual, call.Request)
		}
		if _, ok := request.Context().Deadline(); !ok {
			t.Fatal("missing deadline")
		}
		if call.Status != 0 {
			r := response(call.Status, "application/octet-stream", call.Body)
			r.Header.Set("Location", call.Location)
			r.Body = &unreadBody{t: t}
			return r, nil
		}
		return response(200, "application/json", string(call.Response)), nil
	})
	storage := fixtureTransport(func(request *http.Request) (*http.Response, error) {
		storageCalls++
		if request.Header.Get("Authorization") != "" || request.Header.Get("Cookie") != "" || request.Header.Get("Proxy-Authorization") != "" || request.GetBody != nil {
			t.Fatal("account credentials or replay body entered storage")
		}
		if request.Method == http.MethodPut {
			body, err := io.ReadAll(request.Body)
			requireOK(t, err)
			if request.Header.Get("Content-Type") != "application/octet-stream" || int64(len(body)) != request.ContentLength {
				t.Fatal("wrong upload representation")
			}
			return response(200, "application/json", "{}"), nil
		}
		return response(200, "application/octet-stream", "abc"), nil
	})
	c := newFixtureClient(t, api, storage)
	ctx := context.Background()
	account, err := c.IntrospectToken(ctx)
	requireOK(t, err)
	if !account.Active || account.Username != "fixture_user" {
		t.Fatal(account)
	}
	quota, err := c.AcceleratorQuota(ctx)
	requireOK(t, err)
	if !quota.Known || !quota.FreeOnly || quota.Limit != 19999999999 || quota.Used != 2000000001 || quota.Reserved != 3000000001 {
		t.Fatal(quota)
	}
	dataset, err := c.GetDataset(ctx, fixtureDataset)
	requireOK(t, err)
	if dataset.ID != "123" || dataset.Version == nil || *dataset.Version != 1 || dataset.Private == nil || !*dataset.Private {
		t.Fatal(dataset)
	}
	state, err := c.DatasetStatus(ctx, fixtureDataset)
	requireOK(t, err)
	if state != DatasetReady {
		t.Fatal(state)
	}
	files, err := c.ListDatasetFiles(ctx, fixtureDataset, 2, 3)
	requireOK(t, err)
	if len(files) != 2 || files[0].Bytes != 3 || files[1].Bytes != 0 {
		t.Fatal(files)
	}
	var datasetBytes bytes.Buffer
	transfer, err := c.DownloadDatasetFile(ctx, fixtureDataset, "relay-stage.bin", 3, &datasetBytes)
	requireOK(t, err)
	if datasetBytes.String() != "abc" || transfer.Bytes != 3 || transfer.SHA256 != sha256.Sum256([]byte("abc")) {
		t.Fatal("invalid dataset receipt")
	}
	marker, err := c.StartBlobUpload(ctx, "relay-stage.bin", 3)
	requireOK(t, err)
	uploadedMarker, err := c.UploadBlob(ctx, marker, io.NopCloser(strings.NewReader("abc")), sha256.Sum256([]byte("abc")))
	requireOK(t, err)
	code, err := c.StartBlobUpload(ctx, "code.bin", 0)
	requireOK(t, err)
	uploadedCode, err := c.UploadBlob(ctx, code, io.NopCloser(strings.NewReader("")), sha256.Sum256(nil))
	requireOK(t, err)
	receipt, err := c.CreateDataset(ctx, CreateDatasetRequest{fixtureDataset, "other", "fixture staging", []*UploadedBlob{uploadedMarker, uploadedCode}})
	requireOK(t, err)
	if receipt.Reference != fixtureDataset.reference() {
		t.Fatal(receipt)
	}
	saved, err := c.SaveKernel(ctx, SaveKernelRequest{Resource: fixtureKernel, Source: "print('fixture')\n", Dataset: fixtureDataset, WallSeconds: 60})
	requireOK(t, err)
	if saved.ID != "42" || saved.Version != 1 {
		t.Fatal(saved)
	}
	kernel, err := c.GetKernel(ctx, fixtureKernel)
	requireOK(t, err)
	if kernel.Metadata.ID != "42" || kernel.Metadata.GPU == nil || *kernel.Metadata.GPU || *kernel.Blob.Source != "print('fixture')\n" || len(kernel.Metadata.KernelSources) != 0 {
		t.Fatal("wrong kernel identity")
	}
	kernelState, err := c.KernelSessionStatus(ctx, fixtureKernel)
	requireOK(t, err)
	if kernelState != KernelComplete {
		t.Fatal(kernelState)
	}
	var output bytes.Buffer
	_, err = c.DownloadKernelOutput(ctx, fixtureKernel, "relay-result/outputs/result.txt", 3, &output)
	requireOK(t, err)
	if output.String() != "abc" {
		t.Fatal("wrong output")
	}
	requireOK(t, c.DeleteKernel(ctx, fixtureKernel))
	requireOK(t, c.DeleteDataset(ctx, fixtureDataset))
	if index != len(fixture.Calls) || len(seen) != 14 || storageCalls != 4 {
		t.Fatalf("coverage mismatch: calls=%d, RPCs=%d, storage=%d", index, len(seen), storageCalls)
	}
}

type unreadBody struct{ t *testing.T }

func (b *unreadBody) Read([]byte) (int, error) {
	b.t.Fatal("error/redirect body consumed")
	return 0, io.EOF
}
func (*unreadBody) Close() error { return nil }

func TestTransportFailuresAreSingleAttemptAndSanitized(t *testing.T) {
	for _, mode := range []string{"transport", "http", "redirect", "receipt"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			c := newFixtureClient(t, func(request *http.Request) (*http.Response, error) {
				calls++
				switch mode {
				case "transport":
					return nil, fmt.Errorf("%s https://storage.googleapis.com/?signature=CANARY", fixtureToken)
				case "http":
					r := response(503, "text/html", fixtureToken)
					r.Body = &unreadBody{t}
					return r, nil
				case "redirect":
					r := response(302, "text/html", fixtureToken)
					r.Header.Set("Location", "https://evil.invalid/?token="+fixtureToken)
					r.Body = &unreadBody{t}
					return r, nil
				default:
					return response(200, "application/json", `{"errorMessage":"SYNTHETIC_TOKEN"}`), nil
				}
			}, nil)
			err := c.DeleteKernel(context.Background(), fixtureKernel)
			var failure *Error
			if !errors.As(err, &failure) || !failure.Sent || calls != 1 {
				t.Fatalf("invalid failure: %v calls=%d", err, calls)
			}
			for _, rendered := range []string{fmt.Sprint(err), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err)} {
				if strings.Contains(rendered, fixtureToken) || strings.Contains(rendered, "signature") || strings.Contains(rendered, "https://") {
					t.Fatal("unsafe error")
				}
			}
		})
	}
}

func TestClientCredentialOwnershipAndRedactedHandles(t *testing.T) {
	token := []byte(fixtureToken)
	c, err := New(token, Options{APITransport: fixtureTransport(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Authorization") != "Bearer "+fixtureToken {
			t.Fatal("borrowed credential")
		}
		return response(200, "application/json", `{"active":true,"username":"fixture_user"}`), nil
	})})
	requireOK(t, err)
	clear(token)
	_, err = c.IntrospectToken(context.Background())
	requireOK(t, err)
	ticket := BlobUpload{state: &blobUploadState{token: []byte(fixtureToken), url: "SIGNED_URL_CANARY"}}
	uploaded := UploadedBlob{token: []byte(fixtureToken)}
	for _, value := range []any{c, *c, ticket, &ticket, uploaded, &uploaded} {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			text := fmt.Sprintf(format, value)
			if strings.Contains(text, fixtureToken) || strings.Contains(text, "SIGNED_URL_CANARY") {
				t.Fatal("opaque handle leaked")
			}
		}
		encoded, err := json.Marshal(value)
		requireOK(t, err)
		if string(encoded) != "{}" {
			t.Fatalf("opaque JSON: %s", encoded)
		}
	}
	requireOK(t, c.Close())
	if len(c.credential()) != 0 {
		t.Fatal("credential survived close")
	}
	_, err = c.IntrospectToken(context.Background())
	if !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
}

func TestDefaultsHaveNoProxyCompressionOrConnectionRetry(t *testing.T) {
	c, err := New([]byte(fixtureToken), Options{})
	requireOK(t, err)
	defer c.Close()
	api := c.state.api.(*http.Transport)
	storage := c.state.storage.(*http.Transport)
	if api == storage || api.Proxy != nil || storage.Proxy != nil || !api.DisableKeepAlives || !storage.DisableKeepAlives ||
		!api.DisableCompression || !storage.DisableCompression || api.TLSHandshakeTimeout == 0 || api.ResponseHeaderTimeout == 0 {
		t.Fatal("unsafe production transport")
	}
	for _, options := range []Options{{RequestTimeout: time.Minute}, {TransferTimeout: time.Hour}, {LogTimeout: 3 * time.Second}, {RequestTimeout: -1}} {
		if _, err := New([]byte(fixtureToken), options); !errors.Is(err, ErrConfig) {
			t.Fatal("unbounded timeout accepted")
		}
	}
	for _, token := range [][]byte{nil, []byte("a b"), []byte("a\n"), []byte{0xff}, bytes.Repeat([]byte{'a'}, maxTokenBytes+1)} {
		if _, err := New(token, Options{}); !errors.Is(err, ErrConfig) {
			t.Fatal("invalid token accepted")
		}
	}
}
