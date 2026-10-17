package kaggle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

type apiRoundTripFunc func(*http.Request) (*http.Response, error)

func (f apiRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func jsonResponse(status int, data string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(data)), ContentLength: int64(len(data))}
}

func bytesResponse(data []byte) *http.Response {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/octet-stream"}},
		Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data))}
}

// This fixture speaks only the actual native HTTP protocol. It never bypasses
// request encoding, signed-transfer handling, response validation or hashing.
type nativeFixture struct {
	t           *testing.T
	owner       string
	calls       []string
	dataset     bool
	datasetID   string
	stage       stagingRequest
	staged      map[string][]byte
	uploads     map[string]string
	uploaded    map[string][]byte
	kernel      *executionRequest
	kernelID    string
	state       string
	outputs     map[string][]byte
	logs        string
	fault       string
	quota       string
	getDataset  int
	getKernel   int
	transferred int
	putFailures int
	shared      map[string]*fixtureDataset
}

// fixtureDataset is one shared input dataset; the attempt dataset keeps the fields above.
type fixtureDataset struct {
	id     string
	stage  stagingRequest
	staged map[string][]byte
	state  string
}

func newNativeFixture(t *testing.T, owner string) *nativeFixture {
	t.Helper()
	return &nativeFixture{t: t, owner: owner, datasetID: "123", kernelID: "42", state: "RUNNING", staged: map[string][]byte{},
		uploads: map[string]string{}, uploaded: map[string][]byte{}, outputs: map[string][]byte{}, logs: `[{"data":"first\nsecond\n"}]`, shared: map[string]*fixtureDataset{},
		quota: `{"gpuQuota":{"totalTimeAllowed":"108000s","timeUsed":"0s","timeReserved":"0s"}}`}
}

func (f *nativeFixture) client(token []byte) (*kaggleapi.Client, error) {
	return kaggleapi.New(token, kaggleapi.Options{APITransport: apiRoundTripFunc(f.api), StorageTransport: apiRoundTripFunc(f.storage)})
}

func (f *nativeFixture) api(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" || r.URL.Host != "api.kaggle.com" || r.Header.Get("Authorization") != "Bearer SYNTHETIC_TOKEN" || r.GetBody != nil {
		f.t.Fatal("unexpected API destination, credential or replayable request")
	}
	op := path.Base(r.URL.Path)
	if r.Method == "GET" {
		if !strings.HasPrefix(r.URL.Path, "/v1/kernels/logs/stream/"+f.owner+"/") {
			f.t.Fatal("unexpected API GET")
		}
		op = "Logs"
	}
	f.calls = append(f.calls, op)
	var payload map[string]json.RawMessage
	if r.Method == "POST" && json.NewDecoder(r.Body).Decode(&payload) != nil {
		f.t.Fatal("invalid RPC body")
	}
	get := func(name string) string { var s string; _ = json.Unmarshal(payload[name], &s); return s }
	if strings.HasPrefix(get("datasetSlug"), "crx-") || strings.HasPrefix(get("slug"), "crx-") {
		return f.sharedAPI(op, payload, get)
	}
	switch op {
	case "IntrospectToken":
		return jsonResponse(200, `{"active":true,"username":"`+f.owner+`"}`), nil
	case "GetAcceleratorQuotaStatistics":
		return jsonResponse(200, f.quota), nil
	case "GetDataset":
		f.getDataset++
		if !f.dataset {
			if f.fault == "missing-name-403" {
				return jsonResponse(403, `{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Permission 'datasets.get' was denied"}}`), nil
			}
			return jsonResponse(404, `{}`), nil
		}
		id := f.datasetID
		if f.fault == "dataset-replaced" && f.getDataset > 2 {
			id = "124"
		}
		return f.json(map[string]any{"id": id, "ref": f.owner + "/" + f.stage.Slug, "currentVersionNumber": 1, "isPrivate": f.fault != "public-dataset",
			"description": stagingDescription(f.stage.MarkerSHA256), "licenseName": stagingLicense(f.stage.License), "lastUpdated": "fixture"}), nil
	case "GetDatasetStatus":
		state := "READY"
		if f.fault == "pending-dataset" {
			state = "BLOBS_RECEIVED"
		}
		return f.json(map[string]any{"status": state}), nil
	case "StartBlobUpload":
		token := "upload-" + strconv.Itoa(len(f.uploads))
		f.uploads[token] = get("name")
		return f.json(map[string]any{"token": token, "createUrl": "https://storage.googleapis.com/fixture/" + token + "?signature=fixture"}), nil
	case "CreateDataset":
		if string(payload["isPrivate"]) != "true" || f.dataset {
			f.t.Fatal("non-private or duplicate dataset creation")
		}
		f.dataset = true
		f.stage.Owner, f.stage.Slug, f.stage.License = get("ownerSlug"), get("slug"), get("licenseName")
		f.staged = f.created(payload)
		f.stage.MarkerSHA256 = provider.Digest(f.staged["relay-stage.bin"])
		if f.fault == "lost-create-ack" {
			return nil, errors.New("SYNTHETIC_TOKEN")
		}
		return f.json(map[string]any{"ref": f.owner + "/" + f.stage.Slug}), nil
	case "ListDatasetFiles":
		files := []map[string]any{}
		for name, data := range f.staged {
			files = append(files, map[string]any{"name": name, "totalBytes": len(data)})
		}
		if f.fault == "extra-staging-file" {
			files = append(files, map[string]any{"name": "extra.bin", "totalBytes": 0})
		}
		return f.json(map[string]any{"datasetFiles": files}), nil
	case "DownloadDataset":
		name := get("fileName")
		_, ok := f.staged[name]
		if !ok {
			return jsonResponse(404, `{}`), nil
		}
		if f.fault == "corrupt-staging" && name == "code.bin" {
			return bytesResponse([]byte("changed")), nil
		}
		return f.redirect("staging/" + name), nil
	case "GetKernel":
		f.getKernel++
		if f.kernel == nil {
			return jsonResponse(404, `{}`), nil
		}
		r := f.kernel
		id := f.kernelID
		if f.fault == "kernel-replaced" && f.getKernel > 1 {
			id = "43"
		}
		return f.json(map[string]any{"metadata": map[string]any{"id": id, "ref": r.Owner + "/" + r.Slug, "slug": r.Slug,
			"currentVersionNumber": 1, "language": "python", "kernelType": "script", "isPrivate": f.fault != "public-kernel",
			"enableGpu": r.GPU, "enableTpu": false, "enableInternet": r.Internet, "machineShape": r.MachineShape,
			"datasetDataSources": r.Datasets}, "blob": map[string]any{"source": r.Source}}), nil
	case "SaveKernel":
		if f.kernel != nil || string(payload["isPrivate"]) != "true" || string(payload["enableTpu"]) != "false" {
			f.t.Fatal("duplicate or unsafe kernel save")
		}
		var gpu, internet bool
		_ = json.Unmarshal(payload["enableGpu"], &gpu)
		_ = json.Unmarshal(payload["enableInternet"], &internet)
		var datasets []string
		_ = json.Unmarshal(payload["datasetDataSources"], &datasets)
		f.kernel = &executionRequest{Owner: f.owner, Slug: strings.TrimPrefix(get("slug"), f.owner+"/"), Source: get("text"), GPU: gpu,
			Internet: internet, MachineShape: get("machineShape"), Datasets: datasets}
		if f.fault == "lost-save-ack" {
			return nil, errors.New("SYNTHETIC_TOKEN")
		}
		return f.json(map[string]any{"kernelId": f.kernelID, "ref": get("slug"), "versionNumber": 1}), nil
	case "GetKernelSessionStatus":
		return f.json(map[string]any{"status": f.state}), nil
	case "Logs":
		return jsonResponse(200, f.logs), nil
	case "DownloadKernelOutput":
		name := strings.TrimPrefix(get("filePath"), "relay-result/")
		if _, ok := f.outputs[name]; !ok {
			return jsonResponse(404, `{}`), nil
		}
		return f.redirect("outputs/" + name), nil
	case "DeleteKernel":
		f.kernel = nil
		if f.fault == "lost-delete-ack" {
			return nil, errors.New("SYNTHETIC_TOKEN")
		}
		return jsonResponse(200, `{}`), nil
	case "DeleteDataset":
		f.dataset = false
		if f.fault == "lost-delete-ack" {
			return nil, errors.New("SYNTHETIC_TOKEN")
		}
		return jsonResponse(200, `{}`), nil
	default:
		f.t.Fatal("unexpected RPC", op)
		return nil, errors.New("unexpected RPC")
	}
}

// created returns the uploaded files named by one CreateDataset call.
func (f *nativeFixture) created(payload map[string]json.RawMessage) map[string][]byte {
	var files []struct {
		Token string `json:"token"`
	}
	if json.Unmarshal(payload["files"], &files) != nil {
		f.t.Fatal("invalid dataset files")
	}
	staged := map[string][]byte{}
	for _, file := range files {
		staged[f.uploads[file.Token]] = bytes.Clone(f.uploaded[file.Token])
	}
	return staged
}

func (f *nativeFixture) sharedAPI(op string, payload map[string]json.RawMessage, get func(string) string) (*http.Response, error) {
	slug := get("datasetSlug")
	if op == "CreateDataset" {
		slug = get("slug")
	}
	entry := f.shared[slug]
	switch op {
	case "GetDataset":
		if entry == nil {
			return jsonResponse(404, `{}`), nil
		}
		return f.json(map[string]any{"id": entry.id, "ref": f.owner + "/" + slug, "currentVersionNumber": 1, "isPrivate": true,
			"description": stagingDescription(entry.stage.MarkerSHA256), "licenseName": stagingLicense(entry.stage.License), "lastUpdated": "fixture"}), nil
	case "GetDatasetStatus":
		return f.json(map[string]any{"status": entry.state}), nil
	case "CreateDataset":
		if string(payload["isPrivate"]) != "true" || entry != nil {
			f.t.Fatal("non-private or duplicate shared dataset creation")
		}
		staged := f.created(payload)
		entry = &fixtureDataset{id: strconv.Itoa(900 + len(f.shared)), staged: staged, state: "READY",
			stage: stagingRequest{Owner: get("ownerSlug"), Slug: slug, License: get("licenseName"), MarkerSHA256: provider.Digest(staged[sharedMarkerFile])}}
		f.shared[slug] = entry
		if f.fault == "lost-shared-create" {
			return nil, errors.New("SYNTHETIC_TOKEN")
		}
		return f.json(map[string]any{"ref": f.owner + "/" + slug}), nil
	case "ListDatasetFiles":
		files := []map[string]any{}
		for name, data := range entry.staged {
			files = append(files, map[string]any{"name": name, "totalBytes": len(data)})
		}
		return f.json(map[string]any{"datasetFiles": files}), nil
	case "DownloadDataset":
		f.t.Fatal("shared dataset bytes must not be downloaded")
	}
	f.t.Fatal("unexpected shared dataset RPC", op)
	return nil, errors.New("unexpected RPC")
}

func (f *nativeFixture) json(value any) *http.Response {
	data, err := json.Marshal(value)
	if err != nil {
		f.t.Fatal(err)
	}
	return jsonResponse(200, string(data))
}

func (f *nativeFixture) redirect(name string) *http.Response {
	return &http.Response{StatusCode: 302, Header: http.Header{"Location": {"https://storage.googleapis.com/fixture/" + name + "?signature=fixture"}},
		Body: io.NopCloser(strings.NewReader("discarded")), ContentLength: 9}
}

func (f *nativeFixture) storage(r *http.Request) (*http.Response, error) {
	if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.URL.Host != "storage.googleapis.com" || r.GetBody != nil {
		f.t.Fatal("credential or replayable request escaped to signed storage")
	}
	key := strings.TrimPrefix(r.URL.Path, "/fixture/")
	if r.Method == "PUT" {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		if f.putFailures > 0 {
			f.putFailures--
			return jsonResponse(500, `{}`), nil
		}
		f.uploaded[key] = data
		return jsonResponse(200, `{}`), nil
	}
	f.transferred++
	if strings.HasPrefix(key, "staging/") {
		return bytesResponse(f.staged[strings.TrimPrefix(key, "staging/")]), nil
	}
	return bytesResponse(f.outputs[strings.TrimPrefix(key, "outputs/")]), nil
}

func (f *nativeFixture) count(operation string) int {
	count := 0
	for _, name := range f.calls {
		if name == operation {
			count++
		}
	}
	return count
}

func (f *nativeFixture) bind(p *RuntimeAdapter) {
	p.preflight.run = func(ctx context.Context, c Config, _ Mode, token []byte) (Report, error) {
		client, err := f.client(token)
		if err != nil {
			return Report{}, err
		}
		defer client.Close()
		return preflight(ctx, client, c.AccountName), nil
	}
	p.stager.run = func(ctx context.Context, _ Config, mode string, token []byte, plan stagingPlan, blobs StagingBlobs) (stagingResponse, error) {
		client, err := f.client(token)
		if err != nil {
			return stagingResponse{}, err
		}
		defer client.Close()
		return stage(ctx, client, mode, plan, blobs)
	}
	p.makeExecutor = func(stager *Stager, policy ExecutionPolicy, plan provider.Plan, prepared provider.Prepared, allow bool) (*Executor, error) {
		e, err := NewExecutor(stager, policy, plan, prepared, allow)
		if err != nil {
			return nil, err
		}
		e.run = func(ctx context.Context, _ Config, mode string, token []byte, r executionRequest) (executionResponse, error) {
			client, err := f.client(token)
			if err != nil {
				return executionResponse{}, err
			}
			defer client.Close()
			return execute(ctx, client, mode, r), nil
		}
		return e, nil
	}
	p.monitor.run = func(ctx context.Context, _ Config, mode string, token []byte, r monitorRequest) (monitorResponse, error) {
		client, err := f.client(token)
		if err != nil {
			return monitorResponse{}, err
		}
		defer client.Close()
		return monitor(ctx, client, mode, r), nil
	}
	p.makeArtifacts = func(e *Executor, policy ArtifactPolicy) (*ArtifactReader, error) {
		a, err := NewArtifactReader(e, policy)
		if err != nil {
			return nil, err
		}
		a.run = func(ctx context.Context, _ Config, mode string, token []byte, r artifactRequest, dst io.Writer) ([]byte, error) {
			client, err := f.client(token)
			if err != nil {
				return nil, err
			}
			defer client.Close()
			return artifacts(ctx, client, mode, r, dst)
		}
		return a, nil
	}
	p.runCleanup = func(ctx context.Context, _ Config, mode string, token []byte, r cleanupRequest) (cleanupResponse, error) {
		client, err := f.client(token)
		if err != nil {
			return cleanupResponse{}, err
		}
		defer client.Close()
		return cleanup(ctx, client, mode, r), nil
	}
}
