package sshhost

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"

	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

type storageContainerSummary struct {
	ID     string            `json:"Id"`
	SizeRw *int64            `json:"SizeRw"`
	State  string            `json:"State"`
	Labels map[string]string `json:"Labels"`
	Mounts []json.RawMessage `json:"Mounts"`
}

// Docker's container summary reads its snapshot without taking the container
// mutex held for an archive transfer. Inspect takes that mutex even without size.
func storageContainers(ctx context.Context, d docker, budget *storageScanBudget, scope hoststorage.Scope, nativeEngine string) ([]hoststorage.Object, error) {
	filters, err := json.Marshal(map[string][]string{"label": {storageLabel + "=" + string(scope.InstallationID)}})
	if err != nil {
		return nil, err
	}
	query := url.Values{"all": {"1"}, "size": {"1"}, "filters": {string(filters)}}
	request, err := http.NewRequest(http.MethodGet, "http://docker/containers/json?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	request.Close = true
	raw, err := budget.readDockerJSON(ctx, d, request)
	if err != nil {
		return nil, err
	}
	var summaries []storageContainerSummary
	if json.Unmarshal(raw, &summaries) != nil || summaries == nil || len(summaries) > hoststorage.ScanMaxObjects {
		return nil, ErrRemoteDocker
	}
	objects := make([]hoststorage.Object, 0, len(summaries))
	for _, summary := range summaries {
		object := hoststorage.Object{Kind: hoststorage.Container, ID: summary.ID, Labels: summary.Labels, References: int64(len(summary.Mounts))}
		switch summary.State {
		case "running", "paused", "restarting":
			object.Running = true
		case "created", "exited", "dead", "removing":
		default:
			return nil, ErrRemoteDocker
		}
		if summary.SizeRw != nil && *summary.SizeRw >= 0 {
			object.SizeKnown, object.SizeBytes = true, *summary.SizeRw
		}
		if !object.Valid() || object.Labels[storageLabel] != string(scope.InstallationID) {
			return nil, hoststorage.ErrUnverified
		}
		if !validStorageLabels(object.Kind, object.Labels, scope, nativeEngine) {
			object.SizeKnown = false
		}
		objects = append(objects, object)
	}
	return objects, nil
}

// dial-stdio uses the same pinned local Docker socket as the CLI commands. Keep
// its input open through the response: early EOF can abort Docker Desktop's proxy.
func (b *storageScanBudget) readDockerJSON(ctx context.Context, d docker, request *http.Request) ([]byte, error) {
	if b.remaining <= 0 {
		return nil, errResponseLimit
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	input, send := io.Pipe()
	receive, output := io.Pipe()
	bounded := &limitedWriter{w: output, limit: b.remaining}
	done := make(chan error, 1)
	go func() {
		result, err := d.run(ctx, input, bounded, "system", "dial-stdio")
		if result.ExitCode != 0 {
			err = errors.Join(ErrRemoteDocker, err)
		}
		_ = output.CloseWithError(err)
		_ = input.CloseWithError(err)
		done <- err
	}()
	reader := bufio.NewReader(receive)
	err := request.Write(send)
	var body []byte
	if err == nil {
		var response *http.Response
		response, err = http.ReadResponse(reader, request)
		if err == nil {
			mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
			encoding := response.Header.Get("Content-Encoding")
			if response.StatusCode != http.StatusOK || mediaErr != nil || mediaType != "application/json" || encoding != "" && encoding != "identity" {
				err = ErrRemoteDocker
			} else {
				body, err = io.ReadAll(response.Body)
			}
		}
	}
	_ = send.Close()
	if err == nil {
		var extra int64
		extra, err = io.Copy(io.Discard, reader)
		if extra != 0 {
			err = ErrRemoteDocker
		}
	}
	if err != nil {
		cancel()
	}
	_ = receive.CloseWithError(err)
	runErr := <-done
	b.remaining -= bounded.n
	if err != nil || runErr != nil {
		return nil, errors.Join(ErrRemoteDocker, err, runErr)
	}
	return body, nil
}
