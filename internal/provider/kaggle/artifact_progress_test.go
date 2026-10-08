package kaggle

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestArtifactProgressObservesExactStreamAndPreservesLateFailure(t *testing.T) {
	for _, late := range []bool{false, true} {
		a, ref, data := artifactFixture(t)
		file := artifactFor(ref, "outputs/answer.txt", data["outputs/answer.txt"])
		a.run = func(_ context.Context, _ Config, mode string, _ []byte, r artifactRequest, dst io.Writer) ([]byte, error) {
			if mode != "fetch" || r.Target.Path != file.Path {
				t.Fatal("progress changed exact target")
			}
			if _, err := dst.Write([]byte("y")); err != nil {
				return nil, err
			}
			if _, err := dst.Write([]byte("es")); err != nil {
				return nil, err
			}
			if late {
				return nil, ErrProcess
			}
			return nil, nil
		}
		var samples []int64
		var dst bytes.Buffer
		r, err := a.FetchArtifactWithProgress(context.Background(), ref, file, &dst, file.Bytes, func(n int64) error { samples = append(samples, n); return nil })
		if !reflect.DeepEqual(samples, []int64{1, 3}) || dst.String() != "yes" || (err == nil) == late {
			t.Fatal(samples, r, err)
		}
		if late && r != (provider.TransferResult{}) {
			t.Fatal("full bytes plus failed helper produced receipt", r)
		}
	}
}

func TestArtifactProgressCallbackFailureStopsAndSurvivesBridgeError(t *testing.T) {
	a, ref, data := artifactFixture(t)
	file := artifactFor(ref, "outputs/answer.txt", data["outputs/answer.txt"])
	stop := errors.New("progress ownership lost")
	a.run = func(_ context.Context, _ Config, _ string, _ []byte, _ artifactRequest, dst io.Writer) ([]byte, error) {
		if _, err := dst.Write([]byte("y")); !errors.Is(err, stop) {
			t.Fatal(err)
		}
		return nil, ErrProcess
	}
	var dst bytes.Buffer
	if _, err := a.FetchArtifactWithProgress(context.Background(), ref, file, &dst, file.Bytes, func(int64) error { return stop }); !errors.Is(err, stop) || dst.String() != "y" {
		t.Fatal("callback failure became success or lost its identity", err, dst.String())
	}
}
