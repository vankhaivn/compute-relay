package kaggle

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

type stagingReadFailure struct{ delivered bool }

func (r *stagingReadFailure) Read(b []byte) (int, error) {
	if !r.delivered {
		r.delivered = true
		return copy(b, "abc"), nil
	}
	return 0, errors.New("synthetic failure after the last payload byte")
}

func TestStagingCompletionTrailerSurvivesOnlyAcknowledgedSources(t *testing.T) {
	for _, fail := range []bool{false, true} {
		var body io.Reader = bytes.NewBufferString("abc")
		if fail {
			body = &stagingReadFailure{}
		}
		data, err := io.ReadAll(stagingCreateInput([]byte("header"), []byte("marker"), body))
		if fail {
			if err == nil || string(data) != "headermarkerabc" {
				t.Fatal("failed source issued a completion trailer", err)
			}
		} else if err != nil || string(data) != "headermarkerabc"+stagingUploadComplete {
			t.Fatal("acknowledged source framing differs", err)
		}
	}
}

func TestStagingPipeEOFIsNotSourceAcknowledgement(t *testing.T) {
	python := pythonForTest(t)
	// Execute the exact embedded Python gate in a real child. The os/exec stdin
	// copier closes its pipe on either EOF or error: the child must distinguish
	// the two even after receiving all expected payload bytes. No SDK is imported.
	script := "import sys\nns={'__name__':'completion_fixture'}\nexec(" + strconv.Quote(stagingSource) + ",ns)\n" + `
assert sys.stdin.buffer.read(3)==b'abc'
try:
    ns['require_upload_complete'](sys.stdin.buffer)
except Exception:
    sys.stdout.buffer.write(b'denied\n')
else:
    sys.stdout.buffer.write(b'authorized\n')
`
	for _, fail := range []bool{false, true} {
		var body io.Reader = bytes.NewBufferString("abc")
		if fail {
			body = &stagingReadFailure{}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, python, "-I", "-c", script)
		cmd.Stdin = stagingCreateInput(nil, nil, body)
		cmd.WaitDelay = time.Second
		var out, diagnostic bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &diagnostic
		err := cmd.Run()
		cancel()
		if fail {
			if err == nil || out.String() != "denied\n" {
				t.Fatal("source failure allowed child mutation", err, out.String(), diagnostic.String())
			}
		} else if err != nil || out.String() != "authorized\n" {
			t.Fatal("successful source was not acknowledged", err, out.String(), diagnostic.String())
		}
	}
}
