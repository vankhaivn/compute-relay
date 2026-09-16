package kaggle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"
)

func TestStagingBodyRejectsBoundaryAndCloseFailures(t *testing.T) {
	for _, mode := range []string{"normal", "missing", "short", "long", "late", "close"} {
		t.Run(mode, func(t *testing.T) {
			s, plan, b, _ := newTestStager(t, false)
			p, err := buildStagingPlan(s.config, s.policy, plan, "prep")
			if err != nil {
				t.Fatal(err)
			}
			b.mode = mode
			r := &stagingBody{ctx: context.Background(), blobs: b, plan: p}
			defer r.Close()
			data, err := io.ReadAll(r)
			if mode == "normal" {
				if err != nil || !bytes.Equal(data, []byte("synthetic-codeabc")) || b.reads != len(p.objects) {
					t.Fatal("wrong sequential payload", err, b.reads)
				}
			} else if err == nil {
				t.Fatal("bad stream ended successfully")
			}
		})
	}
}

func TestStagingProcessFramingIsolationAndObserveHasNoPayload(t *testing.T) {
	t.Setenv("KAGGLE_API_TOKEN", "AMBIENT_CANARY")
	t.Setenv("PYTHONPATH", t.TempDir())
	t.Setenv("HTTPS_PROXY", "http://must-not-use.invalid")
	for _, mode := range []string{"create", "observe"} {
		t.Run(mode, func(t *testing.T) {
			s, plan, b, _ := newTestStager(t, true)
			s.config.PythonExecutable = pythonForTest(t)
			p, err := buildStagingPlan(s.config, s.policy, plan, "prep")
			if err != nil {
				t.Fatal(err)
			}
			want := stageResponse(p, "ready")
			encoded, _ := json.Marshal(want)
			source := `import os,sys,json,hashlib
assert sys.flags.isolated
assert os.path.samefile(os.getcwd(),os.environ['HOME'])
assert not any(k in os.environ for k in ('KAGGLE_API_TOKEN','PYTHONPATH','HTTPS_PROXY'))
assert 'SYNTHETIC_TOKEN' not in str(sys.argv)
assert sys.stdin.buffer.readline()==b'SYNTHETIC_TOKEN\n'
r=json.loads(sys.stdin.buffer.readline())
assert set(r)=={'protocol','owner','slug','license','marker_sha256','files'}
if sys.argv[1]=='create':
    for f in r['files']:
        remaining=f['bytes']
        digest=hashlib.sha256()
        while remaining:
            part=sys.stdin.buffer.read(min(remaining,7))
            assert part
            remaining-=len(part)
            digest.update(part)
        assert digest.hexdigest()==f['sha256']
    assert sys.stdin.buffer.read(len(b'\x00compute-relay/staging-upload-complete/v1\n'))==b'\x00compute-relay/staging-upload-complete/v1\n'
assert sys.stdin.buffer.read(1)==b''
`
			source += "\nprint(" + strconv.Quote(string(encoded)) + ")\n"
			got, err := runStagingSource(context.Background(), s.config, mode, []byte("SYNTHETIC_TOKEN"), p, b, source)
			if err != nil || got != want {
				t.Fatal("framing or process isolation failed", got, err)
			}
			if mode == "observe" && b.reads != 0 || mode == "create" && b.reads != len(p.objects) {
				t.Fatal("wrong payload access", b.reads)
			}
		})
	}
}

func TestStagingProcessRejectsInvalidOutputAndExpires(t *testing.T) {
	s, plan, b, _ := newTestStager(t, false)
	s.config.PythonExecutable = pythonForTest(t)
	p, err := buildStagingPlan(s.config, s.policy, plan, "prep")
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		`print('SYNTHETIC_SECRET')`,
		`print('x'*20000)`,
		`import sys;sys.stderr.write('x'*20000)`,
		`import sys;sys.exit(23)`,
		`print('{"protocol":1,"protocol":1}')`,
	} {
		_, err := runStagingSource(context.Background(), s.config, "observe", []byte("SYNTHETIC_TOKEN"), p, b, source)
		if err != ErrProcess && err != ErrProtocol && !errors.Is(err, context.Canceled) {
			t.Fatal("unsafe process result", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = runStagingSource(ctx, s.config, "observe", []byte("SYNTHETIC_TOKEN"), p, b, `import time;time.sleep(60)`)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second || b.reads != 0 {
		t.Fatal("unbounded or mutating observation", err, b.reads)
	}
}

func TestStagingTrailerRequiresFinalPayloadCloseAndEOF(t *testing.T) {
	for _, mode := range []string{"normal", "late", "close"} {
		t.Run(mode, func(t *testing.T) {
			s, plan, b, _ := newTestStager(t, false)
			p, err := buildStagingPlan(s.config, s.policy, plan, "prep")
			if err != nil {
				t.Fatal(err)
			}
			// Focus on the final source boundary: all three payload bytes reach
			// the pipe before either the EOF probe or Close can fail.
			p.objects = p.objects[len(p.objects)-1:]
			b.mode = mode
			body := &stagingBody{ctx: context.Background(), blobs: b, plan: p}
			defer body.Close()
			data, err := io.ReadAll(stagingCreateInput(nil, nil, body))
			if mode == "normal" {
				if err != nil || string(data) != "abc"+stagingUploadComplete {
					t.Fatal("missing source acknowledgement", err)
				}
			} else if err == nil || string(data) != "abc" {
				t.Fatal("failed final payload issued a create trailer", err)
			}
		})
	}
}
