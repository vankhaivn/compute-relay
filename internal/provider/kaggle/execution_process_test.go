package kaggle

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestExecutionProcessKeepsSourceAndTokenOffArgumentsAndEnvironment(t *testing.T) {
	e := newExecutionFixture(t)
	c := e.stager.config
	c.PythonExecutable = pythonForTest(t)
	t.Setenv("KAGGLE_API_TOKEN", "AMBIENT_CANARY")
	t.Setenv("HTTPS_PROXY", "http://must-not-use.invalid")
	t.Setenv("PYTHONPATH", t.TempDir())
	source := `import os,sys,json
assert sys.flags.isolated
assert not any(k in os.environ for k in ('KAGGLE_API_TOKEN','HTTPS_PROXY','PYTHONPATH'))
assert os.path.samefile(os.getcwd(),os.environ['HOME'])
assert sys.stdin.buffer.readline()==b'SYNTHETIC_STDIN_TOKEN\n'
r=json.loads(sys.stdin.buffer.readline())
assert r['source'] and r['kernel_id']==''
assert sys.stdin.buffer.read()==b''
assert 'SYNTHETIC_STDIN_TOKEN' not in str(sys.argv)
assert r['source'] not in str(sys.argv)
print(json.dumps(dict(protocol=1,status='unknown',kernel_id='',reference='',version=0,source_sha256='',raw_state='')))
`
	r, err := runExecutionSource(context.Background(), c, "submit", []byte("SYNTHETIC_STDIN_TOKEN"), e.request, source)
	if err != nil || r.Status != "unknown" || !r.valid() {
		t.Fatal("isolated process protocol failed", r, err)
	}
}

func TestExecutionProcessRejectsLateExitOverflowAndMalformedAcknowledgement(t *testing.T) {
	e := newExecutionFixture(t)
	c := e.stager.config
	c.PythonExecutable = pythonForTest(t)
	for _, source := range []string{
		`print('SYNTHETIC_SECRET')`,
		`print('x'*20000)`,
		`import sys;sys.stderr.write('x'*20000)`,
		`import json,sys;print(json.dumps(dict(protocol=1,status='rejected',kernel_id='',reference='',version=0,source_sha256='',raw_state='')));sys.exit(23)`,
	} {
		if r, err := runExecutionSource(context.Background(), c, "submit", []byte("SYNTHETIC_TOKEN"), e.request, source); err == nil || r != (executionResponse{}) {
			t.Fatal("failed process exposed an acknowledgement", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err := runExecutionSource(ctx, c, "submit", []byte("SYNTHETIC_TOKEN"), e.request, `import time;time.sleep(60)`)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 6*time.Second {
		t.Fatal("execution helper survived its parent deadline", err)
	}
}
