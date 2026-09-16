package kaggle

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func pythonForTest(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"python3", "python"} {
		if p, e := exec.LookPath(name); e == nil {
			abs, e := filepath.Abs(p)
			if e != nil {
				t.Fatal(e)
			}
			return abs
		}
	}
	t.Skip("Python interpreter unavailable; exact SDK integration runs in Kaggle-client CI")
	return ""
}
func TestPreflightFixedHelperLocalProtocol(t *testing.T) {
	c := testConfig(t)
	c.PythonExecutable = pythonForTest(t)
	r, err := runPython(context.Background(), c, Local, nil)
	if err != nil || !r.valid(Local) || r.Authentication != "not_checked" {
		t.Fatal(r, err)
	}
}
func TestPreflightProcessIsolationAndBoundedFailures(t *testing.T) {
	c := testConfig(t)
	c.PythonExecutable = pythonForTest(t)
	t.Setenv("KAGGLE_API_TOKEN", "AMBIENT_CANARY")
	t.Setenv("PYTHONPATH", t.TempDir())
	t.Setenv("HTTPS_PROXY", "http://must-not-use.invalid")
	report := `{"protocol":1,"mode":"read_only","local":"ready","authentication":"verified","account_binding":"matched","quota":"unknown","batch_ready":false,"problem":"none"}`
	source := `import os,sys
assert sys.flags.isolated
assert not any(k in os.environ for k in ('KAGGLE_API_TOKEN','PYTHONPATH','HTTPS_PROXY'))
assert sys.stdin.buffer.read()==b'SYNTHETIC_STDIN'
assert 'SYNTHETIC_STDIN' not in str(sys.argv)
assert os.path.samefile(os.getcwd(),os.environ['HOME'])
print('` + report + `')
`
	if r, err := runHelper(context.Background(), c, ReadOnly, []byte("SYNTHETIC_STDIN"), source); err != nil || r != verified() {
		t.Fatal(r, err)
	}
	for _, source := range []string{`print('SYNTHETIC_SECRET')`, `print('x'*20000)`, `import sys;sys.stderr.write('x'*20000)`, `import sys;sys.exit(23)`} {
		if _, err := runHelper(context.Background(), c, Local, nil, source); err != ErrProcess && err != ErrProtocol {
			t.Fatal("unbounded or raw failure", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := runHelper(ctx, c, Local, nil, `import time;time.sleep(60)`)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 3*time.Second {
		t.Fatal("unbounded timeout", err)
	}
}
