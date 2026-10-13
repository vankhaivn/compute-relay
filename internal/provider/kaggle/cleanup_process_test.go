package kaggle

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestCleanupProgramIsBoundedAndCompilesWithoutCallingProvider(t *testing.T) {
	program, err := cleanupProgram()
	if err != nil || len(program) > 28000 {
		t.Fatal("cleanup program exceeds command limit", err)
	}
	cmd := exec.Command(pythonForTest(t), "-I", "-c", program, "preview", "1")
	// Empty stdin guarantees no credential/provider call even when the SDK is installed.
	cmd.Stdin = strings.NewReader("")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupProcessKeepsTokenOffArgumentsAndEnvironment(t *testing.T) {
	c, _, _ := executionFixture(t)
	c.PythonExecutable = pythonForTest(t)
	t.Setenv("KAGGLE_API_TOKEN", "AMBIENT_CANARY")
	t.Setenv("HTTPS_PROXY", "http://must-not-use.invalid")
	source := `import os,sys,json
assert sys.flags.isolated
assert not any(k in os.environ for k in ('KAGGLE_API_TOKEN','HTTPS_PROXY','PYTHONPATH'))
assert os.path.samefile(os.getcwd(),os.environ['HOME'])
assert sys.stdin.buffer.readline()==b'SYNTHETIC_STDIN_TOKEN\n'
r=json.loads(sys.stdin.buffer.readline())
assert sys.stdin.buffer.read()==b''
assert 'SYNTHETIC_STDIN_TOKEN' not in str(sys.argv)
print('{"protocol":1,"status":"unknown"}')
`
	r, err := runCleanupSource(context.Background(), c, "recover", []byte("SYNTHETIC_STDIN_TOKEN"), cleanupRequest{Protocol: 1}, source)
	if err != nil || r.Status != "unknown" {
		t.Fatal(r, err)
	}
	for _, source := range []string{`print('SYNTHETIC_SECRET')`, `print('x'*20000)`, `import sys;sys.stderr.write('x'*20000)`, `import sys;print('{"protocol":1,"status":"deleted"}');sys.exit(23)`, `print('{"protocol":1,"status":"deleted","extra":true}')`} {
		if r, err := runCleanupSource(context.Background(), c, "apply", []byte("SYNTHETIC_TOKEN"), cleanupRequest{}, source); err == nil || r != (cleanupResponse{}) {
			t.Fatal("invalid acknowledgement escaped", r, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := runCleanupSource(ctx, c, "apply", []byte("SYNTHETIC_TOKEN"), cleanupRequest{}, `import time;time.sleep(60)`); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
