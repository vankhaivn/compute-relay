package kaggle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMonitorProcessAssemblesReadCoreWithoutCallingItsExecutionEntry(t *testing.T) {
	m := monitorFixture(t)
	c := m.config
	c.PythonExecutable = pythonForTest(t)
	// Compile/load the exact composed helper but replace main with an inert
	// repository fixture. This needs neither installed SDK nor any provider call.
	source := strings.Replace(monitorProgram(), "\nif __name__ == \"__main__\":", "\nmain=lambda:empty('unknown','missing_quota')\nif __name__ == \"__main__\":", 1)
	r, err := runMonitorSource(context.Background(), c, "quota", []byte("SYNTHETIC_TOKEN"), monitorRequest{Protocol: 1, Owner: c.AccountName}, source)
	if err != nil || !r.valid("quota") || r.Status != "unknown" {
		t.Fatal("fixed helper composition failed", r, err)
	}
}

func TestMonitorProcessIsolationLargeLogAndFailureBoundaries(t *testing.T) {
	m := monitorFixture(t)
	c := m.config
	c.PythonExecutable = pythonForTest(t)
	t.Setenv("KAGGLE_API_TOKEN", "AMBIENT_CANARY")
	t.Setenv("HTTPS_PROXY", "http://must-not-use.invalid")
	t.Setenv("PYTHONPATH", t.TempDir())
	source := `import os,sys,json,base64
assert sys.flags.isolated
assert not any(k in os.environ for k in ('KAGGLE_API_TOKEN','HTTPS_PROXY','PYTHONPATH'))
assert os.path.samefile(os.getcwd(),os.environ['HOME'])
assert sys.stdin.buffer.readline()==b'SYNTHETIC_STDIN\n'
r=json.loads(sys.stdin.buffer.readline())
assert r['protocol']==1 and sys.stdin.buffer.read()==b''
assert 'SYNTHETIC_STDIN' not in str(sys.argv)
print(json.dumps(dict(protocol=1,status='logs',reason='none',limit_ns='',used_ns='',reserved_ns='',text_b64=base64.b64encode(b'x'*65536).decode(),availability='delayed',truncated=True)))
`
	r, err := runMonitorSource(context.Background(), c, "logs", []byte("SYNTHETIC_STDIN"), monitorRequest{Protocol: 1, Owner: c.AccountName}, source)
	if err != nil || !r.valid("logs") || !r.Truncated {
		t.Fatal("bounded log was not readable", err)
	}
	for _, source := range []string{
		`print('SYNTHETIC_TOKEN')`,
		`print('x'*140000)`,
		`import sys;sys.stderr.write('x'*20000)`,
		`import sys;print('{"protocol":1,"status":"unknown","reason":"missing_quota","limit_ns":"","used_ns":"","reserved_ns":"","text_b64":"","availability":"","truncated":false}');sys.exit(23)`,
	} {
		if r, err := runMonitorSource(context.Background(), c, "quota", []byte("SYNTHETIC_TOKEN"), monitorRequest{Protocol: 1, Owner: c.AccountName}, source); err == nil || r != (monitorResponse{}) || strings.Contains(err.Error(), "SYNTHETIC_TOKEN") {
			t.Fatal("failed process returned data or a secret", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = runMonitorSource(ctx, c, "quota", nil, monitorRequest{}, `import time;time.sleep(60)`)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 4*time.Second {
		t.Fatal("read helper survived timeout", err)
	}
}
