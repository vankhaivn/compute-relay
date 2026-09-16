package kaggle

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"testing"
	"time"
)

func TestArtifactPackedProgramContainsFixedReadOnlyModules(t *testing.T) {
	program, err := artifactProgram()
	if err != nil || len(program) > 28000 {
		t.Fatal("helper exceeds portable command budget", len(program), err)
	}
	check := "import sys;ns={'__name__':'inspection'};exec(sys.argv[1],ns);assert ns['core'].__name__!='__main__';assert ns['contract'].MANIFEST=='control/execution-result.json';assert ns['DOWNLOAD'].endswith('/DownloadKernelOutput');assert not any(x.endswith('/SaveKernel') for x in ns['ALLOWED']);print('fixed-read-only')"
	out, err := exec.Command(pythonForTest(t), "-I", "-c", check, program).CombinedOutput()
	if err != nil || string(bytes.TrimSpace(out)) != "fixed-read-only" {
		t.Fatal("packed helper differs", err, string(out))
	}
}

func TestArtifactProcessIsolationStreamingAndLateExit(t *testing.T) {
	a, ref, data := artifactFixture(t)
	c := a.executor.stager.config
	c.PythonExecutable = pythonForTest(t)
	t.Setenv("KAGGLE_API_TOKEN", "AMBIENT_CANARY")
	t.Setenv("HTTPS_PROXY", "http://must-not-use.invalid")
	t.Setenv("PYTHONPATH", t.TempDir())
	prologue := `import os,sys,json
assert sys.flags.isolated
assert not any(k in os.environ for k in ('KAGGLE_API_TOKEN','HTTPS_PROXY','PYTHONPATH'))
assert os.path.samefile(os.getcwd(),os.environ['HOME'])
assert sys.stdin.buffer.readline()==b'SYNTHETIC_TOKEN\n'
r=json.loads(sys.stdin.buffer.readline())
assert sys.stdin.buffer.read()==b''
assert r['execution']['source']==''
assert r['target']['path']=='outputs/answer.txt'
assert 'SYNTHETIC_TOKEN' not in str(sys.argv)
`
	file := artifactFor(ref, "outputs/answer.txt", data["outputs/answer.txt"])
	for _, fault := range []string{"none", "late-exit", "overflow", "stderr"} {
		source := prologue + "sys.stdout.buffer.write(b'yes');sys.stdout.buffer.flush()\n"
		switch fault {
		case "late-exit":
			source += "sys.exit(23)\n"
		case "overflow":
			source += "sys.stdout.buffer.write(b'extra')\n"
		case "stderr":
			source += "sys.stderr.write('x'*20000)\n"
		}
		a.run = func(ctx context.Context, _ Config, mode string, token []byte, r artifactRequest, dst io.Writer) ([]byte, error) {
			return runArtifactSource(ctx, c, mode, token, r, dst, source)
		}
		var dst bytes.Buffer
		result, err := a.FetchArtifact(context.Background(), ref, file, &dst, file.Bytes)
		if fault == "none" {
			if err != nil || result.Bytes != 3 || dst.String() != "yes" {
				t.Fatal("binary transfer failed", err)
			}
		} else if err == nil {
			t.Fatal("invalid exit/stream accepted", fault)
		}
	}
}

func TestArtifactProcessDeadlineAndCatalogOverflowFailClosed(t *testing.T) {
	a, _, _ := artifactFixture(t)
	c := a.executor.stager.config
	c.PythonExecutable = pythonForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err := runArtifactSource(ctx, c, "catalog", []byte("SYNTHETIC_TOKEN"), a.request, nil, "import time;time.sleep(60)")
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 6*time.Second {
		t.Fatal("unbounded child", err)
	}
	if raw, err := runArtifactSource(context.Background(), c, "catalog", []byte("SYNTHETIC_TOKEN"), a.request, nil, "import sys;sys.stdout.buffer.write(b'x'*(8388608+1))"); err == nil || len(raw) != 0 {
		t.Fatal("oversized catalog escaped", err)
	}
}
