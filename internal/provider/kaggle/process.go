package kaggle

import (
	"bytes"
	"context"
	_ "embed"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

//go:embed preflight.py
var helperSource string

// This fixed helper never spawns children. It is not a general workload/process
// supervisor. Interpreter, its site packages and same-user host code are trusted.
func runPython(ctx context.Context, c Config, mode Mode, secret []byte) (Report, error) {
	return runHelper(ctx, c, mode, secret, helperSource)
}

func runHelper(ctx context.Context, c Config, mode Mode, secret []byte, source string) (Report, error) {
	info, err := os.Stat(c.PythonExecutable)
	if err != nil || !info.Mode().IsRegular() {
		return Report{}, ErrProcess
	}
	if runtime.GOOS == "windows" && !strings.EqualFold(filepath.Ext(c.PythonExecutable), ".exe") {
		return Report{}, ErrProcess
	}
	work, err := os.MkdirTemp("", "compute-relay-preflight-")
	if err != nil {
		return Report{}, ErrProcess
	}
	defer os.RemoveAll(work) // Own empty home/cwd, never provider data or credential files.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.PythonExecutable, "-I", "-X", "utf8", "-c", source, string(mode), c.AccountName)
	cmd.Dir = work
	cmd.Env = []string{"HOME=" + work, "USERPROFILE=" + work, "TMPDIR=" + work, "TMP=" + work, "TEMP=" + work, "LANG=C.UTF-8"}
	if runtime.GOOS == "windows" {
		for _, name := range []string{"SystemRoot", "WINDIR"} {
			if value, ok := os.LookupEnv(name); ok {
				cmd.Env = append(cmd.Env, name+"="+value)
			}
		}
	}
	cmd.Stdin = bytes.NewReader(secret)
	out := &boundedOutput{cancel: cancel}
	diagnostic := &boundedOutput{cancel: cancel, discard: true}
	defer func() { clear(out.data) }()
	cmd.Stdout = out
	cmd.Stderr = diagnostic
	cmd.WaitDelay = time.Second
	err = cmd.Run()
	if out.failed || diagnostic.failed {
		return Report{}, ErrProcess
	}
	if ctx.Err() != nil {
		return Report{}, ctx.Err()
	}
	if err != nil || out.failed || diagnostic.failed {
		return Report{}, ErrProcess
	}
	var result Report
	if closedObject(out.data, &result) != nil || !result.valid(mode) {
		return Report{}, ErrProtocol
	}
	return result, nil
}

type boundedOutput struct {
	data    []byte
	size    int
	failed  bool
	discard bool
	cancel  context.CancelFunc
}

func (w *boundedOutput) Write(p []byte) (int, error) {
	if len(p) > 16384-w.size {
		w.failed = true
		w.cancel()
		return 0, io.ErrShortWrite
	}
	w.size += len(p)
	if !w.discard {
		w.data = append(w.data, p...)
	}
	return len(p), nil
}
