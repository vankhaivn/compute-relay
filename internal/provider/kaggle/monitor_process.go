package kaggle

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

//go:embed monitor.py
var monitorSource string

// Reuse only the fixed SDK guard/identity decoder, not generated workload source.
// The module name prevents execution.py's command entry point from running. Its
// guard receives read_only mode and cannot arm SaveKernel or any cancellation.
func monitorProgram() string {
	return "import types\ncore=types.ModuleType('_compute_relay_read_core')\nexec(compile(" + strconv.Quote(executionSource) + ", 'compute-relay/execution-core.py', 'exec'), core.__dict__)\n" + monitorSource
}

func runMonitor(ctx context.Context, c Config, mode string, token []byte, r monitorRequest) (monitorResponse, error) {
	return runMonitorSource(ctx, c, mode, token, r, monitorProgram())
}

func runMonitorSource(parent context.Context, c Config, mode string, token []byte, r monitorRequest, source string) (monitorResponse, error) {
	var none monitorResponse
	if mode != "quota" && mode != "logs" {
		return none, ErrMode
	}
	info, err := os.Stat(c.PythonExecutable)
	if err != nil || !info.Mode().IsRegular() || runtime.GOOS == "windows" && !strings.EqualFold(filepath.Ext(c.PythonExecutable), ".exe") {
		return none, ErrProcess
	}
	header, err := json.Marshal(r)
	if err != nil || len(header) > 8192 {
		return none, ErrProtocol
	}
	defer clear(header)
	input := make([]byte, 0, len(token)+len(header)+2)
	input = append(input, token...)
	input = append(input, '\n')
	input = append(input, header...)
	input = append(input, '\n')
	defer clear(input)
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	work, err := os.MkdirTemp("", "compute-relay-monitor-")
	if err != nil {
		return none, ErrProcess
	}
	defer os.RemoveAll(work)
	cmd := exec.CommandContext(ctx, c.PythonExecutable, "-I", "-X", "utf8", "-c", source, mode)
	cmd.Dir = work
	cmd.Env = []string{"HOME=" + work, "USERPROFILE=" + work, "TMPDIR=" + work, "TMP=" + work, "TEMP=" + work, "LANG=C.UTF-8"}
	if runtime.GOOS == "windows" {
		for _, name := range []string{"SystemRoot", "WINDIR"} {
			if value, ok := os.LookupEnv(name); ok {
				cmd.Env = append(cmd.Env, name+"="+value)
			}
		}
	}
	cmd.Stdin = bytes.NewReader(input)
	out := &monitorOutput{limit: 128 << 10, cancel: cancel}
	diagnostic := &monitorOutput{limit: 16 << 10, discard: true, cancel: cancel}
	defer func() { clear(out.data) }()
	cmd.Stdout, cmd.Stderr = out, diagnostic
	cmd.WaitDelay = time.Second
	err = cmd.Run()
	if out.failed || diagnostic.failed {
		return none, ErrProcess
	}
	if ctx.Err() != nil {
		return none, ctx.Err()
	}
	if err != nil {
		return none, ErrProcess
	}
	var result monitorResponse
	if closedObject(out.data, &result) != nil || !result.valid(mode) {
		return none, ErrProtocol
	}
	return result, nil
}

type monitorOutput struct {
	data    []byte
	size    int
	limit   int
	discard bool
	failed  bool
	cancel  context.CancelFunc
}

func (w *monitorOutput) Write(p []byte) (int, error) {
	if w.failed || len(p) > w.limit-w.size {
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
