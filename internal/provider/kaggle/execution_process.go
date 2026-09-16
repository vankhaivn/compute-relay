package kaggle

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

//go:embed execution.py
var executionSource string

func runExecution(ctx context.Context, c Config, mode string, secret []byte, request executionRequest) (executionResponse, error) {
	return runExecutionSource(ctx, c, mode, secret, request, executionSource)
}

// Source and token are stdin data, never command-line arguments or host files.
// The fixed SDK leaf process cannot run the generated remote script locally.
func runExecutionSource(parent context.Context, c Config, mode string, secret []byte, request executionRequest, source string) (executionResponse, error) {
	var result executionResponse
	if mode != "submit" && mode != "observe" && mode != "reconcile" {
		return result, ErrMode
	}
	info, err := os.Stat(c.PythonExecutable)
	if err != nil || !info.Mode().IsRegular() || runtime.GOOS == "windows" && !strings.EqualFold(filepath.Ext(c.PythonExecutable), ".exe") {
		return result, ErrProcess
	}
	header, err := json.Marshal(request)
	if err != nil || len(header) > 3<<20 {
		return result, ErrProtocol
	}
	input := make([]byte, 0, len(secret)+len(header)+2)
	input = append(input, secret...)
	input = append(input, '\n')
	input = append(input, header...)
	input = append(input, '\n')
	defer clear(input)
	defer clear(header)
	ctx, cancel := context.WithTimeout(parent, 5*time.Minute)
	defer cancel()
	deadline, _ := ctx.Deadline()
	seconds := int(time.Until(deadline).Seconds())
	if seconds < 1 {
		return result, context.DeadlineExceeded
	}
	work, err := os.MkdirTemp("", "compute-relay-execution-")
	if err != nil {
		return result, ErrProcess
	}
	defer os.RemoveAll(work)
	cmd := exec.CommandContext(ctx, c.PythonExecutable, "-I", "-X", "utf8", "-c", source, mode, strconv.Itoa(seconds))
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
	out := &boundedOutput{cancel: cancel}
	diagnostic := &boundedOutput{cancel: cancel, discard: true}
	defer func() { clear(out.data) }()
	cmd.Stdout, cmd.Stderr = out, diagnostic
	cmd.WaitDelay = time.Second
	err = cmd.Run()
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if err != nil || out.failed || diagnostic.failed {
		return result, ErrProcess
	}
	if closedObject(out.data, &result) != nil || !result.valid() {
		return executionResponse{}, ErrProtocol
	}
	return result, nil
}
