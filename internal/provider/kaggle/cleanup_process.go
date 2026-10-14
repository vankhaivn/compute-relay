package kaggle

import (
	"bytes"
	"compress/zlib"
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"strconv"
	"time"
)

//go:embed cleanup.py
var cleanupSource string

func cleanupProgram() (string, error) {
	source := "import types\ncore=types.ModuleType('_relay_cleanup_core')\nexec(" + strconv.Quote(executionSource) + ",core.__dict__)\n" + cleanupSource
	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)
	if _, err := io.WriteString(w, source); err != nil {
		return "", ErrConfig
	}
	if err := w.Close(); err != nil {
		return "", ErrConfig
	}
	program := "import base64,zlib;exec(compile(zlib.decompress(base64.b64decode('" + base64.StdEncoding.EncodeToString(compressed.Bytes()) + "',validate=True)),'compute-relay/cleanup.py','exec'))"
	if len(program) > 28000 {
		return "", ErrConfig
	}
	return program, nil
}

func runCleanup(ctx context.Context, c Config, mode string, secret []byte, request cleanupRequest) (cleanupResponse, error) {
	source, err := cleanupProgram()
	if err != nil {
		return cleanupResponse{}, err
	}
	return runCleanupSource(ctx, c, mode, secret, request, source)
}

// Request and token are stdin data, never command-line arguments or host files.
// The fixed SDK leaf process has no workload execution entry point.
func runCleanupSource(parent context.Context, c Config, mode string, secret []byte, request cleanupRequest, source string) (cleanupResponse, error) {
	var result cleanupResponse
	if mode != "preview" && mode != "apply" && mode != "recover" {
		return result, ErrMode
	}
	info, err := os.Stat(c.PythonExecutable)
	if err != nil || !info.Mode().IsRegular() {
		return result, ErrProcess
	}
	header, err := json.Marshal(request)
	if err != nil || len(header) > 16<<10 || len(secret) < 1 || len(secret) > 8192 {
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
	work, err := os.MkdirTemp("", "compute-relay-cleanup-")
	if err != nil {
		return result, ErrProcess
	}
	defer os.RemoveAll(work)
	cmd := exec.CommandContext(ctx, c.PythonExecutable, "-I", "-X", "utf8", "-c", source, mode, strconv.Itoa(seconds))
	cmd.Dir = work
	cmd.Env = []string{"HOME=" + work, "USERPROFILE=" + work, "TMPDIR=" + work, "TMP=" + work, "TEMP=" + work, "LANG=C.UTF-8"}
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
		return cleanupResponse{}, ErrProtocol
	}
	return result, nil
}
