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
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

//go:embed artifact_contract.py
var artifactContractSource string

//go:embed artifacts.py
var artifactSource string

// Pack only fixed reviewed helper code, never workload source or credentials.
// Compression keeps the command below Windows' command-line limit. Modules use
// non-main names, so the execution core's mutation entry point is never invoked.
func artifactProgram() (string, error) {
	source := "import types\ncore=types.ModuleType('_relay_artifact_core')\nexec(" + strconv.Quote(executionSource) + ",core.__dict__)\ncontract=types.ModuleType('_relay_artifact_contract')\nexec(" + strconv.Quote(artifactContractSource) + ",contract.__dict__)\n" + artifactSource
	if len(source) > 1<<20 {
		return "", ErrConfig
	}
	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)
	if _, err := io.WriteString(w, source); err != nil {
		return "", ErrConfig
	}
	if err := w.Close(); err != nil {
		return "", ErrConfig
	}
	program := "import base64,zlib;exec(compile(zlib.decompress(base64.b64decode('" + base64.StdEncoding.EncodeToString(compressed.Bytes()) + "',validate=True)),'compute-relay/artifacts.py','exec'))"
	if len(program) > 28000 {
		return "", ErrConfig
	}
	return program, nil
}

func runArtifacts(ctx context.Context, c Config, mode string, token []byte, request artifactRequest, dst io.Writer) ([]byte, error) {
	source, err := artifactProgram()
	if err != nil {
		return nil, err
	}
	return runArtifactSource(ctx, c, mode, token, request, dst, source)
}

// The streaming stdout destination is temporary. A successful pipe EOF alone
// is insufficient; Wait and the independent caller checksum must also succeed.
func runArtifactSource(parent context.Context, c Config, mode string, token []byte, request artifactRequest, dst io.Writer, source string) ([]byte, error) {
	if mode != "catalog" && mode != "fetch" || mode == "fetch" && dst == nil {
		return nil, ErrMode
	}
	info, err := os.Stat(c.PythonExecutable)
	if err != nil || !info.Mode().IsRegular() || runtime.GOOS == "windows" && !strings.EqualFold(filepath.Ext(c.PythonExecutable), ".exe") {
		return nil, ErrProcess
	}
	header, err := json.Marshal(request)
	if err != nil || len(header) > 128<<10 || len(token) < 1 || len(token) > 8192 {
		return nil, ErrProtocol
	}
	defer clear(header)
	input := make([]byte, 0, len(token)+len(header)+2)
	input = append(input, token...)
	input = append(input, '\n')
	input = append(input, header...)
	input = append(input, '\n')
	defer clear(input)
	ctx, cancel := context.WithTimeout(parent, 30*time.Minute)
	defer cancel()
	deadline, _ := ctx.Deadline()
	seconds := int(time.Until(deadline).Seconds())
	if seconds < 1 {
		return nil, context.DeadlineExceeded
	}
	work, err := os.MkdirTemp("", "compute-relay-artifacts-")
	if err != nil {
		return nil, ErrProcess
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
	out := &monitorOutput{limit: artifactCatalogBytes, cancel: cancel}
	diagnostic := &monitorOutput{limit: 16 << 10, discard: true, cancel: cancel}
	defer func() { clear(out.data) }()
	cmd.Stdout = out
	if mode == "fetch" {
		cmd.Stdout = dst
	}
	cmd.Stderr = diagnostic
	cmd.WaitDelay = time.Second
	err = cmd.Run()
	if out.failed || diagnostic.failed {
		return nil, ErrProcess
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, ErrProcess
	}
	if mode == "catalog" {
		return append([]byte(nil), out.data...), nil
	}
	return nil, nil
}
