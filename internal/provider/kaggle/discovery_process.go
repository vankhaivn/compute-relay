package kaggle

import (
	"bytes"
	"context"
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

//go:embed discovery.py
var discoverySource string

func discoveryProgram() string {
	return "import types\npreflight=types.ModuleType('_compute_relay_preflight')\nexec(compile(" +
		strconv.Quote(helperSource) + ", 'compute-relay/preflight.py', 'exec'), preflight.__dict__)\n" +
		"quota_decoder=types.ModuleType('_compute_relay_quota')\nexec(compile(" +
		strconv.Quote(monitorSource) + ", 'compute-relay/quota.py', 'exec'), quota_decoder.__dict__)\n" + discoverySource
}

func runDiscovery(ctx context.Context, python string, token []byte) (discoveryResponse, error) {
	return runDiscoverySource(ctx, python, token, discoveryProgram())
}

func runDiscoverySource(parent context.Context, python string, token []byte, source string) (discoveryResponse, error) {
	var none discoveryResponse
	info, err := os.Stat(python)
	if err != nil || !info.Mode().IsRegular() || runtime.GOOS == "windows" && !strings.EqualFold(filepath.Ext(python), ".exe") {
		return none, ErrProcess
	}
	work, err := os.MkdirTemp("", "compute-relay-discovery-")
	if err != nil {
		return none, ErrProcess
	}
	defer os.RemoveAll(work) // Own empty home/cwd; no provider data or credential files.
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, "-I", "-X", "utf8", "-c", source)
	cmd.Dir = work
	cmd.Env = []string{"HOME=" + work, "USERPROFILE=" + work, "TMPDIR=" + work, "TMP=" + work, "TEMP=" + work, "LANG=C.UTF-8"}
	if runtime.GOOS == "windows" {
		for _, name := range []string{"SystemRoot", "WINDIR"} {
			if value, ok := os.LookupEnv(name); ok {
				cmd.Env = append(cmd.Env, name+"="+value)
			}
		}
	}
	cmd.Stdin = bytes.NewReader(token)
	out := &boundedOutput{cancel: cancel}
	diagnostic := &boundedOutput{cancel: cancel, discard: true}
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
	var result discoveryResponse
	if closedObject(out.data, &result) != nil || !result.valid() {
		return none, ErrProtocol
	}
	return result, nil
}
