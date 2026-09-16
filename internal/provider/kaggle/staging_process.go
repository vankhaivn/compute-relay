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

//go:embed staging.py
var stagingSource string

func runStaging(ctx context.Context, c Config, mode string, secret []byte, p stagingPlan, blobs StagingBlobs) (stagingResponse, error) {
	return runStagingSource(ctx, c, mode, secret, p, blobs, stagingSource)
}

func runStagingSource(parent context.Context, c Config, mode string, secret []byte, p stagingPlan, blobs StagingBlobs, source string) (stagingResponse, error) {
	var result stagingResponse
	if mode != "create" && mode != "observe" {
		return result, ErrMode
	}
	info, err := os.Stat(c.PythonExecutable)
	if err != nil || !info.Mode().IsRegular() || runtime.GOOS == "windows" && !strings.EqualFold(filepath.Ext(c.PythonExecutable), ".exe") {
		return result, ErrProcess
	}
	work, err := os.MkdirTemp("", "compute-relay-staging-")
	if err != nil {
		return result, ErrProcess
	}
	defer os.RemoveAll(work) // Empty private home only; input bytes never touch this tree.
	ctx, cancel := context.WithTimeout(parent, time.Hour)
	defer cancel()
	deadline, _ := ctx.Deadline()
	seconds := int(time.Until(deadline).Seconds())
	if seconds < 1 {
		return result, context.DeadlineExceeded
	}
	if seconds > 3600 {
		seconds = 3600
	}
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
	header, err := json.Marshal(p.request)
	if err != nil || len(header) > 65536 {
		return result, ErrProtocol
	}
	prefix := make([]byte, 0, len(secret)+len(header)+2)
	prefix = append(prefix, secret...)
	prefix = append(prefix, '\n')
	prefix = append(prefix, header...)
	prefix = append(prefix, '\n')
	defer clear(prefix)
	body := &stagingBody{ctx: ctx, blobs: blobs, plan: p}
	defer body.Close()
	cmd.Stdin = bytes.NewReader(prefix)
	if mode == "create" {
		cmd.Stdin = stagingCreateInput(prefix, p.marker, body)
	}
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
	if closedObject(out.data, &result) != nil {
		return stagingResponse{}, ErrProtocol
	}
	return result, nil
}

// Open one original blob at a time. The source contract is cooperative; the fixed
// pool never spawns a replacement for a reader that ignores its context.
type stagingBody struct {
	ctx       context.Context
	blobs     StagingBlobs
	plan      stagingPlan
	index     int
	current   io.ReadCloser
	remaining int64
}

func (r *stagingBody) Close() error {
	if r.current != nil {
		err := r.current.Close()
		r.current = nil
		return err
	}
	return nil
}
func (r *stagingBody) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	for r.index < len(r.plan.objects) {
		if r.current == nil {
			m := r.plan.objects[r.index]
			opened, err := r.blobs.Open(r.ctx, m.WorkspaceID, m.ID)
			if err != nil {
				return 0, ErrStagingInput
			}
			r.current = opened
			r.remaining = m.Bytes
		}
		if r.remaining == 0 {
			var probe [1]byte
			n, err := r.current.Read(probe[:])
			if n != 0 || err != io.EOF {
				return 0, ErrStagingInput
			}
			if err := r.Close(); err != nil {
				return 0, ErrStagingInput
			}
			r.index++
			continue
		}
		if int64(len(b)) > r.remaining {
			b = b[:int(r.remaining)]
		}
		n, err := r.current.Read(b)
		r.remaining -= int64(n)
		if err == io.EOF && r.remaining == 0 {
			err = nil
		}
		if err != nil {
			return n, ErrStagingInput
		}
		return n, nil
	}
	return 0, io.EOF
}
