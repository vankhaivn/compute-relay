package appcli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"os"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/appclient"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func Run(ctx context.Context, args []string, out, diagnostic io.Writer) int {
	options, err := Parse(args)
	if errors.Is(err, ErrHelp) {
		if _, err := io.WriteString(out, Usage); err != nil {
			return 1
		}
		return 0
	}
	if err != nil {
		_, _ = io.WriteString(diagnostic, "Invalid application command; use --help. Do not pass token values as arguments.\n")
		return 2
	}
	raw, err := execute(ctx, options)
	defer clear(raw)
	if err == nil {
		var n int
		n, err = out.Write(append(raw, '\n'))
		if err == nil && n == len(raw)+1 {
			return 0
		}
		err = &appclient.Failure{Stage: "output", RequestMayHaveCommitted: mutating(options.Request.Action)}
	}
	failure := &appclient.Failure{Stage: "before_request"}
	var remote *appclient.Failure
	if errors.As(err, &remote) {
		failure = remote
	}
	_ = json.NewEncoder(diagnostic).Encode(struct {
		Error                   *appclient.Failure `json:"error"`
		AutomaticRetryAttempted bool               `json:"automatic_retry_attempted"`
		NextAction              string             `json:"next_action"`
	}{Error: failure, NextAction: "inspect_receipt_and_preserve_original_request_and_key"})
	return 1
}
func mutating(action string) bool {
	return action != "validate" && action != "status" && action != "operation"
}

func execute(parent context.Context, options Options) ([]byte, error) {
	budget := 30 * time.Second
	if options.Request.Action == "upload" {
		budget = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	if ctx.Err() != nil {
		return nil, ErrArguments
	}
	request := options.Request
	var source *os.File
	var info os.FileInfo
	var streamed *sourceReader
	var requestBytes []byte
	defer func() { clear(requestBytes) }()
	if request.Action == "upload" {
		var err error
		source, info, err = openSource(options.File, 2<<30)
		if err != nil {
			return nil, err
		}
		defer source.Close()
		h := sha256.New()
		n, err := io.Copy(h, io.LimitReader(contextReader{ctx, source}, (2<<30)+1))
		if err != nil || n != info.Size() || !unchanged(source, info) {
			return nil, ErrArguments
		}
		if _, err := source.Seek(0, io.SeekStart); err != nil {
			return nil, ErrArguments
		}
		request.Bytes, request.SHA256 = n, hex.EncodeToString(h.Sum(nil))
		streamed = &sourceReader{ctx: ctx, reader: source, digest: sha256.New()}
		request.Body = streamed
	} else if request.Action == "validate" || request.Action == "submit" {
		raw, err := readSource(ctx, options.File, 1<<20, false)
		if err != nil {
			return nil, err
		}
		defer clear(raw)
		parsed, err := admission.Parse(raw)
		if err != nil {
			return nil, ErrArguments
		}
		requestBytes = parsed.Canonical()
		request.Body, request.Bytes = bytes.NewReader(requestBytes), int64(len(requestBytes))
	}
	token, err := readSource(ctx, options.TokenFile, 64, true)
	if err != nil {
		return nil, err
	}
	defer clear(token)
	// Accept exactly the issuing command's optional final LF, not arbitrary whitespace.
	if len(token) == 48 && token[47] == '\n' {
		token = token[:47]
	}
	if !appclient.ValidToken(token) {
		return nil, ErrArguments
	}
	raw, callErr := appclient.Exchange(ctx, options.Endpoint, token, request)
	if source != nil {
		// Exchange joins the transport's reader before giving ownership back here.
		var extra [1]byte
		n, probeErr := source.Read(extra[:])
		valid := unchanged(source, info) && n == 0 && probeErr == io.EOF && !streamed.failed && streamed.bytes == request.Bytes && hex.EncodeToString(streamed.digest.Sum(nil)) == request.SHA256
		closeErr := source.Close()
		if !valid || closeErr != nil {
			clear(raw)
			return nil, &appclient.Failure{Stage: "source_acknowledgement", RequestMayHaveCommitted: true}
		}
	}
	return raw, callErr
}

// A fixed file path is opened once. Remote names never enter the host filesystem.
// Same-user concurrent file replacement/OS operations are not a hostile-host sandbox.
func openSource(path string, limit int64) (*os.File, os.FileInfo, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() > limit {
		return nil, nil, ErrArguments
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, ErrArguments
	}
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) || opened.Size() != before.Size() || !opened.ModTime().Equal(before.ModTime()) {
		_ = f.Close()
		return nil, nil, ErrArguments
	}
	return f, opened, nil
}
func unchanged(f *os.File, before os.FileInfo) bool {
	after, err := f.Stat()
	return err == nil && os.SameFile(before, after) && before.Size() == after.Size() && before.ModTime().Equal(after.ModTime())
}
func readSource(ctx context.Context, path string, limit int64, private bool) ([]byte, error) {
	if private && statefs.CheckFile(path) != nil {
		return nil, ErrArguments
	}
	f, info, err := openSource(path, limit)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(contextReader{ctx, f}, limit+1))
	same := unchanged(f, info)
	closeErr := f.Close()
	if readErr != nil || closeErr != nil || !same || int64(len(raw)) != info.Size() || ctx.Err() != nil {
		clear(raw)
		return nil, ErrArguments
	}
	return raw, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

type sourceReader struct {
	ctx    context.Context
	reader io.Reader
	digest hash.Hash
	bytes  int64
	failed bool
}

func (r *sourceReader) Read(p []byte) (int, error) {
	n, err := (contextReader{r.ctx, r.reader}).Read(p)
	if n > 0 {
		_, _ = r.digest.Write(p[:n])
		r.bytes += int64(n)
	}
	if err != nil && err != io.EOF {
		r.failed = true
	}
	return n, err
}
