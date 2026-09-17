package appcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/appclient"
	"github.com/vankhaivn/compute-relay/internal/artifactwire"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

const ArtifactUsage = `Published artifact commands (read-only; explicit attempt required):
  compute-relay job artifacts --workspace ID --token-file FILE --id JOB --attempt ATTEMPT [--limit 100] [--cursor CURSOR]
  compute-relay artifact show --workspace ID --token-file FILE --job JOB --attempt ATTEMPT --id ARTIFACT
  compute-relay artifact download --workspace ID --token-file FILE --job JOB --attempt ATTEMPT --id ARTIFACT --output NEW_FILE

All accept --url http://127.0.0.1:7331. Downloads require an existing private
parent directory and never overwrite a file. Paths returned by the server are
labels, never local destinations. No automatic retry, collection or compute.
A failed output receipt can follow a verified file publication; inspect the file.
`

type artifactOptions struct {
	action, endpoint, tokenFile, id, cursor, output string
	target                                        artifactwire.Target
	limit                                         int
}

func parseArtifacts(args []string) (artifactOptions, error) {
	o := artifactOptions{endpoint: "http://127.0.0.1:7331", limit: artifactwire.MaxPage}
	if len(args) < 2 || len(args) > 32 {
		return o, ErrArguments
	}
	for _, arg := range args {
		if len(arg) > 4096 || strings.ContainsRune(arg, 0) {
			return o, ErrArguments
		}
	}
	if args[0] == "artifact" && (args[1] == "--help" || args[1] == "-h") {
		return o, ErrHelp
	}
	if args[0] == "job" && args[1] == "artifacts" {
		o.action = "list"
	} else if args[0] == "artifact" && (args[1] == "show" || args[1] == "download") {
		o.action = args[1]
	} else {
		return o, ErrArguments
	}
	fs := flag.NewFlagSet("artifacts", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	seen := map[string]bool{}
	add := func(name string, dst *string) {
		fs.Func(name, "", func(value string) error {
			if seen[name] {
				return ErrArguments
			}
			seen[name], *dst = true, value
			return nil
		})
	}
	add("url", &o.endpoint)
	add("token-file", &o.tokenFile)
	add("workspace", &o.target.WorkspaceID)
	add("attempt", &o.target.AttemptID)
	if o.action == "list" {
		add("id", &o.target.JobID)
		add("cursor", &o.cursor)
		fs.Func("limit", "", func(value string) error {
			var err error
			o.limit, err = strconv.Atoi(value)
			if seen["limit"] || err != nil || strconv.Itoa(o.limit) != value || o.limit < 1 || o.limit > artifactwire.MaxPage {
				return ErrArguments
			}
			seen["limit"] = true
			return nil
		})
	} else {
		add("id", &o.id)
		add("job", &o.target.JobID)
		if o.action == "download" {
			add("output", &o.output)
		}
	}
	if err := fs.Parse(args[2:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return o, ErrHelp
		}
		return o, ErrArguments
	}
	if fs.NArg() != 0 || !o.target.Valid() || o.tokenFile == "" || o.action == "download" && o.output == "" || o.action != "list" && (!appclient.ValidID(o.id) || !strings.HasPrefix(o.id, "art_")) {
		return o, ErrArguments
	}
	if _, err := appclient.Endpoint(o.endpoint); err != nil {
		return o, ErrArguments
	}
	if o.cursor != "" {
		if len(o.cursor) < 70 || len(o.cursor) > 80 {
			return o, ErrArguments
		}
		if _, err := artifactwire.Offset(o.cursor, o.cursor[4:68], artifactwire.MaxFiles); err != nil {
			return o, ErrArguments
		}
	}
	return o, nil
}

type deliveryError struct{ published bool }
func (e *deliveryError) Error() string { return "artifact file delivery incomplete" }

func RunArtifacts(parent context.Context, args []string, out, diagnostic io.Writer) int {
	o, err := parseArtifacts(args)
	if errors.Is(err, ErrHelp) {
		if _, err := io.WriteString(out, ArtifactUsage); err != nil {
			return 1
		}
		return 0
	}
	if err != nil {
		_, _ = io.WriteString(diagnostic, "Invalid artifact command; use --help. Specify an explicit job and attempt, not token values.\n")
		return 2
	}
	budget := 30 * time.Second
	if o.action == "download" {
		budget = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	var result any
	published := false
	perform := func() error {
		if ctx.Err() != nil {
			return ErrArguments
		}
		if o.action == "download" {
			var err error
			o.output, err = artifactDestination(o.output)
			if err != nil {
				return err
			}
		}
		token, err := readSource(ctx, o.tokenFile, 64, true)
		if err != nil {
			return err
		}
		defer clear(token)
		if len(token) == 48 && token[47] == '\n' {
			token = token[:47]
		}
		if !appclient.ValidToken(token) {
			return ErrArguments
		}
		if o.action == "list" {
			p, err := appclient.ListArtifacts(ctx, o.endpoint, token, o.target, o.cursor, o.limit)
			result = p
			return err
		}
		meta, err := appclient.ArtifactMetadata(ctx, o.endpoint, token, o.target, o.id)
		if err != nil {
			return err
		}
		result = meta
		if o.action == "show" {
			return nil
		}
		err = publishArtifact(ctx, o.output, meta.Artifact, func(dst io.Writer) error {
			return appclient.DownloadArtifact(ctx, o.endpoint, token, meta, dst)
		})
		if err != nil {
			return err
		}
		published = true
		result = struct {
			artifactwire.Metadata
			Delivery string `json:"delivery"`
		}{meta, "verified-new-file"}
		return nil
	}
	err = perform()
	if err == nil {
		err = json.NewEncoder(out).Encode(result)
		if err == nil {
			return 0
		}
	}
	failure := &appclient.Failure{Stage: "artifact_delivery"}
	var remote *appclient.Failure
	if errors.As(err, &remote) {
		failure = remote
	}
	var delivery *deliveryError
	if errors.As(err, &delivery) {
		published = published || delivery.published
	}
	_ = json.NewEncoder(diagnostic).Encode(struct {
		Error                   *appclient.Failure `json:"error"`
		AutomaticRetryAttempted bool               `json:"automatic_retry_attempted"`
		DownloadMayBePublished  bool               `json:"download_may_be_published"`
		NextAction              string             `json:"next_action"`
	}{failure, false, published, "inspect_destination_and_original_artifact_pin"})
	return 1
}

func artifactDestination(output string) (string, error) {
	absolute, err := filepath.Abs(output)
	if err != nil || output == "" || statefs.CheckDir(filepath.Dir(absolute)) != nil {
		return "", &deliveryError{}
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil || statefs.CheckDir(parent) != nil {
		return "", &deliveryError{}
	}
	absolute = filepath.Join(parent, filepath.Base(absolute))
	if _, err := os.Lstat(absolute); !errors.Is(err, os.ErrNotExist) {
		return "", &deliveryError{}
	}
	return absolute, nil
}

// The same-directory hard link publishes without replacement on supported local
// filesystems. Unsupported linking fails closed; rename-overwrite is not a fallback.
// Private operator-controlled parents are required, not a hostile-host sandbox.
func publishArtifact(ctx context.Context, output string, pin artifactwire.File, fetch func(io.Writer) error) error {
	f, err := os.CreateTemp(filepath.Dir(output), ".compute-relay-download-*")
	if err != nil {
		return &deliveryError{}
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	if statefs.CheckFile(name) != nil {
		return &deliveryError{}
	}
	if err := fetch(f); err != nil {
		return err
	}
	if f.Sync() != nil {
		return &deliveryError{}
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil || artifactwire.Copy(ctx, io.Discard, f, pin) != nil {
		return &deliveryError{}
	}
	info, statErr := f.Stat()
	closeErr := f.Close()
	current, currentErr := os.Lstat(name)
	if statErr != nil || closeErr != nil || currentErr != nil || !os.SameFile(info, current) || statefs.CheckFile(name) != nil || ctx.Err() != nil {
		return &deliveryError{}
	}
	if err := os.Link(name, output); err != nil {
		return &deliveryError{}
	}
	if statefs.SyncDir(filepath.Dir(output)) != nil || statefs.CheckFile(output) != nil || ctx.Err() != nil {
		return &deliveryError{published: true}
	}
	return nil
}
