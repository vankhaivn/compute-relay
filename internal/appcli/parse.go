// Package appcli supplies explicit application commands through the running API.
// It never opens the runtime database or constructs provider workers.
package appcli

import (
	"errors"
	"flag"
	"io"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/appclient"
)

var ErrArguments = errors.New("invalid application command")
var ErrHelp = errors.New("application help requested")

const Usage = `Application commands (running loopback API; no automatic retries):
  compute-relay object upload --workspace ID --token-file FILE --file INPUT
  compute-relay job validate|submit --workspace ID --token-file FILE --file JOB_JSON
  compute-relay job status --workspace ID --token-file FILE --id JOB_ID
  compute-relay job cancel|retry|reconcile|collect --workspace ID --token-file FILE --id JOB_ID --attempt ATTEMPT_ID --idempotency-key KEY
  compute-relay operation status --workspace ID --token-file FILE --id OPERATION_ID

All commands accept --url http://127.0.0.1:7331. Submit and control commands
require --idempotency-key; retry also requires --reason. Tokens come only from
an explicit private file, never an argument value or ambient provider credential.
202 means durable local acceptance, not execution. Status preserves unknown states.
`

type Options struct {
	Endpoint  string
	TokenFile string
	File      string
	Request   appclient.Request
}

func Parse(args []string) (Options, error) {
	var o Options
	o.Endpoint = "http://127.0.0.1:7331"
	if len(args) < 2 || len(args) > 32 {
		return o, ErrArguments
	}
	for _, arg := range args {
		if len(arg) > 4096 || strings.ContainsRune(arg, 0) {
			return o, ErrArguments
		}
	}
	if args[1] == "--help" || args[1] == "-h" {
		return o, ErrHelp
	}
	switch args[0] {
	case "object":
		if args[1] != "upload" {
			return o, ErrArguments
		}
		o.Request.Action = "upload"
	case "operation":
		if args[1] != "status" {
			return o, ErrArguments
		}
		o.Request.Action = "operation"
	case "job":
		switch args[1] {
		case "validate", "submit", "status", "cancel", "retry", "reconcile", "collect":
			o.Request.Action = args[1]
		default:
			return o, ErrArguments
		}
	default:
		return o, ErrArguments
	}
	flags := flag.NewFlagSet("application", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	seen := map[string]bool{}
	add := func(name string, dst *string) {
		flags.Func(name, "", func(value string) error {
			if seen[name] {
				return ErrArguments
			}
			seen[name] = true
			*dst = value
			return nil
		})
	}
	add("url", &o.Endpoint)
	add("token-file", &o.TokenFile)
	add("file", &o.File)
	add("workspace", &o.Request.Workspace)
	add("id", &o.Request.ID)
	add("attempt", &o.Request.Attempt)
	add("idempotency-key", &o.Request.Key)
	add("reason", &o.Request.Reason)
	if err := flags.Parse(args[2:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return o, ErrHelp
		}
		return o, ErrArguments
	}
	if flags.NArg() != 0 {
		return o, ErrArguments
	}
	allowed := map[string]bool{"url": true, "token-file": true, "workspace": true}
	r := o.Request
	switch r.Action {
	case "upload", "validate", "submit":
		allowed["file"] = true
		if o.File == "" {
			return o, ErrArguments
		}
		r.Body = strings.NewReader("x")
		r.Bytes = 1
		if r.Action == "upload" {
			r.SHA256 = strings.Repeat("0", 64)
		}
		if r.Action == "submit" {
			allowed["idempotency-key"] = true
		}
	case "status", "operation":
		allowed["id"] = true
	default:
		for _, k := range []string{"id", "attempt", "idempotency-key", "reason"} {
			allowed[k] = true
		}
	}
	for k := range seen {
		if !allowed[k] {
			return o, ErrArguments
		}
	}
	if _, err := appclient.Endpoint(o.Endpoint); err != nil || o.TokenFile == "" || appclient.Validate(r) != nil {
		return o, ErrArguments
	}
	return o, nil
}
