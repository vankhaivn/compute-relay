// Package operatorcli parses the finite local operator surface without I/O.
package operatorcli

import (
	"context"
	"errors"
	"flag"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var ErrArguments = errors.New("invalid operator arguments")
var ErrHelp = errors.New("operator help requested")

type Request struct {
	Command               string
	Action                string
	Root                  string
	ID                    string
	Workspace             string
	Output                string
	File                  string
	Listen                string
	ProviderConfig        string
	ProviderProfile       string
	ProviderMachineShape  string
	MaxProviderAttempts   int
	AllowPrivateStaging   bool
	AllowGPU              bool
	ManagedPython         string
	ManagedMachineShape   string
	ManagedMaxWallSeconds int64
	ManagedMaxWorkers     int
	ManagedAllowInternet  bool
	Scopes                []string
	TTL                   time.Duration
}

const Usage = `Local runtime commands:
  compute-relay init --root DIR
  compute-relay state --root DIR
  compute-relay serve --root DIR [--listen 127.0.0.1:7331]
  compute-relay serve --root DIR --provider-config PRIVATE_JSON --provider-profile PROFILE \
    --provider-machine-shape NvidiaTeslaT4 --max-provider-attempts N --allow-private-staging --allow-gpu
  compute-relay serve --root DIR --managed-python ABS \
    [--managed-machine-shape NvidiaTeslaT4] [--managed-max-wall-seconds 1800] \
    [--managed-max-workers 2] [--managed-allow-internet]
  compute-relay workspace create|show|enable|disable --root DIR --id ID
  compute-relay token issue --root DIR --workspace ID --scope read [--scope write] --ttl 24h --output NEW_PRIVATE_FILE
  compute-relay token revoke --root DIR --id TOKEN_ID
  compute-relay validate --file JOB_JSON

Stop serve before local administration. Secrets are never printed; token output
requires a new file in an existing private directory. Plain serve keeps provider
workers disabled. Standalone provider mode requires every explicit provider flag
and a finite per-process attempt budget. Managed mode requires an absolute Python
interpreter path; each attempt needs separate API execution authorization. Its
policy flags grant no compute consent and cannot be mixed with standalone flags.
Managed shapes: NvidiaTeslaT4 or NvidiaTeslaP100; wall limit: 1-86400 seconds;
workers: 1-16; internet defaults to disabled. Local shutdown is not remote
cancellation. Validation is local schema validation, not admission.
` + "\n" + ProfileUsage

type Action func(context.Context, Request, io.Writer) error

func Run(ctx context.Context, args []string, out, diagnostic io.Writer, perform Action) int {
	request, err := Parse(args)
	if errors.Is(err, ErrHelp) {
		if _, err := io.WriteString(out, Usage); err != nil {
			return 1
		}
		return 0
	}
	if err != nil {
		_, _ = io.WriteString(diagnostic, "Invalid local command or flags; use --help. Do not pass token values as arguments.\n")
		return 2
	}
	if perform == nil {
		return 1
	}
	if err := perform(ctx, request, out); err != nil {
		_, _ = io.WriteString(diagnostic, "Local command did not complete. Preserve state and inspect any receipt; an error does not undo committed changes or prove remote work stopped.\n")
		return 1
	}
	return 0
}
func Parse(args []string) (Request, error) {
	r := Request{TTL: 24 * time.Hour, Listen: "127.0.0.1:7331"}
	if len(args) == 0 || len(args) > 64 {
		return r, ErrArguments
	}
	for _, v := range args {
		if len(v) > 4096 || strings.ContainsRune(v, 0) {
			return r, ErrArguments
		}
	}
	if args[0] == "profile" {
		return parseProfile(args)
	}
	r.Command = args[0]
	rest := args[1:]
	switch r.Command {
	case "workspace", "token":
		if len(rest) == 0 {
			return r, ErrArguments
		}
		if rest[0] == "--help" || rest[0] == "-h" {
			return r, ErrHelp
		}
		r.Action = rest[0]
		rest = rest[1:]
		if r.Command == "workspace" {
			if r.Action != "create" && r.Action != "show" && r.Action != "enable" && r.Action != "disable" {
				return r, ErrArguments
			}
		} else if r.Action != "issue" && r.Action != "revoke" {
			return r, ErrArguments
		}
	case "init", "state", "serve", "validate":
	default:
		return r, ErrArguments
	}
	fs := flag.NewFlagSet("operator", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	seen := map[string]bool{}
	add := func(name string, dst *string) {
		fs.Func(name, "", func(value string) error {
			if seen[name] {
				return ErrArguments
			}
			seen[name] = true
			*dst = value
			return nil
		})
	}
	add("root", &r.Root)
	add("id", &r.ID)
	add("workspace", &r.Workspace)
	add("output", &r.Output)
	add("file", &r.File)
	add("listen", &r.Listen)
	add("provider-config", &r.ProviderConfig)
	add("provider-profile", &r.ProviderProfile)
	add("provider-machine-shape", &r.ProviderMachineShape)
	add("managed-python", &r.ManagedPython)
	add("managed-machine-shape", &r.ManagedMachineShape)
	addBool := func(name string, dst *bool) {
		fs.BoolFunc(name, "", func(value string) error {
			if seen[name] {
				return ErrArguments
			}
			seen[name] = true
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return ErrArguments
			}
			*dst = parsed
			return nil
		})
	}
	addBool("allow-private-staging", &r.AllowPrivateStaging)
	addBool("allow-gpu", &r.AllowGPU)
	addBool("managed-allow-internet", &r.ManagedAllowInternet)
	addNumber := func(name string, maximum int64, set func(int64)) {
		fs.Func(name, "", func(value string) error {
			if seen[name] {
				return ErrArguments
			}
			seen[name] = true
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 1 || n > maximum || strconv.FormatInt(n, 10) != value {
				return ErrArguments
			}
			set(n)
			return nil
		})
	}
	addNumber("max-provider-attempts", 64, func(n int64) { r.MaxProviderAttempts = int(n) })
	addNumber("managed-max-wall-seconds", 86400, func(n int64) { r.ManagedMaxWallSeconds = n })
	addNumber("managed-max-workers", 16, func(n int64) { r.ManagedMaxWorkers = int(n) })
	fs.Func("ttl", "", func(value string) error {
		if seen["ttl"] {
			return ErrArguments
		}
		seen["ttl"] = true
		v, e := time.ParseDuration(value)
		if e != nil {
			return ErrArguments
		}
		r.TTL = v
		return nil
	})
	fs.Func("scope", "", func(value string) error {
		if len(r.Scopes) >= 5 {
			return ErrArguments
		}
		r.Scopes = append(r.Scopes, value)
		seen["scope"] = true
		return nil
	})
	if err := fs.Parse(rest); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return r, ErrHelp
		}
		return r, ErrArguments
	}
	if fs.NArg() != 0 {
		return r, ErrArguments
	}
	allowed := map[string]bool{"root": true}
	switch r.Command {
	case "serve":
		for _, k := range []string{"listen", "provider-config", "provider-profile", "provider-machine-shape", "max-provider-attempts", "allow-private-staging", "allow-gpu", "managed-python", "managed-machine-shape", "managed-max-wall-seconds", "managed-max-workers", "managed-allow-internet"} {
			allowed[k] = true
		}
	case "workspace":
		allowed["id"] = true
	case "token":
		if r.Action == "issue" {
			for _, k := range []string{"workspace", "scope", "ttl", "output"} {
				allowed[k] = true
			}
		} else {
			allowed["id"] = true
		}
	case "validate":
		allowed = map[string]bool{"file": true}
	}
	for k := range seen {
		if !allowed[k] {
			return r, ErrArguments
		}
	}
	if r.Command == "validate" {
		if r.File == "" {
			return r, ErrArguments
		}
	} else if r.Root == "" {
		return r, ErrArguments
	}
	if r.Command == "serve" {
		providerFlags := seen["provider-config"] || seen["provider-profile"] || seen["provider-machine-shape"] ||
			seen["max-provider-attempts"] || seen["allow-private-staging"] || seen["allow-gpu"]
		managedFlags := seen["managed-python"] || seen["managed-machine-shape"] || seen["managed-max-wall-seconds"] ||
			seen["managed-max-workers"] || seen["managed-allow-internet"]
		if managedFlags {
			if providerFlags || !filepath.IsAbs(r.ManagedPython) || strings.ContainsAny(r.ManagedPython, "\r\n\ufffd") {
				return r, ErrArguments
			}
			if !seen["managed-machine-shape"] {
				r.ManagedMachineShape = "NvidiaTeslaT4"
			}
			if !seen["managed-max-wall-seconds"] {
				r.ManagedMaxWallSeconds = 1800
			}
			if !seen["managed-max-workers"] {
				r.ManagedMaxWorkers = 2
			}
			if r.ManagedMachineShape != "NvidiaTeslaT4" && r.ManagedMachineShape != "NvidiaTeslaP100" {
				return r, ErrArguments
			}
		}
		if providerFlags && (r.ProviderConfig == "" || r.ProviderProfile == "" ||
			(r.ProviderMachineShape != "NvidiaTeslaT4" && r.ProviderMachineShape != "NvidiaTeslaP100") ||
			r.MaxProviderAttempts < 1 || !r.AllowPrivateStaging || !r.AllowGPU) {
			return r, ErrArguments
		}
	}
	if r.Command == "workspace" || r.Command == "token" && r.Action == "revoke" {
		if r.ID == "" {
			return r, ErrArguments
		}
	}
	if r.Command == "token" && r.Action == "issue" {
		if r.Workspace == "" || r.Output == "" || len(r.Scopes) == 0 || r.TTL < time.Minute || r.TTL > 30*24*time.Hour {
			return r, ErrArguments
		}
		used := map[string]bool{}
		for _, s := range r.Scopes {
			if used[s] || (s != "read" && s != "write" && s != "operate" && s != "manage" && s != "execute") {
				return r, ErrArguments
			}
			used[s] = true
		}
	}
	for _, v := range []string{r.Root, r.Output, r.File, r.ID, r.Workspace, r.Listen, r.ProviderConfig, r.ProviderProfile, r.ProviderMachineShape, r.ManagedPython, r.ManagedMachineShape} {
		if strings.ContainsRune(v, 0) {
			return r, ErrArguments
		}
	}
	return r, nil
}
