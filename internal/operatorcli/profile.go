package operatorcli

import (
	"errors"
	"flag"
	"io"
)

const ProfileUsage = `Local admission-profile commands (stop serve first):
  compute-relay profile apply --root DIR --file PRIVATE_PROFILE_JSON
  compute-relay profile show --root DIR --name NAME
  compute-relay profile grant|revoke --root DIR --workspace ID --name NAME

Apply selects an immutable revision for new admission only. Workspace access is
separate. No provider is configured, checked or started by these commands.
`

func parseProfile(args []string) (Request, error) {
	r := Request{Command: "profile"}
	if len(args) < 2 {
		return r, ErrArguments
	}
	if args[1] == "--help" || args[1] == "-h" {
		return r, ErrHelp
	}
	r.Action = args[1]
	allowed := map[string]bool{"root": true}
	switch r.Action {
	case "apply":
		allowed["file"] = true
	case "show":
		allowed["name"] = true
	case "grant", "revoke":
		allowed["name"], allowed["workspace"] = true, true
	default:
		return r, ErrArguments
	}
	fs := flag.NewFlagSet("profile", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	seen := map[string]bool{}
	for name, dst := range map[string]*string{"root": &r.Root, "file": &r.File, "name": &r.ID, "workspace": &r.Workspace} {
		fs.Func(name, "", func(value string) error {
			if seen[name] || !allowed[name] || value == "" {
				return ErrArguments
			}
			seen[name] = true
			*dst = value
			return nil
		})
	}
	if err := fs.Parse(args[2:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return r, ErrHelp
		}
		return r, ErrArguments
	}
	if fs.NArg() != 0 {
		return r, ErrArguments
	}
	for name := range allowed {
		if !seen[name] {
			return r, ErrArguments
		}
	}
	return r, nil
}
