// Package cli contains the small local command boundary for the pre-release binary.
package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/vankhaivn/compute-relay/internal/buildinfo"
)

const usage = `Compute Relay (pre-release)

Usage:
  compute-relay help
  compute-relay version [--json]

The runtime and job commands are introduced by later milestones.
`

// Run executes the command and returns a process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = io.WriteString(stdout, usage)
		return 0
	}

	switch args[0] {
	case "help", "-h", "--help":
		_, _ = io.WriteString(stdout, usage)
		return 0
	case "version":
		if err := runVersion(args[1:], stdout, stderr); err != nil {
			fmt.Fprintf(stderr, "compute-relay version: %v\n", err)
			return 2
		}
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 2
	}
}

func runVersion(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("version", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "write machine-readable build metadata")

	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("version accepts no positional arguments")
	}

	info := buildinfo.Current()
	if *jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		return encoder.Encode(info)
	}

	_, err := fmt.Fprintf(stdout, "compute-relay %s (commit=%s built_at=%s)\n", info.Version, info.Commit, info.BuiltAt)
	return err
}
