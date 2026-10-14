// Package cli contains the local command boundary for the pre-release binary.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/buildinfo"
	"github.com/vankhaivn/compute-relay/internal/bundlectl"
	"github.com/vankhaivn/compute-relay/internal/operatorcli"
	"github.com/vankhaivn/compute-relay/internal/runtimehost"
)

const usage = `Compute Relay (pre-release)

Usage:
  compute-relay help
  compute-relay version [--json]
  compute-relay bundle preview --root DIR --include PATH [--include PATH]
  compute-relay bundle create --root DIR --include PATH --output FILE
  compute-relay bundle inspect --file FILE

Bundle commands are local and never execute workload code.
` + "\n" + operatorcli.Usage

// Run retains the embeddable command boundary. The executable uses RunContext
// so interrupt/SIGTERM reaches serving, upload shutdown and finite local work.
func Run(args []string, stdout, stderr io.Writer) int {
	return RunContext(context.Background(), args, stdout, stderr)
}
func RunContext(parent context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		if _, err := io.WriteString(stdout, usage); err != nil {
			return 1
		}
		return 0
	}

	switch args[0] {
	case "help", "-h", "--help":
		if _, err := io.WriteString(stdout, usage); err != nil {
			return 1
		}
		return 0
	case "init", "state", "serve", "workspace", "token", "validate":
		return operatorcli.Run(parent, args, stdout, stderr, runtimehost.Command)
	case "bundle":
		ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
		defer cancel()
		if err := bundlectl.Run(ctx, args[1:], stdout, stderr); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			fmt.Fprintf(stderr, "compute-relay bundle: %v\n", err)
			return 2
		}
		return 0
	case "version":
		if err := runVersion(args[1:], stdout, stderr); err != nil {
			fmt.Fprintf(stderr, "compute-relay version: %v\n", err)
			return 2
		}
		return 0
	default:
		// A token pasted as an unknown command must not be reflected.
		_, _ = io.WriteString(stderr, "unknown command; use compute-relay help\n")
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
