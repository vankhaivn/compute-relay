// kagglepreflight is a finite local/read-only developer/operator check, not serve.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
)

type checker interface {
	Check(context.Context, kaggle.Mode) (kaggle.Report, error)
}

type factory func(kaggle.Config) (checker, error)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, func(c kaggle.Config) (checker, error) {
		resolver, err := credentials.NewEnvironment([]ports.CredentialRef{c.CredentialRef}, os.LookupEnv)
		if err != nil {
			return nil, err
		}
		return kaggle.New(c, resolver)
	})
	cancel()
	os.Exit(code)
}
func run(ctx context.Context, args []string, out, diagnostic io.Writer, makeChecker factory) int {
	flags := flag.NewFlagSet("kagglepreflight", flag.ContinueOnError)
	flags.SetOutput(io.Discard) // Flag errors may contain a mistakenly supplied secret.
	path := flags.String("config", "", "non-secret preflight JSON configuration")
	allow := flags.Bool("allow-read-only", false, "explicitly authorize account and quota reads; never compute")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(out, "Usage: kagglepreflight --config FILE [--allow-read-only]")
			fmt.Fprintf(out, "Local default checks Python %s, kaggle %s, kagglesdk %s; no credential lookup or network.\n", kaggle.PythonSeries, kaggle.ClientVersion, kaggle.SDKVersion)
			return 0
		}
		fmt.Fprintln(diagnostic, "Invalid flags; use --help. Do not pass credential values as arguments.")
		return 2
	}
	if *path == "" || flags.NArg() != 0 {
		fmt.Fprintln(diagnostic, "Exactly one --config file and no positional arguments are required.")
		return 2
	}
	info, err := os.Lstat(*path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 8192 {
		fmt.Fprintln(diagnostic, "Preflight configuration file unavailable or over limit.")
		return 1
	}
	f, err := os.Open(*path)
	if err != nil {
		fmt.Fprintln(diagnostic, "Preflight configuration file unavailable.")
		return 1
	}
	defer f.Close()
	current, err := f.Stat()
	if err != nil || !os.SameFile(info, current) {
		fmt.Fprintln(diagnostic, "Preflight configuration file changed.")
		return 1
	}
	raw, err := io.ReadAll(io.LimitReader(f, 8193))
	if err != nil {
		fmt.Fprintln(diagnostic, "Preflight configuration file unreadable.")
		return 1
	}
	c, err := kaggle.ParseConfig(raw)
	clear(raw)
	if err != nil {
		fmt.Fprintln(diagnostic, "Invalid non-secret preflight configuration; see docs/development/providers/kaggle-preflight.md.")
		return 1
	}
	service, err := makeChecker(c)
	if err != nil {
		fmt.Fprintln(diagnostic, "Preflight configuration unavailable.")
		return 1
	}
	mode := kaggle.Local
	if *allow {
		mode = kaggle.ReadOnly
	}
	report, err := service.Check(ctx, mode)
	if err != nil {
		fmt.Fprintln(diagnostic, "Preflight process failed or exceeded its deadline; no dispatch was attempted.")
		return 1
	}
	result := struct {
		InstanceID string        `json:"instance_id"`
		Revision   string        `json:"revision"`
		CheckedAt  time.Time     `json:"checked_at"`
		Report     kaggle.Report `json:"report"`
	}{c.InstanceID, c.Revision, time.Now().UTC(), report}
	if json.NewEncoder(out).Encode(result) != nil {
		return 1
	}
	if report.Problem != "none" {
		return 1
	}
	return 0
}
