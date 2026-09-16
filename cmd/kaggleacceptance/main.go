// kaggleacceptance runs only the fixed opt-in GPU acceptance experiment.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/kaggleacceptance"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
)

type execution func(context.Context, kaggleacceptance.Options) (kaggleacceptance.Report, error)
type programHash func(context.Context) (domain.SHA256Digest, error)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := command(ctx, os.Args[1:], os.Stdout, os.Stderr, kaggleacceptance.Run, hashProgram)
	cancel()
	os.Exit(code)
}
func usage(out io.Writer) {
	fmt.Fprintln(out, "Usage: kaggleacceptance MODE --root DIRECTORY [MODE FLAGS] [--max-wait 10m]")
	fmt.Fprintln(out, "prepare: --config FILE --machine-shape NvidiaTeslaT4|NvidiaTeslaP100 (local only, new directory)")
	fmt.Fprintln(out, "submit:  --allow-private-staging --allow-gpu (one private staging/120-second GPU attempt)")
	fmt.Fprintln(out, "resume:  --allow-read-only (new process, observe and collect only)")
	fmt.Fprintln(out, "collect: --allow-read-only --collection-key KEY (explicit transfer retry, no compute)")
	fmt.Fprintln(out, "status:  local retained-state and artifact verification only")
	fmt.Fprintln(out, "Build once and retain the same executable. Stop/timeout does not cancel remote work. No cleanup or arbitrary workload flags.")
}
func command(ctx context.Context, args []string, out, diagnostic io.Writer, execute execution, hash programHash) int {
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		usage(out)
		return 0
	}
	if len(args) == 0 {
		usage(diagnostic)
		return 2
	}
	mode := args[0]
	if mode != "prepare" && mode != "submit" && mode != "resume" && mode != "collect" && mode != "status" {
		fmt.Fprintln(diagnostic, "Unknown acceptance mode; use --help.")
		return 2
	}
	flags := flag.NewFlagSet("kaggleacceptance", flag.ContinueOnError)
	flags.SetOutput(io.Discard) // Never echo a mistakenly supplied credential or path.
	root := flags.String("root", "", "dedicated acceptance directory")
	config := flags.String("config", "", "non-secret preflight JSON, prepare only")
	shape := flags.String("machine-shape", "", "explicit T4 or P100, prepare only")
	stage := flags.Bool("allow-private-staging", false, "authorize private synthetic input upload")
	gpu := flags.Bool("allow-gpu", false, "authorize one fixed bounded GPU attempt")
	read := flags.Bool("allow-read-only", false, "authorize provider observation and collection")
	key := flags.String("collection-key", "", "new explicit collection idempotency key")
	wait := flags.Duration("max-wait", 10*time.Minute, "local invocation budget, one second to thirty minutes")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			usage(out)
			return 0
		}
		fmt.Fprintln(diagnostic, "Invalid flags; use --help. Never pass credential values as arguments.")
		return 2
	}
	visited := map[string]bool{}
	flags.Visit(func(f *flag.Flag) { visited[f.Name] = true })
	allowed := map[string]bool{"root": true, "max-wait": true}
	switch mode {
	case "prepare":
		allowed["config"], allowed["machine-shape"] = true, true
	case "submit":
		allowed["allow-private-staging"], allowed["allow-gpu"] = true, true
	case "resume":
		allowed["allow-read-only"] = true
	case "collect":
		allowed["allow-read-only"], allowed["collection-key"] = true, true
	}
	invalid := flags.NArg() != 0 || *root == "" || *wait < time.Second || *wait > 30*time.Minute
	for name := range visited {
		if !allowed[name] {
			invalid = true
		}
	}
	switch mode {
	case "prepare":
		invalid = invalid || *config == "" || (*shape != "NvidiaTeslaT4" && *shape != "NvidiaTeslaP100")
	case "submit":
		invalid = invalid || !*stage || !*gpu
	case "resume":
		invalid = invalid || !*read
	case "collect":
		_, err := admission.KeyDigest(*key)
		invalid = invalid || !*read || err != nil
	}
	if invalid {
		fmt.Fprintln(diagnostic, "Missing or contradictory mode-specific authorization/configuration; use --help.")
		return 2
	}
	options := kaggleacceptance.Options{Mode: mode, Root: *root, MachineShape: *shape, AllowPrivateStaging: *stage, AllowGPU: *gpu, AllowReadOnly: *read, CollectionKey: *key, MaxWait: *wait}
	if mode == "prepare" {
		c, err := readConfig(*config)
		if err != nil {
			fmt.Fprintln(diagnostic, "Invalid non-secret configuration; preserve existing state and inspect the preflight guide.")
			return 1
		}
		options.Config = c
	}
	digest, err := hash(ctx)
	if err != nil || !digest.Valid() {
		fmt.Fprintln(diagnostic, "Cannot identify this executable; no acceptance action was started.")
		return 1
	}
	options.ProgramSHA256 = digest
	if mode == "submit" {
		if _, err := fmt.Fprintln(diagnostic, "Explicit authorization: private synthetic staging and one GPU attempt; remote wall 120s, setup 30s, finalization 15s, internet disabled. Local exit is not remote cancellation."); err != nil {
			return 1
		}
	}
	report, err := execute(ctx, options)
	if report.Protocol == 1 {
		if encodeErr := json.NewEncoder(out).Encode(report); encodeErr != nil {
			return 1
		}
	}
	if err != nil {
		fmt.Fprintln(diagnostic, "Acceptance did not qualify. Preserve the directory and original executable; inspect the sanitized report. Do not repeat compute after an uncertain submission.")
		return 1
	}
	if report.Protocol != 1 {
		return 1
	}
	switch mode {
	case "prepare":
		if report.Status == "prepared-local" {
			return 0
		}
	case "submit":
		if report.Status == "resume-required" {
			return 0
		}
	case "status":
		return 0 // Successful read is NOT an acceptance pass; inspect status/evidence.
	case "resume", "collect":
		if report.Status == "passed-live" && report.Evidence == "passed-live" && report.GPUVerified && report.RestartVerified {
			return 0
		}
	}
	return 1
}
func readConfig(path string) (kaggle.Config, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() > 8192 {
		return kaggle.Config{}, kaggle.ErrConfig
	}
	f, err := os.Open(path)
	if err != nil {
		return kaggle.Config{}, kaggle.ErrConfig
	}
	current, statErr := f.Stat()
	if statErr != nil || !os.SameFile(before, current) {
		_ = f.Close()
		return kaggle.Config{}, kaggle.ErrConfig
	}
	raw, readErr := io.ReadAll(io.LimitReader(f, 8193))
	closeErr := f.Close()
	defer clear(raw)
	if readErr != nil || closeErr != nil {
		return kaggle.Config{}, kaggle.ErrConfig
	}
	return kaggle.ParseConfig(raw)
}
func hashProgram(ctx context.Context) (domain.SHA256Digest, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !before.Mode().IsRegular() || before.Size() == 0 || before.Size() > 256<<20 {
		return "", kaggleacceptance.ErrState
	}
	h := sha256.New()
	buffer := make([]byte, 64<<10)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, readErr := f.Read(buffer)
		size += int64(n)
		if size > 256<<20 {
			return "", kaggleacceptance.ErrState
		}
		if n > 0 {
			_, _ = h.Write(buffer[:n])
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return "", readErr
		}
	}
	after, err := f.Stat()
	if err != nil {
		return "", err
	}
	if size != before.Size() || size != after.Size() || !before.ModTime().Equal(after.ModTime()) || !os.SameFile(before, after) {
		return "", kaggleacceptance.ErrState
	}
	return domain.SHA256Digest(hex.EncodeToString(h.Sum(nil))), nil
}
