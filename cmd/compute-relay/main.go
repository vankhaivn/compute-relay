package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/vankhaivn/compute-relay/internal/cli"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.RunContext(ctx, os.Args[1:], os.Stdout, os.Stderr)
	cancel()
	os.Exit(code)
}
