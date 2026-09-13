package main

import (
	"context"
	"fmt"
	"os"

	"github.com/vankhaivn/compute-relay/internal/devtool"
)

func main() {
	runner := devtool.Runner{
		Dir:    ".",
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	if err := runner.Run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
