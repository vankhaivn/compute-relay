package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/vankhaivn/compute-relay/internal/contracts"
)

func main() {
	root := flag.String("root", ".", "repository root")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: contractcheck [--root PATH] <check|write-lock>")
		os.Exit(2)
	}

	var err error
	switch flag.Arg(0) {
	case "check":
		err = contracts.Check(*root)
	case "write-lock":
		err = contracts.WriteLock(*root)
	default:
		fmt.Fprintf(os.Stderr, "unknown contractcheck command %q\n", flag.Arg(0))
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
