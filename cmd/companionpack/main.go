// companionpack is the native, offline installer and discovery tool for companion archives.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/vankhaivn/compute-relay/internal/companion"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "Companion operation failed:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("use pack, install, or discover")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	var directory, archive, metadata, checksum string
	switch args[0] {
	case "pack":
		f.StringVar(&directory, "source", "", "assembled payload directory")
		f.StringVar(&archive, "output", "", "new tar.gz archive")
		f.StringVar(&metadata, "metadata", "", "build metadata JSON")
	case "install":
		f.StringVar(&directory, "dest", "", "new installation directory")
		f.StringVar(&archive, "archive", "", "companion tar.gz")
		f.StringVar(&checksum, "sha256", "", "trusted external archive checksum")
	case "discover":
		f.StringVar(&directory, "root", "", "installed bundle directory")
	default:
		return fmt.Errorf("use pack, install, or discover")
	}
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 || directory == "" {
		return fmt.Errorf("required arguments missing")
	}
	switch args[0] {
	case "pack":
		data, err := os.ReadFile(metadata)
		if err != nil {
			return err
		}
		var build companion.Build
		if err := json.Unmarshal(data, &build); err != nil {
			return err
		}
		sha, err := companion.Pack(directory, archive, build)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"sha256": sha})
	case "install":
		manifest, err := companion.Install(archive, checksum, directory)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"installed": true, "build": manifest.Build})
	default:
		result, err := companion.Discover(directory)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
}
