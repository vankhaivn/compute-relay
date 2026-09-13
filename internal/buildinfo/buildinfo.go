// Package buildinfo exposes immutable metadata injected when the binary is built.
package buildinfo

// These values may be replaced with -ldflags at release build time.
var (
	Version = "dev"
	Commit  = "unknown"
	BuiltAt = "unknown"
)

// Info is the public, provider-neutral build identity of the local runtime.
type Info struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	BuiltAt string `json:"built_at"`
}

// Current returns a snapshot of the build metadata.
func Current() Info {
	return Info{
		Version: Version,
		Commit:  Commit,
		BuiltAt: BuiltAt,
	}
}
