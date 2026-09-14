// Package schemas contains the authoritative public schemas used by runtime validation.
package schemas

import "embed"

// Files is read-only. Runtime validators must resolve references only from these assets.
//
//go:embed *.schema.json
var Files embed.FS
