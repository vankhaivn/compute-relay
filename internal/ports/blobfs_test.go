package ports_test

import (
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

// Compile-time compatibility with the original M2-04 infrastructure contract.
var _ ports.BlobStore = (*blobfs.Store)(nil)
