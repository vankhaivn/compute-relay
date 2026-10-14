//go:build !linux && !darwin && !windows

package blobfs

func diskAvailable(string) (uint64, error) { return 0, ErrUnavailable }
