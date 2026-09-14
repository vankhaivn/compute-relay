//go:build !linux && !darwin && !windows

package blobfs

import "os"

func privateMode(os.FileMode) bool         { return false }
func acquireLock(*os.File) error           { return ErrUnavailable }
func releaseLock(*os.File) error           { return ErrUnavailable }
func diskAvailable(string) (uint64, error) { return 0, ErrUnavailable }
func syncDirectory(string) error           { return ErrUnavailable }

func prepareRootPermissions(string, bool) error { return nil }
func checkPrivatePath(string) error             { return nil }
