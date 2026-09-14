//go:build !linux && !darwin && !windows

package statefs

import "os"

func privateMode(os.FileMode) bool     { return false }
func secureNewRoot(string, bool) error { return ErrUnavailable }
func checkACL(string) error            { return ErrUnavailable }
func lock(*os.File) error              { return ErrUnavailable }
func unlock(*os.File) error            { return ErrUnavailable }
func SyncDir(string) error             { return ErrUnavailable }
