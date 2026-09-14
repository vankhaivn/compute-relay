//go:build darwin

package safefs

import (
	"os"
	"syscall"
)

func singleLink(_ *os.File, info os.FileInfo) bool {
	return !info.Mode().IsRegular() || info.Sys().(*syscall.Stat_t).Nlink == 1
}
