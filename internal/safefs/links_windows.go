//go:build windows

package safefs

import (
	"os"
	"syscall"
)

func singleLink(f *os.File, info os.FileInfo) bool {
	var data syscall.ByHandleFileInformation
	if syscall.GetFileInformationByHandle(syscall.Handle(f.Fd()), &data) != nil {
		return false
	}
	// Reject reparse points as well as hard-linked files, including non-symlink reparses.
	return data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 && (!info.Mode().IsRegular() || data.NumberOfLinks == 1)
}
