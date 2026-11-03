//go:build windows

package journal

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32    = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx = modkernel32.NewProc("LockFileEx")
)

const (
	lockfileExclusive = 0x00000002
	lockfileFailNow   = 0x00000001
)

func lockExclusive(file *os.File) error {
	var overlapped syscall.Overlapped
	r, _, _ := procLockFileEx.Call(
		file.Fd(),
		uintptr(lockfileExclusive|lockfileFailNow),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&overlapped)),
	)
	if r == 0 {
		return ErrBusy
	}
	return nil
}
