//go:build unix

package journal

import (
	"os"
	"syscall"
)

func lockExclusive(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return ErrBusy
	}
	return nil
}
