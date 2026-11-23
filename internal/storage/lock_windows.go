//go:build windows

package storage

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	lockfileExclusiveLock   = 0x00000002
	lockfileFailImmediately = 0x00000001
	// errorLockViolation 是 Windows 系统错误码 33 (ERROR_LOCK_VIOLATION)。
	// syscall 包未导出该常量，这里按官方数值定义。
	errorLockViolation syscall.Errno = 33
)

func platformLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	kernel32, err := syscall.LoadDLL("kernel32.dll")
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	proc, err := kernel32.FindProc("LockFileEx")
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	var ol syscall.Overlapped
	flags := uintptr(lockfileExclusiveLock | lockfileFailImmediately)
	r1, _, callErr := proc.Call(
		f.Fd(), flags, 0, 1, 0, uintptr(unsafe.Pointer(&ol)),
	)
	if r1 == 0 {
		_ = f.Close()
		if callErr != nil && callErr != errorLockViolation {
			return nil, callErr
		}
		return nil, ErrLocked
	}
	return f, nil
}
