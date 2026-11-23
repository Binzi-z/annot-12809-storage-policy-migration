package storage

import (
	"os"
)

type lockHandle struct {
	file *os.File
}

func acquireLock(path string, opts ...Option) (*lockHandle, error) {
	cfg := openConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	f, err := platformLock(path)
	if err == nil {
		return &lockHandle{file: f}, nil
	}
	if !cfg.forceUnlock || err != ErrLocked {
		return nil, err
	}
	// 测试专用：清掉上一次强杀演示残留的锁文件后重试一次。
	if rmErr := os.Remove(path); rmErr != nil {
		return nil, rmErr
	}
	f, err = platformLock(path)
	if err != nil {
		return nil, err
	}
	return &lockHandle{file: f}, nil
}

func (l *lockHandle) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close() // 关闭句柄即释放 Unix flock / Windows LockFileEx
	l.file = nil
	return err
}

func (s *Store) releaseLock() error { return s.lock.release() }
