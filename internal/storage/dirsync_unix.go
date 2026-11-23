//go:build !windows

package storage

import "os"

// syncDir 在 Unix 上 fsync 目录，保证 rename 结果落盘。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
