//go:build windows

package storage

// syncDir 在 Windows 上是空操作：Windows 不支持对以只读方式打开的目录
// 句柄执行 FlushFileBuffers（会报 Access is denied），而 MoveFileEx 的
// rename 在同卷内本身具备元数据持久性保证，测试与本地单机场景不需要
// 额外的目录刷盘。
func syncDir(dir string) error { return nil }
