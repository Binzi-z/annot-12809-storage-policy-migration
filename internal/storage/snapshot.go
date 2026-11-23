package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// 快照布局（全部大端）：
//
//	magic(2)=0x5A53 | version(2)=1 | walSeq(8) | payloadLen(4) |
//	crc32(payload)(4) | payload
//
// 快照通过“临时文件 + fsync + rename + 目录 fsync”原子发布，
// 读端要么看到完整旧快照，要么看到完整新快照。
const snapVersion uint16 = 1

// Option 控制 Open 的行为。
type Option func(*openConfig)

type openConfig struct {
	forceUnlock bool
	salvage     bool
}

// WithForceUnlock 仅用于本地测试：发现残留锁时强制清除后重试。
func WithForceUnlock() Option {
	return func(c *openConfig) { c.forceUnlock = true }
}

// WithSalvage 打开抢救模式：已提交区域出现中间损坏时不截断、不拒绝启动，
// 而是保留现场，让调用方通过 ReplayPrefix 读取损坏点之前的数据。
// 抢救模式下 Append 会被显式拒绝，避免在不可读字节之后续写。
func WithSalvage() Option {
	return func(c *openConfig) { c.salvage = true }
}

// SaveSnapshot 原子写入一份带校验和的快照，记录其覆盖到的 WAL 帧序号。
func (s *Store) SaveSnapshot(walSeq uint64, payload []byte) error {
	if len(payload) > maxPayload {
		return errors.New("storage: snapshot payload too large")
	}
	var h [20]byte
	binary.BigEndian.PutUint16(h[0:2], snapMagic)
	binary.BigEndian.PutUint16(h[2:4], snapVersion)
	binary.BigEndian.PutUint64(h[4:12], walSeq)
	binary.BigEndian.PutUint32(h[12:16], uint32(len(payload)))
	binary.BigEndian.PutUint32(h[16:20], crc32.ChecksumIEEE(payload))

	dir := filepath.Join(s.dir, snapDirName)
	tmp, err := os.CreateTemp(dir, "tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(h[:]); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	final := s.SnapshotPath()
	if err := os.Rename(tmpName, final); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return syncDir(dir)
}

// LoadSnapshot 读取并校验最新快照。文件不存在返回 io.EOF；
// 任何头部或载荷校验失败都包装为 ErrCorruption。
func (s *Store) LoadSnapshot() (uint64, []byte, error) {
	f, err := os.Open(s.SnapshotPath())
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, io.EOF
		}
		return 0, nil, err
	}
	defer f.Close()
	var h [20]byte
	if _, err := io.ReadFull(f, h[:]); err != nil {
		return 0, nil, &CorruptionError{File: f.Name(), Reason: "snapshot header unreadable: " + err.Error()}
	}
	if binary.BigEndian.Uint16(h[0:2]) != snapMagic {
		return 0, nil, &CorruptionError{File: f.Name(), Reason: "bad snapshot magic"}
	}
	if binary.BigEndian.Uint16(h[2:4]) != snapVersion {
		return 0, nil, &CorruptionError{File: f.Name(), Reason: "unsupported snapshot version"}
	}
	walSeq := binary.BigEndian.Uint64(h[4:12])
	pLen := int(binary.BigEndian.Uint32(h[12:16]))
	wantCRC := binary.BigEndian.Uint32(h[16:20])
	if pLen > maxPayload {
		return 0, nil, &CorruptionError{File: f.Name(), Reason: "snapshot length out of range"}
	}
	payload := make([]byte, pLen)
	if _, err := io.ReadFull(f, payload); err != nil {
		return 0, nil, &CorruptionError{File: f.Name(), Reason: "snapshot payload truncated"}
	}
	if extra, _ := io.ReadAll(f); len(extra) > 0 {
		return 0, nil, &CorruptionError{File: f.Name(), Reason: "trailing bytes after snapshot payload"}
	}
	if crc32.ChecksumIEEE(payload) != wantCRC {
		return 0, nil, &CorruptionError{File: f.Name(), Reason: "snapshot payload crc mismatch"}
	}
	return walSeq, payload, nil
}
