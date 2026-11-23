// Package storage 提供仅依赖 Go 标准库的嵌入式持久化：
// 一条 CRC 分帧的 append-only WAL（支持原子批量写）与带校验和的快照。
//
// 崩溃与损坏语义：
//   - 进程强杀时只可能留下“已完整提交”或“尾部撕裂”两种状态。撕裂的批量
//     （含未置 commit 位的帧及其残片）在 Open 时被隔离到 *.torn 文件并截断，
//     绝不作为有效数据重放。
//   - 已提交区域内任何魔数/长度/CRC/序号异常都返回 ErrCorruption，存储层
//     不会自行改写或跳过该区域，交由上层把对应资源送入隔离态。
//   - 快照与 WAL 分别带独立 CRC，快照损坏可凭仍保留的 WAL 重建（反之亦然）。
package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// 帧布局（全部大端）：
//
//	magic(2)=0x5A4C | flags(2) bit0=commit | seq(8) | payloadLen(4) | crc32(payload)(4) | payload
const (
	frameMagic   uint16 = 0x5A4C
	flagCommit   uint16 = 1 << 0
	headerLen           = 20
	maxPayload   int    = 64 * 1024 * 1024
	snapMagic    uint16 = 0x5A53
	walFileName         = "wal.log"
	snapDirName         = "snapshots"
	snapFileName        = "latest.bin"
)

var (
	// ErrCorruption 表示已提交持久化区域中出现不可自动恢复的损坏。
	ErrCorruption = errors.New("storage: corruption detected")
	// ErrLocked 表示数据目录已被另一进程独占持有。
	ErrLocked = errors.New("storage: directory is locked by another process")
)

// CorruptionError 携带损坏位置与原因，便于上层精确定位并审计。
type CorruptionError struct {
	File   string
	Offset int64
	Reason string
}

func (e *CorruptionError) Error() string {
	return "storage: corruption in " + e.File + " at offset " +
		itoa(e.Offset) + ": " + e.Reason
}

func (e *CorruptionError) Unwrap() error { return ErrCorruption }

// IsCorruption 判断错误是否为数据损坏（区别于普通 I/O 错误）。
func IsCorruption(err error) bool { return errors.Is(err, ErrCorruption) }

// Store 是单个本地进程独占的嵌入式存储句柄。
type Store struct {
	dir          string
	walPath      string
	wal          *os.File
	lock         *lockHandle
	nextSeq      uint64 // 下一个帧序号（lastSeq+1）
	salvage      bool
	salvagePoint int64
	closed       bool
}

// Salvage 报告存储是否处于抢救模式，以及第一个损坏点的字节偏移。
func (s *Store) Salvage() (bool, int64) { return s.salvage, s.salvagePoint }

// Open 打开（必要时创建）数据目录，执行崩溃恢复与损坏检测。
func Open(dir string, opts ...Option) (*Store, error) {
	cfg := openConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	if err := os.MkdirAll(filepath.Join(dir, snapDirName), 0o755); err != nil {
		return nil, err
	}
	// salvage 选项对锁获取无副作用，选项整体透传。
	lock, err := acquireLock(filepath.Join(dir, "lock"), opts...)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir: dir, walPath: filepath.Join(dir, walFileName),
		lock: lock, salvage: cfg.salvage, salvagePoint: -1,
	}

	f, err := os.OpenFile(s.walPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		s.releaseLock()
		return nil, err
	}
	s.wal = f
	if err := s.recover(); err != nil {
		_ = f.Close()
		s.releaseLock()
		return nil, err
	}
	return s, nil
}

// Dir 返回数据目录。
func (s *Store) Dir() string { return s.dir }

// WALPath / SnapshotPath 暴露文件路径，供测试注入崩溃与损坏。
func (s *Store) WALPath() string      { return s.walPath }
func (s *Store) SnapshotPath() string { return filepath.Join(s.dir, snapDirName, snapFileName) }

// Append 把一个批量原子写入 WAL 并 fsync。批量内所有帧要么整体可见，
// 要么整体不可见（崩溃后按撕裂尾部隔离）。返回最后一个帧的序号。
func (s *Store) Append(batch [][]byte) (uint64, error) {
	if len(batch) == 0 {
		return 0, errors.New("storage: empty batch")
	}
	if s.salvage {
		return 0, errors.New("storage: append rejected in salvage mode; corrupted region preserved")
	}
	var buf []byte
	last := s.nextSeq
	for i, payload := range byteSlices(batch) {
		if len(payload) > maxPayload {
			return 0, errors.New("storage: payload too large")
		}
		seq := s.nextSeq + uint64(i)
		flags := uint16(0)
		if i == len(batch)-1 {
			flags |= flagCommit
		}
		buf = appendFrame(buf, flags, seq, payload)
		last = seq
	}
	if _, err := s.wal.Write(buf); err != nil {
		return 0, err
	}
	if err := s.wal.Sync(); err != nil {
		return 0, err
	}
	s.nextSeq = last + 1
	return last, nil
}

// AppendTorn 模拟崩溃发生在批量提交之前：写入若干没有 commit 位的帧并
// flush 到操作系统，但逻辑上永不视为已提交。仅供故障注入测试使用。
// 注意它不推进 nextSeq——撕裂帧占用的序号在恢复后允许被新提交复用。
func (s *Store) AppendTorn(frames [][]byte) error {
	var buf []byte
	seq := s.nextSeq
	for _, p := range frames {
		buf = appendFrame(buf, 0, seq, p)
		seq++
	}
	if _, err := s.wal.Write(buf); err != nil {
		return err
	}
	return s.wal.Sync()
}

// Replay 按序号顺序把每个已提交批量交给 handle 处理。
func (s *Store) Replay(handle func(seq uint64, batch [][]byte) error) error {
	f, err := os.Open(s.walPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = scanWAL(f, handle)
	return err
}

// ReplayPrefix 以“尽力抢救”模式扫描：把第一个损坏点之前的所有已提交批量
// 正常投递，并返回损坏偏移；无损坏时返回 (-1, nil)。它绝不截断或修改
// WAL，供协调器进入 salvage 模式时使用——损坏点之后状态不明的资源会被
// 显式隔离（fail-closed），而不是静默丢弃数据。
func (s *Store) ReplayPrefix(handle func(seq uint64, batch [][]byte) error) (int64, error) {
	f, err := os.Open(s.walPath)
	if err != nil {
		return -1, err
	}
	defer f.Close()
	end, err := scanWAL(f, handle)
	if err != nil {
		if IsCorruption(err) {
			var off int64 = -1
			var ce *CorruptionError
			if errors.As(err, &ce) {
				off = ce.Offset
			}
			return off, err
		}
		return -1, err
	}
	_ = end
	return -1, nil
}

// LastSeq 返回已持久化的最大帧序号（0 表示空 WAL）。
func (s *Store) LastSeq() uint64 {
	if s.nextSeq == 0 {
		return 0
	}
	return s.nextSeq - 1
}

// Close 落盘并释放独占锁。可重复调用，第二次及以后直接返回 nil，
// 这样测试与崩溃演示路径上的“显式关闭 + defer 关闭”不会相互干扰。
func (s *Store) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	err1 := s.wal.Sync()
	err2 := s.wal.Close()
	err3 := s.releaseLock()
	return errors.Join(err1, err2, err3)
}

// recover 重放 WAL：隔离撕裂尾部、拒绝中间损坏，并校准 nextSeq。
func (s *Store) recover() error {
	info, err := s.wal.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		s.nextSeq = 1
		return nil
	}
	// 第一遍：定位最后一个有效提交批量的结束偏移，同时检测中间损坏。
	var lastCommittedEnd int64
	var expectSeq uint64 = 1
	got, err := scanWAL(s.wal, func(seq uint64, batch [][]byte) error {
		if seq != expectSeq {
			return &CorruptionError{File: s.walPath, Offset: -1,
				Reason: "sequence gap: want " + itoa(int64(expectSeq)) + " got " + itoa(int64(seq))}
		}
		expectSeq = seq + uint64(len(batch))
		return nil
	})
	if err != nil {
		if IsCorruption(err) && s.salvage {
			// 抢救模式：保留损坏现场，记录损坏点后按只读前缀视图启动。
			var ce *CorruptionError
			if errors.As(err, &ce) {
				s.salvagePoint = ce.Offset
			}
			s.nextSeq = expectSeq
			return nil
		}
		return err
	}
	lastCommittedEnd = got

	// 尾部若有残余字节，即崩溃留下的撕裂批量：隔离后截断。
	// 抢救模式不动任何字节，损坏现场必须原样保留供审计与人工处置。
	if info.Size() > lastCommittedEnd && !s.salvage {
		if err := s.quarantineTail(lastCommittedEnd, info.Size()); err != nil {
			return err
		}
		if err := s.wal.Truncate(lastCommittedEnd); err != nil {
			return err
		}
		if err := s.wal.Sync(); err != nil {
			return err
		}
	}
	// 关键：截断/恢复后必须把追加句柄定位到文件末尾，否则后续 Append
	// 会从陈旧偏移写入，覆盖已提交帧造成“中间损坏”。
	if _, err := s.wal.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	s.nextSeq = expectSeq
	return nil
}

func (s *Store) quarantineTail(start, end int64) error {
	if _, err := s.wal.Seek(start, io.SeekStart); err != nil {
		return err
	}
	tail := make([]byte, end-start)
	if _, err := io.ReadFull(s.wal, tail); err != nil {
		return err
	}
	qPath := s.walPath + ".torn"
	for i := 0; ; i++ {
		candidate := qPath
		if i > 0 {
			candidate = qPath + "-" + itoa(int64(i))
		}
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			qPath = candidate
			break
		}
	}
	if err := os.WriteFile(qPath, tail, 0o644); err != nil {
		return err
	}
	_, err := s.wal.Seek(0, io.SeekEnd)
	return err
}

// scanWAL 顺序扫描 WAL，提交一个完整批量就回调一次。返回最后提交批量的
// 结束偏移。遇到损坏时通过魔数再同步判断：损坏点之后仍存在有效提交帧，
// 说明是“中间损坏”（ErrCorruption）；否则视为撕裂尾部并正常结束。
func scanWAL(f *os.File, cb func(seq uint64, batch [][]byte) error) (int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	var (
		lastCommittedEnd int64
		batch            [][]byte
		batchStartSeq    uint64
		offset           int64
	)
	hdr := make([]byte, headerLen)
	for {
		_, err := io.ReadFull(f, hdr)
		if err == io.EOF {
			return lastCommittedEnd, nil
		}
		if err == io.ErrUnexpectedEOF {
			// 头部残片：仅可能是撕裂尾部。
			return lastCommittedEnd, checkTailOnly(offset, lastCommittedEnd)
		}
		if err != nil {
			return lastCommittedEnd, err
		}
		magic := binary.BigEndian.Uint16(hdr[0:2])
		flags := binary.BigEndian.Uint16(hdr[2:4])
		seq := binary.BigEndian.Uint64(hdr[4:12])
		pLen := int(binary.BigEndian.Uint32(hdr[12:16]))
		wantCRC := binary.BigEndian.Uint32(hdr[16:20])
		headerBad := magic != frameMagic || pLen > maxPayload
		var payload []byte
		if !headerBad {
			payload = make([]byte, pLen)
			if _, err := io.ReadFull(f, payload); err != nil {
				if err == io.ErrUnexpectedEOF || err == io.EOF {
					// 载荷残片：可能撕裂，也可能中间损坏后再同步。
					return lastCommittedEnd, checkTailOnly(offset, lastCommittedEnd)
				}
				return lastCommittedEnd, err
			}
			if crc32.ChecksumIEEE(payload) != wantCRC {
				headerBad = true
			}
		}
		frameEnd := offset + headerLen + int64(pLen)
		if headerBad {
			// 尝试在该帧之后再同步到下一个魔数；能找到有效提交帧 => 中间损坏。
			if found, ferr := resyncCommitted(f, frameEnd); ferr != nil {
				return lastCommittedEnd, ferr
			} else if found {
				return lastCommittedEnd, &CorruptionError{
					File: f.Name(), Offset: offset,
					Reason: "bad frame header or payload crc",
				}
			}
			return lastCommittedEnd, checkTailOnly(offset, lastCommittedEnd)
		}
		if len(batch) == 0 {
			batchStartSeq = seq
		} else if seq != batchStartSeq+uint64(len(batch)) {
			// 批量内序号不连续：不可能是撕裂（撕裂只发生在尾部），按损坏处理。
			return lastCommittedEnd, &CorruptionError{
				File: f.Name(), Offset: offset, Reason: "non-consecutive sequence inside batch",
			}
		}
		batch = append(batch, payload)
		offset = frameEnd
		if flags&flagCommit != 0 {
			if err := cb(batchStartSeq, batch); err != nil {
				return lastCommittedEnd, err
			}
			batch = nil
			lastCommittedEnd = offset
		}
	}
}

// checkTailOnly 用于扫描在 off 处失败的场景：若 off 严格位于最后一个已
// 提交批量之前，则属于中间损坏；落在提交边界之后则是可截断的撕裂尾部。
func checkTailOnly(off, lastCommittedEnd int64) error {
	if off < lastCommittedEnd {
		return &CorruptionError{File: "wal", Offset: off, Reason: "damaged committed region"}
	}
	return nil
}

// resyncCommitted 从 from 开始逐字节寻找帧魔数，并验证是否存在一个 CRC
// 正确且带 commit 位的帧。
func resyncCommitted(f *os.File, from int64) (bool, error) {
	if _, err := f.Seek(from, io.SeekStart); err != nil {
		return false, err
	}
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	size := info.Size()
	if from >= size {
		return false, nil
	}
	buf := make([]byte, size-from)
	if _, err := io.ReadFull(f, buf); err != nil {
		return false, err
	}
	for i := 0; i+headerLen <= len(buf); i++ {
		if binary.BigEndian.Uint16(buf[i:i+2]) != frameMagic {
			continue
		}
		flags := binary.BigEndian.Uint16(buf[i+2 : i+4])
		pLen := int(binary.BigEndian.Uint32(buf[i+12 : i+16]))
		wantCRC := binary.BigEndian.Uint32(buf[i+16 : i+20])
		end := i + headerLen + pLen
		if pLen > maxPayload || end > len(buf) {
			continue
		}
		if crc32.ChecksumIEEE(buf[i+headerLen:end]) != wantCRC {
			continue
		}
		if flags&flagCommit != 0 {
			return true, nil
		}
	}
	return false, nil
}

func appendFrame(dst []byte, flags uint16, seq uint64, payload []byte) []byte {
	var h [headerLen]byte
	binary.BigEndian.PutUint16(h[0:2], frameMagic)
	binary.BigEndian.PutUint16(h[2:4], flags)
	binary.BigEndian.PutUint64(h[4:12], seq)
	binary.BigEndian.PutUint32(h[12:16], uint32(len(payload)))
	binary.BigEndian.PutUint32(h[16:20], crc32.ChecksumIEEE(payload))
	dst = append(dst, h[:]...)
	dst = append(dst, payload...)
	return dst
}

func byteSlices(in [][]byte) [][]byte { return in }

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [24]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
