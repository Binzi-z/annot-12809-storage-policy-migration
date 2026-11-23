package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openTemp(t *testing.T) (*Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return s, dir
}

func collect(s *Store) ([][][]byte, error) {
	var got [][][]byte
	err := s.Replay(func(_ uint64, batch [][]byte) error {
		got = append(got, append([][]byte(nil), batch...))
		return nil
	})
	return got, err
}

func TestAppendAndReplay(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()

	if _, err := s.Append([][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append([][]byte{[]byte("c")}); err != nil {
		t.Fatal(err)
	}
	if s.LastSeq() != 3 {
		t.Fatalf("LastSeq=%d want 3", s.LastSeq())
	}
	got, err := collect(s)
	if err != nil {
		t.Fatal(err)
	}
	want := [][][]byte{{[]byte("a"), []byte("b")}, {[]byte("c")}}
	if len(got) != len(want) {
		t.Fatalf("batches=%d want %d", len(got), len(want))
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("batch %d len %d want %d", i, len(got[i]), len(want[i]))
		}
		for j := range want[i] {
			if !bytes.Equal(got[i][j], want[i][j]) {
				t.Fatalf("frame %d/%d = %q want %q", i, j, got[i][j], want[i][j])
			}
		}
	}
}

func TestTornBatchIsQuarantined(t *testing.T) {
	s, dir := openTemp(t)
	if _, err := s.Append([][]byte{[]byte("committed-1")}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendTorn([][]byte{[]byte("torn-a"), []byte("torn-b")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 强杀后重新打开：撕裂批量被截断，已提交数据完整可见。
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	got, err := collect(s2)
	if err != nil {
		t.Fatalf("replay after crash: %v", err)
	}
	if len(got) != 1 || string(got[0][0]) != "committed-1" {
		t.Fatalf("replay=%v want one committed batch", got)
	}
	if s2.LastSeq() != 1 {
		t.Fatalf("LastSeq=%d want 1", s2.LastSeq())
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "wal.log.torn*"))
	if len(matches) != 1 {
		t.Fatalf("torn quarantine files=%v want 1", matches)
	}

	// 撕裂帧的序号允许被新提交复用：重开后下一个提交从 seq=2 开始。
	if _, err := s2.Append([][]byte{[]byte("committed-2")}); err != nil {
		t.Fatal(err)
	}
	got, _ = collect(s2)
	if len(got) != 2 || string(got[1][0]) != "committed-2" {
		t.Fatalf("replay=%v want two committed batches", got)
	}
	if s2.LastSeq() != 2 {
		t.Fatalf("LastSeq=%d want 2 (torn seq 2 reused)", s2.LastSeq())
	}
}

func TestCommittedRegionCorruption(t *testing.T) {
	s, dir := openTemp(t)
	if _, err := s.Append([][]byte{bytes.Repeat([]byte("x"), 64)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append([][]byte{bytes.Repeat([]byte("y"), 64)}); err != nil {
		t.Fatal(err)
	}
	wal := s.WALPath()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 翻转第一个已提交帧载荷中的一个字节：CRC 失败且其后还有提交帧，
	// 必须按“中间损坏”拒绝启动，而不是静默跳过。
	if err := FlipByte(wal, int64(headerLen+5)); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorruption) {
		t.Fatalf("Open on corrupted WAL err=%v want ErrCorruption", err)
	}
}

func TestReplayPrefixSalvage(t *testing.T) {
	s, _ := openTemp(t)
	if _, err := s.Append([][]byte{[]byte("good-1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append([][]byte{[]byte("good-2")}); err != nil {
		t.Fatal(err)
	}
	wal := s.WALPath()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := FlipByte(wal, int64(headerLen+2)); err != nil { // 损坏第一个帧
		t.Fatal(err)
	}

	// 直接用底层扫描器验证抢救模式不吞数据、明确报告损坏点。
	f, err := os.Open(wal)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var n int
	_, scanErr := scanWAL(f, func(_ uint64, _ [][]byte) error {
		n++
		return nil
	})
	if !errors.Is(scanErr, ErrCorruption) {
		t.Fatalf("scanWAL err=%v want ErrCorruption", scanErr)
	}
	if n != 0 {
		t.Fatalf("delivered %d batches before first corruption, want 0", n)
	}
}

func TestSnapshotRoundTripAndCorruption(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()
	payload := []byte("snapshot-body")
	if err := s.SaveSnapshot(7, payload); err != nil {
		t.Fatal(err)
	}
	seq, got, err := s.LoadSnapshot()
	if err != nil || seq != 7 || !bytes.Equal(got, payload) {
		t.Fatalf("load seq=%d body=%q err=%v", seq, got, err)
	}

	// 第二次发布必须原子覆盖。
	if err := s.SaveSnapshot(9, []byte("next")); err != nil {
		t.Fatal(err)
	}
	if seq, got, err = s.LoadSnapshot(); err != nil || seq != 9 || string(got) != "next" {
		t.Fatalf("reload seq=%d body=%q err=%v", seq, got, err)
	}

	if err := FlipByte(s.SnapshotPath(), 22); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.LoadSnapshot(); !errors.Is(err, ErrCorruption) {
		t.Fatalf("LoadSnapshot err=%v want ErrCorruption", err)
	}
}

func TestLockExclusivity(t *testing.T) {
	s, dir := openTemp(t)
	defer s.Close()
	if _, err := Open(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open err=%v want ErrLocked", err)
	}
}

func TestGarbageTailIsTornNotCorruption(t *testing.T) {
	s, dir := openTemp(t)
	if _, err := s.Append([][]byte{[]byte("stable")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := AppendGarbage(filepath.Join(dir, walFileName), []byte{0x00, 0x01, 0x02, 0x03}); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("garbage tail should be treated as torn tail, got %v", err)
	}
	defer s2.Close()
	got, err := collect(s2)
	if err != nil || len(got) != 1 {
		t.Fatalf("replay=%v err=%v", got, err)
	}
}
