package storage

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

// 本文件中的函数仅供测试与演示“强杀 / 损坏 / 副本追赶”场景使用。
// 它们直接操作落盘文件，调用方必须先关闭 Store，或刻意绕过句柄。

// FrameOffsets 按分帧格式依次返回各帧起始字节偏移。遇到无法解析的帧
// （魔数/长度/CRC 异常）时停止并返回已解析部分与错误，供故障注入
// 精确定位“中间帧”而不是只能盲打固定偏移。
func FrameOffsets(path string) ([]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var offsets []int64
	hdr := make([]byte, headerLen)
	var off int64
	for {
		offsets = append(offsets, off)
		if _, err := io.ReadFull(f, hdr); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return offsets[:len(offsets)-1], nil
			}
			return offsets[:len(offsets)-1], err
		}
		if binary.BigEndian.Uint16(hdr[0:2]) != frameMagic {
			return offsets[:len(offsets)-1], ErrCorruption
		}
		pLen := int(binary.BigEndian.Uint32(hdr[12:16]))
		wantCRC := binary.BigEndian.Uint32(hdr[16:20])
		if pLen > maxPayload {
			return offsets[:len(offsets)-1], ErrCorruption
		}
		payload := make([]byte, pLen)
		if _, err := io.ReadFull(f, payload); err != nil {
			return offsets[:len(offsets)-1], err
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return offsets[:len(offsets)-1], ErrCorruption
		}
		off += headerLen + int64(pLen)
	}
}

// FlipByte 翻转目标文件中 offset 处的一个字节，模拟位腐/坏块。
func FlipByte(path string, offset int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return err
	}
	buf[0] ^= 0xFF
	_, err = f.WriteAt(buf, offset)
	return err
}

// AppendGarbage 在文件尾部追加一段无法成帧的字节，模拟坏块拖尾。
func AppendGarbage(path string, garbage []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(garbage); err != nil {
		return err
	}
	return f.Sync()
}

// TruncateTo 把文件截断到指定长度，模拟磁盘写穿/不完整副本。
func TruncateTo(path string, length int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(length)
}

// CopyFile 复制文件，用于构造“过期副本”。
func CopyFile(dst, src string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := out.ReadFrom(in); err != nil {
		return err
	}
	return out.Sync()
}
