package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/FrancisWorld/raftkv/internal/raft"
)

const snapMagic = "RKVSNP01"

func encodeSnapshot(s raft.Snapshot) []byte {
	b := make([]byte, 0, len(snapMagic)+24+len(s.Data)+4)
	b = append(b, snapMagic...)
	b = binary.LittleEndian.AppendUint64(b, s.Index)
	b = binary.LittleEndian.AppendUint64(b, s.Term)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(s.Data)))
	b = append(b, s.Data...)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crcTable))
}

func decodeSnapshot(b []byte) (raft.Snapshot, error) {
	const fixed = len(snapMagic) + 24 + 4
	if len(b) < fixed || string(b[:len(snapMagic)]) != snapMagic {
		return raft.Snapshot{}, fmt.Errorf("%w: snapshot header", ErrCorrupt)
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(body, crcTable) != sum {
		return raft.Snapshot{}, fmt.Errorf("%w: snapshot checksum", ErrCorrupt)
	}
	p := body[len(snapMagic):]
	s := raft.Snapshot{
		Index: binary.LittleEndian.Uint64(p[0:8]),
		Term:  binary.LittleEndian.Uint64(p[8:16]),
	}
	n := binary.LittleEndian.Uint64(p[16:24])
	if n != uint64(len(p)-24) {
		return raft.Snapshot{}, fmt.Errorf("%w: snapshot length", ErrCorrupt)
	}
	if n > 0 {
		s.Data = append([]byte(nil), p[24:]...)
	}
	return s, nil
}

func readSnapshot(path string) (raft.Snapshot, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return raft.Snapshot{}, nil
	}
	if err != nil {
		return raft.Snapshot{}, err
	}
	return decodeSnapshot(b)
}

// writeFileAtomic writes data to path so a crash leaves either the old or the
// new content, never a mix: temp file, fsync, rename, fsync of the directory.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(dirOf(path))
}
