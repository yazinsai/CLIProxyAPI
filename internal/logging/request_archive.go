package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

// RequestArchiveEnabled enables lossless compressed request log retention.
// Archives are never considered by ordinary log rotation or error-log cleanup.
func RequestArchiveEnabled() bool {
	return os.Getenv("CLI_PROXY_REQUEST_ARCHIVE") == "1"
}

// ArchiveRequestLog publishes a complete compressed record before removing its
// plaintext source. A failed write leaves the source available for recovery.
func ArchiveRequestLog(path string) error {
	if !RequestArchiveEnabled() {
		return nil
	}
	source, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = source.Close() }()
	info, err := source.Stat()
	if err != nil {
		return err
	}
	target, err := os.CreateTemp(filepath.Dir(path), ".request-archive-*.tmp")
	if err != nil {
		return err
	}
	tmp := target.Name()
	defer func() { _ = target.Close(); _ = os.Remove(tmp) }()
	encoder, err := zstd.NewWriter(target, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedBetterCompression), zstd.WithEncoderCRC(true))
	if err != nil {
		return err
	}
	_, errCopy := io.Copy(encoder, source)
	errFinish := encoder.Close()
	if errCopy != nil {
		return fmt.Errorf("archive request log: %w", errCopy)
	}
	if errFinish != nil {
		return errFinish
	}
	if err = target.Sync(); err != nil {
		return err
	}
	if err = target.Close(); err != nil {
		return err
	}
	if err = os.Chtimes(tmp, info.ModTime(), info.ModTime()); err != nil {
		return err
	}
	// Link is atomic and refuses to overwrite an existing historical record.
	destination := path + ".zst"
	if err = os.Link(tmp, destination); err != nil {
		return err
	}
	if err = syncArchiveDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	if err = source.Close(); err != nil {
		return err
	}
	if err = os.Remove(path); err != nil {
		return err
	}
	return syncArchiveDirectory(filepath.Dir(path))
}

func syncArchiveDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}
