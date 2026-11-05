package logging

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestPackedArchiveNameReservationAndReaderLifetime(t *testing.T) {
	dir := t.TempDir()
	name := "v1-responses-2026-11-05T150000-12345678.log.zst"
	path := filepath.Join(dir, "requests-test.zip")
	stamp := time.Now().UnixNano()
	output, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(output)
	member, err := writer.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Store, Comment: strconv.FormatInt(stamp, 10)})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("compressed bytes")
	if _, err = member.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = output.Close(); err != nil {
		t.Fatal(err)
	}
	// A crash may leave duplicate loose files; listing remains logical.
	if err = os.WriteFile(filepath.Join(dir, name), payload, 0600); err != nil {
		t.Fatal(err)
	}
	entries, err := ListRequestLogFiles(dir)
	if err != nil || len(entries) != 1 || entries[0].Modified.UnixNano() != stamp {
		t.Fatalf("entries %v: %v", entries, err)
	}
	if err = os.Remove(filepath.Join(dir, name)); err != nil {
		t.Fatal(err)
	}
	source, err := OpenRequestArchive(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(source)
	if err != nil || !bytes.Equal(body, payload) {
		t.Fatalf("packed read: %v", err)
	}
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
	if file, errCreate := createUnarchivedLogFile(filepath.Join(dir, name[:len(name)-4])); !os.IsExist(errCreate) {
		if file != nil {
			_ = file.Close()
		}
		t.Fatalf("packed historical name reused: %v", errCreate)
	}
	if _, err = OpenRequestArchive(filepath.Join(dir, "missing.log.zst")); !os.IsNotExist(err) {
		t.Fatalf("missing record: %v", err)
	}
}
