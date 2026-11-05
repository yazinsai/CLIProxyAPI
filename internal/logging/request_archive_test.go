package logging

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestRequestArchiveRoundTripAndCollision(t *testing.T) {
	t.Setenv("CLI_PROXY_REQUEST_ARCHIVE", "1")
	dir := t.TempDir()
	path := filepath.Join(dir, "v1-responses-2026-11-05T150000-12345678.log")
	body := bytes.Repeat([]byte("complete request and streamed response\x00\xff\n"), 100000)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := ArchiveRequestLog(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("plaintext was not removed", err)
	}
	compressed, err := os.ReadFile(path + ".zst")
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	got, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("archive lost payload bytes")
	}
	if len(compressed) >= len(body)/10 {
		t.Fatal("repeated payload did not compress")
	}
	f, next, err := createUniqueLogFile(dir, filepath.Base(path))
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if next == path {
		t.Fatal("reused an archived filename")
	}
	// Refusing to overwrite an archive must retain the new source.
	if err := os.WriteFile(path, []byte("new record"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ArchiveRequestLog(path); err == nil {
		t.Fatal("overwrote historical record")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("lost source on failure", err)
	}
}

func TestStreamingLogDoesNotDropChunksUnderBackpressure(t *testing.T) {
	// More chunks than either production queue holds, consumed concurrently.
	writer := &FileStreamingLogWriter{chunkChan: make(chan []byte, 1)}
	done := make(chan []byte)
	go func() {
		var got []byte
		for i := 0; i < 10000; i++ {
			got = append(got, <-writer.chunkChan...)
		}
		done <- got
	}()
	for i := 0; i < 10000; i++ {
		writer.WriteChunkAsync([]byte("payload\n"))
	}
	if got := <-done; !bytes.Equal(got, bytes.Repeat([]byte("payload\n"), 10000)) {
		t.Fatal("dropped streaming data")
	}
}

func TestStreamingArchiveRetainsEveryChunk(t *testing.T) {
	t.Setenv("CLI_PROXY_REQUEST_ARCHIVE", "1")
	dir := t.TempDir()
	logger := NewFileRequestLogger(true, dir, "", 0)
	writer, err := logger.LogStreamingRequest("/v1/responses", "POST", nil, []byte("full request body"), "12345678")
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteStatus(200, nil); err != nil {
		t.Fatal(err)
	}
	chunk := []byte("data: complete streamed payload\n\n")
	for i := 0; i < 250; i++ {
		writer.WriteChunkAsync(chunk)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.log.zst"))
	if err != nil || len(paths) != 1 {
		t.Fatal("missing archive", err)
	}
	data, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	got, err := decoder.DecodeAll(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(got, chunk) != 250 || !bytes.Contains(got, []byte("full request body")) {
		t.Fatal("archive omitted request or streaming chunks")
	}
}
