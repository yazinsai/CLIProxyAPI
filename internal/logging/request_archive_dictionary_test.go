package logging

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func requestArchiveDictionaryFixture(t *testing.T, dir string, id uint32) ([]byte, string) {
	t.Helper()
	var contents [][]byte
	for sample := 0; sample < 64; sample++ {
		var body bytes.Buffer
		for item := 0; item < 128; item++ {
			_, _ = fmt.Fprintf(&body, `{"marker":"synthetic request archive payload","sample":%d,"item":%d,"data":"complete bytes retained"}`+"\n", sample, item)
		}
		contents = append(contents, body.Bytes())
	}
	history := bytes.Join(contents[:4], nil)
	dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{ID: id, Contents: contents, History: history, Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedBetterCompression})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, requestChunkDirectory, "dictionaries", strconv.FormatUint(uint64(id), 10)+".zdict")
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, dictionary, 0600); err != nil {
		t.Fatal(err)
	}
	return dictionary, path
}

func writeRequestDictionaryFrame(t *testing.T, path string, body, dictionary []byte) {
	t.Helper()
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderDict(dictionary))
	if err != nil {
		t.Fatal(err)
	}
	compressed := encoder.EncodeAll(body, nil)
	if err = encoder.Close(); err != nil {
		t.Fatal(err)
	}
	var header zstd.Header
	if err = header.Decode(compressed); err != nil || header.DictionaryID == 0 {
		t.Fatalf("fixture does not declare a dictionary: %v", err)
	}
	if err = os.WriteFile(path, compressed, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveDictionaryDecodesManifestChunksAndPackedObjects(t *testing.T) {
	for _, packed := range []bool{false, true} {
		t.Run(fmt.Sprintf("packed=%v", packed), func(t *testing.T) {
			dir := t.TempDir()
			name := "v1-responses-2026-11-05T150000-12345678.log.zst"
			parts := [][]byte{bytes.Repeat([]byte("synthetic request archive payload complete bytes retained\n"), 100), []byte("legacy dictionary-zero chunk")}
			doc, want := dedupFixture(t, dir, name, 123, parts...)
			dictionary, _ := requestArchiveDictionaryFixture(t, dir, 56789)
			manifestDictionary := append([]byte(nil), dictionary...)
			binary.LittleEndian.PutUint32(manifestDictionary[4:8], 56790)
			manifestDictionaryPath := filepath.Join(dir, requestChunkDirectory, "dictionaries", "56790.zdict")
			if err := os.WriteFile(manifestDictionaryPath, manifestDictionary, 0600); err != nil {
				t.Fatal(err)
			}
			value := doc.Get("chunks.0.hash").String()
			writeRequestDictionaryFrame(t, filepath.Join(dir, requestChunkDirectory, value[:2], value+".zst"), parts[0], dictionary)
			writeRequestDictionaryFrame(t, filepath.Join(dir, requestManifestPath(name)), doc.Bytes(), manifestDictionary)
			if packed {
				packDedupObjectsFixture(t, filepath.Join(dir, requestManifestDirectory), "records-", false)
				packedShards := make(map[string]bool)
				for _, index := range []int{0, 1} {
					hash := doc.Get(fmt.Sprintf("chunks.%d.hash", index)).String()
					if packedShards[hash[:2]] {
						continue
					}
					packDedupObjectsFixture(t, filepath.Join(dir, requestChunkDirectory, hash[:2]), "chunks-", false)
					packedShards[hash[:2]] = true
				}
			}
			for attempt := 0; attempt < 2; attempt++ {
				reader, err := OpenRequestRecord(filepath.Join(dir, name))
				if err != nil {
					t.Fatal(err)
				}
				got, errRead := io.ReadAll(reader)
				errClose := reader.Close()
				if errRead != nil || errClose != nil || !bytes.Equal(got, want) {
					t.Fatalf("dictionary roundtrip: read=%v close=%v", errRead, errClose)
				}
				entries, errList := ListRequestLogFiles(dir)
				if errList != nil || len(entries) != 1 || entries[0].Name != name {
					t.Fatalf("dictionary metadata listing: %v %v", entries, errList)
				}
			}
		})
	}
}

func TestArchiveDictionaryRejectsMissingCorruptAndOutsideFiles(t *testing.T) {
	for _, failure := range []string{"missing", "corrupt", "mismatched ID", "oversized", "outside symlink"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			name := "v1-responses-2026-11-05T150000-12345678.log.zst"
			doc, _ := dedupFixture(t, dir, name, 123, []byte("payload"))
			dictionary, path := requestArchiveDictionaryFixture(t, dir, 56789)
			writeRequestDictionaryFrame(t, filepath.Join(dir, requestManifestPath(name)), doc.Bytes(), dictionary)
			switch failure {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				if err := os.WriteFile(path, []byte("not a dictionary"), 0600); err != nil {
					t.Fatal(err)
				}
			case "mismatched ID":
				binary.LittleEndian.PutUint32(dictionary[4:8], 123)
				if err := os.WriteFile(path, dictionary, 0600); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				if err := os.WriteFile(path, bytes.Repeat([]byte("x"), maxRequestDictionaryBytes+1), 0600); err != nil {
					t.Fatal(err)
				}
			case "outside symlink":
				outside := filepath.Join(t.TempDir(), "dictionary.zdict")
				if err := os.WriteFile(outside, dictionary, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, path); err != nil {
					t.Fatal(err)
				}
			}
			if reader, err := OpenRequestRecord(filepath.Join(dir, name)); err == nil {
				_ = reader.Close()
				t.Fatal("invalid dictionary opened a record")
			}
		})
	}
}

func TestArchiveDictionaryCacheRevalidatesChangedFiles(t *testing.T) {
	dir := t.TempDir()
	dictionary, path := requestArchiveDictionaryFixture(t, dir, 56789)
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	for attempt := 0; attempt < 2; attempt++ {
		if got, errRead := readRequestDictionary(root, 56789); errRead != nil || !bytes.Equal(got, dictionary) {
			t.Fatalf("dictionary cache: %v", errRead)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(dictionary[4:8], 123)
	if err = os.WriteFile(path, dictionary, 0600); err != nil {
		t.Fatal(err)
	}
	changed := info.ModTime().Add(time.Second)
	if err = os.Chtimes(path, changed, changed); err != nil {
		t.Fatal(err)
	}
	if _, err = readRequestDictionary(root, 56789); err == nil {
		t.Fatal("changed dictionary reused cached bytes")
	}
}

func TestArchiveDictionaryMissingChunkDictionaryFailsDuringRead(t *testing.T) {
	dir := t.TempDir()
	name := "v1-responses-2026-11-05T150000-12345678.log.zst"
	doc, _ := dedupFixture(t, dir, name, 123, []byte("payload"))
	dictionary, path := requestArchiveDictionaryFixture(t, dir, 56789)
	value := doc.Get("chunks.0.hash").String()
	writeRequestDictionaryFrame(t, filepath.Join(dir, requestChunkDirectory, value[:2], value+".zst"), []byte("payload"), dictionary)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenRequestRecord(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	if _, err = io.ReadAll(reader); err == nil {
		t.Fatal("chunk opened without its declared dictionary")
	}
}
