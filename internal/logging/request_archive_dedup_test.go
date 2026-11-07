package logging

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/klauspost/compress/zstd"
)

func writeDedupCompressedFixture(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	compressed := encoder.EncodeAll(body, nil)
	if err = encoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, compressed, 0600); err != nil {
		t.Fatal(err)
	}
}

func dedupFixture(t *testing.T, dir, name string, stamp int64, parts ...[]byte) (*ojson.Doc, []byte) {
	t.Helper()
	var body []byte
	chunks := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		sum := sha256.Sum256(part)
		value := hex.EncodeToString(sum[:])
		chunks = append(chunks, map[string]interface{}{"hash": value, "size": len(part)})
		writeDedupCompressedFixture(t, filepath.Join(dir, requestChunkDirectory, value[:2], value+".zst"), part)
		body = append(body, part...)
	}
	sum := sha256.Sum256(body)
	doc := ojson.NewDocString(`{}`)
	_ = doc.Set("schema_version", 1)
	_ = doc.Set("name", name)
	_ = doc.Set("modified_ns", stamp)
	_ = doc.Set("original_bytes", len(body))
	_ = doc.Set("legacy_compressed_bytes", 1234)
	_ = doc.Set("sha256", hex.EncodeToString(sum[:]))
	_ = doc.Set("chunks", chunks)
	writeDedupCompressedFixture(t, filepath.Join(dir, requestManifestPath(name)), doc.Bytes())
	return doc, body
}

func TestDeduplicatedArchiveRequestRecordPreservesBytesListingAndNames(t *testing.T) {
	dir := t.TempDir()
	name := "v1-responses-2026-11-05T150000-12345678.log.zst"
	stamp := time.Date(2026, 11, 5, 15, 0, 0, 123456789, time.UTC).UnixNano()
	part := bytes.Repeat([]byte{0, 255, 10, 128, 0, 2}, 20000)
	_, want := dedupFixture(t, dir, name, stamp, part, []byte("middle payload"), part)
	// A durable manifest wins over legacy sources retained during crash recovery.
	writeDedupCompressedFixture(t, filepath.Join(dir, name), []byte("stale duplicate"))
	reader, err := OpenRequestRecord(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("dedup read lost bytes: %v", err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := ListRequestLogFiles(dir)
	if err != nil || len(entries) != 1 || entries[0].Name != name || entries[0].Modified.UnixNano() != stamp || entries[0].Size != 1234 {
		t.Fatalf("dedup metadata changed: %v %v", entries, err)
	}
	if err = os.Remove(filepath.Join(dir, name)); err != nil {
		t.Fatal(err)
	}
	if file, errCreate := createUnarchivedLogFile(filepath.Join(dir, strings.TrimSuffix(name, ".zst"))); !os.IsExist(errCreate) {
		if file != nil {
			_ = file.Close()
		}
		t.Fatalf("deduplicated name reused: %v", errCreate)
	}
}

func TestDeduplicatedArchiveRequestRecordRejectsDamagedChunks(t *testing.T) {
	for _, failure := range []string{"missing", "different bytes", "oversized", "damaged frame", "record digest"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			name := "v1-responses-2026-11-05T150000-12345678.log.zst"
			doc, _ := dedupFixture(t, dir, name, 123, []byte("first"), []byte("second"))
			value := doc.Get("chunks.1.hash").String()
			path := filepath.Join(dir, requestChunkDirectory, value[:2], value+".zst")
			switch failure {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "different bytes":
				writeDedupCompressedFixture(t, path, []byte("SECOND"))
			case "oversized":
				writeDedupCompressedFixture(t, path, []byte("second extra payload"))
			case "damaged frame":
				if err := os.WriteFile(path, []byte("not zstd"), 0600); err != nil {
					t.Fatal(err)
				}
			case "record digest":
				_ = doc.Set("sha256", strings.Repeat("0", 64))
				writeDedupCompressedFixture(t, filepath.Join(dir, requestManifestPath(name)), doc.Bytes())
			}
			reader, err := OpenRequestRecord(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Close() }()
			if _, err = io.ReadAll(reader); err == nil {
				t.Fatal("damaged record returned successful EOF")
			}
		})
	}
}

func TestDeduplicatedArchiveRequestRecordRejectsInvalidManifests(t *testing.T) {
	for _, failure := range []string{"version", "string version", "boolean version", "fractional version", "name", "negative size", "noninteger size", "large fractional timestamp", "path hash", "chunk size", "sum mismatch", "oversized manifest"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			name := "v1-responses-2026-11-05T150000-12345678.log.zst"
			doc, _ := dedupFixture(t, dir, name, 123, []byte("payload"))
			switch failure {
			case "version":
				_ = doc.Set("schema_version", 2)
			case "string version":
				_ = doc.Set("schema_version", "1")
			case "boolean version":
				_ = doc.Set("schema_version", true)
			case "fractional version":
				_ = doc.SetRaw("schema_version", "1.00000000000000001")
			case "name":
				_ = doc.Set("name", "other.log.zst")
			case "negative size":
				_ = doc.Set("original_bytes", -1)
			case "noninteger size":
				_ = doc.Set("original_bytes", 1.5)
			case "large fractional timestamp":
				_ = doc.SetRaw("modified_ns", "1793887200123456789.1")
			case "path hash":
				_ = doc.Set("chunks.0.hash", "../../outside")
			case "chunk size":
				_ = doc.Set("chunks.0.size", maxRequestChunkBytes+1)
			case "sum mismatch":
				_ = doc.Set("original_bytes", 999)
			case "oversized manifest":
				_ = doc.Set("ignored", strings.Repeat("x", maxRequestManifestBytes))
			}
			writeDedupCompressedFixture(t, filepath.Join(dir, requestManifestPath(name)), doc.Bytes())
			// Invalid durable metadata must not fall back to a legacy duplicate.
			writeDedupCompressedFixture(t, filepath.Join(dir, name), []byte("legacy"))
			if reader, err := OpenRequestRecord(filepath.Join(dir, name)); err == nil {
				_ = reader.Close()
				t.Fatal("invalid manifest opened")
			}
		})
	}
}

func TestDeduplicatedArchiveRequestRecordRestrictsChunkSymlinks(t *testing.T) {
	dir := t.TempDir()
	name := "v1-responses-2026-11-05T150000-12345678.log.zst"
	doc, _ := dedupFixture(t, dir, name, 123, []byte("payload"))
	value := doc.Get("chunks.0.hash").String()
	path := filepath.Join(dir, requestChunkDirectory, value[:2], value+".zst")
	outside := filepath.Join(t.TempDir(), "outside.zst")
	writeDedupCompressedFixture(t, outside, []byte("payload"))
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenRequestRecord(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	if _, err = io.ReadAll(reader); err == nil {
		t.Fatal("chunk symlink escaped archive directory")
	}
}

func TestOpenRequestArchiveRecordReadsLegacyArchivesAndClosesEarly(t *testing.T) {
	dir := t.TempDir()
	name := "v1-responses-2026-11-05T150000-12345678.log.zst"
	want := []byte("legacy request bytes")
	writeDedupCompressedFixture(t, filepath.Join(dir, name), want)
	reader, err := OpenRequestRecord(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("legacy read: %v", err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	dedupFixture(t, dir, name, 123, []byte("first"), []byte("second"))
	reader, err = OpenRequestRecord(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Read(make([]byte, 1)); err != os.ErrClosed {
		t.Fatalf("read after close: %v", err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDeduplicatedArchiveRequestListingCacheTracksMetadataAndDirectories(t *testing.T) {
	dir := t.TempDir()
	name := "v1-responses-2026-11-05T150000-12345678.log.zst"
	doc, _ := dedupFixture(t, dir, name, 123, []byte("payload"))
	for i := 0; i < 2; i++ {
		entries, err := ListRequestLogFiles(dir)
		if err != nil || len(entries) != 1 || entries[0].Size != 1234 {
			t.Fatalf("list %d: %v %v", i, entries, err)
		}
	}
	path := filepath.Join(dir, requestManifestPath(name))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = doc.Set("legacy_compressed_bytes", 56789)
	writeDedupCompressedFixture(t, path, doc.Bytes())
	changed := info.ModTime().Add(time.Second)
	if err = os.Chtimes(path, changed, changed); err != nil {
		t.Fatal(err)
	}
	entries, err := ListRequestLogFiles(dir)
	if err != nil || len(entries) != 1 || entries[0].Size != 56789 {
		t.Fatalf("changed manifest returned stale metadata: %v %v", entries, err)
	}
	other := t.TempDir()
	dedupFixture(t, other, name, 999, []byte("other payload"))
	entries, err = ListRequestLogFiles(other)
	if err != nil || len(entries) != 1 || entries[0].Size != 1234 || entries[0].Modified.UnixNano() != 999 {
		t.Fatalf("directory change reused another archive's metadata: %v %v", entries, err)
	}
}

func TestDeduplicatedArchiveRequestRecordRestrictsManifestSymlinks(t *testing.T) {
	dir := t.TempDir()
	name := "v1-responses-2026-11-05T150000-12345678.log.zst"
	doc, _ := dedupFixture(t, dir, name, 123, []byte("payload"))
	path := filepath.Join(dir, requestManifestPath(name))
	outside := filepath.Join(t.TempDir(), "outside.zst")
	writeDedupCompressedFixture(t, outside, doc.Bytes())
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	if reader, err := OpenRequestRecord(filepath.Join(dir, name)); err == nil {
		_ = reader.Close()
		t.Fatal("manifest symlink escaped archive directory")
	}
	if _, err := ListRequestLogFiles(dir); err == nil {
		t.Fatal("manifest symlink appeared in listing")
	}
}

func packDedupObjectsFixture(t *testing.T, dir, prefix string, keepLoose bool) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.zst"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no loose objects to pack: %v", err)
	}
	file, err := os.Create(filepath.Join(dir, prefix+"test.zip"))
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	for _, path := range paths {
		data, errRead := os.ReadFile(path)
		if errRead != nil {
			t.Fatal(errRead)
		}
		member, errMember := writer.CreateHeader(&zip.FileHeader{Name: filepath.Base(path), Method: zip.Store})
		if errMember != nil {
			t.Fatal(errMember)
		}
		if _, err = member.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if keepLoose {
			// The durable pack must win over recovery sources, even damaged ones.
			if err = os.WriteFile(path, []byte("damaged recovery duplicate"), 0600); err != nil {
				t.Fatal(err)
			}
		} else if err = os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDeduplicatedArchivePacksPreserveReadsListingAndReservation(t *testing.T) {
	for _, keepLoose := range []bool{false, true} {
		t.Run(map[bool]string{false: "only packs", true: "recovery duplicates"}[keepLoose], func(t *testing.T) {
			dir := t.TempDir()
			name := "v1-responses-2026-11-05T150000-12345678.log.zst"
			_, body := dedupFixture(t, dir, name, 123, []byte("first chunk"), []byte("second chunk"), []byte("first chunk"))
			packDedupObjectsFixture(t, filepath.Join(dir, requestManifestDirectory), "records-", keepLoose)
			shards, err := os.ReadDir(filepath.Join(dir, requestChunkDirectory))
			if err != nil {
				t.Fatal(err)
			}
			for _, shard := range shards {
				packDedupObjectsFixture(t, filepath.Join(dir, requestChunkDirectory, shard.Name()), "chunks-", keepLoose)
			}
			for attempt := 0; attempt < 2; attempt++ {
				reader, errOpen := OpenRequestRecord(filepath.Join(dir, name))
				if errOpen != nil {
					t.Fatal(errOpen)
				}
				got, errRead := io.ReadAll(reader)
				if errRead != nil || !bytes.Equal(got, body) {
					t.Fatalf("packed record read: %v", errRead)
				}
				if errClose := reader.Close(); errClose != nil {
					t.Fatal(errClose)
				}
				entries, errList := ListRequestLogFiles(dir)
				if errList != nil || len(entries) != 1 || entries[0].Name != name || entries[0].Modified.UnixNano() != 123 || entries[0].Size != 1234 {
					t.Fatalf("packed record metadata: %v %v", entries, errList)
				}
			}
			if file, errCreate := createUnarchivedLogFile(filepath.Join(dir, strings.TrimSuffix(name, ".zst"))); !os.IsExist(errCreate) {
				if file != nil {
					_ = file.Close()
				}
				t.Fatalf("packed dedup name reused: %v", errCreate)
			}
		})
	}
}

func TestDeduplicatedArchiveRejectsCorruptPacks(t *testing.T) {
	for _, kind := range []string{"manifest", "chunk"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			name := "v1-responses-2026-11-05T150000-12345678.log.zst"
			doc, _ := dedupFixture(t, dir, name, 123, []byte("payload"))
			path := filepath.Join(dir, requestManifestDirectory, "records-corrupt.zip")
			if kind == "chunk" {
				value := doc.Get("chunks.0.hash").String()
				path = filepath.Join(dir, requestChunkDirectory, value[:2], "chunks-corrupt.zip")
			}
			if err := os.WriteFile(path, []byte("damaged ZIP"), 0600); err != nil {
				t.Fatal(err)
			}
			reader, errOpen := OpenRequestRecord(filepath.Join(dir, name))
			if kind == "manifest" {
				if errOpen == nil {
					_ = reader.Close()
					t.Fatal("corrupt manifest pack fell back to loose data")
				}
				return
			}
			if errOpen != nil {
				t.Fatal(errOpen)
			}
			defer func() { _ = reader.Close() }()
			if _, err := io.ReadAll(reader); err == nil {
				t.Fatal("corrupt chunk pack fell back to loose data")
			}
		})
	}
}

func TestArchiveStoredMemberChecksCompressedCRC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "chunks-test.zip")
	output, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(output)
	member, err := writer.CreateRaw(&zip.FileHeader{Name: "member.zst", Method: zip.Store, CRC32: 1, CompressedSize64: 7, UncompressedSize64: 7})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = member.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = output.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	reader, err := openPackedRequestObject(root, ".", "chunks-", "member.zst")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	if _, err = io.ReadAll(reader); err == nil {
		t.Fatal("damaged compressed member passed ZIP checksum")
	}
}

// An opt-in check against a fixture produced by the Python indexer or a complete
// isolated corpus. It consumes every record so all chunk and whole-record
// integrity checks run, without exposing archived bodies in test output.
func TestArchiveDedupPythonInterop(t *testing.T) {
	dir := os.Getenv("REQUEST_ARCHIVE_VERIFY_DIR")
	if dir == "" {
		t.Skip("REQUEST_ARCHIVE_VERIFY_DIR not set")
	}
	entries, err := ListRequestLogFiles(dir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("archive listing: records=%d err=%v", len(entries), err)
	}
	var expected map[string]ojson.Result
	if data, errExpected := os.ReadFile(filepath.Join(dir, "expected.json")); errExpected == nil {
		if !ojson.ValidBytes(data) || !ojson.ParseBytes(data).IsObject() {
			t.Fatal("invalid expected fixture metadata")
		}
		expected = ojson.ParseBytes(data).Map()
	} else if !os.IsNotExist(errExpected) {
		t.Fatal(errExpected)
	}
	var total int64
	verified := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name, ".log.zst") {
			continue
		}
		reader, errOpen := OpenRequestRecord(filepath.Join(dir, entry.Name))
		if errOpen != nil {
			t.Fatal(errOpen)
		}
		digest := sha256.New()
		size, errRead := io.Copy(digest, reader)
		errClose := reader.Close()
		if errRead != nil || errClose != nil {
			t.Fatalf("record failed integrity validation: read=%v close=%v", errRead, errClose)
		}
		if expected != nil {
			want, exists := expected[entry.Name]
			if !exists || size != want.Get("size").Int() || hex.EncodeToString(digest.Sum(nil)) != want.Get("sha256").String() {
				t.Fatal("reconstruction differs from Python fixture metadata")
			}
		}
		total += size
		verified++
	}
	if expected != nil && verified != len(expected) {
		t.Fatalf("verified %d records, want %d", verified, len(expected))
	}
	t.Logf("verified_records=%d original_bytes=%d", verified, total)
}
