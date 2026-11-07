package logging

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/klauspost/compress/zstd"
)

const (
	requestManifestDirectory = "request-records"
	requestChunkDirectory    = "request-chunks"
	requestManifestSuffix    = ".json.zst"
	maxRequestManifestBytes  = 16 << 20
	maxRequestManifestParts  = 200000
	maxRequestChunkBytes     = 64 << 20
	maxRequestManifestCache  = 100000
)

type requestManifestMetadata struct {
	modifiedNs int64
	fileSize   int64
	record     RequestLogFile
}

// Only listing metadata is cached. Opening a record still validates its manifest
// and streamed bytes. Published manifests are immutable; changed file metadata
// and connection directories invalidate the bounded cache.
var requestManifestCache = struct {
	sync.Mutex
	directory string
	records   map[string]requestManifestMetadata
}{}

func cachedRequestManifestMetadata(dir, name string, info os.FileInfo) (RequestLogFile, bool) {
	requestManifestCache.Lock()
	defer requestManifestCache.Unlock()
	if requestManifestCache.directory != dir {
		requestManifestCache.directory = dir
		requestManifestCache.records = make(map[string]requestManifestMetadata)
	}
	entry, exists := requestManifestCache.records[name]
	return entry.record, exists && entry.modifiedNs == info.ModTime().UnixNano() && entry.fileSize == info.Size()
}

func cacheRequestManifestMetadata(dir, name string, info os.FileInfo, record RequestLogFile) {
	requestManifestCache.Lock()
	defer requestManifestCache.Unlock()
	if requestManifestCache.directory != dir || len(requestManifestCache.records) >= maxRequestManifestCache {
		requestManifestCache.directory = dir
		requestManifestCache.records = make(map[string]requestManifestMetadata)
	}
	requestManifestCache.records[name] = requestManifestMetadata{modifiedNs: info.ModTime().UnixNano(), fileSize: info.Size(), record: record}
}

type requestChunk struct {
	hash string
	size int64
}

type requestManifest struct {
	name       string
	modifiedNs int64
	size       int64
	legacySize int64
	hash       string
	chunks     []requestChunk
}

func requestManifestPath(name string) string {
	return filepath.Join(requestManifestDirectory, name+requestManifestSuffix)
}

func validRequestHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, digit := range value {
		if !(digit >= '0' && digit <= '9' || digit >= 'a' && digit <= 'f') {
			return false
		}
	}
	return true
}

func requestManifestInteger(value ojson.Result) (int64, error) {
	if value.Type != ojson.Number {
		return 0, errors.New("invalid request manifest counter")
	}
	integer, err := strconv.ParseInt(value.Raw, 10, 64)
	if err != nil || integer < 0 {
		return 0, errors.New("invalid request manifest counter")
	}
	return integer, nil
}

func readRequestManifest(root *os.Root, name string) (requestManifest, error) {
	var manifest requestManifest
	if filepath.Base(name) != name || strings.ContainsAny(name, `/\\`) || strings.Contains(name, "..") || !strings.HasSuffix(name, ".log.zst") {
		return manifest, errors.New("invalid request manifest name")
	}
	file, errOpen := openRequestObject(root, requestManifestDirectory, "records-", name+requestManifestSuffix)
	if errOpen != nil {
		return manifest, errOpen
	}
	defer func() { _ = file.Close() }()
	return decodeRequestManifest(root, file, name)
}

func decodeRequestManifest(root *os.Root, source io.Reader, name string) (requestManifest, error) {
	var manifest requestManifest
	if filepath.Base(name) != name || strings.ContainsAny(name, `/\\`) || strings.Contains(name, "..") || !strings.HasSuffix(name, ".log.zst") {
		return manifest, errors.New("invalid request manifest name")
	}
	decoder, errDecoder := newRequestObjectDecoder(root, source)
	if errDecoder != nil {
		return manifest, errors.New("cannot decode request manifest")
	}
	defer decoder.Close()
	body, errRead := io.ReadAll(io.LimitReader(decoder, maxRequestManifestBytes+1))
	if errRead != nil || len(body) > maxRequestManifestBytes || !ojson.ValidBytes(body) {
		return manifest, errors.New("invalid request manifest")
	}
	value := ojson.ParseBytes(body)
	version, errVersion := requestManifestInteger(value.Get("schema_version"))
	if !value.IsObject() || errVersion != nil || version != 1 || value.Get("name").Type != ojson.String || value.Get("name").String() != name || value.Get("sha256").Type != ojson.String || !validRequestHash(value.Get("sha256").String()) || !value.Get("chunks").IsArray() {
		return manifest, errors.New("unsupported request manifest")
	}
	modifiedNs, errModified := requestManifestInteger(value.Get("modified_ns"))
	size, errSize := requestManifestInteger(value.Get("original_bytes"))
	if errModified != nil || errSize != nil {
		return manifest, errors.New("invalid request manifest metadata")
	}
	legacySize := size
	if legacyValue := value.Get("legacy_compressed_bytes"); legacyValue.Exists() {
		var errLegacy error
		legacySize, errLegacy = requestManifestInteger(legacyValue)
		if errLegacy != nil {
			return manifest, errLegacy
		}
	}
	parts := value.Get("chunks").Array()
	if len(parts) > maxRequestManifestParts {
		return manifest, errors.New("too many request manifest chunks")
	}
	chunks := make([]requestChunk, 0, len(parts))
	var total int64
	for _, part := range parts {
		chunkSize, errPart := requestManifestInteger(part.Get("size"))
		chunkHash := part.Get("hash").String()
		if !part.IsObject() || errPart != nil || chunkSize <= 0 || chunkSize > maxRequestChunkBytes || part.Get("hash").Type != ojson.String || !validRequestHash(chunkHash) || chunkSize > size-total {
			return manifest, errors.New("invalid request manifest chunk")
		}
		total += chunkSize
		chunks = append(chunks, requestChunk{hash: chunkHash, size: chunkSize})
	}
	if total != size {
		return manifest, errors.New("request manifest size mismatch")
	}
	return requestManifest{name: name, modifiedNs: modifiedNs, size: size, legacySize: legacySize, hash: value.Get("sha256").String(), chunks: chunks}, nil
}

// OpenRequestRecord opens the original plaintext bytes of a standalone, packed,
// or deduplicated record. Deduplicated records verify chunks and the full record
// as they are consumed; closing early intentionally does not read the remainder.
func OpenRequestRecord(path string) (io.ReadCloser, error) {
	root, errRoot := os.OpenRoot(filepath.Dir(path))
	if errRoot != nil {
		return nil, errRoot
	}
	manifest, errManifest := readRequestManifest(root, filepath.Base(path))
	if errManifest == nil {
		return &deduplicatedRequestReader{root: root, manifest: manifest, recordHash: sha256.New()}, nil
	}
	_ = root.Close()
	if !os.IsNotExist(errManifest) {
		return nil, errManifest
	}
	file, errOpen := OpenRequestArchive(path)
	if errOpen != nil {
		// Publication may finish while the legacy pack is being opened.
		if os.IsNotExist(errOpen) {
			root, errRoot = os.OpenRoot(filepath.Dir(path))
			if errRoot == nil {
				manifest, errManifest = readRequestManifest(root, filepath.Base(path))
				if errManifest == nil {
					return &deduplicatedRequestReader{root: root, manifest: manifest, recordHash: sha256.New()}, nil
				}
				_ = root.Close()
				if !os.IsNotExist(errManifest) {
					return nil, errManifest
				}
			}
		}
		return nil, errOpen
	}
	decoder, errDecoder := zstd.NewReader(file, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true))
	if errDecoder != nil {
		_ = file.Close()
		return nil, errDecoder
	}
	return &decodedRequestReader{Decoder: decoder, source: file}, nil
}

type decodedRequestReader struct {
	*zstd.Decoder
	source io.ReadCloser
}

func (reader *decodedRequestReader) Close() error {
	reader.Decoder.Close()
	return reader.source.Close()
}

type deduplicatedRequestReader struct {
	root       *os.Root
	manifest   requestManifest
	index      int
	file       io.ReadCloser
	decoder    *zstd.Decoder
	chunkHash  hash.Hash
	recordHash hash.Hash
	chunkRead  int64
	totalRead  int64
	terminal   error
	closed     bool
}

func (reader *deduplicatedRequestReader) finishChunk() error {
	reader.decoder.Close()
	reader.decoder = nil
	errClose := reader.file.Close()
	reader.file = nil
	chunk := reader.manifest.chunks[reader.index]
	if reader.chunkRead != chunk.size || hex.EncodeToString(reader.chunkHash.Sum(nil)) != chunk.hash {
		return errors.New("request archive chunk integrity mismatch")
	}
	if errClose != nil {
		return errClose
	}
	reader.index++
	return nil
}

func (reader *deduplicatedRequestReader) Read(buffer []byte) (int, error) {
	if reader.closed {
		return 0, os.ErrClosed
	}
	if len(buffer) == 0 {
		return 0, nil
	}
	if reader.terminal != nil {
		return 0, reader.terminal
	}
	for {
		if reader.decoder == nil {
			if reader.index == len(reader.manifest.chunks) {
				if reader.totalRead != reader.manifest.size || hex.EncodeToString(reader.recordHash.Sum(nil)) != reader.manifest.hash {
					reader.terminal = errors.New("request archive record integrity mismatch")
				} else {
					reader.terminal = io.EOF
				}
				return 0, reader.terminal
			}
			chunk := reader.manifest.chunks[reader.index]
			directory := filepath.Join(requestChunkDirectory, chunk.hash[:2])
			file, errOpen := openRequestObject(reader.root, directory, "chunks-", chunk.hash+".zst")
			if errOpen != nil {
				reader.terminal = errors.New("cannot open request archive chunk")
				return 0, reader.terminal
			}
			decoder, errDecoder := newRequestObjectDecoder(reader.root, file)
			if errDecoder != nil {
				_ = file.Close()
				reader.terminal = errors.New("cannot decode request archive chunk")
				return 0, reader.terminal
			}
			reader.file, reader.decoder = file, decoder
			reader.chunkHash, reader.chunkRead = sha256.New(), 0
		}
		n, errRead := reader.decoder.Read(buffer)
		if n > 0 {
			_, _ = reader.chunkHash.Write(buffer[:n])
			_, _ = reader.recordHash.Write(buffer[:n])
			reader.chunkRead += int64(n)
			reader.totalRead += int64(n)
			if reader.chunkRead > reader.manifest.chunks[reader.index].size {
				reader.terminal = errors.New("request archive chunk exceeds declared size")
				return n, reader.terminal
			}
		}
		if errRead == io.EOF {
			if errFinish := reader.finishChunk(); errFinish != nil {
				reader.terminal = errFinish
				return n, errFinish
			}
			if n > 0 {
				return n, nil
			}
			continue
		}
		if errRead != nil {
			reader.terminal = errors.New("cannot decode request archive chunk")
			return n, reader.terminal
		}
		return n, nil
	}
}

func (reader *deduplicatedRequestReader) Close() error {
	if reader.closed {
		return nil
	}
	reader.closed = true
	var errFile error
	if reader.decoder != nil {
		reader.decoder.Close()
	}
	if reader.file != nil {
		errFile = reader.file.Close()
	}
	return errors.Join(errFile, reader.root.Close())
}

func listDeduplicatedRequestFiles(dir string, records map[string]RequestLogFile) error {
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		return errRoot
	}
	defer func() { _ = root.Close() }()
	file, errOpen := root.Open(requestManifestDirectory)
	if os.IsNotExist(errOpen) {
		return nil
	}
	if errOpen != nil {
		return errOpen
	}
	defer func() { _ = file.Close() }()
	entries, errEntries := file.ReadDir(-1)
	if errEntries != nil {
		return errEntries
	}
	packedNames := make(map[string]bool)
	paths, errPaths := requestPackPaths(root, requestManifestDirectory, "records-")
	if errPaths != nil {
		return errPaths
	}
	for _, path := range paths {
		pack, info, members, errPack := openRequestPack(root, path)
		if os.IsNotExist(errPack) {
			continue
		}
		if errPack != nil {
			return errPack
		}
		for memberName, member := range members {
			if !strings.HasSuffix(memberName, ".log.zst"+requestManifestSuffix) {
				_ = pack.Close()
				return errors.New("invalid packed request manifest name")
			}
			name := strings.TrimSuffix(memberName, requestManifestSuffix)
			packedNames[name] = true
			cacheKey := path + "\x00" + name
			if record, cached := cachedRequestManifestMetadata(dir, cacheKey, info); cached {
				records[name] = record
				continue
			}
			source := &storedRequestMemberReader{reader: io.NewSectionReader(pack, member.offset, member.size), expected: member.crc}
			manifest, errManifest := decodeRequestManifest(root, source, name)
			if errManifest != nil {
				_ = pack.Close()
				return errManifest
			}
			record := RequestLogFile{Name: name, Size: manifest.legacySize, Modified: time.Unix(0, manifest.modifiedNs)}
			records[name] = record
			cacheRequestManifestMetadata(dir, cacheKey, info, record)
		}
		if errClose := pack.Close(); errClose != nil {
			return errClose
		}
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log.zst"+requestManifestSuffix) {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), requestManifestSuffix)
		if packedNames[name] {
			continue
		}
		info, errInfo := entry.Info()
		if os.IsNotExist(errInfo) {
			continue
		}
		if errInfo != nil {
			return errInfo
		}
		if !info.Mode().IsRegular() {
			return errors.New("invalid request manifest file")
		}
		if record, cached := cachedRequestManifestMetadata(dir, name, info); cached {
			records[name] = record
			continue
		}
		manifest, errManifest := readRequestManifest(root, name)
		if os.IsNotExist(errManifest) {
			continue
		}
		if errManifest != nil {
			return fmt.Errorf("cannot list deduplicated request record: %w", errManifest)
		}
		record := RequestLogFile{Name: name, Size: manifest.legacySize, Modified: time.Unix(0, manifest.modifiedNs)}
		records[name] = record
		cacheRequestManifestMetadata(dir, name, info, record)
	}
	return nil
}

func deduplicatedRequestNameExists(dir, name string) (bool, error) {
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		return false, errRoot
	}
	defer func() { _ = root.Close() }()
	file, errOpen := openRequestObject(root, requestManifestDirectory, "records-", name+requestManifestSuffix)
	if os.IsNotExist(errOpen) {
		return false, nil
	}
	if errOpen != nil {
		return false, errOpen
	}
	return true, file.Close()
}
