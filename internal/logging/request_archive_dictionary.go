package logging

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	maxRequestDictionaryBytes = 1 << 20
	maxRequestDictionaryCache = 16
)

type requestDictionaryMetadata struct {
	info       os.FileInfo
	modifiedNs int64
	size       int64
	data       []byte
}

var requestDictionaryCache = struct {
	sync.Mutex
	entries map[string]requestDictionaryMetadata
}{entries: make(map[string]requestDictionaryMetadata)}

func readRequestDictionary(root *os.Root, id uint32) ([]byte, error) {
	path := filepath.Join(requestChunkDirectory, "dictionaries", strconv.FormatUint(uint64(id), 10)+".zdict")
	file, errOpen := root.Open(path)
	if errOpen != nil {
		return nil, errors.New("cannot open request archive dictionary")
	}
	defer func() { _ = file.Close() }()
	info, errStat := file.Stat()
	if errStat != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxRequestDictionaryBytes {
		return nil, errors.New("invalid request archive dictionary file")
	}
	key := filepath.Join(root.Name(), path)
	requestDictionaryCache.Lock()
	entry, cached := requestDictionaryCache.entries[key]
	requestDictionaryCache.Unlock()
	if cached && os.SameFile(entry.info, info) && entry.modifiedNs == info.ModTime().UnixNano() && entry.size == info.Size() {
		return entry.data, nil
	}
	data, errRead := io.ReadAll(io.LimitReader(file, maxRequestDictionaryBytes+1))
	if errRead != nil || len(data) > maxRequestDictionaryBytes {
		return nil, errors.New("cannot read request archive dictionary")
	}
	dictionary, errDictionary := zstd.InspectDictionary(data)
	if errDictionary != nil || dictionary.ID() != id || id == 0 {
		return nil, errors.New("request archive dictionary ID mismatch")
	}
	requestDictionaryCache.Lock()
	if len(requestDictionaryCache.entries) >= maxRequestDictionaryCache {
		requestDictionaryCache.entries = make(map[string]requestDictionaryMetadata)
	}
	requestDictionaryCache.entries[key] = requestDictionaryMetadata{info: info, modifiedNs: info.ModTime().UnixNano(), size: info.Size(), data: data}
	requestDictionaryCache.Unlock()
	return data, nil
}

// Object frames declare their dictionary ID, so older frames and archives using
// several immutable dictionaries remain independently readable.
func newRequestObjectDecoder(root *os.Root, source io.Reader) (*zstd.Decoder, error) {
	buffered := bufio.NewReaderSize(source, 32)
	headerBytes, errPeek := buffered.Peek(18)
	if errPeek != nil && errPeek != io.EOF {
		return nil, errors.New("cannot read request archive frame header")
	}
	var header zstd.Header
	if errHeader := header.Decode(headerBytes); errHeader != nil {
		return nil, errors.New("invalid request archive frame header")
	}
	options := []zstd.DOption{zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxWindow(maxRequestChunkBytes)}
	if header.DictionaryID != 0 {
		dictionary, errDictionary := readRequestDictionary(root, header.DictionaryID)
		if errDictionary != nil {
			return nil, errDictionary
		}
		options = append(options, zstd.WithDecoderDicts(dictionary))
	}
	return zstd.NewReader(buffered, options...)
}
