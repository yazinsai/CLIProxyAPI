package logging

import (
	"archive/zip"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	maxRequestPackCacheMembers = 500000
	maxRequestPackCacheEntries = 8192
)

type requestPackedMember struct {
	offset int64
	size   int64
	crc    uint32
}

type requestPackMetadata struct {
	info       os.FileInfo
	modifiedNs int64
	size       int64
	members    map[string]requestPackedMember
}

// Packs are immutable. Cache their offsets, never their open files or payloads.
var requestPackCache = struct {
	sync.Mutex
	directory string
	entries   map[string]requestPackMetadata
	members   int
}{entries: make(map[string]requestPackMetadata)}

func openRequestPack(root *os.Root, path string) (*os.File, os.FileInfo, map[string]requestPackedMember, error) {
	file, errOpen := root.Open(path)
	if errOpen != nil {
		return nil, nil, nil, errOpen
	}
	info, errStat := file.Stat()
	if errStat != nil {
		_ = file.Close()
		return nil, nil, nil, errStat
	}
	key := filepath.Join(root.Name(), path)
	requestPackCache.Lock()
	if requestPackCache.directory != root.Name() {
		requestPackCache.directory = root.Name()
		requestPackCache.entries = make(map[string]requestPackMetadata)
		requestPackCache.members = 0
	}
	metadata, cached := requestPackCache.entries[key]
	requestPackCache.Unlock()
	if cached && os.SameFile(metadata.info, info) && metadata.modifiedNs == info.ModTime().UnixNano() && metadata.size == info.Size() {
		return file, info, metadata.members, nil
	}
	pack, errPack := zip.NewReader(file, info.Size())
	if errPack != nil {
		_ = file.Close()
		return nil, nil, nil, errors.New("invalid request archive pack")
	}
	if len(pack.File) > maxRequestManifestParts {
		_ = file.Close()
		return nil, nil, nil, errors.New("too many request archive pack members")
	}
	members := make(map[string]requestPackedMember, len(pack.File))
	for _, member := range pack.File {
		maxObjectBytes := uint64(maxRequestChunkBytes + (1 << 20))
		validName := filepath.Base(member.Name) == member.Name && !strings.ContainsAny(member.Name, `/\\`)
		if filepath.Dir(path) == requestManifestDirectory {
			maxObjectBytes = maxRequestManifestBytes + (1 << 20)
			logicalName := strings.TrimSuffix(member.Name, requestManifestSuffix)
			validName = validName && strings.HasSuffix(member.Name, ".log.zst"+requestManifestSuffix) && !strings.Contains(logicalName, "..")
		} else if strings.HasPrefix(path, requestChunkDirectory+string(os.PathSeparator)) {
			chunkHash := strings.TrimSuffix(member.Name, ".zst")
			validName = validName && strings.HasSuffix(member.Name, ".zst") && validRequestHash(chunkHash) && strings.HasPrefix(chunkHash, filepath.Base(filepath.Dir(path)))
		}
		if !validName || member.Method != zip.Store || member.CompressedSize64 > maxObjectBytes || member.CompressedSize64 > math.MaxInt64 || member.CompressedSize64 != member.UncompressedSize64 {
			_ = file.Close()
			return nil, nil, nil, errors.New("invalid request archive pack member")
		}
		offset, errOffset := member.DataOffset()
		if errOffset != nil || offset < 0 || int64(member.CompressedSize64) > info.Size()-offset {
			_ = file.Close()
			return nil, nil, nil, errors.New("invalid request archive pack offset")
		}
		if _, exists := members[member.Name]; exists {
			_ = file.Close()
			return nil, nil, nil, errors.New("duplicate request archive pack member")
		}
		members[member.Name] = requestPackedMember{offset: offset, size: int64(member.CompressedSize64), crc: member.CRC32}
	}
	if len(members) <= maxRequestPackCacheMembers {
		requestPackCache.Lock()
		if requestPackCache.directory != root.Name() || requestPackCache.members+len(members) > maxRequestPackCacheMembers || len(requestPackCache.entries) >= maxRequestPackCacheEntries {
			requestPackCache.directory = root.Name()
			requestPackCache.entries = make(map[string]requestPackMetadata)
			requestPackCache.members = 0
		}
		if old, exists := requestPackCache.entries[key]; exists {
			requestPackCache.members -= len(old.members)
		}
		requestPackCache.entries[key] = requestPackMetadata{info: info, modifiedNs: info.ModTime().UnixNano(), size: info.Size(), members: members}
		requestPackCache.members += len(members)
		requestPackCache.Unlock()
	}
	return file, info, members, nil
}

type requestPackLookupMetadata struct {
	info    os.FileInfo
	members map[string]string
}

// Directory identity and metadata detect atomic pack publication/removal. The
// index locates one containing pack per object and retains no file descriptors.
var requestPackLookupCache = struct {
	sync.Mutex
	directory string
	entries   map[string]requestPackLookupMetadata
	members   int
}{entries: make(map[string]requestPackLookupMetadata)}

func requestPackLookup(root *os.Root, directory, prefix string) (map[string]string, error) {
	info, errStat := root.Stat(directory)
	if errStat != nil {
		return nil, errStat
	}
	key := directory + "\x00" + prefix
	requestPackLookupCache.Lock()
	defer requestPackLookupCache.Unlock()
	if requestPackLookupCache.directory != root.Name() {
		requestPackLookupCache.directory = root.Name()
		requestPackLookupCache.entries = make(map[string]requestPackLookupMetadata)
		requestPackLookupCache.members = 0
	}
	old, cached := requestPackLookupCache.entries[key]
	if cached && os.SameFile(old.info, info) && old.info.ModTime().Equal(info.ModTime()) && old.info.Size() == info.Size() {
		return old.members, nil
	}
	paths, errPaths := requestPackPaths(root, directory, prefix)
	if errPaths != nil {
		return nil, errPaths
	}
	lookup := make(map[string]string)
	for _, path := range paths {
		file, _, members, errPack := openRequestPack(root, path)
		if os.IsNotExist(errPack) {
			continue
		}
		if errPack != nil {
			return nil, errPack
		}
		for name := range members {
			if _, exists := lookup[name]; !exists {
				lookup[name] = path
			}
		}
		if errClose := file.Close(); errClose != nil {
			return nil, errClose
		}
	}
	if len(lookup) <= maxRequestPackCacheMembers {
		if cached {
			requestPackLookupCache.members -= len(old.members)
		}
		if requestPackLookupCache.members+len(lookup) > maxRequestPackCacheMembers || len(requestPackLookupCache.entries) >= maxRequestPackCacheEntries {
			requestPackLookupCache.entries = make(map[string]requestPackLookupMetadata)
			requestPackLookupCache.members = 0
		}
		requestPackLookupCache.entries[key] = requestPackLookupMetadata{info: info, members: lookup}
		requestPackLookupCache.members += len(lookup)
	}
	return lookup, nil
}

func invalidateRequestPackLookup(root *os.Root, directory, prefix string) {
	requestPackLookupCache.Lock()
	defer requestPackLookupCache.Unlock()
	if requestPackLookupCache.directory != root.Name() {
		return
	}
	key := directory + "\x00" + prefix
	if entry, exists := requestPackLookupCache.entries[key]; exists {
		requestPackLookupCache.members -= len(entry.members)
		delete(requestPackLookupCache.entries, key)
	}
}

type storedRequestMemberReader struct {
	reader   *io.SectionReader
	file     *os.File
	checksum uint32
	expected uint32
	terminal error
}

func (reader *storedRequestMemberReader) Read(buffer []byte) (int, error) {
	if reader.terminal != nil {
		return 0, reader.terminal
	}
	n, errRead := reader.reader.Read(buffer)
	if n > 0 {
		reader.checksum = crc32.Update(reader.checksum, crc32.IEEETable, buffer[:n])
	}
	if errRead == io.EOF {
		if reader.checksum != reader.expected {
			reader.terminal = errors.New("request archive pack checksum mismatch")
		} else {
			reader.terminal = io.EOF
		}
		return n, reader.terminal
	}
	return n, errRead
}

func (reader *storedRequestMemberReader) Close() error { return reader.file.Close() }

func requestPackPaths(root *os.Root, directory, prefix string) ([]string, error) {
	file, errOpen := root.Open(directory)
	if errOpen != nil {
		return nil, errOpen
	}
	defer func() { _ = file.Close() }()
	names, errEntries := file.Readdirnames(-1)
	if errEntries != nil {
		return nil, errEntries
	}
	var paths []string
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".zip") {
			continue
		}
		path := filepath.Join(directory, name)
		info, errInfo := root.Lstat(path)
		if os.IsNotExist(errInfo) {
			continue
		}
		if errInfo != nil {
			return nil, errInfo
		}
		if !info.IsDir() {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func openPackedRequestObject(root *os.Root, directory, prefix, name string) (io.ReadCloser, error) {
	for attempt := 0; attempt < 2; attempt++ {
		lookup, errLookup := requestPackLookup(root, directory, prefix)
		if errLookup != nil {
			return nil, errLookup
		}
		path, exists := lookup[name]
		if !exists {
			return nil, os.ErrNotExist
		}
		file, _, members, errPack := openRequestPack(root, path)
		if os.IsNotExist(errPack) {
			invalidateRequestPackLookup(root, directory, prefix)
			continue
		}
		if errPack != nil {
			return nil, errPack
		}
		member, exists := members[name]
		if !exists {
			_ = file.Close()
			invalidateRequestPackLookup(root, directory, prefix)
			continue
		}
		return &storedRequestMemberReader{reader: io.NewSectionReader(file, member.offset, member.size), file: file, expected: member.crc}, nil
	}
	return nil, os.ErrNotExist
}

// A published pack wins over its loose source during durable cleanup. Retrying
// after a missing loose source handles publication between the directory scan
// and opening without extracting or keeping unrelated pack files open.
func openRequestObject(root *os.Root, directory, prefix, name string) (io.ReadCloser, error) {
	packed, errPacked := openPackedRequestObject(root, directory, prefix, name)
	if errPacked == nil || !os.IsNotExist(errPacked) {
		return packed, errPacked
	}
	file, errOpen := root.Open(filepath.Join(directory, name))
	if errOpen == nil || !os.IsNotExist(errOpen) {
		return file, errOpen
	}
	invalidateRequestPackLookup(root, directory, prefix)
	return openPackedRequestObject(root, directory, prefix, name)
}
