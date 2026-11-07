package logging

import (
	"archive/zip"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// RequestLogFile describes a logical record, whether standalone or in an immutable pack.
type RequestLogFile struct {
	Name     string
	Size     int64
	Modified time.Time
}

// ListRequestLogFiles merges standalone logs and packed members by original name.
// Packs are published before standalone files are removed; duplicates count once.
func ListRequestLogFiles(dir string) ([]RequestLogFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	records := make(map[string]RequestLogFile)
	for _, entry := range entries {
		if entry.IsDir() || !(strings.HasSuffix(entry.Name(), ".log") || strings.HasSuffix(entry.Name(), ".log.zst")) {
			continue
		}
		info, errInfo := entry.Info()
		if os.IsNotExist(errInfo) {
			continue
		}
		if errInfo != nil {
			return nil, errInfo
		}
		records[entry.Name()] = RequestLogFile{entry.Name(), info.Size(), info.ModTime()}
	}
	// A fresh directory read also sees packs published during the standalone scan.
	packs, err := filepath.Glob(filepath.Join(dir, "requests-*.zip"))
	if err != nil {
		return nil, err
	}
	if errPacks := addLegacyRequestPacks(packs, records); errPacks != nil {
		return nil, errPacks
	}
	if errDedup := listDeduplicatedRequestFiles(dir, records); errDedup != nil {
		return nil, errDedup
	}
	result := make([]RequestLogFile, 0, len(records))
	for _, record := range records {
		result = append(result, record)
	}
	return result, nil
}

// A migration can remove a legacy pack after its directory snapshot was taken.
// Its replacement manifests are merged after this scan.
func addLegacyRequestPacks(packs []string, records map[string]RequestLogFile) error {
	for _, path := range packs {
		pack, errOpen := zip.OpenReader(path)
		if os.IsNotExist(errOpen) {
			continue
		}
		if errOpen != nil {
			return errOpen
		}
		for _, member := range pack.File {
			if filepath.Base(member.Name) != member.Name || !strings.HasSuffix(member.Name, ".log.zst") {
				continue
			}
			stamp, errStamp := strconv.ParseInt(member.Comment, 10, 64)
			if errStamp != nil {
				_ = pack.Close()
				return errStamp
			}
			records[member.Name] = RequestLogFile{member.Name, int64(member.UncompressedSize64), time.Unix(0, stamp)}
		}
		if errClose := pack.Close(); errClose != nil {
			return errClose
		}
	}
	return nil
}

type packedRequestReader struct {
	io.ReadCloser
	pack *zip.ReadCloser
}

func (r *packedRequestReader) Close() error {
	err := r.ReadCloser.Close()
	errPack := r.pack.Close()
	if err != nil {
		return err
	}
	return errPack
}

// OpenRequestArchive opens compressed bytes without extracting other packed records.
// Retrying via packs handles compaction between directory listing and opening.
func OpenRequestArchive(path string) (io.ReadCloser, error) {
	source, err := os.Open(path)
	if err == nil {
		return source, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	packs, errGlob := filepath.Glob(filepath.Join(filepath.Dir(path), "requests-*.zip"))
	if errGlob != nil {
		return nil, errGlob
	}
	return openLegacyPackedRequest(packs, filepath.Base(path))
}

func openLegacyPackedRequest(packs []string, name string) (io.ReadCloser, error) {
	for _, packPath := range packs {
		pack, errOpen := zip.OpenReader(packPath)
		if os.IsNotExist(errOpen) {
			continue
		}
		if errOpen != nil {
			return nil, errOpen
		}
		for _, member := range pack.File {
			if member.Name != name {
				continue
			}
			reader, errMember := member.Open()
			if errMember != nil {
				_ = pack.Close()
				return nil, errMember
			}
			return &packedRequestReader{reader, pack}, nil
		}
		if errClose := pack.Close(); errClose != nil {
			return nil, errClose
		}
	}
	return nil, os.ErrNotExist
}
