package dashboard

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/google/uuid"
)

const (
	maxRecords      = 100_000
	maxJournalBytes = 32 << 20
	maxLineBytes    = 32 << 10
	retention       = 24 * time.Hour
	journalSchema   = 2
)

type Store struct {
	mu            sync.RWMutex
	rows          []Request
	path          string
	coverageStart time.Time
	degraded      bool
	now           func() time.Time
	journalStart  time.Time
}

func NewStore(directory string) *Store {
	s := &Store{now: time.Now}
	if directory != "" {
		s.path = filepath.Join(directory, "dashboard-completions.jsonl")
		s.load()
	}
	if s.coverageStart.IsZero() {
		s.coverageStart = s.now().UTC()
	}
	if s.path != "" {
		if errMarker := s.appendJournal(nil); errMarker != nil {
			s.degraded = true
		}
	}
	return s
}

func (s *Store) load() {
	for _, path := range []string{s.path + ".previous", s.path} {
		file, errOpen := os.Open(path)
		if errOpen != nil {
			if !os.IsNotExist(errOpen) {
				s.degraded = true
			}
			continue
		}
		info, errStat := file.Stat()
		if errStat != nil || info.Size() > maxJournalBytes {
			s.degraded = true
			_ = file.Close()
			continue
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 4096), maxLineBytes)
		for scanner.Scan() {
			doc := ojson.ParseBytes(scanner.Bytes())
			// Version 1 misclassified successful terminal delivery as cancellation.
			// Do not reinterpret faulty historical telemetry; start fresh coverage.
			if doc.Get("schema_version").Int() == 1 {
				continue
			}
			if doc.Get("schema_version").Int() != journalSchema {
				s.degraded = true
				continue
			}
			if start, errTime := time.Parse(time.RFC3339Nano, doc.Get("coverage_start").String()); errTime == nil && (s.coverageStart.IsZero() || start.After(s.coverageStart)) {
				s.coverageStart = start
			}
			if !doc.Get("request").Exists() {
				continue
			}
			row, errRow := decodeRequest(doc.Get("request"))
			if errRow != nil {
				s.degraded = true
				continue
			}
			s.rows = append(s.rows, row)
			if path == s.path && s.journalStart.IsZero() {
				s.journalStart = row.CompletedAt
			}
		}
		if scanner.Err() != nil {
			s.degraded = true
		}
		_ = file.Close()
	}
	sort.SliceStable(s.rows, func(i, j int) bool { return s.rows[i].CompletedAt.Before(s.rows[j].CompletedAt) })
	s.trim(s.now())
}

func (s *Store) trim(now time.Time) {
	cutoff := now.Add(-retention)
	if s.coverageStart.After(cutoff) {
		cutoff = s.coverageStart
	}
	first := 0
	for first < len(s.rows) && s.rows[first].CompletedAt.Before(cutoff) {
		first++
	}
	if len(s.rows)-first > maxRecords {
		first = len(s.rows) - maxRecords
	}
	if first > 0 {
		if last := s.rows[first-1].CompletedAt.Add(time.Nanosecond); last.After(s.coverageStart) {
			s.coverageStart = last
		}
		for i := 0; i < len(s.rows)-first; i++ {
			s.rows[i] = s.rows[i+first]
		}
		for i := len(s.rows) - first; i < len(s.rows); i++ {
			s.rows[i] = Request{}
		}
		s.rows = s.rows[:len(s.rows)-first]
	}
}

func (s *Store) Add(row Request) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	row = cloneRequest(row)
	if len(s.rows) > 0 && row.CompletedAt.Before(s.rows[len(s.rows)-1].CompletedAt) {
		at := sort.Search(len(s.rows), func(i int) bool { return !s.rows[i].CompletedAt.Before(row.CompletedAt) })
		s.rows = append(s.rows, Request{})
		for i := len(s.rows) - 1; i > at; i-- {
			s.rows[i] = s.rows[i-1]
		}
		s.rows[at] = row
	} else {
		s.rows = append(s.rows, row)
	}
	s.trim(s.now())
	errAppend := s.appendJournal(&row)
	if errAppend != nil {
		s.degraded = true
	}
	return errAppend
}

func (s *Store) appendJournal(row *Request) error {
	if s.path == "" {
		return nil
	}
	if errMkdir := os.MkdirAll(filepath.Dir(s.path), 0700); errMkdir != nil {
		s.degraded = true
		return errMkdir
	}
	if info, errStat := os.Stat(s.path); errStat == nil && info.Size() >= maxJournalBytes-maxLineBytes {
		if _, errPrevious := os.Stat(s.path + ".previous"); errPrevious == nil && s.journalStart.After(s.coverageStart) {
			s.coverageStart = s.journalStart
			s.trim(s.now())
		}
		if errRename := os.Rename(s.path, s.path+".previous"); errRename != nil {
			s.degraded = true
			return errRename
		}
		s.journalStart = time.Time{}
	}
	file, errOpen := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if errOpen != nil {
		s.degraded = true
		return errOpen
	}
	doc := ojson.NewDocString("{}")
	values := map[string]any{"schema_version": journalSchema, "coverage_start": s.coverageStart}
	if row != nil {
		values["request"] = *row
		if s.journalStart.IsZero() {
			s.journalStart = row.CompletedAt
		}
	}
	for key, value := range values {
		if errSet := doc.Set(key, value); errSet != nil {
			_ = file.Close()
			s.degraded = true
			return errSet
		}
	}
	line := append(doc.Bytes(), '\n')
	if len(line) > maxLineBytes {
		_ = file.Close()
		s.degraded = true
		return errors.New("dashboard completion metadata exceeds limit")
	}
	written, errWrite := file.Write(line)
	if errWrite == nil && written != len(line) {
		errWrite = io.ErrShortWrite
	}
	errClose := file.Close()
	if errWrite != nil || errClose != nil {
		s.degraded = true
		return errors.Join(errWrite, errClose)
	}
	return nil
}

// Snapshot returns a consistent feed and summary source for the selected window.
func (s *Store) Snapshot(window time.Duration) (rows []Request, coverage time.Time, partial bool, observed time.Time) {
	if s == nil {
		return nil, time.Time{}, true, time.Now().UTC()
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	observed = s.now().UTC()
	cutoff := observed.Add(-window)
	for _, row := range s.rows {
		if !row.CompletedAt.Before(cutoff) && !row.CompletedAt.After(observed) {
			rows = append(rows, cloneRequest(row))
		}
	}
	coverage = s.coverageStart
	partial = s.degraded || coverage.After(cutoff)
	return
}

func (s *Store) Get(id string) (Request, bool) {
	if s == nil {
		return Request{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := len(s.rows) - 1; i >= 0; i-- {
		if s.rows[i].ID == id && !s.rows[i].CompletedAt.Before(s.now().Add(-retention)) {
			return cloneRequest(s.rows[i]), true
		}
	}
	return Request{}, false
}

func cloneRequest(row Request) Request {
	row.Attempts = append([]Attempt(nil), row.Attempts...)
	return row
}

func (s *Store) LatestProvider(model string) string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := len(s.rows) - 1; i >= 0; i-- {
		if s.rows[i].Model == model && !s.rows[i].CompletedAt.Before(s.now().Add(-retention)) {
			return s.rows[i].Provider
		}
	}
	return ""
}

func (s *Store) LatestProviders() map[string]string {
	result := make(map[string]string)
	if s == nil {
		return result
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	cutoff := s.now().Add(-retention)
	for i := len(s.rows) - 1; i >= 0; i-- {
		row := s.rows[i]
		if row.CompletedAt.Before(cutoff) {
			break
		}
		if _, seen := result[row.Model]; !seen {
			result[row.Model] = row.Provider
		}
	}
	return result
}

func Groups(rows []Request, model bool) []Group {
	groups := make(map[string]*Group)
	for _, row := range rows {
		id := row.Provider
		if model {
			id = row.Model
		}
		group := groups[id]
		if group == nil {
			group = &Group{ID: id}
			groups[id] = group
		}
		group.Total++
		if row.Outcome == "success" {
			group.Success++
		} else {
			group.Failed++
		}
	}
	result := make([]Group, 0, len(groups))
	for _, group := range groups {
		result = append(result, *group)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Total == result[j].Total {
			return result[i].ID < result[j].ID
		}
		return result[i].Total > result[j].Total
	})
	return result
}

func Cursor(row Request) string {
	return strconv.FormatInt(row.CompletedAt.UnixNano(), 10) + ":" + row.ID
}

func ParseCursor(cursor string) (time.Time, string, error) {
	parts := strings.SplitN(cursor, ":", 2)
	if len(parts) != 2 || len(parts[1]) != 36 {
		return time.Time{}, "", errors.New("invalid cursor")
	}
	nanos, errParse := strconv.ParseInt(parts[0], 10, 64)
	if errParse != nil {
		return time.Time{}, "", errors.New("invalid cursor")
	}
	return time.Unix(0, nanos), parts[1], nil
}

func decodeRequest(value ojson.Result) (Request, error) {
	row := Request{ID: value.Get("id").String(), TraceID: value.Get("trace_id").String(), Model: PublicID(value.Get("model").String()), Provider: PublicID(value.Get("provider").String()), StatusCode: int(value.Get("status_code").Int()), Outcome: value.Get("outcome").String(), DurationMS: value.Get("duration_ms").Int(), Reason: value.Get("reason").String()}
	var errTime error
	row.StartedAt, errTime = time.Parse(time.RFC3339Nano, value.Get("started_at").String())
	if errTime != nil {
		return Request{}, errTime
	}
	row.CompletedAt, errTime = time.Parse(time.RFC3339Nano, value.Get("completed_at").String())
	if errTime != nil {
		return Request{}, errTime
	}
	_, errID := uuid.Parse(row.ID)
	if errID != nil || row.DurationMS < 0 || row.StatusCode < 0 || row.StatusCode > 599 || row.CompletedAt.Before(row.StartedAt) || !validOutcome(row.Outcome) {
		return Request{}, fmt.Errorf("invalid completion metadata")
	}
	if !validReason(row.Reason) {
		row.Reason = "unknown"
	}
	archive := value.Get("archive_request_id").String()
	if archive != "" && len(archive) == 36 {
		row.ArchiveRequestID = &archive
	}
	for _, entry := range value.Get("attempts").Array() {
		if len(row.Attempts) >= maxAttempts {
			break
		}
		started, errStart := time.Parse(time.RFC3339Nano, entry.Get("started_at").String())
		completed, errEnd := time.Parse(time.RFC3339Nano, entry.Get("completed_at").String())
		if errStart != nil || errEnd != nil || completed.Before(started) || entry.Get("duration_ms").Int() < 0 || entry.Get("status_code").Int() < 0 || entry.Get("status_code").Int() > 599 || !validOutcome(entry.Get("outcome").String()) {
			continue
		}
		reason := entry.Get("reason").String()
		if !validReason(reason) {
			reason = "unknown"
		}
		row.Attempts = append(row.Attempts, Attempt{ID: PublicID(entry.Get("id").String()), Provider: PublicID(entry.Get("provider").String()), Model: PublicID(entry.Get("model").String()), StartedAt: started, CompletedAt: completed, StatusCode: int(entry.Get("status_code").Int()), Outcome: entry.Get("outcome").String(), DurationMS: entry.Get("duration_ms").Int(), Reason: reason})
	}
	return row, nil
}

func validOutcome(value string) bool {
	return value == "success" || value == "failed" || value == "interrupted"
}

func validReason(value string) bool {
	switch value {
	case "", "rate_limited", "unauthorized", "upstream_error", "invalid_request", "cancelled", "stream_interrupted", "unknown":
		return true
	}
	return false
}
