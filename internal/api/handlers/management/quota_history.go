package management

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	quotaHistoryDefaultSpan = 14 * 24 * time.Hour
	quotaHistoryMaxSpan     = 62 * 24 * time.Hour
	// Archived lines longer than this are request/response bodies; metadata
	// and header lines are always far shorter, so long lines are skipped.
	quotaHistoryMaxLine = 64 * 1024
	// Codex reports reset instants with one-second jitter between responses.
	quotaHistoryResetTolerance = 2 * time.Minute
)

// quotaHistoryWindow is one quota window reading normalized from upstream
// rate-limit headers.
type quotaHistoryWindow struct {
	ID            string
	WindowMinutes int
	UsedPercent   float64
	ResetAtMs     int64
}

// quotaHistorySample is the quota observation carried by one upstream attempt.
type quotaHistorySample struct {
	AuthID     string
	Provider   string
	ObservedMs int64
	Windows    []quotaHistoryWindow
}

type quotaHistoryFileEntry struct {
	modNs   int64
	size    int64
	samples []quotaHistorySample
}

// quotaHistoryCache memoizes parsed archive files. Archived records are
// immutable (published by hard link, never overwritten), so a file whose size
// and modification time are unchanged never needs to be decompressed again.
type quotaHistoryCache struct {
	mu    sync.Mutex
	dir   string
	files map[string]quotaHistoryFileEntry
}

type quotaHistorySeries struct {
	ID            string `json:"id"`
	WindowMinutes int    `json:"window_minutes"`
	// Points are [observed_at_ms, used_percent, reset_at_ms] change points.
	Points [][3]float64 `json:"points"`
}

type quotaHistoryCredential struct {
	AuthID    string               `json:"auth_id"`
	AuthIndex string               `json:"auth_index,omitempty"`
	Name      string               `json:"name,omitempty"`
	Provider  string               `json:"provider"`
	Series    []quotaHistorySeries `json:"series"`
}

type quotaHistoryResponse struct {
	Recording   bool                     `json:"recording"`
	Since       time.Time                `json:"since"`
	Until       time.Time                `json:"until"`
	OldestAt    *time.Time               `json:"oldest_at"`
	Credentials []quotaHistoryCredential `json:"credentials"`
}

// GetQuotaHistory returns per-credential quota usage over time, reconstructed
// from the rate-limit headers retained in the compressed request archive.
//
// The archive is the only durable record of past quota observations: the live
// credential snapshot is replaced on every response. History therefore exists
// only while request archiving is enabled and only for providers whose
// responses carry quota headers (Claude and Codex).
func (h *Handler) GetQuotaHistory(c *gin.Context) {
	if h == nil || h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	now := time.Now()
	since, errSince := parseQuotaHistorySince(c.Query("since"), now)
	if errSince != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errSince.Error()})
		return
	}

	samples := h.quotaHistory.load(h.logDirectory(), since)
	recording := logging.RequestArchiveEnabled() && h.cfg.RequestLog && !h.cfg.CommercialMode && !h.cfg.Home.Enabled

	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	auths := make(map[string]*coreauth.Auth)
	if manager != nil {
		for _, auth := range manager.List() {
			if auth != nil {
				auths[auth.ID] = auth
			}
		}
	}

	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, buildQuotaHistoryResponse(samples, auths, recording, since, now))
}

func parseQuotaHistorySince(raw string, now time.Time) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return now.Add(-quotaHistoryDefaultSpan), nil
	}
	since, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, errors.New("since must be an RFC 3339 timestamp")
	}
	if earliest := now.Add(-quotaHistoryMaxSpan); since.Before(earliest) {
		since = earliest
	}
	return since, nil
}

func buildQuotaHistoryResponse(samples []quotaHistorySample, auths map[string]*coreauth.Auth, recording bool, since, now time.Time) quotaHistoryResponse {
	response := quotaHistoryResponse{Recording: recording, Since: since.UTC(), Until: now.UTC(), Credentials: []quotaHistoryCredential{}}
	sinceMs := since.UnixMilli()

	type seriesKey struct{ authID, provider, window string }
	grouped := make(map[seriesKey][]quotaHistoryWindowPoint)
	minutes := make(map[seriesKey]int)
	var oldest int64
	for _, sample := range samples {
		if sample.ObservedMs < sinceMs {
			continue
		}
		if oldest == 0 || sample.ObservedMs < oldest {
			oldest = sample.ObservedMs
		}
		for _, window := range sample.Windows {
			key := seriesKey{sample.AuthID, sample.Provider, window.ID}
			grouped[key] = append(grouped[key], quotaHistoryWindowPoint{sample.ObservedMs, window.UsedPercent, window.ResetAtMs})
			minutes[key] = window.WindowMinutes
		}
	}
	if oldest != 0 {
		at := time.UnixMilli(oldest).UTC()
		response.OldestAt = &at
	}

	credentials := make(map[string]*quotaHistoryCredential)
	for key, points := range grouped {
		credential := credentials[key.authID+"\x00"+key.provider]
		if credential == nil {
			credential = &quotaHistoryCredential{AuthID: key.authID, Provider: key.provider}
			if auth := auths[key.authID]; auth != nil {
				auth.EnsureIndex()
				credential.AuthIndex = auth.Index
				credential.Name = strings.TrimSpace(auth.FileName)
				if credential.Name == "" {
					credential.Name = auth.ID
				}
			}
			credentials[key.authID+"\x00"+key.provider] = credential
		}
		credential.Series = append(credential.Series, quotaHistorySeries{
			ID:            key.window,
			WindowMinutes: minutes[key],
			Points:        compactQuotaHistoryPoints(points),
		})
	}
	for _, credential := range credentials {
		sort.Slice(credential.Series, func(i, j int) bool {
			if credential.Series[i].WindowMinutes != credential.Series[j].WindowMinutes {
				return credential.Series[i].WindowMinutes < credential.Series[j].WindowMinutes
			}
			return credential.Series[i].ID < credential.Series[j].ID
		})
		response.Credentials = append(response.Credentials, *credential)
	}
	sort.Slice(response.Credentials, func(i, j int) bool {
		a, b := response.Credentials[i], response.Credentials[j]
		if a.Provider != b.Provider {
			return a.Provider < b.Provider
		}
		return a.AuthID < b.AuthID
	})
	return response
}

type quotaHistoryWindowPoint struct {
	observedMs int64
	used       float64
	resetAtMs  int64
}

// compactQuotaHistoryPoints keeps only the endpoints of runs with an unchanged
// reading. A busy credential produces a sample per request; consecutive
// identical readings add bytes without changing the drawn line.
func compactQuotaHistoryPoints(points []quotaHistoryWindowPoint) [][3]float64 {
	sort.Slice(points, func(i, j int) bool { return points[i].observedMs < points[j].observedMs })
	same := func(a, b quotaHistoryWindowPoint) bool {
		return math.Abs(a.used-b.used) < 0.05 &&
			time.Duration(absInt64(a.resetAtMs-b.resetAtMs))*time.Millisecond <= quotaHistoryResetTolerance
	}
	out := make([][3]float64, 0, len(points))
	for i, point := range points {
		if i > 0 && i < len(points)-1 && same(points[i-1], point) && same(point, points[i+1]) {
			continue
		}
		out = append(out, [3]float64{float64(point.observedMs), point.used, float64(point.resetAtMs)})
	}
	return out
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

// load returns every sample from archive files modified at or after since,
// parsing only files not already cached.
func (cache *quotaHistoryCache) load(dir string, since time.Time) []quotaHistorySample {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil
	}
	entries, err := logging.ListRequestLogFiles(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			log.WithError(err).Warn("quota history: failed to read log directory")
		}
		return nil
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.files == nil || cache.dir != dir {
		cache.files = make(map[string]quotaHistoryFileEntry)
		cache.dir = dir
	}

	type pending struct {
		name        string
		modNs, size int64
	}
	var todo []pending
	present := make(map[string]struct{}, len(entries))
	var keep []string
	for _, entry := range entries {
		name := entry.Name
		if !strings.HasSuffix(name, ".log.zst") {
			continue
		}
		present[name] = struct{}{}
		// A record's mtime is when the request finished, so nothing in an older
		// file can fall inside the requested span.
		if entry.Modified.Before(since) {
			continue
		}
		keep = append(keep, name)
		if cached, ok := cache.files[name]; ok && cached.modNs == entry.Modified.UnixNano() && cached.size == entry.Size {
			continue
		}
		todo = append(todo, pending{name, entry.Modified.UnixNano(), entry.Size})
	}
	for name := range cache.files {
		if _, ok := present[name]; !ok {
			delete(cache.files, name)
		}
	}

	if len(todo) > 0 {
		workers := runtime.NumCPU()
		if workers > 8 {
			workers = 8
		}
		jobs := make(chan pending)
		var wg sync.WaitGroup
		var resultMu sync.Mutex
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for job := range jobs {
					samples, errParse := parseQuotaHistoryFile(filepath.Join(dir, job.name))
					if errParse != nil {
						// A damaged record must not hide the rest of the history; it
						// stays uncached so a later request retries it.
						log.WithError(errParse).Debugf("quota history: skipped %s", job.name)
						continue
					}
					resultMu.Lock()
					cache.files[job.name] = quotaHistoryFileEntry{modNs: job.modNs, size: job.size, samples: samples}
					resultMu.Unlock()
				}
			}()
		}
		for _, job := range todo {
			jobs <- job
		}
		close(jobs)
		wg.Wait()
	}

	var out []quotaHistorySample
	for _, name := range keep {
		out = append(out, cache.files[name].samples...)
	}
	return out
}

func parseQuotaHistoryFile(path string) ([]quotaHistorySample, error) {
	file, err := logging.OpenRequestRecord(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			log.WithError(errClose).Debug("quota history: close archive")
		}
	}()
	return parseQuotaHistoryRecord(file)
}

type quotaHistoryAttempt struct {
	provider string
	authID   string
}

// parseQuotaHistoryRecord extracts quota observations from one request log.
//
// The log lists every upstream attempt's request ("=== API REQUEST n ===",
// carrying the Auth line) before the responses ("=== API RESPONSE n ===",
// carrying status and headers). Reading stops at the downstream
// "=== RESPONSE ===" section, so the client-facing body is never decompressed.
func parseQuotaHistoryRecord(source io.Reader) ([]quotaHistorySample, error) {
	reader := bufio.NewReaderSize(source, quotaHistoryMaxLine)
	attempts := make(map[int]quotaHistoryAttempt)

	const (
		sectionNone = iota
		sectionRequest
		sectionResponse
	)
	section := sectionNone
	index := 0
	requestMetaDone := false
	headerState := 0 // 0 before "Headers:", 1 reading headers, 2 done
	var responseMs int64
	var headers http.Header
	var samples []quotaHistorySample

	flush := func() {
		if section != sectionResponse || len(headers) == 0 {
			return
		}
		attempt, ok := attempts[index]
		if !ok || attempt.authID == "" || responseMs == 0 {
			return
		}
		windows := normalizeQuotaHistoryWindows(attempt.provider, coreauth.CollectQuotaSignals(attempt.provider, headers))
		if len(windows) == 0 {
			return
		}
		samples = append(samples, quotaHistorySample{AuthID: attempt.authID, Provider: attempt.provider, ObservedMs: responseMs, Windows: windows})
	}

	skipping := false
	for {
		raw, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			skipping = true
			continue
		}
		if skipping {
			// Tail of an oversized body line.
			skipping = false
			if err != nil && !errors.Is(err, io.EOF) {
				return samples, err
			}
			if errors.Is(err, io.EOF) {
				break
			}
			continue
		}
		line := string(bytes.TrimRight(raw, "\r\n"))

		if strings.HasPrefix(line, "=== ") && strings.HasSuffix(line, " ===") {
			if line == "=== RESPONSE ===" {
				flush()
				return samples, nil
			}
			if n, ok := quotaHistorySectionIndex(line, "=== API REQUEST "); ok {
				flush()
				section, index, requestMetaDone = sectionRequest, n, false
			} else if n, ok := quotaHistorySectionIndex(line, "=== API RESPONSE "); ok {
				flush()
				section, index, headerState, responseMs, headers = sectionResponse, n, 0, 0, nil
			}
		} else {
			switch section {
			case sectionRequest:
				if requestMetaDone {
					break
				}
				if line == "Headers:" || line == "Body:" {
					requestMetaDone = true
				} else if rest, ok := strings.CutPrefix(line, "Auth: "); ok {
					attempts[index] = parseQuotaHistoryAuth(rest)
				}
			case sectionResponse:
				switch headerState {
				case 0:
					if rest, ok := strings.CutPrefix(line, "Timestamp: "); ok {
						if at, errTime := time.Parse(time.RFC3339Nano, strings.TrimSpace(rest)); errTime == nil {
							responseMs = at.UnixMilli()
						}
					} else if line == "Headers:" {
						headerState = 1
						headers = make(http.Header)
					}
				case 1:
					if line == "" {
						headerState = 2
					} else if key, value, ok := strings.Cut(line, ":"); ok {
						headers.Add(strings.TrimSpace(key), strings.TrimSpace(value))
					}
				}
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return samples, err
		}
	}
	flush()
	return samples, nil
}

func quotaHistorySectionIndex(line, prefix string) (int, bool) {
	rest, ok := strings.CutPrefix(line, prefix)
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(rest, " ==="))
	return n, err == nil && n > 0
}

func parseQuotaHistoryAuth(line string) quotaHistoryAttempt {
	var attempt quotaHistoryAttempt
	for _, part := range strings.Split(line, ", ") {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		switch key {
		case "provider":
			attempt.provider = strings.ToLower(strings.TrimSpace(value))
		case "auth_id":
			attempt.authID = strings.TrimSpace(value)
		}
	}
	return attempt
}

// normalizeQuotaHistoryWindows maps provider rate-limit signals to the
// account-level windows the management UI draws. Window IDs match the IDs the
// UI already uses for live quota (Claude five-hour/seven-day, Codex
// five-hour/weekly). Model-scoped and additional limits are not part of the
// account pool and are left out.
func normalizeQuotaHistoryWindows(provider string, signals map[string]string) []quotaHistoryWindow {
	if len(signals) == 0 {
		return nil
	}
	lookup := make(map[string]string, len(signals))
	for key, value := range signals {
		lookup[strings.ToLower(key)] = value
	}
	number := func(key string) (float64, bool) {
		value, ok := lookup[key]
		if !ok {
			return 0, false
		}
		parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return 0, false
		}
		return parsed, true
	}

	var windows []quotaHistoryWindow
	switch provider {
	case "claude":
		for _, claim := range []struct {
			key, id string
			minutes int
		}{{"5h", "five-hour", 300}, {"7d", "seven-day", 7 * 24 * 60}} {
			utilization, ok := number("anthropic-ratelimit-unified-" + claim.key + "-utilization")
			if !ok {
				continue
			}
			window := quotaHistoryWindow{ID: claim.id, WindowMinutes: claim.minutes, UsedPercent: clampQuotaPercent(utilization * 100)}
			if reset, okReset := number("anthropic-ratelimit-unified-" + claim.key + "-reset"); okReset {
				window.ResetAtMs = int64(reset * 1000)
			}
			windows = append(windows, window)
		}
	case "codex":
		for _, slot := range []string{"primary", "secondary"} {
			used, ok := number("x-codex-" + slot + "-used-percent")
			if !ok {
				continue
			}
			minutes, okMinutes := number("x-codex-" + slot + "-window-minutes")
			if !okMinutes || minutes <= 0 {
				continue
			}
			window := quotaHistoryWindow{WindowMinutes: int(minutes), UsedPercent: clampQuotaPercent(used)}
			switch window.WindowMinutes {
			case 300:
				window.ID = "five-hour"
			case 7 * 24 * 60:
				window.ID = "weekly"
			default:
				window.ID = slot
			}
			if reset, okReset := number("x-codex-" + slot + "-reset-at"); okReset {
				window.ResetAtMs = int64(reset * 1000)
			}
			windows = append(windows, window)
		}
	}
	return windows
}

func clampQuotaPercent(value float64) float64 {
	return math.Round(math.Min(100, math.Max(0, value))*100) / 100
}
