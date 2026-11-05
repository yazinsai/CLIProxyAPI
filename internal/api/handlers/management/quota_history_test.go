package management

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

const quotaHistoryRecordFixture = `=== REQUEST INFO ===
URL: /v1/responses
Timestamp: 2026-11-05T16:17:16.144614+03:00

=== HEADERS ===
Content-Type: application/json

=== REQUEST BODY ===
{"input":"X-Codex-Primary-Used-Percent: 1"}

=== API REQUEST 1 ===
Timestamp: 2026-11-05T16:17:16.163878+03:00
Upstream URL: https://chatgpt.com/backend-api/codex/responses
HTTP Method: POST
Auth: provider=codex, auth_id=codex-a.json, label=a@example.com, type=oauth

Headers:
Accept: text/event-stream

Body:
{}

=== API REQUEST 2 ===
Timestamp: 2026-11-05T16:17:17.000000+03:00
Upstream URL: https://api.anthropic.com/v1/messages
HTTP Method: POST
Auth: provider=claude, auth_id=claude-b.json, label=b@example.com, type=oauth

Headers:
Accept: text/event-stream

Body:
{}

=== API RESPONSE 1 ===
Timestamp: 2026-11-05T16:17:16.500000+03:00

Status: 429
Headers:
X-Codex-Primary-Used-Percent: 100
X-Codex-Primary-Window-Minutes: 300
X-Codex-Primary-Reset-At: 1793892278
X-Codex-Secondary-Used-Percent: 38
X-Codex-Secondary-Window-Minutes: 10080
X-Codex-Secondary-Reset-At: 1794396024

{"error":"usage_limit_reached"}

=== API RESPONSE 2 ===
Timestamp: 2026-11-05T16:17:18.000000+03:00

Status: 200
Headers:
Anthropic-Ratelimit-Unified-5h-Utilization: 0.74
Anthropic-Ratelimit-Unified-5h-Reset: 1793898000
Anthropic-Ratelimit-Unified-7d-Utilization: 0.56
Anthropic-Ratelimit-Unified-7d-Reset: 1794337200

data: {"type":"message_start"}

=== RESPONSE ===
X-Codex-Primary-Used-Percent: 5
`

func TestParseQuotaHistoryRecordPairsAttemptsWithResponses(t *testing.T) {
	samples, err := parseQuotaHistoryRecord(strings.NewReader(quotaHistoryRecordFixture))
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 2 {
		t.Fatalf("got %d samples, want 2: %+v", len(samples), samples)
	}
	codex, claude := samples[0], samples[1]
	if codex.AuthID != "codex-a.json" || codex.Provider != "codex" {
		t.Fatalf("codex attempt mismatched: %+v", codex)
	}
	if len(codex.Windows) != 2 || codex.Windows[0].ID != "five-hour" || codex.Windows[0].UsedPercent != 100 ||
		codex.Windows[1].ID != "weekly" || codex.Windows[1].UsedPercent != 38 || codex.Windows[1].ResetAtMs != 1794396024000 {
		t.Fatalf("codex windows: %+v", codex.Windows)
	}
	if claude.AuthID != "claude-b.json" || len(claude.Windows) != 2 ||
		claude.Windows[0].ID != "five-hour" || claude.Windows[0].UsedPercent != 74 ||
		claude.Windows[1].ID != "seven-day" || claude.Windows[1].UsedPercent != 56 {
		t.Fatalf("claude windows: %+v", claude)
	}
	want := time.Date(2026, 11, 5, 13, 17, 18, 0, time.UTC).UnixMilli()
	if claude.ObservedMs != want {
		t.Fatalf("observed %d, want %d", claude.ObservedMs, want)
	}
}

func TestParseQuotaHistoryRecordSkipsOversizedBodyLines(t *testing.T) {
	record := strings.Replace(quotaHistoryRecordFixture, `{"input":"X-Codex-Primary-Used-Percent: 1"}`,
		strings.Repeat("x", quotaHistoryMaxLine*3), 1)
	samples, err := parseQuotaHistoryRecord(strings.NewReader(record))
	if err != nil || len(samples) != 2 {
		t.Fatalf("samples=%d err=%v", len(samples), err)
	}
}

func TestCompactQuotaHistoryPointsKeepsRunEndpoints(t *testing.T) {
	points := []quotaHistoryWindowPoint{
		{1000, 10, 9_000_000}, {2000, 10, 9_000_500}, {3000, 10, 9_000_000}, {4000, 20, 9_000_000}, {5000, 20, 9_000_000},
	}
	got := compactQuotaHistoryPoints(points)
	if len(got) != 4 || got[0][0] != 1000 || got[1][0] != 3000 || got[2][0] != 4000 || got[3][0] != 5000 {
		t.Fatalf("got %v", got)
	}
}

func TestGetQuotaHistoryReadsArchive(t *testing.T) {
	dir := t.TempDir()
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = encoder.Write([]byte(quotaHistoryRecordFixture)); err != nil {
		t.Fatal(err)
	}
	if err = encoder.Close(); err != nil {
		t.Fatal(err)
	}
	recent := filepath.Join(dir, "v1-responses-2026-11-05T161716-a.log.zst")
	old := filepath.Join(dir, "v1-responses-2026-10-01T000000-b.log.zst")
	for _, path := range []string{recent, old} {
		if err = os.WriteFile(path, compressed.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// The old file is outside the default span and must not be parsed at all.
	if err = os.Chtimes(old, time.Now().Add(-30*24*time.Hour), time.Now().Add(-30*24*time.Hour)); err != nil {
		t.Fatal(err)
	}

	h := &Handler{cfg: &config.Config{}, logDir: dir}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v8/management/credentials/quota/history?since="+
		time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), nil)
	h.GetQuotaHistory(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	var response quotaHistoryResponse
	if err = json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Credentials) != 2 || response.Credentials[0].Provider != "claude" || response.Credentials[1].AuthID != "codex-a.json" {
		t.Fatalf("credentials: %+v", response.Credentials)
	}
	if len(h.quotaHistory.files) != 1 {
		t.Fatalf("parsed %d files, want only the recent one", len(h.quotaHistory.files))
	}
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("history response can be cached")
	}

	// Compaction must preserve both cached observations and a fresh cache rebuild.
	packArchiveFixture(t, recent, old)
	for _, handler := range []*Handler{h, {cfg: &config.Config{}, logDir: dir}} {
		packedRec := httptest.NewRecorder()
		packedCtx, _ := gin.CreateTestContext(packedRec)
		packedCtx.Request = ctx.Request.Clone(ctx.Request.Context())
		handler.GetQuotaHistory(packedCtx)
		var packedResponse quotaHistoryResponse
		if err = json.Unmarshal(packedRec.Body.Bytes(), &packedResponse); err != nil {
			t.Fatal(err)
		}
		if packedRec.Code != http.StatusOK || !reflect.DeepEqual(packedResponse.Credentials, response.Credentials) || !reflect.DeepEqual(packedResponse.OldestAt, response.OldestAt) {
			t.Fatalf("compaction changed quota history: %s", packedRec.Body.String())
		}
	}

	bad := httptest.NewRecorder()
	badCtx, _ := gin.CreateTestContext(bad)
	badCtx.Request = httptest.NewRequest(http.MethodGet, "/x?since=yesterday", nil)
	h.GetQuotaHistory(badCtx)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("invalid since accepted: %d", bad.Code)
	}
}

// TestQuotaHistoryLocalArchive parses a real archive directory when
// QUOTA_HISTORY_ARCHIVE_DIR is set; it never runs in CI.
func TestQuotaHistoryLocalArchive(t *testing.T) {
	dir := os.Getenv("QUOTA_HISTORY_ARCHIVE_DIR")
	if dir == "" {
		t.Skip("QUOTA_HISTORY_ARCHIVE_DIR not set")
	}
	var cache quotaHistoryCache
	start := time.Now()
	samples := cache.load(dir, time.Now().Add(-quotaHistoryDefaultSpan))
	first := time.Since(start)
	start = time.Now()
	cache.load(dir, time.Now().Add(-quotaHistoryDefaultSpan))
	response := buildQuotaHistoryResponse(samples, nil, true, time.Now().Add(-quotaHistoryDefaultSpan), time.Now())
	body, _ := json.Marshal(response)
	t.Logf("files=%d samples=%d first=%s cached=%s bytes=%d", len(cache.files), len(samples), first, time.Since(start), len(body))
	if out := os.Getenv("QUOTA_HISTORY_DUMP"); out != "" {
		_ = os.WriteFile(out, body, 0600)
	}
}
