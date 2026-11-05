package management

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestRequestArchiveHealthStates(t *testing.T) {
	t.Setenv("CLI_PROXY_REQUEST_ARCHIVE", "1")
	for _, tt := range []struct {
		name, snapshot, expected string
		enabled                  bool
	}{
		{"missing", "", "index_missing", true},
		{"healthy", archiveStatsFixture(time.Now(), "healthy", 50<<30), "healthy", true},
		{"idle", archiveStatsFixture(time.Now(), "healthy", 50<<30), "healthy", true},
		{"stale", archiveStatsFixture(time.Now().Add(-2*time.Minute), "healthy", 50<<30), "index_stale", true},
		{"future", archiveStatsFixture(time.Now().Add(2*time.Minute), "healthy", 50<<30), "index_stale", true},
		{"failed", archiveStatsFixture(time.Now(), "error", 50<<30), "degraded", true},
		{"low disk", archiveStatsFixture(time.Now(), "healthy", 512<<20), "storage_low", true},
		{"invalid", "not json", "index_invalid", true},
		{"oversized", strings.Repeat(" ", archiveStatsMaxBytes+1), "index_invalid", true},
		{"disabled", archiveStatsFixture(time.Now(), "healthy", 50<<30), "disabled", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if tt.snapshot != "" {
				if err := os.WriteFile(filepath.Join(dir, "request-archive-stats.json"), []byte(tt.snapshot), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cfg := &config.Config{}
			cfg.RequestLog = tt.enabled
			h := &Handler{cfg: cfg, logDir: dir}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			h.GetRequestArchive(ctx)
			if recorder.Code != http.StatusOK {
				t.Fatalf("status %d", recorder.Code)
			}
			got := ojson.GetBytesNoCopy(recorder.Body.Bytes(), "status").String()
			if got != tt.expected {
				t.Fatalf("got %s, want %s", got, tt.expected)
			}
			if recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("health response can be cached")
			}
		})
	}
}

func archiveStatsFixture(updated time.Time, state string, free int64) string {
	return fmt.Sprintf(`{"schema_version":1,"updated_at":%q,"state":%q,"records":0,"archive_files":0,"original_bytes":0,"archived_bytes":0,"index_bytes":0,"disk_free_bytes":%d,"disk_total_bytes":107374182400,"latest_at":null,"daily":[]}`, updated.UTC().Format(time.RFC3339Nano), state, free)
}
