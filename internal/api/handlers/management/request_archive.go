package management

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/buildinfo"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
)

const archiveStatsMaxBytes = 128 * 1024

// GetRequestArchive reports authenticated, payload-free archive health. The indexer
// owns aggregation; this endpoint only reads its bounded atomic stats snapshot.
func (h *Handler) GetRequestArchive(c *gin.Context) {
	if h == nil || h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	archiveEnabled := logging.RequestArchiveEnabled()
	recording := archiveEnabled && h.cfg.RequestLog && !h.cfg.CommercialMode && !h.cfg.Home.Enabled
	runtime := logging.RequestArchiveRuntime()
	result := gin.H{
		"supported": true, "enabled": recording, "archive_enabled": archiveEnabled,
		"request_logging_enabled": h.cfg.RequestLog, "status": "index_missing",
		"compression": "zstd", "retention": "indefinite", "runtime": runtime,
		"build": gin.H{"version": buildinfo.Version, "commit": buildinfo.Commit, "upstream_base": logging.ArchiveUpstreamBase},
		"index": nil,
	}
	snapshot, err := readArchiveStats(filepath.Join(h.logDirectory(), "request-archive-stats.json"))
	if err == nil {
		result["index"] = snapshot.Value()
		updated, errTime := time.Parse(time.RFC3339Nano, snapshot.Get("updated_at").String())
		switch {
		case errTime != nil || time.Since(updated) > time.Minute || time.Until(updated) > time.Minute:
			result["status"] = "index_stale"
		case snapshot.Get("state").String() != "healthy" || runtime["failures_since_start"].(uint64) > 0 || snapshot.Get("archive_files").Int() < snapshot.Get("records").Int():
			result["status"] = "degraded"
		case snapshot.Get("disk_free_bytes").Int() < 1024*1024*1024 || (snapshot.Get("disk_total_bytes").Float() > 0 && snapshot.Get("disk_free_bytes").Float()/snapshot.Get("disk_total_bytes").Float() < 0.05):
			result["status"] = "storage_low"
		default:
			result["status"] = "healthy"
		}
	} else if !os.IsNotExist(err) {
		result["status"] = "index_invalid"
	}
	if !recording {
		result["status"] = "disabled"
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, result)
}

func readArchiveStats(path string) (ojson.Result, error) {
	file, err := os.Open(path)
	if err != nil {
		return ojson.Result{}, err
	}
	defer func() { _ = file.Close() }()
	body, err := io.ReadAll(io.LimitReader(file, archiveStatsMaxBytes+1))
	if err != nil {
		return ojson.Result{}, err
	}
	if len(body) > archiveStatsMaxBytes || !ojson.ValidBytes(body) {
		return ojson.Result{}, fmt.Errorf("invalid archive stats")
	}
	snapshot := ojson.ParseBytes(body)
	if !snapshot.IsObject() || snapshot.Get("schema_version").Int() != 1 || !snapshot.Get("records").Exists() {
		return ojson.Result{}, fmt.Errorf("unsupported archive stats schema")
	}
	if snapshot.Get("state").String() != "healthy" && snapshot.Get("state").String() != "error" {
		return ojson.Result{}, fmt.Errorf("invalid archive state")
	}
	for _, key := range []string{"records", "archive_files", "original_bytes", "archived_bytes", "index_bytes", "disk_free_bytes", "disk_total_bytes"} {
		value := snapshot.Get(key)
		if value.Type != ojson.Number || value.Float() < 0 {
			return ojson.Result{}, fmt.Errorf("invalid archive counter")
		}
	}
	return snapshot, nil
}
