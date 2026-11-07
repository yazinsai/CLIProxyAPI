package management

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
)

func (h *Handler) SetDashboardStore(store *dashboard.Store) {
	if h != nil {
		h.dashboardStore = store
	}
}

func dashboardWindow(c *gin.Context) (string, time.Duration, bool) {
	window := c.DefaultQuery("window", "8h")
	switch window {
	case "1h":
		return window, time.Hour, true
	case "8h":
		return window, 8 * time.Hour, true
	case "24h":
		return window, 24 * time.Hour, true
	}
	c.JSON(http.StatusBadRequest, gin.H{"error": "window must be 1h, 8h or 24h"})
	return "", 0, false
}

func (h *Handler) GetDashboardRequests(c *gin.Context) {
	if h == nil || h.dashboardStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "dashboard request reporting unavailable"})
		return
	}
	window, duration, ok := dashboardWindow(c)
	if !ok {
		return
	}
	limit, errLimit := strconv.Atoi(c.DefaultQuery("limit", "8"))
	if errLimit != nil || limit < 1 || limit > 100 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be between 1 and 100"})
		return
	}
	outcome := c.Query("outcome")
	if outcome != "" && outcome != "failed" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid outcome"})
		return
	}
	var cursorTime time.Time
	var cursorID string
	if cursor := c.Query("cursor"); cursor != "" {
		var errCursor error
		cursorTime, cursorID, errCursor = dashboard.ParseCursor(cursor)
		if errCursor != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cursor"})
			return
		}
	}
	rows, coverage, partial, observed := h.dashboardStore.Snapshot(duration)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].CompletedAt.Equal(rows[j].CompletedAt) {
			return rows[i].ID > rows[j].ID
		}
		return rows[i].CompletedAt.After(rows[j].CompletedAt)
	})
	result := make([]dashboard.Request, 0, limit)
	var nextCursor *string
	for _, row := range rows {
		if outcome == "failed" && row.Outcome == "success" {
			continue
		}
		if !cursorTime.IsZero() && (row.CompletedAt.After(cursorTime) || (row.CompletedAt.Equal(cursorTime) && row.ID >= cursorID)) {
			continue
		}
		if len(result) == limit {
			cursor := dashboard.Cursor(result[len(result)-1])
			nextCursor = &cursor
			break
		}
		row.Attempts = nil
		result = append(result, row)
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"supported": true, "observed_at": observed, "coverage_start": coverage, "partial": partial, "window": window, "requests": result, "next_cursor": nextCursor})
}

func (h *Handler) GetDashboardRequest(c *gin.Context) {
	if h == nil || h.dashboardStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "dashboard request reporting unavailable"})
		return
	}
	row, ok := h.dashboardStore.Get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "request not found"})
		return
	}
	if row.Attempts == nil {
		row.Attempts = []dashboard.Attempt{}
	}
	// Check only archive filenames/pack metadata on an explicit detail request.
	// Request bodies are never decompressed to populate the dashboard.
	if row.TraceID != "" {
		if entries, errList := logging.ListRequestLogFiles(h.logDirectory()); errList == nil {
			suffix := "-" + logging.ShortRequestID(row.TraceID) + ".log"
			for _, entry := range entries {
				if strings.HasSuffix(strings.TrimSuffix(entry.Name, ".zst"), suffix) {
					id := row.TraceID
					row.ArchiveRequestID = &id
					break
				}
			}
		}
	}
	c.Header("Cache-Control", "no-store")
	// Explicit map keeps an empty attempts array visible despite omitempty.
	c.JSON(http.StatusOK, gin.H{"supported": true, "request": gin.H{"id": row.ID, "trace_id": row.TraceID, "model": row.Model, "provider": row.Provider, "started_at": row.StartedAt, "completed_at": row.CompletedAt, "status_code": row.StatusCode, "outcome": row.Outcome, "duration_ms": row.DurationMS, "reason": row.Reason, "archive_request_id": row.ArchiveRequestID, "attempts": row.Attempts}})
}

func (h *Handler) GetDashboardSummary(c *gin.Context) {
	if h == nil || h.dashboardStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "dashboard request reporting unavailable"})
		return
	}
	window, duration, ok := dashboardWindow(c)
	if !ok {
		return
	}
	rows, coverage, partial, observed := h.dashboardStore.Snapshot(duration)
	success := 0
	for _, row := range rows {
		if row.Outcome == "success" {
			success++
		}
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"supported": true, "observed_at": observed, "coverage_start": coverage, "partial": partial, "window": window, "total": len(rows), "success": success, "failed": len(rows) - success, "by_model": dashboard.Groups(rows, true), "by_provider": dashboard.Groups(rows, false)})
}
