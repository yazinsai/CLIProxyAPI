package middleware

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// DashboardCompletionMiddleware observes metadata without reading payloads. It
// is independent of full request logging and never observes management traffic.
func DashboardCompletionMiddleware(store *dashboard.Store, manager *coreauth.Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		if store == nil || c.Request == nil || !dashboardRequest(c.Request) {
			c.Next()
			return
		}
		started := time.Now().UTC()
		tracker := dashboard.NewTracker(func(authID, provider string) string {
			var auth *coreauth.Auth
			if manager != nil {
				auth, _ = manager.GetByID(authID)
			}
			id, _ := coreauth.DashboardProviderGroup(auth, provider)
			return id
		})
		websocket := c.Request.Method == http.MethodGet
		if websocket {
			tracker.SocketRoot(store, logging.GetGinRequestID(c))
		}
		c.Request = c.Request.WithContext(dashboard.WithTracker(c.Request.Context(), tracker))
		writer := &dashboardWriter{ResponseWriter: c.Writer}
		c.Writer = writer
		defer func() {
			if websocket {
				return
			}
			completed := time.Now().UTC()
			row := tracker.Complete(dashboard.Request{ID: uuid.NewString(), TraceID: logging.GetGinRequestID(c), StartedAt: started, CompletedAt: completed, StatusCode: c.Writer.Status(), DurationMS: completed.Sub(started).Milliseconds()}, c.Request.Context().Err() != nil, writer.failed)
			if errAdd := store.Add(row); errAdd != nil {
				log.Warn("dashboard completion persistence unavailable; historical coverage is partial")
			}
		}()
		c.Next()
	}
}

type dashboardWriter struct {
	gin.ResponseWriter
	failed bool
}

func (w *dashboardWriter) Write(body []byte) (int, error) {
	n, errWrite := w.ResponseWriter.Write(body)
	w.failed = w.failed || errWrite != nil
	return n, errWrite
}
func (w *dashboardWriter) WriteString(body string) (int, error) {
	n, errWrite := w.ResponseWriter.WriteString(body)
	w.failed = w.failed || errWrite != nil
	return n, errWrite
}

func dashboardRequest(req *http.Request) bool {
	if req.URL == nil {
		return false
	}
	path := req.URL.Path
	if req.Method == http.MethodGet {
		return (path == "/v1/responses" || path == "/backend-api/codex/responses") && strings.EqualFold(req.Header.Get("Upgrade"), "websocket")
	}
	if req.Method != http.MethodPost {
		return false
	}
	switch path {
	case "/v1/chat/completions", "/v1/completions", "/v1/responses", "/v1/messages", "/v1/embeddings", "/v1/images/generations", "/v1/images/edits", "/v1/audio/speech", "/v1/audio/transcriptions", "/backend-api/codex/responses", "/v1beta/interactions":
		return true
	}
	return (strings.HasPrefix(path, "/v1beta/models/") || strings.HasPrefix(path, "/v1/models/")) && (strings.HasSuffix(path, ":generateContent") || strings.HasSuffix(path, ":streamGenerateContent"))
}
