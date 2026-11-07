package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func TestDashboardMiddlewareUsesFinalInboundStatusAndChildAttempts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := dashboard.NewStore("")
	router := gin.New()
	router.Use(DashboardCompletionMiddleware(store, nil))
	router.POST("/v1/responses", func(c *gin.Context) {
		ctx := c.Request.Context()
		dashboard.ObserveModel(ctx, "requested-alias", true)
		manager := usage.NewManager(16)
		manager.Publish(ctx, usage.Record{Provider: "codex", Model: "upstream", Alias: "requested-alias", RequestedAt: time.Now().UTC(), Failed: true, Fail: usage.Failure{StatusCode: 429, Body: "secret-body-token"}, Stream: true})
		manager.Publish(ctx, usage.Record{Provider: "claude", Model: "upstream", Alias: "requested-alias", RequestedAt: time.Now().UTC(), Stream: true})
		c.Status(http.StatusOK)
	})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"requested-alias"}`)))
	rows, _, _, _ := store.Snapshot(time.Hour)
	if len(rows) != 1 || rows[0].Outcome != "success" || rows[0].StatusCode != 200 || len(rows[0].Attempts) != 2 || rows[0].Model != "requested-alias" {
		t.Fatalf("rows %#v", rows)
	}
}

func TestDashboardMiddlewareExcludesManagementAndCatalogRequests(t *testing.T) {
	store := dashboard.NewStore("")
	router := gin.New()
	router.Use(DashboardCompletionMiddleware(store, nil))
	router.POST("/v8/management/requests/api-call", func(c *gin.Context) { c.Status(200) })
	router.GET("/v1/models", func(c *gin.Context) { c.Status(200) })
	for _, entry := range []struct{ method, path string }{{"POST", "/v8/management/requests/api-call"}, {"GET", "/v1/models"}} {
		router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(entry.method, entry.path, nil))
	}
	rows, _, _, _ := store.Snapshot(time.Hour)
	if len(rows) != 0 {
		t.Fatal("non-generation traffic counted")
	}
}

func TestDashboardMiddlewareRecordsHTTP200InterruptedStream(t *testing.T) {
	store := dashboard.NewStore("")
	router := gin.New()
	router.Use(DashboardCompletionMiddleware(store, nil))
	router.POST("/v1/responses", func(c *gin.Context) {
		dashboard.ObserveCompletion(c.Request.Context(), "model", true, true, false, 502)
		c.Status(200)
	})
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/responses", nil))
	rows, _, _, _ := store.Snapshot(time.Hour)
	if len(rows) != 1 || rows[0].Outcome != "interrupted" || rows[0].StatusCode != 200 {
		t.Fatalf("rows %#v", rows)
	}
}
