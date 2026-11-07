package management

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
)

func dashboardTestHandler(t *testing.T) (*Handler, *gin.Engine, []dashboard.Request) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	store := dashboard.NewStore(t.TempDir())
	rows := []dashboard.Request{}
	for i, outcome := range []string{"success", "failed", "interrupted"} {
		completed := time.Now().UTC()
		row := dashboard.Request{ID: uuid.NewString(), TraceID: uuid.NewString(), Model: "exact-alias", Provider: "codex-oauth", StartedAt: completed.Add(-time.Second), CompletedAt: completed, StatusCode: 200, Outcome: outcome, DurationMS: 1000, Reason: ""}
		if i == 1 {
			row.StatusCode = 429
			row.Reason = "rate_limited"
		}
		if i == 2 {
			row.Reason = "stream_interrupted"
		}
		if errAdd := store.Add(row); errAdd != nil {
			t.Fatal(errAdd)
		}
		rows = append(rows, row)
	}
	h := &Handler{dashboardStore: store}
	router := gin.New()
	router.GET("/requests", h.GetDashboardRequests)
	router.GET("/requests/:id", h.GetDashboardRequest)
	router.GET("/summary", h.GetDashboardSummary)
	return h, router, rows
}

func dashboardGet(router *gin.Engine, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest("GET", path, nil))
	return recorder
}

func TestDashboardRequestSummaryCountsClientOutcomes(t *testing.T) {
	_, router, _ := dashboardTestHandler(t)
	recorder := dashboardGet(router, "/summary?window=8h")
	if recorder.Code != 200 {
		t.Fatalf("summary status %d", recorder.Code)
	}
	doc := ojson.ParseBytes(recorder.Body.Bytes())
	if doc.Get("total").Int() != 3 || doc.Get("success").Int() != 1 || doc.Get("failed").Int() != 2 || !doc.Get("partial").Bool() {
		t.Fatalf("summary %s", recorder.Body.String())
	}
	if doc.Get("by_model.0.id").String() != "exact-alias" || doc.Get("by_provider.0.id").String() != "codex-oauth" {
		t.Fatal("incorrect grouping")
	}
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("missing cache isolation")
	}
}

func TestDashboardRequestsErrorFilterPaginationAndDetails(t *testing.T) {
	_, router, rows := dashboardTestHandler(t)
	recorder := dashboardGet(router, "/requests?window=8h&limit=1&outcome=failed")
	doc := ojson.ParseBytes(recorder.Body.Bytes())
	if recorder.Code != 200 || doc.Get("requests.#").Int() != 1 || doc.Get("requests.0.id").String() != rows[2].ID {
		t.Fatalf("feed %s", recorder.Body.String())
	}
	cursor := doc.Get("next_cursor").String()
	if cursor == "" {
		t.Fatal("missing cursor")
	}
	recorder = dashboardGet(router, "/requests?window=8h&limit=1&outcome=failed&cursor="+url.QueryEscape(cursor))
	doc = ojson.ParseBytes(recorder.Body.Bytes())
	if doc.Get("requests.0.id").String() != rows[1].ID || doc.Get("next_cursor").Type != ojson.Null {
		t.Fatalf("page %s", recorder.Body.String())
	}
	recorder = dashboardGet(router, "/requests/"+rows[0].ID)
	doc = ojson.ParseBytes(recorder.Body.Bytes())
	if recorder.Code != 200 || !doc.Get("request.attempts").IsArray() || doc.Get("request.archive_request_id").Type != ojson.Null {
		t.Fatalf("detail %s", recorder.Body.String())
	}
	if dashboardGet(router, "/requests/"+uuid.NewString()).Code != 404 {
		t.Fatal("missing detail not 404")
	}
}

func TestDashboardRequestQueryValidationAndUnavailableStore(t *testing.T) {
	_, router, _ := dashboardTestHandler(t)
	for _, path := range []string{"/summary?window=forever", "/requests?limit=10000", "/requests?limit=0", "/requests?outcome=success", "/requests?cursor=malformed"} {
		if dashboardGet(router, path).Code != 400 {
			t.Fatalf("invalid query accepted %s", path)
		}
	}
	h := &Handler{}
	empty := gin.New()
	empty.GET("/summary", h.GetDashboardSummary)
	if dashboardGet(empty, "/summary").Code != 503 {
		t.Fatal("missing store became fabricated zero")
	}
}

func TestDashboardRequestsDefaultsToLatestTen(t *testing.T) {
	h, router, _ := dashboardTestHandler(t)
	for range 9 {
		completed := time.Now().UTC()
		if errAdd := h.dashboardStore.Add(dashboard.Request{ID: uuid.NewString(), Model: "model", Provider: "provider", StartedAt: completed, CompletedAt: completed, StatusCode: 200, Outcome: "success"}); errAdd != nil {
			t.Fatal(errAdd)
		}
	}
	recorder := dashboardGet(router, "/requests?window=8h")
	doc := ojson.ParseBytes(recorder.Body.Bytes())
	if recorder.Code != 200 || doc.Get("requests.#").Int() != 10 || doc.Get("next_cursor").String() == "" {
		t.Fatalf("default feed %s", recorder.Body.String())
	}
}
