package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestDashboardRoutesHandlerMissingRuntimeIsUnknown(t *testing.T) {
	h := &Handler{}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	h.GetDashboardRoutes(ctx)
	if recorder.Code != http.StatusOK || ojson.GetBytesNoCopy(recorder.Body.Bytes(), "runtime_known").Bool() {
		t.Fatalf("missing runtime reported ready: %s", recorder.Body.String())
	}
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("route observation can be cached")
	}
}

func TestDashboardRoutesHandlerKeepsLastUsedProviderWithoutCredentials(t *testing.T) {
	const model = "dashboard-handler-model"
	const credential = "dashboard-handler-private-account"
	manager := coreauth.NewManager(nil, nil, nil)
	if _, err := manager.Register(context.Background(), &coreauth.Auth{
		ID: credential, Provider: "claude", Status: coreauth.StatusActive,
		Attributes: map[string]string{"api_key": "dashboard-private-key"},
		Metadata:   map[string]any{"email": "dashboard-private-email@example.test"},
	}); err != nil {
		t.Fatal(err)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(credential, "claude", []*registry.ModelInfo{{ID: model, DisplayName: "Pinned model"}})
	t.Cleanup(func() { reg.UnregisterClient(credential) })
	store := dashboard.NewStore("")
	now := time.Now()
	if err := store.Add(dashboard.Request{
		ID: "dashboard-public-request", Model: model, Provider: "claude-api", StartedAt: now,
		CompletedAt: now, StatusCode: 200, Outcome: "success",
	}); err != nil {
		t.Fatal(err)
	}
	h := &Handler{authManager: manager, dashboardStore: store}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	h.GetDashboardRoutes(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	found := false
	for _, row := range ojson.GetBytesNoCopy(recorder.Body.Bytes(), "models").Array() {
		if row.Get("id").String() == model {
			found = true
			if row.Get("last_used_provider").String() != "claude-api" {
				t.Fatalf("last provider = %s", row.Raw)
			}
		}
	}
	if !found {
		t.Fatal("pinned model disappeared")
	}
	for _, secret := range []string{credential, "dashboard-private-key", "dashboard-private-email"} {
		if strings.Contains(recorder.Body.String(), secret) {
			t.Fatalf("route response leaked %q", secret)
		}
	}
}
