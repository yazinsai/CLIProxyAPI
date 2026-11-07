package auth

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func dashboardRegister(t *testing.T, manager *Manager, auth *Auth, models ...string) {
	t.Helper()
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatal(err)
	}
	infos := make([]*registry.ModelInfo, 0, len(models))
	for _, id := range models {
		infos = append(infos, &registry.ModelInfo{ID: id, DisplayName: id + " display"})
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, infos)
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
}

func dashboardFindModel(t *testing.T, snapshot DashboardRoutes, id string) DashboardModel {
	t.Helper()
	for _, model := range snapshot.Models {
		if model.ID == id {
			return model
		}
	}
	t.Fatalf("model %q missing from %#v", id, snapshot.Models)
	return DashboardModel{}
}

func TestDashboardRoutesPriorityCooldownAndSanitization(t *testing.T) {
	now := time.Now()
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{provider: "claude"})
	const model = "dashboard-model-priority"
	for _, auth := range []*Auth{
		{ID: "dashboard-oauth", Provider: "claude", Status: StatusActive, Attributes: map[string]string{"auth_kind": "oauth", "priority": "5"}, Metadata: map[string]any{"email": "secret@example.test", "access_token": "never-show-token"}, Label: "credential-secret"},
		{ID: "dashboard-apikey", Provider: "claude", Status: StatusActive, Attributes: map[string]string{"api_key": "never-show-key", "priority": "1"}},
		{ID: "dashboard-cooling", Provider: "claude", Status: StatusActive, Attributes: map[string]string{"auth_kind": "oauth", "priority": "9"}, ModelStates: map[string]*ModelState{model: {Unavailable: true, NextRetryAfter: now.Add(4 * time.Minute), Quota: QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: now.Add(4 * time.Minute)}}}},
		{ID: "dashboard-disabled", Provider: "claude", Disabled: true, Attributes: map[string]string{"auth_kind": "oauth", "priority": "20"}},
	} {
		dashboardRegister(t, manager, auth, model, "dashboard-model-sibling")
	}
	snapshot := manager.DashboardRoutesSnapshot(now)
	if !snapshot.RuntimeKnown || !snapshot.ObservedAt.Equal(now) {
		t.Fatalf("invalid observation: %#v", snapshot)
	}
	rows := dashboardFindModel(t, snapshot, model).Routes
	if len(rows) != 4 {
		t.Fatalf("routes = %#v", rows)
	}
	if rows[0].Status != "unavailable" || rows[0].ReadyCredentials != 0 {
		t.Errorf("disabled route is ready: %#v", rows[0])
	}
	if rows[1].Status != "cooling" || rows[1].CooldownReason != "quota" || rows[1].RetryAt == nil || !rows[1].RetryAt.Equal(now.Add(4*time.Minute)) {
		t.Errorf("cooling route = %#v", rows[1])
	}
	if rows[2].Status != "ready" || rows[2].ReadyCredentials != 1 || rows[2].Provider != "claude-oauth" {
		t.Errorf("highest eligible tier = %#v", rows[2])
	}
	if rows[3].Status != "standby" || rows[3].ProviderLabel != "Anthropic API" {
		t.Errorf("lower tier = %#v", rows[3])
	}
	sibling := dashboardFindModel(t, snapshot, "dashboard-model-sibling").Routes
	if sibling[1].Status != "ready" {
		t.Errorf("single-model cooldown blocked sibling: %#v", sibling)
	}
	doc := ojson.NewDocString("{}")
	if err := doc.Set("snapshot", snapshot); err != nil {
		t.Fatal(err)
	}
	encoded := doc.Bytes()
	for _, secret := range []string{"secret@example.test", "never-show", "credential-secret", "dashboard-oauth"} {
		if strings.Contains(string(encoded), secret) {
			t.Errorf("snapshot leaked %q", secret)
		}
	}
}

func TestDashboardRoutesDoNotAdvanceSelectionState(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	const model = "dashboard-model-selection"
	for _, id := range []string{"dashboard-selection-a", "dashboard-selection-b"} {
		dashboardRegister(t, manager, &Auth{ID: id, Provider: "test", Status: StatusActive}, model)
	}
	first, err := manager.SelectAuth(context.Background(), "test", model, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	before := manager.List()
	sort.Slice(before, func(i, j int) bool { return before[i].ID < before[j].ID })
	for range 5 {
		manager.DashboardRoutesSnapshot(time.Now())
	}
	after := manager.List()
	sort.Slice(after, func(i, j int) bool { return after[i].ID < after[j].ID })
	if !reflect.DeepEqual(before, after) {
		t.Fatal("dashboard mutated runtime auth state")
	}
	second, err := manager.SelectAuth(context.Background(), "test", model, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatalf("dashboard advanced the round-robin cursor: %s selected twice", first.ID)
	}
	if len(manager.modelPoolOffsets) != 0 || len(manager.providerOffsets) != 0 {
		t.Fatal("dashboard advanced model/provider rotation")
	}
}

func TestDashboardRoutesUnknownRuntimeAndExpiredToken(t *testing.T) {
	now := time.Now()
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{provider: "codex"})
	const model = "dashboard-model-unknown"
	dashboardRegister(t, manager, &Auth{ID: "dashboard-unknown", Provider: "codex", Status: StatusUnknown, Attributes: map[string]string{"auth_kind": "oauth", "priority": "3"}}, model)
	dashboardRegister(t, manager, &Auth{ID: "dashboard-expired", Provider: "codex", Status: StatusActive, Attributes: map[string]string{"auth_kind": "oauth", "priority": "2"}, Metadata: map[string]any{"access_token": "secret", "expired": now.Add(-time.Minute).Format(time.RFC3339)}}, model)
	dashboardRegister(t, manager, &Auth{ID: "dashboard-lower-known", Provider: "codex", Status: StatusActive, Attributes: map[string]string{"auth_kind": "oauth", "priority": "1"}}, model)
	rows := dashboardFindModel(t, manager.DashboardRoutesSnapshot(now), model).Routes
	if rows[0].Status != "unknown" || rows[0].ReadyCredentials != 0 {
		t.Fatalf("unknown auth is ready: %#v", rows)
	}
	if rows[1].Status != "unavailable" {
		t.Fatalf("expired auth is ready: %#v", rows)
	}
	if rows[2].Status != "unknown" || rows[2].ReadyCredentials != 0 {
		t.Fatalf("higher unknown priority caused premature fallback promotion: %#v", rows)
	}
	manager.SetConfig(&internalconfig.Config{Home: internalconfig.HomeConfig{Enabled: true}})
	snapshot := manager.DashboardRoutesSnapshot(now)
	if snapshot.RuntimeKnown {
		t.Fatal("remote Home eligibility cannot be inferred locally")
	}
	for _, row := range dashboardFindModel(t, snapshot, model).Routes {
		if row.Status == "ready" || row.ReadyCredentials != 0 {
			t.Fatalf("unknown remote state became ready: %#v", row)
		}
	}
	var absent *Manager
	if absent.DashboardRoutesSnapshot(now).RuntimeKnown {
		t.Fatal("missing manager became known")
	}
}

func TestDashboardRoutesWeightedZeroAndPluginProviderGroups(t *testing.T) {
	manager := NewManager(nil, &WeightedRoundRobinSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{provider: "devin"})
	const model = "dashboard-model-plugin"
	dashboardRegister(t, manager, &Auth{ID: "dashboard-plugin-zero", Provider: "devin", Status: StatusActive, Attributes: map[string]string{"weight": "0", "priority": "3"}}, model)
	dashboardRegister(t, manager, &Auth{ID: "dashboard-plugin-positive", Provider: "devin", Status: StatusActive, Attributes: map[string]string{"weight": "1", "priority": "1"}}, model)
	rows := dashboardFindModel(t, manager.DashboardRoutesSnapshot(time.Now()), model).Routes
	if rows[0].Status != "unavailable" || rows[1].Status != "ready" || rows[1].ProviderLabel != "Devin" {
		t.Fatalf("weighted plugin routes = %#v", rows)
	}
	manager.SetPluginScheduler(&fakePluginScheduler{})
	if manager.DashboardRoutesSnapshot(time.Now()).RuntimeKnown {
		t.Fatal("custom plugin scheduling result reported as known without invoking it")
	}
	compat := &Auth{Provider: "openai-compatibility", Label: "private@example.test", Attributes: map[string]string{"compat_name": "Configured group", "provider_key": "configured-group"}}
	id, label := DashboardProviderGroup(compat, "")
	if id != "openai-compatible-configured-group" || label != "Configured group" {
		t.Fatalf("compat group = %q %q", id, label)
	}
	compat.Attributes = nil
	id, label = DashboardProviderGroup(compat, "")
	if strings.Contains(id+label, "private@example.test") {
		t.Fatal("credential label leaked in compatibility fallback")
	}
}

func TestDashboardRoutesAliasAndCredentialWideCooldown(t *testing.T) {
	now := time.Now()
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{provider: "claude"})
	const alias = "dashboard-alias"
	const upstream = "dashboard-upstream"
	manager.SetOAuthModelAlias(map[string][]internalconfig.OAuthModelAlias{
		"claude": {{Name: upstream, Alias: alias}},
	})
	dashboardRegister(t, manager, &Auth{
		ID: "dashboard-alias-credential", Provider: "claude", Status: StatusActive,
		Attributes: map[string]string{"auth_kind": "oauth", "priority": "2"},
		ModelStates: map[string]*ModelState{
			upstream: {Unavailable: true, NextRetryAfter: now.Add(time.Minute), Quota: QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: now.Add(time.Minute)}},
		},
	}, alias, "dashboard-alias-sibling")
	dashboardRegister(t, manager, &Auth{
		ID: "dashboard-global-credential", Provider: "claude", Status: StatusActive,
		Attributes: map[string]string{"auth_kind": "oauth", "priority": "1"},
		Quota:      QuotaState{Exceeded: true, Reason: "credential_quota", NextRecoverAt: now.Add(2 * time.Minute)},
	}, alias, "dashboard-alias-sibling")
	snapshot := manager.DashboardRoutesSnapshot(now)
	rows := dashboardFindModel(t, snapshot, alias).Routes
	if rows[0].Status != "cooling" || rows[0].CooldownReason != "quota" {
		t.Fatalf("upstream alias cooldown ignored: %#v", rows)
	}
	if rows[1].Status != "cooling" || rows[1].CooldownReason != "credential_quota" {
		t.Fatalf("credential quota ignored: %#v", rows)
	}
	sibling := dashboardFindModel(t, snapshot, "dashboard-alias-sibling").Routes
	if sibling[0].Status != "ready" || sibling[1].Status != "cooling" {
		t.Fatalf("cooldown scopes conflated: %#v", sibling)
	}
}

func TestDashboardRoutesExhaustedCompatibilityPoolDoesNotRotate(t *testing.T) {
	now := time.Now()
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.SetConfig(&internalconfig.Config{OpenAICompatibility: []internalconfig.OpenAICompatibility{{
		Name: "dashboard-pool",
		Models: []internalconfig.OpenAICompatibilityModel{
			{Name: "dashboard-pool-upstream-a", Alias: "dashboard-pool-alias"},
			{Name: "dashboard-pool-upstream-b", Alias: "dashboard-pool-alias"},
		},
	}}})
	manager.RegisterExecutor(schedulerTestExecutor{provider: "openai-compatible-dashboard-pool"})
	blockedState := func(minutes int) *ModelState {
		return &ModelState{Unavailable: true, NextRetryAfter: now.Add(time.Duration(minutes) * time.Minute), Quota: QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: now.Add(time.Duration(minutes) * time.Minute)}}
	}
	for _, entry := range []struct {
		id       string
		priority string
		states   map[string]*ModelState
	}{
		{"dashboard-pool-exhausted", "2", map[string]*ModelState{"dashboard-pool-upstream-a": blockedState(2), "dashboard-pool-upstream-b": blockedState(3)}},
		{"dashboard-pool-partial", "1", map[string]*ModelState{"dashboard-pool-upstream-a": blockedState(2)}},
	} {
		dashboardRegister(t, manager, &Auth{
			ID: entry.id, Provider: "openai-compatibility", Status: StatusActive,
			Attributes:  map[string]string{"auth_kind": "apikey", "compat_name": "dashboard-pool", "provider_key": "dashboard-pool", "priority": entry.priority},
			ModelStates: entry.states,
		}, "dashboard-pool-alias")
	}
	rows := dashboardFindModel(t, manager.DashboardRoutesSnapshot(now), "dashboard-pool-alias").Routes
	if rows[0].Status != "cooling" || rows[0].ReadyCredentials != 0 || rows[0].RetryAt == nil || !rows[0].RetryAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("exhausted pool reported ready: %#v", rows)
	}
	if rows[1].Status != "ready" || rows[1].ReadyCredentials != 1 {
		t.Fatalf("one eligible member should keep fallback pool ready: %#v", rows)
	}
	if len(manager.modelPoolOffsets) != 0 {
		t.Fatal("observing pool members advanced model rotation")
	}
}
