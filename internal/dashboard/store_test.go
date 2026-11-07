package dashboard

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/google/uuid"
)

func testRow(at time.Time, model, provider, outcome string) Request {
	return Request{ID: uuid.NewString(), TraceID: uuid.NewString(), Model: model, Provider: provider, StartedAt: at.Add(-time.Second), CompletedAt: at, StatusCode: 200, Outcome: outcome, DurationMS: 1000}
}

func TestStoreWindowOrderingPersistenceAndGrouping(t *testing.T) {
	directory := t.TempDir()
	s := NewStore(directory)
	now := time.Now().UTC()
	s.now = func() time.Time { return now }
	s.coverageStart = now.Add(-25 * time.Hour)
	if errRemove := os.Remove(s.path); errRemove != nil {
		t.Fatal(errRemove)
	}
	for _, row := range []Request{testRow(now.Add(-2*time.Minute), "model-a", "codex-oauth", "success"), testRow(now.Add(-2*time.Hour), "model-b", "codex-api", "failed"), testRow(now.Add(-time.Minute), "model-a", "codex-api", "interrupted"), testRow(now.Add(-time.Hour), "model-c", "plugin", "success")} {
		if errAdd := s.Add(row); errAdd != nil {
			t.Fatal(errAdd)
		}
	}
	rows, _, partial, _ := s.Snapshot(time.Hour)
	if len(rows) != 3 || partial {
		t.Fatalf("window rows=%d partial=%v", len(rows), partial)
	}
	if got := s.LatestProvider("model-a"); got != "codex-api" {
		t.Fatalf("latest provider %q", got)
	}
	groups := Groups(rows, true)
	if groups[0].ID != "model-a" || groups[0].Total != 2 || groups[0].Failed != 1 || groups[0].Success != 1 {
		t.Fatalf("groups %#v", groups)
	}
	reloaded := NewStore(directory)
	reloaded.now = func() time.Time { return now }
	rows, _, _, _ = reloaded.Snapshot(8 * time.Hour)
	if len(rows) != 4 {
		t.Fatalf("persisted rows=%d", len(rows))
	}
	if rows[0].Model != "model-b" || rows[3].Provider != "codex-api" {
		t.Fatalf("ordering %#v", rows)
	}
}

func TestStoreIdleCoverageSurvivesRestart(t *testing.T) {
	directory := t.TempDir()
	s := NewStore(directory)
	start := s.coverageStart
	s2 := NewStore(directory)
	if !s2.coverageStart.Equal(start) {
		t.Fatalf("idle coverage reset: %v != %v", s2.coverageStart, start)
	}
}

func TestStoreRotationAdvancesCoverageBeforeDroppingHistory(t *testing.T) {
	s := NewStore(t.TempDir())
	now := time.Now().UTC()
	s.now = func() time.Time { return now }
	s.coverageStart = now.Add(-8 * time.Hour)
	old := testRow(now.Add(-6*time.Hour), "old", "claude-oauth", "success")
	current := testRow(now.Add(-time.Hour), "new", "codex-api", "success")
	if errAdd := s.Add(old); errAdd != nil {
		t.Fatal(errAdd)
	}
	if errRename := os.Rename(s.path, s.path+".previous"); errRename != nil {
		t.Fatal(errRename)
	}
	s.journalStart = time.Time{}
	if errAdd := s.Add(current); errAdd != nil {
		t.Fatal(errAdd)
	}
	file, errOpen := os.OpenFile(s.path, os.O_WRONLY, 0600)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	if errTruncate := file.Truncate(maxJournalBytes); errTruncate != nil {
		t.Fatal(errTruncate)
	}
	_ = file.Close()
	latest := testRow(now, "latest", "plugin", "success")
	if errAdd := s.Add(latest); errAdd != nil {
		t.Fatal(errAdd)
	}
	if !s.coverageStart.Equal(current.CompletedAt) {
		t.Fatalf("rotation coverage=%v", s.coverageStart)
	}
	if _, ok := s.Get(old.ID); ok {
		t.Fatal("discarded journal history still presented")
	}
	reloaded := NewStore(filepath.Dir(s.path))
	if reloaded.coverageStart.Before(current.CompletedAt) {
		t.Fatalf("restart claims dropped coverage %v", reloaded.coverageStart)
	}
}

func TestTrackerCountsRetriesOnceAndSanitizesErrors(t *testing.T) {
	tracker := NewTracker(func(_ string, provider string) string { return provider + "-oauth" })
	ctx := WithTracker(context.Background(), tracker)
	ObserveModel(ctx, "alias-a", true)
	start := time.Now().UTC().Add(-time.Second)
	ObserveAttempt(ctx, Attempt{ID: "first", Model: "upstream-model", Provider: "codex", StartedAt: start, CompletedAt: start.Add(time.Millisecond), StatusCode: 429, Outcome: "failed", Reason: FailureReason(429, "secret-token")}, "private-auth-id", "alias-a", true)
	ObserveAttempt(ctx, Attempt{ID: "last", Model: "upstream-model", Provider: "claude", StartedAt: start, CompletedAt: start.Add(time.Second), StatusCode: 200, Outcome: "success"}, "private-auth-id", "alias-a", true)
	ObserveAttempt(ctx, Attempt{ID: "last", Model: "side-model", Provider: "claude", Outcome: "success"}, "private-auth-id", "alias-a", true)
	row := tracker.Complete(testRow(time.Now().UTC(), "", "", ""), false, false)
	if row.Model != "alias-a" || row.Provider != "claude-oauth" || row.Outcome != "success" || len(row.Attempts) != 2 {
		t.Fatalf("row %#v", row)
	}
	if row.Attempts[0].Reason != "rate_limited" {
		t.Fatal("raw failure code retained")
	}
	store := NewStore(t.TempDir())
	if errAdd := store.Add(row); errAdd != nil {
		t.Fatal(errAdd)
	}
	body, errRead := os.ReadFile(store.path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	if strings.Contains(string(body), "secret-token") || strings.Contains(string(body), "private-auth-id") {
		t.Fatal("private material persisted")
	}
}

func TestTrackerHTTP200TerminalStreamFailureAndCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		tracker := NewTracker(nil)
		ctx := WithTracker(context.Background(), tracker)
		ObserveCompletion(ctx, "model", true, true, cancelled, 200)
		row := tracker.Complete(testRow(time.Now().UTC(), "", "", ""), false, false)
		if row.StatusCode != 200 || row.Outcome != "interrupted" {
			t.Fatalf("row %#v", row)
		}
		if cancelled && row.Reason != "cancelled" {
			t.Fatal("cancellation reason missing")
		}
	}
}

func TestStoreConcurrentReadsAndOutOfOrderCompletions(t *testing.T) {
	s := NewStore("")
	now := time.Now().UTC()
	s.now = func() time.Time { return now }
	s.coverageStart = now.Add(-time.Hour)
	var group sync.WaitGroup
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			_ = s.Add(testRow(now.Add(-time.Duration(i)*time.Second), "model", "provider", "success"))
			s.Snapshot(time.Hour)
			s.LatestProvider("model")
		}(i)
	}
	group.Wait()
	if len(s.rows) != 20 {
		t.Fatalf("rows=%d", len(s.rows))
	}
	for i := 1; i < len(s.rows); i++ {
		if s.rows[i].CompletedAt.Before(s.rows[i-1].CompletedAt) {
			t.Fatal("unordered completions")
		}
	}
}

func TestWebsocketTurnsHaveIndependentCompletionRowsAndFinalizeOnce(t *testing.T) {
	store := NewStore("")
	root := NewTracker(nil)
	root.SocketRoot(store, uuid.NewString())
	requestCtx := WithTracker(context.Background(), root)
	for _, model := range []string{"first-model", "second-model"} {
		ctx := InheritTracker(context.Background(), requestCtx)
		ObserveModel(ctx, model, true)
		ObserveCompletion(ctx, model, true, false, false, 200)
		CompleteSocket(ctx, uuid.NewString())
		CompleteSocket(ctx, uuid.NewString())
	}
	rows, _, _, _ := store.Snapshot(time.Hour)
	if len(rows) != 2 || rows[0].Model != "first-model" || rows[1].Model != "second-model" {
		t.Fatalf("websocket rows %#v", rows)
	}
}

func TestStoreSchemaTwoIgnoresFaultyTelemetryAndResetsCoverage(t *testing.T) {
	directory := t.TempDir()
	old := testRow(time.Now().UTC().Add(-time.Hour), "model", "provider", "interrupted")
	old.Reason = "cancelled"
	doc := ojson.NewDocString("{}")
	for key, value := range map[string]any{"schema_version": 1, "coverage_start": old.StartedAt, "request": old} {
		if errSet := doc.Set(key, value); errSet != nil {
			t.Fatal(errSet)
		}
	}
	if errWrite := os.WriteFile(filepath.Join(directory, "dashboard-completions.jsonl"), append(doc.Bytes(), '\n'), 0600); errWrite != nil {
		t.Fatal(errWrite)
	}
	store := NewStore(directory)
	rows, coverage, partial, _ := store.Snapshot(24 * time.Hour)
	if len(rows) != 0 || !coverage.After(old.CompletedAt) || !partial || store.degraded {
		t.Fatalf("migration rows=%d coverage=%v partial=%v degraded=%v", len(rows), coverage, partial, store.degraded)
	}
	current := testRow(time.Now().UTC(), "model", "provider", "success")
	if errAdd := store.Add(current); errAdd != nil {
		t.Fatal(errAdd)
	}
	reloaded := NewStore(directory)
	rows, afterCoverage, _, _ := reloaded.Snapshot(24 * time.Hour)
	if len(rows) != 1 || rows[0].ID != current.ID || !afterCoverage.Equal(coverage) {
		t.Fatalf("reloaded migration rows=%#v coverage=%v", rows, afterCoverage)
	}
}
