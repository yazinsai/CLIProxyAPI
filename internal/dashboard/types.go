// Package dashboard stores bounded, sanitized client-request completion metadata.
// It does not retain request bodies, headers, credentials or upstream error text.
package dashboard

import (
	"context"
	"strings"
	"sync"
	"time"
)

type Attempt struct {
	ID          string    `json:"id"`
	Provider    string    `json:"provider"`
	Model       string    `json:"model"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	StatusCode  int       `json:"status_code"`
	Outcome     string    `json:"outcome"`
	DurationMS  int64     `json:"duration_ms"`
	Reason      string    `json:"reason"`
}

type Request struct {
	ID               string    `json:"id"`
	TraceID          string    `json:"trace_id"`
	Model            string    `json:"model"`
	Provider         string    `json:"provider"`
	StartedAt        time.Time `json:"started_at"`
	CompletedAt      time.Time `json:"completed_at"`
	StatusCode       int       `json:"status_code"`
	Outcome          string    `json:"outcome"`
	DurationMS       int64     `json:"duration_ms"`
	Reason           string    `json:"reason"`
	ArchiveRequestID *string   `json:"archive_request_id"`
	Attempts         []Attempt `json:"attempts,omitempty"`
}

type Group struct {
	ID      string `json:"id"`
	Total   int    `json:"total"`
	Success int    `json:"success"`
	Failed  int    `json:"failed"`
}

// Tracker belongs to one inbound request. Synchronous observers avoid racing
// asynchronous usage consumers against the final inbound response.
type Tracker struct {
	mu                sync.Mutex
	model             string
	stream            bool
	closed            bool
	terminalFailed    bool
	terminalCancelled bool
	terminalStatus    int
	socketRoot        bool
	socketStarted     time.Time
	socketTrace       string
	socketStore       *Store
	socketOnce        sync.Once
	attempts          []Attempt
	resolveProvider   func(authID, provider string) string
}

type trackerKey struct{}

func WithTracker(ctx context.Context, tracker *Tracker) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, trackerKey{}, tracker)
}

func InheritTracker(ctx, requestCtx context.Context) context.Context {
	if requestCtx == nil {
		return ctx
	}
	if tracker, ok := requestCtx.Value(trackerKey{}).(*Tracker); ok {
		if tracker.socketRoot {
			child := NewTracker(tracker.resolveProvider)
			child.socketStarted = time.Now().UTC()
			child.socketTrace = tracker.socketTrace
			child.socketStore = tracker.socketStore
			return WithTracker(ctx, child)
		}
		return WithTracker(ctx, tracker)
	}
	return ctx
}

func (t *Tracker) SocketRoot(store *Store, trace string) {
	t.socketRoot = true
	t.socketStore = store
	t.socketTrace = trace
}

// CompleteSocket emits one row per completed websocket client turn instead of
// treating the entire long-lived upgraded connection as one request.
func CompleteSocket(ctx context.Context, id string) {
	t := trackerFrom(ctx)
	if t == nil || t.socketStarted.IsZero() || t.socketStore == nil {
		return
	}
	t.socketOnce.Do(func() {
		t.mu.Lock()
		if t.closed || t.model == "" {
			t.mu.Unlock()
			return
		}
		status := t.terminalStatus
		if status < 200 {
			status = 200
		}
		if t.terminalStatus == 0 && len(t.attempts) > 0 && t.attempts[len(t.attempts)-1].StatusCode >= 400 {
			status = t.attempts[len(t.attempts)-1].StatusCode
		}
		started, trace, store := t.socketStarted, t.socketTrace, t.socketStore
		t.mu.Unlock()
		completed := time.Now().UTC()
		row := t.Complete(Request{ID: id, TraceID: trace, StartedAt: started, CompletedAt: completed, StatusCode: status, DurationMS: completed.Sub(started).Milliseconds()}, false, false)
		_ = store.Add(row)
	})
}

func NewTracker(resolver func(authID, provider string) string) *Tracker {
	return &Tracker{resolveProvider: resolver}
}

func trackerFrom(ctx context.Context) *Tracker {
	if ctx == nil {
		return nil
	}
	tracker, _ := ctx.Value(trackerKey{}).(*Tracker)
	return tracker
}

func ObserveModel(ctx context.Context, model string, stream bool) {
	tracker := trackerFrom(ctx)
	if tracker == nil {
		return
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.closed {
		return
	}
	if tracker.model == "" {
		tracker.model = PublicID(model)
	}
	tracker.stream = tracker.stream || stream
}

func ObserveAttempt(ctx context.Context, attempt Attempt, authID, requestedModel string, stream bool) {
	tracker := trackerFrom(ctx)
	if tracker == nil {
		return
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.closed {
		return
	}
	if tracker.model == "" {
		tracker.model = PublicID(requestedModel)
	}
	tracker.stream = tracker.stream || stream
	if tracker.resolveProvider != nil {
		attempt.Provider = tracker.resolveProvider(authID, attempt.Provider)
	}
	attempt.Provider = PublicID(attempt.Provider)
	attempt.Model = PublicID(attempt.Model)
	// Additional-model usage records share the same execution ID. They must not
	// become extra attempts or inflate completed-client-request counts.
	for _, previous := range tracker.attempts {
		if previous.ID == attempt.ID {
			return
		}
	}
	if len(tracker.attempts) == maxAttempts {
		tracker.attempts = tracker.attempts[1:]
	}
	tracker.attempts = append(tracker.attempts, attempt)
}

// ObserveCompletion tracks failures occurring after usage accounting (for
// example response interceptors and accepted streams without a terminal event).
func ObserveCompletion(ctx context.Context, model string, stream, failed, cancelled bool, status int) {
	tracker := trackerFrom(ctx)
	if tracker == nil {
		return
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.closed {
		return
	}
	if tracker.model == "" {
		tracker.model = PublicID(model)
	}
	tracker.stream = tracker.stream || stream
	tracker.terminalFailed = failed
	tracker.terminalCancelled = cancelled
	tracker.terminalStatus = status
}

func (t *Tracker) Complete(row Request, cancelled, writeFailed bool) Request {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	row.Model = t.model
	row.Attempts = append([]Attempt(nil), t.attempts...)
	row.Outcome = "success"
	if row.StatusCode >= 400 {
		row.Outcome = "failed"
		row.Reason = FailureReason(row.StatusCode, "")
	}
	if len(row.Attempts) > 0 {
		last := row.Attempts[len(row.Attempts)-1]
		row.Provider = last.Provider
		if t.stream && last.Outcome != "success" && row.StatusCode < 400 {
			row.Outcome = "interrupted"
			row.Reason = "stream_interrupted"
		}
	}
	if t.stream && t.terminalFailed && row.StatusCode < 400 {
		row.Outcome = "interrupted"
		row.Reason = "stream_interrupted"
	}
	if cancelled || writeFailed || t.terminalCancelled {
		row.Outcome = "interrupted"
		row.Reason = "cancelled"
	}
	return row
}

const maxAttempts = 16

// PublicID allows only identifiers, not URLs, whitespace-bearing errors or
// arbitrary credential labels. Configured model/group IDs may contain '/'.
func PublicID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 256 || strings.Contains(value, "://") {
		return ""
	}
	for _, r := range value {
		if r < 32 || r == 127 {
			return ""
		}
	}
	return value
}

func FailureReason(status int, code string) string {
	switch code {
	case "stream_truncated", "stream_interrupted":
		return "stream_interrupted"
	case "cancelled", "context_canceled":
		return "cancelled"
	}
	switch {
	case status == 429:
		return "rate_limited"
	case status == 401 || status == 403:
		return "unauthorized"
	case status >= 500:
		return "upstream_error"
	case status >= 400:
		return "invalid_request"
	default:
		return "unknown"
	}
}
