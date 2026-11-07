package openai

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
)

type terminalDeliveryWriter struct {
	gin.ResponseWriter
	disconnect context.CancelFunc
	flushErr   error
}

func (w *terminalDeliveryWriter) FlushError() error {
	w.ResponseWriter.Flush()
	w.disconnect()
	return w.flushErr
}

func TestResponsesDashboardTerminalDeliveryBeforeDisconnect(t *testing.T) {
	for _, tc := range []struct {
		name     string
		initial  bool
		support  bool
		payload  string
		flushErr error
		want     string
		reason   string
	}{
		{name: "codex completion before EOF", payload: `{"type":"response.completed","response":{"status":"completed","output":[]}}`, want: "success"},
		{name: "completion in first chunk", initial: true, payload: `{"type":"response.completed","response":{"status":"completed","output":[]}}`, want: "success"},
		{name: "supported producer", support: true, payload: `{"type":"response.completed","response":{"status":"completed","output":[]}}`, want: "success"},
		{name: "partial response", payload: `{"type":"response.in_progress"}`, want: "interrupted", reason: "cancelled"},
		{name: "terminal flush failure", payload: `{"type":"response.completed","response":{"status":"completed","output":[]}}`, flushErr: io.ErrClosedPipe, want: "interrupted", reason: "stream_interrupted"},
		{name: "terminal payload failure", payload: `{"type":"response.failed","response":{"status":"failed","error":{"message":"test failure"}}}`, want: "interrupted", reason: "stream_interrupted"},
		{name: "initial terminal payload failure", initial: true, payload: `{"type":"response.failed","response":{"status":"failed","error":{"message":"test failure"}}}`, want: "interrupted", reason: "stream_interrupted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, c, _ := newResponsesStreamTestHandler(t)
			tracker := dashboard.NewTracker(nil)
			ctx, disconnect := context.WithCancel(context.Background())
			defer disconnect()
			ctx = dashboard.WithTracker(ctx, tracker)
			c.Request = c.Request.WithContext(ctx)
			c.Writer = &terminalDeliveryWriter{ResponseWriter: c.Writer, disconnect: disconnect, flushErr: tc.flushErr}
			dashboard.ObserveModel(ctx, "gpt-6.1-sol", true)
			dashboard.ObserveAttempt(ctx, dashboard.Attempt{Provider: "codex-api", Model: "gpt-6.1-sol", StatusCode: 200, Outcome: "success"}, "", "gpt-6.1-sol", true)
			data := make(chan []byte, 1)
			framer := &responsesSSEFramer{canComplete: func() bool { return tc.support }}
			chunk := []byte("data: " + tc.payload + "\n\n")
			if tc.initial {
				framer.WriteChunk(c.Writer, chunk)
				if errFlush := flushResponsesSSE(c.Writer); errFlush != nil {
					t.Fatal(errFlush)
				}
			} else {
				data <- chunk
			}
			var deliveryErr error
			h.forwardResponsesStream(c, c.Writer, func(err error) { deliveryErr = err }, data, nil, framer)
			row := tracker.Complete(dashboard.Request{StatusCode: http.StatusOK}, ctx.Err() != nil, false)
			if row.Outcome != tc.want || row.Reason != tc.reason {
				t.Fatalf("outcome=%s/%s, want %s/%s", row.Outcome, row.Reason, tc.want, tc.reason)
			}
			if tc.want == "success" && deliveryErr != nil {
				t.Fatalf("completed delivery reported as %v", deliveryErr)
			}
			if tc.flushErr != nil && !errors.Is(deliveryErr, tc.flushErr) {
				t.Fatalf("flush failure lost: %v", deliveryErr)
			}
		})
	}
}
