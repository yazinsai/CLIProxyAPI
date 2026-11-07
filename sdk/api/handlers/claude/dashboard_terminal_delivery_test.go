package claude

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

type claudeTerminalWriter struct {
	gin.ResponseWriter
	disconnect context.CancelFunc
	flushErr   error
}

func (w *claudeTerminalWriter) FlushError() error {
	w.ResponseWriter.Flush()
	w.disconnect()
	return w.flushErr
}

func TestClaudeDashboardTerminalDeliveryBeforeDisconnect(t *testing.T) {
	for _, tc := range []struct {
		name     string
		payload  string
		initial  bool
		flushErr error
		want     string
		reason   string
	}{
		{name: "message stop before EOF", payload: "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n", want: "success"},
		{name: "initial message stop", initial: true, payload: "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n", want: "success"},
		{name: "partial message", payload: "event: message_start\ndata: {\"type\":\"message_start\"}\n\n", want: "interrupted", reason: "cancelled"},
		{name: "failed flush", payload: "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n", flushErr: io.ErrClosedPipe, want: "interrupted", reason: "stream_interrupted"},
		{name: "error event", payload: "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"test failure\"}}\n\n", want: "interrupted", reason: "stream_interrupted"},
		{name: "initial error event", initial: true, payload: "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"test failure\"}}\n\n", want: "interrupted", reason: "stream_interrupted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			tracker := dashboard.NewTracker(nil)
			ctx, disconnect := context.WithCancel(context.Background())
			defer disconnect()
			ctx = dashboard.WithTracker(ctx, tracker)
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil).WithContext(ctx)
			c.Writer = &claudeTerminalWriter{ResponseWriter: c.Writer, disconnect: disconnect, flushErr: tc.flushErr}
			dashboard.ObserveModel(ctx, "claude", true)
			state := &claudeSSEStreamState{trailingNewlines: 2}
			data := make(chan []byte, 1)
			if tc.initial {
				state.ObserveChunk([]byte(tc.payload))
				_, _ = c.Writer.Write([]byte(tc.payload))
				c.Writer.(*claudeTerminalWriter).FlushError()
			} else {
				data <- []byte(tc.payload)
			}
			h := NewClaudeCodeAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
			var deliveryErr error
			h.forwardClaudeStreamWithState(c, c.Writer, func(err error) { deliveryErr = err }, data, nil, state)
			row := tracker.Complete(dashboard.Request{StatusCode: 200}, ctx.Err() != nil, false)
			if row.Outcome != tc.want || row.Reason != tc.reason {
				t.Fatalf("outcome=%s/%s, want %s/%s", row.Outcome, row.Reason, tc.want, tc.reason)
			}
			if tc.want == "success" && deliveryErr != nil {
				t.Fatalf("completed delivery reported as %v", deliveryErr)
			}
		})
	}
}
