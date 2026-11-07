package openai

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
)

type terminalDisconnectTimeline struct{ disconnect context.CancelFunc }

func (l terminalDisconnectTimeline) Append(string, []byte, time.Time) { l.disconnect() }

func TestWebsocketDashboardTerminalDeliveryBeforeDisconnect(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		want    string
		reason  string
	}{
		{name: "completed", payload: `{"type":"response.completed","response":{"status":"completed","output":[]}}`, want: "success"},
		{name: "done", payload: `{"type":"response.done","response":{"status":"completed","output":[]}}`, want: "success"},
		{name: "incomplete", payload: `{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"interrupted"},"output":[]}}`, want: "interrupted", reason: "cancelled"},
		{name: "failed", payload: `{"type":"response.failed","response":{"status":"failed","error":{"message":"test failure"}}}`, want: "interrupted", reason: "stream_interrupted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			results := make(chan dashboard.Request, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, errUpgrade := upgrader.Upgrade(w, r, nil)
				if errUpgrade != nil {
					t.Error(errUpgrade)
					return
				}
				defer conn.Close()
				h, _, c, _ := newResponsesStreamTestHandler(t)
				tracker := dashboard.NewTracker(nil)
				ctx, disconnect := context.WithCancel(context.Background())
				defer disconnect()
				ctx = dashboard.WithTracker(ctx, tracker)
				c.Request = c.Request.WithContext(ctx)
				dashboard.ObserveModel(ctx, "gpt-6.1-sol", true)
				data := make(chan []byte, 1)
				data <- []byte(tc.payload)
				h.forwardResponsesWebsocket(c, newResponsesWebsocketWriter(conn), func(params ...interface{}) {
					var err error
					if len(params) > 0 {
						err, _ = params[0].(error)
					}
					dashboard.ObserveCompletion(ctx, "gpt-6.1-sol", true, err != nil, errors.Is(err, context.Canceled), 0)
				}, data, nil, terminalDisconnectTimeline{disconnect: disconnect}, "test-session")
				results <- tracker.Complete(dashboard.Request{StatusCode: 200}, ctx.Err() != nil, false)
			}))
			defer server.Close()
			conn, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if errDial != nil {
				t.Fatal(errDial)
			}
			defer conn.Close()
			select {
			case row := <-results:
				if row.Outcome != tc.want || row.Reason != tc.reason {
					t.Fatalf("outcome=%s/%s, want %s/%s", row.Outcome, row.Reason, tc.want, tc.reason)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("forwarder did not finish after disconnect")
			}
		})
	}
}
