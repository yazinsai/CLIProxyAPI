package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestDashboardTerminalDeliverySurvivesExecutionCleanupRace(t *testing.T) {
	for _, tc := range []struct {
		name         string
		deliveryErr  error
		acknowledged bool
		writeFailed  bool
		want         string
		reason       string
	}{
		{name: "validated_terminal_success", acknowledged: true, want: "success"},
		{name: "client_disconnected_before_terminal", deliveryErr: context.Canceled, acknowledged: true, want: "interrupted", reason: "cancelled"},
		{name: "terminal_flush_failed", deliveryErr: io.ErrClosedPipe, acknowledged: true, writeFailed: true, want: "interrupted", reason: "stream_interrupted"},
		{name: "usage_success_without_delivery_ack", want: "interrupted", reason: "cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for range 100 {
				tracker := dashboard.NewTracker(nil)
				requestCtx, requestCancel := context.WithCancel(context.Background())
				requestCtx = dashboard.WithTracker(requestCtx, tracker)
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(requestCtx)
				deliveryCtx, finishDelivery := coreusage.WithStreamDelivery(context.Background())
				handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
				ctx, cancelExecution := handler.GetContextWithCancel(nil, c, deliveryCtx)
				dashboard.ObserveModel(ctx, "gpt-6.1-sol", true)
				started := time.Now().UTC()
				dashboard.ObserveAttempt(ctx, dashboard.Attempt{ID: uuid.NewString(), Provider: "codex-api", Model: "gpt-6.1-sol", StartedAt: started, CompletedAt: started, StatusCode: 200, Outcome: "success"}, "", "gpt-6.1-sol", true)
				chunks := make(chan coreexecutor.StreamChunk, 1)
				chunks <- coreexecutor.StreamChunk{Payload: []byte("event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n")}
				close(chunks)
				terminal, ok, canceled := nextStreamChunk(ctx, nil, nil, chunks)
				if !ok || canceled {
					t.Fatal("terminal chunk not delivered")
				}
				if _, errWrite := c.Writer.Write(terminal.Payload); errWrite != nil {
					t.Fatal(errWrite)
				}
				c.Writer.Flush()
				if tc.acknowledged {
					finishDelivery(tc.deliveryErr)
				}
				cancelExecution(tc.deliveryErr)
				requestCancel()
				// EOF and ctx cancellation are simultaneously ready. A producer can
				// report cancellation before or after middleware finalizes the row.
				_, _, _ = nextStreamChunk(ctx, nil, nil, chunks)
				handler.newRequestLifecycleTracker(ctx, "openai-response", "gpt-6.1-sol", "gpt-6.1-sol", true, nil, "").complete(pluginapi.RequestCompletionCanceled, 0, context.Canceled)
				row := tracker.Complete(dashboard.Request{ID: uuid.NewString(), StartedAt: started, CompletedAt: time.Now().UTC(), StatusCode: 200}, requestCtx.Err() != nil, tc.writeFailed)
				if row.Outcome != tc.want || row.Reason != tc.reason || row.StatusCode != 200 {
					t.Fatalf("outcome=%s/%s/%d, want %s/%s/200", row.Outcome, row.Reason, row.StatusCode, tc.want, tc.reason)
				}
				finishDelivery(context.Canceled)
			}
		})
	}
}

func TestDashboardSharedForwarderAcknowledgesActualDelivery(t *testing.T) {
	for _, tc := range []struct {
		name      string
		chunkDone bool
		flushErr  error
		want      string
	}{
		{name: "validated_channel_close", want: "success"},
		{name: "validated_terminal_chunk", chunkDone: true, want: "success"},
		{name: "failed_flush", flushErr: io.ErrClosedPipe, want: "interrupted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := dashboard.NewTracker(nil)
			requestCtx, requestCancel := context.WithCancel(context.Background())
			requestCtx = dashboard.WithTracker(requestCtx, tracker)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(requestCtx)
			dashboard.ObserveModel(requestCtx, "model", true)
			data := make(chan []byte, 1)
			data <- []byte("data: {\"finish_reason\":\"stop\"}\n\n")
			close(data)
			errs := make(chan *interfaces.ErrorMessage)
			close(errs)
			handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
			handler.ForwardStream(c, c.Writer, func(error) { requestCancel() }, data, errs, StreamForwardOptions{WriteChunk: func(chunk []byte) { _, _ = c.Writer.Write(chunk) }, Flush: func() error { return tc.flushErr }, ChunkDone: func() bool { return tc.chunkDone }, WriteDone: func() { _, _ = c.Writer.Write([]byte("data: [DONE]\n\n")) }})
			dashboard.ObserveCompletion(requestCtx, "model", true, true, true, 0)
			row := tracker.Complete(dashboard.Request{StatusCode: 200}, requestCtx.Err() != nil, tc.flushErr != nil)
			if row.Outcome != tc.want {
				t.Fatalf("outcome %s, want %s", row.Outcome, tc.want)
			}
		})
	}
}
