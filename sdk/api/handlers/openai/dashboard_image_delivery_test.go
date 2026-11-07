package openai

import (
	"context"
	"io"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

func TestImagesDashboardTerminalDelivery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		payload  string
		flushErr error
		want     string
	}{
		{name: "completed image", payload: `{"type":"response.completed","response":{"status":"completed","output":[{"type":"image_generation_call","result":"dGVzdA=="}]}}`, want: "success"},
		{name: "failed image flush", payload: `{"type":"response.completed","response":{"status":"completed","output":[{"type":"image_generation_call","result":"dGVzdA=="}]}}`, flushErr: io.ErrClosedPipe, want: "interrupted"},
		{name: "missing completion", payload: `{"type":"response.in_progress"}`, want: "interrupted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, c, _ := newResponsesStreamTestHandler(t)
			tracker := dashboard.NewTracker(nil)
			ctx, disconnect := context.WithCancel(context.Background())
			defer disconnect()
			ctx = dashboard.WithTracker(ctx, tracker)
			c.Request = c.Request.WithContext(ctx)
			c.Writer = &terminalDeliveryWriter{ResponseWriter: c.Writer, disconnect: disconnect, flushErr: tc.flushErr}
			dashboard.ObserveModel(ctx, "image-model", true)
			data := make(chan []byte)
			close(data)
			h := NewOpenAIAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
			h.forwardImagesStream(ctx, c, c.Writer, func(error) {}, data, nil, []byte("data: "+tc.payload+"\n\n"), "b64_json", "image_generation", func(_ string, payload []byte) error {
				_, _ = c.Writer.Write(payload)
				return flushResponsesSSE(c.Writer)
			})
			row := tracker.Complete(dashboard.Request{StatusCode: 200}, ctx.Err() != nil, false)
			if row.Outcome != tc.want {
				t.Fatalf("outcome=%s/%s, want %s", row.Outcome, row.Reason, tc.want)
			}
		})
	}
}
