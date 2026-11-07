package handlers

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

type deliveryWriteFailure struct {
	gin.ResponseWriter
	short bool
}

func (w *deliveryWriteFailure) Write(body []byte) (int, error) {
	if w.short {
		return len(body) - 1, nil
	}
	return 0, io.ErrClosedPipe
}

func (w *deliveryWriteFailure) WriteString(body string) (int, error) {
	return w.Write([]byte(body))
}

func TestForwardStreamPreservesIgnoredWriteErrors(t *testing.T) {
	for _, short := range []bool{false, true} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
		c.Writer = &deliveryWriteFailure{ResponseWriter: c.Writer, short: short}
		data := make(chan []byte, 1)
		data <- []byte("terminal")
		close(data)
		var got error
		h := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
		h.ForwardStream(c, c.Writer, func(err error) { got = err }, data, nil, StreamForwardOptions{
			WriteChunk: func(chunk []byte) { _, _ = io.WriteString(c.Writer, string(chunk)) },
			ChunkDone:  func() bool { return true },
		})
		want := io.ErrClosedPipe
		if short {
			want = io.ErrShortWrite
		}
		if !errors.Is(got, want) {
			t.Fatalf("write failure lost: %v, want %v", got, want)
		}
	}
}

func TestForwardStreamDrainsAfterTerminalWithoutWritingAgain(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	tracker := dashboard.NewTracker(nil)
	ctx := dashboard.WithTracker(context.Background(), tracker)
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil).WithContext(ctx)
	dashboard.ObserveModel(ctx, "model", true)
	data := make(chan []byte, 2)
	data <- []byte("terminal")
	data <- []byte("cleanup")
	close(data)
	h := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	h.ForwardStream(c, c.Writer, func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}, data, nil, StreamForwardOptions{
		WriteChunk:        func(chunk []byte) { _, _ = c.Writer.Write(chunk) },
		ChunkDone:         func() bool { return true },
		ContinueAfterDone: func() bool { return true },
		WriteDone:         func() { _, _ = c.Writer.Write([]byte("extra terminal")) },
	})
	if recorder.Body.String() != "terminal" || len(data) != 0 {
		t.Fatalf("drain wrote extra output or left data: %q, buffered=%d", recorder.Body.String(), len(data))
	}
	row := tracker.Complete(dashboard.Request{StatusCode: 200}, true, false)
	if row.Outcome != "success" {
		t.Fatalf("completed delivery lost: %s/%s", row.Outcome, row.Reason)
	}
}

func TestForwardStreamStatusOnlyErrorCannotBecomeSuccess(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	tracker := dashboard.NewTracker(nil)
	ctx := dashboard.WithTracker(context.Background(), tracker)
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil).WithContext(ctx)
	dashboard.ObserveModel(ctx, "model", true)
	errs := make(chan *interfaces.ErrorMessage, 1)
	errs <- &interfaces.ErrorMessage{StatusCode: 502}
	h := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	var got error
	h.ForwardStream(c, c.Writer, func(err error) { got = err }, nil, errs, StreamForwardOptions{})
	row := tracker.Complete(dashboard.Request{StatusCode: 200}, false, false)
	if got == nil || row.Outcome != "interrupted" {
		t.Fatalf("status-only terminal error became success: %v, %s", got, row.Outcome)
	}
}
