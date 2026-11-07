package handlers

import (
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/httpwire"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"golang.org/x/net/context"
)

// ExecuteStreamWithPreFTKeepAlive executes a streaming request via ExecuteStreamWithAuthManager.
// When keepAliveC is non-nil (streaming.send-keepalive-before-ft is enabled and keepalive-seconds > 0),
// it runs the synchronous bootstrap in a goroutine and invokes writeKeepAlive on each tick while waiting.
// It returns (dataChan, upstreamHeaders, errChan, ok), where ok is false if the request context was canceled
// or writeKeepAlive returned false.
func (h *BaseAPIHandler) ExecuteStreamWithPreFTKeepAlive(
	c *gin.Context,
	cliCtx context.Context,
	cliCancel func(error),
	handlerType, modelName string,
	rawJSON []byte,
	alt string,
	keepAliveC <-chan time.Time,
	writeKeepAlive func() bool,
) (<-chan []byte, http.Header, <-chan *interfaces.ErrorMessage, bool) {
	if keepAliveC == nil {
		dataChan, upstreamHeaders, errChan := h.ExecuteStreamWithAuthManager(cliCtx, handlerType, modelName, rawJSON, alt)
		return dataChan, upstreamHeaders, errChan, true
	}

	type streamBootstrapResult struct {
		data    <-chan []byte
		headers http.Header
		errs    <-chan *interfaces.ErrorMessage
	}
	bootstrapResult := make(chan streamBootstrapResult, 1)
	go func() {
		data, headers, errs := h.ExecuteStreamWithAuthManager(cliCtx, handlerType, modelName, rawJSON, alt)
		bootstrapResult <- streamBootstrapResult{data: data, headers: headers, errs: errs}
	}()

	for {
		select {
		case <-c.Request.Context().Done():
			if cliCancel != nil {
				cliCancel(c.Request.Context().Err())
			}
			<-bootstrapResult
			return nil, nil, nil, false
		case result := <-bootstrapResult:
			return result.data, result.headers, result.errs, true
		case <-keepAliveC:
			if writeKeepAlive != nil && !writeKeepAlive() {
				if cliCancel != nil {
					cliCancel(context.Canceled)
				}
				<-bootstrapResult
				return nil, nil, nil, false
			}
		}
	}
}

// PendingStreamError returns an immediately available non-nil stream error.
func PendingStreamError(errs <-chan *interfaces.ErrorMessage) (*interfaces.ErrorMessage, bool) {
	if errs == nil {
		return nil, false
	}
	select {
	case errMsg, ok := <-errs:
		if ok && errMsg != nil {
			return errMsg, true
		}
	default:
	}
	return nil, false
}

func streamErrorCause(msg *interfaces.ErrorMessage) error {
	if msg == nil {
		return nil
	}
	if msg.Error != nil {
		return msg.Error
	}
	status := msg.StatusCode
	if status < 400 || http.StatusText(status) == "" {
		status = http.StatusBadGateway
	}
	return errors.New(http.StatusText(status))
}

type StreamForwardOptions struct {
	// KeepAliveInterval overrides the configured streaming keep-alive interval.
	// If nil, the configured default is used. If set to <= 0, keep-alives are disabled.
	KeepAliveInterval *time.Duration

	// WriteChunk writes a single data chunk to the response body. It should not flush.
	WriteChunk func(chunk []byte)

	// ChunkError optionally reports that WriteChunk emitted a terminal failure.
	// The failure is passed to cancel without writing another terminal payload.
	ChunkError func() *interfaces.ErrorMessage

	// ChunkDone reports successful terminal delivery after WriteChunk and Flush.
	ChunkDone func() bool

	// InitiallyDone acknowledges a terminal event already written and flushed by the caller.
	InitiallyDone bool

	// ContinueAfterDone drains producers that cannot yet stop on terminal delivery.
	// Delivery is still acknowledged immediately, independently of execution cleanup.
	ContinueAfterDone func() bool

	// Flush optionally exposes transport flush errors hidden by http.Flusher.
	Flush func() error

	// NormalizeTerminalError optionally replaces an upstream error before it is
	// written or passed to cancel.
	NormalizeTerminalError func(errMsg *interfaces.ErrorMessage) *interfaces.ErrorMessage

	// WriteTerminalError writes an error payload to the response body when streaming fails
	// after headers have already been committed. It should not flush.
	WriteTerminalError func(errMsg *interfaces.ErrorMessage)

	// CloseError optionally validates a clean upstream channel close before WriteDone.
	// Returning an error surfaces it through WriteTerminalError instead of completing the stream.
	CloseError func() *interfaces.ErrorMessage

	// WriteDone optionally writes a terminal marker when the upstream data channel closes
	// without an error (e.g. OpenAI's `[DONE]`). It should not flush.
	WriteDone func()

	// WriteKeepAlive optionally writes a keep-alive heartbeat. It should not flush.
	// When nil, a standard SSE comment heartbeat is used.
	WriteKeepAlive func()
}

type streamResponseWriter struct {
	gin.ResponseWriter
	err error
}

func (w *streamResponseWriter) Write(body []byte) (int, error) {
	n, errWrite := w.ResponseWriter.Write(body)
	w.observeWrite(n, len(body), errWrite)
	return n, errWrite
}

func (w *streamResponseWriter) WriteString(body string) (int, error) {
	n, errWrite := w.ResponseWriter.WriteString(body)
	w.observeWrite(n, len(body), errWrite)
	return n, errWrite
}

func (w *streamResponseWriter) observeWrite(n, size int, errWrite error) {
	if w.err != nil {
		return
	}
	if errWrite != nil {
		w.err = errWrite
	} else if n != size {
		w.err = io.ErrShortWrite
	}
}

func (w *streamResponseWriter) FlushError() error {
	if w.err != nil {
		return w.err
	}
	return httpwire.FlushResponse(w.ResponseWriter)
}

func (h *BaseAPIHandler) ForwardStream(c *gin.Context, flusher http.Flusher, cancel func(error), data <-chan []byte, errs <-chan *interfaces.ErrorMessage, opts StreamForwardOptions) {
	if c == nil {
		return
	}
	if cancel == nil {
		return
	}
	writer := &streamResponseWriter{ResponseWriter: c.Writer}
	c.Writer = writer
	defer func() { c.Writer = writer.ResponseWriter }()
	delivered := false
	consumerCancel := cancel
	cancel = func(err error) {
		if delivered && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			err = nil
		}
		if err != nil && c.Request != nil {
			dashboard.ObserveDelivery(c.Request.Context(), false, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
		}
		consumerCancel(err)
	}
	acknowledgeSuccess := func() {
		delivered = true
		if c.Request != nil {
			dashboard.ObserveDelivery(c.Request.Context(), true, false)
		}
	}

	flush := func() bool {
		if writer.err != nil {
			cancel(writer.err)
			return false
		}
		if opts.Flush != nil {
			if errFlush := opts.Flush(); errFlush != nil {
				cancel(errFlush)
				return false
			}
		} else {
			if errFlush := httpwire.FlushResponse(c.Writer); errFlush != nil {
				cancel(errFlush)
				return false
			}
		}
		return true
	}

	writeChunk := opts.WriteChunk
	if writeChunk == nil {
		writeChunk = func([]byte) {}
	}

	writeKeepAlive := opts.WriteKeepAlive
	if writeKeepAlive == nil {
		writeKeepAlive = func() {
			_, _ = c.Writer.Write([]byte(": keep-alive\n\n"))
		}
	}

	keepAliveInterval := StreamingKeepAliveInterval(h.CurrentConfig())
	if opts.KeepAliveInterval != nil {
		keepAliveInterval = *opts.KeepAliveInterval
	}
	keepAliveC, stopKeepAlive := NewStreamingKeepAliveTicker(keepAliveInterval)
	defer stopKeepAlive()
	continueAfterDone := func() bool {
		return opts.ContinueAfterDone != nil && opts.ContinueAfterDone()
	}
	if opts.ChunkError != nil {
		if initialErr := opts.ChunkError(); initialErr != nil {
			if opts.NormalizeTerminalError != nil {
				initialErr = opts.NormalizeTerminalError(initialErr)
			}
			cancel(streamErrorCause(initialErr))
			return
		}
	}
	if opts.InitiallyDone {
		acknowledgeSuccess()
		if !continueAfterDone() {
			cancel(nil)
			return
		}
	}

	var terminalErr *interfaces.ErrorMessage
	for {
		select {
		case <-c.Request.Context().Done():
			cancel(c.Request.Context().Err())
			return
		case chunk, ok := <-data:
			if !ok {
				// Prefer surfacing a terminal error if one is pending.
				if terminalErr == nil {
					if errMsg, ok := PendingStreamError(errs); ok {
						terminalErr = errMsg
						if opts.NormalizeTerminalError != nil {
							terminalErr = opts.NormalizeTerminalError(terminalErr)
						}
					}
				}
				if terminalErr == nil && opts.CloseError != nil {
					terminalErr = opts.CloseError()
				}
				if terminalErr != nil {
					if delivered {
						cancel(streamErrorCause(terminalErr))
						return
					}
					if opts.WriteTerminalError != nil {
						opts.WriteTerminalError(terminalErr)
					}
					if !flush() {
						return
					}
					cancel(streamErrorCause(terminalErr))
					return
				}
				if delivered {
					cancel(nil)
					return
				}
				if opts.WriteDone != nil {
					opts.WriteDone()
				}
				if !flush() {
					return
				}
				acknowledgeSuccess()
				cancel(nil)
				return
			}
			if delivered {
				continue
			}
			writeChunk(chunk)
			if !flush() {
				return
			}
			if opts.ChunkError != nil {
				chunkErr := opts.ChunkError()
				if chunkErr != nil {
					if opts.NormalizeTerminalError != nil {
						chunkErr = opts.NormalizeTerminalError(chunkErr)
					}
					if chunkErr != nil {
						cancel(streamErrorCause(chunkErr))
					} else {
						cancel(nil)
					}
					return
				}
			}
			if opts.ChunkDone != nil && opts.ChunkDone() {
				acknowledgeSuccess()
				if !continueAfterDone() {
					cancel(nil)
					return
				}
			}
		case errMsg, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if errMsg != nil {
				terminalErr = errMsg
				if opts.NormalizeTerminalError != nil {
					terminalErr = opts.NormalizeTerminalError(terminalErr)
				}
				if opts.WriteTerminalError != nil && !delivered {
					opts.WriteTerminalError(terminalErr)
					if !flush() {
						return
					}
				}
			}
			var execErr error
			if terminalErr != nil {
				execErr = streamErrorCause(terminalErr)
			}
			cancel(execErr)
			return
		case <-keepAliveC:
			if delivered {
				continue
			}
			writeKeepAlive()
			if !flush() {
				return
			}
		}
	}
}
