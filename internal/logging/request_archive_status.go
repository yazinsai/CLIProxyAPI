package logging

import (
	"sync/atomic"
	"time"
)

// ArchiveUpstreamBase is set by tools/build-archive.sh at build time. An upstream
// build without that helper deliberately reports unknown rather than a false base.
var ArchiveUpstreamBase = "unknown"

var archiveCaptureFailures atomic.Uint64
var archiveLastFailure atomic.Int64
var archiveProcessStartedAt = time.Now().UTC()

// RecordRequestArchiveFailure counts failed exchange finalizations, not sampled data.
func RecordRequestArchiveFailure() {
	if !RequestArchiveEnabled() {
		return
	}
	archiveCaptureFailures.Add(1)
	archiveLastFailure.Store(time.Now().Unix())
}

// RequestArchiveRuntime reports process-scoped recorder health without payload data.
func RequestArchiveRuntime() map[string]interface{} {
	var lastFailure interface{}
	if stamp := archiveLastFailure.Load(); stamp > 0 {
		lastFailure = time.Unix(stamp, 0).UTC().Format(time.RFC3339)
	}
	return map[string]interface{}{
		"failures_since_start": archiveCaptureFailures.Load(),
		"last_failure_at":      lastFailure,
		"started_at":           archiveProcessStartedAt.Format(time.RFC3339),
	}
}
