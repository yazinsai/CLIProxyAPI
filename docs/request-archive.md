# Full request archive

The local proxy uses the `feat/request-archive` branch based on v8.0.56.
Set `CLI_PROXY_REQUEST_ARCHIVE=1` in the proxy's environment and enable
`observability.logs.request-log`. Disable size-based plaintext log cleanup with
`observability.logs.logs-max-total-size-mb: 0` and error-file cleanup with
`observability.logs.error-logs-max-files: 0` so failed or interrupted captures
remain available for recovery. Commercial mode must remain disabled.

Each completed exchange produces a private `*.log.zst` file in the request log
directory. Zstandard compression is lossless, has frame checksums, and runs with
one encoder worker to bound compression memory. Each archive is independently
readable. Records include the downstream request/response and upstream attempts
captured by the existing backend logging paths, including websocket transcripts.
GET requests are included. Management routes remain excluded to avoid retaining
management credentials. Authentication headers and sensitive query parameters
retain the backend's existing masking; request and response bodies are not redacted.
Treat the archive as sensitive data.

Archive mode retains original incoming request bytes, including Content-Encoding,
without the normal decoded logging limit. Response logs retain full content as
represented by the backend logger (which may decode upstream content encodings).
These are application exchange records, not packet captures: TLS framing, HTTP
chunk boundaries, and exact original websocket wire frames are not preserved.
Neither bodies nor streaming chunks are sampled. Streaming recording applies disk
backpressure and syncs chunks to the spool; slower storage can increase latency.

A completed plaintext log is synced, compressed to a temporary file, synced again,
then atomically linked into place without overwriting any historical record. The
parent directory is synced before the plaintext source is removed. Archive files
are outside the ordinary `.log`/`.log.gz` rotation rules and have no automatic
expiration. Existing request-log downloads transparently decompress them.

Failures are logged as recording errors. Failed archive publication retains the
plaintext source; streaming assembly failures retain body spool files. A process
crash can leave temporary/spool files and incomplete exchanges. Do not delete
those files automatically. This is not a transactional fail-closed audit system:
a disk error or termination during processing can prevent a complete record,
and data never received from a client/upstream cannot be reconstructed. Disk
capacity must be monitored. No record exists for historical successful requests
made while request logging was disabled.

## SQLite index

`tools/request-archive/archive.py` requires Python 3.14+ (standard-library Zstandard).
It validates and hashes every byte of each completed archive, then stores metadata
in `request-index.sqlite3`, with WAL and FULL synchronization. Files are authoritative:
the index can be deleted and rebuilt without losing payloads. The index contains
request ID, timestamp, method, URL, sizes, and the uncompressed SHA-256 digest.
It does not duplicate bodies. New archives become searchable on the next scan.

```sh
python3.14 tools/request-archive/archive.py --directory /path/to/logs index
python3.14 tools/request-archive/archive.py --directory /path/to/logs watch
python3.14 tools/request-archive/archive.py --directory /path/to/logs search --url /v1/responses
python3.14 tools/request-archive/archive.py --directory /path/to/logs search --request-id 12345678
python3.14 tools/request-archive/archive.py --directory /path/to/logs export RECORD.log.zst --output record.log
```

Search is available through this CLI; this change adds no dashboard archive search
screen. Existing dashboard request-log downloads continue to work. Index scans are
linear in file count and skip already indexed immutable files. At very large
volumes, date partitioning or a dedicated archive service would be appropriate.

## Local installation

- Proxy: `~/.local/share/cliproxyapi/cli-proxy-api`
- Proxy launch agent: `~/Library/LaunchAgents/com.local.cliproxyapi.plist`
- Request archives/index: `~/.config/cliproxyapi/logs/`
- Indexer: `~/.local/share/cliproxyapi/request-archive/archive.py`
- Indexer launch agent: `~/Library/LaunchAgents/com.local.cliproxyapi-request-index.plist`
- Pre-installation backup: `~/.local/share/cliproxyapi/backups/before-request-archive-20261105143904/`

The proxy launch agent sets `WRITABLE_PATH=~/.config/cliproxyapi` (expanded to an
absolute path) so the request writer, application logger, and management downloads
resolve the same directory.

The indexer starts at login and scans every ten seconds. Its diagnostic output is
under `~/.local/share/cliproxyapi/logs/request-index-{stdout,stderr}.log`.
The patched proxy is a local build: replacing it with an upstream release removes
archive support, so preserve/reapply these backend changes during upgrades.

To roll back, stop the proxy/indexer launch agents, restore the proxy binary,
`config.yaml`, and original proxy plist from the backup, then bootstrap the proxy
launch agent again. Keep the archive directory. Restoring the old configuration
also disables future full request capture.

## Validation

Run backend logging, middleware, and management tests plus the normal Go build.
`python3.14 -m unittest discover -s tools/request-archive -p 'test_*.py'` tests the index.
An isolated live HTTP smoke test also verified full response retention, authorization
masking, and archived downloads through `/v8/management/observability/logs/requests/:id`.
