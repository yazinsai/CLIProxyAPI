#!/usr/bin/env python3
"""Rebuildable SQLite index for immutable CLI Proxy API Zstandard request archives.

Python 3.14+. Payload files are authoritative; deleting this index loses no records.
"""
import argparse
from datetime import datetime, timedelta, timezone
from compression import zstd
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import shutil
import tempfile
import sys
import time


def connect(directory):
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = sqlite3.connect(directory / 'request-index.sqlite3')
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.executescript('''
        CREATE TABLE IF NOT EXISTS records (
            name TEXT PRIMARY KEY,
            request_id TEXT NOT NULL,
            modified_ns INTEGER NOT NULL,
            compressed_bytes INTEGER NOT NULL,
            original_bytes INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            method TEXT,
            url TEXT,
            timestamp TEXT
        );
        CREATE INDEX IF NOT EXISTS by_request ON records(request_id);
        CREATE INDEX IF NOT EXISTS by_time ON records(modified_ns);
    ''')
    return db


def index(db, directory):
    added = 0
    failures = []
    for path in directory.glob('*.log.zst'):
        stat = path.stat()
        old = db.execute('SELECT modified_ns, compressed_bytes FROM records WHERE name=?',
                         (path.name,)).fetchone()
        if old == (stat.st_mtime_ns, stat.st_size):
            continue
        digest = hashlib.sha256()
        size = 0
        header = b''
        # Read the complete record to validate the frame checksum and hash every byte.
        try:
            with zstd.open(path, 'rb') as source:
                while chunk := source.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
                    if len(header) < 16384:
                        header += chunk[:16384 - len(header)]
        except Exception as error:
            # A damaged file must not prevent newer intact records being indexed.
            failures.append(error)
            continue
        fields = {}
        # Only parse request metadata before body data, never treat body text as metadata.
        info = header.split(b'=== HEADERS ===', 1)[0]
        for key in ('Method', 'URL', 'Timestamp'):
            match = re.search(rb'^' + key.encode() + rb': (.*)$', info, re.MULTILINE)
            fields[key] = match[1].decode('utf-8', 'replace') if match else None
        name = path.name.removesuffix('.zst').removesuffix('.log')
        request_id = name.rsplit('-', 1)[-1]
        with db:
            db.execute('INSERT OR REPLACE INTO records VALUES (?,?,?,?,?,?,?,?,?)',
                       (path.name, request_id, stat.st_mtime_ns, stat.st_size, size,
                        digest.hexdigest(), fields['Method'], fields['URL'], fields['Timestamp']))
        added += 1
    if failures:
        raise failures[0]
    return added


def publish_stats(db, directory, error=None, now=None):
    """Publish bounded metadata atomically; dashboard requests never scan payloads."""
    now = now or datetime.now(timezone.utc)
    totals = db.execute("""SELECT count(*), coalesce(sum(compressed_bytes),0),
        coalesce(sum(original_bytes),0), min(modified_ns), max(modified_ns) FROM records""").fetchone()
    first_day = (now - timedelta(days=6)).date()
    # modified_ns is the completed record's preserved write time, grouped in UTC.
    rows = db.execute("""SELECT date(modified_ns/1000000000, 'unixepoch'), count(*)
        FROM records WHERE modified_ns >= ? GROUP BY 1""",
        (int(datetime.combine(first_day, datetime.min.time(), timezone.utc).timestamp() * 1e9),)).fetchall()
    daily = dict(rows)
    files = list(directory.glob('*.log.zst'))
    disk = shutil.disk_usage(directory)
    index_bytes = sum(p.stat().st_size for p in directory.glob('request-index.sqlite3*') if p.is_file())
    latest = lambda value: datetime.fromtimestamp(value / 1e9, timezone.utc).isoformat() if value else None
    stats = {
        'schema_version': 1,
        'updated_at': now.isoformat(),
        'state': 'error' if error else 'healthy',
        # Error type only: filenames or exception text can contain private data.
        'error_kind': type(error).__name__ if error else None,
        'records': totals[0],
        'archive_files': len(files),
        'archived_bytes': totals[1],
        'original_bytes': totals[2],
        'index_bytes': index_bytes,
        'oldest_at': latest(totals[3]),
        'latest_at': latest(totals[4]),
        'disk_free_bytes': disk.free,
        'disk_total_bytes': disk.total,
        'pending_plaintext_files': sum(1 for p in directory.glob('*.log')
            if p.name != 'main.log' and re.search(r'-\d{4}-\d{2}-\d{2}T\d{6}.*\.log$', p.name)),
        'daily': [{'date': (first_day + timedelta(days=i)).isoformat(),
                   'records': daily.get((first_day + timedelta(days=i)).isoformat(), 0)}
                  for i in range(7)],
    }
    fd, temporary = tempfile.mkstemp(prefix='.request-archive-stats-', dir=directory)
    try:
        with os.fdopen(fd, 'w') as output:
            json.dump(stats, output, separators=(',', ':'))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, directory / 'request-archive-stats.json')
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return stats


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, type=Path)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('index')
    sub.add_parser('watch')
    search = sub.add_parser('search')
    search.add_argument('--request-id')
    search.add_argument('--url')
    search.add_argument('--limit', type=int, default=50)
    export = sub.add_parser('export')
    export.add_argument('name', help='Archive basename returned by search')
    export.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    directory = args.directory.resolve()
    if args.command == 'export':
        if Path(args.name).name != args.name or not args.name.endswith('.log.zst'):
            parser.error('Expected an archive basename ending in .log.zst')
        # Exclusive creation prevents accidentally replacing an existing file.
        with zstd.open(directory / args.name, 'rb') as source, args.output.open('xb') as target:
            while chunk := source.read(1024 * 1024):
                target.write(chunk)
            target.flush()
            os.fsync(target.fileno())
        return
    with connect(directory) as db:
        if args.command == 'watch':
            while True:
                try:
                    index(db, directory)
                    publish_stats(db, directory)
                except Exception as error:
                    print(f'Archive indexing failed: {type(error).__name__}; will retry', file=sys.stderr, flush=True)
                    try:
                        publish_stats(db, directory, error=error)
                    except Exception as report_error:
                        print(f'Archive health publication failed: {type(report_error).__name__}', file=sys.stderr, flush=True)
                time.sleep(10)
        elif args.command == 'index':
            added = index(db, directory)
            publish_stats(db, directory)
            print(json.dumps({'indexed': added}))
        else:
            index(db, directory)
            db.row_factory = sqlite3.Row
            rows = db.execute('''SELECT * FROM records
                WHERE (? IS NULL OR request_id = ?) AND (? IS NULL OR url LIKE ?)
                ORDER BY modified_ns DESC LIMIT ?''',
                (args.request_id, args.request_id, args.url,
                 '%' + args.url + '%' if args.url else None, max(1, args.limit)))
            print(json.dumps([dict(row) for row in rows], indent=2))


if __name__ == '__main__':
    main()
