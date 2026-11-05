#!/usr/bin/env python3
"""Rebuildable SQLite index for immutable CLI Proxy API Zstandard request archives.

Python 3.14+. Payload files are authoritative; deleting this index loses no records.
"""
import argparse
from compression import zstd
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
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
        with zstd.open(path, 'rb') as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
                if len(header) < 16384:
                    header += chunk[:16384 - len(header)]
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
    return added


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
                except Exception as error:
                    print(f'Archive indexing failed: {type(error).__name__}; will retry', file=sys.stderr, flush=True)
                time.sleep(10)
        elif args.command == 'index':
            print(json.dumps({'indexed': index(db, directory)}))
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
