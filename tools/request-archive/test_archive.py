import importlib.util
from compression import zstd
from pathlib import Path
import sqlite3
import json
from datetime import datetime, timezone
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('archive', Path(__file__).with_name('archive.py'))
archive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(archive)


class ArchiveTests(unittest.TestCase):
    def test_index_validates_all_bytes_and_is_rebuildable(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            path = directory / 'v1-responses-2026-11-05T150000-12345678.log.zst'
            body = (b'=== REQUEST INFO ===\nURL: /v1/responses\nMethod: POST\n'
                    b'Timestamp: 2026-11-05T15:00:00Z\n=== HEADERS ===\n\n'
                    + b'complete payload\x00\xff\n' * 100000)
            with zstd.open(path, 'wb') as output:
                output.write(body)
            with archive.connect(directory) as db:
                self.assertEqual(archive.index(db, directory), 1)
                self.assertEqual(archive.index(db, directory), 0)
                row = db.execute('SELECT request_id, original_bytes, sha256, method, url FROM records').fetchone()
                self.assertEqual(row, ('12345678', len(body), archive.hashlib.sha256(body).hexdigest(), 'POST', '/v1/responses'))
                db.execute('DELETE FROM records')
                db.commit()
                self.assertEqual(archive.index(db, directory), 1)
            with zstd.open(path, 'rb') as source:
                self.assertEqual(source.read(), body)

    def test_stats_include_zero_days_and_separate_heartbeat_from_last_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            now = datetime(2026, 11, 5, 12, tzinfo=timezone.utc)
            with archive.connect(directory) as db:
                stats = archive.publish_stats(db, directory, now=now)
                self.assertEqual(stats['records'], 0)
                self.assertEqual(stats['state'], 'healthy')
                self.assertEqual(stats['latest_at'], None)
                self.assertEqual(len(stats['daily']), 7)
                self.assertEqual(stats['daily'][-1], {'date': '2026-11-05', 'records': 0})
                self.assertEqual(stats['updated_at'], now.isoformat())
                saved = json.loads((directory / 'request-archive-stats.json').read_text())
                self.assertEqual(saved, stats)
                self.assertEqual(list(directory.glob('.request-archive-stats-*')), [])
                failed = archive.publish_stats(db, directory, error=ValueError('private body'), now=now)
                self.assertEqual(failed['state'], 'error')
                self.assertEqual(failed['error_kind'], 'ValueError')
                self.assertNotIn('private body', json.dumps(failed))

    def test_corrupt_record_is_not_indexed(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / 'bad.log.zst').write_bytes(b'not a zstd frame')
            with zstd.open(directory / 'good-12345678.log.zst', 'wb') as output:
                output.write(b'=== REQUEST INFO ===\nMethod: POST\n')
            with archive.connect(directory) as db:
                with self.assertRaises(Exception):
                    archive.index(db, directory)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM records').fetchone()[0], 1)


if __name__ == '__main__':
    unittest.main()
