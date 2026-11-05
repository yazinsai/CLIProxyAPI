import importlib.util
from contextlib import closing
from compression import zstd
from pathlib import Path
import json
from datetime import datetime, timezone
import tempfile
import unittest
from unittest.mock import patch
import subprocess
import sys

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
            with closing(archive.connect(directory)) as db:
                self.assertEqual(archive.index(db, directory), 1)
                self.assertEqual(archive.index(db, directory), 0)
                row = db.execute('SELECT request_id, original_bytes, sha256, method, url FROM records').fetchone()
                self.assertEqual(row, ('12345678', len(body), archive.hashlib.sha256(body).hexdigest(), 'POST', '/v1/responses'))
                db.execute('DELETE FROM records')
                db.commit()
                self.assertEqual(archive.index(db, directory), 1)
            with zstd.open(path, 'rb') as source:
                self.assertEqual(source.read(), body)

    def make_records(self, directory, count, start=0):
        bodies = {}
        for i in range(start, start + count):
            name = f'v1-responses-2026-11-05T150000-{i:08d}.log.zst'
            body = (b'=== REQUEST INFO ===\nURL: /v1/responses\nMethod: POST\n'
                    b'Timestamp: 2026-11-05T15:00:00Z\n=== HEADERS ===\n' + bytes(range(256)) * 10)
            with zstd.open(directory / name, 'wb') as output:
                output.write(body)
            bodies[name] = body
        return bodies

    def test_compaction_threshold_rebuild_export_and_stats(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            bodies = self.make_records(directory, 99)
            # Interrupted spools and plaintext exchanges must never be packed.
            (directory / 'unfinished.log').write_bytes(b'incomplete')
            (directory / '.request-archive-spool.tmp').write_bytes(b'incomplete')
            with closing(archive.connect(directory)) as db:
                self.assertEqual(archive.compact(db, directory), 0)
                self.assertEqual(len(list(directory.glob('*.log.zst'))), 99)
                bodies.update(self.make_records(directory, 102, start=99))
                stamps = {name: (directory / name).stat().st_mtime_ns for name in bodies}
                self.assertEqual(archive.compact(db, directory), 200)
                self.assertEqual(len(list(directory.glob('*.log.zst'))), 1)
                self.assertEqual(len(list(directory.glob('requests-*.zip'))), 2)
                self.assertEqual(archive.compact(db, directory), 0)
                db.execute('DELETE FROM records')
                db.commit()
                self.assertEqual(archive.index(db, directory), 201)
                for name, body in bodies.items():
                    with archive.open_record(directory, name) as source:
                        self.assertEqual(source.read(), body)
                    row = db.execute('SELECT modified_ns, sha256, url FROM records WHERE name=?', (name,)).fetchone()
                    self.assertEqual(row, (stamps[name], archive.hashlib.sha256(body).hexdigest(), '/v1/responses'))
                stats = archive.publish_stats(db, directory)
                self.assertEqual(stats['records'], 201)
                self.assertEqual(stats['archive_files'], 201)
                self.assertEqual(stats['storage_files'], 3)
                self.assertEqual(stats['pack_files'], 2)
                self.assertEqual(stats['archived_bytes'], sum(p.stat().st_size for p in directory.glob('requests-*.zip')) + sum(p.stat().st_size for p in directory.glob('*.log.zst')))
            name = next(iter(bodies))
            result = subprocess.run([sys.executable, str(Path(archive.__file__)), '--directory', tmp,
                                     'search', '--request-id', '00000000'], capture_output=True, check=True)
            self.assertEqual(json.loads(result.stdout)[0]['name'], name)
            output = directory / 'exported.txt'
            subprocess.run([sys.executable, str(Path(archive.__file__)), '--directory', tmp,
                            'export', name, '--output', str(output)], check=True)
            self.assertEqual(output.read_bytes(), bodies[name])
            self.assertEqual((directory / 'unfinished.log').read_bytes(), b'incomplete')

    def test_compaction_recovers_publication_before_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            bodies = self.make_records(directory, 100)
            original_unlink = Path.unlink
            def fail_source_unlink(path, *args, **kwargs):
                if path.name.endswith('.log.zst'):
                    raise OSError('simulated crash after durable publication')
                return original_unlink(path, *args, **kwargs)
            with closing(archive.connect(directory)) as db:
                with patch.object(Path, 'unlink', fail_source_unlink):
                    with self.assertRaises(OSError):
                        archive.compact(db, directory)
                self.assertEqual(len(list(directory.glob('requests-*.zip'))), 1)
                self.assertEqual(len(list(directory.glob('*.log.zst'))), 100)
                self.assertEqual(archive.compact(db, directory), 0)
                self.assertEqual(len(list(directory.glob('*.log.zst'))), 0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM records').fetchone()[0], 100)
                for name, body in bodies.items():
                    with archive.open_record(directory, name) as source:
                        self.assertEqual(source.read(), body)

    def test_old_index_schema_migrates_without_losing_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.make_records(directory, 1)
            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                db.execute('ALTER TABLE records DROP COLUMN container')
                db.commit()
            with closing(archive.connect(directory)) as db:
                self.assertEqual(archive.index(db, directory), 0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM records').fetchone()[0], 1)

    def test_recovery_retains_conflicting_standalone_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            bodies = self.make_records(directory, 100)
            with closing(archive.connect(directory)) as db:
                archive.compact(db, directory)
                path = directory / next(iter(bodies))
                path.write_bytes(b'conflicting bytes')
                with self.assertRaises(ValueError):
                    archive.compact(db, directory)
                self.assertEqual(path.read_bytes(), b'conflicting bytes')
                with archive.open_record(directory, path.name) as source:
                    self.assertEqual(source.read(), bodies[path.name])

    def test_failed_publication_or_corrupt_source_never_removes_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.make_records(directory, 100)
            with closing(archive.connect(directory)) as db:
                with patch.object(archive.os, 'link', side_effect=OSError('disk full')):
                    with self.assertRaises(OSError):
                        archive.compact(db, directory)
                self.assertEqual(len(list(directory.glob('*.log.zst'))), 100)
                self.assertEqual(list(directory.glob('requests-*.zip')), [])
                next(directory.glob('*.log.zst')).write_bytes(b'corrupt')
                with self.assertRaises(Exception):
                    archive.compact(db, directory)
                self.assertEqual(len(list(directory.glob('*.log.zst'))), 100)
                self.assertEqual(list(directory.glob('requests-*.zip')), [])
                with self.assertRaises(ValueError):
                    archive.compact(db, directory, batch_size=99)

    def test_stats_include_zero_days_and_separate_heartbeat_from_last_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            now = datetime(2026, 11, 5, 12, tzinfo=timezone.utc)
            with closing(archive.connect(directory)) as db:
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
            with closing(archive.connect(directory)) as db:
                with self.assertRaises(Exception):
                    archive.index(db, directory)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM records').fetchone()[0], 1)


if __name__ == '__main__':
    unittest.main()
