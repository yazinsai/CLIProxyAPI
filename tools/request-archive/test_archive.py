import importlib.util
from compression import zstd
from pathlib import Path
import sqlite3
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

    def test_corrupt_record_is_not_indexed(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / 'bad.log.zst').write_bytes(b'not a zstd frame')
            with archive.connect(directory) as db:
                with self.assertRaises(Exception):
                    archive.index(db, directory)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM records').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
