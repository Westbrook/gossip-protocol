"""Bounded host smoke of repository-authored M1 intake, never candidate code.

Actual SQLite Store and JobManager run in an isolated child. These tests cover
public parser/transaction integration requirements, not a held-out quality score.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m1_ingestion_reference_v1 import ingestion_files
from gossip_harness.library_m1_reference_v1 import catalog_files
from gossip_harness.library_project_fixture_v1 import seed_files


def _smoke(script: str) -> dict:
    with tempfile.TemporaryDirectory(prefix="authored-m1-intake-") as directory:
        root = Path(directory).resolve()
        for name, source in (seed_files() | catalog_files() | ingestion_files()).items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding="utf-8")
        bootstrap = """
import json, os, resource, stat, sys, zipfile
from pathlib import Path
resource.setrlimit(resource.RLIMIT_CPU, (5, 5))
resource.setrlimit(resource.RLIMIT_FSIZE, (4194304, 4194304))
resource.setrlimit(resource.RLIMIT_NOFILE, (96, 96))
sys.path.insert(0, sys.argv[1])
from library.catalog.store import Store
from library.ingestion.jobs import JobManager
from library.common import LibraryError
root = Path(sys.argv[1])
store = Store(root / 'catalog.db')
store.insert('keep.txt', b'keep')
manager = JobManager(store)
def error(action):
    try:
        action()
    except LibraryError as failure:
        return failure.code
    raise AssertionError('expected domain failure')
"""
        result = subprocess.run(
            [sys.executable, "-I", "-c", bootstrap + script, str(root)], cwd=root,
            text=True, capture_output=True, timeout=12, check=False,
        )
        if result.returncode:
            raise AssertionError(result.stderr[:5000])
        if len(result.stdout) > 65536:
            raise AssertionError("Authored smoke output exceeded bound")
        return json.loads(result.stdout)


class LibraryM1IngestionReferenceV1Tests(unittest.TestCase):
    def test_mapping_scope_and_compilation(self):
        files = ingestion_files()
        self.assertEqual(set(files), {"library/ingestion/jobs.py", "library/ingestion/local.py"})
        for name, source in files.items():
            compile(source, name, "exec")
        files.clear()
        self.assertTrue(ingestion_files())

    def test_directory_sorts_preserves_text_and_reopens_durable_job(self):
        got = _smoke("""
folder = root / 'incoming'
(folder / 'sub').mkdir(parents=True)
(folder / 'z.html').write_bytes(b'<h1>literal</h1>\\r\\n')
(folder / 'sub' / 'a.md').write_bytes('Café'.encode())
job = manager.submit_directory('directory', folder, 'notes')
token = manager.prepare('directory')
receipt = manager.commit(token)
store.close()
store = Store(root / 'catalog.db')
print(json.dumps({'job': store.get_job('directory'), 'sources': [d['source'] for d in receipt['documents']],
                  'texts': [d['text'] for d in receipt['documents']], 'manifest': store.job_manifest('directory')}))
""")
        self.assertEqual(got["sources"], ["notes/sub/a.md", "notes/z.html"])
        self.assertEqual(got["texts"], ["Café", "<h1>literal</h1>\r\n"])
        self.assertEqual(got["job"]["state"], "completed")
        self.assertEqual(got["job"]["completed"], 2)

    def test_directory_leaf_parent_and_root_symlinks_admit_nothing(self):
        got = _smoke("""
folder = root / 'incoming'; folder.mkdir()
(folder / 'a.txt').write_text('A')
(folder / 'link.txt').symlink_to(folder / 'a.txt')
errors = [error(lambda: manager.submit_directory('leaf', folder, 'n'))]
(folder / 'link.txt').unlink()
(root / 'linked').symlink_to(folder, target_is_directory=True)
errors.append(error(lambda: manager.submit_directory('parent', root / 'linked', 'n')))
(root / 'bundle.json').write_text('{"entries":[]}')
(folder / 'bundle.json').write_text('{"entries":[]}')
errors.append(error(lambda: manager.submit_json('json', root / 'linked' / 'bundle.json')))
print(json.dumps({'errors': errors, 'jobs': store.list_jobs(), 'count': len(store.documents())}))
""")
        self.assertEqual(got, {"errors": ["invalid_source"] * 3, "jobs": [], "count": 1})

    def test_directory_resource_and_unsupported_members_are_atomic(self):
        got = _smoke("""
folder = root / 'incoming'; folder.mkdir()
(folder / 'good.txt').write_text('A')
(folder / 'bad.pdf').write_text('unsupported')
codes = [error(lambda: manager.submit_directory('unsupported', folder, 'n'))]
(folder / 'bad.pdf').unlink()
(folder / 'bad.txt').write_bytes(b'\\xff')
codes.append(error(lambda: manager.submit_directory('utf8', folder, 'n')))
(folder / 'bad.txt').write_bytes(b'x' * 32769)
codes.append(error(lambda: manager.submit_directory('large', folder, 'n')))
(folder / 'bad.txt').unlink()
for n in range(64): (folder / ('file-%d.txt' % n)).write_text('x')
codes.append(error(lambda: manager.submit_directory('count', folder, 'n')))
print(json.dumps({'codes': codes, 'jobs': store.list_jobs(), 'sources': [d['source'] for d in store.documents()]}))
""")
        self.assertEqual(got["codes"], ["unsupported_type", "invalid_utf8", "too_large", "invalid_batch"])
        self.assertEqual(got["jobs"], [])
        self.assertEqual(got["sources"], ["keep.txt"])

    def test_zip_shared_blobs_and_ignored_valid_directory(self):
        got = _smoke("""
path = root / 'ok.zip'
with zipfile.ZipFile(path, 'w') as archive:
    archive.writestr('folder/', b'')
    archive.writestr('folder/b.html', 'same')
    archive.writestr('a.md', 'same')
manager.submit_zip('zip', path, 'papers')
receipt = manager.commit(manager.prepare('zip'))
print(json.dumps({'documents': receipt['documents'], 'extracted': (root / 'papers').exists()}))
""")
        docs = got["documents"]
        self.assertEqual([doc["source"] for doc in docs], ["papers/a.md", "papers/folder/b.html"])
        self.assertEqual(docs[0]["blob_id"], docs[1]["blob_id"])
        self.assertNotEqual(docs[0]["document_id"], docs[1]["document_id"])
        self.assertFalse(got["extracted"])

    def test_zip_traversal_symlink_nonregular_duplicate_and_utf8_no_admission(self):
        got = _smoke("""
cases = [('../bad.txt', 'B', None), ('../', '', None),
         ('link.txt', '../keep.txt', stat.S_IFLNK | 0o777),
         ('pipe.txt', '', stat.S_IFIFO | 0o600), ('bad.txt', b'\\xff', None),
         ('a/' * 16 + 'bad.txt', '', None)]
codes = []
for index, (name, body, mode) in enumerate(cases):
    path = root / ('bad%d.zip' % index)
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('good.txt', 'G')
        info = zipfile.ZipInfo(name)
        if mode is not None: info.create_system = 3; info.external_attr = mode << 16
        archive.writestr(info, body)
    codes.append(error(lambda: manager.submit_zip('j%d' % index, path, 'n')))
path = root / 'duplicate.zip'
with zipfile.ZipFile(path, 'w') as archive:
    archive.writestr('a.txt', 'A'); archive.writestr('a.txt', 'A')
codes.append(error(lambda: manager.submit_zip('dup', path, 'n')))
print(json.dumps({'codes': codes, 'jobs': store.list_jobs(), 'count': len(store.documents())}))
""")
        self.assertEqual(got["codes"], ["invalid_source", "invalid_source", "invalid_source", "invalid_archive",
                                        "invalid_utf8", "invalid_source", "invalid_batch"])
        self.assertEqual(got["jobs"], [])
        self.assertEqual(got["count"], 1)

    def test_zip_bounded_decompression_count_crc_and_input(self):
        got = _smoke("""
codes = []
path = root / 'ratio.zip'
with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
    archive.writestr('a.txt', 'x' * 32768)
codes.append(error(lambda: manager.submit_zip('ratio', path, 'n')))
path = root / 'count.zip'
with zipfile.ZipFile(path, 'w') as archive:
    for n in range(65): archive.writestr('%d.txt' % n, '')
codes.append(error(lambda: manager.submit_zip('count', path, 'n')))
path = root / 'crc.zip'
with zipfile.ZipFile(path, 'w') as archive: archive.writestr('a.txt', 'unique-payload')
path.write_bytes(path.read_bytes().replace(b'unique-payload', b'broken-payload'))
codes.append(error(lambda: manager.submit_zip('crc', path, 'n')))
path = root / 'large.zip'; path.write_bytes(b'x' * 1048577)
codes.append(error(lambda: manager.submit_zip('input', path, 'n')))
print(json.dumps({'codes': codes, 'jobs': store.list_jobs()}))
""")
        self.assertEqual(got["codes"], ["too_large", "invalid_batch", "invalid_archive", "too_large"])
        self.assertEqual(got["jobs"], [])

    def test_json_schema_and_resource_failures_do_not_admit(self):
        got = _smoke("""
values = ['{', '{"entries":[],"extra":true}', '{"entries":[],"entries":[]}',
          '{"entries":[{"source":"a.txt","text":1}]}', '{"entries":NaN}',
          json.dumps({'entries': [{'source': 'a.txt', 'text': 'A'}] * 2}),
          json.dumps({'entries': [{'source': 'a.txt', 'text': 'x' * 32769}]})]
codes = []
for index, value in enumerate(values):
    path = root / ('bundle%d.json' % index); path.write_text(value)
    codes.append(error(lambda: manager.submit_json('j%d' % index, path)))
print(json.dumps({'codes': codes, 'jobs': store.list_jobs()}))
""")
        self.assertEqual(got["codes"], ["invalid_json"] * 5 + ["invalid_batch", "too_large"])
        self.assertEqual(got["jobs"], [])

    def test_json_semantic_sources_deferred_and_failed_job_persists(self):
        got = _smoke("""
path = root / 'bundle.json'
path.write_text(json.dumps({'entries': [{'source': '../bad.txt', 'text': 'B'}, {'source': 'ok.txt', 'text': 'A'}]}))
queued = manager.submit_json('bad', path)
code = error(lambda: manager.prepare('bad'))
store.close(); store = Store(root / 'catalog.db')
print(json.dumps({'initial': queued, 'code': code, 'job': store.get_job('bad'), 'count': len(store.documents())}))
""")
        self.assertEqual(got["initial"]["state"], "queued")
        self.assertEqual(got["code"], "invalid_source")
        self.assertEqual(got["job"]["state"], "failed")
        self.assertEqual(got["job"]["completed"], 0)
        self.assertEqual(got["count"], 1)

    def test_empty_batch_and_missing_input_errors(self):
        got = _smoke("""
folder = root / 'empty'; folder.mkdir()
job = manager.submit_directory('empty', folder, 'n')
receipt = manager.commit(manager.prepare('empty'))
code = error(lambda: manager.submit_zip('missing', root / 'absent.zip', 'n'))
print(json.dumps({'receipt': receipt, 'code': code, 'jobs': store.list_jobs()}))
""")
        self.assertEqual(got["receipt"]["documents"], [])
        self.assertEqual(got["receipt"]["job"]["completed"], 0)
        self.assertEqual(got["receipt"]["job"]["state"], "completed")
        self.assertEqual(got["code"], "io_error")
        self.assertEqual(len(got["jobs"]), 1)

    def test_v0_importer_rejects_root_symlink_preserving_formats(self):
        got = _smoke("""
from library.ingestion.local import import_file
folder = root / 'input'; folder.mkdir()
(folder / 'a.txt').write_bytes(b'A\\r\\n')
alias = root / 'alias'; alias.symlink_to(folder, target_is_directory=True)
code = error(lambda: import_file(store, alias, 'a.txt'))
receipt = import_file(store, folder, 'a.txt')
html = error(lambda: import_file(store, folder, 'literal.html'))
print(json.dumps({'code': code, 'receipt': receipt, 'html': html}))
""")
        self.assertEqual(got["code"], "invalid_source")
        self.assertEqual(got["receipt"]["document"]["text"], "A\r\n")
        self.assertEqual(got["html"], "unsupported_type")

    def test_zip_directory_member_still_counts_resource_limits(self):
        got = _smoke("""
path = root / 'directory-payload.zip'
with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
    archive.writestr('payload/', 'x' * 600000)
code = error(lambda: manager.submit_zip('directory', path, 'n'))
print(json.dumps({'code': code, 'jobs': store.list_jobs()}))
""")
        self.assertEqual(got, {"code": "too_large", "jobs": []})

    def test_total_decoded_bytes_bound_applies_to_all_intake_forms(self):
        got = _smoke("""
entries = [{'source': '%d.txt' % n, 'text': 'x' * 32768} for n in range(17)]
folder = root / 'total'; folder.mkdir()
for entry in entries: (folder / entry['source']).write_text(entry['text'])
path = root / 'total.zip'
with zipfile.ZipFile(path, 'w') as archive:
    for entry in entries: archive.writestr(entry['source'], entry['text'])
bundle = root / 'total.json'; bundle.write_text(json.dumps({'entries': entries}))
codes = [error(lambda: manager.submit_directory('d', folder, 'n')),
         error(lambda: manager.submit_zip('z', path, 'n')),
         error(lambda: manager.submit_json('j', bundle))]
print(json.dumps({'codes': codes, 'jobs': store.list_jobs()}))
""")
        self.assertEqual(got, {"codes": ["too_large"] * 3, "jobs": []})

    def test_zip_encryption_and_malformed_utf8_names_rejected_before_admission(self):
        got = _smoke("""
path = root / 'encrypted.zip'
with zipfile.ZipFile(path, 'w') as archive: archive.writestr('a.txt', 'A')
raw = bytearray(path.read_bytes())
local, central = raw.index(b'PK\\x03\\x04'), raw.index(b'PK\\x01\\x02')
raw[local + 6] |= 1; raw[central + 8] |= 1
path.write_bytes(raw)
codes = [error(lambda: manager.submit_zip('encrypted', path, 'n'))]
path = root / 'bad-name.zip'
with zipfile.ZipFile(path, 'w') as archive: archive.writestr('a.txt', 'A')
raw = bytearray(path.read_bytes())
local, central = raw.index(b'PK\\x03\\x04'), raw.index(b'PK\\x01\\x02')
raw[local + 7] |= 8; raw[central + 9] |= 8
raw[local + 30] = 255; raw[central + 46] = 255
path.write_bytes(raw)
codes.append(error(lambda: manager.submit_zip('utf8name', path, 'n')))
print(json.dumps({'codes': codes, 'jobs': store.list_jobs()}))
""")
        self.assertEqual(got, {"codes": ["invalid_archive", "invalid_utf8"], "jobs": []})

    def test_ignored_zip_directory_local_header_and_crc_still_validated(self):
        got = _smoke("""
path = root / 'header.zip'
with zipfile.ZipFile(path, 'w') as archive: archive.writestr('safe/', '')
raw = bytearray(path.read_bytes()); raw[30:35] = b'evil/'
path.write_bytes(raw)
codes = [error(lambda: manager.submit_zip('header', path, 'n'))]
path = root / 'crc-directory.zip'
with zipfile.ZipFile(path, 'w') as archive: archive.writestr('safe/', 'unique-payload')
path.write_bytes(path.read_bytes().replace(b'unique-payload', b'broken-payload'))
codes.append(error(lambda: manager.submit_zip('crc', path, 'n')))
print(json.dumps({'codes': codes, 'jobs': store.list_jobs()}))
""")
        self.assertEqual(got, {"codes": ["invalid_archive"] * 2, "jobs": []})


if __name__ == "__main__":
    unittest.main()
