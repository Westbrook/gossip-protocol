"""Cumulative trusted-reference integration, not model quality evidence."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m2_reference_v1 import m2_files
from gossip_harness.library_m3_reference_v1 import m3_files, package_changes
from gossip_harness.library_project_fixture_v1 import IMMUTABLE_PATHS, PACKAGE_SCOPES, public_cases


def _authored(script):
    with tempfile.TemporaryDirectory(prefix="trusted-library-m3-") as directory:
        root = Path(directory).resolve()
        for name, source in m3_files().items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding="utf-8")
        bootstrap = (
            "import json, resource, sys\n"
            "resource.setrlimit(resource.RLIMIT_CPU, (15, 15))\n"
            "resource.setrlimit(resource.RLIMIT_FSIZE, (67108864, 67108864))\n"
            "resource.setrlimit(resource.RLIMIT_NOFILE, (96, 96))\n"
            "sys.path.insert(0, sys.argv[1])\n"
        )
        result = subprocess.run([sys.executable, "-I", "-c", bootstrap + script, str(root)],
            cwd=root, capture_output=True, text=True, timeout=25, check=False)
        if result.returncode or len(result.stdout.encode()) > 65536:
            raise AssertionError("Trusted M3 smoke failed: " + result.stderr[:6000])
        return json.loads(result.stdout)


class LibraryM3CompositionTests(unittest.TestCase):
    def test_complete_scoped_composition_preserves_frozen_inputs(self):
        previous, files = m2_files(), m3_files()
        self.assertEqual({p: files[p] for p in IMMUTABLE_PATHS},
                         {p: previous[p] for p in IMMUTABLE_PATHS})
        self.assertLess(sum(len(value.encode()) for value in files.values()), 1_048_576)
        for name, value in files.items():
            if name.endswith(".py"):
                compile(value, name, "exec")
        merged = dict(previous)
        for package, prefix in PACKAGE_SCOPES.items():
            patch = package_changes(package)
            self.assertTrue(all(name.startswith(prefix) for name in patch))
            merged.update(patch)
        self.assertEqual(merged, files)
        files["solution.py"] = "changed"
        self.assertEqual(m3_files()["solution.py"], previous["solution.py"])
        with self.assertRaises(ValueError):
            package_changes("undeclared")


class LibraryM3CumulativeReferenceTests(unittest.TestCase):
    def test_inherited_public_m1_histories_execute_against_combined_m3(self):
        cases = public_cases("m1")
        result = _authored("from solution import solve\n"
            + f"cases = json.loads({json.dumps(cases)!r})\n"
            + "print(json.dumps({'count':len(cases),'failures':[c['id'] for c in cases if solve(c['input']) != c['expected']]}))\n")
        self.assertEqual(result, {"count": 8, "failures": []})

    def test_original_job_receipt_and_tombstone_intake_survive_cumulative_changes(self):
        result = _authored(r'''
from library.catalog.store import Store
from library.ingestion.jobs import JobManager
from library.common import LibraryError

def error(call):
    try:
        return call()
    except LibraryError as exc:
        return {'error':exc.code}

store = Store('cumulative.sqlite'); jobs = JobManager(store)
jobs.submit('original', [{'source':'a.txt','text':'first'}])
token = jobs.prepare('original'); original = jobs.commit(token)
doc = store.documents()[0]; identifier = doc['document_id']
store.refresh_document(identifier, 1, text='second')
store.delete_document(identifier, 2)
jobs.submit('changed', [{'source':'a.txt','text':'third'}])
changed = error(lambda:jobs.prepare('changed'))
jobs.submit('identical', [{'source':'a.txt','text':'second'}])
identical = jobs.commit(jobs.prepare('identical'))
replay = jobs.commit(token)
state = store.lifecycle_show(identifier)
generation = store.lifecycle_list(deleted='all')['generation']
store.close(); store = Store('cumulative.sqlite')
print(json.dumps({'original_replayed':original == replay,
 'reopen_replayed':store.commit_job(token['job_id'],token['epoch']) == original,
 'changed':changed,'visible':store.documents(),'state':state,
 'generation':generation,'identical':identical,'original':original}))
store.close()
''')
        self.assertTrue(result['original_replayed'])
        self.assertTrue(result['reopen_replayed'])
        self.assertEqual(result['changed'], {'error':'source_changed'})
        self.assertEqual(result['visible'], [])
        self.assertEqual(result['generation'], 3)
        self.assertEqual({key:result['state'][key] for key in ('revision','edit_version','deleted')},
                         {'revision':2,'edit_version':3,'deleted':True})
        self.assertEqual(result['state']['document']['text'], 'second')
        self.assertIn('first', json.dumps(result['original']))
        self.assertIn('second', json.dumps(result['identical']))
