"""Bounded host smoke for checked-in AUTHORED references only.

No arbitrary candidate source is accepted here. These checks do not replace the
source-bound Docker, real browser, peer/Git or independent acceptance lanes.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m1_reference_v1 import (
    catalog_repair_changes, m1_files, package_changes,
)
from gossip_harness.library_project_fixture_v1 import (
    IMMUTABLE_PATHS, PACKAGE_SCOPES, public_cases, seed_files,
)


def _authored_smoke(script: str, *, defective: bool = False) -> dict:
    """Fresh trusted authored app process, bounded CPU/FD/files/output/wall time."""
    with tempfile.TemporaryDirectory(prefix="trusted-library-m1-") as directory:
        root = Path(directory).resolve()
        for name, source in m1_files(defective_blob_dedup=defective).items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding="utf-8")
        bootstrap = (
            "import json, resource, sys\n"
            "resource.setrlimit(resource.RLIMIT_CPU, (8, 8))\n"
            "resource.setrlimit(resource.RLIMIT_FSIZE, (8388608, 8388608))\n"
            "resource.setrlimit(resource.RLIMIT_NOFILE, (96, 96))\n"
            "sys.path.insert(0, sys.argv[1])\n"
        )
        result = subprocess.run(
            [sys.executable, "-I", "-c", bootstrap + script, str(root)], cwd=root,
            capture_output=True, text=True, timeout=15, check=False,
        )
        if result.returncode:
            raise AssertionError(f"Authored reference smoke failed: {result.stderr[:5000]}")
        if len(result.stdout.encode()) > 65536:
            raise AssertionError("Authored reference smoke output exceeds bound")
        return json.loads(result.stdout)


class LibraryM1ReferenceV1Tests(unittest.TestCase):
    def test_sources_are_compilable_fresh_scoped_and_keep_all_immutable_inputs(self):
        files = m1_files()
        self.assertLess(sum(len(value.encode()) for value in files.values()), 150 * 1024)
        for path, source in files.items():
            if path.endswith(".py"):
                compile(source, path, "exec")
        seed = seed_files()
        self.assertEqual({p: files[p] for p in IMMUTABLE_PATHS}, {p: seed[p] for p in IMMUTABLE_PATHS})
        patches = [package_changes(package) for package in PACKAGE_SCOPES]
        self.assertTrue(all(patches))
        merged = dict(seed)
        for package, patch in zip(PACKAGE_SCOPES, patches):
            self.assertTrue(all(path.startswith(PACKAGE_SCOPES[package]) for path in patch))
            merged.update(patch)
        self.assertEqual(merged, files)
        files["solution.py"] = "changed"
        self.assertEqual(m1_files()["solution.py"], seed["solution.py"])
        with self.assertRaises(ValueError):
            package_changes("other")

    def test_all_frozen_public_histories_run_real_authored_modules(self):
        cases = public_cases("m1")
        answer = _authored_smoke(
            "from solution import solve\n"
            f"cases = json.loads({json.dumps(cases)!r})\n"
            "failures = [case['id'] for case in cases if solve(case['input']) != case['expected']]\n"
            "print(json.dumps({'count':len(cases), 'failures':failures}))\n"
        )
        self.assertEqual(answer, {"count": 8, "failures": []})

    def test_deliberate_batch_blob_identity_defect_fails_probe_but_keeps_v0(self):
        wrong = m1_files(defective_blob_dedup=True)
        corrected = wrong | catalog_repair_changes()
        self.assertEqual(corrected, m1_files())
        differences = {path for path in wrong if wrong[path] != corrected[path]}
        self.assertEqual(differences, {"library/catalog/store.py"})
        cases = public_cases("m1")
        answer = _authored_smoke(
            "from solution import solve\n"
            f"cases = json.loads({json.dumps(cases)!r})\n"
            "failures = [case['id'] for case in cases if solve(case['input']) != case['expected']]\n"
            "print(json.dumps({'failures':failures}))\n", defective=True,
        )
        self.assertEqual(answer["failures"], ["m1-per-source-provenance"])

    def test_catalog_fences_rechecks_and_replays_across_connections_and_restart(self):
        answer = _authored_smoke(r'''
from library.catalog.store import Store
from library.ingestion.jobs import JobManager
from library.common import LibraryError

def code(call):
    try:
        call()
        return None
    except LibraryError as error:
        return error.code

first, second = Store('shared.sqlite3'), Store('shared.sqlite3')
jobs = JobManager(first)
submitted = jobs.submit('stale', [{'source':'a.txt','text':'A'}])
token = jobs.prepare('stale')
cancelled = second.cancel_job('stale')
stale = code(lambda: jobs.commit(token))
queued = second.retry_job('stale')
new = jobs.prepare('stale')
receipt = jobs.commit(new)
first.close()
first = Store('shared.sqlite3')
replay = first.commit_job('stale', new['epoch'])
jobs = JobManager(first)
jobs.submit('conflict', [{'source':'b.txt','text':'new'}, {'source':'c.txt','text':'C'}])
conflict_token = jobs.prepare('conflict')
second.insert('c.txt', b'external')
conflict = jobs.commit(conflict_token)
jobs.submit('empty', [])
empty = jobs.commit(jobs.prepare('empty'))
row = first.db.execute('SELECT manifest,content_hashes FROM jobs WHERE job_id=?', ('stale',)).fetchone()
answer = {'epochs':[submitted['epoch'],cancelled['epoch'],queued['epoch']], 'stale':stale,
          'replay':receipt==replay, 'conflict':conflict, 'failed':jobs.get('conflict'),
          'sources':[doc['source'] for doc in first.documents()], 'empty':empty,
          'manifest':json.loads(row['manifest']), 'hashes':json.loads(row['content_hashes'])}
first.close(); second.close()
print(json.dumps(answer))
''')
        self.assertEqual(answer["epochs"], [1, 2, 3])
        self.assertEqual(answer["stale"], "stale_epoch")
        self.assertTrue(answer["replay"])
        self.assertEqual(answer["conflict"], {"error": "source_changed"})
        self.assertEqual(answer["failed"]["state"], "failed")
        self.assertEqual(answer["failed"]["completed"], 0)
        self.assertEqual(answer["sources"], ["a.txt", "c.txt"])
        self.assertEqual(answer["empty"]["documents"], [])
        self.assertEqual(answer["empty"]["job"]["state"], "completed")
        self.assertEqual(answer["manifest"], [{"source": "a.txt", "text": "A"}])
        self.assertEqual(answer["hashes"], [hashlib.sha256(b"A").hexdigest()])

    def test_provisional_write_failure_rolls_back_blobs_state_and_receipt(self):
        answer = _authored_smoke(r'''
from library.catalog.store import Store
from library.ingestion.jobs import JobManager
from library.common import LibraryError
store = Store('rollback.sqlite3')
store.insert('keep.txt', b'old')
jobs = JobManager(store)
jobs.submit('atomic', [{'source':'new.txt','text':'new'}, {'source':'other.txt','text':'new'}])
token = jobs.prepare('atomic')
try:
    jobs.commit(token, fail_before_commit=True)
except LibraryError as error:
    failure = error.code
state = jobs.get('atomic')
blob_count = store.db.execute('SELECT count(*) FROM blobs').fetchone()[0]
receipt = store.db.execute('SELECT receipt FROM jobs').fetchone()[0]
sources = [doc['source'] for doc in store.documents()]
store.close()
store = Store('rollback.sqlite3')
completed = store.commit_job('atomic', 1)
print(json.dumps({'failure':failure,'state':state,'blobs':blob_count,'receipt':receipt,
                  'sources':sources,'final_count':len(store.documents()),
                  'shared':completed['documents'][0]['blob_id']==completed['documents'][1]['blob_id'],
                  'different':completed['documents'][0]['document_id']!=completed['documents'][1]['document_id']}))
store.close()
''')
        self.assertEqual(answer["failure"], "injected_failure")
        self.assertEqual(answer["state"]["state"], "running")
        self.assertEqual(answer["state"]["completed"], 0)
        self.assertEqual(answer["blobs"], 1)
        self.assertIsNone(answer["receipt"])
        self.assertEqual(answer["sources"], ["keep.txt"])
        self.assertEqual(answer["final_count"], 3)
        self.assertTrue(answer["shared"])
        self.assertTrue(answer["different"])

    def test_capacity_is_rechecked_transactionally_without_leaking_provisional_rows(self):
        answer = _authored_smoke(r'''
from library.catalog.store import Store
from library.ingestion.jobs import JobManager
first, second = Store('capacity.sqlite3'), Store('capacity.sqlite3')
for index in range(254):
    first.insert(str(index)+'.txt', b'shared')
jobs = JobManager(first)
jobs.submit('race', [{'source':'new-a.txt','text':'A'}, {'source':'new-b.txt','text':'B'}])
token = jobs.prepare('race')
second.insert('external.txt', b'external')
result = jobs.commit(token)
failed = jobs.get('race')
count = len(first.documents())
blobs = first.db.execute('SELECT count(*) FROM blobs').fetchone()[0]
second.insert('last.txt', b'last')
jobs.submit('existing', [{'source':'0.txt','text':'shared'}])
receipt = jobs.commit(jobs.prepare('existing'))
print(json.dumps({'result':result,'failed':failed,'count':count,'blobs':blobs,
                  'at_capacity_replay':receipt['documents'][0]['source'],
                  'final_count':len(first.documents())}))
first.close(); second.close()
''')
        self.assertEqual(answer["result"], {"error": "capacity"})
        self.assertEqual(answer["failed"]["state"], "failed")
        self.assertEqual(answer["failed"]["completed"], 0)
        self.assertEqual(answer["count"], 255)
        self.assertEqual(answer["blobs"], 2)
        self.assertEqual(answer["at_capacity_replay"], "0.txt")
        self.assertEqual(answer["final_count"], 256)

    def test_admission_shape_semantics_fresh_values_and_terminal_misuse(self):
        answer = _authored_smoke(r'''
from library.catalog.store import Store
from library.ingestion.jobs import JobManager
from library.common import LibraryError
store = Store('edges.sqlite3'); jobs = JobManager(store)
def code(call):
    try:
        call()
        return None
    except LibraryError as error:
        return error.code
errors = [code(lambda: store.create_job(True, [])),
          code(lambda: store.create_job('shape', [{'source':'a.txt','text':'A','extra':0}]))]
created = jobs.submit('invalid', [{'source':'../bad.txt','text':'B'}])
created['state'] = 'completed'
manifest = store.job_manifest('invalid'); manifest[0]['text'] = 'changed'
errors.append(code(lambda: jobs.prepare('invalid')))
failed = jobs.get('invalid')
errors.append(code(lambda: jobs.cancel('invalid')))
errors.append(code(lambda: jobs.submit('invalid', [{'source':'../bad.txt','text':'different'}])))
jobs.submit('utf8', [{'source':'bad.txt','text':'\ud800'}])
errors.append(code(lambda: jobs.prepare('utf8')))
jobs.submit('done', []); jobs.commit(jobs.prepare('done'))
errors.extend([code(lambda: jobs.prepare('done')), code(lambda: jobs.retry('done')),
               code(lambda: store.start_job('done', 2)), code(lambda: store.commit_job('done', True))])
print(json.dumps({'errors':errors,'failed':failed,'manifest':store.job_manifest('invalid'),
                  'state':jobs.get('utf8')['state']}))
store.close()
''')
        self.assertEqual(answer["errors"], ["invalid_request", "invalid_request", "invalid_source", "job_state",
                                            "job_conflict", "invalid_utf8", "job_state", "job_state",
                                            "stale_epoch", "invalid_request"])
        self.assertEqual(answer["failed"]["state"], "failed")
        self.assertEqual(answer["manifest"], [{"source": "../bad.txt", "text": "B"}])
        self.assertEqual(answer["state"], "failed")

    def test_real_service_job_routes_intake_error_status_and_fault_hook_exclusion(self):
        answer = _authored_smoke(r'''
from pathlib import Path
from library.catalog.store import Store
from library.query.service import Service
root = Path.cwd() / 'input'; root.mkdir()
(root/'bundle.json').write_text(json.dumps({'entries':[{'source':'http.html','text':'<b>literal</b>'}]}))
store = Store('http.sqlite3'); service = Service(store,root)
calls = []
def request(method,path,body=None):
    result = service.request(method,path,body); calls.append(result); return result
request('POST','/api/jobs',{'job_id':'http','json':'bundle.json'})
request('GET','/api/jobs/http')
request('POST','/api/jobs/http/prepare',{})
request('POST','/api/jobs/http/commit',{'epoch':1,'fail_before_commit':True})
request('POST','/api/jobs/http/cancel',{})
request('POST','/api/jobs/http/commit',{'epoch':1})
request('POST','/api/jobs/http/retry',{})
request('POST','/api/jobs/http/prepare',{})
request('POST','/api/jobs/http/commit',{'epoch':3})
request('POST','/api/jobs/http/cancel',{})
request('POST','/api/jobs',{'job_id':'http','entries':[]})
request('POST','/api/jobs',{'job_id':'escape','json':'../bundle.json'})
request('GET','/api/jobs/missing')
request('POST','/api/jobs/http/prepare',{'extra':0})
request('POST','/api/jobs/http/commit',{'epoch':True})
request('GET','/api/jobs')
request('GET','/api/documents?q=%3Cb%3E')
print(json.dumps({'calls':calls,'jobs':store.list_jobs()}))
store.close()
''')
        calls = answer["calls"]
        self.assertEqual([call[0] for call in calls], [200, 200, 200, 400, 200, 409, 200, 200, 200,
                                                    409, 409, 400, 404, 400, 400, 200, 200])
        self.assertEqual(calls[3][1], {"error": "invalid_request"})
        self.assertEqual(calls[5][1], {"error": "stale_epoch"})
        self.assertEqual(calls[8][1]["documents"][0]["text"], "<b>literal</b>")
        self.assertEqual(calls[10][1], {"error": "job_conflict"})
        self.assertEqual(calls[11][1], {"error": "invalid_source"})
        self.assertEqual(calls[-1][1]["total"], 1)
        self.assertEqual([job["job_id"] for job in answer["jobs"]], ["http"])

    def test_actual_cli_entry_reopens_database_and_keeps_json_exit_conventions(self):
        answer = _authored_smoke(r'''
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from pathlib import Path
from library.clients.cli import main
root = Path.cwd() / 'input'; root.mkdir()
(root/'bundle.json').write_text(json.dumps({'entries':[{'source':'cli.md','text':'CLI'}]}))
def run(args):
    out, err = StringIO(), StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        try:
            status = main(['--db','cli.sqlite3','--root',str(root)] + args)
        except SystemExit as error:
            status = error.code
    return {'status':status,'out':json.loads(out.getvalue()) if out.getvalue() else None,
            'err':err.getvalue()}
results = [run(['job-submit','cli','--kind','json','bundle.json']), run(['job-show','cli']),
           run(['job-prepare','cli']), run(['job-cancel','cli']), run(['job-commit','cli','1']),
           run(['job-retry','cli']), run(['job-prepare','cli']), run(['job-commit','cli','3']),
           run(['jobs']), run(['search','CLI']),
           run(['job-submit','missing','--kind','directory','.']),
           run(['job-submit','extra','--kind','json','bundle.json','--namespace','bad'])]
print(json.dumps({'results':results}))
''')
        results = answer["results"]
        self.assertEqual([r["status"] for r in results], [0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 2, 2])
        self.assertEqual(json.loads(results[4]["err"]), {"error": "stale_epoch"})
        self.assertIsNone(results[4]["out"])
        self.assertEqual(results[7]["out"]["job"]["state"], "completed")
        self.assertEqual(results[8]["out"]["jobs"][0]["epoch"], 3)
        self.assertEqual(results[9]["out"]["total"], 1)
        self.assertIn("--namespace", results[10]["err"])
        self.assertIn("forbidden", results[11]["err"])


if __name__ == "__main__":
    unittest.main()
