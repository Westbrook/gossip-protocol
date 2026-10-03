"""Authored-reference recovery qualification, not arbitrary-candidate acceptance.

The two SIGKILL cases interpose the declared Store.commit_job method inside a
fresh trusted application process. They do not expose failure hooks to clients.
"""
from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m2_reference_v1 import m2_files
from gossip_harness.library_m3_worker_reference_v1 import worker_files


_BOOTSTRAP = '''import resource, sys
resource.setrlimit(resource.RLIMIT_CPU, (15, 15))
resource.setrlimit(resource.RLIMIT_FSIZE, (67108864, 67108864))
resource.setrlimit(resource.RLIMIT_NOFILE, (96, 96))
sys.path.insert(0, sys.argv[1])
'''


@contextlib.contextmanager
def _application():
    # Qualify this owner-controlled subset; combined M3 has separate verification.
    from gossip_harness.library_m3_control_reference_v1 import control_files
    files = m2_files() | control_files() | worker_files()
    files['library/catalog/store.py'] = (
        'from library.catalog.control import CoreStore\n'
        'from library.catalog.worker import WorkerMixin\n'
        'class Store(WorkerMixin, CoreStore):\n    pass\n')
    with tempfile.TemporaryDirectory(prefix='authored-m3-worker-') as directory:
        root = Path(directory).resolve()
        for name, source in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding='utf-8')
        yield root


def _run(root, script, *, expected_exit=0):
    result = subprocess.run([sys.executable, '-I', '-c', _BOOTSTRAP + script, str(root)],
                            cwd=root, capture_output=True, text=True, timeout=25, check=False)
    if result.returncode != expected_exit or len(result.stdout.encode()) > 65536:
        raise AssertionError(f'Authored worker process exit={result.returncode}: {result.stderr[:6000]}')
    return json.loads(result.stdout if expected_exit == 0 else result.stderr)


def _once(root, *, expected_exit=0):
    return _run(root, "from library.ingestion.worker import main\n"
                     "raise SystemExit(main(['--db','state.sqlite','--once']))\n",
                expected_exit=expected_exit)


def _pause_process(root, script):
    process = subprocess.Popen([sys.executable, '-I', '-c', _BOOTSTRAP + script, str(root)],
                               cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True)
    if process.stdout is None:
        raise AssertionError('Missing worker stdout')
    ready, _, _ = select.select([process.stdout], [], [], 15)
    if not ready or process.stdout.readline().strip() != 'BOUNDARY':
        process.kill()
        _, error = process.communicate(timeout=5)
        raise AssertionError('Authored worker failed to reach commit boundary: ' + error[:6000])
    return process


class LibraryM3WorkerReferenceTests(unittest.TestCase):
    def test_generated_worker_sources_compile_and_have_no_client_fault_hook(self):
        files = worker_files()
        self.assertEqual(set(files), {'library/catalog/worker.py', 'library/ingestion/worker.py'})
        for path, source in files.items():
            compile(source, path, 'exec')
            self.assertNotIn('getenv', source)
            self.assertNotIn('SIGKILL', source)
        self.assertEqual(files, worker_files())

    def test_enrollment_is_opt_in_ordered_and_does_not_change_manual_semantics(self):
        with _application() as root:
            result = _run(root, r'''
import json
from library.catalog.store import Store
from library.ingestion.worker import run_worker
from library.common import LibraryError
s=Store('state.sqlite')
for name in ['z-last','manual','a-first']:
    s.create_job(name,[{'source':name+'.txt','text':name}])
s.close();s=Store('state.sqlite')
before=s.list_jobs()
first=s.enqueue_job('z-last'); duplicate=s.enqueue_job('z-last');s.enqueue_job('a-first')
a=run_worker(s,once=True); z=run_worker(s,once=True); idle=run_worker(s,once=True)
try:s.enqueue_job('a-first')
except LibraryError as error:terminal=error.code
s.start_job('manual',1);receipt=s.commit_job('manual',1)
print(json.dumps({'before':before,'first':first['status'],'duplicate':duplicate['status'],
 'a':a,'z':z,'idle':idle,'terminal':terminal,'replay':receipt==s.commit_job('manual',1),
 'generation':s.db.execute("SELECT value FROM control WHERE key='worker_generation'").fetchone()[0]}))
s.close()
''')
        self.assertTrue(all(j['state'] == 'queued' and j['epoch'] == 1 for j in result['before']))
        self.assertEqual((result['first'], result['duplicate']), ('enqueued', 'unchanged'))
        self.assertEqual((result['a']['processed'], result['z']['processed']), ('a-first', 'z-last'))
        self.assertEqual(result['idle'], {'processed': None, 'job': None})
        self.assertEqual(result['terminal'], 'job_state')
        self.assertTrue(result['replay'])
        self.assertEqual(int(result['generation']), 3)

    def test_domain_failures_are_durable_and_retry_requires_explicit_transition(self):
        with _application() as root:
            result = _run(root, r'''
import json
from library.catalog.store import Store
from library.ingestion.worker import run_worker
from library.common import LibraryError
s=Store('state.sqlite');s.create_job('invalid',[{'source':'bad.exe','text':'x'}]);s.enqueue_job('invalid')
try:run_worker(s,once=True)
except LibraryError as error:failure=error.code
failed=s.get_job('invalid'); idle=run_worker(s,once=True)
retry=s.retry_job('invalid')
try:run_worker(s,once=True)
except LibraryError as error:retried_error=error.code
print(json.dumps({'failure':failure,'failed':failed,'idle':idle,'retry':retry,
 'retried_error':retried_error,'after':s.get_job('invalid'),'documents':s.documents(),
 'enrolled':s.db.execute("SELECT enrolled FROM job_control WHERE job_id='invalid'").fetchone()[0]}))
s.close()
''')
        self.assertEqual(result['failure'], 'unsupported_type')
        self.assertEqual(result['failed']['state'], 'failed')
        self.assertEqual(result['idle'], {'processed': None, 'job': None})
        self.assertEqual(result['retry']['epoch'], 2)
        self.assertEqual(result['retried_error'], 'unsupported_type')
        self.assertEqual(result['after']['state'], 'failed')
        self.assertEqual(result['enrolled'], 1)
        self.assertEqual(result['documents'], [])

    def test_claims_fence_owner_before_epoch_and_manual_tokens_stay_two_fields(self):
        with _application() as root:
            result = _run(root, r'''
import json
from library.catalog.store import Store
from library.common import LibraryError
s=Store('state.sqlite');s.create_job('job',[{'source':'a.txt','text':'a'}]);s.enqueue_job('job')
def attempt(claim):
    try:
        with s.worker_guard(claim):s.start_job('job',claim['epoch'])
    except LibraryError as error:return error.code
with s.worker_owner() as owner:
    old=dict(owner,job_id='job',epoch=1)
    s.cancel_job('job');s.retry_job('job')
    errors=[attempt(dict(old,generation=owner['generation']+1)),attempt(old)]
    s.start_job('job',3);receipt=s.commit_job('job',3)
with s.worker_owner() as new:
    errors.append(attempt(old))
print(json.dumps({'errors':errors,'job':s.get_job('job'),'receipt':receipt['job']}));s.close()
''')
        self.assertEqual(result['errors'], ['stale_worker', 'stale_epoch', 'stale_worker'])
        self.assertEqual(result['job'], result['receipt'])
        self.assertEqual(result['job']['state'], 'completed')

    def test_io_failure_preserves_retryable_running_manifest_and_diagnostics(self):
        with _application() as root:
            result = _run(root, r'''
import json
from library.catalog.store import Store
from library.ingestion.worker import run_worker
from library.common import LibraryError
s=Store('state.sqlite');s.create_job('job',[{'source':'a.txt','text':'a'}]);s.enqueue_job('job')
original=s.commit_job
def fail(*args,**kwargs):raise OSError('authored reference I/O failure')
s.commit_job=fail
try:run_worker(s,once=True)
except LibraryError as error:failure=error.code
before=s.get_job('job');docs=s.documents()
last=json.loads(s.db.execute("SELECT value FROM control WHERE key='last_error'").fetchone()[0])
s.commit_job=original;after=run_worker(s,once=True)
cleared=json.loads(s.db.execute("SELECT value FROM control WHERE key='last_error'").fetchone()[0])
print(json.dumps({'failure':failure,'before':before,'docs':docs,'last':last,'after':after,'cleared':cleared}));s.close()
''')
        self.assertEqual(result['failure'], 'io_error')
        self.assertEqual(result['before']['state'], 'running')
        self.assertEqual(result['before']['completed'], 0)
        self.assertEqual(result['docs'], [])
        self.assertEqual(result['last'], {'operation': 'worker', 'code': 'io_error'})
        self.assertEqual(result['after']['job']['state'], 'completed')
        self.assertIsNone(result['cleared'])

    def test_initial_sqlite_denial_maps_io_error_and_preserves_retryable_state(self):
        for phase in ('selection', 'idle-clear'):
            with self.subTest(phase=phase), _application() as root:
                script = r'''
import json, sqlite3
from library.catalog.store import Store
from library.ingestion.worker import run_worker
from library.common import LibraryError
s=Store('state.sqlite')
if PHASE == 'selection':
    s.create_job('job',[{'source':'a.txt','text':'original'}]);s.enqueue_job('job')
s.record_error('worker','unsupported_type')
denials=0
def authorizer(action,table,column,database,trigger):
    global denials
    selected = (action == sqlite3.SQLITE_READ and table == 'jobs' and column == 'job_id')
    cleared = (action == sqlite3.SQLITE_UPDATE and table == 'control' and column == 'value')
    if not denials and (selected if PHASE == 'selection' else cleared):
        denials += 1
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK
with s.worker_owner() as owner:
    s.db.set_authorizer(authorizer)
    try:s.worker_process_once(owner)
    except LibraryError as error:failure=error.code
    finally:s.db.set_authorizer(None)
before=s.list_jobs();documents=s.documents()
last=json.loads(s.db.execute("SELECT value FROM control WHERE key='last_error'").fetchone()[0])
after=run_worker(s,once=True)
print(json.dumps({'failure':failure,'denials':denials,'before':before,'documents':documents,
 'last':last,'after':after}));s.close()
'''.replace('PHASE', repr(phase))
                result = _run(root, script)
                self.assertEqual(result['failure'], 'io_error')
                self.assertEqual(result['denials'], 1)
                self.assertEqual(result['last'], {'operation': 'worker', 'code': 'io_error'})
                self.assertEqual(result['documents'], [])
                if phase == 'selection':
                    self.assertEqual(result['before'][0]['state'], 'queued')
                    self.assertEqual(result['before'][0]['epoch'], 1)
                    self.assertEqual(result['after']['job']['state'], 'completed')
                else:
                    self.assertEqual(result['before'], [])
                    self.assertEqual(result['after'], {'processed': None, 'job': None})

    def test_idle_success_clears_only_worker_error_under_current_owner_fence(self):
        with _application() as root:
            result = _run(root, r'''
import json
from library.catalog.store import Store
from library.ingestion.worker import run_worker
from library.common import LibraryError
s=Store('state.sqlite')
def last():
    return json.loads(s.db.execute("SELECT value FROM control WHERE key='last_error'").fetchone()[0])
s.record_error('worker','io_error'); idle=run_worker(s,once=True); cleared=last()
s.record_error('restore','invalid_backup'); run_worker(s,once=True); unrelated=last()
with s.worker_owner() as owner:pass
s.record_error('worker','io_error')
try:s.worker_process_once(owner)
except LibraryError as error:stale=error.code
print(json.dumps({'idle':idle,'cleared':cleared,'unrelated':unrelated,
 'stale':stale,'preserved':last()}));s.close()
''')
        self.assertEqual(result['idle'], {'processed': None, 'job': None})
        self.assertIsNone(result['cleared'])
        self.assertEqual(result['unrelated'], {'operation': 'restore', 'code': 'invalid_backup'})
        self.assertEqual(result['stale'], 'stale_worker')
        self.assertEqual(result['preserved'], {'operation': 'worker', 'code': 'io_error'})

    def test_cleanup_io_failures_persist_diagnostics_without_false_job_completion(self):
        for phase, call_number, with_job in [('startup', 1, True), ('idle', 2, False), ('after', 2, True)]:
            with self.subTest(phase=phase), _application() as root:
                script = r'''
import json
from library.catalog.store import Store
from library.ingestion.worker import run_worker
from library.common import LibraryError
s=Store('state.sqlite')
if WITH_JOB:
    s.create_job('job',[{'source':'a.txt','text':'original'}]);s.enqueue_job('job')
original=s.cleanup_artifacts; calls=0
def fail_cleanup(*args,**kwargs):
    global calls
    calls += 1
    if calls == CALL_NUMBER:raise OSError('authored cleanup I/O failure')
    return original(*args,**kwargs)
s.cleanup_artifacts=fail_cleanup
try:run_worker(s,once=True)
except LibraryError as error:failure=error.code
last=json.loads(s.db.execute("SELECT value FROM control WHERE key='last_error'").fetchone()[0])
print(json.dumps({'failure':failure,'last':last,'jobs':s.list_jobs(),'documents':s.documents()}));s.close()
'''.replace('WITH_JOB', repr(with_job)).replace('CALL_NUMBER', str(call_number))
                result = _run(root, script)
                self.assertEqual(result['failure'], 'io_error')
                self.assertEqual(result['last'], {'operation': 'worker', 'code': 'io_error'})
                if phase == 'startup':
                    self.assertEqual(result['jobs'][0]['state'], 'queued')
                    self.assertEqual(result['documents'], [])
                elif phase == 'after':
                    self.assertEqual(result['jobs'][0]['state'], 'completed')
                    self.assertEqual(len(result['documents']), 1)
                else:
                    self.assertEqual(result['jobs'], [])
                    self.assertEqual(result['documents'], [])

    def test_enrolled_manifest_resumes_without_source_files(self):
        with _application() as root:
            _run(root, r'''
import json
from pathlib import Path
from library.catalog.store import Store
from library.ingestion.jobs import JobManager
p=Path('inputs');p.mkdir();(p/'a.txt').write_text('admitted original',encoding='utf-8')
s=Store('state.sqlite');JobManager(s).submit_directory('admitted',p,'library');s.enqueue_job('admitted')
(p/'a.txt').unlink();p.rmdir();s.close();print(json.dumps(True))
''')
            result = _once(root)
            after = _run(root, "import json\nfrom library.catalog.store import Store\n"
                               "s=Store('state.sqlite');print(json.dumps(s.documents()));s.close()\n")
        self.assertEqual(result['job']['state'], 'completed')
        self.assertEqual(after[0]['text'], 'admitted original')
        self.assertEqual(after[0]['source'], 'library/a.txt')

    def test_second_live_owner_is_rejected_without_epoch_generation_or_job_mutation(self):
        with _application() as root:
            _run(root, "import json\nfrom library.catalog.store import Store\n"
                       "s=Store('state.sqlite');s.create_job('job',[{'source':'a.txt','text':'a'}]);"
                       "s.enqueue_job('job');s.close();print(json.dumps(True))\n")
            process = _pause_process(root, r'''
import time
from library.catalog.store import Store
s=Store('state.sqlite')
with s.worker_owner():
    print('BOUNDARY',flush=True)
    time.sleep(60)
''')
            try:
                self.assertEqual(_once(root, expected_exit=2), {'error': 'worker_busy'})
                state = _run(root, "import json\nfrom library.catalog.store import Store\n"
                                  "s=Store('state.sqlite');print(json.dumps({'job':s.get_job('job'),"
                                  "'generation':s.db.execute(\"SELECT value FROM control WHERE key='worker_generation'\").fetchone()[0]}));s.close()\n")
                self.assertEqual(state['job']['state'], 'queued')
                self.assertEqual(state['job']['epoch'], 1)
                self.assertEqual(int(state['generation']), 1)
            finally:
                process.kill(); process.communicate(timeout=5)
            self.assertEqual(_once(root)['job']['state'], 'completed')

    def test_real_sigkill_before_and_after_commit_recovers_exactly_once(self):
        for boundary in ('before', 'after'):
            with self.subTest(boundary=boundary), _application() as root:
                _run(root, "import json\nfrom library.catalog.store import Store\n"
                           "s=Store('state.sqlite');s.create_job('job',[{'source':'a.txt','text':'persisted'}]);"
                           "s.enqueue_job('job');s.close();print(json.dumps(True))\n")
                script = r'''
import time
from library.catalog.store import Store
from library.ingestion.worker import main
original=Store.commit_job
def boundary(self,*args,**kwargs):
    if BOUNDARY == 'after':result=original(self,*args,**kwargs)
    print('BOUNDARY',flush=True)
    time.sleep(60)
    if BOUNDARY == 'before':return original(self,*args,**kwargs)
    return result
Store.commit_job=boundary
raise SystemExit(main(['--db','state.sqlite','--once']))
'''.replace('BOUNDARY ==', repr(boundary) + ' ==')
                process = _pause_process(root, script)
                os.kill(process.pid, signal.SIGKILL)
                process.communicate(timeout=5)
                self.assertEqual(process.returncode, -signal.SIGKILL)
                inspect = r'''
import json
from library.catalog.store import Store
s=Store('state.sqlite')
row=s.db.execute("SELECT receipt FROM jobs WHERE job_id='job'").fetchone()
print(json.dumps({'job':s.get_job('job'),'documents':s.documents(),'receipt':row[0],
 'blobs':s.db.execute('SELECT count(*) FROM blobs').fetchone()[0],
 'revisions':s.db.execute('SELECT count(*) FROM revisions').fetchone()[0]}));s.close()
'''
                stopped = _run(root, inspect)
                if boundary == 'before':
                    self.assertEqual(stopped['job']['state'], 'running')
                    self.assertEqual(stopped['job']['completed'], 0)
                    self.assertEqual(stopped['documents'], [])
                    self.assertEqual(stopped['blobs'], 0)
                    self.assertIsNone(stopped['receipt'])
                    self.assertEqual(_once(root)['job']['state'], 'completed')
                else:
                    self.assertEqual(stopped['job']['state'], 'completed')
                    self.assertIsNotNone(stopped['receipt'])
                    self.assertEqual(_once(root), {'processed': None, 'job': None})
                recovered = _run(root, inspect)
                self.assertEqual(recovered['job']['epoch'], 1)
                self.assertEqual((len(recovered['documents']), recovered['blobs'], recovered['revisions']), (1, 1, 1))
                self.assertEqual(recovered['documents'][0]['text'], 'persisted')
                if boundary == 'after':
                    self.assertEqual(recovered, stopped)
                self.assertEqual(_once(root), {'processed': None, 'job': None})
