"""Prospective v2 maintenance process regressions, not candidate acceptance."""
from __future__ import annotations

import contextlib
import json
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m4_reference_v1 import m4_files
from gossip_harness.library_v2_catalog_reference_v1 import catalog_files
from gossip_harness.library_v2_maintenance_reference_v1 import maintenance_files


_BOOT = """import json, os, resource, sys
from pathlib import Path
resource.setrlimit(resource.RLIMIT_CPU, (20, 20))
resource.setrlimit(resource.RLIMIT_FSIZE, (134217728, 134217728))
resource.setrlimit(resource.RLIMIT_NOFILE, (96, 96))
sys.path.insert(0, sys.argv[1])
from library.catalog.store import Store
from library.common import LibraryError
from library.counters import COUNTER_MAX

def error(call):
    try:
        call()
    except LibraryError as failure:
        return failure.code
    raise AssertionError('Expected a declared refusal')

def dump(store):
    return tuple(store.db.iterdump())
"""


@contextlib.contextmanager
def _application():
    files = m4_files()
    files.update(catalog_files(files))
    files.update(maintenance_files(files))
    files['library/catalog/store.py'] = (
        'from library.catalog.v2_maintenance import V2MaintenanceMixin\n'
        'from library.catalog.m4_backup import M4BackupMixin\n'
        'from library.catalog.maintenance import MaintenanceMixin\n'
        'from library.catalog.worker import WorkerMixin\n'
        'from library.catalog.m4_store import NormalizedStore\n'
        'class Store(V2MaintenanceMixin, M4BackupMixin, MaintenanceMixin, WorkerMixin, NormalizedStore):\n    pass\n')
    with tempfile.TemporaryDirectory(prefix='authored-v2-maintenance-') as directory:
        root = Path(directory).resolve()
        for name, source in files.items():
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(source, encoding='utf-8')
        yield root


def _run(root, script):
    result = subprocess.run([sys.executable, '-I', '-c', _BOOT + script, str(root)],
                            cwd=root, capture_output=True, text=True, timeout=35, check=False)
    if result.returncode != 0 or len(result.stdout.encode()) > 65536:
        raise AssertionError(f'v2 maintenance process exit={result.returncode}: {result.stderr[:7000]}')
    return json.loads(result.stdout)


def _paused(root, script):
    child = subprocess.Popen([sys.executable, '-I', '-c', _BOOT + script, str(root)],
                             cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert child.stdout is not None
    ready, _, _ = select.select([child.stdout], [], [], 15)
    if not ready or child.stdout.readline().strip() != 'BOUNDARY':
        child.kill()
        _, stderr = child.communicate(timeout=5)
        raise AssertionError('Worker never reached boundary: ' + stderr[:6000])
    return child


class LibraryV2MaintenanceReferenceTests(unittest.TestCase):
    def test_sources_compile_and_overlay_does_not_mutate_frozen_base(self):
        base = m4_files()
        before = dict(base)
        overlay = maintenance_files(base)
        self.assertEqual(base, before)
        self.assertEqual(len(overlay), 6)
        for name, source in overlay.items():
            compile(source, name, 'exec')

    def test_explicit_binding_no_implicit_io_and_old_handle_mismatch(self):
        with _application() as root:
            result = _run(root, r'''
s = Store('state.sqlite', backup_dir='missing')
assert s._bound_backup_root() is None
before = dump(s)
assert error(lambda: s.list_backups()) == 'backup_root_unbound'
assert error(lambda: s.backup('one.json')) == 'backup_root_unbound'
assert error(lambda: s.restore_backup('one.json', 0)) == 'backup_root_unbound'
assert dump(s) == before
Path('first').mkdir(); Path('second').mkdir()
s.configure_backup_dir('first')
assert s.adopt_backup_root(None) == {'adopted': True, 'registered': 0}
old = Store('state.sqlite', backup_dir='first')
s.configure_backup_dir('second')
assert s.adopt_backup_root(str(Path('first').resolve())) == {'adopted': True, 'registered': 0}
assert error(lambda: old.list_backups()) == 'backup_root_mismatch'
assert error(lambda: old.adopt_backup_root(str(Path('first').resolve()))) == 'backup_root_mismatch'
assert s.list_backups() == {'backups': [], 'total': 0}
assert s.adopt_backup_root(str(Path('second').resolve())) == {'adopted': False, 'registered': 0}
assert s.diagnostics()['worker_state'] == 'stopped'
old.close(); s.close()
print(json.dumps({'ok': True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_adoption_validates_all_pages_preserves_registry_and_unowned_files(self):
        with _application() as root:
            result = _run(root, r'''
import shutil
Path('first').mkdir(); Path('second').mkdir()
s = Store('state.sqlite', backup_dir='first'); s.adopt_backup_root(None)
metadata = s.backup('original.json'); raw = Path('first/original.json').read_bytes()
with s._write():
    for number in range(103):
        name = f'archive-{number:03}.json'
        Path('first', name).write_bytes(raw)
        s.db.execute('INSERT INTO backups VALUES (?,?,?,?)', (name, metadata['bytes'], metadata['payload_sha256'], metadata['generation']))
for item in Path('first').iterdir(): shutil.copyfile(item, Path('second', item.name))
Path('second/unowned.txt').write_text('keep')
s.configure_backup_dir('second'); before = dump(s)
Path('second/original.json').write_bytes(b'bad')
assert error(lambda: s.adopt_backup_root(str(Path('first').resolve()))) == 'invalid_backup'
assert dump(s) == before
Path('second/original.json').write_bytes(raw)
assert s.adopt_backup_root(str(Path('first').resolve())) == {'adopted': True, 'registered': 104}
assert s.list_backups(limit=100)['total'] == 104
assert len(s.list_backups(limit=100)['backups']) == 100
assert Path('second/unowned.txt').read_text() == 'keep'
assert len(list(Path('first').iterdir())) == 104
assert s.adopt_backup_root(str(Path('second').resolve())) == {'adopted': False, 'registered': 104}
s.close(); print(json.dumps({'validated': 104}))
''')
            self.assertEqual(result, {'validated': 104})

    def test_lexical_symlink_rejected_before_dotdot_normalization(self):
        with _application() as root:
            result = _run(root, r'''
Path('real').mkdir(); Path('target').mkdir(); Path('alias').symlink_to('real', target_is_directory=True)
s = Store('state.sqlite', backup_dir='alias/../target'); before = dump(s)
assert error(lambda: s.adopt_backup_root(None)) == 'invalid_source'
assert dump(s) == before
s.configure_backup_dir('missing')
assert error(lambda: s.adopt_backup_root(None)) == 'io_error'
assert dump(s) == before
s.close(); print(json.dumps({'ok': True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_adoption_rechecks_directory_and_files_before_atomic_binding(self):
        with _application() as root:
            result = _run(root, r'''
class Replaced(Store):
    def _backup_checkpoint(self, phase):
        if phase == 'adoption_validated':
            Path('target').rename('old-target'); Path('target').mkdir()
Path('target').mkdir()
s = Replaced('state.sqlite', backup_dir='target'); before = dump(s)
assert error(lambda: s.adopt_backup_root(None)) == 'io_error'
assert dump(s) == before and s._bound_backup_root() is None
s.close(); print(json.dumps({'ok': True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_pending_artifacts_block_adoption_but_completed_index_does_not(self):
        with _application() as root:
            result = _run(root, r'''
Path('target').mkdir()
s = Store('state.sqlite', backup_dir='target')
with s._write():
    s.db.execute('INSERT INTO maintenance_artifacts VALUES (?,?,?,?,?,?,?)', ('pending','backup_stage','backup','.owned.partial',s._control('incarnation'),0,'published'))
before = dump(s)
assert error(lambda: s.adopt_backup_root(None)) == 'maintenance_busy'
assert dump(s) == before
# Unbound cleanup may service separately proven maintenance rows, never backup ownership.
s.cleanup_artifacts()
assert s.db.execute('SELECT count(*) FROM maintenance_artifacts').fetchone()[0] == 1
with s._write(): s.db.execute('DELETE FROM maintenance_artifacts')
assert s.reindex_step()['state'] == 'completed'
assert s.adopt_backup_root(None) == {'adopted': True, 'registered': 0}
s.close(); print(json.dumps({'ok': True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_restore_exhaustion_preserves_all_state_and_completed_maximum_epoch(self):
        with _application() as root:
            result = _run(root, r'''
Path('target').mkdir()
s = Store('state.sqlite', backup_dir='target'); s.adopt_backup_root(None)
s.create_job('complete', []); s.start_job('complete', 1)
# An empty completed receipt may carry the maximum original epoch.
with s._write(): s.db.execute('UPDATE jobs SET epoch=? WHERE job_id=?', (COUNTER_MAX, 'complete'))
receipt = s.commit_job('complete', COUNTER_MAX)
s.backup('one.json'); before_receipt = s.db.execute('SELECT receipt FROM jobs').fetchone()[0]
assert s.restore_backup('one.json', s._generation())['restored'] is True
assert s.get_job('complete')['epoch'] == COUNTER_MAX
assert s.db.execute('SELECT epoch_high_water FROM job_control').fetchone()[0] == COUNTER_MAX
assert s.db.execute('SELECT receipt FROM jobs').fetchone()[0] == before_receipt
assert s.commit_job('complete', COUNTER_MAX) == receipt
with s._write():
    s.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'", (str(COUNTER_MAX),))
    s._set_control('last_error', '{"operation":"worker","code":"io_error"}')
before = dump(s); files = {str(p): p.read_bytes() for p in Path('target').iterdir()}
assert error(lambda: s.restore_backup('one.json', COUNTER_MAX)) == 'counter_exhausted'
assert dump(s) == before and files == {str(p): p.read_bytes() for p in Path('target').iterdir()}
assert error(lambda: s.restore_backup('one.json', COUNTER_MAX-1)) == 'stale_generation'
assert error(lambda: s.restore_backup('one.json', COUNTER_MAX+1)) == 'invalid_request'
s.close(); print(json.dumps({'ok': True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_worker_owner_exhaustion_precedes_liveness_publication(self):
        with _application() as root:
            result = _run(root, r'''
s = Store('state.sqlite')
with s._write():
    s._set_control('worker_generation', str(COUNTER_MAX))
    s._set_control('last_error', '{"operation":"reindex","code":"io_error"}')
before = dump(s); files = {str(p):p.read_bytes() for p in s.maintenance_root.iterdir()}
def acquire():
    with s.worker_owner(): pass
assert error(acquire) == 'counter_exhausted'
assert dump(s) == before and files == {str(p):p.read_bytes() for p in s.maintenance_root.iterdir()}
assert s.diagnostics()['worker_state'] == 'stopped'
s.close(); print(json.dumps({'ok': True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_live_owner_idle_claim_running_and_sigkill_stopped(self):
        with _application() as root:
            _run(root, "s=Store('state.sqlite'); s.create_job('work', []); s.enqueue_job('work'); s.close(); print('{}')")
            idle = _paused(root, "import time\ns=Store('state.sqlite')\nwith s.worker_owner():\n print('BOUNDARY',flush=True); time.sleep(20)\n")
            try:
                self.assertEqual(_run(root, "s=Store('state.sqlite'); print(json.dumps(s.diagnostics()['worker_state'])); s.close()"), 'idle')
            finally:
                idle.kill(); idle.communicate(timeout=5)
            active = _paused(root, "import time\ns=Store('state.sqlite')\nwith s.worker_owner() as owner:\n claim=dict(owner,job_id='work',epoch=1)\n with s.active_worker_claim(claim):\n  s.start_job('work',1); print('BOUNDARY',flush=True); time.sleep(20)\n")
            try:
                self.assertEqual(_run(root, "s=Store('state.sqlite'); print(json.dumps(s.diagnostics()['worker_state'])); s.close()"), 'running')
                active.send_signal(signal.SIGKILL); active.communicate(timeout=5)
                self.assertEqual(_run(root, "s=Store('state.sqlite'); assert s.get_job('work')['state']=='running'; print(json.dumps(s.diagnostics()['worker_state'])); s.close()"), 'stopped')
            finally:
                if active.poll() is None:
                    active.kill(); active.communicate(timeout=5)

    def test_live_owner_with_stale_claim_is_idle_and_once_releases_ownership(self):
        with _application() as root:
            result = _run(root, r'''
from library.ingestion.worker import run_worker
s=Store('state.sqlite'); s.create_job('work', []); s.enqueue_job('work')
with s.worker_owner() as owner:
    assert s.worker_state() == 'idle'
    s._write_live_claim(dict(owner, job_id='work', epoch=2))
    assert s.worker_state() == 'idle'
    s._write_live_claim(dict(owner, generation=owner['generation']+1, job_id='work', epoch=1))
    assert s.worker_state() == 'idle'
    s._write_live_claim(dict(owner, job_id='work', epoch=1))
    assert s.worker_state() == 'running'
assert s.worker_state() == 'stopped'
assert run_worker(s, once=True)['processed'] == 'work'
assert s.diagnostics()['worker_state'] == 'stopped'
s.close(); print(json.dumps({'ok':True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_live_owner_at_maximum_is_busy_before_allocation_exhaustion(self):
        with _application() as root:
            _run(root, "s=Store('state.sqlite');\nwith s._write(): s._set_control('worker_generation',str(COUNTER_MAX-1))\ns.close(); print('{}')")
            child = _paused(root, "import time\ns=Store('state.sqlite')\nwith s.worker_owner():\n print('BOUNDARY',flush=True); time.sleep(20)\n")
            try:
                result = _run(root, "s=Store('state.sqlite')\ndef acquire():\n with s.worker_owner(): pass\nprint(json.dumps(error(acquire))); s.close()")
                self.assertEqual(result, 'worker_busy')
            finally:
                child.kill(); child.communicate(timeout=5)

    def test_startup_cleanup_failure_records_worker_diagnostic(self):
        with _application() as root:
            result = _run(root, r'''
class Broken(Store):
    def cleanup_artifacts(self, limit=64):
        raise OSError('authored cleanup failure')
s=Broken('state.sqlite')
def acquire():
    with s.worker_owner(): pass
assert error(acquire) == 'io_error'
assert s.diagnostics()['last_error'] == {'operation':'worker', 'code':'io_error'}
assert s.diagnostics()['worker_state'] == 'stopped'
s.close(); print(json.dumps({'ok':True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_worker_job_exhaustion_does_not_publish_claim_start_job_or_cleanup(self):
        with _application() as root:
            result = _run(root, r'''
s=Store('state.sqlite'); s.create_job('work',[{'source':'new.txt','text':'new'}]); s.enqueue_job('work')
with s._write():
    s.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'", (str(COUNTER_MAX),))
    s._set_control('last_error','{"operation":"reindex","code":"io_error"}')
with s.worker_owner() as owner:
    before = dump(s); files = {str(p):p.read_bytes() for p in s.maintenance_root.iterdir()}
    assert error(lambda: s.worker_process_once(owner)) == 'counter_exhausted'
    assert dump(s) == before and files == {str(p):p.read_bytes() for p in s.maintenance_root.iterdir()}
    assert s.get_job('work')['state'] == 'queued'
    assert s.worker_state() == 'idle'
s.close(); print(json.dumps({'ok':True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_restore_preflights_worker_document_and_noncompleted_job_together(self):
        with _application() as root:
            result = _run(root, r'''
Path('target').mkdir()
s=Store('state.sqlite',backup_dir='target'); s.adopt_backup_root(None)
s.create_job('work',[{'source':'one.txt','text':'one'}]); s.start_job('work',1); s.commit_job('work',1)
s.create_job('queued',[]); s.backup('one.json')
identifier=s.all_documents()[0]['document_id']
for table,field,key,value in [('control','value','key','worker_generation'),
                              ('document_control','edit_high_water','document_id',identifier),
                              ('job_control','epoch_high_water','job_id','queued')]:
    with s._write():
        s.db.execute('UPDATE '+table+' SET '+field+'=? WHERE '+key+'=?', (str(COUNTER_MAX) if table=='control' else COUNTER_MAX,value))
    before=dump(s); files={str(p):p.read_bytes() for p in Path('target').iterdir()}
    assert error(lambda: s.restore_backup('one.json',s._generation())) == 'counter_exhausted'
    assert dump(s)==before and files=={str(p):p.read_bytes() for p in Path('target').iterdir()}
    with s._write(): s.db.execute('UPDATE '+table+' SET '+field+'=? WHERE '+key+'=?',(1,value))
s.close(); print(json.dumps({'ok':True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_stale_handle_precedes_backup_io_after_valid_transport(self):
        with _application() as root:
            result = _run(root, r'''
Path('target').mkdir()
s=Store('state.sqlite',backup_dir='target'); s.adopt_backup_root(None); s.backup('one.json')
old=Store('state.sqlite',backup_dir='missing')
s.restore_backup('one.json',s._generation())
before=dump(s)
assert error(lambda: old.restore_backup('absent.json',0))=='stale_instance'
assert error(lambda: old.backup('absent.json'))=='stale_instance'
assert error(lambda: old.restore_backup('absent.json',True))=='invalid_request'
assert error(lambda: old.reindex_step(True))=='invalid_request'
assert error(lambda: old.enqueue_job([]))=='invalid_request'
with s.worker_owner():
    def acquire_old():
        with old.worker_owner(): pass
    assert error(acquire_old)=='stale_instance'
    assert error(lambda: old.adopt_backup_root(str(Path('target').resolve())))=='stale_instance'
# Worker acquisition has its own permitted generation transition.
assert s.db.execute('SELECT count(*) FROM backups').fetchone()[0]==1
old.close(); s.close(); print(json.dumps({'ok':True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_last_generation_consumed_before_claim_admission_leaves_job_queued(self):
        with _application() as root:
            result = _run(root, r'''
from contextlib import contextmanager
class Interleaved(Store):
    @contextmanager
    def active_worker_claim(self, claim, *, preflight=False):
        with self._write():
            self.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'",(str(COUNTER_MAX),))
        self.before=dump(self)
        self.files={str(p):p.read_bytes() for p in self.maintenance_root.iterdir()}
        with super().active_worker_claim(claim,preflight=preflight):
            yield
s=Interleaved('state.sqlite'); s.create_job('work',[{'source':'new.txt','text':'new'}]); s.enqueue_job('work')
with s._write(): s.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'",(str(COUNTER_MAX-1),))
with s.worker_owner() as owner:
    assert error(lambda:s.worker_process_once(owner))=='counter_exhausted'
    assert dump(s)==s.before
    assert {str(p):p.read_bytes() for p in s.maintenance_root.iterdir()}==s.files
    assert s.get_job('work')['state']=='queued'
s.close(); print(json.dumps({'ok':True}))
''')
            self.assertEqual(result, {'ok': True})

    def test_failed_job_with_matching_live_claim_runs_until_claim_is_cleared(self):
        with _application() as root:
            child = _paused(root, r'''
import time
class PausedFailure(Store):
    def _worker_error(self, owner, code):
        if code is not None:
            print('BOUNDARY',flush=True)
            until=time.monotonic()+15
            while not Path('continue').exists():
                if time.monotonic()>until: raise RuntimeError('Observer did not release worker')
                time.sleep(0.01)
        return super()._worker_error(owner,code)
s=PausedFailure('state.sqlite'); s.create_job('bad',[{'source':'bad.pdf','text':'x'}]); s.enqueue_job('bad')
with s.worker_owner() as owner:
    assert error(lambda:s.worker_process_once(owner))=='unsupported_type'
    assert s.worker_state()=='idle'
assert s.worker_state()=='stopped'
s.close(); print(json.dumps({'finished':True}))
''')
            try:
                result = _run(root, "s=Store('state.sqlite'); assert s.get_job('bad')['state']=='failed'; print(json.dumps(s.diagnostics()['worker_state'])); s.close()")
                self.assertEqual(result, 'running')
                (root/'continue').write_text('continue',encoding='utf-8')
                output, stderr = child.communicate(timeout=5)
                self.assertEqual(child.returncode,0,stderr)
                self.assertEqual(json.loads(output),{'finished':True})
            finally:
                if child.poll() is None:
                    child.kill(); child.communicate(timeout=5)
