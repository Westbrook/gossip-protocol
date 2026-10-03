"""Bounded authored-reference checks; not independent product acceptance."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m2_catalog_reference_v1 import catalog_files
from gossip_harness.library_m2_reference_v1 import m2_files
from gossip_harness.library_m3_control_reference_v1 import control_files


def _authored(script):
    with tempfile.TemporaryDirectory(prefix="trusted-m3-control-") as directory:
        root = Path(directory)
        files = m2_files() | control_files()
        files['library/catalog/store.py'] = 'from library.catalog.control import CoreStore as Store\n'
        for name, source in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding='utf-8')
        bootstrap = (
            'import json, resource, sys\n'
            'resource.setrlimit(resource.RLIMIT_CPU,(12,12))\n'
            'resource.setrlimit(resource.RLIMIT_FSIZE,(67108864,67108864))\n'
            'resource.setrlimit(resource.RLIMIT_NOFILE,(128,128))\n'
            'sys.path.insert(0,sys.argv[1])\n'
            'from library.catalog.store import Store\n'
            'from library.common import LibraryError\n'
            'def error(call):\n'
            ' try: return call()\n'
            " except LibraryError as exc: return {'error':exc.code}\n"
        )
        process = subprocess.run([sys.executable,'-I','-c',bootstrap+script,str(root)],cwd=root,
                                 capture_output=True,text=True,timeout=20,check=False)
        if process.returncode or len(process.stdout.encode())>65536:
            raise AssertionError('Authored M3 control check failed: '+process.stderr[:5000])
        return json.loads(process.stdout)


class LibraryM3ControlReferenceTests(unittest.TestCase):
    def test_checked_migration_derivative_preserves_all_other_m2_source(self):
        files = control_files()
        original = catalog_files()['library/catalog/store.py']
        restored = files['library/catalog/legacy_m2.py'].replace(
            "if schema not in ('0', '2', '3'):","if schema not in ('0', '2'):").replace(
            '                self._initialize_control()\n','')
        self.assertEqual(restored,original)
        for path,source in files.items():
            compile(source,path,'exec')

    def test_nested_atomic_generation_high_water_and_failure_rollback(self):
        result = _authored(r'''
s = Store('catalog.db')
with s._write():
 a=s.insert('a.txt',b'A')['document']['document_id']
 s.insert('b.txt',b'B')
 try:
  with s._write():
   s.insert('rolled.txt',b'R')
   raise LibraryError('injected_failure')
 except LibraryError: pass
before=s._generation()
s.refresh_document(a,1,text='A2')
s.create_job('job',[{'source':'job.txt','text':'J'}])
s.cancel_job('job'); s.retry_job('job')
print(json.dumps({'schema':s.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0],
 'before':before,'generation':s._generation(),'sources':[d['source'] for d in s.documents()],
 'job':dict(s.db.execute('SELECT * FROM job_control').fetchone()),
 'document':s.db.execute('SELECT edit_high_water FROM document_control WHERE document_id=?',(a,)).fetchone()[0],
 'backup_exists':s.backup_dir.exists()}))
''')
        self.assertEqual(result,{'schema':'3','before':1,'generation':2,'sources':['a.txt','b.txt'],
            'job':{'job_id':'job','epoch_high_water':3,'enrolled':0},'document':2,'backup_exists':False})

    def test_owner_lock_real_process_and_guard_precedence(self):
        result = _authored(r'''
import subprocess
s=Store('catalog.db'); s.create_job('j',[])
with s.worker_owner() as owner:
 child="import sys;sys.path.insert(0,sys.argv[1]);from library.catalog.store import Store;from library.common import LibraryError; s=Store('catalog.db')\ntry:\n with s.worker_owner(): print('wrong')\nexcept LibraryError as e: print(e.code)"
 p=subprocess.run([sys.executable,'-I','-c',child,sys.argv[1]],capture_output=True,text=True,timeout=4,check=True)
 def guard(claim):
  with s.worker_guard(claim): return 'guarded'
 claim=owner|{'job_id':'j','epoch':1}
 good=guard(claim)
 s.cancel_job('j')
 stale=error(lambda:guard(claim))
 wrong=error(lambda:guard(claim|{'generation':owner['generation']-1}))
 def exclusive():
  with s.maintenance_authority(): return 'wrong'
 busy=error(exclusive)
 generation=int(s._control('worker_generation'))
print(json.dumps({'child':p.stdout.strip(),'good':good,'stale':stale,'wrong':wrong,'busy':busy,'generation':generation,'state':s.worker_state()}))
''')
        self.assertEqual(result,{'child':'worker_busy','good':'guarded','stale':{'error':'stale_epoch'},
            'wrong':{'error':'stale_worker'},'busy':{'error':'maintenance_busy'},'generation':1,'state':'stopped'})

    def test_restore_fences_stale_handles_and_retains_removed_id_maxima(self):
        result = _authored(r'''
s=Store('catalog.db'); old=Store('catalog.db')
a=s.insert('a.txt',b'first')['document']['document_id']
s.refresh_document(a,1,text='second')
s.create_job('removed',[]);s.cancel_job('removed')
with s.maintenance_authority(),s._write():
 s.db.execute('DELETE FROM lifecycle');s.db.execute('DELETE FROM revisions');s.db.execute('DELETE FROM documents')
 s.db.execute('DELETE FROM jobs')
 s._set_control('incarnation','replacement-incarnation')
s.adopt_incarnation()
stale=error(lambda:old.insert('wrong.txt',b'W'))
s.insert('a.txt',b'reborn');job=s.create_job('removed',[])
print(json.dumps({'stale':stale,'edit':s.lifecycle_show(a)['edit_version'],'job_epoch':job['epoch'],
 'job_high':s.db.execute('SELECT epoch_high_water FROM job_control').fetchone()[0]}))
''')
        self.assertEqual(result,{'stale':{'error':'stale_worker'},'edit':3,'job_epoch':3,'job_high':3})

    def test_cleanup_bounded_cursor_liveness_and_unknown_files(self):
        result = _authored(r'''
s=Store('catalog.db')
unknown=s.maintenance_root/'unknown';unknown.write_text('retain')
for n in range(70):
 aid='a'+str(n).zfill(3)
 with s.artifact_lock(aid):
  s.register_artifact('restore_stage','maintenance',aid,artifact_id=aid)
  (s.maintenance_root/aid).write_text('stage')
with s.artifact_lock('a000'):
 first=s.cleanup_artifacts()
 held=(s.maintenance_root/'a000').exists()
second=s.cleanup_artifacts();third=s.cleanup_artifacts()
print(json.dumps({'first':first,'second':second,'third':third,'held':held,'unknown':unknown.read_text(),
 'remaining':s.db.execute('SELECT count(*) FROM maintenance_artifacts').fetchone()[0]}))
''')
        self.assertEqual(result['first'],{'scanned':64,'removed':63,'cursor':'a063'})
        self.assertEqual(result['second'],{'scanned':6,'removed':6,'cursor':'a069'})
        self.assertEqual(result['third'],{'scanned':1,'removed':1,'cursor':'a000'})
        self.assertTrue(result['held'])
        self.assertEqual(result['unknown'],'retain')
        self.assertEqual(result['remaining'],0)

    def test_cleanup_preserves_live_index_pointers_and_scoped_errors(self):
        result = _authored(r'''
s=Store('catalog.db')
for n in range(3):
 s.register_artifact('index_generation','maintenance',str(n),artifact_id='index'+str(n))
 with s._write():s.db.execute('INSERT INTO search_entries VALUES (?,?,?,?)',(n,'id','source','text'))
with s._write():s.db.execute('UPDATE search_state SET published_generation=1,target_generation=2')
s.cleanup_artifacts()
s.record_error('backup','already_exists');s.record_error('reindex',None)
retained=json.loads(s._control('last_error'));s.record_error('backup',None)
print(json.dumps({'generations':[r[0] for r in s.db.execute('SELECT generation FROM search_entries ORDER BY generation')],
 'retained':retained,'cleared':json.loads(s._control('last_error'))}))
''')
        self.assertEqual(result,{'generations':[1,2],'retained':{'operation':'backup','code':'already_exists'},'cleared':None})

    def test_migration_failure_rolls_back_schema_and_control_tables(self):
        result = _authored(r'''
from library.catalog.legacy_m2 import Store as M2
from library.catalog.control import CoreStore
M2._initialize_control=lambda self:None
legacy=M2('catalog.db');legacy.insert('old.txt',b'old');legacy.close()
class Broken(CoreStore):
 def _initialize_control(self):
  super()._initialize_control()
  raise LibraryError('injected_failure')
failed=error(lambda:Broken('catalog.db'))
import sqlite3
raw=sqlite3.connect('catalog.db')
schema=raw.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0]
control=raw.execute("SELECT count(*) FROM sqlite_master WHERE name='control'").fetchone()[0]
raw.close()
s=Store('catalog.db')
print(json.dumps({'failed':failed,'schema_after_failure':schema,'control_after_failure':control,
 'upgraded':s.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0],
 'documents':[d['source'] for d in s.documents()],'generation':s._generation()}))
''')
        self.assertEqual(result,{'failed':{'error':'injected_failure'},'schema_after_failure':'2',
            'control_after_failure':0,'upgraded':'3','documents':['old.txt'],'generation':1})

    def test_diagnostics_probe_is_read_only_and_maintenance_is_not_worker(self):
        result = _authored(r'''
s=Store('catalog.db'); other=Store('catalog.db')
before=sorted(p.name for p in s.maintenance_root.iterdir())
initial=s.worker_state()
after=sorted(p.name for p in s.maintenance_root.iterdir())
with s.maintenance_authority():
 maintenance=other.worker_state()
with s.worker_owner():
 running=other.worker_state()
print(json.dumps({'unchanged':before==after,'initial':initial,'maintenance':maintenance,'running':running}))
''')
        self.assertEqual(result,{'unchanged':True,'initial':'stopped','maintenance':'stopped','running':'running'})

    def test_custom_root_binding_blocks_wrong_recovery_then_allows_default(self):
        result = _authored(r'''
from pathlib import Path
custom=Path('custom');custom.mkdir()
default=Path('backups');default.mkdir()
s=Store('catalog.db',backup_dir=custom)
with s.artifact_lock('owned'):
 s.register_artifact('restore_stage','backup','.owned',artifact_id='owned')
 (custom/'.owned').write_text('owned')
(default/'.owned').write_text('unowned same name')
s.close()
wrong=error(lambda:Store('catalog.db'))
s=Store('catalog.db',backup_dir=custom)
retained_before=(custom/'.owned').exists()
with s.worker_owner():pass
cleaned=not (custom/'.owned').exists()
s.configure_backup_dir(None)
root=s._control('backup_root');s.close()
reopened=Store('catalog.db')
print(json.dumps({'wrong':wrong,'no_constructor_cleanup':retained_before,'cleaned':cleaned,
 'unowned':(default/'.owned').read_text(),'default_selected':root==str(default.resolve()),
 'reopened_default':reopened.backup_dir==default.resolve()}))
''')
        self.assertEqual(result,{'wrong':{'error':'maintenance_busy'},'no_constructor_cleanup':True,
            'cleaned':True,'unowned':'unowned same name','default_selected':True,'reopened_default':True})

    def test_root_authority_blocks_reassignment_and_preserves_completed_files(self):
        result = _authored(r'''
from pathlib import Path
first=Path('first');second=Path('second');first.mkdir();second.mkdir()
s=Store('catalog.db',backup_dir=first);other=Store('catalog.db',backup_dir=first)
(first/'published.json').write_text('preserved')
with s._write():s.db.execute("INSERT INTO backups VALUES ('published.json',9,'digest',0)")
with s.backup_directory_authority():
 busy=error(lambda:other.configure_backup_dir(second))
 same=Store('catalog.db',backup_dir=first);same.close()
other.configure_backup_dir(second)
def stale_operation():
 with s.backup_directory_authority():return 'wrong'
stale=error(stale_operation)
print(json.dumps({'busy':busy,'stale':stale,'preserved':(first/'published.json').read_text(),
 'registry':other.db.execute('SELECT count(*) FROM backups').fetchone()[0]}))
''')
        self.assertEqual(result,{'busy':{'error':'maintenance_busy'},'stale':{'error':'maintenance_busy'},
            'preserved':'preserved','registry':0})

    def test_semantically_equal_manifest_serialization_replays_without_rewriting(self):
        result = _authored(r'''
s=Store('catalog.db')
entries=[{'source':'unicode.txt','text':'caf\u00e9'}]
original=s.create_job('same',entries)
alternate=json.dumps(entries,ensure_ascii=False,indent=2)
with s._write():s.db.execute('UPDATE jobs SET manifest=? WHERE job_id=?',(alternate,'same'))
replay=s.create_job('same',entries)
conflict=error(lambda:s.create_job('same',[{'source':'unicode.txt','text':'changed'}]))
print(json.dumps({'replay':replay==original,'unchanged':s.db.execute('SELECT manifest FROM jobs').fetchone()[0]==alternate,
 'conflict':conflict}))
''')
        self.assertEqual(result,{'replay':True,'unchanged':True,'conflict':{'error':'job_conflict'}})

    def test_owner_startup_cleanup_io_failure_records_guarded_error(self):
        result = _authored(r'''
s=Store('catalog.db')
s.create_job('not_started',[])
def fail_cleanup(limit=64):raise OSError('authored startup failure')
s.cleanup_artifacts=fail_cleanup
def start():
 with s.worker_owner():return 'wrong'
failed=error(start)
print(json.dumps({'failed':failed,'generation':int(s._control('worker_generation')),
 'error':json.loads(s._control('last_error')),'state':s.worker_state(),'job':s.get_job('not_started')['state']}))
''')
        self.assertEqual(result,{'failed':{'error':'io_error'},'generation':1,
            'error':{'operation':'worker','code':'io_error'},'state':'stopped','job':'queued'})
