"""Public prospective-v2 catalog development controls, isolated subprocesses."""
from __future__ import annotations
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from gossip_harness.library_m3_reference_v1 import m3_files
from gossip_harness.library_m4_reference_v1 import m4_files
from gossip_harness.library_v2_catalog_reference_v1 import catalog_files, schema3_catalog_files


class LibraryV2CatalogTests(unittest.TestCase):
    def run_script(self, body: str, *, predecessor: bool = False) -> dict:
        with tempfile.TemporaryDirectory(prefix='v2-catalog-') as tmp:
            root = Path(tmp).resolve()
            files = m3_files() if predecessor else m4_files()
            files.update(schema3_catalog_files(files) if predecessor else catalog_files(files))
            prior = schema3_catalog_files(m3_files())
            files['library/catalog/v2_schema3_legacy.py'] = prior['library/catalog/legacy_m2.py']
            files['library/catalog/v2_schema3_control.py'] = prior['library/catalog/control.py'].replace(
                'from library.catalog.legacy_m2 import Store as LegacyStore',
                'from library.catalog.v2_schema3_legacy import Store as LegacyStore')
            # Catalog-only control excludes maintenance's root adoption/liveness.
            if predecessor:
                files['library/catalog/store.py'] = 'from library.catalog.control import CoreStore as Store\n'
            else:
                files['library/catalog/store.py'] = 'from library.catalog.m4_store import NormalizedStore as Store\n'
            for name, text in files.items():
                target = root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(text)
            script = '''import json,sys,sqlite3,hashlib
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from library.catalog.store import Store
from library.common import LibraryError,identity
from library.counters import COUNTER_MAX,counter,increment
root=Path(sys.argv[1]); db=root/'catalog.sqlite'; store=Store(db)
def error(call):
    try: call()
    except LibraryError as failure: return failure.code
    raise AssertionError('expected domain failure')
def state():
    return '\\n'.join(store.db.iterdump())
''' + body
            result = subprocess.run([sys.executable,'-I','-c',script,str(root)],capture_output=True,text=True,timeout=30)
            self.assertEqual(result.returncode,0,result.stderr[-8000:])
            return json.loads(result.stdout)

    def test_source_ownership_syntax_and_predecessor(self):
        base=m4_files(); overlays=catalog_files(base)
        self.assertEqual(len(overlays),7)
        self.assertEqual(base,m4_files())
        for name, text in overlays.items(): compile(text,name,'exec')
        older=m3_files(); previous=schema3_catalog_files(older)
        for name, text in previous.items(): compile(text,name,'exec')
        with self.assertRaises(ValueError):
            catalog_files({**base,'library/catalog/m4_legacy.py':'class Store: pass\n'})

    def test_counter_exact_domain_and_maximum(self):
        self.assertEqual(self.run_script('''
values=[True,False,1.0,-1,COUNTER_MAX+1,'1']
assert all(error(lambda v=v:counter(v))=='invalid_request' for v in values)
assert counter(0)==0 and counter(COUNTER_MAX)==COUNTER_MAX
assert increment(COUNTER_MAX-1)==COUNTER_MAX
assert error(lambda:increment(COUNTER_MAX))=='counter_exhausted'
assert error(lambda:counter(0,1))=='invalid_request'
print(json.dumps({'ok':True}))
'''),{'ok':True})

    def test_exhausted_catalog_insert_leaves_no_provisional_sql(self):
        self.run_script('''
store.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'",(str(COUNTER_MAX),))
before=state(); statements=[];store.db.set_trace_callback(statements.append)
assert error(lambda:store.insert('one.txt',b'one'))=='counter_exhausted'
assert before==state()
assert not any(sql.startswith(('INSERT INTO blobs','INSERT OR IGNORE INTO blobs','INSERT INTO documents')) for sql in statements)
print('{}')
''')

    def test_document_noops_maximum_and_effective_changes_atomic(self):
        self.run_script('''
did=store.insert('one.txt',b'one')['document']['document_id']
store.db.execute('UPDATE document_state SET edit_version=?',(COUNTER_MAX,))
store.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'",(str(COUNTER_MAX),))
before=state()
assert store.refresh_document(did,COUNTER_MAX,text='one')['status']=='unchanged'
assert store.replace_annotations(did,COUNTER_MAX,'',[],[])['status']=='unchanged'
assert store.restore_document(did,COUNTER_MAX)['status']=='unchanged'
for call in [lambda:store.refresh_document(did,COUNTER_MAX,text='two'),lambda:store.replace_annotations(did,COUNTER_MAX,'note',[],[]),lambda:store.delete_document(did,COUNTER_MAX)]:
 assert error(call)=='counter_exhausted'; assert state()==before
assert error(lambda:store.delete_document(did,COUNTER_MAX-1))=='stale_version'
assert error(lambda:store.delete_document(did,COUNTER_MAX+1))=='invalid_request'
print('{}')
''')

    def test_collection_exhaustion_and_existing_noop(self):
        self.run_script('''
store.create_collection('kept',0)
store.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'",(str(COUNTER_MAX),))
before=state()
assert store.create_collection('kept',COUNTER_MAX)['status']=='unchanged'
for call in [lambda:store.create_collection('new',COUNTER_MAX),lambda:store.remove_collection('kept',COUNTER_MAX)]:
 assert error(call)=='counter_exhausted';assert state()==before
assert error(lambda:store.remove_collection('missing',COUNTER_MAX))=='not_found'
print('{}')
''')

    def test_job_transitions_tombstones_and_completed_replay(self):
        self.run_script('''
store.create_job('job',[])
store.db.execute('UPDATE jobs SET epoch=? WHERE job_id=?',(COUNTER_MAX,'job'))
assert store.start_job('job',COUNTER_MAX)=={'job_id':'job','epoch':COUNTER_MAX}
before=state();assert error(lambda:store.cancel_job('job'))=='counter_exhausted';assert before==state()
receipt=store.commit_job('job',COUNTER_MAX);assert store.commit_job('job',COUNTER_MAX)==receipt
assert error(lambda:store.cancel_job('job'))=='job_state'
store.db.execute('INSERT INTO job_control VALUES (?,?,0)',('gone',COUNTER_MAX))
before=state();assert error(lambda:store.create_job('gone',[]))=='counter_exhausted';assert before==state()
for value in [True,1.0,0,COUNTER_MAX+1]:
 assert error(lambda v=value:store.start_job('missing',v))=='invalid_request'
 assert error(lambda v=value:store.commit_job('missing',v))=='invalid_request'
print('{}')
''')

    def test_batch_preflight_has_no_partial_failure_state_or_highwater(self):
        self.run_script('''
entries=[{'source':'a.txt','text':'a'},{'source':'b.txt','text':'b'}]
store.create_job('batch',entries);store.start_job('batch',1)
store.db.execute('INSERT INTO document_control VALUES (?,?)',(identity('document','b.txt'),COUNTER_MAX))
before=state();statements=[];store.db.set_trace_callback(statements.append)
assert error(lambda:store.commit_job('batch',1))=='counter_exhausted'
assert before==state();assert store.get_job('batch')['state']=='running'
assert not any(sql.startswith('INSERT INTO documents') for sql in statements)
print('{}')
''')

    def test_semantic_hashes_preserve_strings_and_deferred_surrogate(self):
        self.run_script('''
entries=[{'source':'bad.bin','text':chr(0xd800)},{'source':'ok.txt','text':'é\\n'}]
store.create_job('hashes',entries)
row=store.db.execute('SELECT manifest,content_hashes FROM jobs').fetchone()
assert row['content_hashes']==json.dumps([None,hashlib.sha256('é\\n'.encode()).hexdigest()],ensure_ascii=True,separators=(',',':'),allow_nan=False)
escaped=' [ null , '+json.dumps(hashlib.sha256('é\\n'.encode()).hexdigest()).replace('a','\\\\u0061')+' ] '
store.db.execute('UPDATE jobs SET content_hashes=?',(escaped,));store.close();store=Store(db)
assert store.db.execute('SELECT content_hashes FROM jobs').fetchone()[0]==escaped
assert store.create_job('hashes',entries)['state']=='queued'
print('{}')
''')

    def test_open_refuses_persisted_overflow_without_mutation(self):
        self.run_script('''
store.db.execute("UPDATE control SET value=? WHERE key='worker_generation'",(str(COUNTER_MAX+1),));store.close()
before=db.read_bytes();assert error(lambda:Store(db))=='invalid_database';assert db.read_bytes()==before
print('{}')
''')

    def test_normal_open_preserves_diagnostic_noop_migrate_clears_matching_only(self):
        self.run_script('''
store.record_error('migrate','io_error');store.close();store=Store(db)
assert json.loads(store._control('last_error'))=={'operation':'migrate','code':'io_error'}
inc=store._control('incarnation');assert store.migrate()['migrated'] is False
assert store._control('last_error')=='null' and store._control('incarnation')==inc
store.record_error('backup','io_error');store.migrate()
assert json.loads(store._control('last_error'))['operation']=='backup'
assert store.db.execute("SELECT 1 FROM control WHERE key='backup_root'").fetchone() is None
print('{}')
''')

    def test_stale_instance_precedes_missing_schema_sql_but_not_bad_token(self):
        self.run_script('''
store.db.execute("UPDATE control SET value='replacement' WHERE key='incarnation'")
store.db.execute('DROP TABLE document_state')
assert error(lambda:store.replace_annotations('missing',1,'',[],[]))=='stale_instance'
assert error(lambda:store.replace_annotations('missing',COUNTER_MAX+1,'',[],[]))=='invalid_request'
assert error(lambda:store.start_job('job',COUNTER_MAX+1))=='invalid_request'
assert error(lambda:store.start_job('job',1))=='stale_instance'
print('{}')
''')


    def test_schema3_counter_paths_and_invalid_open(self):
        self.run_script("""
did=store.insert('one.txt',b'one')['document']['document_id']
store.db.execute('UPDATE lifecycle SET edit_version=?',(COUNTER_MAX,))
before=state()
assert error(lambda:store.refresh_document(did,COUNTER_MAX,text='two'))=='counter_exhausted'
assert state()==before
store.db.execute('INSERT INTO document_control VALUES (?,?)',(identity('document','gone.txt'),COUNTER_MAX))
before=state();assert error(lambda:store.insert('gone.txt',b'gone'))=='counter_exhausted';assert state()==before
store.db.execute("UPDATE control SET value=? WHERE key='worker_generation'",(str(COUNTER_MAX+1),));store.close()
before=db.read_bytes();assert error(lambda:Store(db))=='invalid_database';assert before==db.read_bytes()
print('{}')
""",predecessor=True)

    def test_prepare_keeps_failed_state_and_fences_old_layout(self):
        self.run_script("""
from library.ingestion.jobs import JobManager
manager=JobManager(store)
manager.submit('bad',[{'source':'bad.bin','text':'x'}])
assert error(lambda:manager.prepare('bad'))=='unsupported_type'
assert store.get_job('bad')['state']=='failed'
manager.submit('good',[{'source':'ok.txt','text':'x'}])
store.db.execute("UPDATE control SET value='replacement' WHERE key='incarnation'")
store.db.execute('DROP TABLE document_state')
assert error(lambda:manager.prepare('good'))=='stale_instance'
assert error(lambda:manager.prepare([]))=='invalid_request'
assert error(lambda:manager.commit({'job_id':'good','epoch':COUNTER_MAX+1}))=='invalid_request'
print('{}')
""")


    def test_actual_schema3_migration_preserves_controls_and_fences_held_handle(self):
        self.run_script("""
from library.catalog.v2_schema3_control import CoreStore as Predecessor
from library.ingestion.jobs import JobManager
store.close();db.unlink();old=Predecessor(db)
did=old.insert('one.txt',b'one')['document']['document_id']
old.create_job('queued',[{'source':'two.txt','text':'two'}])
old.db.execute("UPDATE control SET value=? WHERE key='worker_generation'",(str(COUNTER_MAX),))
old.db.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'",(str(COUNTER_MAX),))
old.db.execute('INSERT INTO document_control VALUES (?,?)',(identity('document','gone.txt'),COUNTER_MAX))
old.db.execute('INSERT INTO job_control VALUES (?,?,0)',('gone',COUNTER_MAX))
old.record_error('migrate','io_error')
old.db.execute('INSERT INTO control VALUES (?,?)',('backup_root',json.dumps(str(root/'backups'))))
inc=old._control('incarnation')
old.db.execute('INSERT INTO maintenance_artifacts VALUES (?,?,?,?,?,?,?)',('prior','backup_stage','backup','stage.json',inc,COUNTER_MAX,'staging'))
before={name:[tuple(r) for r in old.db.execute('SELECT * FROM '+name+' ORDER BY 1')] for name in ['job_control','document_control','maintenance_artifacts','backups','jobs']}
store=Store(db)
assert store._pending_migration['migrated'] is True
assert store.migrate()['migrated'] is False
assert store._control('incarnation') != inc and store._control('last_error')=='null'
assert store._control('worker_generation')==str(COUNTER_MAX) and store._generation()==COUNTER_MAX
assert store._control('backup_root')==json.dumps(str(root/'backups'))
for name,rows in before.items():assert [tuple(r) for r in store.db.execute('SELECT * FROM '+name+' ORDER BY 1')]==rows
assert error(lambda:old.replace_annotations(did,1,'later',[],[]))=='stale_instance'
assert error(lambda:JobManager(old).prepare('queued'))=='stale_instance'
assert error(lambda:old.replace_annotations([],1,'later',[],[]))=='invalid_request'
assert error(lambda:old.insert([],b'bad'))=='invalid_source'
old.close();print('{}')
""")

    def test_failed_schema3_activation_rolls_back_incarnation_and_diagnostic(self):
        self.run_script("""
from library.catalog.v2_schema3_control import CoreStore as Predecessor
store.close();db.unlink();old=Predecessor(db)
old.insert('one.txt',b'one');old.record_error('migrate','io_error')
before='\\n'.join(old.db.iterdump());inc=old._control('incarnation')
class Failing(Store):
 def _migration_checkpoint(self,phase):
  if phase=='before_commit':raise LibraryError('injected_failure')
assert error(lambda:Failing(db))=='injected_failure'
assert old._control('incarnation')==inc
assert '\\n'.join(old.db.iterdump())==before
assert old.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0]=='3'
old.close();print('{}')
""")


    def test_held_store_migrate_reports_current_schema_and_current_counts(self):
        self.run_script("""
from library.catalog.v2_schema3_control import CoreStore as Predecessor
store.close();db.unlink();old=Predecessor(db);old.close()
store=Store(db)
assert store._pending_migration=={'from_schema':3,'to_schema':4,'migrated':True,'documents':0,'jobs':0}
store.insert('created-after-open.txt',b'new')
store.create_job('created-after-open',[])
store.record_error('migrate','io_error')
result=store.migrate()
assert result=={'from_schema':4,'to_schema':4,'migrated':False,'documents':1,'jobs':1}
assert store._control('last_error')=='null'
assert store.migrate()==result
print('{}')
""")
