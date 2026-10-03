"""Authored M4 storage qualification, separate from independent frozen fixtures."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.library_m1_reference_v1 import m1_files
from gossip_harness.library_m2_reference_v1 import m2_files
from gossip_harness.library_m3_reference_v1 import m3_files
from gossip_harness.library_m4_catalog_reference_v1 import catalog_files
from gossip_harness.library_project_fixture_v1 import seed_files


def _install(root):
    releases = {'m0':seed_files(),'m1':m1_files(),'m2':m2_files(),'m3':m3_files()}
    current = m3_files() | catalog_files()
    current['library/catalog/store.py'] = (
        'from library.catalog.maintenance import MaintenanceMixin\n'
        'from library.catalog.worker import WorkerMixin\n'
        'from library.catalog.m4_store import NormalizedStore\n'
        'class Store(MaintenanceMixin, WorkerMixin, NormalizedStore):\n    pass\n')
    releases['m4'] = current
    for release,files in releases.items():
        for path,source in files.items():
            target = root / release / path
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text(source,encoding='utf-8')


def _invoke(root, script, *, release='m4', expected_exit=0):
    bootstrap = (
        'import hashlib,json,os,resource,sqlite3,sys\n'
        'from pathlib import Path\n'
        'resource.setrlimit(resource.RLIMIT_CPU,(15,15))\n'
        'resource.setrlimit(resource.RLIMIT_FSIZE,(134217728,134217728))\n'
        'resource.setrlimit(resource.RLIMIT_NOFILE,(128,128))\n'
        'sys.path.insert(0,sys.argv[1])\n'
        'from library.catalog.store import Store\n'
        'from library.common import LibraryError\n'
        'def error(call):\n'
        ' try:return call()\n'
        " except LibraryError as exc:return {'error':exc.code}\n")
    process = subprocess.run([sys.executable,'-I','-c',bootstrap+script,str(root/release)],
                             cwd=root,capture_output=True,text=True,timeout=25,check=False)
    sequence = len(list(root.glob('process-*.json')))
    (root/f'process-{sequence:03d}.json').write_text(json.dumps({
        'release':release,'expected_exit':expected_exit,'exit':process.returncode,
        'stdout':process.stdout,'stderr':process.stderr},indent=2)+'\n')
    if process.returncode != expected_exit or len(process.stdout.encode()) > 65536:
        raise AssertionError(f'Authored M4 process exit {process.returncode}: {process.stderr[:8000]}')
    return json.loads(process.stdout) if expected_exit == 0 else None


class LibraryM4CatalogReferenceTests(unittest.TestCase):
    def test_generated_sources_compile_and_old_generators_stay_distinct(self):
        files = catalog_files()
        self.assertNotIn('library/catalog/legacy_m2.py',files)
        self.assertNotIn('library/catalog/control.py',files)
        for path,source in files.items():
            self.assertTrue(source)
            compile(source,path,'exec')
        self.assertNotIn('FROM lifecycle',files['library/catalog/maintenance.py'])
        self.assertNotIn('FROM revisions',files['library/catalog/m4_legacy.py'])

    def test_native_normalized_graph_legacy_projections_and_bounded_v4_export(self):
        with ArtifactDirectory('m4-normalized-catalog') as artifact:
            root = artifact.root.resolve(); _install(root)
            result = _invoke(root,r'''
s=Store('catalog.sqlite')
initial=s.migrate()
a=s.insert('a.txt',b'A')['document']['document_id'];b=s.insert('b.txt',b'A')['document']['document_id']
s.refresh_document(a,1,text='A2');s.create_collection('Group',s._generation())
s.replace_annotations(a,2,'Notes',['TAG'],['group']);s.delete_document(b,1)
from library.catalog.m4_store import revision_id
record=s.show_v1(a);legacy=s.lifecycle_show(a);history=s.revisions_v1(a)
selected=s.revisions_v1(a,history['revisions'][0]['revision_id'])
foreign=error(lambda:s.revisions_v1(b,history['revisions'][0]['revision_id']))
export=s.export_v1([a],include_history=True)
from library.catalog.maintenance import canonical_export_bytes
size=len(canonical_export_bytes(export));exact=s.export_v1([a],include_history=True,max_bytes=size)
small=error(lambda:s.export_v1([a],include_history=True,max_bytes=size-1))
s.reindex_step();diagnostics=s.diagnostics()
tables=[row[0] for row in s.db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
heads=[dict(row) for row in s.db.execute('SELECT s.document_id,s.head_revision_id,r.revision,r.blob_id,d.blob_id AS mirror '
 'FROM document_state s JOIN document_revisions r ON r.revision_id=s.head_revision_id JOIN documents d ON d.document_id=s.document_id')]
checks=[row['head_revision_id']==revision_id(row['document_id'],row['revision'],row['blob_id']) and row['blob_id']==row['mirror'] for row in heads]
print(json.dumps({'initial':initial,'record':record,'legacy':legacy,'history':history,'selected':selected,'foreign':foreign,
 'format':export['format'],'exact':exact==export,'small':small,'tables':tables,'heads':checks,'diagnostics':diagnostics,
 'legacy_sources':[doc['source'] for doc in s.documents()]}))
''')
            self.assertEqual(result['initial'],{'from_schema':4,'to_schema':4,'migrated':False,'documents':0,'jobs':0})
            self.assertEqual(result['record']['current_revision']['text'],'A2')
            self.assertEqual(result['record']['current_revision']['revision'],2)
            self.assertNotIn('document',result['record'])
            self.assertNotIn('revision_id',result['legacy'])
            self.assertEqual(result['selected']['text'],'A')
            self.assertEqual(result['foreign'],{'error':'not_found'})
            self.assertEqual(result['format'],'local-research-library-export-v4')
            self.assertTrue(result['exact']);self.assertEqual(result['small'],{'error':'too_large'})
            self.assertNotIn('lifecycle',result['tables']);self.assertNotIn('revisions',result['tables'])
            self.assertEqual(result['heads'],[True,True])
            self.assertEqual(result['diagnostics']['schema'],4)
            self.assertEqual(result['diagnostics']['index_state'],'current')
            self.assertEqual(result['legacy_sources'],['a.txt'])

    def test_authored_schema0_without_jobs_and_m1_jobs_migrate_without_execution(self):
        for release in ('m0','m1'):
            with self.subTest(release=release), ArtifactDirectory('m4-'+release+'-migration') as artifact:
                root = artifact.root.resolve();_install(root)
                setup = r'''
s=Store('catalog.sqlite');s.insert('empty.txt',b'');s.insert('same.txt',b'')
'''
                if release == 'm1':
                    setup += r'''
s.create_job('done',[{'source':'done.txt','text':'original'}]);s.start_job('done',1);s.commit_job('done',1)
s.create_job('queued',[{'source':'later.txt','text':'future'}])
'''
                setup += "print(json.dumps({'documents':s.documents()}));s.close()\n"
                before = _invoke(root,setup,release=release)
                after = _invoke(root,r'''
s=Store('catalog.sqlite');first=s.migrate();second=s.migrate()
print(json.dumps({'first':first,'second':second,'documents':s.documents(),'jobs':s.list_jobs(),
 'record_versions':[record['edit_version'] for record in s.lifecycle_list(deleted='all')['records']],
 'enrolled':[row[0] for row in s.db.execute('SELECT enrolled FROM job_control')]}))
''')
                self.assertEqual(after['documents'],before['documents'])
                self.assertEqual(after['first']['from_schema'],0);self.assertTrue(after['first']['migrated'])
                self.assertEqual(after['second']['from_schema'],4);self.assertFalse(after['second']['migrated'])
                self.assertEqual(after['record_versions'],[1]*len(before['documents']))
                self.assertTrue(all(value == 0 for value in after['enrolled']))
                if release == 'm1':
                    self.assertEqual([job['state'] for job in after['jobs']],['completed','queued'])

    def test_schema2_and_schema3_keep_graph_raw_jobs_generation_and_tokens(self):
        for release in ('m2','m3'):
            with self.subTest(release=release), ArtifactDirectory('m4-'+release+'-migration') as artifact:
                root=artifact.root.resolve();_install(root)
                before=_invoke(root,r'''
s=Store('catalog.sqlite');s.create_job('done',[{'source':'a.txt','text':'historical'}]);s.start_job('done',1)
receipt=s.commit_job('done',1);a=receipt['documents'][0]['document_id']
s.refresh_document(a,1,text='current');s.create_collection('group',s._generation())
s.replace_annotations(a,2,'annotation',['tag'],['group']);s.delete_document(a,3)
s.create_job('pending',[{'source':'pending.txt','text':'later'}]);s.start_job('pending',1)
s.create_job('invalid',[{'source':'invalid.txt','text':'\ud800'}])
# Schema3 restore explicitly preserves semantically valid serialized strings.
if s.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0]=='3':
 with s._write():
  row=s.db.execute("SELECT manifest FROM jobs WHERE job_id='done'").fetchone()
  s.db.execute("UPDATE jobs SET manifest=? WHERE job_id='done'",(json.dumps(json.loads(row[0]),indent=2),))
print(json.dumps({'listing':s.lifecycle_list(deleted='all'),'history':s.revision_history(a),
 'jobs':[dict(row) for row in s.db.execute('SELECT * FROM jobs ORDER BY job_id')],
 'incarnation':s._control('incarnation') if hasattr(s,'_control') else None}));s.close()
''',release=release)
                after=_invoke(root,r'''
s=Store('catalog.sqlite');listing=s.lifecycle_list(deleted='all');a=listing['records'][0]['document']['document_id']
first=s.migrate();before=s.db.total_changes;second=s.migrate();changes=s.db.total_changes-before
print(json.dumps({'first':first,'second':second,'listing':listing,'history':s.revision_history(a),
 'jobs':[dict(row) for row in s.db.execute('SELECT * FROM jobs ORDER BY job_id')],
 'incarnation':s._control('incarnation'),'changes':changes,
 'replay':s.commit_job('done',1),'foreign_keys':list(s.db.execute('PRAGMA foreign_key_check'))}));s.close()
''')
                self.assertEqual(after['listing'],before['listing'])
                self.assertEqual(after['history'],before['history'])
                self.assertEqual(after['jobs'],before['jobs'])
                self.assertEqual(after['first']['from_schema'],int(release[1:]))
                self.assertTrue(after['first']['migrated']);self.assertEqual(after['changes'],0)
                self.assertEqual(after['foreign_keys'],[])
                self.assertEqual(after['replay']['documents'][0]['text'],'historical')
                if release == 'm3':self.assertNotEqual(after['incarnation'],before['incarnation'])

    def test_corruption_and_unknown_version_preserve_existing_file(self):
        mutations = {
            'unknown':("UPDATE metadata SET value='999' WHERE key='schema'",'unsupported_schema'),
            'missing_jobs':('DROP TABLE jobs','invalid_database'),
            'orphan_state':("DELETE FROM lifecycle",'invalid_database'),
            'bad_head':("UPDATE lifecycle SET current_revision=2",'invalid_database'),
            'bad_blob':("UPDATE blobs SET content=x'78'",'invalid_database'),
            'extra_table':('CREATE TABLE hidden_data (value TEXT)','invalid_database'),
        }
        for name,(sql,code) in mutations.items():
            with self.subTest(name=name), ArtifactDirectory('m4-corrupt-'+name) as artifact:
                root=artifact.root.resolve();_install(root)
                _invoke(root,"s=Store('catalog.sqlite');s.insert('a.txt',b'A');s.close();print('{}')",release='m2')
                import sqlite3
                connection=sqlite3.connect(root/'catalog.sqlite');connection.execute(sql);connection.commit();connection.close()
                before=(root/'catalog.sqlite').read_bytes()
                result=_invoke(root,"print(json.dumps(error(lambda:Store('catalog.sqlite'))))")
                self.assertEqual(result,{'error':code})
                self.assertEqual((root/'catalog.sqlite').read_bytes(),before)

    def test_real_process_exit_before_and_after_migration_commit(self):
        for phase in ('before_commit','after_commit'):
            with self.subTest(phase=phase), ArtifactDirectory('m4-crash-'+phase,retain_success=True) as artifact:
                root=artifact.root.resolve();_install(root)
                before=_invoke(root,r'''
s=Store('catalog.sqlite');a=s.insert('a.txt',b'old')['document']['document_id'];s.refresh_document(a,1,text='new')
s.create_job('queued',[{'source':'pending.txt','text':'later'}])
print(json.dumps({'listing':s.lifecycle_list(deleted='all'),'jobs':s.list_jobs()}));s.close()
''',release='m2')
                _invoke(root, "class Crashing(Store):\n def _migration_checkpoint(self,phase):\n  if phase=="+repr(phase)+":os._exit(73)\nCrashing('catalog.sqlite')\n",expected_exit=73)
                if phase == 'before_commit':
                    restored=_invoke(root,r'''
s=Store('catalog.sqlite');print(json.dumps({'listing':s.lifecycle_list(deleted='all'),'jobs':s.list_jobs(),
 'schema':s.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0]}));s.close()
''',release='m2')
                    self.assertEqual(restored['schema'],'2')
                    self.assertEqual(restored['listing'],before['listing']);self.assertEqual(restored['jobs'],before['jobs'])
                after=_invoke(root,r'''
s=Store('catalog.sqlite');print(json.dumps({'listing':s.lifecycle_list(deleted='all'),'jobs':s.list_jobs(),'migration':s.migrate(),
 'tables':[row[0] for row in s.db.execute("SELECT name FROM sqlite_master WHERE type='table'")]}));s.close()
''')
                self.assertEqual(after['listing'],before['listing']);self.assertEqual(after['jobs'],before['jobs'])
                self.assertEqual(after['migration']['from_schema'],2 if phase=='before_commit' else 4)
                self.assertNotIn('lifecycle',after['tables']);self.assertIn('document_state',after['tables'])

    def test_m3_live_owner_blocks_migration_and_open_handle_is_fenced_after(self):
        with ArtifactDirectory('m4-m3-authority-fence') as artifact:
            root=artifact.root.resolve();_install(root)
            result=_invoke(root,r'''
from library.catalog.control import CoreStore as M3Store
old=M3Store('catalog.sqlite');old.insert('old.txt',b'old')
with old.worker_owner():
 before=Path('catalog.sqlite').read_bytes()
 busy=error(lambda:Store('catalog.sqlite'))
 unchanged=before==Path('catalog.sqlite').read_bytes()
new=Store('catalog.sqlite')
stale=error(lambda:old.insert('wrong.txt',b'wrong'))
new.create_job('queued',[])
with new.worker_owner():
 concurrent=Store('catalog.sqlite')
 explicit=error(concurrent.migrate)
 def claim():
  with concurrent.worker_owner():return 'wrong'
 worker=error(claim);concurrent.close()
print(json.dumps({'busy':busy,'unchanged':unchanged,'stale':stale,'explicit':explicit,'worker':worker,
 'sources':[doc['source'] for doc in new.documents()]}));old.close();new.close()
''')
            self.assertEqual(result,{'busy':{'error':'maintenance_busy'},'unchanged':True,
                'stale':{'error':'stale_worker'},'explicit':{'error':'maintenance_busy'},
                'worker':{'error':'worker_busy'},'sources':['old.txt']})

    def test_schema4_reopen_is_read_only_and_corrupt_revision_identity_rejected(self):
        with ArtifactDirectory('m4-reopen-validation') as artifact:
            root=artifact.root.resolve();_install(root)
            result=_invoke(root,r'''
s=Store('catalog.sqlite');s.insert('a.txt',b'A');s.close()
before=Path('catalog.sqlite').read_bytes();s=Store('catalog.sqlite');changes=s.db.total_changes;result=s.migrate();s.close()
unchanged=before==Path('catalog.sqlite').read_bytes()
raw=sqlite3.connect('catalog.sqlite');raw.execute('PRAGMA foreign_keys=OFF')
raw.execute("UPDATE document_revisions SET revision_id='rev-wrong'");raw.execute("UPDATE document_state SET head_revision_id='rev-wrong'")
raw.commit();raw.close();broken=Path('catalog.sqlite').read_bytes()
failure=error(lambda:Store('catalog.sqlite'))
print(json.dumps({'changes':changes,'unchanged':unchanged,'result':result,'failure':failure,
 'corrupt_unchanged':broken==Path('catalog.sqlite').read_bytes()}))
''')
            self.assertEqual(result['changes'],0);self.assertTrue(result['unchanged'])
            self.assertFalse(result['result']['migrated']);self.assertEqual(result['result']['from_schema'],4)
            self.assertEqual(result['failure'],{'error':'invalid_database'});self.assertTrue(result['corrupt_unchanged'])

    def test_direct_retry_recovers_hot_journal_under_exclusive_authority(self):
        with ArtifactDirectory('m4-hot-journal-retry',retain_success=True) as artifact:
            root=artifact.root.resolve();_install(root)
            _invoke(root,"s=Store('catalog.sqlite');s.insert('a.txt',b'A');s.close();print('{}')",release='m2')
            _invoke(root,r'''
class Crashing(Store):
 def _migration_checkpoint(self,phase):
  if phase=='before_commit':
   # Force dirty pages out of SQLite's cache so this is a genuine hot
   # rollback journal, not merely an abandoned in-memory transaction.
   self.db.execute('PRAGMA cache_size=1')
   self.db.execute("INSERT INTO metadata VALUES ('uncommitted_spill',?)",('x'*100000,))
   os._exit(73)
Crashing('catalog.sqlite')
''',expected_exit=73)
            import sqlite3
            connection=sqlite3.connect((root/'catalog.sqlite').as_uri()+'?mode=ro',uri=True)
            try:
                with self.assertRaises(sqlite3.OperationalError) as failure:
                    connection.execute('SELECT * FROM sqlite_master').fetchall()
                self.assertEqual(failure.exception.sqlite_errorcode,sqlite3.SQLITE_READONLY_ROLLBACK)
            finally:
                connection.close()
            result=_invoke(root,r'''
s=Store('catalog.sqlite')
print(json.dumps({'migration':s.migrate(),'documents':s.documents(),
 'spill':s.db.execute("SELECT value FROM metadata WHERE key='uncommitted_spill'").fetchone()}))
s.close()
''')
            self.assertEqual(result['migration']['from_schema'],2)
            self.assertTrue(result['migration']['migrated'])
            self.assertEqual([row['source'] for row in result['documents']],['a.txt'])
            self.assertIsNone(result['spill'])

    def test_copied_schema3_migration_preserves_backup_registry_and_owned_artifacts(self):
        with ArtifactDirectory('m4-copied-backup-ownership') as artifact:
            root=artifact.root.resolve();_install(root)
            before=_invoke(root,r'''
Path('backups').mkdir()
s=Store('catalog.sqlite');s.insert('a.txt',b'A');s.backup('good.json')
s.register_artifact('backup_stage','backup','pending-owned',artifact_id='pending')
Path('backups/pending-owned').write_text('owned incomplete stage')
print(json.dumps({'control':dict(s.db.execute('SELECT key,value FROM control')),
 'backups':[dict(row) for row in s.db.execute('SELECT * FROM backups')],
 'artifacts':[dict(row) for row in s.db.execute('SELECT * FROM maintenance_artifacts')]}));s.close()
''',release='m3')
            import shutil
            (root/'isolated').mkdir();shutil.copyfile(root/'catalog.sqlite',root/'isolated/catalog.sqlite')
            result=_invoke(root,r'''
s=Store('isolated/catalog.sqlite');migration=s.migrate();s.configure_backup_dir(None)
def backup_authority():
 with s.backup_directory_authority():return 'wrong-root'
blocked=error(backup_authority)
print(json.dumps({'migration':migration,'control':dict(s.db.execute('SELECT key,value FROM control')),
 'backups':[dict(row) for row in s.db.execute('SELECT * FROM backups')],
 'artifacts':[dict(row) for row in s.db.execute('SELECT * FROM maintenance_artifacts')],
 'blocked':blocked,'selected':str(s.backup_dir)}));s.close()
''')
            self.assertEqual(result['migration']['from_schema'],3)
            self.assertTrue(result['migration']['migrated'])
            self.assertEqual(result['backups'],before['backups'])
            self.assertEqual(result['artifacts'],before['artifacts'])
            self.assertEqual({key:value for key,value in result['control'].items() if key!='incarnation'},
                             {key:value for key,value in before['control'].items() if key!='incarnation'})
            self.assertNotEqual(result['control']['incarnation'],before['control']['incarnation'])
            self.assertEqual(result['blocked'],{'error':'maintenance_busy'})
            self.assertEqual(result['selected'],str(root/'isolated/backups'))
            self.assertTrue((root/'backups/good.json').exists())
            self.assertEqual((root/'backups/pending-owned').read_text(),'owned incomplete stage')

    def test_portable_schema3_without_extra_backup_root_preserves_relative_ownership(self):
        with ArtifactDirectory('m4-portable-backup-ownership') as artifact:
            root=artifact.root.resolve();_install(root)
            before=_invoke(root,r'''
Path('backups').mkdir()
s=Store('catalog.sqlite');s.insert('a.txt',b'A');s.backup('good.json')
s.register_artifact('backup_stage','backup','pending-owned',artifact_id='pending')
with s._write():s.db.execute("DELETE FROM control WHERE key='backup_root'")
print(json.dumps({'backups':[dict(row) for row in s.db.execute('SELECT * FROM backups')],
 'artifacts':[dict(row) for row in s.db.execute('SELECT * FROM maintenance_artifacts')]}));s.close()
''',release='m3')
            result=_invoke(root,r'''
s=Store('catalog.sqlite');migration=s.migrate();unbound=s._control('backup_root')
def backup_authority():
 with s.backup_directory_authority():return 'authorized'
blocked=error(backup_authority)
s.configure_backup_dir(str(Path('backups').resolve()))
print(json.dumps({'migration':migration,'unbound':unbound,'blocked':blocked,'adopted':backup_authority(),
 'backups':[dict(row) for row in s.db.execute('SELECT * FROM backups')],
 'artifacts':[dict(row) for row in s.db.execute('SELECT * FROM maintenance_artifacts')]}));s.close()
''')
            self.assertTrue(result['migration']['migrated']);self.assertEqual(result['migration']['from_schema'],3)
            self.assertEqual(result['unbound'],'');self.assertEqual(result['blocked'],{'error':'maintenance_busy'})
            self.assertEqual(result['adopted'],'authorized')
            self.assertEqual(result['backups'],before['backups']);self.assertEqual(result['artifacts'],before['artifacts'])

    def test_portable_schema4_without_authored_triggers_installs_write_fences(self):
        with ArtifactDirectory('m4-portable-fence-installation') as artifact:
            root=artifact.root.resolve();_install(root)
            result=_invoke(root,r'''
s=Store('catalog.sqlite');a=s.insert('a.txt',b'A')['document']['document_id'];s.create_job('j',[])
logical={'listing':s.lifecycle_list(deleted='all'),'jobs':s.list_jobs(),'generation':s._generation(),
 'controls':dict(s.db.execute('SELECT key,value FROM control'))}
s.close();raw=sqlite3.connect('catalog.sqlite')
for row in list(raw.execute("SELECT name FROM sqlite_master WHERE type='trigger'")):
 raw.execute('DROP TRIGGER '+row[0])
raw.commit();raw.close()
s=Store('catalog.sqlite')
reopened={'listing':s.lifecycle_list(deleted='all'),'jobs':s.list_jobs(),'generation':s._generation(),
 'controls':dict(s.db.execute('SELECT key,value FROM control'))}
s.refresh_document(a,1,text='B');s.replace_annotations(a,2,'notes',[],[])
s.cancel_job('j');s.retry_job('j')
print(json.dumps({'unchanged':logical==reopened,'document_high':s.db.execute(
 'SELECT edit_high_water FROM document_control WHERE document_id=?',(a,)).fetchone()[0],
 'job_high':s.db.execute("SELECT epoch_high_water FROM job_control WHERE job_id='j'").fetchone()[0],
 'job_epoch':s.get_job('j')['epoch'],'document_edit':s.lifecycle_show(a)['edit_version'],
 'migration':s.migrate()}));s.close()
''')
            self.assertTrue(result['unchanged'])
            self.assertEqual(result['document_high'],3);self.assertEqual(result['document_edit'],3)
            self.assertEqual(result['job_high'],3);self.assertEqual(result['job_epoch'],3)
            self.assertEqual(result['migration']['from_schema'],4);self.assertFalse(result['migration']['migrated'])

    def test_hot_journal_portable_schema4_recovery_also_installs_fences(self):
        with ArtifactDirectory('m4-hot-journal-portable-fences',retain_success=True) as artifact:
            root=artifact.root.resolve();_install(root)
            _invoke(root,r'''
s=Store('catalog.sqlite');s.insert('a.txt',b'A');s.create_job('j',[]);s.close()
raw=sqlite3.connect('catalog.sqlite')
for row in list(raw.execute("SELECT name FROM sqlite_master WHERE type='trigger'")):
 raw.execute('DROP TRIGGER '+row[0])
raw.commit();raw.close();print('{}')
''')
            _invoke(root,r'''
raw=sqlite3.connect('catalog.sqlite',isolation_level=None)
raw.execute('PRAGMA cache_size=1');raw.execute('BEGIN IMMEDIATE')
raw.execute("INSERT INTO metadata VALUES ('uncommitted_spill',?)",('x'*100000,))
os._exit(73)
''',expected_exit=73)
            import sqlite3
            connection=sqlite3.connect((root/'catalog.sqlite').as_uri()+'?mode=ro',uri=True)
            try:
                with self.assertRaises(sqlite3.OperationalError) as failure:
                    connection.execute('SELECT * FROM sqlite_master').fetchall()
                self.assertEqual(failure.exception.sqlite_errorcode,sqlite3.SQLITE_READONLY_ROLLBACK)
            finally:
                connection.close()
            result=_invoke(root,r'''
s=Store('catalog.sqlite');migration=s.migrate();a=s.documents()[0]['document_id']
s.refresh_document(a,1,text='B');s.cancel_job('j');s.retry_job('j')
print(json.dumps({'migration':migration,'document_high':s.db.execute(
 'SELECT edit_high_water FROM document_control WHERE document_id=?',(a,)).fetchone()[0],
 'job_high':s.db.execute("SELECT epoch_high_water FROM job_control WHERE job_id='j'").fetchone()[0],
 'spill':s.db.execute("SELECT value FROM metadata WHERE key='uncommitted_spill'").fetchone()}));s.close()
''')
            self.assertEqual(result['migration']['from_schema'],4);self.assertFalse(result['migration']['migrated'])
            self.assertEqual(result['document_high'],2);self.assertEqual(result['job_high'],3)
            self.assertIsNone(result['spill'])

    def test_portable_schema4_missing_optional_backup_binding_is_adoptable(self):
        with ArtifactDirectory('m4-portable4-backup-binding') as artifact:
            root=artifact.root.resolve();_install(root)
            _invoke(root,r'''
Path('backups').mkdir();s=Store('catalog.sqlite');s.insert('a.txt',b'A');s.backup('good.json')
s.register_artifact('backup_stage','backup','pending-owned',artifact_id='pending');s.close();print('{}')
''',release='m3')
            result=_invoke(root,r'''
s=Store('catalog.sqlite');s.migrate()
original={'backups':[dict(row) for row in s.db.execute('SELECT * FROM backups')],
 'artifacts':[dict(row) for row in s.db.execute('SELECT * FROM maintenance_artifacts')],
 'generation':s._generation(),'incarnation':s._control('incarnation')}
s.close();raw=sqlite3.connect('catalog.sqlite');raw.execute("DELETE FROM control WHERE key='backup_root'")
raw.commit();raw.close();s=Store('catalog.sqlite');unbound=s._control('backup_root');migration=s.migrate()
s.configure_backup_dir(str(Path('backups').resolve()))
current={'backups':[dict(row) for row in s.db.execute('SELECT * FROM backups')],
 'artifacts':[dict(row) for row in s.db.execute('SELECT * FROM maintenance_artifacts')],
 'generation':s._generation(),'incarnation':s._control('incarnation')}
with s.backup_directory_authority(): authorized=True
print(json.dumps({'unbound':unbound,'migration':migration,'unchanged':original==current,'authorized':authorized}));s.close()
''')
            self.assertEqual(result['unbound'],'');self.assertTrue(result['unchanged']);self.assertTrue(result['authorized'])
            self.assertEqual(result['migration']['from_schema'],4);self.assertFalse(result['migration']['migrated'])
