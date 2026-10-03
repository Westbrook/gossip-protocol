"""Authored normalized-backup qualification, not independent candidate acceptance."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.library_m3_reference_v1 import m3_files


def _install(root: Path, *, legacy: bool = False) -> None:
    files = m3_files()
    if not legacy:
        from gossip_harness.library_m4_catalog_reference_v1 import catalog_files
        from gossip_harness.library_m4_backup_reference_v1 import backup_files
        files.update(catalog_files())
        files.update(backup_files())
        files['library/catalog/store.py'] = (
            'from library.catalog.m4_backup import M4BackupMixin\n'
            'from library.catalog.maintenance import MaintenanceMixin\n'
            'from library.catalog.worker import WorkerMixin\n'
            'from library.catalog.m4_store import NormalizedStore\n'
            'class Store(M4BackupMixin, MaintenanceMixin, WorkerMixin, NormalizedStore):\n    pass\n')
    for name, source in files.items():
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(source, encoding='utf-8')
    (root / 'backups').mkdir(exist_ok=True)


def _invoke(root: Path, script: str) -> dict:
    bootstrap = (
        'import json, os, resource, sys\n'
        'resource.setrlimit(resource.RLIMIT_CPU, (15,15))\n'
        'resource.setrlimit(resource.RLIMIT_FSIZE, (134217728,134217728))\n'
        'resource.setrlimit(resource.RLIMIT_NOFILE, (128,128))\n'
        'sys.path.insert(0, sys.argv[1])\n'
        'from library.catalog.store import Store\n'
        'from library.common import LibraryError\n'
        'def error(call):\n'
        '    try: return call()\n'
        "    except LibraryError as exc: return {'error':exc.code}\n")
    (root / 'qualification-source.py').write_text(bootstrap+script,encoding='utf-8')
    result = subprocess.run([sys.executable,'-I','-c',bootstrap+script,str(root)],
        cwd=root, capture_output=True, text=True, timeout=25, check=False)
    (root / 'qualification-stdout.txt').write_text(result.stdout,encoding='utf-8')
    (root / 'qualification-stderr.txt').write_text(result.stderr,encoding='utf-8')
    if result.returncode != 0 or len(result.stdout.encode()) > 65536:
        raise AssertionError(f'Authored M4 backup process exit {result.returncode}: '+result.stderr[:8000])
    return json.loads(result.stdout)


class LibraryM4BackupReferenceTests(unittest.TestCase):
    def test_normalized_roundtrip_retains_logical_history_and_original_receipt(self):
        with ArtifactDirectory('authored-m4-roundtrip',retain_success=True) as artifacts:
            root = artifacts.root.resolve(); _install(root)
            result = _invoke(root, r'''
from pathlib import Path
from library.catalog.m4_store import revision_id
store=Store('catalog.sqlite')
store.create_job('done',[{'source':'a.txt','text':'original receipt content'}])
store.start_job('done',1); original=store.commit_job('done',1)
identifier=original['documents'][0]['document_id']
store.refresh_document(identifier,1,text='revised content')
store.create_collection('collection',store._generation())
store.replace_annotations(identifier,2,'retained notes',['tag'],['collection'])
store.delete_document(identifier,3)
store.create_job('pending',[{'source':'later.txt','text':'pending'}])
store.enqueue_job('pending')
metadata=store.backup('roundtrip.json')
envelope=json.loads(Path('backups/roundtrip.json').read_bytes())
raw_jobs=[dict(row) for row in store.db.execute('SELECT * FROM jobs ORDER BY job_id')]
store.restore_document(identifier,4)
store.reindex_step(64)
old_generation=store._generation(); old_incarnation=store._control('incarnation')
old_worker_generation=int(store._control('worker_generation'))
restored=store.restore_backup('roundtrip.json',old_generation)
record=store.lifecycle_show(identifier)
normalized=[dict(row) for row in store.db.execute('SELECT * FROM document_revisions ORDER BY revision')]
state=dict(store.db.execute('SELECT * FROM document_state').fetchone())
replay=store.commit_job('done',1)
after_jobs=[dict(row) for row in store.db.execute('SELECT * FROM jobs ORDER BY job_id')]
current_worker_generation=int(store._control('worker_generation'))
result={'restored':restored,'record':record,'payload_schema':envelope['payload']['schema'],
 'format':envelope['format'],'payload_keys':sorted(envelope['payload']['documents'][0]),
 'physical_schema':store.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0],
 'old_tables':store.db.execute("SELECT name FROM sqlite_master WHERE name IN ('lifecycle','revisions')").fetchall(),
 'normalized_ids':all(row['revision_id']==revision_id(identifier,row['revision'],row['blob_id']) for row in normalized),
 'head_matches':state['head_revision_id']==normalized[-1]['revision_id'],
 'replay_same':replay==original,'completed_serialization_same':after_jobs[0]==raw_jobs[0],
 'pending_epoch':after_jobs[1]['epoch'],'pending_enrolled':store.db.execute("SELECT enrolled FROM job_control WHERE job_id='pending'").fetchone()[0],
 'rotated':old_incarnation!=store._control('incarnation'),
 'worker_generation_delta':current_worker_generation-old_worker_generation,
 'generation_delta':store._generation()-old_generation,
 'index':dict(store.db.execute('SELECT * FROM search_state').fetchone()),
 'history':[row['text'] for row in store.revision_history(identifier)['revisions']],
 'metadata_same':store.list_backups()['backups']==[metadata]}
store.close()
reopened=Store('catalog.sqlite'); result['durable']=reopened.lifecycle_show(identifier)==record; reopened.close()
print(json.dumps(result))
''')
        self.assertEqual(result['payload_schema'],3)
        self.assertEqual(result['format'],'local-research-library-backup-v3')
        self.assertEqual(result['physical_schema'],'4')
        self.assertEqual(result['old_tables'],[])
        self.assertEqual(result['payload_keys'],['collections','deleted','document','edit_version','notes','revision','tags'])
        self.assertEqual(result['record']['edit_version'],6)
        self.assertEqual(result['record']['notes'],'retained notes')
        self.assertTrue(result['record']['deleted'])
        self.assertEqual(result['record']['collections'],['collection'])
        self.assertEqual(result['history'],['original receipt content','revised content'])
        for key in ('normalized_ids','head_matches','replay_same','completed_serialization_same','rotated','metadata_same','durable'):
            self.assertTrue(result[key],key)
        self.assertEqual(result['pending_epoch'],2)
        self.assertEqual(result['pending_enrolled'],1)
        self.assertEqual(result['worker_generation_delta'],1)
        self.assertEqual(result['generation_delta'],1)
        self.assertEqual(result['index'],{'singleton':1,'published_generation':None,
            'target_generation':None,'processed':0,'total':0,'cursor':None})

    def test_legacy_v3_backup_restores_into_schema4_without_rewriting_serialization(self):
        with ArtifactDirectory('authored-m4-legacy-backup',retain_success=True) as artifacts:
            parent=artifacts.root.resolve(); old=parent/'old'; current=parent/'current'
            _install(old,legacy=True); _install(current)
            legacy=_invoke(old,r'''
from pathlib import Path
store=Store('catalog.sqlite')
store.create_job('done',[{'source':'a.txt','text':'old content'}]); store.start_job('done',1)
original=store.commit_job('done',1); identifier=original['documents'][0]['document_id']
store.refresh_document(identifier,1,text='new head')
store.create_job('waiting',[{'source':'later.txt','text':'later'}]); store.enqueue_job('waiting')
# Legal serialization choices are part of the immutable persisted job evidence.
for row in store.db.execute('SELECT * FROM jobs').fetchall():
    store.db.execute('UPDATE jobs SET manifest=?,content_hashes=?,receipt=? WHERE job_id=?',
        (json.dumps(json.loads(row['manifest']),indent=2),json.dumps(json.loads(row['content_hashes']),indent=1),
         None if row['receipt'] is None else json.dumps(json.loads(row['receipt']),indent=3),row['job_id']))
store.backup('legacy.json')
print(json.dumps({'jobs':[dict(row) for row in store.db.execute('SELECT * FROM jobs ORDER BY job_id')],
 'receipt':original,'identifier':identifier,'payload':json.loads(Path('backups/legacy.json').read_bytes())['payload']}))
store.close()
''')
            shutil.copyfile(old/'backups/legacy.json',current/'backups/legacy.json')
            result=_invoke(current,r'''
from pathlib import Path
store=Store('catalog.sqlite'); store.insert('target-only.txt',b'removed by restore')
raw=Path('backups/legacy.json').read_bytes()
store.restore_backup('legacy.json',store._generation())
print(json.dumps({'jobs':[dict(row) for row in store.db.execute('SELECT * FROM jobs ORDER BY job_id')],
 'receipt':store.commit_job('done',1),'payload':store._backup_payload(),
 'schema':store.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0],
 'source_unchanged':Path('backups/legacy.json').read_bytes()==raw,
 'foreign_keys':store.db.execute('PRAGMA foreign_key_check').fetchall()}))
store.close()
''')
        self.assertEqual(result['schema'],'4')
        self.assertTrue(result['source_unchanged'])
        self.assertEqual(result['foreign_keys'],[])
        self.assertEqual(result['receipt'],legacy['receipt'])
        self.assertEqual(result['jobs'][0],legacy['jobs'][0])
        for key in ('manifest','content_hashes','receipt'):
            self.assertEqual(result['jobs'][1][key],legacy['jobs'][1][key])
        self.assertEqual(result['jobs'][1]['epoch'],legacy['jobs'][1]['epoch']+1)
        for key in ('revisions','blobs','collections'):
            self.assertEqual(result['payload'][key],legacy['payload'][key])
        self.assertEqual(result['payload']['schema'],3)

    def test_old_snapshot_then_changed_content_cannot_reuse_a_revision_id(self):
        with ArtifactDirectory('authored-m4-revision-aba',retain_success=True) as artifacts:
            root=artifacts.root.resolve(); _install(root)
            result=_invoke(root,r'''
from library.catalog.m4_store import revision_id
store=Store('catalog.sqlite'); identifier=store.insert('a.txt',b'base')['document']['document_id']
store.backup('base.json')
store.refresh_document(identifier,1,text='first branch')
first=dict(store.db.execute('SELECT * FROM document_revisions WHERE revision=2').fetchone())
store.restore_backup('base.json',store._generation())
record=store.lifecycle_show(identifier)
store.refresh_document(identifier,record['edit_version'],text='second branch')
second=dict(store.db.execute('SELECT * FROM document_revisions WHERE revision=2').fetchone())
other=store.insert('other.txt',b'base')['document']['document_id']
store.refresh_document(other,1,text='second branch')
other_id=store.db.execute('SELECT revision_id FROM document_revisions WHERE document_id=? AND revision=2',(other,)).fetchone()[0]
print(json.dumps({'first':first,'second':second,'other_id':other_id,
 'expected':revision_id(identifier,2,second['blob_id']),
 'old_id_absent':store.db.execute('SELECT 1 FROM document_revisions WHERE revision_id=?',(first['revision_id'],)).fetchone() is None,
 'base_version_after_restore':record['edit_version']}))
store.close()
''')
        self.assertEqual(result['first']['revision'],2)
        self.assertEqual(result['second']['revision'],2)
        self.assertNotEqual(result['first']['revision_id'],result['second']['revision_id'])
        self.assertNotEqual(result['other_id'],result['second']['revision_id'])
        self.assertEqual(result['second']['revision_id'],result['expected'])
        self.assertTrue(result['old_id_absent'])
        self.assertEqual(result['base_version_after_restore'],3)

    def test_restore_keeps_removed_document_and_job_high_water_tombstones(self):
        with ArtifactDirectory('authored-m4-restore-fences',retain_success=True) as artifacts:
            root=artifacts.root.resolve(); _install(root)
            result=_invoke(root,r'''
store=Store('catalog.sqlite'); store.backup('empty.json')
identifier=store.insert('removed.txt',b'one')['document']['document_id']
store.refresh_document(identifier,1,text='two')
store.replace_annotations(identifier,2,'notes',[],[])
store.create_job('removedjob',[{'source':'later.txt','text':'later'}])
store.start_job('removedjob',1); store.cancel_job('removedjob'); store.retry_job('removedjob')
store.restore_backup('empty.json',store._generation())
absent=error(lambda:store.lifecycle_show(identifier))
store.insert('removed.txt',b'recreated')
job=store.create_job('removedjob',[{'source':'later.txt','text':'later'}])
print(json.dumps({'absent':absent,'version':store.lifecycle_show(identifier)['edit_version'],
 'epoch':job['epoch'],'old_document_edit':error(lambda:store.delete_document(identifier,3)),
 'old_job_start':error(lambda:store.start_job('removedjob',3)),
 'document_highwater':store.db.execute('SELECT edit_high_water FROM document_control WHERE document_id=?',(identifier,)).fetchone()[0],
 'job_highwater':store.db.execute("SELECT epoch_high_water FROM job_control WHERE job_id='removedjob'").fetchone()[0]}))
store.close()
''')
        self.assertEqual(result,{'absent':{'error':'not_found'},'version':4,'epoch':4,
            'old_document_edit':{'error':'stale_version'},'old_job_start':{'error':'stale_epoch'},
            'document_highwater':4,'job_highwater':4})

    def test_normalized_activation_failure_rolls_back_graph_and_fences(self):
        with ArtifactDirectory('authored-m4-restore-atomic',retain_success=True) as artifacts:
            root=artifacts.root.resolve(); _install(root)
            result=_invoke(root,r'''
import sqlite3
store=Store('catalog.sqlite'); identifier=store.insert('a.txt',b'old')['document']['document_id']
store.backup('old.json'); store.refresh_document(identifier,1,text='live')
store.create_job('live-job',[{'source':'later.txt','text':'later'}])
store.enqueue_job('live-job'); store.reindex_step(64)
def snapshot():
    tables=('documents','blobs','document_state','document_revisions','document_control',
            'jobs','job_control','search_entries','search_state','metadata')
    return {table:[tuple(row) for row in store.db.execute('SELECT * FROM '+table+' ORDER BY 1')] for table in tables}
before=snapshot(); incarnation=store._control('incarnation'); worker=store._control('worker_generation')
def deny_state(action, table, column, database, source):
    if action==sqlite3.SQLITE_INSERT and table=='document_state': return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK
store.db.set_authorizer(deny_state)
failed=error(lambda:store.restore_backup('old.json',store._generation()))
store.db.set_authorizer(None)
print(json.dumps({'failed':failed,'same':snapshot()==before,
 'incarnation_same':store._control('incarnation')==incarnation,
 'worker_same':store._control('worker_generation')==worker,
 'foreign_keys':store.db.execute('PRAGMA foreign_key_check').fetchall()}))
store.close()
''')
        self.assertEqual(result,{'failed':{'error':'io_error'},'same':True,
            'incarnation_same':True,'worker_same':True,'foreign_keys':[]})


if __name__ == '__main__':
    unittest.main()
