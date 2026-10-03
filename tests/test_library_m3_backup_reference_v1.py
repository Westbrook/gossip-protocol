"""Physical authored-reference qualification, not independent candidate acceptance."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m2_reference_v1 import m2_files
from gossip_harness.library_m3_backup_format_v1 import format_files
from gossip_harness.library_m3_backup_reference_v1 import backup_files
from gossip_harness.library_m3_control_reference_v1 import control_files


def _install(root):
    files = m2_files()
    for overlay in (control_files(), format_files(), backup_files()):
        files.update(overlay)
    files['library/catalog/store.py'] = (
        'from library.catalog.backup import BackupMixin\n'
        'from library.catalog.control import CoreStore\n'
        'class Store(BackupMixin, CoreStore):\n    pass\n')
    for name, source in files.items():
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(source, encoding='utf-8')
    (root / 'backups').mkdir(exist_ok=True)


def _invoke(root, script, *, expected_exit=0):
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
    result = subprocess.run([sys.executable,'-I','-c',bootstrap+script,str(root)],
        cwd=root, capture_output=True, text=True, timeout=25, check=False)
    if result.returncode != expected_exit or len(result.stdout.encode()) > 65536:
        raise AssertionError(f'Authored backup process exit {result.returncode}: '+result.stderr[:8000])
    return json.loads(result.stdout) if expected_exit == 0 else None


class LibraryM3BackupReferenceTests(unittest.TestCase):
    def test_roundtrip_restores_revisions_and_receipts_but_fences_removed_ids(self):
        with tempfile.TemporaryDirectory(prefix='authored-m3-backup-') as directory:
            root = Path(directory).resolve(); _install(root)
            result = _invoke(root, r'''
store = Store('catalog.sqlite')
store.create_job('done',[{'source':'a.txt','text':'old'}]); store.start_job('done',1)
original = store.commit_job('done',1)
identifier = original['documents'][0]['document_id']
store.refresh_document(identifier,1,text='new')
store.create_collection('collection',store._generation())
store.replace_annotations(identifier,2,'notes',['tag'],['collection'])
store.delete_document(identifier,3)
store.create_job('pending',[{'source':'pending.txt','text':'later'}])
metadata = store.backup('roundtrip.json')
store.restore_document(identifier,4)
removed = store.insert('removed.txt',b'removed')['document']['document_id']
store.refresh_document(removed,1,text='removed twice')
store.create_job('removedjob',[{'source':'future.txt','text':'future'}])
store.start_job('removedjob',1); store.cancel_job('removedjob')
store.retry_job('removedjob')
other = Store('catalog.sqlite')
result = store.restore_backup('roundtrip.json',store._generation())
restored = store.lifecycle_show(identifier)
replay = store.commit_job('done',1)
pending = store.get_job('pending')
old_handle = error(lambda:other.insert('other.txt',b'bad'))
stale_document = error(lambda:store.restore_document(identifier,4))
stale_job = error(lambda:store.start_job('pending',1))
removed_missing = error(lambda:store.lifecycle_show(removed))
reimport = store.insert('removed.txt',b'new incarnation')
recreated_job = store.create_job('removedjob',[{'source':'future.txt','text':'future'}])
print(json.dumps({'metadata':metadata,'result':result,'restored':restored,
 'replayed':replay==original,'pending':pending,'old_handle':old_handle,
 'stale_document':stale_document,'stale_job':stale_job,'removed_missing':removed_missing,
 'reimport_version':store.lifecycle_show(reimport['document']['document_id'])['edit_version'],
 'recreated_epoch':recreated_job['epoch'],'history':store.revision_history(identifier),
 'registered':store.list_backups(),'schema':store.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0]}))
other.close(); store.close()
''')
        self.assertTrue(result['replayed'])
        self.assertEqual(result['result']['documents'],1)
        self.assertEqual(result['result']['jobs'],2)
        self.assertEqual(result['restored']['edit_version'],6)
        self.assertTrue(result['restored']['deleted'])
        self.assertEqual(result['restored']['notes'],'notes')
        self.assertEqual(result['restored']['collections'],['collection'])
        self.assertEqual(result['pending']['epoch'],2)
        self.assertEqual(result['old_handle'],{'error':'stale_worker'})
        self.assertEqual(result['stale_document'],{'error':'stale_version'})
        self.assertEqual(result['stale_job'],{'error':'stale_epoch'})
        self.assertEqual(result['removed_missing'],{'error':'not_found'})
        self.assertEqual(result['reimport_version'],3)
        self.assertEqual(result['recreated_epoch'],4)
        self.assertEqual([row['text'] for row in result['history']['revisions']],['old','new'])
        self.assertEqual(result['registered'],{'backups':[result['metadata']],'total':1})
        self.assertEqual(result['schema'],'3')

    def test_paths_validation_and_bounds_precede_activation(self):
        with tempfile.TemporaryDirectory(prefix='authored-m3-paths-') as directory:
            root = Path(directory).resolve(); _install(root)
            result = _invoke(root, r'''
from pathlib import Path
store = Store('catalog.sqlite'); store.insert('a.txt',b'unchanged')
store.backup('good.json')
Path('backups/broken.json').write_text('{}')
os.symlink('good.json','backups/link.json')
with open('backups/large.json','wb') as stream: stream.truncate(67108865)
before = store.lifecycle_list(deleted='all')
def forbidden(): raise AssertionError('Invalid backup acquired activation authority')
store.maintenance_authority = forbidden
results = [error(lambda:store.restore_backup(name,before['generation']))
           for name in ('broken.json','link.json','large.json','../good.json')]
results.append(error(lambda:store.restore_backup('good.json',True)))
after = store.lifecycle_list(deleted='all')
print(json.dumps({'results':results,'same':before==after}))
store.close()
''')
        self.assertEqual(result,{'results':[{'error':code} for code in
            ('invalid_backup','invalid_source','too_large','invalid_source','invalid_request')],
            'same':True})

    def test_registry_metadata_pagination_and_manual_restore(self):
        with tempfile.TemporaryDirectory(prefix='authored-m3-registry-') as directory:
            root = Path(directory).resolve(); _install(root)
            result = _invoke(root, r'''
from pathlib import Path
store = Store('catalog.sqlite'); store.insert('a.txt',b'one')
store.backup('z.json'); store.backup('a.json')
original = Path('backups/a.json').read_bytes()
Path('backups/manual.json').write_bytes(original)
Path('backups/unknown.tmp').write_text('operator file')
no_overwrite = error(lambda:store.backup('a.json'))
manual = store.restore_backup('manual.json',store._generation())
store.cleanup_artifacts()
read = store._bounded_backup_read
def forbidden(*args): raise AssertionError('Listing read a payload')
store._bounded_backup_read = forbidden
listing = store.list_backups(offset=1,limit=1)
store._bounded_backup_read = read
print(json.dumps({'listing':listing,'no_overwrite':no_overwrite,'manual':manual,
 'preserved':Path('backups/a.json').read_bytes()==original,
 'unknown':Path('backups/unknown.tmp').read_text()}))
store.close()
''')
        self.assertEqual(result['listing']['total'],2)
        self.assertEqual([row['name'] for row in result['listing']['backups']],['z.json'])
        self.assertEqual(result['no_overwrite'],{'error':'already_exists'})
        self.assertTrue(result['manual']['restored'])
        self.assertTrue(result['preserved'])
        self.assertEqual(result['unknown'],'operator file')

    def test_sqlite_failures_are_domain_io_errors_with_atomic_rollback(self):
        with tempfile.TemporaryDirectory(prefix='authored-m3-sqlite-errors-') as directory:
            root = Path(directory).resolve(); _install(root)
            result = _invoke(root, r'''
import sqlite3
store=Store('catalog.sqlite'); store.insert('a.txt',b'one')
store.backup('good.json')
def deny_registration(action, table, column, database, source):
    if action==sqlite3.SQLITE_INSERT and table=='backups': return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK
store.db.set_authorizer(deny_registration)
backup_error=error(lambda:store.backup('recover.json'))
store.db.set_authorizer(None)
store.cleanup_artifacts(); store.cleanup_artifacts()
before=store.lifecycle_list(deleted='all')
def deny_activation(action, table, column, database, source):
    if action==sqlite3.SQLITE_DELETE and table=='documents': return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK
store.db.set_authorizer(deny_activation)
restore_error=error(lambda:store.restore_backup('good.json',store._generation()))
store.db.set_authorizer(None)
after=store.lifecycle_list(deleted='all')
# Failure of recording diagnostics cannot hide the declared primary error.
def broken_diagnostics(*args): raise sqlite3.OperationalError('unwritable diagnostics')
store.record_error=broken_diagnostics
primary=error(lambda:store.restore_backup('missing.json',store._generation()))
print(json.dumps({'backup_error':backup_error,'restore_error':restore_error,
 'primary':primary,'same':before==after,'registered':store.list_backups()['total']}))
store.close()
''')
        self.assertEqual(result,{'backup_error':{'error':'io_error'},
            'restore_error':{'error':'io_error'},'primary':{'error':'io_error'},
            'same':True,'registered':2})

    def test_counter_overflow_fails_atomically_without_inventing_format_bounds(self):
        with tempfile.TemporaryDirectory(prefix='authored-m3-counter-overflow-') as directory:
            root = Path(directory).resolve(); _install(root)
            result = _invoke(root, r'''
from pathlib import Path
import hashlib
from library.catalog.backup_format import canonical_bytes
store=Store('catalog.sqlite'); store.insert('a.txt',b'one')
store.backup('good.json')
envelope=json.loads(Path('backups/good.json').read_bytes())
envelope['payload']['documents'][0]['edit_version']=2**63-1
envelope['payload_sha256']=hashlib.sha256(canonical_bytes(envelope['payload'])).hexdigest()
Path('backups/large_counter.json').write_bytes(canonical_bytes(envelope))
before=store.lifecycle_list(deleted='all')
failed=error(lambda:store.restore_backup('large_counter.json',store._generation()))
after=store.lifecycle_list(deleted='all')
print(json.dumps({'failed':failed,'same':before==after,
 'version':store.db.execute('SELECT edit_high_water FROM document_control').fetchone()[0]}))
store.close()
''')
        self.assertEqual(result,{'failed':{'error':'io_error'},'same':True,'version':1})

    def test_success_clears_only_its_own_prior_maintenance_error(self):
        with tempfile.TemporaryDirectory(prefix='authored-m3-diagnostics-') as directory:
            root = Path(directory).resolve(); _install(root)
            result = _invoke(root, r'''
store=Store('catalog.sqlite'); store.insert('a.txt',b'one')
store.backup('good.json')
error(lambda:store.backup('good.json'))
store.backup('next.json')
cleared=json.loads(store._control('last_error'))
store.record_error('reindex','io_error')
store.backup('third.json')
after_backup=json.loads(store._control('last_error'))
store.restore_backup('good.json',store._generation())
after_restore=json.loads(store._control('last_error'))
error(lambda:store.restore_backup('good.json',0))
store.restore_backup('good.json',store._generation())
print(json.dumps({'cleared':cleared,'after_backup':after_backup,
 'after_restore':after_restore,'restored_clear':json.loads(store._control('last_error'))}))
store.close()
''')
        self.assertEqual(result,{'cleared':None,'restored_clear':None,
            'after_backup':{'operation':'reindex','code':'io_error'},
            'after_restore':{'operation':'reindex','code':'io_error'}})

    def test_custom_root_restart_fails_closed_until_explicit_configuration(self):
        with tempfile.TemporaryDirectory(prefix='authored-m3-custom-root-') as directory:
            root = Path(directory).resolve(); _install(root)
            (root/'custom').mkdir()
            _invoke(root, "store=Store('catalog.sqlite',backup_dir='custom')\n"
                + "store.insert('a.txt',b'one')\n"
                + "store._backup_checkpoint=lambda phase: os._exit(73) if phase=='after_publish' else None\n"
                + "store.backup('recover.json')\n", expected_exit=73)
            stage = next((root/'custom').glob('.backup-*')).name
            (root/'backups'/'recover.json').write_text('unowned default backup')
            (root/'backups'/stage).write_text('unowned default stage')
            result = _invoke(root, r'''
from pathlib import Path
default=error(lambda:Store('catalog.sqlite'))
store=Store('catalog.sqlite',backup_dir='custom'); store.cleanup_artifacts(); store.cleanup_artifacts()
print(json.dumps({'default':default,'total':store.list_backups()['total'],
 'configured':store.backup_dir.name,
 'stages':[p.name for p in Path('custom').glob('.backup-*')],
 'default_backup':Path('backups/recover.json').read_text(),
 'default_stages':[p.read_text() for p in Path('backups').glob('.backup-*')]}))
store.close()
''')
        self.assertEqual(result,{'default':{'error':'maintenance_busy'},
            'total':1,'configured':'custom','stages':[],
            'default_backup':'unowned default backup','default_stages':['unowned default stage']})

    def test_root_reassignment_is_fenced_before_artifact_enrollment(self):
        with tempfile.TemporaryDirectory(prefix='authored-m3-root-lock-') as directory:
            root = Path(directory).resolve(); _install(root)
            (root/'other').mkdir()
            result = _invoke(root, r'''
from pathlib import Path
store=Store('catalog.sqlite'); peer=Store('catalog.sqlite')
store.insert('a.txt',b'one')
original=store._backup_payload
attempts=[]
def during_snapshot():
    attempts.append(error(lambda:peer.configure_backup_dir('other')))
    return original()
store._backup_payload=during_snapshot
created=store.backup('fenced.json')
print(json.dumps({'attempts':attempts,'created':created['name'],
 'original_exists':Path('backups/fenced.json').exists(),
 'other_exists':Path('other/fenced.json').exists()}))
peer.configure_backup_dir('other')
peer.backup('other.json')
# The old handle must not describe another configured root's registry.
assert error(lambda:store.list_backups()) == {'error':'maintenance_busy'}
peer.close(); store.close()
''')
        self.assertEqual(result,{'attempts':[{'error':'maintenance_busy'}],
            'created':'fenced.json','original_exists':True,'other_exists':False})

    def test_backup_and_restore_owners_run_bounded_cleanup_before_and_after(self):
        with tempfile.TemporaryDirectory(prefix='authored-m3-owner-cleanup-') as directory:
            root = Path(directory).resolve(); _install(root)
            result = _invoke(root, r'''
store=Store('catalog.sqlite'); store.insert('a.txt',b'one')
def abandoned(identifier):
    with store.artifact_lock(identifier):
        store.register_artifact('restore_stage','maintenance',identifier+'.partial',artifact_id=identifier)
        (store.maintenance_root/(identifier+'.partial')).write_text('owned abandoned stage')
unknown=store.maintenance_root/'operator.txt'; unknown.write_text('do not delete')
abandoned('beforebackup')
original=store._backup_payload
seen=[]
def during_backup():
    seen.append(not (store.maintenance_root/'beforebackup.partial').exists())
    abandoned('duringbackup')
    return original()
store._backup_payload=during_backup
store.backup('clean.json')
seen.append(not (store.maintenance_root/'duringbackup.partial').exists())
abandoned('beforerestore')
def after_activation(phase):
    if phase=='before_activate_commit':
        seen.append(not (store.maintenance_root/'beforerestore.partial').exists())
    if phase=='after_activate_commit': abandoned('duringrestore')
store._backup_checkpoint=after_activation
store.restore_backup('clean.json',store._generation())
seen.append(not (store.maintenance_root/'duringrestore.partial').exists())
print(json.dumps({'seen':seen,'unknown':unknown.read_text(),
 'artifacts':store.db.execute('SELECT count(*) FROM maintenance_artifacts').fetchone()[0]}))
store.close()
''')
        self.assertEqual(result,{'seen':[True,True,True,True],
            'unknown':'do not delete','artifacts':0})

    def test_real_exit_before_publication_cleans_only_owned_stage(self):
        self._publication_exit('before_publish',0)

    def test_real_exit_after_publication_recovers_registry(self):
        self._publication_exit('after_publish',1)

    def test_real_exit_after_registry_preserves_publication(self):
        self._publication_exit('after_registry',1)

    def _publication_exit(self, phase, expected_total):
        with tempfile.TemporaryDirectory(prefix='authored-m3-publish-exit-') as directory:
            root = Path(directory).resolve(); _install(root)
            _invoke(root, "store=Store('catalog.sqlite')\nstore.insert('a.txt',b'one')\n"
                + f"store._backup_checkpoint=lambda phase: os._exit(73) if phase=={phase!r} else None\n"
                + "store.backup('recover.json')\n", expected_exit=73)
            result = _invoke(root, r'''
from pathlib import Path
store=Store('catalog.sqlite'); store.cleanup_artifacts(); store.cleanup_artifacts()
print(json.dumps({'listing':store.list_backups(),'stages':[p.name for p in Path('backups').glob('.backup-*')],
 'documents':len(store.documents()),'generation':store._generation()}))
store.close()
''')
        self.assertEqual(result['listing']['total'],expected_total)
        self.assertEqual(result['stages'],[])
        self.assertEqual(result['documents'],1)
        self.assertEqual(result['generation'],1)

    def test_recovery_crash_before_outer_registration_commit_retains_ownership(self):
        with tempfile.TemporaryDirectory(prefix='authored-m3-recovery-exit-') as directory:
            root = Path(directory).resolve(); _install(root)
            _invoke(root, "store=Store('catalog.sqlite')\nstore.insert('a.txt',b'one')\n"
                + "store._backup_checkpoint=lambda phase: os._exit(73) if phase=='after_publish' else None\n"
                + "store.backup('recover.json')\n", expected_exit=73)
            _invoke(root, "class CrashingStore(Store):\n"
                + "    def _backup_checkpoint(self,phase):\n"
                + "        if phase=='recovery_registered': os._exit(74)\n"
                + "store=CrashingStore('catalog.sqlite')\nstore.cleanup_artifacts()\n", expected_exit=74)
            result = _invoke(root, r'''
from pathlib import Path
store=Store('catalog.sqlite'); store.cleanup_artifacts(); store.cleanup_artifacts()
print(json.dumps({'total':store.list_backups()['total'],
 'stages':[p.name for p in Path('backups').glob('.backup-*')]}))
store.close()
''')
        self.assertEqual(result,{'total':1,'stages':[]})

    def test_recovery_cleans_owned_stage_but_preserves_unknown_conflicting_final(self):
        with tempfile.TemporaryDirectory(prefix='authored-m3-conflicting-final-') as directory:
            root = Path(directory).resolve(); _install(root)
            _invoke(root, "store=Store('catalog.sqlite')\nstore.insert('a.txt',b'one')\n"
                + "store._backup_checkpoint=lambda phase: os._exit(73) if phase=='before_publish' else None\n"
                + "store.backup('recover.json')\n", expected_exit=73)
            (root/'backups'/'recover.json').write_text('unowned operator document')
            result = _invoke(root, r'''
from pathlib import Path
store=Store('catalog.sqlite'); store.cleanup_artifacts()
print(json.dumps({'total':store.list_backups()['total'],
 'stages':[p.name for p in Path('backups').glob('.backup-*')],
 'operator_file':Path('backups/recover.json').read_text()}))
store.close()
''')
        self.assertEqual(result,{'total':0,'stages':[],
                                'operator_file':'unowned operator document'})

    def test_real_exit_before_activation_keeps_complete_old_catalog(self):
        self._activation_exit('before_activate_commit',False)

    def test_real_exit_after_activation_keeps_complete_new_catalog(self):
        self._activation_exit('after_activate_commit',True)

    def _activation_exit(self, phase, committed):
        with tempfile.TemporaryDirectory(prefix='authored-m3-restore-exit-') as directory:
            root = Path(directory).resolve(); _install(root)
            _invoke(root, "store=Store('catalog.sqlite')\ndoc=store.insert('a.txt',b'one')['document']\n"
                + "store.backup('recover.json')\nstore.refresh_document(doc['document_id'],1,text='two')\n"
                + "store.insert('later.txt',b'later')\n"
                + f"store._backup_checkpoint=lambda phase: os._exit(73) if phase=={phase!r} else None\n"
                + "store.restore_backup('recover.json',store._generation())\n", expected_exit=73)
            result = _invoke(root, r'''
store=Store('catalog.sqlite')
print(json.dumps({'records':store.lifecycle_list(deleted='all'),'highwater':[
 list(row) for row in store.db.execute('SELECT document_id,edit_high_water FROM document_control ORDER BY document_id')],
 'integrity':store.db.execute('PRAGMA integrity_check').fetchone()[0]}))
store.close()
''')
        self.assertEqual(result['integrity'],'ok')
        records = result['records']
        self.assertEqual(records['generation'],4 if committed else 3)
        self.assertEqual(records['total'],1 if committed else 2)
        self.assertEqual(records['records'][0]['document']['text'],'one' if committed else 'two')
        self.assertEqual(records['records'][0]['edit_version'],3 if committed else 2)
        self.assertEqual(len(result['highwater']),2)
