"""Bounded authored-reference checks, not independent candidate acceptance."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m2_reference_v1 import m2_files
from gossip_harness.library_m3_maintenance_reference_v1 import maintenance_files


def _authored(script):
    from gossip_harness.library_m3_control_reference_v1 import control_files
    files = m2_files()
    files.update(control_files())
    files.update(maintenance_files())
    files['library/catalog/store.py'] = (
        'from library.catalog.control import CoreStore\n'
        'from library.catalog.maintenance import MaintenanceMixin\n'
        'class Store(MaintenanceMixin, CoreStore):\n    pass\n')
    with tempfile.TemporaryDirectory(prefix='trusted-library-m3-maintenance-') as directory:
        root = Path(directory).resolve()
        for name, source in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding='utf-8')
        bootstrap = (
            'import json, resource, sys\n'
            'resource.setrlimit(resource.RLIMIT_CPU, (15, 15))\n'
            'resource.setrlimit(resource.RLIMIT_FSIZE, (67108864, 67108864))\n'
            'resource.setrlimit(resource.RLIMIT_NOFILE, (96, 96))\n'
            'sys.path.insert(0, sys.argv[1])\n'
            'from library.catalog.store import Store\n'
            'from library.common import LibraryError\n'
            'def error(call):\n'
            '    try: return call()\n'
            "    except LibraryError as exc: return {'error': exc.code}\n"
        )
        result = subprocess.run([sys.executable, '-I', '-c', bootstrap + script, str(root)],
                                cwd=root, capture_output=True, text=True, timeout=25, check=False)
        if result.returncode or len(result.stdout.encode()) > 65536:
            raise AssertionError('Trusted M3 maintenance smoke failed: ' + result.stderr[:6000])
        return json.loads(result.stdout)


class LibraryM3MaintenanceReferenceTests(unittest.TestCase):
    def test_additive_generated_source_compiles_and_is_fresh(self):
        files = maintenance_files()
        self.assertEqual(set(files), {'library/catalog/maintenance.py'})
        for name, source in files.items():
            compile(source, name, 'exec')
        files.clear()
        self.assertTrue(maintenance_files())

    def test_cursor_reopens_and_generation_change_discards_only_shadow(self):
        result = _authored(r'''
s = Store('catalog.sqlite')
ids = {name: s.insert(name+'.txt', name.encode())['document']['document_id'] for name in ['z','a','m']}
first = s.reindex_step(1)
s.close(); s = Store('catalog.sqlite')
second = s.reindex_step(1)
s.refresh_document(ids['z'], 1, text='fresh answer')
stale = s.diagnostics()['index_state']
restart = s.reindex_step(1)
shadow_generations = [row[0] for row in s.db.execute('SELECT DISTINCT generation FROM search_entries')]
answers = [r['document']['source'] for r in s.lifecycle_list('fresh answer')['records']]
complete = s.reindex_step(64)
repeat = s.reindex_step(1)
print(json.dumps({'first':first,'second':second,'restart':restart,'complete':complete,'repeat':repeat,
    'ids':ids,'stale':stale,'shadow_generations':shadow_generations,'answers':answers,
    'final':s.diagnostics()['index_state']}))
s.close()
''')
        self.assertEqual(result['first'], {'state':'running','generation':None,'target_generation':3,
                                          'processed':1,'total':3,'cursor':result['ids']['a']})
        self.assertEqual(result['second']['cursor'], result['ids']['m'])
        self.assertEqual(result['second']['processed'], 2)
        self.assertEqual(result['restart']['processed'], 1)
        self.assertEqual(result['restart']['target_generation'], 4)
        self.assertEqual(result['restart']['cursor'], result['ids']['a'])
        self.assertEqual(result['shadow_generations'], [4])
        self.assertEqual(result['answers'], ['z.txt'])
        self.assertEqual(result['stale'], 'stale')
        self.assertEqual(result['complete'], {'state':'completed','generation':4,'target_generation':4,
                                             'processed':3,'total':3,'cursor':result['ids']['z']})
        self.assertEqual(result['repeat'], result['complete'])
        self.assertEqual(result['final'], 'current')

    def test_empty_index_limits_and_operation_specific_error_clear(self):
        result = _authored(r'''
s = Store('catalog.sqlite')
failures = [error(lambda v=v:s.reindex_step(v)) for v in [True,False,0,65,1.0,'1',None]]
failed = s.diagnostics()
empty = s.reindex_step()
cleared = s.diagnostics()
s.record_error('backup', 'already_exists')
s.reindex_step()
retained = s.diagnostics()
print(json.dumps({'failures':failures,'failed':failed,'empty':empty,'cleared':cleared,'retained':retained}))
s.close()
''')
        self.assertEqual(result['failures'], [{'error':'invalid_request'}] * 7)
        self.assertEqual(result['failed']['last_error'], {'operation':'reindex','code':'invalid_request'})
        self.assertEqual(result['failed']['recovery_action'], 'retry_reindex')
        self.assertEqual(result['empty'], {'state':'completed','generation':0,'target_generation':0,
                                          'processed':0,'total':0,'cursor':None})
        self.assertIsNone(result['cleared']['last_error'])
        self.assertEqual(result['cleared']['recovery_action'], 'none')
        self.assertEqual(result['retained']['last_error'], {'operation':'backup','code':'already_exists'})
        self.assertEqual(result['retained']['recovery_action'], 'choose_new_backup')

    def test_export_selection_history_unicode_and_exact_encoded_limit(self):
        result = _authored(r'''
s = Store('catalog.sqlite')
z = s.insert('z.txt', '旧'.encode())['document']['document_id']
a = s.insert('a.txt', b'A')['document']['document_id']
s.refresh_document(z, 1, text='新')
s.replace_annotations(z, 2, 'é\n<script>', ['TAG'], [])
s.delete_document(a, 1)
full = s.export_bundle([z,a], include_deleted=True, include_history=True)
raw = json.dumps(full, ensure_ascii=False, sort_keys=True, separators=(',',':')).encode('utf-8')
exact = s.export_bundle([a,z], include_deleted=True, include_history=True, max_bytes=len(raw))
too_small = error(lambda:s.export_bundle([z,a], include_deleted=True, include_history=True, max_bytes=len(raw)-1))
print(json.dumps({'full':full,'exact':exact,'too_small':too_small,
    'active':s.export_bundle(),'empty':s.export_bundle([]),
    'deleted':error(lambda:s.export_bundle([z,a])),
    'sources':[item['record']['document']['source'] for item in full['documents']]}))
s.close()
''')
        self.assertEqual(result['full'], result['exact'])
        self.assertEqual(result['full']['format'], 'local-research-library-export-v2')
        self.assertEqual(result['full']['generation'], 5)
        self.assertEqual(result['sources'], ['a.txt','z.txt'])
        record = result['full']['documents'][1]
        self.assertEqual([r['text'] for r in record['revisions']], ['旧','新'])
        self.assertEqual(record['record']['notes'], 'é\n<script>')
        self.assertEqual(record['record']['tags'], ['tag'])
        self.assertEqual(result['too_small'], {'error':'too_large'})
        self.assertEqual(result['deleted'], {'error':'not_found'})
        self.assertEqual(result['empty']['documents'], [])
        self.assertEqual(len(result['active']['documents']), 1)
        self.assertEqual(result['active']['documents'][0]['revisions'], [])

    def test_export_closed_types_full_selection_and_read_only_failure(self):
        result = _authored(r'''
s = Store('catalog.sqlite')
a = s.insert('a.txt', b'A')['document']['document_id']
before = list(s.db.iterdump())
calls = [lambda:s.export_bundle([a,a]),lambda:s.export_bundle((a,)),lambda:s.export_bundle([1]),
         lambda:s.export_bundle([str(i) for i in range(257)]),lambda:s.export_bundle(include_deleted=1),
         lambda:s.export_bundle(include_history=0),lambda:s.export_bundle(max_bytes=True),
         lambda:s.export_bundle(max_bytes=0),lambda:s.export_bundle(max_bytes=16777217),
         lambda:s.export_bundle([a,'missing']),lambda:s.export_bundle(['\ud800'])]
failures = [error(call) for call in calls]
print(json.dumps({'failures':failures,'unchanged':before==list(s.db.iterdump())}))
s.close()
''')
        self.assertEqual(result['failures'][:9], [{'error':'invalid_request'}] * 9)
        self.assertEqual(result['failures'][9:], [{'error':'not_found'}, {'error':'invalid_utf8'}])
        self.assertTrue(result['unchanged'])

    def test_diagnostics_graph_counts_exact_shape_and_no_read_mutation(self):
        result = _authored(r'''
s = Store('catalog.sqlite')
a = s.insert('a.txt', b'AA')['document']['document_id']
s.insert('b.txt', b'AA')
s.refresh_document(a, 1, text='BBB')
s.delete_document(a, 2)
s.create_job('waiting', [{'source':'next.txt','text':'future'}])
s.record_error('restore', 'invalid_backup')
before = list(s.db.iterdump())
one = s.diagnostics(); two = s.diagnostics()
print(json.dumps({'one':one,'same':one==two,'unchanged':before==list(s.db.iterdump()),
                  'absent':one['index_state']}))
s.close()
''')
        expected = {'schema':3,'generation':4,'documents':{'active':1,'deleted':1},'revisions':3,
                    'blobs':{'count':2,'bytes':5},'jobs':{'queued':1,'running':0,'completed':0,'cancelled':0,'failed':0},
                    'worker_generation':0,'index_state':'absent',
                    'last_error':{'operation':'restore','code':'invalid_backup'},'recovery_action':'validate_backup'}
        observed = result['one']
        self.assertIn(observed.pop('worker_state'), ('idle','stopped'))
        self.assertEqual(observed, expected)
        self.assertTrue(result['same'])
        self.assertTrue(result['unchanged'])

    def test_diagnostics_reports_only_a_live_worker_lock_as_running(self):
        result = _authored(r'''
s = Store('catalog.sqlite')
files_before = sorted(p.name for p in s.maintenance_root.iterdir())
first = s.diagnostics()
files_after = sorted(p.name for p in s.maintenance_root.iterdir())
observer = Store('catalog.sqlite')
with s.maintenance_authority():
    authority = observer.diagnostics()
observer.close()
with s.worker_owner():
    own = s.diagnostics()
    observer = Store('catalog.sqlite')
    observed = observer.diagnostics()
    observer.close()
after = s.diagnostics()
print(json.dumps({'first':first,'own':own,'observed':observed,'after':after,
                  'authority':authority,'read_created_files':files_before != files_after}))
s.close()
''')
        self.assertIn(result['first']['worker_state'], ('idle', 'stopped'))
        self.assertEqual(result['own']['worker_state'], 'running')
        self.assertEqual(result['observed']['worker_state'], 'running')
        self.assertIn(result['after']['worker_state'], ('idle', 'stopped'))
        self.assertEqual(result['after']['worker_generation'], 1)
        self.assertFalse(result['read_created_files'])
        self.assertIn(result['authority']['worker_state'], ('idle', 'stopped'))

    def test_publication_error_rolls_back_shadow_pointer_and_old_cleanup(self):
        result = _authored(r'''
s = Store('catalog.sqlite')
s.insert('a.txt', b'A'); old = s.reindex_step()
s.insert('b.txt', b'B')
original = s._index_obsolete
def failure(generation):
    original(generation)
    raise OSError('private failure detail must never escape diagnostics')
s._index_obsolete = failure
failed = error(lambda:s.reindex_step())
state = dict(s.db.execute('SELECT * FROM search_state').fetchone())
entries = [row[0] for row in s.db.execute('SELECT generation FROM search_entries')]
diagnostic = s.diagnostics()
s._index_obsolete = original
repaired = s.reindex_step()
remaining = [row[0] for row in s.db.execute('SELECT DISTINCT generation FROM search_entries')]
print(json.dumps({'failed':failed,'state':state,'entries':entries,'diagnostic':diagnostic,
                  'repaired':repaired,'remaining':remaining,'documents':len(s.documents())}))
s.close()
''')
        self.assertEqual(result['failed'], {'error':'io_error'})
        self.assertEqual(result['state']['published_generation'], 1)
        self.assertEqual(result['state']['target_generation'], 1)
        self.assertEqual(result['entries'], [1])
        self.assertEqual(result['diagnostic']['last_error'], {'operation':'reindex','code':'io_error'})
        self.assertNotIn('private failure', json.dumps(result['diagnostic']))
        self.assertEqual(result['repaired']['generation'], 2)
        self.assertEqual(result['remaining'], [2])
        self.assertEqual(result['documents'], 2)

    def test_maintenance_cleanup_retires_superseded_registry_generations(self):
        result = _authored(r'''
s = Store('catalog.sqlite')
s.insert('a.txt', b'A'); s.reindex_step()
s.insert('b.txt', b'B'); partial = s.reindex_step(1)
during = [row[0] for row in s.db.execute('SELECT DISTINCT generation FROM search_entries ORDER BY generation')]
s.reindex_step(1)
entries = [row[0] for row in s.db.execute('SELECT DISTINCT generation FROM search_entries ORDER BY generation')]
artifacts = [dict(row) for row in s.db.execute('SELECT kind,relative_path,state FROM maintenance_artifacts')]
print(json.dumps({'partial':partial,'during':during,'entries':entries,'artifacts':artifacts}))
s.close()
''')
        self.assertEqual(result['partial']['generation'], 1)
        self.assertEqual(result['partial']['state'], 'running')
        self.assertEqual(result['during'], [1, 2])
        self.assertEqual(result['entries'], [2])
        self.assertEqual(result['artifacts'], [{'kind':'index_generation','relative_path':'2','state':'published'}])

    def test_real_process_kill_before_publication_preserves_last_complete_index(self):
        result = _authored(r'''
import signal
import subprocess
s = Store('catalog.sqlite')
s.insert('a.txt', b'A'); s.reindex_step()
s.insert('b.txt', b'B'); s.close()
child = """import os, signal, sys
sys.path.insert(0, sys.argv[1])
from library.catalog.store import Store
s = Store('catalog.sqlite')
def cut(statement):
    if statement.startswith('UPDATE search_state SET published_generation='):
        os.kill(os.getpid(), signal.SIGKILL)
s.db.set_trace_callback(cut)
s.reindex_step()
"""
p = subprocess.run([sys.executable,'-I','-c',child,sys.argv[1]], capture_output=True, timeout=6)
s = Store('catalog.sqlite')
state = dict(s.db.execute('SELECT * FROM search_state').fetchone())
entries = [row[0] for row in s.db.execute('SELECT generation FROM search_entries')]
recovered = s.reindex_step()
print(json.dumps({'killed':p.returncode == -signal.SIGKILL,'state':state,'entries':entries,
                  'recovered':recovered,'documents':len(s.documents()),'diagnostics':s.diagnostics()}))
s.close()
''')
        self.assertTrue(result['killed'])
        self.assertEqual(result['state']['published_generation'], 1)
        self.assertEqual(result['state']['target_generation'], 1)
        self.assertEqual(result['entries'], [1])
        self.assertEqual(result['recovered']['generation'], 2)
        self.assertEqual(result['recovered']['processed'], 2)
        self.assertEqual(result['documents'], 2)
        self.assertEqual(result['diagnostics']['index_state'], 'current')
