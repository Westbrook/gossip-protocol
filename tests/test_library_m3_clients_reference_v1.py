"""Bounded real-client smoke checks for trusted authored M3 reference sources."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m2_clients_reference_v1 import clients_files as m2_clients_files
from gossip_harness.library_m3_clients_reference_v1 import clients_files


def _smoke(script: str) -> dict:
    # This helper executes checked-in authored source only, never candidates.
    from gossip_harness.library_m3_reference_v1 import m3_files
    with tempfile.TemporaryDirectory(prefix='trusted-library-m3-client-') as directory:
        root = Path(directory).resolve()
        for name, source in m3_files().items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding='utf-8')
        bootstrap = (
            'import json, resource, sys\n'
            'resource.setrlimit(resource.RLIMIT_CPU, (20, 20))\n'
            'resource.setrlimit(resource.RLIMIT_FSIZE, (8388608, 8388608))\n'
            'resource.setrlimit(resource.RLIMIT_NOFILE, (128, 128))\n'
            'sys.path.insert(0, sys.argv[1])\n'
        )
        result = subprocess.run([sys.executable, '-I', '-c', bootstrap + script, str(root)],
            cwd=root, capture_output=True, text=True, timeout=45, check=False)
        if result.returncode:
            raise AssertionError(f'Authored M3 client smoke failed: {result.stderr[:6000]}')
        if len(result.stdout.encode()) > 65536:
            raise AssertionError('Authored M3 client output exceeds bound')
        return json.loads(result.stdout)


class LibraryM3ClientsReferenceV1Tests(unittest.TestCase):
    def test_overlays_preserve_inherited_sources_and_compile(self):
        overlay = clients_files()
        legacy = m2_clients_files()
        for path, source in overlay.items():
            compile(source, path, 'exec')
        self.assertEqual(overlay['library/query/legacy_m2_service.py'], legacy['library/query/service.py'])
        self.assertEqual(overlay['library/clients/legacy_m2_cli.py'], legacy['library/clients/cli.py'])
        self.assertEqual(overlay['library/clients/http.py'], legacy['library/clients/http.py'])
        self.assertLess(sum(len(source.encode()) for source in overlay.values()), 70000)
        overlay['library/query/service.py'] = 'changed'
        self.assertNotEqual(clients_files()['library/query/service.py'], 'changed')

    def test_service_routes_use_durable_operations_and_closed_shapes(self):
        value = _smoke(r'''
from pathlib import Path
from library.catalog.store import Store
from library.query.service import Service
root=Path('inputs');root.mkdir();backups=Path('chosen-backups');backups.mkdir()
store=Store('service.sqlite3'); service=Service(store,root,backup_dir=backups)
id=store.insert('é.txt','café'.encode())['document']['document_id']
service.replace_annotations(id,1,'literal <script>',[' TAG '],[])
body={'ids':None,'include_deleted':False,'include_history':True,'max_bytes':16777216}
export=service.request('POST','/api/export-bundle',body)
parsed=json.loads(export[1]);expected=json.dumps(parsed,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
service.submit_job({'job_id':'queued','entries':[{'source':'worker.txt','text':'work'}]})
enqueued=service.request('POST','/api/maintenance/jobs/queued/enqueue',{})
unchanged=service.request('POST','/api/maintenance/jobs/queued/enqueue',{})
index=service.request('POST','/api/maintenance/reindex',{'limit':1})
backup=service.request('POST','/api/maintenance/backups',{'name':'first.json'})
listing=service.request('GET','/api/maintenance/backups?offset=00&limit=001')
existing=service.request('POST','/api/maintenance/backups',{'name':'first.json'})
missing=service.request('POST','/api/export-bundle',body|{'ids':['missing']})
cases=[
 ('GET','/api/maintenance/backups?offset=1&offset=2',None),
 ('GET','/api/maintenance/backups?unknown=x',None),
 ('GET','/api/maintenance/backups?limit=%2B1',None),
 ('GET','/api/maintenance/backups?offset=١',None),
 ('GET','/api/maintenance/backups?limit=0',None),
 ('GET','/api/maintenance/backups?limit=101',None),
 ('GET','/api/maintenance/diagnostics?x=1',None),
 ('GET','/api/maintenance/diagnostics',{}),
 ('POST','/api/maintenance/reindex',{'limit':True}),
 ('POST','/api/maintenance/reindex',{'limit':65}),
 ('POST','/api/maintenance/reindex',{'limit':1,'hook':'stop'}),
 ('POST','/api/maintenance/jobs/queued/enqueue',{'epoch':1}),
 ('POST','/api/maintenance/jobs/queued/enqueue?x=1',{}),
 ('POST','/api/maintenance/backups',{'name':'x.json','payload':{}}),
 ('POST','/api/maintenance/restore',{'payload':{},'expected_generation':2}),
 ('POST','/api/maintenance/restore',{'name':'first.json','expected_generation':True}),
 ('POST','/api/export-bundle',body|{'include_history':1}),
 ('POST','/api/export-bundle',body|{'max_bytes':True}),
 ('POST','/api/export-bundle',body|{'ids':[id,id]}),
 ('POST','/api/export-bundle',body|{'extra':False}),
 ('POST','/api/export-bundle?x=1',body),
 ('GET','/api/maintenance/backups?offset=%ZZ',None),
 ('GET','/api/maintenance/backups?offset=%FF',None),
]
invalid=[service.request(*case) for case in cases]
unknown=[service.request('POST','/api/maintenance/worker',{}),
 service.request('GET','/api/export-bundle'),
 service.request('PUT','/api/maintenance/backups',{}),
 service.request('POST','/api/maintenance/jobs/queued%2Fother/enqueue',{})]
small=service.request('POST','/api/export-bundle',body|{'max_bytes':len(expected)-1})
exact=service.request('POST','/api/export-bundle',body|{'max_bytes':len(expected)})
diag=service.request('GET','/api/maintenance/diagnostics')
legacy=service.request('GET','/api/export')
print(json.dumps({'status':export[0],'canonical':export[1]==expected,'newline':export[1].endswith(b'\n'),
 'unicode':b'caf\xc3\xa9' in export[1],'parsed':parsed,'enqueued':enqueued,'unchanged':unchanged,
 'index':index,'backup':backup,'listing':listing,'existing':existing,'missing':missing,
 'invalid':invalid,'unknown':unknown,'small':small,'exact':exact[0], 'diagnostics':diag,
 'legacy':legacy,'default_created':Path('backups').exists()}));store.close()
''')
        self.assertEqual(value['status'], 200)
        self.assertTrue(value['canonical'])
        self.assertTrue(value['unicode'])
        self.assertFalse(value['newline'])
        self.assertEqual(value['parsed']['documents'][0]['record']['tags'], ['tag'])
        self.assertEqual(value['enqueued'][0], 200)
        self.assertEqual(value['enqueued'][1]['status'], 'enqueued')
        self.assertEqual(value['unchanged'][1]['status'], 'unchanged')
        self.assertEqual(value['index'][0], 200)
        self.assertEqual(value['backup'][0], 200)
        self.assertEqual(value['listing'][1], {'backups': [value['backup'][1]], 'total': 1})
        self.assertEqual(value['existing'], [409, {'error': 'already_exists'}])
        self.assertEqual(value['missing'], [404, {'error': 'not_found'}])
        self.assertEqual(value['invalid'], [[400, {'error': 'invalid_request'}]] * 23)
        # Encoded slashes stay inside one job component and fail the job ID shape.
        self.assertEqual(value['unknown'][:3], [[404, {'error': 'not_found'}]] * 3)
        self.assertEqual(value['unknown'][3], [400, {'error': 'invalid_request'}])
        self.assertEqual(value['small'], [400, {'error': 'too_large'}])
        self.assertEqual(value['exact'], 200)
        self.assertEqual(value['diagnostics'][0], 200)
        self.assertEqual(value['diagnostics'][1]['schema'], 3)
        self.assertEqual(value['legacy'][1]['format'], 'local-research-library-v0')
        self.assertFalse(value['default_created'])

    def test_http_wire_preserves_canonical_bytes_and_rejects_ambiguous_uploads(self):
        value = _smoke(r'''
from pathlib import Path
from http.server import HTTPServer
from http.client import HTTPConnection
import threading
from library.catalog.store import Store
from library.query.service import Service
from library.clients.http import handler_for
holder={};ready=threading.Event()
def run():
 store=Store('wire.sqlite3');store.insert('é.txt','雪 café'.encode())
 service=Service(store,'.');server=HTTPServer(('127.0.0.1',0),handler_for(service))
 holder.update(server=server);ready.set()
 try:server.serve_forever(poll_interval=.02)
 finally:server.server_close();store.close()
thread=threading.Thread(target=run);thread.start();assert ready.wait(3)
def request(method,target,raw=None,headers=None):
 client=HTTPConnection('127.0.0.1',holder['server'].server_port,timeout=3)
 client.request(method,target,raw,headers or {})
 response=client.getresponse();body=response.read();result=(response.status,body,dict(response.getheaders()))
 client.close();return result
headers={'Content-Type':'application/json'}
body={'ids':None,'include_deleted':False,'include_history':True,'max_bytes':16777216}
valid=request('POST','/api/export-bundle',json.dumps(body).encode(),headers)
parsed=json.loads(valid[1]);canonical=json.dumps(parsed,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
tiny=request('POST','/api/export-bundle',json.dumps(body|{'max_bytes':1}).encode(),headers)
invalid=[
 request('POST','/api/export-bundle',b'{"ids":null,"ids":[],"include_deleted":false,"include_history":false,"max_bytes":9}',headers),
 request('POST','/api/maintenance/reindex',b'{"limit":NaN}',headers),
 request('POST','/api/maintenance/reindex',b'{"limit":1e999}',headers),
 request('POST','/api/maintenance/backups',b'{"name":"\xff"}',headers),
 request('POST','/api/maintenance/restore',b'x'*65537,headers),
 request('POST','/api/maintenance/reindex',b'{"limit":1}',{'Content-Type':'text/plain'}),
 request('GET','/api/maintenance/diagnostics',b'x'),
 request('POST','/api/maintenance/worker',b'{}',headers),
]
legacy=request('GET','/api/export')
holder['server'].shutdown();thread.join(3);assert not thread.is_alive()
print(json.dumps({'valid_status':valid[0],'canonical':valid[1]==canonical,
 'length':valid[2]['Content-Length']==str(len(canonical)), 'ctype':valid[2]['Content-Type'],
 'unicode':b'\xe9\x9b\xaa' in valid[1], 'newline':valid[1].endswith(b'\n'),
 'tiny':[tiny[0],json.loads(tiny[1])], 'invalid':[[s,json.loads(b)] for s,b,h in invalid],
 'legacy':[legacy[0],json.loads(legacy[1])]}))
''')
        self.assertEqual(value['valid_status'], 200)
        self.assertTrue(value['canonical'])
        self.assertTrue(value['length'])
        self.assertTrue(value['unicode'])
        self.assertFalse(value['newline'])
        self.assertEqual(value['ctype'], 'application/json; charset=utf-8')
        self.assertEqual(value['tiny'], [400, {'error': 'too_large'}])
        self.assertEqual(value['invalid'][:7], [[400, {'error': 'invalid_request'}]] * 7)
        self.assertEqual(value['invalid'][7], [404, {'error': 'not_found'}])
        self.assertEqual(value['legacy'][1]['format'], 'local-research-library-v0')

    def test_fresh_cli_worker_backup_restore_and_export_channels(self):
        value = _smoke(r'''
from pathlib import Path
import subprocess
root=Path('input');root.mkdir();(root/'a.txt').write_text('café',encoding='utf-8')
backups=Path('chosen');backups.mkdir()
bootstrap="import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module('library',run_name='__main__')"
def cli(*args,configured=True):
 command=[sys.executable,'-I','-c',bootstrap,str(Path.cwd()),'--db','cli.sqlite3','--root',str(root)]
 if configured:command+=['--backup-dir',str(backups)]
 result=subprocess.run(command+list(args),capture_output=True,timeout=5)
 stream=result.stdout if result.returncode==0 else result.stderr
 try:value=json.loads(stream)
 except (ValueError,UnicodeError):value=None
 return {'exit':result.returncode,'value':value,'other_empty':not(result.stderr if result.returncode==0 else result.stdout),
  'raw':stream.decode('utf-8')}
created=cli('import','a.txt',configured=False);id=created['value']['document']['document_id']
no_default=not Path('backups').exists()
export=cli('export-bundle','--include-history')
legacy=cli('export')
refreshed=cli('refresh',id,'--expected-version','1','--text','updated')
backup=cli('backup','snapshot.json')
again=cli('backup','snapshot.json')
listed=cli('backups','--offset','00','--limit','001')
deleted=cli('delete',id,'--expected-version','2')
restored=cli('restore-backup','snapshot.json','--expected-generation','3')
stale=cli('restore-backup','snapshot.json','--expected-generation','3')
shown=cli('document',id)
reindex=cli('reindex','--limit','064')
diag=cli('diagnostics')
# Use the exact public submit API to prepare a durable job for a fresh CLI worker.
from library.catalog.store import Store
from library.query.service import Service
store=Store('cli.sqlite3');Service(store,root).submit_job({'job_id':'background',
 'entries':[{'source':'worker.txt','text':'background'}]});store.close()
enqueued=cli('worker-enqueue','background');processed=cli('worker','--once');empty=cli('worker','--once')
worker_doc=cli('search','background')
invalid=[cli('backups','--offset',token) for token in ('+1','-1','１','1.0',' 1')]
invalid.extend([cli('reindex'),cli('reindex','--limit','0'),cli('backups','--limit','101'),
 cli('restore-backup','snapshot.json'),cli('worker','--hook','stop'),cli('backup','snapshot.json','--payload','x'),
 cli('export-bundle','--max-bytes','1'),cli('backups','--li','1')])
# Legacy parser errors retain their inherited argparse channel even with new global flags.
inherited_error=cli('list','--unknown')
print(json.dumps({'created':created,'no_default':no_default,'export':export,'legacy':legacy,
 'refreshed':refreshed,'backup':backup,'again':again,'listed':listed,'deleted':deleted,
 'restored':restored,'stale':stale,'shown':shown,'reindex':reindex,'diagnostics':diag,
 'enqueued':enqueued,'processed':processed,'empty':empty,'worker_doc':worker_doc,
 'invalid':invalid,'inherited_error':inherited_error}))
''')
        for key in ('created', 'export', 'legacy', 'refreshed', 'backup', 'listed', 'deleted',
                    'restored', 'shown', 'reindex', 'diagnostics', 'enqueued', 'processed',
                    'empty', 'worker_doc'):
            self.assertEqual(value[key]['exit'], 0, (key, value[key]))
            self.assertTrue(value[key]['other_empty'], key)
        self.assertTrue(value['no_default'])
        expected = json.dumps(value['export']['value'], ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        self.assertEqual(value['export']['raw'], expected)
        self.assertIn('café', expected)
        self.assertEqual(value['legacy']['value']['format'], 'local-research-library-v0')
        self.assertEqual(value['again']['value'], {'error': 'already_exists'})
        self.assertEqual(value['stale']['value'], {'error': 'stale_generation'})
        self.assertEqual(value['listed']['value'], {'backups': [value['backup']['value']], 'total': 1})
        self.assertEqual(value['restored']['value']['generation'], 4)
        self.assertEqual(value['shown']['value']['document']['text'], 'updated')
        self.assertFalse(value['shown']['value']['deleted'])
        self.assertEqual(value['processed']['value']['processed'], 'background')
        self.assertEqual(value['processed']['value']['job']['state'], 'completed')
        self.assertEqual(value['empty']['value'], {'processed': None, 'job': None})
        self.assertEqual(value['worker_doc']['value']['total'], 1)
        for index, invalid in enumerate(value['invalid']):
            self.assertEqual(invalid['exit'], 2, (index, invalid))
            self.assertTrue(invalid['other_empty'], index)
            self.assertEqual(invalid['value'], {'error': 'too_large' if index == 11 else 'invalid_request'}, index)
        self.assertEqual(value['inherited_error']['exit'], 2)
        self.assertTrue(value['inherited_error']['other_empty'])
        self.assertIsNone(value['inherited_error']['value'])
        self.assertIn('unrecognized arguments', value['inherited_error']['raw'])
