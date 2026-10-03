"""Real Store/CLI/HTTP qualification of trusted authored M4 client sources."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m3_clients_reference_v1 import clients_files as m3_clients_files
from gossip_harness.library_m4_clients_reference_v1 import clients_files


def _smoke(script: str) -> dict:
    # These bounded host checks execute checked-in authored sources, never candidates.
    from gossip_harness.library_m3_reference_v1 import m3_files
    from gossip_harness.library_m4_reference_v1 import m4_files
    with tempfile.TemporaryDirectory(prefix='trusted-library-m4-client-') as directory:
        root = Path(directory).resolve()
        for folder, files in (('app', m4_files()), ('old', m3_files())):
            for name, source in files.items():
                path = root / folder / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(source, encoding='utf-8')
        bootstrap = (
            'import json, resource, sys\n'
            'resource.setrlimit(resource.RLIMIT_CPU, (20, 20))\n'
            'resource.setrlimit(resource.RLIMIT_FSIZE, (8388608, 8388608))\n'
            'resource.setrlimit(resource.RLIMIT_NOFILE, (128, 128))\n'
            'sys.path.insert(0, sys.argv[1])\n'
        )
        result = subprocess.run([sys.executable, '-I', '-c', bootstrap + script, str(root / 'app')],
            cwd=root, capture_output=True, text=True, timeout=50, check=False)
        if result.returncode:
            raise AssertionError(f'Authored M4 client smoke failed: {result.stderr[:8000]}')
        if len(result.stdout.encode()) > 100000:
            raise AssertionError('Authored M4 client output exceeds bound')
        return json.loads(result.stdout)


class LibraryM4ClientsReferenceV1Tests(unittest.TestCase):
    def test_overlays_preserve_inherited_sources_and_compile(self):
        overlay = clients_files()
        legacy = m3_clients_files()
        for path, source in overlay.items():
            compile(source, path, 'exec')
        self.assertEqual(overlay['library/query/legacy_m3_service.py'], legacy['library/query/service.py'])
        self.assertEqual(overlay['library/clients/legacy_m3_cli.py'], legacy['library/clients/cli.py'])
        self.assertEqual(overlay['library/clients/http.py'], legacy['library/clients/http.py'])
        self.assertLess(sum(len(source.encode()) for source in overlay.values()), 75000)
        overlay['library/query/service.py'] = 'changed'
        self.assertNotEqual(clients_files()['library/query/service.py'], 'changed')

    def test_service_v1_routes_preserve_legacy_shapes_and_mutation_snapshot(self):
        value = _smoke(r'''
from pathlib import Path
import hashlib
from library.catalog.store import Store
from library.query.service import Service
root=Path('inputs');root.mkdir();(root/'refresh.html').write_text('雪 <script>',encoding='utf-8')
store=Store('service.sqlite3');service=Service(store,root)
id=store.insert('é.txt','café'.encode())['document']['document_id']
other=store.insert('other.txt',b'other')['document']['document_id']
health=service.request('GET','/health')
legacy0=service.request('GET','/api/documents/'+id)
legacy2=service.request('GET','/api/lifecycle/documents/'+id)
shown=service.request('GET','/api/v1/documents/'+id)
listing=service.request('GET','/api/v1/documents?offset=00&limit=001')
refreshed=service.request('POST','/api/v1/documents/'+id+'/refresh',{'expected_version':1,'path':'refresh.html'})
history=service.request('GET','/api/v1/documents/'+id+'/revisions')
rid=history[1]['revisions'][0]['revision_id']
single=service.request('GET','/api/v1/documents/'+id+'/revisions/'+rid)
wrong_document=service.request('GET','/api/v1/documents/'+other+'/revisions/'+rid)
wrong_token=service.request('POST','/api/v1/documents/'+id+'/delete',{'expected_version':rid})
annotated=service.request('POST','/api/v1/documents/'+id+'/annotations',
 {'expected_version':2,'notes':'literal <b>','tags':[' TAG '],'collections':[]})
unchanged=service.request('POST','/api/v1/documents/'+id+'/annotations',
 {'expected_version':3,'notes':'literal <b>','tags':['tag'],'collections':[]})
deleted=service.request('POST','/api/v1/documents/'+id+'/delete',{'expected_version':3})
restored=service.request('POST','/api/v1/documents/'+id+'/restore',{'expected_version':4})
# Interpose a real later edit after the first committed mutation snapshot.
# The route must never reread and mix that edit into the response.
class InterleavedService(Service):
 def refresh_document(self,*args,**kwargs):
  result=super().refresh_document(*args,**kwargs)
  self.store.replace_annotations(args[0],result['record']['edit_version'],'later',[],[])
  return result
raced=InterleavedService(store,root).request('POST','/api/v1/documents/'+id+'/refresh',
 {'expected_version':5,'text':'last'})
actual=service.show_v1(id)
legacy_history=service.request('GET','/api/lifecycle/documents/'+id+'/revisions')
expected_id='rev-'+hashlib.sha256(b'revision\0'+id.encode()+b'\0' + b'1\0' +
 shown[1]['current_revision']['blob_id'].encode()).hexdigest()
print(json.dumps({'health':health,'legacy0':legacy0,'legacy2':legacy2,'shown':shown,
 'listing':listing,'refreshed':refreshed,'history':history,'single':single,'expected_id':expected_id,
 'wrong_document':wrong_document,'wrong_token':wrong_token,'annotated':annotated,
 'unchanged':unchanged,'deleted':deleted,'restored':restored,'raced':raced,
 'actual':actual,'legacy_history':legacy_history}));store.close()
''')
        self.assertEqual(value['health'], [200, {'status': 'ok', 'schema': 4}])
        self.assertEqual(set(value['legacy0'][1]), {'document_id', 'source_id', 'source', 'blob_id', 'title', 'text'})
        self.assertEqual(set(value['legacy2'][1]), {'document', 'revision', 'edit_version', 'deleted', 'notes', 'tags', 'collections'})
        record_keys = {'document_id', 'source_id', 'source', 'title', 'current_revision', 'edit_version', 'deleted', 'notes', 'tags', 'collections'}
        revision_keys = {'revision_id', 'revision', 'blob_id', 'text'}
        self.assertEqual(set(value['shown'][1]), record_keys)
        self.assertEqual(set(value['shown'][1]['current_revision']), revision_keys)
        self.assertEqual(value['shown'][1]['current_revision']['revision_id'], value['expected_id'])
        self.assertEqual(value['listing'][1]['total'], 2)
        self.assertEqual(len(value['listing'][1]['records']), 1)
        self.assertEqual(value['refreshed'][1]['record']['current_revision']['text'], '雪 <script>')
        self.assertEqual(value['single'], [200, value['history'][1]['revisions'][0]])
        self.assertEqual(value['wrong_document'], [404, {'error': 'not_found'}])
        self.assertEqual(value['wrong_token'], [400, {'error': 'invalid_request'}])
        self.assertEqual(value['annotated'][1]['record']['tags'], ['tag'])
        self.assertEqual(value['unchanged'][1]['status'], 'unchanged')
        self.assertEqual(value['unchanged'][1]['record']['edit_version'], 3)
        self.assertTrue(value['deleted'][1]['record']['deleted'])
        self.assertFalse(value['restored'][1]['record']['deleted'])
        self.assertEqual(value['raced'][0], 200)
        self.assertEqual(value['raced'][1]['record']['edit_version'], 6)
        self.assertEqual(value['raced'][1]['record']['notes'], 'literal <b>')
        self.assertEqual(value['actual']['edit_version'], 7)
        self.assertEqual(value['actual']['notes'], 'later')
        self.assertTrue(all(set(row) == {'revision', 'blob_id', 'text'} for row in value['legacy_history'][1]['revisions']))

    def test_v1_strict_routes_and_canonical_export_limit(self):
        value = _smoke(r'''
from library.catalog.store import Store
from library.query.service import Service
store=Store('strict.sqlite3');service=Service(store,'.')
id=store.insert('snow.txt','雪 café'.encode())['document']['document_id']
service.refresh_document(id,1,text='second')
base='/api/v1/documents/'+id
body={'ids':None,'include_deleted':False,'include_history':True,'max_bytes':16777216}
export=service.request('POST','/api/v1/export',body)
parsed=json.loads(export[1]);raw=json.dumps(parsed,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
exact=service.request('POST','/api/v1/export',body|{'max_bytes':len(raw)})
small=service.request('POST','/api/v1/export',body|{'max_bytes':len(raw)-1})
empty=json.loads(service.request('POST','/api/v1/export',body|{'ids':[]})[1])
legacy=json.loads(service.request('POST','/api/export-bundle',body)[1])
cases=[
 ('GET','/api/v1/documents?offset=1&offset=2',None),
 ('GET','/api/v1/documents?x=1',None),
 ('GET','/api/v1/documents?limit=%2B1',None),
 ('GET','/api/v1/documents?offset=١',None),
 ('GET','/api/v1/documents?offset=%ZZ',None),
 ('GET','/api/v1/documents?q=%FF',None),
 ('GET','/api/v1/documents?limit=0',None),
 ('GET',base+'?q=x',None),
 ('GET',base+'/revisions?limit=1',None),
 ('GET',base+'/revisions',{}),
 ('POST',base+'/refresh',{'expected_version':2,'text':'x','path':'x.txt'}),
 ('POST',base+'/refresh',{'expected_version':2,'text':None}),
 ('POST',base+'/refresh',{'expected_version':2,'path':None}),
 ('POST',base+'/delete',{'expected_version':True}),
 ('POST',base+'/annotations',{'expected_version':2,'notes':'x','tags':[]}),
 ('POST','/api/v1/export',body|{'ids':[id,id]}),
 ('POST','/api/v1/export',body|{'include_history':1}),
 ('POST','/api/v1/export',body|{'max_bytes':True}),
 ('POST','/api/v1/export',body|{'extra':False}),
 ('POST','/api/v1/export?x=1',body),
]
invalid=[service.request(*case) for case in cases]
unknown=[service.request('GET','/api/v1/export'),service.request('POST','/api/v1/migrate',{}),
 service.request('POST',base+'/revisions',{}),service.request('GET',base+'/revisions/'),
 service.request('PUT',base+'/delete',{'expected_version':2})]
service.delete_document(id,2)
disallowed=service.request('POST','/api/v1/export',body|{'ids':[id]})
with_deleted=json.loads(service.request('POST','/api/v1/export',body|{'ids':[id],'include_deleted':True})[1])
missing=service.request('POST','/api/v1/export',body|{'ids':[id,'missing'],'include_deleted':True})
print(json.dumps({'canonical':export[1]==raw,'unicode':b'\xe9\x9b\xaa' in raw,
 'newline':raw.endswith(b'\n'),'exact':exact[0],'small':small,'parsed':parsed,'empty':empty,
 'legacy':legacy,'invalid':invalid,'unknown':unknown,'disallowed':disallowed,
 'with_deleted':with_deleted,'missing':missing}));store.close()
''')
        self.assertTrue(value['canonical'])
        self.assertTrue(value['unicode'])
        self.assertFalse(value['newline'])
        self.assertEqual(value['exact'], 200)
        self.assertEqual(value['small'], [400, {'error': 'too_large'}])
        self.assertEqual(value['parsed']['format'], 'local-research-library-export-v4')
        self.assertEqual(len(value['parsed']['documents'][0]['revisions']), 2)
        self.assertEqual(value['empty']['documents'], [])
        self.assertEqual(value['legacy']['format'], 'local-research-library-export-v2')
        self.assertIn('document', value['legacy']['documents'][0]['record'])
        self.assertEqual(value['invalid'], [[400, {'error': 'invalid_request'}]] * 20)
        self.assertEqual(value['unknown'], [[404, {'error': 'not_found'}]] * 5)
        self.assertEqual(value['disallowed'], [404, {'error': 'not_found'}])
        self.assertTrue(value['with_deleted']['documents'][0]['record']['deleted'])
        self.assertEqual(value['missing'], [404, {'error': 'not_found'}])

    def test_http_wire_v1_download_and_legacy_channels(self):
        value = _smoke(r'''
from http.server import HTTPServer
from http.client import HTTPConnection
import threading
from library.catalog.store import Store
from library.query.service import Service
from library.clients.http import handler_for
holder={};ready=threading.Event()
def run():
 store=Store('wire.sqlite3');store.insert('é.txt','雪 café'.encode())
 server=HTTPServer(('127.0.0.1',0),handler_for(Service(store,'.')))
 holder['server']=server;ready.set()
 try:server.serve_forever(poll_interval=.02)
 finally:server.server_close();store.close()
thread=threading.Thread(target=run);thread.start();assert ready.wait(3)
def request(method,path,raw=None,headers=None):
 client=HTTPConnection('127.0.0.1',holder['server'].server_port,timeout=3)
 client.request(method,path,raw,headers or {})
 response=client.getresponse();body=response.read();out=(response.status,body,dict(response.getheaders()))
 client.close();return out
headers={'Content-Type':'application/json'}
body={'ids':None,'include_deleted':False,'include_history':True,'max_bytes':16777216}
export=request('POST','/api/v1/export',json.dumps(body).encode(),headers)
parsed=json.loads(export[1]);canonical=json.dumps(parsed,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
health=request('GET','/health');legacy=request('GET','/api/export')
listing=request('GET','/api/v1/documents');id=json.loads(listing[1])['records'][0]['document_id']
mutation=request('POST','/api/v1/documents/'+id+'/refresh',b'{"expected_version":1,"text":"second"}',headers)
invalid=[request('POST','/api/v1/export',b'{"ids":null,"ids":[],"include_deleted":false,"include_history":true,"max_bytes":99}',headers),
 request('POST','/api/v1/export',b'{"ids":[],"include_deleted":false,"include_history":true,"max_bytes":1e999}',headers),
 request('POST','/api/v1/export',b'x'*65537,headers),
 request('POST','/api/v1/export',b'{}',{'Content-Type':'text/plain'}),
 request('POST','/api/v1/documents/'+id+'/refresh',b'{"expected_version":2,"text":"\xff"}',headers),
 request('GET','/api/v1/documents',b'x')]
tiny=request('POST','/api/v1/export',json.dumps(body|{'max_bytes':1}).encode(),headers)
holder['server'].shutdown();thread.join(3);assert not thread.is_alive()
print(json.dumps({'status':export[0],'canonical':export[1]==canonical,'length':export[2]['Content-Length'],
 'expected_length':len(canonical),'ctype':export[2]['Content-Type'],
 'health':[health[0],json.loads(health[1])],'legacy':json.loads(legacy[1]),
 'mutation':[mutation[0],json.loads(mutation[1])],
 'invalid':[[s,json.loads(b)] for s,b,h in invalid],'tiny':[tiny[0],json.loads(tiny[1])]}))
''')
        self.assertEqual(value['status'], 200)
        self.assertTrue(value['canonical'])
        self.assertEqual(int(value['length']), value['expected_length'])
        self.assertEqual(value['ctype'], 'application/json; charset=utf-8')
        self.assertEqual(value['health'], [200, {'status': 'ok', 'schema': 4}])
        self.assertEqual(value['legacy']['format'], 'local-research-library-v0')
        self.assertEqual(value['mutation'][0], 200)
        self.assertEqual(value['mutation'][1]['record']['current_revision']['text'], 'second')
        self.assertEqual(value['invalid'], [[400, {'error': 'invalid_request'}]] * 6)
        self.assertEqual(value['tiny'], [400, {'error': 'too_large'}])

    def test_fresh_cli_migration_and_opt_in_revision_forms(self):
        value = _smoke(r'''
from pathlib import Path
import subprocess
root=Path('inputs');root.mkdir();(root/'a.txt').write_text('café',encoding='utf-8')
backups=Path('chosen');backups.mkdir()
bootstrap="import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module('library',run_name='__main__')"
def cli(*args,folder='app'):
 command=[sys.executable,'-I','-c',bootstrap,str(Path(folder).resolve()),'--db','cli.sqlite3',
  '--root',str(root),'--backup-dir',str(backups)]+list(args)
 result=subprocess.run(command,capture_output=True,timeout=5)
 stream=result.stdout if result.returncode==0 else result.stderr
 try:value=json.loads(stream)
 except (ValueError,UnicodeError):value=None
 return {'exit':result.returncode,'value':value,'other_empty':not(result.stderr if result.returncode==0 else result.stdout),
  'raw':stream.decode('utf-8')}
created=cli('import','a.txt',folder='old');id=created['value']['document']['document_id']
cli('refresh',id,'--expected-version','1','--text','second',folder='old')
legacy_before=cli('document',id,folder='old')
migrated=cli('migrate');again=cli('migrate');oldshape=cli('document',id)
shown=cli('document-v1',id);listed=cli('documents-v1','--query','second','--offset','00','--limit','001')
history=cli('revisions-v1',id);rid=history['value']['revisions'][0]['revision_id']
single=cli('revisions-v1',id,'--revision-id',rid)
export=cli('export-v1','--include-history');v2=cli('export-bundle','--include-history');v0=cli('export')
backup=cli('backup','m4.json');deleted=cli('delete',id,'--expected-version','2')
tombstone=cli('export-v1',id,'--include-deleted','--include-history')
restored=cli('restore-backup','m4.json','--expected-generation','3')
diag=cli('diagnostics')
invalid=[cli('documents-v1','--offset',token) for token in ('+1','-1','１','1.0',' 1')]
invalid.extend([cli('documents-v1','--li','1'),cli('document-v1'),cli('revisions-v1',id,'--revision-id'),
 cli('export-v1','--max-bytes','1'),cli('migrate','--force')])
inherited_error=cli('list','--unknown')
print(json.dumps({'created':created,'legacy_before':legacy_before,'migrated':migrated,'again':again,
 'oldshape':oldshape,'shown':shown,'listed':listed,'history':history,'single':single,
 'export':export,'v2':v2,'v0':v0,'backup':backup,'deleted':deleted,'tombstone':tombstone,
 'restored':restored,'diag':diag,'invalid':invalid,'inherited_error':inherited_error,
 'default_created':Path('backups').exists()}))
''')
        for key in ('created', 'migrated', 'again', 'oldshape', 'shown', 'listed', 'history', 'single',
                    'export', 'v2', 'v0', 'backup', 'deleted', 'tombstone', 'restored', 'diag'):
            self.assertEqual(value[key]['exit'], 0, (key, value[key]))
            self.assertTrue(value[key]['other_empty'], key)
        self.assertEqual(value['migrated']['value'], {'from_schema': 3, 'to_schema': 4, 'migrated': True, 'documents': 1, 'jobs': 0})
        self.assertEqual(value['again']['value'], {'from_schema': 4, 'to_schema': 4, 'migrated': False, 'documents': 1, 'jobs': 0})
        self.assertEqual(value['oldshape']['value'], value['legacy_before']['value'])
        self.assertEqual(value['listed']['value']['records'], [value['shown']['value']])
        self.assertEqual(value['single']['value'], value['history']['value']['revisions'][0])
        self.assertEqual(value['export']['value']['format'], 'local-research-library-export-v4')
        canonical = json.dumps(value['export']['value'], ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        self.assertEqual(value['export']['raw'], canonical)
        self.assertEqual(value['v2']['value']['format'], 'local-research-library-export-v2')
        self.assertEqual(value['v0']['value']['format'], 'local-research-library-v0')
        self.assertIn('document', value['deleted']['value']['record'])
        self.assertTrue(value['tombstone']['value']['documents'][0]['record']['deleted'])
        self.assertEqual(value['diag']['value']['schema'], 4)
        self.assertFalse(value['default_created'])
        for index, item in enumerate(value['invalid']):
            self.assertEqual(item['exit'], 2, (index, item))
            self.assertTrue(item['other_empty'], index)
            self.assertEqual(item['value'], {'error': 'too_large' if index == 8 else 'invalid_request'}, index)
        self.assertEqual(value['inherited_error']['exit'], 2)
        self.assertIsNone(value['inherited_error']['value'])
        self.assertIn('unrecognized arguments', value['inherited_error']['raw'])

    def test_migrate_cli_preserves_copied_m3_backup_ownership_and_rows(self):
        value = _smoke(r'''
from pathlib import Path
import shutil,sqlite3,subprocess
origin=Path('original');origin.mkdir();oldbackups=origin/'backups';oldbackups.mkdir()
newbackups=Path('different-backups');newbackups.mkdir()
seed="""
from pathlib import Path
from library.catalog.store import Store
from library.query.service import Service
store=Store('original/catalog.sqlite3',backup_dir='original/backups')
service=Service(store,'.',backup_dir='original/backups')
id=store.insert('a.txt',b'preserve')['document']['document_id']
service.submit_job({'job_id':'queued','entries':[{'source':'worker.txt','text':'waiting'}]})
service.enqueue_job('queued')
service.backup('registered.json')
store.close()
"""
result=subprocess.run([sys.executable,'-I','-c',
 'import sys;sys.path.insert(0,sys.argv[1]);'+seed,str(Path('old').resolve())],capture_output=True,timeout=5)
assert result.returncode==0,result.stderr
copy=Path('relocated');copy.mkdir();database=copy/'catalog.sqlite3'
shutil.copy2(origin/'catalog.sqlite3',database)
def persistent_rows():
 db=sqlite3.connect(database)
 try:
  return {table:db.execute('SELECT * FROM '+table+' ORDER BY 1').fetchall()
   for table in ('metadata','backups','control','jobs','job_control','document_control','maintenance_artifacts','search_state','search_entries')}
 finally:db.close()
before=persistent_rows()
def preserved_view(rows):
 # Schema3 migration rotates incarnation to fence pre-migration writers.
 return rows | {'control':[row for row in rows['control'] if row[0]!='incarnation'],
  'metadata':[row for row in rows['metadata'] if row[0]!='schema']}
bootstrap="import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module('library',run_name='__main__')"
command=[sys.executable,'-I','-c',bootstrap,str(Path('app').resolve()),'--db',str(database),
 '--backup-dir',str(newbackups),'migrate']
first=subprocess.run(command,capture_output=True,timeout=5);after_first=persistent_rows()
second=subprocess.run(command,capture_output=True,timeout=5);after_second=persistent_rows()
print(json.dumps({'first_exit':first.returncode,'first':json.loads(first.stdout or first.stderr),
 'second_exit':second.returncode,'second':json.loads(second.stdout or second.stderr),
 'first_stderr_empty':not first.stderr,'second_stderr_empty':not second.stderr,
 'preserved_first':preserved_view(before)==preserved_view(after_first),'preserved_second':after_first==after_second,
 'registered':before['backups'],'no_new_backups':not list(newbackups.iterdir())}))
''')
        self.assertEqual(value['first_exit'], 0)
        self.assertEqual(value['second_exit'], 0)
        self.assertEqual(value['first'], {'from_schema': 3, 'to_schema': 4, 'migrated': True, 'documents': 1, 'jobs': 1})
        self.assertEqual(value['second'], {'from_schema': 4, 'to_schema': 4, 'migrated': False, 'documents': 1, 'jobs': 1})
        self.assertTrue(value['first_stderr_empty'])
        self.assertTrue(value['second_stderr_empty'])
        self.assertTrue(value['preserved_first'])
        self.assertTrue(value['preserved_second'])
        self.assertEqual(value['registered'][0][0], 'registered.json')
        self.assertTrue(value['no_new_backups'])


if __name__ == '__main__':
    unittest.main()
