"""Bounded real-client smoke for trusted authored M2 sources only.

This is development verification, not independent acceptance and not permission
to run arbitrary candidate code on the host.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m1_reference_v1 import m1_files
from gossip_harness.library_m2_catalog_reference_v1 import catalog_files
from gossip_harness.library_m2_clients_reference_v1 import clients_files


def _smoke(script: str) -> dict:
    with tempfile.TemporaryDirectory(prefix='trusted-library-m2-client-') as directory:
        root = Path(directory).resolve()
        for name, source in (m1_files() | catalog_files() | clients_files()).items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding='utf-8')
        bootstrap = (
            "import json, resource, sys\n"
            "resource.setrlimit(resource.RLIMIT_CPU, (12, 12))\n"
            "resource.setrlimit(resource.RLIMIT_FSIZE, (8388608, 8388608))\n"
            "resource.setrlimit(resource.RLIMIT_NOFILE, (96, 96))\n"
            "sys.path.insert(0, sys.argv[1])\n"
        )
        result = subprocess.run(
            [sys.executable, '-I', '-c', bootstrap + script, str(root)], cwd=root,
            capture_output=True, text=True, timeout=25, check=False,
        )
        if result.returncode:
            raise AssertionError(f'Authored M2 client smoke failed: {result.stderr[:6000]}')
        if len(result.stdout.encode()) > 65536:
            raise AssertionError('Authored M2 client output exceeds bound')
        return json.loads(result.stdout)


class LibraryM2ClientsReferenceV1Tests(unittest.TestCase):
    def test_overlays_are_fresh_compilable_and_preserve_legacy_service(self):
        overlay = clients_files()
        for path, source in overlay.items():
            compile(source, path, 'exec')
        self.assertEqual(overlay['library/query/legacy_m1_service.py'],
                         m1_files()['library/query/service.py'])
        self.assertLess(sum(len(value.encode()) for value in overlay.values()), 50000)
        overlay['library/query/service.py'] = 'changed'
        self.assertNotEqual(clients_files()['library/query/service.py'], 'changed')

    def test_service_mutations_share_catalog_and_preserve_legacy_shapes(self):
        value = _smoke(r'''
from pathlib import Path
from library.catalog.store import Store
from library.query.service import Service
root = Path('inputs'); root.mkdir(); (root/'source.txt').write_text('old')
store = Store('library.sqlite3'); service = Service(store, root)
status, imported = service.request('POST','/api/import',{'source':'source.txt'})
id = imported['document']['document_id']
record = service.lifecycle_show(id)
collection = service.create_collection(' Work ', 1)
annotated = service.replace_annotations(id, 1, '<b>literal</b>', [' Tag '], ['WORK'])
refreshed = service.refresh_document(id, 2, text='new\n')
legacy = service.request('GET','/api/documents/' + id)
conflict = service.request('POST','/api/import',{'source':'source.txt'})
deleted = service.delete_document(id, 3)
hidden = service.request('GET','/api/documents')
missing = service.request('GET','/api/documents/' + id)
all_rows = service.lifecycle_list(deleted='all',tag='TAG',collection='work')
restored = service.restore_document(id, 4)
history = service.revision_history(id)
store.close(); store = Store('library.sqlite3')
persisted = Service(store,root).lifecycle_show(id)
print(json.dumps({'import_status':status,'record':record,'collection':collection,
 'annotated':annotated,'refreshed':refreshed,'legacy':legacy,'conflict':conflict,
 'deleted':deleted,'hidden':hidden,'missing':missing,'all':all_rows,'restored':restored,
 'history':history,'persisted':persisted}))
store.close()
''')
        self.assertEqual(value['import_status'], 200)
        self.assertEqual(set(value['record']['document']),
                         {'document_id', 'source_id', 'source', 'blob_id', 'title', 'text'})
        self.assertEqual(value['collection'], {'status': 'created', 'name': 'work', 'generation': 2})
        self.assertEqual(value['annotated']['record']['tags'], ['tag'])
        self.assertEqual(value['refreshed']['record']['revision'], 2)
        self.assertEqual(value['refreshed']['record']['document']['text'], 'new\n')
        self.assertEqual(value['legacy'][1], value['refreshed']['record']['document'])
        self.assertEqual(value['conflict'], [409, {'error': 'source_changed'}])
        self.assertEqual(value['hidden'], [200, {'documents': [], 'total': 0}])
        self.assertEqual(value['missing'], [404, {'error': 'not_found'}])
        self.assertEqual(value['all']['total'], 1)
        self.assertTrue(value['all']['records'][0]['deleted'])
        self.assertEqual(value['persisted'], value['restored']['record'])
        self.assertEqual([r['text'] for r in value['history']['revisions']], ['old', 'new\n'])

    def test_refresh_decodes_bounded_inputs_without_changing_provenance(self):
        value = _smoke(r'''
from pathlib import Path
from library.catalog.store import Store
from library.query.service import Service
from library.common import LibraryError
root = Path('inputs').resolve(); root.mkdir(); (root/'replacement.html').write_bytes(b'<b>raw</b>\r\n')
(root/'bad.txt').write_bytes(b'\xff'); (root/'big.txt').write_bytes(b'x'*32769)
(root/'unsupported.pdf').write_bytes(b'text'); (root/'directory.txt').mkdir()
(root/'link.txt').symlink_to(root/'replacement.html')
(root/'linked').symlink_to(root, target_is_directory=True)
alias = Path('alias'); alias.symlink_to(root, target_is_directory=True)
store = Store('library.sqlite3'); id = store.insert('original.txt',b'old')['document']['document_id']
service = Service(store,root)
def code(call):
 try: call(); return None
 except LibraryError as error: return error.code
before = service.lifecycle_show(id)['document']
result = service.refresh_document(id,1,path='replacement.html')
errors = {p:code(lambda p=p: service.refresh_document(id,2,path=p)) for p in
 ['../replacement.html','/tmp/x.txt','link.txt','linked/replacement.html','bad.txt','big.txt',
  'unsupported.pdf','directory.txt','missing.txt','a//b.txt','a\\b.txt']}
errors['root_symlink'] = code(lambda: Service(store,alias).refresh_document(id,2,path='replacement.html'))
errors['ancestor_symlink'] = code(lambda: Service(store,alias/'nested').refresh_document(id,2,path='x.txt'))
errors['surrogate'] = code(lambda: service.refresh_document(id,2,text='\ud800'))
errors['large_utf8'] = code(lambda: service.refresh_document(id,2,text='é'*16385))
errors['both'] = code(lambda: service.refresh_document(id,2,text='',path='replacement.html'))
errors['neither'] = code(lambda: service.refresh_document(id,2))
errors['bool'] = code(lambda: service.refresh_document(id,True,text='anything'))
print(json.dumps({'before':before,'result':result,'errors':errors}))
store.close()
''')
        self.assertEqual(value['result']['record']['document']['text'], '<b>raw</b>\r\n')
        for key in ('document_id', 'source_id', 'source', 'title'):
            self.assertEqual(value['before'][key], value['result']['record']['document'][key])
        errors = value['errors']
        for path in ('../replacement.html', '/tmp/x.txt', 'link.txt', 'linked/replacement.html',
                     'a//b.txt', 'a\\b.txt', 'root_symlink', 'ancestor_symlink'):
            self.assertEqual(errors[path], 'invalid_source', path)
        self.assertEqual(errors['bad.txt'], 'invalid_utf8')
        self.assertEqual(errors['surrogate'], 'invalid_utf8')
        for path in ('big.txt', 'large_utf8'):
            self.assertEqual(errors[path], 'too_large')
        self.assertEqual(errors['unsupported.pdf'], 'unsupported_type')
        for path in ('directory.txt', 'missing.txt'):
            self.assertEqual(errors[path], 'io_error')
        for path in ('both', 'neither', 'bool'):
            self.assertEqual(errors[path], 'invalid_request')

    def test_route_shapes_encoding_pagination_and_status_maps(self):
        value = _smoke(r'''
from library.catalog.store import Store
from library.query.service import Service
store=Store('library.sqlite3'); service=Service(store,'.')
id=store.insert('a.txt',b'a')['document']['document_id']; prefix='/api/lifecycle/documents/'+id
cases = [
 ('GET','/api/lifecycle/documents?q=%ff',None),
 ('GET','/api/lifecycle/documents?q=%ZZ',None),
 ('GET','/api/lifecycle/documents?q=one&q=two',None),
 ('GET','/api/lifecycle/documents?offset=%2B1',None),
 ('GET','/api/lifecycle/documents?limit=1e2',None),
 ('GET','/api/lifecycle/documents?offset=١',None),
 ('GET','/api/lifecycle/documents?unknown=1',None),
 ('GET',prefix+'?q=x',None),
 ('GET',prefix,{}),
 ('POST',prefix+'/refresh',{'expected_version':1,'text':None}),
 ('POST',prefix+'/refresh',{'expected_version':1,'text':'a','path':'a.txt'}),
 ('POST',prefix+'/delete',{'expected_version':True}),
 ('POST',prefix+'/delete',{'expected_version':1,'extra':1}),
 ('POST','/api/lifecycle/collections',{'name':'n','expected_generation':True}),
 ('POST',prefix+'/annotations',{'expected_version':1,'notes':'','tags':[],'collections':['absent']}),
 ('GET','/api/lifecycle/documents?generation=0',None),
 ('POST',prefix+'/delete',{'expected_version':2}),
 ('PUT',prefix,None),('GET','/api/lifecycle/unknown?foo=x',None),
 ('POST','/api/lifecycle/documents?q=x',{}),
 ('GET',prefix+'%2Frevisions',None),
 ('GET',prefix+'%252Frevisions',None),
]
results=[service.request(*case) for case in cases]
page=service.request('GET','/api/lifecycle/documents?offset=00&limit=001&generation=01')
service.delete_document(id,1)
tombstone=service.request('POST',prefix+'/refresh',{'expected_version':2,'text':'b'})
stale=service.request('POST',prefix+'/refresh',{'expected_version':1,'text':'b'})
print(json.dumps({'results':results,'page':page,'tombstone':tombstone,'stale':stale}))
store.close()
''')
        self.assertEqual(value['results'][:14], [[400, {'error': 'invalid_request'}]] * 14)
        self.assertEqual(value['results'][14], [400, {'error': 'collection_not_found'}])
        self.assertEqual(value['results'][15:17], [[409, {'error': 'stale_generation'}],
                                                   [409, {'error': 'stale_version'}]])
        self.assertEqual(value['results'][17:], [[404, {'error': 'not_found'}]] * 5)
        self.assertEqual(value['page'][0], 200)
        self.assertEqual(value['page'][1]['total'], 1)
        self.assertEqual(value['tombstone'], [409, {'error': 'document_deleted'}])
        self.assertEqual(value['stale'], [409, {'error': 'stale_version'}])

    def test_real_http_wire_rejects_ambiguous_json_and_transport(self):
        value = _smoke(r'''
from http.server import HTTPServer
from http.client import HTTPConnection
import threading
from library.catalog.store import Store
from library.query.service import Service
from library.clients.http import handler_for
holder={}; ready=threading.Event()
def run():
 store=Store('wire.sqlite3'); service=Service(store,'.')
 id=store.insert('wire.txt',b'wire')['document']['document_id']
 server=HTTPServer(('127.0.0.1',0),handler_for(service))
 holder.update(server=server,id=id); ready.set()
 try: server.serve_forever(poll_interval=0.02)
 finally: server.server_close();store.close()
thread=threading.Thread(target=run);thread.start();assert ready.wait(3)
port=holder['server'].server_port; route='/api/lifecycle/documents/'+holder['id']+'/refresh'
def request(method,target,body=None,headers=None):
 client=HTTPConnection('127.0.0.1',port,timeout=3)
 client.request(method,target,body=body,headers=headers or {})
 response=client.getresponse();raw=response.read();result=[response.status,json.loads(raw)]
 client.close();return result
json_headers={'Content-Type':'application/json'}
cases=[
 request('POST',route,b'{"expected_version":1,"expected_version":2,"text":"x"}',json_headers),
 request('POST',route,b'{"expected_version":1,"text":NaN}',json_headers),
 request('POST',route,b'{"expected_version":1,"text":1e999}',json_headers),
 request('POST',route,b'{"expected_version":1,"text":"\xff"}',json_headers),
 request('POST',route,b'{"expected_version":1,"text":"x"}',{'Content-Type':'text/plain'}),
 request('POST',route,b'x'*65537,json_headers),
 request('GET','/api/lifecycle/documents?q=%FF'),
 request('GET','/api/lifecycle/documents',b'x'),
 request('PATCH',route),request('BIZARRE',route),
 request('POST',route,b'{}',json_headers|{'Transfer-Encoding':'chunked'}),
]
client=HTTPConnection('127.0.0.1',port,timeout=3)
client.putrequest('POST',route);client.putheader('Content-Type','application/json')
client.putheader('Content-Length','2');client.putheader('Content-Length','2');client.endheaders(b'{}')
response=client.getresponse();cases.append([response.status,json.loads(response.read())]);client.close()
valid=request('POST',route,b'{"expected_version":1,"text":"changed"}',json_headers)
legacy=request('GET','/api/documents')
holder['server'].shutdown();thread.join(3);assert not thread.is_alive()
print(json.dumps({'cases':cases,'valid':valid,'legacy':legacy}))
''')
        for index, result in enumerate(value['cases']):
            self.assertEqual(result, [404, {'error': 'not_found'}] if index in (8, 9)
                             else [400, {'error': 'invalid_request'}], index)
        self.assertEqual(value['valid'][0], 200)
        self.assertEqual(value['valid'][1]['status'], 'refreshed')
        self.assertEqual(value['legacy'][1]['documents'][0]['text'], 'changed')

    def test_fresh_cli_processes_reopen_database_and_return_exact_json_channels(self):
        value = _smoke(r'''
from pathlib import Path
import subprocess
root=Path('inputs');root.mkdir();(root/'a.txt').write_text('first')
bootstrap="import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module('library',run_name='__main__')"
def cli(*args):
 result=subprocess.run([sys.executable,'-I','-c',bootstrap,str(Path.cwd()),'--db','cli.sqlite3',
  '--root',str(root),*args],capture_output=True,text=True,timeout=4)
 stream=result.stdout if result.returncode==0 else result.stderr
 return {'exit':result.returncode,'value':json.loads(stream),'other_empty':not (result.stderr if result.returncode==0 else result.stdout)}
created=cli('import','a.txt');id=created['value']['document']['document_id']
shown=cli('document',id)
collection=cli('collection-create',' Personal ','--expected-generation','01')
annotated=cli('annotate',id,'--expected-version','01','--notes','notes','--tag',' T ','--collection','Personal')
refreshed=cli('refresh',id,'--expected-version','2','--text','second')
revisions=cli('revisions',id)
page=cli('documents','--tag','t','--collection','personal','--generation','4','--limit','01')
deleted=cli('delete',id,'--expected-version','3')
legacy=cli('list')
restored=cli('restore-document',id,'--expected-version','4')
collections=cli('collections')
remove=cli('collection-remove','personal','--expected-generation','6')
invalid=[cli('documents','--offset',token) for token in ('+1','-1','１','1.0',' 1')]
invalid.extend([cli('refresh',id,'--expected-version','5'),
 cli('refresh',id,'--expected-version','5','--text','x','--path','a.txt'),
 cli('document',id,'--unknown'),cli('delete',id,'--expected','5'),
 cli('documents','--db'),cli('refresh',id,'--expected-version','5','--text','x','--root')])
print(json.dumps({'calls':[created,shown,collection,annotated,refreshed,revisions,page,deleted,legacy,restored,collections],
 'remove':remove,'invalid':invalid}))
''')
        self.assertTrue(all(call['exit'] == 0 and call['other_empty'] for call in value['calls']))
        self.assertEqual(value['calls'][4]['value']['record']['document']['text'], 'second')
        self.assertEqual(value['calls'][6]['value']['total'], 1)
        self.assertEqual(value['calls'][8]['value'], {'documents': [], 'total': 0})
        self.assertEqual(value['remove'], {'exit': 2, 'value': {'error': 'collection_not_empty'},
                                          'other_empty': True})
        self.assertEqual(value['invalid'], [{'exit': 2, 'value': {'error': 'invalid_request'},
                                             'other_empty': True}] * 11)

    def test_http_body_exact_limit_and_refresh_unchanged_are_atomic(self):
        value = _smoke(r'''
from http.server import HTTPServer
from http.client import HTTPConnection
import threading
from library.catalog.store import Store
from library.query.service import Service
from library.clients.http import handler_for
holder={};ready=threading.Event()
def serve():
 store=Store('limit.sqlite3'); service=Service(store,'.')
 id=store.insert('a.txt',b'a')['document']['document_id']
 server=HTTPServer(('127.0.0.1',0),handler_for(service));holder.update(server=server,id=id);ready.set()
 try:server.serve_forever(poll_interval=.02)
 finally:server.server_close();store.close()
thread=threading.Thread(target=serve);thread.start();assert ready.wait(3)
body=b'{"expected_version":1,"text":"a"}'
body+=b' '*(65536-len(body))
client=HTTPConnection('127.0.0.1',holder['server'].server_port,timeout=3)
client.request('POST','/api/lifecycle/documents/'+holder['id']+'/refresh',body,{'Content-Type':'application/json'})
response=client.getresponse();result=[response.status,json.loads(response.read())];client.close()
holder['server'].shutdown();thread.join(3);assert not thread.is_alive()
store=Store('limit.sqlite3');state=Service(store,'.').lifecycle_list();store.close()
print(json.dumps({'result':result,'state':state}))
''')
        self.assertEqual(value['result'][0], 200)
        self.assertEqual(value['result'][1]['status'], 'unchanged')
        self.assertEqual(value['state']['generation'], 1)
        self.assertEqual(value['state']['records'][0]['edit_version'], 1)


    def test_refresh_racing_leaf_replacement_cannot_follow_symlinks_or_block_on_fifo(self):
        value = _smoke(r"""
from pathlib import Path
import os
from library.catalog.store import Store
from library.query.service import Service
from library.common import LibraryError
root=Path('inputs').resolve();root.mkdir();target=root/'race.txt'
store=Store('race.sqlite3');id=store.insert('original.txt',b'a')['document']['document_id']
service=Service(store,root);original_stat=os.stat
results={}
for kind in ('symlink','fifo'):
 target.write_text('original')
 swapped=[False]
 def racing_stat(path,*args,**kwargs):
  result=original_stat(path,*args,**kwargs)
  if path=='race.txt' and kwargs.get('follow_symlinks') is False and not swapped[0]:
   swapped[0]=True;target.unlink()
   if kind=='symlink':target.symlink_to(root/'missing.txt')
   else:os.mkfifo(target)
  return result
 os.stat=racing_stat
 try:
  service.refresh_document(id,1,path='race.txt');results[kind]='unexpected_success'
 except LibraryError as error:results[kind]=error.code
 finally:os.stat=original_stat;target.unlink()
results['generation']=service.lifecycle_list()['generation']
results['record']=service.lifecycle_show(id)
print(json.dumps(results));store.close()
""")
        self.assertEqual(value['symlink'], 'invalid_source')
        self.assertEqual(value['fifo'], 'io_error')
        self.assertEqual(value['generation'], 1)
        self.assertEqual(value['record']['document']['text'], 'a')
