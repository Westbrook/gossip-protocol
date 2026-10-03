"""Prospective-v2 token/transport boundaries over generated authored modules.

Exploding Store controls prove validation occurs before database access. Full
real-store/release/browser qualification is a separate combined-root lane.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m4_reference_v1 import m4_files
from gossip_harness.library_v2_catalog_reference_v1 import catalog_files
from gossip_harness.library_v2_clients_reference_v1 import clients_files


def _smoke(script: str) -> dict:
    files = m4_files()
    files.update(catalog_files(files))
    files.update(clients_files(files))
    with tempfile.TemporaryDirectory(prefix="authored-v2-client-") as directory:
        root = Path(directory).resolve()
        for name, source in files.items():
            path = root / "app" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding="utf-8")
        bootstrap = (
            "import json,resource,sys\n"
            "resource.setrlimit(resource.RLIMIT_CPU,(10,10))\n"
            "resource.setrlimit(resource.RLIMIT_FSIZE,(8388608,8388608))\n"
            "resource.setrlimit(resource.RLIMIT_NOFILE,(96,96))\n"
            "sys.path.insert(0,sys.argv[1])\n"
        )
        result = subprocess.run([sys.executable, "-I", "-c", bootstrap + script, str(root / "app")],
                                cwd=root, capture_output=True, text=True, timeout=20, check=False)
        if result.returncode or len(result.stdout.encode()) > 100000:
            raise AssertionError(f"Authored v2 interface control failed: {result.stderr[:8000]}")
        return json.loads(result.stdout)


class LibraryV2ClientsReferenceV1Tests(unittest.TestCase):
    def test_overlay_preserves_base_and_generated_sources_compile(self):
        base = m4_files()
        before = dict(base)
        overlay = clients_files(base)
        self.assertEqual(base, before)
        self.assertEqual(set(overlay), {"library/query/legacy_v1_service.py", "library/query/service.py", "library/clients/cli.py"})
        self.assertEqual(overlay["library/query/legacy_v1_service.py"], base["library/query/service.py"])
        for path, source in overlay.items():
            compile(source, path, "exec")
        changed = dict(base)
        changed["library/clients/cli.py"] += "\ncommand.add_argument('epoch', type=int)\n"
        with self.assertRaises(ValueError):
            clients_files(changed)

    def test_all_python_tokens_fail_before_store_access(self):
        result = _smoke(r'''
from library.query.service import Service
from library.common import LibraryError
class Bomb:
 def configure_backup_dir(self, value): pass
 def __getattr__(self, name): raise AssertionError('database accessed: '+name)
service=Service(Bomb(),'.')
positive=[lambda v:service.refresh_document('id',v,path='missing.txt'),
 lambda v:service.replace_annotations('id',v,'',[],[]),lambda v:service.delete_document('id',v),
 lambda v:service.restore_document('id',v),lambda v:service.commit_job('job',v)]
nonnegative=[lambda v:service.lifecycle_list(generation=v),lambda v:service.list_v1(generation=v),
 lambda v:service.create_collection('name',v),lambda v:service.remove_collection('name',v),
 lambda v:service.restore_backup('backup.json',v)]
count=0
for functions,invalid in [(positive,[None,False,True,0,-1,1.0,'1',9223372036854775808]),
                         (nonnegative,[False,True,-1,0.0,'0',9223372036854775808])]:
 for function in functions:
  for value in invalid:
   try:function(value)
   except LibraryError as error:assert error.code=='invalid_request';count+=1
   else:raise AssertionError('token accepted')
print(json.dumps({'rejections':count}))
''')
        self.assertEqual(result, {"rejections": 70})

    def test_all_http_tokens_validate_before_store_access(self):
        result = _smoke(r'''
from library.query.service import Service
class Bomb:
 def configure_backup_dir(self, value): pass
 def __getattr__(self, name): raise AssertionError('database accessed: '+name)
service=Service(Bomb(),'.');out=[]
for prefix in ('/api/lifecycle/documents/id/','/api/v1/documents/id/'):
 for token in [9223372036854775808,False,1.0,'1',0]:
  for action,body in [('delete',{}),('restore',{}),('refresh',{'path':'missing.txt'}),
                       ('annotations',{'notes':'','tags':[],'collections':[]})]:
   out.append(service.request('POST',prefix+action,body|{'expected_version':token}))
for route in ('/api/lifecycle/collections','/api/lifecycle/collections/remove','/api/maintenance/restore'):
 for token in [9223372036854775808,True,-1,0.0,'0']:
  out.append(service.request('POST',route,{'name':'name','expected_generation':token}))
for token in [9223372036854775808,False,1.0,'1',0]:
 out.append(service.request('POST','/api/jobs/job/commit',{'epoch':token}))
for route in ('/api/lifecycle/documents','/api/v1/documents'):
 for token in ('9223372036854775808','-1','1.0','1e0','%2B1'):
  out.append(service.request('GET',route+'?generation='+token))
print(json.dumps({'count':len(out),'valid':all(x==(400,{'error':'invalid_request'}) for x in out)}))
''')
        self.assertEqual(result, {"count": 70, "valid": True})

    def test_exact_boundary_values_reach_durable_api_unchanged(self):
        result = _smoke(r'''
from library.query.service import Service,decimal_counter,decimal_version
class Probe:
 def configure_backup_dir(self,value):pass
 def lifecycle_list(self,*args,**kwargs):return kwargs['generation']
 def list_v1(self,*args,**kwargs):return kwargs['generation']
 def delete_document(self,id,token):return token
 def create_collection(self,name,token):return token
service=Service(Probe(),'.');out=[]
for number in (0,1,9007199254740993,9223372036854775806,9223372036854775807):
 out.extend([service.lifecycle_list(generation=number),service.list_v1(generation=number),
             service.create_collection('name',number),decimal_counter('000'+str(number))])
 if number:out.extend([service.delete_document('id',number),decimal_version('000'+str(number))])
print(json.dumps({'values':out,'all_ints':all(type(x)is int for x in out)}))
''')
        expected = []
        for number in (0, 1, 9007199254740993, 9223372036854775806, 9223372036854775807):
            expected.extend([number] * (4 if number == 0 else 6))
        self.assertEqual(result, {"values": expected, "all_ints": True})

    def test_new_conflicts_map_409_through_legacy_and_new_routes(self):
        result = _smoke(r'''
from library.query.service import Service
from library.common import LibraryError
class Failure:
 def __init__(self,code):self.code=code
 def configure_backup_dir(self,value):pass
 def __getattr__(self,name):
  def fail(*args,**kwargs):raise LibraryError(self.code)
  return fail
out=[]
for code in ('counter_exhausted','stale_instance','backup_root_unbound','backup_root_mismatch'):
 service=Service(Failure(code),'.')
 for route,body in [('/api/lifecycle/documents/id/delete',{'expected_version':1}),
  ('/api/v1/documents/id/delete',{'expected_version':1}),('/api/jobs/job/cancel',{}),
  ('/api/maintenance/backups',{'name':'new.json'})]:
  out.append(service.request('POST',route,body)==(409,{'error':code}))
print(json.dumps({'all':all(out),'count':len(out)}))
''')
        self.assertEqual(result, {"all": True, "count": 16})

    def test_cli_rejects_bad_tokens_before_open_with_json_exit2(self):
        result = _smoke(r'''
import io,contextlib
from library.clients import cli
def bomb(*args,**kwargs):raise AssertionError('Store opened before validation')
cli.Store=bomb
commands=[['job-commit','job','TOKEN'],['delete','id','--expected-version','TOKEN'],
 ['restore-document','id','--expected-version','TOKEN'],['refresh','id','--expected-version','TOKEN','--text','x'],
 ['annotate','id','--expected-version','TOKEN','--notes','x'],
 ['collection-create','name','--expected-generation','TOKEN'],['collection-remove','name','--expected-generation','TOKEN'],
 ['restore-backup','name','--expected-generation','TOKEN'],['documents','--generation','TOKEN'],
 ['documents-v1','--generation','TOKEN']]
count=0
for command in commands:
 for token in ['9223372036854775808','1.0','1e0','+1',' 1','-1','١']:
  args=[token if x=='TOKEN' else x for x in command]
  out,err=io.StringIO(),io.StringIO()
  with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=cli.main(args)
  assert code==2 and out.getvalue()=='' and json.loads(err.getvalue())=={'error':'invalid_request'},(args,err.getvalue())
  count+=1
print(json.dumps({'rejected':count}))
''')
        self.assertEqual(result, {"rejected": 70})

    def test_cli_accepts_exact_max_and_leading_zero_tokens(self):
        result = _smoke(r'''
import io,contextlib
from library.clients import cli
seen=[]
class Store:
 def __init__(self,*args,**kwargs):pass
 def close(self):pass
class Service:
 def __init__(self,*args,**kwargs):pass
 def commit_job(self,id,token):return {'token':token}
 def delete_document(self,id,token):return {'token':token}
 def create_collection(self,name,token):return {'token':token}
 def list_v1(self,*args,**kwargs):return {'token':kwargs['generation']}
cli.Store=Store;cli.Service=Service
for command in [['job-commit','job','0009223372036854775807'],
 ['delete','id','--expected-version','9007199254740993'],
 ['collection-create','name','--expected-generation','0000'],
 ['documents-v1','--generation','9223372036854775807']]:
 out,err=io.StringIO(),io.StringIO()
 with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=cli.main(command)
 assert code==0 and err.getvalue()==''
 seen.append(json.loads(out.getvalue())['token'])
print(json.dumps({'tokens':seen}))
''')
        self.assertEqual(result, {"tokens": [9223372036854775807, 9007199254740993, 0, 9223372036854775807]})

    def test_backup_adoption_is_explicit_and_process_only(self):
        result = _smoke(r'''
import io,contextlib
from library.clients import cli
from library.query.service import Service
opened=[]
class Store:
 def __init__(self,path,*,backup_dir):opened.append((path,backup_dir))
 def adopt_backup_root(self,expected):return {'adopted':True,'registered':0,'expected':expected}
 def close(self):pass
 def configure_backup_dir(self,value):pass
cli.Store=Store
for args in [['backup-root-adopt','--expect-unbound'],
 ['--backup-dir','target','backup-root-adopt'],
 ['--backup-dir','target','backup-root-adopt','--expect-root','old','--expect-unbound'],
 ['--backup-dir','target','backup-root-adopt','--expect-unbound','--surprise']]:
 out,err=io.StringIO(),io.StringIO()
 with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=cli.main(args)
 assert code==2 and not out.getvalue() and json.loads(err.getvalue())=={'error':'invalid_request'}
assert opened==[]
values=[]
for expectation in [['--expect-unbound'],['--expect-root','/old']]:
 out,err=io.StringIO(),io.StringIO()
 with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
  code=cli.main(['--backup-dir','target','backup-root-adopt',*expectation])
 assert code==0 and not err.getvalue()
 values.append(json.loads(out.getvalue()))
store=Store('api',backup_dir='target');service=Service(store,'.',backup_dir='target')
for path in ['/api/maintenance/backup-root-adopt','/api/backup-root-adopt','/api/v1/backup-root-adopt']:
 assert service.request('POST',path,{'expected_root':None})==(404,{'error':'not_found'})
assert service.adopt_backup_root(None)['expected']is None
from library.common import LibraryError
try:Service(store,'.').adopt_backup_root(None)
except LibraryError as error:assert error.code=='invalid_request'
else:raise AssertionError('implicit process target accepted')
print(json.dumps({'opened':opened,'values':values}))
''')
        self.assertEqual(result["opened"][:2], [["library.sqlite3", "target"], ["library.sqlite3", "target"]])
        self.assertEqual([v["expected"] for v in result["values"]], [None, "/old"])

    def test_real_cli_first_migration_repeat_noop_and_matching_diagnostic(self):
        from gossip_harness.library_v2_reference_v1 import write_schema3_v2_project, write_v2_project
        with tempfile.TemporaryDirectory(prefix="authored-v2-cli-migrate-") as directory:
            root = Path(directory).resolve()
            write_schema3_v2_project(root / "schema3")
            write_v2_project(root / "app")
            database = root / "library.sqlite3"
            seed = """
import json,sys
from library.catalog.store import Store
store=Store(sys.argv[2])
store.insert('retained.txt',b'original document')
store.record_error('migrate','io_error')
store.close()
"""
            prepared = subprocess.run([sys.executable, "-I", "-c",
                "import sys;sys.path.insert(0,sys.argv[1]);" + seed,
                str(root / "schema3"), str(database)], cwd=root, capture_output=True,
                text=True, timeout=10, check=False)
            self.assertEqual(prepared.returncode, 0, prepared.stderr)
            bootstrap = ("import runpy,sys;sys.path.insert(0,sys.argv.pop(1));"
                         "runpy.run_module('library',run_name='__main__')")

            def invoke(command):
                result = subprocess.run([sys.executable, "-I", "-c", bootstrap, str(root / "app"),
                    "--db", str(database), command], cwd=root, capture_output=True,
                    text=True, timeout=10, check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stderr, "")
                return json.loads(result.stdout)

            first = invoke("migrate")
            self.assertEqual(first, {"from_schema": 3, "to_schema": 4, "migrated": True,
                                     "documents": 1, "jobs": 0})
            self.assertIsNone(invoke("diagnostics")["last_error"])
            expected = {"from_schema": 4, "to_schema": 4, "migrated": False,
                        "documents": 1, "jobs": 0}
            self.assertEqual(invoke("migrate"), expected)
            # Publish a new matching diagnostic after the actual migration.
            # Ordinary open/diagnostics preserve it; explicit no-op clears it.
            import sqlite3
            with sqlite3.connect(database) as connection:
                connection.execute("UPDATE control SET value=? WHERE key='last_error'",
                    (json.dumps({"operation": "migrate", "code": "io_error"},
                                sort_keys=True, separators=(",", ":")),))
            self.assertEqual(invoke("diagnostics")["last_error"],
                             {"operation": "migrate", "code": "io_error"})
            self.assertEqual(invoke("migrate"), expected)
            self.assertIsNone(invoke("diagnostics")["last_error"])

    def test_legacy_offsets_are_unbounded_nonnegative(self):
        result = _smoke(r'''
from library.query.service import Service
class Store:
 def configure_backup_dir(self,value):pass
 def documents(self):return []
service=Service(Store(),'.')
value=service.request('GET','/api/documents?offset=9223372036854775808000&limit=1')
print(json.dumps({'result':value}))
''')
        self.assertEqual(result, {"result": [200, {"documents": [], "total": 0}]})

    def test_http_transport_retains_exact_integer_request_and_response(self):
        result = _smoke(r'''
import http.client,threading
from http.server import HTTPServer
from library.clients.http import handler_for
class Service:
 def request(self,method,target,body):
  assert body=={'epoch':9223372036854775807,'expected_version':9007199254740993}
  return 200,body
with HTTPServer(('127.0.0.1',0),handler_for(Service())) as server:
 thread=threading.Thread(target=server.handle_request);thread.start()
 connection=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
 connection.request('POST','/probe',body='{"epoch":9223372036854775807,"expected_version":9007199254740993}',headers={'Content-Type':'application/json'})
 response=connection.getresponse();raw=response.read();status=response.status
 connection.close();thread.join(3);assert not thread.is_alive()
 print(json.dumps({'status':status,'raw':raw.decode(),'value':json.loads(raw)}))
''')
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["value"], {"epoch": 9223372036854775807, "expected_version": 9007199254740993})
        self.assertIn('"epoch": 9223372036854775807', result["raw"])


if __name__ == "__main__":
    unittest.main()
