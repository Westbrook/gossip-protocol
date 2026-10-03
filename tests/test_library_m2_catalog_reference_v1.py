"""Bounded authored catalog checks; no arbitrary candidate execution or acceptance."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_m1_reference_v1 import catalog_files as m1_catalog_files, m1_files
from gossip_harness.library_m2_catalog_reference_v1 import catalog_files


def _authored_smoke(script: str) -> dict:
    with tempfile.TemporaryDirectory(prefix="trusted-library-m2-catalog-") as directory:
        root = Path(directory).resolve()
        for name, source in (m1_files() | catalog_files()).items():
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(source, encoding="utf-8")
        bootstrap = (
            "import json, resource, sys\n"
            "resource.setrlimit(resource.RLIMIT_CPU, (14, 14))\n"
            "resource.setrlimit(resource.RLIMIT_FSIZE, (67108864, 67108864))\n"
            "resource.setrlimit(resource.RLIMIT_NOFILE, (96, 96))\n"
            "sys.path.insert(0,sys.argv[1])\n"
            "from library.catalog.store import Store\n"
            "from library.common import LibraryError\n"
            "def code(call):\n"
            "    try:\n"
            "        call()\n"
            "        return None\n"
            "    except LibraryError as error:\n"
            "        return error.code\n"
        )
        result = subprocess.run(
            [sys.executable, "-I", "-c", bootstrap + script, str(root)], cwd=root,
            capture_output=True, text=True, timeout=30, check=False,
        )
        if result.returncode:
            raise AssertionError(f"Authored M2 catalog smoke failed: {result.stderr[:5000]}")
        if len(result.stdout.encode()) > 65536:
            raise AssertionError("Authored smoke output exceeds bound")
        return json.loads(result.stdout)


class LibraryM2CatalogReferenceV1Tests(unittest.TestCase):
    def test_generated_overlay_compiles_and_preserves_exact_m1_implementation(self):
        files = catalog_files()
        self.assertEqual(set(files), {"library/catalog/store.py", "library/catalog/legacy_m1.py"})
        self.assertEqual(files["library/catalog/legacy_m1.py"], m1_catalog_files()["library/catalog/store.py"])
        for path, source in files.items():
            compile(source, path, "exec")
        files["library/catalog/store.py"] = "changed"
        self.assertNotEqual(catalog_files()["library/catalog/store.py"], "changed")

    def test_migration_preserves_identity_original_job_strings_and_reopen_state(self):
        result = _authored_smoke(r'''
from library.catalog.legacy_m1 import Store as Old
old=Old('catalog.sqlite')
old.create_job('original',[{'source':'a.txt','text':'old'}]);old.start_job('original',1)
receipt=old.commit_job('original',1);document=receipt['documents'][0]
old.create_job('bad',[{'source':'bad.txt','text':'\ud800'}])
serial=[tuple(row) for row in old.db.execute('SELECT * FROM jobs ORDER BY job_id')]
old.close()
s=Store('catalog.sqlite');did=document['document_id']
initial=s.lifecycle_show(did);generation=s.lifecycle_list()['generation']
serial_same=serial==[tuple(row) for row in s.db.execute('SELECT * FROM jobs ORDER BY job_id')]
s.refresh_document(did,1,text='new');s.delete_document(did,2);s.close()
s=Store('catalog.sqlite')
print(json.dumps({'initial':initial,'generation':generation,'serial_same':serial_same,
    'replay':s.commit_job('original',1)==receipt,'deleted':s.lifecycle_show(did),'history':s.revision_history(did),
    'schema':s.db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0]}))
''')
        initial = result["initial"]
        self.assertEqual({k: v for k, v in initial.items() if k != "document"}, {
            "revision": 1, "edit_version": 1, "deleted": False, "notes": "", "tags": [], "collections": [],
        })
        self.assertEqual(result["generation"], 0)
        self.assertTrue(result["serial_same"] and result["replay"])
        self.assertEqual(result["schema"], "2")
        self.assertEqual(result["deleted"]["document"]["source"], "a.txt")
        self.assertEqual(result["deleted"]["edit_version"], 3)
        self.assertTrue(result["deleted"]["deleted"])
        self.assertEqual([r["text"] for r in result["history"]["revisions"]], ["old", "new"])

    def test_migration_failure_is_atomic_including_new_jobs_table(self):
        result = _authored_smoke(r'''
import sqlite3
# An incompatible pre-existing table deliberately interrupts migration after
# earlier DDL would otherwise have committed. This is a fault fixture.
db=sqlite3.connect('broken.sqlite',isolation_level=None)
db.executescript("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);"
 "INSERT INTO metadata VALUES ('schema','0');CREATE TABLE revisions(blocker TEXT);")
before=list(db.execute("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name"))
try:
    Store('broken.sqlite')
except sqlite3.OperationalError:
    failed=True
print(json.dumps({'failed':failed,'same':before==list(db.execute(
    "SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name")),
    'schema':db.execute("SELECT value FROM metadata WHERE key='schema'").fetchone()[0]}))
''')
        self.assertEqual(result, {"failed": True, "same": True, "schema": "0"})

    def test_refresh_aba_revision_capacity_and_error_precedence(self):
        result = _authored_smoke(r'''
s=Store('catalog.sqlite');doc=s.insert('a.txt',b'A')['document'];did=doc['document_id']
a=s.refresh_document(did,1,text='A')
b=s.refresh_document(did,1,text='B')
c=s.refresh_document(did,2,text='A')
for version in range(3,16):
    s.refresh_document(did,version,text=str(version))
head=s.lifecycle_show(did)
fail=code(lambda:s.refresh_document(did,16,text='overflow'))
no_op=s.refresh_document(did,16,text='15')['status']
s.delete_document(did,16)
errors=[code(lambda:s.refresh_document(did,16,text='x')),code(lambda:s.refresh_document(did,17,text='x')),
        code(lambda:s.refresh_document('missing',1,text='x')),code(lambda:s.refresh_document(did,17,text='\ud800')),
        code(lambda:s.refresh_document(did,True,text='x')),code(lambda:s.refresh_document(did,17,text='é'*16385))]
history=s.revision_history(did)['revisions']
print(json.dumps({'unchanged':a['status'],'versions':[b['record']['edit_version'],c['record']['edit_version']],
 'identity':all(c['record']['document'][k]==doc[k] for k in ('source','source_id','document_id','title')),
 'aba_blob':history[0]['blob_id']==history[2]['blob_id'],'history':len(history),'head':head['revision'],
 'fail':fail,'no_op':no_op,'errors':errors,'generation':s.lifecycle_list(deleted='all')['generation']}))
''')
        self.assertEqual(result, {
            "unchanged": "unchanged", "versions": [2, 3], "identity": True, "aba_blob": True,
            "history": 16, "head": 16, "fail": "revision_capacity", "no_op": "unchanged",
            "errors": ["stale_version", "document_deleted", "not_found", "invalid_utf8", "invalid_request", "too_large"],
            "generation": 17,
        })

    def test_annotations_normalization_collections_and_tombstone_membership(self):
        result = _authored_smoke(r'''
s=Store('catalog.sqlite');did=s.insert('a.txt',b'<html>')['document']['document_id']
created=s.create_collection('  CAFÉ  ',1)
changed=s.replace_annotations(did,1,' <literal>\r\n',[' Straße ','E\u0301'],['cafe\u0301'])
no_op=s.replace_annotations(did,2,' <literal>\r\n',['é','STRASSE'],['café'])
errors=[code(lambda:s.replace_annotations(did,2,'',['é','e\u0301'],[])),
        code(lambda:s.replace_annotations(did,2,'',['bad\x00'],[])),
        code(lambda:s.replace_annotations(did,2,'é'*8193,[],[])),
        code(lambda:s.replace_annotations(did,2,'',[],['missing'])),
        code(lambda:s.remove_collection('café',1))]
s.delete_document(did,2)
errors += [code(lambda:s.remove_collection('café',4)),
           code(lambda:s.replace_annotations(did,2,'',[],['missing'])),
           code(lambda:s.replace_annotations(did,3,'',[],['missing']))]
print(json.dumps({'created':created,'changed':changed['record'],'no_op':no_op['status'],'errors':errors,
                  'collections':s.list_collections(),'legacy':s.documents()}))
''')
        self.assertEqual(result["created"], {"status": "created", "name": "café", "generation": 2})
        self.assertEqual(result["changed"]["tags"], ["strasse", "é"])
        self.assertEqual(result["changed"]["collections"], ["café"])
        self.assertEqual(result["changed"]["notes"], " <literal>\r\n")
        self.assertEqual(result["changed"]["revision"], 1)
        self.assertEqual(result["no_op"], "unchanged")
        self.assertEqual(result["errors"], ["invalid_request", "invalid_request", "too_large", "collection_not_found",
                                            "stale_generation", "collection_not_empty", "stale_version", "document_deleted"])
        self.assertEqual(result["collections"], {"collections": [{"name": "café", "total": 1}], "generation": 4})
        self.assertEqual(result["legacy"], [])

    def test_collection_count_name_bounds_and_noop_generation(self):
        result = _authored_smoke(r'''
s=Store('catalog.sqlite')
for index in range(64):s.create_collection('x'+str(index),index)
errors=[code(lambda:s.create_collection('new',64)),code(lambda:s.create_collection('x0',63)),
        code(lambda:s.remove_collection('missing',64)),code(lambda:s.create_collection('é'*33,64)),
        code(lambda:s.create_collection('\ud800',64)),code(lambda:s.create_collection('x',True))]
unchanged=s.create_collection(' X0 ',64)
removed=s.remove_collection('x0',64)
created=s.create_collection('é'*32,65)
print(json.dumps({'errors':errors,'unchanged':unchanged,'removed':removed,'created':created,
                  'total':len(s.list_collections()['collections'])}))
''')
        self.assertEqual(result["errors"], ["capacity", "stale_generation", "not_found", "invalid_request", "invalid_utf8", "invalid_request"])
        self.assertEqual(result["unchanged"], {"status": "unchanged", "name": "x0", "generation": 64})
        self.assertEqual(result["removed"], {"status": "removed", "name": "x0", "generation": 65})
        self.assertEqual(result["created"]["generation"], 66)
        self.assertEqual(result["total"], 64)

    def test_legacy_tombstones_batch_rechecks_replay_and_rollback(self):
        result = _authored_smoke(r'''
s=Store('catalog.sqlite');other=Store('catalog.sqlite')
s.create_job('original',[{'source':'a.txt','text':'A'},{'source':'b.txt','text':'B'}]);s.start_job('original',1)
receipt=s.commit_job('original',1);did=receipt['documents'][0]['document_id']
generation=s.lifecycle_list()['generation']
s.refresh_document(did,1,text='new');s.delete_document(did,2)
identical=s.insert('a.txt',b'new');changed=code(lambda:s.insert('a.txt',b'A'))
s.create_job('conflict',[{'source':'c.txt','text':'C'},{'source':'a.txt','text':'new'}]);s.start_job('conflict',1)
other.restore_document(did,3);other.refresh_document(did,4,text='raced')
conflict=s.commit_job('conflict',1)
s.create_job('rollback',[{'source':'d.txt','text':'D'}]);s.start_job('rollback',1)
before=s.lifecycle_list()['generation'];rollback=code(lambda:s.commit_job('rollback',1,fail_before_commit=True))
rolled=s.lifecycle_list()['generation']==before and s.get_job('rollback')['state']=='running'
s.create_job('empty',[]);s.start_job('empty',1);s.commit_job('empty',1)
print(json.dumps({'generation_batch':generation,'identical':identical['status'],'changed':changed,'conflict':conflict,
 'absent_c':not any(d['source']=='c.txt' for d in s.all_documents()),'rollback':rollback,'rolled':rolled,
 'empty_generation':s.lifecycle_list()['generation']==before,'replay':s.commit_job('original',1)==receipt,
 'old_text':s.commit_job('original',1)['documents'][0]['text']}))
''')
        self.assertEqual(result, {"generation_batch": 1, "identical": "unchanged", "changed": "source_changed",
                                  "conflict": {"error": "source_changed"}, "absent_c": True,
                                  "rollback": "injected_failure", "rolled": True, "empty_generation": True,
                                  "replay": True, "old_text": "A"})

    def test_real_connections_have_one_edit_winner_and_snapshot_pagination(self):
        result = _authored_smoke(r'''
import threading
s=Store('catalog.sqlite');s.db.execute('PRAGMA journal_mode=WAL')
did=s.insert('a.txt',b'old')['document']['document_id']
barrier=threading.Barrier(2);results=[]
def writer(value):
    db=Store('catalog.sqlite');barrier.wait()
    try:results.append(db.refresh_document(did,1,text=value)['status'])
    except LibraryError as error:results.append(error.code)
    finally:db.close()
threads=[threading.Thread(target=writer,args=(value,)) for value in ('A','B')]
for thread in threads:thread.start()
for thread in threads:thread.join(8)
before=s.lifecycle_show(did);old_generation=s.lifecycle_list()['generation'];other=Store('catalog.sqlite')
fired=[]
def interleave(statement):
    if 'SELECT d.*' in statement and not fired:
        fired.append(True)
        other.refresh_document(did,2,text='newer')
s.db.set_trace_callback(interleave)
snapshot=s.lifecycle_list()
s.db.set_trace_callback(None)
print(json.dumps({'results':sorted(results),'snapshot_text':snapshot['records'][0]['document']['text']==before['document']['text'],
 'snapshot_generation':snapshot['generation']==old_generation,'fired':bool(fired),
 'stale':code(lambda:s.lifecycle_list(generation=old_generation)),'current':s.lifecycle_show(did)['document']['text']}))
''')
        self.assertEqual(result, {"results": ["refreshed", "stale_version"], "snapshot_text": True,
                                  "snapshot_generation": True, "fired": True, "stale": "stale_generation", "current": "newer"})

    def test_query_filters_freshness_and_all_document_capacity(self):
        result = _authored_smoke(r'''
s=Store('catalog.sqlite')
s.create_job('seed',[{'source':name,'text':text} for name,text in [('b.txt','beta'),('a.txt','ALPHA'),('z.txt','old')]])
s.start_job('seed',1);receipt=s.commit_job('seed',1)
a=s.documents()[0]['document_id'];z=s.documents()[-1]['document_id']
s.create_collection('C',1);s.replace_annotations(a,1,'beta-not-searchable',[' T '],['C']);s.refresh_document(z,1,text='current')
s.delete_document(z,2)
filtered=s.lifecycle_list('alpha',tag='t',collection=' c ',limit=1)
filtered['records'][0]['tags'].append('mutated')
allrows=s.lifecycle_list(deleted='all',offset=1,limit=1)
errors=[code(lambda:s.lifecycle_list(offset=True)),code(lambda:s.lifecycle_list(limit=0)),
 code(lambda:s.lifecycle_list(generation=-1)),code(lambda:s.lifecycle_list(deleted='other'))]
for index in range(253):s.insert(str(index)+'.txt',b'shared')
print(json.dumps({'ordered':[d['source'] for d in s.documents() if d['source'] in ('a.txt','b.txt','z.txt')],
 'fresh_tags':s.lifecycle_show(a)['tags'],'page_total':allrows['total'],'page_source':allrows['records'][0]['document']['source'],
 'old_search':s.lifecycle_list('old',deleted='all')['total'],'note_search':s.lifecycle_list('beta-not-searchable')['total'],
 'absent_collection':s.lifecycle_list(collection='absent')['total'],'errors':errors,
 'capacity':code(lambda:s.insert('overflow.txt',b'x')),'hidden':code(lambda:s.show(z)),
 'tombstone_count':len(s.all_documents())}))
''')
        self.assertEqual(result, {"ordered": ["a.txt", "b.txt"], "fresh_tags": ["t"], "page_total": 3,
                                  "page_source": "b.txt", "old_search": 0, "note_search": 0, "absent_collection": 0,
                                  "errors": ["invalid_request"] * 4, "capacity": "capacity", "hidden": "not_found",
                                  "tombstone_count": 256})

    def test_distinct_retained_blob_capacity_boundary_and_batch_atomic_failure(self):
        result = _authored_smoke(r'''
s=Store('catalog.sqlite');s.db.execute('PRAGMA journal_mode=WAL')
# Build exactly 16 MiB of distinct real retained content through public writes.
for document in range(32):
    text=(str(document)+':0').ljust(32768,'x')
    did=s.insert(str(document)+'.txt',text.encode())['document']['document_id']
    for revision in range(1,16):
        s.refresh_document(did,revision,text=(str(document)+':'+str(revision)).ljust(32768,'x'))
before=s.lifecycle_list()['generation']
size=s.db.execute('SELECT SUM(length(content)) FROM blobs').fetchone()[0]
failed=code(lambda:s.insert('new.txt',b'new'))
s.create_job('capacity',[{'source':'new.txt','text':'new'},{'source':'also.txt','text':'also'}]);s.start_job('capacity',1)
receipt=s.commit_job('capacity',1)
# Shared content introduces another document but consumes zero distinct bytes.
shared=s.insert('shared.txt','0:0'.ljust(32768,'x').encode())
print(json.dumps({'size':size,'failed':failed,'receipt':receipt,'job':s.get_job('capacity')['state'],
 'rollback_size':s.db.execute('SELECT SUM(length(content)) FROM blobs').fetchone()[0],
 'count':len(s.documents()),'shared':shared['status'],'generation_delta':s.lifecycle_list()['generation']-before}))
''')
        self.assertEqual(result, {"size": 16777216, "failed": "capacity", "receipt": {"error": "capacity"},
                                  "job": "failed", "rollback_size": 16777216, "count": 33,
                                  "shared": "imported", "generation_delta": 1})

    def test_transition_noops_keep_versions_generation_and_history(self):
        result = _authored_smoke(r'''
s=Store('catalog.sqlite');did=s.insert('a.txt',b'x')['document']['document_id']
initial=s.restore_document(did,1)
a=s.delete_document(did,1);b=s.delete_document(did,2)
stale=code(lambda:s.delete_document(did,1));c=s.restore_document(did,2);d=s.restore_document(did,3)
print(json.dumps({'statuses':[initial['status'],a['status'],b['status'],c['status'],d['status']],
 'versions':[x['record']['edit_version'] for x in (initial,a,b,c,d)],'stale':stale,
 'generation':s.lifecycle_list()['generation'],'revisions':len(s.revision_history(did)['revisions'])}))
''')
        self.assertEqual(result, {"statuses": ["unchanged", "deleted", "unchanged", "restored", "unchanged"],
                                  "versions": [1, 2, 2, 3, 3], "stale": "stale_version", "generation": 3, "revisions": 1})


if __name__ == "__main__":
    unittest.main()
