"""Public fixture/oracle qualification, including bounded AUTHORED-seed smoke.

No generated candidate is accepted by these helpers. ``seed_files()`` is the
repository-authored trusted v0; live/modified candidates must use Docker. These
checks do not qualify browser rendering, real HTTP transport or M1 implementation.
"""

from __future__ import annotations

import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from gossip_harness.library_project_fixture_v1 import (
    FAMILY, IMMUTABLE_PATHS, M1_REQUIREMENTS, PACKAGE_SCOPES, RUNTIME_IMAGE,
    oracle, public_cases, public_intake_probes, public_manifest, seed_files,
)


def _trusted_seed_smoke(script: str) -> dict:
    """Run only checked-in authored seed, CPU/file/FD bounded, 10s wall limit."""
    with tempfile.TemporaryDirectory(prefix="trusted-library-v0-") as directory:
        root = Path(directory)
        for name, value in seed_files().items():
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(value, encoding="utf-8")
        bootstrap = (
            "import resource, sys\n"
            "resource.setrlimit(resource.RLIMIT_CPU, (5, 5))\n"
            "resource.setrlimit(resource.RLIMIT_FSIZE, (2097152, 2097152))\n"
            "resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))\n"
            "sys.path.insert(0, sys.argv[1])\n"
        )
        result = subprocess.run(
            [sys.executable, "-I", "-c", bootstrap + script, str(root)],
            cwd=root, capture_output=True, text=True, timeout=10, check=False,
        )
        if result.returncode:
            raise AssertionError(f"Authored seed smoke failed: {result.stderr[:4000]}")
        if len(result.stdout) > 65536:
            raise AssertionError("Authored seed smoke output bound exceeded")
        return json.loads(result.stdout)


class LibraryProjectFixtureV1Tests(unittest.TestCase):
    def test_seed_is_modest_complete_compilable_and_scoped(self):
        files = seed_files()
        self.assertEqual(len(files), 19)
        self.assertLess(sum(len(value.encode()) for value in files.values()), 100 * 1024)
        for path, source in files.items():
            if path.endswith(".py"):
                compile(source, path, "exec")
            owners = [owner for owner, prefixes in PACKAGE_SCOPES.items() if path.startswith(prefixes)]
            self.assertEqual(len(owners), 0 if path in IMMUTABLE_PATHS else 1, path)
        self.assertEqual(set(PACKAGE_SCOPES), {"catalog", "ingestion", "query", "clients"})
        self.assertIn("library/clients/index.html", files)
        self.assertIn("create table", files["library/catalog/store.py"].lower())
        self.assertIn("HTTPServer", files["library/clients/http.py"])

    def test_seed_and_manifest_are_deterministic_fresh_and_no_hidden_bank(self):
        original = seed_files()
        changed = seed_files()
        changed["solution.py"] = "bad"
        self.assertEqual(original, seed_files())
        manifest = public_manifest()
        digest = hashlib.sha256(json.dumps(original, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(manifest["seed_sha256"], digest)
        self.assertEqual(manifest["runtime_image"], RUNTIME_IMAGE)
        self.assertEqual(manifest["full_cohort_trajectories"], 6)
        self.assertEqual(len(manifest["remaining_milestones"]), 3)
        self.assertEqual(manifest["family"], FAMILY)
        self.assertNotIn("hidden_cases", manifest)
        self.assertNotIn("reference_solution", manifest)
        self.assertIn("not held-out", manifest["classification"])

    def test_actual_app_adapter_calls_modules_and_v0_does_not_solve_m1(self):
        files = seed_files()
        self.assertEqual(files["solution.py"], "from library.clients.workflow import solve\n")
        tree = ast.parse(files["library/clients/workflow.py"])
        calls = {node.func.id for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
        self.assertTrue({"Store", "Service", "import_file"} <= calls)
        self.assertNotIn("JobManager", files["library/ingestion/local.py"])
        self.assertIn("SAME SQLite write transaction", M1_REQUIREMENTS)
        self.assertIn("actual merged-tree", M1_REQUIREMENTS)

    def test_oracle_stable_ids_source_identity_and_shared_blob(self):
        result = oracle({"operations": [
            {"op": "import", "source": "b.txt", "text": "same\r\n"},
            {"op": "import", "source": "a.md", "text": "same\r\n"},
            {"op": "reopen"}, {"op": "import", "source": "b.txt", "text": "same\r\n"},
        ]})
        a, b = result["documents"]
        self.assertNotEqual(a["document_id"], b["document_id"])
        self.assertNotEqual(a["source_id"], b["source_id"])
        self.assertEqual(a["blob_id"], b["blob_id"])
        self.assertEqual(b["document_id"], "doc-" + hashlib.sha256(b"document\0b.txt").hexdigest())
        self.assertEqual(result["results"][-1]["status"], "unchanged")

    def test_oracle_rejects_mutation_and_export_errors_without_losing_data(self):
        result = oracle({"operations": [
            {"op": "import", "source": "a.txt", "text": "original"},
            {"op": "import", "source": "a.txt", "text": "changed"},
            {"op": "export", "sources": ["a.txt", "missing.txt"]},
            {"op": "export", "sources": ["a.txt", "a.txt"]},
        ]})
        self.assertEqual(result["documents"][0]["text"], "original")
        self.assertEqual(result["results"][1:], [
            {"error": "source_changed"}, {"error": "not_found"}, {"error": "invalid_request"}])

    def test_oracle_pagination_casefold_and_unicode_newline_preservation(self):
        result = oracle({"operations": [
            {"op": "import", "source": "z.txt", "text": "Straße\r\n"},
            {"op": "import", "source": "a.md", "text": "STRASSE"},
            {"op": "search", "query": "strasse", "offset": 1, "limit": 1},
            {"op": "list", "offset": True}, {"op": "list", "limit": 0},
        ]})
        self.assertEqual(result["results"][2]["total"], 2)
        self.assertEqual(result["results"][2]["documents"][0]["text"], "Straße\r\n")
        self.assertEqual(result["results"][3:], [{"error": "invalid_request"}] * 2)

    def test_oracle_input_admission_is_bounded_and_no_input_mutation(self):
        payload = {"operations": [{"op": "import", "source": "a.txt", "text": "abc"}]}
        before = deepcopy(payload)
        oracle(payload)
        self.assertEqual(payload, before)
        for invalid in ({"operations": []} | {"extra": True}, {"operations": [{"op": "unknown"}]},
                        {"operations": [{"op": "reopen"}] * 65}, {"operations": [{"op": "import", "source": "a"}]},
                        {"operations": [{"op": "import", "source": "a.txt", "text": "x" * 62000}]}):
            with self.subTest(invalid=str(invalid)[:100]), self.assertRaises(ValueError):
                oracle(invalid)

    def test_oracle_source_domain_and_type_bounds(self):
        sources = ["/abs.txt", "../up.txt", "a//b.txt", "./a.txt", "a\\b.txt", "nul\0.txt", "", "a" * 252 + ".txt"]
        result = oracle({"operations": [{"op": "import", "source": s, "text": ""} for s in sources]})
        self.assertEqual(result["results"], [{"error": "invalid_source"}] * len(sources))
        self.assertEqual(result["documents"], [])

    def test_m1_cancel_epoch_retry_and_exact_receipt_replay(self):
        case = next(case for case in public_cases("m1") if case["id"] == "m1-cancel-fences-old-token")
        result = case["expected"]
        self.assertEqual(result["results"][4], {"error": "stale_epoch"})
        self.assertEqual(result["results"][-1], result["results"][-2])
        self.assertEqual(result["jobs"][0], {"job_id": "batch", "epoch": 3, "state": "completed", "total": 1,
                                            "completed": 1, "error": None})

    def test_m1_conflict_after_prepare_rolls_back_every_new_document(self):
        case = next(case for case in public_cases("m1") if case["id"] == "m1-atomic-conflict-after-prepare")
        result = case["expected"]
        self.assertEqual([doc["source"] for doc in result["documents"]], ["z.txt"])
        self.assertEqual(result["documents"][0]["text"], "Other")
        self.assertEqual(result["results"][3], {"error": "source_changed"})
        self.assertEqual(result["jobs"][0]["state"], "failed")
        self.assertEqual(result["jobs"][0]["completed"], 0)

    def test_m1_admission_replay_and_changed_manifest_conflict(self):
        entries = [{"source": "b.txt", "text": "B"}, {"source": "a.md", "text": "A"}]
        result = oracle({"milestone": "m1", "operations": [
            {"op": "submit", "job_id": "job", "entries": entries},
            {"op": "submit", "job_id": "job", "entries": entries[::-1]},
            {"op": "submit", "job_id": "job", "entries": []},
            {"op": "prepare", "job_id": "job"}, {"op": "prepare", "job_id": "job"},
        ]})
        self.assertEqual(result["results"][0], result["results"][1])
        self.assertEqual(result["results"][2], {"error": "job_conflict"})
        self.assertEqual(result["results"][3], result["results"][4])

    def test_m1_empty_batch_terminal_rules_and_duplicate_member_failure(self):
        result = oracle({"milestone": "m1", "operations": [
            {"op": "submit", "job_id": "empty", "entries": []}, {"op": "prepare", "job_id": "empty"},
            {"op": "commit", "job_id": "empty", "epoch": 1}, {"op": "cancel", "job_id": "empty"},
            {"op": "submit", "job_id": "duplicate", "entries": [{"source": "a.txt", "text": "A"}] * 2},
            {"op": "prepare", "job_id": "duplicate"}, {"op": "retry", "job_id": "duplicate"},
        ]})
        self.assertEqual(result["results"][2]["documents"], [])
        self.assertEqual(result["results"][3], {"error": "job_state"})
        self.assertEqual(result["results"][5], {"error": "invalid_batch"})
        self.assertEqual(result["results"][6]["epoch"], 2)
        self.assertEqual(result["documents"], [])

    def test_m1_repeated_prepare_replays_token_until_transactional_conflict_check(self):
        result = oracle({"milestone": "m1", "operations": [
            {"op": "submit", "job_id": "job", "entries": [{"source": "a.txt", "text": "new"}]},
            {"op": "prepare", "job_id": "job"}, {"op": "import", "source": "a.txt", "text": "other"},
            {"op": "prepare", "job_id": "job"}, {"op": "commit", "job_id": "job", "epoch": 1},
        ]})
        self.assertEqual(result["results"][1], result["results"][3])
        self.assertEqual(result["results"][4], {"error": "source_changed"})
        self.assertEqual(result["jobs"][0]["state"], "failed")

    def test_m1_fault_probe_preserves_catalog_and_running_job_then_commits(self):
        case = next(case for case in public_cases("m1") if case["id"] == "m1-injected-transaction-failure")
        result = case["expected"]
        self.assertEqual(result["results"][3], {"error": "injected_failure"})
        self.assertEqual([doc["source"] for doc in result["results"][4]["documents"]], ["keep.txt"])
        self.assertEqual(result["results"][6]["state"], "running")
        self.assertEqual([doc["source"] for doc in result["documents"]], ["keep.txt", "new.txt"])

    def test_public_intake_recipes_are_explicitly_distinct_from_execution_evidence(self):
        probes = public_intake_probes()
        self.assertEqual({probe["kind"] for probe in probes}, {"directory", "zip", "json"})
        self.assertEqual(len({probe["id"] for probe in probes}), len(probes))
        self.assertTrue(all("expect_sources" in probe for probe in probes))
        self.assertIn("library.ingestion.jobs", M1_REQUIREMENTS)
        self.assertIn("commit_job(job_id, epoch", M1_REQUIREMENTS)
        self.assertIn("oracle covers transitions, not archive parser correctness", M1_REQUIREMENTS)

    def test_authored_seed_executes_public_histories_and_stays_unsolved_for_m1(self):
        result = _trusted_seed_smoke('''
import json
from pathlib import Path
from solution import solve
cases = json.loads(Path('public_cases.json').read_text())
for case in cases:
    assert solve(case['input']) == case['expected'], case['id']
probe = solve({'operations':[{'op':'submit','job_id':'x','entries':[]}]})
assert probe['results'] == [{'error':'unsupported_operation'}]
from_payload={'operations':[{'op':'import','source':'a.txt','text':'A'},
    {'op':'import','source':'a.txt/b.txt','text':'B'},
    {'op':'import','source':'book.pdf','text':chr(0xD800)},
    {'op':'import','source':'a'*252+'.txt','text':'A'},
    {'op':'import','source':'broken.txt','text':chr(0xD800)}]}
edge=solve(from_payload)
assert len(edge['documents'])==2
assert edge['results'][2]=={'error':'unsupported_type'}
assert edge['results'][3]=={'error':'invalid_source'}
assert edge['results'][-1]=={'error':'invalid_utf8'}
print(json.dumps({'public_histories':len(cases),'m1_implemented':False}))
''')
        self.assertEqual(result, {"public_histories": 3, "m1_implemented": False})

    def test_authored_seed_file_failures_and_shared_blob_storage(self):
        result = _trusted_seed_smoke('''
import json
from pathlib import Path
from library.catalog.store import Store
from library.common import LibraryError
from library.ingestion.local import import_file
root=Path('inputs');root.mkdir();store=Store('checks.sqlite3')
for name in ('a.txt','b.txt'):
    (root/name).write_bytes(b'same\\r\\n')
    import_file(store,root,name)
assert store.db.execute('SELECT count(*) FROM blobs').fetchone()[0] == 1
before=store.documents()
(root/'a.txt').write_bytes(b'changed')
(root/'invalid.txt').write_bytes(b'\\xff')
(root/'large.txt').write_bytes(b'x'*32769)
(root/'link.txt').symlink_to('b.txt')
errors=[]
for name in ('a.txt','invalid.txt','large.txt','link.txt'):
    try: import_file(store,root,name)
    except LibraryError as error: errors.append(error.code)
assert store.documents() == before
assert store.db.execute('SELECT count(*) FROM blobs').fetchone()[0] == 1
store.close();store=Store('checks.sqlite3');assert store.documents()==before;store.close()
print(json.dumps({'errors':errors,'persisted_documents':len(before)}))
''')
        self.assertEqual(result["errors"], ["source_changed", "invalid_utf8", "too_large", "invalid_source"])
        self.assertEqual(result["persisted_documents"], 2)

    def test_authored_seed_cli_persistence_and_shared_api_contract(self):
        result = _trusted_seed_smoke('''
import json, subprocess, sys
from pathlib import Path
from library.catalog.store import Store
from library.query.service import Service
args=[sys.executable,'-m','library','--db','cli.sqlite3','--root','.']
def cli(*parts):
    p=subprocess.run(args+list(parts),capture_output=True,text=True,timeout=3)
    assert p.returncode==0,p.stderr
    return json.loads(p.stdout)
doc=cli('import','examples/welcome.txt')['document']
assert cli('list')['documents']==[doc]
assert cli('show',doc['document_id'])==doc
assert cli('export')['documents']==[doc]
store=Store('cli.sqlite3');service=Service(store,'.')
assert service.request('GET','/api/documents?q=RESEARCH')[1]['documents']==[doc]
assert service.request('GET','/api/documents/'+doc['document_id'])==(200,doc)
assert service.request('GET','/api/export')[1]==cli('export')
assert service.request('GET','/api/documents?limit=0')==(400,{'error':'invalid_request'})
assert service.request('POST','/api/import',{'source':'examples/welcome.txt','extra':True})==(400,{'error':'invalid_request'})
assert service.request('POST','/api/export',{'ids':[doc['document_id']]})[1]==cli('export')
store.close()
assert service.request('GET','/api/documents')==(400,{'error':'io_error'})
print(json.dumps({'cli_api_agree':True}))
''')
        self.assertEqual(result, {"cli_api_agree": True})


if __name__ == "__main__":
    unittest.main()
