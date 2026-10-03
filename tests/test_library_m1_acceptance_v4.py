"""Acceptance boundary controls; host execution is authored reference smoke only.

No Docker, arbitrary candidate source, provider, browser or production ledger
is used here. Root owns the separately required physical acceptance rehearsal.
"""
from __future__ import annotations

from dataclasses import replace
import ast
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.blackbox_validator import BlackboxValidator, json_equal
from gossip_harness.library_m1_acceptance_v4 import (
    AcceptancePolicy, CHILD_ADAPTER, FrozenSubject, acceptance_cases, make_validator,
    run_once, source_sha256,
)
from gossip_harness.library_m1_reference_v1 import m1_files
from gossip_harness.library_project_fixture_v1 import public_cases


def _authored_observation(scenario: str, *, defective: bool = False,
                         escaped_write: bool = False) -> object:
    """Fixed checked-in authored app only; not the production candidate boundary."""
    if scenario not in ("intake", "rejected_intake", "durability", "cli_restart"):
        raise ValueError("Host smoke excludes network and arbitrary scenarios")
    with tempfile.TemporaryDirectory(prefix="trusted-acceptance-smoke-") as directory:
        root = Path(directory).resolve()
        for name, text in m1_files(defective_blob_dedup=defective).items():
            target = root/name
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text(text)
        child = CHILD_ADAPTER.replace('WORKSPACE = "/workspace"', f"WORKSPACE = {str(root)!r}")
        if escaped_write:
            child = child.replace("def rejected_intake(base):",
                "def rejected_intake(base):\n    (base/'escape.txt').write_text('simulated write before rejection')")
        bootstrap = ("import resource\nresource.setrlimit(resource.RLIMIT_CPU,(8,8))\n"
                     "resource.setrlimit(resource.RLIMIT_FSIZE,(8388608,8388608))\n"
                     "resource.setrlimit(resource.RLIMIT_NOFILE,(96,96))\n")
        result = subprocess.run([sys.executable,"-I","-c",bootstrap+child],
            input=json.dumps({"scenario":scenario}),capture_output=True,text=True,
            timeout=25,cwd=root,check=False)
        if result.returncode or len(result.stdout.encode())>65536:
            raise AssertionError(f"Trusted authored observation failed: {result.stderr[:4000]}")
        return json.loads(result.stdout)


class LibraryM1AcceptanceV4Tests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.files = {"solution.py":"# not executed by mocked controls\n"}
        self.subject = FrozenSubject("cohort","trajectory","a"*64,"b"*40,"c"*40,
                                     source_sha256(self.files))
        self.policy = AcceptancePolicy("sha256:"+"d"*64)
        self.calls = []

    def evaluate(self, files, cases):
        self.calls.append((files,cases))
        return {"source_sha256":source_sha256(files),
            "suite_sha256":hashlib.sha256(json.dumps(cases,ensure_ascii=True,allow_nan=False,
                sort_keys=True,separators=(",",":")).encode()).hexdigest(),
            "adapter_sha256":make_validator(self.policy)._sandbox.checks_sha256,
            "image_id":self.policy.image_id,"passed":True,"status":"passed"}

    def run_mocked(self, output=None, *, verify=None, subject=None):
        with patch("gossip_harness.library_m1_acceptance_v4._runtime",return_value={"mocked":True}), \
             patch.object(BlackboxValidator,"evaluate",side_effect=self.evaluate):
            return run_once(output or self.root/"observation",self.files,subject or self.subject,
                b'{"durable_freeze":"test-only"}',verify or (lambda *_: True),self.policy)

    def test_closed_fresh_suite_is_independent_from_public_oracle(self):
        cases = acceptance_cases()
        self.assertEqual(len(cases),5)
        self.assertFalse({case["id"] for case in cases} & {case["id"] for case in public_cases("m1")})
        self.assertEqual([case["input"] for case in cases],
            [{"scenario":name} for name in ("intake","rejected_intake","durability","cli_restart","http_restart")])
        BlackboxValidator._inputs(self.files,cases)
        cases[0]["expected"]["jobs"].clear()
        self.assertEqual(len(acceptance_cases()[0]["expected"]["jobs"]),3)

    def test_child_compiles_and_never_imports_host_oracle_or_acceptance_verdict(self):
        compile(CHILD_ADAPTER,"acceptance-child.py","exec")
        self.assertNotIn("library_m1_reference",CHILD_ADAPTER)
        self.assertNotIn("library_project_fixture",CHILD_ADAPTER)
        self.assertNotIn("'expected'",CHILD_ADAPTER)
        self.assertNotIn("'passed'",CHILD_ADAPTER)
        self.assertNotIn("start_new_session",CHILD_ADAPTER)

    def test_embedded_child_has_no_undefined_names_across_unexecuted_http_path(self):
        result = subprocess.run([sys.executable,"-m","ruff","check","--select","F821",
                                 "--stdin-filename","acceptance-child.py","-"],
            input=CHILD_ADAPTER,text=True,capture_output=True,timeout=15,check=False)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)

    def test_raw_cli_and_http_json_reject_ambiguous_or_non_utf8_values(self):
        function = next(node for node in ast.parse(CHILD_ADAPTER).body
                        if isinstance(node,ast.FunctionDef) and node.name == "strict_json")
        scope = {"json":json}
        exec(compile(ast.Module(body=[function],type_ignores=[]),"trusted-json-parser","exec"),scope)
        parse = scope["strict_json"]
        self.assertEqual(parse('{"value":"Ω"}'.encode()),{"value":"Ω"})
        for raw in (b'{"value":0,"value":1}',b'{"x":{"n":false,"n":0}}',
                    b'{"x":NaN}',b'{"x":1e999}',b'{"x":1}\n{}',
                    '{"x":1}'.encode("utf-16"),b'{"x":"\xff"}'):
            with self.subTest(raw=raw),self.assertRaises((ValueError,UnicodeError)):
                parse(raw)

    def test_frozen_source_hash_preserves_ascii_encoding_and_json_types(self):
        self.assertNotEqual(source_sha256({"solution.py":"é"}),source_sha256({"solution.py":"e"}))
        self.assertEqual(source_sha256({"b":"B","a":"A"}),source_sha256({"a":"A","b":"B"}))

    def test_validator_composition_preserves_original_and_docker_boundaries(self):
        original = BlackboxValidator(self.policy.image_id)
        original_hash = original._sandbox.checks_sha256
        validator = make_validator(self.policy)
        self.assertNotEqual(validator._sandbox.checks_sha256,original_hash)
        self.assertEqual(BlackboxValidator(self.policy.image_id)._sandbox.checks_sha256,original_hash)
        arguments = validator._sandbox._arguments("check",Path("/workspace"),Path("/checks"))
        for expected in ("--network=none","--read-only","--cap-drop=ALL","--pull=never"):
            self.assertIn(expected,arguments)
        self.assertFalse(any(argument.startswith("--publish") for argument in arguments))

    def test_limits_and_frozen_subject_reject_ambiguous_types_and_mutable_refs(self):
        for kwargs in ({"timeout_seconds":True},{"case_timeout_seconds":4},{"timeout_seconds":301}):
            with self.assertRaises(ValueError):
                AcceptancePolicy(self.policy.image_id,**kwargs)
        for key,value in (("commit_oid","HEAD"),("tree_oid","main"),("cohort_id","../x"),
                          ("source_sha256","a"*63),("execution_contract_sha256",True)):
            with self.assertRaises(ValueError):
                replace(self.subject,**{key:value})

    def test_freeze_is_checked_before_any_runtime_or_candidate_execution(self):
        with patch("gossip_harness.library_m1_acceptance_v4._runtime") as runtime, \
             patch.object(BlackboxValidator,"evaluate") as evaluate:
            for decision in (False,1,None):
                with self.assertRaisesRegex(ValueError,"freeze"):
                    run_once(self.root/"blocked",self.files,self.subject,b"freeze",
                             lambda *_: decision,self.policy)
            runtime.assert_not_called(); evaluate.assert_not_called()
        self.assertFalse((self.root/"blocked").exists())

    def test_changed_source_rejected_before_barrier_callback(self):
        with patch("gossip_harness.library_m1_acceptance_v4._runtime") as runtime:
            checked = []
            with self.assertRaisesRegex(ValueError,"Source"):
                run_once(self.root/"changed",self.files,replace(self.subject,source_sha256="e"*64),
                    b"freeze",lambda *_: checked.append(True),self.policy)
            self.assertFalse(checked); runtime.assert_not_called()

    def test_intent_and_original_receipt_bind_exact_source_policy_barrier_and_purpose(self):
        calls = []
        answer = self.run_mocked(verify=lambda subject,raw: calls.append((subject,raw)) or True)
        path = self.root/"observation"
        intent = json.loads((path/"intent.json").read_text())
        receipt = json.loads((path/"validator-receipt.json").read_text())
        self.assertEqual(intent["purpose"],"independent_acceptance")
        self.assertEqual(intent["subject"]["source_sha256"],self.subject.source_sha256)
        self.assertEqual(intent["freeze_receipt_sha256"],hashlib.sha256(calls[0][1]).hexdigest())
        self.assertEqual((path/"freeze-receipt.bin").read_bytes(),calls[0][1])
        self.assertEqual(answer["validator_receipt_sha256"],hashlib.sha256((path/"validator-receipt.json").read_bytes()).hexdigest())
        self.assertEqual(receipt["source_sha256"],answer["source_sha256"])
        self.assertEqual(intent["sandbox_arguments"][2],"--interactive")
        self.assertFalse(answer["whole_project_acceptance"])
        self.assertFalse(answer["browser_qualified"])
        self.assertFalse(intent["reuse_allowed"])
        self.assertEqual(len(self.calls),1)

    def test_existing_intent_or_terminal_output_never_reexecutes(self):
        self.run_mocked()
        with self.assertRaisesRegex(ValueError,"new canonical"):
            self.run_mocked()
        self.assertEqual(len(self.calls),1)
        (self.root/"partial").mkdir()
        (self.root/"partial/intent.json").write_text("unknown")
        with self.assertRaises(ValueError):
            self.run_mocked(self.root/"partial")
        self.assertEqual((self.root/"partial/intent.json").read_text(),"unknown")

    def test_symlink_and_relative_output_rejected(self):
        (self.root/"link").symlink_to(self.root,target_is_directory=True)
        for output in (Path("relative-output"),self.root/"link/observation"):
            with self.assertRaisesRegex(ValueError,"canonical"):
                self.run_mocked(output)
        self.assertFalse(self.calls)

    def test_runtime_failure_leaves_no_execution_intent_or_candidate_call(self):
        with patch("gossip_harness.library_m1_acceptance_v4._runtime",side_effect=ValueError("offline")), \
             patch.object(BlackboxValidator,"evaluate") as evaluate:
            with self.assertRaisesRegex(ValueError,"offline"):
                run_once(self.root/"offline",self.files,self.subject,b"freeze",lambda *_:True,self.policy)
            evaluate.assert_not_called()
        self.assertFalse((self.root/"offline").exists())

    def test_exception_after_intent_preserves_unknown_without_retry(self):
        with patch("gossip_harness.library_m1_acceptance_v4._runtime",return_value={}), \
             patch.object(BlackboxValidator,"evaluate",side_effect=RuntimeError("interruption")):
            with self.assertRaisesRegex(RuntimeError,"interruption"):
                run_once(self.root/"unknown",self.files,self.subject,b"freeze",lambda *_:True,self.policy)
        self.assertTrue((self.root/"unknown/intent.json").is_file())
        self.assertFalse((self.root/"unknown/receipt.json").exists())
        with self.assertRaises(ValueError):
            self.run_mocked(self.root/"unknown")

    def test_freeze_change_before_dispatch_or_during_execution_retains_intent(self):
        for decisions,expected_calls in (([True,False],0),([True,True,False],1)):
            with self.subTest(decisions=decisions):
                output = self.root/f"freeze-{expected_calls}"
                sequence = iter(decisions)
                with self.assertRaisesRegex(ValueError,"freeze changed"):
                    self.run_mocked(output,verify=lambda *_:next(sequence))
                self.assertTrue((output/"intent.json").is_file())
                self.assertFalse((output/"receipt.json").exists())
                self.assertEqual(len(self.calls),expected_calls)

    def test_evaluator_source_drift_fails_before_any_physical_work(self):
        with patch("gossip_harness.library_m1_acceptance_v4._LOADED_SOURCES",{"sandbox":"a"*64}), \
             patch("gossip_harness.library_m1_acceptance_v4._runtime") as runtime:
            with self.assertRaisesRegex(ValueError,"sources changed"):
                run_once(self.root/"drift",self.files,self.subject,b"freeze",lambda *_:True,self.policy)
            runtime.assert_not_called()

    def test_mismatched_original_receipt_is_retained_without_summary(self):
        original = self.evaluate
        def altered(files,cases):
            receipt = original(files,cases)
            receipt["source_sha256"] = "f"*64
            return receipt
        self.evaluate = altered
        with self.assertRaisesRegex(ValueError,"receipt differs"):
            self.run_mocked()
        self.assertTrue((self.root/"observation/validator-receipt.json").is_file())
        self.assertFalse((self.root/"observation/receipt.json").exists())


class LibraryM1AcceptanceV4AuthoredSmokeTests(unittest.TestCase):
    def test_actual_authored_intake_and_job_interfaces_match_independent_expectations(self):
        for case in acceptance_cases()[:3]:
            with self.subTest(case=case["id"]):
                actual = _authored_observation(case["input"]["scenario"])
                self.assertTrue(json_equal(actual,case["expected"]),json.dumps(actual)[:3000])

    def test_actual_authored_cli_uses_separate_processes_and_persisted_database(self):
        case = acceptance_cases()[3]
        self.assertTrue(json_equal(_authored_observation("cli_restart"),case["expected"]))

    def test_independent_intake_observation_detects_authored_blob_identity_defect(self):
        expected = acceptance_cases()[0]["expected"]
        self.assertFalse(json_equal(_authored_observation("intake",defective=True),expected))

    def test_rejected_intake_observes_traversal_write_outside_input_root(self):
        actual = _authored_observation("rejected_intake",escaped_write=True)
        self.assertTrue(actual["escaped_exists"])
        self.assertFalse(json_equal(actual,acceptance_cases()[1]["expected"]))


if __name__ == "__main__":
    unittest.main()
