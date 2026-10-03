"""Disposable offline controls for live admission; never use a real API key.

Synthetic qualification files test validation only and are not retained study
qualification. Default HTTPS workers are constructed but never invoked.
"""
from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest

from gossip_harness.ledger import Ledger
from gossip_harness.peer_financial_authority_v2 import (
    CumulativeAuthorityV2, FinancialError, canonical_payload, ledger_identity,
)
from gossip_harness.peer_financial_authority_v3 import (
    CumulativeAuthorityV3, FIXTURE_TRANSPORT, LIVE_TRANSPORT, PERMIT_PROTOCOL,
    PROTOCOL, QUALIFICATION_PROTOCOL, REQUIRED_TEST_CLASSES,
    digest, preflight_permit, profile_manifest, source_fingerprints, validate_qualification,
)
from gossip_harness.peer_project_contract_v2 import ActionRequest, Context, EvidenceRef, WorkKey, to_dict
from gossip_harness.worker import HTTPResponse, MODEL, OpenAIWorker


class Payloads:
    def __init__(self):
        self.values = {}

    def read_owned(self, actor, sha):
        return self.values[actor, sha]

    def put_owned(self, actor, raw):
        sha = hashlib.sha256(raw).hexdigest()
        self.values[actor, sha] = raw
        return sha


class FixtureTransport:
    def __init__(self):
        self.calls = 0
        self.timeout = False
        self.malformed = False

    def __call__(self, request, timeout, maximum):
        self.calls += 1
        if self.timeout:
            raise TimeoutError("Synthetic no-network timeout")
        body = {"id": "resp_fixture", "model": MODEL, "status": "completed", "service_tier": "default",
                "usage": {"input_tokens": 101, "output_tokens": 20, "total_tokens": 121},
                "output": [{"type": "message", "role": "assistant", "status": "completed",
                            "content": [{"type": "output_text", "text": "broken" if self.malformed else json.dumps({
                                "changes": [{"path": "src/a.py", "content": "fixed\n"}], "summary": "fixture"})}]}]}
        return HTTPResponse(200, {}, json.dumps(body).encode())


class PeerFinancialAuthorityV3Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.path = self.root / "disposable.sqlite"
        self.ledger = Ledger(self.path, 1_000_000)
        self.ledger.add_task("historical")
        lease = self.ledger.claim("historical", "old", now=1, ttl=1)
        self.ledger.reserve("prior-payment", lease, 2000, now=1)
        self.ledger.settle("prior-payment", 1000)
        self.transport = FixtureTransport()
        self.worker = OpenAIWorker("fixture-never-a-real-credential", max_output_tokens=64, timeout=2,
                                   transport=self.transport)
        self.payloads = Payloads()
        self.context = Context("a" * 64, "fixture-cohort", "trajectory", 0, "b" * 64)
        self.work = WorkKey("catalog", "m1", "B01", 0)
        self.contract = {"cohort_id": self.context.cohort_id, "execution_contract_sha256": "a" * 64,
                         "ledger_identity": ledger_identity(self.path), "journal_root": str(self.root / "journal"),
                         "transport_identity": FIXTURE_TRANSPORT, "task_specs": [{
                             "context": to_dict(self.context), "work": to_dict(self.work), "actors": ["B01"],
                             "kinds": ["build"], "profiles": ["mini"], "allowed_paths": ["src/a.py"],
                             "max_reserved_units": 900_000}]}
        self.authority = None
        self.permit = self.make_permit()
        self.addCleanup(self.cleanup)

    def cleanup(self):
        if self.authority is not None:
            self.authority.close()
        self.temp.cleanup()

    def make_permit(self, mode="fixture"):
        profiles = {"mini": profile_manifest(self.worker)}
        design = {"profiles": profiles, "max_workers": 1, "role_policy": "synthetic-unit-control",
                  "required_test_classes": list(REQUIRED_TEST_CLASSES),
                  "action_limits": {"total": 3, "by_kind": {"build": 3},
                                    "by_kind_generation": {"build": {"0": 3}}, "by_actor": {"B01": 3}},
                  "rehearsal_requirements": {"protocol": "synthetic-unit-rehearsal-control",
                      "exact_counts": {"role_processes": 1}, "minimum_counts": {"protected_releases": 1}}}
        return {"protocol": PERMIT_PROTOCOL, "mode": mode, "approval_ref": "unit-test-fixture-only",
                "execution_design": design, "execution_design_sha256": digest(design),
                "cohort_contract_sha256": digest(self.contract), "sources": source_fingerprints(),
                "profiles": profiles, "incremental_cap_micro_usd": 900_000, "expected_global_cap": 1_000_000,
                "expected_opening_usage": 1000, "max_workers": 1, "qualification": None}

    def arguments(self, **updates):
        args = {"existing_ledger_path": self.path, "service_root": self.root / "service",
                "cohort_contract": self.contract, "incremental_cap_micro_usd": 900_000,
                "expected_global_cap": 1_000_000, "expected_opening_usage": 1000,
                "payloads": self.payloads, "workers": {"mini": self.worker}, "max_workers": 1,
                "mode": self.permit["mode"], "clock": lambda: 100.0, "permit": self.permit,
                "expected_permit_sha256": digest(self.permit)}
        args.update(updates)
        return args

    def open(self, **updates):
        if self.authority is not None:
            self.authority.close()
            self.authority = None
        self.authority = CumulativeAuthorityV3.open(**self.arguments(**updates))
        return self.authority

    def bound(self, name, value):
        path = self.root / name
        raw = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
        path.write_bytes(raw)
        return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()}

    def qualification(self):
        # These synthetic files exercise refusal controls; they are not proof
        # that any project rehearsal or central class was physically executed.
        sources = self.permit["sources"]
        repo = Path(__file__).resolve().parent.parent
        from devtools.verify import runtime_fingerprint
        gate_source_map = {**sources}
        for name in REQUIRED_TEST_CLASSES:
            relative = name.split("::")[0]
            gate_source_map[relative] = hashlib.sha256((repo / relative).read_bytes()).hexdigest()
        gate_inputs = {"inputs": gate_source_map, "runner_sha256": hashlib.sha256(
            (repo / "devtools/verify.py").read_bytes()).hexdigest(), "static_command": None,
            "workers": 1, "runtime": runtime_fingerprint(), "auxiliary_inputs": {}}
        jobs = []
        for index, name in enumerate(REQUIRED_TEST_CLASSES):
            relative, class_name = name.split("::")
            cls = next(node for node in ast.parse((repo / relative).read_bytes()).body
                       if isinstance(node, ast.ClassDef) and node.name == class_name)
            tests = [{"id": name + "." + method, "status": "passed"} for method in sorted(
                node.name for node in cls.body if isinstance(node, ast.FunctionDef) and node.name.startswith("test"))]
            job = {"class": name, "status": "passed", "tests": tests, "returncode": 0,
                   "fixture_errors": [], "physical": True}
            ref = self.bound(f"class-{index}.json", job)
            jobs.append({**job, "evidence_path": ref["path"], "evidence_sha256": ref["sha256"]})
        total = sum(len(job["tests"]) for job in jobs)
        summary = {"status": "passed", "fingerprint": hashlib.sha256(
            json.dumps(gate_inputs, sort_keys=True).encode()).hexdigest(), "selected_total": total,
            "outcomes": {"passed": total}, "static": {"status": "passed", "returncode": 0},
            "stale_inputs": False, "stale_runtime": False, "jobs": jobs}
        contract = {"execution_design": self.permit["execution_design"],
                    "execution_design_sha256": self.permit["execution_design_sha256"], "sources": sources}
        receipt = {"passed": True, "physically_executed": True, "execution_contract_sha256": digest(contract),
                   "execution_design_sha256": self.permit["execution_design_sha256"], "mode": "fixture",
                   "protocol": "synthetic-unit-rehearsal-control", "cleanup_errors": [],
                   "counts": {"api_calls": 0, "api_spend_micro_usd": 0, "role_processes": 1, "protected_releases": 1}}
        fixture_permit = {**self.permit, "mode": "fixture", "qualification": None}
        financial = {"protocol": PROTOCOL, "mode": "fixture", "observation_kind": "simulated", "sources": sources,
                     "execution_design_sha256": self.permit["execution_design_sha256"],
                     "operator_permit": fixture_permit, "operator_permit_sha256": digest(fixture_permit),
                     "contract": self.contract}
        role = {"actor": "B01", "pid": 123, "journal_root": str(self.root / "role-journal"),
                "completed_actions": ["synthetic-completed-action"]}
        role_ref = self.bound("role-final.json", role)
        receipt.update(financial_config=financial, financial_config_sha256=digest(financial),
                       final_roles=[{**role, "receipt": role_ref["path"], "sha256": role_ref["sha256"]}],
                       owned_process_exit_codes={"B01": 0}, accounting={"unsettled_reservations": 0})
        capsule = {"protocol": QUALIFICATION_PROTOCOL,
                   "execution_design_sha256": self.permit["execution_design_sha256"], "sources": sources,
                   "gate": {"summary": self.bound("summary.json", summary),
                            "inputs": self.bound("inputs.json", gate_inputs)},
                   "rehearsal": {"receipt": self.bound("rehearsal.json", receipt),
                                 "execution_contract": self.bound("execution-contract.json", contract)}}
        return self.bound("qualification.json", capsule)

    def live_setup(self):
        self.worker = OpenAIWorker("fixture-never-a-real-credential", max_output_tokens=64, timeout=2)
        self.contract["transport_identity"] = LIVE_TRANSPORT
        self.permit = self.make_permit("live")
        self.permit["qualification"] = self.qualification()

    def action(self, *, work=None, actor="B01", number="one"):
        work = self.work if work is None else work
        task_id = self.authority.task_id(self.context, work)
        request = {"task_id": task_id, "instructions": "Fix the source.", "allowed_paths": ["src/a.py"],
                   "files": {"src/a.py": "broken\n"}, "base_sha": "c" * 40, "attempt": 1, "feedback": ""}
        raw = canonical_payload({"worker_request": request, "view_manifest_sha256": "d" * 64})
        sha = self.payloads.put_owned(actor, raw)
        ref = EvidenceRef("e" * 64, actor, "worker-request", sha)
        return ActionRequest(self.context, "build-" + number, "request-" + number, actor, "build", work, "mini", ref, "d" * 64)

    def invoke_fixture(self):
        action = self.action()
        lease = self.authority.claim("B01", self.context, self.work)
        self.authority.submit("B01", action, lease)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            reply = self.authority.lookup("B01", action.request_id)
            if reply.state not in {"pending", "waiting"}:
                return action, lease, reply
            time.sleep(.005)
        self.fail("Disposable fixture did not become terminal")

    def no_enrollment(self):
        with sqlite3.connect(self.path) as db:
            self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE name='financial_cohorts_v2'").fetchone())
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 1000)
        self.assertEqual(self.transport.calls, 0)

    def test_fixture_is_simulated_and_preserves_exact_settlement_replay_and_recovery(self):
        self.open()
        action, lease, reply = self.invoke_fixture()
        self.assertEqual((reply.state, reply.usage_units), ("completed", 166))
        self.assertEqual(self.authority.config["protocol"], PROTOCOL)
        self.assertEqual(self.authority.config["observation_kind"], "simulated")
        self.assertEqual(self.authority.config["mode"], "fixture")
        self.assertEqual(self.authority.verified_terminal(reply.binding)["result"].changes, {"src/a.py": "fixed\n"})
        self.assertEqual(self.authority.submit("B01", action, lease), reply)
        old_hash = self.authority.config_sha256
        self.open(recovery=True)
        self.assertEqual(self.authority.lookup("B01", action.request_id), reply)
        self.assertEqual(self.authority.config_sha256, old_hash)
        self.assertEqual(self.transport.calls, 1)
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 1166)

    def test_pin_is_mandatory_and_is_not_inferred_from_permit(self):
        for changes in ({"permit": None}, {"expected_permit_sha256": None}, {"expected_permit_sha256": "f" * 64}):
            with self.subTest(changes=tuple(changes)), self.assertRaises(FinancialError):
                self.open(**changes)
        self.no_enrollment()

    def test_permit_is_closed_and_binds_contract_paths_and_allocation(self):
        for key, value in (("unexpected", True), ("incremental_cap_micro_usd", 800_000),
                           ("expected_opening_usage", 999), ("max_workers", 2),
                           ("cohort_contract_sha256", "f" * 64)):
            permit = {**self.permit, key: value}
            with self.subTest(key=key), self.assertRaises(FinancialError):
                self.open(permit=permit, expected_permit_sha256=digest(permit))
        self.no_enrollment()

    def test_source_drift_or_missing_dependency_refuses_before_ledger_mutation(self):
        for sources in ({**self.permit["sources"], "gossip_harness/worker.py": "f" * 64},
                        {k: v for k, v in self.permit["sources"].items() if not k.endswith("worker.py")}):
            permit = {**self.permit, "sources": sources}
            with self.assertRaises(FinancialError):
                self.open(permit=permit, expected_permit_sha256=digest(permit))
        self.no_enrollment()

    def test_fixture_rejects_default_https_and_live_identity(self):
        default = OpenAIWorker("fixture-never-a-real-credential", max_output_tokens=64, timeout=2)
        with self.assertRaises(FinancialError):
            self.open(workers={"mini": default})
        self.contract["transport_identity"] = LIVE_TRANSPORT
        self.permit = self.make_permit()
        with self.assertRaises(FinancialError):
            self.open()
        self.no_enrollment()

    def test_live_requires_actual_qualification_and_exact_default_transport(self):
        self.live_setup()
        permit = {**self.permit, "qualification": None}
        with self.assertRaises(FinancialError):
            self.open(permit=permit, expected_permit_sha256=digest(permit))
        injected = OpenAIWorker("fixture-never-a-real-credential", max_output_tokens=64, timeout=2,
                                transport=self.transport)
        with self.assertRaises(FinancialError):
            self.open(workers={"mini": injected})
        self.no_enrollment()

    def test_qualified_live_construction_does_not_invoke_or_serialize_credential(self):
        self.live_setup()
        self.open()
        self.assertEqual(self.authority.config["mode"], "live")
        self.assertEqual(self.authority.config["observation_kind"], "provider")
        self.assertNotIn("fixture-never-a-real-credential", json.dumps(self.authority.config))
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 1000)
        self.assertEqual(self.transport.calls, 0)

    def test_keyless_preflight_does_not_create_financial_membership_or_service_files(self):
        self.live_setup()
        before = self.path.read_bytes()
        evidence = CumulativeAuthorityV3.preflight_permit(
            self.contract, 900_000, 1_000_000, 1000, workers={"mini": self.worker}, permit=self.permit,
            expected_permit_sha256=digest(self.permit), max_workers=1)
        self.assertFalse(evidence["ledger_opened"])
        self.assertFalse(evidence["provider_invoked"])
        self.assertEqual(before, self.path.read_bytes())
        self.assertFalse((self.root / "service").exists())
        self.assertFalse((self.root / "journal").exists())
        self.no_enrollment()

    def test_public_preflight_takes_only_pinned_profiles_and_rejects_changed_prices(self):
        self.live_setup()
        evidence = preflight_permit(self.permit, digest(self.permit), self.contract, self.permit["profiles"])
        self.assertFalse(evidence["provider_invoked"])
        altered = deepcopy(self.permit["profiles"])
        altered["mini"]["manifest"]["input_micro_usd_per_million"] = 0
        with self.assertRaises(FinancialError):
            preflight_permit(self.permit, digest(self.permit), self.contract, altered)
        self.no_enrollment()

    def test_unknown_transport_retains_reservation_and_recovery_never_reinvokes(self):
        self.transport.timeout = True
        self.open()
        action, lease, reply = self.invoke_fixture()
        self.assertEqual(reply.state, "unknown")
        self.assertIsNone(reply.usage_units)
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 1000 + reply.binding.reserved_units)
        self.assertEqual(self.authority.submit("B01", action, lease), reply)
        self.open(recovery=True)
        self.assertEqual(self.authority.lookup("B01", action.request_id).state, "unknown")
        self.assertEqual(self.transport.calls, 1)

    def test_malformed_known_result_is_settled_without_transport_retry(self):
        self.transport.malformed = True
        self.open()
        action, lease, reply = self.invoke_fixture()
        self.assertEqual((reply.state, reply.usage_units), ("failed", 166))
        self.open(recovery=True)
        self.assertEqual(self.authority.submit("B01", action, lease), reply)
        self.assertEqual(self.transport.calls, 1)
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 1166)

    def test_transport_change_at_last_boundary_never_reaches_https(self):
        default = OpenAIWorker("fixture-never-a-real-credential")._transport
        def mutate(name, request_id):
            if name == "before_invoke":
                self.worker._transport = default
        self.open(crash_hook=mutate)
        action, lease, reply = self.invoke_fixture()
        self.assertEqual(reply.state, "unknown")
        self.assertEqual(self.transport.calls, 0)
        self.assertEqual(self.ledger.budget()["spent_or_reserved"], 1000 + reply.binding.reserved_units)
        self.worker._transport = self.transport
        self.open(recovery=True)
        self.assertEqual(self.authority.lookup("B01", action.request_id).state, "unknown")
        self.assertEqual(self.transport.calls, 0)

    def test_profile_change_at_last_boundary_is_not_invoked(self):
        def mutate(name, request_id):
            if name == "before_invoke":
                self.worker.max_output_tokens = 128
        self.open(crash_hook=mutate)
        _, _, reply = self.invoke_fixture()
        self.assertEqual(reply.state, "unknown")
        self.assertEqual(self.transport.calls, 0)

    def test_recovery_cannot_change_opening_usage_or_operator_permit(self):
        self.open()
        self.invoke_fixture()
        permit = {**self.permit, "expected_opening_usage": 1166}
        with self.assertRaises(FinancialError):
            self.open(recovery=True, expected_opening_usage=1166, permit=permit,
                      expected_permit_sha256=digest(permit))
        self.assertEqual(self.transport.calls, 1)

    def test_allocation_cannot_exceed_approved_remaining_balance(self):
        permit = {**self.permit, "incremental_cap_micro_usd": 1_000_000}
        with self.assertRaises(FinancialError):
            self.open(incremental_cap_micro_usd=1_000_000, permit=permit,
                      expected_permit_sha256=digest(permit))
        self.no_enrollment()

    def test_v2_offline_refusal_remains_intact(self):
        args = self.arguments()
        args.pop("permit")
        args.pop("expected_permit_sha256")
        for mode in ("live", "fixture"):
            with self.assertRaises(FinancialError):
                CumulativeAuthorityV2.open(**{**args, "mode": mode})
        self.no_enrollment()

    def test_live_permit_cannot_be_used_to_relabel_fixture_observation(self):
        self.live_setup()
        with self.assertRaises(FinancialError):
            self.open(mode="fixture")
        self.no_enrollment()

    def test_qualification_rejects_changed_ref_bytes(self):
        ref = self.qualification()
        Path(ref["path"]).write_text("{}")
        with self.assertRaises(FinancialError):
            validate_qualification(ref, self.permit["execution_design"], self.permit["sources"])

    def test_qualification_rejects_nonpassing_or_unexecuted_gate(self):
        for changes in ({"status": "failed"}, {"stale_inputs": True}, {"outcomes": {"skipped": 2}},
                        {"static": {"status": "failed", "returncode": 1}}):
            ref = self.qualification()
            capsule = json.loads(Path(ref["path"]).read_text())
            summary = json.loads(Path(capsule["gate"]["summary"]["path"]).read_text())
            capsule["gate"]["summary"] = self.bound("summary.json", {**summary, **changes})
            ref = self.bound("qualification.json", capsule)
            with self.subTest(changes=changes), self.assertRaises(FinancialError):
                validate_qualification(ref, self.permit["execution_design"], self.permit["sources"])

    def test_qualification_rejects_missing_required_class_and_class_receipt_tamper(self):
        for tamper in ("missing", "receipt"):
            ref = self.qualification()
            capsule = json.loads(Path(ref["path"]).read_text())
            summary = json.loads(Path(capsule["gate"]["summary"]["path"]).read_text())
            if tamper == "missing":
                summary["jobs"].pop()
                capsule["gate"]["summary"] = self.bound("summary.json", summary)
                ref = self.bound("qualification.json", capsule)
            else:
                Path(summary["jobs"][0]["evidence_path"]).write_text("{}")
            with self.subTest(tamper=tamper), self.assertRaises(FinancialError):
                validate_qualification(ref, self.permit["execution_design"], self.permit["sources"])

    def test_qualification_rejects_different_design_and_false_rehearsal(self):
        for changes in ({"passed": False}, {"physically_executed": False}, {"mode": "live"},
                        {"execution_design_sha256": "f" * 64}, {"execution_contract_sha256": "f" * 64},
                        {"counts": {"api_calls": 1, "api_spend_micro_usd": 1}}, {"cleanup_errors": ["orphan"]}):
            ref = self.qualification()
            capsule = json.loads(Path(ref["path"]).read_text())
            receipt = json.loads(Path(capsule["rehearsal"]["receipt"]["path"]).read_text())
            capsule["rehearsal"]["receipt"] = self.bound("rehearsal.json", {**receipt, **changes})
            ref = self.bound("qualification.json", capsule)
            with self.subTest(changes=changes), self.assertRaises(FinancialError):
                validate_qualification(ref, self.permit["execution_design"], self.permit["sources"])

    def test_fixture_qualification_and_runtime_permit_mutation_are_rejected(self):
        permit = {**self.permit, "qualification": self.qualification()}
        with self.assertRaises(FinancialError):
            self.open(permit=permit, expected_permit_sha256=digest(permit))
        def mutate(name, request_id):
            if name == "before_invoke":
                self.authority.permit["approval_ref"] = "altered"
        self.open(crash_hook=mutate)
        _, _, reply = self.invoke_fixture()
        self.assertEqual(reply.state, "unknown")
        self.assertEqual(self.transport.calls, 0)

    def add_second_slot(self, actor="B01"):
        work = replace(self.work, slot_id="second")
        spec = deepcopy(self.contract["task_specs"][0])
        spec.update(work=to_dict(work), actors=[actor])
        self.contract["task_specs"].append(spec)
        self.permit = self.make_permit()
        self.permit["execution_design"]["action_limits"]["by_actor"][actor] = 3
        return work

    def freeze_design(self):
        self.permit["execution_design_sha256"] = digest(self.permit["execution_design"])

    def assert_sequential_limit(self, field):
        second = self.add_second_slot()
        limits = self.permit["execution_design"]["action_limits"]
        if field == "total":
            limits["total"] = 1
        elif field == "kind":
            limits["by_kind"]["build"] = 1
            limits["by_kind_generation"]["build"]["0"] = 1
        elif field == "generation":
            limits["by_kind_generation"]["build"]["0"] = 1
        else:
            limits["by_actor"]["B01"] = 1
        self.freeze_design()
        self.open()
        original, original_lease, known = self.invoke_fixture()
        self.assertEqual(known.state, "completed")
        lease = self.authority.claim("B01", self.context, second)
        action = self.action(work=second, number="two")
        reply = self.authority.submit("B01", action, lease)
        self.assertEqual((reply.state, reply.reason), ("waiting", "cohort_call_limit"))
        self.assertEqual(self.authority.submit("B01", original, original_lease), known)
        self.open(recovery=True)
        self.assertEqual(self.authority.submit("B01", action, lease).reason, "cohort_call_limit")
        self.assertEqual(self.transport.calls, 1)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM financial_actions_v2").fetchone()[0], 1)

    def test_total_admission_limit_survives_settlement_replay_and_reopen(self):
        self.assert_sequential_limit("total")

    def test_kind_admission_limit_survives_settlement_replay_and_reopen(self):
        self.assert_sequential_limit("kind")

    def test_generation_admission_limit_survives_settlement_replay_and_reopen(self):
        self.assert_sequential_limit("generation")

    def test_actor_admission_limit_survives_settlement_replay_and_reopen(self):
        self.assert_sequential_limit("actor")

    def test_concurrent_admissions_cannot_exceed_total_call_limit(self):
        second = self.add_second_slot("B02")
        self.permit["execution_design"]["action_limits"]["total"] = 1
        self.permit["max_workers"] = 2
        self.permit["execution_design"]["max_workers"] = 2
        self.freeze_design()
        self.open(max_workers=2)
        pairs = [(self.action(), self.authority.claim("B01", self.context, self.work)),
                 (self.action(work=second, actor="B02", number="two"),
                  self.authority.claim("B02", self.context, second))]
        with ThreadPoolExecutor(max_workers=2) as pool:
            replies = list(pool.map(lambda pair: self.authority.submit(pair[0].actor, *pair), pairs))
        self.assertEqual(sorted(reply.state for reply in replies), ["pending", "waiting"])
        self.assertEqual(next(reply for reply in replies if reply.state == "waiting").reason, "cohort_call_limit")
        self.authority.close()
        self.assertEqual(self.transport.calls, 1)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM financial_actions_v2").fetchone()[0], 1)

    def test_action_policy_requires_all_actors_kinds_and_generation_scopes(self):
        for field in ("by_actor", "by_kind", "by_kind_generation"):
            permit = deepcopy(self.permit)
            permit["execution_design"]["action_limits"][field] = {}
            permit["execution_design_sha256"] = digest(permit["execution_design"])
            with self.subTest(field=field), self.assertRaises(FinancialError):
                self.open(permit=permit, expected_permit_sha256=digest(permit))
        self.no_enrollment()

    def test_worker_payload_and_usage_override_are_refused(self):
        for method in ("_payload", "_usage"):
            setattr(self.worker, method, lambda *args: 0)
            with self.subTest(method=method), self.assertRaises(FinancialError):
                self.open()
            delattr(self.worker, method)
        self.no_enrollment()

    def test_worker_cannot_adopt_an_unpriced_profile_even_in_a_pinned_permit(self):
        self.worker._profile = replace(self.worker._profile, input_micro_usd_per_million=0)
        self.permit = self.make_permit()
        with self.assertRaises(FinancialError):
            self.open()
        self.no_enrollment()

    def test_qualification_rejects_partial_class_even_when_summary_counts_match(self):
        ref = self.qualification()
        capsule = json.loads(Path(ref["path"]).read_text())
        summary = json.loads(Path(capsule["gate"]["summary"]["path"]).read_text())
        for index, job in enumerate(summary["jobs"]):
            job["tests"] = job["tests"][:1]
            job_ref = self.bound(f"partial-{index}.json", job)
            job.update(evidence_path=job_ref["path"], evidence_sha256=job_ref["sha256"])
        summary.update(selected_total=len(summary["jobs"]), outcomes={"passed": len(summary["jobs"])})
        capsule["gate"]["summary"] = self.bound("summary.json", summary)
        ref = self.bound("qualification.json", capsule)
        with self.assertRaises(FinancialError):
            validate_qualification(ref, self.permit["execution_design"], self.permit["sources"])

    def test_qualification_rejects_changed_runtime_even_with_new_summary_fingerprint(self):
        ref = self.qualification()
        capsule = json.loads(Path(ref["path"]).read_text())
        inputs = json.loads(Path(capsule["gate"]["inputs"]["path"]).read_text())
        inputs["runtime"]["python"] = "different-runtime"
        capsule["gate"]["inputs"] = self.bound("inputs.json", inputs)
        summary = json.loads(Path(capsule["gate"]["summary"]["path"]).read_text())
        summary["fingerprint"] = hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()
        capsule["gate"]["summary"] = self.bound("summary.json", summary)
        ref = self.bound("qualification.json", capsule)
        with self.assertRaises(FinancialError):
            validate_qualification(ref, self.permit["execution_design"], self.permit["sources"])

    def test_qualification_rejects_incomplete_roles_or_financial_fixture_identity(self):
        for changes in ({"final_roles": []}, {"protocol": "different-runner"},
                        {"financial_config_sha256": "f" * 64}, {"accounting": {"unsettled_reservations": 1}},
                        {"owned_process_exit_codes": {"B01": 1}}):
            ref = self.qualification()
            capsule = json.loads(Path(ref["path"]).read_text())
            receipt = json.loads(Path(capsule["rehearsal"]["receipt"]["path"]).read_text())
            capsule["rehearsal"]["receipt"] = self.bound("rehearsal.json", {**receipt, **changes})
            ref = self.bound("qualification.json", capsule)
            with self.subTest(changes=changes), self.assertRaises(FinancialError):
                validate_qualification(ref, self.permit["execution_design"], self.permit["sources"])
