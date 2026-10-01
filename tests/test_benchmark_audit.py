"""Offline tamper tests for the evidence-frontier retained-evidence auditor."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from analysis import audit_benchmark
from analysis.audit_benchmark import (
    POLICIES, _prompts, audit_stage, check_fault_banks, check_roster, digest, freeze_barrier, initial_opportunities,
    review_from_call, roster_for, scope_proposal, scout_proposals, sha,
)
from gossip_harness.benchmark_stage import run_benchmark_stage
from gossip_harness.verification_probes import parse_proposals, revalidate_cases
from gossip_harness.worker import WorkerResult


def plan():
    return dict(protocol="evidence-frontier-v1", projects=["graph-patch", "calendar-exchange"],
        milestones=2, roster_seed=20261002, policies={policy: dict(repetitions=reps, initial_builders=count,
        model=model, formation=formation) for policy, (reps, count, model, formation) in POLICIES.items()})


class BenchmarkRosterAuditTests(unittest.TestCase):
    def test_actual_runner_contract_source_inventory_is_exact(self):
        from gossip_harness.benchmark_experiment import contract
        sources = contract()["sources"]
        audit_benchmark.check_source_inventory(sources)
        self.assertIn("benchmark_diagnostics.py", sources)
        missing = dict(sources)
        del missing["benchmark_diagnostics.py"]
        extra = {**sources, "unexpected.py": "0" * 64}
        for changed in (missing, extra):
            with self.subTest(sources=sorted(changed)), self.assertRaisesRegex(ValueError, "source inventory"):
                audit_benchmark.check_source_inventory(changed)

    def test_fault_bank_uses_its_separate_protocol_and_project_stage_lists(self):
        bank = dict(protocol="evidence-frontier-fault-bank-v1", projects=[dict(project_id=project,
            stages=[dict(stage_index=i, fault_bank=[], correct_controls=[]) for i in range(2)]) for project in plan()["projects"]])
        check_fault_banks(bank, digest(bank))
        changed = deepcopy(bank)
        changed["projects"][0]["stages"].reverse()
        with self.assertRaises(ValueError):
            check_fault_banks(changed, digest(changed))

    def test_exact_ten_asymmetrically_replicated_opportunities(self):
        rows = roster_for(plan())
        check_roster(rows, plan())
        self.assertEqual(len(rows), 10)
        self.assertEqual(sum(policy == "strong-anchor" for _, policy, _ in rows), 2)
        for changed in (rows[:-1], rows + [rows[0]], [rows[0]] + rows[2:] + [rows[0]]):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                check_roster(changed, plan())

    def test_anchor_is_not_silently_given_equal_replication_or_boolean_rep_id(self):
        wrong = plan()
        wrong["policies"]["strong-anchor"]["repetitions"] = 2
        with self.assertRaises(ValueError):
            roster_for(wrong)
        rows = roster_for(plan())
        rows[0][2] = False
        with self.assertRaises(ValueError):
            check_roster(rows, plan())


class BenchmarkFormationAuditTests(unittest.TestCase):
    def setup_formation(self, policy):
        initial = {"app.py": "inert baseline"}
        cases = [dict(id="public", input=1, expected=1, requirement="R")]
        rows, calls, sources = [], {}, {}
        previous = initial
        for i in range(POLICIES[policy][1]):
            source = previous if policy == "sequential-four" else initial
            files = {**source, "verification-context.json": json.dumps(dict(verified_cases=cases))}
            binding = dict(tip_sha=str(i), files_sha256=digest({"app.py": f"inert {i}"}))
            rows.append(dict(slot=f"slot-{i}", round=1, previous_files_sha256=digest(source), binding=binding))
            calls[f"builder-slot-{i}-1"] = (dict(files=files), dict(role="builder", model=POLICIES[policy][2]))
            sources[str(i)] = previous = {"app.py": f"inert {i}"}
        return initial, cases, rows, calls, sources

    def test_serial_consumes_predecessor_and_independent_consumes_stage_start(self):
        for policy in POLICIES:
            initial, cases, rows, calls, sources = self.setup_formation(policy)
            expected = POLICIES[policy][1]
            self.assertEqual(initial_opportunities(policy, initial, rows, calls, lambda b: sources[b["tip_sha"]], cases), expected)
            if expected > 1:
                calls["builder-slot-1-1"][0]["files"]["app.py"] = "unexpected peer/current scout source"
                with self.subTest(policy=policy), self.assertRaisesRegex(ValueError, "formation"):
                    initial_opportunities(policy, initial, rows, calls, lambda b: sources[b["tip_sha"]], cases)

    def test_current_stage_generated_evidence_cannot_enter_initial_calls(self):
        initial, cases, rows, calls, sources = self.setup_formation("independent-four")
        ctx = json.loads(calls["builder-slot-0-1"][0]["files"]["verification-context.json"])
        ctx["verified_cases"].append(dict(id="new-scout", input=2, expected=2, requirement="R"))
        calls["builder-slot-0-1"][0]["files"]["verification-context.json"] = json.dumps(ctx)
        with self.assertRaisesRegex(ValueError, "formation"):
            initial_opportunities("independent-four", initial, rows, calls, lambda b: sources[b["tip_sha"]], cases)

    def test_noop_and_rejected_edits_preserve_only_existing_eligibility(self):
        initial = {"app.py": "inert"}
        for changes in ({}, {"app.py": "inert"}, {"outside.py": "not applied"}, {"app.py": None}, None):
            with self.subTest(changes=changes):
                files, valid, _, changed = scope_proposal(initial, True, changes, ["app.py"])
                self.assertEqual(files, initial)
                self.assertTrue(valid)
                self.assertFalse(changed)
                self.assertFalse(scope_proposal(initial, False, changes, ["app.py"])[1])


class BenchmarkBarrierAuditTests(unittest.TestCase):
    def test_private_and_fault_diagnostics_require_whole_cohort_freeze(self):
        ids = [f"run-{i}" for i in range(10)]
        frozen = dict(protocol="evidence-frontier-v1", trajectories=[{"run_id": name} for name in ids],
                      private_evaluation_started=False, frozen_monotonic_ns=100)
        spans = {0: dict(kind="model_trajectory", finished_monotonic_ns=99),
                 1: dict(kind="docker_validation", purpose="final_private", started_monotonic_ns=101),
                 2: dict(kind="docker_validation", purpose="fault_matrix", started_monotonic_ns=102)}
        freeze_barrier(frozen, ids, spans)
        for purpose in ("final_private", "fault_matrix", "cross_policy"):
            changed = deepcopy(spans)
            changed[2].update(purpose=purpose, started_monotonic_ns=99)
            with self.subTest(purpose=purpose), self.assertRaisesRegex(ValueError, "preceded"):
                freeze_barrier(frozen, ids, changed)
        frozen["trajectories"].pop()
        with self.assertRaises(ValueError):
            freeze_barrier(frozen, ids, spans)


class BenchmarkStageEvidenceAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve() / "run" / "stage-0"
        self.root.mkdir(parents=True)
        self.initial = {"app.py": "inert initial", "README.md": "trusted scaffold"}
        self.project = dict(id="toy", title="Toy", allowed_paths=["app.py"], initial_files=self.initial,
            stages=[dict(specification="Return identity", requirements=["R"], visible_cases=[dict(id="public", input=0, expected=0, requirement="R")], hidden_cases=[])])
        self.calls, self.trees, self.spans = {}, {}, {}
        self.time, self.lock = 0, threading.RLock()
        source = (Path(__file__).parents[1] / "gossip_harness" / "benchmark_stage.py").read_text()
        self.prompts = _prompts(source)
        self.prompts["scout_prompt"] = "Public scout"
        import ast
        self.prompts["escalation_suffix"], = [n.value for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Constant)
            and isinstance(n.value, str) and n.value.startswith(" This is a bounded continuation review")]
        self.contract = dict(image="sha256:" + "f" * 64, case_timeout_seconds=12, suite_timeout_seconds=300)

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, sort_keys=True))

    def timed(self, kind, **labels):
        with self.lock:
            self.time += 10
            identity = len(self.spans)
            self.spans[identity] = dict(span_id=identity, kind=kind, run_id="run", stage_index=0,
                started_monotonic_ns=self.time, finished_monotonic_ns=self.time + 5, status="finished", **labels)
            return identity

    def make(self, policy="independent-four", stalled=False, duplicate=False):
        self.policy = policy
        scouts = {}
        def invoke(**args):
            role, meta = args["role"], args["metadata"]
            call_id = f"{role}-{meta['candidate_id']}-{meta['round']}"
            span_id = self.timed("worker_request", call_id=call_id, role=role, model=args["model"])
            if role == "scout":
                changes = {"probes.json": json.dumps({"probes": [dict(input=meta["scout_index"] + 1, expected=meta["scout_index"] + 1, requirement="R")]})}
            elif role == "builder":
                changes = {} if meta["phase"] == "repair" else {"app.py": "identical inert source" if duplicate else args["files"]["app.py"] + " " + meta["candidate_id"], "notes.json": json.dumps(dict(notes="Keep contract", remaining=[]))}
            else:
                changes = {"review.json": json.dumps(dict(candidate=meta["alias_by_slot"]["slot-0"], action="inspect" if stalled else "accept", notes="Check contract", remaining=["R"] if stalled else [], probes=[]))}
            request = {key: deepcopy(args[key]) for key in ("files", "instructions", "allowed_paths", "feedback")}
            request["allowed_paths"] = list(request["allowed_paths"])
            call = dict(call_id=call_id, role=role, model=args["model"], changes=changes, controller_metadata=deepcopy(meta), request_span_id=span_id)
            self.calls[call_id] = (request, call)
            return WorkerResult(changes, "inert", 0, {"api_calls": 0})
        def retain(files, label):
            binding = dict(store_path=str(self.root / (label + ".git")), tip_sha=digest([label, files]), files_sha256=digest(files))
            self.trees[binding["tip_sha"]] = deepcopy(files)
            return binding
        def evaluate(files, cases, label):
            receipt = dict(source_sha256=digest(files), suite_sha256=digest([{k: c[k] for k in ("input", "expected", "id", "requirement")} for c in cases]),
                image_id=self.contract["image"], case_timeout_seconds=12, timeout_seconds=300, cleanup_verified=True,
                passed=True, status="passed", outcomes=[dict(index=i, passed=True, status="passed", actual=c["expected"], stderr="receipt for " + label) for i, c in enumerate(cases)])
            identity = self.timed("docker_validation", purpose="stage_evidence", case_count=len(cases), label=label)
            path = self.root / "evaluations" / f"{identity}.json"
            self.write(path, receipt)
            self.spans[identity].update(receipt_path=str(path), receipt_sha256=sha(path))
            return receipt
        def gate(envelope, index, origin):
            if envelope["mode"] == "revalidate":
                return revalidate_cases(envelope["probes"], stage_index=index, reference=lambda i, value: value, validate_input=lambda i, value: None, origin=origin)
            return parse_proposals(json.dumps({"probes": envelope["probes"]}), stage_index=index, requirements=["R"],
                reference=lambda i, value: value, validate_input=lambda i, value: None, origin=origin,
                existing_cases=envelope["existing_cases"], max_new=envelope["max_new"], namespace="toy")
        def after_initial(pool):
            self.time += 10
            path = self.root / "initial-pool.json"
            self.write(path, dict(protocol="evidence-frontier-v1", run_id="run", stage_index=0,
                pool=pool, pool_sha256=digest(pool), frozen_monotonic_ns=self.time))
            context = dict(project_id="toy", stage_index=0, milestones=[dict(requirements=["R"], specification="Return identity")], public_cases=self.project["stages"][0]["visible_cases"])
            inputs = {**self.initial, "scout-context.json": json.dumps(context, sort_keys=True)}
            proposals = []
            for i in range(2):
                result = invoke(role="scout", model="cheap", files=inputs, instructions="Public scout", allowed_paths=["probes.json"], feedback="",
                    metadata=dict(role="scout", round=1, candidate_id=f"scout-{i}", scout_index=i, phase="post_initial_freeze"))
                proposals.append(dict(scout_id=f"scout-{i}", probes=json.loads(result.changes["probes.json"])["probes"]))
            scouts.update(calls=2, source_sha256=digest(self.initial), shared_input_sha256=digest(inputs), initial_pool_sha256=digest(pool),
                initial_pool_artifact_sha256=sha(path), proposals=proposals)
            return dict(freeze_id=str(path), pool_sha256=digest(pool), scouts=proposals)
        stage = run_benchmark_stage(self.project, 0, policy, self.initial, {}, invoke=invoke, retain=retain, evaluate=evaluate,
            validate_probes=gate, emit=lambda *a, **k: None, after_initial=after_initial,
            baseline_approval=dict(files_sha256=digest(self.initial), source_valid=True, approval_id="trusted"))
        scouts.update(admitted=stage["metrics"]["admitted_scout_probes"], rejected=stage["metrics"]["rejected_scout_probes"])
        self.write(self.root / "scouts.json", scouts)
        stage.update(stage_index=0, scouts=scouts)
        self.stage = stage

    def audit(self, stage=None):
        return audit_stage(stage or self.stage, self.project, 0, self.policy, self.initial, {}, self.calls, self.contract,
            self.root, lambda binding: self.trees[binding["tip_sha"]], self.spans, "run", self.prompts)

    def test_reconstructs_all_three_policies_from_physical_evidence(self):
        for policy in POLICIES:
            with self.subTest(policy=policy):
                self.setUp()
                self.make(policy)
                self.assertTrue(self.audit()["completed"])

    def test_scout_cannot_run_before_freeze_or_review_forge_matrix(self):
        self.make()
        call = self.calls["scout-scout-0-1"][1]
        timed = self.spans[call["request_span_id"]]
        original = timed["started_monotonic_ns"]
        timed["started_monotonic_ns"] = 0
        with self.assertRaisesRegex(ValueError, "before freeze"):
            self.audit()
        timed["started_monotonic_ns"] = original
        request = self.calls["reviewer-review-1"][0]
        context = json.loads(request["files"]["verification-context.json"])
        context["matrix"][self.stage["selected"]]["public"]["actual"] = "forged"
        request["files"]["verification-context.json"] = json.dumps(context)
        with self.assertRaisesRegex(ValueError, "physical matrix"):
            self.audit()

    def test_noop_repairs_and_reviewer_unresolved_requirements_remain_bounded(self):
        self.make("sequential-four", stalled=True)
        self.assertFalse(self.audit()["completed"])
        changed = deepcopy(self.stage)
        changed["completed"] = True
        with self.assertRaisesRegex(ValueError, "completion"):
            self.audit(changed)

    def test_cheap_arm_strong_repair_requires_its_real_unique_escalation(self):
        self.make("sequential-four", stalled=True)
        self.audit()
        call = next(call for _, call in self.calls.values() if call["role"] == "builder" and call["controller_metadata"].get("escalation_id"))
        call["controller_metadata"]["escalation_id"] = "invented-escalation"
        with self.assertRaisesRegex(ValueError, "Unbound"):
            self.audit()

    def test_scout_provenance_distinguishes_boolean_from_integer(self):
        self.make()
        # Both fields are changed together so Python == would accept the raw
        # response mismatch. The auditor must stop before probe gate equality.
        self.stage["scouts"]["proposals"][0]["probes"][0]["expected"] = True
        self.write(self.root / "scouts.json", self.stage["scouts"])
        with self.assertRaisesRegex(ValueError, "Scout proposal parse"):
            self.audit()

    def test_identical_sources_keep_distinct_candidate_execution_receipts(self):
        self.make("independent-four", duplicate=True)
        self.assertEqual(len({digest(c["files"]) for c in self.stage["initial_pool"]["candidates"].values()}), 1)
        self.assertTrue(self.audit()["completed"])

    def test_acceptance_binds_exact_source_and_is_last_provider_action(self):
        self.make()
        changed = deepcopy(self.stage)
        accepted = next(row for row in changed["trajectory"] if row["kind"] == "reviewer" and row["accepted"])
        another = next(alias for alias in changed["candidate_order"] if alias != changed["selected"])
        accepted["binding"] = changed["candidates"][another]["binding"]
        with self.assertRaisesRegex(ValueError, "acceptance"):
            self.audit(changed)
        changed = deepcopy(self.stage)
        changed["trajectory"].append(deepcopy(next(row for row in changed["trajectory"] if row["kind"] == "reviewer")))
        with self.assertRaisesRegex(ValueError, "followed explicit"):
            self.audit(changed)


class BenchmarkCertificateAuditTests(unittest.TestCase):
    def test_invalid_review_and_scout_envelopes_match_controller_bounds(self):
        probe = 0
        for _ in range(34):
            probe = [probe]
        content = json.dumps(dict(candidate="candidate-A", action="accept", notes="Check", remaining=[], probes=[probe]))
        self.assertIsNone(review_from_call(dict(changes={"review.json": content}), ["R"], ["candidate-A"]))
        small = json.dumps(dict(candidate="candidate-A", action="accept", notes="Check", remaining=[], probes=[]))
        self.assertIsNotNone(review_from_call(dict(changes={"review.json": small}), ["R"], ["candidate-A"]))
        overflow = small.replace('"probes": []', '"probes": [1e999]')
        self.assertIsNone(review_from_call(dict(changes={"review.json": overflow}), ["R"], ["candidate-A"]))
        oversized = json.dumps({"probes": [dict(input=1, expected=1, requirement="R")]}) + " " * 65536
        self.assertEqual(scout_proposals(dict(changes={"probes.json": oversized})), [])

    def test_changed_consumed_bytes_cannot_be_certified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "results.json"
            path.write_text('{}')
            def altered(run, *, accounting_ledger=None):
                audit_benchmark.load(path)
                path.write_text('{"changed":true}')
                return {"passed": True}
            with patch.object(audit_benchmark, "_audit_run", altered):
                with self.assertRaisesRegex(ValueError, "Consumed evidence changed"):
                    audit_benchmark.audit_run(root)


if __name__ == "__main__":
    unittest.main()
