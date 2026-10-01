"""Tamper tests use fabricated receipts and inert source strings, never code execution."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from gossip_harness.verification_audit import (
    audit_fault, audit_receipt, audit_stage, audit_integration, canonical, digest, _prompts, audit_run,
)
from gossip_harness.gitstore import GitStore
from gossip_harness.verification_integration import integrate_policy, _exact_text_validator
from gossip_harness.verification_probes import parse_proposals
from gossip_harness.verification_stage import run_verification_stage
from gossip_harness.worker import WorkerResult


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True))


CONTRACT = {"image": "sha256:" + "f" * 64, "case_timeout_seconds": 12,
            "suite_timeout_seconds": 300}


def receipt(files, cases):
    execution = [{key: case[key] for key in ("input", "expected", "id", "requirement") if key in case}
                 for case in cases]
    return dict(source_sha256=digest(files), suite_sha256=digest(execution), image_id=CONTRACT["image"],
                case_timeout_seconds=12, timeout_seconds=300, cleanup_verified=True,
                status="passed", passed=True,
                outcomes=[dict(index=i, passed=True, status="passed", actual=case["expected"])
                          for i, case in enumerate(cases)])


class ReceiptAuditTests(unittest.TestCase):
    def test_projection_keeps_provenance_out_of_execution_hash(self):
        files = {"app.py": "inert"}
        cases = [dict(id="x", input=1, expected={"n": 2}, requirement="R", origin={"private": "provenance"})]
        row = receipt(files, cases)
        self.assertTrue(audit_receipt(row, files, cases, CONTRACT)["x"]["passed"])
        for key, value in (("source_sha256", "0" * 64), ("suite_sha256", digest(cases)),
                           ("cleanup_verified", False), ("image_id", "different"), ("passed", 1)):
            changed = deepcopy(row)
            changed[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                audit_receipt(changed, files, cases, CONTRACT)
        for key in ("timed_out", "output_truncated", "input_delivery_failed"):
            changed = {**row, key: True}
            with self.subTest(key=key), self.assertRaises(ValueError):
                audit_receipt(changed, files, cases, CONTRACT)
        changed = deepcopy(row)
        changed["outcomes"][0]["status"] = "timeout"
        with self.assertRaises(ValueError):
            audit_receipt(changed, files, cases, CONTRACT)

    def test_forged_pass_typed_value_and_missing_outcomes_rejected(self):
        files, cases = {"x": "inert"}, [dict(id="x", input=1, expected=2, requirement="R")]
        for actual in (True, 2.0, 3):
            changed = receipt(files, cases)
            changed["outcomes"][0]["actual"] = actual
            with self.subTest(actual=actual), self.assertRaises(ValueError):
                audit_receipt(changed, files, cases, CONTRACT)
        changed = receipt(files, cases)
        changed["outcomes"] = []
        with self.assertRaises(ValueError):
            audit_receipt(changed, files, cases, CONTRACT)

    def test_incomplete_or_censored_studies_cannot_be_certified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            for status in ("running", "interrupted"):
                write(root / "results.json", dict(experiment="verification-quality-v1", status=status))
                with self.assertRaisesRegex(ValueError, "fully finalized"):
                    audit_run(root)
            write(root / "results.json", dict(experiment="verification-quality-v1", status="finished",
                                              mode="live", unexecuted=[], censored=[{"run_id": "missing"}]))
            with self.assertRaisesRegex(ValueError, "fully finalized"):
                audit_run(root)


class StageAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.files = {"app.py": "inert initial text", "policy.json": "{}"}
        self.project = dict(id="toy", title="Toy", allowed_paths=["app.py"], initial_files=self.files,
            stages=[dict(requirements=["R"], specification="Return each input unchanged.",
                         visible_cases=[dict(id="public", requirement="R", input={"n": 0}, expected={"n": 0})], hidden_cases=[])])
        self.calls, self.trees = {}, {}
        self.prompts = _prompts((Path(__file__).parents[1] / "gossip_harness" / "verification_stage.py").read_text())

    def make(self, policy="portfolio-reviewed", review_script=None):
        def invoke(*, role, model, files, instructions, allowed_paths, feedback, metadata):
            call_id = f"{role}-{metadata['candidate_id']}-{metadata['round']}"
            req = dict(files=deepcopy(files), instructions=instructions, allowed_paths=list(allowed_paths),
                       feedback=feedback, attempt=metadata["round"])
            if role == "builder":
                changes = {"app.py": "inert candidate " + metadata["candidate_id"],
                           "notes.json": json.dumps(dict(notes="Remember the input contract", remaining=[]))}
            else:
                review = dict(candidate=metadata["alias_by_slot"]["slot-0"],
                    action="accept", notes="Checked the available evidence", remaining=[],
                    probes=[dict(requirement="R", input={"n": 1}, expected={"n": 1})])
                if review_script:
                    review.update(review_script(metadata))
                changes = {"review.json": json.dumps(review)}
            result = WorkerResult(changes, "Inert response", 0, {"api_calls": 0})
            self.calls[call_id] = (req, dict(changes=changes, role=role, model=model))
            return result

        def retain(files, label):
            binding = dict(store_path=str(self.root / (label + ".git")), tip_sha=digest([label, files]), files_sha256=digest(files))
            self.trees[binding["tip_sha"]] = deepcopy(files)
            write(self.root / (label + ".json"), dict(binding=binding, files=files))
            return binding

        def evaluate(files, cases, label):
            result = receipt(files, cases)
            write(self.root / "evaluations" / (digest(dict(source=files, cases=cases)) + ".json"), result)
            return result

        def gate(envelope, stage_index, origin):
            return parse_proposals(json.dumps({"probes": envelope["probes"]}), stage_index=stage_index,
                requirements=["R"], reference=lambda stage, payload: payload,
                validate_input=lambda stage, payload: None, origin=origin,
                existing_cases=envelope["existing_cases"], max_new=envelope["max_new"], namespace="toy")

        seed = int(digest(["toy", 0, 0])[:16], 16)
        self.stage = run_verification_stage(self.project, 0, policy, self.files, {}, invoke=invoke,
            evaluate=evaluate, retain=retain, validate_probes=gate, emit=lambda *args, **kwargs: None, candidate_seed=seed)
        self.policy = policy
        return self.stage

    def audit(self, stage=None):
        return audit_stage(stage or self.stage, self.project, 0, self.policy, self.files, {}, self.calls,
                           CONTRACT, self.root, lambda binding: self.trees[binding["tip_sha"]], self.prompts, 0)

    def test_reconstructs_portfolio_and_common_probe_execution(self):
        self.make()
        result = self.audit()
        self.assertEqual(result, dict(builders=4, reviewers=1, probes=1, completed=True))
        self.make("cheap-reviewed")
        self.calls = {key: value for key, value in self.calls.items() if not key.startswith(("builder-slot-1", "builder-slot-2", "builder-slot-3"))}
        self.assertEqual(self.audit()["builders"], 1)

    def test_false_selection_stale_binding_and_fabricated_matrix_rejected(self):
        self.make()
        other = self.stage["candidate_order"][1]
        mutations = [lambda stage: stage.update(selected=other),
                     lambda stage: stage["selected_binding"].update(tip_sha="wrong"),
                     lambda stage: stage["matrix"][other]["public"].update(passed=1),
                     lambda stage: stage["metrics"].update(admitted_new_probes=2)]
        for mutate in mutations:
            changed = deepcopy(self.stage)
            mutate(changed)
            with self.assertRaises((ValueError, KeyError)):
                self.audit(changed)

    def test_private_context_injection_or_wrong_model_rejected(self):
        self.make()
        call_id = "reviewer-review-1"
        old = deepcopy(self.calls[call_id])
        req, call = self.calls[call_id]
        context = json.loads(req["files"]["verification-context.json"])
        context["private_cases"] = [{"input": 99, "expected": 99}]
        req["files"]["verification-context.json"] = canonical(context)
        with self.assertRaisesRegex(ValueError, "context"):
            self.audit()
        self.calls[call_id] = old
        self.calls[call_id][1]["model"] = "cheap"
        with self.assertRaisesRegex(ValueError, "model"):
            self.audit()

    def test_probe_label_change_and_missing_peer_execution_rejected(self):
        self.make()
        gate = self.stage["probe_receipts"][0]
        changed = deepcopy(self.stage)
        changed["probe_receipts"][0]["receipt"]["admitted_cases"][0]["expected"] = {"n": 2}
        with self.assertRaisesRegex(ValueError, "probe"):
            self.audit(changed)
        other = self.stage["candidate_order"][1]
        source = self.trees[self.stage["candidates"][other]["binding"]["tip_sha"]]
        path = self.root / "evaluations" / (digest(dict(source=source, cases=gate["receipt"]["admitted_cases"])) + ".json")
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.audit()

    def test_incomplete_stage_keeps_reviewer_only_remaining_requirements(self):
        self.make(review_script=lambda metadata: dict(action="inspect", remaining=["R"]))
        self.assertFalse(self.stage["completed"])
        self.assertTrue(all(row["passed"] for row in self.stage["matrix"][self.stage["selected"]].values()))
        self.assertEqual(self.stage["remaining"], ["R"])
        self.assertEqual(self.stage["candidates"][self.stage["selected"]]["remaining"], [])
        self.assertFalse(self.audit()["completed"])
        changed = deepcopy(self.stage)
        changed["remaining"] = []
        with self.assertRaisesRegex(ValueError, "selected source"):
            self.audit(changed)

    def test_reviewer_remaining_survives_repair_and_fallback_to_different_candidate(self):
        self.project["stages"][0]["requirements"].append("R-other")
        def reviews(metadata):
            if metadata["round"] == 1:
                return dict(action="repair", remaining=["R"])
            return dict(candidate=metadata["alias_by_slot"]["slot-1"], action="inspect", remaining=["R-other"])
        self.make(review_script=reviews)
        chosen = self.stage["selected"]
        self.assertEqual(chosen, self.stage["alias_map"]["slot-0"])
        self.assertNotEqual(chosen, self.stage["trajectory"][-1]["review"]["candidate"])
        self.assertEqual(self.stage["remaining"], ["R"])
        self.assertEqual(self.stage["candidates"][chosen]["reviewer_remaining"], ["R"])
        self.assertEqual(self.audit()["builders"], 5)
        changed = deepcopy(self.stage)
        changed["candidates"][chosen]["reviewer_remaining"] = []
        with self.assertRaisesRegex(ValueError, "candidate"):
            self.audit(changed)

    def test_later_review_clears_prior_reviewer_remaining_for_that_candidate(self):
        def reviews(metadata):
            return dict(action="inspect", remaining=["R"] if metadata["round"] == 1 else [])
        self.make(review_script=reviews)
        self.assertFalse(self.stage["completed"])
        self.assertEqual(self.stage["remaining"], [])
        self.assertEqual(self.stage["candidates"][self.stage["selected"]]["reviewer_remaining"], [])
        self.assertFalse(self.audit()["completed"])


class FaultAuditTests(unittest.TestCase):
    def test_saved_unsettled_response_and_different_process_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            call_id = "reviewer-review-1"
            stem = hashlib.sha256(call_id.encode()).hexdigest()
            paths = {kind: root / "stage-1" / "journal" / f"{stem}.{kind}.json" for kind in ("request", "result", "settled")}
            for path in paths.values():
                write(path, {"inert": True})
            request, response = {"task_id": "test"}, {"reservation": "run/stage-1/reviewer-review-1", "usage_units": 0}
            marker = dict(pid=1234, stage_index=1, reservation=response["reservation"], call_id=call_id,
                          request_sha256=digest(request), journal_paths={key: str(value) for key, value in paths.items()}, other_unsettled=0)
            supervisor = dict(previous_pid=1234, returncode=-9, marker_sha256=digest(marker),
                saved_result_sha256=hashlib.sha256(paths["result"].read_bytes()).hexdigest(),
                reservation_at_kill=dict(id=response["reservation"], task_id="run/stage-1", amount=0, spent=None, state="reserved"))
            state = {"stages": [{"files": {"app.py": "inert"}}]}
            checkpoint = dict(contract_sha="contract", next_stage=1, stages=state["stages"], files=state["stages"][0]["files"],
                              store_path=str(root / "stage-0" / "checkpoint.git"), head="head0")
            resume = dict(previous_pid=1234, resumed_pid=5678, verified=True, stage_index=1,
                          initial_head="head0", checkpoint_sha256=digest(checkpoint))
            case = dict(fault=supervisor, resume=resume)
            traces = {1234: [dict(kind="provider_dispatch", stage_index=1, call_id=call_id)],
                      5678: [dict(kind="verification_review", stage_index=1, round=1)]}
            write(root / "fault-ready.json", marker)
            write(root / "fault-supervisor.json", supervisor)
            write(root / "resume.json", resume)
            def check():
                audit_fault(root, case, state, "contract", None, "head0", {call_id: (request, response)}, traces)
            check()
            for mutate in (lambda data: data.update(returncode=0),
                           lambda data: data["reservation_at_kill"].update(spent=0),
                           lambda data: data.update(saved_result_sha256="0" * 64)):
                changed = deepcopy(supervisor)
                mutate(changed)
                write(root / "fault-supervisor.json", changed)
                case["fault"] = changed
                with self.assertRaises(ValueError):
                    check()
            write(root / "fault-supervisor.json", supervisor)
            case["fault"] = supervisor
            resume["resumed_pid"] = 1234
            write(root / "resume.json", resume)
            with self.assertRaises(ValueError):
                check()


class IntegrationAuditTests(unittest.TestCase):
    def test_actual_git_merge_and_stale_candidate_bindings_with_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            initial_files = {"policy.json": '{"version":1}', "app.py": "inert initial"}
            initial = GitStore.create(root / "initial.git", initial_files)
            current = GitStore.fork(initial, root / "current.git")
            current_files = {**initial_files, "app.py": "raise RuntimeError('never execute source')"}
            tip = current.propose({"app.py": current_files["app.py"]})
            prepared = current.prepare(current, tip, current.head(), _exact_text_validator(current_files), ("app.py",))
            self.assertEqual(current.accept(prepared).status, "accepted")
            updates = {"policy.json": '{"version":2}'}
            merged, record = integrate_policy(initial, current, updates, root / "upstream.git")
            def git_files(binding):
                return GitStore(binding["store_path"]).read_files(binding["tip_sha"])
            bindings = dict(initial_binding=dict(store_path=str(initial.path), tip_sha=initial.head()),
                            current_binding=dict(store_path=str(current.path), tip_sha=current.head()))
            audit_integration(record, initial_files, current_files, updates, root, git_files, **bindings)
            for change in (dict(stale_probe_status="accepted"), dict(exact_tested_sha=current.head()),
                           dict(merge_parents=[current.head()]), dict(incoming_changed_paths=["app.py"]),
                           dict(old_evidence_candidate_sha=merged.head()), dict(incoming_tip=current.head())):
                with self.subTest(change=change), self.assertRaises(ValueError):
                    audit_integration({**record, **change}, initial_files, current_files, updates, root, git_files, **bindings)


if __name__ == "__main__":
    unittest.main()
