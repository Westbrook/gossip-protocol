"""Audit tamper controls; inert strings/fabricated receipts, no candidate execution."""
from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from analysis import audit_continuation
from analysis.audit_continuation import audit_calls, audit_run, audit_stage, audit_timings, digest, sha
from gossip_harness.continuation_stage import run_continuation_stage
from gossip_harness.verification_journal import RequestJournal
from gossip_harness.verification_probes import parse_proposals, revalidate_cases
from gossip_harness.worker import WorkerRequest, WorkerResult


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True))


CONTRACT = dict(image="sha256:" + "f" * 64, case_timeout_seconds=12, suite_timeout_seconds=300)


def receipt(files, cases):
    return dict(source_sha256=digest(files), suite_sha256=digest([{k: c[k] for k in ("input", "expected", "id", "requirement") if k in c} for c in cases]),
        image_id=CONTRACT["image"], case_timeout_seconds=12, timeout_seconds=300, cleanup_verified=True,
        passed=True, status="passed", outcomes=[dict(index=i, passed=True, status="passed", actual=c["expected"]) for i, c in enumerate(cases)])


def span(index, kind, start, end, **labels):
    return dict(span_id=index, kind=kind, clock_id="clock", started_monotonic_ns=start,
        finished_monotonic_ns=end, elapsed_seconds=(end - start) / 1e9,
        started_utc="2026-10-01T00:00:00+00:00", finished_utc="2026-10-01T00:00:01+00:00", status="finished", **labels)


class ContinuationTimingAuditTests(unittest.TestCase):
    def setUp(self):
        self.document = dict(clock_id="clock", spans=[span(0, "model_trajectory", 0, 10, run_id="run"),
            span(1, "docker_validation", 20, 30, run_id="run", purpose="final_private")])
        self.freeze = dict(frozen_monotonic_ns=15)

    def test_global_freeze_requires_every_trajectory_terminal_before_private(self):
        self.assertEqual(len(audit_timings(self.document, self.freeze, ["run"])), 2)
        for index, key, value in [(0, "finished_monotonic_ns", 16), (1, "started_monotonic_ns", 14)]:
            changed = deepcopy(self.document)
            row = changed["spans"][index]
            row[key] = value
            row["elapsed_seconds"] = (row["finished_monotonic_ns"] - row["started_monotonic_ns"]) / 1e9
            with self.subTest(index=index), self.assertRaises(ValueError):
                audit_timings(changed, self.freeze, ["run"])

    def test_false_duration_duplicate_clock_or_unclosed_span_rejected(self):
        for key, value in [("elapsed_seconds", 99), ("started_monotonic_ns", False), ("status", "running"), ("clock_id", "other")]:
            changed = deepcopy(self.document)
            changed["spans"][0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                audit_timings(changed, self.freeze, ["run"])
        self.document["spans"].append(deepcopy(self.document["spans"][0]))
        with self.assertRaises(ValueError):
            audit_timings(self.document, self.freeze, ["run"])

    def test_known_failed_physical_call_is_closed_not_free_or_unexecuted(self):
        row = span(2, "physical_provider_request", 2, 5, run_id="run")
        row.update(status="failed", error_type="WorkerFailure")
        self.document["spans"].append(row)
        self.assertEqual(audit_timings(self.document, self.freeze, ["run"])[2]["status"], "failed")


class ContinuationStageAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.initial = {"app.py": "inert initial"}
        self.project = dict(id="toy", title="Toy", allowed_paths=["app.py"], initial_files=self.initial,
            stages=[dict(requirements=["R"], specification="Identity", visible_cases=[dict(id="public", requirement="R", input=1, expected=1)], hidden_cases=[])])
        self.calls, self.trees, self.spans, self.clock = {}, {}, {}, 0

    def make(self, noop=False, unresolved=False, scouts=False):
        def invoke(*, role, model, files, instructions, allowed_paths, feedback, metadata):
            call_id = f"{role}-{metadata['candidate_id']}-{metadata['round']}"
            self.clock += 10
            span_id = len(self.spans)
            self.spans[span_id] = span(span_id, "worker_request", self.clock, self.clock + 5,
                                       run_id="run", stage_index=0, call_id=call_id, role=role, model=model)
            request = dict(files=deepcopy(files), instructions=instructions, allowed_paths=list(allowed_paths), feedback=feedback)
            if role == "builder":
                changes = {} if noop and metadata["round"] > 1 else {"app.py": "inert candidate", "notes.json": json.dumps(dict(notes="Remember contract", remaining=[]))}
            else:
                action = "inspect" if unresolved else "repair" if noop and metadata["round"] == 1 else "accept"
                changes = {"review.json": json.dumps(dict(candidate=metadata["alias_by_slot"]["slot-0"], action=action,
                    notes="Assess contract", remaining=["R"] if unresolved else [], probes=[]))}
            self.calls[call_id] = (request, dict(call_id=call_id, role=role, model=model, changes=changes, request_span_id=span_id))
            return WorkerResult(changes, "inert", 0, {"api_calls": 0})
        def retain(files, label):
            binding = dict(store_path=str(self.root / (label + ".git")), tip_sha=digest([label, files]), files_sha256=digest(files))
            self.trees[binding["tip_sha"]] = files
            return binding
        def gate(envelope, stage_index, origin):
            if envelope["mode"] == "revalidate":
                return revalidate_cases(envelope["probes"], stage_index=stage_index, reference=lambda i, payload: payload,
                    validate_input=lambda i, payload: None, origin=origin)
            return parse_proposals(json.dumps({"probes": envelope["probes"]}), stage_index=stage_index,
                requirements=["R"], reference=lambda i, payload: payload, validate_input=lambda i, payload: None,
                origin=origin, existing_cases=envelope["existing_cases"], max_new=envelope["max_new"], namespace="toy")
        def evaluate(files, cases, label):
            self.clock += 10
            span_id = len(self.spans)
            row = receipt(files, cases)
            path = self.root / "evaluations" / f"{span_id}.json"
            write(path, row)
            self.spans[span_id] = span(span_id, "docker_validation", self.clock, self.clock + 5,
                run_id="run", stage_index=0, label=label, purpose="stage_evidence", case_count=len(cases),
                receipt_path=str(path), receipt_sha256=sha(path))
            return row
        prior, scout_evidence = {}, dict(calls=0, admitted=0, rejected=0, receipts=[])
        if scouts:
            context = dict(project_id="toy", stage_index=0, milestones=[dict(requirements=["R"], specification="Identity")],
                           public_cases=self.project["stages"][0]["visible_cases"])
            files = {**self.initial, "scout-context.json": json.dumps(context)}
            pool, admissions = [], []
            for scout_index in range(2):
                proposed = dict(input=scout_index + 2, expected=scout_index + 2, requirement="R")
                content = json.dumps({"probes": [proposed]})
                call_id = f"scout-scout-{scout_index}-1"
                request = dict(files=files, allowed_paths=["probes.json"], feedback="")
                self.calls[call_id] = (request, dict(call_id=call_id, role="scout", model="cheap", changes={"probes.json": content}))
                admission = parse_proposals(content, stage_index=0, requirements=["R"], reference=lambda i, payload: payload,
                    validate_input=lambda i, payload: None, origin=dict(stage_index=0, scout_index=scout_index),
                    existing_cases=self.project["stages"][0]["visible_cases"] + pool, max_new=8 - len(pool), namespace="toy")
                pool.extend(admission["admitted_cases"])
                admissions.append(admission)
                write(self.root / f"scout-{scout_index}-admission.json", admission)
            prior = dict(probe_pool=pool)
            scout_evidence = dict(calls=2, admitted=2, rejected=0, receipts=admissions,
                                  source_sha256=digest(self.initial), shared_input_sha256=digest(files))
            write(self.root / "scouts.json", scout_evidence)
        result = run_continuation_stage(self.project, 0, "strong-reviewed", self.initial, prior, invoke=invoke,
            evaluate=evaluate, retain=retain, validate_probes=gate,
            emit=lambda *args, **kwargs: None, baseline_approval=dict(files_sha256=digest(self.initial), source_valid=True, approval_id="trusted-baseline"))
        result.update(stage_index=0, scouts=scout_evidence)
        self.stage = result
        self.policy = "scout-assisted" if scouts else "strong-maintainer"

    def audit(self, changed=None):
        return audit_stage(changed or self.stage, self.project, 0, self.policy, self.initial, {}, self.calls,
                           CONTRACT, self.root, lambda binding: self.trees[binding["tip_sha"]], self.spans, "run")

    def test_retained_noop_eligibility_is_reconstructed_and_false_demotion_rejected(self):
        self.make(noop=True)
        self.assertTrue(self.audit()["completed"])
        self.assertEqual(self.audit()["builders"], 2)
        changed = deepcopy(self.stage)
        next(row for row in changed["trajectory"] if row["kind"] == "builder" and row["round"] == 2)["source_valid"] = False
        with self.assertRaisesRegex(ValueError, "eligibility"):
            self.audit(changed)

    def test_private_context_wrong_model_and_numeric_pass_rejected(self):
        self.make()
        call_id = "reviewer-review-1"
        original = deepcopy(self.calls[call_id])
        context = json.loads(self.calls[call_id][0]["files"]["verification-context.json"])
        context["private_cases"] = [dict(input=99, expected=99)]
        self.calls[call_id][0]["files"]["verification-context.json"] = json.dumps(context)
        with self.assertRaisesRegex(ValueError, "nonpublic"):
            self.audit()
        self.calls[call_id] = deepcopy(original)
        context = json.loads(self.calls[call_id][0]["files"]["verification-context.json"])
        alias = self.stage["selected"]
        context["matrix"][alias]["public"]["actual"] = "forged"
        self.calls[call_id][0]["files"]["verification-context.json"] = json.dumps(context)
        with self.assertRaisesRegex(ValueError, "Reviewer matrix"):
            self.audit()
        self.calls[call_id] = deepcopy(original)
        self.calls[call_id][1]["model"] = "cheap"
        with self.assertRaisesRegex(ValueError, "same strong"):
            self.audit()
        self.calls[call_id] = original
        changed = deepcopy(self.stage)
        chosen = changed["candidates"][changed["selected"]]
        chosen["validation_receipts"][0]["receipt"]["outcomes"][0]["passed"] = 1
        with self.assertRaisesRegex(ValueError, "malformed"):
            self.audit(changed)

    def test_stagnation_with_passing_tests_does_not_authorize_acceptance(self):
        self.make(unresolved=True)
        self.assertFalse(self.audit()["completed"])
        changed = deepcopy(self.stage)
        changed["completed"] = True
        changed["remaining"] = []
        with self.assertRaisesRegex(ValueError, "completion"):
            self.audit(changed)

    def test_independent_scouts_share_only_public_source_and_admitted_labels(self):
        self.make(scouts=True)
        self.assertEqual(self.audit()["scouts"], 2)
        request = self.calls["scout-scout-0-1"][0]
        context = json.loads(request["files"]["scout-context.json"])
        context["peer_proposals"] = ["unpermitted"]
        request["files"]["scout-context.json"] = json.dumps(context)
        with self.assertRaisesRegex(ValueError, "Scout inputs"):
            self.audit()


class ContinuationJournalAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.contract = dict(models={"cheap": {"reservation_units": 100}}, output_tokens=10)
        self.call_id, self.billing = "scout-scout-0-1", "namespace/run/stage-0"
        reservation = self.billing + "/" + self.call_id
        request = WorkerRequest("task-" + digest([digest(self.contract), reservation])[:24], "Public instructions", ("probes.json",), {"app.py": "inert"}, "head", 1, "")
        journal = RequestJournal(self.root / "journal")
        result = journal.execute(self.call_id, request, reservation,
            lambda: WorkerResult({"probes.json": '{"probes":[]}'}, "inert", 0, {"api_calls": 0}), lambda: None, lambda units: None)
        self.call = dict(call_id=self.call_id, reservation=reservation, role="scout", model="cheap", request_sha256=digest(asdict(request)),
            physical_dispatch=False, journal_replay=False, journal_paths={k: str(p) for k, p in journal.paths(self.call_id).items()},
            journal_result_sha256=sha(journal.paths(self.call_id)["result"]), request_span_id=1, physical_span_id=2, outcome="result", **asdict(result))
        write(self.root / "requests" / f"{self.call_id}.request.json", asdict(request))
        write(self.root / "requests" / f"{self.call_id}.result.json", self.call)
        labels = dict(run_id="run", stage_index=0, call_id=self.call_id, role="scout", model="cheap")
        self.spans = {1: span(1, "worker_request", 0, 10, **labels), 2: span(2, "scripted_worker", 2, 8, **labels)}

    def audit(self):
        return audit_calls(self.root, [self.call], self.billing, self.contract, "rehearsal", self.spans, "run", 0)

    def test_scout_response_and_its_unique_settlement_are_accounted(self):
        loaded, bindings = self.audit()
        self.assertEqual(list(loaded), [self.call_id])
        self.assertEqual(bindings[0]["reservation"], self.billing + "/" + self.call_id)
        path = Path(self.call["journal_paths"]["settled"])
        value = json.loads(path.read_text())
        value["usage_units"] = 1
        write(path, value)
        with self.assertRaisesRegex(ValueError, "settlement"):
            self.audit()

    def test_unknown_dispatch_and_duplicate_physical_timing_rejected(self):
        self.audit()
        self.spans[3] = {**self.spans[2], "span_id": 3}
        with self.assertRaisesRegex(ValueError, "duplicate physical"):
            self.audit()
        del self.spans[3]
        write(self.root / "journal" / "orphan.json", {})
        with self.assertRaisesRegex(ValueError, "Orphan"):
            self.audit()


class ContinuationFinalizationAuditTests(unittest.TestCase):
    def test_censored_or_partial_study_is_not_certified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for status, censored in [("running", []), ("finished", [{"run_id": "unfinished"}])]:
                write(root / "results.json", dict(experiment="continuation-transport-v1", phase="finished", status=status,
                    mode="live", unexecuted=[], censored=censored, active_case=None))
                with self.assertRaisesRegex(ValueError, "fully finalized"):
                    audit_run(root)

    def test_certificate_rejects_changed_bytes_after_decoding_before_return(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for filename in ("results.json", "timings.json", "frozen-trajectories.json", "fixtures.json", "study-plan.json", "preregistered.json"):
                path = root / filename
                write(path, {"version": 1})
                def audit_body(run, *, accounting_ledger=None):
                    self.assertEqual(audit_continuation.load(path), {"version": 1})
                    inspected_hash = audit_continuation.sha(path)
                    write(path, {"version": 2})
                    return {"passed": True, "results_sha256": inspected_hash}
                with self.subTest(filename=filename), patch.object(audit_continuation, "_audit_run", audit_body):
                    with self.assertRaisesRegex(ValueError, "Consumed evidence changed"):
                        audit_run(root)

    def test_certificate_hashes_decoded_bytes_and_rejects_changed_reread(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "timings.json"
            write(path, {"version": 1})
            expected = sha(path)
            def audit_body(run, *, accounting_ledger=None):
                self.assertEqual(audit_continuation.load(path), {"version": 1})
                return {"passed": True, "timings_sha256": audit_continuation.sha(path)}
            with patch.object(audit_continuation, "_audit_run", audit_body):
                result = audit_run(root)
            self.assertEqual(result["timings_sha256"], expected)
            self.assertEqual(result["consumed_evidence_files"], 1)
            def changed_body(run, *, accounting_ledger=None):
                audit_continuation.load(path)
                write(path, {"version": 2})
                return {"timings_sha256": audit_continuation.sha(path)}
            with patch.object(audit_continuation, "_audit_run", changed_body):
                with self.assertRaisesRegex(ValueError, "Consumed evidence changed"):
                    audit_run(root)


if __name__ == "__main__":
    unittest.main()
