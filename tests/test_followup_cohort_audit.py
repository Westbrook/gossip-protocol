"""Adversarial retained-evidence checks; no candidate execution or provider calls."""
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import tempfile
from typing import Any
import unittest

from analysis import audit_continuation_followup as audit
from gossip_harness.gitstore import GitStore
from gossip_harness.sustained_experiment import local_promote


def sample_receipt(files, cases, *, image="sha256:" + "f" * 64, passed=True):
    outcomes = [dict(index=index, passed=passed, status="passed" if passed else "wrong_answer",
                     actual=case["expected"] if passed else {"fault": True})
                for index, case in enumerate(cases)]
    return dict(source_sha256=audit.digest(files), suite_sha256=audit.digest(cases),
        image_id=image, timeout_seconds=300, case_timeout_seconds=12, cleanup_verified=True,
        passed=passed, status="passed" if passed else "failed", outcomes=outcomes)


class FollowupCohortAuditTests(unittest.TestCase):
    freeze: dict[str, Any]
    state: dict[str, Any]
    case: dict[str, Any]
    def setUp(self):
        self.roster = [[project, arm, rep] for project in ("warehouse", "job-queue")
                       for rep in range(2) for arm in audit.ARMS]
        self.ids = [f"{project}-{arm}-{rep}" for project, arm, rep in self.roster]
        self.sessions = [f"{project}-{rep}-shared-{kind}" for project in ("warehouse", "job-queue")
                         for rep in range(2) for kind in (*audit.FORMATIONS, "scouts")]
        self.freeze = dict(protocol=audit.PROTOCOL, private_evaluation_started=False,
            frozen_monotonic_ns=100, trajectories=[dict(run_id=run_id) for run_id in self.ids])
        self.spans = {
            1: dict(kind="worker_request", run_id=self.sessions[0], stage_index=0,
                    started_monotonic_ns=1, finished_monotonic_ns=10, status="finished"),
            2: dict(kind="docker_validation", run_id=self.ids[0], stage_index=0,
                    purpose="stage_evidence", validator_origin="physical_docker",
                    started_monotonic_ns=10, finished_monotonic_ns=20, status="finished"),
            3: dict(kind="docker_validation", run_id=self.ids[0], purpose="final_private",
                    validator_origin="physical_docker", started_monotonic_ns=100,
                    finished_monotonic_ns=110, status="finished"),
        }
        self.contract = dict(image="sha256:" + "f" * 64, suite_timeout_seconds=300, case_timeout_seconds=12)
        self.files = {"solution.py": "source bytes are data only"}
        self.project = dict(stages=[dict(visible_cases=[dict(id=f"v{n}", input={"n": n},
            expected={"n": n}, requirement=f"R{n}")], hidden_cases=[dict(id=f"h{n}", input={"n": n+2},
            expected={"n": n+2}, requirement=f"R{n}")]) for n in range(2)])
        self.state = dict(terminal="visible_complete", files=self.files,
            stages=[dict(completed=True, files=self.files, probe_pool=[]) for _ in range(2)])
        self.case = dict(accepted=True, milestones_completed=2, requirement_coverage={"R0": True, "R1": True},
            final_visible=sample_receipt(self.files, audit.cases_for(self.project, 1, "visible")),
            final_hidden=sample_receipt(self.files, audit.cases_for(self.project, 1, "hidden")))

    def test_exact_twelve_roster_is_required(self):
        audit.check_roster(self.roster, ["warehouse", "job-queue"])
        for rows in (self.roster[:-1], [self.roster[0]] * 12,
                     [["warehouse", "current-independent", False], *self.roster[1:]]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                audit.check_roster(rows, ["warehouse", "job-queue"])

    def test_successful_barrier_includes_shared_setup_and_arm_execution(self):
        audit.check_cohort_barrier(self.freeze, self.ids, self.sessions, self.spans)

    def test_omitted_bounded_arm_cannot_shrink_denominator(self):
        self.freeze["trajectories"].pop()
        with self.assertRaisesRegex(ValueError, "twelve"):
            audit.check_cohort_barrier(self.freeze, self.ids, self.sessions, self.spans)

    def test_private_outcome_cannot_influence_any_earlier_arm(self):
        self.spans[3]["started_monotonic_ns"] = 99
        with self.assertRaisesRegex(ValueError, "barrier"):
            audit.check_cohort_barrier(self.freeze, self.ids, self.sessions, self.spans)

    def test_final_public_evaluation_also_waits_for_complete_cohort(self):
        self.spans[3].update(purpose="final_visible", started_monotonic_ns=99)
        with self.assertRaisesRegex(ValueError, "barrier"):
            audit.check_cohort_barrier(self.freeze, self.ids, self.sessions, self.spans)

    def test_late_public_receipt_cannot_retroactively_complete_source(self):
        self.spans[2]["finished_monotonic_ns"] = 101
        with self.assertRaisesRegex(ValueError, "freeze"):
            audit.check_cohort_barrier(self.freeze, self.ids, self.sessions, self.spans)

    def test_provider_request_after_freeze_is_rejected(self):
        self.spans[1]["finished_monotonic_ns"] = 101
        with self.assertRaisesRegex(ValueError, "freeze"):
            audit.check_cohort_barrier(self.freeze, self.ids, self.sessions, self.spans)

    def test_fake_validator_origin_cannot_qualify_rehearsal(self):
        self.spans[2]["validator_origin"] = "unit_test_control"
        with self.assertRaisesRegex(ValueError, "synthetic"):
            audit.check_cohort_barrier(self.freeze, self.ids, self.sessions, self.spans)

    def test_unknown_failed_or_unfinished_span_is_not_complete(self):
        for mutation in ({"run_id": "unexpected"}, {"kind": "diagnostic"}, {"status": "failed"}):
            spans = deepcopy(self.spans)
            spans[1].update(mutation)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                audit.check_cohort_barrier(self.freeze, self.ids, self.sessions, spans)

    def failure_case(self, *, live=False):
        call = dict(call_id="builder-slot-0-2", request_span_id=1, physical_span_id=4,
            failure="Known settled failure", outcome="failure", usage_units=7 if live else 0,
            physical_dispatch=live, journal_replay=False, metadata={"api_calls": 1 if live else 0})
        spans = deepcopy(self.spans)
        spans[1].update(call_id=call["call_id"], status="failed", error_type="WorkerFailure")
        spans[4] = {**spans[1], "kind": "physical_provider_request" if live else "scripted_worker"}
        return call, spans

    def test_known_settled_noop_failure_preserves_complete_rehearsal(self):
        call, spans = self.failure_case()
        allowed = audit.known_failure_spans([call], spans)
        self.assertEqual(allowed, {1, 4})
        audit.check_cohort_barrier(self.freeze, self.ids, self.sessions, spans, allowed)

    def test_known_live_failure_retains_real_charge_and_physical_dispatch(self):
        call, spans = self.failure_case(live=True)
        allowed = audit.known_failure_spans([call], spans, "live")
        self.assertEqual(allowed, {1, 4})
        audit.check_cohort_barrier(self.freeze, self.ids, self.sessions, spans, allowed)
        with self.assertRaises(ValueError):
            audit.known_failure_spans([call], spans, "rehearsal")

    def test_unknown_or_unsettled_failure_cannot_be_labeled_known(self):
        for mutation in ({"usage_units": None}, {"usage_units": -1}, {"usage_units": True},
                         {"journal_replay": True}, {"outcome": "stopped"}, {"metadata": {"api_calls": 0, "halt": True}},
                         {"metadata": {"api_calls": 0, "halt": 1}}, {"metadata": {"api_calls": 0, "halt": "yes"}}):
            call, spans = self.failure_case()
            call.update(mutation)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                audit.known_failure_spans([call], spans)

    def test_failure_exception_and_call_outcome_must_match(self):
        call, spans = self.failure_case()
        spans[4]["error_type"] = "TimeoutError"
        with self.assertRaisesRegex(ValueError, "Unknown"):
            audit.known_failure_spans([call], spans)
        spans[4]["error_type"] = "WorkerFailure"
        del call["failure"]
        with self.assertRaisesRegex(ValueError, "status"):
            audit.known_failure_spans([call], spans)

    def test_independent_acceptance_requires_both_milestones_and_all_final_checks(self):
        accepted, suites = audit.check_primary_outcome(self.case, self.state, self.project, self.contract)
        self.assertTrue(accepted)
        self.assertEqual(len(suites["private"]), 2)

    def test_passing_final_checks_do_not_turn_bounded_stop_into_acceptance(self):
        self.state.update(terminal="bounded_incomplete")
        self.state["stages"] = [dict(completed=False, files=self.files, probe_pool=[])]
        self.case.update(milestones_completed=0)
        with self.assertRaisesRegex(ValueError, "Acceptance"):
            audit.check_primary_outcome(self.case, self.state, self.project, self.contract)
        self.case["accepted"] = False
        self.assertFalse(audit.check_primary_outcome(self.case, self.state, self.project, self.contract)[0])

    def test_later_milestone_cannot_follow_failed_earlier_milestone(self):
        self.state["stages"][0]["completed"] = False
        with self.assertRaisesRegex(ValueError, "milestone"):
            audit.check_primary_outcome(self.case, self.state, self.project, self.contract)

    def test_boolean_milestone_and_acceptance_fields_are_strict(self):
        self.state["stages"][0]["completed"] = 1
        with self.assertRaises(ValueError):
            audit.check_primary_outcome(self.case, self.state, self.project, self.contract)
        self.state["stages"][0]["completed"] = True
        self.case["accepted"] = 1
        with self.assertRaises(ValueError):
            audit.check_primary_outcome(self.case, self.state, self.project, self.contract)

    def test_final_wrong_answer_cannot_be_hidden_by_summary_flag(self):
        self.case["final_hidden"] = sample_receipt(self.files, audit.cases_for(self.project, 1, "hidden"), passed=False)
        with self.assertRaisesRegex(ValueError, "Acceptance"):
            audit.check_primary_outcome(self.case, self.state, self.project, self.contract)

    def test_final_receipt_for_other_source_does_not_certify_terminal_source(self):
        self.case["final_hidden"]["source_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "source/suite"):
            audit.check_primary_outcome(self.case, self.state, self.project, self.contract)

    def test_inherited_private_case_cannot_be_omitted(self):
        self.case["final_hidden"] = sample_receipt(self.files, audit.cases_for(self.project, 1, "hidden")[1:])
        with self.assertRaisesRegex(ValueError, "source/suite"):
            audit.check_primary_outcome(self.case, self.state, self.project, self.contract)

    def test_passing_status_requires_exact_typed_actual_output(self):
        self.case["final_hidden"]["outcomes"][0]["actual"] = {"n": True}
        with self.assertRaisesRegex(ValueError, "typed value"):
            audit.check_primary_outcome(self.case, self.state, self.project, self.contract)

    def test_requirement_coverage_is_derived_from_every_private_case(self):
        self.case["requirement_coverage"] = {"R0": True}
        with self.assertRaisesRegex(ValueError, "coverage"):
            audit.check_primary_outcome(self.case, self.state, self.project, self.contract)

    def test_frozen_snapshot_rejects_unbound_wal_and_rollback_journal(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "snapshot.sqlite"
            path.write_bytes(b"unused")
            audit.check_frozen_snapshot(path)
            for suffix in ("-wal", "-journal"):
                sidecar = Path(str(path) + suffix)
                sidecar.write_bytes(b"unbound state")
                with self.assertRaisesRegex(ValueError, "unbound"):
                    audit.check_frozen_snapshot(path)
                sidecar.write_bytes(b"")
                audit.check_frozen_snapshot(path)

    def test_fresh_execution_rejects_duplicate_container_or_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spans = {}
            for number in (1, 2):
                path = root / f"{number}.json"
                path.write_text(json.dumps(dict(image_id=self.contract["image"], container_name=f"gossip-blackbox-{number}")))
                spans[number] = dict(kind="docker_validation", receipt_path=str(path), receipt_sha256=audit.sha(path))
            paths, containers = audit.check_fresh_receipts(spans, root, self.contract)
            self.assertEqual(len(paths), len(containers), 2)
            spans[2] = deepcopy(spans[1])
            with self.assertRaisesRegex(ValueError, "reused"):
                audit.check_fresh_receipts(spans, root, self.contract)

    def test_artifact_binding_rejects_changed_bytes_and_escaping_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "run"
            directory.mkdir()
            path = directory / "receipt.json"
            path.write_text("{}")
            binding = dict(path=str(path), sha256=audit.sha(path))
            self.assertEqual(audit.bound_artifact(binding, directory), path.resolve())
            path.write_text("{\"passed\":true}")
            with self.assertRaises(ValueError):
                audit.bound_artifact(binding, directory)
            outer = root / "other.json"
            outer.write_text("{}")
            with self.assertRaisesRegex(ValueError, "escapes"):
                audit.bound_artifact(dict(path=str(outer), sha256=audit.sha(outer)), directory)

    def test_real_git_promotion_audits_serialized_lease_and_rejects_owner_tampering(self):
        """Exercise the actual Git/SQLite boundary, including Lease.asdict keys."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            initial = {"solution.py": "# trusted test source; never executed\n", "fixed.txt": "adapter"}
            files = {**initial, "solution.py": "# proposed test source; never executed\n"}
            base = GitStore.create(root / "initial.git", initial)
            candidate = GitStore.fork(base, root / "candidate.git")
            tip = candidate.propose({"solution.py": files["solution.py"]})
            binding = dict(store_path=str(candidate.path), tip_sha=tip, files_sha256=audit.digest(files))
            promoted = local_promote(base, binding, root / "checkpoint.git", files, ["solution.py"])
            self.assertEqual(audit.audit_local_promotion(promoted.path, base.head(), files, ["solution.py"]), promoted.head())
            ledger = promoted.path.with_suffix(".sqlite")
            with sqlite3.connect(ledger) as db:
                intent_id, serialized = db.execute("SELECT id,leases FROM intents").fetchone()
                task_worker = db.execute("SELECT worker FROM tasks WHERE id='exact-tree'").fetchone()[0]
            original = json.loads(serialized)
            self.assertEqual(set(original[0]), {"task_id", "worker_id", "epoch", "expires_at"})
            self.assertEqual(original[0]["worker_id"], task_worker)
            variants = [
                [{**original[0], "worker_id": "foreign-owner"}],
                [{**original[0], "epoch": original[0]["epoch"] + 1}],
                [{**original[0], "epoch": True}],
                [{**original[0], "expires_at": original[0]["expires_at"] + 1}],
                [{**original[0], "expires_at": True}],
                [{"task_id": original[0]["task_id"], "worker": task_worker,
                  "epoch": original[0]["epoch"], "expires_at": original[0]["expires_at"]}],
            ]
            for leases in variants:
                with self.subTest(leases=leases):
                    with sqlite3.connect(ledger) as db:
                        db.execute("UPDATE intents SET leases=? WHERE id=?", (json.dumps(leases), intent_id))
                    with self.assertRaises(ValueError):
                        audit.audit_local_promotion(promoted.path, base.head(), files, ["solution.py"])
