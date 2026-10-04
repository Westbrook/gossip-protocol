"""Offline role decisions and original action replay; no provider/process claim."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from gossip_harness.cumulative_study_role_v1 import (
    PROTOCOL, RoleDeadline, RoleDriver, canonical_bytes, checked_config, main, strict_loads,
)
from gossip_harness.peer_project_contract_v2 import Context, WorkKey, to_dict
from gossip_harness.peer_role_loop_v2 import WorkDirective, directive_id
from tests.test_peer_role_loop_v2 import FakeFinance, FakeMesh


class SimulatedExit(BaseException):
    pass


class CumulativeStudyRoleV1Tests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.actor = "child.B01"
        self.mesh = FakeMesh(self.actor)
        self.finance = FakeFinance(self.mesh, time.time)
        self.config = {"protocol": PROTOCOL, "actor": self.actor, "trajectory_id": "S4-G.healthy",
            "placement": "peer_local", "policy_sha256": "d" * 64, "call_limit": 4,
            "max_actions": 8, "requirements_by_milestone": {str(i): str(i) * 64 for i in range(1, 5)},
            "deadline_unix": 4_000_000_000.0,
            "crash_once": False, "finance": {"port": 12345, "capability": "c" * 32, "contract_sha256": "a" * 64},
            "mesh": {"root": str(self.root / "mesh"), "node_id": self.actor, "cohort_id": "cohort",
                "execution_contract_sha256": "a" * 64, "roster": [self.actor, "child.R1", "seed", "finance"],
                "transport_key": "b" * 32}}
        self.context = Context("a" * 64, "cohort", "S4-G.healthy", 1, "1" * 64)
        self.source = self.mesh.arrive("seed", "project-source", canonical_bytes({
            "files": {"src/a.py": "initial\n"}, "base_sha": "a" * 40}))
        self.directive = WorkDirective(self.context, WorkKey("catalog", "req", self.actor, 0),
            "mini", "build", self.source, (), ("src/a.py",), "Implement public requirement")
        self.driver = None

    def tearDown(self):
        if self.driver is not None:
            self.driver.close()
        self.temporary.cleanup()

    def open(self):
        self.driver = RoleDriver(self.root, self.config, self.mesh, self.finance)
        return self.driver

    def work(self, directive=None, *, stage="M1-build", producer="seed"):
        directive = directive or self.directive
        value = {"protocol": PROTOCOL, "stage_id": stage, "context": to_dict(directive.context),
            "source_ref": to_dict(directive.source_ref), "evidence_refs": [to_dict(ref) for ref in directive.evidence_refs],
            "directives": [{"actor": self.actor, "directive": directive.to_dict()}], "placement": self.config["placement"]}
        ref = self.mesh.arrive(producer, "cumulative-work", canonical_bytes(value))
        return ref, value

    def assignment(self, work_ref, work, *, digest=None, producer="seed"):
        decision = {"protocol": PROTOCOL, "stage_id": work["stage_id"], "actor": self.actor,
            "work_ref": to_dict(work_ref), "directive_sha256": digest or directive_id(self.directive)}
        decision_ref = self.mesh.arrive(producer, "cumulative-central-decision", canonical_bytes(decision))
        value = {"protocol": PROTOCOL, "stage_id": work["stage_id"], "actor": self.actor,
            "directive": self.directive.to_dict(), "work_ref": to_dict(work_ref), "central_decision_ref": to_dict(decision_ref)}
        ref = self.mesh.arrive("seed", "cumulative-assignment", canonical_bytes(value))
        return ref, value

    def until(self, state):
        for _ in range(40):
            self.driver.tick()
            snapshots = self.driver.loop.snapshots()
            if snapshots and snapshots[0].state == state:
                return snapshots[0]
        self.fail("Role did not reach " + state)

    def test_local_requires_actual_local_bytes_and_retains_one_decision(self):
        driver = self.open()
        ref, work = self.work()
        source_bytes = self.mesh.blobs.pop(self.source.event_id)
        self.assertIsNone(driver.choose_local(ref, work))
        self.assertEqual(driver.loop.snapshots(), ())
        self.mesh.blobs[self.source.event_id] = source_bytes
        key = driver.choose_local(ref, work)
        self.assertEqual(key, directive_id(self.directive))
        saved = (self.root / "decisions/M1-build.json").read_bytes()
        self.assertEqual(strict_loads(saved)["ready_directive_ids"], [key])
        self.assertEqual(driver.choose_local(ref, work), key)
        self.assertEqual(driver.db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0], 1)
        driver.close()
        self.driver = None
        self.open()
        self.assertEqual((self.root / "decisions/M1-build.json").read_bytes(), saved)
        self.assertEqual(len(self.driver.loop.snapshots()), 1)

    def test_central_work_alone_cannot_enqueue_and_exact_assignment_can(self):
        self.config["placement"] = "durable_central_scheduler"
        driver = self.open()
        work_ref, work = self.work()
        driver.tick()
        self.assertEqual(driver.loop.snapshots(), ())
        with self.assertRaisesRegex(ValueError, "cannot choose locally"):
            driver.choose_local(work_ref, work)
        ref, assignment = self.assignment(work_ref, work)
        self.assertEqual(driver.accept_assignment(ref, assignment), directive_id(self.directive))
        self.assertIsNotNone(strict_loads((self.root / "decisions/M1-build.json").read_bytes())["central_decision_ref"])

    def test_central_assignment_waits_for_seed_decision_bytes(self):
        self.config["placement"] = "durable_central_scheduler"
        driver = self.open()
        work_ref, work = self.work()
        ref, assignment = self.assignment(work_ref, work)
        identifier = assignment["central_decision_ref"]["event_id"]
        raw = self.mesh.blobs.pop(identifier)
        self.assertIsNone(driver.accept_assignment(ref, assignment))
        self.assertEqual(driver.loop.snapshots(), ())
        self.mesh.blobs[identifier] = raw
        self.assertEqual(driver.accept_assignment(ref, assignment), directive_id(self.directive))

    def test_foreign_origin_and_unbound_argument_cannot_select(self):
        driver = self.open()
        ref, work = self.work(producer="child.R1")
        with self.assertRaisesRegex(ValueError, "seed authority"):
            driver.choose_local(ref, work)
        ref, work = self.work()
        work["stage_id"] = "forged-stage"
        with self.assertRaisesRegex(ValueError, "arrived bytes"):
            driver.choose_local(ref, work)

    def test_context_and_stage_rebinding_rejected(self):
        driver = self.open()
        invalid = replace(self.directive, context=replace(self.context, requirements_sha256="f" * 64))
        ref, work = self.work(invalid)
        with self.assertRaisesRegex(ValueError, "frozen role context"):
            driver.choose_local(ref, work)
        ref, work = self.work()
        driver.choose_local(ref, work)
        ref, work = self.work(replace(self.directive, instructions="Changed instruction"))
        with self.assertRaisesRegex(ValueError, "Stage work rebound"):
            driver.choose_local(ref, work)

    def test_central_wrong_hash_and_nonseed_decision_rejected(self):
        self.config["placement"] = "durable_central_scheduler"
        driver = self.open()
        work_ref, work = self.work()
        ref, assignment = self.assignment(work_ref, work, digest="f" * 64)
        with self.assertRaisesRegex(ValueError, "decision binding"):
            driver.accept_assignment(ref, assignment)
        ref, assignment = self.assignment(work_ref, work, producer="child.R1")
        with self.assertRaisesRegex(ValueError, "seed authority"):
            driver.accept_assignment(ref, assignment)

    def test_result_retains_complete_request_reply_and_raw_payload_without_execution(self):
        self.open()
        self.work()
        pending = self.until("pending")
        raw = self.finance.finish(pending.action.request_id)
        published = self.until("published")
        path = self.root / "results" / (published.directive_id + ".json")
        value = strict_loads(path.read_bytes())
        self.assertEqual(value["result_payload"], strict_loads(raw))
        self.assertEqual(value["worker_request"]["files"], {"src/a.py": "initial\n"})
        self.assertEqual(value["snapshot"]["reply"], to_dict(published.reply))
        self.assertEqual(value["snapshot"]["publication_ref"], to_dict(published.publication_ref))
        prior = path.read_bytes()
        self.driver.tick()
        self.assertEqual(path.read_bytes(), prior)
        self.assertEqual(self.finance.invocations, 1)
        self.assertEqual(sum(ref.kind == "cumulative-result" for ref in self.mesh.arrived()), 1)
        self.driver.publish_role_event = Mock()
        self.driver.final_event("stopped")
        self.driver.publish_role_event.assert_called_once_with("final", outcome="stopped",
            completed_action_ids=(pending.action.request_id,), remaining_action_ids=())

    def test_known_terminal_fault_is_one_shot_and_original_action_reconciles(self):
        self.config["crash_once"] = True
        self.open()
        self.work()
        pending = self.until("pending")
        self.finance.finish(pending.action.request_id)
        self.driver.publish_role_event = Mock()
        with patch("gossip_harness.cumulative_study_role_v1.os._exit", side_effect=SimulatedExit) as exit_call:
            with self.assertRaises(SimulatedExit):
                self.driver.tick()
            exit_call.assert_called_once_with(86)
        self.driver.publish_role_event.assert_called_once_with("final", outcome="restart",
            completed_action_ids=(), remaining_action_ids=(pending.action.request_id,))
        marker = (self.root / "restart-fault.json").read_bytes()
        self.assertEqual(self.driver.loop.snapshots()[0].state, "terminal")
        self.assertFalse(any(ref.kind == "role-result" for ref in self.mesh.arrived()))
        self.driver.close()
        self.driver = None
        self.open()
        with patch("gossip_harness.cumulative_study_role_v1.os._exit") as exit_call:
            self.until("published")
            exit_call.assert_not_called()
        self.assertEqual(self.finance.invocations, 1)
        self.assertEqual((self.root / "restart-fault.json").read_bytes(), marker)

    def test_unknown_stops_and_never_acquires_completed_result(self):
        self.open()
        self.work()
        pending = self.until("pending")
        self.finance.finish(pending.action.request_id, state="unknown")
        stopped = self.until("stopped")
        value = strict_loads((self.root / "results" / (stopped.directive_id + ".json")).read_bytes())
        self.assertEqual(value["snapshot"]["reply"]["state"], "unknown")
        self.assertIsNone(value["result_payload"])
        self.assertEqual(self.finance.invocations, 1)
        self.driver.publish_role_event = Mock()
        self.driver.final_event("stopped")
        self.driver.publish_role_event.assert_called_once_with("final", outcome="stopped",
            completed_action_ids=(), remaining_action_ids=(pending.action.request_id,))

    def test_frozen_config_and_decision_membership_cannot_reset_on_reopen(self):
        self.open()
        ref, work = self.work()
        self.driver.choose_local(ref, work)
        self.driver.close()
        self.driver = None
        changed = deepcopy(self.config)
        changed["placement"] = "durable_central_scheduler"
        with self.assertRaisesRegex(ValueError, "Frozen role config"):
            RoleDriver(self.root, changed, self.mesh, self.finance)
        with sqlite3.connect(self.root / "decisions.sqlite") as db:
            db.execute("DELETE FROM decisions")
        with self.assertRaisesRegex(ValueError, "decision membership"):
            self.open()

    def test_config_is_closed_and_domains_are_bound(self):
        changed = deepcopy(self.config)
        changed["extra"] = True
        with self.assertRaisesRegex(ValueError, "Closed fields"):
            checked_config(changed)
        changed = deepcopy(self.config)
        changed["finance"]["contract_sha256"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "domain differs"):
            checked_config(changed)

    def test_decision_committed_before_publication_replays_and_enqueues(self):
        driver = self.open()
        ref, work = self.work()
        with patch("gossip_harness.cumulative_study_role_v1.immutable_write", side_effect=OSError("interrupted publication")):
            with self.assertRaisesRegex(OSError, "interrupted publication"):
                driver.choose_local(ref, work)
        self.assertEqual(driver.db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0], 1)
        self.assertEqual(driver.loop.snapshots(), ())
        self.assertFalse(any(item.kind == "cumulative-decision" for item in self.mesh.arrived()))
        self.assertEqual(driver.choose_local(ref, work), directive_id(self.directive))
        self.assertEqual(len(driver.loop.snapshots()), 1)
        self.assertTrue((self.root / "decisions/M1-build.json").is_file())
        self.assertEqual(sum(item.kind == "cumulative-decision" for item in self.mesh.arrived()), 1)

    def test_deadline_is_required_finite_positive_and_not_boolean(self):
        for invalid in (True, False, 0, -1, float("inf"), float("nan"), "tomorrow"):
            with self.subTest(invalid=invalid):
                config = {**self.config, "deadline_unix": invalid}
                with self.assertRaisesRegex(ValueError, "role deadline"):
                    checked_config(config)
        config = dict(self.config)
        del config["deadline_unix"]
        with self.assertRaisesRegex(ValueError, "Closed fields"):
            checked_config(config)

    def test_local_and_central_admission_stop_at_exact_deadline(self):
        for placement in ("peer_local", "durable_central_scheduler"):
            with self.subTest(placement=placement), tempfile.TemporaryDirectory() as directory:
                config = {**self.config, "placement": placement}
                driver = RoleDriver(Path(directory), config, self.mesh, self.finance)
                try:
                    self.config["placement"] = placement
                    work_ref, work = self.work(stage=placement)
                    assignment = self.assignment(work_ref, work) if placement == "durable_central_scheduler" else None
                    with patch("gossip_harness.cumulative_study_role_v1.time.time", return_value=config["deadline_unix"]):
                        with self.assertRaises(RoleDeadline):
                            driver.accept_assignment(*assignment) if assignment else driver.choose_local(work_ref, work)
                        with self.assertRaises(RoleDeadline):
                            driver.tick()
                    self.assertEqual(driver.loop.snapshots(), ())
                    self.assertEqual(driver.db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0], 0)
                finally:
                    driver.close()
        self.assertEqual(self.finance.invocations, 0)

    def test_expired_restart_preserves_unqueued_decision_without_admission(self):
        driver = self.open()
        ref, work = self.work()
        with patch("gossip_harness.cumulative_study_role_v1.immutable_write", side_effect=OSError("interruption")):
            with self.assertRaises(OSError):
                driver.choose_local(ref, work)
        driver.close()
        self.driver = None
        with patch("gossip_harness.cumulative_study_role_v1.time.time", return_value=self.config["deadline_unix"]):
            driver = self.open()
            with self.assertRaises(RoleDeadline):
                driver.choose_local(ref, work)
        self.assertEqual(driver.loop.snapshots(), ())
        self.assertFalse(any(item.kind == "cumulative-decision" for item in self.mesh.arrived()))
        driver.publish_role_event = Mock()
        driver.final_event("stopped")
        driver.publish_role_event.assert_called_once_with("final", outcome="stopped",
            completed_action_ids=(), remaining_action_ids=(directive_id(self.directive),))

    def test_main_expired_horizon_stops_with_original_pending_action(self):
        driver = self.open()
        self.work()
        pending = self.until("pending")
        driver.close()
        self.driver = None
        config_path = self.root / "config.json"
        config_path.write_bytes(canonical_bytes(self.config))
        self.mesh.start = Mock(return_value={"pid": 1, "port": 12346})
        self.mesh.close = Mock()
        with patch("gossip_harness.cumulative_study_role_v1.MeshNode", return_value=self.mesh), \
             patch("gossip_harness.cumulative_study_role_v1.FinancialClient", return_value=self.finance), \
             patch("gossip_harness.cumulative_study_role_v1.time.time", return_value=self.config["deadline_unix"]), \
             patch("gossip_harness.cumulative_process_evidence_v1.publish_role_event") as events:
            self.assertEqual(main(["--role-config", str(config_path)]), 0)
        self.assertEqual(events.call_args_list[-1].args, ("final",))
        self.assertEqual(events.call_args_list[-1].kwargs, {"outcome": "stopped", "completed_action_ids": (),
            "remaining_action_ids": (pending.action.request_id,)})
        self.assertEqual(self.finance.invocations, 1)


if __name__ == "__main__":
    unittest.main()
