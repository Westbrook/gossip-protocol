"""Pure mapping controls; synthetic observations confer no physical authority."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import unittest

from gossip_harness import candidate_cli_cases_v1 as original
from gossip_harness import candidate_cli_acceptance_profile_v1 as profile
from gossip_harness import candidate_client_execution_v5 as execution
from gossip_harness import candidate_client_observation_source_v1 as bridge
from gossip_harness import candidate_client_observer_v4 as old_observer
from gossip_harness import candidate_client_observer_v5 as observer


def synthetic(case_id, index, stdout=b"{}", stderr=b"", exit_code=0, *, unavailable_stdout=False):
    steps = profile.execution_recipe(case_id)["steps"]
    step = steps[index]
    binding = {key: "a" * 64 for key in observer._DIGEST_FIELDS}
    binding.update(protocol=observer.BINDING_PROTOCOL, execution_id="synthetic-control",
        commit_oid="b" * 40, tree_oid="c" * 40, milestone="M1", purpose="public_release",
        case_id=case_id, step_id=step["step_id"], step_index=index,
        ordered_step_ids=[item["step_id"] for item in steps], argv=step["argv"])
    def stream(raw, path, complete=True):
        return {"path": path, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
                "observed_bytes": len(raw), "truncated": False, "complete": complete}
    transport = {"status": "output_limit" if unavailable_stdout else "completed",
        "capture_complete": not unavailable_stdout, "exit_code": exit_code,
        "history_state_verified": True,
        "stdout": stream(stdout, "stdout.bin", not unavailable_stdout),
        "stderr": stream(stderr, "stderr.bin"),
        "completion": {"container_id": "d" * 64, "started": True, "natural": True,
            "wait_status_code": exit_code, "inspect_exit_code": exit_code,
            "finished_at": "2026-10-04T00:00:00Z", "oom_killed": False, "state_error": "",
            "identity_verified": True, "killed_by_controller": False, "wait_error": None,
            "signal_exit_ambiguous": False}}
    return observer.process_observation(binding, transport, stdout, stderr)


class CandidateClientObservationSourceV1Tests(unittest.TestCase):
    def test_complete_catalog_retains_all_original_recipes_expectations_and_cells(self):
        expected, actual = original.definitions(), profile.definitions()
        self.assertEqual(len(actual), 57)
        cells = []
        steps = 0
        for before, after in zip(expected, actual, strict=True):
            self.assertEqual(before["recipe"], after["recipe"])
            self.assertEqual(before["expectations"], after["expectations"])
            self.assertEqual(after["original_definition_purpose"], "harness_qualification")
            self.assertIn("not held-out or source-blind", after["authoring_disclosure"])
            mapped = bridge._mapped_cells(after["case_id"], ())
            self.assertEqual(tuple(item["case_id"] for item in mapped),
                             profile.ordered_assertion_ids(after["case_id"]))
            cells.extend(mapped)
            steps += len(after["recipe"]["steps"])
        self.assertEqual(steps, 305)
        self.assertEqual(len(cells), 900)
        self.assertEqual(sum(item["status"] == "skipped" for item in cells), 32)
        self.assertEqual(sum(item["status"] == "infrastructure_error" for item in cells), 868)
        self.assertTrue(all(item["status"] not in ("passed", "failed") for item in cells))

    def test_known_failure_survives_unavailable_stream_and_unentered_siblings(self):
        observed = synthetic("cli-empty", 0, exit_code=2, unavailable_stdout=True)
        cells = bridge._mapped_cells("cli-empty", (observed,))
        self.assertEqual(cells[0]["status"], "failed")
        self.assertEqual(cells[0]["assertion_id"], "process.exit")
        self.assertEqual(cells[1]["status"], "infrastructure_error")
        self.assertTrue(all(item["status"] == "infrastructure_error" for item in cells[1:]))
        self.assertTrue(all(not item["step_entered"] for item in cells[3:]))
        self.assertEqual(len(cells), 12)

    def test_matching_prefix_never_turns_missing_tail_into_success(self):
        raw = profile.encoded({"documents": [], "total": 0})
        cells = bridge._mapped_cells("cli-empty", (synthetic("cli-empty", 0, stdout=raw),))
        self.assertEqual([item["status"] for item in cells[:3]], ["passed"] * 3)
        self.assertEqual([item["status"] for item in cells[3:]], ["infrastructure_error"] * 9)

    def test_unspecified_presentation_stays_skipped_even_when_exit_matches(self):
        case_id = "cli-grammar-unknown-command"
        cells = bridge._mapped_cells(case_id, (synthetic(case_id, 0, exit_code=2),))
        self.assertEqual([item["status"] for item in cells], ["passed", "skipped"])
        self.assertEqual(cells[1]["disposition"], "unspecified")
        self.assertIsNone(cells[1]["value"])

    def test_wrong_step_order_history_and_untyped_dicts_are_rejected(self):
        for observation in (synthetic("cli-empty", 1), synthetic("cli-legacy-persistence", 0), {"passed": True}):
            with self.subTest(observation=type(observation)), self.assertRaises(bridge.ObservationError):
                bridge._mapped_cells("cli-empty", (observation,))
        with self.assertRaises(bridge.ObservationError):
            bridge._mapped_cells("cli-empty", tuple(synthetic("cli-empty", 0) for _ in range(5)))

    def test_old_qualification_observations_and_product_purpose_relabel_are_rejected(self):
        observed = synthetic("cli-empty", 0)
        binding = json.loads(observed.binding_json)
        transport = json.loads(observed.transport_json)
        old = old_observer.process_observation(binding | {
            "protocol": old_observer.BINDING_PROTOCOL, "purpose": "harness_qualification"},
            transport, observed.stdout, observed.stderr)
        with self.assertRaises(bridge.ObservationError):
            bridge._mapped_cells("cli-empty", (old,))
        with self.assertRaises(observer.ObservationUnavailable):
            observer.process_observation(binding | {"purpose": "harness_qualification"},
                                         transport, observed.stdout, observed.stderr)
        with self.assertRaises(observer.ObservationUnavailable):
            observer.process_observation(binding | {"protocol": old_observer.BINDING_PROTOCOL},
                                         transport, observed.stdout, observed.stderr)

    def test_original_fork_hashes_remain_exact(self):
        root = Path(execution.__file__).parent
        self.assertEqual(execution.FROZEN_V4_SOURCE_SHA256,
            hashlib.sha256((root / "candidate_client_execution_v4.py").read_bytes()).hexdigest())
        self.assertEqual(observer.FROZEN_V4_SOURCE_SHA256,
            hashlib.sha256((root / "candidate_client_observer_v4.py").read_bytes()).hexdigest())

    def test_consumer_preserves_failure_when_cli_boundary_reports_invalid_or_unavailable(self):
        # The bridge call is real; only its lower original-journal boundary is
        # mocked. This checks exception isolation, not physical authentication.
        from dataclasses import replace
        from unittest.mock import patch
        import subprocess
        from gossip_harness import candidate_release_execution_v2 as release
        from gossip_harness.gitstore import GitError
        from gossip_harness import candidate_observation_admission_v1 as admission
        from gossip_harness import candidate_scope_consumer_v1 as consumer
        from gossip_harness import project_acceptance_registry_v1 as registry
        from tests.test_candidate_scope_consumer_v1 import FixtureAuthority, FixtureSnapshot, duplicate_gate
        from tests.test_project_acceptance_compiler_v1 import inventory, subject, synthetic_declaration, synthetic_scope
        inv = inventory(2)
        declaration = duplicate_gate(synthetic_declaration(inv), "product")
        scope, subj = synthetic_scope(inv, declaration), subject(inv)
        errors = ((admission.AdmissionUnavailable("original absent"), "infrastructure_error"),
                  (admission.AdmissionError("original changed"), "invalid"),
                  (execution.ExecutionError("journal changed"), "invalid"),
                  (execution.process_transport.ProcessError("runtime proof invalid"), "invalid"),
                  (release.ExecutionError("source capture invalid"), "invalid"),
                  (GitError("registered tree unavailable"), "infrastructure_error"),
                  (subprocess.TimeoutExpired("git", 1), "infrastructure_error"),
                  (OSError("storage unavailable"), "infrastructure_error"),
                  (execution.ExecutionUnknown("intent has no terminal"), "infrastructure_error"),
                  (observer.ObservationUnavailable("raw observation invalid"), "invalid"),
                  (bridge.ObservationError("checkpoint differs"), "invalid"))
        for error, issue_status in errors:
            with self.subTest(error=type(error)):
                snapshot = FixtureSnapshot(inv, declaration, scope, subj)
                first = snapshot.observations["product"]
                snapshot.observations["product"] = replace(first, execution=replace(first.execution,
                    outcomes=(registry.CaseResult("case", "failed"),)))
                source = object.__new__(bridge.ClientObservationSource)
                original = snapshot.observation
                def observe(gate, freeze):
                    return source.observation(gate, freeze) if gate.gate_id == "product-two" else original(gate, freeze)
                snapshot.observation = observe
                with patch.object(source, "_observation", side_effect=error):
                    result = consumer.CandidateScopeConsumer(FixtureAuthority(snapshot)).assess(
                        inventory=inv, declaration=declaration, scope=scope, subject=subj)
                self.assertEqual(result.product.failed_gates, ("product",))
                self.assertEqual(result.product.missing_gates, ("product-two",))
                self.assertEqual(result.product.status, "rejected")
                self.assertTrue(any(item.target == "product-two" and item.status == issue_status for item in result.issues))
