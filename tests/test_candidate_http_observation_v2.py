"""Authored parser/boundary controls; these are never physical evidence."""
from __future__ import annotations

import hashlib
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness import candidate_http_execution_v3 as historical
from gossip_harness import candidate_http_execution_v4 as execution
from gossip_harness import candidate_http_observation_v2 as reader
from gossip_harness import candidate_client_process_v4 as engine
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_http_transport_v1 as wire
from tests.test_candidate_http_observation_v1 import transcript, request, HEALTH_RESPONSE


CID = "a" * 64
IMAGE = "sha256:" + "b" * 64


class HttpObservationV2BoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gossip-http-reader-v2-")
        self.root = Path(self.temp.name).resolve()
        self.files = {}

    def tearDown(self):
        self.temp.cleanup()

    def save(self, name, raw):
        (self.root / name).write_bytes(raw)
        self.files[name] = hashlib.sha256(raw).hexdigest()
        return {"path": name, "sha256": self.files[name], "bytes": len(raw)}

    def fixture_reader(self):
        # Deliberately private parser fixture. The public admission API rejects
        # this namespace and cannot turn it into a Registry observation.
        owner = SimpleNamespace(root=self.root)
        return reader._Reader(owner, execution.ControllerCheckpoint(tuple(sorted(self.files.items()))))

    def test_old_owner_dict_namespace_and_fixture_have_no_physical_authority(self):
        checkpoint = execution.ControllerCheckpoint(())
        for owner in ({}, SimpleNamespace(mode="physical"), object.__new__(historical.CandidateHttpExecution)):
            with self.subTest(owner=type(owner).__name__), self.assertRaises(reader.ObservationError):
                reader.observe_execution(owner, checkpoint)
        fixture = object.__new__(execution.CandidateHttpExecution)
        fixture.mode = "fixture"
        with patch.object(execution.CandidateHttpExecution, "verified_execution") as verify:
            with self.assertRaisesRegex(reader.ObservationError, "Fixture"):
                reader.observe_execution(fixture, checkpoint)
            verify.assert_not_called()

    def test_raw_requires_checkpointed_exact_bytes_and_safe_original(self):
        self.save("original.bin", b"original")
        value = self.fixture_reader()
        self.assertEqual(value.raw("original.bin"), b"original")
        self.save("appended.bin", b"foreign")
        with self.assertRaises(reader.ObservationError):
            value.raw("appended.bin")
        with self.assertRaises(reader.ObservationError):
            value.raw("../original.bin")
        (self.root / "original.bin").write_bytes(b"changed")
        with self.assertRaises(reader.ObservationError):
            value.raw("original.bin")

    def test_symlink_and_descriptor_substitution_rejected(self):
        item = self.save("original.bin", b"original")
        value = self.fixture_reader()
        for changed in ({**item, "bytes": True}, {**item, "sha256": "f" * 64}, {**item, "path": "missing.bin"}):
            with self.subTest(changed=changed), self.assertRaises(reader.ObservationError):
                value.descriptor(changed)
        (self.root / "original.bin").unlink()
        (self.root / "target.bin").write_bytes(b"original")
        (self.root / "original.bin").symlink_to(self.root / "target.bin")
        with self.assertRaises(ValueError):
            value.raw("original.bin")

    def test_role_reconstruction_keeps_empty_optional_fields_and_exact_step_identity(self):
        recipe = execution.HttpRecipe("role-history", ("python", "/workspace/app.py", "--db", "/tmp/library.sqlite",
            "--root", "/inputs", "--port", "8765"), (), (),
            (execution.HttpStep("start-1", "start"), execution.HttpStep("request-1", "probe", execution.encoded(request())),
             execution.HttpStep("stop-1", "stop")))
        staging = {"workspace": str(self.root / "workspace"), "inputs": str(self.root / "inputs")}
        owner = SimpleNamespace(recipe=recipe, policy=execution.HttpPolicy(IMAGE), _labels=lambda *args: ())
        fixture = SimpleNamespace(owner=owner, json=lambda name: staging)
        intent = {"containers": ["gossip-server", "gossip-probe", "gossip-stop"], "volume": "gossip-volume"}
        server = reader._spec(fixture, intent, 0, "server")
        probe = reader._spec(fixture, intent, 1, "probe", CID)
        self.assertEqual(server.server_id, "")
        self.assertEqual(server.volume, "gossip-volume")
        self.assertEqual(probe.server_id, CID)
        self.assertEqual(probe.volume, "")
        self.assertEqual(probe.binds, (("/probe", str(self.root / "step-001")),))
        self.assertEqual(probe.argv, execution.PROBE_ARGV)

    def test_control_checks_process_operation_and_strict_framing(self):
        self.save("control-request.bin", engine._request("GET", "/containers/" + CID + "/json"))
        self.save("control-response.bin", b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
        value = self.fixture_reader()
        self.assertEqual(value.control("control", CID), b"{}")
        with self.assertRaises(reader.ObservationError):
            value.control("control", "c" * 64)
        with self.assertRaises(reader.ObservationError):
            value.control("control", CID, "wait?condition=not-running", "POST")
        self.save("control-response.bin", b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\n{}")
        with self.assertRaises(ValueError):
            self.fixture_reader().control("control", CID)

    def test_multiplex_reconstruction_rejects_forged_stream_and_unproven_eof(self):
        policy = engine.ProcessPolicy(IMAGE)
        stdout, stderr = b"candidate output", b"candidate error"
        framed = b"".join(bytes([channel, 0, 0, 0]) + len(raw).to_bytes(4, "big") + raw
                          for channel, raw in ((1, stdout), (2, stderr)))
        self.save("process-attach-request.bin", engine._request("POST", "/containers/" + CID
            + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0", upgrade=True))
        self.save("process-attach-response.bin", b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n"
            b"Content-Type: application/vnd.docker.raw-stream\r\n\r\n" + framed)
        process = {"capture_complete": True, "attach_eof_after_start_confirmation": True}
        for name, raw in (("stdout", stdout), ("stderr", stderr)):
            process[name] = {**self.save(name + ".bin", raw), "observed_bytes": len(raw), "truncated": False, "complete": True}
        value = self.fixture_reader()
        self.assertEqual(reader._streams(value, "process", CID, process, policy), (stdout, stderr))
        with self.assertRaises(reader.ObservationError):
            reader._streams(value, "process", CID, {**process, "attach_eof_after_start_confirmation": False}, policy)
        # A newly hashed descriptor still cannot replace the original raw frames.
        forged = {**self.save("forged.bin", b"forged"), "observed_bytes": 6, "truncated": False, "complete": True}
        with self.assertRaises(reader.ObservationError):
            reader._streams(self.fixture_reader(), "process", CID, {**process, "stdout": forged}, policy)

    def test_unentered_roster_is_preserved_under_explicit_synthetic_owner_patch(self):
        # This fixture tests denominator preservation only. Patching the trusted
        # owner is not an accepted route for physical observations in production.
        owner = object.__new__(execution.CandidateHttpExecution)
        owner.mode, owner.root = "physical", self.root
        owner.recipe = execution.HttpRecipe("missing-history", ("python", "/workspace/app.py", "--db", "/tmp/library.sqlite", "--root", "/inputs", "--port", "8765"), (), (),
            (execution.HttpStep("start-1", "start"), execution.HttpStep("request-1", "probe", execution.encoded(request())),
             execution.HttpStep("stop-1", "stop")))
        owner.policy = execution.HttpPolicy(IMAGE)
        self.save("intent.json", execution.encoded({"keeper": "gossip-fixture-keeper", "volume": "gossip-fixture-volume",
            "execution_id": "http-fixture"}))
        self.save("terminal.json", execution.encoded({"steps": [], "entered_step_indices": []}))
        checkpoint = execution.ControllerCheckpoint(tuple(sorted(self.files.items())))
        missing = tuple(step.step_id for step in owner.recipe.steps)
        result = execution.HttpHistoryResult("http-fixture", "observation_unavailable", (), missing, False,
            ("no source was executed",), self.files["terminal.json"], checkpoint)
        owner.actual_registration = SimpleNamespace(gate=None)
        owner.binding = None
        with patch.object(execution.CandidateHttpExecution, "checkpoint", return_value=checkpoint), \
             patch.object(execution.CandidateHttpExecution, "verified_execution", return_value=result), \
             patch.object(execution.CandidateHttpExecution, "_labels", return_value=()), \
             patch.object(reader.admission, "binding_sha256", return_value="d" * 64):
            observed = reader.observe_execution(owner, checkpoint)
        self.assertEqual(tuple(step.step_id for step in observed.steps), missing)
        self.assertEqual(tuple(step.state for step in observed.steps), ("unentered",) * 3)
        self.assertTrue(all(step.facts is None and step.exit_code is None for step in observed.steps))
        self.assertFalse(observed.cleanup_verified)


class HttpObservationV2FactTests(unittest.TestCase):
    def observed(self, raw=HEALTH_RESPONSE, **kwargs):
        limits = wire.WireLimits()
        return wire.decode_probe_output(transcript(raw, limits=limits, **kwargs), request(), 8765, limits), limits

    def test_known_status_survives_incomplete_body_without_inventing_body(self):
        observation, limits = self.observed(b"HTTP/1.1 500 Error\r\nContent-Type: application/json\r\nContent-Length: 99\r\n\r\n{}",
                                            termination="timeout")
        facts = reader._facts(observation, limits)
        self.assertEqual(facts.status, 500)
        self.assertIsInstance(facts.body, semantics.Missing)
        self.assertIsInstance(facts.listener_before, semantics.ListenerFacts)

    def test_partial_send_does_not_grade_response_as_complete_intended_request(self):
        observation, limits = self.observed(b"", termination="send_error", sent=b"GET /hea")
        facts = reader._facts(observation, limits)
        self.assertIsInstance(facts.status, semantics.Missing)
        self.assertIsInstance(facts.headers, semantics.Missing)
        self.assertIsInstance(facts.body, semantics.Missing)
        self.assertIsInstance(facts.listener_before, semantics.ListenerFacts)

    def test_listener_failure_and_partial_coverage_remain_visible(self):
        observation, limits = self.observed(wildcard=True, missing_tcp6=True)
        facts = reader._facts(observation, limits)
        self.assertEqual(facts.listener_before.addresses, ("0.0.0.0",))
        self.assertIsNotNone(facts.listener_before.limitation)
        self.assertEqual(semantics.listener("listener_before", facts.listener_before).disposition, "fail")

    def test_connection_failure_diagnostic_is_not_live_listener_coverage(self):
        observation, limits = self.observed(b"", connected=False, sent=b"", termination="connect_error")
        facts = reader._facts(observation, limits)
        self.assertIsInstance(facts.listener_before, semantics.Missing)
        self.assertIsInstance(facts.listener_after, semantics.Missing)
        self.assertIsInstance(facts.status, semantics.Missing)


if __name__ == "__main__":
    unittest.main()
