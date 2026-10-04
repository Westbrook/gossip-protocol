"""Authored parser/boundary controls; these are never physical evidence."""
from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness import candidate_http_execution_v3 as historical
from gossip_harness import candidate_http_execution_v4 as execution
from gossip_harness import candidate_http_observation_v2 as reader
from gossip_harness import candidate_client_process_v4 as engine
from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness import candidate_http_journal_v3 as journal
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_http_transport_v1 as wire
from tests.test_candidate_http_observation_v1 import transcript, request, HEALTH_RESPONSE


CID = "a" * 64
IMAGE = "sha256:" + "b" * 64


def fixture_checkpoint(files=(), raw_bytes=0):
    """Parser identity only; no external authority or physical execution."""
    return chain.PrefixCommitment("c" * 64, len(files), execution.digest(dict(files)), len(files), raw_bytes, 0)


class FixtureAuthenticatedOwner:
    """Explicit private reader capability, rejected by public observation admission."""
    def __init__(self, root, files):
        self.root, self.files = root, dict(files)
        self.expected = fixture_checkpoint(files, sum((root / name).stat().st_size for name in self.files))
        self.current = self.expected
        self.reads = []

    def checkpoint(self):
        return self.current

    def read_authenticated(self, name):
        if self.current != self.expected:
            raise chain.ChainUnknown("Fixture external head changed")
        if name not in self.files:
            raise chain.ChainError("Uncommitted fixture artifact")
        raw = journal.read(self.root / name)
        if execution.sha256(raw) != self.files[name]:
            raise chain.ChainError("Fixture original changed")
        self.reads.append(name)
        return raw


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
        owner = FixtureAuthenticatedOwner(self.root, self.files.items())
        return reader._Reader(owner, owner.expected)

    def test_old_owner_dict_namespace_and_fixture_have_no_physical_authority(self):
        checkpoint = fixture_checkpoint()
        for owner in ({}, SimpleNamespace(mode="physical"), FixtureAuthenticatedOwner(self.root, ()),
                      object.__new__(historical.CandidateHttpExecution)):
            with self.subTest(owner=type(owner).__name__), self.assertRaises(reader.ObservationError):
                reader.observe_execution(owner, checkpoint)
        fixture = object.__new__(execution.CandidateHttpExecution)
        fixture.mode = "fixture"
        with patch.object(execution.CandidateHttpExecution, "verified_execution") as verify:
            with self.assertRaisesRegex(reader.ObservationError, "Fixture"):
                reader.observe_execution(fixture, checkpoint)
            fixture.mode = "physical"
            with self.assertRaisesRegex(reader.ObservationError, "externally retained checkpoint"):
                reader.observe_execution(fixture, historical.ControllerCheckpoint(()))
            verify.assert_not_called()

    def test_reader_requires_exact_current_prefix_before_any_raw_read(self):
        self.save("original.bin", b"original")
        owner = FixtureAuthenticatedOwner(self.root, self.files.items())
        with self.assertRaisesRegex(reader.ObservationError, "Checkpoint"):
            reader._Reader(owner, replace(owner.expected, head_sha256="e" * 64))
        self.assertEqual(owner.reads, [])
        with patch.object(owner, "checkpoint", side_effect=chain.ChainError("invalid boundary")):
            with self.assertRaises(reader.ObservationError):
                reader._Reader(owner, owner.expected)

    def test_raw_uses_owner_capability_and_never_falls_back_to_path_reader(self):
        self.save("original.bin", b"original")
        value = self.fixture_reader()
        with patch.object(execution, "_read", side_effect=AssertionError("raw path bypass"), create=True):
            self.assertEqual(value.raw("original.bin"), b"original")
        self.assertEqual(value.owner.reads, ["original.bin"])
        value.owner.current = replace(value.checkpoint, head_sha256="e" * 64)
        with self.assertRaises(chain.ChainUnknown):
            value.raw("original.bin")
        with self.assertRaises(reader.ObservationError):
            value.validate_checkpoint()

    def test_unknown_head_is_not_reclassified_as_invalid_original(self):
        value = self.fixture_reader()
        for error_type in (chain.ChainUnknown, execution.ExecutionUnknown):
            with self.subTest(error_type=error_type.__name__):
                unknown = error_type("anchor unavailable")
                with patch.object(value.owner, "read_authenticated", side_effect=unknown):
                    with self.assertRaises(error_type) as caught:
                        value.raw("original.bin")
                self.assertIs(caught.exception, unknown)
                with patch.object(value.owner, "checkpoint", side_effect=unknown):
                    with self.assertRaises(error_type) as caught:
                        value.validate_checkpoint()
                self.assertIs(caught.exception, unknown)

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
        capability = FixtureAuthenticatedOwner(self.root, self.files.items())
        checkpoint = capability.expected
        missing = tuple(step.step_id for step in owner.recipe.steps)
        result = execution.HttpHistoryResult("http-fixture", "observation_unavailable", (), missing, False,
            ("no source was executed",), self.files["terminal.json"], checkpoint)
        owner.actual_registration = SimpleNamespace(gate=None)
        owner.binding = None
        with patch.object(execution.CandidateHttpExecution, "checkpoint", return_value=checkpoint) as boundary, \
             patch.object(execution.CandidateHttpExecution, "read_authenticated", side_effect=capability.read_authenticated), \
             patch.object(execution.CandidateHttpExecution, "verified_execution", return_value=result), \
             patch.object(execution.CandidateHttpExecution, "_labels", return_value=()), \
             patch.object(reader.admission, "binding_sha256", return_value="d" * 64):
            observed = reader.observe_execution(owner, checkpoint)
            boundary.side_effect = [checkpoint, checkpoint, replace(checkpoint, head_sha256="e" * 64)]
            with self.assertRaisesRegex(reader.ObservationError, "Checkpoint"):
                reader.observe_execution(owner, checkpoint)
        self.assertEqual(tuple(step.step_id for step in observed.steps), missing)
        self.assertEqual(tuple(step.state for step in observed.steps), ("unentered",) * 3)
        self.assertTrue(all(step.facts is None and step.exit_code is None for step in observed.steps))
        self.assertFalse(observed.cleanup_verified)

    def test_partial_cli_preserves_finite_exit_but_does_not_swallow_unknown_authority(self):
        owner = object.__new__(execution.CandidateHttpExecution)
        owner.mode, owner.root = "physical", self.root
        owner.recipe = execution.HttpRecipe("partial-cli", ("python", "/workspace/app.py", "--db", "/tmp/library.sqlite",
            "--root", "/inputs", "--port", "8765"), (), (),
            (execution.HttpStep("start-1", "start"), execution.HttpStep("request-1", "probe", execution.encoded(request())),
             execution.HttpStep("stop-1", "stop"),
             execution.HttpStep("cli-1", "cli", argv=("python", "/workspace/app.py", "/tmp/library.sqlite", "/inputs"))))
        owner.policy = execution.HttpPolicy(IMAGE)
        owner.actual_registration = SimpleNamespace(gate=None)
        owner.binding = SimpleNamespace(source_sha256="a" * 64)
        self.save("intent.json", execution.encoded({}))
        self.save("terminal.json", execution.encoded({"steps": [], "entered_step_indices": [0, 1, 2, 3]}))
        capability = FixtureAuthenticatedOwner(self.root, self.files.items())
        checkpoint = capability.expected
        row = {"step_index": 3, "authenticated": False, "process": {}}
        result = execution.HttpHistoryResult("http-fixture", "observation_unavailable", (),
            tuple(step.step_id for step in owner.recipe.steps), False, ("synthetic partial CLI",),
            self.files["terminal.json"], checkpoint, (row,))
        with patch.object(execution.CandidateHttpExecution, "checkpoint", return_value=checkpoint), \
             patch.object(execution.CandidateHttpExecution, "read_authenticated", side_effect=capability.read_authenticated), \
             patch.object(execution.CandidateHttpExecution, "verified_execution", return_value=result), \
             patch.object(reader, "_spec", return_value=None), \
             patch.object(reader, "_completion", return_value=(CID, "cli", None, 7)) as completion, \
             patch.object(reader.admission, "binding_sha256", return_value="d" * 64):
            observed = reader.observe_execution(owner, checkpoint)
            self.assertEqual(observed.steps[-1].exit_code, 7)
            self.assertEqual(observed.steps[-1].state, "unavailable")
            self.assertIsNone(observed.steps[-1].stdout)
            unknown = chain.ChainUnknown("head unavailable during finite exit reconstruction")
            completion.side_effect = unknown
            with self.assertRaises(chain.ChainUnknown) as caught:
                reader.observe_execution(owner, checkpoint)
            self.assertIs(caught.exception, unknown)


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
