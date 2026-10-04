"""Prospective HTTP-only physical v3 extensions and supplied-transcript controls.

The Docker class measures fixed transport/staging mechanics with inert authored
server bytes. It grants no product acceptance or comparative study credit. The
separate transcript class never contacts a socket, Docker, or a candidate.
"""
from __future__ import annotations

import base64
from dataclasses import asdict
import errno
import json
import os
from pathlib import Path
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_cli_cases_v1 as cli_cases
from gossip_harness import candidate_client_process_v4 as engine
from gossip_harness import candidate_http_execution_v3 as execution
from gossip_harness import candidate_http_transport_v1 as wire
from gossip_harness.gitstore import GitStore
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
from tests import candidate_http_v3_fixtures as fixtures
from tests import test_candidate_http_docker_v3 as raw_checks


PURPOSE = "harness_qualification"
EARLY_CLOSE_SEND_CLASSES = ("sent_complete", "sent_incomplete")
EARLY_CLOSE_TERMINATIONS = ("message_complete", "eof", "receive_error", "send_error")
LONG_ROW = "HTTP-ACTION-STATE/old-token-after-successor-failed"
LONG_DEFINITION_SHA256 = "dc486b5dcfad72782813ddefda8a4a9201e03f2247e3686cc9b97b2acb1e3d97"


def policy():
    return execution.HttpPolicy(RUNTIME_IMAGE, wire_limits=wire.WireLimits(
        response_limit_bytes=65536, timeout_seconds=5.0), lifetime_seconds=7200)


def control_definitions():
    """Freeze the entire ordered slice before its first execution, including negatives."""
    declarations = (
        ("E01", "fixed-70-request-history", fixtures.fixed_long_recipe()),
        ("E02", "per-epoch-roots-and-confined-links", fixtures.root_link_recipe()),
        ("E03", "body-65536-complete", fixtures.body_recipe(65536)),
        ("E04", "body-65537-complete", fixtures.body_recipe(65537)),
        ("E05", "body-65537-early-close", fixtures.body_recipe(65537, early_close=True)),
    )
    return tuple({"control_id": identifier, "name": name, "recipe": recipe,
        "expected": fixtures.expected_observations(recipe), "policy": policy(),
        "purpose": PURPOSE, "acceptance_credit": False,
        "allowed_send_classes": list(EARLY_CLOSE_SEND_CLASSES) if identifier == "E05" else ["sent_complete"],
        "allowed_early_close_terminations": list(EARLY_CLOSE_TERMINATIONS) if identifier == "E05" else None,
        "repeat_until_partial_send": False} for identifier, name, recipe in declarations)


def definition_record(row):
    return {**row, "recipe": row["recipe"].record(), "policy": asdict(row["policy"]),
            "expected": list(row["expected"])}


def request_for(recipe, step):
    return engine.strict_json_loads(step.request_json)


def assert_early_close_classification(test, observed, expected):
    """Classify actual evidence; neither send outcome proves server body consumption."""
    test.assertIs(observed.connected, True)
    test.assertTrue(observed.request.startswith(observed.sent))
    test.assertIn(observed.termination, EARLY_CLOSE_TERMINATIONS)
    send_class = "sent_complete" if observed.sent == observed.request else "sent_incomplete"
    test.assertIn(send_class, EARLY_CLOSE_SEND_CLASSES)
    test.assertIs(observed.sent_complete, send_class == "sent_complete")
    if send_class == "sent_incomplete":
        test.assertEqual(observed.termination, "send_error")
        test.assertEqual(observed.received, b"")
        test.assertIs(observed.exchange_complete, False)
        test.assertIn("request_incomplete", observed.limitations)
    if observed.response.body_complete:
        test.assertIs(observed.exchange_complete, True)
        test.assertEqual(observed.response.status_code, expected["status"])
        # Canonical comparison preserves JSON bool/number type distinctions.
        test.assertEqual(raw_checks.encoded(raw_checks.strict_control_json(observed.response.body)),
                         raw_checks.encoded(expected["json"]))
        response_class = "complete_expected_response"
    else:
        test.assertIs(observed.exchange_complete, False)
        response_class = "incomplete_response_no_server_consumption_claim"
    return {"send_class": send_class, "response_class": response_class,
            "sent_bytes": len(observed.sent), "request_bytes": len(observed.request),
            "received_bytes": len(observed.received), "termination": observed.termination,
            "server_consumption_evidence": "complete_fixture_response" if observed.response.body_complete else "unavailable",
            "partial_send_was_required": False, "acceptance_credit": False,
            "physical_incomplete_send_observed": None,
            "supplied_transcript_is_physical_evidence": False}


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateHttpExtensionsV3DockerTests(raw_checks.HttpV3PhysicalAssertions, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = ArtifactDirectory("candidate-c03-http-extensions-v3", retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.controls = {row["control_id"]: row for row in control_definitions()}
        cls.outcomes = {key: {"control_id": key, "status": "not-run", "acceptance_credit": False}
                        for key in cls.controls}
        cls.addClassCleanup(cls.save_census)
        cls.endpoint = engine.EngineEndpoint.from_environment()

        def retain_runtime(name, raw):
            with (cls.artifacts.root / name).open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())

        cls.runtime = engine.runtime_identity(cls.endpoint, RUNTIME_IMAGE,
            retain=retain_runtime, label="class-runtime")
        cls.files = fixtures.source_files()
        cls.store = GitStore.create(cls.artifacts.root / "fixture.git",
            {name: raw.decode("utf-8") for name, raw in cls.files.items()})
        cls.commit = cls.store.head()
        cls.tree, captured = execution.capture_git_source(cls.store, cls.commit)
        if captured != cls.files:
            raise AssertionError("Committed fixed fixture differs from authored bytes")
        cls.requirements_sha256 = cli_cases.NORMATIVE_SHA256["library-cumulative-product-v2.json"]
        records = [definition_record(row) for row in cls.controls.values()]
        cls.roster_sha256 = raw_checks.sha256(raw_checks.encoded(records))
        raw_checks.write_new(cls.artifacts.root / "prospective-controls.json", {
            "controls": records, "ordered_roster_sha256": cls.roster_sha256,
            "source_commit": cls.commit, "source_tree": cls.tree,
            "source_files": {name: raw_checks.sha256(raw) for name, raw in cls.files.items()},
            "source_sha256": execution.source_sha256(cls.files), "runtime": cls.runtime,
            "helper_sha256": wire.helper_sha256(), "evaluator_sources": execution.evaluator_sources(),
            "definition_sources": {Path(module.__file__).name: raw_checks.sha256(Path(module.__file__).read_bytes())
                for module in (fixtures, raw_checks, raw_checks.control_framing)},
            "control_source_sha256": raw_checks.sha256(Path(__file__).read_bytes()),
            "purpose": PURPOSE, "history_count": 5, "declared_request_count": 77,
            "mixed_http_cli_history_included": False, "complete_v3_gate": False,
            "acceptance_credit": False, "full_requirement_verdict": None})

    @classmethod
    def save_census(cls):
        raw_checks.write_new(cls.artifacts.root / "completion-census.json", {
            "planned": list(cls.controls), "planned_history_count": 5,
            "planned_request_count": 77, "outcomes": list(cls.outcomes.values()),
            "not_run": [key for key, value in cls.outcomes.items() if value["status"] == "not-run"],
            "not_qualified": [key for key, value in cls.outcomes.items() if value["status"] != "qualified"],
            "complete_v3_gate": False, "mixed_http_cli_history_included": False,
            "acceptance_credit": False, "production_acceptance_authority": False,
            "full_requirement_verdict": None})

    def finish_control(self, identifier, assertions):
        self.outcomes[identifier].update(status="qualified", mechanics_assertions=assertions,
            qualification_verified=True, acceptance_credit=False, production_acceptance_authority=False)
        raw_checks.write_new(self.artifacts.root / identifier.lower() / "mechanics-outcome.json",
                             self.outcomes[identifier])

    def run_history(self, identifier):
        self.assertEqual(self.outcomes[identifier]["status"], "not-run", "Never redispatch a control")
        self.outcomes[identifier]["status"] = "started-not-qualified"
        root = self.artifacts.root / identifier.lower()
        root.mkdir()
        row = self.controls[identifier]
        recipe, bounds = row["recipe"], row["policy"]
        self.assertFalse(any(step.kind == "cli" for step in recipe.steps))
        binding = execution.binding_for(self.files, recipe, bounds, self.runtime,
            requirements_sha256=self.requirements_sha256)
        registration = execution.HttpRegistration(binding, self.commit, self.tree,
            "physical-http-extensions-v3-1")
        raw_checks.write_new(root / "prospective-registration.json", {
            "registration": asdict(registration), "definition": definition_record(row),
            "ordered_roster_sha256": self.roster_sha256, "purpose": PURPOSE})
        checkpoints = []

        def checkpoint_sink(checkpoint):
            raw_checks.write_new(root / ("external-checkpoint-" + str(len(checkpoints)).zfill(5) + ".json"),
                                 asdict(checkpoint))
            checkpoints.append(checkpoint)

        journal = root / "execution"
        with execution.CandidateHttpExecution(journal, self.store, registration, recipe, bounds,
                endpoint=self.endpoint, checkpoint_sink=checkpoint_sink) as controller:
            result = controller.execute_once()
            self.assertEqual(result.checkpoint, controller.checkpoint())
        # Retain failure/unknown counts before any qualification assertion can stop this method.
        terminal_raw = raw_checks.read_journal(journal / "terminal.json")
        self.outcomes[identifier].update({"execution_status": result.status,
            "observed_probe_records": len(result.observations), "planned_declared_requests": len(row["expected"]),
            "missing_step_ids": list(result.missing_step_ids), "infrastructure": list(result.infrastructure),
            "cleanup_verified": result.cleanup_verified, "terminal_sha256": result.terminal_sha256,
            "authenticated_sent_complete_requests": terminal_raw["observed_requests"],
            "unknown_request_outcomes": terminal_raw["unknown_request_outcomes"],
            "attempted_probes": terminal_raw["attempted_probes"]})
        raw_checks.write_new(root / "physical-census.json", self.outcomes[identifier])
        terminal = self.authenticate_history(journal, recipe, binding, result, checkpoints)
        probes = [step for step in recipe.steps if step.kind == "probe"]
        starts = [step for step in recipe.steps if step.kind == "start"]
        self.assertEqual(result.status, "completed", result)
        self.assertEqual(result.missing_step_ids, ())
        self.assertEqual(result.infrastructure, ())
        self.assertIs(result.cleanup_verified, True)
        self.assertEqual(result.cli_observations, ())
        self.assertEqual(terminal["planned_requests"], len(probes))
        self.assertEqual(terminal["attempted_probes"], len(probes))
        self.assertEqual(terminal["observed_probe_records"], len(probes))
        self.assertEqual(terminal["unknown_request_outcomes"], 0)
        self.assertEqual(terminal["unentered_step_ids"], [])
        self.assertEqual(terminal["planned_cli"], 0)
        self.assertEqual(terminal["attempted_cli"], 0)
        self.assertEqual(terminal["created_helpers"], len(probes))
        self.assertEqual(terminal["created_servers"], len(starts))
        self.assertEqual(terminal["created_keepers"], 1)
        self.assertEqual(terminal["unconfirmed_creates"], 0)
        self.assertEqual(terminal["create_intents"], len(probes) + len(starts) + 1)
        self.assertEqual([item["step_id"] for item in result.observations], [step.step_id for step in probes])
        self.assertEqual(len({item["probe_id"] for item in result.observations}), len(probes))
        self.assertEqual(len({item["keeper_id"] for item in result.observations}), 1)
        self.assertEqual(len({item["database_volume"] for item in result.observations}), 1)
        self.assertEqual(execution.capture_git_source(self.store, self.commit), (self.tree, self.files))
        return root, journal, recipe, bounds, result, terminal, checkpoints

    def assert_expected_response(self, observed, expected, *, port):
        self.assertIs(observed.exchange_complete, True)
        self.assertIs(observed.sent_complete, True)
        self.assertEqual(observed.response.status_code, expected["status"])
        self.assertEqual(raw_checks.encoded(raw_checks.strict_control_json(observed.response.body)),
                         raw_checks.encoded(expected["json"]))
        self.assert_listener(observed, port=port)

    def test_e01_exact_fixed_seventy_request_history(self):
        _, journal, recipe, bounds, result, terminal, _ = self.run_history("E01")
        self.assertEqual(recipe.row_id, LONG_ROW)
        self.assertEqual(recipe.definition_sha256, LONG_DEFINITION_SHA256)
        self.assertEqual(len(recipe.steps), 72)
        self.assertEqual(len(result.observations), 70)
        expected_rows = self.controls["E01"]["expected"]
        for observation, expected in zip(result.observations, expected_rows, strict=True):
            self.assertEqual(observation["step_id"], expected["step_id"])
            observed = self.authenticated_wire(journal, recipe, bounds, observation)
            self.assert_expected_response(observed, expected, port=recipe.port)
        self.assertEqual(terminal["observed_requests"], 70)
        self.assertEqual(len(terminal["epochs"]), 1)
        self.finish_control("E01", {"exact_full_literal_history_observed": True,
            "request_count": 70, "step_count": 72, "same_continuous_keeper_and_db": True,
            "product_routes_implemented_or_accepted": False})

    def test_e02_per_epoch_roots_and_real_confined_links(self):
        _, journal, recipe, bounds, result, terminal, checkpoints = self.run_history("E02")
        expected_rows = self.controls["E02"]["expected"]
        self.assertEqual(len(expected_rows), 4)
        for observation, expected in zip(result.observations, expected_rows, strict=True):
            self.assertEqual(observation["step_id"], expected["step_id"])
            observed = self.authenticated_wire(journal, recipe, bounds, observation)
            self.assert_expected_response(observed, expected, port=recipe.port)
        first, second = terminal["epochs"]
        self.assertEqual([first["epoch"], second["epoch"]], [1, 2])
        self.assertNotEqual(first["root_path"], second["root_path"])
        self.assertNotEqual(first["server_id"], second["server_id"])
        self.assertEqual(first["database_volume"], second["database_volume"])
        self.assertEqual(first["keeper_id"], second["keeper_id"])
        self.assertEqual(first["keeper_started_at"], second["keeper_started_at"])
        first_seen = {}
        for ordinal, checkpoint in enumerate(checkpoints):
            for name, _ in checkpoint.files:
                first_seen.setdefault(name, ordinal)
        stops = [item for item in terminal["steps"] if item["kind"] == "stop"]
        self.assertIs(stops[0]["removed"], True)
        absence_name = stops[0]["label"] + "-retire-absence.json"
        absence = raw_checks.read_journal(journal / absence_name)
        self.assertEqual(absence["exit_code"], 0)
        self.assertEqual(raw_checks.retained_bytes(self, journal, absence["stdout"]), b"")
        self.assertLess(first_seen[absence_name], first_seen[second["step_label"] + "-start-intent.json"])
        self.assertEqual(terminal["observed_requests"], 4)
        self.finish_control("E02", {"distinct_actual_server_epochs": 2, "per_epoch_root_argv_observed": True,
            "direct_and_ancestor_links_observed_inside_container": True,
            "same_db_state_reopened": True, "crash_recovery_claimed": False,
            "product_path_confinement_accepted": False})

    def assert_body_history(self, identifier, size):
        _, journal, recipe, bounds, result, terminal, _ = self.run_history(identifier)
        self.assertEqual(len(result.observations), 1)
        step = next(step for step in recipe.steps if step.kind == "probe")
        request = request_for(recipe, step)
        body = base64.b64decode(request["body_b64"], validate=True)
        self.assertEqual(len(body), size)
        self.assertIn(["Content-Length", str(size)], request["headers"])
        observed = self.authenticated_wire(journal, recipe, bounds, result.observations[0])
        self.assertEqual(observed.sent, wire.request_bytes(request, recipe.port, bounds.wire_limits))
        self.assertEqual(observed.sent.split(b"\r\n\r\n", 1)[1], body)
        self.assert_expected_response(observed, self.controls[identifier]["expected"][0], port=recipe.port)
        self.assertEqual(terminal["observed_requests"], 1)
        self.finish_control(identifier, {"declared_body_bytes": size, "exact_complete_request_observed": True,
            "server_reported_body_sha256_checked": True, "product_body_limit_verdict": None})

    def test_e03_exact_65536_byte_body(self):
        self.assert_body_history("E03", 65536)

    def test_e04_exact_65537_byte_body(self):
        self.assert_body_history("E04", 65537)

    def test_e05_declared_early_close_single_attempt(self):
        _, journal, recipe, bounds, result, terminal, _ = self.run_history("E05")
        self.assertEqual(len(result.observations), 1)
        observed = self.authenticated_wire(journal, recipe, bounds, result.observations[0],
                                           require_sent_complete=False)
        self.assertEqual(len(observed.request.split(b"\r\n\r\n", 1)[1]), 65537)
        self.assert_listener(observed, port=recipe.port)
        classification = assert_early_close_classification(self, observed, self.controls["E05"]["expected"][0])
        self.assertEqual(terminal["observed_requests"], int(observed.sent_complete))
        self.assertEqual(terminal["attempted_probes"], 1)
        self.finish_control("E05", {**classification, "full_request_redelivered": False,
            "execution_retried_for_partial_send": False, "server_body_drain_required": False,
            "physical_incomplete_send_observed": not observed.sent_complete,
            "physical_incomplete_send_coverage": "observed" if not observed.sent_complete else "unobserved-open",
            "supplied_transcript_negatives_close_physical_coverage": False,
            "product_body_limit_verdict": None})


def supplied_transcript(recipe, bounds, *, sent, received=b"", termination="send_error", socket_errno=errno.EPIPE):
    """Closed supplied-value witness: never physical helper evidence."""
    step = next(step for step in recipe.steps if step.kind == "probe")
    request = request_for(recipe, step)
    raw = wire.request_bytes(request, recipe.port, bounds.wire_limits)

    def record(value):
        return {"b64": base64.b64encode(value).decode("ascii"), "bytes": len(value),
                "sha256": raw_checks.sha256(value)}

    table = b"  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
    snapshot = {"byteorder": "little", "tables": {name: {"status": "ok", "raw": record(table), "errno": None}
        for name in ("tcp", "tcp6")}}
    value = {"protocol": wire.PROTOCOL,
        "input_sha256": raw_checks.sha256(wire.build_probe_input(request, recipe.port, bounds.wire_limits)),
        "request_sha256": raw_checks.sha256(raw), "sent": record(sent), "received": record(received),
        "connected": True, "socket_eof": termination == "eof", "termination": termination,
        "errno": socket_errno, "elapsed_seconds": 0.1, "io_operations": 4,
        "connect_attempts": 1, "connect_refused_attempts": 0,
        "listeners_before_stage": "connected_pre_request", "listeners_before": snapshot,
        "listeners_after": snapshot}
    return request, value


class CandidateHttpExtensionTranscriptV3Tests(unittest.TestCase):
    def test_incomplete_send_preserves_unknown_request_completion(self):
        recipe, bounds = fixtures.body_recipe(65537, early_close=True), policy()
        step = next(step for step in recipe.steps if step.kind == "probe")
        request = request_for(recipe, step)
        raw = wire.request_bytes(request, recipe.port, bounds.wire_limits)
        for size in (1, len(raw) - 1):
            with self.subTest(sent_bytes=size):
                request, value = supplied_transcript(recipe, bounds, sent=raw[:size])
                observed = wire.decode_probe_output(raw_checks.encoded(value), request, recipe.port, bounds.wire_limits)
                self.assertEqual(observed.sent, raw[:size])
                self.assertIs(observed.sent_complete, False)
                self.assertIs(observed.exchange_complete, False)
                self.assertIn("request_incomplete", observed.limitations)
                result = assert_early_close_classification(self, observed, fixtures.expected_observations(recipe)[0])
                self.assertEqual(result["send_class"], "sent_incomplete")
                self.assertEqual(result["server_consumption_evidence"], "unavailable")

    def test_complete_send_with_valid_json_prefix_is_not_complete_response(self):
        recipe, bounds = fixtures.body_recipe(65537, early_close=True), policy()
        step = next(step for step in recipe.steps if step.kind == "probe")
        raw = wire.request_bytes(request_for(recipe, step), recipe.port, bounds.wire_limits)
        request, value = supplied_transcript(recipe, bounds, sent=raw,
            received=b"HTTP/1.1 413 Fixture early close\r\nContent-Length: 9\r\n\r\n{}",
            termination="eof", socket_errno=None)
        observed = wire.decode_probe_output(raw_checks.encoded(value), request, recipe.port, bounds.wire_limits)
        self.assertIs(observed.sent_complete, True)
        self.assertIs(observed.response.body_complete, False)
        self.assertIs(observed.exchange_complete, False)
        self.assertEqual(json.loads(observed.response.body), {})
        result = assert_early_close_classification(self, observed, fixtures.expected_observations(recipe)[0])
        self.assertEqual(result["send_class"], "sent_complete")
        self.assertEqual(result["server_consumption_evidence"], "unavailable")

    def test_response_cannot_turn_incomplete_send_into_complete_exchange(self):
        recipe, bounds = fixtures.body_recipe(65537, early_close=True), policy()
        step = next(step for step in recipe.steps if step.kind == "probe")
        raw = wire.request_bytes(request_for(recipe, step), recipe.port, bounds.wire_limits)
        request, value = supplied_transcript(recipe, bounds, sent=raw[:-1],
            received=b"HTTP/1.1 413 Fixture early close\r\nContent-Length: 2\r\n\r\n{}",
            termination="message_complete", socket_errno=None)
        with self.assertRaises(wire.WireError):
            wire.decode_probe_output(raw_checks.encoded(value), request, recipe.port, bounds.wire_limits)
