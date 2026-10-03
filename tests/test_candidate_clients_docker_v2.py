"""Prospective physical C01/C02 v2 qualification; never candidate imports on host.

The root owner executes these only after the source/definition freeze. Fixture
source generators return bytes; generated modules execute solely in containers.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import unittest
import uuid

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_cli_cases_v1 as cases
from gossip_harness import candidate_client_observer_v2 as observer
from gossip_harness import candidate_client_execution_v2 as execution
from gossip_harness import candidate_client_process_v2 as transport
from gossip_harness.gitstore import GitStore, _run
from gossip_harness.library_project_fixture_v1 import RUNTIME_IMAGE
from gossip_harness.library_v2_json_reference_v2 import (
    corrected_v2_binary_files,
    corrected_v2_files,
    corrected_v2_source_inputs,
)

ROSTER_FAMILIES = {
    "persistence": ("configuration",),
    "legacy": ("empty", "legacy", "changed-source", "selection-rejection", "missing-show"),
    "jobs": ("intake-success", "job-census", "action-matrix", "failed-retry", "prepare-import-conflict", "epoch-fence"),
    "rejection": ("intake-io", "namespace", "invalid-kind", "grammar", "pagination-query", "syntax", "utf8"),
}
BINARY_STDOUT = bytes(range(256)) * 64 + b"\x02\0\0\0\0\0\0\x04FAKE\0stdout-end"
BINARY_STDERR = bytes(reversed(range(256))) * 64 + b"\x01\0\0\0\0\0\0\x04FAKE\0stderr-end"


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def sha256(value):
    return hashlib.sha256(value).hexdigest()


def success_expectation(value):
    return {"kind": "success", "exit_code": 0, "value": value, "error_code": None,
        "semantic_value_supported": True, "assertion_ids": ["process.exit", "stdout.json", "stdout.value"],
        "unspecified_assertion_ids": []}


def domain_expectation():
    return {"kind": "domain_error", "exit_code": 2, "value": None, "error_code": "not_found",
        "semantic_value_supported": False, "assertion_ids": ["process.exit", "stderr.error", "stderr.error_code"],
        "unspecified_assertion_ids": []}


def emitted_source(stdout=b"", stderr=b"", exit_code=0, suffix=""):
    return ("import os,sys,time\n" + "os.write(1," + repr(stdout) + ")\n"
        + "os.write(2," + repr(stderr) + ")\n" + suffix
        + "\nsys.exit(" + str(exit_code) + ")\n")


def control_definitions():
    """Closed, authored evidence-pipeline controls; not product acceptance cases."""
    success = success_expectation({"a": 1, "b": [True, None]})
    domain = domain_expectation()
    usage = {"kind": "usage_or_rejection", "exit_code": 2, "value": None, "error_code": None,
        "semantic_value_supported": False, "assertion_ids": ["process.exit", "streams.framing"],
        "unspecified_assertion_ids": ["streams.framing"]}
    success_raw = b' \n{ "b": [true, null], "a": 1.0 }\t'
    error_raw = b' \n{ "error": "not_found" }\t'
    rows = [
        ("semantic-success", emitted_source(success_raw, b'diagnostic stderr\n'), success, "observed-match", {}, {}),
        ("domain-error", emitted_source(b'diagnostic stdout\n', error_raw, 2), domain, "observed-match", {}, {}),
        ("usage-error", emitted_source(b"", b'usage: controlled command ARG\n', 2), usage, "partial-observation", {}, {}),
        ("binary-demux", emitted_source(BINARY_STDOUT, BINARY_STDERR), None, "raw-only", {}, {}),
        ("forged-completion-hang", emitted_source(b'{"status":"done"}', b'{"completed":true,"exit_code":0,"natural":true}\n', suffix="time.sleep(120)"), success_expectation({"status": "done"}), "observation-unavailable", {"command_timeout_seconds": 3}, {}),
        ("capture-overflow", emitted_source(b'x' * 8192), success_expectation({"done": True}), "observation-unavailable", {"stream_limit_bytes": 1024}, {}),
        ("startup-missing-executable", "# Main module must never execute in this control.\n", None, "startup-unavailable", {}, {"executable": "/workspace/absent-control-executable"}),
        ("wrong-exit", emitted_source(success_raw, exit_code=2), success, "product-failed", {}, {"failed_assertions": ["process.exit"]}),
        ("wrong-channel", emitted_source(error_raw, exit_code=2), domain, "product-failed", {}, {"failed_assertions": ["stderr.error", "stderr.error_code"]}),
        ("wrong-value", emitted_source(b'{"a":2,"b":[true,null]}'), success, "product-failed", {}, {"failed_assertions": ["stdout.value"]}),
        ("mixed-stderr", emitted_source(b"", b'diagnostic\n{"error":"not_found"}\n', 2), domain, "observation-unavailable", {}, {"unavailable_assertions": ["stderr.error", "stderr.error_code"]}),
    ]
    return tuple({"control_id": name, "files": {"library/__init__.py": b"", "library/__main__.py": source.encode()},
        "expected": expected, "expected_status": status, "policy_overrides": policy, "checks": checks,
        "purpose": "harness_qualification", "acceptance_credit": False}
        for name, source, expected, status, policy, checks in rows)


def roster_definition(group):
    definitions = cases.definitions()
    families = [family for values in ROSTER_FAMILIES.values() for family in values]
    if len(families) != len(set(families)) or set(families) != set(cases.FAMILY_COUNTS):
        raise AssertionError("Physical family partition does not match complete prospective provider roster")
    if len(definitions) != 57 or len({case["case_id"] for case in definitions}) != 57:
        raise AssertionError("Finite case roster changed without a prospective qualification amendment")
    selected = tuple(case for case in definitions if case["family_id"] in ROSTER_FAMILIES[group])
    if not selected:
        raise AssertionError("Empty physical roster partition")
    return selected


def assertion_roster(case):
    """Expected exact keys and dispositions come only from authored definitions."""
    return {step["step_id"] + "." + key: "unspecified" if key in case["expectations"][step["step_id"]]["unspecified_assertion_ids"] else "supported"
        for step in case["recipe"]["steps"] for key in case["expectations"][step["step_id"]]["assertion_ids"]}


def evaluate_steps(test, case, observations):
    steps = case["recipe"]["steps"]
    expected_ids = [step["step_id"] for step in steps]
    test.assertFalse(set(observations) - set(expected_ids), "Unexpected observed step")
    records = {step_id: observer.observe_cli_step(case["expectations"][step_id], observations.get(step_id))
        for step_id in expected_ids}
    assertions = {step_id + "." + key: value for step_id, row in records.items() for key, value in row["assertions"].items()}
    dispositions = {step_id + "." + key: value for step_id, row in records.items() for key, value in row["assertion_dispositions"].items()}
    expected = assertion_roster(case)
    test.assertEqual(set(assertions), set(expected), "Missing/extra assertion cannot qualify")
    test.assertEqual(set(dispositions), set(expected))
    test.assertTrue(assertions)
    test.assertTrue(any(value == "supported" for value in expected.values()))
    for key, disposition in expected.items():
        if disposition == "unspecified":
            test.assertIsNone(assertions[key])
            test.assertEqual(dispositions[key], "unspecified")
        else:
            test.assertIn(dispositions[key], ("supported", "unavailable"))
    failed = [key for key, value in assertions.items() if value is False]
    unavailable = [key for key, value in assertions.items() if value is None and expected[key] == "supported"]
    unspecified = [key for key in expected if expected[key] == "unspecified"]
    status = ("product-failed" if failed else "observation-unavailable" if unavailable
              else "partial-observation" if unspecified else "observed-match")
    return {"case_id": case["case_id"], "status": status, "assertions": assertions,
        "assertion_dispositions": dispositions, "failed_assertion_ids": failed,
        "unavailable_assertion_ids": unavailable, "unspecified_assertion_ids": unspecified,
        "assertion_census_sha256": sha256(encoded(expected)), "steps": records,
        "supported_assertions_passed": bool(assertions) and all(value is True for key, value in assertions.items() if expected[key] == "supported"),
        "all_local_assertions_passed": bool(assertions) and all(value is True for value in assertions.values()),
        "qualification_verified": False, "acceptance_credit": False,
        "production_acceptance_authority": False}


def make_store(path, files):
    """Commit inert bytes to an isolated Git store without importing them."""
    texts = {}
    for name, raw in files.items():
        try:
            texts[name] = raw.decode("utf-8")
        except UnicodeError:
            pass
    store = GitStore.create(path, texts)
    if len(texts) != len(files):
        with store._checkout(store.head()) as checkout:
            for name, raw in files.items():
                target = checkout / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
            _run(checkout, "add", "--all")
            _run(checkout, "commit", "-m", "Retain complete qualification binary fixture")
            oid = _run(checkout, "rev-parse", "HEAD").stdout.decode().strip()
            store._git("fetch", "--no-tags", str(checkout), oid + ":refs/heads/accepted")
    return store


def write_new(path, value):
    with path.open("xb") as stream:
        stream.write(encoded(value))


def lifecycle_census(root, observations):
    """Counts remain distinct: creation is not a successful candidate start."""
    containers, started, natural, forced, startup, unknown = [], [], [], [], [], []
    for observation in observations:
        record = json.loads(observation.transport_json)
        binding = json.loads(observation.binding_json)
        completion = record["completion"]
        item = {"step_id": binding["step_id"], "container_id": completion.get("container_id"),
            "status": record["status"], "exit_code": record["exit_code"]}
        containers.append(item)
        if completion.get("started") is True:
            started.append(item)
        if completion.get("natural") is True:
            natural.append(item)
        if completion.get("killed_by_controller") is True:
            forced.append(item)
        if record["status"] == "start_error":
            startup.append(item)
        if record["status"] != "completed":
            unknown.append(item)
    # A creation can precede a transport exception and lack an observation.
    created_records = []
    for path in sorted(root.glob("step-*-create.json")):
        record = json.loads(path.read_bytes())
        if execution.CandidateClientExecution._clean(record):
            descriptor = record["stdout"]
            raw = (root / descriptor["path"]).read_bytes()
            if sha256(raw) != descriptor["sha256"] or len(raw) != descriptor["bytes"]:
                raise AssertionError("Creation record raw identity differs")
            candidate_id = raw.strip().decode("ascii")
            if len(candidate_id) == 64 and all(c in "0123456789abcdef" for c in candidate_id):
                created_records.append({"record": path.name, "container_id": candidate_id})
    terminal = json.loads((root / "terminal.json").read_bytes())
    return {"trusted_volume_holder": terminal.get("keeper_identity"),
        "trusted_volume_holder_cleanup_verified": terminal.get("keeper_cleanup"),"created_container_records": created_records, "observed_container_results": containers,
        "successfully_started_processes": started, "naturally_completed_processes": natural,
        "controller_kill_requested": forced, "startup_unavailable": startup,
        "incomplete_process_observations": unknown,
        "note": "A controller kill request is retained separately from proof of a killed process."}


def retained_command_json(test, root, label):
    record = json.loads((root / (label + ".json")).read_bytes())
    test.assertTrue(execution.CandidateClientExecution._clean(record))
    descriptor = record["stdout"]
    raw = (root / descriptor["path"]).read_bytes()
    test.assertEqual(len(raw), descriptor["bytes"])
    test.assertEqual(sha256(raw), descriptor["sha256"])
    return json.loads(raw)


def retained_engine_json(test, root, label):
    """Parse already-retained Engine HTTP bytes; never opens a network socket."""
    raw = (root / (label + "-response.bin")).read_bytes()
    class RetainedBytes(io.BytesIO):
        def close(self):
            pass  # In-memory evidence remains readable after HTTPResponse closes its file view.
    stream = RetainedBytes(raw)
    class RetainedSocket:
        def makefile(self, *args, **kwargs):
            return stream
    response = http.client.HTTPResponse(RetainedSocket())
    response.begin()
    test.assertEqual(response.status, 200)
    body = response.read(transport.CONTROL_LIMIT + 1)
    test.assertLessEqual(len(body), transport.CONTROL_LIMIT)
    test.assertEqual(stream.tell(), len(raw))
    value = json.loads(body)
    test.assertIs(type(value), dict)
    return value


def assert_startup_comparison(test, comparison, before, after, runtime, phase):
    """Recompute the frozen compatibility policy without mutating raw evidence."""
    test.assertIs(type(comparison), dict)
    test.assertEqual(comparison["policy_id"], transport.STARTUP_POLICY_ID)
    test.assertEqual(comparison["policy_sha256"], transport.startup_policy_sha256())
    test.assertEqual(comparison["phase"], phase)
    test.assertEqual(comparison["runtime_sha256"], sha256(encoded(runtime)))
    without_digest = {key: value for key, value in comparison.items() if key != "comparison_sha256"}
    test.assertEqual(comparison["comparison_sha256"], sha256(encoded(without_digest)))
    reproduced = transport.startup_identity_comparison(before, after, runtime, phase=phase)
    test.assertEqual(encoded(comparison), encoded(reproduced))
    test.assertIs(comparison["matches"], True)
    test.assertEqual(comparison["reasons"], [])
    test.assertEqual(comparison["comparison_before_sha256"], comparison["comparison_after_sha256"])
    test.assertLessEqual(len(comparison["transformations"]), 1)
    for change in comparison["transformations"]:
        test.assertEqual(set(change), {"path", "before", "after", "comparison_after", "phase", "runtime_sha256"})
        test.assertEqual(change["path"], "HostConfig.OomKillDisable")
        test.assertIs(change["before"], False)
        test.assertIsNone(change["after"])
        test.assertIs(change["comparison_after"], False)
        test.assertEqual(change["phase"], phase)
        test.assertEqual(change["runtime_sha256"], comparison["runtime_sha256"])
    return comparison


def assert_startup_policy_binding(test, root):
    config = json.loads((root / "config.json").read_bytes())
    policy = config["startup_compatibility_policy"]
    test.assertEqual(policy["policy_id"], transport.STARTUP_POLICY_ID)
    test.assertEqual(policy["policy_sha256"], transport.startup_policy_sha256())
    test.assertEqual(encoded(policy["definition"]), encoded(transport.startup_policy()))
    test.assertEqual(config["protocol"], execution.PROTOCOL)
    return config["runtime"]


def assert_process_startup_record(test, root, label, record, runtime, *, created_label=None):
    test.assertEqual(record["protocol"], transport.PROTOCOL)
    intent = json.loads((root / (label + "-intent.json")).read_bytes())
    test.assertEqual(intent["protocol"], transport.PROTOCOL)
    test.assertEqual(encoded(intent["expected_runtime"]), encoded(runtime))
    test.assertEqual(intent["startup_policy_sha256"], transport.startup_policy_sha256())
    test.assertEqual(encoded(intent["startup_policy"]), encoded(transport.startup_policy()))
    before = retained_command_json(test, root, created_label or label + "-created")
    test.assertEqual(intent["container_id"], before["Id"])
    test.assertEqual(intent["expected_inspection_sha256"], sha256(encoded(transport.immutable_inspection(before))))
    if created_label is None:
        controller_intent = json.loads((root / (label + "-controller-intent.json")).read_bytes())
        test.assertEqual(controller_intent["binding"]["protocol"], execution.PROTOCOL)
        test.assertEqual(controller_intent["argv"], before["Config"]["Entrypoint"] + before["Config"]["Cmd"])
        test.assertNotEqual(encoded(controller_intent), encoded(intent))
    comparison = record["startup_comparison"]
    if comparison is None:
        test.assertIs(record["completion"]["natural"], False)
        test.assertNotEqual(record["status"], "completed")
        return None
    retained = json.loads((root / (label + "-startup-comparison.json")).read_bytes())
    test.assertEqual(encoded(retained), encoded(comparison))
    after = retained_engine_json(test, root, label + "-inspect-final")
    return assert_startup_comparison(test, comparison, transport.immutable_inspection(before),
        transport.immutable_inspection(after), runtime, "candidate-created-to-exited")


def assert_keeper_boundaries(test, root, observations, lifecycle):
    keeper = lifecycle["trusted_volume_holder"]
    boundaries = []
    if not observations:
        return boundaries
    test.assertIs(type(keeper), dict)
    runtime = assert_startup_policy_binding(test, root)
    before_raw = retained_command_json(test, root, "keeper-created")
    after_raw = retained_command_json(test, root, "keeper-running")
    projection_keys = set(keeper) - {"StartedAt"}
    before = {key: before_raw[key] for key in projection_keys}
    after = {key: after_raw[key] for key in projection_keys}
    comparison = json.loads((root / "keeper-startup-comparison.json").read_bytes())
    assert_startup_comparison(test, comparison, before, after, runtime, "keeper-created-to-running")
    test.assertEqual(execution.encoded(after), execution.encoded({key: keeper[key] for key in projection_keys}))
    lifecycle["keeper_startup_comparison"] = comparison
    test.assertEqual(keeper["RestartCount"], 0)
    test.assertEqual(len(keeper["Mounts"]), 1)
    test.assertEqual(keeper["Mounts"][0]["Destination"], "/tmp")
    test.assertIs(keeper["Mounts"][0]["RW"], False)
    lifecycle["candidate_startup_comparisons"] = []
    for index, observation in enumerate(observations):
        binding = json.loads(observation.binding_json)
        label = "step-" + str(index).zfill(3)
        record = json.loads(observation.transport_json)
        process_comparison = assert_process_startup_record(test, root, label, record, runtime)
        lifecycle["candidate_startup_comparisons"].append({"step_id": binding["step_id"],
            "comparison": process_comparison})
        for phase in ("before", "after"):
            name = "step-" + str(index).zfill(3) + "-keeper-" + phase
            path = root / (name + ".json")
            record = json.loads(path.read_bytes())
            test.assertTrue(execution.CandidateClientExecution._clean(record))
            descriptor = record["stdout"]
            raw = (root / descriptor["path"]).read_bytes()
            test.assertEqual(len(raw), descriptor["bytes"])
            test.assertEqual(sha256(raw), descriptor["sha256"])
            inspected = json.loads(raw)
            projected = {key: inspected[key] for key in keeper if key != "StartedAt"}
            projected["StartedAt"] = inspected["State"]["StartedAt"]
            test.assertEqual(execution.encoded(projected), execution.encoded(keeper))
            test.assertIs(inspected["State"]["Running"], True)
            test.assertIs(inspected["State"]["Restarting"], False)
            test.assertIs(inspected["State"]["OOMKilled"], False)
            boundaries.append({"step_id": binding["step_id"], "phase": phase,
                "inspection_sha256": descriptor["sha256"], "keeper_id": keeper["Id"],
                "started_at": keeper["StartedAt"]})
    return boundaries


def observations_by_step(test, observations):
    result = {}
    for observation in observations:
        binding = json.loads(observation.binding_json)
        test.assertNotIn(binding["step_id"], result)
        result[binding["step_id"]] = observation
    return result


def qualification_setup(cls, label, *, reference):
    cls.artifacts = ArtifactDirectory(label, retain_success=True)
    cls.addClassCleanup(cls.artifacts.close)
    cls.endpoint = transport.EngineEndpoint.from_environment()
    cls.runtime = execution.runtime_identity(cls.endpoint, RUNTIME_IMAGE)
    cls.requirements_sha256 = cases.NORMATIVE_SHA256["library-cumulative-product-v2.json"]
    cls.policy = execution.ClientPolicy(RUNTIME_IMAGE)
    if reference:
        cls.reference_sources = corrected_v2_source_inputs()
        cls.files = {name: text.encode("utf-8") for name, text in corrected_v2_files().items()} | corrected_v2_binary_files()
        cls.store = make_store(cls.artifacts.root / "reference.git", cls.files)
        write_new(cls.artifacts.root / "reference-source.json", {"sources": cls.reference_sources,
            "files": {name: sha256(raw) for name, raw in cls.files.items()},
            "source_sha256": execution.source_sha256(cls.files), "purpose": "harness_qualification"})


def run_history(test, root, store, files, case_id, policy):
    commit = store.head()
    tree, captured = execution.capture_git_source(store, commit)
    test.assertEqual(captured, files)
    binding = execution.binding_for(files, case_id, policy, test.runtime,
        requirements_sha256=test.requirements_sha256)
    registration = execution.ClientRegistration(binding, commit, tree, "physical-qualification-v2-1")
    root.mkdir()
    write_new(root / "prospective-registration.json", asdict(registration))
    checkpoints = []
    def checkpoint_sink(checkpoint):
        index = len(checkpoints)
        write_new(root / ("external-checkpoint-" + str(index).zfill(4) + ".json"), asdict(checkpoint))
        checkpoints.append(checkpoint)
    with execution.CandidateClientExecution(root / "execution", store, registration, policy,
            endpoint=test.endpoint, checkpoint_sink=checkpoint_sink) as controller:
        result = controller.execute_once()
        test.assertEqual(result.checkpoint, controller.checkpoint())
    test.assertTrue(checkpoints, "No independently retained dispatch checkpoint")
    test.assertEqual(result.case_id, case_id)
    observations = observations_by_step(test, result.observations)
    for observation in result.observations:
        observed_binding = json.loads(observation.binding_json)
        test.assertEqual(observed_binding["protocol"], execution.PROTOCOL)
        test.assertEqual(json.loads(observation.transport_json)["protocol"], transport.PROTOCOL)
        test.assertEqual(observed_binding["commit_oid"], commit)
        test.assertEqual(observed_binding["tree_oid"], tree)
        test.assertEqual(observed_binding["source_sha256"], binding.source_sha256)
        test.assertEqual(observed_binding["purpose"], "harness_qualification")
        test.assertEqual(observed_binding["definition_sha256"], binding.definition_sha256)
        test.assertEqual(observed_binding["ordered_suite_sha256"], binding.ordered_suite_sha256)
    terminal_path = root / "execution" / "terminal.json"
    test.assertEqual(sha256(terminal_path.read_bytes()), result.terminal_sha256)
    lifecycle = lifecycle_census(root / "execution", result.observations)
    write_new(root / "physical-lifecycle.json", lifecycle)
    return result, observations, lifecycle


def assert_independent_processes(test, case, observations, lifecycle):
    expected_ids = [step["step_id"] for step in case["recipe"]["steps"]]
    test.assertEqual(list(observations), expected_ids)
    containers = [json.loads(observations[sid].transport_json)["completion"]["container_id"] for sid in expected_ids]
    test.assertEqual(len(set(containers)), len(expected_ids), "Each invocation requires its own real container")
    test.assertTrue(all(type(cid) is str and len(cid) == 64 for cid in containers))
    test.assertEqual(len(lifecycle["created_container_records"]), len(expected_ids))
    keeper = lifecycle["trusted_volume_holder"]
    test.assertIs(type(keeper), dict)
    test.assertTrue(keeper["Id"])
    test.assertNotIn(keeper["Id"], containers)
    test.assertIs(lifecycle["trusted_volume_holder_cleanup_verified"], True)
    test.assertEqual(len(lifecycle["successfully_started_processes"]), len(expected_ids))
    test.assertEqual(len(lifecycle["naturally_completed_processes"]), len(expected_ids))
    test.assertFalse(lifecycle["controller_kill_requested"])
    for step in case["recipe"]["steps"]:
        binding = json.loads(observations[step["step_id"]].binding_json)
        test.assertEqual(binding["argv"], step["argv"])
        test.assertEqual(binding["ordered_step_ids"], expected_ids)
        test.assertIs(json.loads(observations[step["step_id"]].transport_json)["history_state_verified"], True)
    if case["case_id"] == "cli-db-root-isolation":
        dbs = {step["argv"][step["argv"].index("--db") + 1] for step in case["recipe"]["steps"]}
        roots = {step["argv"][step["argv"].index("--root") + 1] for step in case["recipe"]["steps"]}
        test.assertEqual(dbs, {"/tmp/db-a.sqlite", "/tmp/db-b.sqlite"})
        test.assertEqual(roots, {"/inputs/root-a", "/inputs/root-b"})
        test.assertGreaterEqual(len(containers), 2)


def run_roster(test, group):
    roster = roster_definition(group)
    root = test.artifacts.root
    initial = [evaluate_steps(test, case, {}) | {"family_id": case["family_id"], "status": "not-run",
        "qualification_verified": False, "acceptance_credit": False,
        "missing_step_ids": [step["step_id"] for step in case["recipe"]["steps"]]} for case in roster]
    write_new(root / "prospective-roster.json", {"group": group,
        "provider_sha256": cases.definition_sha256(), "definition_sources": cases.definition_sources(),
        "full_ordered_case_ids": [case["case_id"] for case in cases.definitions()],
        "selected_case_ids": [case["case_id"] for case in roster],
        "expected_assertions": {case["case_id"]: assertion_roster(case) for case in roster},
        "planned_invocations": sum(len(case["recipe"]["steps"]) for case in roster), "outcomes": initial})
    outcomes = [dict(row) for row in initial]
    try:
        for index, case in enumerate(roster):
            with test.subTest(case_id=case["case_id"]):
                outcome = outcomes[index]
                try:
                    result, observations, lifecycle = run_history(test, root / case["case_id"], test.store,
                        test.files, case["case_id"], test.policy)
                    outcome.update(evaluate_steps(test, case, observations))
                    outcome.update({"history_transport_status": result.status,
                        "missing_step_ids": list(result.missing_step_ids), "cleanup_verified": result.cleanup_verified,
                        "infrastructure": list(result.infrastructure), "lifecycle": lifecycle,
                        "terminal_sha256": result.terminal_sha256})
                    lifecycle["keeper_boundaries"] = assert_keeper_boundaries(test,
                        root / case["case_id"] / "execution", result.observations, lifecycle)
                    write_new(root / case["case_id"] / "keeper-boundary-qualification.json", lifecycle["keeper_boundaries"])
                    test.assertEqual(result.status, "completed", outcome)
                    test.assertTrue(result.cleanup_verified, outcome)
                    test.assertFalse(result.infrastructure, outcome)
                    test.assertFalse(result.missing_step_ids, outcome)
                    assert_independent_processes(test, case, observations, lifecycle)
                    test.assertFalse(outcome["failed_assertion_ids"], outcome)
                    test.assertFalse(outcome["unavailable_assertion_ids"], outcome)
                    test.assertTrue(outcome["supported_assertions_passed"], outcome)
                    test.assertIs(outcome["all_local_assertions_passed"], not bool(outcome["unspecified_assertion_ids"]))
                    outcome["qualification_verified"] = True
                except BaseException as error:
                    if outcome["status"] == "not-run":
                        outcome["status"] = "infrastructure-failure"
                    outcome["qualification_error_kind"] = "qualification-or-infrastructure-failure"
                    outcome["qualification_verified"] = False
                    outcome["qualification_error"] = type(error).__name__ + ": " + str(error)[:1000]
                    raise
                finally:
                    write_new(root / ("case-" + str(index).zfill(4) + ".json"), outcome)
    finally:
        write_new(root / "completion-census.json", {"planned": [row["case_id"] for row in initial],
            "outcomes": outcomes, "not_run": [row["case_id"] for row in outcomes if row["status"] == "not-run"],
            "acceptance_credit": False, "full_requirement_verdict": None})
    test.assertEqual(corrected_v2_source_inputs(), test.reference_sources)


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateCliPersistenceV2DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls, "candidate-b03-v2-cli-persistence", reference=True)

    def test_independent_process_persistence_and_root_isolation(self):
        run_roster(self, "persistence")


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateCliLegacyV2DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls, "candidate-b03-v2-cli-legacy", reference=True)

    def test_complete_legacy_and_configuration_roster(self):
        run_roster(self, "legacy")


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateCliJobsV2DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls, "candidate-b03-v2-cli-jobs", reference=True)

    def test_complete_job_lifecycle_roster(self):
        run_roster(self, "jobs")


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateCliRejectionV2DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls, "candidate-b03-v2-cli-rejection", reference=True)

    def test_complete_rejection_and_usage_roster(self):
        run_roster(self, "rejection")


def control_manifest(row):
    return {key: value for key, value in row.items() if key != "files"} | {
        "qualification_protocol": "candidate-client-qualification-v2",
        "execution_protocol": execution.PROTOCOL, "transport_protocol": transport.PROTOCOL,
        "observer_protocol": observer.PROTOCOL, "provider_protocol": cases.PROTOCOL,
        "provider_definition_sha256": cases.definition_sha256(),
        "unchanged_control_data_from": "tests/test_candidate_clients_docker_v1.py@3ff8d94238f673a0af0660e7ddff196dceec4130",
        "source_files": {name: sha256(raw) for name, raw in row["files"].items()},
        "source_sha256": execution.source_sha256(row["files"]),
        "invocation_recipe": "cli-grammar-missing-command" if row["control_id"] != "startup-missing-executable" else None,
        "actual_argv": cases.execution_recipe("cli-grammar-missing-command")["steps"][0]["argv"] if row["control_id"] != "startup-missing-executable" else [row["checks"]["executable"]],
        "registered_case_expected": cases.case_definition("cli-grammar-missing-command")["expectations"] if row["control_id"] != "startup-missing-executable" else None,
        "scope": "Synthetic evidence-pipeline control; registered provider verdict retained separately from authored control verdict."}


def startup_transport_control(test, row, root):
    """One explicitly raw transport attempt, outside the closed product recipe."""
    root.mkdir()
    store = make_store(root / "source.git", row["files"])
    tree, files = execution.capture_git_source(store, store.head())
    binding = execution.binding_for(files, "cli-grammar-missing-command", test.policy, test.runtime,
        requirements_sha256=test.requirements_sha256)
    reg = execution.ClientRegistration(binding, store.head(), tree, "raw-startup-transport-control-v2")
    workspace, inputs = root / "workspace", root / "inputs"
    workspace.mkdir()
    inputs.mkdir()
    (inputs / "root-a").mkdir()
    for name, raw in files.items():
        path = workspace / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        path.chmod(0o444)
    execution_id = uuid.uuid4().hex
    name, volume = "gossip-startup-v2-" + execution_id, "gossip-startup-volume-v2-" + execution_id
    intent = {"execution_id": execution_id, "volume": volume}
    step = {"step_id": "raw-startup", "argv": [row["checks"]["executable"]]}
    write_new(root / "prospective-raw-transport.json", {"control": control_manifest(row),
        "registration": asdict(reg), "actual_step": step, "execution_id": execution_id,
        "container_name": name, "volume": volume, "volume_options": execution.VOLUME_OPTIONS,
        "provider_recipe_executed": False, "product_case_verdict": None})
    claimed_volume = claimed_container = False
    created_volume = created_container = False
    container_id = None
    record = None
    cleanup = {"container": False, "volume": False}
    cleanup_errors = []
    with execution.CandidateClientExecution(root / "execution", store, reg, test.policy, endpoint=test.endpoint) as controller:
        try:
            before = controller._checked("raw-volume-before", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"])
            test.assertFalse(controller._raw(before).strip())
            command = ["docker", "volume", "create", "--driver", "local", "--label", "gossip.execution=" + execution_id,
                "--label", "gossip.snapshot=" + execution.SNAPSHOT_PROTOCOL]
            for key, value in execution.VOLUME_OPTIONS.items():
                command.extend(("--opt", key + "=" + value))
            claimed_volume = True
            made = controller._checked("raw-volume-create", command + [volume])
            test.assertEqual(controller._raw(made).strip(), volume.encode())
            created_volume = True
            inspected = controller._checked("raw-volume-created", ["docker", "volume", "inspect", "--format", "{{json .}}", volume])
            test.assertTrue(controller._volume_valid(json.loads(controller._raw(inspected)), intent))
            before = controller._checked("raw-container-before", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"])
            test.assertFalse(controller._raw(before).strip())
            claimed_container = True
            made = controller._checked("raw-container-create", controller._create_argv(name, workspace, inputs, volume, step, execution_id))
            container_id = controller._raw(made).strip().decode("ascii")
            test.assertEqual(len(container_id), 64)
            created_container = True
            inspected = controller._checked("raw-container-created", ["docker", "inspect", "--format", "{{json .}}", container_id])
            expected = json.loads(controller._raw(inspected))
            controller._validate_created(expected, container_id=container_id, name=name,
                workspace=workspace, inputs=inputs, intent=intent, step=step)
            # This independent checkpoint precedes the single start request.
            write_new(root / "external-before-start-checkpoint.json", asdict(controller.checkpoint()))
            record = transport.run_process(test.endpoint, container_id=container_id, expected=expected,
                policy=test.policy.process_policy(), retain=controller._retain, label="raw-startup", expected_runtime=test.runtime)
            controller._retain("raw-startup-result.json", execution.encoded(record))
            inspected = controller._checked("raw-container-after-start", ["docker", "inspect", "--format", "{{json .}}", container_id])
            after = json.loads(controller._raw(inspected))
            test.assertEqual(after["Id"], container_id)
            assert_process_startup_record(test, root / "execution", "raw-startup", record,
                assert_startup_policy_binding(test, root / "execution"), created_label="raw-container-created")
            test.assertEqual(record["status"], "start_error", record)
            test.assertIs(record["completion"]["started"], False)
            test.assertIs(record["completion"]["natural"], False)
            test.assertIsNone(record["exit_code"])
            test.assertEqual(controller._raw(record, "stdout"), b"")
            test.assertEqual(controller._raw(record, "stderr"), b"")
        finally:
            if claimed_container:
                try:
                    cleanup["container"] = controller._remove_container("raw-container", name, intent, step, container_id)
                except Exception as error:
                    cleanup_errors.append("container: " + type(error).__name__ + ": " + str(error)[:500])
            if claimed_volume and (not claimed_container or cleanup["container"]):
                try:
                    owned = controller._command("raw-volume-cleanup-inspect", ["docker", "volume", "inspect", "--format", "{{json .}}", volume])
                    if controller._clean(owned):
                        test.assertTrue(controller._volume_valid(json.loads(controller._raw(owned)), intent))
                        removed = controller._command("raw-volume-remove", ["docker", "volume", "rm", volume])
                        removal_ok = controller._clean(removed)
                    else:
                        removal_ok = True  # Exact-name absence must still be independently established below.
                    absent = controller._command("raw-volume-after", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"])
                    cleanup["volume"] = removal_ok and controller._clean(absent) and not controller._raw(absent).strip()
                except Exception as error:
                    cleanup_errors.append("volume: " + type(error).__name__ + ": " + str(error)[:500])
            write_new(root / "raw-transport-terminal.json", {"record": record, "cleanup": cleanup,
                "cleanup_errors": cleanup_errors, "created_volume": created_volume,
                "created_container_id": container_id, "created_container": created_container,
                "claimed_container": claimed_container, "claimed_volume": claimed_volume,
                "successfully_started_candidate_processes": int(record is not None and record["completion"]["started"] is True),
                "naturally_completed_candidate_processes": int(record is not None and record["completion"]["natural"] is True),
                "trusted_volume_holders": 0, "acceptance_credit": False})
    test.assertTrue(cleanup["container"])
    test.assertTrue(cleanup["volume"])
    return {"status": "startup-unavailable-control-observed", "transport": record,
        "created_container_id": container_id, "successfully_started_processes": 0,
        "naturally_completed_processes": 0, "cleanup": cleanup}


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateClientProcessControlV2DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls, "candidate-b03-v2-process-controls", reference=False)
        cls.controls = {row["control_id"]: row for row in control_definitions()}
        cls.outcomes = {name: {"control_id": name, "status": "not-run", "qualification_verified": False,
            "expected_assertions": [] if row["expected"] is None else row["expected"]["assertion_ids"],
            "observation": None if row["expected"] is None else observer.observe_cli_step(row["expected"], None),
            "registered_case_observation": None if name == "startup-missing-executable" else observer.observe_cli_step(
                cases.case_definition("cli-grammar-missing-command")["expectations"]["s01"], None),
            "acceptance_credit": False} for name, row in cls.controls.items()}
        write_new(cls.artifacts.root / "prospective-controls.json", {"controls": [control_manifest(row) for row in cls.controls.values()],
            "initial_outcomes": list(cls.outcomes.values()), "purpose": "harness_qualification"})
        cls.addClassCleanup(cls.save_census)

    @classmethod
    def save_census(cls):
        write_new(cls.artifacts.root / "completion-census.json", {"planned": list(cls.controls),
            "outcomes": list(cls.outcomes.values()), "not_run": [name for name, row in cls.outcomes.items() if row["status"] == "not-run"],
            "acceptance_credit": False, "full_requirement_verdict": None})

    def run_control(self, name):
        row = self.controls[name]
        outcome = self.outcomes[name]
        root = self.artifacts.root / name
        try:
            if name == "startup-missing-executable":
                outcome.update(startup_transport_control(self, row, root))
            else:
                store = make_store(self.artifacts.root / (name + ".git"), row["files"])
                policy = execution.ClientPolicy(RUNTIME_IMAGE, **row["policy_overrides"])
                result, observations, lifecycle = run_history(self, root, store, row["files"],
                    "cli-grammar-missing-command", policy)
                self.assertFalse(result.missing_step_ids)
                self.assertEqual(len(observations), 1)
                observation = observations["s01"]
                process = json.loads(observation.transport_json)
                outcome.update({"transport_status": result.status, "process_status": process["status"],
                    "terminal_sha256": result.terminal_sha256, "lifecycle": lifecycle,
                    "infrastructure": list(result.infrastructure), "cleanup_verified": result.cleanup_verified})
                registered_local = observer.observe_cli_step(cases.case_definition("cli-grammar-missing-command")["expectations"]["s01"], observation)
                write_new(root / "registered-case-observation.json", registered_local)
                outcome["registered_case_observation"] = registered_local
                outcome["registered_case_id"] = "cli-grammar-missing-command"
                if process["completion"]["natural"] and process["exit_code"] == 0:
                    self.assertEqual(registered_local["status"], "product-failed")
                    self.assertIs(registered_local["assertions"]["process.exit"], False)
                if row["expected"] is not None:
                    local = observer.observe_cli_step(row["expected"], observation)
                    write_new(root / "control-observation.json", local)
                    outcome["observation"] = local
                    self.assertEqual(set(local["assertions"]), set(row["expected"]["assertion_ids"]))
                    self.assertEqual(local["status"], row["expected_status"], local)
                    for key in row["expected"]["unspecified_assertion_ids"]:
                        self.assertIsNone(local["assertions"][key])
                        self.assertEqual(local["assertion_dispositions"][key], "unspecified")
                    if row["expected_status"] in ("observed-match", "partial-observation"):
                        self.assertTrue(local["supported_assertions_passed"], local)
                        self.assertFalse(local["failed_assertion_ids"])
                        self.assertFalse(local["unavailable_assertion_ids"])
                    if "failed_assertions" in row["checks"]:
                        self.assertEqual(set(local["failed_assertion_ids"]), set(row["checks"]["failed_assertions"]))
                        self.assertFalse(local["unavailable_assertion_ids"])
                    if "unavailable_assertions" in row["checks"]:
                        self.assertEqual(set(local["unavailable_assertion_ids"]), set(row["checks"]["unavailable_assertions"]))
                        self.assertFalse(local["failed_assertion_ids"])
                else:
                    self.assertEqual(observation.stdout, BINARY_STDOUT)
                    self.assertEqual(observation.stderr, BINARY_STDERR)
                    self.assertEqual(process["stdout"]["sha256"], sha256(BINARY_STDOUT))
                    self.assertEqual(process["stderr"]["sha256"], sha256(BINARY_STDERR))
                lifecycle["keeper_boundaries"] = assert_keeper_boundaries(self, root / "execution", result.observations, lifecycle)
                write_new(root / "keeper-boundary-qualification.json", lifecycle["keeper_boundaries"])
                self.assertTrue(result.cleanup_verified, outcome)
                self.assertIs(type(lifecycle["trusted_volume_holder"]), dict)
                self.assertIs(lifecycle["trusted_volume_holder_cleanup_verified"], True)
                self.assertEqual(len(lifecycle["created_container_records"]), 1)
                self.assertIs(process["history_state_verified"], True)
                if name == "forged-completion-hang":
                    self.assertEqual(process["status"], "timeout")
                    self.assertIs(process["completion"]["started"], True)
                    self.assertIs(process["completion"]["natural"], False)
                    self.assertIsNone(process["exit_code"])
                    self.assertTrue(process["completion"]["killed_by_controller"])
                    self.assertIn(b'"completed":true', observation.stderr)
                    self.assertFalse(outcome["observation"]["failed_assertion_ids"])
                elif name == "capture-overflow":
                    self.assertEqual(process["status"], "output_limit")
                    self.assertTrue(process["stdout"]["truncated"])
                    self.assertGreater(process["stdout"]["observed_bytes"], policy.stream_limit_bytes)
                    self.assertLessEqual(len(observation.stdout), policy.stream_limit_bytes)
                    self.assertFalse(outcome["observation"]["failed_assertion_ids"])
                else:
                    self.assertEqual(result.status, "completed", outcome)
                    self.assertEqual(process["status"], "completed")
                    self.assertTrue(process["completion"]["started"])
                    self.assertTrue(process["completion"]["natural"])
                    self.assertFalse(process["completion"]["killed_by_controller"])
                    self.assertFalse(result.infrastructure)
                outcome["status"] = "control-observed"
            outcome["qualification_verified"] = True
        except BaseException as error:
            outcome.update({"status": "control-qualification-failure", "qualification_verified": False,
                "qualification_error": type(error).__name__ + ": " + str(error)[:1000]})
            raise
        finally:
            write_new(self.artifacts.root / ("outcome-" + name + ".json"), outcome)

    def test_actual_exit_zero_two_usage_and_opposite_diagnostics(self):
        for name in ("semantic-success", "domain-error", "usage-error"):
            with self.subTest(control=name):
                self.run_control(name)

    def test_binary_raw_stream_demultiplexing(self):
        self.run_control("binary-demux")

    def test_forged_completion_and_hang_remain_unknown(self):
        self.run_control("forged-completion-hang")

    def test_capture_overflow_remains_observation_limit(self):
        self.run_control("capture-overflow")

    def test_startup_failure_is_not_candidate_exit(self):
        self.run_control("startup-missing-executable")

    def test_completed_wrong_exit_channel_and_value_are_distinguished(self):
        for name in ("wrong-exit", "wrong-channel", "wrong-value"):
            with self.subTest(control=name):
                self.run_control(name)

    def test_mixed_stderr_framing_remains_unqualified(self):
        self.run_control("mixed-stderr")
