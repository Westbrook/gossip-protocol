"""Prospective physical C01/C02 v4 qualification; never candidate imports on host.

The root owner executes these only after the source/definition freeze. Fixture
source generators return bytes; generated modules execute solely in containers.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import re
import json
import os
from pathlib import Path
import unittest
import uuid

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_cli_cases_v1 as cases
from gossip_harness import candidate_client_observer_v4 as observer
from gossip_harness import candidate_client_execution_v4 as execution
from gossip_harness import candidate_client_process_v4 as transport
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


class QualificationError(ValueError):
    """Incomplete or inconsistent qualification evidence; never a product verdict."""


def qrequire(condition, message):
    if not condition:
        raise QualificationError(message)


def parse_retained_http(raw, *, eof_observed):
    """Independent finite parser; file EOF never substitutes for socket EOF."""
    qrequire(type(raw) is bytes and len(raw) <= 2392064, "HTTP wire bound/type")
    qrequire(eof_observed is True, "Socket EOF completion unavailable")
    offset = 0
    def line(limit):
        nonlocal offset
        end = raw.find(b"\r\n", offset)
        qrequire(end >= offset and end + 2 - offset <= limit, "Incomplete/excessive HTTP line")
        result = raw[offset:end]
        offset = end + 2
        return result
    first = line(32768)
    match = re.fullmatch(rb"HTTP/1\.[01] ([0-9]{3}) [\x20-\x7e\x80-\xff]*", first)
    qrequire(match is not None, "Invalid status line")
    status = int(match[1])
    headers = {}
    for _ in range(128):
        item = line(32768 - offset)
        if not item:
            break
        qrequire(b":" in item and item[:1] not in (b" ", b"\t"), "Malformed header")
        key, value = item.split(b":", 1)
        qrequire(re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+", key) is not None, "Invalid header name")
        name = key.decode("ascii").lower()
        qrequire(name not in headers and all((c >= 32 and c != 127) or c == 9 for c in value), "Duplicate/control header")
        headers[name] = value.decode("latin1").strip(" \t")
    else:
        raise QualificationError("Header count exceeded")
    length, transfer = headers.get("content-length"), headers.get("transfer-encoding")
    qrequire(length is None or transfer is None, "Ambiguous CL/TE framing")
    if status in (204, 304):
        qrequire(transfer is None and length in (None, "0"), "Invalid no-body response")
        body, framing = b"", "no-body"
    elif transfer is not None:
        qrequire(transfer.lower() == "chunked", "Unsupported transfer encoding")
        body = bytearray()
        for _ in range(65536):
            chunk = line(128)
            qrequire(re.fullmatch(rb"[0-9a-fA-F]{1,16}", chunk) is not None, "Invalid chunk size")
            size = int(chunk, 16)
            if size == 0:
                qrequire(line(2) == b"", "Trailers unsupported")
                break
            qrequire(len(body) + size <= 1048576, "Body bound exceeded")
            qrequire(offset + size + 2 <= len(raw), "Truncated chunk")
            body.extend(raw[offset:offset + size])
            offset += size
            qrequire(raw[offset:offset + 2] == b"\r\n", "Invalid chunk terminator")
            offset += 2
        else:
            raise QualificationError("Chunk count exceeded")
        body, framing = bytes(body), "chunked"
    elif length is not None:
        qrequire(re.fullmatch(r"[0-9]{1,12}", length) is not None, "Invalid content length")
        size = int(length)
        qrequire(size <= 1048576 and offset + size <= len(raw), "Truncated/excessive body")
        body = raw[offset:offset + size]
        offset += size
        framing = "content-length"
    else:
        body, framing = raw[offset:], "connection-close"
        offset = len(raw)
    qrequire(offset == len(raw) and len(body) <= 1048576, "Trailing/excessive HTTP bytes")
    return {"status": status, "headers": headers, "body": body, "framing": framing}


REJECTION_PHASE = "candidate-created-to-start-rejected"
REJECTION_CONTINUITY_PHASE = "candidate-start-rejected-continuity"
INVALID_COMMAND_FRAGMENTS = ("executable file not found", "no such file or directory",
    "system cannot find the file specified", "failed to run runc create/exec call")


def rejection_policy():
    return {"policy_id": "docker-rejected-start-diagnostic-identity-v1",
        "protocol": "candidate-client-qualification-v4", "purpose": "harness_qualification",
        "plan_sha256": transport.EMPTY_COMMAND_PLAN_SHA256,
        "phases": [REJECTION_PHASE, REJECTION_CONTINUITY_PHASE],
        "mount_policy": transport.identity_policy_sha256(),
        "oom_startup_subrule": transport.startup_policy_sha256(),
        "command_policy": transport.command_policy_sha256(),
        "classifier": {"source_commit": "6bc6209b88a7a834c91f77d848e025c79e0227a1",
            "source": "daemon/errors.go", "source_sha256": "3c80f755911575e182130b5794f703af4d4d35c6288eece17d6807fee534846f",
            "lowercase_contains_any": list(INVALID_COMMAND_FRAGMENTS)},
        "rejection_status": 400, "daemon_bookkeeping_exit": 127,
        "oom_transition": {"phase": REJECTION_PHASE, "before": False, "after": None, "comparison_after": False},
        "continuity_oom": "raw-exact", "product_acceptance_authority": False}


def raw_descriptor(name, raw):
    return {"path": name, "bytes": len(raw), "sha256": sha256(raw)}


def _runtime_rejection_gate(runtime):
    qrequire(type(runtime) is dict, "Runtime unavailable")
    pinned = {"protocol": transport.PROTOCOL, "api_version": "1.47", "engine_version": "29.2.1",
        "os": "linux", "cgroup_version": "2", "oom_kill_disable_supported": False}
    qrequire(all(k in runtime and encoded(runtime[k]) == encoded(v) for k, v in pinned.items()), "Unqualified runtime")
    qrequire(runtime.get("engine_git_commit") in ("6bc6209", "6bc6209b88a7a834c91f77d848e025c79e0227a1"), "Engine commit differs")
    endpoint = runtime.get("endpoint")
    qrequire(type(endpoint) is dict and set(endpoint) == {"socket_path", "device", "inode"}
        and type(endpoint["socket_path"]) is str and endpoint["socket_path"].startswith("/")
        and type(endpoint["device"]) is int and type(endpoint["inode"]) is int, "Endpoint identity unavailable")
    for key in ("daemon_id", "image_id", "image_inspect_sha256", "architecture", "kernel_version", "cgroup_driver"):
        qrequire(type(runtime.get(key)) is str and bool(runtime[key]), "Runtime identity incomplete: " + key)


def _no_start_state(value, baseline, message):
    state = value.get("State")
    qrequire(type(state) is dict and type(baseline.get("State")) is dict, "No-start State missing")
    zero = baseline["State"].get("StartedAt")
    qrequire(type(zero) is str and re.fullmatch(r"0001-01-01T00:00:00(?:\.0+)?Z", zero) is not None, "Baseline has started")
    required = {"Status": "created", "Running": False, "Paused": False, "Restarting": False,
        "Dead": False, "OOMKilled": False, "Pid": 0, "ExitCode": 127, "StartedAt": zero, "Error": message}
    qrequire(all(k in state and encoded(state[k]) == encoded(v) for k, v in required.items()), "No-start State differs")
    qrequire(type(value.get("RestartCount")) is int and value["RestartCount"] == 0, "Restarted container")


def validate_start_rejection(bundle):
    """Reconstruct rejection from authenticated raw evidence, not passed flags."""
    try:
        return _validate_start_rejection(bundle)
    except QualificationError:
        raise
    except (AssertionError, ValueError, KeyError, TypeError, IndexError, transport.ProcessError) as error:
        raise QualificationError(str(error)[:500]) from error


def _validate_start_rejection(bundle):
    qrequire(type(bundle) is dict and set(bundle) == {"artifacts", "checkpoints", "runtime", "expected_argv", "container_id", "labels"}, "Incomplete proof bundle")
    artifacts, checkpoints, runtime = bundle["artifacts"], bundle["checkpoints"], bundle["runtime"]
    qrequire(type(artifacts) is dict and type(checkpoints) is dict, "Missing raw/checkpoint inventory")
    _runtime_rejection_gate(runtime)
    argv, identity, labels = bundle["expected_argv"], bundle["container_id"], bundle["labels"]
    qrequire(argv == ["/workspace/absent-control-executable"] and type(argv) is list, "Wrong raw control argv")
    qrequire(type(identity) is str and re.fullmatch(r"[0-9a-f]{64}", identity) is not None, "Full owned ID missing")
    sources = {}
    def raw(name):
        qrequire(type(name) is str and re.fullmatch(r"[a-z][a-z0-9.-]{0,180}", name) is not None, "Unsafe artifact name")
        value, checkpoint = artifacts[name], checkpoints[name]
        qrequire(type(value) is bytes and type(checkpoint) is dict and type(checkpoint["index"]) is int
            and checkpoint["index"] >= 0 and checkpoint["sha256"] == sha256(value), "Artifact/checkpoint mismatch: " + name)
        sources[name] = raw_descriptor(name, value)
        return value
    def owned(name):
        return strict_inspection_json(raw(name))
    def described(descriptor, name):
        value = raw(name)
        qrequire(type(descriptor) is dict and all(k in descriptor and encoded(descriptor[k]) == encoded(v) for k, v in raw_descriptor(name, value).items()), "Descriptor mismatch: " + name)
        return value
    def command(name):
        record = owned(name + ".json")
        qrequire(execution.CandidateClientExecution._clean(record), "Incomplete CLI control record: " + name)
        return strict_inspection_json(described(record["stdout"], name + "-stdout.bin"))
    def engine(name):
        message = parse_retained_http(raw(name + "-response.bin"), eof_observed=True)
        qrequire(message["status"] == 200, "Cleanup/prestart inspection HTTP failed")
        request = ("GET /v1.47/containers/" + identity + "/json HTTP/1.1\r\nHost: docker\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").encode()
        qrequire(raw(name + "-request.bin") == request, "Wrong inspection target")
        return strict_inspection_json(message["body"])
    config, intent = owned("config.json"), owned("raw-startup-intent.json")
    qrequire(encoded(config["runtime"]) == encoded(runtime) == encoded(intent["expected_runtime"]), "Runtime binding mismatch")
    qrequire(intent["protocol"] == transport.PROTOCOL and intent["container_id"] == identity, "Process intent identity differs")
    qrequire(encoded(intent["expected_argv"]) == encoded(argv) and intent["expected_argv_sha256"] == sha256(encoded(argv)), "Declared argv binding differs")
    qrequire(intent["start_response_policy_sha256"] == transport.start_response_policy_sha256()
        and encoded(intent["start_response_policy"]) == encoded(transport.start_response_policy()), "Response policy differs")
    before, prestart = command("raw-container-created"), engine("raw-startup-inspect-before")
    testcase = unittest.TestCase()
    precompare = owned("raw-startup-prestart-comparison.json")
    assert_identity_comparison(testcase, precompare, inspection_projection(testcase, before, "candidate"),
        inspection_projection(testcase, prestart, "candidate"), runtime, "candidate-created-to-prestart", before, prestart)
    qrequire(intent["expected_full_inspection_sha256"] == sha256(encoded(before)), "Created inspection binding differs")
    process = owned("raw-startup-process.json")
    qrequire(encoded(process) == encoded(owned("raw-startup-result.json")), "Final transport result differs")
    completion = process["completion"]
    qrequire(process["protocol"] == transport.PROTOCOL and process["status"] == "start_error"
        and process["exit_code"] is None and completion["started"] is False and completion["natural"] is False
        and process["startup_comparison"] is None and encoded(process["prestart_comparison"]) == encoded(precompare), "Wrong rejection disposition")
    qrequire(not any(name.startswith(("raw-startup-wait-", "raw-startup-inspect-final-"))
        or name == "raw-startup-startup-comparison.json" for name in artifacts), "Invented natural-completion evidence")
    response_proof = process["start_response"]
    qrequire(type(response_proof) is dict, "Durable start-response proof unavailable")
    receipt_raw = described(process["start_response_receipt"], "raw-startup-start-response-completion.json")
    qrequire(encoded(strict_inspection_json(receipt_raw)) == encoded(response_proof), "Completion receipt differs")
    request = described(response_proof["request"], "raw-startup-start-request.bin")
    target = "/v1.47/containers/" + identity + "/start"
    qrequire(request == ("POST " + target + " HTTP/1.1\r\nHost: docker\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").encode(), "Wrong start request")
    wire = described(response_proof["response"], "raw-startup-start-response.bin")
    qrequire(response_proof["protocol"] == transport.START_RESPONSE_PROTOCOL and response_proof["method"] == "POST"
        and response_proof["request_path"] == target and response_proof["container_id"] == identity
        and response_proof["endpoint_sha256"] == sha256(encoded(runtime["endpoint"]))
        and response_proof["runtime_sha256"] == sha256(encoded(runtime))
        and response_proof["framing_complete"] is True and response_proof["eof_observed"] is True
        and response_proof["durably_retained"] is True, "Completion receipt binding incomplete")
    response = parse_retained_http(wire, eof_observed=response_proof["eof_observed"])
    qrequire(type(response_proof["status"]) is int and response_proof["status"] == response["status"] == 400
        and type(response_proof["body_bytes"]) is int and response_proof["body_bytes"] == len(response["body"])
        and response_proof["body_sha256"] == sha256(response["body"]), "Not the qualified complete HTTP400 rejection")
    error_body = strict_inspection_json(response["body"])
    qrequire(set(error_body) == {"message"} and type(error_body["message"]) is str and bool(error_body["message"]), "ErrorResponse shape differs")
    message = error_body["message"]
    qrequire(any(fragment in message.lower() for fragment in INVALID_COMMAND_FRAGMENTS), "Not pinned invalid-command classification")
    after = engine("raw-startup-inspect-after-kill")
    qrequire(encoded(after) == encoded(owned("raw-startup-cleanup-state.json")), "Cleanup State differs from raw Engine body")
    for view in (before, prestart, after):
        qrequire(view["Id"] == identity and encoded(view["Config"]["Labels"]) == encoded(labels), "Source/execution identity differs")
        transport.validate_sandbox(view, transport.ProcessPolicy(runtime["image_id"]), expected_argv=argv, runtime=runtime)
    qrequire(prestart["State"]["Status"] == "created" and before["State"]["Status"] == "created", "Not fresh created state")
    for view in (after, command("raw-container-after-start"), command("raw-container-cleanup-inspect")):
        qrequire(view["Id"] == identity and encoded(view["Config"]["Labels"]) == encoded(labels), "Cleanup identity substituted")
        transport.validate_sandbox(view, transport.ProcessPolicy(runtime["image_id"]), expected_argv=argv, runtime=runtime)
        _no_start_state(view, before, message)
    for channel in ("stdout", "stderr"):
        qrequire(described(process[channel], "raw-startup-" + channel + ".bin") == b"", "Engine response leaked to candidate stream")
    chain = ["raw-container-create.json", "raw-container-created.json", "raw-startup-intent.json",
        "raw-startup-inspect-before-response.bin", "raw-startup-prestart-comparison.json", "raw-startup-attach-request.bin",
        "raw-startup-start-request.bin", "raw-startup-start-response.bin", "raw-startup-start-response-completion.json",
        "raw-startup-kill-request.bin", "raw-startup-inspect-after-kill-response.bin", "raw-startup-cleanup-state.json",
        "raw-startup-process.json", "raw-startup-result.json", "raw-container-after-start.json", "raw-container-cleanup-inspect.json"]
    for name in chain:
        raw(name)
    indexes = [checkpoints[name]["index"] for name in chain]
    qrequire(all(a < b for a, b in zip(indexes, indexes[1:])), "Durable chronology differs")
    attach_target = "/v1.47/containers/" + identity + "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0"
    qrequire(raw("raw-startup-attach-request.bin") == ("POST " + attach_target + " HTTP/1.1\r\nHost: docker\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Length: 0\r\n\r\n").encode(), "Wrong attach target")
    qrequire(raw("raw-startup-kill-request.bin") == ("POST /v1.47/containers/" + identity + "/kill?signal=SIGKILL HTTP/1.1\r\nHost: docker\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").encode(), "Wrong defensive kill target")
    start_requests = [name for name, value in artifacts.items() if name.endswith("-request.bin")
        and type(value) is bytes and value.startswith(b"POST ") and b"/start HTTP/" in value]
    qrequire(start_requests == ["raw-startup-start-request.bin"], "Duplicate/alternate start dispatch")
    qrequire(set(sources) <= set(process["evidence"]) | {"config.json", "raw-container-created.json",
        "raw-container-created-stdout.bin", "raw-container-create.json", "raw-startup-process.json", "raw-startup-result.json",
        "raw-container-after-start.json", "raw-container-after-start-stdout.bin", "raw-container-cleanup-inspect.json",
        "raw-container-cleanup-inspect-stdout.bin"}, "Transport evidence roster missing proof input")
    proof = {"protocol": "candidate-start-rejection-proof-v1", "purpose": "harness_qualification",
        "policy_sha256": sha256(encoded(rejection_policy())), "runtime_sha256": sha256(encoded(runtime)),
        "container_id": identity, "expected_argv": argv, "labels": labels, "raw_sources": sources,
        "response": {"status": 400, "message": message, "body_sha256": sha256(response["body"])},
        "before_full_sha256": sha256(encoded(before)), "after_full_sha256": sha256(encoded(after)),
        "durable_order": {"records": chain, "first_checkpoint_indices": indexes},
        "successful_candidate_starts": 0, "natural_candidate_completions": 0, "candidate_exit_code": None,
        "daemon_bookkeeping_exit_code": 127, "product_acceptance_authority": False}
    proof["proof_sha256"] = sha256(encoded(proof))
    return proof


def rejected_start_identity_comparison(before_full, after_full, *, runtime, rejection_proof, phase=REJECTION_PHASE):
    """Qualification-only comparison; never passed into the product observer."""
    policy = rejection_policy()
    result = {"protocol": "candidate-rejected-start-comparison-v1", "phase": phase,
        "policy_id": policy["policy_id"], "policy": policy, "policy_sha256": sha256(encoded(policy)),
        "runtime": runtime, "runtime_sha256": sha256(encoded(runtime)), "matches": False,
        "product_acceptance_authority": False}
    try:
        proof = validate_start_rejection(rejection_proof)
        qrequire(encoded(runtime) == encoded(rejection_proof["runtime"]), "Proof runtime differs")
        qrequire(phase in (REJECTION_PHASE, REJECTION_CONTINUITY_PHASE), "Unapproved diagnostic phase")
        if phase == REJECTION_PHASE:
            qrequire(sha256(encoded(before_full)) == proof["before_full_sha256"] and sha256(encoded(after_full)) == proof["after_full_sha256"], "Rejected-start full view substitution")
        else:
            qrequire(sha256(encoded(before_full)) == proof["after_full_sha256"], "Continuity baseline substituted")
            allowed = []
            for label in ("raw-container-after-start", "raw-container-cleanup-inspect"):
                command = strict_inspection_json(rejection_proof["artifacts"][label + ".json"])
                allowed.append(strict_inspection_json(rejection_proof["artifacts"][command["stdout"]["path"]]))
            qrequire(any(encoded(after_full) == encoded(view) for view in allowed), "Continuity view not authenticated")
        testcase = unittest.TestCase()
        before = inspection_projection(testcase, before_full, "candidate")
        after = inspection_projection(testcase, after_full, "candidate")
        before_map, before_mounts = mount_inventory(testcase, before, "candidate")
        after_map, after_mounts = mount_inventory(testcase, after, "candidate")
        left, right = json.loads(encoded(before)), json.loads(encoded(after))
        left["Mounts"], right["Mounts"] = before_map, after_map
        transformations = []
        if before_mounts["order"] != after_mounts["order"]:
            transformations.append({"path": "Mounts", "rule_id": transport.IDENTITY_POLICY_ID,
                "comparison_format": "destination-keyed-map-of-complete-rows", "before_order": before_mounts["order"],
                "after_order": after_mounts["order"], "phase": phase, "runtime_sha256": result["runtime_sha256"]})
        old, new = before["HostConfig"]["OomKillDisable"], after["HostConfig"]["OomKillDisable"]
        qrequire((old is False or old is None) and (new is False or new is None), "Invalid OOM raw type")
        if phase == REJECTION_PHASE and old is False and new is None:
            right["HostConfig"]["OomKillDisable"] = False
            transformations.append({"path": "HostConfig.OomKillDisable", "before": False, "after": None,
                "comparison_after": False, "phase": phase, "runtime_sha256": result["runtime_sha256"]})
        else:
            qrequire(old is new, "Unapproved OOM transition")
        qrequire(encoded(left) == encoded(right), "Unapproved immutable field/type/order change")
        proof_raw = encoded(proof)
        result.update({"rejection_proof": raw_descriptor("raw-startup-rejection-proof.json", proof_raw),
            "raw_sources": proof["raw_sources"], "before_full_inspection_sha256": sha256(encoded(before_full)),
            "after_full_inspection_sha256": sha256(encoded(after_full)), "before_sha256": sha256(encoded(before)),
            "after_sha256": sha256(encoded(after)), "mounts_before": before_mounts, "mounts_after": after_mounts,
            "comparison_before_sha256": sha256(encoded(left)), "comparison_after_sha256": sha256(encoded(right)),
            "transformations": transformations, "transformations_sha256": sha256(encoded(transformations)), "matches": True, "error": None})
    except (AssertionError, ValueError, KeyError, TypeError, transport.ProcessError) as error:
        result["error"] = type(error).__name__ + ": " + str(error)[:500]
    result["comparison_sha256"] = sha256(encoded(result))
    return result


def strict_inspection_json(raw):
    qrequire(type(raw) is bytes and len(raw) <= 1048576, "Inspection byte bound/type")
    def pairs(items):
        value = {}
        for key, item in items:
            qrequire(key not in value, "Duplicate inspection key")
            value[key] = item
        return value
    def constant(value):
        raise QualificationError("Nonfinite JSON token: " + value)
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
        qrequire(type(value) is dict, "Inspection must be an object")
        stack, count = [(value, 0)], 0
        while stack:
            item, depth = stack.pop()
            count += 1
            qrequire(count <= 100000 and depth <= 32, "Inspection structure bound")
            if type(item) is dict:
                stack.extend((child, depth + 1) for child in item.values())
            elif type(item) is list:
                stack.extend((child, depth + 1) for child in item)
        encoded(value)  # Reject overflowing floats even though Python's decoder accepts them.
        return value
    except (ValueError, UnicodeError, RecursionError, OverflowError) as error:
        raise QualificationError(str(error)[:500]) from error


def retained_command_json(test, root, label):
    record = json.loads((root / (label + ".json")).read_bytes())
    test.assertTrue(execution.CandidateClientExecution._clean(record))
    descriptor = record["stdout"]
    raw = (root / descriptor["path"]).read_bytes()
    test.assertEqual(len(raw), descriptor["bytes"])
    test.assertEqual(sha256(raw), descriptor["sha256"])
    return strict_inspection_json(raw)


def retained_engine_json(test, root, label):
    """Parse retained Engine bytes without opening a socket."""
    response = parse_retained_http((root / (label + "-response.bin")).read_bytes(), eof_observed=True)
    test.assertEqual(response["status"], 200)
    return strict_inspection_json(response["body"])


def mount_inventory(test, projection, role):
    rows = projection["Mounts"]
    test.assertIs(type(rows), list)
    expected = {"/tmp"} if role == "keeper" else {"/workspace", "/inputs", "/tmp"}
    test.assertEqual(len(rows), len(expected))
    inventory = {}
    order = []
    for row in rows:
        test.assertIs(type(row), dict)
        destination = row["Destination"]
        test.assertIs(type(destination), str)
        test.assertIn(destination, expected)
        test.assertNotIn(destination, inventory, "Duplicate Destination must not be overwritten")
        order.append(destination)
        inventory[destination] = row  # Keep every original field and nested value.
    test.assertEqual(set(inventory), expected)
    return inventory, {"order": order,
        "row_sha256": {destination: sha256(encoded(row)) for destination, row in inventory.items()},
        "inventory_sha256": sha256(encoded(inventory))}


def checkpoint_first_occurrences(test, root):
    first = {}
    for path in sorted(root.parent.glob("external-checkpoint-*.json")):
        index = int(path.stem.rsplit("-", 1)[1])
        pairs = json.loads(path.read_bytes())["files"]
        test.assertIs(type(pairs), list)
        test.assertEqual(len(pairs), len({row[0] for row in pairs}))
        for name, digest in pairs:
            if name not in first:
                first[name] = {"index": index, "sha256": digest}
            else:
                test.assertEqual(first[name]["sha256"], digest)
    return first


def assert_prestart_durable_order(test, root, label, checkpoint_records, *, raw=False):
    names = ["raw-container-create.json"] if raw else [label + "-controller-intent.json", label + "-create.json"]
    names += [label + suffix for suffix in ("-intent.json", "-inspect-before-response.bin",
        "-prestart-comparison.json", "-attach-request.bin", "-start-request.bin")]
    for name in names:
        test.assertIn(name, checkpoint_records)
        test.assertEqual(sha256((root / name).read_bytes()), checkpoint_records[name]["sha256"])
    indexes = [checkpoint_records[name]["index"] for name in names]
    test.assertTrue(all(left < right for left, right in zip(indexes, indexes[1:])), indexes)
    return {"records": names, "first_checkpoint_indices": indexes}


def inspection_projection(test, raw, role, *, running=False):
    keys = {"Id", "Image", "Name", "Path", "Args", "Config", "HostConfig", "Mounts"}
    keys.add("Created" if role == "candidate" else "RestartCount")
    test.assertTrue(keys <= set(raw))
    result = {key: raw[key] for key in keys}
    if role == "keeper" and running:
        result["StartedAt"] = raw["State"]["StartedAt"]
    return result


def assert_identity_comparison(test, comparison, before, after, runtime, phase, before_full, after_full):
    """Reconstruct full inventories without trusting a precomputed matches flag."""
    test.assertIs(type(comparison), dict)
    role = "keeper" if phase.startswith("keeper-") else "candidate"
    test.assertEqual(comparison["role"], role)
    test.assertEqual(comparison["policy_id"], transport.IDENTITY_POLICY_ID)
    test.assertEqual(comparison["policy_sha256"], transport.identity_policy_sha256())
    test.assertEqual(comparison["startup_policy_id"], transport.STARTUP_POLICY_ID)
    test.assertEqual(comparison["startup_policy_sha256"], transport.startup_policy_sha256())
    test.assertEqual(comparison["phase"], phase)
    runtime_digest = sha256(encoded(runtime))
    test.assertEqual(comparison["runtime_sha256"], runtime_digest)
    for side, projection, full in (("before", before, before_full), ("after", after, after_full)):
        test.assertEqual(comparison[side + "_sha256"], sha256(encoded(projection)))
        test.assertEqual(comparison[side + "_full_inspection_sha256"], sha256(encoded(full)))
        for key, value in projection.items():
            source = full["State"][key] if key == "StartedAt" else full[key]
            test.assertEqual(encoded(value), encoded(source))
    before_inventory, before_metadata = mount_inventory(test, before, role)
    after_inventory, after_metadata = mount_inventory(test, after, role)
    test.assertEqual(encoded(comparison["mounts_before"]), encoded(before_metadata))
    test.assertEqual(encoded(comparison["mounts_after"]), encoded(after_metadata))
    test.assertEqual(encoded(before_inventory), encoded(after_inventory))
    # Only the top-level Mounts list changes representation in comparison copies.
    before_view, after_view = json.loads(encoded(before)), json.loads(encoded(after))
    before_view["Mounts"], after_view["Mounts"] = before_inventory, after_inventory
    transformations = []
    if before_metadata["order"] != after_metadata["order"]:
        transformations.append({"path": "Mounts", "rule_id": transport.IDENTITY_POLICY_ID,
            "comparison_format": "destination-keyed-map-of-complete-rows",
            "before_order": before_metadata["order"], "after_order": after_metadata["order"],
            "phase": phase, "runtime_sha256": runtime_digest})
    old, new = before["HostConfig"]["OomKillDisable"], after["HostConfig"]["OomKillDisable"]
    test.assertTrue(old is False or old is None)
    test.assertTrue(new is False or new is None)
    if old is False and new is None and phase in ("keeper-created-to-running", "candidate-created-to-exited"):
        after_view["HostConfig"]["OomKillDisable"] = False
        transformations.append({"path": "HostConfig.OomKillDisable", "before": False,
            "after": None, "comparison_after": False, "phase": phase, "runtime_sha256": runtime_digest})
    else:
        test.assertIs(old, new)
    test.assertEqual(encoded(comparison["transformations"]), encoded(transformations))
    test.assertEqual(encoded(before_view), encoded(after_view), "Other fields or nested ordering changed")
    test.assertEqual(comparison["comparison_before_sha256"], sha256(encoded(before_view)))
    test.assertEqual(comparison["comparison_after_sha256"], sha256(encoded(after_view)))
    without_digest = {key: value for key, value in comparison.items() if key != "comparison_sha256"}
    test.assertEqual(comparison["comparison_sha256"], sha256(encoded(without_digest)))
    reproduced = transport.identity_comparison(before, after, runtime, phase=phase,
        before_full_inspection=before_full, after_full_inspection=after_full)
    test.assertEqual(encoded(comparison), encoded(reproduced))
    test.assertIs(comparison["matches"], True)
    test.assertEqual(comparison["reasons"], [])
    return comparison


def assert_startup_policy_binding(test, root):
    config = json.loads((root / "config.json").read_bytes())
    policy = config["startup_compatibility_policy"]
    test.assertEqual(policy["policy_id"], transport.STARTUP_POLICY_ID)
    test.assertEqual(policy["policy_sha256"], transport.startup_policy_sha256())
    test.assertEqual(encoded(policy["definition"]), encoded(transport.startup_policy()))
    policy = config["identity_comparison_policy"]
    test.assertEqual(policy["policy_id"], transport.IDENTITY_POLICY_ID)
    test.assertEqual(policy["policy_sha256"], transport.identity_policy_sha256())
    test.assertEqual(encoded(policy["definition"]), encoded(transport.identity_policy()))
    command = config["command_validation_policy"]
    test.assertEqual(command["policy_id"], transport.COMMAND_POLICY_ID)
    test.assertEqual(command["policy_sha256"], transport.command_policy_sha256())
    test.assertEqual(encoded(command["definition"]), encoded(transport.command_policy()))
    response = config["start_response_policy"]
    test.assertEqual(response["protocol"], transport.START_RESPONSE_PROTOCOL)
    test.assertEqual(response["policy_sha256"], transport.start_response_policy_sha256())
    test.assertEqual(encoded(response["definition"]), encoded(transport.start_response_policy()))
    test.assertEqual(config["protocol"], execution.PROTOCOL)
    return config["runtime"]


def assert_process_startup_record(test, root, label, record, runtime, *, created_label=None, checkpoints=None):
    test.assertEqual(record["protocol"], transport.PROTOCOL)
    intent = json.loads((root / (label + "-intent.json")).read_bytes())
    test.assertEqual(intent["protocol"], transport.PROTOCOL)
    test.assertEqual(intent["command_policy_sha256"], transport.command_policy_sha256())
    test.assertEqual(encoded(intent["command_policy"]), encoded(transport.command_policy()))
    test.assertEqual(intent["start_response_policy_sha256"], transport.start_response_policy_sha256())
    test.assertEqual(encoded(intent["start_response_policy"]), encoded(transport.start_response_policy()))
    test.assertEqual(intent["expected_argv_sha256"], sha256(encoded(intent["expected_argv"])))
    test.assertEqual(encoded(intent["expected_runtime"]), encoded(runtime))
    test.assertEqual(intent["startup_policy_sha256"], transport.startup_policy_sha256())
    test.assertEqual(encoded(intent["startup_policy"]), encoded(transport.startup_policy()))
    test.assertEqual(intent["identity_policy_sha256"], transport.identity_policy_sha256())
    test.assertEqual(encoded(intent["identity_policy"]), encoded(transport.identity_policy()))
    before = retained_command_json(test, root, created_label or label + "-created")
    before_projection = inspection_projection(test, before, "candidate")
    test.assertEqual(intent["container_id"], before["Id"])
    test.assertEqual(intent["expected_inspection_sha256"], sha256(encoded(before_projection)))
    test.assertEqual(intent["expected_full_inspection_sha256"], sha256(encoded(before)))
    if created_label is None:
        controller_intent = json.loads((root / (label + "-controller-intent.json")).read_bytes())
        test.assertEqual(controller_intent["binding"]["protocol"], execution.PROTOCOL)
        test.assertEqual(controller_intent["argv"], before["Config"]["Entrypoint"] + before["Config"]["Cmd"])
        test.assertNotEqual(encoded(controller_intent), encoded(intent))
    prestart = retained_engine_json(test, root, label + "-inspect-before")
    prestart_record = json.loads((root / (label + "-prestart-comparison.json")).read_bytes())
    test.assertEqual(encoded(prestart_record), encoded(record["prestart_comparison"]))
    assert_identity_comparison(test, prestart_record, before_projection,
        inspection_projection(test, prestart, "candidate"), runtime, "candidate-created-to-prestart", before, prestart)
    test.assertEqual(prestart["State"]["Status"], "created")
    order = assert_prestart_durable_order(test, root, label,
        checkpoints if checkpoints is not None else checkpoint_first_occurrences(test, root), raw=created_label is not None)
    comparison = record["startup_comparison"]
    if comparison is None:
        test.assertIs(record["completion"]["natural"], False)
        test.assertNotEqual(record["status"], "completed")
        return {"prestart": prestart_record, "final": None, "durable_order": order}
    retained = json.loads((root / (label + "-startup-comparison.json")).read_bytes())
    test.assertEqual(encoded(retained), encoded(comparison))
    after = retained_engine_json(test, root, label + "-inspect-final")
    final = assert_identity_comparison(test, comparison, before_projection,
        inspection_projection(test, after, "candidate"), runtime, "candidate-created-to-exited", before, after)
    return {"prestart": prestart_record, "final": final, "durable_order": order}


def assert_keeper_boundaries(test, root, observations, lifecycle):
    keeper = lifecycle["trusted_volume_holder"]
    boundaries = []
    if not observations:
        return boundaries
    test.assertIs(type(keeper), dict)
    runtime = assert_startup_policy_binding(test, root)
    before_raw = retained_command_json(test, root, "keeper-created")
    after_raw = retained_command_json(test, root, "keeper-running")
    before = inspection_projection(test, before_raw, "keeper")
    after = inspection_projection(test, after_raw, "keeper")
    comparison = json.loads((root / "keeper-startup-comparison.json").read_bytes())
    assert_identity_comparison(test, comparison, before, after, runtime, "keeper-created-to-running", before_raw, after_raw)
    baseline = inspection_projection(test, after_raw, "keeper", running=True)
    test.assertEqual(encoded(baseline), encoded(keeper))
    lifecycle["keeper_startup_comparison"] = comparison
    test.assertEqual(keeper["RestartCount"], 0)
    test.assertEqual(len(keeper["Mounts"]), 1)
    test.assertEqual(keeper["Mounts"][0]["Destination"], "/tmp")
    test.assertIs(keeper["Mounts"][0]["RW"], False)
    checkpoints = checkpoint_first_occurrences(test, root)
    lifecycle["candidate_startup_comparisons"] = []
    for index, observation in enumerate(observations):
        binding = json.loads(observation.binding_json)
        label = "step-" + str(index).zfill(3)
        record = json.loads(observation.transport_json)
        process_comparison = assert_process_startup_record(test, root, label, record, runtime, checkpoints=checkpoints)
        lifecycle["candidate_startup_comparisons"].append({"step_id": binding["step_id"], "comparison": process_comparison})
        for phase in ("before", "after"):
            name = label + "-keeper-" + phase
            inspected = retained_command_json(test, root, name)
            projected = inspection_projection(test, inspected, "keeper", running=True)
            comparison = json.loads((root / (name + "-comparison.json")).read_bytes())
            assert_identity_comparison(test, comparison, baseline, projected, runtime,
                "keeper-running-to-running", after_raw, inspected)
            # A keeper has one row, so inventory comparison cannot hide any ordering change.
            test.assertEqual(encoded(projected), encoded(keeper))
            test.assertIs(inspected["State"]["Running"], True)
            test.assertIs(inspected["State"]["Restarting"], False)
            test.assertIs(inspected["State"]["OOMKilled"], False)
            boundaries.append({"step_id": binding["step_id"], "phase": phase,
                "full_inspection_sha256": sha256(encoded(inspected)), "comparison_sha256": comparison["comparison_sha256"],
                "keeper_id": keeper["Id"], "started_at": keeper["StartedAt"]})
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
    registration = execution.ClientRegistration(binding, commit, tree, "physical-qualification-v4-1")
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
class CandidateCliPersistenceV4DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls, "candidate-b03-v4-cli-persistence", reference=True)

    def test_independent_process_persistence_and_root_isolation(self):
        run_roster(self, "persistence")


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateCliLegacyV4DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls, "candidate-b03-v4-cli-legacy", reference=True)

    def test_complete_legacy_and_configuration_roster(self):
        run_roster(self, "legacy")


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateCliJobsV4DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls, "candidate-b03-v4-cli-jobs", reference=True)

    def test_complete_job_lifecycle_roster(self):
        run_roster(self, "jobs")


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateCliRejectionV4DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls, "candidate-b03-v4-cli-rejection", reference=True)

    def test_complete_rejection_and_usage_roster(self):
        run_roster(self, "rejection")


def control_manifest(row):
    return {key: value for key, value in row.items() if key != "files"} | {
        "qualification_protocol": "candidate-client-qualification-v4",
        "rejected_start_policy_sha256": sha256(encoded(rejection_policy())),
        "execution_protocol": execution.PROTOCOL, "transport_protocol": transport.PROTOCOL,
        "observer_protocol": observer.PROTOCOL, "provider_protocol": cases.PROTOCOL,
        "provider_definition_sha256": cases.definition_sha256(),
        "unchanged_control_data_from": "tests/test_candidate_clients_docker_v3.py@f2bdbf1",
        "source_files": {name: sha256(raw) for name, raw in row["files"].items()},
        "source_sha256": execution.source_sha256(row["files"]),
        "invocation_recipe": "cli-grammar-missing-command" if row["control_id"] != "startup-missing-executable" else None,
        "actual_argv": cases.execution_recipe("cli-grammar-missing-command")["steps"][0]["argv"] if row["control_id"] != "startup-missing-executable" else [row["checks"]["executable"]],
        "registered_case_expected": cases.case_definition("cli-grammar-missing-command")["expectations"] if row["control_id"] != "startup-missing-executable" else None,
        "scope": "Synthetic evidence-pipeline control; registered provider verdict retained separately from authored control verdict."}


def qualify_rejected_start(test, controller, root, record, *, runtime, step, intent, name, volume, container_id):
    """Read complete retained evidence after finally cleanup, then qualify once."""
    terminal = strict_inspection_json((root / "raw-transport-terminal.json").read_bytes())
    test.assertEqual(terminal["cleanup"], {"container": True, "volume": True})
    test.assertEqual(terminal["cleanup_errors"], [])
    test.assertEqual(encoded(terminal["record"]), encoded(record))
    test.assertEqual(terminal["created_container_id"], container_id)
    test.assertEqual(terminal["successfully_started_candidate_processes"], 0)
    test.assertEqual(terminal["naturally_completed_candidate_processes"], 0)
    execution_root = root / "execution"
    checkpoints = checkpoint_first_occurrences(test, execution_root)
    labels = {"gossip.execution": intent["execution_id"], "gossip.source": controller.binding.source_sha256,
        "gossip.fixture": controller.config["fixture_sha256"], "gossip.step": step["step_id"]}
    authenticated = dict(controller.checkpoint().files)
    artifacts = {path: (execution_root / path).read_bytes() for path in authenticated}
    for path, raw in artifacts.items():
        test.assertEqual(sha256(raw), authenticated[path])
    bundle = {"artifacts": artifacts, "checkpoints": checkpoints, "runtime": runtime,
        "expected_argv": step["argv"], "container_id": container_id, "labels": labels}
    proof = validate_start_rejection(bundle)
    controller._retain("raw-startup-rejection-proof.json", encoded(proof))
    before = retained_command_json(test, execution_root, "raw-container-created")
    after = retained_engine_json(test, execution_root, "raw-startup-inspect-after-kill")
    comparison = rejected_start_identity_comparison(before, after, runtime=runtime, rejection_proof=bundle)
    controller._retain("raw-startup-rejected-start-comparison.json", encoded(comparison))
    test.assertIs(comparison["matches"], True, comparison)
    continuity = []
    for label in ("raw-container-after-start", "raw-container-cleanup-inspect"):
        later = retained_command_json(test, execution_root, label)
        observed = rejected_start_identity_comparison(after, later, runtime=runtime,
            rejection_proof=bundle, phase=REJECTION_CONTINUITY_PHASE)
        controller._retain(label + "-continuity-comparison.json", encoded(observed))
        test.assertIs(observed["matches"], True, observed)
        continuity.append(observed)
    before_checkpoint_raw = (root / "external-before-start-checkpoint.json").read_bytes()
    before_checkpoint = strict_inspection_json(before_checkpoint_raw)
    before_files = dict(before_checkpoint["files"])
    test.assertEqual(len(before_files), len(before_checkpoint["files"]))
    test.assertIn("raw-container-created.json", before_files)
    test.assertNotIn("raw-startup-intent.json", before_files)
    for path, digest in before_files.items():
        test.assertEqual(authenticated[path], digest)
    # Authenticate each owned removal and exact-name absence independently.
    prefix = ["docker", "--host", "unix://" + runtime["endpoint"]["socket_path"]]
    removal = {"raw-container-remove": ["rm", "--force", container_id],
        "raw-container-absent": ["container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"],
        "raw-volume-remove": ["volume", "rm", volume],
        "raw-volume-after": ["volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"]}
    removal_receipts = {}
    for label, argv in removal.items():
        command = strict_inspection_json(artifacts[label + ".json"])
        test.assertEqual(command["argv"], prefix + argv)
        test.assertTrue(controller._clean(command))
        for channel in ("stdout", "stderr"):
            descriptor = command[channel]
            raw = artifacts[descriptor["path"]]
            test.assertEqual(descriptor["sha256"], sha256(raw))
            test.assertEqual(descriptor["bytes"], len(raw))
        if label.endswith(("-absent", "-after")):
            test.assertEqual(artifacts[command["stdout"]["path"]].strip(), b"")
        removal_receipts[label] = raw_descriptor(label + ".json", artifacts[label + ".json"])
    cleanup_chain = ["raw-startup-result.json", "raw-container-after-start.json",
        "raw-container-cleanup-inspect.json", "raw-container-remove.json", "raw-container-absent.json",
        "raw-volume-cleanup-inspect.json", "raw-volume-remove.json", "raw-volume-after.json"]
    cleanup_indices = []
    for path in cleanup_chain:
        test.assertEqual(checkpoints[path]["sha256"], authenticated[path])
        cleanup_indices.append(checkpoints[path]["index"])
    test.assertTrue(all(left < right for left, right in zip(cleanup_indices, cleanup_indices[1:])),
        "Owned cleanup records are not after final result/inspection in causal order")
    volume_view = retained_command_json(test, execution_root, "raw-volume-cleanup-inspect")
    test.assertTrue(controller._volume_valid(volume_view, intent))
    for channel in ("stdout", "stderr"):
        test.assertEqual(controller._raw(record, channel), b"")
    result = {"proof": proof, "rejected_start_comparison": comparison, "continuity": continuity,
        "before_start_checkpoint": raw_descriptor("external-before-start-checkpoint.json", before_checkpoint_raw),
        "owned_cleanup": removal_receipts,
        "cleanup_durable_order": {"records": cleanup_chain, "first_checkpoint_indices": cleanup_indices}, "terminal": raw_descriptor("raw-transport-terminal.json", (root / "raw-transport-terminal.json").read_bytes()),
        "purpose": "harness_qualification", "product_acceptance_authority": False}
    controller._retain("raw-startup-qualification.json", encoded(result))
    return result


def startup_transport_control(test, row, root):
    """One explicitly raw transport attempt, outside the closed product recipe."""
    root.mkdir()
    store = make_store(root / "source.git", row["files"])
    tree, files = execution.capture_git_source(store, store.head())
    binding = execution.binding_for(files, "cli-grammar-missing-command", test.policy, test.runtime,
        requirements_sha256=test.requirements_sha256)
    reg = execution.ClientRegistration(binding, store.head(), tree, "raw-startup-transport-control-v4")
    workspace, inputs = root / "workspace", root / "inputs"
    workspace.mkdir()
    inputs.mkdir()
    (inputs / "root-a").mkdir()
    for name, raw in files.items():
        path = workspace / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        path.chmod(0o444)
    test.assertFalse((workspace / "absent-control-executable").exists())
    execution_id = uuid.uuid4().hex
    name, volume = "gossip-startup-v4-" + execution_id, "gossip-startup-volume-v4-" + execution_id
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
    checkpoints = []
    def checkpoint_sink(checkpoint):
        write_new(root / ("external-checkpoint-" + str(len(checkpoints)).zfill(4) + ".json"), asdict(checkpoint))
        checkpoints.append(checkpoint)
    with execution.CandidateClientExecution(root / "execution", store, reg, test.policy,
            endpoint=test.endpoint, checkpoint_sink=checkpoint_sink) as controller:
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
            test.assertTrue(controller._volume_valid(strict_inspection_json(controller._raw(inspected)), intent))
            before = controller._checked("raw-container-before", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"])
            test.assertFalse(controller._raw(before).strip())
            claimed_container = True
            made = controller._checked("raw-container-create", controller._create_argv(name, workspace, inputs, volume, step, execution_id))
            container_id = controller._raw(made).strip().decode("ascii")
            test.assertEqual(len(container_id), 64)
            created_container = True
            inspected = controller._checked("raw-container-created", ["docker", "inspect", "--format", "{{json .}}", container_id])
            expected = strict_inspection_json(controller._raw(inspected))
            controller._validate_created(expected, container_id=container_id, name=name,
                workspace=workspace, inputs=inputs, intent=intent, step=step)
            # This independent checkpoint precedes the single start request.
            write_new(root / "external-before-start-checkpoint.json", asdict(controller.checkpoint()))
            record = transport.run_process(test.endpoint, container_id=container_id, expected=expected,
                policy=test.policy.process_policy(), retain=controller._retain, label="raw-startup", expected_runtime=test.runtime, expected_argv=step["argv"])
            controller._retain("raw-startup-result.json", execution.encoded(record))
            inspected = controller._checked("raw-container-after-start", ["docker", "inspect", "--format", "{{json .}}", container_id])
            after = strict_inspection_json(controller._raw(inspected))
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
                        test.assertTrue(controller._volume_valid(strict_inspection_json(controller._raw(owned)), intent))
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
        qualification = qualify_rejected_start(test, controller, root, record, runtime=test.runtime,
            step=step, intent=intent, name=name, volume=volume, container_id=container_id)
    test.assertTrue(cleanup["container"])
    test.assertTrue(cleanup["volume"])
    return {"status": "startup-unavailable-control-observed", "transport": record,
        "created_container_id": container_id, "successfully_started_processes": 0,
        "naturally_completed_processes": 0, "cleanup": cleanup, "qualification": qualification}


@unittest.skipUnless(os.environ.get("GOSSIP_RUN_DOCKER_TESTS") == "1", "explicit real Docker lane")
class CandidateClientProcessControlV4DockerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        qualification_setup(cls, "candidate-b03-v4-process-controls", reference=False)
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
