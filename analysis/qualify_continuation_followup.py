"""Qualify continuation-followup fixtures without API calls or host candidate execution.

Each source/suite/purpose receives one fresh Docker execution. A semantic fault
counts only when its private-acceptance witness produces a wrong answer in a
public-passing runnable source;
infrastructure and runtime failures never count as successful fault detection.
The receipt validator reconstructs the matrix and conclusions from retained raw
receipts, so a hand-edited success flag cannot qualify a study.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import importlib
import json
from pathlib import Path
import platform
import re
import subprocess
import sys
from typing import Any

from gossip_harness.blackbox_validator import (
    BlackboxValidator, CHILD_ADAPTER, PROTOCOL as BLACKBOX_PROTOCOL,
    SUPERVISOR_ADAPTER, json_equal,
)
from gossip_harness.pilot import DEFAULT_IMAGE
from gossip_harness.sandbox import DockerValidator
from gossip_harness.sustained_checkpoint import save_checkpoint
from gossip_harness.sustained_experiment import digest
from gossip_harness.verification_experiment import CASE_TIMEOUT, SUITE_TIMEOUT, verify_receipt


PROTOCOL = "continuation-followup-fixture-qualification-v1"
STUDY_PROTOCOL = "continuation-followup-v1"
PROJECT_IDS = ("warehouse", "job-queue")
POLICIES = ("current-independent", "improved-independent", "improved-sequential")
REPOSITORY = Path(__file__).resolve().parents[1]
MINIMUM_SURVIVING_FAMILIES = 4
ADAPTER_SHA256 = digest({"supervisor.py": SUPERVISOR_ADAPTER, "child.py": CHILD_ADAPTER})


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "Duplicate JSON key")
            result[key] = value
        return result

    def constant(value):
        raise ValueError("Nonfinite JSON: " + value)

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def _plain_file(path):
    require(path.is_file() and not path.is_symlink(), "Expected ordinary file: " + str(path))
    return path.read_bytes()


def _safe_relative(name):
    require(isinstance(name, str), "Unsafe retained path")
    path = Path(name)
    require(not path.is_absolute() and name
            and all(part not in ("", ".", "..") for part in name.split("/"))
            and "\\" not in name, "Unsafe retained path")
    return path


def ordered_suite(groups):
    """Deduplicate exact JSON inputs, retain order and reject conflicting labels."""
    cases: list[dict[str, Any]] = []
    indexes: dict[str, list[int]] = {}
    seen: dict[str, int] = {}
    for group, members in groups.items():
        indexes[group] = []
        require(isinstance(members, list), "A case group must be a list")
        for case in members:
            require(isinstance(case, dict) and "input" in case and "expected" in case,
                    "Each case needs input and expected")
            key = digest(case["input"])
            if key in seen:
                index = seen[key]
                require(json_equal(cases[index]["expected"], case["expected"]),
                        "Conflicting expectations for one input")
            else:
                index = len(cases)
                seen[key] = index
                cases.append({key: deepcopy(case[key]) for key in
                              ("input", "expected", "id", "requirement") if key in case})
            if index not in indexes[group]:
                indexes[group].append(index)
    return cases, indexes


def matrix_for(modules):
    """Build only trusted data; never import or execute a source tree in files."""
    require([module.PROJECT["id"] for module in modules] == list(PROJECT_IDS),
            "The exact ordered warehouse/job-queue fixture roster is required")
    rows: list[dict[str, Any]] = []
    stage_keys = []
    for module in modules:
        project = module.PROJECT
        require(len(project["stages"]) == 2, "Exactly two benchmark milestones required")
        for stage_index, stage in enumerate(project["stages"]):
            project_id = project["id"]
            stage_keys.append([project_id, stage_index])
            public = [case for part in project["stages"][:stage_index + 1]
                      for case in part["visible_cases"]]
            private = [case for part in project["stages"][:stage_index + 1]
                       for case in part["hidden_cases"]]
            private_labels = {digest(case["input"]): case["expected"] for case in private}
            faults = module.fault_bank(stage_index)
            controls = module.correct_controls(stage_index)
            require(isinstance(faults, list) and bool(faults), "Nonempty frozen fault bank required")
            require(isinstance(controls, list) and len(controls) == 2,
                    "Exactly two independently named correct controls required")
            ids = [row["id"] for row in faults + controls]
            require(all(isinstance(name, str) and re.fullmatch(r"[a-z0-9][a-z0-9_-]*", name)
                        for name in ids)
                    and len(ids) == len(set(ids)) and "golden" not in ids,
                    "Fixture source IDs must be unique and nonempty")
            witnesses = []
            for fault in faults:
                require(isinstance(fault.get("family"), str) and fault["family"]
                        and isinstance(fault.get("witness_cases"), list)
                        and fault["witness_cases"], "Each named fault requires family and witnesses")
                require(all(digest(case["input"]) in private_labels
                            and json_equal(case["expected"], private_labels[digest(case["input"])])
                            for case in fault["witness_cases"]),
                        "Each fault witness must be an exact private acceptance case")
                witnesses.extend(fault["witness_cases"])
            # This also catches conflicts between two different fault witnesses.
            correct_cases, correct_indexes = ordered_suite(
                dict(public=public, private=private, witness=witnesses))
            require(correct_indexes["public"] and correct_indexes["private"],
                    "Public and private acceptance groups must be nonempty")
            sources = [dict(id="golden", kind="golden", files=stage["known_files"])]
            sources.extend(dict(row, kind="control") for row in controls)
            sources.extend(dict(row, kind="mutant") for row in faults)
            golden_hash = digest(stage["known_files"])
            require(any(digest(control["files"]) != golden_hash for control in controls),
                    "At least one semantics-preserving control must have distinct source bytes")
            source_hashes = set()
            for source in sources:
                if source["kind"] == "mutant":
                    cases, indexes = ordered_suite(dict(public=public, private=private,
                                                       witness=source["witness_cases"]))
                else:
                    cases, indexes = deepcopy(correct_cases), deepcopy(correct_indexes)
                files, cases, _ = BlackboxValidator._inputs(source["files"], cases)
                source_hash = digest(files)
                correct_baseline = (source["kind"] == "control" and source["id"] == "correct"
                                    and source_hash == golden_hash)
                require(source_hash not in source_hashes or correct_baseline,
                        "Faults and nonbaseline controls require distinct exact source trees")
                source_hashes.add(source_hash)
                number = len(rows)
                rows.append(dict(index=number, project_id=project_id, stage_index=stage_index,
                    source_id=source["id"], kind=source["kind"], family=source.get("family"),
                    fixture_id=f"{project_id}/stage-{stage_index}/{source['kind']}/{source['id']}",
                    purpose=f"{PROTOCOL}/{project_id}/stage-{stage_index}/{source['kind']}/{source['id']}",
                    files=files, source_sha256=source_hash, cases=cases,
                    suite_sha256=digest(cases), groups=indexes,
                    receipt=f"receipts/{number:04d}.json"))
    require(len(stage_keys) == len({tuple(item) for item in stage_keys}), "Duplicate project/stage")
    return dict(stages=stage_keys, rows=rows)


def validate_study_contract(contract, image):
    """Fixture readiness applies to this exact planned cohort, not a subset."""
    require(contract.get("protocol") == STUDY_PROTOCOL
            and contract.get("project_ids") == list(PROJECT_IDS)
            and contract.get("milestones") == 2
            and set(contract.get("policies", {})) == set(POLICIES),
            "Unsupported continuation-followup contract")
    expected_roster = {(project, policy, repetition) for project in PROJECT_IDS
                       for policy in POLICIES for repetition in range(2)}
    roster = contract.get("roster")
    require(isinstance(roster, list) and len(roster) == len(expected_roster)
            and all(isinstance(row, list) and len(row) == 3
                    and isinstance(row[0], str) and isinstance(row[1], str)
                    and type(row[2]) is int for row in roster)
            and {tuple(row) for row in roster} == expected_roster,
            "The exact twelve-trajectory study roster is required")
    require(re.fullmatch(r"sha256:[a-f0-9]{64}", image) is not None,
            "Pinned content-addressed image required")
    require(contract.get("image") == image, "Qualification image differs from study image")
    require(contract["case_timeout_seconds"] == CASE_TIMEOUT
            and contract["suite_timeout_seconds"] == SUITE_TIMEOUT,
            "Unexpected qualification execution limits")


def prepare(image=DEFAULT_IMAGE):
    # The runner imports this module locally. Keeping this dependency local
    # avoids an import cycle and binds the same full contract used by the run.
    runner = importlib.import_module("gossip_harness.continuation_followup")
    contract = runner.contract(image)
    validate_study_contract(contract, image)
    modules = [runner.fixture(project) for project in contract["project_ids"]]
    matrix = matrix_for(modules)
    names = {"continuation-followup-study-plan.json": contract["plan_sha256"]}
    names.update({"gossip_harness/" + name: value for name, value in contract["sources"].items()})
    names.update(contract.get("extra_sources", {}))
    own_name = Path(__file__).resolve().relative_to(REPOSITORY).as_posix()
    require(names.get(own_name) == sha(_plain_file(Path(__file__))),
            "Runner contract must bind the qualification implementation")
    require({"gossip_harness/continuation_followup.py", "gossip_harness/benchmark_warehouse.py",
             "gossip_harness/benchmark_job_queue.py", "gossip_harness/blackbox_validator.py",
             "gossip_harness/sandbox.py", "gossip_harness/verification_experiment.py",
             "gossip_harness/sustained_experiment.py", "gossip_harness/sustained_checkpoint.py"}
            <= names.keys(), "Qualification dependency inventory is incomplete")
    inputs = {}
    for name, expected in sorted(names.items()):
        raw = _plain_file(REPOSITORY / _safe_relative(name))
        require(sha(raw) == expected, "Contract source changed: " + name)
        inputs[name] = raw
    environment = execution_environment()
    return dict(contract=contract, contract_sha256=digest(contract), matrix=matrix,
                inputs=inputs, execution_environment=environment,
                execution_environment_sha256=digest(environment))


def execution_environment():
    """Bind execution-relevant host state without retaining environment values."""
    return dict(python=sys.version, implementation=sys.implementation.name,
        executable=sys.executable, platform=sys.platform,
        host=dict(system=platform.system(), release=platform.release(), machine=platform.machine()),
        docker_cli_environment_sha256=digest(DockerValidator._environment()),
        candidate_environment="pinned image; isolated Python adapter; sandbox.py arguments",
        seed="no stochastic fixture or evaluator seed; exact ordered case inputs")


def runtime_identity(image):
    """Read daemon/context/image identity, never launch or execute a container.

    The CLI environment alone does not identify Docker's configured current
    context. Retain hashes of endpoint/configuration details, not their values.
    Mutable counters (running containers, free memory) are not runtime identity.
    """
    def query(arguments):
        result = subprocess.run(["docker", *arguments], capture_output=True, text=True,
            timeout=15, env=DockerValidator._environment(), check=False)
        require(result.returncode == 0, "Docker runtime identity query failed")
        return decode(result.stdout)

    server = query(["version", "--format", "{{json .Server}}"])
    info = query(["info", "--format", "{{json .}}"])
    context = query(["context", "inspect"])
    inspected = query(["image", "inspect", "--format", "{{json .}}", image])
    require(isinstance(server, dict) and all(isinstance(server.get(key), str) and server[key]
            for key in ("Version", "ApiVersion", "Os", "Arch")), "Incomplete Docker server identity")
    require(isinstance(info, dict) and isinstance(info.get("ID"), str) and info["ID"],
            "Incomplete Docker daemon identity")
    require(isinstance(context, list) and len(context) == 1 and isinstance(context[0], dict)
            and context[0].get("Name") and context[0].get("Endpoints"),
            "Incomplete selected Docker context")
    require(isinstance(inspected, dict) and inspected.get("Id") == image
            and inspected.get("Os") and inspected.get("Architecture"),
            "Incomplete pinned Docker image identity")
    configuration = {key: info.get(key) for key in ("Driver", "OperatingSystem", "OSType",
        "Architecture", "KernelVersion", "ServerVersion", "Runtimes", "DefaultRuntime",
        "CgroupDriver", "CgroupVersion", "SecurityOptions")}
    return dict(server_sha256=digest(server), daemon_id_sha256=sha(info["ID"].encode()),
        configuration_sha256=digest(configuration), context_sha256=digest(context),
        server={key: server[key] for key in ("Version", "ApiVersion", "Os", "Arch")},
        image={key: inspected.get(key) for key in ("Id", "Os", "Architecture", "Variant")})


def _runtime_unchanged(image, expected):
    require(runtime_identity(image) == expected, "Qualification Docker runtime identity changed")


def _unchanged(prepared):
    for name, raw in prepared["inputs"].items():
        require(_plain_file(REPOSITORY / name) == raw, "Qualification dependency changed: " + name)
    current_environment = execution_environment()
    require(prepared["execution_environment"] == current_environment
            and prepared["execution_environment_sha256"] == digest(current_environment),
            "Qualification execution environment changed")


def _verified(raw, row, contract):
    receipt = verify_receipt(raw, row["files"], row["cases"], contract["image"])
    require(receipt.get("protocol") == BLACKBOX_PROTOCOL
            and receipt.get("adapter_sha256") == ADAPTER_SHA256
            and receipt.get("case_count") == len(row["cases"])
            and receipt.get("exit_code") == 0 and receipt.get("timed_out") is False
            and receipt.get("input_delivery_failed") is False
            and receipt.get("output_truncated") is False,
            "Receipt adapter, completeness or runtime binding failed")
    for case, outcome in zip(row["cases"], receipt["outcomes"]):
        require(all(outcome.get(key) == case.get(key) for key in ("id", "requirement")),
                "Receipt case labels changed")
        if outcome.get("status") in ("passed", "wrong_answer"):
            require("actual" in outcome, "Runnable receipt outcome requires actual JSON output")
            correct = json_equal(outcome["actual"], case["expected"])
            require(outcome["passed"] is correct
                    and outcome["status"] == ("passed" if correct else "wrong_answer"),
                    "Receipt outcome disagrees with actual JSON output")
        else:
            require(outcome.get("status") in
                    ("error", "timeout", "output_error", "output_limit", "invalid_output")
                    and outcome["passed"] is False, "Unknown receipt outcome")
    return receipt


def classify(row, receipt):
    """A runnable wrong answer is evidence; a crash is a qualification defect."""
    outcomes = receipt["outcomes"]
    runnable = all(item["status"] in ("passed", "wrong_answer") for item in outcomes)
    public_passed = all(outcomes[index]["passed"] for index in row["groups"]["public"])
    witness_failures = [index for index in row["groups"]["witness"]
                        if outcomes[index]["status"] == "wrong_answer"]
    qualified = (runnable and public_passed and bool(witness_failures) if row["kind"] == "mutant"
                 else receipt["passed"] and runnable)
    return dict(index=row["index"], project_id=row["project_id"], stage_index=row["stage_index"],
        source_id=row["source_id"], kind=row["kind"], family=row["family"], fixture_id=row["fixture_id"],
        runnable=runnable, public_surviving=public_passed and runnable,
        witness_wrong_answers=witness_failures, all_cases_passed=receipt["passed"], qualified=qualified)


def conclusions(matrix, classifications):
    expected = [row["fixture_id"] for row in matrix["rows"]]
    complete = ([row["fixture_id"] for row in classifications] == expected
                and len(set(expected)) == len(expected))
    stages = []
    for project_id, stage_index in matrix["stages"]:
        members = [row for row in classifications
                   if row["project_id"] == project_id and row["stage_index"] == stage_index]
        families = sorted({row["family"] for row in members
                           if row["kind"] == "mutant" and row["public_surviving"] and row["qualified"]})
        expected_count = sum(row["project_id"] == project_id and row["stage_index"] == stage_index
                             for row in matrix["rows"])
        stages.append(dict(project_id=project_id, stage_index=stage_index,
            qualified=len(members) == expected_count and all(row["qualified"] for row in members)
                and len(families) >= MINIMUM_SURVIVING_FAMILIES,
            public_surviving_families=families, public_surviving_family_count=len(families),
            source_count=expected_count))
    return dict(qualified=complete and all(stage["qualified"] for stage in stages),
                complete=complete, stages=stages)


def run(output, image=DEFAULT_IMAGE):
    output = Path(output).absolute()
    require(not output.exists() and not output.is_symlink(), "Output already exists; preserve prior evidence")
    prepared = prepare(image)
    output.mkdir(parents=True, exist_ok=False)
    (output / "receipts").mkdir()
    bindings = []
    for name, raw in prepared["inputs"].items():
        target = output / "inputs" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        bindings.append(dict(path=name, sha256=sha(raw)))
    manifest = dict(protocol=PROTOCOL, contract=prepared["contract"],
        contract_sha256=prepared["contract_sha256"], matrix=prepared["matrix"],
        matrix_sha256=digest(prepared["matrix"]), inputs=bindings,
        execution_environment=prepared["execution_environment"],
        execution_environment_sha256=prepared["execution_environment_sha256"],
        adapter_sha256=ADAPTER_SHA256, minimum_surviving_families=MINIMUM_SURVIVING_FAMILIES,
        execution="fresh Docker for every exact source/suite/purpose; no observation reuse",
        api_calls=0, candidate_execution_on_host=False)
    save_checkpoint(output / "manifest.json", manifest)
    result = dict(protocol=PROTOCOL, status="running", api_calls=0,
        contract_sha256=prepared["contract_sha256"], manifest_sha256=sha((output / "manifest.json").read_bytes()),
        execution_environment_sha256=prepared["execution_environment_sha256"],
        started_utc=datetime.now(timezone.utc).isoformat(), executions=[], classifications=[],
        conclusion=conclusions(prepared["matrix"], []))
    save_checkpoint(output / "results.json", result)
    try:
        _unchanged(prepared)
        preflight = BlackboxValidator(image, timeout_seconds=SUITE_TIMEOUT, case_timeout_seconds=CASE_TIMEOUT)
        okay, detail = preflight.preflight()
        result["preflight"] = dict(passed=okay, detail=detail)
        require(okay, "Docker preflight failed: " + detail)
        runtime = runtime_identity(image)
        result["runtime_identity"] = runtime
        result["runtime_identity_sha256"] = digest(runtime)
        save_checkpoint(output / "results.json", result)
        for row in prepared["matrix"]["rows"]:
            _unchanged(prepared)
            _runtime_unchanged(image, runtime)
            validator = BlackboxValidator(image, timeout_seconds=SUITE_TIMEOUT, case_timeout_seconds=CASE_TIMEOUT)
            record = dict(index=row["index"], purpose=row["purpose"], receipt=row["receipt"],
                fixture_id=row["fixture_id"],
                source_sha256=row["source_sha256"], suite_sha256=row["suite_sha256"],
                runtime_identity_sha256=digest(runtime),
                physically_executed=True, reused=False, verified=False)
            result["executions"].append(record)
            save_checkpoint(output / "results.json", result)
            path = output / row["receipt"]
            try:
                raw = validator.evaluate(deepcopy(row["files"]), deepcopy(row["cases"]))
            except BaseException as error:
                save_checkpoint(path, dict(error_type=type(error).__name__,
                    last_receipt=getattr(validator, "last_receipt", {})))
                record["receipt_sha256"] = sha(path.read_bytes())
                raise
            save_checkpoint(path, raw)  # Retain even an invalid or failed receipt.
            record["receipt_sha256"] = sha(path.read_bytes())
            save_checkpoint(output / "results.json", result)
            _runtime_unchanged(image, runtime)
            verified = _verified(raw, row, prepared["contract"])
            record["verified"] = True
            result["classifications"].append(classify(row, verified))
            result["conclusion"] = conclusions(prepared["matrix"], result["classifications"])
            save_checkpoint(output / "results.json", result)
        _unchanged(prepared)
        result["status"] = "qualified" if result["conclusion"]["qualified"] else "qualification_failed"
    except BaseException as error:
        result.update(status="qualification_failed", error_type=type(error).__name__, error=str(error))
        raise
    finally:
        result["finished_utc"] = datetime.now(timezone.utc).isoformat()
        save_checkpoint(output / "results.json", result)
    return result


def validate_qualification(path, expected_contract_digest):
    """Read-only eligibility gate, including exact current dependencies and matrix."""
    path = Path(path).absolute()
    require(path.name == "results.json", "Pass the qualification results.json receipt")
    captured = {}

    def read(file):
        raw = _plain_file(file)
        captured[file] = raw
        return decode(raw)

    report = read(path)
    manifest = read(path.parent / "manifest.json")
    require(report.get("protocol") == manifest.get("protocol") == PROTOCOL
            and report.get("status") == "qualified" and report.get("api_calls") == 0
            and report.get("manifest_sha256") == sha(captured[path.parent / "manifest.json"]),
            "Not a finalized zero-API qualification receipt")
    prepared = prepare(manifest["contract"]["image"])
    require(prepared["contract_sha256"] == expected_contract_digest == report.get("contract_sha256")
            == manifest.get("contract_sha256")
            and manifest.get("contract") == prepared["contract"], "Qualification contract changed")
    require(manifest.get("matrix") == prepared["matrix"]
            and manifest.get("matrix_sha256") == digest(prepared["matrix"])
            and manifest.get("adapter_sha256") == ADAPTER_SHA256
            and manifest.get("minimum_surviving_families") == MINIMUM_SURVIVING_FAMILIES
            and manifest.get("execution_environment") == prepared["execution_environment"]
            and manifest.get("execution_environment_sha256") == report.get("execution_environment_sha256")
                == prepared["execution_environment_sha256"]
            and manifest.get("api_calls") == 0 and manifest.get("candidate_execution_on_host") is False,
            "Qualification matrix or execution contract changed")
    runtime = runtime_identity(prepared["contract"]["image"])
    require(report.get("runtime_identity") == runtime
            and report.get("runtime_identity_sha256") == digest(runtime),
            "Qualification Docker runtime identity changed")
    expected_inputs = [dict(path=name, sha256=sha(raw)) for name, raw in prepared["inputs"].items()]
    require(manifest.get("inputs") == expected_inputs, "Qualification dependency inventory changed")
    for name, raw in prepared["inputs"].items():
        retained = path.parent / "inputs" / name
        retained_raw = _plain_file(retained)
        captured[retained] = retained_raw
        require(retained_raw == raw, "Retained qualification dependency changed: " + name)
    executions = report.get("executions")
    require(isinstance(executions, list) and len(executions) == len(prepared["matrix"]["rows"]),
            "Qualification execution matrix incomplete")
    classifications = []
    container_names = set()
    for row, execution in zip(prepared["matrix"]["rows"], executions):
        require(all(execution.get(key) == row[key] for key in
                    ("index", "fixture_id", "purpose", "receipt", "source_sha256", "suite_sha256"))
                and execution.get("physically_executed") is True and execution.get("reused") is False
                and execution.get("runtime_identity_sha256") == digest(runtime)
                and execution.get("verified") is True, "Qualification execution binding changed")
        raw_path = path.parent / row["receipt"]
        receipt = read(raw_path)
        require(sha(captured[raw_path]) == execution.get("receipt_sha256"), "Raw qualification receipt changed")
        receipt = _verified(receipt, row, prepared["contract"])
        name = receipt.get("container_name")
        require(isinstance(name, str) and name.startswith("gossip-blackbox-") and name not in container_names,
                "Each qualification source requires its own fresh container")
        container_names.add(name)
        classifications.append(classify(row, receipt))
    conclusion = conclusions(prepared["matrix"], classifications)
    require(report.get("classifications") == classifications and report.get("conclusion") == conclusion
            and conclusion["qualified"], "Qualification findings do not follow from raw receipts")
    _unchanged(prepared)
    _runtime_unchanged(prepared["contract"]["image"], runtime)
    require(all(_plain_file(file) == raw for file, raw in captured.items()),
            "Qualification evidence changed during validation")
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    args = parser.parse_args(argv)
    result = run(args.output, args.image)
    print(json.dumps(dict(status=result["status"], executions=len(result["executions"]),
                          stages=result["conclusion"]["stages"]), indent=2))
    return 0 if result["status"] == "qualified" else 1


if __name__ == "__main__":
    sys.exit(main())
