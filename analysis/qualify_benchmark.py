"""Qualify trusted benchmark fixtures without API calls or host candidate execution.

Each source/suite/purpose receives one fresh Docker execution. A semantic fault
counts only when its own witness produces a wrong answer in a runnable source;
infrastructure and runtime failures never count as successful fault detection.
The receipt validator reconstructs the matrix and conclusions from retained raw
receipts, so a hand-edited success flag cannot qualify a study.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

from gossip_harness.blackbox_validator import (
    BlackboxValidator, CHILD_ADAPTER, PROTOCOL as BLACKBOX_PROTOCOL,
    SUPERVISOR_ADAPTER, json_equal,
)
from gossip_harness.pilot import DEFAULT_IMAGE
from gossip_harness.sustained_checkpoint import save_checkpoint
from gossip_harness.sustained_experiment import digest
from gossip_harness.verification_experiment import CASE_TIMEOUT, SUITE_TIMEOUT, verify_receipt


PROTOCOL = "evidence-frontier-fixture-qualification-v1"
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
    path = Path(name)
    require(isinstance(name, str) and not path.is_absolute() and name
            and all(part not in ("", ".", "..") for part in name.split("/"))
            and "\\" not in name, "Unsafe retained path")
    return path


def ordered_suite(groups):
    """Deduplicate exact JSON inputs, retain order and reject conflicting labels."""
    cases, indexes, seen = [], {}, {}
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
    rows, stage_keys = [], []
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
            faults = module.fault_bank(stage_index)
            controls = module.correct_controls(stage_index)
            require(isinstance(faults, list) and bool(faults), "Nonempty frozen fault bank required")
            require(isinstance(controls, list) and len(controls) == 2,
                    "Exactly two independently named correct controls required")
            ids = [row["id"] for row in faults + controls]
            require(all(isinstance(name, str) and name for name in ids)
                    and len(ids) == len(set(ids)) and "golden" not in ids,
                    "Fixture source IDs must be unique and nonempty")
            witnesses = []
            for fault in faults:
                require(isinstance(fault.get("family"), str) and fault["family"]
                        and isinstance(fault.get("witness_cases"), list)
                        and fault["witness_cases"], "Each named fault requires family and witnesses")
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
                    cases, indexes = ordered_suite(dict(public=public, witness=source["witness_cases"]))
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
                    purpose=f"{PROTOCOL}/{project_id}/stage-{stage_index}/{source['kind']}/{source['id']}",
                    files=files, source_sha256=source_hash, cases=cases,
                    suite_sha256=digest(cases), groups=indexes,
                    receipt=f"receipts/{number:04d}.json"))
    require(len(stage_keys) == len({tuple(item) for item in stage_keys}), "Duplicate project/stage")
    return dict(stages=stage_keys, rows=rows)


def prepare(image=DEFAULT_IMAGE):
    # The runner imports this module locally. Keeping this dependency local
    # avoids an import cycle and binds the same full contract used by the run.
    from gossip_harness import benchmark_experiment as runner
    contract = runner.contract(image)
    require(re.fullmatch(r"sha256:[a-f0-9]{64}", image) is not None,
            "Pinned content-addressed image required")
    require(contract["case_timeout_seconds"] == CASE_TIMEOUT
            and contract["suite_timeout_seconds"] == SUITE_TIMEOUT,
            "Unexpected qualification execution limits")
    modules = [runner.fixture(project) for project in contract["project_ids"]]
    matrix = matrix_for(modules)
    names = {"benchmark-study-plan.json": contract["plan_sha256"]}
    names.update({"gossip_harness/" + name: value for name, value in contract["sources"].items()})
    names.update(contract.get("extra_sources", {}))
    own_name = Path(__file__).resolve().relative_to(REPOSITORY).as_posix()
    require(names.get(own_name) == sha(_plain_file(Path(__file__))),
            "Runner contract must bind the qualification implementation")
    inputs = {}
    for name, expected in sorted(names.items()):
        raw = _plain_file(REPOSITORY / _safe_relative(name))
        require(sha(raw) == expected, "Contract source changed: " + name)
        inputs[name] = raw
    return dict(contract=contract, contract_sha256=digest(contract), matrix=matrix, inputs=inputs)


def _unchanged(prepared):
    for name, raw in prepared["inputs"].items():
        require(_plain_file(REPOSITORY / name) == raw, "Qualification dependency changed: " + name)


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
    qualified = (runnable and bool(witness_failures) if row["kind"] == "mutant"
                 else receipt["passed"] and runnable)
    return dict(index=row["index"], project_id=row["project_id"], stage_index=row["stage_index"],
        source_id=row["source_id"], kind=row["kind"], family=row["family"],
        runnable=runnable, public_surviving=public_passed and runnable,
        witness_wrong_answers=witness_failures, all_cases_passed=receipt["passed"], qualified=qualified)


def conclusions(matrix, classifications):
    complete = len(classifications) == len(matrix["rows"])
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
        adapter_sha256=ADAPTER_SHA256, minimum_surviving_families=MINIMUM_SURVIVING_FAMILIES,
        execution="fresh Docker for every exact source/suite/purpose; no observation reuse",
        api_calls=0, candidate_execution_on_host=False)
    save_checkpoint(output / "manifest.json", manifest)
    result = dict(protocol=PROTOCOL, status="running", api_calls=0,
        contract_sha256=prepared["contract_sha256"], manifest_sha256=sha((output / "manifest.json").read_bytes()),
        started_utc=datetime.now(timezone.utc).isoformat(), executions=[], classifications=[],
        conclusion=conclusions(prepared["matrix"], []))
    save_checkpoint(output / "results.json", result)
    try:
        _unchanged(prepared)
        preflight = BlackboxValidator(image, timeout_seconds=SUITE_TIMEOUT, case_timeout_seconds=CASE_TIMEOUT)
        okay, detail = preflight.preflight()
        result["preflight"] = dict(passed=okay, detail=detail)
        require(okay, "Docker preflight failed: " + detail)
        for row in prepared["matrix"]["rows"]:
            _unchanged(prepared)
            validator = BlackboxValidator(image, timeout_seconds=SUITE_TIMEOUT, case_timeout_seconds=CASE_TIMEOUT)
            record = dict(index=row["index"], purpose=row["purpose"], receipt=row["receipt"],
                source_sha256=row["source_sha256"], suite_sha256=row["suite_sha256"],
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
            and manifest.get("api_calls") == 0 and manifest.get("candidate_execution_on_host") is False,
            "Qualification matrix or execution contract changed")
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
                    ("index", "purpose", "receipt", "source_sha256", "suite_sha256"))
                and execution.get("physically_executed") is True and execution.get("reused") is False
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
