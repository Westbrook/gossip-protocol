#!/usr/bin/env python3
"""Post-hoc final-pool diagnostic, run only after the entire live study freezes.

This is a secondary analysis, outside the preregistered primary study. The pool
contains the final retained versions after unequal repair allocation and shared
earlier checkpoints, not four independent projects or the initial four proposals.
Private results never feed a model, promote source, or alter the primary scores.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import itertools
import json
from pathlib import Path
import re
import subprocess
import sys
import time

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY))
from gossip_harness.blackbox_validator import (BlackboxValidator, CHILD_ADAPTER,
                                               SUPERVISOR_ADAPTER, json_equal)
from gossip_harness.gitstore import GitStore
from gossip_harness.sustained_checkpoint import save_checkpoint
from gossip_harness.verification_experiment import CORE

PROTOCOL = "verification-final-pool-diagnostic-v1"
LABEL = "Post-hoc final retained pool after repairs; secondary diagnostic, primary scores unchanged"
POLICIES = ("strong-reviewed", "cheap-reviewed", "portfolio-reviewed")
MAX_FRESH_VALIDATIONS = 18
EXECUTION_FIELDS = ("input", "expected", "id", "requirement")
ADAPTER_SHA256 = hashlib.sha256(json.dumps(
    {"child.py": CHILD_ADAPTER, "supervisor.py": SUPERVISOR_ADAPTER},
    sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
        allow_nan=False, separators=(",", ":")).encode()).hexdigest()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def execution_cases(cases):
    return [{key: case[key] for key in EXECUTION_FIELDS if key in case} for case in cases]


def runtime():
    return dict(python=sys.version, implementation=sys.implementation.name,
        executable=sys.executable, platform=sys.platform,
        git=subprocess.check_output(["git", "--version"], text=True).strip())


def _object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "Duplicate JSON member in frozen evidence")
        value[key] = item
    return value


def _constant(_):
    raise ValueError("Nonfinite JSON in frozen evidence")


def decode(raw):
    return json.loads(raw, object_pairs_hook=_object, parse_constant=_constant)


def receipt_rows(receipt, files, cases, contract):
    """Check every execution binding and recompute typed correctness on the host."""
    suite = execution_cases(cases)
    require(isinstance(receipt, dict), "Missing validation receipt")
    for key, expected in dict(protocol="gossip-blackbox-v1", schema_version=1,
            source_sha256=digest(files), suite_sha256=digest(suite),
            image_id=contract["image"], adapter_sha256=ADAPTER_SHA256,
            case_timeout_seconds=contract["case_timeout_seconds"],
            timeout_seconds=contract["suite_timeout_seconds"]).items():
        require(type(receipt.get(key)) is type(expected) and receipt[key] == expected,
                "Receipt binding mismatch: " + key)
    require(receipt.get("status") in {"passed", "failed"}
            and receipt.get("cleanup_verified") is True
            and type(receipt.get("exit_code")) is int and receipt["exit_code"] == 0
            and receipt.get("timed_out") is False
            and receipt.get("output_truncated") is False
            and receipt.get("input_delivery_failed") is False,
            "Receipt contains infrastructure failure; not a reusable correctness judgment")
    rows = receipt.get("outcomes")
    require(isinstance(rows, list) and len(rows) == len(suite)
            and type(receipt.get("case_count")) is int
            and receipt["case_count"] == len(suite), "Incomplete receipt outcomes")
    for index, (case, row) in enumerate(zip(suite, rows)):
        require(isinstance(row, dict) and type(row.get("index")) is int and row["index"] == index
                and type(row.get("passed")) is bool, "Unordered or malformed receipt outcome")
        require(all(row.get(key) == case.get(key) for key in ("id", "requirement")),
                "Receipt outcome case labels changed")
        status = row.get("status")
        if status in {"passed", "wrong_answer"}:
            require("actual" in row, "Executed receipt outcome omits actual value")
            passed = json_equal(row["actual"], case["expected"])
            require(row["passed"] == passed and status == ("passed" if passed else "wrong_answer"),
                    "Receipt correctness flag disagrees with exact actual value")
        else:
            require(status in {"error", "timeout", "output_error", "output_limit", "invalid_output"}
                    and row["passed"] is False and "actual" not in row,
                    "Unrecognized or inconsistent candidate execution failure")
    passed = all(row["passed"] for row in rows)
    require(type(receipt.get("passed")) is bool and receipt["passed"] == passed
            and receipt["status"] == ("passed" if passed else "failed"), "Inconsistent receipt aggregate")
    return rows


def completion_gate(report, preregistered):
    require(report.get("experiment") == "verification-quality-v1" and report.get("mode") == "live",
            "Expected the live verification study")
    require(report.get("status") == "finished" and report.get("unexecuted") == []
            and report.get("censored") == [] and report.get("active_case") is None,
            "Entire live study must finish before the pool diagnostic")
    require(type(report.get("repetitions")) is int and report["repetitions"] == 3,
            "Exactly three repetitions and six portfolio trajectories are required")
    contract = report["contract"]
    require(contract == preregistered.get("contract") and digest(contract) == report.get("contract_sha"),
            "Frozen contract mismatch")
    require(contract.get("protocol") == "verification-quality-v1"
            and contract.get("policies") == list(POLICIES) and contract.get("milestones") == 4
            and contract.get("project_ids") == ["buildgraph", "calendar"]
            and contract.get("initial_candidates", {}).get("portfolio-reviewed") == 4,
            "Unexpected study design")
    expected = {(project, policy, repetition) for project in contract["project_ids"]
                for policy in POLICIES for repetition in range(3)}
    rows = report.get("cases", [])
    actual = [(row.get("project_id"), row.get("policy"), row.get("repetition")) for row in rows]
    roster = [tuple(row) for row in preregistered.get("roster", [])]
    require(len(actual) == len(expected) and set(actual) == expected
            and len(roster) == len(expected) and set(roster) == expected,
            "Frozen study roster is incomplete, duplicate, or unexpected")
    return sorted((row for row in rows if row["policy"] == "portfolio-reviewed"),
                  key=lambda row: (row["project_id"], row["repetition"]))


def validate_output(study, output):
    study, output = Path(study), Path(output)
    require(not output.exists() and not output.is_symlink(), "Use a fresh output directory")
    study, output = study.resolve(), output.resolve()
    require(output != study and not output.is_relative_to(study) and not study.is_relative_to(output),
            "Diagnostic output must be outside the frozen study and its ancestors")


def prepare(study, output):
    """Bind all inputs and final Git trees before output creation or Docker use."""
    validate_output(study, output)
    study, output = Path(study).resolve(), Path(output).resolve()
    inputs = {}

    def read(path):
        path = Path(path)
        require(path.is_file() and not path.is_symlink(), "Expected ordinary evidence file: " + str(path))
        raw = path.read_bytes()
        inputs[path.resolve()] = raw
        return raw

    def data(path):
        return decode(read(path))

    report = data(study / "results.json")
    targets = completion_gate(report, data(study / "preregistered.json"))
    contract = report["contract"]
    require(re.fullmatch(r"sha256:[a-f0-9]{64}", contract.get("image", "")) is not None,
            "Frozen container image must be content-addressed")
    require(contract.get("case_timeout_seconds") == 12 and contract.get("suite_timeout_seconds") == 300,
            "Unexpected frozen validation limits")
    require(contract.get("controller_runtime") == runtime(), "Use the exact frozen controller runtime")
    require(set(contract.get("sources", {})) == set(CORE), "Frozen helper source roster changed")
    for name, expected in contract["sources"].items():
        require(Path(name).name == name and name.endswith(".py"), "Unsafe helper source path")
        require(sha(read(study / "source-snapshot" / "gossip_harness" / name)) == expected,
                "Frozen helper changed: " + name)
        require(sha(read(REPOSITORY / "gossip_harness" / name)) == expected, "Current helper changed: " + name)
    require(sha(read(study / "study-plan.json")) == contract["plan_sha256"], "Frozen plan changed")
    fixtures = data(study / "fixtures.json")["projects"]
    require(digest(fixtures) == contract["fixture_sha256"], "Frozen fixture changed")
    projects = {project["id"]: project for project in fixtures}
    require(len(projects) == len(fixtures) == 2 and set(projects) == set(contract["project_ids"]),
            "Frozen fixture roster changed")
    pools, skipped = [], []
    for primary in targets:
        identity = primary["run_id"]
        require(identity == f"{primary['project_id']}-portfolio-reviewed-{primary['repetition']}", "Unsafe run identity")
        root = study / identity
        require(Path(primary["root"]).resolve() == root and data(root / "result.json") == primary,
                "Primary result binding changed: " + identity)
        raw = read(root / "trajectory.json")
        require(sha(raw) == primary["trajectory_sha256"], "Frozen trajectory changed: " + identity)
        state = decode(raw)
        require(state["contract_sha"] == report["contract_sha"], "Trajectory contract changed")
        stages = state["stages"]
        common = dict(run_id=identity, project_id=primary["project_id"], repetition=primary["repetition"],
                      primary_accepted=primary["accepted"], primary_status=primary["status"])
        if len(stages) < 4:
            require(primary["accepted"] is False, "Accepted trajectory did not reach final stage")
            skipped.append(dict(**common, reason="Final milestone not reached; earlier candidate pools are not substituted"))
            continue
        require(len(stages) == 4 and stages[-1]["stage_index"] == 3, "Invalid final milestone index")
        stage = stages[-1]
        require(data(root / "stage-3" / "result.json") == stage, "Final stage differs from frozen trajectory")
        require(stage["files"] == state["files"] and digest(state["files"]) == primary["files_sha256"],
                "Final selected source binding changed")
        project = projects[primary["project_id"]]
        hidden = [case for milestone in project["stages"] for case in milestone["hidden_cases"]]
        public = [case for milestone in project["stages"] for case in milestone["visible_cases"]]
        evidence = stage["evidence_cases"]
        require(evidence == public + stage["probe_pool"] and digest(evidence) == stage["evidence_sha256"],
                "Final public/probe evidence changed")
        evidence_map = {case["id"]: case for case in evidence}
        require(len(evidence_map) == len(evidence), "Duplicate active evidence IDs")
        require(hidden and len({case["id"] for case in hidden}) == len(hidden), "Hidden case IDs must be unique")
        roster = stage["candidate_order"]
        require(len(roster) == len(set(roster)) == 4 and set(roster) == {"candidate-A", "candidate-B", "candidate-C", "candidate-D"}
                and set(roster) == set(stage["candidates"])
                and set(roster) == set(stage["matrix"]) and stage["selected"] in roster,
                "Final pool roster changed")
        trusted = dict(project["initial_files"])
        for milestone in project["stages"]:
            trusted.update(milestone.get("trusted_updates", {}))
        candidates = []
        for alias in roster:
            candidate = stage["candidates"][alias]
            binding = candidate["binding"]
            path = Path(binding["store_path"]).resolve()
            require(path.is_relative_to(root / "stage-3") and path.suffix == ".git", "Candidate Git path escapes final stage")
            files = GitStore(path).read_files(binding["tip_sha"])
            require(digest(files) == binding["files_sha256"], "Candidate Git source binding changed")
            require(data(path.with_suffix(".json")) == dict(files=files, binding=binding), "Candidate source export changed")
            history = [row for row in stage["trajectory"] if row["kind"] == "builder" and row["candidate"] == alias]
            require(history and history[-1]["binding"] == binding
                    and history[-1]["files_sha256"] == digest(files)
                    and history[-1]["source_valid"] == candidate["source_valid"], "Candidate is not the final retained version")
            require(set(files) == set(trusted) and all(files[name] == trusted[name]
                    for name in set(trusted) - set(project["allowed_paths"])), "Trusted repository source changed")
            require(type(candidate.get("source_valid")) is bool, "Candidate source validity missing")
            matrix = {}
            for validation in candidate["validation_receipts"]:
                ids = validation["case_ids"]
                require(ids and len(ids) == len(set(ids)) and set(ids) <= set(evidence_map), "Unknown validation case IDs")
                suite = [evidence_map[case_id] for case_id in ids]
                rows = receipt_rows(validation["receipt"], files, suite, contract)
                matrix.update({case_id: {key: row[key] for key in ("passed", "status", "actual", "error", "stderr") if key in row}
                               for case_id, row in zip(ids, rows)})
            require(set(matrix) == set(evidence_map) and matrix == candidate["matrix"] == stage["matrix"][alias],
                    "Final visible matrix is not bound to the retained source")
            remaining = candidate["reviewer_remaining"]
            require(isinstance(remaining, list) and all(isinstance(item, str) for item in remaining), "Invalid reviewer remaining record")
            visible = candidate["source_valid"] and all(row["passed"] for row in matrix.values())
            candidates.append(dict(alias=alias, binding=binding, files=files,
                source_sha256=digest(files), source_valid=candidate["source_valid"],
                visible_eligible=visible, reviewer_remaining=remaining,
                accept_gate_eligible=visible and not remaining,
                visible_failed_case_ids=sorted(case_id for case_id, row in matrix.items() if not row["passed"])))
        selected = next(candidate for candidate in candidates if candidate["alias"] == stage["selected"])
        require(selected["files"] == state["files"] and selected["binding"] == stage["selected_binding"],
                "Selected candidate does not match the primary final source")
        primary_hidden_path = root / "final-private-receipt.json"
        require(data(primary_hidden_path) == primary["final_hidden"], "Primary hidden receipt copy changed")
        receipt_rows(primary["final_hidden"], selected["files"], hidden, contract)
        require(data(root / "final-visible-receipt.json") == primary["final_visible"], "Primary public receipt copy changed")
        receipt_rows(primary["final_visible"], selected["files"], evidence, contract)
        require(all(type(milestone.get("completed")) is bool
                    and type(milestone.get("stage_index")) is int and milestone["stage_index"] == index
                    for index, milestone in enumerate(stages)), "Malformed milestone completion records")
        full = all(milestone["completed"] for milestone in stages)
        accepted = full and primary["final_visible"]["passed"] and primary["final_hidden"]["passed"]
        require(type(primary.get("accepted")) is bool and primary["accepted"] == accepted
                and type(primary.get("milestones_completed")) is int
                and primary["milestones_completed"] == sum(milestone["completed"] for milestone in stages)
                and primary["status"] == ("accepted" if accepted else "final_quality_failed" if full else state["terminal"]),
                "Primary acceptance label disagrees with milestone and final receipt evidence")
        pools.append(dict(**common, stage_completed=stage["completed"], selected=stage["selected"],
            candidates=candidates, hidden_cases=hidden, selected_primary_receipt=primary["final_hidden"],
            selected_primary_receipt_path=str(primary_hidden_path),
            selected_primary_receipt_sha256=sha(inputs[primary_hidden_path.resolve()])))
    require(len(pools) + len(skipped) == 6, "Every portfolio trajectory must be accounted for")
    planned = sum(len({candidate["source_sha256"] for candidate in pool["candidates"]}
                     - {next(candidate["source_sha256"] for candidate in pool["candidates"]
                             if candidate["alias"] == pool["selected"])}) for pool in pools)
    require(planned <= MAX_FRESH_VALIDATIONS, "Fresh validation limit exceeded")
    return dict(report=report, contract=contract, pools=pools, skipped=skipped,
                inputs=inputs, planned_fresh_validations=planned)


def unchanged(inputs):
    for path, raw in inputs.items():
        require(path.is_file() and not path.is_symlink() and path.read_bytes() == raw,
                "Frozen input changed during diagnostic: " + str(path))


def score(receipt, cases):
    failed = [case["id"] for case, row in zip(cases, receipt["outcomes"]) if not row["passed"]]
    coverage = {requirement: all(row["passed"] for case, row in zip(cases, receipt["outcomes"])
                if case["requirement"] == requirement) for requirement in sorted({case["requirement"] for case in cases})}
    return dict(hidden_passed=receipt["passed"], hidden_failed_case_ids=failed,
                requirement_coverage=coverage, requirements_passed=sum(coverage.values()), requirements_total=len(coverage))


def summarize_pool(pool, rows):
    selected = next(row for row in rows if row["alias"] == pool["selected"])
    eligible = [row for row in rows if row["visible_eligible"]]
    acceptance_eligible = [row for row in rows if row["accept_gate_eligible"]]
    visible_any = any(row["hidden_passed"] for row in eligible)
    accepted_any = any(row["hidden_passed"] for row in acceptance_eligible)
    selected_qualified = selected["visible_eligible"] and selected["hidden_passed"]
    pairs = []
    for left, right in itertools.combinations(rows, 2):
        first, second = set(left["hidden_failed_case_ids"]), set(right["hidden_failed_case_ids"])
        pairs.append(dict(left=left["alias"], right=right["alias"], shared_failures=sorted(first & second),
                          union_failures=len(first | second), jaccard=len(first & second) / len(first | second) if first | second else None))
    counts = {case["id"]: sum(case["id"] in row["hidden_failed_case_ids"] for row in rows) for case in pool["hidden_cases"]}
    best = max((row["requirements_passed"] for row in eligible), default=None)
    envelope = {requirement: any(row["requirement_coverage"][requirement] for row in eligible)
                for requirement in selected["requirement_coverage"]}
    return dict(run_id=pool["run_id"], project_id=pool["project_id"], repetition=pool["repetition"],
        primary_accepted=pool["primary_accepted"], primary_status=pool["primary_status"],
        stage_completed=pool["stage_completed"], selected=pool["selected"],
        candidates=rows, distinct_source_count=len({row["source_sha256"] for row in rows}),
        selected_hidden_passed=selected["hidden_passed"], selected_visible_eligible=selected["visible_eligible"],
        selected_visible_eligible_hidden_passed=selected_qualified,
        selected_accept_gate_eligible_hidden_passed=selected["accept_gate_eligible"] and selected["hidden_passed"],
        pool_any_hidden_passed=any(row["hidden_passed"] for row in rows),
        pool_any_visible_eligible_hidden_passed=visible_any,
        pool_any_accept_gate_eligible_hidden_passed=accepted_any,
        visible_selector_opportunity=visible_any and not selected_qualified,
        selector_miss=accepted_any and not (selected["accept_gate_eligible"] and selected["hidden_passed"]),
        selected_requirements_passed=selected["requirements_passed"],
        best_single_visible_eligible_requirements_passed=best,
        best_single_coverage_gain=None if best is None else best - selected["requirements_passed"],
        per_requirement_pool_envelope=envelope,
        envelope_warning="Coverage union may combine different sources and is not an executable implementation",
        failed_aliases_per_hidden_case=counts,
        all_pool_failed_case_ids=sorted(case_id for case_id, count in counts.items() if count == len(rows)),
        all_visible_eligible_failed_case_ids=sorted(case["id"] for case in pool["hidden_cases"]
            if eligible and all(case["id"] in row["hidden_failed_case_ids"] for row in eligible)),
        pairwise_failure_overlap=pairs)


def run(study, output):
    validate_output(study, output)
    study, output = Path(study).resolve(), Path(output).resolve()
    plan = prepare(study, output)
    output.mkdir(parents=True, exist_ok=False)
    (output / "inputs").mkdir()
    (output / "receipts").mkdir()
    inventory = []
    for index, (path, raw) in enumerate(sorted(plan["inputs"].items(), key=lambda pair: str(pair[0]))):
        destination = output / "inputs" / f"{index:04d}-{path.name}"
        destination.write_bytes(raw)
        inventory.append(dict(original=str(path), retained=str(destination.relative_to(output)), sha256=sha(raw)))
    own_source = Path(__file__).read_bytes()
    (output / "runner.py").write_bytes(own_source)
    contract = plan["contract"]
    context = dict(protocol=PROTOCOL, purpose="descriptive final-pool correctness only; not repeatability or new acceptance",
        evaluator_sha256=contract["sources"]["blackbox_validator.py"], adapter_sha256=ADAPTER_SHA256,
        runtime=runtime(), image=contract["image"], case_timeout_seconds=contract["case_timeout_seconds"],
        suite_timeout_seconds=contract["suite_timeout_seconds"],
        environment="Same frozen DockerValidator environment allowlist and sandbox settings",
        seed=None, normalization="ordered projection to input/expected/id/requirement, v1")
    save_checkpoint(output / "manifest.json", dict(label=LABEL, preregistered_primary=False,
        primary_scores_modified=False, hidden_feedback=False, original_study=str(study),
        original_contract_sha=plan["report"]["contract_sha"], original_contract=contract,
        runner_sha256=sha(own_source), execution_context=context, inputs=inventory,
        maximum_fresh_validations=MAX_FRESH_VALIDATIONS,
        planned_fresh_validations=plan["planned_fresh_validations"]))
    progress = dict(protocol=PROTOCOL, label=LABEL, status="running", primary_scores_modified=False,
        skipped=plan["skipped"], pools=[], executions=[], fresh_validation_count=0,
        planned_fresh_validations=plan["planned_fresh_validations"],
        unexecuted=[pool["run_id"] for pool in plan["pools"]], started_at=time.time())
    save_checkpoint(output / "results.json", progress)
    validator = None
    try:
        unchanged(plan["inputs"])
        if plan["planned_fresh_validations"]:
            validator = BlackboxValidator(contract["image"], timeout_seconds=contract["suite_timeout_seconds"],
                                          case_timeout_seconds=contract["case_timeout_seconds"])
            okay, detail = validator.preflight()
            require(okay, "Docker preflight failed: " + detail)
        for pool in plan["pools"]:
            rows, cache = [], {}
            selected = next(candidate for candidate in pool["candidates"] if candidate["alias"] == pool["selected"])
            # Deliberately scope reuse to this frozen pool. Each primary selected
            # observation remains its original measurement, even across repeats.
            cache[selected["source_sha256"]] = dict(receipt=pool["selected_primary_receipt"],
                origin="selected_primary", original_receipt=pool["selected_primary_receipt_path"],
                original_receipt_sha256=pool["selected_primary_receipt_sha256"],
                original_alias=selected["alias"])
            ordered = [selected] + [candidate for candidate in pool["candidates"] if candidate is not selected]
            for candidate in ordered:
                unchanged(plan["inputs"])
                key = candidate["source_sha256"]
                execution_key = digest(dict(context=context, source_sha256=key,
                    suite_sha256=digest(execution_cases(pool["hidden_cases"])), run_id=pool["run_id"]))
                reused = key in cache
                if not reused:
                    require(progress["fresh_validation_count"] < MAX_FRESH_VALIDATIONS, "Fresh validation ceiling reached")
                    path = output / "receipts" / f"{pool['run_id']}-{candidate['alias']}.json"
                    progress["fresh_validation_count"] += 1
                    try:
                        receipt = validator.evaluate(dict(candidate["files"]), deepcopy(pool["hidden_cases"]))
                    except BaseException as error:
                        save_checkpoint(path, dict(error=type(error).__name__, detail=str(error),
                            execution_key=execution_key, last_receipt=getattr(validator, "last_receipt", {})))
                        progress["executions"].append(dict(run_id=pool["run_id"], alias=candidate["alias"],
                            execution_key=execution_key, source_sha256=key, receipt=str(path.relative_to(output)),
                            receipt_sha256=sha(path.read_bytes()), verified=False, raised=type(error).__name__))
                        raise
                    save_checkpoint(path, receipt)  # Preserve bad/infrastructure receipts before checking.
                    progress["executions"].append(dict(run_id=pool["run_id"], alias=candidate["alias"],
                        execution_key=execution_key, source_sha256=key, receipt=str(path.relative_to(output)),
                        receipt_sha256=sha(path.read_bytes()), verified=False))
                    save_checkpoint(output / "results.json", progress)
                    receipt_rows(receipt, candidate["files"], pool["hidden_cases"], contract)
                    progress["executions"][-1]["verified"] = True
                    cache[key] = dict(receipt=receipt, origin="fresh_diagnostic",
                        original_receipt=str(path), original_receipt_sha256=sha(path.read_bytes()),
                        original_alias=candidate["alias"])
                observation = cache[key]
                receipt_rows(observation["receipt"], candidate["files"], pool["hidden_cases"], contract)
                row = {name: deepcopy(value) for name, value in candidate.items() if name != "files"}
                row.update(score(observation["receipt"], pool["hidden_cases"]))
                row.update(measurement_origin=("reused_selected_primary" if observation["origin"] == "selected_primary"
                    else "reused_identical_diagnostic_source" if reused else "fresh_diagnostic"),
                    physically_executed_for_diagnostic=not reused, execution_key=execution_key,
                    original_alias=observation["original_alias"],
                    original_receipt=observation["original_receipt"],
                    original_receipt_sha256=observation["original_receipt_sha256"])
                rows.append(row)
            progress["pools"].append(summarize_pool(pool, rows))
            progress["unexecuted"].remove(pool["run_id"])
            save_checkpoint(output / "results.json", progress)
        unchanged(plan["inputs"])
        progress.update(status="finished", inputs_unchanged=True, summary=dict(
            planned_portfolio_trajectories=6, final_pools_evaluated=len(progress["pools"]),
            unreached_final_stage=len(progress["skipped"]),
            selected_hidden_passed=sum(row["selected_hidden_passed"] for row in progress["pools"]),
            selected_visible_eligible_hidden_passed=sum(row["selected_visible_eligible_hidden_passed"] for row in progress["pools"]),
            selected_accept_gate_eligible_hidden_passed=sum(row["selected_accept_gate_eligible_hidden_passed"] for row in progress["pools"]),
            pool_any_visible_eligible_hidden_passed=sum(row["pool_any_visible_eligible_hidden_passed"] for row in progress["pools"]),
            pool_any_accept_gate_eligible_hidden_passed=sum(row["pool_any_accept_gate_eligible_hidden_passed"] for row in progress["pools"]),
            selector_misses=sum(row["selector_miss"] for row in progress["pools"]),
            visible_selector_opportunities=sum(row["visible_selector_opportunity"] for row in progress["pools"])))
    except BaseException as error:
        progress.update(status="interrupted", failure=str(error) or type(error).__name__)
        raise
    finally:
        progress["finished_at"] = time.time()
        save_checkpoint(output / "results.json", progress)
    return progress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True, help="Entire finished live verification study directory")
    parser.add_argument("--output", type=Path, required=True, help="Fresh directory outside frozen results")
    arguments = parser.parse_args()
    try:
        result = run(arguments.run, arguments.output)
    except Exception as error:
        print("Final-pool diagnostic stopped: " + str(error), file=sys.stderr)
        return 1
    print(json.dumps(dict(status=result["status"], summary=result["summary"],
                          fresh_validation_count=result["fresh_validation_count"])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
