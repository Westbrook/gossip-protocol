"""Frozen-cohort source/evidence matrix; private evidence never feeds selection.

This module has no provider/API path. Candidate programs execute only through
fresh bounded BlackboxValidator instances when the diagnostic is explicitly run.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

from .blackbox_validator import BlackboxValidator, CHILD_ADAPTER, SUPERVISOR_ADAPTER, json_equal
from .gitstore import GitStore


PROTOCOL = "evidence-frontier-diagnostic-v1"
STUDY_PROTOCOL = "evidence-frontier-v1"
PURPOSE = "post-cohort-freeze-cross-source-evidence-and-fault-matrix"
ADAPTER_SHA256 = hashlib.sha256(json.dumps(
    {"child.py": CHILD_ADAPTER, "supervisor.py": SUPERVISOR_ADAPTER},
    sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True,
                                    allow_nan=False, separators=(",", ":")).encode()).hexdigest()


def decode(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, "Duplicate JSON member")
            value[key] = item
        return value
    def constant(_):
        raise ValueError("Nonfinite JSON value")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def execution_cases(cases):
    return [{key: case[key] for key in ("input", "expected", "id", "requirement") if key in case}
            for case in cases]


def save_new(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as stream:
        stream.write(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n")


def deduplicate_probes(probes):
    """One execution per exact input; inconsistent typed labels fail closed."""
    unique = {}
    for probe in probes:
        require(isinstance(probe, dict) and {"input", "expected", "requirement"} <= set(probe),
                "Malformed generated probe")
        key = digest(probe["input"])
        if key not in unique:
            unique[key] = dict(id="probe-" + key, input=deepcopy(probe["input"]),
                              expected=deepcopy(probe["expected"]), requirement=probe["requirement"],
                              requirements=[], origins=[])
        target = unique[key]
        require(json_equal(target["input"], probe["input"]) and json_equal(target["expected"], probe["expected"]),
                "Same probe input has inconsistent exact-typed expected labels")
        if probe["requirement"] not in target["requirements"]:
            target["requirements"].append(probe["requirement"])
        origins = probe.get("origins", [probe.get("origin", {})])
        require(isinstance(origins, list) and all(isinstance(origin, dict) for origin in origins),
                "Malformed generated probe provenance")
        known = {digest(origin) for origin in target["origins"]}
        for origin in origins:
            if digest(origin) not in known:
                target["origins"].append(deepcopy(origin))
                known.add(digest(origin))
    return [unique[key] for key in sorted(unique)]


def select_source(pool, matrix, extra_ids, *, public_ids):
    """Predeclared source/public eligibility, unweighted evidence count and ties."""
    order = pool["candidate_order"]
    require(len(set(order)) == len(order) and set(order) == set(pool["candidates"]),
            "Candidate tie order is missing, duplicated, or changed")
    require(len(extra_ids) == len(set(extra_ids)), "Duplicate selected extra evidence")
    options = []
    for index, alias in enumerate(order):
        candidate = pool["candidates"][alias]
        if candidate["source_valid"] is not True:
            continue
        source = candidate["source_id"]
        if source not in matrix or not all(matrix[source].get(key, {}).get("passed") is True for key in public_ids):
            continue
        require(all(key in matrix[source] and type(matrix[source][key].get("passed")) is bool for key in extra_ids),
                "Extra evidence matrix is incomplete")
        passed = sum(matrix[source][key]["passed"] for key in extra_ids)
        options.append((-passed, index, alias, source))
    if not options:
        return None
    negative_passed, _, alias, source = min(options)
    return dict(candidate=alias, source_id=source, extra_passed=-negative_passed)


def receipt_rows(receipt, files, cases, contract):
    """Verify bindings and recompute exact typed correctness; reject infrastructure."""
    suite = execution_cases(cases)
    require(isinstance(receipt, dict), "Missing validation receipt")
    bindings = dict(protocol="gossip-blackbox-v1", schema_version=1,
        source_sha256=digest(files), suite_sha256=digest(suite), image_id=contract["image"],
        adapter_sha256=ADAPTER_SHA256, case_timeout_seconds=contract["case_timeout_seconds"],
        timeout_seconds=contract["suite_timeout_seconds"])
    for key, expected in bindings.items():
        require(type(receipt.get(key)) is type(expected) and receipt[key] == expected,
                "Receipt binding mismatch: " + key)
    require(receipt.get("status") in {"passed", "failed"} and receipt.get("cleanup_verified") is True
            and type(receipt.get("exit_code")) is int and receipt["exit_code"] == 0
            and receipt.get("timed_out") is False and receipt.get("output_truncated") is False
            and receipt.get("input_delivery_failed") is False, "Infrastructure failure is not correctness evidence")
    outcomes = receipt.get("outcomes")
    require(isinstance(outcomes, list) and len(outcomes) == len(suite)
            and type(receipt.get("case_count")) is int and receipt["case_count"] == len(suite),
            "Incomplete receipt outcomes")
    require(len({case["id"] for case in suite}) == len(suite), "Duplicate execution case IDs")
    for index, (case, row) in enumerate(zip(suite, outcomes)):
        require(isinstance(row, dict) and type(row.get("index")) is int and row["index"] == index and type(row.get("passed")) is bool
                and all(row.get(key) == case.get(key) for key in ("id", "requirement")),
                "Outcome order or case labels changed")
        status = row.get("status")
        if status in {"passed", "wrong_answer"}:
            require("actual" in row, "Executed outcome omits actual value")
            passed = json_equal(row["actual"], case["expected"])
            require(row["passed"] == passed and status == ("passed" if passed else "wrong_answer"),
                    "Outcome flag disagrees with exact typed actual value")
        else:
            require(status in {"error", "timeout", "output_error", "output_limit", "invalid_output"}
                    and row["passed"] is False and "actual" not in row, "Unknown candidate failure outcome")
    passed = all(row["passed"] for row in outcomes)
    require(type(receipt.get("passed")) is bool and receipt["passed"] == passed
            and receipt["status"] == ("passed" if passed else "failed"), "Receipt aggregate mismatch")
    return outcomes


def runtime():
    return dict(python=sys.version, implementation=sys.implementation.name, executable=sys.executable,
                platform=sys.platform, git=subprocess.check_output(["git", "--version"], text=True).strip())


class BoundInputs:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.hashes = {}

    def raw(self, path):
        path = Path(path)
        if not path.is_absolute():
            path = self.root / path
        require(path.resolve().is_relative_to(self.root) and not path.is_symlink(), "Input path escapes frozen cohort")
        raw = path.read_bytes()
        relative = str(path.relative_to(self.root))
        current = hashlib.sha256(raw).hexdigest()
        require(relative not in self.hashes or self.hashes[relative] == current, "Previously bound input changed")
        self.hashes[relative] = current
        return raw

    def read(self, path):
        return decode(self.raw(path))

    def unchanged(self):
        return all(hashlib.sha256((self.root / path).read_bytes()).hexdigest() == sha for path, sha in self.hashes.items())


def _bound_files(value, inputs):
    """Validate every path/hash leaf of a runner-produced immutable manifest."""
    if isinstance(value, dict):
        if {"path", "sha256"} <= set(value):
            require(hashlib.sha256(inputs.raw(value["path"])).hexdigest() == value["sha256"],
                    "Globally frozen artifact bytes changed")
        for item in value.values():
            _bound_files(item, inputs)
    elif isinstance(value, list):
        for item in value:
            _bound_files(item, inputs)


def completion_gate(report, prereg, frozen):
    require(report.get("experiment") == STUDY_PROTOCOL and report.get("status") == "finished"
            and report.get("phase") == "finished" and report.get("mode") in {"live", "rehearsal"}
            and report.get("active_case") is None and not report.get("unexecuted") and not report.get("censored"),
            "Full terminal provider cohort and final primary results are required")
    contract = report["contract"]
    require(report["contract_sha"] == digest(contract) and prereg["contract"] == contract
            and contract["protocol"] == STUDY_PROTOCOL and contract["milestones"] == 2,
            "Frozen study contract mismatch")
    expected = {(project, policy, rep) for project in ("graph-patch", "calendar-exchange")
                for policy, count in (("sequential-four", 2), ("independent-four", 2), ("strong-anchor", 1))
                for rep in range(count)}
    roster = [tuple(row) for row in contract["roster"]]
    actual = [(row["project_id"], row["policy"], row["repetition"]) for row in report["cases"]]
    require(len(roster) == len(actual) == 10 and set(roster) == expected and actual == roster
            and all(type(row[2]) is int for row in roster + actual)
            and prereg["roster"] == contract["roster"], "Exact complete ten-trajectory roster required")
    require(frozen["protocol"] == STUDY_PROTOCOL and frozen["contract_sha"] == digest(contract)
            and frozen["private_evaluation_started"] is False
            and all(type(row["repetition"]) is int for row in frozen["trajectories"])
            and [(row["project_id"], row["policy"], row["repetition"]) for row in frozen["trajectories"]] == roster,
            "Global candidate freeze mismatch")
    return contract


def prepare(study, *, require_current_evaluator=True):
    """Check the complete frozen provider cohort before any candidate execution."""
    inputs = BoundInputs(study)
    report = inputs.read("results.json")
    prereg = inputs.read("preregistered.json")
    frozen = inputs.read("frozen-trajectories.json")
    contract = completion_gate(report, prereg, frozen)
    fixtures = inputs.read("fixtures.json")
    faults = inputs.read("fault-banks.json")
    inputs.read("timings.json")
    require(report["freeze_manifest_sha256"] == inputs.hashes["frozen-trajectories.json"], "Global freeze bytes changed")
    for name, expected_sha in contract["sources"].items():
        require(hashlib.sha256(inputs.raw(f"source-snapshot/gossip_harness/{name}")).hexdigest() == expected_sha,
                "Frozen source snapshot changed")
    evaluator_sha = contract["sources"].get("benchmark_diagnostics.py")
    require(isinstance(evaluator_sha, str), "Diagnostic evaluator must be frozen before model calls")
    if require_current_evaluator:
        require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == evaluator_sha,
                "Current diagnostic evaluator differs from frozen study contract")
    require(hashlib.sha256(inputs.raw("study-plan.json")).hexdigest() == contract["plan_sha256"], "Frozen plan changed")
    require(faults["protocol"] == "evidence-frontier-fault-bank-v1"
            and digest(faults) == contract["fault_banks_sha256"], "Fault bank was not frozen in the pre-live contract")
    _bound_files(frozen, inputs)
    project_map = {project["id"]: project for project in fixtures["projects"]}
    require(set(project_map) == {"graph-patch", "calendar-exchange"}, "Fixture project roster changed")
    banks = {(project["project_id"], stage["stage_index"]): stage
             for project in faults["projects"] for stage in project["stages"]}
    require(set(banks) == {(project, index) for project in project_map for index in range(2)}, "Incomplete frozen fault bank")
    groups, git_cache = {}, {}
    for project_id, project in project_map.items():
        require(digest(project) == contract["fixtures"][project_id]["fixture_sha256"], "Fixture digest mismatch")
        for index in range(2):
            groups[(project_id, index)] = dict(project_id=project_id, stage_index=index,
                fixture_sha256=digest(project), sources={}, probes=[], pools=[], faults=[], controls=[],
                public_cases=[deepcopy(case) for stage in project["stages"][:index + 1] for case in stage["visible_cases"]],
                private_cases=[deepcopy(case) for stage in project["stages"][:index + 1] for case in stage["hidden_cases"]])

    def add_source(group, files, occurrence, binding=None):
        require(isinstance(files, dict) and all(isinstance(key, str) and isinstance(value, str) for key, value in files.items()),
                "Malformed retained source")
        source_id = digest(files)
        if binding is not None:
            path = Path(binding["store_path"]).resolve()
            require(path.is_relative_to(inputs.root) and binding["files_sha256"] == source_id,
                    "Candidate binding escapes frozen cohort or source differs")
            key = str(path), binding["tip_sha"]
            if key not in git_cache:
                git_cache[key] = GitStore(path).read_files(binding["tip_sha"])
            require(git_cache[key] == files, "Retained Git source differs from exported files")
        row = group["sources"].setdefault(source_id, dict(files=deepcopy(files), source_sha256=source_id, occurrences=[]))
        occurrence = {**deepcopy(occurrence), "binding": deepcopy(binding)}
        if occurrence not in row["occurrences"]:
            row["occurrences"].append(occurrence)
        return source_id

    for case, frozen_row in zip(report["cases"], frozen["trajectories"]):
        run_id = case["run_id"]
        state = inputs.read(f"{run_id}/trajectory.json")
        require(inputs.hashes[f"{run_id}/trajectory.json"] == frozen_row["trajectory_sha256"] == case["trajectory_sha256"]
                and inputs.read(f"{run_id}/result.json") == case, "Frozen trajectory or primary result changed")
        require(all(state.get(key) == case.get(key) == frozen_row.get(key) for key in
                    ("run_id", "project_id", "policy", "repetition")) and type(state.get("repetition")) is int,
                "Frozen state/primary trajectory identity mismatch")
        artifacts = frozen_row["stage_artifacts"]
        require(len(artifacts) == len(state["stages"]) and 1 <= len(artifacts) <= 2, "Missing frozen stage artifacts")
        completed = [row.get("completed") for row in state["stages"]]
        require(all(type(value) is bool for value in completed) and all(completed[:-1])
                and case["milestones_completed"] == frozen_row["milestones_completed"] == sum(completed)
                and state["terminal"] == frozen_row["terminal"] ==
                    ("visible_complete" if len(completed) == 2 and all(completed) else "bounded_incomplete"),
                "Frozen terminal status disagrees with stage completion")
        for index, (stage, artifact) in enumerate(zip(state["stages"], artifacts)):
            require(artifact["stage_index"] == stage["stage_index"] == index, "Stage order changed")
            group = groups[(case["project_id"], index)]
            initial_document = inputs.read(artifact["initial_pool"]["path"])
            initial = initial_document["pool"]
            require(inputs.read(artifact["result"]["path"]) == stage and stage["initial_pool"] == initial
                    and initial_document["pool_sha256"] == stage["initial_pool_sha256"] == digest(initial),
                    "Initial pool or final stage differs from frozen evidence")
            require(initial_document["protocol"] == STUDY_PROTOCOL and initial_document["run_id"] == run_id
                    and initial_document["stage_index"] == initial["stage_index"] == index
                    and initial["project_id"] == case["project_id"] and initial["policy"] == case["policy"]
                    and initial["candidate_order"] == stage["candidate_order"]
                    and set(initial["candidates"]) == set(stage["candidates"]),
                    "Initial identity or anonymous candidate tie order changed")
            manifest = inputs.read(artifact["candidate_exports"]["path"])
            require(manifest["exports"] == artifact["exports"], "Candidate export manifest differs from global freeze")
            linked = {digest(row["binding"]): row for row in stage["trajectory"] if row.get("kind") == "builder"}
            for row in list(initial["candidates"].values()) + list(stage["candidates"].values()):
                linked.setdefault(digest(row["binding"]), row)
            exports = {}
            for item in manifest["exports"]:
                exported = inputs.read(item["path"])
                require(digest(item["binding"]) in linked and exported["binding"] == item["binding"]
                        and digest(exported["files"]) == item["files_sha256"] == item["binding"]["files_sha256"],
                        "Unlinked or modified candidate export cannot extend a frozen pool")
                project = project_map[case["project_id"]]
                require(set(exported["files"]) == set(project["initial_files"])
                        and all(exported["files"][name] == content for name, content in project["initial_files"].items()
                                if name not in project["allowed_paths"]), "Candidate export violates original source scope")
                history = linked[digest(item["binding"])]
                occurrence = dict(run_id=run_id, policy=case["policy"], repetition=case["repetition"],
                    kind="candidate", phase=history.get("phase", "retained"), label=item["label"],
                    source_valid=history.get("source_valid"), scope_provenance="original_controller_retained_proposal")
                source_id = add_source(group, exported["files"], occurrence, item["binding"])
                exports[digest(item["binding"])] = source_id
            require(set(exports) == set(linked), "Frozen source history has an unexported candidate binding")
            own_probes = []
            for probe in stage["probe_pool"]:
                require(probe.get("validated_stage_index") == index
                        and probe.get("origin", {}).get("source") in {"scout", "reviewer"},
                        "Generated evidence is not an admitted probe at this stage")
                item = deepcopy(probe)
                item["origin"] = {**item.get("origin", {}), "run_id": run_id, "policy": case["policy"],
                                  "repetition": case["repetition"], "matrix_stage_index": index}
                own_probes.append(item)
                group["probes"].append(item)
            for phase, pool in (("initial", initial), ("final", stage)):
                candidates = {}
                for alias, candidate in pool["candidates"].items():
                    source_id = exports[digest(candidate["binding"])]
                    require(type(candidate["source_valid"]) is bool, "Missing original source eligibility")
                    require("files" not in candidate or digest(candidate["files"]) == source_id, "Frozen candidate file body changed")
                    candidates[alias] = dict(source_id=source_id, source_valid=candidate["source_valid"],
                                             eligibility_origin=deepcopy(candidate.get("eligibility_origin")))
                group["pools"].append(dict(run_id=run_id, policy=case["policy"], repetition=case["repetition"], phase=phase,
                    candidate_order=deepcopy(pool["candidate_order"]), candidates=candidates,
                    selected_alias=stage["selected"], selected_final_source_id=stage["selected_binding"]["files_sha256"],
                    own_probe_inputs=[digest(probe["input"]) for probe in own_probes]))
    for (project_id, index), group in groups.items():
        bank = banks[(project_id, index)]
        require(len({fault["id"] for fault in bank["fault_bank"]}) == len(bank["fault_bank"]), "Duplicate semantic fault IDs")
        for fault in bank["fault_bank"]:
            source_id = add_source(group, fault["files"], dict(kind="fault", id=fault["id"], family=fault["family"]))
            group["faults"].append(dict(id=fault["id"], family=fault["family"], source_id=source_id,
                                       witness_cases=deepcopy(fault["witness_cases"])))
        controls = [{"id": "trusted-golden", "files": project_map[project_id]["stages"][index]["known_files"]}] + bank["correct_controls"]
        require(len({control["id"] for control in controls}) == len(controls), "Duplicate correct-control IDs")
        for control in controls:
            source_id = add_source(group, control["files"], dict(kind="correct_control", id=control["id"]))
            group["controls"].append(dict(id=control["id"], source_id=source_id))
        group["probes"] = deduplicate_probes(group["probes"])
        cases = []
        for kind in ("public", "private"):
            current = group.pop(kind + "_cases")
            group[kind + "_ids"] = []
            for case in current:
                case["id"] = kind + ":" + case["id"]
                group[kind + "_ids"].append(case["id"])
                cases.append(case)
        cases.extend(execution_cases(group["probes"]))
        for fault in group["faults"]:
            fault["witness_ids"] = []
            for number, witness in enumerate(fault.pop("witness_cases")):
                case = deepcopy(witness)
                case["id"] = f"witness:{fault['id']}:{number}"
                fault["witness_ids"].append(case["id"])
                cases.append(case)
        group["cases"] = execution_cases(cases)
        group["suite_sha256"] = digest(group["cases"])
    require(inputs.unchanged(), "Frozen study changed during diagnostic preparation")
    return dict(protocol=PROTOCOL, purpose=PURPOSE, primary_run=str(inputs.root), mode=report["mode"],
        input_primary_results_sha256=inputs.hashes["results.json"], primary_contract_sha256=digest(contract),
        global_freeze_sha256=inputs.hashes["frozen-trajectories.json"], input_timings_sha256=inputs.hashes["timings.json"],
        freeze_monotonic_ns=frozen["frozen_monotonic_ns"], frozen_utc=frozen["frozen_utc"],
        fault_banks_sha256=contract["fault_banks_sha256"], input_hashes=inputs.hashes,
        contract={key: contract[key] for key in ("image", "case_timeout_seconds", "suite_timeout_seconds")},
        runtime=runtime(), adapter_sha256=ADAPTER_SHA256, groups=[groups[key] for key in sorted(groups)],
        frozen_diagnostic_source_sha256=evaluator_sha,
        invocation_count=0, planned_candidate_validations=sum(len(group["sources"]) for group in groups.values()))


def analyze_group(group, matrix):
    """Score retained sources without using private outcomes to choose sources."""
    require(set(matrix) == set(group["sources"]), "Incomplete cross-source diagnostic matrix")
    case_map = {case["id"]: case for case in group["cases"]}
    require(all(set(outcomes) == set(case_map) for outcomes in matrix.values()), "Incomplete cross-case diagnostic matrix")
    public_ids, private_ids = group["public_ids"], group["private_ids"]
    requirement_ids = sorted({case_map[key]["requirement"] for key in private_ids})
    require(bool(requirement_ids), "Private requirement suite is empty")

    def quality(source_id):
        if source_id is None:
            return None
        outcomes = matrix[source_id]
        requirements = {requirement: all(outcomes[key]["passed"] for key in private_ids
                        if case_map[key]["requirement"] == requirement) for requirement in requirement_ids}
        return dict(source_id=source_id, public_passed=all(outcomes[key]["passed"] for key in public_ids),
                    private_perfect=all(outcomes[key]["passed"] for key in private_ids),
                    private_passed=sum(outcomes[key]["passed"] for key in private_ids), private_total=len(private_ids),
                    requirements_passed=sum(requirements.values()), requirements_total=len(requirements),
                    requirement_coverage=requirements)

    faults = []
    for fault in group["faults"]:
        outcomes = matrix[fault["source_id"]]
        faults.append({**fault, "public_survived": all(outcomes[key]["passed"] for key in public_ids),
            "witness_killed": bool(fault["witness_ids"]) and any(not outcomes[key]["passed"] for key in fault["witness_ids"])})
    controls = [{**control, "passed": all(row["passed"] for row in matrix[control["source_id"]].values())}
                for control in group["controls"]]
    qualified = bool(controls) and all(control["passed"] for control in controls) and all(fault["witness_killed"] for fault in faults)
    surviving = [fault for fault in faults if fault["public_survived"] and fault["witness_killed"]]
    candidate_sources = {source_id for source_id, source in group["sources"].items()
                         if any(row["kind"] == "candidate" for row in source["occurrences"])}
    eligible_ever = {source_id for source_id, source in group["sources"].items()
                    if any(row["kind"] == "candidate" and row.get("source_valid") is True for row in source["occurrences"])}
    probe_rows = []
    for probe in group["probes"]:
        killed = [fault for fault in surviving if not matrix[fault["source_id"]][probe["id"]]["passed"]]
        candidate_failures = sorted(source for source in candidate_sources if not matrix[source][probe["id"]]["passed"])
        probe_rows.append(dict(id=probe["id"], origins=probe["origins"],
            killed_public_surviving_fault_ids=sorted(fault["id"] for fault in killed),
            killed_public_surviving_families=sorted({fault["family"] for fault in killed}),
            candidate_failure_source_ids=candidate_failures,
            discriminates_retained_candidates=0 < len(candidate_failures) < len(candidate_sources),
            control_failures=[control["id"] for control in controls if not matrix[control["source_id"]][probe["id"]]["passed"]]))
    probe_map = {probe["id"]: probe for probe in probe_rows}
    for probe in probe_rows:
        other_families = {family for other in probe_rows if other["id"] != probe["id"]
                          for family in other["killed_public_surviving_families"]}
        probe["unique_family_contribution_within_pooled_probes"] = sorted(set(probe["killed_public_surviving_families"]) - other_families)
        probe["same_fault_kill_vector_as"] = sorted(other["id"] for other in probe_rows if other["id"] != probe["id"]
            and other["killed_public_surviving_fault_ids"] == probe["killed_public_surviving_fault_ids"])

    def utility(ids):
        selected = [probe_map[key] for key in ids]
        families = {family for probe in selected for family in probe["killed_public_surviving_families"]}
        informative = [tuple(probe["killed_public_surviving_fault_ids"]) for probe in selected if probe["killed_public_surviving_fault_ids"]]
        return dict(probes=len(ids), public_surviving_families_killed=sorted(families),
            public_surviving_family_count=len({fault["family"] for fault in surviving}),
            no_observed_incremental_fault_kill=sum(not probe["killed_public_surviving_fault_ids"] for probe in selected),
            duplicate_nonempty_fault_kill_vectors=len(informative) - len(set(informative)),
            candidate_discriminating_probes=sum(probe["discriminates_retained_candidates"] for probe in selected),
            candidate_discrimination_scope="All unique retained candidate sources across policies and attempts at this project/stage.")

    pools = []
    for pool in group["pools"]:
        unique_ids = {candidate["source_id"] for candidate in pool["candidates"].values()}
        eligible = {candidate["source_id"] for candidate in pool["candidates"].values() if candidate["source_valid"]}
        usable = {source for source in eligible if quality(source)["public_passed"]}
        best_all = max((quality(source)["requirements_passed"] for source in unique_ids), default=None)
        best_eligible = max((quality(source)["requirements_passed"] for source in usable), default=None)
        correct_available = any(quality(source)["private_perfect"] for source in usable)
        actual_source = pool["selected_final_source_id"]
        require(actual_source in matrix, "Actual selected source was not included in diagnostic matrix")
        actual_in_pool = actual_source in unique_ids
        actual = quality(actual_source)
        own_inputs = set(pool["own_probe_inputs"])
        scopes = {}
        for scope in ("own_history", "pooled_same_project_stage"):
            probes = [probe for probe in group["probes"] if scope != "own_history" or digest(probe["input"]) in own_inputs]
            evidence = {"public_only": []}
            for source in ("scout", "reviewer"):
                evidence[source] = [probe["id"] for probe in probes if any(origin.get("source") == source
                    and (scope != "own_history" or origin.get("run_id") == pool["run_id"]) for origin in probe["origins"])]
            evidence["combined"] = sorted(set(evidence["scout"]) | set(evidence["reviewer"]))
            selectors = {}
            for name, ids in evidence.items():
                selected = select_source(pool, matrix, ids, public_ids=public_ids)
                observed = quality(selected["source_id"]) if selected else None
                selectors[name] = dict(selection=selected, quality=observed, evidence_ids=ids, utility=utility(ids),
                    pool_unique_source_discriminating_probes=sum(0 < sum(not matrix[source][key]["passed"]
                        for source in unique_ids) < len(unique_ids) for key in ids),
                    requirement_selection_regret=(best_eligible - observed["requirements_passed"] if observed and best_eligible is not None else None),
                    correct_available_but_not_selected=correct_available and (observed is None or not observed["private_perfect"]))
            scopes[scope] = selectors
        pools.append(dict(run_id=pool["run_id"], policy=pool["policy"], repetition=pool["repetition"], phase=pool["phase"],
            candidate_count=len(pool["candidates"]), unique_sources=len(unique_ids), originally_eligible_sources=len(eligible),
            public_passing_eligible_sources=len(usable), best_any_requirements_passed=best_all,
            best_eligible_public_passing_requirements_passed=best_eligible, requirements_total=len(requirement_ids),
            correct_eligible_public_passing_source_available=correct_available,
            actual_final_selection=actual, actual_final_selection_in_this_pool=actual_in_pool,
            actual_same_pool_requirement_regret=(best_eligible - actual["requirements_passed"]
                if actual_in_pool and actual_source in usable and best_eligible is not None else None),
            actual_same_pool_correct_selection_miss=(correct_available and
                (actual_source not in usable or not actual["private_perfect"]) if actual_in_pool else None),
            selected_alias_initial_or_final_ancestor=quality(pool["candidates"][pool["selected_alias"]]["source_id"]),
            counterfactuals=scopes))
    return dict(project_id=group["project_id"], stage_index=group["stage_index"], qualified=qualified,
        unique_sources=len(matrix), unique_candidate_sources=len(candidate_sources),
        source_qualities={source: quality(source) for source in sorted(candidate_sources)},
        best_ever_retained_requirements_passed=max((quality(source)["requirements_passed"] for source in candidate_sources), default=None),
        best_ever_retained_scope="All retained candidate sources, including originally ineligible occurrences; diagnostic only.",
        best_ever_scope_eligible_requirements_passed=max((quality(source)["requirements_passed"] for source in eligible_ever), default=None),
        best_ever_public_passing_scope_eligible_requirements_passed=max((quality(source)["requirements_passed"]
            for source in eligible_ever if quality(source)["public_passed"]), default=None),
        controls=controls, faults=faults, generated_probes=probe_rows, pooled_probe_utility=utility(list(probe_map)), pools=pools)


def _unchanged(plan):
    root = Path(plan["primary_run"])
    return all(hashlib.sha256((root / path).read_bytes()).hexdigest() == expected for path, expected in plan["input_hashes"].items())


def run(study, output, *, max_workers=1, validator_factory=None):
    require(type(max_workers) is int and 1 <= max_workers <= 2, "Diagnostic concurrency is bounded at two")
    study, output = Path(study).resolve(), Path(output)
    require(not output.exists() and not output.is_symlink(), "Use a fresh diagnostic output")
    output = output.resolve()
    require(not output.is_relative_to(study) and not study.is_relative_to(output), "Output must be outside frozen cohort and its ancestors")
    plan = prepare(study)
    require(_unchanged(plan), "Primary evidence changed before diagnostic execution")
    evaluator = Path(__file__).read_bytes()
    plan["diagnostic_source_sha256"] = hashlib.sha256(evaluator).hexdigest()
    require(plan["diagnostic_source_sha256"] == plan["frozen_diagnostic_source_sha256"], "Diagnostic evaluator version changed")
    plan["max_workers"] = max_workers
    output.mkdir(parents=True)
    with (output / "evaluator.py").open("xb") as stream:
        stream.write(evaluator)
    save_new(output / "plan.json", plan)
    plan_sha = hashlib.sha256((output / "plan.json").read_bytes()).hexdigest()
    contract = plan["contract"]
    if validator_factory is None:
        validator_factory = lambda: BlackboxValidator(contract["image"], timeout_seconds=contract["suite_timeout_seconds"],
                                                       case_timeout_seconds=contract["case_timeout_seconds"])
    jobs = [(index, source_id) for index, group in enumerate(plan["groups"]) for source_id in sorted(group["sources"])]
    def evaluate(job):
        index, source_id = job
        group = plan["groups"][index]
        job_id = f"{group['project_id']}-stage-{group['stage_index']}-{source_id}"
        source = group["sources"][source_id]
        started_ns, started_utc = time.monotonic_ns(), datetime.now(timezone.utc).isoformat()
        require(started_ns >= plan["freeze_monotonic_ns"] and datetime.fromisoformat(started_utc) >= datetime.fromisoformat(plan["frozen_utc"]),
                "Candidate diagnostic precedes whole-cohort freeze")
        validator = receipt = None
        attempted = False
        try:
            validator = validator_factory()
            attempted = True
            receipt = validator.evaluate(deepcopy(source["files"]), deepcopy(group["cases"]))
            save_new(output / "receipts" / f"{job_id}.json", receipt)
            outcomes = receipt_rows(receipt, source["files"], group["cases"], contract)
            status, error = "verified", None
        except Exception as exc:
            if receipt is None:
                receipt = getattr(validator, "last_receipt", None)
                if receipt is not None:
                    save_new(output / "receipts" / f"{job_id}.json", receipt)
            outcomes, status, error = None, "failed", dict(type=type(exc).__name__, message=str(exc))
        finished_ns = time.monotonic_ns()
        path = output / "receipts" / f"{job_id}.json"
        row = dict(job_id=job_id, project_id=group["project_id"], stage_index=group["stage_index"], purpose=PURPOSE,
            source_id=source_id, source_sha256=digest(source["files"]), suite_sha256=group["suite_sha256"],
            ordered_case_ids=[case["id"] for case in group["cases"]], scope_provenance=source["occurrences"],
            image=contract["image"], adapter_sha256=ADAPTER_SHA256, runtime=plan["runtime"],
            case_timeout_seconds=contract["case_timeout_seconds"], suite_timeout_seconds=contract["suite_timeout_seconds"],
            global_freeze_sha256=plan["global_freeze_sha256"], plan_sha256=plan_sha,
            physical_execution=status == "verified", evaluation_attempted=attempted,
            invocation_count=0, started_utc=started_utc,
            started_monotonic_ns=started_ns, finished_monotonic_ns=finished_ns,
            elapsed_seconds=(finished_ns - started_ns) / 1e9, status=status, error=error,
            receipt_path=str(path.relative_to(output)) if path.exists() else None,
            receipt_sha256=hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None)
        save_new(output / "rows" / f"{job_id}.json", row)
        return row, outcomes
    # Executor.map preserves declared job order. Every job executes at most once;
    # failures retain artifacts and cannot trigger a retry or model repair.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        observed = list(executor.map(evaluate, jobs))
    index = dict(protocol=PROTOCOL, purpose=PURPOSE, plan_sha256=plan_sha, invocation_count=0,
                 planned_jobs=len(jobs), evaluation_attempts=sum(row["evaluation_attempted"] for row, _ in observed),
                 confirmed_candidate_executions=sum(row["physical_execution"] for row, _ in observed),
                 rows=[row for row, _ in observed])
    save_new(output / "receipt-index.json", index)
    require(_unchanged(plan), "Primary frozen evidence changed during diagnostic")
    failed = [row["job_id"] for row, _ in observed if row["status"] != "verified"]
    if failed:
        save_new(output / "results.json", dict(protocol=PROTOCOL, status="execution_failed", failed_jobs=failed,
                 plan_sha256=plan_sha, invocation_count=0, primary_scores_changed=False, inputs_unchanged=True))
        raise ValueError("Diagnostic execution failed; retained receipts require investigation, never automatic retry")
    matrices = defaultdict(dict)
    for row, outcomes in observed:
        matrices[(row["project_id"], row["stage_index"])][row["source_id"]] = {
            outcome["id"]: {key: outcome[key] for key in ("passed", "status")} for outcome in outcomes}
    groups = [analyze_group(group, matrices[(group["project_id"], group["stage_index"])]) for group in plan["groups"]]
    result = dict(protocol=PROTOCOL, status="finished" if all(group["qualified"] for group in groups) else "qualification_failed",
        purpose=PURPOSE, plan_sha256=plan_sha, invocation_count=0, physical_candidate_executions=len(observed),
        primary_scores_changed=False, inputs_unchanged=True, groups=groups,
        limitations=["Secondary post-freeze counterfactuals do not change the actual policy decisions or primary acceptance.",
            "Initial and final pools are separate; the ultimately selected repaired source may be outside its initial pool, so same-pool regret is then undefined.",
            "Own-history evidence and cross-policy pooled evidence are separate counterfactual scopes. Pooled evidence was unavailable to the original selector.",
            "Own-history reviewer evidence applied to initial pools is also retrospective: it can have been generated after reviews and repairs. Candidate-blind scouts are separately identified.",
            "Scope eligibility and anonymous tie order are frozen before diagnostics; private outcomes never determine counterfactual selection.",
            "Mutant families measure sensitivity to this frozen fault bank, not all real software bugs. Repeated probe kill vectors are observed redundancy only.",
            "Candidate sources share history and may be identical. Each unique source is physically executed once per project/stage/purpose; occurrences reuse that explicitly bound observation."])
    save_new(output / "results.json", result)
    return result


def audit(output):
    """Read-only independent row reconciliation; never launches a validator."""
    output = Path(output).resolve()
    bound = BoundInputs(output)
    plan = bound.read("plan.json")
    index = bound.read("receipt-index.json")
    result = bound.read("results.json")
    plan_sha = bound.hashes["plan.json"]
    require(plan["protocol"] == index["protocol"] == result["protocol"] == PROTOCOL
            and plan["purpose"] == index["purpose"] == result["purpose"] == PURPOSE
            and index["plan_sha256"] == result["plan_sha256"] == plan_sha
            and plan["invocation_count"] == index["invocation_count"] == result["invocation_count"] == 0,
            "Diagnostic protocol, purpose, or plan binding differs")
    require(result["status"] == "finished" and result["primary_scores_changed"] is False
            and result["inputs_unchanged"] is True, "Only qualified complete diagnostics can be certified")
    # Rebuild identities from the frozen study, not from mutable output claims.
    rebuilt = prepare(plan["primary_run"], require_current_evaluator=False)
    for key, value in rebuilt.items():
        if key != "runtime":
            require(plan.get(key) == value, "Diagnostic plan differs from original frozen inputs: " + key)
    require(_unchanged(plan), "Primary inputs changed before diagnostic audit")
    require(type(plan.get("max_workers")) is int and 1 <= plan["max_workers"] <= 2,
            "Diagnostic concurrency contract is outside its frozen bound")
    require(hashlib.sha256(bound.raw("evaluator.py")).hexdigest()
            == plan["diagnostic_source_sha256"] == plan["frozen_diagnostic_source_sha256"],
            "Diagnostic evaluator bytes differ from the pre-live source contract")
    jobs = {(group["project_id"], group["stage_index"], source_id): (group, source)
            for group in plan["groups"] for source_id, source in group["sources"].items()}
    identities = [(row["project_id"], row["stage_index"], row["source_id"]) for row in index["rows"]]
    require(len(identities) == len(set(identities)) == len(jobs) and set(identities) == set(jobs),
            "Missing, duplicate, or unplanned matrix execution")
    require(index["planned_jobs"] == index["evaluation_attempts"] == index["confirmed_candidate_executions"]
            == result["physical_candidate_executions"] == len(jobs), "Matrix execution counts differ")
    matrices, rows_seen, receipts_seen = defaultdict(dict), set(), set()
    concurrency_events = []
    for row in index["rows"]:
        group, source = jobs[(row["project_id"], row["stage_index"], row["source_id"])]
        expected_job = f"{group['project_id']}-stage-{group['stage_index']}-{row['source_id']}"
        require(row["job_id"] == expected_job, "Matrix job identity changed")
        row_path, receipt_path = f"rows/{expected_job}.json", f"receipts/{expected_job}.json"
        require(bound.read(row_path) == row and row["receipt_path"] == receipt_path, "Row sidecar or receipt path differs")
        receipt = bound.read(receipt_path)
        require(bound.hashes[receipt_path] == row["receipt_sha256"], "Raw typed receipt changed")
        for key, expected in dict(source_sha256=digest(source["files"]), suite_sha256=group["suite_sha256"],
                purpose=PURPOSE, ordered_case_ids=[case["id"] for case in group["cases"]],
                scope_provenance=source["occurrences"], image=plan["contract"]["image"],
                adapter_sha256=plan["adapter_sha256"], runtime=plan["runtime"],
                case_timeout_seconds=plan["contract"]["case_timeout_seconds"],
                suite_timeout_seconds=plan["contract"]["suite_timeout_seconds"],
                global_freeze_sha256=plan["global_freeze_sha256"], plan_sha256=plan_sha,
                status="verified", error=None, physical_execution=True, evaluation_attempted=True, invocation_count=0).items():
            require(type(row.get(key)) is type(expected) and row[key] == expected, "Diagnostic row binding differs: " + key)
        start, finish = row["started_monotonic_ns"], row["finished_monotonic_ns"]
        require(type(start) is int and type(finish) is int and finish >= start >= plan["freeze_monotonic_ns"]
                and row["elapsed_seconds"] == (finish - start) / 1e9
                and datetime.fromisoformat(row["started_utc"]) >= datetime.fromisoformat(plan["frozen_utc"]),
                "Diagnostic execution timing precedes freeze or duration is inconsistent")
        if finish > start:
            concurrency_events.extend(((start, 1), (finish, -1)))
        outcomes = receipt_rows(receipt, source["files"], group["cases"], plan["contract"])
        matrices[(row["project_id"], row["stage_index"])][row["source_id"]] = {
            outcome["id"]: {key: outcome[key] for key in ("passed", "status")} for outcome in outcomes}
        rows_seen.add(row_path)
        receipts_seen.add(receipt_path)
    require({str(path.relative_to(output)) for path in (output / "rows").glob("*.json")} == rows_seen
            and {str(path.relative_to(output)) for path in (output / "receipts").glob("*.json")} == receipts_seen,
            "Orphan diagnostic rows or receipts")
    active = peak = 0
    for _, change in sorted(concurrency_events):
        active += change
        peak = max(peak, active)
    require(active == 0 and peak <= plan["max_workers"], "Observed executions exceed bounded concurrency")
    recomputed = [analyze_group(group, matrices[(group["project_id"], group["stage_index"])]) for group in plan["groups"]]
    require(all(group["qualified"] for group in recomputed) and recomputed == result["groups"],
            "Control/fault qualification or derived selection/fault scores disagree")
    require(bound.unchanged() and _unchanged(plan), "Evidence changed during diagnostic audit")
    return dict(protocol="evidence-frontier-diagnostic-audit-v1", passed=True,
        plan_sha256=plan_sha, results_sha256=bound.hashes["results.json"],
        receipt_index_sha256=bound.hashes["receipt-index.json"], input_primary_results_sha256=plan["input_primary_results_sha256"],
        primary_contract_sha256=plan["primary_contract_sha256"], global_freeze_sha256=plan["global_freeze_sha256"],
        input_timings_sha256=plan["input_timings_sha256"], invocation_count=0,
        verified_matrix_rows=len(jobs), audited_groups=len(recomputed), inputs_unchanged=True,
        observed_peak_concurrency=peak, configured_max_workers=plan["max_workers"],
        auditor_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        limitations=["Retained trusted-host consistency, not external execution attestation.",
            "This separately audits the diagnostic matrix and scores; the primary study needs its own independent audit."])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "audit"))
    parser.add_argument("--study", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-workers", type=int, default=1)
    args = parser.parse_args()
    if args.command == "run":
        require(args.study is not None, "A frozen study is required")
        result = run(args.study, args.output, max_workers=args.max_workers)
        print(json.dumps(dict(status=result["status"], executions=result["physical_candidate_executions"], invocation_count=0)))
    else:
        result = audit(args.output)
        save_new(args.output / "audit.json", result)
        print(json.dumps(dict(passed=result["passed"], verified_matrix_rows=result["verified_matrix_rows"], invocation_count=0)))


if __name__ == "__main__":
    main()
