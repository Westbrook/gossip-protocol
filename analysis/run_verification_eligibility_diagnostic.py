#!/usr/bin/env python3
"""Secondary observation of two retained sources after the entire study freezes.

This evaluates the preselected B/C source trees against the 36 cumulative private
cases through milestone three. It changes no primary score, source, or decision;
it is not a replay of a different reviewer or controller policy.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from pathlib import Path
import re
import sys
import time

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY))
from analysis import run_verification_pool_diagnostic as frozen
from gossip_harness.blackbox_validator import BlackboxValidator
from gossip_harness.gitstore import GitStore
from gossip_harness.sustained_checkpoint import save_checkpoint

PROTOCOL = "verification-stage2-retained-availability-diagnostic-v1"
FINDING_SHA256 = "eaf77e84b9efc1cbcfc9e27c76a11140719077bd127cf1b0ee43a49ab584879c"
DEFAULT_FINDING = REPOSITORY / "analysis/verification-eligibility-finding.json"
RUN_ID = "buildgraph-portfolio-reviewed-0"
LABELS = ("candidate_B_latest_ineligible", "final_retained_candidate_C")
require, digest, sha, decode = frozen.require, frozen.digest, frozen.sha, frozen.decode


def prepare(study, output, finding_path=DEFAULT_FINDING):
    """Read immutable sources and bind the preplanned suite before any execution."""
    frozen.validate_output(study, output)
    study = Path(study).resolve()
    inputs = {}

    def read(path):
        path = Path(path).resolve()
        require(path.is_file() and not path.is_symlink(), "Expected frozen ordinary file: " + str(path))
        raw = path.read_bytes()
        inputs[path] = raw
        return raw

    def data(path):
        return decode(read(path))

    raw = read(finding_path)
    require(sha(raw) == FINDING_SHA256, "Preplanned eligibility finding changed")
    finding = decode(raw)
    require(finding.get("purpose") == "verification-retained-source-eligibility-finding-v1"
            and Path(finding["study_root"]).resolve() == study, "Finding belongs to another study")
    report = data(study / "results.json")
    frozen.completion_gate(report, data(study / "preregistered.json"))
    contract = report["contract"]
    require(report["contract_sha"] == finding["study_contract_sha256"], "Finding contract changed")
    require(contract.get("controller_runtime") == frozen.runtime(), "Use the frozen controller runtime")
    require(set(contract["sources"]) == set(frozen.CORE), "Frozen helper roster changed")
    require(re.fullmatch(r"sha256:[a-f0-9]{64}", contract.get("image", "")) is not None,
            "Frozen image must be content-addressed")
    for name, expected in contract["sources"].items():
        require(Path(name).name == name and name.endswith(".py"), "Unsafe frozen source name")
        require(sha(read(study / "source-snapshot/gossip_harness" / name)) == expected
                and sha(read(REPOSITORY / "gossip_harness" / name)) == expected,
                "Frozen helper source changed: " + name)
    read(Path(__file__))
    read(Path(frozen.__file__))
    require(sha(read(study / "study-plan.json")) == contract["plan_sha256"], "Frozen study plan changed")

    # The old results.json snapshot intentionally remains historical; finalized
    # artifacts captured by the partial finding must still be byte-identical.
    for item in finding["inputs"]:
        if not item["snapshot_only_mutable"]:
            require(sha(read(item["path"])) == item["sha256"], "Preplanned evidence changed: " + item["path"])
    fixtures_path = study / "fixtures.json"
    fixtures = data(fixtures_path)["projects"]
    require(digest(fixtures) == contract["fixture_sha256"] == finding["fixture_binding"]["projects_sha256"]
            and sha(inputs[fixtures_path]) == finding["fixture_binding"]["file_sha256"], "Frozen fixture changed")
    project = next(project for project in fixtures if project["id"] == "buildgraph")
    suite = frozen.execution_cases([case for stage in project["stages"][:3] for case in stage["hidden_cases"]])
    planned = finding["optional_postfreeze_diagnostic"]
    evaluation = planned["evaluation"]
    require(planned["purpose"] == PROTOCOL and planned["source_labels"] == list(LABELS)
            and planned["maximum_fresh_validations"] == 2 and planned["maximum_per_source"] == 1,
            "Preplanned diagnostic purpose or bounds changed")
    require(evaluation["project_id"] == "buildgraph" and evaluation["cumulative_stage_indices"] == [0, 1, 2]
            and evaluation["case_kind"] == "hidden_cases" and evaluation["case_count"] == len(suite) == 36
            and len({case["id"] for case in suite}) == 36 and digest(suite) == evaluation["suite_sha256"],
            "Preplanned 36-case private suite changed")
    require(all(evaluation[key] == contract[key] for key in ("image", "case_timeout_seconds", "suite_timeout_seconds"))
            and contract["case_timeout_seconds"] == 12 and contract["suite_timeout_seconds"] == 300
            and evaluation["validator_source_sha256"] == contract["sources"]["blackbox_validator.py"],
            "Preplanned execution environment changed")

    root = study / RUN_ID
    primary = next(row for row in report["cases"] if row["run_id"] == RUN_ID)
    require(Path(primary["root"]).resolve() == root and data(root / "result.json") == primary,
            "Primary result binding changed")
    trajectory_raw = read(root / "trajectory.json")
    require(sha(trajectory_raw) == primary["trajectory_sha256"], "Frozen trajectory changed")
    trajectory = decode(trajectory_raw)
    require(trajectory["contract_sha"] == report["contract_sha"] and len(trajectory["stages"]) == 3,
            "Preplanned stopped trajectory changed")
    stage_root = root / "stage-2"
    stage = data(stage_root / "result.json")
    require(trajectory["stages"][2] == stage and stage["stage_index"] == 2 and stage["completed"] is False
            and stage["selected"] == "candidate-C", "Preplanned stopped stage changed")

    trusted = dict(project["initial_files"])
    for milestone in project["stages"][:3]:
        trusted.update(milestone.get("trusted_updates", {}))
    sources = {}
    for label in ("candidate_B_prior_valid", *LABELS):
        record = finding["sources"][label]
        path, export = Path(record["git_store"]).resolve(), Path(record["export_path"]).resolve()
        require(path.is_relative_to(stage_root) and path.suffix == ".git" and export == path.with_suffix(".json"),
                "Preplanned source escapes its stage")
        stored = data(export)
        binding = dict(store_path=str(path), tip_sha=record["git_tip_sha"], files_sha256=record["source_sha256"])
        require(sha(inputs[export]) == record["export_sha256"] and stored["binding"] == binding,
                "Preplanned Git export changed")
        store = GitStore(path)
        files = store.read_files(binding["tip_sha"])
        require(stored["files"] == files and digest(files) == record["source_sha256"]
                and store._git("rev-parse", binding["tip_sha"] + "^{tree}") == record["git_tree_sha"]
                and {name: sha(text.encode()) for name, text in files.items()} == record["file_sha256"],
                "Preplanned immutable Git source changed")
        require(set(files) == set(trusted) and all(files[name] == trusted[name]
                for name in set(trusted) - set(project["allowed_paths"])), "Trusted source or fixed CLI changed")
        history = [row for row in stage["trajectory"] if row["kind"] == "builder"
                   and row["candidate"] == record["candidate"] and row["round"] == record["builder_round"]]
        require(len(history) == 1 and history[0]["binding"] == binding
                and history[0]["source_valid"] is record["source_valid"], "Preplanned proposal history changed")
        BlackboxValidator._inputs(files, suite)  # Static input validation only.
        sources[label] = dict(label=label, candidate=record["candidate"], binding=binding,
                              source_valid=record["source_valid"], files=files, source_sha256=digest(files))
    before, after = sources["candidate_B_prior_valid"], sources[LABELS[0]]
    selected = sources[LABELS[1]]
    require(before["source_valid"] is True and after["source_valid"] is False and before["files"] == after["files"],
            "Preplanned same-source demotion changed")
    require(stage["candidates"]["candidate-B"]["binding"] == after["binding"]
            and stage["candidates"]["candidate-B"]["source_valid"] is False
            and stage["selected_binding"] == selected["binding"] and selected["source_valid"] is True
            and trajectory["files"] == stage["files"] == selected["files"]
            and primary["files_sha256"] == selected["source_sha256"], "Final retained choices changed")
    require([sources[label]["source_sha256"] for label in LABELS] == planned["source_sha256"],
            "Preplanned evaluated source roster changed")
    return dict(inputs=inputs, contract=contract, suite=suite, sources=[sources[label] for label in LABELS],
                study=study, report=report, finding=finding)


def run(study, output, finding_path=DEFAULT_FINDING):
    plan = prepare(study, output, finding_path)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "inputs").mkdir()
    (output / "receipts").mkdir()
    inventory = []
    for index, (path, raw) in enumerate(sorted(plan["inputs"].items(), key=lambda pair: str(pair[0]))):
        retained = Path("inputs") / f"{index:04d}-{path.name}"
        (output / retained).write_bytes(raw)
        inventory.append(dict(original=str(path), retained=str(retained), sha256=sha(raw)))
    save_checkpoint(output / "manifest.json", dict(protocol=PROTOCOL, preregistered_primary=False,
        primary_scores_modified=False, model_calls=0, promotions=0, maximum_fresh_validations=2,
        original_study=str(plan["study"]), original_contract_sha256=plan["report"]["contract_sha"],
        finding_sha256=FINDING_SHA256, suite_sha256=digest(plan["suite"]), case_count=36,
        image=plan["contract"]["image"], case_timeout_seconds=12, suite_timeout_seconds=300,
        adapter_sha256=frozen.ADAPTER_SHA256, inputs=inventory,
        sources=[{key: value for key, value in source.items() if key != "files"} for source in plan["sources"]],
        limitations=plan["finding"]["optional_postfreeze_diagnostic"]["limitations"]))
    result = dict(protocol=PROTOCOL, status="running", primary_scores_modified=False, model_calls=0, promotions=0,
                  sources=[], executions=[], unexecuted=list(LABELS), active_source=None, started_at=time.time())
    save_checkpoint(output / "results.json", result)
    try:
        frozen.unchanged(plan["inputs"])
        validator = BlackboxValidator(plan["contract"]["image"], timeout_seconds=300, case_timeout_seconds=12)
        okay, detail = validator.preflight()
        require(okay, "Docker preflight failed: " + detail)
        for source in plan["sources"]:
            frozen.unchanged(plan["inputs"])
            label = source["label"]
            path = output / "receipts" / (label + ".json")
            execution = dict(label=label, source_sha256=source["source_sha256"], suite_sha256=digest(plan["suite"]),
                             receipt=str(path.relative_to(output)), verified=False)
            result["executions"].append(execution)
            result["unexecuted"].remove(label)
            result["active_source"] = label
            save_checkpoint(output / "results.json", result)
            try:
                receipt = validator.evaluate(dict(source["files"]), deepcopy(plan["suite"]))
            except BaseException as error:
                save_checkpoint(path, dict(error=type(error).__name__, last_receipt=getattr(validator, "last_receipt", {})))
                execution.update(raised=type(error).__name__, receipt_sha256=sha(path.read_bytes()))
                raise
            save_checkpoint(path, receipt)
            execution["receipt_sha256"] = sha(path.read_bytes())
            save_checkpoint(output / "results.json", result)
            frozen.receipt_rows(receipt, source["files"], plan["suite"], plan["contract"])
            execution["verified"] = True
            result["sources"].append(dict(label=label, binding=source["binding"], original_source_valid=source["source_valid"],
                **frozen.score(receipt, plan["suite"]), receipt=execution["receipt"], receipt_sha256=execution["receipt_sha256"]))
            result["active_source"] = None
            save_checkpoint(output / "results.json", result)
        frozen.unchanged(plan["inputs"])
        result.update(status="finished", inputs_unchanged=True)
    except BaseException as error:
        result.update(status="interrupted", error_type=type(error).__name__)
        raise
    finally:
        result["finished_at"] = time.time()
        save_checkpoint(output / "results.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True, help="The entire finished live study")
    parser.add_argument("--output", type=Path, required=True, help="Fresh secondary diagnostic directory")
    args = parser.parse_args()
    result = run(args.run, args.output)
    print({"status": result["status"], "evaluated_sources": len(result["sources"]), "primary_scores_modified": False})
    return 0 if result["status"] == "finished" else 2


if __name__ == "__main__":
    raise SystemExit(main())
