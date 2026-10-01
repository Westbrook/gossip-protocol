from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from gossip_harness.sustained_audit import audit_run, digest, summarize_completed


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def receipt(files, cases, passed=True):
    return dict(source_sha256=digest(files), suite_sha256=digest(cases),
                cleanup_verified=True, status="passed" if passed else "failed", passed=passed,
                outcomes=[dict(index=index, passed=passed, actual=case["expected"] if passed else None)
                          for index, case in enumerate(cases)])


def bundle(root):
    """Build data-only evidence and a fake immutable Git object reader."""
    stores = {}

    def git(path, files):
        sha = hashlib.sha1((str(path) + digest(files)).encode()).hexdigest()
        stores[str(path.resolve())] = (sha, deepcopy(files))
        return dict(store_path=str(path), tip_sha=sha, files_sha256=digest(files))

    class ReadOnlyGit:
        def __init__(self, path):
            self.value = stores[str(Path(path).resolve())]

        def head(self):
            return self.value[0]

        def read_files(self, sha):
            if sha != self.value[0]:
                raise ValueError("Unknown fake Git commit")
            return deepcopy(self.value[1])

    initial = {"engine.py": "initial candidate source", "README.md": "trusted"}
    stages = [dict(requirements=[f"R{index}"], spec=f"Milestone {index}",
                   visible_cases=[dict(id=f"v{index}", input=index, expected=index, requirement=f"R{index}")],
                   hidden_cases=[dict(id=f"h{index}", input=index + 10, expected=index, requirement=f"R{index}")])
              for index in range(3)]
    project = dict(id="example", title="Example", initial_files=initial, stages=stages, allowed_paths=["engine.py"])
    fixture = [project]
    source = root / "source-snapshot" / "gossip_harness" / "example.py"
    source.parent.mkdir(parents=True)
    source.write_text("raise RuntimeError('Snapshot execution is forbidden')\n")
    contract = dict(project_ids=["example"], policies=["strong-single"], milestones=3,
                    baseline_calls_per_stage=8, portfolio_repairs=4, reviewer_calls_per_stage=5,
                    sources={"example.py": hashlib.sha256(source.read_bytes()).hexdigest()},
                    fixture_sha256=digest(fixture))
    run_id = "example-strong-single-0"
    case_root = root / run_id
    ledger = root / "budget.sqlite"
    config = dict(root=str(case_root), contract_sha=digest(contract), project_id="example", policy="strong-single",
                  repetition=0, run_id=run_id, namespace="test-study/" + run_id, budget_ledger=str(ledger))
    save(case_root / "config.json", config)
    git(case_root / "initial.git", initial)
    state = dict(contract_sha=digest(contract), files=initial, stages=[], invocations=[], completed_stages=[],
                 pid=101, next_stage=0, budget_ledger=str(ledger))
    traces = []
    billing_records = []
    for stage_index in range(3):
        before = state["files"]
        files = {**before, "engine.py": "candidate source " + str(stage_index)}
        stage_root = case_root / f"stage-{stage_index}"
        label = f"stage-{stage_index}-strong-single-single-build-1"
        binding = git(stage_root / (label + ".git"), files)
        save(stage_root / (label + ".json"), dict(binding=binding, files=files))
        visible = [case for item in stages[:stage_index + 1] for case in item["visible_cases"]]
        validation = receipt(files, visible)
        save(stage_root / (label + ".validation.json"), validation)
        control = dict(action="complete", remaining=[], notes="Checkpoint")
        row = dict(kind="builder", candidate="single", round=1, binding=binding,
                   files_sha256=digest(files), source_valid=True, control=control,
                   complete=True, visible_passed=True, regressions=[], stagnant=False, errors=[])
        billing = config["namespace"] + f"/stage-{stage_index}"
        call_id = "builder-single-1"
        invocation = dict(call_id=call_id, reservation=billing + "/" + call_id,
                          usage_units=1, role="builder", model="strong", metadata={}, seconds=0.1)
        prior = state["stages"][-1] if state["stages"] else {}
        context = dict(project_id="example", project_title="Example", stage_index=stage_index,
                       milestones=[dict(index=index, spec=item["spec"], requirements=item["requirements"])
                                   for index, item in enumerate(stages[:stage_index + 1])],
                       visible_cases=visible, prior_agent_notes=prior.get("notes", ""),
                       prior_machine_history=prior.get("check_history", []),
                       current_agent_notes=prior.get("notes", ""), machine_history=[])
        request = dict(task_id=billing + "/" + call_id,
                       files={**before, "__stage_context__.json": json.dumps(context)},
                       allowed_paths=["engine.py", "control.json"], feedback=json.dumps({"resume_notes": prior.get("notes", "")}))
        response = {**invocation, "changes": {"engine.py": files["engine.py"], "control.json": json.dumps(control)}}
        save(stage_root / "requests" / (call_id + ".request.json"), request)
        save(stage_root / "requests" / (call_id + ".result.json"), response)
        traces.append(dict(kind="request_started", stage_index=stage_index, call_id=call_id,
                           request_sha256=digest(request), reserved_units=10, model="strong"))
        metrics = dict(builder_calls=1, reviewer_calls=0, repairs=0, premature_completion=0,
                       regressions=0, stagnation_events=0)
        result = dict(stage_index=stage_index, files=files, selected="single", selected_binding=binding,
                      visible_receipt=validation, completed=True, status="complete", trajectory=[row],
                      metrics=metrics, invocations=[invocation], notes="Checkpoint",
                      check_history=prior.get("check_history", []) + [dict(
                          stage_index=stage_index, candidate="single", round=1, passed=True,
                          outcomes=[dict(id=case["id"], requirement=case["requirement"], input=case["input"],
                                         expected=case["expected"], passed=True, status=None, actual=case["expected"])
                                    for case in visible], errors=[], regressions=[], stagnant=False)])
        save(stage_root / "result.json", result)
        record = git(stage_root / "record.git", {"record.json": json.dumps({
            "completed": True, "status": "complete", "selected_binding": binding})})
        billing_records.append((billing, record["tip_sha"], invocation))
        checkpoint = git(stage_root / "checkpoint.git", files)
        state["stages"].append(result)
        state["files"] = files
        state["invocations"].append(invocation)
        state["completed_stages"].append(dict(stage_index=stage_index, completed=True, head=checkpoint["tip_sha"]))
        state.update(store_path=checkpoint["store_path"], head=checkpoint["tip_sha"], next_stage=stage_index + 1)
        if stage_index == 1:
            save(case_root / "checkpoint.json", state)
            sha = hashlib.sha256((case_root / "checkpoint.json").read_bytes()).hexdigest()
            save(case_root / "handoff.json", dict(checkpoint_sha256=sha, previous_pid=101, head=state["head"]))
            resumed = dict(checkpoint_sha256=sha, previous_pid=101, resumed_pid=202, head=state["head"],
                           files_sha256=digest(files), verified=True)
            save(case_root / "resume.json", resumed)
            state["pid"] = 202
    save(case_root / "trajectory.json", state)
    (case_root / "trace-begin.jsonl").write_text("\n".join(json.dumps(row) for row in traces) + "\n")
    final_files = state["files"]
    release = git(case_root / "release.git", final_files)
    for path, text in final_files.items():
        destination = case_root / "accepted" / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text)
    case = dict(project_id="example", policy="strong-single", repetition=0, run_id=run_id,
                accepted=True, milestones_completed=3, status="accepted", root=str(case_root),
                files_sha256=digest(final_files), trajectory_sha256=hashlib.sha256((case_root / "trajectory.json").read_bytes()).hexdigest(),
                release_head=release["tip_sha"], exact_tested_sha=release["tip_sha"],
                final_visible=receipt(final_files, [case for stage in stages for case in stage["visible_cases"]]),
                final_hidden=receipt(final_files, [case for stage in stages for case in stage["hidden_cases"]]),
                historical=[dict(stage_index=index, binding=result["selected_binding"],
                                 hidden=receipt(result["files"], [case for stage in stages[:index + 1] for case in stage["hidden_cases"]]))
                            for index, result in enumerate(state["stages"])],
                requirement_coverage={f"R{index}": True for index in range(3)}, handoff=resumed,
                metrics={name: sum(stage["metrics"][name] for stage in state["stages"]) for name in state["stages"][0]["metrics"]},
                usage_micro_usd=3, invocations=state["invocations"])
    save(case_root / "result.json", case)
    report = dict(experiment="sustained-quality-v1", mode="rehearsal", contract=contract, contract_sha=digest(contract), repetitions=1,
                  cases=[case], unexecuted=[], status="finished", incremental_micro_usd=3,
                  budget_before={"spent_or_reserved": 0}, budget={"spent_or_reserved": 3})
    save(root / "results.json", report)
    save(root / "fixtures.json", fixture)
    save(root / "preregistered.json", dict(contract=contract, roster=[["example", "strong-single", 0]]))
    db = sqlite3.connect(ledger)
    db.executescript("""
        CREATE TABLE reservations(id TEXT, task_id TEXT, amount INTEGER, spent INTEGER, state TEXT);
        CREATE TABLE tasks(id TEXT, status TEXT, accepted_commit TEXT, intent_id TEXT);
        CREATE TABLE intents(state TEXT);
        CREATE TABLE settings(key TEXT, value INTEGER);
        INSERT INTO settings VALUES('budget',100);
    """)
    for billing, head, invocation in billing_records:
        db.execute("INSERT INTO tasks VALUES(?,?,?,NULL)", (billing, "complete", head))
        db.execute("INSERT INTO reservations VALUES(?,?,?,?,?)", (invocation["reservation"], billing, 10, 1, "settled"))
    db.commit()
    db.close()
    return ReadOnlyGit, report


class SustainedAuditTests(unittest.TestCase):
    def test_complete_evidence_audits_without_importing_or_executing_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reader, _ = bundle(root)
            with patch("gossip_harness.sustained_audit.GitStore", reader):
                audit = audit_run(root)
            self.assertTrue(audit["passed"])
            self.assertEqual(audit["verified_usage_micro_usd"], 3)
            self.assertEqual(audit["verified_billing_tasks"], 3)
            self.assertEqual(audit["summary"]["policies"]["strong-single"]["accepted"], 1)

    def test_tampered_evidence_is_rejected(self):
        for target in ("source", "trajectory", "proposal", "validation", "request", "export", "ledger", "handoff",
                       "control", "context", "model", "history"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                reader, report = bundle(root)
                case_root = Path(report["cases"][0]["root"])
                stage = case_root / "stage-0"
                if target == "source":
                    (root / "source-snapshot/gossip_harness/example.py").write_text("tampered")
                elif target == "trajectory":
                    (case_root / "trajectory.json").write_text("{}")
                elif target == "proposal":
                    path = stage / "stage-0-strong-single-single-build-1.json"
                    value = json.loads(path.read_text())
                    value["files"]["engine.py"] = "tampered"
                    save(path, value)
                elif target == "validation":
                    path = stage / "stage-0-strong-single-single-build-1.validation.json"
                    value = json.loads(path.read_text())
                    value["outcomes"][0]["actual"] = "wrong despite pass"
                    save(path, value)
                elif target == "request":
                    path = stage / "requests/builder-single-1.request.json"
                    value = json.loads(path.read_text())
                    value["task_id"] = "wrong"
                    save(path, value)
                elif target == "export":
                    (case_root / "accepted/engine.py").write_text("tampered")
                elif target == "ledger":
                    db = sqlite3.connect(root / "budget.sqlite")
                    db.execute("UPDATE reservations SET spent=2")
                    db.commit()
                    db.close()
                elif target == "handoff":
                    path = case_root / "resume.json"
                    value = json.loads(path.read_text())
                    value["resumed_pid"] = value["previous_pid"]
                    save(path, value)
                elif target == "control":
                    path = stage / "requests/builder-single-1.result.json"
                    value = json.loads(path.read_text())
                    value["changes"]["control.json"] = json.dumps({"action": "continue", "notes": "", "remaining": []})
                    save(path, value)
                elif target == "context":
                    path = stage / "requests/builder-single-1.request.json"
                    value = json.loads(path.read_text())
                    value["files"]["hidden-evidence.json"] = "private answers"
                    save(path, value)
                elif target == "model":
                    path = stage / "requests/builder-single-1.result.json"
                    value = json.loads(path.read_text())
                    value["model"] = "cheap"
                    save(path, value)
                elif target == "history":
                    path = case_root / "stage-1/requests/builder-single-1.request.json"
                    value = json.loads(path.read_text())
                    context = json.loads(value["files"]["__stage_context__.json"])
                    context["prior_machine_history"] = []
                    value["files"]["__stage_context__.json"] = json.dumps(context)
                    save(path, value)
                with patch("gossip_harness.sustained_audit.GitStore", reader), self.assertRaises(ValueError):
                    audit_run(root)

    def test_partial_summary_never_counts_unexecuted_projects_as_results(self):
        with tempfile.TemporaryDirectory() as directory:
            _, report = bundle(Path(directory))
            report["status"] = "interrupted"
            report["unexecuted"] = [["later", "strong-single", 0]]
            summary = summarize_completed(report)
            self.assertEqual(summary["completed_case_records"], 1)
            self.assertEqual(summary["policies"]["strong-single"]["project_runs"], 1)
            self.assertIn("Partial study", summary["warning"])

    def test_selected_hidden_recovery_is_separate_from_proposal_regressions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reader, report = bundle(root)
            case = report["cases"][0]
            earlier = case["historical"][0]["hidden"]
            earlier.update(passed=False, status="failed")
            earlier["outcomes"][0].update(passed=False, actual=None)
            save(Path(case["root"]) / "result.json", case)
            save(root / "results.json", report)
            with patch("gossip_harness.sustained_audit.GitStore", reader):
                audit = audit_run(root)
            policy = audit["summary"]["policies"]["strong-single"]
            self.assertEqual(policy["selected_hidden_recoveries"], 1)
            self.assertEqual(policy["selected_hidden_regressions"], 0)
            self.assertEqual(policy["regressions"], 0)
            self.assertEqual(policy["builder_calls"], 3)
            self.assertEqual(audit["cases"][0]["selected_hidden_transitions"][0]["recovered_case_ids"], ["h0"])


if __name__ == "__main__":
    unittest.main()
