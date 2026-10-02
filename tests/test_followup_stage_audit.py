"""Hand-authored adversarial transcripts for independent stage certification.

Fixtures are synthetic data, not outputs of the production controller. They use
complete raw receipt shapes but do not claim physical Docker execution.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from analysis.audit_followup_stage import (
    CONTEXT, LIMITS, METRICS, PROTOCOL, SHARED_PROTOCOL, StageReplay, audit_stage,
    controller_prompts, digest, same_case_comparison, scout_parse, sha, terminal_reason,
)


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(",", ":"))


def receipt(files, cases, passed=True):
    rows = [dict(index=i, passed=passed, status="passed" if passed else "wrong_answer",
                 actual=case["expected"] if passed else {"incorrect": True}) for i, case in enumerate(cases)]
    return dict(source_sha256=digest(files), suite_sha256=digest(cases), image_id="sha256:test",
        timeout_seconds=30, case_timeout_seconds=5, cleanup_verified=True, outcomes=rows,
        passed=passed, status="passed" if passed else "failed")


def observation(passed=True):
    return dict(passed=passed, status="passed" if passed else "wrong_answer", actual=1 if passed else {"incorrect": True})


class Transcript:
    """A minimal fully specified imported-arm transcript, built from stated facts."""

    def __init__(self, root, mode="improved", notes=""):
        self.root, self.mode, self.run_id = Path(root).resolve(), mode, "warehouse-arm-0"
        self.root.joinpath("evaluations").mkdir()
        self.initial = {"app.py": "baseline"}
        self.cases = [dict(id="public", input={"é": 1}, expected=1, requirement="R")]
        self.project = dict(id="warehouse", title="Warehouse", allowed_paths=["app.py"], initial_files=self.initial,
            stages=[dict(specification="Return one", requirements=["R"], visible_cases=self.cases, hidden_cases=[])])
        self.contract = dict(image="sha256:test", suite_timeout_seconds=30, case_timeout_seconds=5)
        self.prompts = dict(builder_prompt="build", reviewer_prompt="review current", improved_reviewer_prompt="review improved",
                            scout_prompt="scout", escalation_suffix=" escalated", focus_suffix=" focused")
        self.aliases = [f"candidate-{letter}" for letter in "ABCD"]
        self.alias_map = {f"slot-{i}": alias for i, alias in enumerate(self.aliases)}
        self.baseline = dict(files_sha256=digest(self.initial), source_valid=True, approval_id="trusted baseline")
        self.sources, self.calls, self.spans = {}, {}, {}
        original, fresh = {}, {}
        for i, alias in enumerate(self.aliases):
            files = {"app.py": f"source-{i}"}
            binding = dict(tip_sha=f"original-{i}", files_sha256=digest(files))
            original_receipt = receipt(files, self.cases)
            item = dict(slot=f"slot-{i}", alias=alias, files=files, binding=binding, source_valid=True,
                latest_attempt_status="effective_source_proposal", latest_attempt_admissible=True,
                eligibility_origin=dict(kind="admissible_proposal", label=f"shared-build-{i}", files_sha256=digest(files)),
                notes=notes, remaining=[], reviewer_remaining=[], matrix={"public": observation()},
                validation_receipts=[dict(label=f"original-{i}", case_ids=["public"], receipt=original_receipt)])
            original[alias] = deepcopy(item)
            current = deepcopy(item)
            current["binding"] = dict(tip_sha=f"consumer-{i}", files_sha256=digest(files))
            current["shared_original_binding"] = binding
            current["shared_original_validation_receipts"] = deepcopy(item["validation_receipts"])
            label = f"stage-0-shared-slot-{i}-fresh-evidence"
            current["validation_receipts"] = [dict(label=label, case_ids=["public"], receipt=original_receipt)]
            fresh[alias] = current
            self.sources[current["binding"]["tip_sha"]] = files
            path = self.root / "evaluations" / f"{label}.json"
            self.save(path, original_receipt)
            self.spans[i] = dict(kind="docker_validation", run_id=self.run_id, stage_index=0,
                purpose="stage_evidence", status="finished", label=label, started_monotonic_ns=10 * i + 1,
                finished_monotonic_ns=10 * i + 2, receipt_path=str(path), receipt_sha256=sha(path), case_count=1)
        common = dict(protocol=PROTOCOL, project_id="warehouse", stage_index=0, policy="independent-four", formation="independent",
            alias_map=deepcopy(self.alias_map), candidate_order=deepcopy(self.aliases), evidence_cases=self.cases, evidence_sha256=digest(self.cases))
        original_pool = dict(**common, candidates=original)
        fresh_pool = dict(**common, candidates=deepcopy(fresh))
        self.shared = dict(pool=original_pool, binding=dict(pool=original_pool, pool_sha256=digest(original_pool),
            initial_files_sha256=digest(self.initial), candidate_seed=123, purpose="shared_initial_setup"),
            scouts=dict(proposals=[dict(scout_id=f"scout-{i}", probes=[]) for i in range(2)], rejected_batches=0))
        frozen = dict(protocol=SHARED_PROTOCOL, session_id=self.run_id, stage_index=0, pool=fresh_pool,
                      pool_sha256=digest(fresh_pool), frozen_monotonic_ns=50)
        self.save(self.root / "initial-pool.json", frozen)
        scouts = dict(source="shared_initial_setup", shared_manifest={"path": "authenticated-by-parent", "sha256": "parent"},
                      proposals=deepcopy(self.shared["scouts"]["proposals"]), calls=0, attributed_calls=2, rejected_batches=0,
                      admitted=0, rejected=0)
        self.save(self.root / "scouts.json", scouts)
        self.stage = dict(protocol=PROTOCOL, controller_mode=mode, formation="independent", stage_index=0, limits=deepcopy(LIMITS),
            completed=False, baseline_approval=self.baseline, initial_pool=fresh_pool, initial_pool_sha256=digest(fresh_pool),
            initial_freeze=dict(freeze_id=str(self.root / "initial-pool.json"), pool_sha256=digest(fresh_pool)),
            shared_setup={k: deepcopy(v) for k, v in self.shared["binding"].items() if k != "pool"}, scouts=scouts,
            candidate_order=deepcopy(self.aliases), alias_map=deepcopy(self.alias_map), trajectory=[], probe_receipts=[],
            escalations=[], repair_comparisons=[], metrics=dict.fromkeys(METRICS, 0))
        self.stage["metrics"]["shared_initial_builder_opportunities"] = 4
        self.candidates = fresh
        for i in range(2):
            self.gate(dict(project_id="warehouse", stage_index=0, source="scout", scout_id=f"scout-{i}"))
        self.previous = None

    @staticmethod
    def save(path, data):
        path.write_text(json.dumps(data, ensure_ascii=True))

    def gate(self, origin):
        self.stage["probe_receipts"].append(dict(mode="new", origin=origin,
            envelope=dict(mode="new", probes=[], existing_cases=deepcopy(self.cases), max_new=8),
            receipt=dict(mode="new", stage_index=0, oracle_assisted=True, admitted_cases=[], rejected=[],
                validation_policy="verification-fixture-oracle-v1", origin=origin, proposed_count=0,
                max_new=8, namespace="warehouse", content_sha256=hashlib.sha256(json.dumps({"probes": []}).encode()).hexdigest())))

    def review(self, number, *, action="accept", remaining=None, pending=None, alias="candidate-A", accepted=True):
        remaining = [] if remaining is None else remaining
        review = dict(candidate=alias, action=action, notes="check", remaining=remaining, probes=[])
        continuation = dict(protocol=PROTOCOL, limits=deepcopy(LIMITS), reviews_remaining_after_this=4 - number,
                            repairs_remaining=2 - self.stage["metrics"]["repairs"], escalations_remaining=1 - self.stage["metrics"]["escalations"])
        if self.mode == "improved":
            continuation.update(controller_mode="improved", repairs_without_failure_progress=0,
                                required_next_action=deepcopy(pending), repair_comparisons=[{k: deepcopy(row[k]) for k in ("review_round", "predecessor", "repaired", "case_ids", "resolved_failures", "introduced_failures", "persisting_failures", "regression", "failure_progress")} for row in self.stage["repair_comparisons"]])
        else:
            continuation.update(consecutive_reviews_without_progress=0 if pending else number - 1, escalation=deepcopy(pending))
        context = dict(project_id="warehouse", project_title="Warehouse", stage_index=0,
            milestones=[dict(index=0, specification="Return one", requirements=["R"])], prior_notes="", prior_remaining=[],
            prior_machine_history=[], verified_cases=deepcopy(self.cases),
            candidate_source_hashes={a: digest(item["files"]) for a, item in self.candidates.items()},
            candidate_eligibility={a: {k: item[k] for k in ("source_valid", "latest_attempt_status")} for a, item in self.candidates.items()},
            matrix={a: deepcopy(item["matrix"]) for a, item in self.candidates.items()}, previous_decision=deepcopy(self.previous),
            new_probe_capacity_remaining=8, continuation=continuation)
        files = {f"candidates/{a}/{path}": text for a, item in self.candidates.items() for path, text in item["files"].items()}
        files[CONTEXT] = canonical(context)
        meta = dict(project_id="warehouse", stage_index=0, policy=getattr(self, "engine_policy", "independent-four"), role="reviewer", round=number,
                    candidate_id="review", alias_by_slot=deepcopy(self.alias_map), protocol=PROTOCOL,
                    escalation_id=pending["id"] if pending and self.mode == "current" else None)
        span_id = 100 + number
        suffix = " escalated" if pending and self.mode == "current" else " focused" if pending and pending["action"] == "request_focused_probe" else ""
        self.calls[f"reviewer-review-{number}"] = (dict(files=files, instructions=f"review {self.mode}" + suffix,
            allowed_paths=["review.json"], feedback=""), dict(role="reviewer", model="strong", request_span_id=span_id,
            controller_metadata=meta, changes={"review.json": json.dumps(review)}))
        self.spans[span_id] = dict(kind="worker_request", started_monotonic_ns=60 + number * 20, finished_monotonic_ns=61 + number * 20)
        self.gate(dict(project_id="warehouse", stage_index=0, source="reviewer", review_round=number))
        self.candidates[alias]["reviewer_remaining"] = remaining
        row = dict(kind="reviewer", round=number, review=review, accepted=accepted, evidence_before_sha256=digest(self.cases),
                   errors=[], admitted_probe_ids=[], selected=alias, binding=deepcopy(self.candidates[alias]["binding"]),
                   evidence_after_sha256=digest(self.cases))
        self.stage["trajectory"].append(row)
        self.stage["metrics"]["reviewer_calls"] += 1
        self.stage["metrics"]["acceptances_without_new_probes"] += int(accepted)
        self.previous = {k: deepcopy(row[k]) for k in ("round", "review", "accepted", "errors", "admitted_probe_ids")}

    def route(self, number, action, **extra):
        row = dict(kind="routing", round=number, candidate="candidate-A", source_sha256=digest(self.candidates["candidate-A"]["files"]),
            evidence_sha256=digest(self.cases), executed_failure_ids=[], source_valid=True, focused_admitted_probe_ids=[],
            repairs_remaining=2, reviews_remaining=4 - number, failure_progress=False, action=action, **extra)
        self.stage["trajectory"].append(row)
        return deepcopy(row)

    def finish(self, *, completed=True, code="accepted", reason=None, routing=None, selected="candidate-A"):
        item = self.candidates[selected]
        if reason is None:
            reason = ("Explicit reviewer acceptance passed every active source and execution gate" if self.mode == "improved" else
                      "Reviewer acceptance passed every active public and oracle-admissible probe")
        notes = item["notes"]
        if self.mode == "improved" and "candidate-E" in notes:
            notes = ""
        self.stage.update(files=deepcopy(item["files"]), completed=completed, status="complete" if completed else "bounded_incomplete",
            reason=reason, reason_code=code, source_valid=True, selected=selected, selected_binding=deepcopy(item["binding"]),
            notes=notes, remaining=[] if completed else deepcopy(item["reviewer_remaining"]),
            check_history=[dict(stage_index=0, completed=completed, source_sha256=digest(item["files"]), evidence_sha256=digest(self.cases),
                                outcomes=[dict(case_id="public", requirement="R", passed=True)])],
            probe_pool=[], retired_probes=[], evidence_cases=deepcopy(self.cases), evidence_sha256=digest(self.cases),
            matrix={a: deepcopy(candidate["matrix"]) for a, candidate in self.candidates.items()}, attempts=self.stage["metrics"]["builder_calls"],
            candidates={a: {k: deepcopy(v) for k, v in candidate.items() if k != "files"} for a, candidate in self.candidates.items()})
        if routing is not None:
            self.stage["routing_stop"] = routing

    def failing_initial_pool(self):
        for alias in self.aliases:
            for item in (self.candidates[alias], self.shared["pool"]["candidates"][alias]):
                item["matrix"] = {"public": observation(False)}
                for row in item["validation_receipts"]:
                    row["receipt"] = receipt(item["files"], self.cases, False)
            original = self.shared["pool"]["candidates"][alias]
            self.candidates[alias]["shared_original_validation_receipts"] = deepcopy(original["validation_receipts"])
        for i, alias in enumerate(self.aliases):
            self.save(Path(self.spans[i]["receipt_path"]), self.candidates[alias]["validation_receipts"][0]["receipt"])
            self.spans[i]["receipt_sha256"] = sha(Path(self.spans[i]["receipt_path"]))
        self.shared["binding"]["pool_sha256"] = digest(self.shared["pool"])
        self.stage["shared_setup"]["pool_sha256"] = self.shared["binding"]["pool_sha256"]
        self.stage["initial_pool"]["candidates"] = deepcopy(self.candidates)
        self.stage["initial_pool_sha256"] = digest(self.stage["initial_pool"])
        self.stage["initial_freeze"]["pool_sha256"] = self.stage["initial_pool_sha256"]
        self.save(self.root / "initial-pool.json", dict(protocol=SHARED_PROTOCOL, session_id=self.run_id, stage_index=0,
            pool=self.stage["initial_pool"], pool_sha256=self.stage["initial_pool_sha256"], frozen_monotonic_ns=50))

    def current_progress(self, number):
        state = digest(dict(evidence=digest(self.cases), sources={alias: dict(source=digest(item["files"]), eligible=True)
            for alias, item in self.candidates.items()}))
        self.stage["trajectory"].append(dict(kind="progress", round=number, before_sha256=state, after_sha256=state,
            novel_progress=False, revisited_state=False, effective_source_changes=0, admitted_probes=0,
            consecutive_stagnant_reviews=number))
        return state

    def current_noop_escalation(self, state):
        predecessor = deepcopy(self.candidates["candidate-A"])
        identifier = "stage-0-escalation-1"
        label = "stage-0-build-slot-0-2"
        binding = dict(tip_sha="current-repair", files_sha256=digest(predecessor["files"]))
        observed = receipt(predecessor["files"], self.cases)
        context = dict(project_id="warehouse", project_title="Warehouse", stage_index=0,
            milestones=[dict(index=0, specification="Return one", requirements=["R"])], prior_notes="",
            prior_remaining=[], prior_machine_history=[], verified_cases=deepcopy(self.cases))
        feedback = dict(review_notes="check", remaining=["R"],
            evidence=[dict(case=deepcopy(self.cases[0]), outcome=observation())],
            continuation_escalation=dict(id=identifier,
                instruction="Review has stalled. Reconcile each unresolved requirement with the exact "
                    "current source and executed evidence. Make a concrete repair if justified. "
                    "Do not make cosmetic changes to manufacture progress. An unchanged or rejected "
                    "proposal preserves retained-source eligibility, but never grants acceptance."))
        meta = dict(project_id="warehouse", stage_index=0, policy="independent-four", role="builder", round=2,
                    candidate_id="slot-0", escalation_id=identifier, protocol=PROTOCOL, phase="repair")
        self.calls["builder-slot-0-2"] = (dict(files={**predecessor["files"], CONTEXT: canonical(context)}, instructions="build",
            allowed_paths=["app.py", "notes.json"], feedback=canonical(feedback)), dict(role="builder", model="strong",
                request_span_id=401, controller_metadata=meta, changes={}))
        self.spans[401] = dict(kind="worker_request", started_monotonic_ns=102, finished_monotonic_ns=103)
        path = self.root / "evaluations" / (label + "-all-evidence.json")
        self.save(path, observed)
        self.spans[402] = dict(kind="docker_validation", run_id=self.run_id, stage_index=0, purpose="stage_evidence",
            status="finished", label=label + "-all-evidence", started_monotonic_ns=104, finished_monotonic_ns=105,
            receipt_path=str(path), receipt_sha256=sha(path), case_count=1)
        candidate = deepcopy(predecessor)
        candidate.pop("shared_original_binding")
        candidate.pop("shared_original_validation_receipts")
        candidate.update(binding=binding, latest_attempt_status="no_effective_source_change",
                         validation_receipts=[dict(label=label + "-all-evidence", case_ids=["public"], receipt=observed)])
        self.candidates["candidate-A"] = candidate
        self.sources["current-repair"] = candidate["files"]
        self.stage["trajectory"].append(dict(kind="builder", slot="slot-0", candidate="candidate-A", round=2, phase="repair",
            inherited_from=None, source_valid=True, notes_valid=False, binding=binding, attempt_status="no_effective_source_change",
            attempt_admissible=True, effective_source_change=False, prior_source_valid=True,
            previous_files_sha256=digest(predecessor["files"]), model="strong", files_sha256=digest(candidate["files"]),
            visible_passed=True, failures=[]))
        escalation = dict(id=identifier, trigger="review_stagnation", review_round=2, candidate="candidate-A",
            consecutive_stagnant_reviews=2, state_before_sha256=state, source_before_sha256=digest(predecessor["files"]),
            evidence_before_sha256=digest(self.cases), action="strong_repair", repair_capacity_before=2, review_capacity_after=2,
            new_probe_capacity_remaining=8, state_after_sha256=state, source_after_sha256=digest(candidate["files"]),
            evidence_after_sha256=digest(self.cases), effective_source_change=False)
        self.stage["escalations"].append(deepcopy(escalation))
        self.stage["trajectory"].append(dict(kind="escalation", **deepcopy(escalation)))
        self.stage["metrics"].update(builder_calls=1, repairs=1, invalid_builder_notes=1, no_effective_source_proposals=1,
            retained_eligibility_preserved=1, escalations=1, escalated_repairs=1, stagnation_events=1, strongest_builder_calls=1)
        return escalation

    def append_passing_repair(self):
        predecessor = self.candidates["candidate-A"]
        files = {"app.py": "repaired"}
        label = "stage-0-build-repair-1-2"
        binding = dict(tip_sha="repair-1", files_sha256=digest(files))
        observed = receipt(files, self.cases)
        saved = dict(label=label + "-all-evidence", case_ids=["public"], receipt=observed)
        repaired = dict(slot="repair-1", alias="candidate-E", files=files, binding=binding, source_valid=True,
            latest_attempt_status="effective_source_proposal", latest_attempt_admissible=True,
            eligibility_origin=dict(kind="admissible_proposal", label=label, files_sha256=digest(files)), notes="",
            remaining=[], reviewer_remaining=[], matrix={"public": observation()}, validation_receipts=[saved])
        self.sources["repair-1"] = files
        self.alias_map["repair-1"] = "candidate-E"
        self.stage["alias_map"]["repair-1"] = "candidate-E"
        self.stage["candidate_order"].append("candidate-E")
        context = dict(project_id="warehouse", project_title="Warehouse", stage_index=0,
            milestones=[dict(index=0, specification="Return one", requirements=["R"])], prior_notes="",
            prior_remaining=[], prior_machine_history=[], verified_cases=deepcopy(self.cases))
        feedback = dict(review_notes="check", remaining=["R"],
            controller_instruction="Repair the bound executed failures while preserving every passing active case. "
                "Your predecessor remains selectable; a source change alone is not progress.",
            predecessor_source_sha256=digest(predecessor["files"]),
            evidence=[dict(case=deepcopy(self.cases[0]), outcome=observation(False))])
        meta = dict(project_id="warehouse", stage_index=0, policy="independent-four", role="builder", round=2,
                    candidate_id="repair-1", escalation_id=None, protocol=PROTOCOL, phase="repair")
        self.calls["builder-repair-1-2"] = (dict(files={**predecessor["files"], CONTEXT: canonical(context)},
            instructions="build", allowed_paths=["app.py", "notes.json"], feedback=canonical(feedback)),
            dict(role="builder", model="cheap", request_span_id=401, controller_metadata=meta, changes=files))
        self.spans[401] = dict(kind="worker_request", started_monotonic_ns=82, finished_monotonic_ns=83)
        path = self.root / "evaluations" / (label + "-all-evidence.json")
        self.save(path, observed)
        self.spans[402] = dict(kind="docker_validation", run_id=self.run_id, stage_index=0, purpose="stage_evidence",
            status="finished", label=label + "-all-evidence", started_monotonic_ns=84, finished_monotonic_ns=85,
            receipt_path=str(path), receipt_sha256=sha(path), case_count=1)
        self.stage["trajectory"].append(dict(kind="builder", slot="repair-1", candidate="candidate-E", round=2, phase="repair",
            inherited_from="candidate-A", source_valid=True, notes_valid=False, binding=binding,
            attempt_status="effective_source_proposal", attempt_admissible=True, effective_source_change=True,
            prior_source_valid=True, previous_files_sha256=digest(predecessor["files"]), model="cheap",
            files_sha256=digest(files), visible_passed=True, failures=[]))
        comparison = dict(kind="repair_comparison", review_round=1, predecessor="candidate-A", repaired="candidate-E",
            case_ids=["public"], cases_sha256=digest(self.cases), resolved_failures=["public"], introduced_failures=[],
            persisting_failures=[], regression=False, failure_progress=True, predecessor_binding=deepcopy(predecessor["binding"]),
            repaired_binding=binding, predecessor_receipts=deepcopy(predecessor["validation_receipts"]), repaired_receipts=[saved],
            predecessor_observations="retained source-bound active evidence; no new execution claimed",
            repaired_observations="fresh execution of the full ordered active suite")
        self.stage["trajectory"].append(deepcopy(comparison))
        self.stage["repair_comparisons"].append(comparison)
        self.candidates["candidate-E"] = repaired
        self.stage["metrics"].update(builder_calls=1, repairs=1, invalid_builder_notes=1, effective_source_changes=1,
            retained_repair_checkpoints=1, repair_failure_progress=1, resolved_known_failures=1)
        route = dict(kind="routing", round=1, candidate="candidate-A", source_sha256=digest(predecessor["files"]),
            evidence_sha256=digest(self.cases), executed_failure_ids=["public"], source_valid=True, focused_admitted_probe_ids=[],
            repairs_remaining=2, reviews_remaining=3, failure_progress=True, action="review_repair", target="candidate-E",
            target_source_sha256=digest(files), regression=False)
        self.stage["trajectory"].append(route)
        return deepcopy(route)

    def local_generation(self, formation="independent"):
        """Replace setup imports with hand-authored four-call local formation."""
        self.shared = None
        self.formation = formation
        self.engine_policy = "sequential-four" if formation == "sequential" else "independent-four"
        self.stage["formation"] = formation
        self.stage["shared_setup"] = None
        self.stage["metrics"].update(shared_initial_builder_opportunities=0, initial_builder_calls=4,
            builder_calls=4, effective_source_changes=4, invalid_builder_notes=4)
        predecessor = None
        for i, alias in enumerate(self.aliases):
            item = self.candidates[alias]
            item.pop("shared_original_binding")
            item.pop("shared_original_validation_receipts")
            previous = predecessor["files"] if predecessor and formation == "sequential" else self.initial
            inherited = predecessor if formation == "sequential" else None
            label = f"stage-0-build-slot-{i}-1"
            item["eligibility_origin"]["label"] = label
            item["validation_receipts"][0]["label"] = label + "-all-evidence"
            span = self.spans[i]
            old_path = Path(span["receipt_path"])
            path = self.root / "evaluations" / (label + "-all-evidence.json")
            old_path.rename(path)
            span.update(label=label + "-all-evidence", receipt_path=str(path),
                        started_monotonic_ns=i * 10 + 3, finished_monotonic_ns=i * 10 + 4)
            context = dict(project_id="warehouse", project_title="Warehouse", stage_index=0,
                milestones=[dict(index=0, specification="Return one", requirements=["R"])], prior_notes="",
                prior_remaining=[], prior_machine_history=[], verified_cases=deepcopy(self.cases))
            feedback = dict(checkpoint_notes="")
            if inherited:
                feedback.update(preceding_source_sha256=digest(previous),
                    evidence=[dict(case=deepcopy(self.cases[0]), outcome=observation())])
            meta = dict(project_id="warehouse", stage_index=0, policy=self.engine_policy, role="builder", round=1,
                candidate_id=f"slot-{i}", escalation_id=None, protocol=PROTOCOL, phase="initial")
            self.calls[f"builder-slot-{i}-1"] = (dict(files={**previous, CONTEXT: canonical(context)}, instructions="build",
                allowed_paths=["app.py", "notes.json"], feedback=canonical(feedback)), dict(role="builder", model="cheap",
                request_span_id=200 + i, controller_metadata=meta, changes=deepcopy(item["files"])))
            start = i * 10 + 1 if formation == "sequential" else 1
            self.spans[200 + i] = dict(kind="worker_request", started_monotonic_ns=start, finished_monotonic_ns=start + 1)
            self.stage["trajectory"].append(dict(kind="builder", slot=f"slot-{i}", candidate=alias, round=1, phase="initial",
                inherited_from=inherited["alias"] if inherited else None, source_valid=True, notes_valid=False,
                binding=deepcopy(item["binding"]), attempt_status="effective_source_proposal", attempt_admissible=True,
                effective_source_change=True, prior_source_valid=True, previous_files_sha256=digest(previous), model="cheap",
                files_sha256=digest(item["files"]), visible_passed=True, failures=[]))
            predecessor = item
        pool = self.stage["initial_pool"]
        pool.update(formation=formation, policy=self.engine_policy, candidates=deepcopy(self.candidates))
        self.stage["initial_pool_sha256"] = digest(pool)
        self.stage["initial_freeze"]["pool_sha256"] = digest(pool)
        self.save(self.root / "initial-pool.json", dict(protocol=SHARED_PROTOCOL, session_id=self.run_id, stage_index=0,
            pool=pool, pool_sha256=digest(pool), frozen_monotonic_ns=50))
        scout_context = dict(project_id="warehouse", stage_index=0,
            milestones=[dict(requirements=["R"], specification="Return one")], public_cases=deepcopy(self.cases))
        inputs = {**self.initial, "scout-context.json": json.dumps(scout_context, sort_keys=True)}
        for i in range(2):
            meta = dict(role="scout", round=1, candidate_id=f"scout-{i}", scout_index=i, stage_index=0,
                        project_id="warehouse", phase="post_initial_freeze")
            self.calls[f"scout-scout-{i}-1"] = (dict(files=deepcopy(inputs), instructions="scout", allowed_paths=["probes.json"], feedback=""),
                dict(role="scout", model="cheap", request_span_id=300 + i, controller_metadata=meta,
                     changes={"probes.json": json.dumps({"probes": []})}))
            self.spans[300 + i] = dict(kind="worker_request", started_monotonic_ns=51, finished_monotonic_ns=52)
        scouts = self.stage["scouts"]
        scouts.update(source="arm_specific", calls=2, source_sha256=digest(self.initial), shared_input_sha256=digest(inputs),
            initial_pool_sha256=digest(pool), initial_pool_artifact_sha256=sha(self.root / "initial-pool.json"),
            parses=[dict(scout_id=f"scout-{i}", status="parsed", proposed_count=0) for i in range(2)])
        scouts.pop("shared_manifest")
        self.save(self.root / "scouts.json", scouts)

    def audit(self):
        return audit_stage(self.stage, self.project, 0, self.mode + ("-sequential" if getattr(self, "formation", "independent") == "sequential" else "-independent"), self.initial, {}, self.calls,
            self.contract, self.root, lambda b: self.sources[b["tip_sha"]], self.spans, self.run_id, self.prompts, self.shared)


class FollowupStageAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def transcript(self, mode="improved", notes=""):
        path = Path(self.tmp.name) / str(len(list(Path(self.tmp.name).iterdir())))
        path.mkdir()
        return Transcript(path, mode, notes)

    def accepted(self, mode="improved", notes=""):
        fixture = self.transcript(mode, notes)
        fixture.review(1)
        fixture.finish()
        return fixture

    def test_full_handwritten_imported_stage_acceptance_both_modes(self):
        for mode in ("current", "improved"):
            with self.subTest(mode=mode):
                result = self.accepted(mode).audit()
                self.assertTrue(result["completed"])
                self.assertEqual((result["builders"], result["scouts"], result["physical_validations"]), (0, 0, 4))
                self.assertEqual(result["shared_initial_opportunities"], 4)

    def test_current_stagnation_strong_noop_and_escalated_acceptance_are_replayed(self):
        fixture = self.transcript("current")
        fixture.review(1, action="inspect", remaining=["R"], accepted=False)
        fixture.current_progress(1)
        fixture.review(2, action="inspect", remaining=["R"], accepted=False)
        state = fixture.current_progress(2)
        pending = fixture.current_noop_escalation(state)
        fixture.review(3, pending=pending)
        fixture.finish()
        result = fixture.audit()
        self.assertEqual((result["builders"], result["reviewers"]), (1, 3))
        request = fixture.calls["reviewer-review-3"][0]
        request["instructions"] = "review current"
        with self.assertRaisesRegex(ValueError, "prompt"):
            fixture.audit()

    def test_immutable_repair_checkpoint_is_freshly_checked_then_explicitly_accepted(self):
        fixture = self.transcript()
        fixture.failing_initial_pool()
        fixture.review(1, action="repair", remaining=["R"], accepted=False)
        route = fixture.append_passing_repair()
        fixture.review(2, alias="candidate-E", pending=route)
        fixture.finish(selected="candidate-E")
        result = fixture.audit()
        self.assertEqual((result["builders"], result["reviewers"], result["physical_validations"]), (1, 2, 5))
        fixture.stage["repair_comparisons"][0]["resolved_failures"] = []
        with self.assertRaisesRegex(ValueError, "repair_comparisons"):
            fixture.audit()

    def test_handwritten_local_initial_formation_and_scout_boundary(self):
        for formation in ("independent", "sequential"):
            fixture = self.transcript()
            fixture.local_generation(formation)
            fixture.review(1)
            fixture.finish()
            result = fixture.audit()
            self.assertEqual((result["builders"], result["scouts"]), (4, 2))
            changed = fixture.calls["builder-slot-1-1"][0]
            changed["files"]["app.py"] = "wrong preceding source"
            with self.subTest(formation=formation), self.assertRaisesRegex(ValueError, "source/context"):
                fixture.audit()

    def test_independent_initial_pool_waits_for_every_call_before_first_validation(self):
        fixture = self.transcript()
        fixture.local_generation()
        fixture.review(1)
        fixture.finish()
        fixture.spans[203]["finished_monotonic_ns"] = 40
        with self.assertRaisesRegex(ValueError, "Execution preceded"):
            fixture.audit()

    def test_probe_raw_batch_identity_is_checked_even_when_no_probes_admitted(self):
        for key, value in (("content_sha256", "forged"), ("max_new", 7), ("namespace", "another-project"),
                           ("proposed_count", True), ("origin", {"source": "other"})):
            fixture = self.accepted()
            fixture.stage["probe_receipts"][0]["receipt"][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "receipt"):
                fixture.audit()

    def test_unicode_raw_probe_batch_expansion_can_be_rejected_before_counting(self):
        fixture = self.accepted()
        proposal = dict(input="é" * 20000, expected=1, requirement="R")
        fixture.shared["scouts"]["proposals"][0]["probes"] = [proposal]
        fixture.stage["scouts"]["proposals"][0]["probes"] = [proposal]
        fixture.stage["scouts"]["rejected"] = 1
        fixture.save(fixture.root / "scouts.json", fixture.stage["scouts"])
        gate = fixture.stage["probe_receipts"][0]
        gate["envelope"]["probes"] = [proposal]
        gate["receipt"].update(proposed_count=0, rejected=[dict(index=None, reason="batch_too_large")],
            content_sha256=hashlib.sha256(json.dumps({"probes": [proposal]}).encode()).hexdigest())
        fixture.stage["metrics"].update(scout_proposal_attempts=1, rejected_scout_probes=1, rejected_probes=1)
        self.assertTrue(fixture.audit()["completed"])

    def test_current_keeps_ef_notes_improved_suppresses_only_output(self):
        for mode in ("current", "improved"):
            fixture = self.accepted(mode, "candidate-E is advisory")
            fixture.audit()
            self.assertEqual(fixture.stage["initial_pool"]["candidates"]["candidate-A"]["notes"], "candidate-E is advisory")
            fixture.stage["notes"] = "" if mode == "current" else "candidate-E is advisory"
            with self.assertRaisesRegex(ValueError, "notes"):
                fixture.audit()

    def test_one_focused_retry_then_explicit_unresolved_stop(self):
        fixture = self.transcript()
        fixture.review(1, action="inspect", remaining=["R"], accepted=False)
        pending = fixture.route(1, "request_focused_probe", target="candidate-A",
            target_source_sha256=digest(fixture.candidates["candidate-A"]["files"]), focus_remaining=["R"])
        fixture.stage["metrics"]["focused_review_requests"] = 1
        fixture.review(2, action="inspect", remaining=["R"], pending=pending, accepted=False)
        stop = fixture.route(2, "stop", reason_code="uncertainty_without_focused_evidence")
        fixture.finish(completed=False, code="reviewer_requirements_unresolved",
            reason="Bounded stop: explicit acceptance or a permitted evidence/repair-and-review step is absent", routing=stop)
        result = fixture.audit()
        self.assertFalse(result["completed"])
        self.assertEqual(result["reviewers"], 2)
        fixture.stage["reason_code"] = "uncertainty_without_focused_evidence"
        with self.assertRaisesRegex(ValueError, "reason_code"):
            fixture.audit()

    def test_focus_target_is_source_bound_and_cannot_acquire_another_free_retry(self):
        fixture = self.transcript()
        fixture.review(1, action="inspect", remaining=["R"], accepted=False)
        pending = fixture.route(1, "request_focused_probe", target="candidate-A", target_source_sha256="forged", focus_remaining=["R"])
        fixture.review(2, pending=pending)
        fixture.stage["metrics"]["focused_review_requests"] = 1
        fixture.finish()
        with self.assertRaisesRegex(ValueError, "routing"):
            fixture.audit()

    def test_tampered_report_metrics_and_candidate_fields_are_rejected(self):
        for key in ("repair_regressions", "builder_calls", "shared_initial_builder_opportunities", "focused_evidence_steps"):
            fixture = self.accepted()
            fixture.stage["metrics"][key] += 1
            with self.subTest(metric=key), self.assertRaisesRegex(ValueError, "metrics"):
                fixture.audit()
        fixture = self.accepted()
        fixture.stage["metrics"]["reviewer_calls"] = True
        with self.assertRaisesRegex(ValueError, "integers"):
            fixture.audit()
        fixture = self.accepted()
        fixture.stage["candidates"]["candidate-B"]["source_valid"] = False
        with self.assertRaisesRegex(ValueError, "candidates"):
            fixture.audit()

    def test_original_shared_receipt_never_substitutes_consumer_execution(self):
        fixture = self.accepted()
        del fixture.spans[1]
        with self.assertRaisesRegex(ValueError, "fresh stage execution"):
            fixture.audit()
        fixture = self.accepted()
        fixture.stage["initial_pool"]["candidates"]["candidate-A"]["validation_receipts"] = deepcopy(
            fixture.shared["pool"]["candidates"]["candidate-A"]["validation_receipts"])
        with self.assertRaisesRegex(ValueError, "freeze"):
            fixture.audit()

    def test_reviewer_cannot_receive_unexecuted_or_future_observations(self):
        fixture = self.accepted()
        fixture.spans[101]["started_monotonic_ns"] = 20
        with self.assertRaisesRegex(ValueError, "future observations"):
            fixture.audit()
        fixture = self.accepted()
        request = fixture.calls["reviewer-review-1"][0]
        context = json.loads(request["files"][CONTEXT])
        context["private_quality"] = 1
        request["files"][CONTEXT] = canonical(context)
        with self.assertRaisesRegex(ValueError, "context"):
            fixture.audit()

    def test_tampered_source_receipt_and_fresh_execution_timing_rejected(self):
        fixture = self.accepted()
        span = fixture.spans[0]
        path = Path(span["receipt_path"])
        value = json.loads(path.read_text())
        value["outcomes"][0]["actual"] = 2
        fixture.save(path, value)
        span["receipt_sha256"] = sha(path)
        with self.assertRaisesRegex(ValueError, "wrong typed value"):
            fixture.audit()
        fixture = self.accepted()
        fixture.spans[2]["started_monotonic_ns"] = 1
        with self.assertRaisesRegex(ValueError, "Execution preceded"):
            fixture.audit()

    def test_unlinked_calls_events_gates_receipts_and_validations_rejected(self):
        for kind in ("call", "event", "gate", "receipt", "validation"):
            fixture = self.accepted()
            if kind == "call":
                fixture.calls["builder-slot-0-1"] = deepcopy(fixture.calls["reviewer-review-1"])
            elif kind == "event":
                fixture.stage["trajectory"].append(dict(kind="routing"))
            elif kind == "gate":
                fixture.gate(dict(project_id="warehouse", stage_index=0, source="reviewer", review_round=2))
            elif kind == "receipt":
                fixture.save(fixture.root / "evaluations" / "orphan.json", {})
            else:
                fixture.spans[999] = {**fixture.spans[0], "label": "orphan"}
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, "Orphan|Unlinked"):
                fixture.audit()

    def test_explicit_acceptance_cannot_be_fabricated_from_passing_checks(self):
        fixture = self.accepted()
        call = fixture.calls["reviewer-review-1"][1]
        review = json.loads(call["changes"]["review.json"])
        review["action"] = "inspect"
        call["changes"]["review.json"] = json.dumps(review)
        with self.assertRaisesRegex(ValueError, "reviewer"):
            fixture.audit()

    def test_duplicate_gate_inputs_and_mutated_shared_pool_fail(self):
        fixture = self.accepted()
        fixture.stage["probe_receipts"][0]["envelope"]["max_new"] = 9
        with self.assertRaisesRegex(ValueError, "cap"):
            fixture.audit()
        fixture = self.accepted()
        fixture.shared["pool"]["candidates"]["candidate-A"]["files"]["app.py"] = "mutated"
        with self.assertRaisesRegex(ValueError, "Shared original"):
            fixture.audit()

    def test_same_case_progress_rejects_traded_failure_and_count_only_gain(self):
        cases = [dict(id=str(i)) for i in range(3)]
        before = {"0": {"passed": False}, "1": {"passed": False}, "2": {"passed": True}}
        after = {"0": {"passed": True}, "1": {"passed": True}, "2": {"passed": False}}
        result = same_case_comparison(before, after, cases)
        self.assertEqual(result["resolved_failures"], ["0", "1"])
        self.assertEqual(result["introduced_failures"], ["2"])
        self.assertTrue(result["regression"])
        self.assertFalse(result["failure_progress"])
        after["2"]["passed"] = True
        self.assertTrue(same_case_comparison(before, after, cases)["failure_progress"])
        after["2"]["passed"] = 1
        with self.assertRaisesRegex(ValueError, "typed"):
            same_case_comparison(before, after, cases)

    def test_alternative_requires_strict_failure_subset_and_unrouted_source_edge(self):
        replay = object.__new__(StageReplay)
        replay.cases = [dict(id=str(i)) for i in range(3)]
        def item(alias, source, failures, valid=True):
            return dict(alias=alias, files={"app.py": source}, source_valid=valid,
                        matrix={str(i): {"passed": i not in failures} for i in range(3)})
        original = item("candidate-A", "A", {0, 1})
        replacement = item("candidate-B", "B", {2})
        strict = item("candidate-C", "C", {0})
        replay.candidates = {row["alias"]: row for row in (original, replacement, strict)}
        replay.order = list(replay.candidates)
        self.assertEqual(replay.alternative(original, set())["alias"], "candidate-C")
        edge = (digest(original["files"]), digest(strict["files"]), digest(replay.cases))
        self.assertIsNone(replay.alternative(original, {edge}))
        strict["source_valid"] = False
        self.assertIsNone(replay.alternative(original, set()))

    def test_terminal_reason_describes_returned_source_not_routing_candidate(self):
        item = dict(source_valid=True, reviewer_remaining=[], matrix={"x": {"passed": True}})
        cases = [dict(id="x")]
        self.assertEqual(terminal_reason(item, cases), "review_acceptance_missing")
        item["reviewer_remaining"] = ["R"]
        self.assertEqual(terminal_reason(item, cases), "reviewer_requirements_unresolved")
        item["matrix"]["x"]["passed"] = False
        self.assertEqual(terminal_reason(item, cases), "active_checks_failed")
        item["source_valid"] = False
        self.assertEqual(terminal_reason(item, cases), "source_ineligible")

    def test_scout_empty_batch_and_rejected_batch_are_distinct(self):
        self.assertEqual(scout_parse({"changes": {"probes.json": '{"probes": []}'}}),
                         {"status": "parsed", "proposed_count": 0})
        for value in ('{"probes": [], "probes": []}', '{"probes": NaN}', '[1]', '{"probes": [1,2,3,4,5]}'):
            with self.subTest(value=value):
                self.assertEqual(scout_parse({"changes": {"probes.json": value}})["status"], "rejected_batch")
        fixture = self.transcript()
        fixture.local_generation()
        fixture.review(1)
        fixture.finish()
        fixture.stage["scouts"]["rejected_batches"] = 1
        fixture.save(fixture.root / "scouts.json", fixture.stage["scouts"])
        with self.assertRaisesRegex(ValueError, "rejected-batch"):
            fixture.audit()

    def test_prompt_extraction_reads_ast_without_importing_controller(self):
        source = Path("gossip_harness/continuation_controller.py").read_text()
        prompts = controller_prompts(source)
        self.assertTrue(prompts["improved_reviewer_prompt"].startswith(prompts["reviewer_prompt"]))
        self.assertIn("required_next_action", prompts["focus_suffix"])
        self.assertIn("bounded continuation", prompts["escalation_suffix"])


if __name__ == "__main__":
    unittest.main()
