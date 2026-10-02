"""Independent, retained-evidence replay for the continuation follow-up stage.

This module never imports a controller, runner, fixture or candidate. Each next
request, source transition, observation and decision is checked against the raw
journal and physical receipts. The caller authenticates shared-setup provenance,
Git stores, accounting and the cohort-wide private-evaluation barrier separately.
"""
from __future__ import annotations

import ast
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from analysis.audit_benchmark import review_from_call, scope_proposal, scout_proposals
from analysis.audit_continuation import load, sha
from gossip_harness.verification_audit import (
    _builder_note, _probe_binding, _prompts, audit_receipt, bare, cases_for, decode,
    digest, inside, public_cases, public_view, require, safe_notes, same,
)

PROTOCOL = "failure-directed-checkpoint-stage-v1"
SHARED_PROTOCOL = "continuation-followup-shared-setup-v1"
CONTEXT = "verification-context.json"
LIMITS = dict(max_reviews=4, max_repairs=2, max_new_probes=8,
              max_escalations=1, stagnation_reviews=2)
POLICIES = {"current-independent": ("current", "independent"),
            "improved-independent": ("improved", "independent"),
            "improved-sequential": ("improved", "sequential")}
METRICS = tuple("builder_calls reviewer_calls repairs invalid_reviews invalid_builder_notes "
    "invalid_source_proposals blocked_acceptances admitted_new_probes generated_probe_attempts "
    "rejected_probes retired_probes probe_failures_found probe_discriminating_cases "
    "acceptances_without_new_probes no_effective_source_proposals retained_eligibility_preserved "
    "effective_source_changes escalations escalated_repairs escalated_reviews stagnation_events "
    "strongest_builder_calls initial_builder_calls scout_proposal_attempts admitted_scout_probes "
    "rejected_scout_probes scout_probe_failures_found scout_probe_discriminating_cases "
    "focused_review_requests retained_repair_checkpoints repair_regressions repair_failure_progress "
    "resolved_known_failures introduced_failures alternative_routes focused_evidence_steps "
    "shared_initial_builder_opportunities".split())


def controller_prompts(source):
    """Extract literal prose from a frozen source AST without executing it."""
    result = _prompts(source)
    tree = ast.parse(source)
    extensions = [ast.literal_eval(node.value) for node in ast.walk(tree)
                  if isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name)
                  and node.target.id == "reviewer_prompt"]
    require(len(extensions) == 1 and type(extensions[0]) is str,
            "Missing literal improved reviewer extension")
    result["improved_reviewer_prompt"] = result["reviewer_prompt"] + extensions[0]
    for key, prefix in (("escalation_suffix", " This is a bounded continuation review"),
                        ("focus_suffix", " This existing reviewer opportunity")):
        values = [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant)
                  and type(node.value) is str and node.value.startswith(prefix)]
        require(len(values) == 1, "Missing literal continuation prompt suffix")
        result[key] = values[0]
    return result


def scout_parse(call):
    """Retain rejected-batch accounting, including parsed empty proposals."""
    try:
        changes = call.get("changes", {})
        content = changes.get("probes.json")
        require(set(changes) == {"probes.json"} and type(content) is str
                and len(content.encode()) <= 65536, "Malformed scout envelope")
        parsed = decode(content)
        require(type(parsed) is dict and set(parsed) == {"probes"}
                and type(parsed["probes"]) is list and len(parsed["probes"]) <= 4, "Malformed scout envelope")
        return dict(status="parsed", proposed_count=len(parsed["probes"]))
    except (ValueError, TypeError, AttributeError, UnicodeError, RecursionError):
        return dict(status="rejected_batch", proposed_count=None, reason="missing_or_malformed_scout_envelope")


def same_case_comparison(before, after, cases):
    """Classify independently: losing any previously passing case is regression."""
    ids = [case["id"] for case in cases]
    require(len(ids) == len(set(ids)) and all(type(matrix.get(identity, {}).get("passed")) is bool
            for matrix in (before, after) for identity in ids), "Incomplete typed same-case observations")
    resolved = [identity for identity in ids if not before[identity]["passed"] and after[identity]["passed"]]
    introduced = [identity for identity in ids if before[identity]["passed"] and not after[identity]["passed"]]
    return dict(case_ids=ids, cases_sha256=digest(cases), resolved_failures=resolved,
                introduced_failures=introduced,
                persisting_failures=[identity for identity in ids if not before[identity]["passed"]
                                     and not after[identity]["passed"]],
                regression=bool(introduced), failure_progress=bool(resolved) and not introduced)


def terminal_reason(item, cases):
    if not item["source_valid"]:
        return "source_ineligible"
    if any(not item["matrix"][case["id"]]["passed"] for case in cases):
        return "active_checks_failed"
    return "reviewer_requirements_unresolved" if item["reviewer_remaining"] else "review_acceptance_missing"


def _history(rows):
    return [{**{key: deepcopy(row[key]) for key in
                ("stage_index", "completed", "source_sha256", "evidence_sha256") if key in row},
             "outcomes": [{key: deepcopy(outcome[key]) for key in ("case_id", "requirement", "passed")
                           if key in outcome} for outcome in row.get("outcomes", [])]} for row in rows]


class StageReplay:
    """A strict event consumer; expected transitions never call production code."""

    def __init__(self, stage, project, index, policy, initial, prior, calls, contract,
                 root, git_files, spans, run_id, prompts, shared=None):
        self.stage, self.project, self.index = stage, project, index
        self.mode, self.formation = POLICIES[policy]
        self.engine_policy = "sequential-four" if self.formation == "sequential" else "independent-four"
        self.initial, self.prior, self.calls, self.contract = initial, prior, calls, contract
        self.root, self.git_files, self.spans, self.run_id = Path(root).resolve(), git_files, spans, run_id
        self.prompts, self.shared = prompts, shared
        self.reqs = list(dict.fromkeys(r for s in project["stages"][:index + 1] for r in s["requirements"]))
        self.cases, self.pool, self.retired = public_cases(project, index), [], []
        self.metrics = dict.fromkeys(METRICS, 0)
        self.candidates, self.alias_map, self.order = {}, {}, []
        self.used_calls, self.used_validations = set(), set()
        self.event_index, self.gate_index, self.frontier = 0, 0, 0
        self.escalations, self.comparisons = [], []
        self.baseline = stage["baseline_approval"]
        self.baseline_valid = self.baseline is not None
        if self.baseline_valid:
            require(set(self.baseline) == {"files_sha256", "source_valid", "approval_id"}
                    and self.baseline["files_sha256"] == digest(initial)
                    and self.baseline["source_valid"] is True
                    and type(self.baseline["approval_id"]) is str and bool(self.baseline["approval_id"]),
                    "Baseline approval/source binding differs")
        self.context = dict(project_id=project["id"], project_title=project["title"], stage_index=index,
            milestones=[dict(index=i, specification=s.get("specification", s.get("spec")), requirements=s["requirements"])
                        for i, s in enumerate(project["stages"][:index + 1])],
            prior_notes=self.notes(prior.get("notes", "")),
            prior_remaining=sorted(set(prior.get("remaining", []))),
            prior_machine_history=_history(prior.get("check_history", [])))
        require(stage["protocol"] == PROTOCOL and stage["controller_mode"] == self.mode
                and stage["formation"] == self.formation and same(stage["limits"], LIMITS)
                and type(stage["stage_index"]) is int and stage["stage_index"] == index
                and type(stage["completed"]) is bool, "Stage policy/protocol/limits differ")
        require(set(stage["metrics"]) == set(METRICS)
                and all(type(value) is int and value >= 0 for value in stage["metrics"].values()),
                "Stage counters must be complete exact nonnegative integers")
        frozen = load(self.root / "initial-pool.json")
        require(frozen["protocol"] == SHARED_PROTOCOL and frozen["session_id"] == run_id
                and frozen["stage_index"] == index and same(frozen["pool"], stage["initial_pool"])
                and digest(frozen["pool"]) == frozen["pool_sha256"] == stage["initial_pool_sha256"]
                and same(stage["initial_freeze"], dict(freeze_id=str(self.root / "initial-pool.json"),
                                                      pool_sha256=frozen["pool_sha256"])),
                "Initial pool freeze bytes/identity differ")
        self.frozen, self.barrier = frozen["pool"], frozen["frozen_monotonic_ns"]
        require(type(self.barrier) is int and self.barrier >= 0, "Invalid initial freeze clock")
        self.alias_map = {f"slot-{i}": self.frozen["alias_map"][f"slot-{i}"] for i in range(4)}
        self.order = list(self.alias_map.values())
        require(len(set(self.order)) == 4 and set(self.order) == {f"candidate-{letter}" for letter in "ABCD"}
                and same(self.frozen["alias_map"], self.alias_map)
                and same(self.frozen["candidate_order"], self.order)
                and self.frozen["protocol"] == PROTOCOL and self.frozen["project_id"] == project["id"]
                and self.frozen["stage_index"] == index and self.frozen["policy"] == self.engine_policy
                and self.frozen["formation"] == self.formation, "Initial candidate identity/order differs")
        self.validations: dict[str, tuple[Any, dict]] = {}
        for identity, span in spans.items():
            if span["kind"] != "docker_validation" or span.get("run_id") != run_id or span.get("stage_index") != index:
                continue
            require(span.get("purpose") == "stage_evidence" and span["status"] == "finished",
                    "Unexpected or failed stage validation")
            require(span["label"] not in self.validations, "Duplicate stage validation label")
            self.validations[span["label"]] = (identity, span)

    def notes(self, value):
        return "" if self.mode == "improved" and isinstance(value, str) and any(
            alias.lower() in value.lower() for alias in ("candidate-E", "candidate-F")) else safe_notes(value)

    def consume(self, expected):
        require(self.event_index < len(self.stage["trajectory"]), "Missing controller event")
        actual = self.stage["trajectory"][self.event_index]
        require(same(actual, expected), "Controller event differs: " + expected["kind"])
        self.event_index += 1
        return actual

    def raw_call(self, call_id, role, model, files, instructions, allowed, feedback, metadata, *, initial=False):
        require(call_id in self.calls and call_id not in self.used_calls, "Missing or repeated provider opportunity")
        request, call = self.calls[call_id]
        require(call["role"] == role and call["model"] == model
                and same(request["files"], files) and request["instructions"] == instructions
                and request["allowed_paths"] == allowed and request["feedback"] == feedback
                and same(call["controller_metadata"], metadata), "Provider source/context/prompt/opportunity differs")
        span = self.spans[call["request_span_id"]]
        if not (initial and self.formation == "independent"):
            require(span["started_monotonic_ns"] >= self.frontier, "Provider consumed future observations")
        if initial:
            require(span["finished_monotonic_ns"] <= self.barrier, "Initial call crossed pool freeze")
        else:
            require(span["started_monotonic_ns"] >= self.barrier, "Review/repair preceded pool freeze")
        self.frontier = max(self.frontier, span["finished_monotonic_ns"])
        self.used_calls.add(call_id)
        return call

    def evaluate(self, item, cases, label):
        require(label in self.validations, "Missing fresh stage execution: " + label)
        identity, span = self.validations[label]
        require(identity not in self.used_validations, "Repeated physical execution attribution")
        require(span["started_monotonic_ns"] >= self.frontier, "Execution preceded source/evidence availability")
        path = inside(span["receipt_path"], self.root)
        require(path.parent == self.root / "evaluations" and sha(path) == span["receipt_sha256"]
                and span["case_count"] == len(cases), "Physical receipt path/bytes/count differs")
        receipt = load(path)
        observed = audit_receipt(receipt, item["files"], cases, self.contract, label)
        item["matrix"].update(observed)
        item["validation_receipts"].append(dict(label=label, case_ids=[case["id"] for case in cases], receipt=receipt))
        self.frontier = span["finished_monotonic_ns"]
        self.used_validations.add(identity)
        return observed

    def gate(self, proposals, mode, source, origin):
        require(self.gate_index < len(self.stage["probe_receipts"]), "Missing probe gate")
        row = self.stage["probe_receipts"][self.gate_index]
        counter = "admitted_scout_probes" if source == "scout" else "admitted_new_probes"
        capacity = len(proposals) if mode == "revalidate" else 8 - self.metrics[counter]
        require(same({key: row[key] for key in ("mode", "origin", "envelope")}, dict(mode=mode, origin=origin,
            envelope=dict(mode=mode, probes=proposals, existing_cases=self.cases, max_new=capacity))),
            "Probe admission input/order/cap differs")
        receipt = row["receipt"]
        raw_batch = json.dumps({"probes": proposals}).encode() if mode == "new" else None
        oversized = raw_batch is not None and len(raw_batch) > 65536
        require(receipt.get("oracle_assisted") is True and receipt.get("mode") == mode
                and type(receipt.get("stage_index")) is int and receipt["stage_index"] == self.index
                and type(receipt.get("admitted_cases")) is list
                and type(receipt.get("rejected", [])) is list and type(receipt.get("retired", [])) is list
                and receipt.get("validation_policy") == "verification-fixture-oracle-v1"
                and same(receipt.get("origin"), origin)
                and type(receipt.get("proposed_count")) is int
                and receipt["proposed_count"] == (0 if oversized else len(proposals)),
                "Probe receipt identity differs")
        admitted = deepcopy(receipt["admitted_cases"])
        if oversized:
            require(not admitted and same(receipt["rejected"], [{"index": None, "reason": "batch_too_large"}]),
                    "Oversized raw probe batch was not rejected intact")
        require(len(admitted) <= capacity and (mode == "revalidate" or len(proposals) <= 4), "Probe budget exceeded")
        inputs = {digest(case["input"]) for case in self.cases} if mode == "new" else set()
        ids = {case["id"] for case in self.cases} if mode == "new" else set()
        for case in admitted:
            _probe_binding(case)
            require(case["requirement"] in self.reqs and case["id"] not in ids
                    and digest(case["input"]) not in inputs
                    and any(same(bare(case), bare(p)) and (mode != "revalidate" or p.get("id") == case["id"])
                            for p in proposals if type(p) is dict and {"input", "expected", "requirement"} <= set(p)),
                    "Injected, corrected or duplicate admitted probe")
            require(type(case.get("validated_stage_index")) is int and case["validated_stage_index"] == self.index,
                    "Admitted probe validation stage differs")
            if mode == "new":
                require(same(case.get("origin"), origin) and type(case.get("admitted_stage_index")) is int
                        and case["admitted_stage_index"] == self.index and case.get("namespace") == self.project["id"],
                        "New probe stage/origin/namespace differs")
            else:
                require(any(case["id"] == old.get("id") and same(case.get("origin"), old.get("origin"))
                            and case.get("admitted_stage_index") == old.get("admitted_stage_index") for old in proposals),
                        "Retained probe origin/admission stage changed")
            inputs.add(digest(case["input"]))
            ids.add(case["id"])
        self.metrics["rejected_probes"] += len(receipt.get("rejected", []))
        if mode == "new":
            assert raw_batch is not None
            require(type(receipt.get("max_new")) is int and receipt["max_new"] == capacity
                    and receipt.get("namespace") == self.project["id"]
                    and receipt.get("content_sha256") == hashlib.sha256(raw_batch).hexdigest(),
                    "Probe receipt raw batch/cap binding differs")
            self.metrics[counter] += len(admitted)
            self.metrics["scout_proposal_attempts" if source == "scout" else "generated_probe_attempts"] += len(proposals)
            if source == "scout":
                self.metrics["rejected_scout_probes"] += len(receipt.get("rejected", []))
        self.gate_index += 1
        return admitted, receipt

    def retained_probes(self):
        old = self.prior.get("probe_pool", [])
        if not old:
            return
        admitted, receipt = self.gate(old, "revalidate", "reviewer",
            dict(project_id=self.project["id"], stage_index=self.index, source="retained_probes"))
        ids, retired = {case["id"] for case in admitted}, {row.get("id") for row in receipt.get("retired", [])}
        require(ids.isdisjoint(retired) and ids | retired == {case["id"] for case in old}
                and len(retired) == len(receipt.get("retired", []))
                and len(admitted) + len(retired) == len(old)
                and all(row.get("reason") == "contract_changed" and row.get("stage_index") == self.index
                        and "expected" not in row and "actual" not in row for row in receipt.get("retired", [])),
                "Retained probe loss/duplicate retirement or corrected label")
        self.retired.extend(dict(case=case, reason="not_admissible_under_current_stage",
                                 gate_retirements=deepcopy(receipt.get("retired", []))) for case in old if case["id"] not in ids)
        public_inputs = {digest(case["input"]): case for case in self.cases}
        for case in admitted:
            existing = public_inputs.get(digest(case["input"]))
            if existing is None:
                self.pool.append(case)
            else:
                require(same(existing["expected"], case["expected"]), "Retained/public expectations conflict")
                self.retired.append(dict(case=case, reason="equivalent_input_now_in_public_suite"))
        self.cases.extend(self.pool)
        require(len({case["id"] for case in self.cases}) == len(self.cases), "Duplicate active evidence identity")
        self.metrics["retired_probes"] = len(self.retired)

    def build(self, slot, round_number, predecessor, phase, feedback, model="cheap", escalation_id=None):
        previous = predecessor["files"] if predecessor else self.initial
        alias = self.alias_map[slot]
        files = {**previous, CONTEXT: self._canonical({**self.context, "verified_cases": public_view(self.cases)})}
        meta = dict(project_id=self.project["id"], stage_index=self.index, policy=self.engine_policy,
            role="builder", round=round_number, candidate_id=slot, escalation_id=escalation_id,
            protocol=PROTOCOL, phase=phase)
        call = self.raw_call(f"builder-{slot}-{round_number}", "builder", model, files,
            self.prompts["builder_prompt"], [*self.project["allowed_paths"], "notes.json"], self._canonical(feedback),
            meta, initial=phase == "initial")
        previous_valid = predecessor["source_valid"] if predecessor else self.baseline_valid
        source, valid, admissible, changed = scope_proposal(previous, previous_valid, call.get("changes"), self.project["allowed_paths"])
        note = _builder_note(call.get("changes"), self.reqs)
        label = f"stage-{self.index}-build-{slot}-{round_number}"
        event = self.stage["trajectory"][self.event_index]
        require(event.get("kind") == "builder" and same(self.git_files(event["binding"]), source)
                and event["binding"]["files_sha256"] == digest(source), "Retained Git proposal differs")
        origin = (dict(kind="admissible_proposal", label=label, files_sha256=digest(source)) if changed else
                  deepcopy(predecessor["eligibility_origin"]) if predecessor else
                  dict(kind="approved_baseline" if self.baseline_valid else "unapproved_baseline",
                       approval=deepcopy(self.baseline), files_sha256=digest(source)))
        item = dict(slot=slot, alias=alias, files=source, binding=deepcopy(event["binding"]), source_valid=valid,
            latest_attempt_status="effective_source_proposal" if changed else "no_effective_source_change" if admissible else "rejected_source_proposal",
            latest_attempt_admissible=admissible, eligibility_origin=origin,
            notes=self.notes(note["notes"]) if note and admissible else predecessor["notes"] if predecessor else self.context["prior_notes"],
            remaining=note["remaining"] if note and admissible else deepcopy(predecessor["remaining"]) if predecessor else [],
            reviewer_remaining=deepcopy(self.candidates.get(alias, {}).get("reviewer_remaining", [])),
            matrix={}, validation_receipts=[])
        self.candidates[alias] = item
        self.evaluate(item, self.cases, label + "-all-evidence")
        inherited = predecessor if phase == "initial" and self.formation == "sequential" or self.mode == "improved" and phase == "repair" else None
        self.consume(dict(kind="builder", slot=slot, candidate=alias, round=round_number, phase=phase,
            inherited_from=inherited["alias"] if inherited else None, source_valid=valid, notes_valid=note is not None,
            binding=item["binding"], attempt_status=item["latest_attempt_status"], attempt_admissible=admissible,
            effective_source_change=changed, prior_source_valid=previous_valid,
            previous_files_sha256=digest(previous), model=model, files_sha256=digest(source),
            visible_passed=all(value["passed"] for value in item["matrix"].values()), failures=self.failures(item, ordered=True)))
        self.metrics["builder_calls"] += 1
        self.metrics["initial_builder_calls"] += int(phase == "initial")
        self.metrics["strongest_builder_calls"] += int(model == "strong")
        self.metrics["invalid_builder_notes"] += int(note is None)
        self.metrics["invalid_source_proposals"] += int(not admissible)
        self.metrics["no_effective_source_proposals"] += int(admissible and not changed)
        self.metrics["effective_source_changes"] += int(changed)
        self.metrics["retained_eligibility_preserved"] += int(not changed and previous_valid)
        return item

    @staticmethod
    def _canonical(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)

    def initialize(self):
        self.retained_probes()
        require(same(self.frozen["evidence_cases"], self.cases)
                and self.frozen["evidence_sha256"] == digest(self.cases), "Initial pool acquired later evidence")
        if self.shared is not None:
            require(self.index == 0 and self.shared["binding"]["pool_sha256"] == digest(self.shared["pool"])
                    and same(self.shared["binding"]["pool"], self.shared["pool"])
                    and same(self.stage["shared_setup"], {k: v for k, v in self.shared["binding"].items() if k != "pool"}),
                    "Shared original pool/envelope differs")
            originals = self.shared["pool"]
            require(same({k: originals[k] for k in ("project_id", "stage_index", "policy", "formation", "alias_map", "candidate_order", "evidence_cases", "evidence_sha256")},
                         {k: self.frozen[k] for k in ("project_id", "stage_index", "policy", "formation", "alias_map", "candidate_order", "evidence_cases", "evidence_sha256")}),
                    "Imported initial formation/source/evidence identity differs")
            for alias in self.order:
                item = deepcopy(originals["candidates"][alias])
                saved = self.frozen["candidates"][alias]
                require(same(self.git_files(saved["binding"]), item["files"])
                        and saved["binding"]["files_sha256"] == digest(item["files"]), "Imported source changed in consumer Git")
                item["shared_original_binding"] = deepcopy(item["binding"])
                item["shared_original_validation_receipts"] = deepcopy(item["validation_receipts"])
                item["binding"] = deepcopy(saved["binding"])
                item["matrix"], item["validation_receipts"] = {}, []
                self.candidates[alias] = item
                self.evaluate(item, self.cases, f"stage-{self.index}-shared-{item['slot']}-fresh-evidence")
            self.metrics["shared_initial_builder_opportunities"] = 4
        else:
            require(self.stage["shared_setup"] is None, "Nonimported stage acquired shared setup")
            predecessor = None
            if self.formation == "independent":
                # The production engine joins all four futures before retaining/evaluating slot 0.
                call_ids = [f"builder-{slot}-1" for slot in self.alias_map]
                require(all(identity in self.calls for identity in call_ids), "Missing independent formation opportunity")
                self.frontier = max(self.spans[self.calls[identity][1]["request_span_id"]]["finished_monotonic_ns"]
                                    for identity in call_ids)
            for slot in self.alias_map:
                parent = predecessor if self.formation == "sequential" else None
                feedback = dict(checkpoint_notes=parent["notes"] if parent else self.context["prior_notes"])
                if parent:
                    feedback.update(preceding_source_sha256=digest(parent["files"]),
                        evidence=[dict(case=case, outcome=deepcopy(parent["matrix"][case["id"]])) for case in public_view(self.cases)])
                predecessor = self.build(slot, 1, parent, "initial", feedback)
        require(same(self.frozen["candidates"], self.candidates) and self.frontier <= self.barrier,
                "Initial retained pool/source/outcomes differ or crossed freeze")
        self.frontier = self.barrier
        self.scouts()

    def scouts(self):
        scouts = self.stage["scouts"]
        require(same(load(self.root / "scouts.json"), scouts), "Scout artifact changed")
        gate_scouts = scouts["proposals"]
        if self.shared is not None:
            require(scouts["source"] == "shared_initial_setup" and scouts["calls"] == 0
                    and scouts["attributed_calls"] == 2 and same(scouts["proposals"], self.shared["scouts"]["proposals"])
                    and scouts["rejected_batches"] == self.shared["scouts"]["rejected_batches"], "Shared scout provenance/count differs")
        else:
            require(scouts["source"] == "arm_specific" and scouts["calls"] == scouts["attributed_calls"] == 2
                    and scouts["source_sha256"] == digest(self.initial)
                    and scouts["initial_pool_artifact_sha256"] == sha(self.root / "initial-pool.json")
                    and scouts["initial_pool_sha256"] == self.stage["initial_pool_sha256"], "Arm scout pool/source binding differs")
            context = dict(project_id=self.project["id"], stage_index=self.index,
                milestones=[{k: s[k] for k in ("requirements", "specification", "spec") if k in s}
                            for s in self.project["stages"][:self.index + 1]],
                public_cases=cases_for(self.project, self.index, "visible"))
            ends, parses, gate_scouts = [], [], []
            for i in range(2):
                call_id = f"scout-scout-{i}-1"
                require(call_id in self.calls and call_id not in self.used_calls, "Missing scout opportunity")
                request, call = self.calls[call_id]
                span = self.spans[call["request_span_id"]]
                expected = dict(request["files"])
                received_context = decode(expected.pop("scout-context.json"))
                expected_meta = dict(role="scout", round=1, candidate_id=f"scout-{i}", scout_index=i,
                    stage_index=self.index, project_id=self.project["id"], phase="post_initial_freeze")
                require(call["role"] == "scout" and call["model"] == "cheap" and same(expected, self.initial)
                        and same(received_context, context) and same(call["controller_metadata"], expected_meta)
                        and digest(request["files"]) == scouts["shared_input_sha256"] and request["allowed_paths"] == ["probes.json"]
                        and request["feedback"] == "" and request["instructions"] == self.prompts["scout_prompt"]
                        and span["started_monotonic_ns"] >= self.barrier
                        and same(scouts["proposals"][i], dict(scout_id=f"scout-{i}", probes=scout_proposals(call))),
                        "Scout saw candidate/private information or preceded initial freeze")
                # Fresh scouts enter the gate before sorted artifact serialization.
                # Reconstruct their original JSON member order from the raw response;
                # imported M1 scouts intentionally retain persisted artifact order.
                gate_scouts.append(dict(scout_id=f"scout-{i}", probes=scout_proposals(call)))
                self.used_calls.add(call_id)
                parses.append(dict(scout_id=f"scout-{i}", **scout_parse(call)))
                ends.append(span["finished_monotonic_ns"])
            require(same(scouts["parses"], parses)
                    and type(scouts["rejected_batches"]) is int
                    and scouts["rejected_batches"] == sum(row["status"] == "rejected_batch" for row in parses),
                    "Scout parse/rejected-batch accounting differs")
            self.frontier = max([self.frontier, *ends])
        require(len(scouts["proposals"]) == 2 and [s["scout_id"] for s in scouts["proposals"]] == ["scout-0", "scout-1"],
                "Scout roster/order changed")
        for scout in gate_scouts:
            admitted, _ = self.gate(scout["probes"], "new", "scout", dict(project_id=self.project["id"],
                stage_index=self.index, source="scout", scout_id=scout["scout_id"]))
            self.apply_probes(admitted, f"stage-{self.index}-{scout['scout_id']}", "scout-probes", scout=True)

    def apply_probes(self, admitted, label, suffix, *, scout=False):
        if not admitted:
            return
        self.pool.extend(admitted)
        self.cases.extend(admitted)
        matrices = [self.evaluate(self.candidates[alias], admitted, f"{label}-{alias}-{suffix}") for alias in self.order]
        for case in admitted:
            outcomes = [matrix[case["id"]]["passed"] for matrix in matrices]
            self.metrics["scout_probe_failures_found" if scout else "probe_failures_found"] += sum(not passed for passed in outcomes)
            self.metrics["scout_probe_discriminating_cases" if scout else "probe_discriminating_cases"] += int(len(set(outcomes)) > 1)

    def failures(self, item, *, ordered=False):
        ids = [case["id"] for case in self.cases if not item["matrix"][case["id"]]["passed"]]
        return ids if ordered else set(ids)

    def fallback(self):
        return min(self.candidates.values(), key=lambda item: (not item["source_valid"], len(self.failures(item)), self.order.index(item["alias"])))

    def progress(self):
        return digest(dict(evidence=digest(self.cases), sources={alias: dict(source=digest(item["files"]), eligible=item["source_valid"])
                                                               for alias, item in self.candidates.items()}))

    def review(self, number, previous, continuation, suffix=""):
        contents = {f"candidates/{alias}/{path}": text for alias, item in self.candidates.items() for path, text in item["files"].items()}
        contents[CONTEXT] = self._canonical({**self.context, "verified_cases": public_view(self.cases),
            "candidate_source_hashes": {a: digest(item["files"]) for a, item in self.candidates.items()},
            "candidate_eligibility": {a: {k: item[k] for k in ("source_valid", "latest_attempt_status")} for a, item in self.candidates.items()},
            "matrix": {a: {case["id"]: item["matrix"][case["id"]] for case in self.cases} for a, item in self.candidates.items()},
            "previous_decision": previous, "new_probe_capacity_remaining": 8 - self.metrics["admitted_new_probes"],
            "continuation": continuation})
        escalation = continuation.get("escalation")
        meta = dict(project_id=self.project["id"], stage_index=self.index, policy=self.engine_policy, role="reviewer",
            round=number, candidate_id="review", alias_by_slot=dict(self.alias_map), protocol=PROTOCOL,
            escalation_id=escalation["id"] if escalation else None)
        prompt = self.prompts["improved_reviewer_prompt" if self.mode == "improved" else "reviewer_prompt"] + suffix
        call = self.raw_call(f"reviewer-review-{number}", "reviewer", "strong", contents, prompt, ["review.json"], "", meta)
        self.metrics["reviewer_calls"] += 1
        review = review_from_call(call, self.reqs, self.candidates)
        row = dict(kind="reviewer", round=number, review=review, accepted=False,
            evidence_before_sha256=digest(self.cases), errors=[], admitted_probe_ids=[])
        admitted = []
        item = self.fallback()
        if review is None:
            self.metrics["invalid_reviews"] += 1
            row["errors"].append("Invalid review.json; no reviewer acceptance authorized" if self.mode == "improved" else
                                 "Invalid review.json; no source change or acceptance authorized")
        else:
            admitted, _ = self.gate(review["probes"], "new", "reviewer", dict(project_id=self.project["id"], stage_index=self.index,
                                                                          source="reviewer", review_round=number))
            self.apply_probes(admitted, f"stage-{self.index}-review-{number}", "new-probes")
            row["admitted_probe_ids"] = [case["id"] for case in admitted]
            item = self.candidates[review["candidate"]]
            item["reviewer_remaining"] = list(review["remaining"])
            row.update(selected=item["alias"], binding=deepcopy(item["binding"]))
            if review["action"] == "accept":
                row["accepted"] = bool(item["source_valid"] and not self.failures(item) and not review["remaining"])
                if not row["accepted"]:
                    self.metrics["blocked_acceptances"] += 1
                    row["errors"].append("Acceptance denied: source, remaining requirements, or verified checks are not passing")
                elif self.metrics["admitted_new_probes"] + self.metrics["admitted_scout_probes"] == 0:
                    self.metrics["acceptances_without_new_probes"] += 1
            elif self.mode == "current" and review["action"] == "repair" and (self.metrics["repairs"] >= 2 or number == 4):
                row["errors"].append("Repair denied: no repair opportunity or subsequent review remains")
        row["evidence_after_sha256"] = digest(self.cases)
        self.consume(row)
        previous = {k: deepcopy(row[k]) for k in ("round", "review", "accepted", "errors", "admitted_probe_ids")}
        return row, review, item, admitted, previous

    def finish(self, item, completed, reason, code="accepted", routing=None):
        history = self.context["prior_machine_history"] + [dict(stage_index=self.index, completed=completed,
            source_sha256=digest(item["files"]), evidence_sha256=digest(self.cases),
            outcomes=[dict(case_id=case["id"], requirement=case["requirement"], passed=item["matrix"][case["id"]]["passed"])
                      for case in self.cases])]
        remaining = sorted({case["requirement"] for case in self.cases if not item["matrix"][case["id"]]["passed"]}
                           | set(item["remaining"]) | set(item["reviewer_remaining"]))
        expected = dict(files=item["files"], completed=completed, status="complete" if completed else "bounded_incomplete",
            reason=reason, reason_code=code, source_valid=item["source_valid"], selected=item["alias"], selected_binding=item["binding"],
            notes=self.notes(item["notes"]), remaining=[] if completed else remaining, check_history=history,
            probe_pool=self.pool, retired_probes=self.retired, evidence_cases=self.cases, evidence_sha256=digest(self.cases),
            matrix={alias: {case["id"]: candidate["matrix"][case["id"]] for case in self.cases} for alias, candidate in self.candidates.items()},
            candidate_order=self.order, alias_map=self.alias_map, metrics=self.metrics, attempts=self.metrics["builder_calls"],
            candidates={alias: {key: value for key, value in candidate.items() if key != "files"} for alias, candidate in self.candidates.items()},
            escalations=self.escalations, repair_comparisons=self.comparisons)
        for key, value in expected.items():
            require(same(self.stage.get(key), value), "Final reconstructed stage differs: " + key)
        require(("routing_stop" not in self.stage if routing is None else same(self.stage.get("routing_stop"), routing)),
                "Terminal/routing stop conflated")
        require(self.event_index == len(self.stage["trajectory"]) and self.gate_index == len(self.stage["probe_receipts"])
                and self.used_calls == set(self.calls) and self.used_validations == {row[0] for row in self.validations.values()},
                "Orphan controller event, call, probe gate or execution")
        paths = {inside(span["receipt_path"], self.root) for _, span in self.validations.values()}
        require(set((self.root / "evaluations").glob("*.json")) == paths, "Unlinked raw stage receipt")
        require(self.stage["scouts"]["admitted"] == self.metrics["admitted_scout_probes"]
                and self.stage["scouts"]["rejected"] == self.metrics["rejected_scout_probes"], "Scout outcome summary differs")
        return dict(initial_builders=self.metrics["initial_builder_calls"],
            shared_initial_opportunities=self.metrics["shared_initial_builder_opportunities"],
            builders=self.metrics["builder_calls"], reviewers=self.metrics["reviewer_calls"],
            scouts=0 if self.shared else 2, completed=completed, reason_code=code,
            initial_pool_sha256=self.stage["initial_pool_sha256"], physical_validations=len(self.used_validations))

    def current_repair(self, item, review, *, model="cheap", escalation_id=None):
        self.metrics["repairs"] += 1
        feedback = dict(review_notes=review["notes"] if review else "Review produced no valid decision",
            remaining=review["remaining"] if review else list(self.reqs),
            evidence=[dict(case=case, outcome=deepcopy(item["matrix"][case["id"]])) for case in public_view(self.cases)])
        if escalation_id is not None:
            feedback["continuation_escalation"] = dict(id=escalation_id,
                instruction="Review has stalled. Reconcile each unresolved requirement with the exact "
                "current source and executed evidence. Make a concrete repair if justified. "
                "Do not make cosmetic changes to manufacture progress. An unchanged or rejected "
                "proposal preserves retained-source eligibility, but never grants acceptance.")
        return self.build(item["slot"], self.metrics["repairs"] + 1, item, "repair", feedback, model, escalation_id)

    def current(self):
        previous, pending, stagnant = None, None, 0
        seen = {self.progress()}
        for number in range(1, 5):
            before, changes, probes = self.progress(), self.metrics["effective_source_changes"], self.metrics["admitted_new_probes"]
            continuation = dict(protocol=PROTOCOL, limits=LIMITS, reviews_remaining_after_this=4 - number,
                repairs_remaining=2 - self.metrics["repairs"], escalations_remaining=1 - self.metrics["escalations"],
                consecutive_reviews_without_progress=stagnant, escalation=deepcopy(pending))
            row, review, item, admitted, previous = self.review(number, previous, continuation,
                self.prompts["escalation_suffix"] if pending else "")
            pending = None
            if row["accepted"]:
                return self.finish(item, True, "Reviewer acceptance passed every active public and oracle-admissible probe")
            did_repair = False
            defer = (review is not None and review["action"] == "repair" and not admitted and stagnant + 1 >= 2
                     and self.metrics["escalations"] < 1 and self.metrics["repairs"] < 2 and number < 4)
            if review and review["action"] == "repair" and self.metrics["repairs"] < 2 and number < 4 and not defer:
                self.current_repair(item, review)
                did_repair = True
            after = self.progress()
            novel, revisited = after != before and after not in seen, after != before and after in seen
            seen.add(after)
            stagnant = 0 if novel else stagnant + 1
            self.consume(dict(kind="progress", round=number, before_sha256=before, after_sha256=after,
                novel_progress=novel, revisited_state=revisited,
                effective_source_changes=self.metrics["effective_source_changes"] - changes,
                admitted_probes=self.metrics["admitted_new_probes"] - probes, consecutive_stagnant_reviews=stagnant))
            if stagnant < 2:
                continue
            self.metrics["stagnation_events"] += 1
            if number == 4 or self.metrics["escalations"] >= 1:
                return self.finish(self.fallback(), False,
                    "Review stalled without new evidence or an effective novel source; continuation capacity exhausted",
                    "stagnation_capacity_exhausted")
            item = self.candidates[review["candidate"]] if review else self.fallback()
            repair = not did_repair and self.metrics["repairs"] < 2
            escalation = dict(id=f"stage-{self.index}-escalation-{self.metrics['escalations'] + 1}",
                trigger="review_stagnation", review_round=number, candidate=item["alias"], consecutive_stagnant_reviews=stagnant,
                state_before_sha256=after, source_before_sha256=digest(item["files"]), evidence_before_sha256=digest(self.cases),
                action="strong_repair" if repair else "escalated_review", repair_capacity_before=2 - self.metrics["repairs"],
                review_capacity_after=4 - number, new_probe_capacity_remaining=8 - self.metrics["admitted_new_probes"])
            self.metrics["escalations"] += 1
            if repair:
                self.metrics["escalated_repairs"] += 1
                self.current_repair(item, review, model="strong", escalation_id=escalation["id"])
            else:
                self.metrics["escalated_reviews"] += 1
            escalation.update(state_after_sha256=self.progress(), source_after_sha256=digest(self.candidates[item["alias"]]["files"]),
                              evidence_after_sha256=digest(self.cases))
            escalation["effective_source_change"] = escalation["source_after_sha256"] != escalation["source_before_sha256"]
            self.escalations.append(deepcopy(escalation))
            self.consume(dict(kind="escalation", **escalation))
            pending = escalation
            seen.add(self.progress())
            stagnant = 0
        item = self.fallback()
        return self.finish(item, False,
            "Review capacity exhausted; retained candidate still requires explicit valid acceptance", terminal_reason(item, self.cases))

    def alternative(self, item, routed):
        failures = self.failures(item)
        possible = [other for alias in self.order if (other := self.candidates[alias])["source_valid"]
                    and digest(other["files"]) != digest(item["files"])
                    and (digest(item["files"]), digest(other["files"]), digest(self.cases)) not in routed
                    and self.failures(other) < failures]
        return min(possible, key=lambda other: (len(self.failures(other)), self.order.index(other["alias"]))) if possible else None

    def improved_repair(self, item, review, number, stagnant):
        self.metrics["repairs"] += 1
        count = self.metrics["repairs"]
        slot, alias = f"repair-{count}", ("candidate-E", "candidate-F")[count - 1]
        self.alias_map[slot] = alias
        self.order.append(alias)
        model, escalation = "cheap", None
        if stagnant and self.metrics["escalations"] < 1:
            model = "strong"
            escalation = dict(id=f"stage-{self.index}-escalation-{self.metrics['escalations'] + 1}",
                trigger="repair_without_failure_progress", review_round=number, candidate=item["alias"], action="strong_repair",
                source_before_sha256=digest(item["files"]), evidence_before_sha256=digest(self.cases))
            self.metrics["escalations"] += 1
            self.metrics["escalated_repairs"] += 1
        failures = self.failures(item)
        feedback = dict(review_notes=review["notes"] if review else "No valid reviewer decision; repair executed failures only",
            remaining=sorted({case["requirement"] for case in self.cases if case["id"] in failures}),
            controller_instruction="Repair the bound executed failures while preserving every passing active case. "
                "Your predecessor remains selectable; a source change alone is not progress.",
            predecessor_source_sha256=digest(item["files"]),
            evidence=[dict(case=case, outcome=deepcopy(item["matrix"][case["id"]])) for case in public_view(self.cases)])
        repaired = self.build(slot, count + 1, item, "repair", feedback, model, escalation["id"] if escalation else None)
        comparison = {**same_case_comparison(item["matrix"], repaired["matrix"], self.cases),
            "kind": "repair_comparison", "review_round": number, "predecessor": item["alias"], "repaired": alias,
            "predecessor_binding": deepcopy(item["binding"]), "repaired_binding": deepcopy(repaired["binding"]),
            "predecessor_receipts": deepcopy(item["validation_receipts"]), "repaired_receipts": deepcopy(repaired["validation_receipts"]),
            "predecessor_observations": "retained source-bound active evidence; no new execution claimed",
            "repaired_observations": "fresh execution of the full ordered active suite"}
        self.comparisons.append(deepcopy(comparison))
        self.consume(comparison)
        self.metrics["retained_repair_checkpoints"] += 1
        self.metrics["repair_regressions"] += int(comparison["regression"])
        self.metrics["repair_failure_progress"] += int(comparison["failure_progress"])
        self.metrics["resolved_known_failures"] += len(comparison["resolved_failures"])
        self.metrics["introduced_failures"] += len(comparison["introduced_failures"])
        if escalation:
            escalation.update(source_after_sha256=digest(repaired["files"]), evidence_after_sha256=digest(self.cases))
            self.escalations.append(deepcopy(escalation))
            self.consume(dict(kind="escalation", **escalation))
        return repaired, comparison

    def improved(self):
        previous, pending, stagnant = None, None, 0
        routed: set[tuple[str, str, str]] = set()
        focused_requests: set[tuple[str, tuple[str, ...]]] = set()
        for number in range(1, 5):
            continuation = dict(protocol=PROTOCOL, controller_mode="improved", limits=LIMITS,
                reviews_remaining_after_this=4 - number, repairs_remaining=2 - self.metrics["repairs"],
                escalations_remaining=1 - self.metrics["escalations"], repairs_without_failure_progress=stagnant,
                required_next_action=deepcopy(pending), repair_comparisons=[{k: deepcopy(row[k]) for k in
                    ("review_round", "predecessor", "repaired", "case_ids", "resolved_failures", "introduced_failures",
                     "persisting_failures", "regression", "failure_progress")} for row in self.comparisons])
            row, review, item, admitted, previous = self.review(number, previous, continuation,
                self.prompts["focus_suffix"] if pending and pending["action"] == "request_focused_probe" else "")
            if row["accepted"]:
                return self.finish(item, True, "Explicit reviewer acceptance passed every active source and execution gate")
            failures = self.failures(item)
            focused = [case["id"] for case in admitted if review and case["requirement"] in review["remaining"]]
            route = dict(kind="routing", round=number, candidate=item["alias"], source_sha256=digest(item["files"]),
                evidence_sha256=digest(self.cases), executed_failure_ids=sorted(failures), source_valid=item["source_valid"],
                focused_admitted_probe_ids=focused, repairs_remaining=2 - self.metrics["repairs"], reviews_remaining=4 - number,
                failure_progress=False)
            stop = None
            if number == 4:
                route["action"] = "stop"
                stop = "active_checks_failed" if failures else "source_ineligible" if not item["source_valid"] else "review_acceptance_missing"
            elif failures:
                alternative = self.alternative(item, routed)
                if alternative:
                    edge = (digest(item["files"]), digest(alternative["files"]), digest(self.cases))
                    routed.add(edge)
                    route.update(action="review_alternative", target=alternative["alias"], target_source_sha256=edge[1],
                                 target_failure_ids=sorted(self.failures(alternative)))
                    self.metrics["alternative_routes"] += 1
                elif self.metrics["repairs"] < 2:
                    repaired, comparison = self.improved_repair(item, review, number, stagnant)
                    stagnant = 0 if comparison["failure_progress"] else stagnant + 1
                    route.update(action="review_repair", target=repaired["alias"], target_source_sha256=digest(repaired["files"]),
                                 failure_progress=comparison["failure_progress"], regression=comparison["regression"])
                else:
                    route["action"], stop = "stop", "failure_repair_capacity_exhausted"
            elif not item["source_valid"]:
                route["action"], stop = "stop", "source_ineligible"
            elif focused:
                route["action"] = "review_focused_evidence"
                self.metrics["focused_evidence_steps"] += 1
            else:
                key = (digest(item["files"]), tuple(sorted(review["remaining"]))) if review else None
                if review and review["remaining"] and key is not None and key not in focused_requests:
                    focused_requests.add(key)
                    route.update(action="request_focused_probe", target=item["alias"], target_source_sha256=digest(item["files"]),
                                 focus_remaining=list(key[1]))
                    self.metrics["focused_review_requests"] += 1
                else:
                    route["action"] = "stop"
                    stop = "invalid_review_unresolved" if review is None else "uncertainty_without_focused_evidence"
            if stop:
                route["reason_code"] = stop
            self.consume(route)
            if stop:
                selected = self.fallback()
                return self.finish(selected, False,
                    "Bounded stop: explicit acceptance or a permitted evidence/repair-and-review step is absent",
                    terminal_reason(selected, self.cases), route)
            pending = route
        raise ValueError("Unterminated bounded continuation")


def audit_stage(stage, project, index, policy, initial, prior, calls, contract, root,
                git_files, spans, run_id, prompts, shared=None):
    """Audit one exact stage; caller must authenticate shared input and raw calls.

    ``shared`` is either None or ``{pool, binding, scouts}``, where binding is the
    complete authenticated frozen_initial_pool envelope and scouts is the shared
    raw scout evidence artifact. No original shared observation grants a consumer
    arm correctness judgment: its full initial suite must execute freshly.
    """
    replay = StageReplay(stage, project, index, policy, initial, prior, calls, contract,
                         root, git_files, spans, run_id, prompts, shared)
    replay.initialize()
    return replay.current() if replay.mode == "current" else replay.improved()
