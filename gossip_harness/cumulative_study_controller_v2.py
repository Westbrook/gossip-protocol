"""Prospective six-trajectory, four-milestone composition; not quality authority.

Public progress is cumulative and durable. S task eligibility and reviewer choices
originate in role processes; O task admissions originate in the central journal.
Both use the same mesh. A financial seal closes spending, never certifies quality.
This module deliberately has no credential discovery, wallet creation or live CLI.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from pathlib import Path
import time
from typing import Any, Callable, Protocol

from .candidate_checkpoint_chain_v1 import CheckpointChain, PrefixCommitment
from .peer_financial_authority_v5 import REQUIRED_SOURCES as FINANCIAL_SOURCES
from .peer_financial_authority_v2 import ledger_identity
from .peer_financial_terminal_v1 import (ChildRegistration, ChildTerminalSeal,
    TerminalRoster, canonical_payload)
from .peer_financial_terminal_v2 import CLOSURE_POLICY, checked_policy, verified_study_barrier
from .peer_project_contract_v2 import (Context, EvidenceRef, WorkKey, identifier,
    sha256, to_dict)
from .peer_role_loop_v2 import WorkDirective
from .project_acceptance_compiler_v1 import (ARMS, BLOCKS, FAULTS, CohortDesign,
    PRODUCT_V2_SHA256)

PROTOCOL = "cumulative-study-controller-v2"
MILESTONES = ("M1", "M2", "M3", "M4")
PACKAGES = ("catalog", "ingestion", "query", "clients")
REVIEWERS = ("R1", "R2", "R3", "R4")
PUBLIC_PURPOSE = "public-cumulative-development-v1"
TRANSPORT_CONTRACT: dict[str, Any] = {"protocol": "peer-mesh-v2", "mode": "gossip", "interval": .05, "fanout": 2,
    "max_payloads": 2048, "max_events": 8192, "max_reserved_bytes": 1073741824}
SHARED_POLICY = {"protocol": PROTOCOL, "eligibility": "complete-arrived-source-and-evidence",
    "roles": "fixed-package-builder-pools-and-one-reviewer-per-package",
    "selection": "one-exact-scoped-reviewer-choice-per-package",
    "repair": "all-builders-next-generation-with-full-public-feedback",
    "unknown_effect": "never-redispatch", "full_acceptance": "unavailable",
    "financial_closure_policy": CLOSURE_POLICY}

SOURCE_CLOSURE = tuple(sorted(set(FINANCIAL_SOURCES) | {"gossip_harness/" + name for name in (
    "cumulative_study_controller_v2.py", "cumulative_study_runtime_v2.py", "cumulative_study_role_v1.py",
    "cumulative_process_evidence_v1.py", "peer_mesh_v2.py", "peer_mesh_store_v2.py", "peer_mesh_finance_v2.py",
    "peer_role_loop_v2.py", "gitstore.py", "sandbox.py", "project_acceptance_compiler_v1.py",
    "candidate_observation_admission_v1.py", "candidate_release_execution_v2.py",
    "cumulative_cli_projection_v1.py", "cumulative_workflow_exposure_v1.py",
    "cumulative_observation_profile_v1.py", "project_acceptance_registry_v1.py",
    "library_cumulative_public_fixture_v1.py", "library_cumulative_public_fixture_v2.py")}
    | {"library-cumulative-product-v2.json", "docs/cumulative-cli-result-projection-v1.txt", "docs/cumulative-workflow-v2-addendum-v4.txt"}))


def fault_schedule(block: str, partition_seconds: float) -> dict[str, Any]:
    return {"protocol": PROTOCOL + "-faults", "block": block,
        "faults": [] if block == "healthy" else [
            {"kind": FAULTS[0], "role": "B01", "milestone": 1, "generation": 0,
             "boundary": "durable-completed-financial-result-before-role-publication", "exit_code": 86},
            {"kind": FAULTS[1], "role": "B01", "milestone": 1, "generation": 0,
             "boundary": "before-first-build-work-publication", "seconds": partition_seconds,
             "duration_origin": "all-peer-block-acknowledgments",
             "heal_after": "initial-work-frontier-committed-and-duration-elapsed",
             "scope": "symmetric-all-peer-links"}]}


class StudyError(ValueError):
    """Controller/provenance failure; never product rejection or acceptance."""


class StudyUnknown(StudyError):
    """An effect intent exists without a known result. Never redispatch it."""


class StudyStop(StudyError):
    """A bounded unsuccessful trajectory (horizon, resources or public failure)."""


def require(condition: bool, detail: str) -> None:
    if not condition:
        raise StudyError(detail)


def plain(value: Any) -> Any:
    if isinstance(value, (tuple, list)):
        return [plain(item) for item in value]
    if type(value) is dict:
        return {key: plain(item) for key, item in value.items()}
    return value


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_payload(plain(value))).hexdigest()


def actors_for(arm: str) -> tuple[str, ...]:
    require(arm in ARMS, "Unknown study arm")
    builders = ("B01", "B05", "B09", "B13") if arm == "S4-G" else tuple(
        f"B{i:02}" for i in range(1, 17))
    return builders + REVIEWERS


def package_for(actor: str) -> str:
    role = actor.rsplit(".", 1)[-1]
    if role in REVIEWERS:
        return PACKAGES[REVIEWERS.index(role)]
    require(role in actors_for("S16-G"), "Unknown builder")
    return PACKAGES[(int(role[1:]) - 1) // 4]


@dataclass(frozen=True)
class Release:
    """Caller-authored staged public requirements/checks, exact bytes frozen.

    Public checks are deliberately not generated from the prose here. Missing
    tests keep this release unavailable. A complete release requires independent
    semantic review outside this controller; these declarations grant none.
    """
    milestone: str
    instructions: str
    files: dict[str, str]
    checks: dict[str, str]
    command: tuple[str, ...]
    ordered_check_ids: tuple[str, ...]
    requirement_ids: tuple[str, ...]
    source_contract_sha256: str = PRODUCT_V2_SHA256
    purpose: str = PUBLIC_PURPOSE

    def __post_init__(self) -> None:
        require(self.milestone in MILESTONES and self.source_contract_sha256 == PRODUCT_V2_SHA256
                and self.purpose == PUBLIC_PURPOSE, "Wrong cumulative release identity")
        require(type(self.instructions) is str and bool(self.instructions), "Missing authored requirements")
        for files in (self.files, self.checks):
            require(type(files) is dict, "Exact release file mapping required")
            for path, value in files.items():
                require(type(path) is str and bool(path) and not path.startswith("/")
                        and not any(x in ("", ".", "..", ".git") for x in path.split("/"))
                        and "\\" not in path and type(value) is str, "Unsafe release file")
        require(bool(self.command) and all(type(x) is str and x for x in self.command), "Missing public command")
        for values in (self.ordered_check_ids, self.requirement_ids):
            require(type(values) is tuple and bool(values) and len(set(values)) == len(values),
                    "Missing/duplicate ordered release inventory")
            for value in values:
                identifier(value)

    @property
    def sha256(self) -> str:
        return digest(asdict(self))


@dataclass(frozen=True)
class StudyPlan:
    cohort: CohortDesign
    releases: tuple[Release, ...]
    initial_files: dict[str, str]
    package_paths: dict[str, tuple[str, ...]]
    source_pins: dict[str, str]
    runtime: dict[str, Any]
    financial_closure_policy: dict[str, Any]
    horizon_seconds: int = 7200
    source_generations: int = 3
    partition_seconds: float = 2.0
    executor_slots: int = 4

    def __post_init__(self) -> None:
        checked_policy(self.financial_closure_policy)
        require(type(self.runtime) is dict and self.runtime.get("protocol") == "cumulative-study-runtime-v2"
                and self.runtime.get("financial_protocol") == "peer-financial-authority-v5"
                and self.runtime.get("financial_rpc_protocol") == "peer-financial-rpc-v5",
                "Exact V2 runtime and V5 financial identity required")
        require(type(self.cohort) is CohortDesign, "Exact compiler cohort required")
        require(self.cohort.milestones == MILESTONES and len(self.cohort.trajectories) == 6,
                "All four milestones and six trajectories are mandatory")
        require({(t.arm, t.block) for t in self.cohort.trajectories} == {
            (a, b) for a in ARMS for b in BLOCKS}, "Incomplete matched arm/block matrix")
        require(len({t.id for t in self.cohort.trajectories}) == 6, "Duplicate trajectory")
        for t in self.cohort.trajectories:
            identifier(t.id)
            require(t.role_ids == actors_for(t.arm), "Changed cognitive role census")
            require(t.decision_placement == ("durable_central_scheduler" if t.arm == "O16-G" else "peer_local"),
                    "Changed placement contrast")
            require(t.faults == (FAULTS if t.block == "compound_recovery" else ()), "Changed fault contract")
            require(t.fault_schedule_sha256 == digest(fault_schedule(t.block, self.partition_seconds)),
                    "Fault hash is not the implemented schedule")
            for value in (t.block_seed_sha256, t.fault_schedule_sha256):
                sha256(value)
        for block in BLOCKS:
            require(len({(t.block_seed_sha256, t.fault_schedule_sha256) for t in self.cohort.trajectories
                         if t.block == block}) == 1, "Unmatched exogenous seed/fault schedule")
        require(type(self.releases) is tuple and tuple(x.milestone for x in self.releases) == MILESTONES,
                "Complete ordered requirement releases required")
        require(self.cohort.requirements_release_sha256 == digest([asdict(x) for x in self.releases]),
                "Requirement release pin differs")
        require(type(self.horizon_seconds) is int and 1 <= self.horizon_seconds <= 86400
                and type(self.source_generations) is int and 1 <= self.source_generations <= 3
                and type(self.partition_seconds) in (int, float) and 0 < self.partition_seconds <= 60
                and type(self.executor_slots) is int and self.executor_slots == 4, "Invalid common limits")
        require(self.cohort.transport_sha256 == digest(TRANSPORT_CONTRACT)
                and self.cohort.shared_policy_sha256 == digest(SHARED_POLICY), "Actual transport/policy pin differs")
        resource = {"horizon_seconds": self.horizon_seconds, "source_generations": self.source_generations,
            "partition_seconds": self.partition_seconds, "executor_slots": self.executor_slots,
            "runtime": self.runtime}
        require(self.cohort.resource_contract_sha256 == digest(resource), "Actual common resource pin differs")
        require(set(self.package_paths) == set(PACKAGES) and bool(self.initial_files)
                and bool(self.source_pins) and bool(self.runtime), "Incomplete study identity")
        require(set(SOURCE_CLOSURE) <= set(self.source_pins), "Actual controller dependency closure incomplete")
        require(all(type(paths) is tuple and bool(paths) and all(not p.endswith("/") for p in paths)
                    for paths in self.package_paths.values()), "Exact nonempty writable file rosters required")
        for path, value in self.source_pins.items():
            require(not Path(path).is_absolute() and ".." not in Path(path).parts, "Unsafe source pin")
            sha256(value)
        scopes = [path for paths in self.package_paths.values() for path in paths]
        require(len(scopes) == len(set(scopes)), "Shared writable path needs an explicit ownership policy")
        for index, path in enumerate(scopes):
            require(bool(path) and not path.startswith("/") and ".." not in Path(path).parts, "Unsafe candidate scope")
            require(not any(other.startswith(path.rstrip("/") + "/") for other in scopes[index+1:])
                    and not any(path.startswith(other.rstrip("/") + "/") for other in scopes[index+1:]),
                    "Overlapping ownership roots")
        for release in self.releases:
            for path in release.files:
                require(not any(path == p.rstrip("/") or path.startswith(p.rstrip("/") + "/") for p in scopes),
                        "Public immutable fixture overlaps candidate scope")

    def record(self) -> dict[str, Any]:
        return {"protocol": PROTOCOL, **plain(asdict(self))}

    @property
    def sha256(self) -> str:
        return digest(self.record())

    @property
    def roster(self) -> TerminalRoster:
        return TerminalRoster(self.sha256, tuple(ChildRegistration(
            self.cohort.cohort_id + "." + t.id, t.id, tuple(t.id + "." + role for role in t.role_ids))
            for t in self.cohort.trajectories))

    def verify_sources(self, repository: Path) -> None:
        self.__post_init__()  # Recheck nested prospective policy/runtime before each dispatch boundary.
        from . import cumulative_cli_projection_v1 as projection
        projection.validate_plan(self, repository)
        from . import cumulative_workflow_exposure_v1 as workflow
        workflow.validate_plan(self, repository)
        require(all((repository / name).is_file()
                    and hashlib.sha256((repository / name).read_bytes()).hexdigest() == pin
                    for name, pin in self.source_pins.items()), "Controller source inventory changed")


class Records:
    """Deterministic immutable slots in the caller's independently anchored chain."""
    def __init__(self, chain: CheckpointChain):
        require(type(chain) is CheckpointChain, "Actual checkpoint chain required")
        self.chain = chain

    @staticmethod
    def name(key: str) -> str:
        return "study-" + hashlib.sha256(key.encode()).hexdigest() + ".json"

    def read(self, key: str) -> dict[str, Any] | None:
        import json
        name = self.name(key)
        if not self.chain.has(name):
            return None
        raw = self.chain.read(name)
        value = json.loads(raw)
        require(type(value) is dict and canonical_payload(value) == raw and value.get("slot") == key,
                "Controller slot changed")
        return value["value"]

    def put(self, key: str, value: dict[str, Any]) -> dict[str, Any]:
        value = plain(value)
        existing = self.read(key)
        require(existing is None or existing == value, "Immutable controller slot conflict: " + key)
        if existing is None:
            self.chain.retain(self.name(key), canonical_payload({"slot": key, "value": value}))
        return value

    def effect(self, key: str, request: dict[str, Any], operation: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        """Unknown effects are locked; a missing response is never a retry permit."""
        result = self.read(key + ".result")
        intent = self.read(key + ".intent")
        if intent is not None:
            require(intent == request, "Effect identity changed")
            if result is None:
                raise StudyUnknown("Unresolved effect: " + key)
            return result
        require(result is None, "Result without original intent")
        self.put(key + ".intent", request)
        self.chain.validate_boundary()
        result = operation()
        self.chain.validate_boundary()
        return self.put(key + ".result", result)


@dataclass(frozen=True)
class PublicResult:
    status: str
    commit_oid: str
    source_sha256: str
    release_sha256: str
    ordered_check_ids: tuple[str, ...]
    outcomes: tuple[tuple[str, str], ...]
    raw_receipt: dict[str, Any]
    purpose: str = PUBLIC_PURPOSE

    @property
    def passed(self) -> bool:
        return self.status == "completed" and bool(self.outcomes) and all(x[1] == "passed" for x in self.outcomes)

    def validate(self, release: Release, commit: str, source: str) -> None:
        require(self.purpose == PUBLIC_PURPOSE and self.release_sha256 == release.sha256
                and self.commit_oid == commit and self.source_sha256 == source
                and self.ordered_check_ids == release.ordered_check_ids
                and tuple(x[0] for x in self.outcomes) == release.ordered_check_ids
                and self.status in ("completed", "unavailable", "infrastructure_error")
                and all(x[1] in ("passed", "failed", "unknown") for x in self.outcomes),
                "Public evaluator returned incomplete or rebound evidence")


class ChildRuntime(Protocol):
    """Concrete mesh/financial/Git runtime supplied by cumulative_study_runtime_v2."""
    def source(self) -> dict[str, Any]: ...
    def release(self, release: Release) -> dict[str, Any]: ...
    def publish(self, kind: str, value: dict[str, Any], key: str) -> EvidenceRef: ...
    def work(self, stage: str, directives: tuple[tuple[str, WorkDirective], ...]) -> tuple[dict[str, Any], ...]: ...
    def integrate(self, stage: str, builds: tuple[dict[str, Any], ...], reviews: tuple[dict[str, Any], ...]) -> dict[str, Any]: ...
    def evaluate(self, release: Release) -> PublicResult: ...
    def stop_and_seal(self, status: str, milestone_records: tuple[str, ...]) -> ChildTerminalSeal: ...
    def close(self) -> None: ...


class UnavailableAcceptance:
    """No public pass or financial terminal may stand in for full product authority."""
    def evaluate(self, plan: StudyPlan, barrier: dict[str, Any], originals: tuple[dict[str, Any], ...]) -> dict[str, Any]:
        return {"protocol": PROTOCOL + "-acceptance-unavailable", "status": "unavailable",
            "accepted": False, "study_sha256": plan.sha256, "barrier_sha256": digest(barrier),
            "trajectory_ids": [t.id for t in plan.cohort.trajectories],
            "reason": "Full reviewed compiler scope, independent product observers, promotion provenance and repeatability remain mandatory",
            "public_progress_is_acceptance": False}


class StudyController:
    def __init__(self, plan: StudyPlan, *, checkpoint: CheckpointChain,
                 expected_checkpoint: PrefixCommitment, repository: Path,
                 existing_ledger_path: Path, expected_ledger_identity: dict[str, Any],
                 runtime_factory: Callable[[StudyPlan, int, Records, float], ChildRuntime],
                 clock: Callable[[], float] = time.time):
        require(type(plan) is StudyPlan and type(checkpoint) is CheckpointChain, "Typed original plan and chain required")
        checkpoint.validate_boundary(expected=expected_checkpoint)
        plan.verify_sources(repository)
        self.plan, self.records, self.repository = plan, Records(checkpoint), repository
        require(ledger_identity(existing_ledger_path) == expected_ledger_identity,
                "Original cumulative ledger identity differs before controller admission")
        self.ledger, self.ledger_identity = existing_ledger_path, dict(expected_ledger_identity)
        self.runtime_factory, self.clock = runtime_factory, clock
        self.records.put("contract", plan.record())
        self.records.put("ledger", expected_ledger_identity)
        # Until the full acceptance adapter is integrated, the only route is
        # explicit unavailable. Arbitrary callback booleans cannot grant acceptance.
        self.acceptance = UnavailableAcceptance()

    def _directives(self, child_index: int, milestone: int, generation: int,
                    source: EvidenceRef, evidence: tuple[EvidenceRef, ...], *, reviews: bool) -> tuple[tuple[str, WorkDirective], ...]:
        t = self.plan.cohort.trajectories[child_index]
        release = self.plan.releases[milestone - 1]
        context = Context(self.plan.sha256, self.plan.roster.children[child_index].cohort,
                          t.id, milestone, release.sha256)
        out = []
        for role in t.role_ids:
            if (role in REVIEWERS) != reviews:
                continue
            actor = t.id + "." + role
            package = package_for(actor)
            paths: tuple[str, ...]
            if reviews:
                instructions = (release.instructions + "\nReview every locally arrived complete proposal for " + package
                    + ". Return only decision.json with JSON fields stage_id, package, selected_actor, reasons, tests. "
                      "selected_actor names one eligible candidate for this package, or null if none is defensible; "
                      "tests is an array of concrete high-signal checks. Do not claim execution of unrun tests.")
                paths, profile, kind = ("decision.json",), "strong", "review"
            else:
                instructions = (release.instructions + "\nImplement all currently released inherited and new requirements for "
                    + package + ". Read complete current source and public feedback. Preserve other packages and immutable fixtures. "
                    "Return full changed-file contents within the exact writable path roster.")
                paths, profile, kind = self.plan.package_paths[package], "mini", "build" if generation == 0 else "repair"
            directive = WorkDirective(context, WorkKey(package, release.milestone, role, generation),
                profile, kind, source, evidence, paths, instructions)
            out.append((actor, directive))
        return tuple(out)

    def _retain_results(self, key: str, results: tuple[dict[str, Any], ...]) -> None:
        originals = []
        for index, value in enumerate(results):
            slot = key + ".original." + str(index)
            self.records.put(slot, value)
            originals.append({"slot": slot, "name": self.records.name(slot), "sha256": digest(value)})
        self.records.put(key, {"originals": originals, "count": len(originals)})

    def _child(self, index: int) -> dict[str, Any]:
        self.plan.verify_sources(self.repository)
        t = self.plan.cohort.trajectories[index]
        key = "child." + t.id
        terminal = self.records.read(key + ".terminal")
        if terminal is not None:
            return terminal
        if index:
            prior = self.records.read("child." + self.plan.cohort.trajectories[index - 1].id + ".terminal")
            require(prior is not None and prior.get("financial_seal") is not None, "Previous child has no immutable financial seal")
        require(ledger_identity(self.ledger) == self.ledger_identity,
                "Original cumulative ledger identity differs before child admission")
        start = self.records.read(key + ".begin")
        if start is None:
            start = self.records.put(key + ".begin", {"trajectory": plain(asdict(t)),
                "started_at": self.clock(), "deadline": self.clock() + self.plan.horizon_seconds,
                "contract_sha256": self.plan.sha256})
        deadline = start["deadline"]
        runtime = self.runtime_factory(self.plan, index, self.records, deadline)
        try:
            milestones: list[str] = []
            status, reason = "completed", "all_four_public_milestones_completed"
            try:
                for number, release in enumerate(self.plan.releases, 1):
                    mkey = key + "." + release.milestone
                    if self.records.read(mkey + ".terminal") is not None:
                        milestones.append(mkey + ".terminal")
                        require(self.records.read(mkey + ".terminal")["status"] == "public_completed", "Prior milestone unsuccessful")  # type: ignore[index]
                        continue
                    if self.clock() >= deadline:
                        raise StudyStop("deadline_exhausted")
                    self.plan.verify_sources(self.repository)
                    self.records.chain.validate_boundary()
                    released = runtime.release(release)
                    self.records.put(mkey + ".release", released)
                    reached = False
                    for generation in range(self.plan.source_generations):
                        gkey = mkey + ".g" + str(generation)
                        done = self.records.read(gkey + ".terminal")
                        if done is not None:
                            if done["public_passed"]:
                                reached = True
                                break
                            continue
                        if self.clock() >= deadline:
                            raise StudyStop("deadline_exhausted")
                        source = runtime.source()
                        source_ref = runtime.publish("project-source", {"files": source["files"], "base_sha": source["commit_oid"]}, gkey + ".source")
                        evidence: tuple[EvidenceRef, ...] = ()
                        if generation:
                            previous = self.records.read(mkey + ".g" + str(generation-1) + ".terminal")
                            require(previous is not None, "Missing preceding public repair evidence")
                            assert previous is not None
                            evidence = (runtime.publish("cumulative-feedback", previous, gkey + ".feedback"),)
                        builds = runtime.work(gkey + ".build", self._directives(index, number, generation, source_ref, evidence, reviews=False))
                        self._retain_results(gkey + ".builds", builds)
                        # Every original stays in the checkpoint. Reviewers receive
                        # source once plus every complete proposal for their scope;
                        # repeated whole source is never multiplied by builder count.
                        review_directives = []
                        for actor, directive in self._directives(index, number, generation, source_ref, (), reviews=True):
                            package = package_for(actor)
                            scoped = [{"actor": item["actor"], "directive_id": item["directive_id"],
                                "original_ref": item["original_ref"], "snapshot": item["snapshot"],
                                "proposal": item["result_payload"]} for item in builds if package_for(item["actor"]) == package]
                            frontier = runtime.publish("cumulative-frontier", {"stage_id": gkey, "package": package,
                                "builds": scoped, "shared_source_ref": to_dict(source_ref)}, gkey + ".frontier." + package)
                            from dataclasses import replace
                            review_directives.append((actor, replace(directive, evidence_refs=(frontier,))))
                        reviews = runtime.work(gkey + ".review", tuple(review_directives))
                        self._retain_results(gkey + ".reviews", reviews)
                        # Git prepare/CAS is reconciled by exact before/after refs in
                        # runtime; a crash cannot cause a second cognitive dispatch.
                        integrated = runtime.integrate(gkey, builds, reviews)
                        self.records.put(gkey + ".integration", integrated)
                        current = runtime.source()
                        evaluation = self.records.effect(gkey + ".public-evaluation",
                            {"source_sha256": current["source_sha256"], "commit_oid": current["commit_oid"],
                             "release_sha256": release.sha256, "purpose": PUBLIC_PURPOSE},
                            lambda: plain(asdict(runtime.evaluate(release))))
                        result = PublicResult(**{**evaluation,
                            "ordered_check_ids": tuple(evaluation["ordered_check_ids"]),
                            "outcomes": tuple(tuple(x) for x in evaluation["outcomes"])})
                        result.validate(release, current["commit_oid"], current["source_sha256"])
                        if self.clock() >= deadline:
                            raise StudyStop("deadline_exhausted_after_public_evaluation")
                        public_passed = result.passed and integrated["status"] == "integrated"
                        record = self.records.put(gkey + ".terminal", {"milestone": release.milestone,
                            "generation": generation, "source_before": source["source_sha256"],
                            "source_after": current["source_sha256"], "commit_oid": current["commit_oid"],
                            "public_result": evaluation, "integration": integrated,
                            "public_passed": public_passed, "acceptance_authority": False})
                        runtime.publish("cumulative-progress", record, gkey + ".progress")
                        if result.status != "completed":
                            raise StudyStop("public_evaluation_" + result.status)
                        if public_passed:
                            reached = True
                            break
                    if not reached:
                        raise StudyStop("public_source_generations_exhausted")
                    if self.clock() >= deadline:
                        raise StudyStop("deadline_exhausted_before_milestone_completion")
                    self.records.put(mkey + ".terminal", {"milestone": release.milestone,
                        "status": "public_completed", "source": runtime.source(), "acceptance_authority": False})
                    milestones.append(mkey + ".terminal")
            except (StudyStop, StudyUnknown) as error:
                status, reason = "stopped_failure", str(error)
            except Exception as error:
                status, reason = "stopped_failure", type(error).__name__ + ": " + str(error)
            for release in self.plan.releases:
                mkey = key + "." + release.milestone + ".terminal"
                if self.records.read(mkey) is None:
                    self.records.put(mkey, {"milestone": release.milestone, "status": "unfinished",
                        "reason": reason, "acceptance_authority": False})
                    milestones.append(mkey)
            self.records.chain.validate_boundary()
            seal = runtime.stop_and_seal(status, tuple(milestones))
            require(type(seal) is ChildTerminalSeal, "Actual policy-bound V5 financial seal required")
            value = self.records.put(key + ".terminal", {"trajectory_id": t.id, "status": status,
                "reason": reason, "milestone_records": [key + "." + m + ".terminal" for m in MILESTONES],
                "final_source": runtime.source(), "financial_seal": {
                    "raw_utf8": seal.raw.decode(), "publication_name": seal.publication_name,
                    "commitment": asdict(seal.commitment)}, "acceptance_authority": False})
            return value
        finally:
            runtime.close()

    def run(self) -> dict[str, Any]:
        self.plan.verify_sources(self.repository)
        self.records.chain.validate_boundary()
        originals = tuple(self._child(index) for index in range(6))
        receipts = tuple(ChildTerminalSeal(row["financial_seal"]["raw_utf8"].encode(),
            row["financial_seal"]["publication_name"], PrefixCommitment(**row["financial_seal"]["commitment"]))
            for row in originals)
        expected = self.records.chain.validate_boundary().commitment
        barrier = verified_study_barrier(self.plan.roster, self.records.chain, expected, receipts,
            existing_ledger_path=self.ledger, expected_ledger_identity=self.ledger_identity)
        # Always reauthenticate original ledger/seals on resume, before using a
        # retained global barrier or acceptance record.
        barrier_key = "complete-cohort-barrier"
        prior = self.records.read(barrier_key)
        if prior is not None:
            require({k:v for k,v in prior.items() if k != "checkpoint"} ==
                    {k:v for k,v in barrier.items() if k != "checkpoint"}, "Original cohort barrier changed")
            barrier = prior
        else:
            self.records.put(barrier_key, barrier)
        self.records.chain.validate_boundary()
        result = self.records.effect("final-acceptance", {"protocol": PROTOCOL,
            "purpose": "independent-acceptance", "barrier_sha256": digest(barrier),
            "study_sha256": self.plan.sha256}, lambda: self.acceptance.evaluate(self.plan, barrier, originals))
        require(result["accepted"] is False and result["status"] == "unavailable",
                "This version has no complete product acceptance authority")
        self.plan.verify_sources(self.repository)
        self.records.chain.validate_boundary()
        return {"protocol": PROTOCOL, "status": "acceptance_unavailable", "accepted": False,
            "trajectory_count": 6, "role_count": 96, "milestone_count": 24,
            "public_completed": sum(row["status"] == "completed" for row in originals),
            "barrier": barrier, "acceptance": result, "checkpoint": asdict(self.records.chain.commitment)}
