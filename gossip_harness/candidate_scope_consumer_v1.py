"""Controller-capability consumer of a complete, independently registered design.

This module has no receipt-upload API. Its authority is injected by the trusted
host, owns retained originals and authenticates them before returning normalized
records. A Python interface or hash cannot establish that authority. The module
ships neither a production ScopePlan nor a qualification authority/recipe.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, replace
import hashlib
import json
import re
from typing import Any

from . import candidate_release_execution_v2 as release_v2
from . import project_acceptance_compiler_v1 as compiler
from . import project_acceptance_registry_v1 as registry
from .candidate_release_execution_v1 import CandidateReleaseExecution, ControllerCheckpoint

PROTOCOL = "candidate-scope-consumer-v1"
FACETS = {
    "M1-GATE-SCOPE": ("scope",),
    "M1-GATE-ADMISSION": ("admission",),
    "M1-GATE-CONTROL": ("semantic_conflict", "ancestry", "package_review", "integration_review"),
}


class AuthorityError(ValueError):
    """Invalid provenance or substituted registration; never a product defect."""


class AuthorityUnavailable(RuntimeError):
    """Trusted authority cannot currently determine a result; never a pass."""


def require(ok: bool, detail: str) -> None:
    if not ok:
        raise AuthorityError(detail)


def digest(value: Any) -> str:
    raw = json.dumps({"protocol": PROTOCOL, "body": value}, sort_keys=True,
                     ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()


def record_digest(value: Any) -> str:
    return digest({"kind": type(value).__name__, "value": asdict(value)})


@dataclass(frozen=True, slots=True)
class Revision:
    commit_oid: str
    tree_oid: str
    source_sha256: str

    def __post_init__(self) -> None:
        require(all(type(x) is str and re.fullmatch(r"[0-9a-f]{40}", x)
                    for x in (self.commit_oid, self.tree_oid)), "Expected full Git revisions")
        registry.sha256(self.source_sha256)


@dataclass(frozen=True, slots=True)
class ReviewTarget:
    role: str
    reviewer_id: str
    source: Revision
    decision: str
    definition_sha256: str

    def __post_init__(self) -> None:
        require(self.role in ("package", "integration") and self.decision in ("accept", "reject"),
                "Unknown review role/decision")
        registry.identifier(self.reviewer_id)
        require(type(self.source) is Revision, "Review requires exact source")
        registry.sha256(self.definition_sha256)


@dataclass(frozen=True, slots=True)
class ControlRecipe:
    """Prospective identities, never proof of clean merge, ancestry or review.

    Probe hashes bind complete ordered suite/evaluator/runtime/environment/limits/
    seed/protocol/purpose definitions retained by the authority, separately for
    merged-before and repaired-after source. Authority verification must inspect
    original actual Git ancestry, source changes, probes and both reviews.
    """
    base: Revision
    left: Revision
    right: Revision
    merged: Revision
    repaired: Revision
    left_paths: tuple[str, ...]
    right_paths: tuple[str, ...]
    before_probe_contract_sha256: str
    after_probe_contract_sha256: str
    reviews: tuple[ReviewTarget, ...]
    before_expected: str = "failed"
    after_expected: str = "passed"

    def __post_init__(self) -> None:
        revisions = (self.base, self.left, self.right, self.merged, self.repaired)
        require(all(type(row) is Revision for row in revisions), "Incomplete control source lineage")
        require(len({row.commit_oid for row in revisions}) == 5, "Collapsed control history")
        require(self.before_expected == "failed" and self.after_expected == "passed", "Control must fail then pass")
        for paths in (self.left_paths, self.right_paths):
            require(type(paths) is tuple and 0 < len(paths) <= registry.MAX_ITEMS
                    and len(paths) == len(set(paths)), "Invalid control changed-path census")
            require(all(type(path) is str and path and not path.startswith("/")
                        and all(part not in ("", ".", "..") for part in path.split("/"))
                        for path in paths), "Unsafe changed path")
        require(not set(self.left_paths) & set(self.right_paths), "Control branches must be disjoint")
        for value in (self.before_probe_contract_sha256, self.after_probe_contract_sha256):
            registry.sha256(value)
        require(type(self.reviews) is tuple and len(self.reviews) == 2
                and all(type(item) is ReviewTarget for item in self.reviews)
                and {item.role for item in self.reviews} == {"package", "integration"}
                and len({item.reviewer_id for item in self.reviews}) == 2,
                "Both distinct appropriate reviewers must be registered")
        require(all(item.source in revisions for item in self.reviews), "Review source outside control recipe")


@dataclass(frozen=True, slots=True)
class QualificationSpec:
    gate_id: str
    qualification_source_sha256: str
    definition_sha256: str
    # Exact assertion selectors into retained evaluator observations, not labels
    # assigned to arbitrary passed fields. Their semantic adequacy is reviewed.
    facet_selectors: tuple[tuple[str, str], ...]
    control_recipe: ControlRecipe | None = None

    def __post_init__(self) -> None:
        registry.identifier(self.gate_id)
        registry.sha256(self.qualification_source_sha256)
        registry.sha256(self.definition_sha256)
        require(type(self.facet_selectors) is tuple and 0 < len(self.facet_selectors) <= 6,
                "Qualification facets missing")
        require(all(type(row) is tuple and len(row) == 2 and type(row[0]) is str
                    and type(row[1]) is str and 0 < len(row[1]) <= 4096 for row in self.facet_selectors),
                "Invalid qualification selectors")
        require(len({row[0] for row in self.facet_selectors}) == len(self.facet_selectors), "Repeated facet")
        require(self.control_recipe is None or type(self.control_recipe) is ControlRecipe, "Wrong control recipe")


@dataclass(frozen=True, slots=True)
class RegisteredAcceptance:
    """Loaded from controller-owned prospective registration, never submission."""
    inventory_sha256: str
    scope_sha256: str
    declaration_sha256: str
    registry_design_sha256: str
    execution_contract_sha256: str
    qualification_specs: tuple[QualificationSpec, ...]

    def __post_init__(self) -> None:
        for value in (self.inventory_sha256, self.scope_sha256, self.declaration_sha256,
                      self.registry_design_sha256, self.execution_contract_sha256):
            registry.sha256(value)
        registry.members(self.qualification_specs, QualificationSpec)
        require(len({item.gate_id for item in self.qualification_specs}) == len(self.qualification_specs),
                "Duplicate qualification registration")


@dataclass(frozen=True, slots=True)
class QualificationRequest:
    registration: RegisteredAcceptance
    gate: compiler.ExecutionGate
    suite: compiler.SuiteDefinition
    specification: QualificationSpec
    # Subject-independent declared evaluator lineage; qualification's source is
    # its separate harness subject. Candidate source is authenticated downstream.
    product_lineages_sha256: str

    @property
    def sha256(self) -> str:
        return record_digest(self)


@dataclass(frozen=True, slots=True)
class QualificationEvidence:
    request_sha256: str
    qualification_source_sha256: str
    execution_id: str
    receipt_sha256: str
    verifier_receipt_sha256: str
    original_purpose: str
    terminal_status: str
    outcomes: tuple[registry.CaseResult, ...]
    facets: tuple[registry.CaseResult, ...]
    mode: str = "physical"
    reuse_receipt_sha256: str | None = None

    def __post_init__(self) -> None:
        for value in (self.request_sha256, self.qualification_source_sha256,
                      self.receipt_sha256, self.verifier_receipt_sha256):
            registry.sha256(value)
        registry.identifier(self.execution_id)
        registry.identifier(self.original_purpose)
        require(self.terminal_status in ("completed", "infrastructure_error"), "Unknown qualification terminal state")
        registry.members(self.outcomes, registry.CaseResult, nonempty=False)
        registry.members(self.facets, registry.CaseResult, nonempty=False)
        require(self.mode in ("physical", "reused"), "Unknown qualification mode")
        if self.mode == "reused":
            registry.sha256(self.reuse_receipt_sha256)
        else:
            require(self.reuse_receipt_sha256 is None, "Physical qualification carries reuse receipt")


class AuthoritySnapshot(ABC):
    """Host-only capability, with provenance verified against retained originals.

    Implementations must be constructed from controller-owned registrations,
    journal owners and externally retained checkpoints, never candidate artifacts.
    They must authenticate chronology and qualifying semantics, not trust JSON
    passed fields. `checkpoint` is an opaque retained snapshot identity;
    `check_current` rejects rollback, additions, substitution and revocation.
    Catching ordinary unavailability cannot bypass that final integrity check.
    """
    @property
    @abstractmethod
    def checkpoint(self) -> str: ...

    @abstractmethod
    def check_current(self, checkpoint: str) -> None: ...

    @abstractmethod
    def registration(self, subject: registry.Subject) -> RegisteredAcceptance | None: ...

    @abstractmethod
    def qualification(self, request: QualificationRequest) -> QualificationEvidence | None:
        """Authenticate original execution, exact lineage, facets and reuse chain.

        Scope means independent semantic review of the complete pinned mapping,
        purposes, scope disclosure and applicability; it is never hash equality.
        CONTROL verifies recipe's real Git ancestry/disjoint clean merge, source-
        bound failing and repaired passing public observations, and both actual
        role-appropriate review decisions. ADMISSION verifies exact target suite,
        evaluator and runtime isolation; it never qualifies product functionality.
        """

    @abstractmethod
    def freeze(self) -> registry.CohortFreeze | None:
        """Authenticate complete irreversible controller stop, not a boolean upload."""

    @abstractmethod
    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation | None:
        """Authenticate original dispatch/verdict and after-freeze chronology.

        A reused public observation must authenticate its original and full reuse
        authorization. Independent/repeatability are fresh physical observations.
        """

    @abstractmethod
    def promotion(self, subject: registry.Subject) -> registry.Promotion | None:
        """Verify exact Git source/CAS, scoped reviews and original promotion receipt."""


class EvidenceAuthority(ABC):
    @abstractmethod
    def open_snapshot(self) -> AuthoritySnapshot: ...


@dataclass(frozen=True, slots=True)
class Issue:
    phase: str
    target: str
    status: str
    detail: str


@dataclass(frozen=True, slots=True)
class ConsumerResult:
    status: str
    issues: tuple[Issue, ...]
    qualifications: tuple[QualificationEvidence, ...]
    product: registry.Assessment | None
    authority_checkpoint: str | None

    @property
    def accepted_against_registry(self) -> bool:
        return self.status == "accepted_against_registry" and self.product is not None


def _lineages(gates: tuple[registry.Gate, ...]) -> str:
    rows = []
    for gate in gates:
        body = asdict(gate)
        del body["binding"]["subject"]
        rows.append(body)
    return digest(rows)


def qualification_requests(registered: RegisteredAcceptance, design: compiler.CompilationResult,
                           declaration: compiler.Declaration) -> tuple[QualificationRequest, ...]:
    """Bind already registered specs; this does not register/approve a mapping."""
    require(design.registry is not None, "Incomplete compiler design")
    assert design.registry is not None
    specs = {row.gate_id: row for row in registered.qualification_specs}
    gates = design.prerequisites.execution_gates
    require(set(specs) == {gate.id for gate in gates}, "Qualification gate census differs")
    suites = {suite.id: suite for suite in declaration.suites}
    requests = []
    for gate in gates:
        spec = specs[gate.id]
        expected_facets = tuple(facet for key in gate.logical_gate_ids for facet in FACETS[key])
        require(tuple(row[0] for row in spec.facet_selectors) == expected_facets,
                "Qualification facets do not cover distinct gate duties")
        require((spec.control_recipe is not None) == ("M1-GATE-CONTROL" in gate.logical_gate_ids),
                "Control lineage missing or attached to unrelated qualification")
        require(spec.qualification_source_sha256 != design.registry.subject.source_sha256,
                "Qualification harness cannot be relabeled candidate source")
        requests.append(QualificationRequest(registered, gate, suites[gate.suite_id], spec,
                                             _lineages(design.registry.gates)))
    return tuple(requests)


def _qualification_issues(request: QualificationRequest, evidence: QualificationEvidence) -> list[Issue]:
    require(type(evidence) is QualificationEvidence, "Unchecked qualification record")
    require(evidence.request_sha256 == request.sha256
            and evidence.qualification_source_sha256 == request.specification.qualification_source_sha256,
            "Qualification source or full lineage differs")
    require(request.suite.execution_purpose in ("harness_qualification", "registry_qualification"),
            "Unsupported qualification purpose; product-independent purposes need a separately frozen adapter")
    require(evidence.original_purpose == request.suite.execution_purpose,
            "Qualification original purpose was relabeled")
    expected_cases = request.suite.ordered_case_ids
    expected_facets = tuple(row[0] for row in request.specification.facet_selectors)
    issues = []
    for label, rows, expected in (("cases", evidence.outcomes, expected_cases),
                                  ("facets", evidence.facets, expected_facets)):
        actual = tuple(row.case_id for row in rows)
        require(len(actual) == len(set(actual)) and set(actual) <= set(expected)
                and actual == tuple(key for key in expected if key in actual), "Qualification order/census differs")
        if actual != expected:
            issues.append(Issue("prerequisite", request.gate.id, "missing", "Incomplete qualification " + label))
        for row in rows:
            if row.status != "passed":
                issues.append(Issue("prerequisite", request.gate.id + ":" + row.case_id,
                                    row.status, "Qualification " + label + " did not pass"))
    if evidence.terminal_status == "infrastructure_error":
        issues.append(Issue("prerequisite", request.gate.id, "infrastructure_error", "Qualification infrastructure failed"))
    if evidence.mode == "reused":
        # Frozen SCOPE/CONTROL/ADMISSION allow exact reuse; no purpose relabeling.
        require(evidence.original_purpose not in ("independent_acceptance", "repeatability"),
                "Independent qualification requires fresh physical observation")
        require(not issues, "Incomplete/failed/infrastructure qualification cannot be reused as qualified admission")
    return issues


def _complete_freeze(freeze: registry.CohortFreeze, product: registry.Registry) -> None:
    require(type(freeze) is registry.CohortFreeze, "Wrong freeze authority response")
    subject = product.subject
    require(freeze.no_further_model_actions and {s.trajectory_id for s in freeze.subjects}
            == set(product.cohort_trajectory_ids), "Incomplete irreversible cohort barrier")
    require(all(s.cohort_id == subject.cohort_id and s.execution_contract_sha256 == subject.execution_contract_sha256
                and s.requirements_sha256 == subject.requirements_sha256 and s.milestone == subject.milestone
                for s in freeze.subjects), "Frozen cohort contract/product/milestone differs")
    require([s for s in freeze.subjects if s.trajectory_id == subject.trajectory_id] == [subject],
            "Frozen final source differs")


class CandidateScopeConsumer:
    def __init__(self, authority: EvidenceAuthority):
        require(isinstance(authority, EvidenceAuthority), "Host-installed authority capability required")
        self.authority = authority

    def assess(self, inventory: compiler.Inventory, declaration: compiler.Declaration,
               scope: compiler.ScopePlan, subject: registry.Subject) -> ConsumerResult:
        """No receipts, flags, expected hashes or authority callbacks are accepted here."""
        issues: list[Issue] = []
        qualifications: list[QualificationEvidence] = []
        assessment = None
        snapshot = None
        checkpoint = None
        phase, target = "registration", "snapshot"
        try:
            require(type(inventory) is compiler.Inventory and type(declaration) is compiler.Declaration
                    and type(scope) is compiler.ScopePlan and type(subject) is registry.Subject,
                    "Typed complete design and subject required")
            snapshot = self.authority.open_snapshot()
            require(isinstance(snapshot, AuthoritySnapshot), "Wrong host snapshot capability")
            checkpoint = snapshot.checkpoint
            registry.sha256(checkpoint)
            snapshot.check_current(checkpoint)
            registered = snapshot.registration(subject)
            if registered is None:
                issues.append(Issue(phase, "registration", "missing", "No independently retained prospective registration"))
            else:
                require(type(registered) is RegisteredAcceptance, "Invalid independent registration")
                require((inventory.sha256, scope.sha256, compiler.declaration_fingerprint(declaration),
                         subject.execution_contract_sha256) ==
                        (registered.inventory_sha256, registered.scope_sha256, registered.declaration_sha256,
                         registered.execution_contract_sha256), "Submitted design differs from independent registration")
                design = compiler.compile_design(inventory, declaration, subject, scope_plan=scope,
                                                   expected_scope_sha256=registered.scope_sha256)
                if design.registry is None:
                    issues.extend(Issue("design", row.target, "missing", row.code) for row in design.blockers)
                else:
                    product = design.registry
                    require(registry.design_fingerprint(product) == registered.registry_design_sha256,
                            "Compiled registry differs from independently registered design")
                    requests = qualification_requests(registered, design, declaration)
                    phase = "prerequisite"
                    origins: dict[str, str] = {}
                    receipts: dict[str, str] = {}
                    for request in requests:
                        target = request.gate.id
                        try:
                            evidence = snapshot.qualification(request)
                            if evidence is None:
                                issues.append(Issue(phase, target, "missing", "Qualification has no authenticated original"))
                                continue
                            require(type(evidence) is QualificationEvidence, "Unchecked qualification record")
                            prior = origins.setdefault(evidence.execution_id, target)
                            receipt_prior = receipts.setdefault(evidence.receipt_sha256, target)
                            require(prior == target and receipt_prior == target,
                                    "One original substitutes for separate qualification slots")
                            qualifications.append(evidence)
                            issues.extend(_qualification_issues(request, evidence))
                        except AuthorityUnavailable as error:
                            issues.append(Issue(phase, target, "infrastructure_error", str(error)))
                        except (AuthorityError, registry.AcceptanceError) as error:
                            issues.append(Issue(phase, target, "invalid", str(error)))
                    snapshot.check_current(checkpoint)
                    if not issues:
                        phase, target = "product", "freeze"
                        freeze = snapshot.freeze()
                        if freeze is not None:
                            _complete_freeze(freeze, product)
                        observations: list[registry.Observation] = []
                        for gate in product.gates:
                            target = gate.gate_id
                            if gate.binding.purpose != "public_release" and freeze is None:
                                continue
                            try:
                                observation = snapshot.observation(gate, freeze)
                                if observation is not None:
                                    require(type(observation) is registry.Observation
                                            and observation.gate_id == gate.gate_id and observation.binding == gate.binding,
                                            "Authority observation differs from exact product gate")
                                    # Validate each returned record before retaining it so one
                                    # unavailable/substituted sibling cannot erase known failures.
                                    registry.assess(product, (*observations, observation), registry.Promotion(subject, "missing"), freeze,
                                        expected_design_sha256=registered.registry_design_sha256,
                                        expected_execution_contract_sha256=registered.execution_contract_sha256)
                                    observations.append(observation)
                            except AuthorityUnavailable as error:
                                issues.append(Issue(phase, target, "infrastructure_error", str(error)))
                            except (AuthorityError, registry.AcceptanceError) as error:
                                issues.append(Issue(phase, target, "invalid", str(error)))
                        target = "promotion"
                        promotion = registry.Promotion(subject, "missing")
                        try:
                            supplied = snapshot.promotion(subject)
                            if supplied is not None:
                                require(type(supplied) is registry.Promotion and supplied.subject == subject,
                                        "Promotion belongs to a different final subject")
                                promotion = supplied
                        except AuthorityUnavailable as error:
                            issues.append(Issue(phase, target, "infrastructure_error", str(error)))
                        except (AuthorityError, registry.AcceptanceError) as error:
                            issues.append(Issue(phase, target, "invalid", str(error)))
                        assessment = registry.assess(product, tuple(observations), promotion, freeze,
                            expected_design_sha256=registered.registry_design_sha256,
                            expected_execution_contract_sha256=registered.execution_contract_sha256)
        except AuthorityUnavailable as error:
            issues.append(Issue(phase, target, "infrastructure_error", str(error)))
        except (AuthorityError, compiler.CompilerError, registry.AcceptanceError) as error:
            issues.append(Issue(phase, target, "invalid", str(error)))
        finally:
            if snapshot is not None and checkpoint is not None:
                try:
                    snapshot.check_current(checkpoint)
                except (AuthorityError, registry.AcceptanceError, AuthorityUnavailable) as error:
                    issues.append(Issue("authority", "checkpoint", "invalid", str(error)))
        if issues:
            status = "invalid_evidence" if any(row.status == "invalid" for row in issues) else (
                "prerequisites_failed" if any(row.status == "failed" for row in issues) else "incomplete")
            if assessment is not None:
                # Retain known authenticated defects despite a separate missing or
                # invalid item. No nested result may accidentally claim acceptance.
                assessment = replace(assessment, status="rejected" if assessment.failed_gates
                    or assessment.promotion_status == "rejected" else "incomplete")
        else:
            require(assessment is not None, "No assessment or blocker")
            assert assessment is not None
            status = assessment.status
        return ConsumerResult(status, tuple(issues), tuple(qualifications), assessment, checkpoint)


class ReleaseObservationSource:
    """Concrete bridge from a controller-owned v1 release execution journal.

    The expected checkpoint must be separately retained by the controller after
    verifier publication, never computed from candidate-supplied local files.
    This bridge only authenticates this executor's narrow release observation;
    it supplies no scope, admission, control, review, promotion or freeze proof.
    Reading a completed original does not rerun it. Its original purpose remains.
    """
    def __init__(self, execution: CandidateReleaseExecution, expected_checkpoint: ControllerCheckpoint):
        require(type(execution) is CandidateReleaseExecution and type(expected_checkpoint) is ControllerCheckpoint,
                "Actual release journal owner and external checkpoint required")
        self.execution = execution
        self.expected_checkpoint = expected_checkpoint

    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
        from .candidate_release_execution_v1 import ExecutionError
        try:
            require(self.execution.registration.gate == gate, "Release bridge gate changed")
            require(self.execution.checkpoint() == self.expected_checkpoint, "Release journal changed since retained checkpoint")
            if gate.binding.purpose != "public_release":
                require(freeze is not None and self.execution._freeze() == freeze, "Release bridge freeze differs")
            original = self.execution.verified_execution()
            require(self.execution.checkpoint() == self.expected_checkpoint, "Uncheckpointed verifier suffix")
            return registry.Observation(gate.gate_id, gate.binding, original)
        except ExecutionError as error:
            raise AuthorityError(str(error)) from error


class V2ReleaseObservationSource:
    """Exact typed v2 counterpart; v1 and v2 execution contracts stay distinct."""
    def __init__(self, execution: release_v2.CandidateReleaseExecution,
                 expected_checkpoint: release_v2.ControllerCheckpoint):
        require(type(execution) is release_v2.CandidateReleaseExecution
                and type(expected_checkpoint) is release_v2.ControllerCheckpoint,
                "Actual v2 release journal owner and external checkpoint required")
        self.execution = execution
        self.expected_checkpoint = expected_checkpoint

    def observation(self, gate: registry.Gate, freeze: registry.CohortFreeze | None) -> registry.Observation:
        from .candidate_release_execution_v2 import ExecutionError
        try:
            require(self.execution.registration.gate == gate, "V2 release bridge gate changed")
            require(self.execution.checkpoint() == self.expected_checkpoint, "V2 release journal changed since retained checkpoint")
            if gate.binding.purpose != "public_release":
                require(freeze is not None and self.execution._freeze() == freeze, "V2 release bridge freeze differs")
            original = self.execution.verified_execution()
            require(self.execution.checkpoint() == self.expected_checkpoint, "Uncheckpointed v2 verifier suffix")
            return registry.Observation(gate.gate_id, gate.binding, original)
        except ExecutionError as error:
            raise AuthorityError(str(error)) from error
