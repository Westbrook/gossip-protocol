"""Prospective cumulative coverage compiler; never an execution authenticator.

Pinned source documents supply the denominator. Explicit reviewed declarations
supply assertion semantics; this module does not invent tests from prose.
Compilation produces a design, not acceptance, promotion or cohort-freeze proof.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
from pathlib import Path
from typing import Any

from . import project_acceptance_registry_v1 as registry

PROTOCOL = "project-acceptance-compiler-v1"
M1_SHA256 = "f5d808866d6f18f3fd095de5d447bb5b0e2a43e20771761fe91a38cf4f59ad4a"
PRODUCT_V1_SHA256 = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
PRODUCT_V2_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
M1_FILE = "library-m1-acceptance-inventory-v1.json"
V1_FILE = "library-cumulative-product-v1.json"
V2_FILE = "library-cumulative-product-v2.json"
SHARED_SECTIONS = ("constants", "value_types", "interfaces", "persistence",
                   "compatibility", "release_ownership")
ADDED_SECTIONS = ("amendments", "counter_domains", "worker_state_table", "portable_m1_hash_vectors")
METADATA_PATHS = ("/schema_version", "/contract_id", "/freeze_policy/state",
    "/freeze_policy/qualification_missing", "/registry_rules/amendment_record_keys",
    "/registry_rules/worker_state_record_keys", "/registry_rules/hash_vector_record_keys",
    "/registry_rules/document_precedence")
PRODUCT_KINDS = ("positive", "boundary", "negative", "history")
KINDS = (*PRODUCT_KINDS, "inspection")
INDEPENDENT_LANES = ("independent-integration", "migration", "process-restart", "release-install")
MAX_DECLARATIONS = 16384
ARMS = ("S4-G", "S16-G", "O16-G")
BLOCKS = ("healthy", "compound_recovery")
FAULTS = ("known_result_before_publication_builder_restart", "bounded_peer_message_partition")
M1_LANES = {
    "PUBLIC": "public-contract", "V0-SUPPLEMENT": "public-contract", "INTAKE": "intake",
    "JOBS": "jobs", "ATOMIC": "durability", "CLI": "cli", "HTTP": "http",
    "BROWSER": "browser", "ARCHITECTURE": "source-inspection", "ADAPTER": "workflow",
    "INTEGRATION": "independent-integration", "CONTROL": "harness-control",
    "SCOPE": "registry-scope", "ADMISSION": "harness-admission",
}


class CompilerError(ValueError):
    """Malformed/unrecognized inventory or declaration, not a product failure."""


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise CompilerError(message)


def _bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                      separators=(",", ":")).encode("utf-8")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _digest(value: Any) -> str:
    return _sha(_bytes(value))


def _json(raw: bytes) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in pairs:
            _require(key not in out, "Duplicate JSON key: " + key)
            out[key] = value
        return out
    def constant(value: str) -> Any:
        raise CompilerError("Nonfinite JSON: " + value)
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique, parse_constant=constant)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise CompilerError("Invalid UTF-8 JSON") from error
    _require(type(value) is dict, "Expected object")
    return value


def _id(value: str) -> None:
    try:
        registry.identifier(value)
    except registry.AcceptanceError as error:
        raise CompilerError(str(error)) from error


def _hash(value: str) -> None:
    try:
        registry.sha256(value)
    except registry.AcceptanceError as error:
        raise CompilerError(str(error)) from error


def _unique(values: tuple[str, ...], label: str, *, empty: bool = False) -> None:
    _require(type(values) is tuple and (empty or bool(values)), "Empty/non-tuple " + label)
    _require(len(values) <= registry.MAX_ITEMS, "Oversized " + label)
    for value in values:
        _id(value)
    _require(len(values) == len(set(values)), "Duplicate " + label)


def _escape(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _changes(before: Any, after: Any, pointer: str = "") -> set[str]:
    """Canonical changed/addition nodes; deletion cannot silently shrink norms."""
    if pointer in METADATA_PATHS:
        return set() if type(before) is type(after) and before == after else {pointer}
    if type(before) is dict and type(after) is dict:
        _require(set(before) <= set(after), "Removed source keys at " + pointer)
        found: set[str] = set()
        for key, value in after.items():
            path = pointer + "/" + _escape(key)
            found |= {path} if key not in before else _changes(before[key], value, path)
        return found
    if type(before) is list and type(after) is list:
        _require(len(after) >= len(before), "Removed source items at " + pointer)
        found = set()
        for index, value in enumerate(after):
            path = pointer + "/" + str(index)
            found |= {path} if index >= len(before) else _changes(before[index], value, path)
        return found
    return set() if type(before) is type(after) and before == after else {pointer}


@dataclass(frozen=True, slots=True)
class Requirement:
    id: str
    milestone: str
    role: str
    logical_gate_ids: tuple[str, ...]
    prerequisite_gate_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class LogicalGate:
    id: str
    role: str
    lane: str
    requirement_ids: tuple[str, ...]
    original_purpose: str | None


@dataclass(frozen=True, slots=True)
class Obligation:
    id: str
    source_pointer: str
    source_sha256: str
    value_sha256: str
    requirement_ids: tuple[str, ...]
    role: str
    kind: str


@dataclass(frozen=True, slots=True)
class QualificationRule:
    id: str
    source_pointer: str
    source_sha256: str
    value_sha256: str


@dataclass(frozen=True, slots=True)
class Inventory:
    product_sha256: str
    product_version: int
    requirements: tuple[Requirement, ...]
    logical_gates: tuple[LogicalGate, ...]
    obligations: tuple[Obligation, ...]
    compatibility_authorities: tuple[str, ...]
    planning_notes: tuple[tuple[str, str, str, str], ...]
    qualification_rules: tuple[QualificationRule, ...]
    source_payloads: tuple[bytes, bytes, bytes] = field(repr=False)

    @property
    def product_ids(self) -> tuple[str, ...]:
        return tuple(row.id for row in self.requirements if row.role == "product")

    @property
    def prerequisite_ids(self) -> tuple[str, ...]:
        return tuple(row.id for row in self.requirements if row.role == "prerequisite")

    @property
    def sha256(self) -> str:
        return _digest({"protocol": PROTOCOL, "m1": M1_SHA256, "product": self.product_sha256,
            "requirements": [asdict(row) for row in self.requirements],
            "logical_gates": [asdict(row) for row in self.logical_gates],
            "obligations": [asdict(row) for row in self.obligations],
            "compatibility_authorities": self.compatibility_authorities,
            "planning_notes": self.planning_notes,
            "qualification_rules": [asdict(row) for row in self.qualification_rules]})


def _inventory(m1_raw: bytes, v1_raw: bytes, product_raw: bytes,
               expected_product_sha256: str) -> Inventory:
    _require(_sha(m1_raw) == M1_SHA256, "Unrecognized M1 inventory bytes")
    _require(_sha(v1_raw) == PRODUCT_V1_SHA256, "Unrecognized frozen v1 product bytes")
    _hash(expected_product_sha256)
    _require(_sha(product_raw) == expected_product_sha256, "Product pin differs")
    _require(expected_product_sha256 in (PRODUCT_V1_SHA256, PRODUCT_V2_SHA256),
             "Unrecognized product revision; version the loader before activation")
    m1, base, product = _json(m1_raw), _json(v1_raw), _json(product_raw)
    version = product["schema_version"]
    _require(type(version) is int and version in (1, 2), "Unknown product version")
    if version == 1:
        _require(expected_product_sha256 == PRODUCT_V1_SHA256, "Wrong v1 identity")
    else:
        _require(product["contract_id"] == "library-cumulative-product-v2", "Wrong v2 identity")
        revision = product["revision"]
        _require(revision["superseded_sha256"] == PRODUCT_V1_SHA256, "Wrong v2 ancestor")
        _require(set(revision["added_normative_sections"]) == set(ADDED_SECTIONS), "Unknown v2 norms")
        _require(set(revision["metadata_change_paths"]) == set(METADATA_PATHS), "Metadata escape")
        amendments = {row["id"] for row in product["amendments"]}
        _require(len(amendments) == 6, "Incomplete amendments")
        changes = _changes(base, {key: value for key, value in product.items()
                                 if key not in (*ADDED_SECTIONS, "revision")})
        semantic = {path for path in changes if not any(path == meta or path.startswith(meta+"/")
                                                        for meta in METADATA_PATHS)}
        _require(semantic == set(revision["semantic_change_map"]), "Incomplete semantic change map")
        for owners in revision["semantic_change_map"].values():
            _require(type(owners) is list and bool(owners) and len(owners) == len(set(owners))
                     and set(owners) <= amendments, "Invalid amendment authority")
        for old, new in zip(base["requirements"], product["requirements"]):
            for key in ("id", "milestone", "mandatory", "packages", "depends_on", "evidence_lanes"):
                _require(old[key] == new[key], "Changed requirement structure: " + key)
        _require(product["dependency_graph"] == base["dependency_graph"], "Changed dependency graph")
    later = product["requirements"]
    _require(len(later) == 17 and [r["id"] for r in later] ==
             base["acceptance_boundary"]["mandatory_requirement_ids"], "Incomplete later inventory")
    _require(product["acceptance_boundary"]["mandatory_requirement_ids"] ==
             base["acceptance_boundary"]["mandatory_requirement_ids"], "Changed mandatory census")
    boundary = m1["consumer_boundary"]
    product_ids = tuple(boundary["product_requirement_ids"])
    prerequisite_ids = tuple(boundary["prerequisite_requirement_ids"])
    _require(len(product_ids) == 106 and len(prerequisite_ids) == 3, "Incomplete M1 census")
    requirements: list[Requirement] = []
    gates: list[LogicalGate] = []
    obligations: list[Obligation] = []
    authorities: list[str] = []
    planning_notes = tuple((row["id"], gap["id"], gap["type"], gap["scope"])
        for row in m1["requirements"] for gap in row["coverage_gaps"])

    def add(identifier: str, pointer: str, value: Any, owners: tuple[str, ...] = (),
            role: str = "product", kind: str = "shared", source: str = expected_product_sha256) -> None:
        obligations.append(Obligation(identifier, pointer, source, _digest(value), owners, role, kind))

    for row in m1["requirements"]:
        role = "product" if row["id"] in product_ids else "prerequisite"
        requirements.append(Requirement(row["id"], "M1", role, tuple(row["required_gate_ids"]),
                                        tuple(row["required_prerequisite_gate_ids"])))
        index = len(requirements)-1
        add("m1:"+row["id"], "/requirements/"+str(index)+"/obligation", row["obligation"],
            (row["id"],), role, "requirement", M1_SHA256)
    for row in m1["gate_definitions"]:
        gates.append(LogicalGate(row["id"],
            "product" if row["consumer_role"] == "product_registry" else "prerequisite",
            M1_LANES[row["id"].removeprefix("M1-GATE-")], tuple(row["requirement_ids"]), row["purpose"]))
    admission = next(row for row in m1["gate_definitions"] if row["id"] == "M1-GATE-ADMISSION")
    admission_index = m1["gate_definitions"].index(admission)
    add("prerequisite:M1-GATE-ADMISSION", f"/gate_definitions/{admission_index}/scope",
        admission["scope"], tuple(admission["requirement_ids"]), "prerequisite",
        "admission", M1_SHA256)
    for index, clarification in enumerate(m1["contract_clarifications"]):
        owners = tuple(clarification["requirements"])
        role = "prerequisite" if set(owners) <= set(prerequisite_ids) else "product"
        add(clarification["id"], f"/contract_clarifications/{index}/resolution",
            clarification["resolution"], owners, role, "clarification", M1_SHA256)
    for index, row in enumerate(later):
        _require(row["mandatory"] is True, "Optionalized product requirement")
        logical = tuple(row["id"]+"."+lane for lane in row["evidence_lanes"])
        requirements.append(Requirement(row["id"], row["milestone"], "product", logical))
        for name, lane in zip(logical, row["evidence_lanes"]):
            gates.append(LogicalGate(name, "product", lane, (row["id"],), None))
        _require(bool(row["clauses"]), "Empty requirement clauses")
        for number, clause in enumerate(row["clauses"]):
            add(row["id"]+":clause:"+str(number), f"/requirements/{index}/clauses/{number}",
                clause, (row["id"],), kind="requirement")
    for section in SHARED_SECTIONS:
        value = product[section]
        entries = enumerate(value) if type(value) is list else value.items()
        for entry_key, item in entries:
            pointer = "/"+section+"/"+_escape(str(entry_key))
            identifier = "shared:"+section+":"+str(entry_key)
            add(identifier, pointer, item)
            if section == "compatibility":
                authorities.append(identifier)
    if version == 2:
        for index, amendment in enumerate(product["amendments"]):
            owners = tuple(amendment["requirement_ids"])
            _require(set(owners) <= {row["id"] for row in later}, "Unknown amendment requirement")
            name = amendment["id"]
            add(name+":decision", f"/amendments/{index}/decision", amendment["decision"], owners,
                kind="amendment")
            authorities.append(name+":decision")
            for field_name in ("clauses", "required_observations"):
                for number, value in enumerate(amendment[field_name]):
                    add(name+":"+field_name+":"+str(number),
                        f"/amendments/{index}/{field_name}/{number}", value, owners, kind="amendment")
        for section in ADDED_SECTIONS[1:]:
            add("shared:"+section, "/"+section, product[section])
    out = Inventory(expected_product_sha256, version, tuple(requirements), tuple(gates),
                    tuple(obligations), tuple(authorities), planning_notes,
                    tuple(QualificationRule("acceptance-boundary:"+key,
                        "/acceptance_boundary/"+_escape(key), expected_product_sha256, _digest(value))
                        for key, value in product["acceptance_boundary"].items()) +
                    tuple(QualificationRule("m1-gate-definition:"+row["id"],
                        "/gate_definitions/"+str(index), M1_SHA256, _digest(row))
                        for index, row in enumerate(m1["gate_definitions"])),
                    (m1_raw, v1_raw, product_raw))
    _require(len(out.product_ids) == 123 and len(set(out.product_ids)) == 123, "Wrong product census")
    _require(len({o.id for o in obligations}) == len(obligations), "Duplicate obligation")
    return out


def load_inventory(root: Path, *, product_file: str = V1_FILE,
                   expected_product_sha256: str = PRODUCT_V1_SHA256) -> Inventory:
    """Read recognized pinned sources; expected pins must come from host registration."""
    _require(product_file in (V1_FILE, V2_FILE), "Unrecognized inventory filename")
    root = Path(root)
    return _inventory((root/M1_FILE).read_bytes(), (root/V1_FILE).read_bytes(),
                      (root/product_file).read_bytes(), expected_product_sha256)


@dataclass(frozen=True, slots=True)
class Assertion:
    id: str
    kind: str
    logical_gate_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ObligationPlan:
    obligation_id: str
    requirement_ids: tuple[str, ...]
    assertions: tuple[Assertion, ...]
    review_sha256: str
    ownership_reason: str = ""


@dataclass(frozen=True, slots=True)
class SuiteDefinition:
    id: str
    ordered_case_ids: tuple[str, ...]
    ordered_suite_sha256: str
    definition_sha256: str
    definition_purpose: str
    execution_purpose: str
    source_contract_sha256: str
    evaluator_sha256: str
    candidate_portable: bool
    candidate_suitable: bool
    capabilities: tuple[str, ...]
    compatibility_review_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class QualificationBinding:
    """Admission's declared target lineage, not the admission process's own subject."""
    product_gate_id: str
    ordered_suite_sha256: str
    evaluator_sha256: str
    runtime_image_sha256: str
    environment_sha256: str
    limits_sha256: str
    execution_protocol: str


@dataclass(frozen=True, slots=True)
class ExecutionGate:
    id: str
    physical_slot: str
    suite_id: str
    role: str
    logical_gate_ids: tuple[str, ...]
    runtime_image_sha256: str
    environment_sha256: str
    limits_sha256: str
    seed_sha256: str
    execution_protocol: str
    qualification_targets: tuple[QualificationBinding, ...] = ()


@dataclass(frozen=True, slots=True)
class CoverageEdge:
    obligation_id: str
    assertion_id: str
    gate_id: str
    case_id: str
    assertion_selector: str


@dataclass(frozen=True, slots=True)
class PurposeAssignment:
    logical_gate_id: str
    original_purpose: str
    execution_purpose: str
    prospective_authority_sha256: str


@dataclass(frozen=True, slots=True)
class CompatibilityOverride:
    authority_obligation_id: str
    mode: str
    milestone: str
    inherited_obligation_ids: tuple[str, ...]
    successor_obligation_ids: tuple[str, ...]
    reason: str
    review_sha256: str


@dataclass(frozen=True, slots=True)
class Trajectory:
    id: str
    arm: str
    block: str
    role_ids: tuple[str, ...]
    decision_placement: str
    block_seed_sha256: str
    fault_schedule_sha256: str
    faults: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CohortDesign:
    cohort_id: str
    trajectories: tuple[Trajectory, ...]
    milestones: tuple[str, ...]
    transport_sha256: str
    shared_policy_sha256: str
    requirements_release_sha256: str
    resource_contract_sha256: str
    model_profiles_sha256: str
    generated_test_contract_sha256: str
    barrier_protocol_sha256: str
    held_out_plan_sha256: str


@dataclass(frozen=True, slots=True)
class Declaration:
    inventory_sha256: str
    obligations: tuple[ObligationPlan, ...]
    suites: tuple[SuiteDefinition, ...]
    gates: tuple[ExecutionGate, ...]
    edges: tuple[CoverageEdge, ...]
    purposes: tuple[PurposeAssignment, ...]
    compatibility: tuple[CompatibilityOverride, ...]
    cohort: CohortDesign


@dataclass(frozen=True, slots=True)
class ApplicabilityCell:
    kind: str
    logical_gate_id: str


@dataclass(frozen=True, slots=True)
class NotApplicable:
    cell: ApplicabilityCell
    reason: str


@dataclass(frozen=True, slots=True)
class ObligationApplicability:
    obligation_id: str
    requirement_ids: tuple[str, ...]
    required_cells: tuple[ApplicabilityCell, ...]
    not_applicable: tuple[NotApplicable, ...]
    reason: str


@dataclass(frozen=True, slots=True)
class PlanningDisposition:
    requirement_id: str
    gap_id: str
    obligation_ids: tuple[str, ...]
    reason: str


@dataclass(frozen=True, slots=True)
class SuiteCompatibility:
    suite_id: str
    definition_sha256: str
    source_contract_sha256: str
    target_contract_sha256: str
    authority_obligation_ids: tuple[str, ...]
    reason: str


@dataclass(frozen=True, slots=True)
class ScopePlan:
    """Externally registered reviewed decomposition; a hash is not review authentication.

    No real plan is generated by this module. The host must obtain semantic scope
    qualification for this exact identity before any candidate acceptance claim.
    """
    inventory_sha256: str
    reviewed_declaration_sha256: str
    applicability: tuple[ObligationApplicability, ...]
    planning_dispositions: tuple[PlanningDisposition, ...]
    qualification_rule_ids: tuple[str, ...]
    purposes: tuple[PurposeAssignment, ...]
    compatibility: tuple[CompatibilityOverride, ...]
    suite_compatibility: tuple[SuiteCompatibility, ...]

    @property
    def sha256(self) -> str:
        return _digest(asdict(self))


def declaration_fingerprint(declaration: Declaration) -> str:
    return _digest(asdict(declaration))


@dataclass(frozen=True, slots=True)
class CoverageBlocker:
    code: str
    target: str
    detail: str


@dataclass(frozen=True, slots=True)
class PrerequisitePlan:
    requirement_ids: tuple[str, ...]
    logical_gate_ids: tuple[str, ...]
    execution_gates: tuple[ExecutionGate, ...]
    product_dependencies: tuple[tuple[str, tuple[str, ...]], ...]
    scope_plan_sha256: str | None
    qualification_rules: tuple[QualificationRule, ...]


@dataclass(frozen=True, slots=True)
class CompilationResult:
    registry: registry.Registry | None
    blockers: tuple[CoverageBlocker, ...]
    prerequisites: PrerequisitePlan
    declaration_sha256: str
    compiled_inventory_sha256: str
    compatibility: tuple[CompatibilityOverride, ...]

    @property
    def declaration_complete(self) -> bool:
        return self.registry is not None and not self.blockers


def compile_design(inventory: Inventory, declaration: Declaration,
                   subject: registry.Subject, *, scope_plan: ScopePlan | None = None,
                   expected_scope_sha256: str | None = None) -> CompilationResult:
    """Compile only complete final-M4 designs; no evidence or results are consumed.

    The declaration and review identities are host-owned prospective inputs.
    Their hash is included in the resulting Registry inventory identity. Identity
    is not authentication of review, execution, promotion or the stop barrier.
    """
    _require(type(inventory) is Inventory and type(declaration) is Declaration, "Wrong compiler input")
    authoritative = _inventory(*inventory.source_payloads, inventory.product_sha256)
    _require(authoritative == inventory, "Invented/shrunken derived inventory")
    _require(declaration.inventory_sha256 == inventory.sha256, "Declaration inventory pin differs")
    _require(type(subject) is registry.Subject, "Wrong product subject")
    _require(subject.requirements_sha256 == inventory.product_sha256, "Subject product contract differs")
    _require(subject.milestone == "M4", "This compiler declares final cumulative M4 only")
    blocks: list[CoverageBlocker] = []

    def block(code: str, target: str, detail: str) -> None:
        blocks.append(CoverageBlocker(code, target, detail))

    types = {"obligation plan": ObligationPlan, "suite": SuiteDefinition,
        "execution gate": ExecutionGate, "purpose assignment": PurposeAssignment,
        "compatibility authority": CompatibilityOverride, "trajectory": Trajectory,
        "assertion": Assertion, "applicability": ObligationApplicability,
        "suite compatibility": SuiteCompatibility, "qualification target": QualificationBinding}

    def keyed(items: tuple[Any, ...], field_name: str, label: str) -> dict[str, Any]:
        _require(type(items) is tuple and len(items) <= MAX_DECLARATIONS,
                 "Expected bounded immutable "+label)
        out: dict[str, Any] = {}
        for item in items:
            _require(type(item) is types[label], "Wrong record type: "+label)
            key = getattr(item, field_name)
            _id(key)
            _require(key not in out, "Duplicate "+label+": "+key)
            out[key] = item
        return out

    requirements = {row.id: row for row in inventory.requirements}
    obligations = {row.id: row for row in inventory.obligations}
    logical = {row.id: row for row in inventory.logical_gates}
    plans = keyed(declaration.obligations, "obligation_id", "obligation plan")
    suites = keyed(declaration.suites, "id", "suite")
    gates = keyed(declaration.gates, "id", "execution gate")
    purposes = keyed(declaration.purposes, "logical_gate_id", "purpose assignment")
    overrides = keyed(declaration.compatibility, "authority_obligation_id", "compatibility authority")
    _require(set(plans) <= set(obligations), "Undeclared obligation")
    _require(set(purposes) <= set(logical), "Undeclared purpose gate")
    _require(set(overrides) <= set(inventory.compatibility_authorities), "Undeclared compatibility authority")
    for gate in gates.values():
        _id(gate.physical_slot)
    _require(len({g.physical_slot for g in gates.values()}) == len(gates), "Physical slot split across gates")
    if len(gates) > registry.MAX_ITEMS:
        block("registry_size_limit", "execution-gates", "Do not split physical executions to evade limits")
    declaration_sha = declaration_fingerprint(declaration)
    applicability: dict[str, Any] = {}
    suite_compatibility: dict[str, Any] = {}
    if scope_plan is None:
        block("missing_scope_plan", "scope", "No externally reviewed applicability/compatibility decomposition")
    else:
        _require(type(scope_plan) is ScopePlan, "Wrong scope plan")
        _require(scope_plan.inventory_sha256 == inventory.sha256, "Scope inventory differs")
        _hash(scope_plan.reviewed_declaration_sha256)
        if expected_scope_sha256 is None:
            block("missing_scope_registration", "scope", "Host must separately register the approved scope digest")
        else:
            _hash(expected_scope_sha256)
            if expected_scope_sha256 != scope_plan.sha256:
                block("scope_registration_mismatch", "scope", "Scope differs from externally registered identity")
        if scope_plan.reviewed_declaration_sha256 != declaration_sha:
            block("unreviewed_declaration", "scope", "Actual plans/suites/selectors differ from the reviewed design")
        applicability = keyed(scope_plan.applicability, "obligation_id", "applicability")
        _require(set(applicability) <= set(obligations), "Undeclared applicability obligation")
        for missing in set(obligations)-set(applicability):
            block("missing_applicability", missing, "Every source obligation needs reviewed kind/lane applicability")
        _unique(scope_plan.qualification_rule_ids, "qualification rules")
        _require(set(scope_plan.qualification_rule_ids) <= {r.id for r in inventory.qualification_rules},
                 "Unknown qualification rule")
        for rule in inventory.qualification_rules:
            if rule.id not in scope_plan.qualification_rule_ids:
                block("missing_qualification_rule", rule.id, "Source acceptance boundary remains mandatory")
        approved_purposes = keyed(scope_plan.purposes, "logical_gate_id", "purpose assignment")
        approved_overrides = keyed(scope_plan.compatibility, "authority_obligation_id", "compatibility authority")
        if approved_purposes != purposes:
            block("unapproved_purpose_mapping", "scope", "Purpose assignments differ from registered scope")
        if approved_overrides != overrides:
            block("unapproved_compatibility_mapping", "scope", "Exact authority/predecessor/successor mapping differs")
        suite_compatibility = keyed(scope_plan.suite_compatibility, "suite_id", "suite compatibility")
        _require(set(suite_compatibility) <= set(suites), "Unknown compatibility suite")
        _require(type(scope_plan.planning_dispositions) is tuple and
                 len(scope_plan.planning_dispositions) <= MAX_DECLARATIONS, "Invalid planning dispositions")
        notes = {(r, g): (kind, text) for r, g, kind, text in inventory.planning_notes}
        seen_notes: set[tuple[str, str]] = set()
        for disposition in scope_plan.planning_dispositions:
            _require(type(disposition) is PlanningDisposition, "Wrong planning disposition")
            note_key = (disposition.requirement_id, disposition.gap_id)
            _require(note_key in notes and note_key not in seen_notes, "Unknown/duplicate planning note")
            seen_notes.add(note_key)
            _unique(disposition.obligation_ids, "gap disposition obligations")
            _require(set(disposition.obligation_ids) <= set(obligations), "Unknown gap disposition obligation")
            _require(type(disposition.reason) is str and bool(disposition.reason.strip()), "Missing gap disposition reason")
            if not any(disposition.requirement_id in obligations[x].requirement_ids
                       for x in disposition.obligation_ids):
                block("gap_owner_mismatch", disposition.gap_id, "Disposition must retain original requirement scope")
            # Gap type/scope are review inputs, not claims that an old gap is still an outcome.
            kind, _text = notes[note_key]
            if kind in KINDS and not any(cell.kind == kind for x in disposition.obligation_ids
                    if x in applicability for cell in applicability[x].required_cells):
                block("gap_kind_undisposed", disposition.gap_id, "Retain the declared positive/boundary/negative/history class")
        for requirement_id, gap_id in set(notes)-seen_notes:
            block("missing_gap_disposition", requirement_id+":"+gap_id,
                  "Review retained gap scope/type as design input; it is not a current product verdict")
    for missing in set(obligations)-set(plans):
        block("missing_obligation", missing, "Every source obligation requires an explicit assertion plan")
    cohort = declaration.cohort
    _require(type(cohort) is CohortDesign, "Wrong cohort design")
    _id(cohort.cohort_id)
    _require(cohort.cohort_id == subject.cohort_id, "Subject cohort differs")
    for key, value in asdict(cohort).items():
        if key.endswith("_sha256"):
            _hash(value)
    roster = keyed(cohort.trajectories, "id", "trajectory")
    combinations = {(t.arm, t.block) for t in roster.values()}
    if len(roster) != 6 or combinations != {(arm, b) for arm in ARMS for b in BLOCKS}:
        block("incomplete_cohort", cohort.cohort_id, "Require all S4-G/S16-G/O16-G healthy/fault trajectories")
    if cohort.milestones != ("M1", "M2", "M3", "M4"):
        block("milestone_contract", cohort.cohort_id, "All four cumulative milestones remain mandatory")
    _require(subject.trajectory_id in roster, "Subject outside declared cohort")
    for trajectory in roster.values():
        _require(trajectory.arm in ARMS and trajectory.block in BLOCKS, "Unknown arm/block")
        builders = ("B01", "B05", "B09", "B13") if trajectory.arm == "S4-G" else tuple(
            "B"+str(i).zfill(2) for i in range(1, 17))
        expected_roles = builders + ("R1", "R2", "R3", "R4")
        if trajectory.role_ids != expected_roles:
            block("role_contract", trajectory.id, "Declared cognitive identities differ from arm")
        placement = "durable_central_scheduler" if trajectory.arm == "O16-G" else "peer_local"
        if trajectory.decision_placement != placement:
            block("decision_placement", trajectory.id, "Preserve placement contrast over common transport")
        _hash(trajectory.block_seed_sha256)
        _hash(trajectory.fault_schedule_sha256)
        if trajectory.faults != (FAULTS if trajectory.block == "compound_recovery" else ()):
            block("fault_contract", trajectory.id, "Actual restart and partition declarations required")
    for name in BLOCKS:
        members = [t for t in roster.values() if t.block == name]
        if len({(t.block_seed_sha256, t.fault_schedule_sha256) for t in members}) > 1:
            block("unmatched_block", name, "Arms must share exogenous seed/fault schedule")

    for suite in suites.values():
        _unique(suite.ordered_case_ids, "ordered cases")
        _unique(suite.capabilities, "suite capabilities")
        for key in ("ordered_suite_sha256", "definition_sha256", "source_contract_sha256", "evaluator_sha256"):
            _hash(getattr(suite, key))
        _id(suite.definition_purpose)
        _id(suite.execution_purpose)
        if suite.compatibility_review_sha256 is not None:
            _hash(suite.compatibility_review_sha256)
        if suite.source_contract_sha256 != inventory.product_sha256:
            transition = suite_compatibility.get(suite.id)
            if transition is None:
                block("suite_contract_mapping", suite.id, "Different source contract needs scope-approved exact suite mapping")
            else:
                _unique(transition.authority_obligation_ids, "suite compatibility authorities")
                _require(set(transition.authority_obligation_ids) <= set(inventory.compatibility_authorities),
                         "Unknown suite compatibility authority")
                _require(type(transition.reason) is str and bool(transition.reason.strip()), "Missing suite mapping reason")
                if (suite.source_contract_sha256 not in (M1_SHA256, PRODUCT_V1_SHA256, PRODUCT_V2_SHA256)
                        or transition.definition_sha256 != suite.definition_sha256
                        or transition.source_contract_sha256 != suite.source_contract_sha256
                        or transition.target_contract_sha256 != inventory.product_sha256):
                    block("suite_contract_mapping", suite.id, "Unknown source or mismatched exact source/target/definition")
        elif suite.id in suite_compatibility:
            block("unnecessary_suite_mapping", suite.id, "Same-contract suite cannot hide a spurious compatibility conversion")
        if type(suite.candidate_portable) is not bool or type(suite.candidate_suitable) is not bool:
            raise CompilerError("Nonboolean suite suitability")
    for gate in gates.values():
        _id(gate.physical_slot)
        _id(gate.execution_protocol)
        _unique(gate.logical_gate_ids, "logical gates")
        _require(gate.role in ("product", "prerequisite") and gate.suite_id in suites, "Invalid execution gate")
        _require(set(gate.logical_gate_ids) <= set(logical), "Undeclared logical gate")
        _require(all(logical[key].role == gate.role for key in gate.logical_gate_ids), "Gate category mismatch")
        for key in ("runtime_image_sha256", "environment_sha256", "limits_sha256", "seed_sha256"):
            _hash(getattr(gate, key))
        suite = suites[gate.suite_id]
        if gate.role == "product":
            if not suite.candidate_portable or not suite.candidate_suitable:
                block("nonportable_suite", gate.id, "Reference-only or unsuitable suite cannot grade candidate")
            if suite.execution_purpose not in registry.PURPOSES:
                block("unsupported_purpose", gate.id, "No supported prospective execution purpose")
            if suite.definition_purpose.startswith("authored_reference"):
                block("reference_only_definition", gate.id, "Extract/version candidate definition; do not relabel reference")
        for key in gate.logical_gate_ids:
            lane = logical[key]
            if (lane.original_purpose == "independent_acceptance" or lane.lane in INDEPENDENT_LANES):
                if suite.execution_purpose != "independent_acceptance":
                    block("independent_lane_purpose", key, "This lane requires a fresh independent-acceptance execution")
            if lane.lane not in suite.capabilities:
                block("lane_capability", key, "Suite does not observe required lane: "+lane.lane)
            if lane.original_purpose is not None and lane.original_purpose != suite.execution_purpose:
                assignment = purposes.get(key)
                if assignment is None:
                    block("missing_purpose_mapping", key, "Freeze prospective purpose assignment without relabeling receipts")
                else:
                    _hash(assignment.prospective_authority_sha256)
                    _require(assignment.original_purpose == lane.original_purpose and
                             assignment.execution_purpose == suite.execution_purpose, "Purpose mapping mismatch")
            elif key in purposes:
                assignment = purposes[key]
                _hash(assignment.prospective_authority_sha256)
                _require(assignment.original_purpose == lane.original_purpose and
                         assignment.execution_purpose == suite.execution_purpose, "Purpose mapping mismatch")

    assertions: dict[tuple[str, str], tuple[Assertion, tuple[str, ...]]] = {}
    covered_pairs: set[tuple[str, str]] = set()
    group_kinds: dict[str, set[str]] = {key: set() for key in requirements}
    for name, plan in plans.items():
        obligation = obligations[name]
        _unique(plan.requirement_ids, "obligation owners")
        _hash(plan.review_sha256)
        _require(set(plan.requirement_ids) <= set(requirements), "Unknown obligation owner")
        if obligation.requirement_ids:
            _require(plan.requirement_ids == obligation.requirement_ids, "Changed source obligation owners")
        else:
            if not plan.ownership_reason.strip():
                block("unassigned_shared_norm", name, "Shared norms need explicit reviewed ownership; none is inferred")
        _require(obligation.kind == "admission" or
                 all(requirements[key].role == obligation.role for key in plan.requirement_ids),
                 "Product/prerequisite ownership mismatch")
        approved = applicability.get(name)
        required_cells: set[tuple[str, str]] = set()
        excluded_cells: set[tuple[str, str]] = set()
        if approved is not None:
            _unique(approved.requirement_ids, "applicability owners")
            if approved.requirement_ids != plan.requirement_ids:
                block("applicability_owner_mismatch", name, "Actual owners differ from registered scope")
            _require(type(approved.reason) is str and bool(approved.reason.strip()), "Missing applicability reason")
            eligible = {key for key, row in logical.items() if row.role == obligation.role
                        and set(approved.requirement_ids) & set(row.requirement_ids)}
            kinds = KINDS if obligation.role == "product" else ("inspection",)
            universe = {(kind, key) for key in eligible for kind in kinds}
            _require(type(approved.required_cells) is tuple and
                     type(approved.not_applicable) is tuple, "Mutable applicability cells")
            _require(len(approved.required_cells)+len(approved.not_applicable) <= MAX_DECLARATIONS,
                     "Oversized applicability cells")
            for cell in approved.required_cells:
                _require(type(cell) is ApplicabilityCell, "Wrong applicability cell")
                pair = (cell.kind, cell.logical_gate_id)
                _require(pair in universe and pair not in required_cells, "Invalid/duplicate required cell")
                required_cells.add(pair)
            for exclusion in approved.not_applicable:
                _require(type(exclusion) is NotApplicable and type(exclusion.cell) is ApplicabilityCell,
                         "Wrong not-applicable cell")
                pair = (exclusion.cell.kind, exclusion.cell.logical_gate_id)
                _require(pair in universe and pair not in excluded_cells and pair not in required_cells,
                         "Invalid/duplicate excluded cell")
                _require(type(exclusion.reason) is str and bool(exclusion.reason.strip()), "Unreviewed N/A reason")
                excluded_cells.add(pair)
            if required_cells | excluded_cells != universe:
                block("incomplete_applicability", name, "Explicit required or reasoned N/A needed for every owner/class/lane cell")
            if not required_cells:
                block("empty_applicability", name, "Each obligation requires a decisive observation")
        items = keyed(plan.assertions, "id", "assertion")
        actual_cells: set[tuple[str, str]] = set()
        if not items:
            block("missing_assertions", name, "Explicit finite assertions required; no prose-to-test guesses")
        owner_seen: set[str] = set()
        for assertion in items.values():
            _require(assertion.kind in KINDS, "Unknown assertion kind")
            _unique(assertion.logical_gate_ids, "assertion logical gates")
            _require(set(assertion.logical_gate_ids) <= set(logical), "Unknown assertion gate")
            for key in assertion.logical_gate_ids:
                actual_cells.add((assertion.kind, key))
                row = logical[key]
                _require(row.role == obligation.role, "Assertion gate role mismatch")
                matching_owners = set(plan.requirement_ids) & set(row.requirement_ids)
                _require(bool(matching_owners), "Assertion gate has no declared requirement owner")
                for owner in matching_owners:
                    covered_pairs.add((owner, key))
                    owner_seen.add(owner)
                    group_kinds[owner].add(assertion.kind)
            assertions[(name, assertion.id)] = (assertion, plan.requirement_ids)
        if approved is not None:
            for kind, key in required_cells-actual_cells:
                block("missing_required_cell", name+":"+kind+":"+key, "Actual assertion plan omits approved applicability")
            if actual_cells & excluded_cells:
                block("excluded_assertion_cell", name, "Actual assertion contradicts approved N/A applicability")
        for missing in set(plan.requirement_ids)-owner_seen:
            block("missing_owner_assertion", name, "No assertion covers owner "+missing)
    for requirement_row in requirements.values():
        if requirement_row.role == "product":
            for kind in set(PRODUCT_KINDS)-group_kinds[requirement_row.id]:
                block("missing_requirement_class", requirement_row.id+":"+kind,
                      "Mandatory product groups need positive/boundary/negative/history representatives")
        for key in requirement_row.logical_gate_ids:
            if (requirement_row.id, key) not in covered_pairs:
                block("missing_required_lane", requirement_row.id+":"+key, "Required logical gate has no assertion")
        for key in requirement_row.prerequisite_gate_ids:
            if key not in {g for gate in gates.values() if gate.role == "prerequisite" for g in gate.logical_gate_ids}:
                block("missing_prerequisite", requirement_row.id+":"+key, "Required admission dependency is not declared")
    for logical_id, row in logical.items():
        if not any(logical_id in gate.logical_gate_ids for gate in gates.values()):
            block("missing_logical_gate", logical_id, "Every declared product/prerequisite logical gate is required")

    observed_assertions: set[tuple[str, str, str]] = set()
    used_gate_logical: set[tuple[str, str]] = set()
    gate_owners: dict[str, set[str]] = {key: set() for key in gates}
    edge_keys: set[tuple[str, str, str, str, str]] = set()
    _require(type(declaration.edges) is tuple and len(declaration.edges) <= MAX_DECLARATIONS,
             "Expected bounded immutable coverage edges")
    for edge in declaration.edges:
        _require(type(edge) is CoverageEdge, "Wrong coverage edge")
        edge_key = (edge.obligation_id, edge.assertion_id)
        _require(edge_key in assertions and edge.gate_id in gates, "Undeclared coverage edge")
        gate = gates[edge.gate_id]
        suite = suites[gate.suite_id]
        _require(edge.case_id in suite.ordered_case_ids, "Edge case outside ordered suite")
        _require(type(edge.assertion_selector) is str and bool(edge.assertion_selector.strip()),
                 "Missing concrete observation selector")
        identity = (*edge_key, edge.gate_id, edge.case_id, edge.assertion_selector)
        _require(identity not in edge_keys, "Duplicate coverage edge")
        edge_keys.add(identity)
        assertion, owners = assertions[edge_key]
        intersection = set(assertion.logical_gate_ids) & set(gate.logical_gate_ids)
        _require(bool(intersection), "Edge physical gate cannot implement assertion logical gate")
        for logical_id in intersection:
            observed_assertions.add((*edge_key, logical_id))
            used_gate_logical.add((edge.gate_id, logical_id))
            gate_owners[edge.gate_id].update(set(owners) & set(logical[logical_id].requirement_ids))
    for edge_key, (assertion, _owners) in assertions.items():
        for logical_id in assertion.logical_gate_ids:
            if (*edge_key, logical_id) not in observed_assertions:
                block("missing_assertion_edge", edge_key[0]+":"+edge_key[1]+":"+logical_id,
                      "No declared case/selector implements this assertion/lane edge")
    for gate in gates.values():
        for key in gate.logical_gate_ids:
            if (gate.id, key) not in used_gate_logical:
                block("unused_gate_mapping", gate.id+":"+key, "Logical membership alone supplies no assertion")

    for missing in set(inventory.compatibility_authorities)-set(overrides):
        block("missing_compatibility_mapping", missing, "Explicit inherited/successor interpretation required")
    for override in overrides.values():
        _hash(override.review_sha256)
        _require(override.mode in ("preserve", "override"), "Unknown compatibility mode")
        _require(override.milestone == subject.milestone, "Compatibility milestone differs")
        _unique(override.inherited_obligation_ids, "inherited obligations")
        _unique(override.successor_obligation_ids, "successor obligations")
        _require(set(override.inherited_obligation_ids+override.successor_obligation_ids) <= set(obligations),
                 "Unknown compatibility obligation")
        _require(bool(override.reason.strip()), "Missing compatibility reason")
        _require(all(key in plans for key in override.inherited_obligation_ids+override.successor_obligation_ids),
                 "Compatibility points to undeclared assertion plans")
    health = overrides.get("shared:compatibility:2")
    if health is not None and (health.mode != "override"
            or "m1:V0-HTTP-04" not in health.inherited_obligation_ids
            or "M4-API-SCHEMA:clause:2" not in health.successor_obligation_ids):
        block("health_override", "V0-HTTP-04", "Retain schema0 obligation and explicit schema4 successor authority")

    # Admission qualifies the product evaluator lineage, while its own execution
    # remains a separate harness subject. Never relabel the harness tree as product.
    for gate in gates.values():
        targets = keyed(gate.qualification_targets, "product_gate_id", "qualification target")
        expected_targets: set[str] = set()
        if "M1-GATE-ADMISSION" in gate.logical_gate_ids:
            dependent_logical = {key for row in requirements.values()
                if "M1-GATE-ADMISSION" in row.prerequisite_gate_ids for key in row.logical_gate_ids}
            expected_targets = {g.id for g in gates.values() if g.role == "product"
                                and dependent_logical & set(g.logical_gate_ids)}
        _require(set(targets) <= expected_targets, "Unrelated admission qualification target")
        for missing in expected_targets-set(targets):
            block("missing_admission_lineage", gate.id+":"+missing, "Declare exact dependent product evaluator lineage")
        for key, target in targets.items():
            candidate = gates[key]
            candidate_suite = suites[candidate.suite_id]
            expected = QualificationBinding(key, candidate_suite.ordered_suite_sha256,
                candidate_suite.evaluator_sha256, candidate.runtime_image_sha256,
                candidate.environment_sha256, candidate.limits_sha256, candidate.execution_protocol)
            if target != expected:
                block("admission_lineage_mismatch", gate.id+":"+key, "Admission target differs from actual product bindings")

    prerequisites = PrerequisitePlan(inventory.prerequisite_ids,
        tuple(key for key, row in logical.items() if row.role == "prerequisite"),
        tuple(gate for gate in gates.values() if gate.role == "prerequisite"),
        tuple((row.id, row.prerequisite_gate_ids) for row in requirements.values()
              if row.role == "product" and row.prerequisite_gate_ids),
        scope_plan.sha256 if scope_plan is not None else None, inventory.qualification_rules)
    compiled_sha = _digest({"protocol": PROTOCOL, "inventory": inventory.sha256,
                             "declaration": declaration_sha,
                             "scope": scope_plan.sha256 if scope_plan is not None else None})
    output: registry.Registry | None = None
    if not blocks:
        try:
            compiled: list[registry.Gate] = []
            for gate in gates.values():
                if gate.role != "product":
                    continue
                suite = suites[gate.suite_id]
                binding = registry.Binding(subject, suite.ordered_suite_sha256, suite.evaluator_sha256,
                    gate.runtime_image_sha256, gate.environment_sha256, gate.limits_sha256, gate.seed_sha256,
                    gate.execution_protocol, suite.execution_purpose)
                owners = tuple(key for key in inventory.product_ids if key in gate_owners[gate.id])
                compiled.append(registry.Gate(gate.id, owners, suite.ordered_case_ids, binding))
            output = registry.Registry(subject, compiled_sha, inventory.product_ids, tuple(roster), tuple(compiled))
        except registry.AcceptanceError as error:
            block("registry_contract", "registry", str(error))
    return CompilationResult(output, tuple(sorted(set(blocks), key=lambda b: (b.code, b.target, b.detail))),
                             prerequisites, declaration_sha, compiled_sha, declaration.compatibility)
