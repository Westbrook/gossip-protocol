"""Offline capacity controls; normalized fixtures never establish product evidence.

Full-flow fixtures retain the real 312-unit/123-product/3-prerequisite denominator
but their synthetic selectors/statuses have no semantic acceptance authority.
"""
from __future__ import annotations

from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import unittest

from gossip_harness import candidate_http_journal_v3 as journal
from gossip_harness import cumulative_scope_authority_v1 as authority
from gossip_harness import cumulative_scope_source_v1 as source
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import project_acceptance_compiler_v1 as compiler
from gossip_harness import project_acceptance_registry_v1 as registry
from tests.test_candidate_scope_consumer_v1 import FixtureAuthority, FixtureSnapshot
from tests.test_project_acceptance_compiler_v1 import inventory, sha, subject, synthetic_declaration, synthetic_scope
from tests.test_project_acceptance_registry_v1 import registry as legacy_registry

# Captured by executing unmodified files at 6d3e71d before drafting the extension.
# This is a historical identity vector, not a recomputation using new functions.
LEGACY = {
    "registry": "69fe726ce962c9c39d1c8a323bf952c640db97bf006f80a4cb28f9d37248300d",
    "design": "51eefef7b707b7a9c493d3bf4c8ffcc6705913f1e90ad8c056ef37f422669c60",
    "subject": "1ff6170a80dc502d82466bd3b5e4f824ff4335be5da80621735829e8121084f0",
    "inventory": "ce0852452501153be2ef81495773b99ae70519c5cd577da774372e509d793487",
    "declaration": "e73d379e0641d22a856bf39747079645acdb0e3c4d27f84c2d009740cf7004a5",
    "declaration_bytes": 662220,
    "scope": "4261d91f98b0840d1ee856db17c7984104cd3614b34a0a81459859ee8ef66852",
    "compiled_registry": "78ca902f5a5a739e81a743c06422a5a3bf71636472d79cb2d2c40b796f527b5a",
    "compiled_design": "cc17572bc49e32bc910c70452febc62ebb4b758a9c23cded519b83c15293d628",
}
PUBLIC_FAMILIES = (("cli", 57), ("http", 276), ("product", 8), ("storage", 249))


def expanded_declaration(inv, *, profile=registry.HISTORY_CAPACITY_PROFILE):
    """Exercise all aggregate containers without claiming real selector adequacy.

    Family labels size the exact current 590-history roster; semantic selectors
    intentionally remain synthetic. A full physical rehearsal is still required.
    """
    base = synthetic_declaration(inv)
    public_key = "M1-GATE-PUBLIC"
    product_gate, prerequisite_gate = base.gates
    gates = [replace(product_gate, logical_gate_ids=tuple(k for k in product_gate.logical_gate_ids if k != public_key))]
    suites = list(base.suites)
    by_assertion = {(p.obligation_id, a.id): a for p in base.obligations for a in p.assertions}
    selected = next(edge for edge in base.edges
                    if edge.obligation_id == "m1:V0-ADAPTER-01" and edge.assertion_id == "assert-positive")
    names = tuple(f"{family}-history-{i:03d}" for family, count in PUBLIC_FAMILIES for i in range(1, count + 1))
    first_public = names[0]
    edges = []
    for edge in base.edges:
        assertion = by_assertion[edge.obligation_id, edge.assertion_id]
        if edge.gate_id != "product":
            edges.append(edge)
            continue
        if set(assertion.logical_gate_ids) & set(gates[0].logical_gate_ids):
            edges.append(edge)
        if public_key in assertion.logical_gate_ids:
            edges.append(replace(edge, gate_id=first_public))
    public_template = replace(base.suites[0], definition_purpose="public_release", execution_purpose="public_release", capabilities=("public-contract",))
    for name in names:
        suite = replace(public_template, id="suite-" + name,
                        ordered_suite_sha256=sha("suite-" + name), definition_sha256=sha("definition-" + name))
        suites.append(suite)
        gates.append(replace(product_gate, id=name, physical_slot=name + "-slot", suite_id=suite.id, logical_gate_ids=(public_key,)))
        if name != first_public:
            edges.append(replace(selected, gate_id=name))
    # Later logical gates with no original-purpose override allow a separately
    # declared repeatability execution; no historical purpose is relabelled.
    later = next(g for g in inv.logical_gates if g.role == "product" and g.original_purpose is None
                 and g.lane not in compiler.INDEPENDENT_LANES)
    repeat_edge = next(edge for edge in base.edges if later.id in by_assertion[edge.obligation_id, edge.assertion_id].logical_gate_ids)
    repeat_suite = replace(base.suites[0], id="suite-repeatability", definition_purpose="repeatability",
                           execution_purpose="repeatability", capabilities=(later.lane,))
    suites.append(repeat_suite)
    gates.append(replace(product_gate, id="repeatability", physical_slot="repeatability-slot",
                         suite_id=repeat_suite.id, logical_gate_ids=(later.id,)))
    edges.append(replace(repeat_edge, gate_id="repeatability"))
    suite_map = {row.id: row for row in suites}
    targets = tuple(compiler.QualificationBinding(g.id, suite_map[g.suite_id].ordered_suite_sha256,
                    suite_map[g.suite_id].evaluator_sha256, g.runtime_image_sha256, g.environment_sha256,
                    g.limits_sha256, g.execution_protocol) for g in gates if g.id != "repeatability")
    gates.append(replace(prerequisite_gate, qualification_targets=targets))
    return replace(base, suites=tuple(suites), gates=tuple(gates), edges=tuple(edges),
                   purposes=tuple(replace(p, execution_purpose="public_release", prospective_authority_sha256=sha("capacity-public-purpose-review"))
                                  if p.logical_gate_id == public_key else p for p in base.purposes), capacity_profile=profile)


def normalized_registry(count, *, profile=registry.HISTORY_CAPACITY_PROFILE):
    """Bounded synthetic aggregate to probe capacity independent of the compiler."""
    original = legacy_registry()
    gates = tuple(replace(original.gates[0], gate_id=f"gate-{i:04d}", requirement_ids=original.requirement_ids)
                  for i in range(count - 1)) + (original.gates[1],)
    return replace(original, gates=gates, capacity_profile=profile)


def observations(product, freeze_sha):
    return tuple(registry.Observation(g.gate_id, g.binding, registry.PhysicalExecution(
        g.binding, "execution-" + g.gate_id, sha("receipt-" + g.gate_id), sha("verifier-" + g.gate_id),
        "completed", tuple(registry.CaseResult(case, "passed") for case in g.ordered_case_ids),
        freeze_sha if g.binding.purpose != "public_release" else None)) for g in product.gates)


def freeze(product):
    return registry.CohortFreeze(tuple(replace(product.subject, trajectory_id=t,
        source_sha256=product.subject.source_sha256 if t == product.subject.trajectory_id else sha(t))
        for t in product.cohort_trajectory_ids), sha("capacity-freeze"), sha("freeze-verifier"), True)


def assess(product, rows, frozen=None, *, expected_design=None):
    frozen = frozen or freeze(product)
    return registry.assess(product, rows, registry.Promotion(product.subject, "promoted", sha("promotion"), sha("promotion-verifier")),
        frozen, expected_design_sha256=expected_design or registry.design_fingerprint(product),
        expected_execution_contract_sha256=product.subject.execution_contract_sha256)


class AcceptanceHistoryCapacityV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.inv = inventory(2)
        cls.declaration = expanded_declaration(cls.inv)
        cls.scope = synthetic_scope(cls.inv, cls.declaration)
        cls.subject = subject(cls.inv)
        cls.design = compiler.compile_design(cls.inv, cls.declaration, cls.subject,
            scope_plan=cls.scope, expected_scope_sha256=cls.scope.sha256)

    def test_historical_hashes_and_serialized_declaration_match_prechange_vectors(self):
        old = legacy_registry()
        self.assertEqual(registry.fingerprint(old), LEGACY["registry"])
        self.assertEqual(registry.design_fingerprint(old), LEGACY["design"])
        self.assertEqual(registry.fingerprint(old.subject), LEGACY["subject"])
        declaration = synthetic_declaration(self.inv)
        scope = synthetic_scope(self.inv, declaration)
        raw = json.dumps(compiler.declaration_record(declaration), sort_keys=True, ensure_ascii=True,
                         allow_nan=False, separators=(",", ":")).encode()
        self.assertEqual(len(raw), LEGACY["declaration_bytes"])
        self.assertEqual(hashlib.sha256(raw).hexdigest(), LEGACY["declaration"])
        self.assertEqual(compiler.declaration_fingerprint(declaration), LEGACY["declaration"])
        self.assertEqual(self.inv.sha256, LEGACY["inventory"])
        self.assertEqual(scope.sha256, LEGACY["scope"])
        design = compiler.compile_design(self.inv, declaration, self.subject, scope_plan=scope, expected_scope_sha256=scope.sha256)
        self.assertEqual(registry.fingerprint(design.registry), LEGACY["compiled_registry"])
        self.assertEqual(registry.design_fingerprint(design.registry), LEGACY["compiled_design"])
        self.assertNotIn("capacity_profile", compiler.declaration_record(declaration))
        self.assertIn("capacity_profile", asdict(declaration))  # No raw-asdict compatibility claim.

    def test_590_public_plus_independent_repeatability_full_registered_consumer_flow(self):
        self.assertTrue(self.design.declaration_complete, self.design.blockers)
        self.assertEqual(len(self.declaration.gates), 593)
        self.assertEqual(len(self.declaration.suites), 593)
        self.assertEqual(len(self.design.registry.gates), 592)
        self.assertEqual(sum(g.binding.purpose == "public_release" for g in self.design.registry.gates), 590)
        self.assertEqual(len(self.design.prerequisites.execution_gates[0].qualification_targets), 591)
        self.assertEqual((len(self.inv.obligations), len(self.inv.product_ids), len(self.inv.prerequisite_ids)), (312, 123, 3))
        snapshot = FixtureSnapshot(self.inv, self.declaration, self.scope, self.subject)
        result = consumer.CandidateScopeConsumer(FixtureAuthority(snapshot)).assess(
            self.inv, self.declaration, self.scope, self.subject)
        self.assertTrue(result.accepted_against_registry, result.issues)
        self.assertEqual(len(result.product.physical_gates), 592)
        self.assertEqual(len(result.product.requirements), 123)
        self.assertEqual(snapshot.calls.count("observation"), 592)
        self.assertEqual(snapshot.reg.declaration_sha256, compiler.declaration_fingerprint(self.declaration))
        self.assertEqual(result.authority_checkpoint, snapshot.checkpoint)

    def test_assembly_requires_explicit_profile_without_size_inference(self):
        catalog = source.load_catalog(Path(__file__).resolve().parents[1])
        default = source.assemble_declaration(catalog, self.declaration.cohort, (), review_sha256=sha("fixture-review"))
        opted = source.assemble_declaration(catalog, self.declaration.cohort, (), review_sha256=sha("fixture-review"),
                                           capacity_profile=registry.HISTORY_CAPACITY_PROFILE)
        self.assertEqual(default.capacity_profile, registry.LEGACY_CAPACITY_PROFILE)
        self.assertEqual(opted.capacity_profile, registry.HISTORY_CAPACITY_PROFILE)
        self.assertEqual(len(opted.obligations), 312)
        self.assertEqual(default.gates, opted.gates)
        self.assertNotEqual(compiler.declaration_fingerprint(default), compiler.declaration_fingerprint(opted))

    def test_authority_review_request_exposes_bound_capacity_manifest(self):
        catalog = source.load_catalog(Path(__file__).resolve().parents[1])
        submission = authority.ScopeSubmission(catalog, self.declaration, self.scope, self.subject, (), ())
        request = submission.request()
        self.assertEqual(request["declaration"]["capacity_profile"], registry.HISTORY_CAPACITY_PROFILE)
        self.assertEqual(request["declaration_sha256"], compiler.declaration_fingerprint(self.declaration))
        target = [row for row in request["targets"] if row["id"] == "capacity-contract"]
        self.assertEqual(len(target), 1)
        self.assertEqual(target[0]["value"], registry.capacity_manifest(registry.HISTORY_CAPACITY_PROFILE))
        self.assertEqual(sum(row["id"].startswith("suite:") for row in request["targets"]), 593)
        self.assertEqual(sum(row["id"].startswith("obligation:") for row in request["targets"]), 312)
        raw = source.encoded(request)
        self.assertLessEqual(len(raw), journal.MAX_RECORD_BYTES)
        decoded = journal.decode(raw)
        self.assertEqual(len(decoded["declaration"]["gates"]), 593)
        self.assertEqual(source.encoded(decoded), raw)
        old = synthetic_declaration(self.inv)
        legacy = replace(submission, declaration=old, scope=synthetic_scope(self.inv, old)).request()
        self.assertNotIn("capacity_profile", legacy["declaration"])
        self.assertFalse(any(row["id"] == "capacity-contract" for row in legacy["targets"]))
        self.assertEqual(legacy["declaration"], compiler.declaration_record(old))

    def test_compiler_default_profile_still_blocks_complete_expanded_declaration(self):
        declaration = replace(self.declaration, capacity_profile=registry.LEGACY_CAPACITY_PROFILE)
        scope = synthetic_scope(self.inv, declaration)
        result = compiler.compile_design(self.inv, declaration, self.subject, scope_plan=scope, expected_scope_sha256=scope.sha256)
        self.assertIsNone(result.registry)
        self.assertIn("registry_size_limit", {b.code for b in result.blockers})

    def test_registry_explicit_profile_capacity_boundaries(self):
        normalized_registry(512, profile=registry.LEGACY_CAPACITY_PROFILE)
        with self.assertRaises(registry.AcceptanceError):
            normalized_registry(513, profile=registry.LEGACY_CAPACITY_PROFILE)
        normalized_registry(4096)
        with self.assertRaises(registry.AcceptanceError):
            normalized_registry(4097)
        for bad in ("unknown", "", None, 4096, True):
            with self.subTest(profile=bad), self.assertRaises(registry.AcceptanceError):
                replace(legacy_registry(), capacity_profile=bad)

    def test_assessment_expanded_missing_gate_and_known_failure_remain_visible(self):
        product = self.design.registry
        rows = observations(product, freeze(product).receipt_sha256)
        result = assess(product, rows[:-1])
        self.assertEqual(result.status, "incomplete")
        self.assertEqual(result.missing_gates, (product.gates[-1].gate_id,))
        failing = replace(rows[0], execution=replace(rows[0].execution, outcomes=(registry.CaseResult("case", "failed"),)))
        result = assess(product, (failing, *rows[1:-1]))
        self.assertEqual(result.status, "rejected")
        self.assertIn(product.gates[0].gate_id, result.failed_gates)
        self.assertIn(product.gates[-1].gate_id, result.missing_gates)

    def test_expanded_duplicate_gate_case_and_physical_origin_rejected(self):
        product = self.design.registry
        rows = observations(product, freeze(product).receipt_sha256)
        with self.assertRaises(registry.AcceptanceError):
            replace(product, gates=(*product.gates, product.gates[0]))
        with self.assertRaises(registry.AcceptanceError):
            assess(product, (*rows, rows[0]))
        with self.assertRaises(registry.AcceptanceError):
            replace(rows[0].execution, outcomes=rows[0].execution.outcomes * 2)
        wrong = replace(rows[1], execution=replace(rows[1].execution,
                        execution_id=rows[0].execution.execution_id, receipt_sha256=rows[0].execution.receipt_sha256))
        with self.assertRaises(registry.AcceptanceError):
            assess(product, (rows[0], wrong, *rows[2:]))

    def test_observation_bound_and_undeclared_extra_rejected(self):
        product = normalized_registry(4096)
        rows = observations(product, freeze(product).receipt_sha256)
        self.assertTrue(assess(product, rows).accepted_against_registry)
        with self.assertRaises(registry.AcceptanceError):
            assess(product, (*rows, rows[0]))
        small = normalized_registry(590)
        small_rows = observations(small, freeze(small).receipt_sha256)
        rogue = replace(small_rows[0], gate_id="rogue", execution=replace(small_rows[0].execution,
                        execution_id="rogue-execution", receipt_sha256=sha("rogue-receipt")))
        with self.assertRaises(registry.AcceptanceError):
            assess(small, (*small_rows, rogue))

    def test_profile_substitution_fails_independent_registration_and_design(self):
        snapshot = FixtureSnapshot(self.inv, self.declaration, self.scope, self.subject)
        changed = replace(self.declaration, capacity_profile=registry.LEGACY_CAPACITY_PROFILE)
        result = consumer.CandidateScopeConsumer(FixtureAuthority(snapshot)).assess(self.inv, changed, self.scope, self.subject)
        self.assertEqual(result.status, "invalid_evidence")
        self.assertNotIn("observation", snapshot.calls)
        old = legacy_registry()
        expanded = replace(old, capacity_profile=registry.HISTORY_CAPACITY_PROFILE)
        with self.assertRaises(registry.AcceptanceError):
            assess(expanded, (), expected_design=registry.design_fingerprint(old))
        self.assertNotEqual(registry.fingerprint(old), registry.fingerprint(expanded))

    def test_source_contract_freeze_and_purpose_barriers_survive_expansion(self):
        product = self.design.registry
        frozen = freeze(product)
        rows = observations(product, frozen.receipt_sha256)
        with self.assertRaises(registry.AcceptanceError):
            assess(product, rows, replace(frozen, subjects=frozen.subjects[:-1]))
        for row in rows:
            if row.binding.purpose != "public_release":
                with self.subTest(purpose=row.binding.purpose), self.assertRaises(registry.AcceptanceError):
                    replace(row, mode="reused", reuse_receipt_sha256=sha("reuse"))
        wrong = replace(rows[1].binding, subject=replace(rows[1].binding.subject, source_sha256=sha("wrong-source")))
        with self.assertRaises(registry.AcceptanceError):
            assess(product, (rows[0], replace(rows[1], binding=wrong, execution=replace(rows[1].execution, binding=wrong)), *rows[2:]))

    def test_unrelated_identifier_and_per_history_limits_stay_512(self):
        self.assertEqual(registry.MAX_ITEMS, 512)
        product = self.design.registry
        with self.assertRaises(registry.AcceptanceError):
            replace(product.gates[0], ordered_case_ids=tuple(f"case-{i}" for i in range(513)))
        with self.assertRaises(registry.AcceptanceError):
            replace(product, requirement_ids=tuple(f"requirement-{i}" for i in range(513)))
        with self.assertRaises(registry.AcceptanceError):
            replace(product, cohort_trajectory_ids=(product.subject.trajectory_id,) + tuple(f"trajectory-{i}" for i in range(512)))
        with self.assertRaises(registry.AcceptanceError):
            registry.PhysicalExecution(product.gates[0].binding, "oversize", sha("receipt"), sha("verifier"), "completed",
                tuple(registry.CaseResult(f"case-{i}", "passed") for i in range(513)), sha("freeze"))

    def test_compiler_over_profile_limit_and_duplicate_slots_remain_blocked(self):
        base = self.declaration.gates[1]
        extras = tuple(replace(base, id=f"extra-{i}", physical_slot=f"extra-slot-{i}")
                       for i in range(4097 - len(self.declaration.gates)))
        declaration = replace(self.declaration, gates=(*self.declaration.gates, *extras))
        scope = synthetic_scope(self.inv, declaration)
        result = compiler.compile_design(self.inv, declaration, self.subject, scope_plan=scope, expected_scope_sha256=scope.sha256)
        self.assertIsNone(result.registry)
        self.assertIn("registry_size_limit", {b.code for b in result.blockers})
        duplicate = replace(base, id="duplicate-slot")
        declaration = replace(self.declaration, gates=(*self.declaration.gates, duplicate))
        with self.assertRaises(compiler.CompilerError):
            compiler.compile_design(self.inv, declaration, self.subject)

    def test_profile_contract_binds_only_aggregate_limit(self):
        self.assertEqual(registry.capacity_manifest(registry.HISTORY_CAPACITY_PROFILE), {
            "profile": registry.HISTORY_CAPACITY_PROFILE, "execution_gates": 4096,
            "observations": 4096, "other_collections": 512})
        record = compiler.declaration_record(self.declaration)
        self.assertEqual(record["capacity_profile"], registry.HISTORY_CAPACITY_PROFILE)
        self.assertNotEqual(compiler.declaration_fingerprint(self.declaration),
                            compiler.declaration_fingerprint(replace(self.declaration, capacity_profile=registry.LEGACY_CAPACITY_PROFILE)))
        for profile in ("unknown", None):
            with self.subTest(profile=profile), self.assertRaises(compiler.CompilerError):
                compiler.declaration_fingerprint(replace(self.declaration, capacity_profile=profile))


if __name__ == "__main__":
    unittest.main()
