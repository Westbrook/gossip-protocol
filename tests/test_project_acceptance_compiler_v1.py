"""Compiler structure only: synthetic declarations are not product evidence."""
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from gossip_harness import project_acceptance_compiler_v1 as c
from gossip_harness import project_acceptance_registry_v1 as r

ROOT = Path(__file__).resolve().parents[1]


def sha(value):
    return hashlib.sha256(value.encode()).hexdigest()


def inventory(version=1):
    return c.load_inventory(ROOT, product_file=c.V1_FILE if version == 1 else c.V2_FILE,
                           expected_product_sha256=c.PRODUCT_V1_SHA256 if version == 1 else c.PRODUCT_V2_SHA256)


def synthetic_declaration(inv):
    """Explicit structural fixture, never the real clause-to-test mapping."""
    requirements = {row.id: row for row in inv.requirements}
    logical = {row.id: row for row in inv.logical_gates}
    plans = []
    edges = []
    for obligation in inv.obligations:
        owners = obligation.requirement_ids or ("M4-RELEASE-HANDOFF",)
        if obligation.kind == "admission":
            keys = ("M1-GATE-ADMISSION",)
        else:
            keys = tuple(dict.fromkeys(key for owner in owners for key in requirements[owner].logical_gate_ids))
        kinds = c.PRODUCT_KINDS if obligation.role == "product" else ("inspection",)
        assertions = tuple(c.Assertion("assert-"+kind, kind, keys) for kind in kinds)
        plans.append(c.ObligationPlan(obligation.id, owners, assertions, sha("synthetic-review"),
                                      "Synthetic structural ownership, not a qualified semantic mapping"))
        for assertion in assertions:
            edges.append(c.CoverageEdge(obligation.id, assertion.id, obligation.role, "case",
                                       "$.synthetic."+obligation.id+"."+assertion.kind))
    suites = []
    gates = []
    purposes = []
    for role in ("product", "prerequisite"):
        keys = tuple(row.id for row in inv.logical_gates if row.role == role)
        lanes = tuple(sorted({logical[key].lane for key in keys}))
        purpose = "independent_acceptance" if role == "product" else "harness_qualification"
        suites.append(c.SuiteDefinition(role, ("case",), sha("suite-"+role), sha("definition-"+role),
            purpose, purpose, inv.product_sha256, sha("evaluator-"+role), True, True, lanes))
        gates.append(c.ExecutionGate(role, role+"-slot", role, role, keys, sha("runtime"),
            sha("environment"), sha("limits"), sha("seed"), "synthetic-executor-v1"))
        for key in keys:
            original = logical[key].original_purpose
            if original is not None and original != purpose:
                purposes.append(c.PurposeAssignment(key, original, purpose, sha("prospective-purpose-review")))
    product_gate, prerequisite_gate = gates
    target = c.QualificationBinding(product_gate.id, suites[0].ordered_suite_sha256,
        suites[0].evaluator_sha256, product_gate.runtime_image_sha256,
        product_gate.environment_sha256, product_gate.limits_sha256, product_gate.execution_protocol)
    gates[1] = replace(prerequisite_gate, qualification_targets=(target,))
    overrides = []
    for authority in inv.compatibility_authorities:
        health = authority == "shared:compatibility:2"
        overrides.append(c.CompatibilityOverride(authority, "override" if health else "preserve", "M4",
            ("m1:V0-HTTP-04",) if health else ("m1:V0-ID-01",),
            ("M4-API-SCHEMA:clause:2",) if health else ("M4-COMPATIBILITY:clause:0",),
            "Synthetic explicit interpretation, not evaluated compatibility", sha("compatibility-review")))
    trajectories = []
    for block in c.BLOCKS:
        for arm in c.ARMS:
            builders = ("B01", "B05", "B09", "B13") if arm == "S4-G" else tuple(
                "B"+str(n).zfill(2) for n in range(1, 17))
            trajectories.append(c.Trajectory(arm+"."+block, arm, block,
                builders+("R1", "R2", "R3", "R4"),
                "durable_central_scheduler" if arm == "O16-G" else "peer_local",
                sha("seed-"+block), sha("faults-"+block), c.FAULTS if block == "compound_recovery" else ()))
    cohort = c.CohortDesign("cohort", tuple(trajectories), ("M1", "M2", "M3", "M4"),
        *(sha(name) for name in ("transport", "policy", "releases", "resources", "models",
                                "generated-tests", "barrier", "held-out")))
    return c.Declaration(inv.sha256, tuple(plans), tuple(suites), tuple(gates), tuple(edges),
                         tuple(purposes), tuple(overrides), cohort)


def subject(inv):
    return r.Subject("cohort", "S4-G.healthy", "M4", sha("execution-contract"),
                     inv.product_sha256, sha("final-candidate-source"))


def synthetic_scope(inv, declaration):
    """Mechanical test data simulating registration; never a real semantic approval.

    Generic selectors/ownership below have NO qualified meaning. Production must
    obtain a separately reviewed mapping, then authenticate its scope prerequisite.
    """
    logical = {row.id: row for row in inv.logical_gates}
    source = {row.id: row for row in inv.obligations}
    applicability = []
    for plan in declaration.obligations:
        obligation = source[plan.obligation_id]
        eligible = tuple(key for key, row in logical.items() if row.role == obligation.role
                         and set(plan.requirement_ids) & set(row.requirement_ids))
        kinds = c.KINDS if obligation.role == "product" else ("inspection",)
        required = tuple(c.ApplicabilityCell(a.kind, key) for a in plan.assertions for key in a.logical_gate_ids)
        required_set = {(cell.kind, cell.logical_gate_id) for cell in required}
        excluded = tuple(c.NotApplicable(c.ApplicabilityCell(kind, key), "Synthetic N/A for structural testing")
                         for key in eligible for kind in kinds if (kind, key) not in required_set)
        applicability.append(c.ObligationApplicability(plan.obligation_id, plan.requirement_ids,
            required, excluded, "Synthetic applicability, not a reviewed production oracle"))
    dispositions = tuple(c.PlanningDisposition(owner, gap_id, ("m1:"+owner,),
        "Synthetic disposition retaining source type and scope; not a current verdict")
        for owner, gap_id, _kind, _text in inv.planning_notes)
    return c.ScopePlan(inv.sha256, c.declaration_fingerprint(declaration), tuple(applicability),
        dispositions, tuple(rule.id for rule in inv.qualification_rules), declaration.purposes,
        declaration.compatibility, ())


def compile_case(inv=None, declaration=None, scope=None):
    inv = inv or inventory()
    baseline = synthetic_declaration(inv)
    declaration = declaration or baseline
    scope = scope or synthetic_scope(inv, baseline)
    return c.compile_design(inv, declaration, subject(inv), scope_plan=scope,
                            expected_scope_sha256=scope.sha256)


def codes(result):
    return {row.code for row in result.blockers}


class AcceptanceCompilerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.v1 = inventory()
        cls.v2 = inventory(2)
        cls.full = synthetic_declaration(cls.v1)

    def test_authentic_source_census_and_pins(self):
        self.assertEqual(len(self.v1.product_ids), 123)
        self.assertEqual(len(self.v1.prerequisite_ids), 3)
        self.assertEqual(self.v1.prerequisite_ids,
                         ("M1-INTEGRATION-02", "M1-SCOPE-01", "M1-SCOPE-02"))
        self.assertEqual(sum(row.kind == "requirement" and row.source_sha256 == c.PRODUCT_V1_SHA256
                             for row in self.v1.obligations), 65)
        self.assertEqual(len(self.v1.logical_gates), 99)  # 14 inherited + 85 later group/lane pairs
        self.assertEqual(len(self.v1.planning_notes), 188)  # Retained design notes, never outcomes.
        self.assertTrue(any(row.id == "shared:interfaces:browser" for row in self.v1.obligations))
        self.assertTrue(any(row.id == "shared:release_ownership:immutable_preserved_paths"
                            for row in self.v1.obligations))
        self.assertTrue(any(row.id == "prerequisite:M1-GATE-ADMISSION" for row in self.v1.obligations))

    def test_v2_preserves_ids_and_adds_real_amendment_obligations(self):
        self.assertEqual(self.v1.product_ids, self.v2.product_ids)
        self.assertEqual(self.v1.logical_gates, self.v2.logical_gates)
        self.assertGreater(len(self.v2.obligations), len(self.v1.obligations))
        names = {row.id for row in self.v2.obligations}
        self.assertIn("V2-BACKUP-ROOT:required_observations:0", names)
        self.assertIn("V2-COUNTER-DOMAIN:clauses:0", names)
        self.assertIn("shared:worker_state_table", names)
        self.assertIn("shared:portable_m1_hash_vectors", names)
        self.assertEqual(len(self.v2.compatibility_authorities), 15)

    def test_shrunk_document_with_matching_self_declared_hash_is_not_recognized(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in (c.M1_FILE, c.V1_FILE, c.V2_FILE):
                (root/name).write_bytes((ROOT/name).read_bytes())
            changed = json.loads((root/c.V2_FILE).read_bytes())
            changed["requirements"][0]["clauses"].pop()
            raw = json.dumps(changed).encode()
            (root/c.V2_FILE).write_bytes(raw)
            with self.assertRaises(c.CompilerError):
                c.load_inventory(root, product_file=c.V2_FILE,
                                 expected_product_sha256=hashlib.sha256(raw).hexdigest())

    def test_known_m1_and_base_pins_cannot_be_replaced(self):
        for index in (0, 1):
            raw = list(self.v2.source_payloads)
            value = json.loads(raw[index])
            value["requirements"].pop()
            raw[index] = json.dumps(value).encode()
            forged = replace(self.v2, source_payloads=tuple(raw))
            with self.subTest(index=index), self.assertRaises(c.CompilerError):
                c.compile_design(forged, synthetic_declaration(forged), subject(forged))

    def test_derived_inventory_cannot_omit_requirement_clause_or_shared_norm(self):
        variants = (
            replace(self.v1, requirements=self.v1.requirements[1:]),
            replace(self.v1, obligations=self.v1.obligations[1:]),
            replace(self.v1, logical_gates=self.v1.logical_gates[1:]),
        )
        for inv in variants:
            with self.subTest(inv=inv.sha256), self.assertRaises(c.CompilerError):
                c.compile_design(inv, replace(self.full, inventory_sha256=inv.sha256), subject(inv))

    def test_complete_synthetic_design_produces_one_product_execution_gate(self):
        result = compile_case(self.v1, self.full)
        self.assertEqual(result.blockers, ())
        self.assertTrue(result.declaration_complete)
        self.assertEqual(len(result.registry.gates), 1)
        self.assertEqual(result.registry.requirement_ids, self.v1.product_ids)
        self.assertEqual(len(result.registry.cohort_trajectory_ids), 6)
        self.assertEqual(result.prerequisites.requirement_ids, self.v1.prerequisite_ids)
        self.assertEqual(len(result.prerequisites.execution_gates), 1)
        self.assertEqual(len(result.prerequisites.product_dependencies), 3)
        self.assertFalse(hasattr(result, "accepted_against_registry"))
        self.assertFalse(hasattr(result, "physically_executed"))

    def test_complete_v2_structure_does_not_claim_v2_execution(self):
        result = compile_case(self.v2)
        self.assertTrue(result.declaration_complete, result.blockers[:5])
        self.assertEqual(result.registry.subject.requirements_sha256, c.PRODUCT_V2_SHA256)

    def test_omitted_clause_shared_norm_and_amendment_are_blockers(self):
        for inv, target in (
            (self.v1, "M4-MIGRATION:clause:3"),
            (self.v1, "shared:persistence:backup_rows"),
            (self.v2, "V2-BACKUP-ROOT:required_observations:0"),
        ):
            declaration = synthetic_declaration(inv)
            declaration = replace(declaration,
                obligations=tuple(p for p in declaration.obligations if p.obligation_id != target),
                edges=tuple(e for e in declaration.edges if e.obligation_id != target))
            result = compile_case(inv, declaration)
            self.assertIsNone(result.registry)
            self.assertTrue(any(b.code == "missing_obligation" and b.target == target for b in result.blockers))

    def test_empty_declaration_reports_missing_obligations_not_current_product_failure(self):
        declaration = replace(self.full, obligations=(), gates=(), suites=(), edges=(), purposes=(), compatibility=())
        result = compile_case(self.v1, declaration)
        self.assertIsNone(result.registry)
        self.assertIn("missing_obligation", codes(result))
        self.assertIn("missing_prerequisite", codes(result))
        self.assertIn("missing_compatibility_mapping", codes(result))

    def test_undeclared_obligation_or_unknown_owner_is_error(self):
        first = self.full.obligations[0]
        for plan in (replace(first, obligation_id="invented"),
                     replace(first, requirement_ids=("invented",))):
            with self.subTest(plan=plan), self.assertRaises(c.CompilerError):
                compile_case(self.v1, replace(self.full, obligations=(plan,)+self.full.obligations[1:]))

    def test_shared_norm_requires_reviewed_owner_reason(self):
        target = next(p for p in self.full.obligations if p.obligation_id.startswith("shared:"))
        plans = tuple(replace(p, ownership_reason="") if p == target else p for p in self.full.obligations)
        self.assertIn("unassigned_shared_norm", codes(compile_case(self.v1, replace(self.full, obligations=plans))))

    def test_required_group_lane_cannot_be_omitted(self):
        missing = "M2-IDENTITY-REVISIONS.durability"
        plans = []
        for plan in self.full.obligations:
            assertions = tuple(replace(a, logical_gate_ids=tuple(k for k in a.logical_gate_ids if k != missing))
                               for a in plan.assertions)
            plans.append(replace(plan, assertions=assertions))
        result = compile_case(self.v1, replace(self.full, obligations=tuple(plans)))
        self.assertIn("missing_required_lane", codes(result))

    def test_each_declared_assertion_needs_its_case_edge(self):
        result = compile_case(self.v1, replace(self.full, edges=self.full.edges[1:]))
        self.assertIn("missing_assertion_edge", codes(result))

    def test_assertion_requiring_two_logical_gates_needs_edges_for_both(self):
        # Other assertions still use every gate; this one must not be counted by intersection alone.
        declaration = self.full
        plan = next(p for p in declaration.obligations if len(p.assertions[0].logical_gate_ids) > 1
                    and not p.obligation_id.startswith("shared:"))
        logical = plan.assertions[0].logical_gate_ids
        small = replace(declaration.gates[0], id="partial", physical_slot="partial-slot",
                        logical_gate_ids=(logical[0],))
        edge = next(e for e in declaration.edges if e.obligation_id == plan.obligation_id)
        changed = tuple(replace(e, gate_id="partial") if e == edge else e for e in declaration.edges)
        result = compile_case(self.v1, replace(declaration, gates=declaration.gates+(small,), edges=changed))
        self.assertIn("missing_assertion_edge", codes(result))

    def test_missing_admission_dependency_stays_outside_product_registry(self):
        declaration = replace(self.full, gates=self.full.gates[:1], suites=self.full.suites[:1],
            edges=tuple(e for e in self.full.edges if e.gate_id == "product"))
        result = compile_case(self.v1, declaration)
        self.assertIsNone(result.registry)
        self.assertIn("missing_prerequisite", codes(result))
        self.assertEqual(result.prerequisites.product_dependencies,
            (("V0-ADAPTER-01", ("M1-GATE-ADMISSION",)),
             ("V0-ADAPTER-02", ("M1-GATE-ADMISSION",)),
             ("M1-ADAPTER-03", ("M1-GATE-ADMISSION",))))

    def test_prerequisite_gate_cannot_be_retyped_as_product(self):
        gate = replace(self.full.gates[1], role="product")
        with self.assertRaises(c.CompilerError):
            compile_case(self.v1, replace(self.full, gates=(self.full.gates[0], gate)))

    def test_no_physical_slot_duplication_for_logical_gates(self):
        gate = replace(self.full.gates[0], id="second-product")
        with self.assertRaises(c.CompilerError):
            compile_case(self.v1, replace(self.full, gates=self.full.gates+(gate,)))

    def test_missing_purpose_assignment_blocks_and_mismatch_is_error(self):
        result = compile_case(self.v1, replace(self.full, purposes=self.full.purposes[1:]))
        self.assertIn("missing_purpose_mapping", codes(result))
        bad = replace(self.full.purposes[0], original_purpose="authored_reference_qualification")
        with self.assertRaises(c.CompilerError):
            compile_case(self.v1, replace(self.full, purposes=(bad,)+self.full.purposes[1:]))

    def test_reference_only_nonportable_or_unsupported_purpose_cannot_compile(self):
        first = self.full.suites[0]
        cases = (
            (replace(first, candidate_portable=False), "nonportable_suite"),
            (replace(first, candidate_suitable=False), "nonportable_suite"),
            (replace(first, definition_purpose="authored_reference_qualification"), "reference_only_definition"),
        )
        for suite, expected in cases:
            with self.subTest(expected=expected):
                result = compile_case(self.v1, replace(self.full, suites=(suite, self.full.suites[1])))
                self.assertIn(expected, codes(result))
        # Original purposes cannot be changed merely to satisfy Registry.PURPOSES.
        suite = replace(first, execution_purpose="public_contract_regression")
        purposes = tuple(replace(p, execution_purpose=suite.execution_purpose)
                         if p.logical_gate_id != "M1-GATE-SCOPE" else p for p in self.full.purposes)
        # This remains invalid even when a declaration tries to repair every purpose mapping.
        result = compile_case(self.v1, replace(self.full, suites=(suite, self.full.suites[1]), purposes=purposes))
        self.assertIn("unsupported_purpose", codes(result))
        self.assertIn("independent_lane_purpose", codes(result))

    def test_service_api_cannot_claim_http_browser_or_cli_without_capabilities(self):
        suite = replace(self.full.suites[0], capabilities=("independent-integration",))
        result = compile_case(self.v1, replace(self.full, suites=(suite, self.full.suites[1])))
        self.assertIn("lane_capability", codes(result))

    def test_prior_contract_suite_needs_explicit_compatibility_review(self):
        suite = replace(self.full.suites[0], source_contract_sha256=sha("old-contract"))
        result = compile_case(self.v1, replace(self.full, suites=(suite, self.full.suites[1])))
        self.assertIn("suite_contract_mapping", codes(result))

    def test_every_compatibility_authority_needs_explicit_mapping(self):
        result = compile_case(self.v1, replace(self.full, compatibility=self.full.compatibility[1:]))
        self.assertIn("missing_compatibility_mapping", codes(result))

    def test_health_override_preserves_old_and_successor_obligations(self):
        health = next(x for x in self.full.compatibility if x.authority_obligation_id == "shared:compatibility:2")
        for bad in (replace(health, mode="preserve"),
                    replace(health, inherited_obligation_ids=("m1:V0-ID-01",)),
                    replace(health, successor_obligation_ids=("M4-COMPATIBILITY:clause:0",))):
            values = tuple(bad if x == health else x for x in self.full.compatibility)
            self.assertIn("health_override", codes(compile_case(self.v1, replace(self.full, compatibility=values))))
        with self.assertRaises(c.CompilerError):
            c.compile_design(self.v1, self.full, replace(subject(self.v1), milestone="M3"))

    def test_partial_duplicate_arm_block_or_changed_roles_blocks(self):
        cohort = self.full.cohort
        cases = (
            replace(cohort, trajectories=cohort.trajectories[:-1]),
            replace(cohort, trajectories=cohort.trajectories[:-1]+(
                replace(cohort.trajectories[-1], arm="S4-G"),)),
            replace(cohort, trajectories=(replace(cohort.trajectories[0], role_ids=("B01",)),)+cohort.trajectories[1:]),
            replace(cohort, milestones=("M1", "M4")),
        )
        for value in cases:
            with self.subTest(value=value):
                self.assertFalse(compile_case(self.v1, replace(self.full, cohort=value)).declaration_complete)

    def test_faults_policy_matching_and_common_metadata_are_required(self):
        cohort = self.full.cohort
        for trajectory in (
            replace(cohort.trajectories[-1], faults=()),
            replace(cohort.trajectories[-1], decision_placement="peer_local"),
            replace(cohort.trajectories[-1], block_seed_sha256=sha("unmatched")),
        ):
            value = replace(cohort, trajectories=cohort.trajectories[:-1]+(trajectory,))
            self.assertFalse(compile_case(self.v1, replace(self.full, cohort=value)).declaration_complete)
        with self.assertRaises(c.CompilerError):
            compile_case(self.v1, replace(self.full, cohort=replace(cohort, held_out_plan_sha256="")))

    def test_design_identity_binds_assertion_selectors_and_reviewed_mapping(self):
        before = compile_case(self.v1, self.full)
        edge = replace(self.full.edges[0], assertion_selector="$.different_observation")
        after = compile_case(self.v1, replace(self.full, edges=(edge,)+self.full.edges[1:]))
        self.assertFalse(after.declaration_complete)
        self.assertIn("unreviewed_declaration", codes(after))
        self.assertNotEqual(before.compiled_inventory_sha256, after.compiled_inventory_sha256)
        revised = replace(self.full, edges=(edge,)+self.full.edges[1:])
        # Explicitly simulate a NEW host registration; never reuse the old scope pin.
        new_scope = synthetic_scope(self.v1, revised)
        registered = compile_case(self.v1, revised, new_scope)
        self.assertTrue(registered.declaration_complete)
        self.assertNotEqual(r.design_fingerprint(before.registry), r.design_fingerprint(registered.registry))

    def test_clarifications_and_qualification_rules_are_source_bound(self):
        raw = json.loads(self.v1.source_payloads[0])
        obligations = {row.id: row for row in self.v1.obligations}
        for row in raw["contract_clarifications"]:
            self.assertEqual(obligations[row["id"]].requirement_ids, tuple(row["requirements"]))
            self.assertEqual(obligations[row["id"]].source_sha256, c.M1_SHA256)
        self.assertEqual(obligations["CLARIFY-INTEGRATION-CONTROL"].role, "prerequisite")
        rules = {rule.id for rule in self.v1.qualification_rules}
        self.assertIn("acceptance-boundary:finite_coverage", rules)
        self.assertIn("acceptance-boundary:public_reuse", rules)
        self.assertEqual(len(rules), len(json.loads(self.v1.source_payloads[2])["acceptance_boundary"])+14)
        self.assertIn("m1-gate-definition:M1-GATE-CONTROL", rules)
        self.assertIn("m1-gate-definition:M1-GATE-SCOPE", rules)

    def test_scope_requires_separate_registration_and_exact_declaration(self):
        scope = synthetic_scope(self.v1, self.full)
        missing = c.compile_design(self.v1, self.full, subject(self.v1))
        self.assertIn("missing_scope_plan", codes(missing))
        unregistered = c.compile_design(self.v1, self.full, subject(self.v1), scope_plan=scope)
        self.assertIn("missing_scope_registration", codes(unregistered))
        different = c.compile_design(self.v1, self.full, subject(self.v1), scope_plan=scope,
                                     expected_scope_sha256=sha("not-the-approved-scope"))
        self.assertIn("scope_registration_mismatch", codes(different))
        self.assertIsNone(different.registry)

    def test_no_all_positive_or_inspection_only_completion(self):
        for kind in ("positive", "inspection"):
            plans = tuple(replace(p, assertions=(replace(p.assertions[0], kind=kind),))
                          if p.obligation_id not in {o.id for o in self.v1.obligations if o.role == "prerequisite"}
                          else p for p in self.full.obligations)
            allowed = {(p.obligation_id, a.id) for p in plans for a in p.assertions}
            declaration = replace(self.full, obligations=plans,
                edges=tuple(e for e in self.full.edges if (e.obligation_id, e.assertion_id) in allowed))
            result = compile_case(self.v1, declaration)
            self.assertIn("missing_requirement_class", codes(result))
            self.assertIn("missing_required_cell", codes(result))
            self.assertIsNone(result.registry)

    def test_applicability_cannot_omit_clause_lane_or_na_reason(self):
        scope = synthetic_scope(self.v1, self.full)
        target = next(p for p in scope.applicability if len(p.required_cells) > 4)
        incomplete = replace(target, required_cells=target.required_cells[1:])
        changed = replace(scope, applicability=tuple(incomplete if p == target else p for p in scope.applicability))
        self.assertIn("incomplete_applicability", codes(compile_case(self.v1, self.full, changed)))
        missing = replace(scope, applicability=scope.applicability[1:])
        self.assertIn("missing_applicability", codes(compile_case(self.v1, self.full, missing)))
        invalid = replace(target, not_applicable=(replace(target.not_applicable[0], reason=""),)+target.not_applicable[1:])
        changed = replace(scope, applicability=tuple(invalid if p == target else p for p in scope.applicability))
        with self.assertRaises(c.CompilerError):
            compile_case(self.v1, self.full, changed)

    def test_scope_cannot_drop_rule_or_gap_disposition(self):
        scope = synthetic_scope(self.v1, self.full)
        missing_rule = replace(scope, qualification_rule_ids=scope.qualification_rule_ids[1:])
        self.assertIn("missing_qualification_rule", codes(compile_case(self.v1, self.full, missing_rule)))
        missing_gap = replace(scope, planning_dispositions=scope.planning_dispositions[1:])
        self.assertIn("missing_gap_disposition", codes(compile_case(self.v1, self.full, missing_gap)))

    def test_independent_lanes_cannot_be_converted_to_public(self):
        product = replace(self.full.suites[0], execution_purpose="public_release")
        logical = {row.id: row for row in self.v1.logical_gates}
        purposes = tuple(c.PurposeAssignment(key, logical[key].original_purpose, "public_release", sha("review"))
                         for key in self.full.gates[0].logical_gate_ids if logical[key].original_purpose is not None)
        purposes += tuple(p for p in self.full.purposes if logical[p.logical_gate_id].role == "prerequisite")
        declaration = replace(self.full, suites=(product, self.full.suites[1]), purposes=purposes)
        result = compile_case(self.v1, declaration, synthetic_scope(self.v1, declaration))
        targets = {b.target for b in result.blockers if b.code == "independent_lane_purpose"}
        self.assertIn("M1-GATE-HTTP", targets)
        self.assertIn("M4-MIGRATION.migration", targets)
        self.assertIn("M3-WORKER-RECOVERY.process-restart", targets)
        self.assertIn("M4-RELEASE-HANDOFF.release-install", targets)
        self.assertIsNone(result.registry)

    def test_admission_requires_exact_declared_product_evaluator_lineage(self):
        prerequisite = self.full.gates[1]
        missing = replace(prerequisite, qualification_targets=())
        result = compile_case(self.v1, replace(self.full, gates=(self.full.gates[0], missing)))
        self.assertIn("missing_admission_lineage", codes(result))
        wrong = replace(prerequisite.qualification_targets[0], evaluator_sha256=sha("other-evaluator"))
        changed = replace(prerequisite, qualification_targets=(wrong,))
        result = compile_case(self.v1, replace(self.full, gates=(self.full.gates[0], changed)))
        self.assertIn("admission_lineage_mismatch", codes(result))
        self.assertIsNone(result.registry)

    def test_arbitrary_old_suite_hash_and_review_digest_are_insufficient(self):
        old = replace(self.full.suites[0], source_contract_sha256=sha("invented-contract"),
                      compatibility_review_sha256=sha("invented-review"))
        declaration = replace(self.full, suites=(old, self.full.suites[1]))
        transition = c.SuiteCompatibility(old.id, old.definition_sha256, old.source_contract_sha256,
            self.v1.product_sha256, ("shared:compatibility:0",), "Synthetic conversion")
        scope = replace(synthetic_scope(self.v1, declaration), suite_compatibility=(transition,))
        self.assertIn("suite_contract_mapping", codes(compile_case(self.v1, declaration, scope)))

    def test_compatibility_authority_and_predecessors_are_scope_bound(self):
        first = self.full.compatibility[0]
        changed = replace(first, successor_obligation_ids=("M2-REFRESH:clause:0",))
        declaration = replace(self.full, compatibility=(changed,)+self.full.compatibility[1:])
        self.assertIn("unapproved_compatibility_mapping", codes(compile_case(self.v1, declaration)))

    def test_bounded_and_immutable_input_fail_closed(self):
        oversized = replace(self.full.suites[0], ordered_case_ids=tuple("case-"+str(i) for i in range(513)))
        with self.assertRaises(c.CompilerError):
            compile_case(self.v1, replace(self.full, suites=(oversized, self.full.suites[1])))
        for declaration in (replace(self.full, edges=list(self.full.edges)),
                            replace(self.full, obligations=(object(),)),
                            replace(self.full, suites=(replace(self.full.suites[0], capabilities=(["bad"],)),))):
            with self.subTest(declaration=type(declaration)), self.assertRaises(c.CompilerError):
                compile_case(self.v1, declaration)

    def test_immutable_records_and_wrong_subject_contract(self):
        with self.assertRaises(FrozenInstanceError):
            self.v1.product_version = 2
        with self.assertRaises(c.CompilerError):
            c.compile_design(self.v1, self.full, replace(subject(self.v1), requirements_sha256=sha("other")))


if __name__ == "__main__":
    unittest.main()
