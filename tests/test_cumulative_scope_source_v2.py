"""Prospective original factories, not candidate execution or semantic approval."""
from dataclasses import asdict, replace
from pathlib import Path
import tempfile
import unittest

from gossip_harness import cumulative_scope_source_v1 as legacy
from gossip_harness import cumulative_scope_source_v2 as source
from gossip_harness import candidate_storage_product_profile_v1 as profiles
from gossip_harness import candidate_storage_product_execution_v1 as execution
from gossip_harness import candidate_storage_review_authority_v1 as review
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness import project_acceptance_compiler_v1 as compiler
from tests.test_project_acceptance_compiler_v1 import synthetic_declaration
from tests.test_cumulative_scope_source_v1 import cli_registration, http_registration, product_registration
from tests.test_candidate_storage_product_execution_v1 import enroll, SQLITE_LAYOUT

ROOT = Path(__file__).resolve().parents[1]
COHORT = tuple(arm + '.' + block for block in compiler.BLOCKS for arm in compiler.ARMS)


def storage_registration(family='b02', case_id='intake-json-missing', *, purpose='public_release'):
    """Identity-only prospective proposal. No fabricated approval capability."""
    value = profiles.profile_for(family, case_id, purpose)
    binding = execution.StorageBinding('a' * 64, execution.TARGET_CONTRACT, 'M4', purpose,
        family, case_id, 'b' * 64, execution.digest(value.record()), value.sha256,
        'c' * 64, 'd' * 64, execution.digest(execution.evaluator_sources()), 'e' * 64,
        'f' * 64, '1' * 64, '2' * 64)
    subject = registry.Subject('cohort', COHORT[0], 'M4', '3' * 64, execution.TARGET_CONTRACT, binding.source_sha256)
    gate = execution.gate_for(subject, binding, gate_id='storage-' + source.sha((family + case_id + purpose).encode())[:24])
    return execution.StorageRegistration(binding, 'a' * 40, 'b' * 40, 'fresh-1', gate, COHORT)


class CumulativeScopeSourceV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = source.load_catalog(ROOT)
        cls.component = source.storage_slice(storage_registration())

    def test_positive_storage_original_selector_to_full_declaration_path(self):
        declaration = source.assemble_declaration(self.catalog, synthetic_declaration(self.catalog.inventory).cohort,
            (self.component,), review_sha256='d' * 64, capacity_profile=registry.HISTORY_CAPACITY_PROFILE)
        self.assertEqual(declaration.capacity_profile, registry.HISTORY_CAPACITY_PROFILE)
        self.assertEqual(len(declaration.obligations), 312)
        self.assertEqual(len(declaration.cohort.trajectories), 6)
        self.assertEqual(declaration.cohort.milestones, ('M1', 'M2', 'M3', 'M4'))
        self.assertEqual(len(declaration.gates), 1)
        self.assertEqual(declaration.suites[0].capabilities, ('intake', 'jobs', 'durability'))
        self.assertTrue(declaration.edges)
        self.assertEqual({edge.assertion_selector for edge in declaration.edges},
                         {row.observation_pointer for row in self.component.assertions})
        census = source.coverage_census(self.catalog, declaration)
        self.assertEqual((census['product_requirements'], census['prerequisites'], census['source_units'],
                          census['authority_rules'], census['gap_notes']), (123, 3, 312, 22, 188))
        self.assertEqual(census['eligible_cells'], 12975)
        self.assertGreater(census['unresolved_cells'], 0)
        self.assertFalse(census['semantic_approval'])
        self.assertEqual(census['product_observations'], 0)
        scope = source.scope_review_input(self.catalog, declaration)
        self.assertTrue(all(not row.not_applicable for row in scope.applicability))
        self.assertEqual(len(scope.planning_dispositions), 188)

    def test_original_factories_roundtrip_without_new_meaning(self):
        pairs = [(source.cli_slice(cli_registration()), legacy.cli_slice(cli_registration())),
            (source.http_slice(*http_registration()), legacy.http_slice(*http_registration())),
            (source.product_process_slice(*product_registration()), legacy.product_process_slice(*product_registration()))]
        for current, old in pairs:
            self.assertEqual(asdict(current), asdict(old))
            source.verify_slice(current)
            with self.assertRaises(source.ScopeSourceError):
                source.verify_slice(old)

    def test_b01_declares_whole_history_and_guard_but_invents_no_source_edges(self):
        component = source.storage_slice(storage_registration('b01', 'rollback'))
        source.verify_slice(component)
        self.assertTrue(component.selectors)
        self.assertFalse(component.assertions)
        self.assertTrue(component.gate.ordered_case_ids[-1].endswith(':mechanics'))
        suite, gate, edges = component.compiler_records(suite_id='b01', physical_slot='b01-slot')
        self.assertEqual(suite.ordered_case_ids, component.gate.ordered_case_ids)
        self.assertFalse(gate.logical_gate_ids)
        self.assertFalse(edges)

    def test_storage_has_no_accidental_http_or_process_death_capability(self):
        suite, _, _ = self.component.compiler_records(suite_id='b02', physical_slot='b02-slot')
        self.assertEqual(suite.capabilities, source.STORAGE_LANES)
        lanes = {row.id: row.lane for row in self.catalog.inventory.logical_gates}
        self.assertTrue(all(lanes[gate] in source.STORAGE_LANES for row in self.component.assertions
                            for gate in row.assertion.logical_gate_ids))

    def test_wrong_kind_and_exact_error_unit_pruning(self):
        for case in profiles.b02.POLICY_LIMITED_CASE_IDS:
            item = source.storage_slice(storage_registration(case_id=case))
            self.assertFalse(any(row.obligation_id == 'm1:M1-I28' for row in item.assertions))
            self.assertTrue(any(row.disposition == 'unspecified' for row in item.selectors))
        for case, unit in (('intake-json-missing', 'm1:M1-I28'), ('intake-json-syntax', 'm1:M1-I27')):
            item = source.storage_slice(storage_registration(case_id=case))
            rows = [row for row in item.assertions if row.obligation_id == unit]
            self.assertTrue(rows)
            self.assertEqual({row.case_id for row in rows}, {'after.result.0'})

    def test_adapter_bind_mutate_results_and_signature_state_get_no_semantic_credit(self):
        for case, excluded in (('fresh-value-create_job', {'after.result.1'}),
                               ('input-manifest-snapshot', {'after.result.0', 'after.result.2'})):
            item = source.storage_slice(storage_registration(case_id=case))
            self.assertTrue(excluded <= {row.case_id for row in item.selectors})
            self.assertFalse(excluded & {row.case_id for row in item.assertions})
        item = source.storage_slice(storage_registration(case_id='signatures-offline-hook'))
        self.assertEqual({row.case_id for row in item.assertions}, {'after.result.0'})
        self.assertEqual({row.obligation_id for row in item.assertions}, {'m1:M1-A08', 'm1:M1-T02'})

    def test_selector_edges_can_never_select_unspecified_auxiliary_or_mechanics(self):
        for case in ('intake-json-wrong-kind', 'input-manifest-snapshot'):
            item = source.storage_slice(storage_registration(case_id=case))
            excluded = {(row.case_id, row.observation_pointer) for row in item.selectors
                        if row.disposition != 'normative' or row.evidence_kind != 'semantic'}
            self.assertFalse(excluded & {(row.case_id, row.observation_pointer) for row in item.assertions})
            self.assertTrue(any('auxiliary' in row.case_id for row in item.selectors))

    def test_fresh_purposes_are_new_profiles_not_relabelled_old_bindings(self):
        for purpose in registry.PURPOSES:
            registration = storage_registration(purpose=purpose)
            item = source.storage_slice(registration)
            self.assertEqual(item.original_definition_purpose, 'harness_qualification')
            self.assertEqual(item.gate.binding.purpose, purpose)
            source.verify_slice(item)
        stale = replace(storage_registration().binding, purpose='independent_acceptance')
        with self.assertRaises(ValueError):
            source.storage_slice(replace(storage_registration(), binding=stale))

    def test_closed_factory_rejects_substituted_assertion_and_selector(self):
        claim = self.component.assertions[0]
        for item in (replace(self.component, assertions=(replace(claim, obligation_id='m1:M1-I27'),)),
                     replace(self.component, selectors=self.component.selectors[:-1]),
                     replace(self.component, original_definition_purpose='public_release'),
                     replace(self.component, evaluator_sources=())):
            with self.subTest(item=item.family), self.assertRaises(source.ScopeSourceError):
                source.verify_slice(item)

    def test_mismatched_definition_and_evaluator_reject_before_slice_creation(self):
        registration = storage_registration()
        for key in ('definition_sha256', 'profile_sha256'):
            with self.subTest(key=key):
                binding = replace(registration.binding, **{key: '0' * 64})
                with self.assertRaisesRegex(execution.ExecutionError,
                                            'Stored binding differs from exact closed profile'):
                    execution.gate_for(registration.gate.binding.subject, binding,
                                       gate_id=registration.gate.gate_id)
                with self.assertRaisesRegex(execution.ExecutionError,
                                            'Stored binding differs from exact closed profile'):
                    source.storage_slice(replace(registration, binding=binding))
        binding = replace(registration.binding, evaluator_sha256='0' * 64)
        gate = execution.gate_for(registration.gate.binding.subject, binding,
                                   gate_id=registration.gate.gate_id)
        with self.assertRaisesRegex(source.ScopeSourceError,
                                    'Storage definition/profile/evaluator/ordered roster differs'):
            source.storage_slice(replace(registration, binding=binding, gate=gate))

    def test_actual_enrolled_layout_binding_composes_without_claiming_real_review(self):
        value = profiles.profile_for('b02', 'intake-json-missing', 'public_release')
        files = {'library/__init__.py': b'# Never imported or run\n'}
        plan = review.LayoutPlan('b02', admission.source_sha256(files), execution.b02.source_sha256(files),
            'a' * 40, 'b' * 40, value.case_id, value.sha256, SQLITE_LAYOUT, ('catalog.sqlite',), 'c' * 64)
        with tempfile.TemporaryDirectory() as path:
            authority, journal, head = enroll(Path(path).resolve(), plan)
            try:
                binding = execution.binding_for(files, value, execution.StoragePolicy(),
                    {'fixture': 'identity only, no Engine execution'}, plan, review_authority=authority)
                old = storage_registration()
                subject = replace(old.gate.binding.subject, source_sha256=binding.source_sha256)
                registration = replace(old, binding=binding,
                    gate=execution.gate_for(subject, binding, gate_id=old.gate.gate_id))
                item = source.storage_slice(registration)
                self.assertEqual(item.profile_sha256, value.sha256)
                self.assertEqual(item.gate.binding.subject.source_sha256, admission.source_sha256(files))
                self.assertEqual(binding.review_sha256, authority.enrollment.report_sha256)
                source.verify_slice(item)
            finally:
                journal.close(); head.close()


class CumulativeStorageCatalogV2Tests(unittest.TestCase):
    def test_all_249_histories_have_actual_factories_and_keep_42_partial_units(self):
        components = []
        files = {'library/__init__.py': b'# prospective source only, not executed\n'}
        with tempfile.TemporaryDirectory() as path:
            root = Path(path).resolve()
            for family, cases in (('b01', profiles.b01.CASE_IDS), ('b02', profiles.b02.CASE_IDS)):
                for case in cases:
                    value = profiles.profile_for(family, case, 'public_release')
                    native = execution.b01 if family == 'b01' else execution.b02
                    plan = review.LayoutPlan(family, admission.source_sha256(files), native.source_sha256(files),
                        'a' * 40, 'b' * 40, case, value.sha256, SQLITE_LAYOUT, ('catalog.sqlite',), 'c' * 64,
                        'forced_schedule_unavailable' if family == 'b02' and case in execution.b02.FORCED_CASE_IDS else 'ordinary_public_operations')
                    original_root = root / str(len(components)); original_root.mkdir()
                    authority, journal, head = enroll(original_root, plan)
                    try:
                        binding = execution.binding_for(files, value, execution.StoragePolicy(),
                            {'fixture': 'prospective catalog binding, no Engine'}, plan, review_authority=authority)
                        subject = registry.Subject('cohort', COHORT[0], 'M4', '3' * 64,
                            execution.TARGET_CONTRACT, binding.source_sha256)
                        gate = execution.gate_for(subject, binding, gate_id='catalog-' + str(len(components)))
                        registration = execution.StorageRegistration(binding, 'a' * 40, 'b' * 40, 'fresh-1', gate, COHORT)
                        components.append(source.storage_slice(registration))
                    finally:
                        journal.close(); head.close()
        self.assertEqual(len(components), 249)
        self.assertEqual(sum(row.family == 'storage-b01' for row in components), 8)
        self.assertEqual(sum(row.family == 'storage-b02' for row in components), 241)
        self.assertEqual(len({row.gate.gate_id for row in components}), 249)
        self.assertEqual(len({claim.obligation_id for row in components for claim in row.assertions}), 42)
        self.assertEqual(sum(len(row.selectors) - 1 for row in components), 6307)
        self.assertEqual(sum(item.disposition == 'unspecified' for row in components for item in row.selectors), 3)
        self.assertEqual(max(len(row.gate.ordered_case_ids) for row in components), 41)
        self.assertTrue(all(row.source_contract_sha256 == compiler.M1_SHA256 for row in components))
        self.assertTrue(all(row.gate.binding.subject.requirements_sha256 == compiler.PRODUCT_V2_SHA256 for row in components))
