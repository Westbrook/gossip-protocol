"""Source declarations only; no semantic approval or candidate execution."""
from dataclasses import replace
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from gossip_harness import candidate_client_execution_v5 as cli
from gossip_harness import candidate_cli_cases_v1 as cli_cases
from gossip_harness import candidate_http_execution_v4 as http
from gossip_harness import candidate_http_cases_v1 as http_cases
from gossip_harness import cumulative_observation_profile_v1 as profiles
from gossip_harness import cumulative_scope_source_v1 as source
from gossip_harness import project_acceptance_compiler_v1 as compiler
from gossip_harness import project_acceptance_registry_v1 as registry
from tests.test_project_acceptance_compiler_v1 import synthetic_declaration

ROOT = Path(__file__).resolve().parents[1]
COHORT = tuple(arm + '.' + block for block in compiler.BLOCKS for arm in compiler.ARMS)
FILES = {'library/__init__.py': b'# prospective source only, never imported or run\n'}


def cli_registration(case_id='cli-empty', *, purpose='public_release'):
    profile = profiles.cli_profile(case_id, purpose=purpose)
    binding = cli.binding_for(FILES, case_id, cli.ClientPolicy('sha256:' + 'a' * 64),
        {'kind': 'host-only-registration-fixture'}, requirements_sha256=compiler.PRODUCT_V2_SHA256,
        milestone='M4', purpose=purpose, cumulative_profile=profile)
    subject = registry.Subject('cohort', COHORT[0], 'M4', 'e' * 64,
                               compiler.PRODUCT_V2_SHA256, binding.source_sha256)
    return cli.ClientRegistration(binding, 'a' * 40, 'b' * 40, 'fresh-fixture-1',
        cli.gate_for(subject, binding, gate_id='cli-' + case_id), COHORT)


def http_registration(case_id='HTTP-EMPTY-HEALTH/health', *, purpose='public_release'):
    case = next(row for row in http_cases.definitions() if row.row_id == case_id)
    profile = http.HttpProductProfile(case, profiles.ORIGINAL_DEFINITION_PURPOSE,
                                     profiles.http_profile(case_id, purpose=purpose))
    policy = http.HttpPolicy('sha256:' + 'a' * 64)
    binding = http.binding_for(FILES, http.recipe_from_case(case), policy,
        {'kind': 'host-only-registration-fixture'}, requirements_sha256=compiler.PRODUCT_V2_SHA256,
        profile=profile, purpose=purpose)
    subject = registry.Subject('cohort', COHORT[0], 'M4', 'e' * 64,
                               compiler.PRODUCT_V2_SHA256, binding.source_sha256)
    observed = http.observation_registration_for(binding, profile, policy, subject=subject,
        gate_id='http-' + source.sha(case_id.encode())[:16], commit_oid='a' * 40,
        tree_oid='b' * 40, repetition_id='fresh-fixture-1', cohort_trajectory_ids=COHORT)
    return http.HttpRegistration(binding, 'a' * 40, 'b' * 40, 'fresh-fixture-1', observed), profile, policy


def product_registration(case_id='process-worker-explicit-enrollment-order', *, purpose='public_release'):
    from gossip_harness import candidate_product_process_core_v1 as core
    from gossip_harness import candidate_product_process_execution_v1 as execution
    profile = execution.HttpProductProfile(core.case_definition(case_id), core.ORIGINAL_DEFINITION_PURPOSE)
    policy = execution.HttpPolicy('sha256:' + 'a' * 64)
    binding = execution.binding_for(FILES, execution.recipe_from_case(profile.case), policy,
        {'kind': 'host-only-registration-fixture'}, requirements_sha256=compiler.PRODUCT_V2_SHA256,
        profile=profile, purpose=purpose)
    subject = registry.Subject('cohort', COHORT[0], 'M4', 'e' * 64,
                               compiler.PRODUCT_V2_SHA256, binding.source_sha256)
    observed = execution.observation_registration_for(binding, profile, policy, subject=subject,
        gate_id=case_id, commit_oid='a' * 40, tree_oid='b' * 40, repetition_id='fresh-fixture-1', cohort_trajectory_ids=COHORT)
    return execution.HttpRegistration(binding, 'a' * 40, 'b' * 40, 'fresh-fixture-1', observed), profile, policy


class CumulativeScopeSourceV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = source.load_catalog(ROOT)
        cls.component = source.cli_slice(cli_registration())

    def test_full_denominator_and_values_are_retained(self):
        catalog = self.catalog
        self.assertEqual((len(catalog.inventory.product_ids), len(catalog.inventory.prerequisite_ids),
            len(catalog.units), len(catalog.inventory.qualification_rules), len(catalog.inventory.planning_notes)),
            (123, 3, 312, 22, 188))
        self.assertEqual(sum(len(unit.eligible_cells) for unit in catalog.units), 12975)
        self.assertEqual({row.obligation.id for row in catalog.units}, {row.id for row in catalog.inventory.obligations})
        for unit in catalog.units:
            self.assertEqual(source.sha(unit.value_json), unit.obligation.value_sha256)
            self.assertTrue(unit.requirement_ids)
        self.assertEqual(len(self.catalog.sha256), 64)

    def test_ownership_review_is_not_applicability_and_interactions_are_separate(self):
        shared = [row for row in self.catalog.units if not row.obligation.requirement_ids]
        self.assertEqual(len(shared), 57)
        for row in shared:
            self.assertTrue(row.ownership_reason)
            self.assertTrue(row.applicability_input)
        with_interactions = [row for row in shared if row.interaction_requirement_ids]
        self.assertTrue(with_interactions)
        for row in with_interactions:
            self.assertTrue(row.interaction_reason)
            eligible = {cell.logical_gate_id for cell in row.eligible_cells}
            self.assertEqual(eligible, {gate.id for gate in self.catalog.inventory.logical_gates
                if gate.role == row.obligation.role and set(row.requirement_ids) & set(gate.requirement_ids)})

    def test_ownership_and_review_edits_are_rejected_even_if_self_consistent(self):
        with tempfile.TemporaryDirectory() as path:
            root = Path(path)
            for name, _ in self.catalog.source_pins:
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / name, root / name)
            record = json.loads((root / source.OWNERSHIP).read_bytes())
            record['records'][0]['ownership_reason'] = 'caller changed reviewed meaning'
            raw = source.encoded(record)
            (root / source.OWNERSHIP).write_bytes(raw)
            review = json.loads((root / source.REVIEW).read_bytes())
            for row in review['reviewed_files']:
                if row['path'] == source.OWNERSHIP:
                    row['sha256'] = source.sha(raw)
            (root / source.REVIEW).write_bytes(source.encoded(review))
            with self.assertRaisesRegex(source.ScopeSourceError, 'Unrecognized source'):
                source.load_catalog(root)

    def test_cli_concrete_selectors_rederive_and_keep_original_purpose(self):
        value = self.component
        source.verify_slice(value)
        self.assertEqual(value.original_definition_purpose, 'harness_qualification')
        self.assertEqual(value.gate.binding.purpose, 'public_release')
        self.assertEqual(value.source_contract_sha256, compiler.M1_SHA256)
        self.assertTrue(value.assertions)
        self.assertTrue(all(row.observation_pointer.startswith('/cells/') for row in value.assertions))
        self.assertTrue(all(row.obligation_id in {'m1:V0-CLI-01', 'm1:V0-CLI-02', 'm1:M1-CLI-01'} for row in value.assertions))
        self.assertTrue(value.remaining_coverage)

    def test_all_cli_diagnostics_stay_present_and_unspecified_has_no_edges(self):
        # One full declaration census, no candidate commands or evaluator runs.
        counts = [0, 0, 0]
        for case in cli_cases.definitions():
            value = source.cli_slice(cli_registration(case['case_id']))
            counts[0] += len(value.selectors)
            counts[1] += sum(row.disposition == 'normative' for row in value.selectors)
            excluded = {row.case_id for row in value.selectors if row.disposition != 'normative'}
            counts[2] += len(excluded)
            self.assertFalse(excluded & {row.case_id for row in value.assertions})
        self.assertEqual(counts, [900, 868, 32])

    def test_replaced_assertion_selector_or_evaluator_is_not_a_source_factory(self):
        original = self.component
        for altered in (replace(original, profile_sha256='f' * 64),
                replace(original, selectors=original.selectors[:-1]),
                replace(original, assertions=(replace(original.assertions[0], obligation_id='m1:V0-HTTP-04'),)),
                replace(original, evaluator_sources=()),
                replace(original, original_definition_purpose='independent_acceptance')):
            with self.subTest(change=altered):
                with self.assertRaises(source.ScopeSourceError):
                    source.verify_slice(altered)

    def test_old_m1_and_changed_original_binding_cannot_be_relabelled(self):
        reg = cli_registration()
        m1 = cli.binding_for(FILES, 'cli-empty', cli.ClientPolicy('sha256:' + 'a' * 64), {},
                            requirements_sha256=compiler.PRODUCT_V2_SHA256)
        subject = replace(reg.gate.binding.subject, milestone='M1', requirements_sha256=compiler.PRODUCT_V2_SHA256)
        old = cli.ClientRegistration(m1, 'a' * 40, 'b' * 40, 'old-1', cli.gate_for(subject, m1, gate_id='old-cli'), COHORT)
        with self.assertRaisesRegex(source.ScopeSourceError, 'Final-M4'):
            source.cli_slice(old)
        body = json.loads(self.component.factory_input_json)
        body['registration']['binding']['purpose'] = 'independent_acceptance'
        with self.assertRaises(ValueError):
            source.verify_slice(replace(self.component, factory_input_json=source.encoded(body).decode()))

    def test_http_health_uses_real_decisive_status_and_retains_compatibility_gap(self):
        value = source.http_slice(*http_registration())
        source.verify_slice(value)
        self.assertTrue(any(row.obligation_id == 'm1:V0-HTTP-04' for row in value.assertions))
        self.assertFalse(any(row.obligation_id.startswith('M4-') for row in value.assertions))
        self.assertEqual(value.selectors[-1].evidence_kind, 'mechanics')
        self.assertFalse(any(row.case_id == value.selectors[-1].case_id for row in value.assertions))
        self.assertTrue(all(row.observation_pointer.startswith('/diagnostics/') for row in value.assertions))

    def test_http_raw_and_mixed_cli_have_no_invented_semantic_credit(self):
        value = source.http_slice(*http_registration('HTTP-PERSIST-LISTENER/root-page-reachable'))
        excluded = {row.case_id for row in value.selectors if row.disposition != 'normative' or row.evidence_kind != 'semantic'}
        self.assertFalse(excluded & {row.case_id for row in value.assertions})
        self.assertTrue(excluded)
        self.assertEqual(value.gate.ordered_case_ids[-1], value.selectors[-1].case_id)

    def test_declaration_preserves_every_missing_obligation_and_exact_gate(self):
        cohort = synthetic_declaration(self.catalog.inventory).cohort  # explicitly structural fixture
        declaration = source.assemble_declaration(self.catalog, cohort, (self.component,), review_sha256='d' * 64)
        self.assertEqual(len(declaration.obligations), 312)
        self.assertEqual(declaration.cohort.milestones, ('M1', 'M2', 'M3', 'M4'))
        self.assertEqual(len(declaration.cohort.trajectories), 6)
        self.assertGreater(sum(not row.assertions for row in declaration.obligations), 300)
        self.assertEqual(declaration.suites[0].ordered_case_ids, self.component.gate.ordered_case_ids)
        self.assertEqual(declaration.gates[0].physical_slot, self.component.gate.gate_id)
        result = compiler.compile_design(self.catalog.inventory, declaration, self.component.gate.binding.subject)
        self.assertIsNone(result.registry)
        self.assertTrue(result.blockers)

    def test_duplicate_physical_gate_and_changed_shared_owner_are_rejected(self):
        cohort = synthetic_declaration(self.catalog.inventory).cohort
        with self.assertRaisesRegex(source.ScopeSourceError, 'Duplicate physical'):
            source.assemble_declaration(self.catalog, cohort, (self.component, self.component), review_sha256='d' * 64)
        shared = next(row for row in self.catalog.units if not row.obligation.requirement_ids)
        extra = compiler.ObligationPlan(shared.obligation.id, ('M4-RELEASE-HANDOFF',), (), 'd' * 64, 'fixture')
        if extra.requirement_ids == shared.requirement_ids:
            extra = replace(extra, requirement_ids=('V0-CLI-01',))
        with self.assertRaisesRegex(source.ScopeSourceError, 'changed source/reviewed owners'):
            source.assemble_declaration(self.catalog, cohort, (), review_sha256='d' * 64, additional_plans=(extra,))

    def test_http_error_with_unspecified_status_is_classified_without_crashing(self):
        names = [row.row_id for row in http_cases.definitions() if any(
            step.expectation is not None and step.expectation.semantic is not None
            and step.expectation.semantic.shape == 'error' and step.expectation.semantic.status is None
            for step in row.steps)]
        self.assertTrue(names)
        for name in names:
            value = source.http_slice(*http_registration(name))
            errors = [row for row in value.assertions if row.obligation_id == 'm1:V0-HTTP-02']
            self.assertTrue(errors)
            self.assertTrue(all(row.assertion.kind == 'negative' for row in errors))

    def test_health_and_setup_success_cannot_cover_error_semantics(self):
        value = source.http_slice(*http_registration())
        self.assertFalse(any(row.obligation_id == 'm1:V0-HTTP-02' for row in value.assertions))

    def test_real_product_process_selectors_bind_justified_clauses_and_keep_gaps(self):
        value = source.product_process_slice(*product_registration())
        source.verify_slice(value)
        targets = {row.obligation_id for row in value.assertions}
        self.assertIn('M3-WORKER-RECOVERY:clause:0', targets)
        self.assertIn('M3-WORKER-RECOVERY:clause:1', targets)
        self.assertNotIn('M3-WORKER-RECOVERY:clause:3', targets)
        self.assertNotIn('M3-WORKER-RECOVERY:clause:4', targets)
        excluded = {(row.case_id, row.observation_pointer) for row in value.selectors
                    if row.disposition != 'normative' or row.evidence_kind != 'semantic'}
        self.assertFalse(excluded & {(row.case_id, row.observation_pointer) for row in value.assertions})
        self.assertTrue(all('Selected facet:' in row.rationale for row in value.assertions))
        self.assertTrue(all(not any('durability' in key or 'browser' in key for key in row.assertion.logical_gate_ids)
                            for row in value.assertions))
        self.assertEqual(value.original_definition_purpose, 'public_product_definition')
        suite, _, _ = value.compiler_records(suite_id='product-suite', physical_slot='whole-history')
        self.assertIn('http', suite.capabilities)
        self.assertIn('cli', suite.capabilities)

    def test_all_product_process_selectors_preserve_unspecified_and_no_crash_credit(self):
        from gossip_harness import candidate_product_process_core_v1 as core
        total = unspecified = ordered = 0
        for case in core.definitions():
            value = source.product_process_slice(*product_registration(case.row_id))
            total += len(value.selectors)
            unspecified += sum(row.disposition == 'unspecified' for row in value.selectors)
            ordered += len(value.gate.ordered_case_ids)
            self.assertFalse(any(row.obligation_id in ('M4-MIGRATION:clause:0', 'M3-WORKER-RECOVERY:clause:4')
                                 for row in value.assertions))
        self.assertEqual((total, unspecified, ordered), (934, 33, 158))

    def test_concrete_scope_draft_leaves_all_other_cells_unresolved(self):
        component = source.product_process_slice(*product_registration())
        declaration = source.assemble_declaration(self.catalog, synthetic_declaration(self.catalog.inventory).cohort,
            (self.component, component), review_sha256='d' * 64)
        scope = source.scope_review_input(self.catalog, declaration)
        self.assertEqual(len(scope.applicability), 312)
        self.assertEqual(len(scope.planning_dispositions), 188)
        self.assertEqual(len(scope.qualification_rule_ids), 22)
        self.assertFalse(any(row.not_applicable for row in scope.applicability))
        census = source.coverage_census(self.catalog, declaration)
        self.assertGreater(census['declared_cells'], 0)
        self.assertGreater(census['unresolved_cells'], 0)
        self.assertEqual(census['declared_cells'] + census['unresolved_cells'], 12975)
        self.assertFalse(census['semantic_approval'])
        result = compiler.compile_design(self.catalog.inventory, declaration, self.component.gate.binding.subject,
                                         scope_plan=scope, expected_scope_sha256=scope.sha256)
        self.assertIsNone(result.registry)

    def test_backup_facets_do_not_borrow_one_anothers_meaning(self):
        reg, profile, policy = product_registration('process-backup-restore-fences-edits-and-removed-id')
        value = source.product_process_slice(reg, profile, policy)
        catalog = __import__('gossip_harness.candidate_product_process_observation_v1', fromlist=['selector_catalog']).selector_catalog(profile.case.row_id, purpose='public_release')
        by_pointer = {row['observation_pointer']: row for row in catalog['selectors']}
        metadata = [row for row in value.assertions if row.obligation_id == 'M3-BACKUP-RESTORE:clause:1']
        self.assertTrue(metadata)
        for row in metadata:
            facet = by_pointer[row.observation_pointer]['facet']
            if facet == 'json_field:name':
                self.assertIn('basename only', row.rationale)
                self.assertNotIn('payload digest', row.rationale)
            if facet == 'integer_range:/bytes':
                self.assertIn('byte metadata', row.rationale)
                self.assertFalse(any(other.obligation_id == 'M3-BACKUP-RESTORE:clause:0' and other.observation_pointer == row.observation_pointer for other in value.assertions))
        original = json.loads(profile.case.original_json)
        indices = [i for i, action in enumerate(original['input']['actions']) if action.get('path') == '/api/maintenance/restore']
        self.assertTrue(indices)
        for index in indices:
            self.assertTrue(any(row.obligation_id == 'M3-BACKUP-RESTORE:clause:5' and row.observation_pointer.startswith('/diagnostics/' + str(index) + '/') for row in value.assertions))

    def test_path_setup_and_census_are_not_path_evidence(self):
        reg, profile, policy = http_registration('HTTP-ROOT-PATH/directory-traversal')
        value = source.http_slice(reg, profile, policy)
        claims = [row for row in value.assertions if row.obligation_id == 'm1:M1-I31']
        self.assertTrue(claims)
        for row in claims:
            index = int(row.observation_pointer.split('/')[2])
            request = profile.case.steps[index].request
            self.assertEqual(request.method, 'POST')
            if request.target == '/api/jobs':
                self.assertEqual(json.loads(request.body)['job_id'], 'subject')
            else:
                self.assertEqual(request.target, '/api/jobs/subject/prepare')

    def test_generation_restart_and_export_error_have_exact_clause_routing(self):
        reg, profile, policy = product_registration('process-reindex-resume-and-generation-restart')
        value = source.product_process_slice(reg, profile, policy)
        claims = [row for row in value.assertions if row.obligation_id == 'M3-REINDEX:clause:1']
        self.assertTrue(claims)
        for row in claims:
            index = int(row.observation_pointer.split('/')[2])
            expected = profile.case.steps[index].expectation.record()['json']
            self.assertIn(expected['target_generation'], (4, 5))
            self.assertEqual(expected['processed'], 1)
        reg, profile, policy = product_registration('process-export-canonical-selection-and-byte-limit')
        value = source.product_process_slice(reg, profile, policy)
        for row in value.assertions:
            index = int(row.observation_pointer.split('/')[2])
            expected = profile.case.steps[index].expectation.record()
            if expected.get('json') == {'error': 'not_found'}:
                self.assertNotIn(row.obligation_id, ('M3-EXPORT:clause:1', 'M3-EXPORT:clause:2'))
            if expected.get('json') == {'error': 'too_large'}:
                self.assertNotEqual(row.obligation_id, 'M3-EXPORT:clause:2')

    def test_deferred_source_admission_is_not_key_validity(self):
        names = [row.row_id for row in http_cases.definitions() if row.row_id.startswith('HTTP-ROOT-PATH/entries-source-')]
        self.assertTrue(names)
        for name in names:
            reg, profile, policy = http_registration(name)
            value = source.http_slice(reg, profile, policy)
            for claim in value.assertions:
                if claim.obligation_id != 'm1:M1-I31':
                    continue
                step = profile.case.steps[int(claim.observation_pointer.split('/')[2])]
                self.assertTrue(step.request.target == '/api/jobs/subject/prepare' or step.expectation.semantic.shape == 'error')
