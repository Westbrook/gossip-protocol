"""Prospective mapping/composition controls; no candidate or semantic authority."""
from dataclasses import replace
from pathlib import Path
import tempfile
from typing import Any, cast
import unittest

from gossip_harness import candidate_http_cases_v1 as cases
from gossip_harness import candidate_http_execution_v4 as http
from gossip_harness import cumulative_finite_mapping_v1 as finite
from gossip_harness import cumulative_observation_profile_v1 as cumulative
from gossip_harness import cumulative_rehearsal_codec_v1 as codec
from gossip_harness import cumulative_scope_source_v1 as scope
from gossip_harness import cumulative_scope_source_v3 as latest
from gossip_harness import project_acceptance_compiler_v1 as compiler
from gossip_harness import project_acceptance_registry_v1 as registry

ROOT = Path(__file__).resolve().parents[1]
COHORT = tuple(arm + '.' + block for block in compiler.BLOCKS for arm in compiler.ARMS)


def profile_for(case: Any, marker: str | None = finite.HTTP_MAPPING,
                purpose: str = 'independent_acceptance') -> http.HttpProductProfile:
    return http.HttpProductProfile(case, cumulative.ORIGINAL_DEFINITION_PURPOSE,
        cumulative.http_profile(case.row_id, purpose=purpose), marker)


def registered(profile: http.HttpProductProfile) -> tuple[http.HttpRegistration, http.HttpPolicy]:
    policy = http.HttpPolicy('sha256:' + 'a' * 64)
    binding = http.binding_for({'library/__init__.py': b'# never executed\n'},
        http.recipe_from_case(profile.case), policy, {'kind': 'inert-prospective-fixture'},
        requirements_sha256=compiler.PRODUCT_V2_SHA256, profile=profile,
        purpose='independent_acceptance')
    subject = registry.Subject('cohort', COHORT[0], 'M4', 'e' * 64,
        compiler.PRODUCT_V2_SHA256, binding.source_sha256)
    observed = http.observation_registration_for(binding, profile, policy, subject=subject,
        gate_id='finite-http-' + scope.sha(profile.case.row_id.encode())[:16],
        commit_oid='a' * 40, tree_oid='b' * 40, repetition_id='fixture-1',
        cohort_trajectory_ids=COHORT)
    return http.HttpRegistration(binding, 'a' * 40, 'b' * 40, 'fixture-1', observed), policy


def cohort() -> compiler.CohortDesign:
    trajectories = []
    for block in compiler.BLOCKS:
        for arm in compiler.ARMS:
            builders = ('B01', 'B05', 'B09', 'B13') if arm == 'S4-G' else tuple(
                'B' + str(i).zfill(2) for i in range(1, 17))
            trajectories.append(compiler.Trajectory(arm + '.' + block, arm, block,
                builders + ('R1', 'R2', 'R3', 'R4'),
                'durable_central_scheduler' if arm == 'O16-G' else 'peer_local',
                '1' * 64, '2' * 64, compiler.FAULTS if block == 'compound_recovery' else ()))
    return compiler.CohortDesign('cohort', tuple(trajectories), ('M1', 'M2', 'M3', 'M4'),
        '3' * 64, '3' * 64, '3' * 64, '3' * 64, '3' * 64, '3' * 64, '3' * 64, '3' * 64)


class CumulativeFiniteHttpMappingV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cases = {row.row_id: row for row in cases.definitions()}
        cls.catalog = scope.load_catalog(ROOT)

    def test_exact_allocations_resolve_real_original_required_facets(self):
        rows = finite.allocations('http')
        self.assertEqual(len(rows), 116)
        self.assertEqual(len({row.unit_id for row in rows}), 13)
        actual = []
        for history in sorted({row.history_id for row in rows}):
            profile = profile_for(self.cases[history])
            for resolved in finite.resolve_http(profile):
                row = resolved.allocation
                original = profile.case.steps[resolved.step_index]
                self.assertIsNotNone(original.expectation)
                self.assertEqual(row.history_id, history)
                actual.append(row)
            selectors, assertions = scope._http_declarations(profile, mapping_profile=finite.HTTP_MAPPING)
            added = [row for row in assertions if row.assertion.id.startswith('readback-')]
            expected = [row for row in rows if row.history_id == history]
            self.assertEqual(len(added), len(expected))
            self.assertEqual({(r.obligation_id, r.assertion.kind, r.assertion.logical_gate_ids[0]) for r in added},
                {(r.unit_id, r.kind, r.logical_gate_id) for r in expected})
            self.assertEqual(len(selectors), len(profile.diagnostic_case_ids) + 1)
        self.assertCountEqual(actual, rows)

    def test_kind_assignments_do_not_expand_cartesian_products(self):
        rows = finite.allocations('http')
        def kinds(unit, history):
            return {r.kind for r in rows if r.unit_id == unit and r.history_id == history}
        self.assertEqual(kinds('m1:V0-QUERY-03', 'HTTP-DOCUMENT-ROUTES/offset-beyond'), {'positive'})
        self.assertEqual(kinds('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/6-offset'), {'negative'})
        self.assertEqual(kinds('m1:V0-QUERY-03', 'HTTP-QUERY-VALUES/4-limit'), {'boundary'})
        self.assertEqual(kinds('m1:V0-EXPORT-01', 'HTTP-DOCUMENT-ROUTES/export-duplicate'), {'negative'})
        for row in rows:
            if row.unit_id.startswith('shared:'):
                self.assertNotIn(row.logical_gate_id.split('.')[0], ('M2-QUERY', 'M3-BACKUP-RESTORE', 'M4-COMPATIBILITY'))

    def test_fresh_independent_purpose_is_mandatory_and_legacy_kept(self):
        case = self.cases['HTTP-DOCUMENT-ROUTES/show-0']
        for purpose in ('public_release', 'repeatability'):
            with self.assertRaises(ValueError):
                profile_for(case, purpose=purpose)
        for marker in (None, scope.MAP_A_MAPPING):
            old = profile_for(case, marker, 'public_release')
            self.assertEqual(old.cumulative_profile.purpose, 'public_release')
        with self.assertRaises(ValueError):
            profile_for(self.cases['HTTP-EMPTY-HEALTH/health'])
        current = profile_for(case)
        with self.assertRaises(ValueError):
            current.check_current(purpose='public_release')
        self.assertEqual(finite.HTTP_PURPOSE_CONVERSIONS, (
            ('M1-GATE-PUBLIC', 'public_development', 'independent_acceptance'),
            ('M1-GATE-V0-SUPPLEMENT', 'public_contract_regression', 'independent_acceptance')))

    def test_exact_original_history_and_selector_cannot_be_substituted(self):
        original = self.cases['HTTP-DOCUMENT-ROUTES/show-0']
        profile = profile_for(original)
        index = finite.resolve_http(profile)[0].step_index
        step = original.steps[index]
        steps = list(original.steps)
        steps[index] = replace(step, step_id=step.step_id + '-caller')
        with self.assertRaises(ValueError):
            profile_for(replace(original, steps=tuple(steps)))
        with self.assertRaises(ValueError):
            finite.resolve_http(profile_for(original, scope.MAP_A_MAPPING))

    def test_typed_capsule_final_and_scope_roundtrip_retains_actual_new_gate(self):
        from gossip_harness import cumulative_final_acceptance_v1 as final
        profile = profile_for(self.cases['HTTP-DOCUMENT-ROUTES/show-0'])
        registration, policy = registered(profile)
        reconstructed = codec.unpack(codec.pack((registration, profile, policy)))
        self.assertEqual(reconstructed, (registration, profile, policy))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = codec.encoded(codec.pack(reconstructed))
            path = root / 'typed.json'
            path.write_bytes(raw)
            decoded = codec.read({'path': str(path.resolve()), 'sha256': scope.sha(raw)})
            spec = final.ObservationSpec('http', root/'raw', root/'delta', root/'cleanup',
                cast(Any, None), cast(Any, None), decoded[0], decoded[2],
                recipe=http.recipe_from_case(decoded[1].case), profile=decoded[1])
            self.assertEqual(spec.observation_registration(), registration.observation)
            component = latest.http_slice(*decoded)
            latest.verify_slice(component)
            suite, gate, _ = component.compiler_records(suite_id='finite-suite', physical_slot='finite-slot')
            self.assertEqual(suite.capabilities, finite.HTTP_CAPABILITIES)
            self.assertIn('M1-GATE-PUBLIC', gate.logical_gate_ids)
            self.assertIn('M1-GATE-HTTP', gate.logical_gate_ids)
            wrong = profile_for(profile.case, scope.MAP_A_MAPPING)
            with self.assertRaises(ValueError):
                replace(spec, profile=wrong).observation_registration()
            bad = codec.pack(reconstructed)
            bad['input']['value'][1]['value']['mapping_profile'] = 'caller-approval'
            with self.assertRaises(ValueError):
                codec.unpack(bad)

    def test_missing_real_purpose_authority_still_blocks_actual_compiler(self):
        profile = profile_for(self.cases['HTTP-DOCUMENT-ROUTES/query-256'])
        registration, policy = registered(profile)
        component = latest.http_slice(registration, profile, policy)
        declaration = latest.assemble_declaration(self.catalog, cohort(), (component,), review_sha256='4' * 64)
        design = compiler.compile_design(self.catalog.inventory, declaration,
            registration.observation.gate.binding.subject)
        self.assertFalse(design.declaration_complete)
        missing = {row.target for row in design.blockers if row.code == 'missing_purpose_mapping'}
        self.assertTrue({'M1-GATE-PUBLIC', 'M1-GATE-V0-SUPPLEMENT'} <= missing)
        self.assertEqual(len(declaration.obligations), 312)
        self.assertEqual(declaration.purposes, ())
        self.assertNotIn('prospective_authority_sha256', profile.mapping_record())
