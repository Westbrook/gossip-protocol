"""Finite source/compiler mapping controls; no candidate or semantic approval."""
from dataclasses import replace
from pathlib import Path
import unittest

from gossip_harness import cumulative_scope_source_v1 as source
from gossip_harness import cumulative_scope_source_v3 as source3
from gossip_harness import project_acceptance_compiler_v1 as compiler
from tests.test_cumulative_scope_source_v1 import http_registration
from tests.test_project_acceptance_compiler_v1 import synthetic_declaration

ROOT = Path(__file__).resolve().parents[1]
CASE = 'HTTP-BODY-WIRE/raw-bytes-65536'


class CumulativeHttpBoundaryMappingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registration, cls.profile, cls.policy = http_registration(CASE)
        cls.component = source.http_slice(cls.registration, cls.profile, cls.policy)
        cls.catalog = source.load_catalog(ROOT)

    def test_only_actual_exact_bound_subject_has_finite_boundary_edge(self):
        component = self.component
        self.assertEqual(len(component.assertions), 1)
        edge = component.assertions[0]
        index = next(i for i, step in enumerate(self.profile.case.steps)
                     if step.step_id.endswith('-subject-request'))
        self.assertEqual(index, 18)
        step = self.profile.case.steps[index]
        self.assertEqual(step.step_id, 's0018-subject-request')
        self.assertEqual((step.request.method, step.request.target, len(step.request.body)),
                         ('POST', '/api/jobs', 65536))
        self.assertEqual(dict(step.request.headers)['Content-Length'], '65536')
        self.assertIn('é'.encode(), step.request.body)
        self.assertEqual((step.expectation.semantic.shape, step.expectation.semantic.status,
                          step.expectation.raw_facts_only), ('unspecified', 200, False))
        self.assertEqual((edge.obligation_id, edge.assertion.kind, edge.assertion.logical_gate_ids,
                          edge.observation_pointer),
                         ('m1:V0-HTTP-03', 'boundary', ('M1-GATE-HTTP',), '/diagnostics/18/status'))
        self.assertEqual(edge.case_id, component.selectors[index].case_id)
        self.assertIn('no exact response wrapper', edge.rationale)
        self.assertEqual(component.original_definition_purpose, 'harness_qualification')
        self.assertEqual(component.gate.binding.purpose, 'public_release')
        self.assertEqual(component.source_contract_sha256, compiler.M1_SHA256)

    def test_all_original_diagnostics_and_content_prerequisites_remain(self):
        component = self.component
        self.assertEqual((len(component.selectors), len(component.gate.ordered_case_ids)), (38, 38))
        self.assertEqual(len(self.profile.case.steps), 37)
        self.assertEqual(component.selectors[-1].evidence_kind, 'mechanics')
        self.assertNotIn(component.selectors[-1].case_id, [row.case_id for row in component.assertions])
        for index, suffix in ((19, 'subject-request-after-jobs'),
                              (26, 'confirm-accepted-manifest-prepare-after-jobs'),
                              (33, 'confirm-accepted-content-commit-after-jobs'),
                              (34, 'confirm-accepted-content-commit-after-documents-0'),
                              (35, 'confirm-accepted-content-commit-after-export')):
            step = self.profile.case.steps[index]
            self.assertTrue(step.step_id.endswith(suffix))
            self.assertTrue(step.expectation.content_requires)
            self.assertEqual(component.selectors[index].disposition, 'normative')
            self.assertNotEqual(component.assertions[0].case_id, component.selectors[index].case_id)
        source.verify_slice(component)

    def test_one_over_and_wrong_media_do_not_gain_success_boundary_credit(self):
        for name in ('HTTP-BODY-WIRE/raw-bytes-65537', 'HTTP-BODY-WIRE/wrong-media'):
            with self.subTest(case=name):
                component = source.http_slice(*http_registration(name))
                edges = [row for row in component.assertions if row.obligation_id == 'm1:V0-HTTP-03']
                self.assertTrue(edges)
                self.assertTrue(all(row.assertion.kind == 'negative' for row in edges))
                self.assertTrue(all('exact-bound non-rejection' not in row.rationale for row in edges))

    def test_real_compiler_advances_past_empty_gate_but_full_scope_stays_blocked(self):
        # Only the cohort identity is a disclosed structural fixture. The exact
        # source inventory, actual history selectors and compiler are unchanged.
        cohort = synthetic_declaration(self.catalog.inventory).cohort
        declaration = source3.assemble_declaration(self.catalog, cohort,
            (source3.ExecutableSlice(**{field: getattr(self.component, field)
                for field in self.component.__dataclass_fields__}),), review_sha256='d' * 64)
        self.assertEqual(len(declaration.obligations), 312)
        self.assertEqual(declaration.gates[0].logical_gate_ids, ('M1-GATE-HTTP',))
        original_empty = replace(declaration,
            gates=(replace(declaration.gates[0], logical_gate_ids=()),))
        with self.assertRaisesRegex(compiler.CompilerError, 'Empty/non-tuple logical gates'):
            compiler.compile_design(self.catalog.inventory, original_empty,
                                    self.component.gate.binding.subject)
        result = compiler.compile_design(self.catalog.inventory, declaration,
                                         self.component.gate.binding.subject)
        self.assertIsNone(result.registry)
        self.assertTrue(result.blockers)
        self.assertEqual((len(cohort.milestones), len(cohort.trajectories)), (4, 6))

    def test_changed_selector_or_old_empty_factory_is_not_the_new_source_mapping(self):
        edge = self.component.assertions[0]
        for altered in (replace(self.component, assertions=()),
                replace(self.component, assertions=(replace(edge,
                    case_id=self.component.selectors[0].case_id,
                    observation_pointer='/diagnostics/0/status'),))):
            with self.subTest(assertions=altered.assertions):
                with self.assertRaises(source.ScopeSourceError):
                    source.verify_slice(altered)
