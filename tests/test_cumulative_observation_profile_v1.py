"""Prospective declaration checks only; no candidate execution or review authority."""
from collections import Counter
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
import base64
import unittest
from unittest.mock import patch

from gossip_harness import candidate_cli_cases_v1 as cli
from gossip_harness import candidate_http_cases_v1 as http
from gossip_harness import candidate_http_semantics_v1 as semantics
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_observation_profile_v1 as profile
from gossip_harness import project_acceptance_registry_v1 as registry


def diagnostics(value):
    return tuple(registry.CaseResult(cell.case_id,
        'skipped' if cell.applicability == 'unspecified' else 'passed')
        for cell in value.diagnostic_cells)


class CumulativeObservationProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cli_definitions = cli.definitions()
        cls.cli_profiles = tuple(profile.cli_profile(row['case_id'], purpose='public_release')
                                 for row in cls.cli_definitions)
        cls.http_definitions = http.definitions()

    def test_complete_cli_census_and_exact_source_declared_exclusions(self):
        cells = tuple(cell for value in self.cli_profiles for cell in value.diagnostic_cells)
        self.assertEqual(len(self.cli_profiles), 57)
        self.assertEqual(sum(len(row['recipe']['steps']) for row in self.cli_definitions), 305)
        self.assertEqual(len(cells), 900)
        self.assertEqual(len({cell.case_id for cell in cells}), 900)
        self.assertEqual(Counter(cell.applicability for cell in cells), {'normative': 868, 'unspecified': 32})
        self.assertEqual(Counter(cell.assertion_id for cell in cells if cell.applicability == 'unspecified'),
                         {'stdout.wrapper': 15, 'streams.framing': 15, 'stderr.error_code': 2})
        excluded = {cell.case_id for cell in cells if cell.applicability == 'unspecified'}
        wrappers = {
            'cli-legacy-persistence': ('s01', 's02', 's03', 's14'),
            'cli-db-root-isolation': ('s01', 's04'), 'cli-source-changed': ('s01',),
            'cli-export-duplicate': ('s01',), 'cli-export-missing': ('s01',),
            'cli-intake-directory': ('s03',), 'cli-intake-zip': ('s03',), 'cli-intake-json': ('s03',),
            'cli-jobs-replay-conflict': ('s04', 's11'), 'cli-prepare-import-conflict': ('s03',)}
        expected = {case + ':' + step + ':stdout.wrapper' for case, steps in wrappers.items() for step in steps}
        expected.update(case + ':s02:stderr.error_code' for case in ('cli-export-duplicate', 'cli-export-missing'))
        framing = ['cli-namespace-' + kind for kind in ('directory', 'zip', 'json')]
        framing += ['cli-invalid-kind']
        framing += ['cli-grammar-' + name for name in ('missing-command', 'unknown-command',
            'missing-import-source', 'missing-show-id', 'missing-epoch', 'noninteger-epoch', 'unknown-flag')]
        framing += ['cli-invalid-' + name for name in ('offset-negative', 'limit-zero', 'limit-over', 'query-over')]
        expected.update(case + ':s01:streams.framing' for case in framing)
        self.assertEqual(excluded, expected)

    def test_cli_preserves_every_original_expectation_recipe_and_order(self):
        for original, value in zip(self.cli_definitions, self.cli_profiles, strict=True):
            with self.subTest(case=value.case_id):
                self.assertEqual(value.record()['original_definition'], original)
                self.assertEqual(value.target_definition()['definition'], original)
                self.assertEqual(value.health_successors, ())
                self.assertEqual(value.record()['original_definition_sha256'], profile.digest(original))
                self.assertEqual(value.record()['decisive_case_ids'], list(value.decisive_case_ids))

    def test_complete_http_catalog_and_only_three_allowlisted_successors(self):
        counts = Counter()
        changed = Counter()
        for case in self.http_definitions:
            value = profile.http_profile(case.row_id, purpose='public_release')
            original = value.record()['original_definition']
            self.assertEqual(original, case.record())
            self.assertEqual(len(value.diagnostic_cells), len(case.steps))
            self.assertEqual(tuple(cell.step_id for cell in value.diagnostic_cells), tuple(s.step_id for s in case.steps))
            for step, cell in zip(case.steps, value.diagnostic_cells, strict=True):
                raw_only = step.expectation is not None and step.expectation.raw_facts_only
                self.assertEqual(cell.applicability, "unqualified" if raw_only else "normative")
                if raw_only:
                    self.assertNotIn(cell.case_id, value.decisive_case_ids)
                    self.assertIn(cell.step_id, tuple(s["step_id"] for s in original["steps"]))
            counts['histories'] += 1
            counts['steps'] += len(case.steps)
            counts['requests'] += sum(s.kind == 'request' for s in case.steps)
            target = value.target_definition()['definition']
            restored = deepcopy(target)
            for successor in value.health_successors:
                changed[case.row_id] += 1
                index = successor.step_index
                self.assertEqual(case.steps[index].request.method, 'GET')
                self.assertEqual(case.steps[index].request.target, '/health')
                self.assertEqual(base64.b64decode(target['steps'][index]['expectation']['semantic']['expected_body']['base64']),
                                 b'{"status":"ok","schema":4}')
                restored['steps'][index]['expectation']['semantic']['expected_body'] = original['steps'][index]['expectation']['semantic']['expected_body']
            self.assertEqual(restored, original, 'Only the health expected body may change')
        self.assertEqual(counts, {'histories': 276, 'steps': 8203, 'requests': 7602})
        self.assertEqual(changed, {'HTTP-EMPTY-HEALTH/health': 1,
                                 'HTTP-PERSIST-LISTENER/listener-all-epochs': 2})

    def test_health_target_is_a_separate_data_view_not_an_old_semantic_object(self):
        value = profile.http_profile('HTTP-EMPTY-HEALTH/health', purpose='independent_acceptance')
        target = value.target_definition()
        self.assertTrue(target['requires_versioned_comparator'])
        original = next(case for case in self.http_definitions if case.row_id == value.case_id)
        expected = next(s.expectation.semantic for s in original.steps
                        if s.expectation and s.expectation.semantic and s.expectation.semantic.shape == 'health')
        self.assertEqual(expected.expected_body, b'{"status":"ok","schema":0}')
        with self.assertRaises(ValueError):
            semantics.Expectation('health', 200, b'{"status":"ok","schema":4}')
        with self.assertRaises(profile.ProfileError):
            replace(value.health_successors[0], target_body=b'{"status":"ok","schema":5}')

    def test_profiles_bind_exact_original_target_milestone_and_original_purpose(self):
        for value in (self.cli_profiles[0], profile.http_profile('HTTP-EMPTY-HEALTH/documents', purpose='repeatability')):
            record = value.record()
            self.assertEqual(record['original_contract_sha256'], profile.ORIGINAL_CONTRACT_SHA256)
            self.assertEqual(record['target_contract_sha256'], profile.TARGET_CONTRACT_SHA256)
            self.assertEqual(record['original_milestone'], 'M1')
            self.assertEqual(record['target_milestone'], 'M4')
            self.assertEqual(record['original_definition_purpose'], 'harness_qualification')
            self.assertEqual(record['execution_purpose'], value.purpose)
            clauses = record['compatibility_clauses']
            self.assertEqual(len(clauses), 5)
            self.assertTrue(all(row['source_sha256'] == profile.TARGET_CONTRACT_SHA256 for row in clauses))
            self.assertIn('/amendments', [row['pointer'] for row in clauses])

    def test_purpose_changes_profile_and_complete_review_request_not_original_definitions(self):
        values = tuple(profile.cli_profile(self.cli_profiles[0].case_id, purpose=purpose) for purpose in registry.PURPOSES)
        self.assertEqual(len({value.sha256 for value in values}), 3)
        self.assertEqual(len({profile.review_request(value).sha256 for value in values}), 3)
        self.assertEqual(len({value.record()['original_definition_sha256'] for value in values}), 1)
        for value in values:
            request = profile.review_request(value)
            self.assertEqual(request.record()['profile'], value.record())
            self.assertFalse(request.record()['profile']['dispatch_authority'])
            self.assertFalse(request.record()['profile']['acceptance_authority'])
            self.assertTrue(request.record()['profile']['review_required'])
            self.assertIn('No old M1', request.record()['profile']['observation_reuse'])
            self.assertFalse(hasattr(request, 'approved'))

    def test_no_generic_milestone_contract_compatibility_or_receipt_override(self):
        for bad in ('M1', 'M2', 'M3', True, None):
            with self.subTest(milestone=bad), self.assertRaises(TypeError):
                profile.cli_profile('cli-empty', purpose='public_release', milestone=bad)
        with self.assertRaises(TypeError):
            profile.CumulativeProfile('cli', 'cli-empty', 'public_release', compatibility_sha256='a' * 64)
        with self.assertRaises(profile.ProfileError):
            profile.review_request({'profile_sha256': self.cli_profiles[0].sha256, 'approved': True})
        for purpose in ('harness_qualification', 'registry_qualification', 'public_development', True, None):
            with self.subTest(purpose=purpose), self.assertRaises(profile.ProfileError):
                profile.cli_profile('cli-empty', purpose=purpose)

    def test_invalid_family_history_and_mutability_rejected(self):
        for family, name in [('cli', 'unknown'), ('http', 'unknown'), ('other', 'cli-empty')]:
            with self.subTest(family=family), self.assertRaises(profile.ProfileError):
                profile.CumulativeProfile(family, name, 'public_release')
        value = self.cli_profiles[0]
        before = value.sha256
        external = value.record()
        external['target_milestone'] = 'M1'
        external['diagnostic_cells'].clear()
        self.assertEqual(value.sha256, before)
        self.assertEqual(value.record()['target_milestone'], 'M4')
        with self.assertRaises(FrozenInstanceError):
            value.purpose = 'repeatability'

    def test_common_source_digest_and_profile_drift(self):
        files = {'library/a.py': b'one', 'library/b.py': b'two'}
        self.assertEqual(profile.source_sha256(files), admission.source_sha256(files))
        self.assertNotEqual(profile.source_sha256(files), profile.source_sha256({**files, 'library/a.py': b'changed'}))
        value = self.cli_profiles[0]
        profile.assert_profile_current(value)
        actual_sources = profile.definition_sources()
        actual_sources['gossip_harness/cumulative_observation_profile_v1.py'] = 'f' * 64
        with patch.object(profile, 'definition_sources', return_value=actual_sources):
            with self.assertRaisesRegex(profile.ProfileError, 'changed'):
                profile.assert_profile_current(value)

    def test_stale_loaded_profile_cannot_mint_a_new_identity(self):
        with patch.object(profile, "LOADED_SOURCE_SHA256", "0" * 64):
            with self.assertRaisesRegex(profile.ProfileError, "fresh worker"):
                profile.cli_profile("cli-empty", purpose="public_release")
        with self.assertRaises(profile.ProfileError):
            profile.HealthSuccessor("health", 0, target_body=bytearray(profile.HEALTH_AFTER))

    def test_new_mandatory_amendments_and_unqualified_facets_stay_explicit(self):
        value = self.cli_profiles[0]
        amendments = next(row['value'] for row in value.record()['compatibility_clauses'] if row['pointer'] == '/amendments')
        self.assertEqual({item['id'] for item in amendments}, {'V2-MIGRATION-FENCE', 'V2-MIGRATION-DIAGNOSTIC',
            'V2-COUNTER-DOMAIN', 'V2-M1-HASH-SERIALIZATION', 'V2-WORKER-LIVENESS', 'V2-BACKUP-ROOT'})
        remaining = ' '.join(value.record()['remaining_coverage'])
        for text in ('signed64', 'Migration', 'Worker', 'Backup-root', 'Browser', 'Scope', 'six-trajectory'):
            self.assertIn(text.lower(), remaining.lower())
        self.assertIn('no full-product', value.record()['scope'])


class CumulativeCliProjectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.profile = profile.cli_profile('cli-export-duplicate', purpose='public_release')

    def test_full_diagnostics_remain_retained_and_only_source_unspecified_excluded(self):
        rows = diagnostics(self.profile)
        result = profile.project_cli_outcomes(self.profile, rows)
        self.assertEqual(result.diagnostics, rows)
        self.assertEqual(tuple(row.case_id for row in result.decisive), self.profile.decisive_case_ids)
        self.assertEqual(result.profile_sha256, self.profile.sha256)
        self.assertEqual(profile.decisive_cli_outcomes(self.profile, rows), result.decisive)
        self.assertEqual(len(rows) - len(result.decisive), 2)
        self.assertTrue(all(row.status == 'passed' for row in result.decisive))

    def test_failed_unknown_and_skipped_required_cells_are_never_excluded(self):
        rows = diagnostics(self.profile)
        indexes = [index for index, cell in enumerate(self.profile.diagnostic_cells) if cell.applicability == 'normative']
        changed = list(rows)
        for index, status in zip(indexes, ('failed', 'infrastructure_error', 'skipped'), strict=False):
            changed[index] = replace(rows[index], status=status)
        result = profile.project_cli_outcomes(self.profile, tuple(changed))
        self.assertEqual([row.status for row in result.decisive[:3]], ['failed', 'infrastructure_error', 'skipped'])
        self.assertEqual(len(result.decisive), len(self.profile.decisive_case_ids))

    def test_unavailable_values_cannot_invent_diagnostic_exclusions(self):
        rows = tuple(replace(row, status='infrastructure_error') if row.status == 'passed' else row
                     for row in diagnostics(self.profile))
        result = profile.project_cli_outcomes(self.profile, rows)
        self.assertTrue(all(row.status == 'infrastructure_error' for row in result.decisive))
        self.assertEqual(tuple(row.case_id for row in result.decisive), self.profile.decisive_case_ids)

    def test_unspecified_may_not_be_scored_as_pass_failure_or_infrastructure(self):
        rows = diagnostics(self.profile)
        index = next(i for i, cell in enumerate(self.profile.diagnostic_cells) if cell.applicability == 'unspecified')
        for status in ('passed', 'failed', 'infrastructure_error'):
            changed = list(rows)
            changed[index] = replace(changed[index], status=status)
            with self.subTest(status=status), self.assertRaises(profile.ProfileError):
                profile.project_cli_outcomes(self.profile, tuple(changed))

    def test_missing_extra_reordered_or_untyped_diagnostics_rejected(self):
        rows = diagnostics(self.profile)
        for wrong in (rows[:-1], rows + (rows[0],), tuple(reversed(rows)), list(rows),
                      tuple({'case_id': row.case_id, 'status': row.status} for row in rows)):
            with self.subTest(kind=type(wrong).__name__), self.assertRaises(profile.ProfileError):
                profile.project_cli_outcomes(self.profile, wrong)
        wrong = (replace(rows[0], case_id='another-cell'), *rows[1:])
        with self.assertRaises(profile.ProfileError):
            profile.project_cli_outcomes(self.profile, wrong)
        with self.assertRaises(profile.ProfileError):
            profile.project_cli_outcomes(profile.http_profile('HTTP-EMPTY-HEALTH/health', purpose='public_release'), ())
