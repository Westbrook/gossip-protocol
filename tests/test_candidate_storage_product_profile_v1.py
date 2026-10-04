"""Host-side declaration/projection controls; no candidate execution or review."""
from collections import Counter
from copy import deepcopy
from dataclasses import replace
import unittest
from unittest.mock import patch

from gossip_harness import candidate_storage_cases_v1 as b01
from gossip_harness import candidate_storage_observer_v1 as storage
from gossip_harness import candidate_intake_store_cases_v3 as b02
from gossip_harness import candidate_intake_store_observer_v1 as intake
from gossip_harness import candidate_storage_product_profile_v1 as profile


def observations(family, expected):
    """Authored projection unit-test inputs, explicitly not execution evidence."""
    cls = storage.Observation if family == 'b01' else intake.Observation
    values = {}
    for phase in profile.PHASES:
        state = deepcopy(expected[phase])
        strings = {row['public']['job_id']: {
            field: None if row[field] is None else profile.encoded(row[field]).decode()
            for field in ('manifest', 'content_hashes', 'receipt')} for row in state['jobs']}
        values[phase] = cls(state, strings, (), '1' * 64, '2' * 64, {}, storage.V2_SQLITE_LAYOUT)
    return values


def independent_b02_census(case, expected):
    """Write out native scorer order independently of the new profile helper."""
    ids = ['admission-variant'] if 'expected_alternatives' in case else []
    for phase in ('before', 'after', 'reopened'):
        ids += [f'{phase}.persisted-state', f'{phase}.result-count']
        ids += [f'{phase}.result.{index}' for index, _ in enumerate(expected['results'][phase])]
    ids += [f'{phase}.persisted-string-job-set' for phase in ('before', 'after', 'reopened')]
    for job in expected['before']['jobs']:
        jid = job['public']['job_id']
        for phase in ('after', 'reopened'):
            ids += [f'{phase}.{jid}.immutable-manifest', f'{phase}.{jid}.immutable-content_hashes']
            target = next((row for row in expected[phase]['jobs'] if row['public']['job_id'] == jid), None)
            if target is not None and target['receipt'] == job['receipt']:
                ids.append(f'{phase}.{jid}.receipt-conservation')
    ids.append('reopened.persisted-strings')
    if expected['before'] == expected['after']:
        ids.append('after.auxiliary-conservation')
    ids.append('reopened.auxiliary-conservation')
    if case['case_id'] in b02.POLICY_LIMITED_CASE_IDS:
        ids.remove('after.result.0')
        ids += ['after.result.0.native-error-rejection', 'after.result.0.exact-error-code']
    return tuple(ids)


class CandidateStorageProductProfileTests(unittest.TestCase):
    def test_full_authored_rosters_and_all_ordered_assertions(self):
        self.assertEqual(len(b01.CASE_IDS), 8)
        self.assertEqual(len(b02.CASE_IDS), 241)
        counts = Counter()
        for case in b02.definitions():
            value = profile.profile_for('b02', case['case_id'], 'public_release')
            variants = [case['expected'], *case.get('expected_alternatives', {}).values()]
            ordered = list(dict.fromkeys(key for expected in variants for key in independent_b02_census(case, expected)))
            self.assertEqual(value.diagnostic_ids, tuple(ordered))
            self.assertEqual(value.ordered_case_ids, value.decisive_ids)
            self.assertEqual(value.native_history_ids, b02.CASE_IDS)
            self.assertEqual(value.record()['original_definition'], case)
            for row in value.record()['diagnostics']:
                counts[row['applicability']] += 1
        self.assertEqual(counts['unspecified'], 3)
        self.assertGreater(counts['unqualified'], 241)
        for case in b01.definitions():
            value = profile.profile_for('b01', case['case_id'], 'public_release')
            self.assertEqual(value.native_history_ids, b01.CASE_IDS)
            self.assertEqual(value.record()['original_definition'], case)
            self.assertEqual(value.ordered_case_ids, value.decisive_ids)

    def test_exact_source_contract_purpose_and_no_semantic_authority(self):
        values = [profile.profile_for('b01', 'rollback', purpose) for purpose in
                  ('public_release', 'independent_acceptance', 'repeatability')]
        self.assertEqual(len({value.sha256 for value in values}), 3)
        for value in values:
            record = value.record()
            self.assertEqual(record['original_contract_sha256'], profile.ORIGINAL_CONTRACT_SHA256)
            self.assertEqual(record['target_contract_sha256'], profile.TARGET_CONTRACT_SHA256)
            self.assertEqual(record['original_milestone'], 'M1')
            self.assertEqual(record['target_milestone'], 'M4')
            self.assertEqual(record['original_definition_purpose'], 'harness_qualification')
            self.assertFalse(record['semantic_authority'])
            self.assertFalse(record['execution_authenticated'])
            self.assertIsNone(record['full_requirement_verdict'])
            self.assertEqual(record['m4_expectation_transformations'], [])
            self.assertEqual(record['common_source_helper']['function'], 'source_sha256')
            for path, digest in record['definition_sources'].items():
                self.assertEqual(profile.hashlib.sha256((profile.ROOT / path).read_bytes()).hexdigest(), digest)
            self.assertEqual([row['pointer'] for row in record['compatibility_clauses']], ['/compatibility', '/amendments'])
        self.assertEqual(values[0].record()['original_case_sha256'], values[1].record()['original_case_sha256'])
        self.assertEqual(values[0].record()['excluded_fixture_projection']['assertion_count'], 40)

    def test_profile_and_returned_records_cannot_mutate_declared_roster(self):
        value = profile.profile_for('b01', 'rollback', 'public_release')
        record = value.record()
        record['diagnostics'].clear()
        self.assertTrue(value.diagnostic_ids)
        forged = replace(value, _record_bytes=profile.encoded(record))
        with self.assertRaises(profile.ProfileError):
            profile.project(forged, {}, {})
        for family, case, purpose in [('B01', 'rollback', 'public_release'), ('b01', 'missing', 'public_release'),
                                      ('b01', 'rollback', 'harness_qualification')]:
            with self.assertRaises(profile.ProfileError):
                profile.profile_for(family, case, purpose)

    def test_source_pin_change_fails_even_cached_profile(self):
        value = profile.profile_for('b01', 'rollback', 'public_release')
        with patch.object(profile, 'LOADED_SOURCE_SHA256', '0' * 64):
            with self.assertRaises(profile.ProfileError):
                profile.profile_for(value.family, value.case_id, value.purpose)

    def test_b01_authored_jobs_define_checks_even_when_candidate_omits_them(self):
        value = profile.profile_for('b01', 'rollback', 'public_release')
        case = value.record()['original_definition']
        actual = observations('b01', case)
        actual['before'].persisted_strings.clear()
        result = profile.project(value, actual, {'after': case['result']})
        self.assertEqual(tuple(result['checks']), value.diagnostic_ids)
        self.assertFalse(result['checks']['case.immutable-manifest'])
        self.assertFalse(result['checks']['case.immutable-content_hashes'])
        self.assertFalse(result['checks']['case.receipt-conservation'])
        actual['before'].persisted_strings['injected-job'] = {'manifest': '[]'}
        again = profile.project(value, actual, {'after': case['result']})
        self.assertEqual(tuple(again['checks']), value.diagnostic_ids)
        self.assertFalse(any('injected-job' in key for key in again['checks']))

    def test_b01_partial_failure_survives_missing_later_capture(self):
        value = profile.profile_for('b01', 'rollback', 'public_release')
        case = value.record()['original_definition']
        actual = observations('b01', case)
        actual['before'].data['documents'] = []
        actual['after'].persisted_strings['case']['manifest'] = 'changed'
        del actual['reopened']
        result = profile.project(value, actual, {'after': {'error': 'wrong'}})
        self.assertFalse(result['checks']['before.persisted-state'])
        self.assertFalse(result['checks']['case.immutable-manifest'])
        self.assertFalse(result['checks']['result'])
        self.assertIsNone(result['checks']['reopened.persisted-state'])
        self.assertIn('reopened.persisted-state', {row['check_id'] for row in result['observation_unavailable']})

    def test_m4_auxiliary_differences_never_gain_decisive_credit(self):
        for family, case_id in [('b01', 'empty-batch'), ('b02', b02.CASE_IDS[0])]:
            value = profile.profile_for(family, case_id, 'public_release')
            case = value.record()['original_definition']
            expected = case if family == 'b01' else case['expected']
            actual = observations(family, expected)
            actual['after'].auxiliary_tables['metadata'] = [{'catalog_generation': '2'}]
            responses = {'after': case['result']} if family == 'b01' else deepcopy(expected['results'])
            result = profile.project(value, actual, responses)
            for row in result['diagnostics']:
                if 'auxiliary' in row['check_id']:
                    self.assertEqual(row['applicability'], 'unqualified')
                    self.assertIsNone(row['value'])
                    self.assertNotIn(row['check_id'], value.decisive_ids)
            self.assertIn('M4 schema4', result['remaining_coverage'][3])
            self.assertFalse(result['execution_authenticated'])

    def test_complete_b02_reuses_native_comparator_and_retains_false(self):
        value = profile.profile_for('b02', 'store-start_job-queued-lower', 'public_release')
        case = value.record()['original_definition']
        actual = observations('b02', case['expected'])
        responses = deepcopy(case['expected']['results'])
        responses['before'][0] = {'error': 'wrong'}
        native = b02.evaluate_case(value.case_id, *(actual[phase] for phase in profile.PHASES), responses)
        result = profile.project(value, actual, responses)
        for check in value.decisive_ids:
            self.assertEqual(result['checks'][check], native['checks'][check])
        self.assertFalse(result['checks']['before.result.0'])

    def test_b02_partial_projection_uses_only_actual_phases(self):
        value = profile.profile_for('b02', 'store-start_job-queued-lower', 'public_release')
        case = value.record()['original_definition']
        actual = observations('b02', case['expected'])
        responses = deepcopy(case['expected']['results'])
        responses['before'][0] = {'error': 'wrong'}
        del actual['reopened']
        del responses['reopened']
        with patch.object(b02, 'evaluate_case', side_effect=AssertionError('must not fabricate missing snapshot')):
            # Source-code authentication intentionally sees this patched method;
            # disable that guard only in this unit control, not the production API.
            with patch.object(profile, '_check_sources'):
                result = profile.project(value, actual, responses)
        self.assertFalse(result['checks']['before.result.0'])
        self.assertTrue(result['checks']['after.persisted-state'])
        self.assertIsNone(result['checks']['reopened.persisted-state'])
        self.assertIsNone(result['checks']['reopened.result-count'])

    def test_forced_schedule_unavailable_keeps_earlier_failures(self):
        value = profile.profile_for('b02', 'interfere-start_job-cancel', 'public_release')
        case = value.record()['original_definition']
        actual = observations('b02', case['expected'])
        responses = deepcopy(case['expected']['results'])
        responses['before'][0] = {'error': 'wrong'}
        responses['after'][0] = {'observation_unavailable': 'schedule-not-reviewed'}
        result = profile.project(value, actual, responses)
        self.assertFalse(result['checks']['before.result.0'])
        self.assertIsNone(result['checks']['after.result.0'])
        self.assertIsNone(result['checks']['after.persisted-state'])
        self.assertIn('after.result.0', value.decisive_ids)
        self.assertEqual(result['unspecified_assertions'], [])

    def test_wrong_kind_unspecified_is_distinct_from_native_frame_unavailable(self):
        for case_id in b02.POLICY_LIMITED_CASE_IDS:
            value = profile.profile_for('b02', case_id, 'public_release')
            case = value.record()['original_definition']
            actual = observations('b02', case['expected'])
            responses = deepcopy(case['expected']['results'])
            for frame, expected_rejection in [({'error': 'any_nonempty_native_code'}, True),
                                               ({'unexpected_exception': 'Unknown'}, None)]:
                responses['after'][0] = frame
                result = profile.project(value, actual, responses)
                self.assertEqual(result['checks'][b02.NATIVE_REJECTION_ASSERTION_ID], expected_rejection)
                self.assertIsNone(result['checks'][b02.UNSPECIFIED_ASSERTION_IDS[0]])
                self.assertEqual(result['unspecified_assertions'], list(b02.UNSPECIFIED_ASSERTION_IDS))
                self.assertFalse(result['legacy_exact_error_diagnostic']['matched'])
                self.assertFalse(result['legacy_exact_error_diagnostic']['qualification_credit'])
                unavailable = {row['check_id'] for row in result['observation_unavailable']}
                self.assertNotIn(b02.UNSPECIFIED_ASSERTION_IDS[0], unavailable)
                self.assertEqual(b02.NATIVE_REJECTION_ASSERTION_ID in unavailable, expected_rejection is None)

    def test_branch_selection_never_changes_ordered_decisive_roster(self):
        value = profile.profile_for('b02', 'intake-json-duplicate-same', 'public_release')
        case = value.record()['original_definition']
        for expected in case['expected_alternatives'].values():
            actual = observations('b02', expected)
            result = profile.project(value, actual, deepcopy(expected['results']))
            self.assertEqual(tuple(result['checks']), value.diagnostic_ids)
            self.assertEqual(tuple(result['decisive_ids']), value.ordered_case_ids)
            self.assertTrue(result['checks']['admission-variant'])
            self.assertTrue(all(result['checks'][key] is True for key in value.decisive_ids))
        branch_only = [row for row in value.record()['diagnostics'] if row['reason'].startswith('Branch-specific')]
        self.assertTrue(branch_only)
        self.assertTrue(all(row['check_id'] not in value.decisive_ids for row in branch_only))

    def test_missing_branch_response_preserves_every_branch_independent_failure(self):
        value = profile.profile_for('b02', 'intake-json-duplicate-same', 'public_release')
        case = value.record()['original_definition']
        expected = case['expected_alternatives']['deferred']
        actual = observations('b02', expected)
        actual['reopened'].persisted_strings['untouched']['manifest'] = 'changed'
        responses = deepcopy(expected['results'])
        del responses['before']
        responses['after'].append({'error': 'extra'})
        result = profile.project(value, actual, responses)
        self.assertIsNone(result['checks']['admission-variant'])
        self.assertFalse(result['checks']['after.result-count'])
        self.assertFalse(result['checks']['reopened.persisted-strings'])

    def test_b01_complete_authored_projection_matches_native_inherited_checks(self):
        for case in b01.definitions():
            value = profile.profile_for('b01', case['case_id'], 'public_release')
            actual = observations('b01', case)
            native = storage.evaluate_case(case['case_id'], *(actual[phase] for phase in profile.PHASES), case['result'])
            result = profile.project(value, actual, {'after': case['result']})
            for check in value.decisive_ids:
                self.assertEqual(result['checks'][check], native['checks'][check])
                self.assertTrue(result['checks'][check])

    def test_selectors_bind_original_raw_files_and_only_declared_partial_facets(self):
        value = profile.profile_for('b02', 'store-start_job-queued-lower', 'public_release')
        record = value.record()
        row = next(row for row in record['diagnostics'] if row['check_id'] == 'after.result.0')
        self.assertEqual(row['selector'], '/checks/after.result.0')
        self.assertEqual(row['raw_dependencies'], [{'path': 'after-response.json', 'pointer': '/value/0'}])
        self.assertEqual([item['path'] for item in row['evaluation_response_dependencies']],
                         ['before-response.json', 'after-response.json', 'reopened-response.json'])
        state = next(row for row in record['diagnostics'] if row['check_id'] == 'after.persisted-state')
        self.assertEqual(state['raw_dependencies'], [{'path': 'after-capture-stdout.bin', 'pointer': '', 'observer_projection': '/data'}])
        self.assertTrue(row['source_unit_facets'])
        self.assertTrue(all(item['full_source_unit'] is False for item in row['source_unit_facets']))
        setup = next(row for row in record['diagnostics'] if row['check_id'] == 'before.result.0')
        self.assertEqual(setup['source_unit_facets'], [])
        b01_record = profile.profile_for('b01', 'rollback', 'public_release').record()
        self.assertTrue(all(row['source_unit_facets'] == [] for row in b01_record['diagnostics']))

    def test_mixed_registration_wrong_types_and_unknown_phases_rejected(self):
        value = profile.profile_for('b01', 'rollback', 'public_release')
        actual = observations('b01', value.record()['original_definition'])
        actual['after'] = replace(actual['after'], registration_sha256='3' * 64)
        with self.assertRaises(profile.ProfileError):
            profile.project(value, actual, {})
        with self.assertRaises(profile.ProfileError):
            profile.project(value, {'before': {}}, {})
        with self.assertRaises(profile.ProfileError):
            profile.project(value, {}, {'final': None})


if __name__ == '__main__':
    unittest.main()
