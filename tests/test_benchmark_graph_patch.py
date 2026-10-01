"""Trusted oracle/contract checks; candidate execution belongs in Docker lanes."""
from copy import deepcopy
import ast
import hashlib
import json
from pathlib import Path
import unittest

from gossip_harness import benchmark_graph_patch as fixture
from gossip_harness import verification_buildgraph as legacy


class BenchmarkGraphPatchTests(unittest.TestCase):
    def run_commands(self, *commands, stage=1):
        return fixture.reference(stage, {'commands': list(commands)})

    def test_contract_has_two_incremental_stages_and_exact_mature_seed(self):
        self.assertEqual(len(fixture.PROJECT['stages']), 2)
        self.assertEqual(fixture.PROJECT['initial_files'], legacy._files(4))
        self.assertEqual(fixture.PROJECT['allowed_paths'], legacy.ALLOWED)
        ids = [case['id'] for stage in fixture.STAGES
               for field in ('visible_cases', 'hidden_cases') for case in stage[field]]
        self.assertEqual(len(ids), len(set(ids)))
        requirements = set()
        for stage in fixture.STAGES:
            requirements.update(stage['requirements'])
            self.assertTrue(all(case['requirement'] in requirements
                                for field in ('visible_cases', 'hidden_cases') for case in stage[field]))

    def test_exact_old_cases_use_mature_contract_and_disjoint_new_cases(self):
        for old_stage in legacy.STAGES if hasattr(legacy, 'STAGES') else legacy.PROJECT['stages']:
            for field in ('visible_cases', 'hidden_cases'):
                for case in old_stage[field]:
                    self.assertEqual(fixture.reference(0, case['input']), legacy.reference(3, case['input']))
        public = {json.dumps(case['input'], sort_keys=True) for stage in fixture.STAGES for case in stage['visible_cases']}
        private = {json.dumps(case['input'], sort_keys=True) for stage in fixture.STAGES for case in stage['hidden_cases']}
        self.assertFalse(public & private)

    def test_simultaneous_swap_preserves_rows_and_existing_root_caches(self):
        answers = self.run_commands(legacy.put('a', 'A'), legacy.put('b', 'B'),
                                    legacy.build('a'), legacy.build('b'),
                                    fixture.rename('swap', 4, ('a', 'b'), ('b', 'a')),
                                    legacy.LIST, legacy.AUDIT)
        self.assertEqual(answers[4]['invalidated'], [])
        self.assertEqual([(row['id'], row['source']) for row in answers[5]], [('a', 'B'), ('b', 'A')])
        self.assertEqual(answers[5][0]['digest'], answers[3]['digest'])
        self.assertEqual(answers[5][1]['digest'], answers[2]['digest'])
        self.assertEqual(answers[-1][-1], {'version': 5, 'op': 'rename', 'ids': ['a', 'b']})

    def test_rename_root_cache_survives_but_full_dependent_closure_is_dirty(self):
        answers = self.run_commands(legacy.put('a'), legacy.put('b', deps=['a']),
                                    legacy.put('c', deps=['b']), legacy.put('d', deps=['c']),
                                    legacy.commit('build', 4, 'd'),
                                    fixture.rename('rename', 5, ('a', 'z')), legacy.LIST)
        self.assertEqual(answers[-2]['invalidated'], ['b', 'c', 'd'])
        self.assertIsNotNone(answers[-1][-1]['digest'])
        self.assertTrue(all(row['digest'] is None for row in answers[-1][:-1]))

    def test_swapped_dependency_bindings_invalidate_unchanged_fanin_id_set(self):
        answers = self.run_commands(legacy.put('a', 'A'), legacy.put('b', 'B'),
                                    legacy.put('join', 'J', deps=['a', 'b']), legacy.commit('all', 3, 'join'),
                                    fixture.rename('swap', 4, ('a', 'b'), ('b', 'a')), legacy.LIST,
                                    legacy.commit('after', 5, 'join'))
        self.assertEqual(answers[4]['invalidated'], ['join'])
        self.assertEqual(answers[5][-1]['deps'], ['a', 'b'])
        self.assertIsNone(answers[5][-1]['digest'])
        self.assertNotEqual(answers[3]['digests']['join'], answers[6]['digests']['join'])

    def test_rename_retry_normalizes_mapping_order_and_precedes_changed_state(self):
        first = fixture.rename('swap', 2, ('a', 'b'), ('b', 'a'))
        answers = self.run_commands(legacy.put('a'), legacy.put('b'), first,
                                    legacy.put('a', 'changed'),
                                    fixture.rename('swap', 2, ('b', 'a'), ('a', 'b')),
                                    fixture.rename('swap', 0, ('a', 'z')), legacy.AUDIT)
        self.assertEqual(answers[2], answers[4])
        self.assertEqual(answers[5], {'error': 'conflict'})
        self.assertEqual(len(answers[6]), 4)

    def test_rename_preserves_historical_receipts_and_self_map_has_one_event(self):
        answers = self.run_commands(legacy.put('a'), legacy.commit('build', 1, 'a'),
                                    fixture.rename('rename', 2, ('a', 'z')),
                                    legacy.commit('build', 1, 'a'),
                                    fixture.rename('self', 3, ('z', 'z')),
                                    fixture.rename('self', 3, ('z', 'z')), legacy.AUDIT)
        self.assertEqual(answers[1], answers[3])
        self.assertEqual(answers[4], answers[5])
        self.assertEqual(answers[-1][-1], {'version': 4, 'op': 'rename', 'ids': []})
        self.assertEqual(len(answers[-1]), 4)

    def test_rename_errors_preserve_state_and_leave_key_reusable(self):
        answers = self.run_commands(legacy.put('a'), legacy.put('b'),
                                    fixture.rename('key', 2, ('ghost', 'a'), ('a', 'b')),
                                    fixture.rename('key', 2, ('a', 'b')),
                                    fixture.rename('key', 2, ('a', 'z')), legacy.LIST, legacy.AUDIT)
        self.assertEqual(answers[2:4], [{'error': 'missing'}, {'error': 'collision'}])
        self.assertEqual(answers[4]['version'], 3)
        self.assertEqual([row['id'] for row in answers[5]], ['b', 'z'])
        self.assertEqual(len(answers[6]), 3)

    def test_patch_final_validation_allows_transient_missing_and_cycles(self):
        missing = self.run_commands(legacy.put('a'), legacy.put('b', deps=['a']),
                                    fixture.patch('edit', 2, legacy.remove('a'), legacy.put('b')), legacy.LIST)
        self.assertEqual(missing[2]['ok'], 'patched')
        self.assertEqual([row['id'] for row in missing[-1]], ['b'])
        cycle = self.run_commands(legacy.put('a'), legacy.put('b', deps=['a']),
                                  fixture.patch('edit', 2, legacy.put('a', deps=['b']), legacy.put('b')),
                                  legacy.LIST)
        self.assertEqual(cycle[2]['ok'], 'patched')
        self.assertEqual([row['deps'] for row in cycle[-1]], [['b'], []])

    def test_patch_net_revert_and_priority_keep_cache(self):
        answers = self.run_commands(legacy.put('a', 'stable'), legacy.build('a'),
                                    fixture.patch('revert', 2, legacy.put('a', 'temporary'), legacy.put('a', 'stable')),
                                    legacy.build('a'), fixture.patch('priority', 3, legacy.put('a', 'stable', 3)),
                                    legacy.build('a'), legacy.AUDIT)
        self.assertEqual(answers[2]['invalidated'], [])
        self.assertTrue(answers[3]['cached'])
        self.assertEqual(answers[4]['invalidated'], [])
        self.assertTrue(answers[5]['cached'])
        self.assertEqual(answers[-1][-2]['ids'], [])

    def test_patch_remove_recreate_discards_provenance(self):
        answers = self.run_commands(legacy.put('a'), legacy.build('a'),
                                    fixture.patch('replace', 2, legacy.remove('a'), legacy.put('a')),
                                    legacy.build('a'), legacy.AUDIT)
        self.assertEqual(answers[2]['invalidated'], ['a'])
        self.assertFalse(answers[3]['cached'])
        self.assertEqual(answers[-1][-2], {'version': 3, 'op': 'patch', 'ids': []})

    def test_patch_normalizes_defaults_but_preserves_edit_order(self):
        answers = self.run_commands(fixture.patch('new', 0, {'op': 'put', 'id': 'a', 'source': 'x'}),
                                    fixture.patch('new', 0, legacy.put('a')),
                                    fixture.patch('ordered', 1, legacy.put('a', 'A'), legacy.put('a', 'B')),
                                    fixture.patch('ordered', 1, legacy.put('a', 'B'), legacy.put('a', 'A')),
                                    legacy.LIST, legacy.AUDIT)
        self.assertEqual(answers[0], answers[1])
        self.assertEqual(answers[3], {'error': 'conflict'})
        self.assertEqual(answers[4][0]['source'], 'B')
        self.assertEqual([row['version'] for row in answers[5]], [1, 2])

    def test_patch_validates_whole_syntax_first_and_rolls_back_state_failures(self):
        answers = self.run_commands(legacy.put('a'), legacy.build('a'),
                                    fixture.patch('bad', 2, legacy.remove('ghost'), {'op': 'put', 'id': 'b', 'source': True}),
                                    fixture.patch('bad', 2, legacy.put('a', 'new'), legacy.remove('ghost')),
                                    legacy.build('a'), fixture.patch('bad', 2, legacy.put('a', 'good')), legacy.AUDIT)
        self.assertEqual(answers[2:4], [{'error': 'invalid'}, {'error': 'missing'}])
        self.assertTrue(answers[4]['cached'])
        self.assertEqual(answers[5]['version'], 3)
        self.assertEqual(len(answers[6]), 3)

    def test_probe_domains_preserve_invalid_commands_and_do_not_mutate(self):
        for stage in (0, 1):
            probes = [fixture.rehearsal_probe(stage, slot) for slot in range(4)]
            self.assertEqual(len({json.dumps(probe) for probe in probes}), 4)
            for probe in probes:
                before = deepcopy(probe)
                fixture.reference(stage, probe)
                self.assertEqual(before, probe)
        invalid = {'commands': [fixture.rename('key', True, ('a', 'b')), {'op': []}, None]}
        fixture.validate_input(0, invalid)
        self.assertEqual(fixture.reference(0, invalid), [{'error': 'invalid'}] * 3)
        with self.assertRaises(ValueError):
            fixture.validate_input(0, {'commands': [fixture.patch('key', 0, legacy.put('a'))]})
        for value in (False, -1, 2):
            with self.assertRaises(ValueError):
                fixture.known_files(value)

    def test_fault_banks_are_bound_distinct_scoped_and_witnessed_without_execution(self):
        for stage in (0, 1):
            known = fixture.known_files(stage)
            bank = fixture.fault_bank(stage)
            self.assertGreaterEqual(len(bank), 6)
            self.assertEqual(len({row['family'] for row in bank}), len(bank))
            self.assertEqual(len({json.dumps(row['files'], sort_keys=True) for row in bank}), len(bank))
            for row in bank:
                self.assertEqual(set(row), {'id', 'family', 'files', 'witness_cases'})
                changed = {path for path in known if known[path] != row['files'][path]}
                self.assertTrue(changed)
                self.assertTrue(changed <= set(fixture.ALLOWED))
                for path, source in row['files'].items():
                    if path.endswith('.py'):
                        ast.parse(source)
                for case in row['witness_cases']:
                    self.assertEqual(case['expected'], fixture.reference(stage, case['input']))
            controls = fixture.correct_controls(stage)
            self.assertEqual(len(controls), 2)
            self.assertEqual(controls[0]['files'], known)
            self.assertNotEqual(controls[0]['files'], controls[1]['files'])
            self.assertEqual(ast.dump(ast.parse(controls[0]['files'][fixture.ALLOWED[2]])),
                             ast.dump(ast.parse(controls[1]['files'][fixture.ALLOWED[2]])))


if __name__ == '__main__':
    unittest.main()
