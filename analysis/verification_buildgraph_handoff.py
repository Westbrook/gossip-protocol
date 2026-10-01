"""Post-freeze buildgraph database handoff stories: data only, no import-time I/O.

The same SQLite file must pass through selected milestone source/policy versions.
Expected outputs were frozen after manual checks plus the independent pure
reference and trusted known CLI execution. These are supplementary robustness
checks; they never alter primary acceptance. Each story starts a fresh database.
"""

PROJECT_ID = 'buildgraph'
CLI_RELATIVE_PATH = 'buildgraph_app/cli.py'
STAGE_INDICES = (0, 1, 2, 3)
CLASSIFICATION = 'supplementary_persisted_data_upgrade_robustness'

CASES = ({'id': 'buildgraph-upgrade-cache-plan-receipt',
  'requirements': ['B1-persist',
                   'B2-invalidate',
                   'B2-durable',
                   'B3-plan',
                   'B3-commit',
                   'B3-retry',
                   'B4-policy',
                   'B4-integrate'],
  'classification': 'supplementary_persisted_data_upgrade_robustness',
  'contract_basis': {'existing_command_requirements': ['Source edits clear transitive caches; '
                                                       'priority-only edits preserve them.',
                                                       'A plan builds dirty dependencies before '
                                                       'dependents, and a successful commit receipt '
                                                       'replays without reapplying after later edits.',
                                                       'Milestone4 policy/API integration preserves '
                                                       'earlier command behavior and exports schema2.'],
                     'robustness_bonus': ['The frozen primary evaluation starts each scenario with a '
                                          'fresh database. Opening a database written by a different '
                                          'selected milestone implementation is an additional, '
                                          'post-freeze compatibility test.',
                                          'These stories require historical version increments to '
                                          'remain continuous across milestone upgrades. Version '
                                          'persistence is explicit once milestone3 introduces it; '
                                          'preserving counts of earlier operations across schema '
                                          'versions is a robustness assumption, not a retroactive '
                                          'primary requirement.',
                                          'The expected trace preserves existing source definitions, '
                                          'dependency edges, cached artifacts and successful receipts; '
                                          'it does not require historical audit-event backfilling.'],
                     'primary_score_effect': 'None. Report separately; a cross-version failure alone '
                                             'does not overturn frozen primary acceptance.'},
  'steps': [{'stage_index': 0,
             'command': {'op': 'put', 'id': 'root', 'source': 'core-v1', 'priority': 2, 'deps': []}},
            {'stage_index': 0,
             'command': {'op': 'put', 'id': 'app', 'source': 'app-v1', 'priority': 1, 'deps': ['root']}},
            {'stage_index': 0, 'command': {'op': 'list'}},
            {'stage_index': 1, 'command': {'op': 'build', 'id': 'root'}},
            {'stage_index': 1, 'command': {'op': 'build', 'id': 'app'}},
            {'stage_index': 1,
             'command': {'op': 'put', 'id': 'root', 'source': 'core-v2', 'priority': 2, 'deps': []}},
            {'stage_index': 1, 'command': {'op': 'list'}},
            {'stage_index': 2, 'command': {'op': 'plan', 'targets': ['app']}},
            {'stage_index': 2,
             'command': {'op': 'commit', 'key': 'release', 'version': 5, 'targets': ['app']}},
            {'stage_index': 2,
             'command': {'op': 'put', 'id': 'root', 'source': 'core-v2', 'priority': 3, 'deps': []}},
            {'stage_index': 2,
             'command': {'op': 'commit', 'key': 'release', 'version': 5, 'targets': ['app']}},
            {'stage_index': 3, 'command': {'op': 'build', 'id': 'app'}},
            {'stage_index': 3,
             'command': {'op': 'commit', 'key': 'release', 'version': 5, 'targets': ['app']}},
            {'stage_index': 3, 'command': {'op': 'policy'}},
            {'stage_index': 3, 'command': {'op': 'export'}}],
  'expected_outputs': [{'ok': 'put', 'id': 'root', 'changed': True},
                       {'ok': 'put', 'id': 'app', 'changed': True},
                       [{'id': 'app',
                         'source': 'app-v1',
                         'priority': 1,
                         'deps': ['root'],
                         'digest': None},
                        {'id': 'root', 'source': 'core-v1', 'priority': 2, 'deps': [], 'digest': None}],
                       {'id': 'root',
                        'digest': '5c1f4264b0b3e126cc2b8c93274ddf4e4ae560fd91bdf707aa974656ce6cf7b1',
                        'cached': False},
                       {'id': 'app',
                        'digest': 'f7da5765d357c9963b50b6ea88ad6009cfbb9e885c3ac0d7596cddb3f31f2c21',
                        'cached': False},
                       {'ok': 'put', 'id': 'root', 'changed': True},
                       [{'id': 'app',
                         'source': 'app-v1',
                         'priority': 1,
                         'deps': ['root'],
                         'digest': None},
                        {'id': 'root', 'source': 'core-v2', 'priority': 2, 'deps': [], 'digest': None}],
                       {'order': ['root', 'app'], 'version': 5},
                       {'key': 'release',
                        'built': ['root', 'app'],
                        'version': 6,
                        'digests': {'app': '3e4659896a0756796ab5e3d8662c8d5e1d0d8cd5302e84d54d7621b0d27e5957'}},
                       {'ok': 'put', 'id': 'root', 'changed': True},
                       {'key': 'release',
                        'built': ['root', 'app'],
                        'version': 6,
                        'digests': {'app': '3e4659896a0756796ab5e3d8662c8d5e1d0d8cd5302e84d54d7621b0d27e5957'}},
                       {'id': 'app',
                        'digest': '3e4659896a0756796ab5e3d8662c8d5e1d0d8cd5302e84d54d7621b0d27e5957',
                        'cached': True},
                       {'key': 'release',
                        'built': ['root', 'app'],
                        'version': 6,
                        'digests': {'app': '3e4659896a0756796ab5e3d8662c8d5e1d0d8cd5302e84d54d7621b0d27e5957'}},
                       {'api': 2, 'schema': 2, 'max_import': 6},
                       {'schema': 2,
                        'targets': [{'id': 'app', 'source': 'app-v1', 'priority': 1, 'deps': ['root']},
                                    {'id': 'root', 'source': 'core-v2', 'priority': 3, 'deps': []}]}],
  'required_contract_vs_extra_assumptions': {'established_command_contract': ['Source edits clear '
                                                                              'transitive caches; '
                                                                              'priority-only edits '
                                                                              'preserve them.',
                                                                              'A plan builds dirty '
                                                                              'dependencies before '
                                                                              'dependents, and a '
                                                                              'successful commit '
                                                                              'receipt replays without '
                                                                              'reapplying after later '
                                                                              'edits.',
                                                                              'Milestone4 policy/API '
                                                                              'integration preserves '
                                                                              'earlier command behavior '
                                                                              'and exports schema2.'],
                                             'additional_robustness_assumptions': ['The frozen primary '
                                                                                   'evaluation starts '
                                                                                   'each scenario with '
                                                                                   'a fresh database. '
                                                                                   'Opening a database '
                                                                                   'written by a '
                                                                                   'different selected '
                                                                                   'milestone '
                                                                                   'implementation is '
                                                                                   'an additional, '
                                                                                   'post-freeze '
                                                                                   'compatibility test.',
                                                                                   'These stories '
                                                                                   'require historical '
                                                                                   'version increments '
                                                                                   'to remain '
                                                                                   'continuous across '
                                                                                   'milestone upgrades. '
                                                                                   'Version persistence '
                                                                                   'is explicit once '
                                                                                   'milestone3 '
                                                                                   'introduces it; '
                                                                                   'preserving counts '
                                                                                   'of earlier '
                                                                                   'operations across '
                                                                                   'schema versions is '
                                                                                   'a robustness '
                                                                                   'assumption, not a '
                                                                                   'retroactive primary '
                                                                                   'requirement.',
                                                                                   'The expected trace '
                                                                                   'preserves existing '
                                                                                   'source definitions, '
                                                                                   'dependency edges, '
                                                                                   'cached artifacts '
                                                                                   'and successful '
                                                                                   'receipts; it does '
                                                                                   'not require '
                                                                                   'historical '
                                                                                   'audit-event '
                                                                                   'backfilling.'],
                                             'historical_version_anchor': {'first_public_version_step': 7,
                                                                           'expected_version_at_entry': 5,
                                                                           'effective_mutation_steps_before_public_versioning': [0,
                                                                                                                                 1,
                                                                                                                                 3,
                                                                                                                                 4,
                                                                                                                                 5],
                                                                           'direct_counter_dependent_steps': [7,
                                                                                                              8,
                                                                                                              10,
                                                                                                              12],
                                                                           'downstream_receipt_or_cache_checks_dependent_on_first_commit': [11],
                                                                           'qualification': 'These '
                                                                                            'exact '
                                                                                            'numeric '
                                                                                            'versions '
                                                                                            'require '
                                                                                            'continuity '
                                                                                            'of '
                                                                                            'internal '
                                                                                            'version '
                                                                                            'metadata '
                                                                                            'before it '
                                                                                            'became an '
                                                                                            'explicitly '
                                                                                            'exposed '
                                                                                            'milestone-three '
                                                                                            'API. A '
                                                                                            'difference '
                                                                                            'here alone '
                                                                                            'is not an '
                                                                                            'established '
                                                                                            'frozen-contract '
                                                                                            'bug.'},
                                             'counter_independent_observations': 'Stage0 graph '
                                                                                 'definitions; stage1 '
                                                                                 'cache digests and '
                                                                                 'transitive '
                                                                                 'invalidation; stage4 '
                                                                                 'policy and exported '
                                                                                 'definitions.',
                                             'audit_history_policy': 'No assertion about synthesizing '
                                                                     'or backfilling audit history from '
                                                                     'earlier code versions.'}},
 {'id': 'buildgraph-upgrade-import-preserves-old-receipt',
  'requirements': ['B1-persist',
                   'B1-remove',
                   'B2-durable',
                   'B3-commit',
                   'B3-retry',
                   'B4-migrate',
                   'B4-integrate'],
  'classification': 'supplementary_persisted_data_upgrade_robustness',
  'contract_basis': {'existing_command_requirements': ['A first commit with an empty build plan still '
                                                       'persists a receipt and advances version once.',
                                                       'Import replaces the graph but preserves old '
                                                       'successful receipts, so replay can succeed '
                                                       'after its old target has disappeared.',
                                                       'Equivalent legacy/current manifests replay one '
                                                       'import receipt, and replay must not discard a '
                                                       'subsequently built cache.'],
                     'robustness_bonus': ['The frozen primary evaluation starts each scenario with a '
                                          'fresh database. Opening a database written by a different '
                                          'selected milestone implementation is an additional, '
                                          'post-freeze compatibility test.',
                                          'These stories require historical version increments to '
                                          'remain continuous across milestone upgrades. Version '
                                          'persistence is explicit once milestone3 introduces it; '
                                          'preserving counts of earlier operations across schema '
                                          'versions is a robustness assumption, not a retroactive '
                                          'primary requirement.',
                                          'The expected trace preserves existing source definitions, '
                                          'dependency edges, cached artifacts and successful receipts; '
                                          'it does not require historical audit-event backfilling.'],
                     'primary_score_effect': 'None. Report separately; a cross-version failure alone '
                                             'does not overturn frozen primary acceptance.'},
  'steps': [{'stage_index': 0,
             'command': {'op': 'put', 'id': 'seed', 'source': 'seed-body', 'priority': -1, 'deps': []}},
            {'stage_index': 0,
             'command': {'op': 'put', 'id': 'spare', 'source': 'spare-body', 'priority': 0, 'deps': []}},
            {'stage_index': 0, 'command': {'op': 'list'}},
            {'stage_index': 1, 'command': {'op': 'build', 'id': 'seed'}},
            {'stage_index': 1, 'command': {'op': 'build', 'id': 'spare'}},
            {'stage_index': 1, 'command': {'op': 'remove', 'id': 'spare'}},
            {'stage_index': 2,
             'command': {'op': 'commit', 'key': 'old-receipt', 'version': 5, 'targets': ['seed']}},
            {'stage_index': 2,
             'command': {'op': 'put',
                         'id': 'seed',
                         'source': 'changed-body',
                         'priority': -1,
                         'deps': []}},
            {'stage_index': 2, 'command': {'op': 'plan', 'targets': ['seed']}},
            {'stage_index': 3,
             'command': {'op': 'import',
                         'key': 'migration',
                         'manifest': {'schema': 1,
                                      'targets': [{'name': 'migrated',
                                                   'body': 'new-body',
                                                   'needs': []}]}}},
            {'stage_index': 3,
             'command': {'op': 'commit', 'key': 'old-receipt', 'version': 5, 'targets': ['seed']}},
            {'stage_index': 3,
             'command': {'op': 'import',
                         'key': 'migration',
                         'manifest': {'schema': 2,
                                      'targets': [{'id': 'migrated',
                                                   'source': 'new-body',
                                                   'priority': 0,
                                                   'deps': []}]}}},
            {'stage_index': 3, 'command': {'op': 'build', 'id': 'migrated'}},
            {'stage_index': 3,
             'command': {'op': 'import',
                         'key': 'migration',
                         'manifest': {'schema': 1,
                                      'targets': [{'name': 'migrated',
                                                   'body': 'new-body',
                                                   'needs': []}]}}},
            {'stage_index': 3, 'command': {'op': 'list'}},
            {'stage_index': 3, 'command': {'op': 'export'}}],
  'expected_outputs': [{'ok': 'put', 'id': 'seed', 'changed': True},
                       {'ok': 'put', 'id': 'spare', 'changed': True},
                       [{'id': 'seed',
                         'source': 'seed-body',
                         'priority': -1,
                         'deps': [],
                         'digest': None},
                        {'id': 'spare',
                         'source': 'spare-body',
                         'priority': 0,
                         'deps': [],
                         'digest': None}],
                       {'id': 'seed',
                        'digest': 'b045f645d6a8f1d08303096b968fd297aa0516a152e8d09a8b6dbaa733cdb165',
                        'cached': False},
                       {'id': 'spare',
                        'digest': 'e574e30cedb663a875eba6c285726b345a11c1505d128699396e50c80006f75b',
                        'cached': False},
                       {'ok': 'removed', 'id': 'spare'},
                       {'key': 'old-receipt',
                        'built': [],
                        'version': 6,
                        'digests': {'seed': 'b045f645d6a8f1d08303096b968fd297aa0516a152e8d09a8b6dbaa733cdb165'}},
                       {'ok': 'put', 'id': 'seed', 'changed': True},
                       {'order': ['seed'], 'version': 7},
                       {'ok': 'imported', 'key': 'migration', 'count': 1, 'version': 8},
                       {'key': 'old-receipt',
                        'built': [],
                        'version': 6,
                        'digests': {'seed': 'b045f645d6a8f1d08303096b968fd297aa0516a152e8d09a8b6dbaa733cdb165'}},
                       {'ok': 'imported', 'key': 'migration', 'count': 1, 'version': 8},
                       {'id': 'migrated',
                        'digest': '0d1da3b44269f862db4c9134bee1d186e01865a7535e2e49a4947082e1b402a2',
                        'cached': False},
                       {'ok': 'imported', 'key': 'migration', 'count': 1, 'version': 8},
                       [{'id': 'migrated',
                         'source': 'new-body',
                         'priority': 0,
                         'deps': [],
                         'digest': '0d1da3b44269f862db4c9134bee1d186e01865a7535e2e49a4947082e1b402a2'}],
                       {'schema': 2,
                        'targets': [{'id': 'migrated',
                                     'source': 'new-body',
                                     'priority': 0,
                                     'deps': []}]}],
  'required_contract_vs_extra_assumptions': {'established_command_contract': ['A first commit with an '
                                                                              'empty build plan still '
                                                                              'persists a receipt and '
                                                                              'advances version once.',
                                                                              'Import replaces the '
                                                                              'graph but preserves old '
                                                                              'successful receipts, so '
                                                                              'replay can succeed after '
                                                                              'its old target has '
                                                                              'disappeared.',
                                                                              'Equivalent '
                                                                              'legacy/current manifests '
                                                                              'replay one import '
                                                                              'receipt, and replay must '
                                                                              'not discard a '
                                                                              'subsequently built '
                                                                              'cache.'],
                                             'additional_robustness_assumptions': ['The frozen primary '
                                                                                   'evaluation starts '
                                                                                   'each scenario with '
                                                                                   'a fresh database. '
                                                                                   'Opening a database '
                                                                                   'written by a '
                                                                                   'different selected '
                                                                                   'milestone '
                                                                                   'implementation is '
                                                                                   'an additional, '
                                                                                   'post-freeze '
                                                                                   'compatibility test.',
                                                                                   'These stories '
                                                                                   'require historical '
                                                                                   'version increments '
                                                                                   'to remain '
                                                                                   'continuous across '
                                                                                   'milestone upgrades. '
                                                                                   'Version persistence '
                                                                                   'is explicit once '
                                                                                   'milestone3 '
                                                                                   'introduces it; '
                                                                                   'preserving counts '
                                                                                   'of earlier '
                                                                                   'operations across '
                                                                                   'schema versions is '
                                                                                   'a robustness '
                                                                                   'assumption, not a '
                                                                                   'retroactive primary '
                                                                                   'requirement.',
                                                                                   'The expected trace '
                                                                                   'preserves existing '
                                                                                   'source definitions, '
                                                                                   'dependency edges, '
                                                                                   'cached artifacts '
                                                                                   'and successful '
                                                                                   'receipts; it does '
                                                                                   'not require '
                                                                                   'historical '
                                                                                   'audit-event '
                                                                                   'backfilling.'],
                                             'historical_version_anchor': {'first_public_version_step': 6,
                                                                           'expected_version_at_entry': 5,
                                                                           'effective_mutation_steps_before_public_versioning': [0,
                                                                                                                                 1,
                                                                                                                                 3,
                                                                                                                                 4,
                                                                                                                                 5],
                                                                           'direct_counter_dependent_steps': [6,
                                                                                                              8,
                                                                                                              9,
                                                                                                              10,
                                                                                                              11,
                                                                                                              13],
                                                                           'downstream_receipt_or_cache_checks_dependent_on_first_commit': [10],
                                                                           'qualification': 'These '
                                                                                            'exact '
                                                                                            'numeric '
                                                                                            'versions '
                                                                                            'require '
                                                                                            'continuity '
                                                                                            'of '
                                                                                            'internal '
                                                                                            'version '
                                                                                            'metadata '
                                                                                            'before it '
                                                                                            'became an '
                                                                                            'explicitly '
                                                                                            'exposed '
                                                                                            'milestone-three '
                                                                                            'API. A '
                                                                                            'difference '
                                                                                            'here alone '
                                                                                            'is not an '
                                                                                            'established '
                                                                                            'frozen-contract '
                                                                                            'bug.'},
                                             'counter_independent_observations': 'Stage0 definitions; '
                                                                                 'stage1 cache digests '
                                                                                 'and removal; imported '
                                                                                 'graph contents; '
                                                                                 'normalized import '
                                                                                 'replay preserves '
                                                                                 'later cache; final '
                                                                                 'schema2 export.',
                                             'audit_history_policy': 'No assertion about synthesizing '
                                                                     'or backfilling audit history from '
                                                                     'earlier code versions.'}})
