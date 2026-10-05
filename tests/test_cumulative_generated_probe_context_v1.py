"""Fixed-peer source algebra and real inert Git; no model/Engine observations."""
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from gossip_harness import cumulative_generated_probe_context_v1 as ctx
from gossip_harness import cumulative_study_controller_v2 as study
from gossip_harness.gitstore import GitStore, _run


def fixture(arm='S16-G', base=None):
    files = {'shared.txt': b'immutable', **{f'pkg/{p}/old.txt': b'base' for p in ctx.PACKAGES}}
    if base is None:
        snapshot = tuple(sorted(files.items())); modes = tuple((p, '100644') for p, _ in snapshot)
        base = ctx.Base('a'*40, ctx.source_tree_oid(snapshot, modes), snapshot, modes)
    builders = study.actors_for(arm)[:-4]
    proposals = tuple(ctx.Proposal(a, 'eligible', base.sha256, study.digest(['proposal', a]),
        ((f'pkg/{study.package_for(a)}/old.txt', a),)) for a in builders)
    ranks = tuple(ctx.Ranking(p, study.REVIEWERS[i], study.digest(['review', p]), 'completed',
        tuple(a for a in builders if study.package_for(a) == p)) for i, p in enumerate(ctx.PACKAGES))
    return ctx.Generation('cohort', 'trajectory', 'M2', 0, arm, base,
                          tuple((p, (f'pkg/{p}',)) for p in ctx.PACKAGES), proposals, ranks)


def proposal_change(generation, actor, **changes):
    return replace(generation, proposals=tuple(replace(p, **changes) if p.actor == actor else p
                                              for p in generation.proposals))


class GeneratedProbeContextTests(unittest.TestCase):
    def test_complete_large_and_small_census_preserves_all_logical_exposures(self):
        for arm, builders, vectors in [('S4-G', 4, 1), ('S16-G', 16, 13), ('O16-G', 16, 13)]:
            with self.subTest(arm=arm):
                rows = ctx.census(fixture(arm))
                self.assertEqual(rows['planned_builders'], builders)
                self.assertEqual(len(rows['base_overlays']), builders)
                self.assertEqual(rows['logical_contextual_exposures'], builders)
                self.assertEqual(rows['distinct_contextual_vectors'], vectors)
                self.assertTrue(rows['mandatory_fresh_merged_retest'])
                self.assertIs(rows['acceptance_authority'], False)
                self.assertIs(rows['dispatch_authority'], False)

    def test_unranked_eligible_alternatives_still_appear_but_cannot_be_selected(self):
        gen = fixture(); gen = replace(gen, rankings=tuple(replace(r, actors=r.actors[:1]) for r in gen.rankings))
        self.assertEqual(len(ctx.census(gen)['contextual']), 16)
        with self.assertRaisesRegex(ValueError, 'not_endorsed'):
            ctx.compose(gen, 'merged', selected=('B02', 'B05', 'B09', 'B13'))

    def test_every_context_holds_all_other_packages_at_one_anchor(self):
        gen = fixture(); anchor = dict(ctx.compose(gen, 'anchor').files)
        for proposal in gen.proposals:
            result = dict(ctx.compose(gen, 'contextual', actor=proposal.actor).files)
            target = study.package_for(proposal.actor)
            for package in ctx.PACKAGES:
                path = f'pkg/{package}/old.txt'
                self.assertEqual(result[path], proposal.actor.encode() if package == target else anchor[path])
            self.assertEqual(result['shared.txt'], b'immutable')

    def test_restore_base_removes_anchor_additions_and_restores_anchor_deletions(self):
        gen = fixture()
        gen = proposal_change(gen, 'B01', changes=(('pkg/catalog/added.txt', 'anchor only'),
                                                  ('pkg/catalog/old.txt', None)))
        gen = proposal_change(gen, 'B02', changes=())
        anchor = dict(ctx.compose(gen, 'anchor').files)
        self.assertIn('pkg/catalog/added.txt', anchor); self.assertNotIn('pkg/catalog/old.txt', anchor)
        alternate = dict(ctx.compose(gen, 'contextual', actor='B02').files)
        self.assertNotIn('pkg/catalog/added.txt', alternate)
        self.assertEqual(alternate['pkg/catalog/old.txt'], b'base')
        self.assertEqual(alternate['pkg/ingestion/old.txt'], b'B05')

    def test_candidate_deletion_and_unchanged_source_survive_composition(self):
        gen = proposal_change(fixture(), 'B02', changes=(('pkg/catalog/old.txt', None),))
        result = ctx.compose(gen, 'contextual', actor='B02')
        self.assertNotIn('pkg/catalog/old.txt', dict(result.files))
        self.assertEqual(ctx.delta_from_base(gen, result)['pkg/catalog/old.txt'], None)
        self.assertNotIn('shared.txt', ctx.delta_from_base(gen, result))

    def test_base_overlay_does_not_invent_new_peer_capabilities(self):
        gen = fixture(); local = dict(ctx.compose(gen, 'base-overlay', actor='B02').files)
        self.assertEqual(local['pkg/catalog/old.txt'], b'B02')
        self.assertEqual(local['pkg/query/old.txt'], b'base')
        self.assertNotEqual(local, dict(ctx.compose(gen, 'contextual', actor='B02').files))

    def test_missing_anchor_retains_eligible_unattempted_rows_and_base(self):
        gen = fixture(); original_base = gen.base
        gen = replace(gen, rankings=(replace(gen.rankings[0], actors=(), disposition='unknown'), *gen.rankings[1:]))
        census = ctx.census(gen)
        self.assertIsNone(census['anchor']); self.assertEqual(census['contextual'], [])
        self.assertEqual(census['unavailable_contextual_actors'], list(study.actors_for('S16-G')[:-4]))
        self.assertEqual(census['missing_anchor_packages'], ['catalog'])
        self.assertEqual(census['planned_contextual_exposures'], 16)
        self.assertEqual(gen.base, original_base)
        with self.assertRaisesRegex(ValueError, 'anchor_unavailable'): ctx.compose(gen, 'contextual', actor='B02')

    def test_failed_stopped_unknown_roles_remain_in_denominator(self):
        gen = fixture(); ranks = (replace(gen.rankings[0], actors=('B01',)), *gen.rankings[1:])
        gen = replace(gen, rankings=ranks)
        for actor, disposition in [('B02', 'failed'), ('B03', 'stopped'), ('B04', 'unknown')]:
            gen = proposal_change(gen, actor, disposition=disposition, changes=())
        census = ctx.census(gen)
        self.assertEqual(census['planned_builders'], 16); self.assertEqual(census['eligible_builders'], 13)
        self.assertEqual([r['disposition'] for r in census['generation']['proposals'][1:4]], ['failed', 'stopped', 'unknown'])
        with self.assertRaises(ValueError): ctx.compose(gen, 'contextual', actor='B02')

    def test_omitted_duplicate_and_reordered_roles_are_rejected(self):
        gen = fixture()
        for proposals in [gen.proposals[:-1], (gen.proposals[0],)*16, tuple(reversed(gen.proposals))]:
            with self.subTest(proposals=proposals), self.assertRaisesRegex(ValueError, 'builder_census'):
                replace(gen, proposals=proposals)
        with self.assertRaisesRegex(ValueError, 'reviewer_census'): replace(gen, rankings=gen.rankings[:-1])

    def test_wrong_base_scope_and_cross_package_endorsement_are_rejected(self):
        gen = fixture()
        with self.assertRaisesRegex(ValueError, 'base_substitution'): proposal_change(gen, 'B01', base_sha256='0'*64)
        with self.assertRaisesRegex(ValueError, 'package_scope'):
            proposal_change(gen, 'B01', changes=(('shared.txt', 'forged'),))
        with self.assertRaisesRegex(ValueError, 'not_eligible_in_package'):
            replace(gen, rankings=(replace(gen.rankings[0], actors=('B05',)), *gen.rankings[1:]))
        with self.assertRaisesRegex(ValueError, 'unique_ordered'): replace(gen.rankings[0], actors=('B01', 'B01'))

    def test_nonadjacent_prefix_scope_and_file_collisions_are_rejected(self):
        gen = fixture(); scopes = list(gen.scopes); scopes[0] = ('catalog', ('pkg/a', 'pkg/a-b', 'pkg/a/nested'))
        with self.assertRaisesRegex(ValueError, 'overlapping'): replace(gen, scopes=tuple(scopes))
        with self.assertRaisesRegex(ValueError, 'collision'):
            replace(gen.base, files=(('a', b'x'), ('a-b', b'x'), ('a/x', b'x')))

    def test_unsafe_paths_mutable_sequences_and_boolean_generation_rejected(self):
        gen = fixture()
        for path in ['../escape', '/absolute', 'a//b', 'a/.GIT/config']:
            with self.subTest(path=path), self.assertRaises(ValueError):
                replace(gen.proposals[0], changes=((path, 'x'),))
        for bad in [True, -1, 3]:
            with self.subTest(generation=bad), self.assertRaises(ValueError): replace(gen, generation=bad)
        with self.assertRaises(ValueError): replace(gen, proposals=list(gen.proposals))
        with self.assertRaises(ValueError): replace(gen.base, files=list(gen.base.files))

    def test_rank_order_changes_anchor_and_generation_identity(self):
        gen = fixture(); other = replace(gen, rankings=(replace(gen.rankings[0], actors=('B02', 'B01')), *gen.rankings[1:]))
        self.assertNotEqual(gen.sha256, other.sha256)
        self.assertEqual(ctx.compose(other, 'anchor').vector[0], 'B02')
        self.assertEqual(gen.anchor_vector[0], 'B01')

    def test_equal_source_vectors_do_not_collapse_logical_contexts_or_grant_reuse(self):
        gen = fixture('S4-G'); rows = [ctx.compose(gen, 'contextual', actor=p.actor) for p in gen.proposals]
        self.assertEqual(len({r.files for r in rows}), 1)
        self.assertEqual(len({r.sha256 for r in rows}), 4)
        merged = ctx.compose(gen, 'merged', selected=gen.anchor_vector)
        self.assertEqual(merged.files, rows[0].files); self.assertNotEqual(merged.sha256, rows[0].sha256)

    def test_simultaneous_choices_can_create_a_new_cross_package_regression(self):
        gen = fixture()
        # Host-authored counterexample: each changed package is compatible with
        # the anchor, but their combination is incompatible. No candidate runs.
        gen = proposal_change(gen, 'B01', changes=(('pkg/catalog/old.txt', '0'),))
        gen = proposal_change(gen, 'B02', changes=(('pkg/catalog/old.txt', '1'),))
        gen = proposal_change(gen, 'B05', changes=(('pkg/ingestion/old.txt', '0'),))
        gen = proposal_change(gen, 'B06', changes=(('pkg/ingestion/old.txt', '1'),))
        def predicate(context):
            files = dict(context.files)
            return int(files['pkg/catalog/old.txt']) + int(files['pkg/ingestion/old.txt']) <= 1
        self.assertTrue(predicate(ctx.compose(gen, 'contextual', actor='B02')))
        self.assertTrue(predicate(ctx.compose(gen, 'contextual', actor='B06')))
        merged = ctx.compose(gen, 'merged', selected=('B02', 'B06', 'B09', 'B13'))
        self.assertFalse(predicate(merged))
        self.assertIs(merged.record()['acceptance_authority'], False)

    def test_incomplete_choice_and_context_substitution_cannot_materialize(self):
        gen = fixture(); context = ctx.compose(gen, 'contextual', actor='B02')
        with self.assertRaisesRegex(ValueError, 'complete_simultaneous'): ctx.compose(gen, 'merged', selected=('B02',))
        for changed in [replace(context, generation_sha256='0'*64),
                        replace(context, vector=gen.anchor_vector),
                        replace(context, files=gen.base.files), replace(context, target_actor='B03')]:
            with self.subTest(context=changed), self.assertRaisesRegex(ValueError, 'substitution'):
                ctx.delta_from_base(gen, changed)

    def test_returned_records_cannot_mutate_the_frozen_generation(self):
        gen = fixture(); identity = gen.sha256; result = ctx.census(gen)
        result['generation']['proposals'][0]['changes']['pkg/catalog/old.txt'] = 'changed'
        result['anchor']['vector'][0] = 'B02'
        self.assertEqual(gen.sha256, identity); self.assertEqual(gen.anchor_vector[0], 'B01')


class GeneratedProbeContextGitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='probe-context-'); self.addCleanup(self.temp.cleanup)
        source = fixture().base.files
        self.store = GitStore.create(Path(self.temp.name)/'source.git', {p: b.decode() for p, b in source})
        self.base = ctx.capture_base(self.store); self.gen = fixture(base=self.base)

    def test_real_base_overlay_anchor_substitution_and_merge_are_unapproved(self):
        for phase, args in [('base-overlay', {'actor': 'B02'}), ('anchor', {}),
                            ('contextual', {'actor': 'B02'}),
                            ('merged', {'selected': ('B02', 'B06', 'B10', 'B14')})]:
            with self.subTest(phase=phase):
                context = ctx.compose(self.gen, phase, **args)
                commit = self.store.propose(ctx.delta_from_base(self.gen, context), self.base.commit_oid,
                                            message=context.sha256)
                evidence = ctx.verify_materialized(self.store, self.gen, context, commit)
                self.assertEqual(evidence['source_sha256'], context.record()['source_sha256'])
                self.assertEqual(evidence['commit_oid'], commit)
                self.assertIs(evidence['acceptance_authority'], False)
                self.assertIs(evidence['execution_reuse_authority'], False)
                self.assertEqual(self.store.head(), self.base.commit_oid)

    def test_real_git_deletion_and_anchor_only_addition_do_not_leak(self):
        gen = proposal_change(self.gen, 'B01', changes=(('pkg/catalog/new.txt', 'anchor'), ('pkg/catalog/old.txt', None)))
        gen = proposal_change(gen, 'B02', changes=())
        alternate = ctx.compose(gen, 'contextual', actor='B02')
        commit = self.store.propose(ctx.delta_from_base(gen, alternate), self.base.commit_oid)
        ctx.verify_materialized(self.store, gen, alternate, commit)
        actual = self.store.read_files(commit)
        self.assertNotIn('pkg/catalog/new.txt', actual); self.assertEqual(actual['pkg/catalog/old.txt'], 'base')

    def test_actual_wrong_peer_tree_is_rejected_even_with_the_requested_target(self):
        target = ctx.compose(self.gen, 'contextual', actor='B02')
        wrong = ctx.compose(self.gen, 'merged', selected=('B02', 'B06', 'B09', 'B13'))
        commit = self.store.propose(ctx.delta_from_base(self.gen, wrong), self.base.commit_oid)
        with self.assertRaisesRegex(ValueError, 'materialized_context_source_differs'):
            ctx.verify_materialized(self.store, self.gen, target, commit)

    def test_equal_tree_different_commits_retain_distinct_git_binding(self):
        context = ctx.compose(self.gen, 'anchor'); changes = ctx.delta_from_base(self.gen, context)
        one = self.store.propose(changes, self.base.commit_oid, message='one')
        two = self.store.propose(changes, self.base.commit_oid, message='two')
        a = ctx.verify_materialized(self.store, self.gen, context, one)
        b = ctx.verify_materialized(self.store, self.gen, context, two)
        self.assertNotEqual(a['commit_oid'], b['commit_oid']); self.assertEqual(a['tree_oid'], b['tree_oid'])
        self.assertIs(a['execution_reuse_authority'], False)

    def test_identical_bytes_with_an_unproposed_executable_bit_are_rejected(self):
        context = ctx.compose(self.gen, 'anchor')
        commit = self.store.propose(ctx.delta_from_base(self.gen, context), self.base.commit_oid)
        with self.store._checkout(commit) as checkout:
            _run(checkout, 'update-index', '--chmod=+x', 'pkg/catalog/old.txt')
            _run(checkout, 'commit', '-m', 'unproposed mode change')
            wrong = _run(checkout, 'rev-parse', 'HEAD').stdout.decode().strip()
            self.store._git('fetch', '--no-tags', str(checkout), f'{wrong}:refs/harness/proposals/{wrong}')
        self.assertEqual(self.store.read_files(commit), self.store.read_files(wrong))
        with self.assertRaisesRegex(ValueError, 'materialized_context_tree_differs'):
            ctx.verify_materialized(self.store, self.gen, context, wrong)

    def test_inherited_executable_mode_and_git_directory_order_round_trip(self):
        with self.store._checkout(self.base.commit_oid) as checkout:
            _run(checkout, 'update-index', '--chmod=+x', 'pkg/catalog/old.txt')
            _run(checkout, 'commit', '-m', 'inherited executable')
            head = _run(checkout, 'rev-parse', 'HEAD').stdout.decode().strip()
            self.store._git('fetch', '--no-tags', str(checkout), f'{head}:refs/harness/proposals/{head}')
        self.store._git('update-ref', 'refs/heads/accepted', head, self.base.commit_oid)
        base = ctx.capture_base(self.store); gen = fixture(base=base)
        changes = tuple(sorted({'pkg/catalog/old.txt': 'changed executable', 'pkg/catalog/a.txt': 'file',
                                'pkg/catalog/a/file.txt': 'nested', 'pkg/catalog/a-.txt': 'sort first',
                                'pkg/catalog/é.txt': 'unicode'}.items()))
        gen = proposal_change(gen, 'B01', changes=changes)
        context = ctx.compose(gen, 'anchor')
        commit = self.store.propose(ctx.delta_from_base(gen, context), base.commit_oid)
        result = ctx.verify_materialized(self.store, gen, context, commit)
        self.assertEqual(result['tree_oid'], context.record()['tree_oid'])
        self.assertEqual(dict(context.modes)['pkg/catalog/old.txt'], '100755')
        self.assertEqual(dict(context.modes)['pkg/catalog/a/file.txt'], '100644')
        self.assertEqual(self.store.head(), head)
