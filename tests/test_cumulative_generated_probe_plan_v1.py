"""Declaration and real-Git binding controls; candidates are never imported."""
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_generated_probe_plan_v1 as plans
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness import cumulative_generated_probe_wire_v1 as wire
from gossip_harness import cumulative_study_controller_v2 as study
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore
from tests.test_cumulative_generated_probe_values_v2 import admitted, proposal


def release(milestone='M2', requirements=('M2-REFRESH',)):
    return study.Release(milestone, 'Public requirements: café 🌍', {'README.md': 'Public é'},
                         {'public.py': 'print("public")\n'}, ('python', 'public.py'),
                         ('public-check',), requirements)


def policy():
    return plans.ProbePolicy(wire.WireLimits(16384, 131072, 256, 16), 4096, 30, 10, 'fixed-seed')


def target(files=None, commit='a'*40, tree='b'*40, milestone='M2'):
    if files is None:
        files = {'solution.py': b'raise RuntimeError("must not import")\n'}
    subject = registry.Subject('cohort', 'trajectory', milestone, 'c'*64,
                               values.PRODUCT_SHA256, admission.source_sha256(files))
    return plans.ProbeTarget(subject, 0, 'candidate-1', 'anchor-1', 'candidate-context', commit, tree)


def declaration(*, proposed=None, public=None, subject=None, limits=None, layout=None):
    public = release() if public is None else public
    proposed = admitted(proposal()) if proposed is None else proposed
    return plans.ProbePlan(target() if subject is None else subject,
        study.canonical_payload(study.plain(asdict(public))), values.canonical(proposed),
        policy() if limits is None else limits, layout, tuple(plans.evaluator_sources().items()))


class GeneratedProbePlanTests(unittest.TestCase):
    def test_request_binds_exact_public_release_helpers_and_target_without_authority(self):
        public = release(); plan = declaration(public=public); request = plan.review_request()
        record = request['plan']
        self.assertEqual(record['release_sha256'], public.sha256)
        self.assertEqual(record['target']['candidate_id'], 'candidate-1')
        self.assertEqual(record['target']['context_id'], 'anchor-1')
        self.assertEqual(record['command'], ['python', '-I', '-B', '/checks/child_driver.py'])
        self.assertEqual({row['path'] for row in record['helper_manifest']}, {'child_driver.py', 'probe-request.json'})
        self.assertIs(record['acceptance_authority'], False)
        self.assertIs(record['dispatch_authority'], False)
        self.assertEqual(request['duties'], list(plans.DUTIES))
        self.assertNotIn('approved', request)

    def test_candidate_context_generation_and_merge_stage_change_identity(self):
        plan = declaration(); original = values.digest(plan.record())
        for change in ({'candidate_id': 'candidate-2'}, {'context_id': 'anchor-2'},
                       {'generation': 1}, {'stage': 'merged'}):
            with self.subTest(change=change):
                changed = replace(plan, target=replace(plan.target, **change))
                self.assertNotEqual(values.digest(changed.record()), original)

    def test_target_rejects_boolean_generation_short_git_and_wrong_contract(self):
        good = target()
        for change in ({'generation': True}, {'generation': -1}, {'commit_oid': 'main'},
                       {'tree_oid': 'HEAD'}, {'stage': 'accepted'}, {'candidate_id': '../other'}):
            with self.subTest(change=change), self.assertRaises(ValueError): replace(good, **change)
        with self.assertRaises(ValueError):
            replace(good, subject=replace(good.subject, requirements_sha256='0'*64))

    def test_limits_are_explicit_inside_existing_bounds_and_change_identity(self):
        good = policy()
        for change in ({'history_seconds': 301}, {'history_seconds': True}, {'control_seconds': 31},
                       {'history_seconds': 1}, {'stderr_bytes': 65537},
                       {'wire_limits': wire.WireLimits(131073, 524288, 256, 16)}):
            with self.subTest(change=change), self.assertRaises(ValueError): replace(good, **change)
        plan = declaration()
        self.assertNotEqual(values.digest(plan.record()),
                            values.digest(replace(plan, policy=replace(good, seed='another')).record()))

    def test_unreleased_requirement_and_milestone_mismatch_are_refused(self):
        plan = declaration(public=release(requirements=('M1-JOBS',)))
        with self.assertRaises(ValueError): plan.record()
        with self.assertRaises(ValueError): declaration(public=release('M3')).record()

    def test_only_capture_template_accepts_and_requires_layout(self):
        layout = plans.CaptureLayout(('m2/library.sqlite',), 'd'*64)
        with self.assertRaises(ValueError): declaration(layout=layout).record()
        probe = admitted(proposal('manifest-content-hash-v1'))
        public = release('M3', ('M3-BACKUP-RESTORE',))
        base = declaration(proposed=probe, public=public, subject=target(milestone='M3'))
        with self.assertRaises(ValueError): base.record()
        self.assertEqual(replace(base, layout=layout).record()['layout']['schema_sha256'], 'd'*64)
        early = declaration(proposed=probe, public=release('M2', ('M3-BACKUP-RESTORE',)), layout=layout)
        with self.assertRaisesRegex(ValueError, 'capture_template_not_released'): early.record()

    def test_capture_paths_reject_traversal_duplicates_and_unsorted_census(self):
        for paths in [('../m2/library.sqlite',), ('/tmp/m2/library.sqlite',),
                      ('m2/library.sqlite', 'm2/library.sqlite'), ('z', 'm2/library.sqlite'),
                      ('m2/../other', 'm2/library.sqlite'), ('m2/library.sqlite', 'm2\\escape')]:
            with self.subTest(paths=paths), self.assertRaises(ValueError): plans.CaptureLayout(paths, 'd'*64)

    def test_forged_probe_snapshot_cannot_enter_review_request(self):
        plan = declaration(); bad = json.loads(plan.admitted_raw); bad['acceptance_authority'] = True
        with self.assertRaises(ValueError): replace(plan, admitted_raw=values.canonical(bad)).review_request()
        with self.assertRaises(ValueError): replace(plan, admitted_raw=b' ' + plan.admitted_raw).record()

    def test_evaluator_pin_substitution_is_rejected(self):
        plan = declaration(); pins = list(plan.source_pins); pins[0] = (pins[0][0], '0'*64)
        with self.assertRaisesRegex(ValueError, 'evaluator_source_binding_changed'):
            replace(plan, source_pins=tuple(pins)).record()

    def test_plan_snapshots_are_independent_of_later_caller_mutation(self):
        public = release(); proposed = admitted(proposal()); plan = declaration(public=public, proposed=proposed)
        original = values.canonical(plan.record())
        public.files['README.md'] = 'mutated'
        proposed['parameters']['initial_text'] = 'mutated'
        record = plan.record(); record['probe']['parameters']['initial_text'] = 'mutated again'
        self.assertEqual(values.canonical(plan.record()), original)


class GeneratedProbePlanGitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='generated-probe-plan-git-')
        self.addCleanup(self.temp.cleanup)
        self.files = {'solution.py': 'raise RuntimeError("candidate must never import on host")\n',
                      'nested/data.txt': 'old\n', 'obsolete.txt': 'remove later\n'}
        self.store = GitStore.create(Path(self.temp.name) / 'source.git', self.files)

    def actual_target(self):
        commit = self.store.head(); tree = self.store._git('rev-parse', commit + '^{tree}')
        return target({k: v.encode() for k, v in self.store.read_files().items()}, commit, tree)

    def plan(self):
        return plans.prepare_plan(self.store, self.actual_target(), admitted(proposal()), release(), policy())

    def advance(self, changes):
        old = self.store.head(); offered = self.store.propose(changes, old)
        self.store._git('update-ref', 'refs/heads/accepted', offered, old)

    def test_real_complete_git_capture_and_review_request_without_candidate_import(self):
        plan = self.plan()
        self.assertEqual(plans.verify_current_source(self.store, plan), {k: v.encode() for k, v in self.files.items()})
        self.assertEqual(plan.review_request()['plan']['target']['commit_oid'], self.store.head())

    def test_moved_head_is_rejected_instead_of_using_cached_files(self):
        plan = self.plan(); self.advance({'nested/data.txt': 'changed\n'})
        with self.assertRaisesRegex(ValueError, 'registered_head_changed'):
            plans.verify_current_source(self.store, plan)

    def test_wrong_tree_or_full_source_hash_is_rejected(self):
        actual = self.actual_target()
        for wrong in [replace(actual, tree_oid='0'*40),
                      replace(actual, subject=replace(actual.subject, source_sha256='0'*64))]:
            with self.subTest(target=wrong), self.assertRaisesRegex(ValueError, 'complete_registered_source_differs'):
                plans.prepare_plan(self.store, wrong, admitted(proposal()), release(), policy())

    def test_new_context_captures_deletions_and_all_remaining_files(self):
        old = self.plan()
        self.advance({'obsolete.txt': None, 'nested/data.txt': 'new\n', 'new.txt': 'added\n'})
        fresh = self.plan(); files = plans.verify_current_source(self.store, fresh)
        self.assertNotIn('obsolete.txt', files)
        self.assertEqual(files['new.txt'], b'added\n')
        self.assertEqual(files['solution.py'], self.files['solution.py'].encode())
        self.assertNotEqual(fresh.target.subject.source_sha256, old.target.subject.source_sha256)

    def test_captured_dictionary_mutation_does_not_pollute_next_capture(self):
        plan = self.plan(); first = plans.verify_current_source(self.store, plan)
        first['solution.py'] = b'forged'; first['extra.py'] = b'forged'
        self.assertEqual(plans.verify_current_source(self.store, plan), {k: v.encode() for k, v in self.files.items()})


if __name__ == '__main__':
    unittest.main()
