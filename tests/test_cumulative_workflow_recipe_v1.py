"""Real recipe/source/original-review composition, without product execution.

Fixture decisions and admission callbacks are explicitly synthetic linkage
controls. They do not approve source semantics, purpose conversion, full scope,
prerequisites, a capsule, or product acceptance. Actual Git, protected review
journals, typed input files and fixture owners remain real and independently
checked here. Candidate fixture Python is never imported or executed.
"""
from collections import Counter
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness import candidate_journal_batch_read_v1 as batch
from gossip_harness import candidate_http_journal_v3 as stable
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_workflow_execution_v1 as execution
from gossip_harness import candidate_workflow_profile_v1 as profiles
from gossip_harness import candidate_workflow_review_v1 as review
from gossip_harness import cumulative_final_acceptance_v1 as final
from gossip_harness import cumulative_rehearsal_codec_v1 as codec
from gossip_harness.cumulative_rehearsal_export_v1 import InputWriter
from gossip_harness import cumulative_scope_source_v3 as scope
from gossip_harness import cumulative_workflow_exposure_v1 as exposure
from gossip_harness import cumulative_workflow_observation_recipe_v1 as factory
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore
from tests.test_candidate_workflow_review_v1 import COHORT, synthetic_delivery

ROOT = Path(__file__).resolve().parents[1]
MECHANISM_CONTEXT = {'fixture_only': True, 'semantic_approval': False}


def recipe_fixture(root, stack, kind, *, store=None, case_id=None):
    """Both real builders and .spec(), with all borrowed originals kept live.

    ``stack`` owns heads and journals, in the correct reverse-close order. The
    caller owns each constructed fixture owner and must close it before stack.
    The inert boundary mapping is solely a constructor/transport control; it is
    never offered as valid physical placement or actual semantic approval.
    """
    if kind not in ('workflow', 'workflow_inspection'):
        raise ValueError('Closed workflow fixture kind required')
    root = Path(root).resolve()
    root.mkdir()
    if store is None:
        store = GitStore.create(root/'candidate.git', {
            'library/__init__.py': 'def solve(payload):\n    raise RuntimeError("fixture only; never executed")\n',
            exposure.cli.RELEASE_PATH: exposure.cli.text(), exposure.RELEASE_PATH: exposure.text()})
    commit = store.head()
    tree, files = review.capture_git_source(store, commit)
    value = (profiles.profile_for(case_id or profiles.CASE_IDS[0], 'independent_acceptance')
             if kind == 'workflow' else review.WorkflowInspectionProfile())
    boundaries = ()
    points = ()
    if kind == 'workflow':
        boundary = review.WorkflowBoundary('fixture-events', 'library/__init__.py', 'solve',
            2, 'line', None, None, 'post_fault' if case_id == 'WF19-provisional-fault-boundary' else 'solve_exit', 4)
        boundaries = (boundary,)
        if value.case_id == 'WF19-provisional-fault-boundary':
            points = tuple(review.WorkflowCapturePoint(role, 0, boundary.id, index)
                for index, role in enumerate(('initial', 'post_fault', 'reopened', 'final')))
    plan = review.WorkflowSourcePlan(admission.source_sha256(files), review.source_sha256(files), commit, tree,
        value.case_id, value.sha256, 'reviewed-workflow-final-sqlite-v1', ('library.sqlite',), 'f'*64,
        value.purpose, boundaries, profile_kind=kind, capture_points=points)
    authority, mechanism_journal, mechanism_head = synthetic_delivery(root/'mechanism', plan.request())
    stack.callback(mechanism_head.close)
    stack.callback(mechanism_journal.close)
    subject = registry.Subject('recipe-fixture', COHORT[0], 'M4', 'c'*64, exposure.BASE_SHA256,
        admission.source_sha256(files))
    common = dict(source_plan=plan, source_authority=authority, purpose=value.purpose,
        gate_id='recipe-' + kind, repetition_id='fresh-1', cohort_trajectory_ids=COHORT)
    if kind == 'workflow':
        recipe = factory.build_workflow_recipe(store, subject, case_id=value.case_id,
            policy=execution.WorkflowPolicy(), runtime={'kind': 'fixture-no-Docker'}, **common)
    else:
        recipe = factory.build_workflow_inspection_recipe(store, subject, **common)
    raw, delta, cleanup = root/'raw', root/'delta', root/'cleanup'
    head = ExternalHead.create(root/'head', journal_roots=(raw, delta))
    stack.callback(head.close)
    spec = recipe.spec(root=raw, delta_root=delta, cleanup_root=cleanup, checkpoint_authority=head)
    registration = spec.observation_registration()
    state = {'registration': registration, 'freeze': registry.CohortFreeze(
        tuple(replace(subject, trajectory_id=key) for key in COHORT), '1'*64, '2'*64, True)}
    issued = admission.ObservationAdmission(registration,
        verify_registration=lambda: state['registration'], verify_cohort=lambda: state['freeze'])
    return SimpleNamespace(root=root, store=store, files=files, commit=commit, tree=tree, subject=subject,
        profile=value, plan=plan, authority=authority, mechanism_journal=mechanism_journal,
        mechanism_head=mechanism_head, mechanism_context=dict(MECHANISM_CONTEXT), recipe=recipe, spec=spec,
        state=state, issued=issued, head=head, raw=raw, delta=delta, cleanup=cleanup)


def typed_inputs(fixture):
    """The real exporter input shape; live capability objects stay external."""
    fields = ('kind', 'root', 'delta_root', 'cleanup_root', 'registration', 'policy', 'recipe',
              'profile', 'cumulative_profile', 'endpoint', 'layout_plan')
    return {**{name: getattr(fixture.spec, name) for name in fields}, 'store': fixture.store.path.resolve()}


def restore_spec(fixture, inputs):
    """Rebind decoded named inputs to the still-live independently held originals."""
    admission.require(type(inputs) is dict and set(inputs) == set(typed_inputs(fixture)),
        'Exact exported workflow input fields required')
    admission.require(inputs['store'] == fixture.store.path.resolve(), 'Different original source repository')
    return final.ObservationSpec(**{name: value for name, value in inputs.items() if name != 'store'},
        store=fixture.store, checkpoint_authority=fixture.head, layout_authority=fixture.authority)


def construct_fixture_owner(fixture, spec=None):
    actual = fixture.spec if spec is None else spec
    constructor = (factory.construct_workflow_owner if fixture.spec.kind == 'workflow'
                   else factory.construct_workflow_inspection_owner)
    return constructor(actual, fixture.issued, mode='fixture')


class CandidateWorkflowGitCompositionTests(unittest.TestCase):
    """Actual builder, source, codec and owner seams; no Engine or full scope."""
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.base = Path(cls.temp.name).resolve()
        cls.store = GitStore.create(cls.base/'source.git', {
            'library/__init__.py': 'def solve(payload):\n    raise RuntimeError("fixture only; never executed")\n',
            exposure.cli.RELEASE_PATH: exposure.cli.text(), exposure.RELEASE_PATH: exposure.text()})

    def setUp(self):
        self.root = self.base/self.id().rsplit('.', 1)[-1]
        self.root.mkdir()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)

    def fixture(self, kind, *, case_id=None, name=None):
        return recipe_fixture(self.root/(name or kind), self.stack, kind, store=self.store, case_id=case_id)

    def roundtrip(self, fixture):
        writer = InputWriter(fixture.root/'typed-inputs')
        reference = writer.put(typed_inputs(fixture), typed=True)
        decoded = codec.read(reference)
        self.assertEqual(decoded, typed_inputs(fixture))
        self.assertIs(type(decoded['registration']), type(fixture.recipe.registration))
        self.assertIs(type(decoded['profile']), type(fixture.profile))
        self.assertIs(type(decoded['layout_plan']), review.WorkflowSourcePlan)
        spec = restore_spec(fixture, decoded)
        self.assertIs(type(spec), final.ObservationSpec)
        self.assertEqual(spec.observation_registration(), fixture.state['registration'])
        return spec, reference

    def check_fixture_owner(self, fixture, spec):
        owner = construct_fixture_owner(fixture, spec)
        self.addCleanup(owner.close)
        self.assertEqual(owner.observation_registration, fixture.state['registration'])
        self.assertEqual(owner.read_authenticated('config.json'), profiles.encoded(owner.config))
        self.assertFalse(owner.has_retained('intent.json'))
        with self.assertRaises(ValueError):
            owner.execute_once() if spec.kind == 'workflow' else owner.begin_review()
        self.assertFalse(owner.has_retained('intent.json'))
        self.assertFalse(owner.has_retained('terminal.json'))
        owner.close()
        with self.assertRaises((ValueError, chain.ChainError)):
            owner.checkpoint()
        # Closing the owned execution cannot silently close its borrowed proof.
        self.assertEqual(fixture.authority.authenticate(fixture.plan), fixture.authority.enrollment.report_sha256)
        self.assertEqual(fixture.mechanism_journal.commitment, fixture.authority.expected)

    def check_batch_owner(self, owner, freeze=None):
        self.addCleanup(owner.close)
        policy = execution.JOURNAL_READ_POLICY.record()
        self.assertEqual(owner.config['journal_read_policy'], policy)
        self.assertEqual(json.loads(owner.read_authenticated('config.json'))['journal_read_policy'], policy)
        genesis = json.loads((owner.delta_root/'genesis.json').read_bytes())
        self.assertEqual(genesis['journal_read'], policy)
        self.assertEqual(genesis['context']['journal_read'], policy)
        self.assertEqual(genesis['context']['execution_context']['config_sha256'], execution.digest(owner.config))
        for name, digest in batch.evaluator_sources().items():
            self.assertEqual(owner.sources[name], digest)
        original = batch.CheckpointReader.read
        reads = []

        def record(reader, path, *, max_bytes=stable.MAX_RECORD_BYTES):
            reads.append((reader, Path(path)))
            return original(reader, path, max_bytes=max_bytes)

        owner._retain('fresh-read.bin', b'one')
        expected = owner.checkpoint()
        with mock.patch.object(batch.CheckpointReader, 'read', new=record):
            self.assertEqual(owner.checkpoint(), expected)
            self.assertEqual(owner.checkpoint(), expected)
        paths = {p for directory in (owner.root, owner.delta_root)
                 for p in directory.iterdir() if p.name != 'owner.lock'}
        self.assertEqual(Counter(path for _, path in reads), Counter({p: 2 for p in paths}))
        readers = {reader for reader, _ in reads}
        self.assertEqual(len(readers), 2)
        for reader in readers:
            with self.assertRaises(stable.JournalError):
                reader.validate()
        owner.current(freeze)
        owner.config['journal_read_policy'] = None
        with self.assertRaisesRegex(ValueError, 'journal read policy differs'):
            owner.current(freeze)
        owner.config['journal_read_policy'] = policy
        (owner.root/'fresh-read.bin').write_bytes(b'two')
        with self.assertRaises(chain.ChainUnknown):
            owner.checkpoint()
        self.assertEqual(owner.checkpoint_authority.read(), expected)

    def test_current_uses_fresh_combined_review_and_checks_both_identities(self):
        fixture = self.fixture('workflow')
        owner = construct_fixture_owner(fixture)
        self.addCleanup(owner.close)
        authority = fixture.authority
        self.assertEqual(owner.config['review_read_policy'], execution.review_read_policy())
        with mock.patch.object(authority, 'authenticate', side_effect=AssertionError('legacy duplicate read')), \
             mock.patch.object(authority, 'provenance', side_effect=AssertionError('legacy duplicate read')), \
             mock.patch.object(authority, 'authenticate_with_provenance', wraps=authority.authenticate_with_provenance) as reads:
            owner.current(fixture.state['freeze'])
            owner.current(fixture.state['freeze'])
            self.assertEqual(reads.call_count, 2)
        report_sha256, provenance = authority.authenticate_with_provenance(fixture.plan)
        for result in (('f'*64, provenance), (report_sha256, {**provenance, 'positions': {}})):
            with mock.patch.object(authority, 'authenticate_with_provenance', return_value=result):
                with self.assertRaisesRegex(ValueError, 'Layout authority changed'):
                    owner.current(fixture.state['freeze'])
        owner.config['review_read_policy'] = None
        with self.assertRaisesRegex(ValueError, 'review read policy differs'):
            owner.current(fixture.state['freeze'])
        owner.config['review_read_policy'] = execution.review_read_policy()
        (fixture.mechanism_journal.raw_root/'report.json').write_bytes(b'{}')
        with self.assertRaises(chain.ChainError):
            owner.current(fixture.state['freeze'])

    def test_product_owner_binds_policy_and_freshly_reads_each_checkpoint(self):
        fixture = self.fixture('workflow')
        self.check_batch_owner(construct_fixture_owner(fixture), fixture.state['freeze'])

    def test_qualifier_owner_binds_policy_and_freshly_reads_each_checkpoint(self):
        # Actual Git/owner/admission classes; synthetic controller, no candidate dispatch.
        value = execution.WorkflowQualificationProfile('WQ-FRAME-EXACT')
        files = execution.qualification_source_files(value.control_id)
        store = GitStore.create(self.root/'qualifier.git', {name: raw.decode('ascii') for name, raw in files.items()})
        tree, actual = execution.capture_git_source(store, store.head())
        self.assertEqual(actual, files)
        policy = execution.WorkflowPolicy()
        binding = execution.qualification_binding_for(files, value, policy, {'kind': 'fixture-no-Docker'})
        subject = registry.Subject('recipe-fixture', COHORT[0], 'M4', 'c'*64,
            exposure.BASE_SHA256, admission.source_sha256(files))
        gate = execution.qualification_gate_for(subject, binding, gate_id='qualifier-fixture')
        registration = execution.WorkflowQualificationRegistration(binding, store.head(), tree,
            'fresh-qualifier', gate, COHORT)
        registered = execution.qualification_observation_registration(registration)
        issued = admission.ObservationAdmission(registered, verify_registration=lambda: registered,
            verify_cohort=lambda: None)
        raw, delta = self.root/'raw', self.root/'delta'
        head = ExternalHead.create(self.root/'head', journal_roots=(raw, delta))
        self.stack.callback(head.close)
        owner = execution.CandidateWorkflowQualificationExecution(raw, store, registration, policy,
            value=value, admission_authority=issued, checkpoint_authority=head, delta_root=delta,
            cleanup_root=self.root/'cleanup', mode='fixture')
        self.check_batch_owner(owner)

    def test_actual_runtime_builder_spec_disk_codec_scope_and_fixture_owner(self):
        fixture = self.fixture('workflow')
        spec, _ = self.roundtrip(fixture)
        fixture.recipe.revalidate()
        component = fixture.recipe.scope_slice()
        scope.verify_slice(component)
        self.assertEqual(component.gate, fixture.recipe.registration.gate)
        self.assertEqual(component.family, exposure.FAMILY)
        self.assertTrue(component.assertions)
        self.assertFalse(fixture.recipe.record()['whole_scope_authority'])
        self.assertFalse(fixture.recipe.record()['physical_execution_supplied'])
        self.check_fixture_owner(fixture, spec)

    def test_actual_inspection_builder_spec_disk_codec_scope_and_fixture_owner(self):
        fixture = self.fixture('workflow_inspection')
        spec, _ = self.roundtrip(fixture)
        fixture.recipe.revalidate()
        component = fixture.recipe.scope_slice()
        scope.verify_slice(component)
        self.assertEqual(component.gate, fixture.recipe.registration.gate)
        self.assertEqual(component.family, exposure.INSPECTION_FAMILY)
        self.assertEqual(len(component.selectors), len(review.INSPECTION_DUTIES))
        self.assertFalse(fixture.recipe.record()['product_inspection_requested'])
        self.assertFalse(fixture.recipe.record()['product_inspection_supplied'])
        self.check_fixture_owner(fixture, spec)

    def test_wf19_builder_preserves_exact_four_capture_points_and_all_sixteen_semantic_cells(self):
        fixture = self.fixture('workflow', case_id='WF19-provisional-fault-boundary')
        spec, _ = self.roundtrip(fixture)
        self.assertTrue(all(type(point) is review.WorkflowCapturePoint for point in spec.layout_plan.capture_points))
        self.assertEqual(spec.layout_plan.capture_points, fixture.plan.capture_points)
        expected = {row['case_id'] for row in profiles.capture_selectors(fixture.profile.case_id)}
        component = fixture.recipe.scope_slice()
        self.assertEqual(len(expected), 16)
        self.assertEqual({row.case_id for row in component.selectors if row.case_id in expected}, expected)
        self.check_fixture_owner(fixture, spec)

    def test_decoded_wrong_kind_profile_type_purpose_and_auxiliary_recipe_refuse_before_owner(self):
        for kind in ('workflow', 'workflow_inspection'):
            fixture = self.fixture(kind)
            original = typed_inputs(fixture)
            wrong_kind = 'workflow_inspection' if kind == 'workflow' else 'workflow'
            altered = ({**original, 'kind': wrong_kind},
                {**original, 'profile': asdict(fixture.profile)},
                {**original, 'profile': replace(fixture.profile, purpose='repeatability')},
                {**original, 'recipe': {'invented': 'execution recipe'}})
            writer = InputWriter(fixture.root/'negative-inputs')
            for index, fields in enumerate(altered):
                with self.subTest(kind=kind, mutation=index):
                    decoded = codec.read(writer.put(fields, typed=True))
                    spec = restore_spec(fixture, decoded)
                    with self.assertRaises(ValueError):
                        construct_fixture_owner(fixture, spec)
                    self.assertFalse(fixture.raw.exists())
                    self.assertEqual(fixture.authority.authenticate(fixture.plan), fixture.authority.enrollment.report_sha256)

    def test_each_paired_identity_in_original_recipe_record_is_revalidated(self):
        for kind in ('workflow', 'workflow_inspection'):
            fixture = self.fixture(kind)
            original = fixture.recipe.record()
            for key, expected in (('effective_requirements', exposure.cli.manifest()),
                                  ('workflow_requirements', exposure.manifest()),
                                  ('combined_effective_requirements', exposure.combined_manifest())):
                self.assertEqual(original['profile'][key], expected)
                changed = deepcopy(original)
                del changed['profile'][key]
                with self.subTest(kind=kind, dropped=key), self.assertRaises(ValueError):
                    replace(fixture.recipe, original_bytes=profiles.encoded(changed)).revalidate()
            self.assertFalse(fixture.raw.exists())

    def test_original_review_prefix_revocation_blocks_revalidation_and_construction(self):
        for kind in ('workflow', 'workflow_inspection'):
            fixture = self.fixture(kind)
            fixture.mechanism_journal.retain('later-unenrolled.json', b'{}')
            for action in (fixture.recipe.revalidate, lambda: construct_fixture_owner(fixture)):
                with self.subTest(kind=kind), self.assertRaises((ValueError, chain.ChainError)):
                    action()
            self.assertFalse(fixture.raw.exists())

    def test_missing_six_subject_freeze_or_revoked_registration_prevents_fixture_journal(self):
        for kind in ('workflow', 'workflow_inspection'):
            fixture = self.fixture(kind)
            saved = fixture.state['freeze']
            fixture.state['freeze'] = None
            with self.subTest(kind=kind, authority='freeze'), self.assertRaises(admission.AdmissionError):
                construct_fixture_owner(fixture)
            self.assertFalse(fixture.raw.exists())
            fixture.state['freeze'] = saved
            fixture.state['registration'] = None
            with self.subTest(kind=kind, authority='registration'), self.assertRaises(admission.AdmissionError):
                construct_fixture_owner(fixture)
            self.assertFalse(fixture.raw.exists())

    def test_proof_overlap_and_closed_borrowed_authority_block_real_recipe_consumption(self):
        for kind in ('workflow', 'workflow_inspection'):
            fixture = self.fixture(kind)
            invalid = replace(fixture.spec, cleanup_root=fixture.mechanism_journal.raw_root/'nested')
            with self.subTest(kind=kind, boundary='overlap'), self.assertRaises(ValueError):
                construct_fixture_owner(fixture, invalid)
            self.assertFalse(fixture.raw.exists())
            fixture.mechanism_journal.close()
            with self.subTest(kind=kind, boundary='closed'), self.assertRaises(chain.ChainError):
                fixture.recipe.revalidate()
            self.assertFalse(fixture.raw.exists())

    def test_pinned_disk_input_tamper_and_unregistered_type_are_rejected(self):
        fixture = self.fixture('workflow')
        _, reference = self.roundtrip(fixture)
        path = Path(reference['path'])
        original = path.read_bytes()
        path.write_bytes(original + b' ')
        with self.assertRaises(ValueError):
            codec.read(reference)
        writer = InputWriter(fixture.root/'wrong-type')
        packed = codec.pack(typed_inputs(fixture))
        packed['input']['value']['profile']['type'] = 'gossip_harness.candidate_workflow_profile_v1.ForgedProfile'
        # Persist malformed typed bytes as a raw input; read() must still apply
        # its closed type decoder, independent of the writer's typed shortcut.
        with self.assertRaises(ValueError):
            codec.read(writer.put(packed))

    def test_actual_source_identity_and_recipe_implementation_closure_remain_exact(self):
        fixture = self.fixture('workflow')
        for capture in (review.capture_git_source, execution.capture_git_source):
            with self.subTest(capture=capture.__module__), mock.patch.object(
                    subprocess, 'Popen', wraps=subprocess.Popen) as launches:
                tree, files = capture(fixture.store, fixture.commit)
            self.assertEqual((tree, files), (fixture.tree, fixture.files))
            self.assertEqual(launches.call_count, 2)
        self.assertEqual(execution.SOURCE_CAPTURE_POLICY, review.SOURCE_CAPTURE_POLICY)
        record = fixture.recipe.record()
        self.assertEqual(record['source_manifest'], admission.source_manifest(files))
        self.assertEqual(fixture.recipe.registration.gate.binding.subject.source_sha256,
                         admission.source_sha256(files))
        for name in ('candidate_workflow_review_v1.py', 'candidate_workflow_execution_v1.py',
                     'candidate_workflow_observation_v1.py', 'cumulative_workflow_observation_recipe_v1.py',
                     'cumulative_rehearsal_codec_v1.py', 'cumulative_final_acceptance_v1.py',
                     'candidate_source_capture_policy_v1.py', 'candidate_git_source_batch_v1.py',
                     'candidate_source_capture_policy_v2.py', 'candidate_git_source_two_process_v1.py'):
            self.assertEqual(record['sources']['gossip_harness/' + name],
                             hashlib.sha256((ROOT/'gossip_harness'/name).read_bytes()).hexdigest())
        self.assertEqual(record['source_capture_policy']['protocol'], 'candidate-source-capture-policy-v2')
        from gossip_harness.candidate_source_capture_policy_v1 import BatchCapturePolicy
        with mock.patch.object(execution, 'SOURCE_CAPTURE_POLICY', BatchCapturePolicy()):
            with self.assertRaisesRegex(ValueError, 'two-process source-capture policy'):
                fixture.recipe.revalidate()
        for broken in (replace(fixture.recipe, runtime_bytes=b'{}'),
                       replace(fixture.recipe, registration=replace(fixture.recipe.registration, tree_oid='e'*40))):
            with self.assertRaises(ValueError):
                broken.revalidate()
        self.assertFalse(fixture.raw.exists())
