"""Mapped inputs through actual cold-reader and prerequisite target seams.

Only the layout journals, pinned bytes, typed reads and guard mechanics are real
in these controls. Scope/barrier records are explicit synthetic method fixtures.
The cold reader must stop at missing full semantic scope, before constructing a
physical owner. The complete capsule audit and a full accepted rehearsal remain
unqualified; no successful authority or product verdict is mocked here.
"""
from contextlib import ExitStack
from dataclasses import asdict, replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness import candidate_checkpoint_chain_v1 as checkpoint
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_m2_product_profile_v1 as m2_profile
from gossip_harness import candidate_m2_product_execution_v1 as m2_execution
from gossip_harness import candidate_m2_review_authority_v1 as m2_review
from gossip_harness import candidate_storage_product_profile_v1 as storage_profile
from gossip_harness import candidate_storage_product_execution_v1 as storage_execution
from gossip_harness import candidate_storage_review_authority_v1 as storage_review
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import cumulative_final_acceptance_v1 as shared
from gossip_harness import cumulative_final_originals_v1 as cold
from gossip_harness import cumulative_m2_observation_recipe_v1 as m2_factory
from gossip_harness import cumulative_prerequisite_qualification_v1 as prerequisite
from gossip_harness import cumulative_rehearsal_capsule_v2 as capsule_v2
from gossip_harness import cumulative_rehearsal_capsule_v3 as capsule_v3
from gossip_harness import cumulative_rehearsal_codec_v1 as codec
from gossip_harness import cumulative_rehearsal_export_v1 as exporter
from gossip_harness import cumulative_scope_source_v3 as scope
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.cumulative_study_controller_v1 import Records, plain
from gossip_harness.peer_financial_terminal_v1 import FinancialError
from tests.test_candidate_storage_finite_mapping_v1 import CandidateStorageFiniteMappingCompositionTests as StorageFixture
from tests.test_candidate_m2_finite_mapping_v2 import CandidateM2FiniteMappingOwnerV2Tests as M2Fixture
from tests.test_cumulative_terminal_originals_v2 import synthetic_plan


class CumulativeFiniteOriginalTransportV1Tests(unittest.TestCase):
    # Reuse one hostile, unimported candidate Git fixture; no inherited methods.
    setUpClass = classmethod(StorageFixture.setUpClass.__func__)

    def setUp(self):
        self.root = self.base / self.id().rsplit('.', 1)[-1]
        self.root.mkdir()

    def fixture(self, kind):
        """Real recipe/layout enrollment and fixture-only execution config."""
        if kind == 'storage':
            recipe, journal = StorageFixture.recipe(self)
            owner, spec = StorageFixture.owner(self, recipe)
        else:
            M2Fixture.make(self, 'independent_acceptance')
            recipe = M2Fixture.recipe(self)
            spec = recipe.spec(root=self.raw, delta_root=self.delta,
                cleanup_root=self.root/'cleanup', checkpoint_authority=self.head)
            registration = spec.observation_registration()
            subject = registration.gate.binding.subject
            freeze = registry.CohortFreeze(tuple(replace(subject, trajectory_id=name)
                for name in registration.cohort_trajectory_ids), '1'*64, '2'*64, True)
            issued = admission.ObservationAdmission(registration, verify_registration=lambda: registration,
                verify_cohort=lambda: freeze)  # Explicit synthetic barrier.
            owner = m2_factory.construct_m2_owner(spec, issued, mode='fixture')
            self.addCleanup(owner.close)
            journal = self.review.journal
        expected = owner.checkpoint()
        self.assertFalse(owner.has_retained('intent.json'))
        return recipe, owner, spec, journal, expected

    def original_packet(self, kind):
        recipe, owner, spec, journal, expected = self.fixture(kind)
        writer = exporter.InputWriter(self.root/'export')
        inputs = {name: getattr(spec, name) for name in (
            'kind', 'root', 'delta_root', 'cleanup_root', 'registration', 'policy',
            'recipe', 'profile', 'cumulative_profile', 'endpoint', 'layout_plan')}
        inputs['store'] = spec.store.path.resolve()
        context = ({'fixture': 'not actual semantic approval'} if kind == 'storage'
            else {'fixture': 'M2 linkage only, not source approval'})
        descriptor = exporter.proof_descriptor(journal, context)
        value = {'inputs': writer.put(inputs, typed=True),
            'layout_review': {'chain': 'layout',
                'enrollment': writer.put(spec.layout_authority.enrollment, typed=True)},
            'proof': {'raw': str(spec.root), 'delta': str(spec.delta_root),
                'head': str(spec.checkpoint_authority.root), 'expected': asdict(expected)}}
        # Exercise the exact bound-reader shared by capsule V2 and V3. This is
        # an observation input packet, deliberately not a full rehearsal capsule.
        reference = writer.put({'observation': value, 'proofs': {'layout': descriptor}})
        self.assertEqual(capsule_v2.bound(reference), capsule_v3.bound(reference))
        packet = capsule_v3.bound(reference)
        owner.close(); spec.checkpoint_authority.close()
        journal.close(); journal.authority.close()
        return recipe, spec, inputs, writer, packet

    def method_owner(self, registration, spec, expected):
        """Synthetic final scaffolding with NO registered semantic scope."""
        raw, delta, anchor = (self.root/name for name in ('final-raw', 'final-delta', 'final-head'))
        head = ExternalHead.create(anchor, journal_roots=(raw, delta))
        self.addCleanup(head.close)
        chain = checkpoint.CheckpointChain.create(raw, delta,
            context={'fixture': 'transport only, no semantic or physical authority'}, authority=head)
        self.addCleanup(chain.close)
        records = Records(chain)
        subject = registration.gate.binding.subject
        freeze = registry.CohortFreeze(tuple(replace(subject, trajectory_id=name)
            for name in registration.cohort_trajectory_ids), '1'*64, '2'*64, True)
        provenance = consumer.Revision(registration.commit_oid, registration.tree_oid, subject.source_sha256)
        study_expected = chain.commitment
        key = 'final.observation.' + registry.fingerprint(registration.gate)
        records.put(key+'.admission', plain({'registration': asdict(registration), 'freeze': asdict(freeze),
            'scope': asdict(provenance), 'kind': spec.kind, 'root': str(spec.root),
            'delta_root': str(spec.delta_root), 'head_root': str(spec.checkpoint_authority.root),
            'cleanup_root': str(spec.cleanup_root), 'protocol': cold.final.PROTOCOL,
            'study_checkpoint': asdict(study_expected)}))
        records.put(key+'.verified', {'post_checkpoint': asdict(expected)})
        owner = SimpleNamespace(plan=synthetic_plan(), records=records, chain=chain, freeze=freeze,
            study_expected=study_expected, _registered_gate=lambda value: provenance,
            originals=SimpleNamespace(slots=(SimpleNamespace(trajectory=subject.trajectory_id,
                final_source={'repository': str(spec.store.path.resolve())}),)),
            _protected_roots=lambda: [spec.store.path.resolve()],
            submissions={subject.trajectory_id: SimpleNamespace(subject=subject)},
            scope_snapshot=SimpleNamespace(registration=lambda value: None))
        return owner, key

    def cold_transport(self, kind):
        _, spec, inputs, writer, packet = self.original_packet(kind)
        registration = spec.observation_registration()
        expected = checkpoint.PrefixCommitment(**packet['observation']['proof']['expected'])
        owner, key = self.method_owner(registration, spec, expected)
        seen = []
        original = shared.ObservationSpec.observation_registration

        def normalize(value):
            actual = original(value)
            # The cold reader's nested ExitStack still owns this layout chain.
            # Authenticate the retained request/report/delivery while it is live.
            review_sha256 = value.layout_authority.authenticate(value.layout_plan)
            seen.append((value, actual, review_sha256))
            return actual

        with ExitStack() as stack:
            pool = cold.ProofPool(packet['proofs'], stack)
            with patch.object(shared.ObservationSpec, 'observation_registration', normalize), \
                    patch.object(cold, '_prerequisite_prefix', wraps=cold._prerequisite_prefix) as guard:
                with self.assertRaisesRegex(FinancialError, 'complete scope registration missing'):
                    cold._observation(owner, key, packet['observation'], pool, registration.gate, owner.freeze)
                self.assertEqual(guard.call_count, 1)
            self.assertEqual(seen[-1][1], registration)
            self.assertIs(type(seen[-1][0].profile), type(spec.profile))
            self.assertIs(type(seen[-1][0].layout_plan), type(spec.layout_plan))
            self.assertEqual(seen[-1][2], spec.layout_authority.enrollment.report_sha256)
            # _observation has released its local proof owner on refusal. The
            # outer stack does not extend that borrowed authority's lifetime.
            with self.assertRaisesRegex(checkpoint.ChainError, 'Closed or foreign-thread/process chain'):
                seen[-1][0].layout_authority.authenticate(spec.layout_plan)
            pool.current()
            self.assertIsNone(owner.records.read(key+'.dispatch'))
            self.assertIsNone(owner.records.read('final.assessment'))
            if kind == 'storage':
                default = storage_profile.profile_for('b02', spec.profile.case_id, spec.profile.purpose)
                plan_fields = asdict(spec.layout_plan); plan_fields.pop('mapping_profile')
                default_plan = storage_review.LayoutPlan(**plan_fields)
                old_protocol = storage_execution.BATCH_PROTOCOL
            else:
                default = m2_profile.profile_for(spec.profile.case_id, spec.profile.purpose)
                default_plan = m2_review.LayoutPlan(**asdict(spec.layout_plan))
                old_protocol = m2_execution.PROTOCOL
            changes = (
                {'profile': default}, {'layout_plan': default_plan},
                {'registration': replace(spec.registration,
                    binding=replace(spec.registration.binding, protocol=old_protocol))},
            )
            for index, change in enumerate(changes):
                value = {**packet['observation'], 'inputs': writer.put({**inputs, **change}, typed=True)}
                with self.subTest(kind=kind, mutation=index), \
                        patch.object(cold, '_prerequisite_prefix', wraps=cold._prerequisite_prefix) as guard:
                    with self.assertRaises(ValueError):
                        cold._observation(owner, key, value, pool, registration.gate, owner.freeze)
                    guard.assert_not_called()
            # Purpose mutation is performed on the original typed wire record,
            # so even constructor-level refusal is tested through codec.read.
            wire = codec.pack(inputs)
            wire['input']['value']['registration']['value']['binding']['value']['purpose'] = 'public_release'
            value = {**packet['observation'], 'inputs': writer.put(wire)}
            with patch.object(cold, '_prerequisite_prefix', wraps=cold._prerequisite_prefix) as guard:
                with self.assertRaises(ValueError):
                    cold._observation(owner, key, value, pool, registration.gate, owner.freeze)
                guard.assert_not_called()

    def test_b02_capsule_bound_input_cold_named_types_and_missing_scope_refusal(self):
        self.cold_transport('storage')

    def test_m2_capsule_bound_input_cold_named_types_and_missing_scope_refusal(self):
        self.cold_transport('m2')

    def prerequisite_target(self, kind):
        recipe, _, spec, _, expected = self.fixture(kind)
        writer = exporter.InputWriter(self.root/'target-input')
        # Same typed transport as the capsule's observation input, then actual
        # Final ObservationSpec normalization and source-defined target factory.
        fields = {name: getattr(spec, name) for name in ('registration', 'profile', 'layout_plan')}
        transported = codec.read(writer.put(fields, typed=True))
        spec = replace(spec, **transported)
        registration = spec.observation_registration()
        owner, _ = self.method_owner(registration, spec, expected)
        owner._put = owner.records.put
        owner._current = lambda: None  # Explicit authority fixture, not approval.
        component = recipe.scope_slice()
        scope.verify_slice(component)
        revision = consumer.Revision(self.commit, self.tree, registration.gate.binding.subject.source_sha256)
        definition = prerequisite.definition_for('admission', revision, (registration.gate,),
            gate_id='fixture-admission', suite_id='fixture-admission-suite', physical_slot='fixture-admission-slot')
        registered = consumer.RegisteredAcceptance(*(['a'*64]*5), (definition.specification,))
        request = consumer.QualificationRequest(registered, definition.execution, definition.suite,
            definition.specification, definition.product_lineages_sha256)
        producer = object.__new__(prerequisite.PrerequisiteQualification)
        producer.owner, producer.source = owner, revision
        # Isolate the existing target-execution method from the missing full
        # semantic registration. This is not the production _resolve route.
        producer._resolve = lambda value: (SimpleNamespace(subject=registration.gate.binding.subject,
            slices=(component,)), SimpleNamespace(gates=(registration.gate,)), definition, revision)
        result = producer.execute_once(request, (spec,))
        target = owner.records.read(producer._key(request)+'.target.0')
        self.assertEqual(target['registration'], plain(asdict(registration)))
        self.assertEqual(target['slice_sha256'], component.sha256)
        self.assertTrue(all(row['passed'] for row in target['guards']))
        self.assertEqual(result['body']['candidate_effects'], 0)
        self.assertIsNone(owner.records.read('final.assessment'))
        # No review publication or QualificationEvidence was produced. A
        # changed named profile is rejected before a second intent can exist.
        default = (storage_profile.profile_for('b02', spec.profile.case_id, spec.profile.purpose)
            if kind == 'storage' else m2_profile.profile_for(spec.profile.case_id, spec.profile.purpose))
        with self.assertRaises(ValueError):
            replace(spec, profile=default).observation_registration()

    def test_b02_actual_prerequisite_target_factory_and_guard_transport(self):
        self.prerequisite_target('storage')

    def test_m2_actual_prerequisite_target_factory_and_guard_transport(self):
        self.prerequisite_target('m2')
