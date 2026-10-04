"""Real Git/compact-head controls with synthetic review originals; no candidate run.

The enrolled reviewer fixture tests linkage, never source adequacy. Fixture owners
are categorically rejected by the physical Registry bridge.
"""
from dataclasses import asdict, replace
from pathlib import Path
import tempfile
import subprocess
import io
import tarfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from gossip_harness import candidate_storage_product_execution_v1 as execution
from gossip_harness import candidate_storage_cases_v1 as b01_cases
from gossip_harness import candidate_storage_product_observation_v1 as observer
from gossip_harness import candidate_storage_review_authority_v1 as review
from gossip_harness import candidate_storage_product_profile_v1 as profile
from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.gitstore import GitStore
from tests.test_candidate_storage_observer_v1 import physical_files, SQLITE_LAYOUT

COHORT = tuple('trajectory-' + str(i) for i in range(6))


def enroll(root, plan, *, changes=None, delivery_first=False):
    raw_root, delta_root = root / 'review-raw', root / 'review-deltas'
    head = ExternalHead.create(root / 'review-head', journal_roots=(raw_root, delta_root))
    journal = chain.CheckpointChain.create(raw_root, delta_root, context={'fixture': 'not actual semantic approval'}, authority=head)
    request = review.encoded(plan.request())
    report = {'protocol': review.PROTOCOL, 'purpose': review.PURPOSE, 'reviewer_id': 'fixture-reviewer',
        'request_sha256': execution.sha(request), 'decisions': [{'id': duty, 'decision': 'approved',
            'rationale': 'Fixture linkage only, never a real source review.', 'source_references': ['fixture-only']}
            for duty in review.DUTIES], 'remaining_obligations': ['Everything outside this fixture remains unqualified.']}
    report.update(changes or {})
    report_raw = review.encoded(report)
    delivery = review.encoded({'protocol': review.PROTOCOL, 'purpose': review.PURPOSE,
        'reviewer_id': 'fixture-reviewer', 'request_sha256': execution.sha(request),
        'report_sha256': execution.sha(report_raw), 'origin': 'independently_delivered_host_review'})
    journal.retain('request.json', request)
    for name, raw in ([('delivery.json', delivery), ('report.json', report_raw)] if delivery_first else
                      [('report.json', report_raw), ('delivery.json', delivery)]):
        journal.retain(name, raw)
    enrollment = review.ReviewEnrollment('fixture-reviewer', 'request.json', execution.sha(request),
        'report.json', execution.sha(report_raw), 'delivery.json', execution.sha(delivery))
    return review.StorageReviewAuthority(journal, journal.commitment, enrollment), journal, head


class StorageReviewAuthorityV1Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.plan = review.LayoutPlan('b01', 'a' * 64, 'b' * 64, 'c' * 40, 'd' * 40, 'rollback', profile.profile_for('b01', 'rollback', 'public_release').sha256,
            SQLITE_LAYOUT, ('catalog.sqlite',), 'f' * 64)

    def authority(self, **kwargs):
        authority, journal, head = enroll(self.root, self.plan, **kwargs)
        self.addCleanup(head.close); self.addCleanup(journal.close)
        return authority, journal

    def test_original_request_report_delivery_are_authenticated_each_read(self):
        authority, journal = self.authority()
        self.assertEqual(authority.authenticate(self.plan), authority.enrollment.report_sha256)
        mapped = authority.observer_registration(self.plan)
        self.assertEqual(mapped.source_sha256, self.plan.native_source_sha256)
        self.assertEqual(mapped.review_sha256, authority.enrollment.report_sha256)
        self.assertEqual(journal.commitment, authority.expected)

    def test_matching_hashes_with_wrong_review_purpose_are_rejected(self):
        authority, _ = self.authority(changes={'purpose': 'harness_qualification'})
        with self.assertRaises(admission.AdmissionError):
            authority.authenticate(self.plan)

    def test_delivery_must_follow_original_report(self):
        authority, _ = self.authority(delivery_first=True)
        with self.assertRaisesRegex(admission.AdmissionError, 'order'):
            authority.authenticate(self.plan)

    def test_source_profile_schema_and_scope_changes_are_rejected(self):
        authority, _ = self.authority()
        for item in (replace(self.plan, source_sha256='1' * 64), replace(self.plan, profile_sha256='2' * 64),
                     replace(self.plan, schema_sha256='3' * 64), replace(self.plan, case_id='capacity')):
            with self.subTest(item=item), self.assertRaises(admission.AdmissionError):
                authority.authenticate(item)

    def test_rejected_duty_cannot_authenticate(self):
        decisions = [{'id': duty, 'decision': 'rejected' if i == 1 else 'approved',
            'rationale': 'fixture', 'source_references': ['fixture']} for i, duty in enumerate(review.DUTIES)]
        authority, _ = self.authority(changes={'decisions': decisions})
        with self.assertRaises(admission.AdmissionError):
            authority.authenticate(self.plan)

    def test_changed_or_extra_originals_revoke_current_authority(self):
        authority, journal = self.authority()
        journal.retain('late.json', b'{}')
        with self.assertRaises(admission.AdmissionError):
            authority.authenticate(self.plan)

    def test_claimed_qualified_forced_schedule_is_not_a_supported_profile(self):
        with self.assertRaises(admission.AdmissionError):
            replace(self.plan, schedule='instrumentable-control')


class CandidateStorageProductExecutionV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.base = Path(cls.temp.name).resolve()
        cls.store = GitStore.create(cls.base / 'repo.git', {'library/__init__.py': "raise RuntimeError('never import candidate on host')\n"})
        cls.commit = cls.store.head()
        cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)

    def setUp(self):
        self.root = self.base / self.id().rsplit('.', 1)[-1]
        self.root.mkdir()
        self.make()

    def make(self, purpose='public_release', family='b01', case_id='rollback'):
        self.value = profile.profile_for(family, case_id, purpose=purpose)
        self.case = next(case for case in b01_cases.definitions() if case['case_id'] == 'rollback')
        files = physical_files(SQLITE_LAYOUT, self.case['before'])
        schema = observer.b01_observer.sqlite_schema_sha256(files['catalog.sqlite'])
        self.plan = review.LayoutPlan(family, admission.source_sha256(self.files),
            (execution.b01 if family == 'b01' else execution.b02).source_sha256(self.files), self.commit, self.tree,
            case_id, self.value.sha256, SQLITE_LAYOUT, ('catalog.sqlite',), schema,
            'forced_schedule_unavailable' if family == 'b02' and case_id in execution.b02.FORCED_CASE_IDS else 'ordinary_public_operations', purpose)
        self.review, self.review_journal, self.review_head = enroll(self.root, self.plan)
        self.addCleanup(self.review_head.close); self.addCleanup(self.review_journal.close)
        self.policy = execution.StoragePolicy()
        self.binding = execution.binding_for(self.files, self.value, self.policy, {'kind': 'fixture-no-Docker'},
            self.plan, review_authority=self.review)
        self.subject = registry.Subject('cohort', COHORT[0], 'M4', 'a' * 64, execution.TARGET_CONTRACT, self.binding.source_sha256)
        self.gate = execution.gate_for(self.subject, self.binding, gate_id='storage-slice')
        self.registration = execution.StorageRegistration(self.binding, self.commit, self.tree, 'fresh-1', self.gate, COHORT)
        registered = execution.observation_registration(self.registration)
        self.current = {'registration': registered, 'freeze': None}
        self.admission = admission.ObservationAdmission(registered, verify_registration=lambda: self.current['registration'],
            verify_cohort=lambda: self.current['freeze'])
        self.raw, self.delta = self.root / 'raw', self.root / 'delta'
        self.head = ExternalHead.create(self.root / 'head', journal_roots=(self.raw, self.delta))
        self.addCleanup(self.head.close)

    def owner(self, *, expected=None, registration=None):
        owner = execution.CandidateStorageExecution(self.raw, self.store, registration or self.registration, self.policy,
            value=self.value, plan=self.plan, review_authority=self.review, admission_authority=self.admission,
            checkpoint_authority=self.head, delta_root=self.delta, cleanup_root=self.root / 'cleanup', mode='fixture',
            expected_checkpoint=expected)
        self.addCleanup(owner.close)
        return owner

    def intent(self, owner):
        owner._retain('intent.json', execution.encoded({'protocol': execution.PROTOCOL, 'execution_id': 'fixture-execution',
            'source_sha256': self.binding.source_sha256, 'original_binding': asdict(self.binding),
            'registration': asdict(owner.observation_registration), 'cohort_freeze': None,
            'container': 'fixture-container', 'volume': 'fixture-volume', 'ordered_phases': list(execution.b01.PHASES)}))

    def test_common_and_native_source_both_bound_without_historical_relabel(self):
        owner = self.owner()
        self.assertEqual(owner.binding.source_sha256, admission.source_sha256(self.files))
        self.assertNotEqual(owner.binding.source_sha256, owner.binding.native_source_sha256)
        self.assertEqual(owner.observation_registration.original_definition_purpose, 'harness_qualification')
        self.assertEqual(self.gate.ordered_case_ids, self.value.decisive_ids + (execution.mechanics_case_id(self.value),))

    def test_fixture_cannot_dispatch_or_become_physical_observation(self):
        owner = self.owner()
        with self.assertRaises(execution.ExecutionError):
            owner.execute_once()
        with self.assertRaises(observer.AuthorityError):
            observer.StorageObservationSource(owner, owner.checkpoint())
        self.assertFalse(owner.has_retained('intent.json'))

    def test_wrong_gate_or_profile_purpose_cannot_be_adopted(self):
        changed = replace(self.registration, gate=replace(self.gate, ordered_case_ids=self.gate.ordered_case_ids[:-1]))
        with self.assertRaises(execution.ExecutionError):
            self.owner(registration=changed)
        with self.assertRaises(execution.ExecutionError):
            replace(self.binding, purpose='harness_qualification')

    def test_reopen_authenticates_external_prefix_before_config(self):
        owner = self.owner()
        expected = owner.checkpoint()
        owner.close()
        reopened = self.owner(expected=expected)
        self.assertEqual(reopened.checkpoint(), expected)
        reopened.close()
        (self.raw / 'config.json').write_bytes(b'{}')
        with self.assertRaises(chain.ChainError):
            self.owner(expected=expected)

    def test_full_capture_limit_is_preserved_by_authenticated_chunks(self):
        owner = self.owner()
        raw = b'x' * (execution.CHUNK_BYTES + 123)
        owner._retain_blob('large-capture.bin', raw)
        self.assertEqual(owner.read_blob('large-capture.bin'), raw)
        expected = owner.checkpoint()
        owner.close()
        reopened = self.owner(expected=expected)
        self.assertEqual(reopened.read_blob('large-capture.bin'), raw)
        (self.raw / 'large-capture.bin-part-0').write_bytes(b'z')
        with self.assertRaises(chain.ChainError):
            reopened.read_blob('large-capture.bin')

    def test_revoked_registration_stops_command_before_dispatch_intent_or_popen(self):
        owner = self.owner()
        self.current['registration'] = None
        commands = execution._Commands(owner)
        real_popen = subprocess.Popen
        def no_docker(argv, *args, **kwargs):
            self.assertNotEqual(argv[0], 'docker')
            return real_popen(argv, *args, **kwargs)
        with patch.object(execution.b01.subprocess, 'Popen', side_effect=no_docker):
            with self.assertRaises(admission.AdmissionUnavailable):
                commands.run('would-run', ['docker', 'version'])
        self.assertFalse(owner.has_retained('would-run-dispatch.json'))

    def test_lost_acknowledgment_prevents_effect_and_current_authority(self):
        owner = self.owner()
        real_popen = subprocess.Popen
        def no_docker(argv, *args, **kwargs):
            self.assertNotEqual(argv[0], 'docker')
            return real_popen(argv, *args, **kwargs)
        with patch.object(self.head, 'compare_and_set', return_value=False), patch.object(execution.b01.subprocess, 'Popen', side_effect=no_docker):
            with self.assertRaises(chain.ChainError):
                execution._Commands(owner).run('would-run', ['docker', 'version'])
        with self.assertRaises(chain.ChainError):
            owner.checkpoint()

    def test_no_original_terminal_still_preserves_known_failed_result_diagnostic(self):
        owner = self.owner(); self.intent(owner)
        # No process origin is fabricated: result alone must remain unavailable
        # until actual created-container proof and raw response chronology exist.
        owner._retain('after-response.json', b'{"phase":"after","value":{"error":"wrong"}}\n')
        result = observer.reconstruct(owner)
        self.assertIsNone(result['projection']['checks']['result'])
        self.assertEqual(result['mechanics']['status'], 'infrastructure_error')
        self.assertIsNone(result['original_terminal_sha256'])
        self.assertFalse(result['whole_project_acceptance'])

    def stage_proof(self, owner, phase):
        application = {'protocol': execution.PROTOCOL, 'decision': 'not-requested',
            'review_sha256': owner.review_sha256, 'production_forced_schedule_qualified': False}
        proof = {'source_manifest': admission.source_manifest(owner.files),
            'helper_manifest': admission.source_manifest(execution.adapter_files('b01', owner.binding.case_id, application)),
            'fixtures_sha256': execution.digest([])}
        for boundary in ('before', 'after'):
            owner._retain(phase + '-staging-' + boundary + '.json', execution.encoded(proof))
            owner._retain(phase + '-runtime-' + boundary + '-verified.json', execution.encoded(
                {'runtime': owner.runtime, 'runtime_sha256': execution.digest(owner.runtime)}))

    def test_invalid_original_endpoint_prevents_new_effect_intent(self):
        owner = self.owner()
        # Exercise only the endpoint guard branch; this does not turn the
        # fixture journal into a physical observation or dispatch candidate code.
        owner.mode = 'physical'
        endpoint = Mock()
        endpoint.validate.side_effect = execution.ExecutionError('fixture endpoint replaced')
        owner.endpoint = endpoint
        with self.assertRaisesRegex(execution.ExecutionError, 'endpoint replaced'):
            execution._Commands(owner).run('blocked-endpoint', ['docker', 'version'])
        self.assertFalse(owner.has_retained('blocked-endpoint-dispatch.json'))
        owner.mode = 'fixture'

    def test_changed_runtime_does_not_publish_phase_provenance(self):
        owner = self.owner()
        original = execution.process
        fake = SimpleNamespace(runtime_identity=lambda *args, **kwargs: {'kind': 'changed-runtime'})
        with patch.object(execution, 'process', fake):
            with self.assertRaisesRegex(execution.ExecutionError, 'runtime changed'):
                owner._phase_runtime('before-runtime-before')
        self.assertIs(execution.process, original)
        self.assertFalse(owner.has_retained('before-runtime-before-verified.json'))

    def test_strict_staging_rejects_same_bytes_symlink_extra_directory_and_helper_change(self):
        root = self.root / 'stage'; root.mkdir()
        (root / 'file').write_bytes(b'original')
        execution._verify_regular_tree(root, {'file': b'original'})
        (root / 'extra').mkdir()
        with self.assertRaises(execution.ExecutionError):
            execution._verify_regular_tree(root, {'file': b'original'})
        (root / 'extra').rmdir()
        (root / 'file').unlink(); (root / 'file').symlink_to(self.root / 'other')
        (self.root / 'other').write_bytes(b'original')
        with self.assertRaises(execution.ExecutionError):
            execution._verify_regular_tree(root, {'file': b'original'})
        (root / 'file').unlink(); (root / 'file').write_bytes(b'changed')
        with self.assertRaises(execution.ExecutionError):
            execution._verify_regular_tree(root, {'file': b'original'})

    def test_phase_and_finish_lost_ack_never_write_candidate_stdin(self):
        for method in ('phase', 'finish'):
            with self.subTest(method=method):
                root = self.root / method; root.mkdir()
                previous = self.root; self.root = root; self.make(); self.root = previous
                owner = self.owner()
                session = object.__new__(execution._Session)
                session.owner, session.finished, session.original = owner, False, Mock()
                with patch.object(self.head, 'compare_and_set', return_value=False):
                    with self.assertRaises(chain.ChainError):
                        session.phase('before') if method == 'phase' else session.finish(True)
                session.original.phase.assert_not_called()
                session.original.finish.assert_not_called()

    def test_post_ack_registration_revocation_prevents_command_dispatch(self):
        owner = self.owner()
        real_cas = self.head.compare_and_set
        def revoke(expected, replacement):
            result = real_cas(expected, replacement)
            self.current['registration'] = None
            return result
        real_popen = subprocess.Popen
        def no_docker(argv, *args, **kwargs):
            self.assertNotEqual(argv[0], 'docker')
            return real_popen(argv, *args, **kwargs)
        with patch.object(self.head, 'compare_and_set', side_effect=revoke), patch.object(subprocess, 'Popen', side_effect=no_docker):
            with self.assertRaises(admission.AdmissionUnavailable):
                execution._Commands(owner).run('blocked-after-ack', ['docker', 'version'])
        self.assertTrue(owner.has_retained('blocked-after-ack-dispatch.json'))

    def fake_local_process(self, *, interrupt_wait=False):
        class Child:
            def __init__(self):
                self.stdin, self.stdout, self.stderr = io.BytesIO(), io.BytesIO(), io.BytesIO()
                self.returncode = None; self.killed = False; self.waited = 0
            def poll(self): return self.returncode
            def kill(self): self.killed = True; self.returncode = -9
            def wait(self, timeout):
                self.waited += 1
                if interrupt_wait and self.waited == 1:
                    raise KeyboardInterrupt('fixture interrupted wait')
                self.returncode = 0 if self.returncode is None else self.returncode
                return self.returncode
        return Child()

    def fake_threads(self, *, fail_second=False):
        made = []
        class Reader:
            def __init__(self, **kwargs): self.ident = None; self.joined = False; made.append(self)
            def start(self):
                if fail_second and len([row for row in made if row.ident is not None]) == 1:
                    raise RuntimeError('fixture partial reader start')
                self.ident = 1
            def join(self, timeout): self.joined = True
            def is_alive(self): return False
        return Reader, made

    def test_session_partial_reader_start_kills_and_joins_local_child(self):
        owner = self.owner(); child = self.fake_local_process()
        reader, made = self.fake_threads(fail_second=True)
        real_popen = subprocess.Popen
        def popen(argv, *args, **kwargs):
            return child if argv[0] == 'docker' else real_popen(argv, *args, **kwargs)
        with patch.object(subprocess, 'Popen', side_effect=popen), patch.object(execution, 'threading', SimpleNamespace(Thread=reader, get_ident=threading.get_ident)):
            with self.assertRaisesRegex(RuntimeError, 'partial reader'):
                execution._Session(owner, execution._Commands(owner), ['docker', 'exec', 'fixture'])
        self.assertTrue(child.killed)
        self.assertTrue(made[0].joined)
        self.assertFalse(made[1].joined)
        self.assertTrue(all(pipe.closed for pipe in (child.stdin, child.stdout, child.stderr)))

    def test_interrupted_control_wait_kills_and_joins_local_child(self):
        owner = self.owner(); child = self.fake_local_process(interrupt_wait=True)
        reader, made = self.fake_threads()
        real_popen = subprocess.Popen
        def popen(argv, *args, **kwargs):
            return child if argv[0] == 'docker' else real_popen(argv, *args, **kwargs)
        with patch.object(subprocess, 'Popen', side_effect=popen), patch.object(execution, 'threading', SimpleNamespace(Thread=reader, get_ident=threading.get_ident)):
            with self.assertRaises(KeyboardInterrupt):
                execution._Commands(owner).run('interrupted', ['docker', 'version'])
        self.assertTrue(child.killed)
        self.assertTrue(all(row.joined for row in made))
        self.assertTrue(all(pipe.closed for pipe in (child.stdin, child.stdout, child.stderr)))
        self.assertFalse(owner.has_retained('interrupted.json'))

    def command(self, owner, label, argv, raw):
        argv = owner.docker + argv[1:]
        owner._retain(label + '-dispatch.json', execution.encoded({'argv': argv, 'limit': execution.b01.MAX_CAPTURE_BYTES}))
        owner._retain_blob(label + '-stdout.bin', raw)
        owner._retain(label + '-stderr.bin', b'')
        record = {'argv': argv, 'arguments': argv, 'exit_code': 0, 'timed_out': False, 'capture_complete': True}
        for kind, value in (('stdout', raw), ('stderr', b'')):
            record[kind] = {'path': label + '-' + kind + '.bin', 'sha256': execution.sha(value),
                'bytes': len(value), 'observed_bytes': len(value), 'truncated': False}
        owner._retain(label + '.json', execution.encoded(record))

    def test_authenticated_earlier_failure_survives_missing_reopen_resume_terminal(self):
        owner = self.owner(); self.intent(owner)
        container_id = 'c' * 64
        self.command(owner, 'container-create', ['docker', 'create'], container_id.encode() + b'\n')
        self.stage_proof(owner, 'after')
        owner._retain('after-request.json', execution.encoded({'phase': 'after', 'request': 'after\n'}))
        owner._retain('after-response.json', b'{"phase":"after","value":{"error":"wrong"}}\n')
        self.command(owner, 'after-pause', ['docker', 'pause', container_id], container_id.encode())
        inspection = {'Id': container_id, 'Name': '/fixture-container', 'Image': self.policy.image_id,
            'State': {'Running': True, 'Paused': True, 'Pid': 123, 'StartedAt': 'fixture-start'},
            'Config': {'Labels': {'gossip.execution': 'fixture-execution', 'gossip.source': self.binding.source_sha256,
                'gossip.fixture': execution.digest({'recipe': None, 'adapter': execution.sha(execution.b01.CHILD_ADAPTER.encode())})}},
            'HostConfig': {'NetworkMode': 'none', 'ReadonlyRootfs': True},
            'Mounts': [{'Destination': '/tmp', 'Type': 'volume', 'Name': 'fixture-volume', 'Driver': 'local', 'RW': True}]}
        self.command(owner, 'after-state', ['docker', 'inspect', '--format', '{{json .}}', container_id], execution.encoded(inspection))
        files = physical_files(SQLITE_LAYOUT, self.case['after'], edit=lambda blobs, docs, jobs: blobs.append(
            {'blob_id': 'blob-' + execution.sha(b'leak'), 'content': b'leak'}))
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode='w') as archive:
            directory = tarfile.TarInfo('tmp'); directory.type = tarfile.DIRTYPE; archive.addfile(directory)
            for path, raw in files.items():
                entry = tarfile.TarInfo('tmp/' + path); entry.size = len(raw); archive.addfile(entry, io.BytesIO(raw))
        self.command(owner, 'after-capture', ['docker', 'cp', container_id + ':/tmp', '-'], output.getvalue())
        result = observer.reconstruct(owner)
        self.assertIs(result['projection']['checks']['after.persisted-state'], False)
        self.assertIs(result['projection']['checks']['result'], False)
        self.assertIsNone(result['projection']['checks']['reopened.persisted-state'])
        self.assertEqual(result['mechanics']['status'], 'infrastructure_error')
        self.assertIn('after:resume-original-unavailable', result['mechanics']['unavailable'])
        self.assertEqual(result['physical_receipt_basis'], 'durable-intent-partial-observations')
        self.partial_fixture = (owner, container_id, inspection, output.getvalue())

    def test_same_phase_wrong_identity_censors_its_result_but_keeps_earlier_failure(self):
        owner = self.owner(); self.intent(owner)
        container_id = 'c' * 64
        self.command(owner, 'container-create', ['docker', 'create'], container_id.encode() + b'\n')
        inspection = {'Id': container_id, 'Name': '/fixture-container', 'Image': self.policy.image_id,
            'State': {'Running': True, 'Paused': True, 'Pid': 123, 'StartedAt': 'fixture-start'},
            'Config': {'Labels': {'gossip.execution': 'fixture-execution', 'gossip.source': self.binding.source_sha256,
                'gossip.fixture': execution.digest({'recipe': None, 'adapter': execution.sha(execution.b01.CHILD_ADAPTER.encode())})}},
            'HostConfig': {'NetworkMode': 'none', 'ReadonlyRootfs': True},
            'Mounts': [{'Destination': '/tmp', 'Type': 'volume', 'Name': 'fixture-volume', 'Driver': 'local', 'RW': True}]}
        for phase in ('before', 'after'):
            self.stage_proof(owner, phase)
            owner._retain(phase + '-request.json', execution.encoded({'phase': phase, 'request': phase + '\n'}))
            owner._retain(phase + '-response.json', execution.encoded({'phase': phase, 'value': {'error': 'wrong'}}) + b'\n')
            self.command(owner, phase + '-pause', ['docker', 'pause', container_id], container_id.encode())
            if phase == 'after':
                inspection['Config']['Labels']['gossip.fixture'] = 'd' * 64
            self.command(owner, phase + '-state', ['docker', 'inspect', '--format', '{{json .}}', container_id], execution.encoded(inspection))
            files = physical_files(SQLITE_LAYOUT, self.case['before'], edit=lambda blobs, docs, jobs: blobs.append(
                {'blob_id': 'blob-' + execution.sha(b'leak'), 'content': b'leak'}))
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode='w') as archive:
                entry = tarfile.TarInfo('tmp'); entry.type = tarfile.DIRTYPE; archive.addfile(entry)
                for path, raw in files.items():
                    entry = tarfile.TarInfo('tmp/' + path); entry.size = len(raw); archive.addfile(entry, io.BytesIO(raw))
            self.command(owner, phase + '-capture', ['docker', 'cp', container_id + ':/tmp', '-'], output.getvalue())
        result = observer.reconstruct(owner)
        self.assertIs(result['projection']['checks']['before.persisted-state'], False)
        self.assertIsNone(result['projection']['checks']['after.persisted-state'])
        self.assertIsNone(result['projection']['checks']['result'])
        self.assertFalse(result['phase_facts'][1]['response_authenticated'])

    def _later_attribution_mismatch(self, kind):
        self.test_authenticated_earlier_failure_survives_missing_reopen_resume_terminal()
        owner, container_id, inspection, raw = self.partial_fixture
        self.stage_proof(owner, 'reopened')
        owner._retain('reopened-request.json', execution.encoded({'phase': 'reopened', 'request': 'reopened\n'}))
        owner._retain('reopened-response.json', b'{"phase":"reopened","value":null}\n')
        self.command(owner, 'reopened-pause', ['docker', 'pause', container_id], container_id.encode())
        if kind == 'pid':
            inspection['State']['Pid'] += 1
        else:
            inspection['Config']['Labels']['gossip.fixture'] = 'd' * 64
        self.command(owner, 'reopened-state', ['docker', 'inspect', '--format', '{{json .}}', container_id], execution.encoded(inspection))
        self.command(owner, 'reopened-capture', ['docker', 'cp', container_id + ':/tmp', '-'], raw)
        result = observer.reconstruct(owner)
        self.assertIs(result['projection']['checks']['after.persisted-state'], False)
        self.assertIsNone(result['projection']['checks']['reopened.persisted-state'])
        self.assertEqual(result['mechanics']['status'], 'infrastructure_error')
        expected = 'lineage changed' if kind == 'pid' else 'sandbox or execution lineage differs'
        self.assertIn(expected, result['phase_facts'][2]['reason'])

    def test_later_pid_change_withholds_phase_but_preserves_prior_known_failure(self):
        self._later_attribution_mismatch('pid')

    def test_later_fixture_label_change_is_unavailable_not_product_failure(self):
        self._later_attribution_mismatch('fixture')

    def test_existing_incomplete_intent_forbids_redispatch(self):
        owner = self.owner(); self.intent(owner)
        with self.assertRaisesRegex(execution.ExecutionError, 'redispatch'):
            owner.execute_once()

    def test_independent_gate_requires_exact_full_freeze_and_rejects_revocation(self):
        self.root = self.root / 'independent'; self.root.mkdir()
        self.make('independent_acceptance')
        owner = self.owner()
        with self.assertRaises(admission.AdmissionUnavailable):
            owner._effect_boundary()
        subjects = tuple(replace(self.subject, trajectory_id=trajectory,
            source_sha256=self.subject.source_sha256 if i == 0 else str(i) * 64)
            for i, trajectory in enumerate(COHORT))
        freeze = registry.CohortFreeze(subjects, 'b' * 64, 'c' * 64, True)
        self.current['freeze'] = freeze
        owner._freeze = freeze
        owner._effect_boundary()
        self.current['freeze'] = replace(freeze, subjects=freeze.subjects[:-1])
        with self.assertRaises(admission.AdmissionError):
            owner._effect_boundary()
        self.current['freeze'] = replace(freeze, receipt_sha256='d' * 64)
        with self.assertRaises(admission.AdmissionError):
            owner._effect_boundary()

    def test_finish_admission_failure_still_permits_local_failure_teardown(self):
        owner = self.owner()
        session = object.__new__(execution._Session)
        session.owner, session.finished, session.original = owner, False, Mock()
        self.current['registration'] = None
        with self.assertRaises(admission.AdmissionUnavailable):
            session.finish(True)
        self.assertFalse(session.finished)
        session.original.finish.assert_not_called()
        session.finish(False)
        session.original.finish.assert_called_once_with(False)

    def test_selector_catalog_matches_exact_gate_and_keeps_remaining_scope(self):
        catalog = observer.selector_catalog('b01', 'rollback', purpose='public_release')
        self.assertEqual(tuple(catalog['ordered_case_ids']), self.gate.ordered_case_ids)
        self.assertTrue(catalog['required_unfinished_coverage'])
        self.assertFalse(catalog['semantic_authority'])
        self.assertFalse(catalog['scope_factory_registered'])
        self.assertTrue(all(row['observation_pointer'].startswith(('/projection/', '/mechanics/'))
                            for row in catalog['selectors']))

    def test_old_storageexecution_dict_is_not_an_owner(self):
        with self.assertRaises(observer.AuthorityError):
            observer.reconstruct({'snapshots': {}, 'completed': True})

    def test_intent_record_does_not_allow_qualification_to_product_relabel(self):
        owner = self.owner(); self.intent(owner)
        self.assertEqual(owner.retained_freeze(), None)
        with self.assertRaises(admission.AdmissionError):
            admission.ObservationRegistration(replace(self.gate, binding=replace(self.gate.binding, purpose='repeatability')),
                self.commit, self.tree, 'fresh-1', COHORT, self.binding.definition_sha256,
                self.binding.profile_sha256, 'harness_qualification', admission.binding_sha256(self.binding,
                    gate=replace(self.gate, binding=replace(self.gate.binding, purpose='repeatability'))))
