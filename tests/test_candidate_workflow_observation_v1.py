"""Actual original-reader sensitivity over explicitly synthetic retained bytes.

These offline fixtures never execute a candidate or Engine and stay fixture mode.
The live process/semantic qualification remains the separate physical lane.
"""
from copy import deepcopy
from dataclasses import asdict, dataclass, replace
import io
import os
from pathlib import Path
import tarfile
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

from gossip_harness import candidate_workflow_execution_v1 as execution
from gossip_harness import candidate_workflow_observation_v1 as observer
from gossip_harness import candidate_workflow_profile_v1 as profile
from gossip_harness import candidate_workflow_review_v1 as review
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_execution_journal_v1 as journals
from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from tests.test_candidate_storage_prestart_v1 import created_fixture
from tests.test_candidate_workflow_execution_v1 import exec_value


def http_original(value):
    body = execution.encoded(value)
    return b'HTTP/1.1 200 OK\r\nContent-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body


class ByteOwner:
    def __init__(self, records):
        self.records = records
        self.reads = []
        self.docker = ['docker']

    def has_retained(self, name):
        return name in self.records

    def read_authenticated(self, name):
        self.reads.append(name)
        return self.records[name]

    read_blob = read_authenticated


class CandidateWorkflowObservationTests(unittest.TestCase):
    def test_engine_request_and_full_response_are_bound(self):
        path = '/exec/' + 'e' * 64 + '/json'
        records = {'x-request.bin': execution.process._request('GET', path), 'x-response.bin': http_original(exec_value())}
        owner = ByteOwner(records)
        self.assertEqual(observer._engine(owner, 'x', path), exec_value())
        records['x-request.bin'] = execution.process._request('GET', '/exec/' + 'f' * 64 + '/json')
        with self.assertRaisesRegex(observer.AuthorityError, 'request identity'):
            observer._engine(owner, 'x', path)

    def test_absent_prefix_member_is_unavailable_without_reading_it(self):
        owner = ByteOwner({'x-request.bin': b'present'})
        with self.assertRaises(observer.AuthorityUnavailable):
            observer._engine(owner, 'x', '/exec/absent/json')
        self.assertEqual(owner.reads, [])

    def test_incomplete_engine_body_is_not_accepted(self):
        path = '/exec/' + 'e' * 64 + '/json'
        records = {'x-request.bin': execution.process._request('GET', path), 'x-response.bin': http_original(exec_value())[:-1]}
        with self.assertRaises(ValueError):
            observer._engine(ByteOwner(records), 'x', path)

    def test_duplicate_engine_json_fields_are_not_accepted(self):
        path = '/exec/' + 'e' * 64 + '/json'
        raw = b'{"Running":true,"Running":false}'
        wire = b'HTTP/1.1 200 OK\r\nContent-Length: ' + str(len(raw)).encode() + b'\r\n\r\n' + raw
        with self.assertRaises(ValueError):
            observer._engine(ByteOwner({'x-request.bin': execution.process._request('GET', path), 'x-response.bin': wire}), 'x', path)

    def test_missing_or_ambiguous_exec_census_unavailable(self):
        cid = 'c' * 64
        for ids in (None, [], ['e' * 64, 'f' * 64]):
            owner = ByteOwner({'r-exec-container-request.bin': execution.process._request('GET', '/containers/' + cid + '/json'),
                'r-exec-container-response.bin': http_original({'Id': cid, 'ExecIDs': ids})})
            with self.subTest(ids=ids), self.assertRaises(observer.AuthorityUnavailable):
                observer._exec(owner, 'r', cid, None)

    def test_fixed_capture_paths_and_active_sidecars_remain_unavailable(self):
        plan = SimpleNamespace(storage_paths=('catalog.sqlite',), schema_sha256='a' * 64)
        for files, paths in (({}, {'root': None, 'database': None}),
            ({'one/catalog.sqlite': b'raw'}, {'root': '/tmp/two', 'database': '/tmp/two/catalog.sqlite'}),
            ({'one/catalog.sqlite': b'raw', 'one/catalog.sqlite-wal': b'raw'}, {'root': '/tmp/one', 'database': '/tmp/one/catalog.sqlite'})):
            with self.subTest(paths=paths), self.assertRaises(observer.AuthorityUnavailable):
                observer.captured_state(files, paths, plan)

    def test_missing_tail_preserves_known_call_failure_in_profile(self):
        value = profile.profile_for('WF18-same-process-call-isolation')
        actual = deepcopy(value.calls[0]['expected'])
        actual['documents'] = []
        record = profile.project(value, {0: actual})
        first = [row for row in record['observations'] if row['call_index'] == 0]
        later = [row for row in record['observations'] if row['call_index'] > 0]
        self.assertIn('fail', [row['disposition'] for row in first])
        self.assertEqual({row['disposition'] for row in later}, {'unavailable'})

    def test_closed_qualifier_reconstructs_actual_frame_semantics_and_normalized_size(self):
        for control in ('WQ-NORMALIZED-61824', 'WQ-FRAME-EXACT'):
            raw, _ = execution.qualification_bytes(control)
            row = observer._qualification_semantics(control, raw)
            self.assertTrue(row['applicable'])
            self.assertTrue(row['comparison'])
            self.assertEqual(row['frame_sha256'], execution.sha(raw[len(execution._READY):]))
            if control == 'WQ-NORMALIZED-61824':
                self.assertEqual(row['normalized_bytes'], 61824)
            self.assertIsNone(observer._qualification_semantics(control, raw[:-1])['comparison'])
        wrong = execution._READY + execution.encoded({'kind': 'result', 'phase': 'call-000', 'value': 'y' * 61822}) + b'\n'
        self.assertFalse(observer._qualification_semantics('WQ-NORMALIZED-61824', wrong)['comparison'])
        wrong_type = execution._READY + execution.encoded({'kind': 'result', 'phase': 'call-000', 'value': 61824}) + b'\n'
        self.assertFalse(observer._qualification_semantics('WQ-NORMALIZED-61824', wrong_type)['comparison'])
        raw, _ = execution.qualification_bytes('WQ-FRAME-OVER')
        self.assertFalse(observer._qualification_semantics('WQ-FRAME-OVER', raw)['applicable'])
        self.assertIsNone(observer._qualification_semantics('WQ-FRAME-EXACT', raw)['comparison'])
        malformed = execution._READY + b'{"kind":"result","phase":"call-000","value":{},"value":{}}\n'
        self.assertIsNone(observer._qualification_semantics('WQ-FRAME-EXACT', malformed)['comparison'])

    def test_runtime_and_host_catalog_capabilities_match_actual_declared_gate_lanes(self):
        from gossip_harness import cumulative_scope_source_v3 as source
        catalog = source.load_catalog(Path(__file__).resolve().parents[1])
        logical = {row.id: row for row in catalog.inventory.logical_gates}
        self.assertEqual(logical['M1-GATE-PUBLIC'].lane, 'public-contract')
        self.assertEqual(logical['M1-GATE-ADAPTER'].lane, 'workflow')
        self.assertEqual(logical['M4-COMPATIBILITY.public-contract'].lane, 'public-contract')
        runtime_lanes = set()
        # Only source-hash enumeration is replaced: the real profile selectors
        # and source catalog determine every logical gate and lane association.
        with mock.patch.object(execution, 'evaluator_sources', return_value={}):
            for case_id in profile.CASE_IDS:
                record = observer.selector_catalog(case_id, purpose='public_release')
                self.assertEqual(record['capabilities'], ['public-contract', 'workflow'])
                self.assertFalse(record['semantic_authority'])
                for row in record['selectors']:
                    lanes = {logical[gate_id].lane for facet in row['source_unit_facets']
                        for gate_id in facet['logical_gate_ids']}
                    self.assertTrue(lanes.issubset(set(record['capabilities'])))
                    runtime_lanes.update(lanes)
        self.assertEqual(runtime_lanes, {'public-contract', 'workflow'})
        with mock.patch.object(review, 'inspection_evaluator_sources', return_value={}):
            host = observer.inspection_selector_catalog(purpose='independent_acceptance')
        self.assertEqual(host['capabilities'], ['source-inspection'])
        self.assertFalse(host['semantic_authority'])
        self.assertEqual({logical[gate_id].lane for row in host['selectors']
            for facet in row['source_unit_facets'] for gate_id in facet['logical_gate_ids']},
            {'source-inspection'})

    def test_uploaded_verdict_and_qualifier_owner_never_publish_product(self):
        with self.assertRaises(observer.AuthorityError):
            observer.reconstruct({'passed': True})
        owner = object.__new__(execution.CandidateWorkflowQualificationExecution)
        owner.mode = 'physical'
        with self.assertRaises(observer.AuthorityError):
            observer.publish_verifier(owner)
        with self.assertRaises(observer.AuthorityError):
            observer.WorkflowObservationSource(owner, None)
        with self.assertRaises(observer.AuthorityError):
            observer.WorkflowInspectionObservationSource(owner, None)


class CandidateWorkflowOriginalReaderTests(unittest.TestCase):
    """Real journal/prefix and parser composition; synthetic source/Engine facts."""
    def setUp(self):
        self.prepare_owner()

    def prepare_owner(self, case_id='WF18-same-process-call-isolation', schema_sha256='d' * 64):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name).resolve()
        owner = object.__new__(execution.CandidateWorkflowExecution)
        owner._pid, owner._thread, owner.closed = os.getpid(), threading.get_ident(), False
        owner.mode, owner.root, owner.delta_root = 'fixture', root / 'raw', root / 'delta'
        owner._cleanup_phase, owner._cleanup, owner._session = False, None, None
        head = ExternalHead.create(root / 'head', journal_roots=(owner.root, owner.delta_root))
        self.addCleanup(head.close)
        owner.journal = journals.OwnerJournal(owner.root, owner.delta_root,
            context={'fixture': 'synthetic reader mechanism only'}, authority=head, limits=execution.LIMITS)
        self.addCleanup(owner.close)
        owner.policy = execution.WorkflowPolicy()
        owner.profile = profile.profile_for(case_id)
        owner.files = {'solution.py': b'raise RuntimeError("never import fixture")\n'}
        boundaries = (review.WorkflowBoundary('initial', 'solution.py', 'solve', 1, 'line', 'base', 'path', 'solve_enter'),)
        points = ()
        if case_id == 'WF19-provisional-fault-boundary':
            meanings = {'initial': 'solve_enter', 'post_fault': 'post_fault', 'reopened': 'reopened', 'final': 'solve_exit'}
            boundaries = tuple(review.WorkflowBoundary(role, 'solution.py', 'solve', index + 1, 'line',
                'base', 'path', meaning) for index, (role, meaning) in enumerate(meanings.items()))
            points = tuple(review.WorkflowCapturePoint(role, 0, role, 0) for role in meanings)
        owner.plan = review.WorkflowSourcePlan(admission.source_sha256(owner.files), profile.source_sha256(owner.files),
            'a' * 40, 'b' * 40, owner.profile.case_id, owner.profile.sha256,
            'reviewed-workflow-final-sqlite-v1', ('catalog.sqlite',), schema_sha256, 'public_release',
            boundaries, capture_points=points)
        owner.binding = execution.WorkflowBinding(admission.source_sha256(owner.files), execution.TARGET_CONTRACT,
            'M4', 'public_release', execution.FAMILY, owner.profile.case_id, profile.source_sha256(owner.files),
            execution.digest(owner.profile.record()), owner.profile.sha256, execution.fixture_identity(owner.profile.case_id, owner.plan),
            *(['d' * 64] * 7))
        owner.runtime = {'kind': 'fixture-no-Docker'}
        owner.docker = ['docker']
        owner.review_sha256 = 'd' * 64
        owner.config = {'fixture': True}
        owner.observation_registration = SimpleNamespace()
        # Runtime admission/source authentication is separately tested by actual
        # constructor controls; this fixture isolates the retained-reader path.
        owner.current = lambda freeze=None: None
        owner.observation_registration = SyntheticRegistration()
        owner._retain('config.json', b'{"fixture":true}')
        self.owner = owner
        self.created, expected = created_fixture(inputs=True)
        self.cid = expected['container_id']
        expected.update(image_id=owner.policy.image_id, labels={'gossip.execution': 'fixture-execution',
            'gossip.source': owner.binding.source_sha256, 'gossip.fixture': owner.binding.fixture_sha256})
        self.created['Image'] = owner.policy.image_id
        self.created['Config'].update(Image=owner.policy.image_id, Labels=expected['labels'])
        self.expected = expected
        owner._retain('intent.json', execution.encoded({'protocol': execution.PROTOCOL, 'execution_id': 'fixture-execution',
            'source_sha256': owner.binding.source_sha256, 'original_binding': asdict(owner.binding),
            'registration': asdict(owner.observation_registration), 'cohort_freeze': None,
            'container': expected['name'], 'volume': expected['volume'], 'ordered_phases': list(owner.profile.phases)}))
        helpers = execution.adapter_files(owner.profile.case_id, owner.plan)
        self.stage = {'source_manifest': admission.source_manifest(owner.files), 'helper_manifest': admission.source_manifest(helpers),
            'fixtures_sha256': execution.digest(admission.source_manifest(profile.input_files(owner.profile.case_id)))}
        stage = {'workspace': expected['mounts']['/workspace'], 'checks': expected['mounts']['/checks'],
            'inputs': expected['mounts']['/inputs'], 'source_manifest': self.stage['source_manifest'], 'proof': self.stage}
        owner._retain('staging.json', execution.encoded(stage))
        sandbox = execution.DockerValidator(owner.policy.image_id, {'workflow_adapter.py': execution.ADAPTER}, command=execution.prestart.COMMAND)
        argv = execution.b02._start_arguments(sandbox, expected['name'], Path(stage['workspace']), Path(stage['checks']),
            Path(stage['inputs']), expected['volume'])
        argv[1] = 'create'
        argv.remove('--detach')
        index = argv.index('--entrypoint')
        for key, value in expected['labels'].items():
            argv[index:index] = ['--label', key + '=' + value]
            index += 2
        self.command('volume-created', ['docker', 'volume', 'inspect', '--format', '{{json .}}', expected['volume']],
            execution.encoded({'Name': expected['volume'], 'Driver': 'local', 'Scope': 'local', 'Options': execution.b01.VOLUME_OPTIONS,
                'Labels': {'gossip.execution': 'fixture-execution', 'gossip.snapshot': execution.b01.SNAPSHOT_PROTOCOL}}))
        self.command('container-create', argv, self.cid.encode())
        self.command('container-prestart', ['docker', 'inspect', '--format', '{{json .}}', self.cid], execution.encoded(self.created))
        owner._retain(execution.prestart.PROOF_FILE, execution.encoded(execution.prestart.proof_for(execution.encoded(self.created), **expected)))
        self.command('container-start', ['docker', 'start', self.cid], self.cid.encode())
        owner._retain('session-dispatch.json', execution.encoded({'argv': ['docker', 'exec', '--interactive', '--user', '65534:65534',
            self.cid, 'python', '-I', '-B', '/checks/workflow_adapter.py']}))
        self.running = deepcopy(self.created)
        self.running['State'].update(Status='running', Running=True, Paused=True, Pid=123,
            StartedAt='2026-10-04T00:00:01.000000001Z')
        owner._retain('session-ready.bin', execution._READY)
        self.engine('session-ready', first=True)
        self.capture('session-ready')
        owner._retain('session-ready-ack.json', execution.encoded({'request': 'ready\n'}))

    def command(self, label, argv, raw):
        owner = self.owner
        owner._retain(label + '-dispatch.json', execution.encoded({'argv': argv, 'limit': execution.b02.MAX_CAPTURE_BYTES}))
        record = {'argv': argv, 'arguments': argv, 'exit_code': 0, 'timed_out': False, 'capture_complete': True}
        for kind, data in (('stdout', raw), ('stderr', b'')):
            name = label + '-' + kind + '.bin'
            owner._retain_blob(name, data)
            record[kind] = {'path': name, 'bytes': len(data), 'observed_bytes': len(data), 'sha256': execution.sha(data), 'truncated': False}
        owner._retain(label + '.json', execution.encoded(record))

    def engine(self, label, *, first=False, pid=17):
        owner = self.owner
        self.guard(label, 'before')
        if first:
            owner._retain(label + '-exec-container-request.bin', execution.process._request('GET', '/containers/' + self.cid + '/json'))
            owner._retain(label + '-exec-container-response.bin', http_original({'Id': self.cid, 'ExecIDs': ['e' * 64]}))
        value = exec_value()
        value.update(ContainerID=self.cid, Pid=pid)
        owner._retain(label + '-exec-request.bin', execution.process._request('GET', '/exec/' + 'e' * 64 + '/json'))
        owner._retain(label + '-exec-response.bin', http_original(value))
        owner._retain(label + '-exec-verified.json', execution.encoded({'exec_id': 'e' * 64, 'pid': pid,
            'completed': False, 'value_sha256': execution.digest(value)}))

    def guard(self, label, side):
        owner = self.owner
        owner._retain(label + '-runtime-' + side + '-verified.json', execution.encoded({
            'runtime': owner.runtime, 'runtime_sha256': execution.digest(owner.runtime)}))
        owner._retain(label + '-staging-' + side + '.json', execution.encoded(self.stage))

    def capture(self, label, files=None):
        self.command(label + '-pause', ['docker', 'pause', self.cid], self.cid.encode())
        self.command(label + '-state', ['docker', 'inspect', '--format', '{{json .}}', self.cid], execution.encoded(self.running))
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode='w') as archive:
            root = tarfile.TarInfo('tmp')
            root.type = tarfile.DIRTYPE
            archive.addfile(root)
            directories = {'tmp'}
            for path, raw in (files or {}).items():
                full = 'tmp/' + path
                parent = str(Path(full).parent)
                for directory in reversed((Path(parent), *Path(parent).parents)):
                    name = str(directory)
                    if name not in ('.', 'tmp') and name not in directories:
                        entry = tarfile.TarInfo(name)
                        entry.type = tarfile.DIRTYPE
                        archive.addfile(entry)
                        directories.add(name)
                entry = tarfile.TarInfo(full)
                entry.size = len(raw)
                archive.addfile(entry, io.BytesIO(raw))
        self.command(label + '-capture', ['docker', 'cp', self.cid + ':/tmp', '-'], output.getvalue())
        self.command(label + '-unpause', ['docker', 'unpause', self.cid], self.cid.encode())
        self.guard(label, 'after')

    def call(self, index, value, *, pid=17, capture=True, after=True):
        owner = self.owner
        phase = owner.profile.phases[index]
        owner._retain(phase + '-runtime-before-verified.json', execution.encoded({'runtime': owner.runtime, 'runtime_sha256': execution.digest(owner.runtime)}))
        owner._retain(phase + '-staging-before.json', execution.encoded(self.stage))
        owner._retain(phase + '-request.json', execution.encoded({'request': phase + '\n'}))
        raw = execution.encoded({'kind': 'result', 'phase': phase, 'value': value}) + b'\n'
        owner._retain(phase + '-frame-000.bin', raw)
        owner._retain(phase + '-response.bin', raw)
        self.engine(phase + '-result', pid=pid)
        if capture:
            self.capture(phase + '-result')
            owner._retain(phase + '-next.json', execution.encoded({'request': 'next:' + phase + '\n'}))
        else:
            self.guard(phase + '-result', 'after')
        if after:
            owner._retain(phase + '-runtime-after-verified.json', execution.encoded({'runtime': owner.runtime, 'runtime_sha256': execution.digest(owner.runtime)}))
            owner._retain(phase + '-staging-after.json', execution.encoded(self.stage))
        return raw

    def test_actual_reader_keeps_known_false_and_complete_missing_denominator(self):
        self.call(0, {'wrong': True})
        result = observer.reconstruct(self.owner)
        self.assertEqual(result['projection']['observations'][0]['disposition'], 'fail')
        self.assertEqual(len(result['phase_facts']), 3)
        self.assertEqual([row['response_authenticated'] for row in result['phase_facts']], [True, False, False])
        self.assertEqual(result['mechanics']['status'], 'infrastructure_error')
        self.assertIsNone(result['original_terminal_sha256'])

    def test_missing_post_result_capture_does_not_erase_known_false(self):
        self.call(0, {'wrong': True}, capture=False)
        result = observer.reconstruct(self.owner)
        self.assertEqual(result['projection']['observations'][0]['disposition'], 'fail')
        self.assertFalse(result['phase_facts'][0]['capture_authenticated'])

    def test_later_actual_exec_pid_drift_preserves_earlier_failure(self):
        self.call(0, {'wrong': True})
        self.call(1, {'wrong': 'not attributable'}, pid=18)
        result = observer.reconstruct(self.owner)
        self.assertEqual(result['projection']['observations'][0]['disposition'], 'fail')
        self.assertFalse(result['phase_facts'][1]['response_authenticated'])
        self.assertIn('Same live solve process', result['phase_facts'][1]['reason'])

    def test_retained_raw_tampering_is_fatal_not_missing_tail(self):
        self.call(0, {'wrong': True})
        self.owner.checkpoint()
        (self.owner.root / 'call-000-response.bin').write_bytes(b'changed')
        with self.assertRaises((chain.ChainError, chain.ChainUnknown)):
            observer.reconstruct(self.owner)

    def test_expired_work_deadline_keeps_read_only_prior_failure_but_blocks_effects(self):
        self.call(0, {'wrong': True}, after=False)
        owner = self.owner
        owner.mode, owner._deadline, owner._freeze = 'physical', 1.0, None
        owner.endpoint = mock.Mock()
        owner.store, owner.tree = mock.Mock(), 'b' * 40
        owner.registration = SimpleNamespace(commit_oid='a' * 40)
        owner.sources = {}
        provenance = {'test': 'source authentication seam only'}
        original_binding = owner.binding
        owner.binding = replace(owner.binding, review_origin_sha256=execution.digest(provenance))
        owner.review_authority = mock.Mock()
        owner.review_authority.authenticate.return_value = owner.review_sha256
        owner.review_authority.provenance.return_value = provenance
        owner.admission = mock.Mock()
        # Actual current() remains a source/admission read, even after time.
        with mock.patch.object(execution, 'evaluator_sources', return_value={}), \
                mock.patch.object(execution, 'capture_git_source', return_value=(owner.tree, owner.files)):
            execution.CandidateWorkflowExecution.current(owner, None)
        owner.admission.check_current.assert_called_once()
        with self.assertRaisesRegex(ValueError, 'history deadline'):
            owner._effect_boundary()
        # Restore the original binding, which the retained intent binds.
        owner.binding = original_binding
        owner.mode = 'fixture'
        result = observer.reconstruct(owner)
        self.assertEqual(result['projection']['observations'][0]['disposition'], 'fail')
        self.assertEqual(result['mechanics']['status'], 'infrastructure_error')
        self.assertIn('call-000:whole-call-after-boundary-unavailable', result['mechanics']['unavailable'])

    def test_original_post_fault_capture_failure_survives_missing_reopen_and_call_tail(self):
        from tests.test_candidate_storage_observer_v1 import physical_files, SQLITE_LAYOUT
        import hashlib
        expected = profile.capture_expectations('WF19-provisional-fault-boundary')['post_fault']
        def mutate(blobs, documents, jobs):
            blobs.append({'blob_id': 'blob-' + hashlib.sha256(b'new').hexdigest(), 'content': b'new'})
            for job in jobs:
                job['content_hashes'] = expected['persisted_fields'][job['job_id']]['content_hashes']
        files = physical_files(SQLITE_LAYOUT, expected['data'], edit=mutate)
        self.prepare_owner('WF19-provisional-fault-boundary', observer.storage.sqlite_schema_sha256(files['catalog.sqlite']))
        owner, phase = self.owner, 'call-000'
        self.guard(phase, 'before')
        owner._retain(phase + '-request.json', execution.encoded({'request': phase + '\n'}))
        event = {'kind': 'boundary', 'phase': phase, 'ordinal': 0, 'boundary': 'post_fault', 'occurrence': 0,
            'paths': {'root': '/tmp/one', 'database': '/tmp/one/catalog.sqlite'},
            'path_origins': {'root': 0, 'database': 0}}
        owner._retain(phase + '-frame-000.bin', execution.encoded(event) + b'\n')
        label = phase + '-boundary-000'
        self.engine(label)
        owner._retain(label + '-event.json', execution.encoded(event))
        path_facts = execution.encoded({'root': {'path': '/tmp/one', 'kind': 'directory'},
            'database': {'path': '/tmp/one/catalog.sqlite', 'kind': 'file'}})
        self.command(label + '-paths', ['docker', 'exec', '--user', '65534:65534', self.cid,
            'python', '-I', '-B', '/checks/workflow_paths.py', execution.encoded(event['paths']).decode('ascii')], path_facts)
        owner._retain(label + '-path-facts.json', path_facts)
        self.capture(label, {'one/' + name: raw for name, raw in files.items()})
        # No resume, later boundary, response, whole-call-after, or terminal.
        # Every supplied fact is synthetic and fixture-only, but the reader,
        # journals, exact point selection, SQLite decoder and scorer are real.
        result = observer.reconstruct(owner)
        rows = {row['case_id']: row['disposition'] for row in result['projection']['observations']}
        prefix = 'WF19-provisional-fault-boundary:call-000:capture:'
        self.assertEqual(rows[prefix + 'post_fault:blobs'], 'fail')
        self.assertEqual(rows[prefix + 'post_fault:documents'], 'pass')
        self.assertEqual(rows[prefix + 'reopened:blobs'], 'unavailable')
        self.assertEqual(rows[prefix + 'final:blobs'], 'unavailable')
        self.assertFalse(result['phase_facts'][0]['response_authenticated'])
        self.assertEqual(result['mechanics']['status'], 'infrastructure_error')

    def test_fixture_originals_never_publish_physical_qualification(self):
        self.call(0, profile.expected_for(self.owner.profile.case_id, 0))
        with self.assertRaises(observer.AuthorityError):
            observer.publish_verifier(self.owner)
        with self.assertRaises(observer.AuthorityError):
            observer.WorkflowObservationSource(self.owner, self.owner.checkpoint())
        self.assertFalse(self.owner.has_retained(observer.VERIFIER_FILE))


@dataclass(frozen=True)
class SyntheticRegistration:
    fixture_only: bool = True


class CandidateWorkflowCaptureSelectionTests(unittest.TestCase):
    def test_exact_role_occurrence_and_call_selects_only_authenticated_row(self):
        point = review.WorkflowCapturePoint('post_fault', 0, 'after-error', 1)
        plan = SimpleNamespace(capture_points=(point,))
        earlier = {'data': {'blobs': ['leaked-known-value']}}
        later = {'data': {'blobs': []}}
        facts = [{'boundary_facts': [
            {'event': {'boundary': 'after-error', 'occurrence': 0}, 'storage': later},
            {'event': {'boundary': 'after-error', 'occurrence': 1}, 'storage': earlier}]}]
        self.assertEqual(observer.selected_captures(plan, facts), {0: {'post_fault': earlier}})
        self.assertEqual(observer.selected_captures(plan, []), {})
        facts[0]['boundary_facts'][1]['storage'] = None
        self.assertEqual(observer.selected_captures(plan, facts), {})

    def test_duplicate_exact_capture_is_fatal_not_arbitrarily_selected(self):
        point = review.WorkflowCapturePoint('final', 0, 'close', 0)
        plan = SimpleNamespace(capture_points=(point,))
        row = {'event': {'boundary': 'close', 'occurrence': 0}, 'storage': {'data': {}}}
        with self.assertRaisesRegex(observer.AuthorityError, 'Ambiguous'):
            observer.selected_captures(plan, [{'boundary_facts': [row, deepcopy(row)]}])

    def test_actual_sqlite_orphan_fails_before_later_commit_can_absorb_it(self):
        from tests.test_candidate_storage_observer_v1 import physical_files, SQLITE_LAYOUT
        import hashlib
        expectations = profile.capture_expectations('WF19-provisional-fault-boundary')
        state = expectations['post_fault']
        def mutate(blobs, documents, jobs):
            blobs.append({'blob_id': 'blob-' + hashlib.sha256(b'new').hexdigest(), 'content': b'new'})
            for job in jobs:
                fields = state['persisted_fields'][job['job_id']]
                parsed = next(row for row in state['data']['jobs'] if row['public']['job_id'] == job['job_id'])
                job['manifest'] = execution.encoded(parsed['manifest']).decode('ascii')
                job['content_hashes'] = fields['content_hashes']
        files = physical_files(SQLITE_LAYOUT, state['data'], edit=mutate)
        plan = SimpleNamespace(storage_paths=('catalog.sqlite',),
            schema_sha256=observer.storage.sqlite_schema_sha256(files['catalog.sqlite']),
            capture_points=(review.WorkflowCapturePoint('post_fault', 0, 'after-error', 0),
                            review.WorkflowCapturePoint('final', 0, 'close', 0)))
        captured = observer.captured_state({'one/' + key: raw for key, raw in files.items()},
            {'root': '/tmp/one', 'database': '/tmp/one/catalog.sqlite'}, plan)
        self.assertEqual(captured['data']['documents'], state['data']['documents'])
        facts = [{'boundary_facts': [{'event': {'boundary': 'after-error', 'occurrence': 0}, 'storage': captured}]}]
        selected = observer.selected_captures(plan, facts)
        value = profile.profile_for('WF19-provisional-fault-boundary')
        projected = profile.project(value, {0: value.calls[0]['expected']}, selected)
        by_id = {row['case_id']: row for row in projected['observations']}
        self.assertEqual(by_id['WF19-provisional-fault-boundary:call-000:capture:post_fault:blobs']['disposition'], 'fail')
        self.assertEqual(by_id['WF19-provisional-fault-boundary:call-000:capture:post_fault:documents']['disposition'], 'pass')
        self.assertEqual(by_id['WF19-provisional-fault-boundary:call-000:capture:final:blobs']['disposition'], 'unavailable')
        # The exact expected public response passed; it could not conceal the
        # independently captured leak, even though final state would include new.
        self.assertEqual(by_id['WF19-provisional-fault-boundary:call-000:shape']['disposition'], 'pass')

    def test_wrong_receipt_remains_failure_while_later_capture_is_unavailable(self):
        state = deepcopy(profile.capture_expectations('WF19-provisional-fault-boundary')['post_fault'])
        captured = {'data': state['data'], 'persisted_strings': {jid: {
            'manifest': execution.encoded(next(job['manifest'] for job in state['data']['jobs']
                if job['public']['job_id'] == jid)).decode('ascii'), 'content_hashes': row['content_hashes'],
            'receipt': None} for jid, row in state['persisted_fields'].items()}}
        captured['data']['jobs'][0]['receipt'] = {'forged': 'committed'}
        point = review.WorkflowCapturePoint('post_fault', 0, 'after-error', 0)
        selected = observer.selected_captures(SimpleNamespace(capture_points=(point,)), [{'boundary_facts': [
            {'event': {'boundary': 'after-error', 'occurrence': 0}, 'storage': captured}]}])
        value = profile.profile_for('WF19-provisional-fault-boundary')
        projected = profile.project(value, {}, selected)
        rows = {row['case_id']: row['disposition'] for row in projected['observations']}
        self.assertEqual(rows['WF19-provisional-fault-boundary:call-000:capture:post_fault:jobs'], 'fail')
        self.assertEqual(rows['WF19-provisional-fault-boundary:call-000:capture:reopened:jobs'], 'unavailable')
