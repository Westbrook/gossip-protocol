"""Six fresh real-owner histories, gated by independently delivered layout reviews.

No review is manufactured here. GOSSIP_STORAGE_M2_REVIEW_PACKAGE is a host-owned
index path and GOSSIP_STORAGE_M2_REVIEW_SHA256 its controller-pinned exact digest.
Without both, explicit Docker qualification fails before contacting the Engine.
"""
from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
import tempfile
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import candidate_storage_product_execution_v1 as se
from gossip_harness import candidate_storage_product_observation_v1 as so
from gossip_harness import candidate_storage_product_profile_v1 as sp
from gossip_harness import candidate_storage_review_authority_v1 as sr
from gossip_harness import candidate_m2_product_execution_v1 as me
from gossip_harness import candidate_m2_product_observation_v1 as mo
from gossip_harness import candidate_m2_product_profile_v1 as mp
from gossip_harness import candidate_m2_review_authority_v1 as mr
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness.candidate_source_capture_policy_v1 import BatchCapturePolicy
from gossip_harness import candidate_storage_prestart_v1 as prestart
from tests import candidate_storage_m2_physical_v1 as preparation
from tests.test_candidate_clients_docker_v4 import make_store


def bounded(path, limit=2 * 1024 * 1024):
    if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= limit:
        raise ValueError('Review original must be bounded ordinary file: ' + str(path))
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError('Review original exceeded bound')
    return raw


def review_originals(index_path, expected_sha, control_id):
    """Pinned delivery input, never self-issued semantic approval."""
    registry.sha256(expected_sha)
    raw = bounded(index_path, 1024 * 1024)
    if se.sha(raw) != expected_sha:
        raise ValueError('Independent review index digest differs')
    package = se.process.strict_json_loads(raw)
    if set(package) != {'protocol', 'controls', 'scope'} or package['protocol'] != preparation.PROTOCOL:
        raise ValueError('Closed independent review package required')
    rows = package['controls']
    if type(rows) is not list or [row['id'] for row in rows] != [row['id'] for row in preparation.controls()]:
        raise ValueError('Exact six independent review deliveries required')
    row = next(item for item in rows if item['id'] == control_id)
    if set(row) != {'id', 'reviewer_id', 'request_sha256', 'report_sha256', 'delivery_sha256'}:
        raise ValueError('Review enrollment fields differ')
    result = {}
    for name in ('request', 'report', 'delivery'):
        value = bounded(index_path.parent / control_id / (name + '.json'))
        if se.sha(value) != row[name + '_sha256']:
            raise ValueError('Independent ' + name + ' digest differs')
        result[name] = value
    return row, result


def replace_fault_bytes(path, raw):
    with path.open('wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def enroll_originals(root, plan, row, originals):
    api = mr if plan.family == 'm2-direct-api' else sr
    if originals['request'] != api.encoded(plan.request()):
        raise ValueError('Independent request does not match actual candidate Git/source/profile/layout')
    raw_root, delta_root = root / 'review-raw', root / 'review-deltas'
    head = ExternalHead.create(root / 'review-head', journal_roots=(raw_root, delta_root))
    journal = None
    try:
        journal = chain.CheckpointChain.create(raw_root, delta_root,
            context={'protocol': preparation.PROTOCOL, 'purpose': 'retain independently delivered source-layout review'}, authority=head)
        for name in ('request', 'report', 'delivery'):
            journal.retain(name + '.json', originals[name])
        enrollment = api.ReviewEnrollment(row['reviewer_id'], 'request.json', row['request_sha256'],
            'report.json', row['report_sha256'], 'delivery.json', row['delivery_sha256'])
        kind = mr.M2ReviewAuthority if plan.family == 'm2-direct-api' else sr.StorageReviewAuthority
        authority = kind(journal, journal.commitment, enrollment)
        authority.authenticate(plan)
        return authority, journal, head
    except BaseException:
        if journal is not None:
            journal.close()
        head.close()
        raise


def original_activity(owner, phases):
    """Diagnostic lower bounds from originals; never acceptance or dispatch authority.

    Under uncertainty, individual prior facts remain explicitly prior-only.
    Their absence cannot prove no dispatch or cleanup. No command is retried.
    """
    result = {'basis': 'unavailable', 'current_prefix_verified': False, 'prefix': None,
        'intent_retained': None, 'terminal_retained': None, 'terminal_infrastructure': None, 'container_created_acknowledged': None,
        'container_start_acknowledged': None, 'session_dispatch_intent': None,
        'candidate_histories': None, 'observed_response_phases': [],
        'complete_capture_command_phases': [], 'captured_content_reparsed_by_census': False,
        'cleanup_verified': None, 'originals': {}, 'errors': [], 'acceptance_authority': False}
    if owner is None:
        result.update(basis='owner-not-constructed', candidate_histories=0,
            intent_retained=False, session_dispatch_intent=False)
        return result
    if owner.closed:
        result['errors'].append('Owner already closed; no new original authentication possible')
        return result
    healthy = True
    try:
        result['prefix'] = asdict(owner.checkpoint())
        result.update(basis='current-authenticated-originals', current_prefix_verified=True)
    except Exception as error:
        healthy = False
        result['basis'] = 'individually-authenticated-prior-facts-only'
        result['errors'].append(type(error).__name__ + ':checkpoint')
    cache = {}
    def raw(name):
        nonlocal healthy
        if name in cache:
            return cache[name]
        value = None
        basis = 'current'
        prefix = result['prefix']
        try:
            if healthy:
                if not owner.has_retained(name):
                    cache[name] = None
                    return None
                value = owner.read_authenticated(name)
            else:
                prior = owner.journal.read_prior(name)
                value, prefix, basis = prior.raw, asdict(prior.commitment), 'prior-only'
        except Exception as error:
            if healthy:
                healthy = False
                result.update(basis='individually-authenticated-prior-facts-only', current_prefix_verified=False)
                try:
                    prior = owner.journal.read_prior(name)
                    value, prefix, basis = prior.raw, asdict(prior.commitment), 'prior-only'
                except Exception:
                    pass
            result['errors'].append(type(error).__name__ + ':' + name)
        if value is not None:
            result['originals'][name] = {'sha256': se.sha(value), 'bytes': len(value), 'basis': basis, 'prefix': prefix}
        cache[name] = value
        return value
    def value(name):
        data = raw(name)
        if data is None:
            return None
        try:
            return se.process.strict_json_loads(data)
        except (ValueError, TypeError, UnicodeError) as error:
            result['errors'].append(type(error).__name__ + ':' + name + ':parse')
            return None
    def clean_command(label, argv=None):
        record = value(label + '.json')
        if type(record) is not dict:
            return None
        if argv is not None and record.get('argv') != argv:
            return None
        try:
            clean = se.b01._clean(record)
        except (KeyError, TypeError):
            return None
        if not clean:
            return None
        for kind in ('stdout', 'stderr'):
            item = record.get(kind)
            if type(item) is not dict or item.get('path') != label + '-' + kind + '.bin':
                return None
            data = raw(item['path'])
            if (data is None or item.get('sha256') != se.sha(data) or item.get('bytes') != len(data)
                or item.get('observed_bytes') != len(data) or item.get('truncated') is not False):
                return None
        return raw(label + '-stdout.bin')
    terminal = value('terminal.json')
    result['terminal_retained'] = True if type(terminal) is dict else (False if healthy and raw('terminal.json') is None else None)
    if type(terminal) is dict and type(terminal.get('infrastructure')) is list:
        result['terminal_infrastructure'] = terminal['infrastructure']
    intent = value('intent.json')
    result['intent_retained'] = True if type(intent) is dict else (False if healthy and raw('intent.json') is None else None)
    if type(intent) is dict:
        result['owned_execution'] = {key: intent[key] for key in ('execution_id', 'container', 'volume')
            if type(intent.get(key)) is str and len(intent[key]) <= 200}
    identifier = clean_command('container-create')
    if identifier is not None:
        try:
            identifier = identifier.strip().decode('ascii')
            registry.sha256(identifier)
            result['container_created_acknowledged'] = True
            result['created_container_id'] = identifier
        except (ValueError, UnicodeError):
            identifier = None
    if identifier is not None:
        started = clean_command('container-start', owner.docker + ['start', identifier])
        if started is not None and started.strip() == identifier.encode():
            result['container_start_acknowledged'] = True
    dispatched = value('session-dispatch.json')
    result['session_dispatch_intent'] = True if type(dispatched) is dict else (False if healthy and raw('session-dispatch.json') is None else None)
    for phase in phases:
        response = value(phase + '-response.json')
        if type(response) is dict and response.get('phase') == phase and 'value' in response:
            result['observed_response_phases'].append(phase)
        capture = value(phase + '-capture.json')
        # A complete command record is not independently reparsed capture content.
        try:
            if type(capture) is dict and se.b01._clean(capture):
                result['complete_capture_command_phases'].append(phase)
        except (KeyError, TypeError):
            result['errors'].append('Malformed capture command:' + phase)
    if result['observed_response_phases']:
        result['candidate_histories'] = 1
    elif healthy and result['session_dispatch_intent'] is False:
        result['candidate_histories'] = 0
    if identifier is not None and type(intent) is dict:
        name, volume = intent.get('container'), intent.get('volume')
        if type(name) is str and type(volume) is str:
            removed = clean_command('container-remove', owner.docker + ['rm', '--force', identifier])
            absent = clean_command('container-after', owner.docker + ['container', 'ls', '--all', '--quiet', '--filter', 'name=^/' + name + '$'])
            volume_removed = clean_command('volume-remove', owner.docker + ['volume', 'rm', volume])
            volume_absent = clean_command('volume-after', owner.docker + ['volume', 'ls', '--quiet', '--filter', 'name=^' + volume + '$'])
            if (removed is not None and removed.strip() == identifier.encode() and absent == b''
                and volume_removed is not None and volume_removed.strip() == volume.encode() and volume_absent == b''):
                result['cleanup_verified'] = True
    # Absence conclusions require a still-current complete prefix. Prior facts
    # may establish activity but cannot establish absence of later activity.
    try:
        if not healthy or asdict(owner.checkpoint()) != result['prefix']:
            raise chain.ChainUnknown('Current prefix unavailable at diagnostic completion')
    except Exception as error:
        result.update(current_prefix_verified=False, basis='individually-authenticated-prior-facts-only')
        result['errors'].append(type(error).__name__ + ':final-checkpoint')
        if result['candidate_histories'] == 0:
            result['candidate_histories'] = None
        if result['session_dispatch_intent'] is False:
            result['session_dispatch_intent'] = None
        if result['intent_retained'] is False:
            result['intent_retained'] = None
        if result['terminal_retained'] is False:
            result['terminal_retained'] = None
        result['cleanup_verified'] = None
    return result


def sqlite_witness(raw, expected_schema, kind):
    """Fixed source-qualified raw queries, immutable/read-only and tightly bounded."""
    import sqlite3
    if mo.sqlite_schema_sha256(raw) != expected_schema:
        raise ValueError('Witness SQLite schema differs from independently reviewed profile')
    queries = {
        'orphan': ('SELECT blob_id,content FROM blobs NOT INDEXED WHERE blob_id=?',
            ('blob-' + se.sha(b'unreferenced-after-rollback'),)),
        'receipt': ('SELECT receipt FROM jobs NOT INDEXED WHERE job_id=?', ('case',)),
        'notes': ('SELECT notes FROM document_state NOT INDEXED', ()),
    }
    statement, parameters = queries[kind]
    with tempfile.TemporaryDirectory(prefix='storage-m2-raw-witness-') as directory:
        path = Path(directory) / 'capture.sqlite'; path.write_bytes(raw)
        connection = sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)
        try:
            connection.enable_load_extension(False)
            connection.execute('PRAGMA trusted_schema=OFF')
            connection.execute('PRAGMA query_only=ON')
            connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 2 * 1024 * 1024)
            connection.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 4096)
            connection.setlimit(sqlite3.SQLITE_LIMIT_COLUMN, 128)
            ticks = 0
            def progress():
                nonlocal ticks
                ticks += 1000
                return int(ticks > 200000)
            connection.set_progress_handler(progress, 1000)
            allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ}
            connection.set_authorizer(lambda action, *_: sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY)
            return connection.execute(statement, parameters).fetchmany(2)
        finally:
            connection.close()


class CandidateStorageM2QualificationPreparationV1Tests(unittest.TestCase):
    def test_mutants_are_single_file_inert_changes_with_independent_declared_failures(self):
        baseline, _ = preparation.candidate_files('reference')
        for row in preparation.controls():
            files, provenance = preparation.candidate_files(row['variant'])
            changed = {name for name in files if files[name] != baseline[name]}
            self.assertEqual(changed, set() if row['variant'] == 'reference' else {preparation.SOURCE_PATH})
            self.assertEqual(bool(row['expected_failed']), row['variant'] != 'reference')
            self.assertEqual(provenance['scientific_samples'], 0)

    def test_schema_is_bound_to_authored_literals_and_unknown_source_is_rejected(self):
        files, _ = preparation.candidate_files('reference')
        raw, profile = preparation.schema_fixture(files)
        self.assertTrue(raw.startswith(b'SQLite format 3\0'))
        self.assertEqual(len(profile['statements']), 18)
        changed = dict(files); changed['library/catalog/m4_store.py'] += b'\n'
        with self.assertRaisesRegex(ValueError, 'schema source differs'):
            preparation.schema_fixture(changed)

    def test_m2_schema_preserves_actual_seed_tables_instead_of_fresh_store_spelling(self):
        files, _ = preparation.candidate_files('reference')
        fresh, _ = preparation.schema_fixture(files)
        migrated, profile = preparation.schema_fixture(files, m2_case='m2-competing-connections')
        self.assertNotEqual(mo.sqlite_schema_sha256(fresh), mo.sqlite_schema_sha256(migrated))
        self.assertEqual(len(profile['statements']), 14)
        self.assertEqual([row[0] for row in profile['preserved_seed_tables']], ['blobs', 'documents', 'jobs', 'metadata'])
        self.assertIn('Schema construction only', profile['scope'])
        with self.assertRaisesRegex(ValueError, 'Only the reviewed'):
            preparation.schema_fixture(files, m2_case='m2-query-generation-pages')

    def test_missing_or_wrong_pinned_review_cannot_become_enrollment(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaises(ValueError):
                review_originals(root / 'missing.json', 'a' * 64, 'P01')
            preparation.write_raw(root / 'index.json', b'{}')
            with self.assertRaisesRegex(ValueError, 'digest differs'):
                review_originals(root / 'index.json', 'a' * 64, 'P01')
            with self.assertRaisesRegex(ValueError, 'Closed'):
                review_originals(root / 'index.json', se.sha(b'{}'), 'P01')


class CandidateStorageM2QualificationCensusV1Tests(unittest.TestCase):
    """Synthetic diagnostic inputs only; these never become physical receipts."""
    def fake_owner(self, records, *, uncertain=False):
        from types import SimpleNamespace
        prefix = chain.PrefixCommitment('a' * 64, 0, 'b' * 64, 0, 0, 0)
        def checkpoint():
            if uncertain:
                raise chain.ChainUnknown('synthetic uncertainty')
            return prefix
        def prior(name):
            if name not in records:
                raise chain.ChainError('synthetic absent prior')
            return chain.PriorFact(name, records[name], se.sha(records[name]), prefix)
        return SimpleNamespace(closed=False, docker=['docker'], checkpoint=checkpoint,
            has_retained=lambda name: name in records, read_authenticated=lambda name: records[name],
            journal=SimpleNamespace(read_prior=prior))

    def test_current_absence_is_zero_and_dispatch_without_response_is_unknown(self):
        empty = original_activity(self.fake_owner({}), ('before', 'after', 'reopened'))
        self.assertEqual(empty['candidate_histories'], 0)
        self.assertIs(empty['session_dispatch_intent'], False)
        dispatched = original_activity(self.fake_owner({'session-dispatch.json': se.encoded({'argv': ['docker', 'exec']})}), ('before',))
        self.assertIs(dispatched['session_dispatch_intent'], True)
        self.assertIsNone(dispatched['candidate_histories'])
        self.assertIsNone(dispatched['cleanup_verified'])

    def test_partial_response_survives_a_later_failure_as_observed_activity(self):
        records = {'session-dispatch.json': se.encoded({'argv': ['docker', 'exec']}),
                   'before-response.json': se.encoded({'phase': 'before', 'value': None})}
        result = original_activity(self.fake_owner(records), ('before', 'after', 'reopened'))
        self.assertEqual(result['candidate_histories'], 1)
        self.assertEqual(result['observed_response_phases'], ['before'])
        self.assertIs(result['current_prefix_verified'], True)
        self.assertFalse(result['acceptance_authority'])

    def test_uncertain_prior_fact_never_establishes_absence_or_current_cleanup(self):
        result = original_activity(self.fake_owner({}, uncertain=True), ('before',))
        self.assertIsNone(result['candidate_histories'])
        self.assertIsNone(result['session_dispatch_intent'])
        self.assertIsNone(result['cleanup_verified'])
        prior = original_activity(self.fake_owner({'before-response.json': se.encoded({'phase': 'before', 'value': None})}, uncertain=True), ('before',))
        self.assertEqual(prior['candidate_histories'], 1)
        self.assertIs(prior['current_prefix_verified'], False)
        self.assertEqual(prior['originals']['before-response.json']['basis'], 'prior-only')

    def test_malformed_retained_command_is_unavailable_in_diagnostic_census(self):
        result = original_activity(self.fake_owner({'container-create.json': b'{}', 'before-capture.json': b'{}'}), ('before',))
        self.assertIsNone(result['container_created_acknowledged'])
        self.assertEqual(result['complete_capture_command_phases'], [])
        malformed = original_activity(self.fake_owner({'session-dispatch.json': b'null'}), ('before',))
        self.assertIsNone(malformed['session_dispatch_intent'])
        self.assertIsNone(malformed['candidate_histories'])

    def test_teardown_census_cannot_leave_a_terminal_class_running(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as directory:
            cls = SimpleNamespace(outcome={'status': 'running', 'qualified': False},
                artifacts=SimpleNamespace(root=Path(directory)))
            _PhysicalStorageM2Control.save_census.__func__(cls)
            retained = json.loads((Path(directory) / 'completion-census.json').read_bytes())
            self.assertEqual(retained['status'], 'unavailable')
            self.assertFalse(retained['qualified'])

    def test_failed_negative_admission_retains_its_unexpected_observed_history(self):
        from types import SimpleNamespace
        records = {}
        owner = self.fake_owner(records)
        owner.observation_registration = owner.checkpoint()
        owner._cleanup = None
        def unexpected_dispatch():
            records['intent.json'] = se.encoded({'execution_id': 'unexpected-negative'})
            records['session-dispatch.json'] = se.encoded({'argv': ['docker', 'exec']})
            records['before-response.json'] = se.encoded({'phase': 'before', 'value': None})
            return {}
        owner.execute_once = unexpected_dispatch
        class Control(_PhysicalStorageM2Control, unittest.TestCase):
            pass
        with tempfile.TemporaryDirectory() as directory:
            control = Control()
            control.artifacts = SimpleNamespace(root=Path(directory))
            control.outcome = {'attempted_histories': 1}
            control.make_owner = lambda *_: (owner, None, {'revoked': False}, None)
            with self.assertRaises(AssertionError):
                control.negative_revocation()
            retained = json.loads((Path(directory) / 'negative-revocation.json').read_bytes())
            self.assertEqual(retained['status'], 'failed')
            self.assertEqual(retained['candidate_histories'], 1)
            self.assertEqual(retained['activity']['observed_response_phases'], ['before'])
            self.assertEqual(control.outcome['attempted_histories'], 2)
            self.assertEqual(control.outcome['negative_controls']['N01'], retained)


class _PhysicalStorageM2Control:
    CONTROL_ID = ''

    @classmethod
    def setUpClass(cls):
        cls.definition = next(row for row in preparation.controls() if row['id'] == cls.CONTROL_ID)
        # Required external package is checked before even a read-only Engine call.
        index = os.environ.get('GOSSIP_STORAGE_M2_REVIEW_PACKAGE')
        pin = os.environ.get('GOSSIP_STORAGE_M2_REVIEW_SHA256')
        if not index or not pin:
            raise ValueError('Independent source-specific review package and controller digest are required before physical qualification')
        cls.review_row, cls.review_raw = review_originals(Path(index).resolve(), pin, cls.CONTROL_ID)
        cls.artifacts = ArtifactDirectory('storage-m2-physical-v1-' + cls.CONTROL_ID.lower(), retain_success=True)
        cls.addClassCleanup(cls.artifacts.close)
        cls.outcome = {'id': cls.CONTROL_ID, 'status': 'not-run', 'qualified': False,
            'candidate_histories': 0, 'attempted_histories': 0, 'scientific_samples': 0, 'whole_project_acceptance': False}
        cls.addClassCleanup(cls.save_census)
        preparation.write_new(cls.artifacts.root / 'planned-control.json', cls.definition)
        try:
            cls.endpoint = se.process.EngineEndpoint.from_environment()
            cls.runtime = se.process.runtime_identity(cls.endpoint, se.b01.RUNTIME_IMAGE,
                retain=lambda name, raw: preparation.write_raw(cls.artifacts.root / name, raw), label='class-runtime')
        except BaseException as error:
            cls.outcome.update(status='unavailable', failure_stage='prerequisite', failure_type=type(error).__name__)
            raise

    @classmethod
    def save_census(cls):
        if cls.outcome['status'] in ('running', 'not-run'):
            cls.outcome.update(status='unavailable', qualified=False,
                terminal_reason='Class finished without a terminal qualification result')
        preparation.write_new(cls.artifacts.root / 'completion-census.json', cls.outcome)

    def make_owner(self, directory, repetition):
        directory.mkdir()
        row = self.definition
        self.is_m2 = row['family'] == 'm2-direct-api'
        self.api, self.obs = (me, mo) if self.is_m2 else (se, so)
        files, provenance = preparation.candidate_files(row['variant'])
        preparation.write_new(directory / 'source-provenance.json', provenance)
        store = make_store(directory / 'candidate.git', files)
        commit = store.head()
        tree, actual = se.capture_source(store, commit, policy=BatchCapturePolicy())
        self.assertEqual(actual, files)
        schema, schema_record = preparation.schema_fixture(files, m2_case=row['case_id'] if self.is_m2 else None)
        preparation.write_raw(directory / 'schema.sqlite', schema)
        preparation.write_new(directory / 'schema-profile.json', schema_record)
        value = mp.profile_for(row['case_id'], preparation.PURPOSE) if self.is_m2 else sp.profile_for(row['family'], row['case_id'], preparation.PURPOSE)
        plan = preparation.plan_for(row, files, commit, tree, schema)
        authority, journal, review_head = enroll_originals(directory, plan, self.review_row, self.review_raw)
        self.addCleanup(review_head.close); self.addCleanup(journal.close)
        policy = me.M2Policy() if self.is_m2 else se.StoragePolicy()
        kwargs = {} if self.is_m2 else {'capture_policy': BatchCapturePolicy()}
        binding = self.api.binding_for(files, value, policy, self.runtime, plan, review_authority=authority, **kwargs)
        subject = registry.Subject('cohort', preparation.COHORT[0], 'M4',
            se.digest({'protocol': preparation.PROTOCOL, 'control': row}), self.api.TARGET_CONTRACT, binding.source_sha256)
        gate = self.api.gate_for(subject, binding, gate_id='qualification-' + row['id'].lower())
        kind = me.M2Registration if self.is_m2 else se.StorageRegistration
        registration = kind(binding, commit, tree, repetition, gate, preparation.COHORT)
        registered = self.api.observation_registration(registration)
        registration_path = directory / 'qualification-registration.json'
        original_registration = preparation.write_new(registration_path, asdict(registered))
        state = {'revoked': False}
        def current_registration():
            if state['revoked'] or bounded(registration_path) != original_registration:
                return None
            return registered
        admitted = admission.ObservationAdmission(registered, verify_registration=current_registration, verify_cohort=lambda: None)
        raw, delta = directory / 'raw', directory / 'delta'
        head = ExternalHead.create(directory / 'head', journal_roots=(raw, delta))
        self.addCleanup(head.close)
        owner_kind = me.CandidateM2Execution if self.is_m2 else se.CandidateStorageExecution
        def open_owner(expected=None):
            owner = owner_kind(raw, store, registration, policy, value=value, plan=plan, review_authority=authority,
                admission_authority=admitted, checkpoint_authority=head, delta_root=delta,
                cleanup_root=directory / 'cleanup', endpoint=self.endpoint, mode='physical', expected_checkpoint=expected)
            self.addCleanup(owner.close)
            return owner
        return open_owner(), open_owner, state, gate

    def assert_originals(self, owner, record):
        self.assertEqual(record['mechanics'], {'status': 'passed', 'cleanup_verified': True, 'session_complete': True, 'prestart_verified': True, 'unavailable': []})
        intent = se.process.strict_json_loads(owner.read_authenticated('intent.json'))
        staging = se.process.strict_json_loads(owner.read_authenticated('staging.json'))
        binds = {'/workspace': staging['workspace'], '/checks': staging['checks']}
        if owner.binding.family != 'b01':
            binds['/inputs'] = staging['inputs']
        if self.is_m2:
            fixture_digest = owner.binding.fixture_sha256
        else:
            recipe = None if owner.binding.family == 'b01' else se.b02.validate_recipe(se.b02.cases.execution_recipe(owner.binding.case_id))
            native = se.b01 if recipe is None else se.b02
            fixture_digest = se.digest({'recipe': recipe, 'adapter': se.sha(native.CHILD_ADAPTER.encode())})
        labels = {'gossip.execution': intent['execution_id'], 'gossip.source': owner.binding.source_sha256,
                  'gossip.fixture': fixture_digest}
        identifier = owner.read_blob('container-create-stdout.bin').strip().decode('ascii')
        proof = prestart.proof_for(owner.read_blob('container-prestart-stdout.bin'), container_id=identifier,
            name=intent['container'], image_id=owner.policy.image_id, volume=intent['volume'], labels=labels, mounts=binds)
        self.assertEqual(se.encoded(proof), owner.read_authenticated(prestart.PROOF_FILE))
        prestart.validate_order({name: owner.authenticated_position(name) for name in prestart.ORDER})
        phases = owner.profile.phases if self.is_m2 else se.b01.PHASES
        self.assertEqual([fact['phase'] for fact in record['phase_facts']], list(phases))
        expected_paths = set(owner.plan.storage_paths)
        for phase, fact in zip(phases, record['phase_facts']):
            self.assertIs(fact['capture_authenticated'], True)
            self.assertIs(fact['response_authenticated'], True)
            self.assertEqual(fact['mapping'], 'available')
            captured = se.b02.parse_capture(owner.read_blob(phase + '-capture-stdout.bin'))
            self.assertEqual(set(captured), expected_paths)
            sqlite_path = 'm2/library.sqlite' if self.is_m2 else 'catalog.sqlite'
            schema = mo.sqlite_schema_sha256(captured[sqlite_path]) if self.is_m2 else so.b01_observer.sqlite_schema_sha256(captured[sqlite_path])
            self.assertEqual(schema, owner.plan.schema_sha256)
            self.assertTrue(all(raw == b'' for path, raw in captured.items() if path != sqlite_path))
            state = se.process.strict_json_loads(owner.read_blob(phase + '-state-stdout.bin'))
            host = state['HostConfig']
            self.assertEqual(host['NetworkMode'], 'none')
            self.assertIs(host['ReadonlyRootfs'], True)
            self.assertEqual(host['CapDrop'], ['ALL'])
            self.assertEqual(state['Config']['User'], '0:0')
        session = se.process.strict_json_loads(owner.read_authenticated('session-dispatch.json'))
        argv = session['argv']
        self.assertIn('65534:65534', argv)
        self.assertIn('-I', argv)
        self.assertIn('-B', argv)
        self.assertIsNone(owner.cleanup_result)
        self.assertEqual(owner.checkpoint(), owner.journal.checkpoint())
        return len(phases)

    def assert_mutant_witness(self, owner):
        if self.definition['variant'] == 'reference':
            return
        witness = {'control_id': self.CONTROL_ID, 'qualified': False,
            'purpose': 'exact authored fault discrimination only; no new public obligation',
            'originals': {}, 'observed': {}}
        def response(phase):
            name = phase + '-response.json'
            raw = owner.read_authenticated(name)
            witness['originals'][name] = {'sha256': se.sha(raw), 'bytes': len(raw)}
            return se.process.strict_json_loads(raw)['value']
        def database(phase):
            name = phase + '-capture-stdout.bin'
            raw = owner.read_blob(name)
            path = 'm2/library.sqlite' if self.is_m2 else 'catalog.sqlite'
            data = se.b02.parse_capture(raw)[path]
            witness['originals'][name] = {'sha256': se.sha(raw), 'bytes': len(raw),
                'sqlite_path': path, 'sqlite_sha256': se.sha(data), 'sqlite_bytes': len(data)}
            return data
        try:
            if self.CONTROL_ID == 'D01':
                expected = [('blob-' + se.sha(b'unreferenced-after-rollback'), b'unreferenced-after-rollback')]
                observed = {}
                for phase in se.b01.PHASES:
                    rows = sqlite_witness(database(phase), owner.plan.schema_sha256, 'orphan')
                    observed[phase] = {'row_count': len(rows), 'exact_declared_orphan': rows == expected}
                    self.assertEqual(rows, [] if phase == 'before' else expected)
                after, reopened = response('after'), response('reopened')
                witness['observed'] = {'orphan': observed, 'after_response': after,
                    'reopened_response': reopened, 'reopen_succeeded': False}
                self.assertEqual(after, {'error': 'injected_failure'})
                self.assertEqual(reopened, {'error': 'invalid_database'})
            elif self.CONTROL_ID == 'D02':
                values = response('after')
                first = values[0]['value']['documents'][0]['text']
                replay = values[2]['value']['documents'][0]['text']
                persisted = {}
                for phase in ('after', 'reopened'):
                    rows = sqlite_witness(database(phase), owner.plan.schema_sha256, 'receipt')
                    self.assertEqual(len(rows), 1)
                    self.assertIs(type(rows[0][0]), str)
                    receipt = se.process.strict_json_loads(rows[0][0].encode('utf-8'))
                    persisted[phase] = receipt['documents'][0]['text']
                witness['observed'] = {'first_return_text': first, 'replayed_return_text': replay,
                    'persisted_receipt_text': persisted}
                self.assertEqual(first, 'same')
                self.assertEqual(replay, 'MUTATED')
                self.assertEqual(persisted, {'after': 'same', 'reopened': 'same'})
            elif self.CONTROL_ID == 'D03':
                listing, show = response('action-001'), response('action-003')
                listed = listing['result']['records'][0]['notes']
                returned = show['result']['notes']
                rows = sqlite_witness(database('action-003'), owner.plan.schema_sha256, 'notes')
                witness['observed'] = {'listed_notes': listed, 'returned_notes': returned,
                    'persisted_notes': [row[0] for row in rows]}
                self.assertEqual(listed, 'winner')
                self.assertEqual(returned, 'qualification-corrupted-response')
                self.assertEqual(rows, [('winner',)])
            else:
                raise AssertionError('Closed declared mutant required')
            witness['qualified'] = True
        finally:
            preparation.write_new(self.artifacts.root / 'raw-mutant-witness.json', witness)

    def negative_revocation(self):
        owner = None
        record = {'id': 'N01', 'status': 'running', 'candidate_histories': None,
            'fresh_attempt': True, 'meaning': 'revoked authentic controller registration refused before durable intent and cleanup claims'}
        self.outcome['attempted_histories'] += 1
        self.outcome.setdefault('negative_controls', {})['N01'] = record
        try:
            owner, _, state, _ = self.make_owner(self.artifacts.root / 'N01', 'negative-revoked-before-intent')
            prior = owner.checkpoint()
            record.update(registration=asdict(owner.observation_registration), prefix=asdict(prior))
            state['revoked'] = True
            with self.assertRaises(admission.AdmissionError):
                owner.execute_once()
            self.assertFalse(owner.has_retained('intent.json'))
            self.assertEqual(owner.checkpoint(), prior)
            self.assertIsNone(owner._cleanup)
            record['status'] = 'passed'
        except BaseException as error:
            record.update(status='failed' if isinstance(error, AssertionError) else 'unavailable',
                failure_type=type(error).__name__, failure_detail=str(error)[:1000])
            raise
        finally:
            try:
                record['activity'] = original_activity(owner, se.b01.PHASES)
                record['candidate_histories'] = record['activity']['candidate_histories']
                if record['status'] == 'passed' and (record['candidate_histories'] != 0
                    or not record['activity']['current_prefix_verified']):
                    raise ValueError('Negative admission original prefix unavailable or contains candidate activity')
            except Exception as error:
                record['census_error'] = type(error).__name__ + ':' + str(error)[:500]
                record['candidate_histories'] = None
                if record['status'] == 'passed':
                    record['status'] = 'unavailable'
                    raise
            finally:
                preparation.write_new(self.artifacts.root / 'negative-revocation.json', record)

    def negative_original_corruption(self, owner, reopen, checkpoint, original_observation):
        # Reuse only this already closed physical history for parser fault injection.
        # This does not execute a second candidate or supply independent acceptance.
        target = owner.root / 'after-response.json'
        original = owner.read_authenticated(target.name)
        evidence = self.artifacts.root / 'N02'; evidence.mkdir()
        preparation.write_raw(evidence / 'original.bin', original)
        changed = original + b' '
        preparation.write_raw(evidence / 'tampered.bin', changed)
        try:
            replace_fault_bytes(target, changed)
            with self.assertRaises(chain.ChainError):
                owner.checkpoint()
        finally:
            # Restore original evidence bytes before closing the poisoned reader;
            # retained attack originals disclose the deliberate temporary corruption.
            replace_fault_bytes(target, original)
            owner.close()
        restored = reopen(checkpoint)
        source = so.StorageObservationSource(restored, checkpoint)
        reread = source.observation(restored.registration.gate, None)
        self.assertEqual(reread, original_observation)
        preparation.write_new(evidence / 'result.json', {'id': 'N02', 'status': 'passed',
            'candidate_histories': 0, 'basis': 'parser integrity reread of P01 original; not a new acceptance or repeatability run',
            'original_execution_id': original_observation.execution.execution_id,
            'original_receipt_sha256': original_observation.execution.receipt_sha256,
            'prefix': asdict(checkpoint), 'original_sha256': se.sha(original), 'tampered_sha256': se.sha(changed),
            'restored_exact_original': True, 'poisoned_reader_reused': False})
        return restored

    def qualify(self):
        self.outcome.update(status='running', attempted_histories=1)
        owner, terminal = None, None
        phases = mp.phases_for(self.definition['case_id']) if self.definition['family'] == 'm2-direct-api' else se.b01.PHASES
        try:
            owner, reopen, _, gate = self.make_owner(self.artifacts.root / 'execution', 'fresh-' + self.CONTROL_ID.lower())
            terminal = owner.execute_once()
            self.assertEqual(terminal['infrastructure'], [])
            checkpoint = self.obs.publish_verifier(owner)
            source_kind = mo.M2ObservationSource if self.is_m2 else so.StorageObservationSource
            observation = source_kind(owner, checkpoint).observation(gate, None)
            record = se.process.strict_json_loads(owner.read_authenticated(self.obs.VERIFIER_FILE))
            self.assertEqual(observation.execution.terminal_status, 'completed')
            self.assertEqual([row.case_id for row in observation.execution.outcomes if row.status == 'failed'], self.definition['expected_failed'])
            self.assertTrue(all(row.status in ('passed', 'failed') for row in observation.execution.outcomes))
            self.assertEqual(tuple(row.case_id for row in observation.execution.outcomes), gate.ordered_case_ids)
            count = self.assert_originals(owner, record)
            self.assert_mutant_witness(owner)
            preparation.write_new(self.artifacts.root / 'physical-observation.json', asdict(observation))
            preparation.write_new(self.artifacts.root / 'original-prefix.json', asdict(checkpoint))
            self.outcome['activity'] = original_activity(owner, phases)
            self.outcome['candidate_histories'] = self.outcome['activity']['candidate_histories']
            if self.CONTROL_ID == 'P01':
                owner = self.negative_original_corruption(owner, reopen, checkpoint, observation)
                self.negative_revocation()
            self.outcome.update(status='qualified', qualified=True, authenticated_phases=count,
                execution_id=observation.execution.execution_id, expected_failed=self.definition['expected_failed'],
                original_receipt_sha256=observation.execution.receipt_sha256,
                layout_review_sha256=self.review_row['report_sha256'])

        except BaseException as error:
            infrastructure = type(terminal) is dict and bool(terminal.get('infrastructure'))
            self.outcome.update(status='failed' if isinstance(error, AssertionError) and not infrastructure else 'unavailable',
                qualified=False, failure_type=type(error).__name__, failure_detail=str(error)[:1000])
            raise
        finally:
            # An already authenticated census survives deliberate N02 reader
            # poisoning; a closed reader cannot generate new authority.
            try:
                if owner is not None and not owner.closed:
                    self.outcome['activity'] = original_activity(owner, phases)
                elif 'activity' not in self.outcome:
                    self.outcome['activity'] = original_activity(owner, phases)
                primary = self.outcome['activity']['candidate_histories']
                self.outcome['primary_candidate_histories'] = primary
                counts = [primary] + [item['candidate_histories']
                    for item in self.outcome.get('negative_controls', {}).values()]
                self.outcome['candidate_histories'] = None if any(count is None for count in counts) else sum(counts)
                if self.outcome['qualified'] and not self.outcome['activity']['current_prefix_verified']:
                    raise ValueError('Final diagnostic original prefix unavailable')
            except Exception as census_error:
                self.outcome['census_error'] = type(census_error).__name__ + ':' + str(census_error)[:500]
                self.outcome['candidate_histories'] = None
                if self.outcome['status'] == 'qualified':
                    self.outcome.update(status='unavailable', qualified=False)
                    raise


@unittest.skipUnless(os.environ.get('GOSSIP_RUN_DOCKER_TESTS') == '1', 'explicit real Docker lane')
class CandidateStorageM2PhysicalP01Tests(_PhysicalStorageM2Control, unittest.TestCase):
    CONTROL_ID = 'P01'
    def test_b01_rollback_and_admission_original_negatives(self):
        self.qualify()


@unittest.skipUnless(os.environ.get('GOSSIP_RUN_DOCKER_TESTS') == '1', 'explicit real Docker lane')
class CandidateStorageM2PhysicalD01Tests(_PhysicalStorageM2Control, unittest.TestCase):
    CONTROL_ID = 'D01'
    def test_b01_orphan_content_is_detected_from_physical_storage(self):
        self.qualify()


@unittest.skipUnless(os.environ.get('GOSSIP_RUN_DOCKER_TESTS') == '1', 'explicit real Docker lane')
class CandidateStorageM2PhysicalP02Tests(_PhysicalStorageM2Control, unittest.TestCase):
    CONTROL_ID = 'P02'
    def test_b02_commit_result_freshness(self):
        self.qualify()


@unittest.skipUnless(os.environ.get('GOSSIP_RUN_DOCKER_TESTS') == '1', 'explicit real Docker lane')
class CandidateStorageM2PhysicalD02Tests(_PhysicalStorageM2Control, unittest.TestCase):
    CONTROL_ID = 'D02'
    def test_b02_cached_return_alias_is_detected(self):
        self.qualify()


@unittest.skipUnless(os.environ.get('GOSSIP_RUN_DOCKER_TESTS') == '1', 'explicit real Docker lane')
class CandidateStorageM2PhysicalP03Tests(_PhysicalStorageM2Control, unittest.TestCase):
    CONTROL_ID = 'P03'
    def test_m2_ordinary_two_handle_history(self):
        self.qualify()


@unittest.skipUnless(os.environ.get('GOSSIP_RUN_DOCKER_TESTS') == '1', 'explicit real Docker lane')
class CandidateStorageM2PhysicalD03Tests(_PhysicalStorageM2Control, unittest.TestCase):
    CONTROL_ID = 'D03'
    def test_m2_lifecycle_response_defect_is_detected(self):
        self.qualify()
