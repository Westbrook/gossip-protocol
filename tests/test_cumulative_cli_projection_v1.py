"""Prospective addendum mechanics only; no candidate execution or semantic approval."""
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness import cumulative_cli_projection_v1 as projection
from gossip_harness import cumulative_observation_profile_v1 as legacy
from gossip_harness import candidate_cli_acceptance_profile_v1 as cases
from gossip_harness import candidate_client_execution_v5 as execution
from gossip_harness import candidate_client_observer_v5 as observer
from gossip_harness import candidate_client_observation_source_v1 as bridge
from gossip_harness import candidate_observation_admission_v1 as admission
from gossip_harness import project_acceptance_registry_v1 as registry
from gossip_harness import cumulative_final_acceptance_v1 as shared
from gossip_harness import cumulative_final_acceptance_v3 as final
from gossip_harness import cumulative_rehearsal_codec_v1 as codec
from gossip_harness import cumulative_scope_source_v3 as scope_source
from gossip_harness import cumulative_scope_authority_v3 as scope_authority
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import candidate_checkpoint_chain_v1 as checkpoint
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.cumulative_study_controller_v2 import StudyController, digest
from gossip_harness.peer_role_loop_v2 import materialize
from gossip_harness.peer_project_contract_v2 import canonical_bytes
from gossip_harness.gitstore import GitStore
from tests.test_candidate_client_observation_source_v1 import synthetic
from tests.test_candidate_client_cumulative_v1 import m4_observation
from tests.test_cumulative_terminal_originals_v2 import synthetic_plan
from tests.test_peer_role_loop_v2 import FakeMesh

ROOT = Path(__file__).resolve().parents[1]
COHORT = tuple('clarified-trajectory-' + str(index) for index in range(6))


def observation(value, index=0, **kwargs):
    old = synthetic(value.case_id, index, **kwargs)
    binding = json.loads(old.binding_json)
    binding.update(protocol=projection.EXECUTION_PROTOCOL, milestone='M4', purpose=value.purpose,
        requirements_sha256=legacy.TARGET_CONTRACT_SHA256, profile_sha256=value.sha256,
        target_definition_sha256=execution.digest(value.target_definition()))
    return observer.process_observation(binding, json.loads(old.transport_json), old.stdout, old.stderr)


def rebalance(plan, **changes):
    """Rehash a changed declaration so negatives reach exposure checks, not old hashes."""
    releases = changes.get('releases', plan.releases)
    runtime = changes.get('runtime', plan.runtime)
    resources = {name:getattr(plan, name) for name in
        ('horizon_seconds', 'source_generations', 'partition_seconds', 'executor_slots')}
    cohort = replace(plan.cohort, requirements_release_sha256=digest([asdict(row) for row in releases]),
        resource_contract_sha256=digest({**resources, 'runtime':runtime}))
    return replace(plan, cohort=cohort, **changes)


def input_registration(value, files=None):
    files = files or {'library/__init__.py':b'', 'library/__main__.py':b'',
                     projection.RELEASE_PATH:projection.text().encode()}
    policy = execution.ClientPolicy('sha256:' + 'a' * 64)
    binding = execution.binding_for(files, value.case_id, policy, {'kind':'fixture-no-Docker'},
        requirements_sha256=legacy.TARGET_CONTRACT_SHA256, milestone='M4',
        purpose=value.purpose, cumulative_profile=value)
    subject = registry.Subject('clarified-offline', COHORT[0], 'M4', 'e'*64,
        legacy.TARGET_CONTRACT_SHA256, binding.source_sha256)
    gate = execution.gate_for(subject, binding, gate_id='clarified-cli')
    return execution.ClientRegistration(binding, 'b'*40, 'c'*40, 'fresh-1', gate, COHORT)


class CumulativeCliProjectionProfileV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.definitions = cases.definitions()
        cls.profiles = tuple(projection.profile_for(row['case_id'], purpose='public_release')
                             for row in cls.definitions)

    def test_exact_approved_text_and_unchanged_base_identity(self):
        self.assertEqual(hashlib.sha256(projection.text().encode()).hexdigest(), projection.TEXT_SHA256)
        self.assertEqual(hashlib.sha256((ROOT / projection.BASE_PATH).read_bytes()).hexdigest(), projection.BASE_SHA256)
        self.assertFalse(projection.manifest()['historical_application'])
        self.assertEqual(projection.manifest()['unchanged_unspecified_commands'], ['import', 'jobs'])

    def test_all_57_histories_and_900_diagnostics_retain_original_expectations(self):
        totals = [0, 0, 0, 0]
        commands = set()
        for original, value in zip(self.definitions, self.profiles, strict=True):
            with self.subTest(case_id=value.case_id):
                before = legacy.cli_profile(value.case_id, purpose=value.purpose)
                self.assertEqual(value.target_definition()['definition'], before.target_definition()['definition'])
                self.assertEqual(value.diagnostic_cells, before.diagnostic_cells)
                self.assertNotEqual(value.sha256, before.sha256)
                mapped = bridge._mapped_cells(value.case_id, (), cumulative_profile=value)
                projected = bridge._projected_outcomes(mapped, value)
                totals[0] += len(original['recipe']['steps'])
                totals[1] += len(mapped)
                totals[2] += len(projected)
                totals[3] += sum(row['status'] == 'skipped' for row in mapped)
                self.assertTrue(all(row.status == 'infrastructure_error' for row in projected))
                for row in value.record()['projection_applicability']:
                    commands.add(row['command'])
                    self.assertEqual(row['unchanged_expectation'], original['expectations'][row['step_id']])
                    self.assertEqual(row['source_sha256'], projection.TEXT_SHA256)
        self.assertEqual((len(self.profiles), *totals), (57, 305, 900, 868, 32))
        self.assertEqual(commands, set(projection.COMMANDS))

    def test_six_success_commands_keep_exact_fields_epochs_receipts_and_noncanonical_json(self):
        found = set()
        for value in self.profiles:
            definition = value.record()['original_definition']
            for index, step in enumerate(definition['recipe']['steps']):
                args = step['argv'][7:]
                if not args or args[0] not in projection.COMMANDS or args[0] in found:
                    continue
                expected = definition['expectations'][step['step_id']]
                if expected['kind'] != 'success':
                    continue
                found.add(args[0])
                answer = expected['value']
                # Comparator controls use synthetic process frames, never physical observations.
                raw = json.dumps(dict(reversed(list(answer.items()))), indent=3).encode() + b'\n'
                graded = observer.observe_cli_step(expected, observation(value, index, stdout=raw))
                self.assertTrue(graded['assertions']['stdout.value'])
                mutations = [{'result':answer}, {**answer, 'unexpected':True}]
                if 'epoch' in answer:
                    mutations.append({**answer, 'epoch':answer['epoch'] + 1})
                if 'job' in answer:
                    mutations.append({**answer, 'job':{**answer['job'], 'epoch':answer['job']['epoch'] + 1}})
                    mutations.append({**answer, 'documents':[]})
                for changed in mutations:
                    with self.subTest(command=args[0], changed=changed):
                        result = observer.observe_cli_step(expected,
                            observation(value, index, stdout=json.dumps(changed).encode()))
                        self.assertFalse(result['assertions']['stdout.value'])
        self.assertEqual(found, set(projection.COMMANDS))

    def test_literal_import_and_jobs_wrappers_remain_explicitly_unspecified(self):
        found = set()
        for value in self.profiles:
            original = value.record()['original_definition']
            for index, step in enumerate(original['recipe']['steps']):
                command = step['argv'][7:]
                expected = original['expectations'][step['step_id']]
                if not command or command[0] not in ('import','jobs') or expected['kind'] != 'success':
                    continue
                found.add(command[0])
                self.assertIn('stdout.wrapper',expected['unspecified_assertion_ids'])
                actual = observer.observe_cli_step(expected,observation(value,index,stdout=b'{"arbitrary-wrapper":[]}'))
                self.assertIsNone(actual['assertions']['stdout.wrapper'])
                self.assertTrue(actual['assertions']['stdout.json'])
                failed = observer.observe_cli_step(expected,observation(value,index,stdout=b'{}',exit_code=2))
                self.assertFalse(failed['assertions']['process.exit'])
        self.assertEqual(found,{'import','jobs'})

    def test_known_failure_unknown_tail_and_unspecified_cells_are_preserved(self):
        value = projection.profile_for('cli-empty', purpose='repeatability')
        raw = observation(value, exit_code=2, unavailable_stdout=True)
        mapped = bridge._mapped_cells(value.case_id, (raw,), cumulative_profile=value)
        actual = bridge._projected_outcomes(mapped, value)
        self.assertEqual(actual[0].status, 'failed')
        self.assertTrue(all(row.status == 'infrastructure_error' for row in actual[1:]))
        value = projection.profile_for('cli-grammar-unknown-command', purpose='public_release')
        mapped = bridge._mapped_cells(value.case_id, (observation(value, exit_code=2),), cumulative_profile=value)
        self.assertEqual([row['status'] for row in mapped], ['passed', 'skipped'])
        self.assertEqual(len(bridge._projected_outcomes(mapped, value)), 1)

    def test_old_receipt_new_profile_and_purpose_target_substitution_are_rejected(self):
        value = self.profiles[0]
        old = legacy.cli_profile(value.case_id, purpose=value.purpose)
        for raw in (synthetic(value.case_id, 0), m4_observation(old)):
            with self.assertRaises(bridge.ObservationError):
                bridge._mapped_cells(value.case_id, (raw,), cumulative_profile=value)
        current = observation(value)
        for change in ({'purpose':'repeatability'}, {'profile_sha256':old.sha256},
                       {'target_definition_sha256':'f'*64}):
            raw = observer.process_observation(json.loads(current.binding_json) | change,
                json.loads(current.transport_json), current.stdout, current.stderr)
            with self.assertRaises(bridge.ObservationError):
                bridge._mapped_cells(value.case_id, (raw,), cumulative_profile=value)
        with self.assertRaises(bridge.ObservationError):
            bridge._mapped_cells(value.case_id, (current,), cumulative_profile=old)

    def test_closed_codec_preserves_new_type_and_cannot_drop_its_marker(self):
        value = self.profiles[0]
        restored = codec.unpack(codec.pack({'profile':value, 'plan':projection.expose_plan(synthetic_plan())}))
        self.assertIs(type(restored['profile']), projection.CliProjectionProfile)
        self.assertEqual(restored['profile'], value)
        projection.validate_plan(restored['plan'], ROOT)
        registration = input_registration(value)
        spec = shared.ObservationSpec('cli', ROOT, ROOT, ROOT, None, None,
            registration, execution.ClientPolicy('sha256:'+'a'*64), cumulative_profile=restored['profile'])
        self.assertEqual(spec.observation_registration(), execution.observation_registration(registration))
        with self.assertRaises(consumer.AuthorityError):
            replace(spec, cumulative_profile=legacy.cli_profile(value.case_id, purpose=value.purpose)).observation_registration()
        with self.assertRaises(consumer.AuthorityError):
            replace(spec, cumulative_profile=None).observation_registration()


class CumulativeCliProjectionExposureV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = synthetic_plan()
        cls.plan = projection.expose_plan(cls.base)

    def test_all_four_releases_bind_pair_and_retain_public_checks_and_limits(self):
        self.assertIsNone(projection.validate_plan(self.base))
        self.assertEqual(projection.validate_plan(self.plan, ROOT), projection.manifest())
        self.assertNotEqual(self.plan.sha256, self.base.sha256)
        self.assertNotEqual(self.plan.cohort.requirements_release_sha256, self.base.cohort.requirements_release_sha256)
        for before, after in zip(self.base.releases, self.plan.releases, strict=True):
            self.assertEqual(after.instructions, before.instructions + '\n\n' + projection.text())
            self.assertEqual(after.files, before.files | {projection.RELEASE_PATH:projection.text()})
            for name in ('checks','command','ordered_check_ids','requirement_ids','purpose','source_contract_sha256'):
                self.assertEqual(getattr(before, name), getattr(after, name))
        for name in ('horizon_seconds','source_generations','partition_seconds','executor_slots','financial_closure_policy'):
            self.assertEqual(getattr(self.plan, name), getattr(self.base, name))
        self.assertEqual(len(self.plan.roster.children), 6)
        self.assertEqual(sum(len(row.actors) for row in self.plan.roster.children), 96)

    def test_actual_builder_repair_reviewer_materialization_has_full_text_and_file(self):
        controller = object.__new__(StudyController)
        controller.plan = self.plan
        counts = {'build':0, 'repair':0, 'review':0}
        actors = set()
        for milestone, release in enumerate(self.plan.releases, 1):
            mesh = FakeMesh()
            files = self.plan.initial_files | release.files
            ref = mesh.arrive('seed', 'source', canonical_bytes({'files':files,'base_sha':'a'*40}))
            for index in range(6):
                for generation, reviews in ((0, False), (1, False), (0, True), (1, True)):
                    for actor, directive in controller._directives(index, milestone, generation, ref, (), reviews=reviews):
                        request, view = materialize(directive, mesh, 'd'*64)
                        actors.add(actor)
                        counts[directive.kind] += 1
                        self.assertEqual(request.instructions.count(projection.text()), 1)
                        self.assertEqual(request.files[projection.RELEASE_PATH], projection.text())
                        self.assertNotIn(projection.RELEASE_PATH, request.allowed_paths)
                        self.assertEqual(directive.context.requirements_sha256, release.sha256)
                        self.assertEqual(view.context, directive.context)
        self.assertEqual(len(actors), 96)
        self.assertEqual(counts, {'build':288, 'repair':288, 'review':192})

    def test_dropped_or_changed_text_file_marker_and_source_pin_fail_before_runtime_constructor(self):
        from gossip_harness import cumulative_study_runtime_v2 as runtime
        variants = []
        for index in range(4):
            release = self.plan.releases[index]
            for changed in (replace(release, instructions='omitted full text'),
                            replace(release, files={}),
                            replace(release, files={projection.RELEASE_PATH:'changed'})):
                releases = list(self.plan.releases)
                releases[index] = changed
                variants.append(rebalance(self.plan, releases=tuple(releases)))
        variants.append(rebalance(self.plan, runtime={key:value for key,value in self.plan.runtime.items()
            if key not in ('cli_projection_protocol', 'cli_projection_contract')}))
        variants.append(rebalance(self.plan, runtime=self.plan.runtime | {'cli_projection_contract':{'sha256':projection.TEXT_SHA256}}))
        variants.append(replace(self.plan, source_pins=self.plan.source_pins | {projection.TEXT_PATH:'f'*64}))
        for changed in variants:
            with self.subTest(contract=changed.sha256), patch('gossip_harness.cumulative_study_controller_v2.StudyController') as constructor:
                with self.assertRaises(projection.legacy.ProfileError):
                    runtime.run_study(changed, checkpoint=None, expected_checkpoint=None, repository=ROOT,
                        existing_ledger_path=ROOT/'not-a-wallet', expected_ledger_identity={}, output=ROOT,
                        workers={}, mode='fixture', permit_provider=None, evaluator=None)
                constructor.assert_not_called()

    def test_addendum_path_cannot_be_a_writable_package_scope(self):
        for path in (projection.RELEASE_PATH, 'requirements', projection.RELEASE_PATH+'/child'):
            paths = dict(self.plan.package_paths)
            paths[next(iter(paths))] = (path,)
            with self.assertRaises(ValueError):
                projection.validate_plan(replace(self.plan, package_paths=paths))

    def test_final_v3_rejects_mixed_profile_before_original_lookups_or_publication(self):
        owner = object.__new__(final.FinalAcceptanceV3)
        owner.plan = self.plan
        old = legacy.cli_profile('cli-empty', purpose='public_release')
        spec = SimpleNamespace(kind='cli', cumulative_profile=old)
        with patch.object(owner, '_registered_gate') as lookup, patch.object(owner, '_put') as retain:
            with self.assertRaises(consumer.AuthorityError):
                owner.dispatch(spec)
            lookup.assert_not_called()
            retain.assert_not_called()
        with self.assertRaises(ValueError):
            projection.validate_spec(self.base, SimpleNamespace(kind='cli', cumulative_profile=
                projection.profile_for('cli-empty', purpose='public_release')))

    def test_cold_restore_rejects_dropped_plan_marker_before_opening_originals(self):
        from gossip_harness import cumulative_final_originals_v1 as cold
        dropped = rebalance(self.plan, runtime={key:value for key,value in self.plan.runtime.items()
            if not key.startswith('cli_projection_')})
        with patch.object(cold, '_prospective_sources') as sources, patch.object(cold, '_restore_scope') as scope:
            with self.assertRaises(ValueError):
                cold._restore_owner({'repository':str(ROOT)}, None, dropped)
            sources.assert_not_called()
            scope.assert_not_called()


class CumulativeCliProjectionOwnerV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='gossip-clarified-cli-')
        cls.addClassCleanup(cls.temp.cleanup)
        cls.base = Path(cls.temp.name).resolve()
        cls.store = GitStore.create(cls.base/'source.git', {
            'library/__init__.py':"raise RuntimeError('never execute on host')\n",
            'library/__main__.py':"raise RuntimeError('never execute on host')\n",
            projection.RELEASE_PATH:projection.text()})
        cls.commit = cls.store.head()
        cls.tree, cls.files = execution.capture_git_source(cls.store, cls.commit)
        cls.policy = execution.ClientPolicy('sha256:'+'a'*64)
        cls.runtime = {'kind':'fixture-no-Docker'}

    def setUp(self):
        self.root = self.base/self.id().rsplit('.',1)[-1]
        self.root.mkdir()
        self.value = projection.profile_for('cli-empty', purpose='public_release')
        self.registration = input_registration(self.value, self.files)
        self.registration = replace(self.registration, commit_oid=self.commit, tree_oid=self.tree)
        self.current = execution.observation_registration(self.registration)
        self.freeze = None
        self.admission = admission.ObservationAdmission(self.current,
            verify_registration=lambda:self.current, verify_cohort=lambda:self.freeze)
        self.head = ExternalHead.create(self.root/'head', journal_roots=(self.root/'raw', self.root/'delta'))
        self.addCleanup(self.head.close)

    def owner(self, *, expected=None, value=None, registration=None):
        owner = execution.CandidateClientExecution(self.root/'raw', self.store,
            registration or self.registration, self.policy, mode='fixture',
            checkpoint_authority=self.head, delta_root=self.root/'delta', cleanup_root=self.root/'cleanup',
            admission_authority=self.admission, expected_checkpoint=expected,
            cumulative_profile=self.value if value is None else value)
        self.addCleanup(owner.close)
        return owner

    def test_real_git_owner_reopen_preserves_pair_profile_and_original_source(self):
        owner = self.owner()
        self.assertEqual(owner.protocol, projection.EXECUTION_PROTOCOL)
        self.assertEqual(owner.config['cumulative_profile']['effective_requirements'], projection.manifest())
        self.assertEqual(owner.files[projection.RELEASE_PATH], projection.text().encode())
        self.assertEqual(owner._step_binding({'execution_id':'offline'}, 0)['protocol'], projection.EXECUTION_PROTOCOL)
        expected = owner.checkpoint()
        owner.close()
        reopened = self.owner(expected=expected)
        self.assertEqual(reopened.checkpoint(), expected)
        self.assertEqual(reopened.observation_registration, self.current)
        spec = shared.ObservationSpec('cli', self.root/'raw', self.root/'delta', self.root/'cleanup',
            self.head, self.store, self.registration, self.policy, cumulative_profile=self.value)
        self.assertEqual(spec.observation_registration(), reopened.observation_registration)

    def test_missing_changed_file_old_profile_and_relabelled_binding_do_not_admit(self):
        for files, value in (({k:v for k,v in self.files.items() if k != projection.RELEASE_PATH}, self.value),
                             (self.files | {projection.RELEASE_PATH:b'changed'}, self.value),
                             (self.files, legacy.cli_profile(self.value.case_id, purpose=self.value.purpose))):
            with self.assertRaises(ValueError):
                execution.binding_for(files, self.value.case_id, self.policy, self.runtime,
                    requirements_sha256=legacy.TARGET_CONTRACT_SHA256, milestone='M4',
                    purpose=self.value.purpose, cumulative_profile=value)
        old_files = {k:v for k,v in self.files.items() if k != projection.RELEASE_PATH}
        old = execution.binding_for(old_files, self.value.case_id, self.policy, self.runtime,
            requirements_sha256=legacy.TARGET_CONTRACT_SHA256, milestone='M4',
            cumulative_profile=legacy.cli_profile(self.value.case_id,purpose=self.value.purpose))
        relabelled = replace(old, protocol=projection.EXECUTION_PROTOCOL,
            source_sha256=self.registration.binding.source_sha256, profile_sha256=self.value.sha256,
            target_definition_sha256=execution.digest(self.value.target_definition()))
        gate = execution.gate_for(self.registration.gate.binding.subject, relabelled, gate_id='clarified-cli')
        with self.assertRaises(execution.ExecutionError):
            self.owner(registration=replace(self.registration,binding=relabelled,gate=gate))
        self.assertFalse((self.root/'raw').exists())

    def test_new_purpose_still_requires_original_six_source_freeze_and_never_fixture_acceptance(self):
        self.value = projection.profile_for('cli-empty', purpose='independent_acceptance')
        self.registration = replace(input_registration(self.value,self.files), commit_oid=self.commit,tree_oid=self.tree)
        self.current = execution.observation_registration(self.registration)
        self.admission = admission.ObservationAdmission(self.current,
            verify_registration=lambda:self.current,verify_cohort=lambda:self.freeze)
        owner = self.owner()
        with self.assertRaises(admission.AdmissionUnavailable):
            owner.execute_once()
        self.assertFalse(owner.has_retained('intent.json'))
        subject = self.registration.gate.binding.subject
        self.freeze = registry.CohortFreeze(tuple(replace(subject,trajectory_id=t) for t in COHORT),'a'*64,'b'*64,True)
        with self.assertRaises(execution.ExecutionError):
            owner.execute_once()
        self.assertEqual(owner.retained_freeze(), self.freeze)
        with self.assertRaises(bridge.ObservationError):
            bridge.publish_verifier(owner)

    def test_projected_pass_cannot_hide_original_cleanup_failure(self):
        # Only raw process verification is stubbed. The original chain, profile,
        # admission, publication and reader are real; this is no physical claim.
        self.value = projection.profile_for('cli-grammar-unknown-command',purpose='public_release')
        self.registration = replace(input_registration(self.value,self.files),commit_oid=self.commit,tree_oid=self.tree)
        self.current = execution.observation_registration(self.registration)
        self.admission = admission.ObservationAdmission(self.current,
            verify_registration=lambda:self.current,verify_cohort=lambda:self.freeze)
        owner = self.owner()
        owner._retain('intent.json',execution.encoded({'cohort_freeze':None}))
        terminal = execution.encoded({'synthetic_process_only':True})
        owner._retain('terminal.json',terminal)
        owner.mode = 'physical'
        def original():
            return execution.ClientHistoryResult('offline',self.value.case_id,'observation_unavailable',
                (observation(self.value,exit_code=2),),(),False,('cleanup unavailable',),
                execution.sha256(terminal),owner.checkpoint())
        with patch.object(owner,'verified_execution',side_effect=original):
            expected = bridge.publish_verifier(owner)
            result = bridge.ClientObservationSource(owner,expected).observation(self.registration.gate,None)
        record = owner.json_authenticated(bridge.VERIFIER_FILE)
        self.assertEqual(record['protocol'],projection.OBSERVATION_PROTOCOL)
        self.assertEqual(record['cumulative_profile']['effective_requirements'],projection.manifest())
        self.assertEqual(result.execution.outcomes,(registry.CaseResult(self.value.decisive_case_ids[0],'passed'),))
        self.assertEqual(result.execution.terminal_status,'infrastructure_error')
        self.assertFalse(record['cleanup_verified'])
        self.assertEqual(record['cells'][1]['status'],'skipped')


class CumulativeCliProjectionScopeV1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = scope_source.load_catalog(ROOT)
        cls.value = projection.profile_for('cli-empty', purpose='public_release')
        cls.plan = projection.expose_plan(synthetic_plan())
        registration = input_registration(cls.value)
        subject = replace(registration.gate.binding.subject,cohort_id=cls.plan.cohort.cohort_id,
            trajectory_id=cls.plan.roster.children[0].trajectory,execution_contract_sha256=cls.plan.sha256)
        gate = execution.gate_for(subject,registration.binding,gate_id=registration.gate.gate_id)
        registration = replace(registration,gate=gate,
            cohort_trajectory_ids=tuple(row.trajectory for row in cls.plan.roster.children))
        cls.component = scope_source.cli_slice(registration)
        cls.declaration = scope_source.assemble_declaration(cls.catalog,
            cls.plan.cohort, (cls.component,),
            review_sha256='d'*64, capacity_profile=registry.HISTORY_CAPACITY_PROFILE)
        cls.scope = scope_source.scope_review_input(cls.catalog, cls.declaration)
        cls.submission = scope_authority.ScopeSubmission(cls.catalog, cls.declaration, cls.scope,
            cls.component.gate.binding.subject, (cls.component,), (),
            cli_projection_contract=projection.manifest())

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='gossip-cli-addendum-review-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        raw, delta = self.root/'raw', self.root/'delta'
        self.head = ExternalHead.create(self.root/'head', journal_roots=(raw,delta))
        self.addCleanup(self.head.close)
        self.chain = checkpoint.CheckpointChain.create(raw,delta,
            context={'protocol':projection.SCOPE_PROTOCOL,'purpose':'offline review mechanics only'},authority=self.head)
        self.addCleanup(self.chain.close)
        self.owner = scope_authority.ScopeRegistrationController(ROOT,self.chain,self.chain.commitment)

    def deliver(self, staged, mutate):
        # Real anchored records, deliberately fabricated reports; no full-scope
        # declaration or positive production semantic assessment is supplied.
        request = json.loads(self.chain.read(staged.request_name))
        decisions = [{'target_id':row['id'],'target_sha256':row['sha256'],'decision':'approved',
            'rationale':'Synthetic delivery mechanism fixture only.',
            'inspected_references':['fixture-only:'+row['id']]} for row in request['targets']]
        mutate(decisions)
        report = {'protocol':'complete-scope-semantic-review-v1','purpose':scope_authority.REVIEW_PURPOSE,
            'reviewer_id':'fixture-reviewer','reviewed_request_sha256':staged.request_sha256,
            'decisions':decisions,'limitations':['No actual semantic review.']}
        raw = scope_source.encoded(report)
        provenance = scope_source.encoded({'protocol':'scope-independent-review-delivery-v1',
            'reviewer_id':'fixture-reviewer','role':'independent_scope_reviewer','report_name':'review.json',
            'report_sha256':scope_source.sha(raw),'request_sha256':staged.request_sha256,
            'delivery_reference':'Synthetic independently enrolled fixture only'})
        self.chain.retain('review.json',raw)
        self.chain.retain('delivery.json',provenance)
        enrollment = scope_authority.ReviewEnrollment('fixture-reviewer','independent_scope_reviewer',
            'review.json',scope_source.sha(raw),'delivery.json',scope_source.sha(provenance))
        self.owner = scope_authority.ScopeRegistrationController(ROOT,self.chain,self.chain.commitment,
            reviewer_enrollments=(enrollment,))

    def test_addendum_is_an_extra_mandatory_target_not_a_replacement_for_full_scope(self):
        request = self.submission.request()
        targets = request['targets']
        for prefix,count in (('obligation:',312),('authority:',22),('gap:',188)):
            self.assertEqual(sum(row['id'].startswith(prefix) for row in targets),count)
        extra = [row for row in targets if row['id'] == projection.REVIEW_TARGET]
        self.assertEqual(len(extra),1)
        target = extra[0]
        self.assertEqual(target['value']['public_addendum_text'],projection.text())
        self.assertEqual(target['value']['effective_requirements'],projection.manifest())
        self.assertEqual(target['value']['affected_cli_slices'],[asdict(self.component)])
        self.assertEqual(target['value']['execution_contract_sha256'],self.submission.subject.execution_contract_sha256)
        self.assertEqual(target['value']['successful_command_projections'],projection.COMMANDS)
        self.assertFalse(request['acceptance_authority'])
        self.assertEqual(request['protocol'],projection.SCOPE_PROTOCOL)
        self.assertEqual(len(request['declaration']['obligations']),312)
        self.assertIn(projection.TEXT_PATH,request['implementation_sources'])
        self.assertTrue(all(row['sha256'] == scope_source.sha(scope_source.encoded(row['value'])) for row in targets))

    def test_reopened_typed_submission_cannot_drop_or_change_review_identity(self):
        reopened = codec.unpack(codec.pack(self.submission))
        self.assertEqual(reopened.request(),self.submission.request())
        for marker in (None, {'text_sha256':projection.TEXT_SHA256}):
            with self.assertRaises(consumer.AuthorityError):
                replace(reopened,cli_projection_contract=marker).request()
        legacy_value = legacy.cli_profile(self.value.case_id,purpose=self.value.purpose)
        old_files = {'library/__init__.py':b'', 'library/__main__.py':b''}
        old = scope_source.cli_slice(input_registration(legacy_value,old_files))
        with self.assertRaises(consumer.AuthorityError):
            replace(reopened,slices=(old,)).request()
        projection.validate_submission(self.plan,(reopened,))
        with self.assertRaisesRegex(ValueError,'different original study'):
            projection.validate_submission(replace(self.plan,initial_files={**self.plan.initial_files,'new.py':'pass\n'}),(reopened,))
        with self.assertRaises(ValueError):
            projection.validate_submission(synthetic_plan(),(reopened,))

    def test_staged_full_target_with_no_actual_review_cannot_register(self):
        staged = self.owner.stage(self.submission)
        self.assertIsNone(staged.design.registry)
        self.assertTrue(staged.design.blockers)
        self.assertEqual(json.loads(self.chain.read(staged.request_name))['protocol'],projection.SCOPE_PROTOCOL)
        with self.assertRaises(scope_authority.RegistrationMissing):
            self.owner.authenticate_review(staged,report_name='absent-original.json')
        with self.assertRaises(scope_authority.RegistrationMissing):
            self.owner.register(self.submission,report_name='absent-original.json')
        self.assertIsNone(self.owner.open_snapshot().registration(self.submission.subject))

    def test_missing_independent_addendum_decision_is_not_digest_only_approval(self):
        staged = self.owner.stage(self.submission)
        self.deliver(staged,lambda rows:rows.pop(next(i for i,row in enumerate(rows)
            if row['target_id'] == projection.REVIEW_TARGET)))
        with self.assertRaisesRegex(consumer.AuthorityError,'Incomplete semantic review'):
            self.owner.authenticate_review(staged,report_name='review.json')

    def test_unresolved_addendum_remains_unavailable_despite_other_fixture_approvals(self):
        staged = self.owner.stage(self.submission)
        def unresolved(rows):
            next(row for row in rows if row['target_id'] == projection.REVIEW_TARGET)['decision'] = 'unresolved'
        self.deliver(staged,unresolved)
        with self.assertRaisesRegex(scope_authority.RegistrationMissing,projection.REVIEW_TARGET):
            self.owner.authenticate_review(staged,report_name='review.json')
        self.assertIsNone(self.owner.open_snapshot().registration(self.submission.subject))

    def test_changed_addendum_digest_in_enrolled_review_is_rejected(self):
        staged = self.owner.stage(self.submission)
        def changed(rows):
            next(row for row in rows if row['target_id'] == projection.REVIEW_TARGET)['target_sha256'] = 'f'*64
        self.deliver(staged,changed)
        with self.assertRaisesRegex(consumer.AuthorityError,'Substituted'):
            self.owner.authenticate_review(staged,report_name='review.json')


class CumulativeCliProjectionCompositionV1Tests(unittest.TestCase):
    """Actual entry/read composition; all absent review/physical authority stays absent."""
    def setUp(self):
        from gossip_harness import cumulative_study_successor_v1 as old_successor
        from gossip_harness import cumulative_study_successor_v3 as successor
        from gossip_harness import cumulative_prerequisite_qualification_v1 as qualification
        from gossip_harness import cumulative_source_promotion_v1 as promotion
        temporary = tempfile.TemporaryDirectory(prefix='gossip-cli-projection-composition-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        base = synthetic_plan()
        runtime = {**base.runtime,'final_acceptance_protocol':final.PROTOCOL,
            'study_successor_protocol':successor.PROTOCOL,
            'prerequisite_qualification_protocol':qualification.PROTOCOL,
            'final_promotion_protocol':promotion.PROTOCOL,
            'final_acceptance_financial_mode':'fixture',
            'final_acceptance_roots':{name:str(self.root/'final'/name) for name in ('raw','delta','head')}}
        sources = {**base.source_pins,**final.implementation_sources(),**final.terminal_implementation_sources(),
            **{'gossip_harness/'+Path(module.__file__).name:hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
               for module in (old_successor,successor)}}
        self.plan = projection.expose_plan(rebalance(replace(base,source_pins=sources),runtime=runtime))

    def chain(self, name):
        root = self.root/name
        root.mkdir()
        raw,delta = root/'raw',root/'delta'
        head = ExternalHead.create(root/'head',journal_roots=(raw,delta))
        self.addCleanup(head.close)
        context = {'protocol':'offline-cli-projection-composition-v1','name':name}
        chain = checkpoint.CheckpointChain.create(raw,delta,context=context,authority=head)
        self.addCleanup(chain.close)
        return chain,head,context

    def test_actual_final_v3_constructor_and_reader_retain_paired_contract_and_six_unavailable_slots(self):
        import sqlite3
        from gossip_harness import cumulative_study_successor_v3 as successor
        from gossip_harness import cumulative_terminal_originals_v2 as terminal
        from gossip_harness.cumulative_study_controller_v2 import Records
        from gossip_harness.peer_financial_authority_v2 import ledger_identity
        study,_,_ = self.chain('study')
        journal,_,_ = self.chain('final')
        scope_chain,_,_ = self.chain('scope')
        ledger = self.root/'original-unenrolled-ledger.sqlite'
        with sqlite3.connect(ledger) as db:
            db.execute('CREATE TABLE offline_fixture_only (id INTEGER PRIMARY KEY)')
        identity = ledger_identity(ledger)
        Records(study).put('contract',self.plan.record())
        Records(study).put('ledger',identity)
        scope = scope_authority.ScopeRegistrationController(ROOT,scope_chain,scope_chain.commitment)
        # Empty submissions honestly mean no semantic registration. The real
        # reader reports absent originals; no completed trajectory is simulated.
        owner = final.FinalAcceptanceV3(plan=self.plan,repository=ROOT,ledger_identity=identity,
            study_chain=study,study_expected=study.commitment,journal=journal,expected=journal.commitment,
            scope=scope,submissions=())
        self.addCleanup(owner.close)
        contract = owner.records.read('final.contract')
        self.assertEqual(contract['effective_requirements'],projection.manifest())
        self.assertEqual(contract['study_sha256'],self.plan.sha256)
        self.assertEqual(contract['scope_subjects'],[])
        self.assertEqual(contract['sources'],final.implementation_sources())
        result = successor.run_final_phase(owner,())
        self.assertIs(type(owner.originals),terminal.TerminalCohortOriginals)
        self.assertEqual(len(result['final_phase']['slots']),6)
        self.assertEqual([slot['terminal_outcome'] for slot in result['final_phase']['slots']],['unattempted']*6)
        self.assertEqual(sum(slot['process_counts']['planned'] for slot in result['final_phase']['slots']),96)
        self.assertFalse(result['accepted'] or result['completed_and_accepted'])
        self.assertFalse(result['final_phase']['freeze_available'])
        self.assertIsNone(owner.freeze)
        self.assertIsNone(owner.records.read('final.freeze'))
        self.assertEqual(owner.records.read('successor.contract')['effective_requirements'],projection.manifest())
        self.assertFalse(owner.records.read('final.terminal-verifier')['acceptance_authority'])
        self.assertFalse(owner.enrollments)

    def test_direct_successor_rejects_rehashed_missing_or_changed_exposure_before_runtime(self):
        from gossip_harness import cumulative_study_successor_v3 as successor
        from gossip_harness import cumulative_study_runtime_v2 as runtime
        self.assertEqual(successor.validate_successor(self.plan,ROOT)['effective_requirements'],projection.manifest())
        variants = []
        for index in range(4):
            for change in ({'instructions':'Missing approved text'},
                           {'files':{projection.RELEASE_PATH:'Different text'}}):
                releases = list(self.plan.releases)
                releases[index] = replace(releases[index],**change)
                variants.append(rebalance(self.plan,releases=tuple(releases)))
        for changed in variants:
            with self.subTest(releases=changed.cohort.requirements_release_sha256):
                with self.assertRaisesRegex(ValueError,'Exact text and immutable file'):
                    successor.validate_successor(changed,ROOT)
                with patch.object(runtime,'run_study') as dispatch:
                    with self.assertRaisesRegex(ValueError,'Exact text and immutable file'):
                        successor.run_public_phase(changed,repository=ROOT,mode='fixture')
                    dispatch.assert_not_called()

    def test_direct_child_admission_and_tick_revoke_exposure_before_effects(self):
        from gossip_harness import cumulative_study_runtime_v2 as runtime
        from gossip_harness.cumulative_study_controller_v2 import Records
        from unittest.mock import Mock
        records,_,_ = self.chain('child-proof')
        before = records.commitment
        releases = list(self.plan.releases)
        releases[2] = replace(releases[2],files={projection.RELEASE_PATH:'Changed before child construction'})
        invalid = rebalance(self.plan,releases=tuple(releases))
        output = self.root/'child-not-created'
        # Exact child constructor reaches the exposure guard before it may
        # inspect a wallet, construct source/finance/mesh, or request a permit.
        permit,evaluator,clock = Mock(),Mock(),Mock()
        with patch.object(runtime,'ledger_identity') as ledger, \
                patch.object(runtime.GossipChildRuntime,'_initialize_source') as source, \
                patch.object(runtime,'MeshNode') as mesh, \
                patch.object(runtime.CumulativeAuthorityV5,'open') as finance, \
                patch.object(runtime.GossipChildRuntime,'_launch') as launch:
            with self.assertRaisesRegex(ValueError,'Exact text and immutable file'):
                runtime.GossipChildRuntime(invalid,0,Records(records),10,root=output,repository=ROOT,
                    existing_ledger_path=self.root/'absent-wallet',expected_ledger_identity={},workers={},
                    mode='fixture',permit_provider=permit,evaluator=evaluator,clock=clock)
            for effect in (ledger,source,mesh,finance,launch,permit,evaluator,clock):
                effect.assert_not_called()
        self.assertFalse(output.exists())
        self.assertEqual(records.commitment,before)
        # Isolate an already-created runtime's tick method from real processes.
        # The healthy guard reaches its partition effect; nested exposure
        # revocation must stop before clock/deadline, poll, restart or publication.
        child = object.__new__(runtime.GossipChildRuntime)
        child.plan,child.repository = self.plan,ROOT
        child.clock,child.deadline = Mock(return_value=2),10
        child.partition_until,child.partition_publication_pending = 1,False
        child._partition,child._launch = Mock(),Mock()
        process = Mock()
        process.poll.return_value = None
        child.processes = {'offline-role':process}
        child.records = Records(records)
        child._tick()
        child._partition.assert_called_once_with(False)
        process.poll.assert_called_once_with()
        child.clock.reset_mock();child._partition.reset_mock();process.poll.reset_mock()
        self.plan.releases[1].files[projection.RELEASE_PATH] = 'Revoked after initial exposure'
        with self.assertRaises(ValueError):
            child._tick()
        for effect in (child.clock,child._partition,process.poll,child._launch):
            effect.assert_not_called()
        self.assertEqual(records.commitment,before)

    def test_clarified_typed_cold_requests_reconstruct_then_stop_at_missing_complete_scope(self):
        from contextlib import ExitStack
        from gossip_harness import cumulative_final_originals_v1 as cold
        from gossip_harness import cumulative_rehearsal_export_v1 as export
        from gossip_harness.peer_financial_terminal_v1 import FinancialError
        chain,head,context = self.chain('cold-scope')
        authority = scope_authority.ScopeRegistrationController(ROOT,chain,chain.commitment)
        catalog = scope_source.load_catalog(ROOT)
        value = projection.profile_for('cli-empty',purpose='public_release')
        base_registration = input_registration(value)
        trajectories = tuple(row.trajectory for row in self.plan.roster.children)
        submissions,registrations = [],[]
        for trajectory in trajectories:
            subject = replace(base_registration.gate.binding.subject,cohort_id=self.plan.cohort.cohort_id,
                trajectory_id=trajectory,execution_contract_sha256=self.plan.sha256)
            gate = execution.gate_for(subject,base_registration.binding,gate_id='clarified-cli')
            registration = replace(base_registration,gate=gate,cohort_trajectory_ids=trajectories)
            component = scope_source.cli_slice(registration)
            declaration = scope_source.assemble_declaration(catalog,self.plan.cohort,(component,),
                review_sha256='d'*64,capacity_profile=registry.HISTORY_CAPACITY_PROFILE)
            submission = scope_authority.ScopeSubmission(catalog,declaration,
                scope_source.scope_review_input(catalog,declaration),subject,(component,),(),
                cli_projection_contract=projection.manifest())
            staged = authority.stage(submission)
            self.assertIsNone(staged.design.registry)
            self.assertTrue(staged.design.blockers)
            submissions.append(submission);registrations.append(registration)
        writer = export.InputWriter(self.root/'cold-inputs')
        profile_ref = writer.put(value,typed=True)
        refs = [writer.put(row,typed=True) for row in submissions]
        packet = {'chain':'scope','enrollments':writer.put((),typed=True),'submissions':refs}
        proof = export.proof_descriptor(chain,context)
        # Original authority owns these locks until all records and external
        # expected head are retained. Cold readers reopen after the owner closes.
        chain.close();head.close()
        with ExitStack() as stack:
            pool = cold.ProofPool({'scope':proof},stack)
            actual = codec.read(profile_ref)
            self.assertIs(type(actual),projection.CliProjectionProfile)
            self.assertEqual(actual,value)
            head = pool.open('scope').authority
            for registration,ref,original in zip(registrations,refs,submissions,strict=True):
                restored = codec.read(ref)
                self.assertEqual(restored.request(),original.request())
                spec = shared.ObservationSpec('cli',self.root/'not-dispatched',self.root/'not-dispatched-delta',
                    self.root/'not-dispatched-cleanup',head,None,registration,
                    execution.ClientPolicy('sha256:'+'a'*64),cumulative_profile=actual)
                projection.validate_spec(self.plan,spec)
                self.assertEqual(spec.observation_registration(),execution.observation_registration(registration))
            # Observe entry into the real effect methods without replacing
            # source-authenticated functions/classes with mocks.
            import sys
            effect_codes = {execution.CandidateClientExecution.__init__.__code__,
                final.FinalAcceptanceV3.dispatch.__wrapped__.__code__}
            entered = []
            previous_profile = sys.getprofile()
            def profile(frame,event,arg):
                if event == 'call' and frame.f_code in effect_codes:
                    entered.append(frame.f_code.co_name)
            try:
                sys.setprofile(profile)
                with self.assertRaisesRegex(FinancialError,'Complete executable semantic scope unavailable'):
                    cold._restore_scope(pool,packet,ROOT)
            finally:
                sys.setprofile(previous_profile)
            self.assertEqual(entered,[])
            self.assertFalse((self.root/'not-dispatched').exists())
            self.assertEqual(pool.open('scope').commitment,checkpoint.PrefixCommitment(**proof['expected']))
            self.assertFalse(pool.open('scope').has('final.freeze.json'))

    def test_minimal_legacy_plan_binds_actual_guard_and_refuses_missing_wrong_or_stale_sources(self):
        from copy import deepcopy
        from unittest.mock import Mock
        from gossip_harness import cumulative_study_controller_v2 as controller
        from gossip_harness import cumulative_study_runtime_v2 as runtime
        bare = synthetic_plan()
        guard = projection.guard_sources()
        self.assertEqual(set(guard),{
            'gossip_harness/cumulative_cli_projection_v1.py',
            'gossip_harness/cumulative_observation_profile_v1.py',
            'gossip_harness/candidate_observation_admission_v1.py',
            'gossip_harness/project_acceptance_registry_v1.py',
            projection.TEXT_PATH,projection.BASE_PATH})
        self.assertTrue(set(guard) <= set(controller.SOURCE_CLOSURE))
        self.assertEqual({name:bare.source_pins[name] for name in guard},guard)
        self.assertNotIn('cli_projection_protocol',bare.runtime)
        self.assertIsNone(projection.validate_plan(bare,ROOT))
        bare.verify_sources(ROOT)
        for name in guard:
            for missing in (True,False):
                invalid = deepcopy(bare)
                if missing:
                    invalid.source_pins.pop(name)
                else:
                    invalid.source_pins[name] = 'f'*64
                with self.subTest(name=name,missing=missing):
                    with self.assertRaisesRegex(ValueError,'guard source binding'):
                        projection.validate_plan(invalid,ROOT)
                    with patch.object(runtime,'ledger_identity') as wallet, \
                            patch('gossip_harness.cumulative_study_controller_v2.StudyController') as start:
                        with self.assertRaises(ValueError):
                            runtime.run_study(invalid,output=self.root/'never-created',repository=ROOT,
                                checkpoint=None,expected_checkpoint=None,existing_ledger_path=self.root/'absent-wallet',
                                expected_ledger_identity={},workers={},mode='fixture',permit_provider=None,evaluator=None)
                        with self.assertRaises(ValueError):
                            runtime.GossipChildRuntime(invalid,0,None,10,root=self.root/'never-created',repository=ROOT,
                                existing_ledger_path=self.root/'absent-wallet',expected_ledger_identity={},workers={},
                                mode='fixture',permit_provider=None,evaluator=None)
                        wallet.assert_not_called();start.assert_not_called()
                    child = object.__new__(runtime.GossipChildRuntime)
                    child.plan,child.repository,child.clock = invalid,ROOT,Mock()
                    with self.assertRaisesRegex(ValueError,'guard source binding'):
                        child._tick()
                    child.clock.assert_not_called()
        self.assertFalse((self.root/'never-created').exists())
        # A fresh disk hash may not bless an older loaded guard dependency.
        with patch.object(legacy,'LOADED_SOURCE_SHA256','f'*64):
            with self.assertRaises(ValueError):
                projection.validate_plan(bare,ROOT)

    def test_legacy_original_controller_reopen_and_next_child_reject_guard_repository_drift(self):
        import shutil
        from unittest.mock import Mock
        from gossip_harness import cumulative_study_controller_v2 as controller
        from gossip_harness.peer_financial_authority_v2 import ledger_identity
        bare = synthetic_plan()
        repository = self.root/'legacy-repository'
        repository.mkdir()
        for name in bare.source_pins:
            target = repository/name
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(ROOT/name,target)
        chain,_,_ = self.chain('legacy-controller')
        wallet = self.root/'offline-identity-only.sqlite'
        wallet.write_bytes(b'Owned identity fixture, never opened as a financial database')
        identity = ledger_identity(wallet)
        factory,clock = Mock(),Mock(return_value=100)
        owner = controller.StudyController(bare,checkpoint=chain,expected_checkpoint=chain.commitment,
            repository=repository,existing_ledger_path=wallet,expected_ledger_identity=identity,
            runtime_factory=factory,clock=clock)
        before = chain.commitment
        # Alter only the copied prospective artifact, never the loaded source
        # or the root repository; the original plan stays exactly unchanged.
        (repository/projection.TEXT_PATH).write_text('Changed independent repository addendum')
        with self.assertRaisesRegex(ValueError,'guard repository source'):
            owner._child(0)
        with self.assertRaisesRegex(ValueError,'guard repository source'):
            controller.StudyController(bare,checkpoint=chain,expected_checkpoint=chain.commitment,
                repository=repository,existing_ledger_path=wallet,expected_ledger_identity=identity,
                runtime_factory=factory,clock=clock)
        self.assertEqual(chain.commitment,before)
        self.assertIsNone(owner.records.read('child.'+bare.cohort.trajectories[0].id+'.begin'))
        factory.assert_not_called();clock.assert_not_called()
