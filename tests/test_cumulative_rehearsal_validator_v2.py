"""Closed V2 design/origin controls; no physical rehearsal or acceptance credit.

These tests use synthetic prospective plans and the actual pure V2 contract
producer. They do not run a runtime, provider, role, Git operation or Docker.
"""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import unittest
from unittest.mock import patch

from gossip_harness import cumulative_rehearsal_validator_v2 as reader
from gossip_harness.candidate_checkpoint_chain_v1 import PrefixCommitment
from gossip_harness.cumulative_study_controller_v2 import StudyPlan, digest
from gossip_harness.cumulative_study_runtime_v2 import GossipChildRuntime
from gossip_harness.peer_financial_authority_v3 import FIXTURE_TRANSPORT, LIVE_TRANSPORT
from gossip_harness.peer_financial_authority_v5 import PERMIT_PROTOCOL
from gossip_harness.peer_financial_terminal_v1 import FinancialError
from gossip_harness.peer_financial_terminal_v2 import CLOSURE_POLICY, CLOSURE_POLICY_SHA256
from tests.financial_rehearsal_fixture_v1 import synthetic_plan as v1_plan
from tests.test_cumulative_terminal_originals_v2 import synthetic_plan


class CumulativeRehearsalValidatorV2Tests(unittest.TestCase):
    def setUp(self):
        base = synthetic_plan()
        self.profiles = {name: {'manifest': {'unit_fixture': name}, 'timeout': 2.0}
                         for name in ('mini', 'strong')}
        runtime = {**base.runtime, 'final_acceptance_protocol': 'cumulative-final-acceptance-v3',
            'study_successor_protocol': 'cumulative-study-successor-v3',
            'final_acceptance_financial_mode': 'fixture'}
        limits = {'horizon_seconds': base.horizon_seconds, 'source_generations': base.source_generations,
            'partition_seconds': base.partition_seconds, 'executor_slots': base.executor_slots, 'runtime': runtime}
        self.plan = replace(base, runtime=runtime, cohort=replace(base.cohort,
            resource_contract_sha256=digest(limits), model_profiles_sha256=digest(self.profiles)))
        self.envelope = {'protocol': reader.DESIGN_PROTOCOL, 'execution_contract_sha256': self.plan.sha256,
            'terminal_roster': self.plan.roster.record(), 'sources': self.plan.source_pins,
            'runtime': self.plan.runtime, 'profiles': self.profiles,
            'financial_closure_policy': deepcopy(CLOSURE_POLICY), 'children': []}
        for child in self.plan.roster.children:
            design = {'terminal_roster': self.plan.roster.record(), 'profiles': self.profiles,
                'runtime': self.plan.runtime, 'financial_closure_policy': deepcopy(CLOSURE_POLICY),
                'max_workers': 4, 'action_limits': {'total': 512,
                    'by_kind': {'build': 512, 'repair': 512, 'review': 512},
                    'by_kind_generation': {kind: {'0': 512, '1': 512} for kind in ('build', 'repair', 'review')},
                    'by_actor': dict.fromkeys(child.actors, 12)}}
            self.envelope['children'].append({'cohort': child.cohort, 'trajectory': child.trajectory,
                                              'execution_design': design})

    def origin(self, index=0):
        # Invoke the actual pure contract producer without entering its lifecycle.
        runtime = object.__new__(GossipChildRuntime)
        runtime.plan, runtime.child = self.plan, self.plan.roster.children[index]
        runtime.root = Path('/unexecuted-fixture/' + runtime.child.trajectory)
        runtime.expected_ledger_identity = {'path': '/unexecuted-fixture/wallet.sqlite', 'device': 1, 'inode': 2}
        contract = runtime._financial_contract(Path(runtime.expected_ledger_identity['path']), 'fixture')
        design = deepcopy(self.envelope['children'][index]['execution_design'])
        permit = {'protocol': PERMIT_PROTOCOL, 'mode': 'fixture', 'qualification': None,
            'approval_ref': 'unit-origin-only', 'execution_design': design, 'execution_design_sha256': digest(design),
            'cohort_contract_sha256': digest(contract), 'sources': self.plan.source_pins,
            'profiles': self.profiles, 'max_workers': 4, 'expected_global_cap': 1000000,
            'expected_opening_usage': 1000, 'incremental_cap_micro_usd': 800000}
        config = {'protocol': 'peer-financial-authority-v5', 'financial_closure_policy': deepcopy(CLOSURE_POLICY),
            'financial_closure_policy_sha256': CLOSURE_POLICY_SHA256,
            'operator_permit': permit, 'operator_permit_sha256': digest(permit), 'contract': contract,
            'mode': 'fixture', 'observation_kind': 'simulated',
            'profiles': {name: {**profile, 'transport_identity': FIXTURE_TRANSPORT, 'observation_kind': 'simulated'}
                         for name, profile in self.profiles.items()},
            'sources': permit['sources'], 'execution_design_sha256': permit['execution_design_sha256'],
            'expected_opening_usage': permit['expected_opening_usage'], 'expected_global_cap': permit['expected_global_cap'],
            'incremental_cap_micro_usd': permit['incremental_cap_micro_usd'], 'max_workers': 4}
        manifest = {'config': config, 'config_sha256': digest(config)}
        return {'plan': self.plan, 'index': index, 'root': runtime.root,
            'ledger_identity': runtime.expected_ledger_identity, 'manifest': manifest,
            'config': config, 'permit': permit, 'contract': contract, 'expected_design': design}

    @staticmethod
    def rehash(origin):
        origin['permit']['cohort_contract_sha256'] = digest(origin['contract'])
        origin['config']['operator_permit_sha256'] = digest(origin['permit'])
        origin['manifest']['config_sha256'] = digest(origin['config'])

    def test_complete_v2_envelope_preserves_exact_ordered_child_designs(self):
        designs = reader.checked_design_envelope(self.envelope, self.plan)
        self.assertEqual(designs, tuple(row['execution_design'] for row in self.envelope['children']))
        self.assertEqual(len(designs), 6)
        self.assertEqual(sum(len(row['action_limits']['by_actor']) for row in designs), 96)

    def test_v1_envelope_and_v1_plan_are_rejected(self):
        old = deepcopy(self.envelope)
        old['protocol'] = 'cumulative-execution-design-envelope-v1'
        with self.assertRaises(FinancialError):
            reader.checked_design_envelope(old, self.plan)
        with self.assertRaises(FinancialError):
            reader.checked_design_envelope(self.envelope, v1_plan())

    def test_partial_or_reordered_child_envelope_is_rejected(self):
        for mutation in ('missing', 'reverse', 'foreign'):
            with self.subTest(mutation=mutation):
                changed = deepcopy(self.envelope)
                if mutation == 'missing':
                    changed['children'].pop()
                elif mutation == 'reverse':
                    changed['children'].reverse()
                else:
                    changed['children'][0]['cohort'] = 'foreign'
                with self.assertRaises(FinancialError):
                    reader.checked_design_envelope(changed, self.plan)

    def test_child_runtime_and_closure_policy_are_not_ignored(self):
        for field, value in (('runtime', {'final_acceptance_financial_mode': 'live'}),
                             ('financial_closure_policy', {'acceptance_authority': True}),
                             ('max_workers', True)):
            with self.subTest(field=field):
                changed = deepcopy(self.envelope)
                changed['children'][0]['execution_design'][field] = value
                with self.assertRaises(FinancialError):
                    reader.checked_design_envelope(changed, self.plan)

    def test_roster_and_profile_changes_are_not_equivalent(self):
        for field, value in (('terminal_roster', {}), ('profiles', {})):
            with self.subTest(field=field):
                changed = deepcopy(self.envelope)
                changed['children'][0]['execution_design'][field] = value
                with self.assertRaises(FinancialError):
                    reader.checked_design_envelope(changed, self.plan)

    def test_actual_v2_contract_has_exact_fixture_origin_for_every_child(self):
        for index in range(6):
            with self.subTest(index=index):
                reader._checked_financial_origin(**self.origin(index))

    def test_v4_origin_cannot_be_rehashed_into_v5(self):
        origin = self.origin()
        origin['config']['protocol'] = 'peer-financial-authority-v4'
        self.rehash(origin)
        with self.assertRaises(FinancialError):
            reader._checked_financial_origin(**origin)

    def test_unapproved_or_extended_permit_cannot_be_rehashed_into_original(self):
        for change in ('approval', 'extra'):
            with self.subTest(change=change):
                origin = self.origin()
                if change == 'approval':
                    origin['permit']['approval_ref'] = ''
                else:
                    origin['permit']['allow_unreviewed'] = True
                self.rehash(origin)
                with self.assertRaises(FinancialError):
                    reader._checked_financial_origin(**origin)

    def test_live_transport_or_live_mode_cannot_qualify_as_fixture(self):
        for change in ('transport', 'mode', 'qualification'):
            with self.subTest(change=change):
                origin = self.origin()
                if change == 'transport':
                    origin['contract']['transport_identity'] = LIVE_TRANSPORT
                elif change == 'mode':
                    origin['config']['mode'] = origin['permit']['mode'] = 'live'
                else:
                    origin['permit']['qualification'] = {'accepted': True}
                self.rehash(origin)
                with self.assertRaises(FinancialError):
                    reader._checked_financial_origin(**origin)

    def test_admitted_task_scope_and_path_cannot_change_after_rehash(self):
        for change in ('scope', 'units', 'journal', 'cohort'):
            with self.subTest(change=change):
                origin = self.origin()
                if change == 'scope':
                    origin['contract']['task_specs'][0]['allowed_paths'] = ['foreign.py']
                elif change == 'units':
                    origin['contract']['task_specs'][0]['max_reserved_units'] += 1
                elif change == 'journal':
                    origin['contract']['journal_root'] = '/foreign/provider-journals'
                else:
                    origin['contract']['cohort_id'] = 'foreign'
                self.rehash(origin)
                with self.assertRaises(FinancialError):
                    reader._checked_financial_origin(**origin)

    def test_extended_contract_or_missing_generation_limit_is_rejected(self):
        origin = self.origin()
        origin['contract']['allow_foreign'] = True
        self.rehash(origin)
        with self.assertRaises(FinancialError):
            reader._checked_financial_origin(**origin)
        origin = self.origin()
        origin['permit']['execution_design']['action_limits']['by_kind_generation']['repair'].pop('1')
        origin['permit']['execution_design_sha256'] = digest(origin['permit']['execution_design'])
        origin['config']['execution_design_sha256'] = origin['permit']['execution_design_sha256']
        self.rehash(origin)
        with self.assertRaises(FinancialError):
            reader._checked_financial_origin(**origin)

    def test_wallet_and_worker_limits_cannot_change_after_rehash(self):
        for field in ('expected_opening_usage', 'expected_global_cap', 'incremental_cap_micro_usd', 'max_workers'):
            with self.subTest(field=field):
                origin = self.origin()
                origin['config'][field] += 1
                self.rehash(origin)
                with self.assertRaises(FinancialError):
                    reader._checked_financial_origin(**origin)

    def test_cohort_facts_cannot_claim_final_acceptance(self):
        facts = reader.CohortOriginals('a' * 64, (), (), (), {}, 0,
            PrefixCommitment('a' * 64, 0, 'b' * 64, 0, 0, 0), (), (), ())
        self.assertFalse(facts.accepted)
        self.assertFalse(facts.live_qualification)
        self.assertEqual(facts.protocol, reader.PROTOCOL)
        with self.assertRaises(ValueError):
            replace(facts, accepted=True)

    def test_unknown_chain_or_old_plan_cannot_open_or_run_any_authority(self):
        with patch.object(GossipChildRuntime, '__init__', side_effect=AssertionError('No runtime')), \
                patch.object(StudyPlan, 'verify_sources', side_effect=AssertionError('No proof reading')):
            with self.assertRaises(FinancialError):
                reader.audit_cohort_originals(object(), object(), plan=v1_plan(), ledger_identity={},
                    repository=Path(__file__).resolve().parents[1], gate={}, ordered_test_classes=(),
                    expected_design_envelope={})
