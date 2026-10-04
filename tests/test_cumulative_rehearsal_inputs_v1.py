"""Typed-input, protected export and exact mapping controls; no study execution."""
from copy import deepcopy
from dataclasses import asdict, replace
from pathlib import Path
import tempfile
import unittest

from gossip_harness import cumulative_rehearsal_codec_v1 as codec
from gossip_harness import cumulative_rehearsal_export_v1 as export
from gossip_harness import cumulative_rehearsal_matching_v1 as matching
from gossip_harness import cumulative_rehearsal_validator_v2 as reader
from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.cumulative_study_controller_v2 import digest
from gossip_harness.peer_financial_terminal_v1 import FinancialError
from tests.test_cumulative_terminal_originals_v2 import synthetic_plan


def plans():
    base = synthetic_plan()
    profiles = {'mini':{'unit_fixture':'mini'},'strong':{'unit_fixture':'strong'}}
    fixture_roots = {key:'/unexecuted-fixture/'+key for key in ('raw','delta','head')}
    live_roots = {key:'/unexecuted-live/'+key for key in ('raw','delta','head')}
    pairs = tuple((row.id,'live.'+row.id) for row in base.cohort.trajectories)
    mapping = matching.mapping_for('fixture-study','live-study',pairs,fixture_roots,live_roots)
    def make(live):
        runtime = {**base.runtime,'final_acceptance_protocol':'cumulative-final-acceptance-v3',
            'study_successor_protocol':'cumulative-study-successor-v3',
            'final_acceptance_financial_mode':'live' if live else 'fixture',
            'final_acceptance_roots':live_roots if live else fixture_roots,
            'rehearsal_instance_mapping_sha256':digest(mapping)}
        resources = {name:getattr(base,name) for name in ('horizon_seconds','source_generations','partition_seconds','executor_slots')}
        resources['runtime'] = runtime
        cohort = replace(base.cohort,cohort_id='live-study' if live else 'fixture-study',
            trajectories=tuple(replace(row,id=pair[1]) if live else row for row,pair in zip(base.cohort.trajectories,pairs)),
            resource_contract_sha256=digest(resources),model_profiles_sha256=digest(profiles))
        return replace(base,cohort=cohort,runtime=runtime)
    return mapping,make(False),make(True),profiles


def envelope(plan,profiles):
    return {'protocol':reader.DESIGN_PROTOCOL,'execution_contract_sha256':plan.sha256,
        'terminal_roster':plan.roster.record(),'sources':plan.source_pins,'runtime':plan.runtime,'profiles':profiles,
        'financial_closure_policy':plan.financial_closure_policy,'children':[
            {'cohort':child.cohort,'trajectory':child.trajectory,'execution_design':{
                'runtime':plan.runtime,'profiles':profiles,'terminal_roster':plan.roster.record(),
                'financial_closure_policy':plan.financial_closure_policy,'max_workers':plan.executor_slots,
                'action_limits':{'total':512,'by_kind':{'build':512,'repair':512,'review':512},
                    'by_kind_generation':{kind:{'0':512,'1':512} for kind in ('build','repair','review')},
                    'by_actor':dict.fromkeys(child.actors,12)}}} for child in plan.roster.children]}


class CumulativeRehearsalInputsV1Tests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);self.root=Path(temp.name).resolve()

    def test_complete_authored_v2_plan_roundtrips_without_code_or_verdict(self):
        value=synthetic_plan()
        self.assertEqual(codec.unpack(codec.pack(value)),value)
        self.assertEqual(codec.unpack(codec.pack({'raw':b'\x00\xff','surrogate':'\ud800','path':self.root})),
                         {'raw':b'\x00\xff','surrogate':'\ud800','path':self.root})

    def test_unknown_dataclass_module_is_not_imported(self):
        record={'protocol':codec.PROTOCOL,'input':{'tag':'dataclass','type':'foreign.exec.Run','value':{}}}
        with self.assertRaises(FinancialError):codec.unpack(record)

    def test_foreign_objects_and_relative_paths_are_rejected(self):
        for value in (object(),Path('relative'),float('nan')):
            with self.subTest(value=type(value)),self.assertRaises(FinancialError):codec.pack(value)

    def test_unknown_fields_and_noncanonical_bytes_are_rejected(self):
        record=codec.pack(('value',));record['extra']=True
        with self.assertRaises(FinancialError):codec.unpack(record)
        raw=b'{ "protocol":"anything" }';path=self.root/'bad.json';path.write_bytes(raw)
        with self.assertRaises(FinancialError):codec.read({'path':str(path),'sha256':export.sha(raw)})

    def test_protected_writer_is_exclusive_and_returns_exact_byte_pin(self):
        writer=export.InputWriter(self.root/'package')
        ref=writer.put({'bytes':b'original'},typed=True)
        self.assertEqual(codec.read(ref),{'bytes':b'original'})
        self.assertEqual(Path(ref['path']).stat().st_mode & 0o777,0o600)
        with self.assertRaises(FinancialError):export.InputWriter(self.root/'package')

    def test_actual_chain_descriptor_requires_original_genesis_context(self):
        raw,delta,anchor=(self.root/name for name in ('raw','delta','anchor'))
        head=ExternalHead.create(anchor,journal_roots=(raw,delta));self.addCleanup(head.close)
        original=chain.CheckpointChain.create(raw,delta,context={'actual':'unit'},authority=head);self.addCleanup(original.close)
        original.retain('original.json',b'{}')
        descriptor=export.proof_descriptor(original,{'actual':'unit'})
        self.assertEqual(descriptor['expected'],asdict(original.commitment))
        with self.assertRaisesRegex(FinancialError,'context'):
            export.proof_descriptor(original,{'actual':'different'})

    def test_mapping_allows_only_predeclared_fixture_live_instances(self):
        mapping,fixture,live,profiles=plans()
        a,b=matching.validate_mapping(mapping,fixture,live,envelope(fixture,profiles),envelope(live,profiles))
        self.assertEqual((len(a),len(b)),(6,6))
        self.assertNotEqual(fixture.sha256,live.sha256)
        self.assertFalse(mapping['candidate_acceptance_transfer'])

    def test_mapping_rejects_unlisted_escape_hatch(self):
        mapping,fixture,live,profiles=plans();mapping['ignore_fields']=['sources']
        with self.assertRaises(FinancialError):
            matching.validate_mapping(mapping,fixture,live,envelope(fixture,profiles),envelope(live,profiles))

    def test_recomputed_runtime_or_seed_change_is_not_instance_equivalence(self):
        mapping,fixture,live,profiles=plans()
        for change in ('runtime','seed'):
            with self.subTest(change=change),self.assertRaises((FinancialError,ValueError)):
                if change=='runtime':
                    runtime={**live.runtime,'public_timeout_seconds':999}
                    limits={name:getattr(live,name) for name in ('horizon_seconds','source_generations','partition_seconds','executor_slots')}
                    changed=replace(live,runtime=runtime,cohort=replace(live.cohort,resource_contract_sha256=digest({**limits,'runtime':runtime})))
                else:
                    trajectories=(replace(live.cohort.trajectories[0],block_seed_sha256='f'*64),*live.cohort.trajectories[1:])
                    changed=replace(live,cohort=replace(live.cohort,trajectories=trajectories))
                matching.validate_mapping(mapping,fixture,changed,envelope(fixture,profiles),envelope(changed,profiles))

    def test_live_action_allowance_cannot_expand_with_actor_renaming(self):
        mapping,fixture,live,profiles=plans();changed=deepcopy(envelope(live,profiles))
        actor=live.roster.children[0].actors[0]
        changed['children'][0]['execution_design']['action_limits']['by_actor'][actor]+=1
        with self.assertRaises(FinancialError):
            matching.validate_mapping(mapping,fixture,live,envelope(fixture,profiles),changed)

    def test_wallet_instance_change_preserves_the_whole_effective_child_budget(self):
        fixture=dict(expected_opening_usage=1000,expected_global_cap=10000,incremental_cap_micro_usd=5000,max_workers=4)
        live={**fixture,'expected_opening_usage':3000,'expected_global_cap':12000}
        mapping={'protocol':matching.WALLET_POLICY,'fixture':fixture,'live':live}
        self.assertEqual(matching.validate_wallet_mapping(mapping,fixture,live),mapping)

    def test_wallet_mapping_cannot_reduce_or_expand_the_matched_child_cap(self):
        fixture=dict(expected_opening_usage=1000,expected_global_cap=10000,incremental_cap_micro_usd=5000,max_workers=4)
        for cap in (4000,6000):
            live={**fixture,'incremental_cap_micro_usd':cap}
            with self.subTest(cap=cap),self.assertRaises(FinancialError):
                matching.validate_wallet_mapping({'protocol':matching.WALLET_POLICY,'fixture':fixture,'live':live},fixture,live)

    def test_wallet_mapping_cannot_hide_smaller_global_remaining_room(self):
        fixture=dict(expected_opening_usage=1000,expected_global_cap=10000,incremental_cap_micro_usd=5000,max_workers=4)
        live={**fixture,'expected_opening_usage':9000}
        with self.assertRaises(FinancialError):
            matching.validate_wallet_mapping({'protocol':matching.WALLET_POLICY,'fixture':fixture,'live':live},fixture,live)

    def test_wallet_mapping_requires_exact_actual_approved_permit_values(self):
        fixture=dict(expected_opening_usage=1000,expected_global_cap=10000,incremental_cap_micro_usd=5000,max_workers=4)
        mapping={'protocol':matching.WALLET_POLICY,'fixture':fixture,'live':fixture}
        with self.assertRaises(FinancialError):
            matching.validate_wallet_mapping(mapping,fixture,{**fixture,'max_workers':3})


    def test_actual_defining_http_fixture_type_roundtrips(self):
        from gossip_harness.candidate_http_cases_core_v1 import Fixture
        value=(Fixture('directory','directory'),Fixture('directory/input.json','file',b'{}'))
        self.assertEqual(codec.unpack(codec.pack(value)),value)


    def test_actual_http_literal_case_and_nested_request_roundtrip(self):
        from gossip_harness.candidate_http_cases_actions_v1 import definitions
        values=definitions()
        self.assertEqual(len(values),58)
        for value in values:
            with self.subTest(row_id=value.row_id):
                self.assertEqual(codec.unpack(codec.pack(value)),value)


    def test_actual_nested_batch_product_profile_roundtrips_through_protected_export(self):
        from gossip_harness import candidate_product_process_core_v1 as product
        from gossip_harness.candidate_product_process_execution_v1 import HttpProductProfile, BATCH_PROTOCOL
        from gossip_harness.candidate_source_capture_policy_v1 import BatchCapturePolicy
        profiles=tuple(HttpProductProfile(case,capture_policy=BatchCapturePolicy()) for case in product.definitions())
        self.assertEqual(len(profiles),8)
        value={'observations':tuple({'profile':profile} for profile in profiles)}
        writer=export.InputWriter(self.root/'batch-profile-package')
        restored=codec.read(writer.put(value,typed=True))
        self.assertEqual(restored,value)
        for observation,profile in zip(restored['observations'],profiles):
            actual=observation['profile']
            self.assertIs(type(actual),HttpProductProfile)
            self.assertIs(type(actual.capture_policy),BatchCapturePolicy)
            self.assertEqual(actual.capture_policy.record(),profile.capture_policy.record())
            self.assertEqual(actual.execution_protocol,BATCH_PROTOCOL)
            self.assertEqual(actual.ordered_case_ids,profile.ordered_case_ids)

    def test_nested_batch_policy_cannot_admit_an_unknown_dataclass(self):
        from dataclasses import dataclass
        from gossip_harness.candidate_source_capture_policy_v1 import BatchCapturePolicy
        record=codec.pack({'capture_policy':BatchCapturePolicy()})
        record['input']['value']['capture_policy']['type']='gossip_harness.candidate_source_capture_policy_v1.UnknownCapturePolicy'
        with self.assertRaisesRegex(FinancialError,'closed source contract'):codec.unpack(record)
        @dataclass(frozen=True)
        class UnregisteredCapturePolicy:
            timeout_seconds: int = 60
            cleanup_reap_seconds: int = 5
        with self.assertRaisesRegex(FinancialError,'Unknown rehearsal input type'):
            codec.pack({'capture_policy':UnregisteredCapturePolicy()})


    def test_typed_writer_refuses_reader_structural_overflow_before_output(self):
        value='leaf'
        for _ in range(40): value=[value]
        writer=export.InputWriter(self.root/'deep-package')
        with self.assertRaises(ValueError): writer.put(value,typed=True)
        self.assertEqual(list(writer.root.iterdir()),[])
