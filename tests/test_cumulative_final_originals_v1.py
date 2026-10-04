"""Cold original refusal and authenticated prefix controls, never full acceptance."""
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import cumulative_final_originals_v1 as reader
from gossip_harness import cumulative_rehearsal_export_v1 as exporter
from gossip_harness import cumulative_rehearsal_capsule_v2 as capsule
from gossip_harness import cumulative_rehearsal_codec_v1 as codec
from gossip_harness.peer_financial_terminal_v1 import FinancialError
from tests.test_cumulative_terminal_originals_v2 import synthetic_plan
from dataclasses import replace
from gossip_harness.cumulative_study_controller_v2 import digest


class CumulativeFinalOriginalsV1Tests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);self.root=Path(temp.name).resolve()

    def original(self,first='config.json'):
        raw,delta,anchor=(self.root/name for name in ('raw','delta','head'))
        head=ExternalHead.create(anchor,journal_roots=(raw,delta))
        original=chain.CheckpointChain.create(raw,delta,context={'unit':'original'},authority=head)
        self.addCleanup(head.close);self.addCleanup(original.close)
        original.retain(first,b'{"original":true}')
        first_prefix=original.commitment
        if first!='config.json':original.retain('config.json',b'{"original":true}')
        original.retain('terminal.json',b'{}')
        owner=SimpleNamespace(delta_root=delta,checkpoint=lambda:original.validate_boundary().commitment,
                              read_authenticated=original.read)
        return original,head,owner,first_prefix

    def test_config_prefix_is_actual_first_delta_not_caller_claim(self):
        original,_,owner,before=self.original()
        self.assertEqual(reader.config_prefix(owner),before)
        self.assertNotEqual(before,original.commitment)

    def test_late_config_cannot_be_a_fresh_before_dispatch_binding(self):
        _,_,owner,_=self.original('other.json')
        with self.assertRaisesRegex(FinancialError,'first committed'):
            reader.config_prefix(owner)

    def test_tampered_delta_is_rejected_before_prefix_reconstruction(self):
        _,_,owner,_=self.original()
        (owner.delta_root/'delta-00000001.json').write_bytes(b'{}')
        with self.assertRaises(chain.ChainError):reader.config_prefix(owner)

    def test_empty_semantic_scope_keeps_fabricated_acceptance_unavailable(self):
        original,head,_,_=self.original()
        descriptor=exporter.proof_descriptor(original,{'unit':'original'})
        original.close();head.close()
        writer=exporter.InputWriter(self.root/'inputs')
        with ExitStack() as stack:
            pool=reader.ProofPool({'actual':descriptor},stack)
            value={'chain':'actual','enrollments':writer.put((),typed=True),'submissions':[]}
            with self.assertRaisesRegex(FinancialError,'six typed complete scope'):
                reader._restore_scope(pool,value,Path(__file__).resolve().parents[1])
            self.assertEqual(pool.open('actual').commitment,chain.PrefixCommitment(**descriptor['expected']))

    def test_fabricated_final_assessment_has_no_original_authority(self):
        base=synthetic_plan();runtime={**base.runtime,'final_acceptance_financial_mode':'fixture'}
        limits={name:getattr(base,name) for name in ('horizon_seconds','source_generations','partition_seconds','executor_slots')}
        plan=replace(base,runtime=runtime,cohort=replace(base.cohort,resource_contract_sha256=digest({**limits,'runtime':runtime})))
        with self.assertRaises(FinancialError):
            reader.audit_final_originals({'accepted':True,'completed_and_accepted':True},plan=plan)

    def test_external_head_substitution_is_not_recovery(self):
        original,head,_,_=self.original()
        descriptor=exporter.proof_descriptor(original,{'unit':'original'})
        original.close();head.close();descriptor['expected']['head_sha256']='f'*64
        with ExitStack() as stack,self.assertRaises(ValueError):
            reader.ProofPool({'actual':descriptor},stack).open('actual')

    def test_bound_capsule_is_canonical_and_independently_pinned(self):
        raw=codec.encoded({'protocol':capsule.PROTOCOL});path=self.root/'capsule.json';path.write_bytes(raw)
        reference={'path':str(path),'sha256':exporter.sha(raw)}
        self.assertEqual(capsule.bound(reference),{'protocol':capsule.PROTOCOL})
        path.write_bytes(raw+b'\n')
        with self.assertRaises(FinancialError):capsule.bound(reference)

    def test_concurrent_auditor_is_rejected_without_wait_or_original_read(self):
        self.assertTrue(capsule._LOCK.acquire(False))
        try:
            with patch.object(capsule,'bound') as read,self.assertRaisesRegex(FinancialError,'active'):
                capsule.audit_closed_capsule({},execution_design={},sources={},approved_child={})
            read.assert_not_called()
        finally:capsule._LOCK.release()

    def test_rejected_capsule_releases_auditor_lock(self):
        raw=codec.encoded({'accepted':True});path=self.root/'capsule.json';path.write_bytes(raw)
        with self.assertRaises(FinancialError):
            capsule.audit_closed_capsule({'path':str(path),'sha256':exporter.sha(raw)},execution_design={},sources={},approved_child={})
        self.assertTrue(capsule._LOCK.acquire(False));capsule._LOCK.release()


    def test_cold_restore_requires_prospective_final_source_closure(self):
        with patch.object(reader.final,'implementation_sources',return_value={'missing-reader.py':'a'*64}), \
                patch.object(reader.final,'terminal_implementation_sources',return_value={}):
            with self.assertRaisesRegex(FinancialError,'prospectively'):
                reader._prospective_sources(SimpleNamespace(source_pins={}))

    def test_prerequisite_publication_must_precede_candidate_admission(self):
        # Method-isolated chronology, not fabricated complete scope acceptance.
        gate=SimpleNamespace(binding=SimpleNamespace(subject=SimpleNamespace(trajectory_id='t')))
        submission=SimpleNamespace(subject='subject',catalog=SimpleNamespace(inventory='inventory'),declaration='declaration',scope='scope')
        owner=SimpleNamespace(submissions={'t':submission},scope_snapshot=SimpleNamespace(registration=lambda _:SimpleNamespace(scope_sha256='pin')),
            records=SimpleNamespace(read=lambda _: {'protocol':reader.final.PROTOCOL,'gate':{},'qualification_requests':[]}),
            chain=SimpleNamespace(position=lambda name: 3 if name == reader.Records.name('final.original-authorities') else 5))
        with patch.object(reader.compiler,'compile_design',return_value=SimpleNamespace(registry=True,blockers=())), \
                patch.object(reader.consumer,'qualification_requests',return_value=()), \
                patch.object(reader.registry,'fingerprint',return_value='gate'), \
                patch.object(reader,'asdict',return_value={}):
            with self.assertRaisesRegex(FinancialError,'chronology'):
                reader._prerequisite_prefix(owner,gate,4)
