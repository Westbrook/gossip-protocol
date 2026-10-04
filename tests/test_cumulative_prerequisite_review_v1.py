"""Original delivery mechanics, using explicitly synthetic reviewer artifacts."""
from dataclasses import asdict,replace
import hashlib
from pathlib import Path
import tempfile
import unittest

from gossip_harness import cumulative_prerequisite_review_v1 as review
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import candidate_checkpoint_chain_v1 as checkpoint
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead


class CumulativePrerequisiteReviewV1Tests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='gossip-original-review-');self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name).resolve()
        self.head=ExternalHead.create(self.root/'head',journal_roots=(self.root/'raw',self.root/'delta'))
        self.addCleanup(self.head.close)
        self.chain=checkpoint.CheckpointChain.create(self.root/'raw',self.root/'delta',context={'fixture':'review-mechanics'},authority=self.head)
        self.addCleanup(self.chain.close)
        self.owner=review.OriginalReviewAuthority(self.chain,self.chain.commitment)
        self.source=consumer.Revision('a'*40,'b'*40,'c'*64)
        self.args={'purpose':'candidate_source_promotion','role':'integration','source':self.source,
            'context':{'synthetic':'provenance fixture only','full_source_review_supplied':False}}
        self.request=self.owner.stage(**self.args)

    def enroll(self,request=None,*,decisions=None,report_change=None,delivery_first=False):
        request=request or self.request
        body=review._read(request.raw)
        report={'protocol':review.PROTOCOL,'kind':'independent-review-report','reviewer_id':'synthetic-independent',
            'purpose':body['purpose'],'role':body['role'],'request_sha256':request.sha256,'source':body['source'],
            'decisions':[{'duty':duty,'decision':'accept' if decisions is None else decisions[i],
                'rationale':'Synthetic linkage fixture, not semantic approval','inspected_references':['fixture:'+duty]}
                for i,duty in enumerate(body['duties'])],'limitations':['Not an actual independent review']}
        if report_change:report_change(report)
        raw=review.encoded(report);pin=hashlib.sha256(raw).hexdigest()
        delivery={'protocol':review.PROTOCOL,'kind':'independent-review-delivery','reviewer_id':report['reviewer_id'],
            'purpose':report['purpose'],'role':report['role'],'request_sha256':request.sha256,
            'report_name':'report.json','report_sha256':pin,'delivery_reference':'explicit synthetic host fixture'}
        delivery_raw=review.encoded(delivery)
        rows=[('report.json',raw),('delivery.json',delivery_raw)]
        if delivery_first:rows.reverse()
        for name,value in rows:self.chain.retain(name,value)
        enrolled=review.ReviewEnrollment(report['reviewer_id'],body['role'],body['purpose'],'report.json',pin,
            'delivery.json',hashlib.sha256(delivery_raw).hexdigest())
        self.owner=review.OriginalReviewAuthority(self.chain,self.chain.commitment,enrollments=(enrolled,))
        return self.owner

    def test_actual_chain_original_request_report_delivery_authenticates_linkage(self):
        owner=self.enroll();before=self.chain.commitment
        original=owner.original(**self.args);result=owner.authenticate(original)
        self.assertEqual(result.decision,'accept')
        self.assertEqual(result.request_sha256,self.request.sha256)
        self.assertLess(self.request.position,result.report_position)
        self.assertLess(result.report_position,result.provenance_position)
        self.assertEqual(before,self.chain.commitment)
        self.assertEqual(result.checkpoint,before)

    def test_missing_delivery_stays_unavailable(self):
        with self.assertRaises(consumer.AuthorityUnavailable):self.owner.authenticate(self.request)

    def test_rejected_duty_is_not_hidden_by_accepted_siblings(self):
        result=self.enroll(decisions=['accept','reject','accept','unresolved','accept']).authenticate(self.request)
        self.assertEqual(result.decision,'reject')

    def test_unresolved_duty_cannot_be_approved(self):
        result=self.enroll(decisions=['accept']*4+['unresolved']).authenticate(self.request)
        self.assertEqual(result.decision,'unresolved')

    def test_control_review_cannot_substitute_for_final_candidate_review(self):
        owner=self.enroll(report_change=lambda row:row.update(purpose='control_review'))
        with self.assertRaises(consumer.AuthorityError):owner.authenticate(self.request)

    def test_another_final_source_cannot_reuse_same_review(self):
        self.enroll()
        with self.assertRaises(consumer.AuthorityUnavailable):
            self.owner.original(**(self.args|{'source':replace(self.source,source_sha256='d'*64)}))

    def test_changed_final_context_cannot_reuse_original_review(self):
        self.enroll()
        with self.assertRaises(consumer.AuthorityUnavailable):
            self.owner.original(**(self.args|{'context':{'substituted':True}}))

    def test_delivery_must_follow_report_in_same_original_chain(self):
        owner=self.enroll(delivery_first=True)
        with self.assertRaisesRegex(consumer.AuthorityError,'chronology'):owner.authenticate(self.request)

    def test_new_suffix_revokes_fixed_review_authority(self):
        owner=self.enroll();self.chain.retain('foreign-suffix.json',b'{}')
        with self.assertRaises(consumer.AuthorityUnavailable):owner.authenticate(self.request)

    def test_direct_retained_request_cannot_bypass_revision_context_and_name(self):
        for field,value in (('source',{}),('context',{})):
            body=self.owner.request_for(**self.args);body[field]=value
            raw=review.encoded(body);pin=hashlib.sha256(raw).hexdigest();name='direct-'+field+'.json'
            self.chain.retain(name,raw)
            owner=review.OriginalReviewAuthority(self.chain,self.chain.commitment)
            supplied=review.ReviewRequest(name,pin,self.chain.position(name),raw)
            with self.assertRaises(consumer.AuthorityError):owner.authenticate(supplied)

    def test_exact_valid_body_requires_source_derived_request_name(self):
        self.chain.retain('arbitrary-name.json',self.request.raw)
        owner=review.OriginalReviewAuthority(self.chain,self.chain.commitment)
        with self.assertRaisesRegex(consumer.AuthorityError,'schema'):
            owner.authenticate(replace(self.request,name='arbitrary-name.json',position=self.chain.position('arbitrary-name.json')))

    def test_structure_bound_rejects_deep_review_target_before_append(self):
        value={};cursor=value
        for _ in range(100):cursor['next']={};cursor=cursor['next']
        before=self.chain.commitment
        with self.assertRaises(consumer.AuthorityError):self.owner.stage(**(self.args|{'context':value}))
        self.assertEqual(before,self.chain.commitment)

    def test_duplicate_or_reordered_duties_are_invalid(self):
        owner=self.enroll(report_change=lambda row:row['decisions'].reverse())
        with self.assertRaises(consumer.AuthorityError):owner.authenticate(self.request)

    def test_complete_stable_reader_dependency_is_bound(self):
        self.assertIn('gossip_harness/candidate_http_journal_v3.py',self.owner.sources)
        value=review._read(self.request.raw)
        self.assertEqual(value['sources'],self.owner.sources)
        self.assertEqual(value['source'],asdict(self.source))
