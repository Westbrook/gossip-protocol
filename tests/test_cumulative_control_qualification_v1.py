"""Real local Git/probes; synthetic review delivery tests linkage, not approval."""
from dataclasses import asdict, replace
import hashlib
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness import candidate_checkpoint_chain_v1 as checkpoint
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import cumulative_control_qualification_v1 as control
from gossip_harness import cumulative_prerequisite_review_v1 as review
from gossip_harness.gitstore import GitStore


class CumulativeControlQualificationV1Tests(unittest.TestCase):
    def setUp(self):
        artifacts=ArtifactDirectory('control-'+self._testMethodName,retain_success=True)
        self.addCleanup(artifacts.close)
        self.root=artifacts.root.resolve()
        self.chain=self.new_chain('control')
        self.review_chain=self.new_chain('review')
        self.args={'root':self.root/'fixture','product_lineages_sha256':'a'*64,
                   'reviewer_ids':('package-reviewer','integration-reviewer')}
        self.owner=control.ControlQualification(self.chain,self.chain.commitment,**self.args)

    def new_chain(self,name):
        root=self.root/name;root.mkdir()
        head=ExternalHead.create(root/'head',journal_roots=(root/'raw',root/'delta'))
        self.addCleanup(head.close)
        chain=checkpoint.CheckpointChain.create(root/'raw',root/'delta',context={'fixture':'control-mechanics','name':name},authority=head)
        self.addCleanup(chain.close)
        return chain

    def enroll(self,requests,*,decisions=('accept','accept'),identities=None,purpose=None):
        enrollments=[]
        for index,request in enumerate(requests):
            body=review._read(request.raw)
            identity=(identities or self.args['reviewer_ids'])[index]
            report={'protocol':review.PROTOCOL,'kind':'independent-review-report','reviewer_id':identity,
                'purpose':purpose or body['purpose'],'role':body['role'],'request_sha256':request.sha256,
                'source':body['source'],'decisions':[{'duty':duty,'decision':decisions[index],
                    'rationale':'Synthetic delivery fixture; no independent semantic approval claimed',
                    'inspected_references':['original-control:'+request.sha256]} for duty in body['duties']],
                'limitations':['Fixture linkage only; not an independent review']}
            raw=review.encoded(report);pin=hashlib.sha256(raw).hexdigest()
            name=f'control-report-{index}.json';delivery_name=f'control-delivery-{index}.json'
            delivery={'protocol':review.PROTOCOL,'kind':'independent-review-delivery','reviewer_id':identity,
                'purpose':report['purpose'],'role':body['role'],'request_sha256':request.sha256,
                'report_name':name,'report_sha256':pin,'delivery_reference':'Synthetic original host delivery'}
            delivery_raw=review.encoded(delivery)
            self.review_chain.retain(name,raw);self.review_chain.retain(delivery_name,delivery_raw)
            enrollments.append(review.ReviewEnrollment(identity,body['role'],body['purpose'],name,pin,
                delivery_name,hashlib.sha256(delivery_raw).hexdigest()))
        return review.OriginalReviewAuthority(self.review_chain,self.review_chain.commitment,enrollments=tuple(enrollments))

    def executed(self):
        definition=self.owner.prepare()
        probes=self.owner.execute_once()
        return definition,probes

    def reviewed(self,**kwargs):
        self.executed()
        authority=review.OriginalReviewAuthority(self.review_chain,self.review_chain.commitment)
        requests=self.owner.stage_reviews(authority)
        return self.enroll(requests,**kwargs)

    def test_actual_five_revisions_clean_merge_failure_repair_and_original_reviews(self):
        definition,probes=self.executed()
        self.assertEqual([row.exit_code for row in probes],[0,0,0,1,0])
        self.assertEqual(probes[3].stdout,b'{"error":"ValueError"}\n')
        self.assertEqual(probes[4].stdout,b'{"value":12}\n')
        self.assertTrue(all(row.matches_expected for row in probes))
        self.assertEqual(len({getattr(definition.recipe,label).commit_oid for label in control.LABELS}),5)
        store=GitStore(self.args['root']/'fixture.git')
        self.assertEqual(store._git('rev-list','--parents','-n','1',definition.recipe.merged.commit_oid).split()[1:],
            [definition.recipe.left.commit_oid,definition.recipe.right.commit_oid])
        authority=review.OriginalReviewAuthority(self.review_chain,self.review_chain.commitment)
        requests=self.owner.stage_reviews(authority)
        authority=self.enroll(requests)
        result=self.owner.complete(authority)
        self.assertEqual(result.terminal_status,'completed')
        self.assertEqual([row.status for row in result.outcomes],['passed']*5)
        self.assertEqual([row.status for row in result.facets],['passed']*4)
        before=self.chain.commitment
        self.assertEqual(self.owner.verify_current(authority),result)
        self.assertEqual(self.chain.commitment,before)
        reopened=control.ControlQualification(self.chain,before,**self.args)
        with patch.object(reopened,'_run_probe',side_effect=AssertionError('must not replay')):
            self.assertEqual(reopened.execute_once(),result.probes)
            self.assertEqual(reopened.verify_current(authority),result)
        self.assertEqual(result.product_lineages_sha256,'a'*64)
        self.assertNotEqual(result.receipt_sha256,result.verifier_receipt_sha256)

    def test_candidate_purpose_cannot_be_labeled_control_qualification(self):
        for purpose in ('independent_acceptance','repeatability','public_release'):
            with self.subTest(purpose=purpose),self.assertRaises(consumer.AuthorityError):
                control.ControlQualification(self.chain,self.chain.commitment,**self.args,purpose=purpose)

    def test_reviewers_must_be_distinct_before_any_git_or_probe(self):
        with self.assertRaisesRegex(consumer.AuthorityError,'distinct'):
            control.ControlQualification(self.chain,self.chain.commitment,
                **(self.args|{'reviewer_ids':('same','same')}))
        self.assertFalse(self.args['root'].exists())

    def test_product_lineage_cannot_be_attached_after_original_config(self):
        with self.assertRaisesRegex(consumer.AuthorityError,'configuration'):
            control.ControlQualification(self.chain,self.chain.commitment,
                **(self.args|{'product_lineages_sha256':'b'*64}))

    def test_foreign_fixture_is_not_adopted(self):
        self.args['root'].mkdir()
        with self.assertRaisesRegex(consumer.AuthorityError,'Foreign'):
            self.owner.prepare()
        self.assertFalse(self.chain.has('control-prepare-intent.json'))

    def test_interrupted_preparation_preserves_unknown_root_without_retry(self):
        self.owner._put('control-prepare-intent.json',{'protocol':control.PROTOCOL,'config_sha256':control.digest(self.owner.config)})
        self.args['root'].mkdir();(self.args['root']/'partial').write_text('preserve me')
        with self.assertRaisesRegex(consumer.AuthorityUnavailable,'unknown'):
            self.owner.prepare()
        self.assertEqual((self.args['root']/'partial').read_text(),'preserve me')

    def test_interrupted_probe_intent_never_dispatches_any_successor(self):
        definition=self.owner.prepare()
        self.owner._put('control-execution-intent.json',{'protocol':control.PROTOCOL,'definition_sha256':definition.definition_sha256})
        with patch.object(self.owner,'_run_probe',side_effect=AssertionError('must not replay')):
            with self.assertRaisesRegex(consumer.AuthorityUnavailable,'no replay'):
                self.owner.execute_once()
        self.assertFalse((self.args['root']/'probe-base').exists())

    def test_missing_independent_review_stays_unavailable_after_actual_probes(self):
        self.executed()
        authority=review.OriginalReviewAuthority(self.review_chain,self.review_chain.commitment)
        self.owner.stage_reviews(authority)
        result=self.owner.complete(authority)
        self.assertEqual(result.terminal_status,'infrastructure_error')
        self.assertEqual([row.status for row in result.facets[-2:]],['infrastructure_error']*2)
        self.assertTrue(all(row.status=='passed' for row in result.outcomes))
        self.assertEqual(result.reviews,())

    def test_unresolved_review_cannot_be_relabelled_accepted(self):
        authority=self.reviewed(decisions=('accept','unresolved'))
        result=self.owner.complete(authority)
        self.assertEqual(result.facets[-1].status,'infrastructure_error')
        self.assertEqual(result.reviews[-1].decision,'unresolved')
        self.assertEqual(result.terminal_status,'infrastructure_error')

    def test_rejected_review_preserves_failed_facet_and_real_probe_passes(self):
        authority=self.reviewed(decisions=('reject','accept'))
        result=self.owner.complete(authority)
        self.assertEqual(result.facets[2].status,'failed')
        self.assertEqual(result.facets[3].status,'passed')
        self.assertTrue(all(row.status=='passed' for row in result.outcomes))

    def test_other_reviewer_cannot_replace_prospective_identity(self):
        authority=self.reviewed(identities=('foreign-package','integration-reviewer'))
        with self.assertRaisesRegex(consumer.AuthorityError,'reviewer'):
            self.owner.complete(authority)

    def test_wrong_review_purpose_cannot_supply_control(self):
        authority=self.reviewed(purpose='candidate_source_promotion')
        with self.assertRaises(consumer.AuthorityError):self.owner.complete(authority)

    def test_advanced_review_prefix_revokes_existing_original_authority(self):
        authority=self.reviewed()
        self.review_chain.retain('later.json',b'{}')
        with self.assertRaises(consumer.AuthorityUnavailable):self.owner.complete(authority)

    def test_changed_raw_probe_is_rejected_before_review_or_acceptance(self):
        self.executed()
        path=self.chain.raw_root/'control-merged-stdout.bin'
        path.write_bytes(b'{"value":12}\n')
        with self.assertRaises(consumer.AuthorityUnavailable):self.owner.probes()

    def test_unknown_unanchored_suffix_cannot_become_current_control(self):
        (self.chain.raw_root/'foreign.json').write_bytes(b'{}')
        with self.assertRaises(consumer.AuthorityUnavailable):self.owner.prepare()

    def test_actual_git_parent_change_is_not_a_claimed_ancestry_pass(self):
        definition=self.owner.prepare()
        store=GitStore(self.args['root']/'fixture.git')
        store._git('update-ref','refs/heads/accepted',definition.recipe.left.commit_oid,definition.recipe.base.commit_oid)
        with self.assertRaisesRegex(consumer.AuthorityError,'baseline'):
            self.owner.definition()

    def test_runtime_change_revokes_original_config_before_effect(self):
        self.owner.runtime=self.owner.runtime|{'python_version':'changed'}
        with self.assertRaisesRegex(consumer.AuthorityError,'runtime'):
            self.owner.prepare()
        self.assertFalse(self.args['root'].exists())

    def test_output_bound_kills_owned_probe_and_retains_finite_original_bytes(self):
        result=self.owner._run_probe([sys.executable,'-I','-S','-c','import os;os.write(1,b"x"*200000)'],self.root)
        self.assertTrue(result['overflow'])
        self.assertFalse(result['capture_complete'])
        self.assertEqual(len(result['stdout']),control.OUTPUT_LIMIT)
        self.assertGreater(result['observed_bytes']['stdout'],control.OUTPUT_LIMIT)
        self.assertIsNotNone(result['exit_code'])

    def test_deadline_kills_owned_probe_without_journal_dependency(self):
        with patch.object(control,'TIMEOUT_SECONDS',.1):
            result=self.owner._run_probe([sys.executable,'-I','-S','-c','import time;time.sleep(5)'],self.root)
        self.assertTrue(result['timed_out'])
        self.assertFalse(result['capture_complete'])
        self.assertIsNotNone(result['exit_code'])

    def test_launch_error_is_known_infrastructure_not_probe_failure(self):
        result=self.owner._run_probe(['/no/such/control-executable'],self.root)
        self.assertEqual(result['launch_error'],'FileNotFoundError')
        self.assertFalse(result['capture_complete'])
        self.assertIsNone(result['exit_code'])

    def test_control_and_review_journals_cannot_share_mutable_authority(self):
        authority=review.OriginalReviewAuthority(self.chain,self.chain.commitment)
        with self.assertRaisesRegex(consumer.AuthorityError,'separately'):
            self.owner.stage_reviews(authority)

    def test_definition_identity_exposes_exact_closed_contract_not_success(self):
        definition=self.owner.prepare()
        self.assertEqual(definition.ordered_case_ids,control.CASE_IDS)
        self.assertEqual(definition.facet_selectors,control.FACET_SELECTORS)
        self.assertEqual(definition.environment_sha256,control.digest(control.ENVIRONMENT))
        self.assertEqual(definition.limits_sha256,control.digest(control.LIMITS))
        self.assertEqual(definition.source_revision,definition.recipe.base)
        self.assertNotIn('passed',asdict(definition))
        self.assertFalse(self.chain.has('control-execution-intent.json'))

    def _receipt_fixture(self, probes):
        # Pure receipt classification fixture: no original Git/review authority
        # is created or claimed by these private-helper controls.
        source=consumer.Revision('1'*40,'2'*40,'3'*64)
        revisions=[replace(source,commit_oid=str(i)*40) for i in range(1,6)]
        targets=tuple(consumer.ReviewTarget(role,identity,revisions[-1],'accept','a'*64)
            for role,identity in zip(('package','integration'),self.args['reviewer_ids'],strict=True))
        recipe=consumer.ControlRecipe(*revisions,('producer.py',),('consumer.py',),'a'*64,'b'*64,targets)
        definition=control.ControlDefinition(recipe,source,'a'*64,'harness_qualification','c'*64,{},'d'*64,'e'*64,'f'*64)
        self.owner._put('control-execution.json',{'fixture_only':'receipt classification, no qualification'})
        originals=tuple(review.AuthenticatedReview(identity,role,'control_review','accept','a'*64,
            'report.json','b'*64,2,'delivery.json','c'*64,3,self.review_chain.commitment)
            for role,identity in zip(('package','integration'),self.args['reviewer_ids'],strict=True))
        authority=review.OriginalReviewAuthority(self.review_chain,self.review_chain.commitment)
        return self.owner._receipt(definition,probes,originals,{'package':'passed','integration':'passed'},authority)

    def _probe_fixture(self):
        source=consumer.Revision('1'*40,'2'*40,'3'*64)
        return tuple(control.ProbeFact(label,source,'intent.json','terminal.json','a'*64,
            'stdout.bin','b'*64,'stderr.bin','c'*64,0,b'',b'',True,True) for label in control.LABELS)

    def test_receipt_infrastructure_only_keeps_semantic_facet_unknown(self):
        rows=list(self._probe_fixture());rows[3]=replace(rows[3],infrastructure_complete=False,matches_expected=False)
        receipt=self._receipt_fixture(tuple(rows))
        self.assertEqual(receipt['facets']['semantic_conflict'],'infrastructure_error')
        self.assertEqual(receipt['outcomes'][3]['status'],'infrastructure_error')
        self.assertFalse(self.chain.has('control-qualification.json'))

    def test_receipt_known_failure_survives_separate_infrastructure_unknown(self):
        rows=list(self._probe_fixture());rows[0]=replace(rows[0],matches_expected=False)
        rows[3]=replace(rows[3],infrastructure_complete=False,matches_expected=False)
        receipt=self._receipt_fixture(tuple(rows))
        self.assertEqual(receipt['facets']['semantic_conflict'],'failed')
        self.assertEqual(receipt['outcomes'][0]['status'],'failed')
        self.assertEqual(receipt['outcomes'][3]['status'],'infrastructure_error')

    def test_actual_signal_exit_is_retained_but_has_no_semantic_attribution(self):
        result=self.owner._run_probe([sys.executable,'-I','-S','-c','import os,signal;os.kill(os.getpid(),signal.SIGTERM)'],self.root)
        self.assertLess(result['exit_code'],0)
        self.assertFalse(control._complete_capture(result,{'stdout':result['stdout'],'stderr':result['stderr']}))

    def test_cleanup_attempts_every_pipe_after_kill_wait_and_close_failures(self):
        process=Mock();process.poll.return_value=None;process.wait.side_effect=TimeoutError('fixture wait')
        process.stdout.close.side_effect=OSError('fixture close')
        with patch.object(control.subprocess,'Popen',return_value=process), \
             patch.object(control.selectors,'DefaultSelector',side_effect=ValueError('fixture setup')), \
             patch.object(control.os,'killpg',side_effect=PermissionError('fixture kill')):
            with self.assertRaisesRegex(consumer.AuthorityUnavailable,'cleanup uncertain'):
                self.owner._run_probe(['fixture-only'],self.root)
        process.stdin.close.assert_called_once()
        process.stdout.close.assert_called_once()
        process.stderr.close.assert_called_once()
        process.wait.assert_called_once_with(timeout=2)

    def test_cleanup_preserves_interruption_after_independent_release_attempts(self):
        process=Mock();process.poll.return_value=None;process.wait.side_effect=TimeoutError('fixture wait')
        with patch.object(control.subprocess,'Popen',return_value=process), \
             patch.object(control.selectors,'DefaultSelector',side_effect=KeyboardInterrupt), \
             patch.object(control.os,'killpg',return_value=None):
            with self.assertRaises(KeyboardInterrupt):self.owner._run_probe(['fixture-only'],self.root)
        process.stdout.close.assert_called_once()
        process.stderr.close.assert_called_once()

    def test_shared_capture_protocol_deadline_and_exact_helper_are_bound(self):
        contract=self.owner.runtime['source_capture']
        self.assertEqual(contract['protocol'],'candidate-git-source-batch-v1')
        self.assertEqual(contract['per_revision_timeout_seconds'],60)
        self.assertIn('whole-capture-monotonic-v1',contract['deadline_contract'])
        self.assertIn('gossip_harness/candidate_git_source_batch_v1.py',self.owner.sources)

    def test_later_original_review_gets_new_publication_without_probe_replay(self):
        self.executed()
        authority=review.OriginalReviewAuthority(self.review_chain,self.review_chain.commitment)
        requests=self.owner.stage_reviews(authority)
        partial=self.owner.complete(authority)
        authority=self.enroll(requests)
        with self.assertRaisesRegex(consumer.AuthorityUnavailable,'publication'):
            self.owner.verify_current(authority)
        with patch.object(self.owner,'_run_probe',side_effect=AssertionError('must not replay')):
            final=self.owner.complete(authority)
        self.assertNotEqual(partial.receipt_name,final.receipt_name)
        self.assertTrue(self.chain.has(partial.receipt_name))
        self.assertTrue(self.chain.has(partial.verifier_name))
        self.assertTrue(all(row.status=='passed' for row in final.facets))
        self.assertEqual(partial.probes,final.probes)

    def test_known_actual_fixture_failure_survives_missing_original_reviews(self):
        # Deliberately altered trusted fixture, never a candidate or fabricated
        # subprocess result. Its actual Python output/exit is retained below.
        alternate=control.BASE|{'producer.py':'def produce():\n    return "13"\n'}
        right=control.RIGHT|{'producer.py':alternate['producer.py']}
        chain=self.new_chain('deliberate-control-fault')
        with patch.object(control,'BASE',alternate),patch.object(control,'RIGHT',right), \
             patch.dict(control.FIXTURES,{'base':alternate,'right':right}):
            owner=control.ControlQualification(chain,chain.commitment,**(self.args|{'root':self.root/'fault-fixture'}))
            owner.prepare();owner.execute_once()
            authority=review.OriginalReviewAuthority(self.review_chain,self.review_chain.commitment)
            owner.stage_reviews(authority)
            result=owner.complete(authority)
        self.assertEqual(result.probes[0].exit_code,2)
        self.assertEqual(result.probes[0].stdout,b'{"value":13}\n')
        self.assertTrue(result.probes[0].infrastructure_complete)
        self.assertEqual(result.outcomes[0].status,'failed')
        self.assertEqual(result.facets[0].status,'failed')
        self.assertEqual([row.status for row in result.facets[-2:]],['infrastructure_error']*2)

    def test_authenticated_rejection_survives_unresolved_sibling_review(self):
        authority=self.reviewed(decisions=('reject','unresolved'))
        result=self.owner.complete(authority)
        self.assertEqual([row.status for row in result.facets[-2:]],['failed','infrastructure_error'])
        self.assertEqual([row.decision for row in result.reviews],['reject','unresolved'])
        self.assertEqual(result.terminal_status,'infrastructure_error')

    def test_first_cleanup_interruption_survives_remaining_pipe_closes(self):
        process=Mock();process.poll.return_value=None;process.wait.side_effect=KeyboardInterrupt
        process.stdout.close.side_effect=OSError('fixture close')
        with patch.object(control.subprocess,'Popen',return_value=process), \
             patch.object(control.selectors,'DefaultSelector',side_effect=ValueError('fixture setup')), \
             patch.object(control.os,'killpg',return_value=None):
            with self.assertRaises(KeyboardInterrupt):self.owner._run_probe(['fixture-only'],self.root)
        process.stdin.close.assert_called_once()
        process.stdout.close.assert_called_once()
        process.stderr.close.assert_called_once()
