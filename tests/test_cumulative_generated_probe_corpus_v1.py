"""Probe lineage/reduction tests; financial and mesh integration seams are synthetic."""
from dataclasses import asdict, replace
import copy
import unittest

from gossip_harness import cumulative_generated_probe_corpus_v1 as corpus
from gossip_harness import cumulative_generated_probe_ranked_originals_v1 as ranked
from gossip_harness import cumulative_generated_probe_values_v2 as values
from gossip_harness import cumulative_study_controller_v2 as study
from tests.ranked_originals_fixture_v1 import OriginalFixture
from tests.test_cumulative_generated_probe_context_v1 import fixture as generation_fixture
from tests.test_cumulative_generated_probe_plan_v1 import release
from tests.test_cumulative_generated_probe_values_v2 import proposal


def offered(text='alpha', template='refresh-noop-v1'):
    value=proposal(template)
    if 'text' in value['parameters']:value['parameters']['text']=text
    else:value['parameters']['initial_text']=text
    return value


def proof(gen, offers):
    notes=[]
    for package in study.PACKAGES:
        decision=None
        if package in offers:
            decision={'decision_sha256':study.digest(['decision',package]),'probes':[
                {'index':i,'status':'admitted_definition','probe':values.admit(p,
                    released_requirements=('M2-REFRESH','M3-BACKUP-RESTORE'),contract_sha256=values.PRODUCT_SHA256)}
                for i,p in enumerate(offers[package])]}
        notes.append({'package':package,'decision':decision})
    return {'stage':'stage-'+gen.milestone+'-'+str(gen.generation),'review_definitions':notes}


class GeneratedProbeCorpusReductionTests(unittest.TestCase):
    def gen(self, milestone='M2', generation=0):
        return replace(generation_fixture('S4-G'),milestone=milestone,generation=generation)

    def advance(self, active, offers, *, quotas=None, gen=None, public=None):
        gen=self.gen() if gen is None else gen
        return corpus._advance(active,gen,proof(gen,offers),corpus.Quotas(2,8) if quotas is None else quotas,
            release(gen.milestone,('M2-REFRESH','M3-BACKUP-RESTORE')) if public is None else public)

    def test_duplicate_offers_keep_first_original_and_all_offer_positions(self):
        active,events=self.advance([],{p:[offered()] for p in study.PACKAGES})
        self.assertEqual(len(active),1);self.assertEqual(active[0]['origin']['package'],'catalog')
        self.assertEqual([e['status'] for e in events],['active_definition']+['duplicate_definition']*3)
        self.assertEqual([e['origin']['proposal_index'] for e in events],[0]*4)

    def test_package_quota_rejections_are_explicit_and_do_not_remove_prior_definitions(self):
        active,events=self.advance([],{'catalog':[offered('a'),offered('b')]},quotas=corpus.Quotas(1,8))
        self.assertEqual(len(active),1);self.assertEqual(events[1]['reason'],'package_milestone_quota')
        later,events=self.advance(active,{'catalog':[offered('c')]},gen=self.gen(generation=1),quotas=corpus.Quotas(1,8))
        self.assertEqual(later,active);self.assertEqual(events[0]['status'],'quota_rejected')

    def test_trajectory_quota_is_shared_across_packages_and_milestones(self):
        active,events=self.advance([],{p:[offered(p)] for p in study.PACKAGES},quotas=corpus.Quotas(2,1))
        self.assertEqual(len(active),1);self.assertEqual([e['status'] for e in events],['active_definition']+['quota_rejected']*3)
        later,events=self.advance(active,{'catalog':[offered('later')]},gen=self.gen('M3'),quotas=corpus.Quotas(2,1))
        self.assertEqual(later,active);self.assertEqual(events[0]['reason'],'trajectory_quota')

    def test_carried_probe_keeps_its_original_meaning_and_origin(self):
        active,_=self.advance([],{'catalog':[offered()]});before=copy.deepcopy(active)
        later,_=self.advance(active,{'ingestion':[offered()]},gen=self.gen('M3'))
        self.assertEqual(active,before);self.assertEqual(later,before)
        self.assertEqual(corpus.active_probes(later)[0].origin_milestone,'M2')
        self.assertEqual(corpus.active_probes(later)[0].original_record_sha256,values.digest(before[0]['origin']))

    def test_missing_reviews_preserve_the_previous_corpus(self):
        active,_=self.advance([],{'catalog':[offered()]})
        later,events=self.advance(active,{})
        self.assertEqual(later,active);self.assertEqual(len(events),4)
        self.assertTrue(all(e['status']=='no_valid_review' for e in events))

    def test_unsupported_carry_is_not_silently_filtered(self):
        active,_=self.advance([],{'catalog':[offered()]})
        with self.assertRaises(ValueError):self.advance(active,{},gen=self.gen('M3'),public=release('M3',('M3-BACKUP-RESTORE',)))

    def test_quotas_are_explicit_exact_positive_integers(self):
        for args in [(True,4),(0,4),(1,16385)]:
            with self.subTest(args=args),self.assertRaises(ValueError):corpus.Quotas(*args)


    def test_seeded_review_order_cannot_change_deterministic_quota_allocation(self):
        gen=self.gen();original=proof(gen,{p:[offered(p)] for p in study.PACKAGES})
        reordered={**original,'review_definitions':list(reversed(original['review_definitions']))}
        public=release();quota=corpus.Quotas(2,1)
        self.assertEqual(corpus._advance([],gen,original,quota,public),corpus._advance([],gen,reordered,quota,public))

    def test_missing_or_duplicate_review_scope_cannot_supply_a_complete_corpus(self):
        gen=self.gen();original=proof(gen,{})
        for rows in (original['review_definitions'][:-1],original['review_definitions'][:-1]+[original['review_definitions'][0]]):
            with self.subTest(rows=rows),self.assertRaisesRegex(ValueError,'complete_unique_review_definition'):
                corpus._advance([],gen,{**original,'review_definitions':rows},corpus.Quotas(2,8),release())


def probe_plan(plan):
    releases=tuple(replace(r,requirement_ids=(r.milestone+'-requirement',)
        + (() if r.milestone=='M1' else ('M2-REFRESH',))
        + (('M3-BACKUP-RESTORE',) if r.milestone in ('M3','M4') else ())) for r in plan.releases)
    cohort=replace(plan.cohort,requirements_release_sha256=study.digest([asdict(r) for r in releases]))
    return replace(plan,cohort=cohort,releases=releases)


class GeneratedProbeCorpusOriginalGitTests(unittest.TestCase):
    quotas=corpus.Quotas(2,8)

    def install(self, f):
        corpus.install_contract(f.owner,expected=f.chain.commitment,quotas=self.quotas,limits=f.limits)

    @staticmethod
    def freeze(f):
        ranked.freeze_build_phase(f.owner,expected=f.chain.commitment,milestone=f.milestone,generation=f.generation,limits=f.limits)

    def fixture(self, **changes):
        args={'before_contract':self.install,'plan_change':probe_plan,'at_build_boundary':self.freeze,
              'review_change':lambda d:d.update(probes=[offered()])};args.update(changes)
        f=OriginalFixture(**args);self.addCleanup(f.close);return f

    def enroll(self, f, **changes):
        args={'expected':f.chain.commitment,'milestone':f.milestone,'generation':f.generation,
              'quotas':self.quotas,'limits':f.limits};args.update(changes)
        return corpus.enroll(f.owner,**args)

    def read(self, f):
        return corpus.read(f.owner,expected=f.chain.commitment,milestone=f.milestone,generation=f.generation,
                           quotas=self.quotas,limits=f.limits)

    def test_original_reviews_supply_the_complete_corpus_and_frozen_matrix(self):
        f=self.fixture();receipt=self.enroll(f);active,matrix,record=self.read(f)
        self.assertEqual(len(active),1);self.assertEqual(receipt['execution_cells']['total'],13)
        self.assertEqual(len(record['review_originals']),4)
        self.assertEqual([o['status'] for o in record['offers']],['active_definition']+['duplicate_definition']*3)
        self.assertEqual(matrix.record(),record['matrix']);self.assertFalse(record['resource_admission'])
        self.assertFalse(record['dispatch_authority'])

    def test_probe_origin_survives_source_advance_and_another_generation(self):
        f=self.fixture();self.enroll(f);old_active,_,_=self.read(f)
        old=f.owner.protected.head();new=f.owner.protected.propose({'base.py':'# later accepted source\n'},old)
        f.owner.protected._git('update-ref','refs/heads/accepted',new,old)
        f.start_stage('M2',1,at_build_boundary=self.freeze,review_change=lambda d:d.update(probes=[offered('beta')]))
        self.enroll(f);active,matrix,_=self.read(f)
        self.assertEqual(len(active),2);self.assertIn(old_active[0],active)
        self.assertEqual(matrix.record()['execution_cell_counts']['total'],17)
        self.assertEqual(f.owner.protected.head(),new)

    def test_cross_milestone_revalidation_keeps_old_origin_and_adds_new_template(self):
        f=self.fixture();self.enroll(f)
        f.start_stage('M3',0,at_build_boundary=self.freeze,
                      review_change=lambda d:d.update(probes=[offered('backup','manifest-content-hash-v1')]))
        self.enroll(f);active,matrix,_=self.read(f)
        self.assertEqual(sorted(p.origin_milestone for p in active),['M2','M3'])
        self.assertEqual(matrix.record()['execution_cell_counts']['total'],16)

    def test_missing_earlier_enrollment_cannot_erase_previous_review_probes(self):
        f=self.fixture()
        f.start_stage('M3',0,at_build_boundary=self.freeze)
        before=f.chain.commitment
        with self.assertRaisesRegex(ValueError,'earlier_review_corpus_missing'):self.enroll(f)
        self.assertEqual(f.chain.commitment,before)

    def test_changed_prior_financial_original_blocks_carry_and_new_enrollment(self):
        f=self.fixture();self.enroll(f)
        old_review=next(p for p in f.proofs.values() if 'decision.json' in p['result_payload']['payload']['changes'])
        f.start_stage('M3',0,at_build_boundary=self.freeze)
        old_review['result_payload']={'kind':'failure','payload':{}}
        before=f.chain.commitment
        with self.assertRaisesRegex(ValueError,'finance request/result'):self.enroll(f)
        self.assertEqual(f.chain.commitment,before);self.assertIsNone(f.owner.records.read(f.stage+'.corpus'))

    def test_missing_or_changed_prospective_quota_contract_is_rejected(self):
        f=self.fixture(before_contract=None)
        with self.assertRaisesRegex(ValueError,'corpus_contract_missing_or_changed'):self.enroll(f)
        f=self.fixture()
        with self.assertRaisesRegex(ValueError,'corpus_contract_missing_or_changed'):
            self.enroll(f,quotas=corpus.Quotas(3,8))

    def test_enrollment_is_once_only_and_requires_the_original_review_boundary(self):
        f=self.fixture();self.enroll(f);before=f.chain.commitment
        with self.assertRaisesRegex(ValueError,'already_enrolled'):self.enroll(f)
        self.assertEqual(f.chain.commitment,before)
        f=self.fixture();f.owner.records.put('later-effect',{'intent':True})
        with self.assertRaisesRegex(ValueError,'immediately_follow_reviews'):self.enroll(f)
