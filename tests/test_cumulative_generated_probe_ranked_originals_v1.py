"""Bounded ranked data and original joins; financial/mesh fixtures are synthetic."""
from dataclasses import replace
import json
import unittest

from gossip_harness import cumulative_generated_probe_ranked_originals_v1 as ranked
from gossip_harness import cumulative_generated_probe_context_v1 as contexts
from gossip_harness import cumulative_generated_probe_values_v2 as values
from tests.test_cumulative_generated_probe_plan_v1 import release
from tests.test_cumulative_generated_probe_values_v2 import proposal
from tests.ranked_originals_fixture_v1 import OriginalFixture


def decision(**changes):
    return {'protocol':ranked.DECISION_PROTOCOL,'stage_id':'stage','package':'catalog',
            'ranked_actors':['t.B02','t.B01'],'reasons':'Fixed before new results','probes':[],**changes}


def decode(value,**changes):
    args={'stage':'stage','package':'catalog','eligible':('t.B01','t.B02'),'release':release(),
          'limits':ranked.ReviewLimits(16384,4,256,16)};args.update(changes)
    return ranked.decode_decision(value if type(value) is bytes else values.canonical(value),**args)


class GeneratedProbeRankedDecisionTests(unittest.TestCase):
    def test_rank_order_and_empty_endorsement_are_preserved_without_authority(self):
        result=decode(decision());self.assertEqual(result['ranked_actors'],('t.B02','t.B01'))
        self.assertIs(result['dispatch_authority'],False);self.assertIs(result['acceptance_authority'],False)
        self.assertEqual(decode(decision(ranked_actors=[]))['ranked_actors'],())

    def test_duplicate_unknown_foreign_and_nonstring_aliases_are_refused(self):
        for ranks in [['t.B01','t.B01'],['t.B03'],['other.B01'],[True],{'first':'t.B01'}]:
            with self.subTest(ranks=ranks),self.assertRaises(ValueError):decode(decision(ranked_actors=ranks))

    def test_closed_schema_rejects_old_decision_extra_commands_and_scope_changes(self):
        for changes in [{'selected_actor':'t.B01'},{'command':'anything'},{'stage_id':'other'},
                        {'package':'clients'},{'protocol':'legacy'},{'reasons':True}]:
            with self.subTest(changes=changes),self.assertRaises(ValueError):decode(decision(**changes))

    def test_duplicate_json_keys_nonfinite_invalid_utf8_and_deep_json_are_rejected(self):
        for raw in [b'{"protocol":1,"protocol":2}',b'{"x":NaN}',b'\xff',b'['*30+b'0'+b']'*30]:
            with self.subTest(raw=raw),self.assertRaises(ValueError):decode(raw)

    def test_all_parser_limits_are_explicit_and_enforced(self):
        for field in ['review_bytes','proposal_count','json_nodes','json_depth']:
            with self.subTest(field=field),self.assertRaises(ValueError):replace(ranked.ReviewLimits(16384,4,256,16),**{field:True})
        for limits in [ranked.ReviewLimits(2,4,256,16),ranked.ReviewLimits(16384,4,1,16),ranked.ReviewLimits(16384,4,256,1)]:
            with self.subTest(limits=limits),self.assertRaises(ValueError):decode(decision(),limits=limits)
        with self.assertRaisesRegex(ValueError,'proposal_count'):decode(decision(probes=[proposal()]*5))

    def test_probe_definitions_admitted_in_order_with_generic_rejections_and_no_refund(self):
        good=proposal();bad={**good,'expectation':{'kind':'exact_scalar','value':'wrong'}}
        result=decode(decision(probes=[good,bad,good]))
        self.assertEqual([p['status'] for p in result['probes']],['admitted_definition','rejected_definition','rejected_definition'])
        self.assertEqual([p['index'] for p in result['probes']],[0,1,2])
        self.assertEqual(result['probes'][1]['reason'],result['probes'][2]['reason'])
        self.assertNotIn('corrected',json.dumps(result))
        self.assertIs(result['admitted_definition_is_qualified_execution'],False)

    def test_unreleased_probe_does_not_remove_valid_rankings(self):
        result=decode(decision(probes=[proposal('manifest-content-hash-v1')]))
        self.assertEqual(result['ranked_actors'],('t.B02','t.B01'))
        self.assertEqual(result['probes'][0]['status'],'rejected_definition')

    def test_instructions_request_one_closed_action_and_only_released_templates(self):
        text=ranked.instructions(release(),'stage','catalog',ranked.ReviewLimits(16384,4,256,16))
        self.assertIn('one review action',text);self.assertIn('ranked_actors',text)
        self.assertIn('refresh-noop-v1',text);self.assertNotIn('manifest-content-hash-v1',text)
        self.assertNotIn('selected_actor',text)


class GeneratedProbeRankedOriginalGitTests(unittest.TestCase):
    def fixture(self,**args):
        fixture=OriginalFixture(**args);self.addCleanup(fixture.close);return fixture

    def test_original_seeded_rosters_join_to_canonical_small_and_large_contexts(self):
        for index,expected in [(0,4),(2,16)]:
            with self.subTest(index=index):
                f=self.fixture(index=index);gen,proof=f.read()
                self.assertEqual(len(gen.proposals),expected)
                self.assertEqual(tuple(p.actor for p in gen.proposals),tuple(sorted(p.actor for p in gen.proposals)))
                self.assertEqual(len(contexts.census(gen)['contextual']),expected)
                self.assertEqual(f.owner.finance.verified_terminal.call_count,expected+4)
                self.assertFalse(proof['durably_frozen']);self.assertFalse(proof['acceptance_authority'])
                self.assertEqual(f.chain.commitment,f.expected)

    def test_stopped_builder_and_reviewer_preserve_denominators_and_unavailable_anchor(self):
        f=self.fixture(index=2,stopped_builder='B02',stopped_reviewer='R2');gen,_=f.read()
        self.assertEqual(len(gen.proposals),16);self.assertEqual(gen.proposals[1].disposition,'stopped')
        self.assertEqual(gen.rankings[1].disposition,'stopped')
        census=contexts.census(gen);self.assertEqual(census['planned_contextual_exposures'],15)
        self.assertEqual(len(census['unavailable_contextual_actors']),15)

    def test_bad_reviewer_payload_yields_no_endorsement_without_erasing_originals(self):
        f=self.fixture(review_change=lambda d:d.update(ranked_actors=['foreign.B01']))
        gen,proof=f.read();self.assertTrue(all(not r.actors for r in gen.rankings))
        self.assertEqual(len(proof['review_originals']),4);self.assertIsNone(gen.anchor_vector)

    def test_omitted_frontier_candidate_is_an_integrity_failure(self):
        f=self.fixture(frontier_change=lambda d:d['builds'].clear())
        with self.assertRaisesRegex(ValueError,'frontier_omitted'):f.read()

    def test_legacy_directive_cannot_be_reinterpreted_as_ranked_review(self):
        f=self.fixture(legacy_review=True)
        with self.assertRaisesRegex(ValueError,'not_prospectively_requested'):f.read()

    def test_retrospective_contract_is_rejected_before_context_construction(self):
        f=self.fixture(late_contract=True)
        with self.assertRaisesRegex(ValueError,'must_precede_role_work'):f.read()

    def test_original_seed_order_cannot_be_replaced_with_an_arbitrary_roster(self):
        f=self.fixture(reverse_roster=True)
        with self.assertRaisesRegex(ValueError,'original_actor_or_bytes'):f.read()

    def test_financial_original_mismatch_propagates_instead_of_becoming_bad_candidate(self):
        f=self.fixture()
        first=next(iter(f.proofs.values()));first['result_payload']={'kind':'failure','payload':{}}
        with self.assertRaisesRegex(ValueError,'finance request/result'):f.read()

    def test_missing_mesh_original_and_moved_git_base_cannot_supply_contexts(self):
        f=self.fixture();ref=next(ref for ref in f.owner.seed.values if ref.kind=='cumulative-result')
        del f.owner.seed.values[ref]
        with self.assertRaisesRegex(ValueError,'evidence_unavailable'):f.read()
        f=self.fixture();old=f.owner.protected.head();new=f.owner.protected.propose({'extra.txt':'changed'},old)
        f.owner.protected._git('update-ref','refs/heads/accepted',new,old)
        with self.assertRaisesRegex(ValueError,'generation_source_differs'):f.read()

    def test_changed_limits_or_ledger_and_stale_checkpoint_are_rejected(self):
        f=self.fixture()
        with self.assertRaisesRegex(ValueError,'contract_missing_or_changed'):f.read(limits=replace(f.limits,proposal_count=3))
        f=self.fixture();f.owner.ledger.rename(f.root/'retained-prior-ledger.sqlite')
        f.owner.ledger.write_bytes(b'different file identity')
        with self.assertRaisesRegex(ValueError,'ledger_identity_changed'):f.read()
        f=self.fixture();f.owner.records.put('later',{'value':'another prefix'})
        with self.assertRaisesRegex(ValueError,'anchor uncertain'):f.read()

    def test_foreign_result_stage_is_rejected_despite_matching_financial_proposal(self):
        f=self.fixture(result_change=lambda value:value.update(stage_id='foreign-stage'))
        with self.assertRaisesRegex(ValueError,'original_result_stage_differs'):f.read()

    def test_an_expanded_directive_attempt_allowance_is_not_silently_adopted(self):
        f=self.fixture(directive_change=lambda d:replace(d,attempt_limit=2))
        with self.assertRaisesRegex(ValueError,'original_role_or_scope_differs'):f.read()
