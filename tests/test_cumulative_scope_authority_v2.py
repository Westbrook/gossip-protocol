"""Real durable request staging only; incomplete semantic scope cannot register."""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

from gossip_harness import candidate_checkpoint_chain_v1 as chain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness import candidate_scope_consumer_v1 as consumer
from gossip_harness import cumulative_scope_authority_v1 as legacy
from gossip_harness import cumulative_scope_authority_v2 as authority
from gossip_harness import cumulative_scope_source_v2 as source
from gossip_harness import project_acceptance_registry_v1 as registry
from tests.test_cumulative_scope_source_v2 import ROOT, storage_registration
from tests.test_project_acceptance_compiler_v1 import synthetic_declaration


class CumulativeScopeAuthorityV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = source.load_catalog(ROOT)
        cls.component = source.storage_slice(storage_registration())
        cls.declaration = source.assemble_declaration(cls.catalog, synthetic_declaration(cls.catalog.inventory).cohort,
            (cls.component,), review_sha256='d' * 64, capacity_profile=registry.HISTORY_CAPACITY_PROFILE)
        cls.scope = source.scope_review_input(cls.catalog, cls.declaration)
        cls.submission = authority.ScopeSubmission(cls.catalog, cls.declaration, cls.scope,
            cls.component.gate.binding.subject, (cls.component,), ())

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.raw, self.delta = self.root / 'raw', self.root / 'delta'
        self.head = ExternalHead.create(self.root / 'head', journal_roots=(self.raw, self.delta))
        self.addCleanup(self.head.close)
        self.context = {'protocol': authority.PROTOCOL, 'purpose': 'offline request mechanics only'}
        self.chain = chain.CheckpointChain.create(self.raw, self.delta, context=self.context, authority=self.head)
        self.addCleanup(self.chain.close)
        self.owner = authority.ScopeRegistrationController(ROOT, self.chain, self.chain.commitment)

    def test_complete_request_builder_inserts_real_storage_selectors_and_capacity_review(self):
        request = self.submission.request()
        self.assertEqual(request['protocol'], authority.PROTOCOL)
        self.assertEqual(request['factory_protocol'], source.PROTOCOL)
        self.assertFalse(request['acceptance_authority'])
        self.assertEqual(request['candidate_observations'], 'none')
        targets = request['targets']
        self.assertEqual(len({row['id'] for row in targets}), len(targets))
        for prefix, count in (('obligation:', 312), ('authority:', 22), ('gap:', 188), ('capacity-contract', 1)):
            self.assertEqual(sum(row['id'].startswith(prefix) for row in targets), count)
        suite = next(row for row in targets if row['id'].startswith('suite:'))
        self.assertEqual(suite['value']['source_catalogs'][0]['family'], 'storage-b02')
        self.assertTrue(suite['value']['source_catalogs'][0]['assertions'])
        self.assertEqual(request['declaration']['capacity_profile'], registry.HISTORY_CAPACITY_PROFILE)
        self.assertTrue(all(row['sha256'] == source.sha(source.encoded(row['value'])) for row in targets))
        self.assertIn('gossip_harness/cumulative_scope_source_v2.py', request['implementation_sources'])

    def test_positive_durable_request_staging_replays_and_reopens_exactly(self):
        staged = self.owner.stage(self.submission)
        expected = self.chain.commitment
        self.assertEqual(json.loads(self.chain.read(staged.request_name))['protocol'], authority.PROTOCOL)
        self.assertEqual(self.owner.stage(self.submission), staged)
        self.assertEqual(self.chain.commitment, expected)
        self.assertTrue(staged.design.blockers)
        self.assertIsNone(staged.design.registry)
        self.assertFalse(staged.missing_executable_edges)
        self.chain.close(); self.head.close()
        head = ExternalHead.reopen(self.root / 'head', journal_roots=(self.raw, self.delta), expected=expected)
        self.addCleanup(head.close)
        journal = chain.CheckpointChain.reopen(self.raw, self.delta, context=self.context, authority=head, expected=expected)
        self.addCleanup(journal.close)
        owner = authority.ScopeRegistrationController(ROOT, journal, expected)
        self.assertEqual(owner.stage(self.submission), staged)
        self.assertEqual(journal.commitment, expected)

    def test_no_new_semantic_authority_or_complete_registration(self):
        with self.assertRaises(authority.RegistrationMissing):
            self.owner.register(self.submission, report_name='absent-real-review.json')
        snapshot = self.owner.open_snapshot()
        self.assertIs(type(snapshot), authority.ScopeSnapshot)
        self.assertIsNone(snapshot.registration(self.submission.subject))
        self.assertIsNone(snapshot.registration_provenance(self.submission.subject))
        self.assertIsNone(snapshot.freeze())
        self.assertIsNone(snapshot.promotion(self.submission.subject))

    def test_old_controller_and_old_slice_type_cannot_implicitly_adopt_extension(self):
        old_owner = legacy.ScopeRegistrationController(ROOT, self.chain, self.chain.commitment)
        with self.assertRaises(consumer.AuthorityError):
            old_owner.stage(self.submission)
        old = legacy.ScopeSubmission(self.catalog, self.declaration, self.scope, self.submission.subject, (), ())
        with self.assertRaises(consumer.AuthorityError):
            self.owner.stage(old)
        self.assertEqual(self.chain.commitment.raw_file_count, 0)

    def test_capacity_opt_in_changes_request_without_implying_scope_approval(self):
        legacy_declaration = replace(self.declaration, capacity_profile=registry.LEGACY_CAPACITY_PROFILE)
        legacy_scope = source.scope_review_input(self.catalog, legacy_declaration)
        old = replace(self.submission, declaration=legacy_declaration, scope=legacy_scope).request()
        current = self.submission.request()
        self.assertNotEqual(old['declaration_sha256'], current['declaration_sha256'])
        self.assertEqual(old['capacity_contract']['execution_gates'], 512)
        self.assertEqual(current['capacity_contract']['execution_gates'], 4096)
        self.assertFalse(old['acceptance_authority'] or current['acceptance_authority'])

    def test_foreign_assertion_cannot_be_hidden_in_durable_review_request(self):
        broken = replace(self.component, assertions=())
        submission = replace(self.submission, slices=(broken,))
        with self.assertRaises(consumer.AuthorityError):
            self.owner.stage(submission)
        self.assertEqual(self.chain.commitment.raw_file_count, 0)

    def test_shared_review_delivery_still_requires_protected_enrollment(self):
        staged = self.owner.stage(self.submission)
        with self.assertRaises(authority.RegistrationMissing):
            self.owner.authenticate_review(staged, report_name='candidate-self-approval.json')
        snapshot = self.owner.open_snapshot()
        self.chain.retain('external-change.json', b'{}')
        with self.assertRaises(consumer.AuthorityUnavailable):
            snapshot.check_current(snapshot.checkpoint)
