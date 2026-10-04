"""Disposable fixture only; never imports credentials or a live ledger."""
from pathlib import Path
import sqlite3
import tempfile
import time

from gossip_harness.candidate_checkpoint_chain_v1 import CheckpointChain
from gossip_harness.candidate_checkpoint_head_v1 import ExternalHead
from gossip_harness.ledger import Ledger
from gossip_harness.peer_financial_authority_v2 import canonical_payload, ledger_identity
from gossip_harness.peer_financial_authority_v3 import FIXTURE_TRANSPORT, profile_manifest
from gossip_harness.peer_financial_authority_v4 import CumulativeAuthorityV4, PERMIT_PROTOCOL, source_fingerprints
from gossip_harness.peer_financial_terminal_v1 import (
    ChildRegistration, TerminalRoster, EvidenceReference, ChildTerminalSealRequest, digest, sha,
)
from gossip_harness.peer_project_contract_v2 import ActionRequest, Context, EvidenceRef, WorkKey, to_dict
from gossip_harness.worker import OpenAIWorker
from tests.test_peer_financial_authority_v2 import MemoryPayloads, OfflineTransport


class Fixture:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.path = self.root / 'fixture.sqlite'
        self.ledger = Ledger(self.path, 1_000_000)
        self.ledger.add_task('historical')
        old = self.ledger.claim('historical', 'old', now=1, ttl=1)
        self.ledger.reserve('historical-payment', old, 2000, now=1)
        self.ledger.settle('historical-payment', 1000)
        children = tuple(ChildRegistration('cohort' + str(i), 'trajectory' + str(i),
                         tuple(f't{i}.r{j}' for j in range(8 if i < 2 else 20))) for i in range(6))
        self.roster = TerminalRoster('a' * 64, children)
        self.authority = None
        self.head = ExternalHead.create(self.root / 'anchor', journal_roots=(self.root / 'raw', self.root / 'delta'))
        self.chain = CheckpointChain.create(self.root / 'raw', self.root / 'delta',
                                           context={'fixture_study': self.roster.sha256}, authority=self.head)
        self.transport = OfflineTransport()
        self.worker = OpenAIWorker('fixture-never-a-real-key', max_output_tokens=64, timeout=2, transport=self.transport)
        self.payloads = MemoryPayloads()
        self.select(0)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.transport.release.set()
        if self.authority is not None:
            self.authority.close()
        self.chain.close()
        self.head.close()
        self.temp.cleanup()

    def select(self, index):
        self.child = self.roster.children[index]
        self.actor = self.child.actors[0]
        self.context = Context('a' * 64, self.child.cohort, self.child.trajectory, 0, 'b' * 64)
        self.works = [WorkKey('catalog', 'm1', 'slot' + str(i), 0) for i in range(2)]
        self.contract = {'cohort_id': self.child.cohort, 'execution_contract_sha256': 'a' * 64,
            'ledger_identity': ledger_identity(self.path), 'journal_root': str(self.root / ('journal-' + self.child.cohort)),
            'transport_identity': FIXTURE_TRANSPORT, 'task_specs': [
                {'context': to_dict(self.context), 'work': to_dict(work), 'actors': list(self.child.actors),
                 'kinds': ['build'], 'profiles': ['mini'], 'allowed_paths': ['src/a.py'],
                 'max_reserved_units': 400_000} for work in self.works]}

    def permit(self, opening):
        profiles = {'mini': profile_manifest(self.worker)}
        design = {'profiles': profiles, 'max_workers': 2, 'terminal_roster': self.roster.record(),
            'action_limits': {'total': 20, 'by_kind': {'build': 20},
                             'by_kind_generation': {'build': {'0': 20}},
                             'by_actor': dict.fromkeys(self.child.actors, 20)}}
        return {'protocol': PERMIT_PROTOCOL, 'mode': 'fixture', 'approval_ref': 'isolated-fixture-only',
                'execution_design': design, 'execution_design_sha256': digest(design),
                'cohort_contract_sha256': digest(self.contract), 'sources': source_fingerprints(),
                'profiles': profiles, 'incremental_cap_micro_usd': 800_000,
                'expected_global_cap': 1_000_000, 'expected_opening_usage': opening,
                'max_workers': 2, 'qualification': None}

    def open(self, recovery=False, **changes):
        if self.authority is not None:
            self.authority.close()
            self.authority = None
        if recovery:
            opening = self.last_permit['expected_opening_usage']
        else:
            opening = self.ledger.budget()['spent_or_reserved']
        permit = self.permit(opening) if not recovery else self.last_permit
        kwargs = dict(existing_ledger_path=self.path, service_root=self.root / ('service-' + self.child.cohort),
                      cohort_contract=self.contract, incremental_cap_micro_usd=800_000,
                      expected_global_cap=1_000_000, expected_opening_usage=opening,
                      payloads=self.payloads, workers={'mini': self.worker}, max_workers=2,
                      clock=lambda: 100.0, permit=permit, expected_permit_sha256=digest(permit),
                      mode='fixture', recovery=recovery, terminal_roster=self.roster,
                      checkpoint=self.chain, expected_checkpoint=self.chain.commitment)
        kwargs.update(changes)
        self.last_permit = kwargs['permit']
        self.authority = CumulativeAuthorityV4.open(**kwargs)
        return self.authority

    def action(self, number=0):
        task = self.authority.task_id(self.context, self.works[number])
        raw = canonical_payload({'worker_request': {'task_id': task, 'instructions': 'Fix source.',
            'allowed_paths': ['src/a.py'], 'files': {'src/a.py': 'broken\n'}, 'base_sha': 'c' * 40,
            'attempt': 1, 'feedback': ''}, 'view_manifest_sha256': 'd' * 64})
        ref = EvidenceRef('e' * 64, self.actor, 'worker-request', self.payloads.put_owned(self.actor, raw))
        return ActionRequest(self.context, 'action' + str(number), 'request' + str(number), self.actor,
                             'build', self.works[number], 'mini', ref, 'd' * 64)

    def start(self, number=0):
        action = self.action(number)
        lease = self.authority.claim(self.actor, self.context, self.works[number], ttl=600)
        self.authority.submit(self.actor, action, lease)
        return action, lease

    def terminal(self, action):
        until = time.monotonic() + 5
        while time.monotonic() < until:
            reply = self.authority.lookup(self.actor, action.request_id)
            if reply is not None and reply.state not in {'waiting', 'pending'}:
                return reply
            time.sleep(.002)
        self.fail('Fixture did not become terminal')

    def evidence(self, status='completed'):
        refs = []
        for actor in self.child.actors:
            value = {'protocol': 'financial-role-stop-v1', 'roster_sha256': self.roster.sha256,
                     'cohort': self.child.cohort, 'trajectory': self.child.trajectory, 'actor': actor, 'stopped': True}
            name = 'stop-' + actor + '.json'
            raw = canonical_payload(value)
            if not self.chain.has(name):
                self.chain.retain(name, raw)
            refs.append(EvidenceReference(name, sha(raw)))
        value = {'protocol': 'financial-child-terminal-v1', 'roster_sha256': self.roster.sha256,
                 'cohort': self.child.cohort, 'trajectory': self.child.trajectory,
                 'status': status, 'final_source_sha256': 'f' * 64}
        name = 'terminal-' + self.child.cohort + '-' + status.replace('_', '-') + '.json'
        raw = canonical_payload(value)
        if not self.chain.has(name):
            self.chain.retain(name, raw)
        return ChildTerminalSealRequest(tuple(refs), EvidenceReference(name, sha(raw)))

    def seal(self, status='completed'):
        preparation = self.authority.prepare_terminal_snapshot(self.evidence(status))
        return self.authority.seal_terminal(preparation)

    def rows(self, table):
        with sqlite3.connect(self.path) as db:
            return db.execute('SELECT * FROM ' + table + ' ORDER BY rowid').fetchall()

    def snapshot(self):
        return {name: self.rows(name) for name in ('tasks', 'reservations', 'financial_requests_v2',
                    'financial_actions_v2', 'financial_cohorts_v2', 'budget_changes')}
