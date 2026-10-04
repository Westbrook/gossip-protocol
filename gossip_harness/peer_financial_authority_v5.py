"""Versioned settled-failure closure; V4 originals remain frozen.

Failed cognitive calls remain failed and charged. A completed project request
may close after repair only when every original outcome is known and settled.
Financial closure is neither successful candidate publication nor acceptance.
"""
from __future__ import annotations
from dataclasses import asdict
from pathlib import Path
import sqlite3
from typing import Any
from . import peer_financial_authority_v3 as v3
from . import peer_financial_authority_v4 as v4
from .peer_financial_authority_v2 import FinancialError, canonical_payload
from .peer_financial_terminal_v1 import TerminalRoster, SealBusy, digest, sha, require
from .peer_financial_terminal_v2 import checked_policy, checked_financial_config, CLOSURE_POLICY_SHA256
from .peer_store_v1 import strict_loads
from .worker import OpenAIWorker
from . import verification_journal as journal_module

PROTOCOL = 'peer-financial-authority-v5'
PERMIT_PROTOCOL = 'peer-financial-operator-permit-v5'
QUALIFICATION_PROTOCOL = 'peer-financial-qualification-v5'
REQUIRED_SOURCES = (*v4.REQUIRED_SOURCES,
    'gossip_harness/peer_financial_authority_v5.py', 'gossip_harness/peer_financial_rpc_v5.py',
    'gossip_harness/peer_financial_terminal_v2.py')
REQUIRED_TEST_CLASSES = (*v4.REQUIRED_TEST_CLASSES,
    'tests/test_peer_financial_authority_v5.py::PeerFinancialAuthorityV5Tests',
    'tests/test_peer_financial_authority_v5.py::PeerFinancialBarrierV5Tests',
    'tests/test_peer_financial_rpc_v5.py::PeerFinancialRPCV5Tests')
_REPOSITORY = Path(__file__).resolve().parent.parent


def source_fingerprints() -> dict[str, str]:
    return {name:sha((_REPOSITORY/name).read_bytes()) for name in REQUIRED_SOURCES}


def validate_qualification(reference: dict, design: dict, sources: dict) -> dict:
    raise FinancialError('V5 live qualification unavailable: complete matching repaired-success rehearsal and acceptance required')


class CumulativeAuthorityV5(v4.CumulativeAuthorityV4):
    @classmethod
    def open(cls, *args: Any, **kwargs: Any) -> CumulativeAuthorityV5:
        return cls(*args, **kwargs)

    @classmethod
    def preflight_permit(cls, cohort_contract: dict, incremental_cap_micro_usd: int,
                         expected_global_cap: int, expected_opening_usage: int, *,
                         workers: dict[str, OpenAIWorker], permit: dict,
                         expected_permit_sha256: str, max_workers: int = 4,
                         mode: str = 'live', terminal_roster: TerminalRoster | None = None) -> dict:
        require(type(terminal_roster) is TerminalRoster, 'V5 preflight requires prospective terminal roster')
        assert isinstance(terminal_roster, TerminalRoster)
        TerminalRoster(terminal_roster.execution_contract_sha256, terminal_roster.children, terminal_roster.policy)
        require(cohort_contract.get('execution_contract_sha256') == terminal_roster.execution_contract_sha256,
                'Child execution contract differs from prospective study roster')
        checking = object.__new__(cls)
        checking.terminal_roster = terminal_roster
        checking._prepare_permit(cohort_contract, incremental_cap_micro_usd, expected_global_cap,
            expected_opening_usage, workers=workers, permit=permit, expected_permit_sha256=expected_permit_sha256,
            max_workers=max_workers, mode=mode)
        checking.contract = strict_loads(canonical_payload(cohort_contract), max_bytes=2_100_000)
        require(set(checking.contract) == {'cohort_id', 'execution_contract_sha256', 'ledger_identity',
                'journal_root', 'transport_identity', 'task_specs'}, 'Closed V5 cohort contract required')
        child = [item for item in terminal_roster.children if item.cohort == cohort_contract['cohort_id']]
        require(len(child) == 1 and set(child[0].actors) == {
            actor for spec in cohort_contract['task_specs'] for actor in spec['actors']}, 'V5 preflight child identity differs')
        profiles = {name: checking._profile(name, worker) for name, worker in workers.items()}
        return {'protocol': PROTOCOL, 'mode': mode, 'operator_permit_sha256': expected_permit_sha256,
                'execution_design_sha256': checking.permit['execution_design_sha256'],
                'cohort_contract_sha256': digest(cohort_contract), 'profiles': profiles,
                'terminal_roster_sha256': terminal_roster.sha256, 'ledger_opened': False, 'provider_invoked': False}

    def _prepare_permit(self, cohort_contract: dict, incremental_cap_micro_usd: int,
                        expected_global_cap: int, expected_opening_usage: int, *,
                        workers: dict[str, OpenAIWorker], permit: dict | None,
                        expected_permit_sha256: str | None, max_workers: int, mode: str) -> None:
        self._validate_mode(mode)
        require(type(permit) is dict and set(permit) == v3._PERMIT_FIELDS
                and v3._hash(expected_permit_sha256), 'Explicit closed V5 permit required')
        self._permit_bytes = canonical_payload(permit)
        require(sha(self._permit_bytes) == expected_permit_sha256, 'V5 permit pin differs')
        self.permit = strict_loads(self._permit_bytes, max_bytes=2_100_000)
        self._permit_sha256, self._mode = expected_permit_sha256, mode
        profiles = {name: v3.profile_manifest(worker) for name, worker in workers.items()}
        require(self.permit['protocol'] == PERMIT_PROTOCOL and self.permit['mode'] == mode
                and type(self.permit['approval_ref']) is str and 1 <= len(self.permit['approval_ref']) <= 1024
                and self.permit['cohort_contract_sha256'] == digest(cohort_contract)
                and canonical_payload(self.permit['profiles']) == canonical_payload(profiles), 'V5 permit scope differs')
        for key, value in (('incremental_cap_micro_usd', incremental_cap_micro_usd),
                           ('expected_global_cap', expected_global_cap),
                           ('expected_opening_usage', expected_opening_usage), ('max_workers', max_workers)):
            require(type(value) is int and type(self.permit[key]) is int and self.permit[key] == value,
                    'V5 limits differ')
        require(0 <= expected_opening_usage <= expected_global_cap
                and 0 < incremental_cap_micro_usd <= expected_global_cap - expected_opening_usage,
                'V5 allowance exceeds cumulative cap')
        design = self.permit['execution_design']
        require(type(design) is dict and canonical_payload(design.get('profiles')) == canonical_payload(profiles)
                and type(design.get('max_workers')) is int and design['max_workers'] == max_workers
                and self.permit['execution_design_sha256'] == digest(design)
                and canonical_payload(design.get('terminal_roster')) == canonical_payload(self.terminal_roster.record()), 'V5 design differs')
        checked_policy(design.get('financial_closure_policy'))
        v3._action_limits(design.get('action_limits'))
        self._guard_evidence()

    def _guard_evidence(self) -> None:
        require(canonical_payload(self.permit) == self._permit_bytes, 'V5 permit changed')
        require(set(REQUIRED_SOURCES) <= set(self.permit['sources']), 'V5 source closure incomplete')
        v3._source_map(self.permit['sources'])
        if self._mode == 'live':
            require(type(self.permit['qualification']) is dict, 'V5 live qualification missing')
            validate_qualification(self.permit['qualification'], self.permit['execution_design'], self.permit['sources'])
        else:
            require(self.permit['qualification'] is None, 'Fixture cannot claim live qualification')

    def _finalize_config(self, config: dict) -> dict:
        result = super()._finalize_config(config)
        result = {**result, 'protocol':PROTOCOL,
            'financial_closure_policy':checked_policy(self.permit['execution_design'].get('financial_closure_policy')),
            'financial_closure_policy_sha256':CLOSURE_POLICY_SHA256}
        checked_financial_config(result)
        return result

    def _validate_terminal_config(self, db: sqlite3.Connection) -> None:
        super()._validate_terminal_config(db)
        checked_financial_config(self.config)

    def _prior_sealed_tasks(self, db: sqlite3.Connection) -> set[str]:
        allowed = super()._prior_sealed_tasks(db)
        for child in self.terminal_roster.children[:self.terminal_config['child_index']]:
            row = db.execute('SELECT config FROM financial_cohorts_v2 WHERE cohort=?',(child.cohort,)).fetchone()
            require(row is not None, 'Prior V5 financial origin is missing')
            checked_financial_config(strict_loads(row['config'],max_bytes=2_100_000))
        return allowed

    def _verify_terminal(self, actor: str, request_id: str) -> dict:
        proof = super()._verify_terminal(actor, request_id)
        # Preserve frozen accounting/provider joins, then require the exact
        # producer encoding for all four originals. Python numeric equality is
        # insufficient for journal identity and settlement fields.
        binding, reply, request = proof['binding'], proof['reply'], proof['worker_request']
        identity = {'schema_version':1, 'call_id':binding.call_id, 'reservation_id':binding.reservation_id,
                    'request_sha256':sha(journal_module._bytes(asdict(request)))}
        payload = proof['result_payload']
        result = {**identity, 'kind':payload['kind'], 'payload':payload['payload'],
                  'payload_sha256':sha(journal_module._bytes(payload['payload']))}
        result_raw = journal_module._bytes(result)
        expected = {'owner':journal_module._bytes(identity),
            'request':journal_module._bytes({**identity,'request':asdict(request)}), 'result':result_raw,
            'settled':journal_module._bytes({**identity,'result_sha256':sha(result_raw),'usage_units':reply.usage_units})}
        paths = self.journal.paths(binding.call_id)
        paths = {**paths,'owner':self.journal.root/(sha(binding.reservation_id.encode())+'.reservation.json')}
        for kind, raw in expected.items():
            _, actual = journal_module._read(paths[kind])
            require(actual == raw, 'V5 journal original bytes differ: '+kind)
        return proof

    def _quiescent(self, db: sqlite3.Connection, status: str) -> None:
        # Original result/publication/journal validation happens in inherited
        # prepare/seal BEFORE this joined transaction. Never nest that verifier.
        if self.active_executions or db.execute(
            "SELECT 1 FROM financial_actions_v2 WHERE cohort=? AND state IN ('pending','publication_pending') LIMIT 1",
            (self.cohort_id,)).fetchone():
            raise SealBusy('Active, queued or publication-pending work')
        self._audit_membership(db)
        for row in db.execute("""SELECT a.state,r.spent,r.amount,r.state AS reservation_state FROM financial_actions_v2 a
            JOIN reservations r ON r.id=a.reservation_id WHERE a.cohort=?""", (self.cohort_id,)):
            require(row['state'] in {'completed','failed','unknown'}, 'Unclassified admitted state')
            require(row['spent'] is not None or row['state'] == 'unknown', 'Unclassified conservative reservation')
            if status == 'completed':
                require(row['state'] in {'completed','failed'} and type(row['spent']) is int
                        and type(row['amount']) is int and 0 <= row['spent'] <= row['amount']
                        and row['reservation_state'] == 'settled', 'Unknown or unsettled action cannot produce successful closure')
        if status == 'completed':
            require(not self._halted(db) and not self.persistence_failed, 'Halted owner cannot claim successful closure')
        require(not db.execute("SELECT 1 FROM intents WHERE state='pending' LIMIT 1").fetchone(),
                'Pending Git transaction prevents terminal closure')
