"""V5 source/terminal adapter; unchanged authenticated V2 wire and 64-role cap."""
from __future__ import annotations

import os
from pathlib import Path
import sqlite3
from typing import Any

from .peer_financial_authority_v2 import CumulativeAuthorityV2
from .peer_financial_authority_v5 import CumulativeAuthorityV5, PROTOCOL as FINANCIAL_PROTOCOL
from .peer_financial_rpc_v2 import (
    FinancialRPC, FinancialRPCError, FinancialDenied, PROTOCOL as WIRE_PROTOCOL,
    SCHEMA_VERSION as WIRE_SCHEMA_VERSION, _bytes, _digest,
)
from .peer_financial_terminal_v1 import CohortSealed, digest, sha
from .peer_financial_terminal_v2 import checked_financial_config, CLOSURE_POLICY_SHA256

PROTOCOL = 'peer-financial-rpc-v5'
SOURCES = ('gossip_harness/peer_financial_rpc_v2.py', 'gossip_harness/peer_financial_rpc_v5.py')
_REPOSITORY = Path(__file__).resolve().parent.parent


def _sources() -> dict[str, str]:
    return {name: sha((_REPOSITORY / name).read_bytes()) for name in SOURCES}


class FinancialRPCV5(FinancialRPC):
    def __init__(self, finance: CumulativeAuthorityV5, capabilities: dict[str, str], *,
                 expected_financial_config_sha256: str, **kwargs: Any):
        self._financial_pin = _digest(expected_financial_config_sha256)
        self._source_identity = _sources()
        self._validate_current_finance(finance)
        with finance.terminal_gate, finance._active():
            with finance.ledger.atomic() as db:
                tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                registered = ('financial_rpc_config_v2' in tables and db.execute(
                    'SELECT 1 FROM financial_rpc_config_v2 WHERE cohort=?', (finance.cohort_id,)).fetchone())
                if not registered:
                    finance.assert_mutations_open(db)
            super().__init__(finance, capabilities, **kwargs)

    def _validate_current_finance(self, finance: CumulativeAuthorityV2) -> None:
        if type(finance) is not CumulativeAuthorityV5:
            raise FinancialRPCError('An exact V5 financial owner is required')
        checked_financial_config(finance.config)
        if (finance.owner_pid != os.getpid() or finance.closed or finance.closing
                or finance.config.get('protocol') != FINANCIAL_PROTOCOL
                or digest(finance.config) != self._financial_pin
                or finance.config_sha256 != self._financial_pin
                or finance.config.get('operator_permit') != finance.permit
                or finance.config.get('operator_permit_sha256') != digest(finance.permit)
                or any(finance.permit['sources'].get(name) != value for name, value in self._source_identity.items())
                or _sources() != self._source_identity):
            raise FinancialRPCError('V5 owner, permit or RPC source changed')

    def _validate_finance(self, finance: CumulativeAuthorityV2) -> None:
        self._validate_current_finance(finance)
        assert isinstance(finance, CumulativeAuthorityV5)
        finance._guard_evidence()
        if any(finance._profile(name, worker) != finance.profiles[name] for name, worker in finance.workers.items()):
            raise FinancialRPCError('V5 worker profile changed')

    def _finalize_config(self, config: dict) -> dict:
        assert isinstance(self.finance, CumulativeAuthorityV5)
        return {**config, 'protocol': PROTOCOL, 'schema_version': 5,
                'wire_protocol': WIRE_PROTOCOL, 'wire_schema_version': WIRE_SCHEMA_VERSION,
                'adapter_sources': self._source_identity, 'financial_protocol': FINANCIAL_PROTOCOL,
                'terminal_config_sha256': digest(self.finance.terminal_config),
                'financial_closure_policy_sha256': CLOSURE_POLICY_SHA256}

    def _request(self, envelope: Any) -> tuple[dict, dict, bytes]:
        parsed = super()._request(envelope)
        self._validate_current_finance(self.finance)
        return parsed

    def _insert(self, db: sqlite3.Connection, actor: str, request_id: str, raw: bytes) -> None:
        # Already in the RPC's BEGIN IMMEDIATE; never take the gate here.
        assert isinstance(self.finance, CumulativeAuthorityV5)
        try:
            self.finance.assert_mutations_open(db)
        except CohortSealed:
            raise FinancialDenied('cohort_sealed') from None
        super()._insert(db, actor, request_id, raw)

    def _dispatch(self, body: dict, parsed: dict, raw: bytes) -> dict:
        finance = self.finance
        assert isinstance(finance, CumulativeAuthorityV5)
        # This short gate also covers the missing-payload early return in V2.
        with finance.terminal_gate, finance._active():
            with finance.ledger.atomic() as db:
                row = self._row(db, body['actor'], body['request_id'])
                if row is not None:
                    self._retained(row, raw)
                financial = finance._row(db, body['actor'], body['request_id'])
                admitted = False
                if financial is not None:
                    if financial['identity'] != _bytes(body['payload']):
                        raise FinancialDenied('request_identity_conflict')
                    admitted = finance._reply(financial).state != 'waiting'
                if not admitted:
                    try:
                        finance.assert_mutations_open(db)
                    except CohortSealed:
                        raise FinancialDenied('cohort_sealed') from None
            return super()._dispatch(body, parsed, raw)
