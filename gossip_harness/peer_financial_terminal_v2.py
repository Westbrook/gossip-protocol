"""V5 closure policy over the frozen V4 structural seal format.

A V4-shaped seal is structural evidence only. Every consumer of repaired-success
semantics must authenticate the V5 financial config and this exact policy.
No policy record grants acceptance or authorizes live admission.
"""
from __future__ import annotations
from contextlib import closing
import sqlite3
from typing import Any
from .candidate_checkpoint_chain_v1 import CheckpointChain, PrefixCommitment
from .peer_financial_authority_v2 import canonical_payload
from .peer_financial_terminal_v1 import (TerminalRoster, ChildTerminalSeal, ChildTerminalSealRequest,
    EvidenceReference, seal_name, seal_row, authenticate_seal, authenticate_request, census, sha, digest, require, prefix)
from .peer_store_v1 import strict_loads

PROTOCOL = 'financial-terminal-policy-v2'
FINANCIAL_PROTOCOL = 'peer-financial-authority-v5'
CLOSURE_POLICY = {'protocol':'financial-repaired-project-closure-v1',
    'completed_action_states':['completed','failed'],
    'known_failure':'original-request-result-publication-and-settlement-required',
    'failure_accounting':'retain-count-and-charge-every-failed-call',
    'release':'successful-dispatch-only',
    'project_success':'original-process-and-project-terminal-request-required',
    'unknown':'never-completed-no-implicit-retry',
    'blocked':['active-executions','pending','publication_pending','unknown','halted-owner','persistence-failure','pending-git'],
    'seal_format':'financial-normal-seal-v4',
    'acceptance_authority':False}
_POLICY_BYTES = canonical_payload(CLOSURE_POLICY)
CLOSURE_POLICY_SHA256 = sha(_POLICY_BYTES)


def checked_policy(value: Any) -> dict:
    require(type(value) is dict and canonical_payload(value) == _POLICY_BYTES,
            'Exact versioned repaired-closure policy required')
    return strict_loads(_POLICY_BYTES)


def checked_financial_config(config: dict) -> None:
    require(type(config) is dict and config.get('protocol') == FINANCIAL_PROTOCOL,
            'Exact V5 financial origin required')
    checked_policy(config.get('financial_closure_policy'))
    require(config.get('financial_closure_policy_sha256') == CLOSURE_POLICY_SHA256,
            'V5 financial closure policy pin differs')
    permit = config.get('operator_permit')
    require(type(permit) is dict and permit.get('protocol') == 'peer-financial-operator-permit-v5'
            and config.get('operator_permit_sha256') == digest(permit), 'V5 operator permit origin differs')
    assert isinstance(permit, dict)
    design = permit.get('execution_design')
    require(type(design) is dict and permit.get('execution_design_sha256') == digest(design),
            'V5 execution design differs')
    assert isinstance(design, dict)
    checked_policy(design.get('financial_closure_policy'))


def verified_study_barrier(roster: TerminalRoster, chain: CheckpointChain,
                           expected: PrefixCommitment, receipts: tuple[ChildTerminalSeal, ...], *,
                           existing_ledger_path: Any, expected_ledger_identity: dict) -> dict:
    """Authenticate one original-ledger snapshot; never a private quality verdict."""
    chain.validate_boundary(expected=expected)
    require(type(receipts) is tuple and len(receipts) == 6, 'All six original child publications required')
    from .peer_financial_authority_v2 import ledger_identity
    require(ledger_identity(existing_ledger_path) == expected_ledger_identity, 'Study ledger identity differs')
    # One explicit read transaction observes all six cohorts in one SQLite
    # snapshot. Path identity is checked around that observation; this does not
    # claim immunity to a transient host-controlled replace/restore (ABA).
    with closing(sqlite3.connect('file:' + str(existing_ledger_path) + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        require(ledger_identity(existing_ledger_path) == expected_ledger_identity,
                'Study ledger identity changed while opening')
        db.execute('BEGIN')
        ids = []
        for child, receipt in zip(roster.children, receipts, strict=True):
            require(type(receipt) is ChildTerminalSeal and receipt.publication_name == seal_name(child.cohort)
                    and chain.read(receipt.publication_name) == receipt.raw, 'Original child publication differs')
            value = authenticate_seal(chain, receipt.raw)
            row = seal_row(db, child.cohort)
            financial = db.execute('SELECT config,config_sha FROM financial_cohorts_v2 WHERE cohort=?',
                                   (child.cohort,)).fetchone()
            require(row is not None and row['receipt'] == receipt.raw and row['receipt_sha'] == sha(receipt.raw)
                    and row['seal_id'] == value['seal_id'] and row['snapshot_sha'] == sha(row['snapshot'])
                    and row['snapshot_sha'] == value['snapshot_sha256']
                    and row['boundary'] == canonical_payload(value['boundary'])
                    and financial is not None and sha(financial['config']) == financial['config_sha']
                    and financial['config_sha'] == value['financial_config_sha256']
                    and census(db, child.cohort, financial['config_sha']) == row['snapshot'],
                    'Original SQL seal or financial census differs')
            config_value = strict_loads(financial['config'], max_bytes=2_100_000)
            checked_financial_config(config_value)
            require(config_value['contract']['execution_contract_sha256'] == roster.execution_contract_sha256,
                    'Child execution contract differs from prospective study roster')
            require(config_value['contract']['ledger_identity'] == expected_ledger_identity
                    and config_value['terminal_config'] == value['terminal_config'], 'Financial origin differs')
            config = value['terminal_config']
            require(config['roster'] == roster.record() and config['child'] == child.record(), 'Global child identity differs')
            request = value['request']
            authenticate_request(chain, config, ChildTerminalSealRequest(
                tuple(EvidenceReference(**item) for item in request['role_stops']),
                EvidenceReference(**request['terminal']), request['purpose']))
            ids.append(value['seal_id'])
        require(len(set(ids)) == 6, 'Duplicate original seal')
        require(ledger_identity(existing_ledger_path) == expected_ledger_identity,
                'Study ledger identity changed during observation')
        db.rollback()
    chain.require_current()
    return {'protocol': PROTOCOL, 'roster_sha256': roster.sha256, 'seal_ids': ids,
            'role_count': 96, 'checkpoint': prefix(expected), 'acceptance_authority': False,
            'financial_closure_policy_sha256': CLOSURE_POLICY_SHA256, 'financial_protocol': FINANCIAL_PROTOCOL}
