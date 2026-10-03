"""Source-bound RPC adapter for an already permitted V3 financial owner.

The role-facing wire remains the closed V2 protocol and schema. Its typed
client, HMAC envelopes, response nonce binding, frame/deadline limits and durable
request journal are reused unchanged. This adapter changes only the admitted
owner type and the immutable service registration; it cannot activate a live
owner, load credentials, grant funding or replace a qualification permit.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any, Callable

from .peer_financial_authority_v2 import CumulativeAuthorityV2
from .peer_financial_authority_v3 import CumulativeAuthorityV3, PROTOCOL as FINANCIAL_PROTOCOL, digest
from .peer_financial_rpc_v2 import (
    MAX_HANDLERS, MAX_REQUESTS_PER_PRINCIPAL, PROTOCOL as WIRE_PROTOCOL,
    SCHEMA_VERSION as WIRE_SCHEMA_VERSION, FinancialRPC as FinancialRPCV2,
    FinancialRPCError, _digest,
)
from .peer_project_contract_v2 import ActionRequest

PROTOCOL = "peer-financial-rpc-v3"
SCHEMA_VERSION = 3
_REPOSITORY = Path(__file__).resolve().parent.parent
SOURCES = ("gossip_harness/peer_financial_rpc_v2.py", "gossip_harness/peer_financial_rpc_v3.py")


def _sources() -> dict[str, str]:
    return {name: hashlib.sha256((_REPOSITORY / name).read_bytes()).hexdigest() for name in SOURCES}


class FinancialRPCV3(FinancialRPCV2):
    """Exact V3 ownership with unchanged bounded authenticated V2 RPC semantics."""
    def __init__(self, finance: CumulativeAuthorityV3, capabilities: dict[str, str], *,
                 expected_financial_config_sha256: str,
                 request_guard: Callable[[ActionRequest], None], request_guard_sha256: str,
                 max_requests_per_principal: int = MAX_REQUESTS_PER_PRINCIPAL,
                 max_handlers: int = MAX_HANDLERS,
                 crash_hook: Callable[[str, str], None] | None = None):
        self._financial_pin = _digest(expected_financial_config_sha256)
        self._source_identity = _sources()
        super().__init__(finance, capabilities, request_guard=request_guard,
                         request_guard_sha256=request_guard_sha256,
                         max_requests_per_principal=max_requests_per_principal,
                         max_handlers=max_handlers, crash_hook=crash_hook)

    def _validate_current_finance(self, finance: CumulativeAuthorityV2) -> None:
        if type(finance) is not CumulativeAuthorityV3:
            raise FinancialRPCError("An exact permitted V3 financial owner is required")
        if (finance.owner_pid != os.getpid() or finance.closing or finance.closed
                or finance.config.get("protocol") != FINANCIAL_PROTOCOL
                or finance.config.get("mode") not in {"fixture", "live"}
                or digest(finance.config) != self._financial_pin
                or finance.config_sha256 != self._financial_pin
                or finance.config.get("operator_permit_sha256") != digest(finance.permit)
                or finance.config.get("operator_permit") != finance.permit
                or finance.config.get("mode") != finance.permit.get("mode")
                or finance.config.get("execution_design_sha256") != finance.permit.get("execution_design_sha256")
                or any(finance.permit["sources"].get(name) != sha for name, sha in self._source_identity.items())
                or _sources() != self._source_identity):
            raise FinancialRPCError("Pinned financial ownership, permit or RPC sources differ")

    def _validate_finance(self, finance: CumulativeAuthorityV2) -> None:
        self._validate_current_finance(finance)
        assert isinstance(finance, CumulativeAuthorityV3)
        finance._guard_evidence()
        if any(finance._profile(name, worker) != finance.profiles[name]
               for name, worker in finance.workers.items()):
            raise FinancialRPCError("Financial worker profiles changed before RPC registration")

    def _finalize_config(self, config: dict) -> dict:
        return {**config, "protocol": PROTOCOL, "schema_version": SCHEMA_VERSION,
                "wire_protocol": WIRE_PROTOCOL, "wire_schema_version": WIRE_SCHEMA_VERSION,
                "adapter_sources": self._source_identity,
                "financial_protocol": FINANCIAL_PROTOCOL, "financial_mode": self.finance.config["mode"],
                "financial_operator_permit_sha256": self.finance.config["operator_permit_sha256"],
                "execution_design_sha256": self.finance.config["execution_design_sha256"]}

    def _request(self, envelope: Any) -> tuple[dict, dict, bytes]:
        parsed = super()._request(envelope)
        self._validate_current_finance(self.finance)
        return parsed
