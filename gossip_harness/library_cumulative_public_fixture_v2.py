"""Prospective-v2 public positive adapter, explicitly derived from the v1 fixture.

The unchanged examples do not establish the six new v2 persistence obligations.
The grammar adds the declared signed64 positive-token bound; owned temporary
fixture paths are canonicalized before passing them to the strict product.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from typing import Any

from .library_cumulative_public_fixture_v1 import (
    PUBLIC_PATHS, PUBLIC_TEST_SOURCE as V1_PUBLIC_TEST_SOURCE,
    WORKFLOW_FORMAT, public_manifest as v1_public_manifest,
    workflow_contract as v1_workflow_contract,
)

FIXTURE_VERSION = "local-research-library-cumulative-public-v2"
CONTRACT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
COUNTER_MAX = 9223372036854775807
_TOKEN_CHECK = 'type(op[key]) is not int or op[key] < 1'
if V1_PUBLIC_TEST_SOURCE.count(_TOKEN_CHECK) != 1:
    raise ValueError("The frozen public token-check source changed")
PUBLIC_TEST_SOURCE = V1_PUBLIC_TEST_SOURCE.replace(
    _TOKEN_CHECK, 'type(op[key]) is not int or not 1 <= op[key] <= 9223372036854775807')
PUBLIC_TEST_SOURCE = PUBLIC_TEST_SOURCE.replace(
    '"""Public positive cumulative checks; not exhaustive or independent acceptance.',
    '"""Public positive cumulative v2 checks; not exhaustive or independent acceptance.')

# These are fixture-owned TemporaryDirectory values, never caller source paths.
for _fixture_path in ("home = Path(directory)", "home = Path(temporary)"):
    if PUBLIC_TEST_SOURCE.count(_fixture_path) != 1:
        raise ValueError("Frozen temporary fixture construction changed")
    PUBLIC_TEST_SOURCE = PUBLIC_TEST_SOURCE.replace(_fixture_path, _fixture_path + ".resolve()")


def workflow_contract() -> dict[str, Any]:
    value = v1_workflow_contract()
    value["positive_token_max"] = COUNTER_MAX
    value["semantics"].append(
        "Epoch and expected_version are exact integers 1..9223372036854775807; booleans and floats are rejected.")
    return value


def public_manifest() -> dict[str, Any]:
    raw = (Path(__file__).resolve().parents[1] / "library-cumulative-product-v2.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != CONTRACT_SHA256:
        raise ValueError("normative v2 contract changed; version the public fixture")
    contract = json.loads(raw)
    previous = v1_public_manifest()
    value = deepcopy(previous)
    value.update({
        "format": FIXTURE_VERSION,
        "product_contract_sha256": CONTRACT_SHA256,
        "normative_contract_utf8": raw.decode("utf-8"),
        "runtime": deepcopy(contract["inherited_contract"]),
        "ownership": deepcopy(contract["release_ownership"]),
        "workflow": workflow_contract(),
        "adapter_sha256": hashlib.sha256(PUBLIC_TEST_SOURCE.encode()).hexdigest(),
        "derivation": {
            "fixture_format": previous["format"],
            "product_contract_sha256": previous["product_contract_sha256"],
            "adapter_sha256": previous["adapter_sha256"],
            "changes": ["Explicit v2 product binding and classification", "Signed64 positive-token grammar",
                        "Canonicalize fixture-owned temporary roots before strict product path checks"],
            "inherited_positive_expected_values": "unchanged",
        },
    })
    value["coverage_gaps"].append(
        "The inherited positive histories do not qualify the six v2 persistence amendments")
    return value


def public_files() -> dict[str, str]:
    return {"cumulative-public-contract.json": json.dumps(public_manifest(), ensure_ascii=False,
                sort_keys=True, indent=2, allow_nan=False) + "\n",
            "test_cumulative_public.py": PUBLIC_TEST_SOURCE}


__all__ = ["CONTRACT_SHA256", "COUNTER_MAX", "FIXTURE_VERSION", "PUBLIC_PATHS",
           "PUBLIC_TEST_SOURCE", "WORKFLOW_FORMAT", "public_files", "public_manifest", "workflow_contract"]
