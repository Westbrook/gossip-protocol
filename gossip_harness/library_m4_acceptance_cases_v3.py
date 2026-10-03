"""Narrow v3 observation correction for the explicit versioned export route.

V2 evidence and definitions remain frozen. Its execution exposed serialized JSON
bytes in the status/body pair returned by Service.request for POST /api/v1/export.
The public contract specifies the JSON response value, not the internal Python
body representation; actual HTTP canonical bytes have a separate evidence lane.
This correction follows observed v2 failure and is not an unseen/held-out oracle.
No M4 implementation or implementation tests were inspected for this correction.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
from typing import Any

from gossip_harness import library_m4_acceptance_cases_v2 as prior

PROTOCOL = "library-m4-independent-cases-v3"
PURPOSE = prior.PURPOSE
CONTRACT_SHA256 = prior.CONTRACT_SHA256
REQUIREMENT_IDS = prior.REQUIREMENT_IDS
SCORER_PROTOCOL = prior.SCORER_PROTOCOL
PRIOR_SOURCE_SHA256 = "aa735cb889b09f29d97f771830bdcaaf637060a97c333112ff950437148accba"
PRIOR_CASES_SHA256 = "87104867f4bfdf573a7ff05d6af09e03c0afe2f48270ca47ed8da285f77139d5"
LIMITATIONS = (*prior.LIMITATIONS,
    "V3 decodes only a byte-valued status/body pair from explicit Service.request POST /api/v1/export; no other route or shape is normalized.",
    "This route observation checks the specified JSON value; canonical HTTP wire bytes remain separately qualified.",
)


def _adapter() -> str:
    old = '        return result\n    except LibraryError as error:'
    new = '''        if (action["target"] == "service" and action["method"] == "request"
                and len(action["args"]) == 3 and action["args"][:2] == ["POST", "/api/v1/export"]
                and type(result) is tuple and len(result) == 2
                and type(result[0]) is int and type(result[1]) is bytes):
            result = (result[0], strict_json(result[1]))
        return result
    except LibraryError as error:'''
    if prior.CHILD_ADAPTER.count(old) != 1:
        raise ValueError("Frozen v2 adapter seam changed")
    return prior.CHILD_ADAPTER.replace(old, new)


CHILD_ADAPTER = _adapter()
acceptance_cases = prior.acceptance_cases
score_case = prior.score_case


def registry_manifest() -> dict[str, Any]:
    manifest = deepcopy(prior.registry_manifest())
    manifest.update(protocol=PROTOCOL, prior_protocol=prior.PROTOCOL,
                    prior_source_sha256=PRIOR_SOURCE_SHA256, prior_cases_sha256=PRIOR_CASES_SHA256,
                    adapter_sha256=hashlib.sha256(CHILD_ADAPTER.encode()).hexdigest(),
                    limitations=list(LIMITATIONS))
    manifest["corrections"].append({
        "id": "versioned-export-route-json-representation",
        "scope": "Service.request('POST', '/api/v1/export', body) with a two-element tuple, integer status and bytes body only",
        "reason": "The declared POST route returns the exact v4 JSON payload; serialized internal body bytes need strict decoding before host observation comparison.",
        "authority": ["library-cumulative-product-v1.json:interfaces M4 POST /api/v1/export",
                      "docs/library-cumulative-product-v1.md:M4 routes and commands"],
        "unchanged": "All 18 case inputs, expected values and host scorer; all statuses, other methods/routes, non-byte bodies and malformed shapes remain exact.",
    })
    return manifest
