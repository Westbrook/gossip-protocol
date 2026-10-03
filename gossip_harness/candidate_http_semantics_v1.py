"""Pure M1 HTTP value comparisons; no journal, execution or acceptance authority.

Inputs are caller-supplied values, including synthetic test values. A production
adapter must independently verify source-bound journal evidence before using
these comparisons. There is deliberately no ``authenticated`` boolean, receipt
loader, dispatch, registry publication, or aggregate product verdict here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
from typing import Literal

Disposition = Literal["pass", "fail", "unavailable", "unspecified"]
Shape = Literal["health", "documents", "document", "export", "jobs", "error", "unspecified"]
PROTOCOL = "candidate-http-semantics-v1"
# Capture once: a later on-disk edit must not relabel this imported implementation.
# This detects source drift; it supplies no execution or acceptance authority.
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
MAX_BODY_BYTES = 4 * 1024 * 1024  # Evaluator allocation bound, never a product cap.
MAX_COMPARE_DEPTH = 128
DOCUMENT_KEYS = frozenset(("document_id", "source_id", "source", "blob_id", "title", "text"))
JOB_KEYS = frozenset(("job_id", "epoch", "state", "total", "completed", "error"))
CONFLICT_CODES = frozenset(("source_changed", "job_conflict", "job_state", "stale_epoch"))
# This is an expectation-construction vocabulary from source, not an allowlist
# for observed codes. Unknown observed codes retain the general otherwise400 rule.
ASSIGNED_CODES = CONFLICT_CODES | frozenset((
    "not_found", "invalid_request", "io_error", "invalid_archive", "invalid_json",
    "invalid_source", "invalid_utf8", "invalid_batch", "too_large",
    "unsupported_type", "capacity",
))


@dataclass(frozen=True)
class Missing:
    reason: str

    def __post_init__(self) -> None:
        if type(self.reason) is not str or not self.reason:
            raise ValueError("missing evidence needs a nonempty string reason")


@dataclass(frozen=True)
class JsonNumber:
    text: str
    value: Decimal
    integer_token: bool


@dataclass(frozen=True)
class JsonObject:
    pairs: tuple[tuple[str, Node], ...]


Node = None | bool | str | JsonNumber | JsonObject | list["Node"]


@dataclass(frozen=True)
class ListenerFacts:
    """Decoded addresses for one fixed service port at one observed instant.

    ``limitation`` preserves incomplete table coverage even if an address was
    observed. These fields assert no process ownership or capture provenance.
    """

    addresses: tuple[str, ...]
    limitation: str | None = None

    def __post_init__(self) -> None:
        if type(self.addresses) is not tuple or any(type(address) is not str for address in self.addresses):
            raise TypeError("listener addresses must be an immutable tuple of strings")
        if self.limitation is not None and (type(self.limitation) is not str or not self.limitation):
            raise ValueError("listener limitation must be None or a nonempty string")


@dataclass(frozen=True)
class ResponseFacts:
    """Body bytes must be the complete transfer-decoded body, else use Missing.

    A final status may remain independently known when headers/body are Missing.
    The future adapter, not this dataclass, establishes each value's eligibility.
    """

    status: int | Missing
    headers: tuple[tuple[str, str], ...] | Missing
    body: bytes | Missing
    listener_before: ListenerFacts | Missing = Missing("listener snapshot not supplied")
    listener_after: ListenerFacts | Missing = Missing("listener snapshot not supplied")

    def __post_init__(self) -> None:
        if type(self.status) is not Missing and (
            type(self.status) is not int or not 200 <= self.status <= 599
        ):
            raise ValueError("status must be an exact final HTTP status integer 200..599 or Missing")
        if type(self.headers) is not Missing:
            if type(self.headers) is not tuple or any(
                type(pair) is not tuple or len(pair) != 2
                or any(type(value) is not str for value in pair)
                for pair in self.headers
            ):
                raise TypeError("headers must be an immutable tuple of two-string tuples or Missing")
        if type(self.body) not in (bytes, Missing):
            raise TypeError("body must be immutable bytes or Missing")
        if any(type(value) not in (ListenerFacts, Missing)
               for value in (self.listener_before, self.listener_after)):
            raise TypeError("listener facts must be ListenerFacts or Missing")


@dataclass(frozen=True)
class Expectation:
    shape: Shape
    status: int | None
    expected_body: bytes | None
    code: str | None = None

    def __post_init__(self) -> None:
        if type(self.shape) is not str or self.shape not in (
            "health", "documents", "document", "export", "jobs", "error", "unspecified"
        ):
            raise ValueError("unknown semantic expectation shape")
        if self.status is not None and type(self.status) is not int:
            raise TypeError("expected status must be an exact integer or None")
        if self.code is not None and type(self.code) is not str:
            raise TypeError("expected code must be a string or None")
        if self.expected_body is not None and type(self.expected_body) is not bytes:
            raise ValueError("expected bytes must be immutable")
        if self.shape == "error":
            if self.expected_body is not None:
                raise ValueError("error expectations use explicit code facets")
            if self.code is None:
                if self.status not in (None, 400, 404):
                    raise ValueError("unsupported unclassified-error status")
            elif self.code not in ASSIGNED_CODES or self.status != status_for_code(self.code):
                raise ValueError("unsupported error classification or status mapping")
        elif self.status != 200 or self.code is not None:
            raise ValueError("M1 success must prescribe status200 without an error code")
        elif self.shape == "health":
            if self.expected_body != b'{"status":"ok","schema":0}':
                raise ValueError("M1 health expectation is fixed")
        elif self.shape == "unspecified":
            if self.expected_body is not None:
                raise ValueError("unspecified wrapper cannot prescribe a body")
        else:
            _validate_success_body(self.shape, self.expected_body)


@dataclass(frozen=True)
class Facet:
    name: str
    disposition: Disposition
    reason: str
    citations: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.name) is not str or not self.name or type(self.reason) is not str:
            raise ValueError("facet name and reason must be strings; name must be nonempty")
        if type(self.disposition) is not str or self.disposition not in (
            "pass", "fail", "unavailable", "unspecified"
        ):
            raise ValueError("unknown facet disposition")
        if type(self.citations) is not tuple or any(type(value) is not str for value in self.citations):
            raise TypeError("facet citations must be an immutable tuple of strings")


@dataclass(frozen=True)
class Comparison:
    facets: tuple[Facet, ...]
    protocol: str = field(default=PROTOCOL, init=False)
    authority: str = field(default="pure_value_comparison_only", init=False)
    product_verdict: str = field(default="not_evaluated", init=False)

    def __post_init__(self) -> None:
        if type(self.facets) is not tuple or any(type(value) is not Facet for value in self.facets):
            raise TypeError("comparison facets must be an immutable tuple of Facet values")
        if len({value.name for value in self.facets}) != len(self.facets):
            raise ValueError("comparison facet names must be unique")

    def facet(self, name: str) -> Facet:
        return next(row for row in self.facets if row.name == name)


def _number(text: str) -> JsonNumber:
    return JsonNumber(text, Decimal(text), not any(c in text for c in ".eE"))


def _constant(text: str) -> Node:
    raise ValueError(f"non-JSON numeric constant: {text}")


def parse_json(raw: bytes) -> Node:
    """Keep all object members and numeric lexemes; never use last-key-wins."""
    return json.loads(raw, parse_int=_number, parse_float=_number,
                      parse_constant=_constant,
                      object_pairs_hook=lambda pairs: JsonObject(tuple(pairs)))


def semantic_compare(actual: Node, expected: Node, depth: int = 0) -> Disposition:
    """Ignore legal spelling/key order; preserve arrays and bool vs number.

    Duplicate fields remain unspecified. An independently decisive mismatch
    elsewhere wins over that ambiguity. M1 response numeric value equality does
    not invent a later milestone's integer-token spelling rule.
    """
    if depth > MAX_COMPARE_DEPTH:
        return "unavailable"
    if type(actual) is not type(expected):
        return "fail"
    if isinstance(actual, JsonObject) and isinstance(expected, JsonObject):
        wanted = dict(expected.pairs)
        if len(wanted) != len(expected.pairs):
            raise ValueError("expected object cannot have duplicate members")
        keys = {key for key, _ in actual.pairs}
        if keys != set(wanted):
            return "fail"
        results: list[Disposition] = []
        for key, value in wanted.items():
            found = [item for name, item in actual.pairs if name == key]
            results.append(semantic_compare(found[0], value, depth + 1)
                           if len(found) == 1 else "unspecified")
        return _combine(results)
    if isinstance(actual, list) and isinstance(expected, list):
        if len(actual) != len(expected):
            return "fail"
        return _combine([semantic_compare(a, e, depth + 1)
                         for a, e in zip(actual, expected, strict=True)])
    if isinstance(actual, JsonNumber) and isinstance(expected, JsonNumber):
        return "pass" if actual.value == expected.value else "fail"
    return "pass" if actual == expected else "fail"


def _combine(results: list[Disposition]) -> Disposition:
    for disposition in ("fail", "unavailable", "unspecified"):
        if disposition in results:
            return disposition
    return "pass"


def _plain(node: Node) -> object:
    """Only for trusted expectation validation; never unwrap observed responses."""
    if isinstance(node, JsonObject):
        if len({key for key, _ in node.pairs}) != len(node.pairs):
            raise ValueError("expectations cannot contain duplicate members")
        return {key: _plain(value) for key, value in node.pairs}
    if isinstance(node, list):
        return [_plain(value) for value in node]
    return node


def _document(value: object) -> bool:
    return isinstance(value, dict) and set(value) == DOCUMENT_KEYS and all(
        isinstance(item, str) for item in value.values())


def _job(value: object) -> bool:
    return isinstance(value, dict) and set(value) == JOB_KEYS and all(
        isinstance(value[key], JsonNumber) for key in ("epoch", "total", "completed")
    ) and isinstance(value["job_id"], str) and value["state"] in (
        "queued", "running", "completed", "cancelled", "failed"
    ) and (value["error"] is None or isinstance(value["error"], str))


def _validate_success_body(shape: Shape, expected_body: bytes | None) -> None:
    if shape not in ("documents", "document", "export", "jobs") or not isinstance(expected_body, bytes):
        raise ValueError("unsupported M1 success expectation")
    if len(expected_body) > MAX_BODY_BYTES:
        raise ValueError("expected body exceeds observer allocation bound")
    value = _plain(parse_json(expected_body))
    valid = _document(value) if shape == "document" else False
    if isinstance(value, dict):
        docs = value.get("documents")
        documents = isinstance(docs, list) and all(_document(item) for item in docs)
        if shape == "documents":
            valid = (set(value) == {"documents", "total"} and documents
                     and isinstance(value["total"], JsonNumber))
        elif shape == "export":
            valid = (set(value) == {"format", "documents"} and documents
                     and value["format"] == "local-research-library-v0")
        elif shape == "jobs":
            jobs = value.get("jobs")
            valid = (set(value) == {"jobs"} and isinstance(jobs, list)
                     and all(_job(item) for item in jobs))
    if not valid:
        raise ValueError("expected body does not have the source-supported shape")


def success(shape: Shape, expected_body: bytes | None = None) -> Expectation:
    """Catalog supplies independent expected state; this does not derive it.

    Use unspecified for HTTP import, submit, single-job and action wrappers.
    Their state effects require separately authenticated public-state readback.
    """
    if shape == "health":
        if expected_body is not None:
            raise ValueError("M1 health expectation is fixed")
        expected_body = b'{"status":"ok","schema":0}'
    return Expectation(shape, 200, expected_body)


def classified_error(code: str) -> Expectation:
    """The future frozen catalog must justify this code for its isolated cause."""
    if code not in ASSIGNED_CODES:
        raise ValueError("code is not an explicitly assigned M1 error classification")
    return Expectation("error", status_for_code(code), None, code)


def unclassified_error(status: int | None = None) -> Expectation:
    """400: isolated generic validation; 404: unsupported route; None: unresolved.

    This fixes no unknown code. Caller must establish that a response is required
    to be an error; this constructor does not inspect the request or product.
    """
    if status not in (None, 400, 404):
        raise ValueError("unsupported fixed unclassified-error status")
    return Expectation("error", status, None)


def status_for_code(code: str) -> int:
    return 404 if code == "not_found" else 409 if code in CONFLICT_CODES else 400


def _facet(name: str, disposition: Disposition, reason: str, *citations: str) -> Facet:
    return Facet(name, disposition, reason, citations)


def listener(name: str, facts: ListenerFacts | Missing) -> Facet:
    citation = "V0-CLI-LISTENER"
    if isinstance(facts, Missing):
        return _facet(name, "unavailable", facts.reason, citation)
    if any(address != "127.0.0.1" for address in facts.addresses):
        return _facet(name, "fail", "observed service-port address differs from 127.0.0.1", citation)
    if facts.limitation or not facts.addresses:
        return _facet(name, "unavailable", facts.limitation or "no service listener observed", citation)
    return _facet(name, "pass", "supplied snapshot lists only 127.0.0.1; ownership unproved", citation)


def _header(facts: ResponseFacts) -> Facet:
    if isinstance(facts.headers, Missing):
        return _facet("response_media_type", "unavailable", facts.headers.reason, "V0-HTTP")
    values = [value for name, value in facts.headers if name.lower() == "content-type"]
    return _facet("response_media_type", "unspecified",
                  f"response Content-Type values={values!r}; exact response header requirement is not explicit",
                  "V0-HTTP", "INV-V0-HTTP-01")


def _error_facets(node: Node, expectation: Expectation, facts: ResponseFacts) -> list[Facet]:
    envelope: Disposition = "pass"
    code: str | None = None
    if not isinstance(node, JsonObject) or {key for key, _ in node.pairs} != {"error"}:
        envelope = "fail"
    elif len(node.pairs) != 1:
        envelope = "unspecified"
    elif not isinstance(node.pairs[0][1], str):
        envelope = "fail"
    if isinstance(node, JsonObject):
        values = [value for key, value in node.pairs if key == "error"]
        if len(values) == 1 and isinstance(values[0], str):
            code = values[0]
    rows = [_facet("body_shape_value", envelope, "exact error envelope with a string CODE", "V0-HTTP")]
    disposition: Disposition = "unspecified" if expectation.code is None else (
        "unavailable" if code is None else "pass" if code == expectation.code else "fail")
    rows.append(_facet("error_code", disposition, "only independently assigned exact codes are compared",
                       "V0-HTTP", "M1-HTTP-STATUS"))
    relation: Disposition = "unavailable" if code is None or isinstance(facts.status, Missing) else (
        "pass" if facts.status == status_for_code(code) else "fail")
    if expectation.code is None and expectation.status == 404:
        relation = "unspecified"  # Explicit unsupported-route status overrides generic mapping.
    rows.append(_facet("reported_code_status_relation", relation,
                       "generic mapping does not assign a code; unsupported-route404 is an explicit override",
                       "V0-HTTP", "M1-HTTP-STATUS"))
    return rows


def _representation_limit(facts: ResponseFacts) -> str | None:
    if isinstance(facts.headers, Missing):
        return "response content-coding metadata unavailable"
    codings = [coding.strip().lower() for name, value in facts.headers
               if name.lower() == "content-encoding" for coding in value.split(",")]
    if any(coding != "identity" for coding in codings):
        return "response content-coding exceeds this observer's identity-only decoder"
    return None


def compare(facts: ResponseFacts, expectation: Expectation) -> Comparison:
    """Judge facets only. No source identity, request effect or product closure."""
    rows: list[Facet] = []
    status: Disposition = "unspecified" if expectation.status is None else (
        "unavailable" if isinstance(facts.status, Missing) else
        "pass" if facts.status == expectation.status else "fail")
    rows.append(_facet("status", status, "independent final status comparison", "V0-HTTP", "M1-HTTP-STATUS"))
    rows.append(_header(facts))
    missing = facts.body if isinstance(facts.body, Missing) else None
    node: Node = None
    syntax: Disposition = "unavailable"
    reason = missing.reason if missing else ""
    if missing is None:
        assert isinstance(facts.body, bytes)
        representation_limit = _representation_limit(facts)
        if representation_limit:
            reason = representation_limit
        elif len(facts.body) > MAX_BODY_BYTES:
            reason = "semantic observer allocation bound exceeded"
        else:
            try:
                node = parse_json(facts.body)
                syntax, reason = "pass", "complete JSON text decoded without discarding object members"
            except (UnicodeDecodeError, RecursionError, InvalidOperation):
                reason = "representation exceeds the supported decoder; no product failure inferred"
            except (json.JSONDecodeError, ValueError):
                syntax, reason = "fail", "complete captured body is not JSON syntax"
    rows.append(_facet("json_syntax", syntax, reason, "V0-HTTP"))
    if expectation.shape == "unspecified":
        rows.append(_facet("body_shape_value", "unspecified", "exact outer HTTP wrapper is not explicit",
                           "M1-HTTP-INTERFACES", "V0-ADAPTER"))
    elif syntax != "pass":
        rows.append(_facet("body_shape_value", "unavailable", "no complete decodable JSON value", "V0-HTTP"))
        if expectation.shape == "error":
            rows.extend([
                _facet("error_code", "unspecified" if expectation.code is None else "unavailable",
                       "no independently readable code", "V0-HTTP"),
                _facet("reported_code_status_relation", "unavailable", "no independently readable code", "V0-HTTP"),
            ])
    elif expectation.shape == "error":
        rows.extend(_error_facets(node, expectation, facts))
    else:
        if expectation.expected_body is None:
            raise ValueError("supported shape needs independent expected bytes")
        outcome = semantic_compare(node, parse_json(expectation.expected_body))
        rows.append(_facet("body_shape_value", outcome, "exact source-supported shape and independent expected value",
                           "V0-IDENTITY", "V0-QUERY-EXPORT", "V0-HTTP", "M1-HTTP-INTERFACES"))
    rows.extend((listener("listener_before", facts.listener_before), listener("listener_after", facts.listener_after)))
    return Comparison(tuple(rows))
