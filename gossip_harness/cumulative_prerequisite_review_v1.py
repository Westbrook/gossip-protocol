"""Protected original review delivery, distinct from candidate observations.

Only the host enrolls reviewer originals. Hashes and checkpoints authenticate
bytes and order, not authorship or semantic judgment. Tests may exercise this
mechanism with synthetic originals but cannot manufacture independent approval.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from functools import wraps
from pathlib import Path
from typing import Any, Callable, ParamSpec, TypeVar

from . import candidate_observation_admission_v1 as admission
from . import candidate_scope_consumer_v1 as consumer
from . import candidate_checkpoint_chain_v1 as checkpoint
from . import candidate_http_journal_v3 as journal
from .candidate_checkpoint_head_v1 import ExternalHead
from . import project_acceptance_registry_v1 as registry

PROTOCOL = 'cumulative-prerequisite-review-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
MAX_BYTES = 4 * 1024 * 1024
DUTIES = {
    ('candidate_source_promotion', 'integration'): (
        'exact-final-source', 'original-git-cas-lineage', 'authenticated-scoped-package-reviews',
        'actual-final-integration', 'original-purpose-and-provenance'),
    ('control_review', 'package'): (
        'exact-control-source', 'package-semantics', 'original-probe-observations', 'original-purpose-and-provenance'),
    ('control_review', 'integration'): (
        'exact-control-source', 'cross-package-semantics', 'original-probe-observations', 'original-purpose-and-provenance'),
    ('admission_fixture_review', 'admission'): (
        'complete-target-suite-lineages', 'authored-fixture-and-expectations', 'runtime-isolation',
        'negative-controls', 'original-purpose-and-provenance'),
}
require = consumer.require


_P = ParamSpec('_P')
_R = TypeVar('_R')


def normalized(operation: Callable[_P, _R]) -> Callable[_P, _R]:
    @wraps(operation)
    def guarded(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return operation(*args, **kwargs)
        except (consumer.AuthorityError, consumer.AuthorityUnavailable):
            raise
        except (checkpoint.ChainUnknown, admission.AdmissionUnavailable, OSError) as error:
            raise consumer.AuthorityUnavailable(str(error)) from error
        except (ValueError, TypeError, KeyError, RecursionError) as error:
            raise consumer.AuthorityError(str(error)) from error
    return guarded


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def _read(raw: bytes) -> dict[str, Any]:
    require(len(raw) <= MAX_BYTES, 'Review original exceeds finite bound')
    value = journal.decode(raw,max_bytes=MAX_BYTES)
    require(type(value) is dict and encoded(value) == raw, 'Review original is not canonical exact JSON')
    return value


def implementation_sources() -> dict[str, str]:
    names = ('cumulative_prerequisite_review_v1.py', 'candidate_observation_admission_v1.py',
        'candidate_scope_consumer_v1.py', 'candidate_checkpoint_chain_v1.py', 'candidate_checkpoint_head_v1.py',
        'project_acceptance_registry_v1.py', 'project_acceptance_compiler_v1.py', 'candidate_http_journal_v3.py')
    result = {'gossip_harness/' + name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
              for name in names}
    admission.verify_loaded_sources(result)
    return result


@dataclass(frozen=True, slots=True)
class ReviewEnrollment:
    """Independent protected-host authorship enrollment; never a submission."""
    reviewer_id: str
    role: str
    purpose: str
    report_name: str
    report_sha256: str
    provenance_name: str
    provenance_sha256: str

    def __post_init__(self) -> None:
        registry.identifier(self.reviewer_id)
        require((self.purpose, self.role) in DUTIES, 'Unsupported original review duty')
        for name in (self.report_name, self.provenance_name):
            require(type(name) is str and bool(name) and Path(name).name == name and name.endswith('.json'),
                    'Review originals must name single bounded journal records')
        for value in (self.report_sha256, self.provenance_sha256):registry.sha256(value)
        require(self.report_name != self.provenance_name, 'Report cannot replace delivery provenance')


@dataclass(frozen=True, slots=True)
class ReviewRequest:
    name: str
    sha256: str
    position: int
    raw: bytes


@dataclass(frozen=True, slots=True)
class AuthenticatedReview:
    reviewer_id: str
    role: str
    purpose: str
    decision: str
    request_sha256: str
    report_name: str
    report_sha256: str
    report_position: int
    provenance_name: str
    provenance_sha256: str
    provenance_position: int
    checkpoint: checkpoint.PrefixCommitment


class OriginalReviewAuthority:
    """Own one fixed current original prefix; grading never refreshes its head."""
    @normalized
    def __init__(self, chain: checkpoint.CheckpointChain, expected: checkpoint.PrefixCommitment, *,
                 enrollments: tuple[ReviewEnrollment, ...] = ()):
        require(type(chain) is checkpoint.CheckpointChain and type(chain.authority) is ExternalHead
            and type(expected) is checkpoint.PrefixCommitment, 'Actual independently retained review chain required')
        require(type(enrollments) is tuple and all(type(row) is ReviewEnrollment for row in enrollments),
                'Exact protected reviewer enrollments required')
        require(len({row.report_name for row in enrollments}) == len(enrollments)
                and len({row.provenance_name for row in enrollments}) == len(enrollments), 'Duplicate review originals')
        self.chain, self.expected, self.enrollments = chain, expected, enrollments
        self.sources = implementation_sources()
        self._current()

    @normalized
    def _current(self) -> None:
        self.chain.validate_boundary(expected=self.expected)
        require(implementation_sources() == self.sources, 'Review authority implementation changed')

    @normalized
    def request_for(self, *, purpose: str, role: str, source: consumer.Revision,
                    context: dict[str, Any]) -> dict[str, Any]:
        require((purpose, role) in DUTIES and type(source) is consumer.Revision and type(context) is dict and bool(context),
                'Exact original source and supported review target required')
        return {'protocol': PROTOCOL, 'kind': 'independent-review-request', 'purpose': purpose, 'role': role,
            'source': asdict(source), 'context': context, 'sources': self.sources,
            'duties': list(DUTIES[(purpose, role)]), 'acceptance_authority': False}

    @normalized
    def stage(self, *, purpose: str, role: str, source: consumer.Revision,
              context: dict[str, Any]) -> ReviewRequest:
        self._current()
        raw = encoded(self.request_for(purpose=purpose, role=role, source=source, context=context))
        _read(raw)  # Bound structure/scalars before retaining or accepting a target.
        pin = hashlib.sha256(raw).hexdigest()
        name = 'original-review-request-' + pin + '.json'
        if self.chain.has(name):
            require(self.chain.read(name) == raw, 'Original review request changed')
        else:
            self.chain.retain(name, raw)
            self.expected = self.chain.commitment
        result = ReviewRequest(name, pin, self.chain.position(name), raw)
        self._current()
        return result

    @normalized
    def original(self, *, purpose: str, role: str, source: consumer.Revision,
                 context: dict[str, Any]) -> ReviewRequest:
        """Read the exact source-derived request without appending any record."""
        self._current()
        raw = encoded(self.request_for(purpose=purpose,role=role,source=source,context=context))
        _read(raw)  # Bound structure/scalars before retaining or accepting a target.
        pin = hashlib.sha256(raw).hexdigest()
        name = 'original-review-request-' + pin + '.json'
        if not self.chain.has(name):raise consumer.AuthorityUnavailable('Exact independent review request is absent')
        require(self.chain.read(name) == raw, 'Original review request changed')
        result = ReviewRequest(name,pin,self.chain.position(name),raw)
        self._current()
        return result

    @normalized
    def authenticate(self, request: ReviewRequest) -> AuthenticatedReview:
        self._current()
        require(type(request) is ReviewRequest and self.chain.read(request.name) == request.raw
                and hashlib.sha256(request.raw).hexdigest() == request.sha256
                and self.chain.position(request.name) == request.position, 'Original review request differs')
        value = _read(request.raw)
        require(set(value) == {'protocol','kind','purpose','role','source','context','sources','duties','acceptance_authority'}
            and value['protocol'] == PROTOCOL and value['kind'] == 'independent-review-request'
            and value['sources'] == self.sources and value['acceptance_authority'] is False
            and value['duties'] == list(DUTIES.get((value['purpose'],value['role']), ())),
            'Review target protocol/duties/source binding differs')
        require(type(value['source']) is dict and set(value['source']) == {'commit_oid','tree_oid','source_sha256'},
                'Review original source must be an exact revision')
        rebuilt = self.request_for(purpose=value['purpose'], role=value['role'],
            source=consumer.Revision(**value['source']), context=value['context'])
        require(encoded(rebuilt) == request.raw
                and request.name == 'original-review-request-' + request.sha256 + '.json',
                'Review request bypassed its exact source-derived schema')
        matches = []
        for enrollment in self.enrollments:
            if (enrollment.purpose, enrollment.role) != (value['purpose'], value['role']):continue
            if not self.chain.has(enrollment.report_name) or not self.chain.has(enrollment.provenance_name):
                raise consumer.AuthorityUnavailable('Enrolled independent review original is absent')
            raw = self.chain.read(enrollment.report_name)
            require(hashlib.sha256(raw).hexdigest() == enrollment.report_sha256, 'Enrolled report bytes changed')
            report = _read(raw)
            if report.get('request_sha256') == request.sha256:matches.append((enrollment, report))
        if not matches:raise consumer.AuthorityUnavailable('Independent review has not been delivered and enrolled')
        require(len(matches) == 1, 'Ambiguous original independent review')
        enrollment, report = matches[0]
        delivery_raw = self.chain.read(enrollment.provenance_name)
        require(hashlib.sha256(delivery_raw).hexdigest() == enrollment.provenance_sha256,
                'Enrolled original delivery changed')
        delivery = _read(delivery_raw)
        require(set(delivery) == {'protocol','kind','reviewer_id','purpose','role','request_sha256',
                                 'report_name','report_sha256','delivery_reference'}
            and delivery['protocol'] == PROTOCOL and delivery['kind'] == 'independent-review-delivery'
            and delivery['reviewer_id'] == enrollment.reviewer_id and delivery['purpose'] == enrollment.purpose
            and delivery['role'] == enrollment.role and delivery['request_sha256'] == request.sha256
            and delivery['report_name'] == enrollment.report_name and delivery['report_sha256'] == enrollment.report_sha256
            and type(delivery['delivery_reference']) is str and bool(delivery['delivery_reference'].strip()),
            'Independent delivery provenance differs')
        require(set(report) == {'protocol','kind','reviewer_id','purpose','role','request_sha256','source','decisions','limitations'}
            and report['protocol'] == PROTOCOL and report['kind'] == 'independent-review-report'
            and report['reviewer_id'] == enrollment.reviewer_id and report['purpose'] == value['purpose']
            and report['role'] == value['role'] and report['source'] == value['source']
            and type(report['limitations']) is list and all(type(x) is str for x in report['limitations'])
            and type(report['decisions']) is list and len(report['decisions']) == len(value['duties']),
            'Original report source, role, purpose or duty census differs')
        decisions = []
        for duty, row in zip(value['duties'], report['decisions'], strict=True):
            require(type(row) is dict and set(row) == {'duty','decision','rationale','inspected_references'}
                and row['duty'] == duty and row['decision'] in ('accept','reject','unresolved')
                and type(row['rationale']) is str and bool(row['rationale'].strip())
                and type(row['inspected_references']) is list and bool(row['inspected_references'])
                and all(type(ref) is str and bool(ref.strip()) for ref in row['inspected_references']),
                'Incomplete, reordered or empty original review decision')
            decisions.append(row['decision'])
        report_position, delivery_position = (self.chain.position(enrollment.report_name),
                                               self.chain.position(enrollment.provenance_name))
        require(request.position < report_position < delivery_position, 'Independent review chronology differs')
        decision = 'reject' if 'reject' in decisions else 'unresolved' if 'unresolved' in decisions else 'accept'
        result = AuthenticatedReview(enrollment.reviewer_id,enrollment.role,enrollment.purpose,decision,request.sha256,
            enrollment.report_name,enrollment.report_sha256,report_position,enrollment.provenance_name,
            enrollment.provenance_sha256,delivery_position,self.expected)
        self._current()
        return result
