"""Authenticate independently delivered exact probe reviews, never issue them.

The trusted controller must independently establish reviewer identity and review
adequacy. Hashes and a protected journal authenticate retained bytes, not the
truth of a review. Synthetic unit fixtures are not production approvals.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from . import candidate_checkpoint_chain_v1 as chain
from .candidate_checkpoint_head_v1 import ExternalHead
from . import project_acceptance_registry_v1 as registry
from . import cumulative_generated_probe_plan_v1 as plans
from . import cumulative_generated_probe_values_v2 as values

PROTOCOL = 'cumulative-generated-probe-review-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
MAX_ORIGINAL_BYTES = 2 * 1024 * 1024
require = plans.require


@dataclass(frozen=True, slots=True)
class ReviewEnrollment:
    reviewer_id: str
    request_name: str
    request_sha256: str
    report_name: str
    report_sha256: str
    delivery_name: str
    delivery_sha256: str

    def __post_init__(self) -> None:
        registry.identifier(self.reviewer_id)
        for pin in (self.request_sha256, self.report_sha256, self.delivery_sha256):
            registry.sha256(pin)
        for name in (self.request_name, self.report_name, self.delivery_name):
            require(type(name) is str and name.endswith('.json') and '/' not in name and '\\' not in name,
                    'closed_original_review_name_required')
            registry.identifier(name)
        require(len({self.request_name, self.report_name, self.delivery_name}) == 3,
                'distinct_original_review_names_required')


def _original(raw: bytes) -> dict[str, Any]:
    require(type(raw) is bytes and 0 < len(raw) <= MAX_ORIGINAL_BYTES, 'bounded_original_review_required')
    result = json.loads(raw, object_pairs_hook=values._pairs, parse_constant=values._constant)
    values._bounded(result, max_nodes=65536, max_depth=64)
    require(type(result) is dict and values.canonical(result) == raw, 'canonical_original_review_required')
    return result


class ProbeReviewAuthority:
    def __init__(self, journal: chain.CheckpointChain, expected: chain.PrefixCommitment,
                 enrollment: ReviewEnrollment) -> None:
        require(type(journal) is chain.CheckpointChain and type(expected) is chain.PrefixCommitment
                and type(enrollment) is ReviewEnrollment, 'exact_review_capabilities_required')
        require(type(journal.authority) is ExternalHead, 'independently_held_review_head_required')
        self.journal, self.expected, self.enrollment = journal, expected, enrollment

    def authenticate_with_provenance(self, plan: plans.ProbePlan) -> tuple[str, dict[str, Any]]:
        require(type(plan) is plans.ProbePlan, 'exact_probe_plan_required')
        before = self.journal.validate_boundary().commitment
        require(before == self.expected, 'independent_review_prefix_changed')
        enrollment = self.enrollment
        decoded = []
        positions = {}
        for kind, name, pin in (
            ('request', enrollment.request_name, enrollment.request_sha256),
            ('report', enrollment.report_name, enrollment.report_sha256),
            ('delivery', enrollment.delivery_name, enrollment.delivery_sha256),
        ):
            raw = self.journal.read(name)
            require(hashlib.sha256(raw).hexdigest() == pin, 'original_' + kind + '_digest_differs')
            decoded.append(_original(raw))
            positions[kind] = self.journal.position(name)
        request, report, delivery = decoded
        require(values.exact(request, json.loads(values.canonical(plan.review_request()))),
                'review_request_differs_from_exact_probe_plan')
        require(set(report) == {'protocol', 'purpose', 'reviewer_id', 'request_sha256', 'decisions', 'remaining_obligations'}
                and report['protocol'] == PROTOCOL and report['purpose'] == plans.REVIEW_PURPOSE
                and report['reviewer_id'] == enrollment.reviewer_id
                and report['request_sha256'] == enrollment.request_sha256, 'review_report_identity_differs')
        decisions = report['decisions']
        require(type(decisions) is list and len(decisions) == len(plans.DUTIES), 'complete_review_duties_required')
        for duty, decision in zip(plans.DUTIES, decisions, strict=True):
            require(type(decision) is dict and set(decision) == {'id', 'decision', 'rationale', 'source_references'}
                    and decision['id'] == duty and decision['decision'] == 'approved'
                    and type(decision['rationale']) is str and bool(decision['rationale'].strip())
                    and type(decision['source_references']) is list and bool(decision['source_references'])
                    and all(type(ref) is str and bool(ref.strip()) for ref in decision['source_references']),
                    'unresolved_or_ungrounded_probe_review')
        remaining = report['remaining_obligations']
        require(type(remaining) is list and bool(remaining)
                and all(type(item) is str and bool(item.strip()) for item in remaining),
                'named_probe_cannot_claim_complete_project_scope')
        require(values.exact(delivery, {'protocol': PROTOCOL, 'purpose': plans.REVIEW_PURPOSE,
            'reviewer_id': enrollment.reviewer_id, 'request_sha256': enrollment.request_sha256,
            'report_sha256': enrollment.report_sha256, 'origin': 'independently_delivered_host_review'}),
            'independent_probe_delivery_differs')
        require(positions['request'] < positions['report'] < positions['delivery'], 'review_original_chronology_differs')
        provenance = {'protocol': PROTOCOL, 'expected_prefix': asdict(self.expected),
                      'enrollment': asdict(enrollment), 'positions': positions,
                      'scope': 'exact source/layout/invocation only; no execution or product acceptance'}
        require(self.journal.validate_boundary().commitment == before, 'review_changed_during_authentication')
        return enrollment.report_sha256, provenance

    def authenticate(self, plan: plans.ProbePlan) -> str:
        return self.authenticate_with_provenance(plan)[0]

    def provenance(self, plan: plans.ProbePlan) -> dict[str, Any]:
        return self.authenticate_with_provenance(plan)[1]
