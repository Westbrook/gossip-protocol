"""Protected host enrollment of exact storage-layout and invocation review.

The external checkpoint authenticates originals, not semantic correctness or
reviewer identity. Enrollment is installed by the trusted controller after
independent source inspection; candidate data cannot enroll itself. No actual
candidate review is shipped. This authority covers the named storage slice only.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from pathlib import Path
from typing import Any

from . import candidate_checkpoint_chain_v1 as chain
from .candidate_checkpoint_head_v1 import ExternalHead
from . import candidate_observation_admission_v1 as admission
from . import project_acceptance_registry_v1 as registry
from .candidate_m2_product_profile_v1 import encoded, decode

PROTOCOL = "candidate-m2-review-authority-v1-ascii-json-v1"
PURPOSE = "independent_final_m4_m2_layout_and_invocation"
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
DUTIES = (
    "complete_source_and_all_persisted_state",
    "exact_capture_paths_schema_and_auxiliary_representation",
    "public_api_recipe_and_final_m4_inheritance",
    "snapshot_limitations_and_remaining_obligations",
    "public_direct_api_instrumentation_and_untrusted_same_process_output",
)


@dataclass(frozen=True, slots=True)
class LayoutPlan:
    family: str
    source_sha256: str
    native_source_sha256: str
    commit_oid: str
    tree_oid: str
    case_id: str
    profile_sha256: str
    layout: str
    storage_paths: tuple[str, ...]
    schema_sha256: str
    # Forced interleavings have no qualified production instrumentation here.
    schedule: str = "ordinary_public_operations"
    purpose: str = "public_release"

    def __post_init__(self) -> None:
        admission.require(self.family == "m2-direct-api", "Closed storage family required")
        for value in (self.source_sha256, self.native_source_sha256, self.profile_sha256, self.schema_sha256):
            registry.sha256(value)
        admission.require(self.layout == "reviewed-m2-final-sqlite-v1",
                          "Only source-reviewed SQLite production mapping supported")
        admission.require(type(self.storage_paths) is tuple and "m2/library.sqlite" in self.storage_paths
            and len(set(self.storage_paths)) == len(self.storage_paths), "Exact storage paths required")
        admission.require(self.schedule == "ordinary_public_operations",
                          "No production forced instrumentation qualification is supplied")
        for oid in (self.commit_oid, self.tree_oid):
            admission.require(type(oid) is str and len(oid) == 40
                and all(c in '0123456789abcdef' for c in oid), "Complete Git identity required")
        registry.identifier(self.case_id)
        admission.require(self.purpose in registry.PURPOSES, "Prospective product purpose required")

    def request(self) -> dict[str, Any]:
        from . import candidate_m2_product_profile_v1 as profile
        value = profile.profile_for(self.case_id, self.purpose)
        admission.require(value.sha256 == self.profile_sha256, "Review profile differs from actual definitions")
        return {"protocol": PROTOCOL, "purpose": PURPOSE, "plan": asdict(self),
                "profile": value.record(), "duties": list(DUTIES), "scope": "named inherited M2 direct-API slice only",
                "forced_schedule_qualification_supplied": False}


@dataclass(frozen=True, slots=True)
class ReviewEnrollment:
    """Protected host configuration; constructing it is not reviewer proof."""
    reviewer_id: str
    request_name: str
    request_sha256: str
    report_name: str
    report_sha256: str
    delivery_name: str
    delivery_sha256: str

    def __post_init__(self) -> None:
        registry.identifier(self.reviewer_id)
        for value in (self.request_sha256, self.report_sha256, self.delivery_sha256):
            registry.sha256(value)


class M2ReviewAuthority:
    def __init__(self, journal: chain.CheckpointChain, expected: chain.PrefixCommitment,
                 enrollment: ReviewEnrollment):
        admission.require(type(journal) is chain.CheckpointChain
            and type(expected) is chain.PrefixCommitment and type(enrollment) is ReviewEnrollment,
            "Exact durable original review capability required")
        admission.require(type(journal.authority) is ExternalHead,
                          "Storage review requires independently held external checkpoint")
        self.journal, self.expected, self.enrollment = journal, expected, enrollment

    def authenticate(self, plan: LayoutPlan) -> str:
        """Read all originals and chronology each time; no hash-as-approval shortcut."""
        admission.require(type(plan) is LayoutPlan, "Exact prospective layout plan required")
        before = self.journal.validate_boundary().commitment
        admission.require(before == self.expected, "Review external prefix changed")
        enrollment = self.enrollment
        values = []
        for name, digest in ((enrollment.request_name, enrollment.request_sha256),
            (enrollment.report_name, enrollment.report_sha256), (enrollment.delivery_name, enrollment.delivery_sha256)):
            raw = self.journal.read(name)
            admission.require(hashlib.sha256(raw).hexdigest() == digest, "Original review bytes differ")
            values.append(decode(raw))
        request, report, delivery = values
        admission.require(encoded(request) == encoded(plan.request()),
                          "Review request differs from complete exact layout/source/recipe plan")
        admission.require(type(report) is dict and set(report) == {
            "protocol", "purpose", "reviewer_id", "request_sha256", "decisions", "remaining_obligations"}
            and report['protocol'] == PROTOCOL and report['purpose'] == PURPOSE
            and report['reviewer_id'] == enrollment.reviewer_id
            and report['request_sha256'] == enrollment.request_sha256,
            "Original report has wrong authority or source request")
        decisions = report['decisions']
        admission.require(type(decisions) is list and len(decisions) == len(DUTIES),
                          "Complete independent storage review duties required")
        for duty, row in zip(DUTIES, decisions, strict=True):
            admission.require(type(row) is dict and set(row) == {'id', 'decision', 'rationale', 'source_references'}
                and row['id'] == duty and row['decision'] == 'approved'
                and type(row['rationale']) is str and bool(row['rationale'].strip())
                and type(row['source_references']) is list and bool(row['source_references'])
                and all(type(ref) is str and bool(ref.strip()) for ref in row['source_references']),
                "Unresolved, rejected or ungrounded storage review duty")
        admission.require(type(report['remaining_obligations']) is list and bool(report['remaining_obligations'])
            and all(type(item) is str and bool(item.strip()) for item in report['remaining_obligations']),
            "A named storage slice cannot claim complete project scope")
        admission.require(delivery == {"protocol": PROTOCOL, "purpose": PURPOSE,
            "reviewer_id": enrollment.reviewer_id, "request_sha256": enrollment.request_sha256,
            "report_sha256": enrollment.report_sha256, "origin": "independently_delivered_host_review"},
            "Independent reviewer delivery differs from protected enrollment")
        admission.require(self.journal.position(enrollment.request_name) < self.journal.position(enrollment.report_name)
            < self.journal.position(enrollment.delivery_name), "Original request/report/delivery order differs")
        admission.require(self.journal.validate_boundary().commitment == before, "Review prefix changed while authenticating")
        return enrollment.report_sha256

    def provenance(self, plan: LayoutPlan) -> dict[str, Any]:
        self.authenticate(plan)
        enrollment = self.enrollment
        result = {'protocol': PROTOCOL, 'expected_prefix': asdict(self.expected),
            'enrollment': asdict(enrollment), 'positions': {
                'request': self.journal.position(enrollment.request_name),
                'report': self.journal.position(enrollment.report_name),
                'delivery': self.journal.position(enrollment.delivery_name)}}
        admission.require(self.journal.validate_boundary().commitment == self.expected,
                          'Review origin changed while binding provenance')
        return result

