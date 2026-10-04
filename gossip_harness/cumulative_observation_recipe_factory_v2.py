"""Prospective V3 storage recipes with an explicit closed Git-capture policy.

V1 recipe construction remains unchanged. This successor derives the complete
recipe from fresh Git, original reviewed layout, and the policy selected by the
registered execution protocol. It reuses the actual storage owner and history
reader, while supplying no semantic ScopePlan, freeze, or acceptance authority.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from pathlib import Path
from typing import Any, TYPE_CHECKING

from . import cumulative_observation_recipe_factory_v1 as previous
from . import candidate_storage_product_execution_v1 as storage
from . import candidate_storage_product_profile_v1 as profile
from . import candidate_storage_review_authority_v1 as review
from . import candidate_source_capture_policy_v1 as source_capture
from . import candidate_observation_admission_v1 as admission
from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_scope_source_v3 as scope_source
from .candidate_checkpoint_head_v1 import ExternalHead
from .gitstore import GitStore
from . import project_acceptance_registry_v1 as registry

if TYPE_CHECKING:
    from .cumulative_final_acceptance_v1 import ObservationSpec

PROTOCOL = 'cumulative-observation-recipe-factory-v2'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = consumer.require
StorageHistorySummary = previous.StorageHistorySummary
storage_history = previous.storage_history


def _policy_record(selected: source_capture.BatchCapturePolicy | None) -> dict[str, Any] | None:
    require(selected is None or type(selected) is source_capture.BatchCapturePolicy,
            'Exact explicit source-capture policy required; callbacks are not supported')
    return None if selected is None else selected.record()


def _derive(store: GitStore, registration: storage.StorageRegistration, policy: storage.StoragePolicy,
            value: profile.StorageProductProfile, runtime: dict[str, Any], plan: review.LayoutPlan,
            authority: review.StorageReviewAuthority) -> dict[str, Any]:
    require(type(store) is GitStore and type(registration) is storage.StorageRegistration
        and type(policy) is storage.StoragePolicy and type(value) is profile.StorageProductProfile
        and review.accepted_layout_plan(plan) and type(authority) is review.StorageReviewAuthority
        and type(runtime) is dict,
        'Concrete storage source, recipe and original layout authority required')
    require(store.head() == registration.commit_oid == plan.commit_oid,
            'Recipe source is not the current protected final Git head')
    selected = storage.capture_policy_for(registration.binding.protocol)
    capture_record = _policy_record(selected)
    tree, files = storage.capture_source(store, registration.commit_oid, policy=selected)
    require(tree == registration.tree_oid == plan.tree_oid, 'Recipe final Git tree differs')
    original_profile = storage.profile_for_binding(registration.binding)
    require(original_profile == value, 'Recipe profile differs from source-authored definition')
    original_binding = storage.binding_for(files, value, policy, runtime, plan,
        review_authority=authority, capture_policy=selected)
    require(original_binding == registration.binding, 'Complete recipe binding differs')
    normalized = storage.observation_registration(registration)
    original_gate = storage.gate_for(registration.gate.binding.subject, original_binding,
                                    gate_id=registration.gate.gate_id)
    require(registration.gate == original_gate, 'Recipe gate differs from independently derived source')
    application = {'protocol': registration.binding.protocol,
        'decision': 'unavailable' if plan.schedule == 'forced_schedule_unavailable' else 'not-requested',
        'review_sha256': authority.enrollment.report_sha256, 'production_forced_schedule_qualified': False}
    adapters = storage.adapter_files(value.family, value.case_id, application)
    original_recipe = (value.record()['original_definition'] if value.family == 'b01' else
                       storage.b02.validate_recipe(storage.b02.cases.execution_recipe(value.case_id)))
    slice_value = scope_source.storage_slice(registration)
    scope_source.verify_slice(slice_value)
    sources = {**storage.evaluator_sources(), **profile.definition_sources(),
        **dict(scope_source.load_catalog(Path(__file__).resolve().parents[1]).source_pins)}
    for name in ('cumulative_observation_recipe_factory_v1.py', 'cumulative_observation_recipe_factory_v2.py',
                 'cumulative_scope_source_v1.py', 'cumulative_scope_source_v2.py',
                 'cumulative_scope_source_v3.py', 'candidate_scope_consumer_v1.py',
                 'project_acceptance_compiler_v1.py', 'cumulative_final_acceptance_v1.py'):
        sources['gossip_harness/' + name] = hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
    admission.verify_loaded_sources(sources)
    result = {'protocol': PROTOCOL, 'repository': str(store.path.resolve()),
        'registration': asdict(registration), 'observation_registration': asdict(normalized),
        'source_manifest': admission.source_manifest(files), 'profile': value.record(),
        'execution_recipe': original_recipe, 'adapter_manifest': admission.source_manifest(adapters),
        'ordered_phases': list(storage.b01.PHASES), 'policy': asdict(policy), 'runtime': runtime,
        'source_capture_policy': capture_record,
        'layout_plan': asdict(plan), 'layout_review': authority.provenance(plan),
        'sources': sources, 'executable_scope_slice': asdict(slice_value),
        'scope_factory_protocol': scope_source.PROTOCOL, 'whole_scope_authority': False,
        'physical_execution_supplied': False}
    require(store.head() == registration.commit_oid, 'Final Git head changed while constructing recipe')
    authority.authenticate(plan)
    return result


@dataclass(frozen=True, slots=True)
class ProspectiveStorageRecipe(previous.ProspectiveStorageRecipe):
    """Exact successor recipe; inherited spec() invokes this revalidation."""
    def record(self) -> dict[str, Any]:
        return profile.decode(self.original_bytes)

    def revalidate(self) -> None:
        require(type(self) is ProspectiveStorageRecipe, 'Exact version2 storage recipe required')
        actual = _derive(self.store, self.registration, self.policy, self.profile,
                         profile.decode(self.runtime_bytes), self.layout_plan, self.layout_authority)
        require(storage.encoded(actual) == self.original_bytes, 'Original prospective recipe changed')

    def scope_slice(self) -> scope_source.ExecutableSlice:
        self.revalidate()
        result = scope_source.storage_slice(self.registration)
        scope_source.verify_slice(result)
        return result


def build_storage_recipe(store: GitStore, subject: registry.Subject, *, family: str, case_id: str,
        purpose: str, policy: storage.StoragePolicy, runtime: dict[str, Any],
        layout_plan: review.LayoutPlan, layout_authority: review.StorageReviewAuthority,
        gate_id: str, repetition_id: str, cohort_trajectory_ids: tuple[str, ...],
        capture_policy: source_capture.BatchCapturePolicy | None,
        mapping_profile: str | None = None) -> ProspectiveStorageRecipe:
    """Build from exact original inputs and an explicit batch-or-legacy choice."""
    require(type(store) is GitStore and type(subject) is registry.Subject
        and review.accepted_layout_plan(layout_plan) and type(layout_authority) is review.StorageReviewAuthority,
        'Exact final source subject and original layout authority required')
    require(type(cohort_trajectory_ids) is tuple and len(cohort_trajectory_ids) == 6
        and len(set(cohort_trajectory_ids)) == 6 and subject.trajectory_id in cohort_trajectory_ids,
        'Exact six distinct prospective trajectory IDs required')
    _policy_record(capture_policy)
    value = profile.profile_for(family, case_id, purpose, mapping_profile=mapping_profile)
    tree, files = storage.capture_source(store, layout_plan.commit_oid, policy=capture_policy)
    binding = storage.binding_for(files, value, policy, runtime, layout_plan,
        review_authority=layout_authority, capture_policy=capture_policy)
    require(storage.capture_policy_for(binding.protocol) == capture_policy,
            'Registered execution protocol changed the explicit capture selection')
    gate = storage.gate_for(subject, binding, gate_id=gate_id)
    registration = storage.StorageRegistration(binding, layout_plan.commit_oid, tree, repetition_id, gate, cohort_trajectory_ids)
    original = _derive(store, registration, policy, value, runtime, layout_plan, layout_authority)
    return ProspectiveStorageRecipe(store, registration, policy, value, layout_plan, layout_authority,
                                    storage.encoded(runtime), storage.encoded(original))


def construct_storage_owner(spec: ObservationSpec, issued: admission.ObservationAdmission, *,
                            mode: str) -> storage.CandidateStorageExecution:
    """Validate the closed protocol before reusing the real original constructor."""
    from .cumulative_final_acceptance_v1 import ObservationSpec
    require(type(spec) is ObservationSpec and spec.kind == 'storage', 'Exact storage specification required')
    require(type(spec.checkpoint_authority) is ExternalHead, 'Exact external recipe head required')
    roots = (spec.root, spec.delta_root, spec.cleanup_root, spec.checkpoint_authority.root)
    require(all(isinstance(path, Path) and path.is_absolute() and path.resolve() == path for path in roots),
            'Canonical absolute recipe roots required')
    spec.observation_registration()
    selected = storage.capture_policy_for(spec.registration.binding.protocol)
    _policy_record(selected)
    return previous.construct_storage_owner(spec, issued, mode=mode)
