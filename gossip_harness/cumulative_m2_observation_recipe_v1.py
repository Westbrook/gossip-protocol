"""Complete prospective M2 recipes, without granting whole-project authority.

The factory re-reads real Git and independently anchored layout review originals.
Fixture construction exercises exactly the same typed recipe/configuration path;
fixture owners cannot execute or supply a physical Registry observation. Complete
semantic ScopePlan registration and the six-source freeze remain the final
adapter's separate mandatory admission boundary.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, TYPE_CHECKING

from . import candidate_m2_product_execution_v1 as storage
from . import candidate_m2_product_profile_v1 as profile
from . import candidate_m2_product_observation_v1 as observer
from . import candidate_m2_review_authority_v1 as review
from . import candidate_observation_admission_v1 as admission
from . import candidate_scope_consumer_v1 as consumer
from . import cumulative_scope_source_v3 as scope_source
from .candidate_checkpoint_head_v1 import ExternalHead
from .candidate_checkpoint_chain_v1 import PrefixCommitment
from .gitstore import GitStore
from . import project_acceptance_registry_v1 as registry

if TYPE_CHECKING:
    from .cumulative_final_acceptance_v1 import ObservationSpec

PROTOCOL = 'cumulative-m2-observation-recipe-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = consumer.require


def _derive(store: GitStore, registration: storage.M2Registration, policy: storage.M2Policy,
            value: profile.M2Profile, runtime: dict[str, Any], plan: review.LayoutPlan,
            authority: review.M2ReviewAuthority) -> dict[str, Any]:
    require(type(store) is GitStore and type(registration) is storage.M2Registration
        and type(policy) is storage.M2Policy and type(value) is profile.M2Profile
        and type(plan) is review.LayoutPlan and type(authority) is review.M2ReviewAuthority,
        'Concrete M2 source, recipe and original layout authority required')
    require(store.head() == registration.commit_oid == plan.commit_oid,
            'Recipe source is not the current protected final Git head')
    tree, files = storage.capture_git_source(store, registration.commit_oid)
    require(tree == registration.tree_oid == plan.tree_oid, 'Recipe final Git tree differs')
    original_profile = profile.profile_for(value.case_id, value.purpose)
    require(original_profile == value, 'Recipe profile differs from source-authored definition')
    original_binding = storage.binding_for(files, value, policy, runtime, plan, review_authority=authority)
    require(original_binding == registration.binding, 'Complete recipe binding differs')
    normalized = storage.observation_registration(registration)
    original_gate = storage.gate_for(registration.gate.binding.subject, original_binding,
                                    gate_id=registration.gate.gate_id)
    require(registration.gate == original_gate, 'Recipe gate differs from independently derived source')
    adapters = profile.adapter_files(value.case_id)
    original_recipe = profile.recipe_for(value.case_id)
    slice_value = scope_source.m2_slice(registration)
    scope_source.verify_slice(slice_value)
    root = Path(__file__).resolve().parent
    factory_names = ('cumulative_scope_source_v1.py', 'cumulative_scope_source_v2.py',
        'cumulative_scope_source_v3.py', 'project_acceptance_compiler_v1.py', 'candidate_scope_consumer_v1.py',
        'cumulative_final_acceptance_v1.py', 'cumulative_m2_observation_recipe_v1.py')
    sources = {**storage.evaluator_sources(), **dict(scope_source.load_catalog(root.parent).source_pins),
        **{'gossip_harness/' + name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in factory_names}}
    admission.verify_loaded_sources(sources)
    result = {'protocol': PROTOCOL, 'repository': str(store.path.resolve()),
        'registration': asdict(registration), 'observation_registration': asdict(normalized),
        'source_manifest': admission.source_manifest(files), 'profile': value.record(),
        'execution_recipe': original_recipe, 'adapter_manifest': admission.source_manifest(adapters),
        'ordered_phases': list(value.phases), 'policy': asdict(policy), 'runtime': runtime,
        'source_capture_policy': storage.SOURCE_CAPTURE_POLICY.record(),
        'layout_plan': asdict(plan), 'layout_review': authority.provenance(plan),
        'sources': sources, 'executable_scope_slice': asdict(slice_value),
        'scope_factory_protocol': scope_source.PROTOCOL, 'whole_scope_authority': False, 'physical_execution_supplied': False}
    require(store.head() == registration.commit_oid, 'Final Git head changed while constructing recipe')
    authority.authenticate(plan)
    return result


@dataclass(frozen=True, slots=True)
class ProspectiveM2Recipe:
    store: GitStore
    registration: storage.M2Registration
    policy: storage.M2Policy
    profile: profile.M2Profile
    layout_plan: review.LayoutPlan
    layout_authority: review.M2ReviewAuthority
    runtime_bytes: bytes
    original_bytes: bytes

    def record(self) -> dict[str, Any]:
        return json.loads(self.original_bytes)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.original_bytes).hexdigest()

    def revalidate(self) -> None:
        actual = _derive(self.store, self.registration, self.policy, self.profile,
                         json.loads(self.runtime_bytes), self.layout_plan, self.layout_authority)
        require(storage.encoded(actual) == self.original_bytes, 'Original prospective recipe changed')

    def scope_slice(self) -> scope_source.ExecutableSlice:
        self.revalidate()
        result = scope_source.m2_slice(self.registration)
        scope_source.verify_slice(result)
        return result

    def spec(self, *, root: Path, delta_root: Path, cleanup_root: Path,
             checkpoint_authority: ExternalHead, endpoint: Any = None) -> ObservationSpec:
        from .cumulative_final_acceptance_v1 import ObservationSpec
        self.revalidate()
        require(type(checkpoint_authority) is ExternalHead
            and checkpoint_authority.journal_roots == (root, delta_root), 'Recipe journal/head roots differ')
        return ObservationSpec('m2', root, delta_root, cleanup_root, checkpoint_authority,
            self.store, self.registration, self.policy, profile=self.profile, endpoint=endpoint,
            layout_plan=self.layout_plan, layout_authority=self.layout_authority)


def build_m2_recipe(store: GitStore, subject: registry.Subject, *, case_id: str,
        purpose: str, policy: storage.M2Policy, runtime: dict[str, Any],
        layout_plan: review.LayoutPlan, layout_authority: review.M2ReviewAuthority,
        gate_id: str, repetition_id: str, cohort_trajectory_ids: tuple[str, ...]) -> ProspectiveM2Recipe:
    """Create the complete versioned recipe from original inputs, never a verdict."""
    require(type(store) is GitStore and type(subject) is registry.Subject
        and type(layout_plan) is review.LayoutPlan and type(layout_authority) is review.M2ReviewAuthority,
        'Exact final source subject and original layout authority required')
    require(type(cohort_trajectory_ids) is tuple and len(cohort_trajectory_ids) == 6
        and len(set(cohort_trajectory_ids)) == 6 and subject.trajectory_id in cohort_trajectory_ids,
        'Exact six distinct prospective trajectory IDs required')
    value = profile.profile_for(case_id, purpose)
    tree, files = storage.capture_git_source(store, layout_plan.commit_oid)
    binding = storage.binding_for(files, value, policy, runtime, layout_plan, review_authority=layout_authority)
    gate = storage.gate_for(subject, binding, gate_id=gate_id)
    registration = storage.M2Registration(binding, layout_plan.commit_oid, tree, repetition_id, gate, cohort_trajectory_ids)
    original = _derive(store, registration, policy, value, runtime, layout_plan, layout_authority)
    return ProspectiveM2Recipe(store, registration, policy, value, layout_plan, layout_authority,
                                    storage.encoded(runtime), storage.encoded(original))


def construct_m2_owner(spec: ObservationSpec, issued: admission.ObservationAdmission, *,
                            mode: str) -> storage.CandidateM2Execution:
    """Shared real constructor; mode is explicit and final adapter uses physical.

    This supplies no complete scope or freeze authority. A fixture owner can only
    exercise configuration/admission mechanics and is refused by the raw bridge.
    """
    from .cumulative_final_acceptance_v1 import ObservationSpec
    require(type(spec) is ObservationSpec and spec.kind == 'm2' and mode in ('physical', 'fixture'),
            'Exact M2 specification and explicit construction mode required')
    require(type(spec.registration) is storage.M2Registration and type(spec.policy) is storage.M2Policy
        and type(spec.profile) is profile.M2Profile and type(spec.layout_plan) is review.LayoutPlan
        and type(spec.layout_authority) is review.M2ReviewAuthority
        and spec.recipe is None and spec.cumulative_profile is None, 'Exact closed M2 construction inputs required')
    registered = storage.observation_registration(spec.registration)
    require(type(issued) is admission.ObservationAdmission and issued.registration == registered,
            'Original issued admission differs from complete recipe')
    authority = spec.layout_authority
    require(type(authority) is review.M2ReviewAuthority and type(authority.journal.authority) is ExternalHead,
            'Exact independently anchored layout review required')
    protected = (authority.journal.raw_root, authority.journal.delta_root, authority.journal.authority.root)
    require(type(spec.checkpoint_authority) is ExternalHead, 'Exact external recipe head required')
    roots = (spec.root, spec.delta_root, spec.cleanup_root, spec.checkpoint_authority.root)
    require(all(isinstance(path, Path) and path.is_absolute() and path.resolve() == path for path in roots),
            'Canonical absolute recipe roots required')
    require(all(not a.is_relative_to(b) and not b.is_relative_to(a) for a in roots for b in protected),
            'Execution roots overlap original layout review authority')
    freeze = issued.before_intent(registered)
    owner: storage.CandidateM2Execution | None = None
    try:
        owner = storage.CandidateM2Execution(spec.root, spec.store, spec.registration, spec.policy,
            value=spec.profile, plan=spec.layout_plan, review_authority=authority, admission_authority=issued,
            checkpoint_authority=spec.checkpoint_authority, delta_root=spec.delta_root,
            cleanup_root=spec.cleanup_root, endpoint=spec.endpoint, mode=mode)
        issued.check_current(registered, freeze)
        return owner
    except BaseException:
        if owner is not None:
            try:
                owner.close()
            except BaseException:
                pass  # Preserve original admission loss; closing grants no authority.
        raise


@dataclass(frozen=True, slots=True)
class M2HistorySummary:
    execution_id: str
    case_id: str
    status: str
    missing_step_ids: tuple[str, ...]
    cleanup_verified: bool
    infrastructure: tuple[str, ...]
    terminal_sha256: str
    checkpoint: PrefixCommitment


def m2_history(owner: storage.CandidateM2Execution, terminal: dict[str, Any]) -> M2HistorySummary:
    """Derive cleanup from actual raw reconstruction, never terminal booleans."""
    require(type(owner) is storage.CandidateM2Execution and owner.mode == 'physical',
            'Actual physical original storage history required')
    before = owner.checkpoint()
    raw = owner.read_authenticated('terminal.json')
    require(storage.encoded(terminal) == raw, 'Returned storage terminal differs from authenticated original')
    original = observer.reconstruct(owner)
    require(owner.read_authenticated(observer.VERIFIER_FILE) == storage.encoded(original)
            and owner.checkpoint() == before, 'Original M2 verifier or prefix changed')
    mechanics = original['mechanics']
    return M2HistorySummary(terminal['execution_id'], terminal['case_id'],
        'completed' if mechanics['status'] == 'passed' else 'infrastructure_error',
        tuple(row['phase'] for row in original['phase_facts'] if not row.get('capture_authenticated')),
        mechanics['cleanup_verified'], tuple(terminal['infrastructure']) + tuple(mechanics['unavailable']),
        hashlib.sha256(raw).hexdigest(), before)
