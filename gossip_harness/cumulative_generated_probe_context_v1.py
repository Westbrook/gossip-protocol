"""Compile the prospective fixed-peer B/C/A/X/M contexts from whole proposals.

This is source construction and a complete logical census, not a selector or an
execution authority. Callers must authenticate original financial/reviewer
records and freeze this plan before observations. Controller enrollment, budget
admission, original-derived conditional choice and fresh merged execution remain
separate requirements. No candidate is imported, executed, accepted or promoted.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any, cast

from . import candidate_observation_admission_v1 as admission
from . import candidate_source_capture_policy_v2 as capture
from . import cumulative_study_controller_v2 as study
from . import project_acceptance_registry_v1 as registry
from .gitstore import GitStore, _path, _run

PROTOCOL = 'cumulative-generated-probe-context-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
PACKAGES = study.PACKAGES
Files = tuple[tuple[str, bytes], ...]
Modes = tuple[tuple[str, str], ...]
Delta = tuple[tuple[str, str | None], ...]
Vector = tuple[str | None, ...]
Scopes = tuple[tuple[str, tuple[str, ...]], ...]
DISPOSITIONS = ('eligible', 'failed', 'stopped', 'unknown')


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def oid(value: str) -> None:
    require(type(value) is str and len(value) == 40 and all(c in '0123456789abcdef' for c in value),
            'exact_git_oid_required')


def entries(value: tuple, *, source: bool) -> None:
    require(type(value) is tuple and all(type(row) is tuple and len(row) == 2 for row in value),
            'immutable_path_entries_required')
    paths = []
    for path, content in value:
        require(type(path) is str, 'string_path_required')
        _path(path)
        require(type(content) is bytes if source else content is None or type(content) is str,
                'exact_file_or_delta_value_required')
        if type(content) is str:
            content.encode('utf-8')
        paths.append(path)
    require(paths == sorted(set(paths)), 'sorted_unique_paths_required')
    path_set = set(paths)
    require(not any('/'.join(path.split('/')[:i]) in path_set
                    for path in paths for i in range(1, len(path.split('/')))),
            'file_directory_collision')


def under(path: str, roots: tuple[str, ...]) -> bool:
    return any(path == root or path.startswith(root + '/') for root in roots)



def object_oid(kind: str, body: bytes) -> str:
    return hashlib.sha1(kind.encode() + b' ' + str(len(body)).encode() + b'\0' + body).hexdigest()


def source_tree_oid(files: Files, modes: Modes) -> str:
    """Compute the exact ordinary-file Git tree, including inherited modes."""
    entries(files, source=True)
    require(type(modes) is tuple and all(type(row) is tuple and len(row) == 2 for row in modes)
            and tuple(p for p, _ in modes) == tuple(p for p, _ in files)
            and all(mode in ('100644', '100755') for _, mode in modes), 'complete_regular_file_modes_required')
    tree: dict[str, Any] = {}
    for (path, content), (_, mode) in zip(files, modes):
        node = tree
        parts = path.split('/')
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = (mode, object_oid('blob', content))

    def encode(node: dict[str, Any]) -> str:
        rows = []
        for name, value in node.items():
            directory = type(value) is dict
            mode, digest = ('40000', encode(value)) if directory else value
            name_bytes = name.encode('utf-8')
            rows.append((name_bytes + (b'/' if directory else b''),
                         mode.encode() + b' ' + name_bytes + b'\0' + bytes.fromhex(digest)))
        return object_oid('tree', b''.join(raw for _, raw in sorted(rows)))
    return encode(tree)


@dataclass(frozen=True, slots=True)
class Base:
    commit_oid: str
    tree_oid: str
    files: Files
    modes: Modes

    def __post_init__(self) -> None:
        oid(self.commit_oid); oid(self.tree_oid); entries(self.files, source=True)
        require(bool(self.files), 'nonempty_generation_base_required')
        require(source_tree_oid(self.files, self.modes) == self.tree_oid, 'base_git_tree_differs')

    def record(self) -> dict[str, Any]:
        return {'commit_oid': self.commit_oid, 'tree_oid': self.tree_oid,
                'source_sha256': admission.source_sha256(dict(self.files))}

    @property
    def sha256(self) -> str:
        return study.digest(self.record())


def capture_base(store: GitStore) -> Base:
    require(type(store) is GitStore, 'exact_git_store_required')
    head = store.head()
    tree, files = capture.capture_registered_source(store, head, policy=capture.TwoProcessCapturePolicy())
    # Capture already bounded the immutable ordinary-blob tree. Read its modes
    # and independently reconstruct the complete tree hash from captured bytes.
    listing = _run(store.path, 'ls-tree', '-r', '-z', head).stdout
    require(listing.endswith(b'\0'), 'complete_mode_inventory_required')
    modes = []
    for raw in listing[:-1].split(b'\0'):
        metadata, path = raw.split(b'\t', 1)
        mode, kind, _ = metadata.split(b' ')
        require(kind == b'blob', 'ordinary_blob_required')
        modes.append((path.decode('utf-8'), mode.decode('ascii')))
    require(store.head() == head, 'base_head_changed_during_capture')
    return Base(head, tree, tuple(sorted(files.items())), tuple(sorted(modes)))


@dataclass(frozen=True, slots=True)
class Proposal:
    actor: str
    disposition: str
    base_sha256: str
    original_sha256: str
    changes: Delta

    def __post_init__(self) -> None:
        require(self.actor in study.actors_for('S16-G')[:16], 'exact_builder_alias_required')
        require(self.disposition in DISPOSITIONS, 'explicit_candidate_disposition_required')
        registry.sha256(self.base_sha256); registry.sha256(self.original_sha256)
        entries(self.changes, source=False)
        require(self.disposition == 'eligible' or not self.changes, 'ineligible_source_cannot_enter_context')

    def record(self) -> dict[str, Any]:
        return {'actor': self.actor, 'disposition': self.disposition, 'base_sha256': self.base_sha256,
                'original_sha256': self.original_sha256, 'changes': dict(self.changes)}


@dataclass(frozen=True, slots=True)
class Ranking:
    package: str
    reviewer: str
    original_sha256: str
    disposition: str
    actors: tuple[str, ...]

    def __post_init__(self) -> None:
        require(self.package in PACKAGES, 'known_package_required')
        require(self.reviewer == study.REVIEWERS[PACKAGES.index(self.package)], 'exact_scoped_reviewer_required')
        registry.sha256(self.original_sha256)
        require(self.disposition in ('completed', 'failed', 'stopped', 'unknown'), 'explicit_review_disposition_required')
        require(type(self.actors) is tuple and all(type(a) is str for a in self.actors)
                and len(set(self.actors)) == len(self.actors), 'unique_ordered_endorsements_required')
        require(self.disposition == 'completed' or not self.actors, 'unavailable_review_cannot_endorse')

    def record(self) -> dict[str, Any]:
        return {'package': self.package, 'reviewer': self.reviewer, 'original_sha256': self.original_sha256,
                'disposition': self.disposition, 'actors': list(self.actors)}


@dataclass(frozen=True, slots=True)
class Generation:
    cohort_id: str
    trajectory_id: str
    milestone: str
    generation: int
    arm: str
    base: Base
    scopes: Scopes
    proposals: tuple[Proposal, ...]
    rankings: tuple[Ranking, ...]

    def __post_init__(self) -> None:
        registry.identifier(self.cohort_id); registry.identifier(self.trajectory_id)
        require(self.milestone in ('M2', 'M3', 'M4'), 'released_context_milestone_required')
        require(type(self.generation) is int and 0 <= self.generation < 3, 'existing_generation_range_required')
        require(self.arm in ('S4-G', 'S16-G', 'O16-G'), 'registered_study_arm_required')
        require(type(self.base) is Base, 'exact_generation_base_required')
        require(type(self.scopes) is tuple and all(type(row) is tuple and len(row) == 2 for row in self.scopes)
                and tuple(p for p, _ in self.scopes) == PACKAGES, 'complete_ordered_package_scopes_required')
        roots = []
        for _, paths in self.scopes:
            require(type(paths) is tuple and bool(paths) and all(type(p) is str for p in paths)
                    and tuple(sorted(set(paths))) == paths, 'exact_sorted_scope_roster_required')
            for path in paths:
                _path(path)
                roots.append(path)
        roots.sort()
        root_set = set(roots)
        require(len(root_set) == len(roots) and not any(
            '/'.join(path.split('/')[:i]) in root_set for path in roots
            for i in range(1, len(path.split('/')))), 'overlapping_package_scopes')
        require(type(self.proposals) is tuple and all(type(p) is Proposal for p in self.proposals)
                and tuple(p.actor for p in self.proposals) == study.actors_for(self.arm)[:-4],
                'complete_ordered_builder_census_required')
        candidates = {p.actor: p for p in self.proposals}
        for proposal in self.proposals:
            require(proposal.base_sha256 == self.base.sha256, 'proposal_base_substitution')
            require(all(under(path, dict(self.scopes)[study.package_for(proposal.actor)])
                        for path, _ in proposal.changes), 'proposal_exceeds_package_scope')
        require(type(self.rankings) is tuple and all(type(r) is Ranking for r in self.rankings)
                and tuple(r.package for r in self.rankings) == PACKAGES, 'complete_ordered_reviewer_census_required')
        for rank in self.rankings:
            require(all(a in candidates and candidates[a].disposition == 'eligible'
                        and study.package_for(a) == rank.package for a in rank.actors),
                    'endorsement_not_eligible_in_package')

    def record(self) -> dict[str, Any]:
        require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == LOADED_SOURCE_SHA256,
                'loaded_context_compiler_changed')
        return {'protocol': PROTOCOL, 'compiler_source_sha256': LOADED_SOURCE_SHA256, 'cohort_id': self.cohort_id, 'trajectory_id': self.trajectory_id,
                'milestone': self.milestone, 'generation': self.generation, 'arm': self.arm,
                'base': self.base.record(), 'scopes': {p: list(s) for p, s in self.scopes},
                'proposals': [p.record() for p in self.proposals], 'rankings': [r.record() for r in self.rankings],
                'dispatch_authority': False, 'acceptance_authority': False}

    @property
    def sha256(self) -> str:
        return study.digest(self.record())

    @property
    def anchor_vector(self) -> Vector | None:
        return tuple(r.actors[0] for r in self.rankings) if all(r.actors for r in self.rankings) else None


@dataclass(frozen=True, slots=True)
class Context:
    generation_sha256: str
    phase: str
    target_actor: str | None
    vector: Vector
    files: Files
    modes: Modes

    def record(self) -> dict[str, Any]:
        return {'protocol': PROTOCOL, 'generation_sha256': self.generation_sha256, 'phase': self.phase,
                'target_actor': self.target_actor, 'vector': list(self.vector),
                'tree_oid': source_tree_oid(self.files, self.modes),
                'source_sha256': admission.source_sha256(dict(self.files)),
                'dispatch_authority': False, 'acceptance_authority': False}

    @property
    def sha256(self) -> str:
        return study.digest(self.record())


def compose(generation: Generation, phase: str, *, actor: str | None = None,
            selected: tuple[str, ...] | None = None) -> Context:
    """Rebuild every context from B, never layer one alternative onto another.

    A merged vector is only a source-construction request. Endorsements constrain
    it, but do not prove conditional selection, complete evidence or acceptance.
    """
    require(type(generation) is Generation, 'exact_generation_required')
    candidates = {p.actor: p for p in generation.proposals}
    vector: Vector
    if phase == 'base-overlay':
        require(type(actor) is str and actor in candidates and candidates[actor].disposition == 'eligible' and selected is None,
                'eligible_base_overlay_required')
        assert actor is not None
        vector = tuple(actor if package == study.package_for(actor) else None for package in PACKAGES)
    elif phase in ('anchor', 'contextual'):
        anchor = generation.anchor_vector
        require(anchor is not None and selected is None, 'complete_endorsed_anchor_unavailable')
        assert anchor is not None
        if phase == 'anchor':
            require(actor is None, 'anchor_has_no_target')
            vector = anchor
        else:
            require(type(actor) is str and actor in candidates and candidates[actor].disposition == 'eligible', 'eligible_context_target_required')
            assert actor is not None
            vector = tuple(actor if package == study.package_for(actor) else anchor[i]
                           for i, package in enumerate(PACKAGES))
    elif phase == 'merged':
        require(actor is None and type(selected) is tuple and len(selected) == len(PACKAGES),
                'complete_simultaneous_choice_vector_required')
        assert selected is not None
        require(all(a in rank.actors for a, rank in zip(selected, generation.rankings)), 'merged_choice_not_endorsed')
        vector = selected
    else:
        raise ValueError('unknown_context_phase')
    files = dict(generation.base.files)
    for alias in vector:
        if alias is None:
            continue
        for path, content in candidates[alias].changes:
            if content is None:
                files.pop(path, None)
            else:
                files[path] = content.encode('utf-8')
    snapshot = tuple(sorted(files.items()))
    entries(snapshot, source=True)
    base_modes = dict(generation.base.modes)
    modes = tuple((path, base_modes.get(path, '100644')) for path, _ in snapshot)
    return Context(generation.sha256, phase, actor, vector, snapshot, modes)


def verify_context(generation: Generation, context: Context) -> None:
    require(type(context) is Context, 'exact_context_required')
    selected = context.vector if context.phase == 'merged' else None
    if selected is not None:
        require(all(type(a) is str for a in selected), 'complete_merged_vector_required')
    # compose validates string membership before using any selected alias.
    expected = compose(generation, context.phase, actor=context.target_actor, selected=cast(tuple[str, ...] | None, selected))
    require(context == expected, 'context_source_or_vector_substitution')


def delta_from_base(generation: Generation, context: Context) -> dict[str, str | None]:
    """Whole-context delta for the controller's existing durable Git effect path."""
    verify_context(generation, context)
    base, files = dict(generation.base.files), dict(context.files)
    return {path: files[path].decode('utf-8') if path in files else None
            for path in sorted(base.keys() | files.keys()) if base.get(path) != files.get(path)}


def verify_materialized(store: GitStore, generation: Generation, context: Context,
                        commit_oid: str) -> dict[str, Any]:
    """Read actual unapproved Git objects; never advance the accepted ref.

    Durable intent/CAS and independently held checkpoints belong to the caller.
    Byte equality is construction evidence, not an execution-reuse permission.
    """
    verify_context(generation, context); oid(commit_oid)
    tree, files = capture.capture_registered_source(store, commit_oid, policy=capture.TwoProcessCapturePolicy())
    require(tuple(sorted(files.items())) == context.files, 'materialized_context_source_differs')
    require(tree == source_tree_oid(context.files, context.modes), 'materialized_context_tree_differs')
    return {'protocol': PROTOCOL, 'context_sha256': context.sha256, 'generation_sha256': generation.sha256,
            'commit_oid': commit_oid, 'tree_oid': tree, 'source_sha256': admission.source_sha256(files),
            'acceptance_authority': False, 'dispatch_authority': False, 'execution_reuse_authority': False}


def census(generation: Generation) -> dict[str, Any]:
    """Retain every role and eligible logical exposure, including unranked rows.

    Distinct vectors are a diagnostic only: no row is deduplicated or admitted to
    a resource budget here. Probe applicability and cell quotas remain external.
    """
    eligible = [p for p in generation.proposals if p.disposition == 'eligible']
    base = [compose(generation, 'base-overlay', actor=p.actor).record() for p in eligible]
    anchor = compose(generation, 'anchor') if generation.anchor_vector is not None else None
    contexts = [compose(generation, 'contextual', actor=p.actor) for p in eligible] if anchor else []
    return {'protocol': PROTOCOL, 'generation': generation.record(), 'generation_sha256': generation.sha256,
            'base_overlays': base, 'anchor': anchor.record() if anchor else None,
            'contextual': [c.record() for c in contexts],
            'unavailable_contextual_actors': [p.actor for p in eligible] if anchor is None else [],
            'missing_anchor_packages': [r.package for r in generation.rankings if not r.actors],
            'planned_builders': len(generation.proposals), 'eligible_builders': len(eligible),
            'planned_contextual_exposures': len(eligible), 'logical_contextual_exposures': len(contexts),
            'distinct_contextual_vectors': len({c.vector for c in contexts}),
            'mandatory_fresh_merged_retest': True, 'dispatch_authority': False, 'acceptance_authority': False}
