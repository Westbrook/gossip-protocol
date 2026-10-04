"""Closed prospective base/CLI/workflow exposure; no semantic or runtime authority.

The two addenda have distinct identities. Workflow text precedes the exact CLI
suffix; no original product, old profile, observation or receipt is relabelled.
"""
from __future__ import annotations

from dataclasses import asdict, replace
import hashlib
from pathlib import Path
from typing import Any

from . import cumulative_cli_projection_v1 as cli

PROTOCOL = 'cumulative-workflow-exposure-v1'
STUDY_PROTOCOL = 'cumulative-study-workflow-projection-v1'
COMBINED_PROTOCOL = 'cumulative-public-base-cli-workflow-v1'
SCOPE_PROTOCOL = 'cumulative-scope-authority-v3-workflow-v1'
FAMILY = 'workflow-final-m4-v1'
INSPECTION_FAMILY = 'workflow-source-inspection-v1'
KINDS = ('workflow', 'workflow_inspection')
REVIEW_TARGET = 'public-addendum:workflow-v2-v4'
FAMILY_REVIEW_TARGET = 'workflow-family:complete-history-v1'
INSPECTION_REVIEW_TARGET = 'workflow-inspection:source-duties-v1'
BASE_PATH, BASE_SHA256 = cli.BASE_PATH, cli.BASE_SHA256
TEXT_PATH = 'docs/cumulative-workflow-v2-addendum-v4.txt'
RELEASE_PATH = 'requirements/workflow-v2-addendum-v4.txt'
TEXT_SHA256 = 'dfba7ee907eb3fb6b60795e6c8fe1b2ed78fe566e1009b7fc8c9774de268d6cf'
ROOT = Path(__file__).resolve().parents[1]
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
require = cli.require
_MODULES = (
    'cumulative_workflow_exposure_v1.py', 'candidate_workflow_profile_v1.py',
    'candidate_workflow_review_v1.py', 'candidate_workflow_execution_v1.py',
    'candidate_workflow_observation_v1.py', 'cumulative_workflow_observation_recipe_v1.py',
    'cumulative_study_controller_v2.py', 'cumulative_study_runtime_v2.py',
    'cumulative_study_successor_v3.py', 'cumulative_scope_source_v3.py',
    'cumulative_scope_authority_v3.py', 'cumulative_final_acceptance_v1.py',
    'cumulative_final_acceptance_v3.py', 'cumulative_rehearsal_codec_v1.py',
    'cumulative_rehearsal_capsule_v2.py', 'cumulative_rehearsal_capsule_v3.py',
    'cumulative_rehearsal_export_v1.py', 'cumulative_final_originals_v1.py',
    'cumulative_prerequisite_qualification_v1.py', 'library_project_fixture_v1.py')


def text() -> str:
    cli.text()
    raw = (ROOT / TEXT_PATH).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == TEXT_SHA256, 'Exact approved workflow addendum required')
    require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == LOADED_SOURCE_SHA256,
            'Loaded workflow exposure helper changed')
    return raw.decode('utf-8')


def manifest() -> dict[str, Any]:
    text()
    return {'protocol': PROTOCOL, 'base_product_path': BASE_PATH, 'base_product_sha256': BASE_SHA256,
        'text_path': TEXT_PATH, 'text_sha256': TEXT_SHA256, 'release_path': RELEASE_PATH,
        'milestones': ['M1','M2','M3','M4'], 'family': FAMILY,
        'inspection_family': INSPECTION_FAMILY, 'historical_application': False,
        'original_histories': 8, 'complete_runtime_histories': 20,
        'solve_calls': 23, 'planned_operations': 212, 'separate_host_inspection_gates': 1,
        'original_admission_bytes': 61440, 'normalized_v2_support_bytes': 61824}


def guard_sources() -> dict[str, str]:
    text()
    result = {**cli.guard_sources(), TEXT_PATH: TEXT_SHA256,
        'gossip_harness/cumulative_workflow_exposure_v1.py': LOADED_SOURCE_SHA256}
    cli.legacy.admission.verify_loaded_sources(result)
    return result


def definition_sources() -> dict[str, str]:
    """Complete prospective family source closure, without importing executors."""
    return {**cli.definition_sources(), **guard_sources(),
        **{'gossip_harness/'+name: hashlib.sha256((ROOT/'gossip_harness'/name).read_bytes()).hexdigest()
           for name in _MODULES}}


def combined_manifest() -> dict[str, Any]:
    return {'protocol': COMBINED_PROTOCOL, 'base_product_path': BASE_PATH, 'base_product_sha256': BASE_SHA256,
        'cli': cli.manifest(), 'workflow': manifest(), 'ordered_addenda': [TEXT_PATH, cli.TEXT_PATH],
        'milestones': ['M1','M2','M3','M4'], 'implementation_sources': definition_sources()}


def validate_plan(plan: Any, repository: Path | None = None) -> dict[str, Any] | None:
    # Even exact legacy paths execute this guard: they must pin its active code.
    cli_identity = cli.validate_plan(plan, repository)
    pins = guard_sources()
    require(all(plan.source_pins.get(name) == pin for name,pin in pins.items()),
            'Minimum workflow exposure guard source binding differs')
    if repository is not None:
        require(all((repository/name).is_file() and hashlib.sha256((repository/name).read_bytes()).hexdigest() == pin
            for name,pin in pins.items()), 'Workflow exposure guard repository differs')
    keys = ('workflow_projection_protocol','workflow_projection_contract','cumulative_public_contract')
    claimed = any(key in plan.runtime for key in keys)
    full = text()
    exposed = any(RELEASE_PATH in row.files or full in row.instructions or TEXT_SHA256 in row.instructions
                  for row in plan.releases) or RELEASE_PATH in plan.initial_files
    if not claimed:
        require(not exposed, 'Workflow exposure cannot drop its explicit study identity')
        return None
    from .cumulative_study_controller_v2 import StudyPlan
    require(type(plan) is StudyPlan and cli_identity == cli.manifest(),
            'Workflow study requires exact V2 plan and separately approved CLI exposure')
    plan.__post_init__()
    expected = combined_manifest()
    require(plan.runtime.get(keys[0]) == STUDY_PROTOCOL
        and plan.runtime.get(keys[1]) == manifest() and plan.runtime.get(keys[2]) == expected,
        'Exact disjoint workflow and combined study identities required')
    require(tuple(row.milestone for row in plan.releases) == ('M1','M2','M3','M4'),
            'All four workflow releases required')
    suffix = '\n\n'+full+'\n\n'+cli.text()
    for row in plan.releases:
        require(row.source_contract_sha256 == BASE_SHA256 and row.instructions.endswith(suffix)
            and row.instructions.count(full) == 1 and row.instructions.count(cli.text()) == 1
            and row.files.get(RELEASE_PATH) == full and row.files.get(cli.RELEASE_PATH) == cli.text(),
            'Exact ordered workflow/CLI text and immutable files required at every release')
    if RELEASE_PATH in plan.initial_files:
        require(plan.initial_files[RELEASE_PATH] == full, 'Initial workflow addendum differs')
    for paths in plan.package_paths.values():
        for path in paths:
            require(path != RELEASE_PATH and not RELEASE_PATH.startswith(path.rstrip('/')+'/')
                and not path.startswith(RELEASE_PATH+'/'), 'Workflow addendum cannot enter writable scope')
    sources = definition_sources()
    require(all(plan.source_pins.get(name) == pin for name,pin in sources.items()),
            'Complete workflow family/exposure/reader closure was not prospectively pinned')
    cli.legacy.admission.verify_loaded_sources(sources)
    if repository is not None:
        require(all((repository/name).is_file() and hashlib.sha256((repository/name).read_bytes()).hexdigest() == pin
            for name,pin in sources.items()), 'Workflow prospective repository source differs')
    return expected


def expose_plan(plan: Any) -> Any:
    """Author a fresh combined plan; independent review and dispatch remain absent."""
    from .cumulative_study_controller_v2 import StudyPlan, digest
    require(type(plan) is StudyPlan and cli.validate_plan(plan) == cli.manifest(),
            'Expose approved CLI identity before composing workflow exposure')
    require(validate_plan(plan) is None, 'Workflow study was already exposed')
    cli_suffix = '\n\n'+cli.text()
    releases = tuple(replace(row,
        instructions=row.instructions[:-len(cli_suffix)]+'\n\n'+text()+cli_suffix,
        files={**row.files,RELEASE_PATH:text()}) for row in plan.releases)
    runtime = {**plan.runtime,'workflow_projection_protocol':STUDY_PROTOCOL,
        'workflow_projection_contract':manifest(),'cumulative_public_contract':combined_manifest()}
    resources = {name:getattr(plan,name) for name in
        ('horizon_seconds','source_generations','partition_seconds','executor_slots')}
    cohort = replace(plan.cohort,requirements_release_sha256=digest([asdict(row) for row in releases]),
        resource_contract_sha256=digest({**resources,'runtime':runtime}))
    result = replace(plan,cohort=cohort,releases=releases,runtime=runtime,
        source_pins={**plan.source_pins,**definition_sources()})
    validate_plan(result)
    return result


def contract_fields(plan: Any) -> dict[str, Any]:
    value = validate_plan(plan)
    result = {} if value is None else {'workflow_requirements':manifest(), 'combined_effective_requirements':value}
    require(not set(result) & set(cli.contract_fields(plan)), 'Workflow cannot overwrite CLI original identity')
    return result


def validate_source(files: dict[str, bytes]) -> None:
    require(files.get(RELEASE_PATH) == text().encode() and files.get(cli.RELEASE_PATH) == cli.text().encode(),
            'Exact base/CLI/workflow candidate exposure required')


def validate_spec(plan: Any, spec: Any) -> None:
    cli.validate_spec(plan, spec)
    expected = validate_plan(plan)
    if spec.kind in KINDS:
        require(expected is not None, 'Workflow observations require the prospective combined study')
        record = spec.profile.record()
        require(record.get('effective_requirements') == cli.manifest()
            and record.get('workflow_requirements') == manifest()
            and record.get('combined_effective_requirements') == expected,
            'Workflow profile dropped or changed one original requirement identity')


def validate_submission(plan: Any, submissions: tuple) -> None:
    cli.validate_submission(plan, submissions)
    expected = validate_plan(plan)
    for submission in submissions:
        require(getattr(submission,'workflow_projection_contract',None) == (manifest() if expected else None)
            and getattr(submission,'cumulative_public_contract',None) == expected,
            'Workflow study and mandatory semantic review context differ')
        if expected is None:
            require(not any(row.family in (FAMILY,INSPECTION_FAMILY) for row in submission.slices),
                    'Legacy study cannot admit workflow slices')
            continue
        require(submission.subject.execution_contract_sha256 == plan.sha256 and submission.declaration.cohort == plan.cohort,
                'Workflow semantic request belongs to another original study')
        request = submission.request()
        require(all(sum(row['id'] == key for row in request['targets']) == 1
            for key in (REVIEW_TARGET,FAMILY_REVIEW_TARGET,INSPECTION_REVIEW_TARGET)),
            'Complete workflow addendum/family/source-inspection semantic targets required')
