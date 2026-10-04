"""Closed prospective CLI clarification; source exposure never grants review authority.

The unchanged base product and this exact public addendum form a paired identity.
Historical profiles remain explicit legacy inputs; no old observation is upgraded.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import hashlib
from pathlib import Path
from typing import Any

from . import cumulative_observation_profile_v1 as legacy

PROTOCOL = 'cumulative-cli-result-projection-v1'
STUDY_PROTOCOL = 'cumulative-study-cli-projection-v1'
PROFILE_PROTOCOL = 'cumulative-cli-observation-profile-projection-v1'
EXECUTION_PROTOCOL = 'candidate-client-execution-v5-compact-m4-projection-v1'
OBSERVATION_PROTOCOL = 'candidate-client-observation-source-v1-compact-m4-projection-v1'
SCOPE_PROTOCOL = 'cumulative-scope-authority-v3-cli-projection-v1'
REVIEW_TARGET = 'public-addendum:cli-result-projection-v1'
BASE_PATH = legacy.TARGET_CONTRACT
BASE_SHA256 = legacy.TARGET_CONTRACT_SHA256
TEXT_PATH = 'docs/cumulative-cli-result-projection-v1.txt'
RELEASE_PATH = 'requirements/cli-result-projection-v1.txt'
TEXT_SHA256 = 'd568aabfddfbf87b0a1bb8cddd11d22cfdf5e0d5b5b6830124ecbf1df86f7295'
ROOT = Path(__file__).resolve().parents[1]
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
COMMANDS = {'job-submit':'JOB','job-show':'JOB','job-cancel':'JOB','job-retry':'JOB',
            'job-prepare':'TOKEN','job-commit':'RECEIPT'}
_STUDY_SOURCES = ('cumulative_cli_projection_v1.py','cumulative_study_successor_v3.py',
    'cumulative_study_runtime_v2.py','cumulative_study_controller_v2.py','cumulative_study_role_v1.py',
    'peer_role_loop_v2.py','cumulative_final_acceptance_v1.py','cumulative_final_acceptance_v3.py',
    'cumulative_scope_authority_v3.py','cumulative_scope_source_v1.py','cumulative_rehearsal_codec_v1.py',
    'cumulative_final_originals_v1.py','candidate_client_execution_v5.py','candidate_client_observer_v5.py',
    'candidate_client_observation_source_v1.py')
require = legacy.require


def text() -> str:
    raw = (ROOT / TEXT_PATH).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == TEXT_SHA256, 'Exact approved CLI addendum bytes required')
    require(hashlib.sha256((ROOT / BASE_PATH).read_bytes()).hexdigest() == BASE_SHA256,
            'Unchanged base product identity required')
    require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == LOADED_SOURCE_SHA256,
            'Loaded CLI addendum helper changed')
    return raw.decode('utf-8')


def manifest() -> dict[str, Any]:
    text()
    return {'protocol':PROTOCOL,'base_product_path':BASE_PATH,'base_product_sha256':BASE_SHA256,
        'text_path':TEXT_PATH,'text_sha256':TEXT_SHA256,'release_path':RELEASE_PATH,
        'milestones':['M1','M2','M3','M4'],'successful_projections':dict(COMMANDS),
        'unchanged_unspecified_commands':['import','jobs'],'historical_application':False}


def definition_sources() -> dict[str, str]:
    text()
    return {**legacy.definition_sources(),TEXT_PATH:TEXT_SHA256,
        'gossip_harness/cumulative_cli_projection_v1.py':LOADED_SOURCE_SHA256}


def study_sources() -> dict[str, str]:
    text()
    return {TEXT_PATH:TEXT_SHA256,**{'gossip_harness/'+name:hashlib.sha256((ROOT/'gossip_harness'/name).read_bytes()).hexdigest()
        for name in _STUDY_SOURCES}}


def guard_sources() -> dict[str, str]:
    """Minimum active guard closure, including legacy calls and loaded-code proof.

    The legacy path uses require/TARGET constants, not the declaration factories.
    Full profile paths separately bind their complete definition source inventory.
    """
    text()
    pins = {'gossip_harness/cumulative_cli_projection_v1.py':LOADED_SOURCE_SHA256,
        'gossip_harness/cumulative_observation_profile_v1.py':legacy.LOADED_SOURCE_SHA256,
        TEXT_PATH:TEXT_SHA256,BASE_PATH:BASE_SHA256,
        'gossip_harness/candidate_observation_admission_v1.py':hashlib.sha256(Path(legacy.admission.__file__).read_bytes()).hexdigest(),
        'gossip_harness/project_acceptance_registry_v1.py':hashlib.sha256(Path(legacy.registry.__file__).read_bytes()).hexdigest()}
    legacy.admission.verify_loaded_sources(pins)
    return pins


def _validate_guard(plan: Any, repository: Path | None) -> None:
    pins = guard_sources()
    require(all(plan.source_pins.get(name) == pin for name,pin in pins.items()),
            'Minimum CLI exposure guard source binding differs')
    if repository is not None:
        require(all((repository/name).is_file() and hashlib.sha256((repository/name).read_bytes()).hexdigest() == pin
            for name,pin in pins.items()), 'CLI exposure guard repository source differs')


def validate_plan(plan: Any, repository: Path | None = None) -> dict[str, Any] | None:
    """Check the exact opt-in or reject an attempted downgrade, before effects."""
    _validate_guard(plan, repository)
    runtime = plan.runtime
    claimed = 'cli_projection_protocol' in runtime or 'cli_projection_contract' in runtime
    full_text = text()
    exposed = any(RELEASE_PATH in release.files or TEXT_SHA256 in release.instructions
        or full_text in release.instructions for release in plan.releases)
    exposed = exposed or RELEASE_PATH in plan.initial_files
    if not claimed:
        require(not exposed, 'Clarified releases cannot drop their explicit study identity')
        return None
    from .cumulative_study_controller_v2 import StudyPlan
    require(type(plan) is StudyPlan, 'Exact V2/V5 plan required for CLI clarification')
    plan.__post_init__()
    expected = manifest()
    require(runtime.get('cli_projection_protocol') == STUDY_PROTOCOL
        and runtime.get('cli_projection_contract') == expected, 'Exact paired CLI study identity required')
    require(tuple(release.milestone for release in plan.releases) == ('M1','M2','M3','M4'),
            'All four clarified releases required')
    for release in plan.releases:
        require(release.source_contract_sha256 == BASE_SHA256
            and release.instructions.endswith('\n\n'+full_text) and release.instructions.count(full_text) == 1
            and release.files.get(RELEASE_PATH) == full_text, 'Exact text and immutable file required in every release')
    if RELEASE_PATH in plan.initial_files:
        require(plan.initial_files[RELEASE_PATH] == text(), 'Initial addendum file differs')
    for paths in plan.package_paths.values():
        for path in paths:
            require(path != RELEASE_PATH and not RELEASE_PATH.startswith(path.rstrip('/')+'/')
                and not path.startswith(RELEASE_PATH+'/'), 'Addendum cannot enter writable candidate scope')
    pins = study_sources()
    require(all(plan.source_pins.get(name) == pin for name,pin in pins.items()),
            'Clarification exposure/reader source closure is not prospectively pinned')
    if repository is not None:
        require(all(hashlib.sha256((repository/name).read_bytes()).hexdigest() == pin
            for name,pin in pins.items()), 'Clarification repository source differs')
    return expected


def expose_plan(plan: Any) -> Any:
    """Author a fresh prospective plan; does not dispatch, register or approve it."""
    from .cumulative_study_controller_v2 import StudyPlan, digest
    require(type(plan) is StudyPlan, 'Exact V2/V5 input plan required')
    require(validate_plan(plan) is None, 'Do not relabel an already clarified plan')
    releases = tuple(replace(release,instructions=release.instructions+'\n\n'+text(),
        files={**release.files,RELEASE_PATH:text()}) for release in plan.releases)
    runtime = {**plan.runtime,'cli_projection_protocol':STUDY_PROTOCOL,'cli_projection_contract':manifest()}
    resources = {name:getattr(plan,name) for name in ('horizon_seconds','source_generations','partition_seconds','executor_slots')}
    cohort = replace(plan.cohort,requirements_release_sha256=digest([asdict(row) for row in releases]),
        resource_contract_sha256=digest({**resources,'runtime':runtime}))
    result = replace(plan,cohort=cohort,releases=releases,runtime=runtime,source_pins={**plan.source_pins,**study_sources()})
    validate_plan(result)
    return result


@dataclass(frozen=True, slots=True)
class CliProjectionProfile(legacy.CumulativeProfile):
    def __post_init__(self) -> None:
        require(self.family == 'cli', 'CLI clarification cannot relabel another interface')
        legacy.CumulativeProfile.__post_init__(self)
        body = legacy.CumulativeProfile.record(self)
        definition = body['original_definition']
        applicability = []
        for step in definition['recipe']['steps']:
            command = step['argv'][7:]
            if command and command[0] in COMMANDS:
                applicability.append({'step_id':step['step_id'],'command':command[0],
                    'success_projection':COMMANDS[command[0]],'source':TEXT_PATH,'source_sha256':TEXT_SHA256,
                    'expectation_pointer':'/expectations/'+step['step_id'],
                    'unchanged_expectation':definition['expectations'][step['step_id']]})
        body.update(protocol=PROFILE_PROTOCOL,effective_requirements=manifest(),public_addendum_text=text(),
            projection_applicability=applicability,definition_sources=definition_sources())
        object.__setattr__(self,'_record_json',legacy.encoded(body))

    def target_definition(self) -> dict[str, Any]:
        return {**legacy.CumulativeProfile.target_definition(self),'profile_protocol':PROFILE_PROTOCOL,
            'effective_requirements':manifest()}


def profile_for(case_id: str, *, purpose: str) -> CliProjectionProfile:
    return CliProjectionProfile('cli',case_id,purpose)


def accepted_profile(value: Any) -> bool:
    return type(value) in (legacy.CumulativeProfile,CliProjectionProfile)


def assert_profile_current(value: legacy.CumulativeProfile) -> None:
    if type(value) is CliProjectionProfile:
        require(value == profile_for(value.case_id,purpose=value.purpose), 'Clarified profile source/identity changed')
    else:
        legacy.assert_profile_current(value)


def profile_sources(value: legacy.CumulativeProfile) -> dict[str, str]:
    assert_profile_current(value)
    return definition_sources() if type(value) is CliProjectionProfile else legacy.definition_sources()


def execution_protocol(value: legacy.CumulativeProfile) -> str:
    assert_profile_current(value)
    return EXECUTION_PROTOCOL if type(value) is CliProjectionProfile else 'candidate-client-execution-v5-compact-m4-v1'


def project_outcomes(value: legacy.CumulativeProfile, diagnostics: tuple) -> legacy.CliOutcomeProjection:
    assert_profile_current(value)
    base = legacy.cli_profile(value.case_id,purpose=value.purpose)
    projected = legacy.project_cli_outcomes(base,diagnostics)
    return legacy.CliOutcomeProjection(value.sha256,projected.diagnostics,projected.decisive)


def validate_source(files: dict[str, bytes], value: legacy.CumulativeProfile | None) -> None:
    clarified = type(value) is CliProjectionProfile
    require((RELEASE_PATH in files) == clarified, 'CLI source and clarified profile must agree')
    if clarified:
        require(files[RELEASE_PATH] == text().encode(), 'Candidate source lost the original addendum bytes')


def validate_submission(plan: Any, submissions: tuple) -> None:
    expected = validate_plan(plan)
    for submission in submissions:
        require(getattr(submission,'cli_projection_contract',None) == expected,
                'Study and mandatory addendum semantic-review context differ')
        if expected is None:
            continue
        require(submission.subject.execution_contract_sha256 == plan.sha256
            and submission.declaration.cohort == plan.cohort,
            'Addendum review belongs to a different original study contract')
        request = submission.request()
        targets = [row for row in request['targets'] if row['id'] == REVIEW_TARGET]
        require(len(targets) == (1 if expected is not None else 0), 'Exact mandatory addendum review target required')


def validate_spec(plan: Any, spec: Any) -> None:
    expected = validate_plan(plan)
    if spec.kind == 'cli':
        require((type(spec.cumulative_profile) is CliProjectionProfile) == (expected is not None),
                'Clarified study requires its exact CLI profile; legacy profile cannot substitute')
        if spec.cumulative_profile is not None:
            assert_profile_current(spec.cumulative_profile)


def contract_fields(plan: Any) -> dict[str, Any]:
    """Explicit paired identity in successor/final originals; absent for legacy."""
    value = validate_plan(plan)
    return {} if value is None else {'effective_requirements': value}
