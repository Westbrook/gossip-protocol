"""Issue complete live qualification before any cumulative child starts its clock.

This entry owns only the fresh grant authority and issued capability. The caller
retains the study checkpoint, original ledger, workers and approved permit
provider. Public results retain their diagnostic, acceptance-unavailable meaning.
No permit, credential, wallet, review decision or candidate acceptance is invented.
"""
from __future__ import annotations

from pathlib import Path
import platform
import sys
from typing import Any, Callable

from .candidate_checkpoint_chain_v1 import CheckpointChain, PrefixCommitment
from .candidate_checkpoint_head_v1 import ExternalHead
from . import cumulative_issued_qualification_v1 as issued
from . import cumulative_rehearsal_capsule_v3 as capsule
from . import cumulative_study_runtime_v2 as runtime
from . import cumulative_study_successor_v3 as successor
from .cumulative_study_controller_v2 import Records, Release, PublicResult, StudyPlan, digest, require
from .gitstore import GitStore
from .peer_financial_authority_v2 import ledger_identity
from .peer_financial_authority_v3 import profile_manifest, _HTTPS_TRANSPORT
from .worker import OpenAIWorker

PROTOCOL = 'cumulative-qualified-study-v1'


def _canonical(value: Path, label: str) -> Path:
    require(isinstance(value, Path) and value.is_absolute() and value.resolve() == value
            and not value.is_symlink(), 'Canonical ' + label + ' required')
    return value


def _validate_inputs(plan: StudyPlan, *, grant_root: Path, grant_delta_root: Path,
                     grant_authority: ExternalHead, output: Path, repository: Path,
                     checkpoint: CheckpointChain, expected_checkpoint: PrefixCommitment,
                     existing_ledger_path: Path, expected_ledger_identity: dict[str, Any],
                     workers: dict[str, OpenAIWorker], mode: str,
                     permit_provider: Callable[[dict[str, Any]], dict[str, Any]],
                     evaluator: Callable[[GitStore, Release], PublicResult]) -> None:
    require(type(plan) is StudyPlan and type(checkpoint) is CheckpointChain
            and type(expected_checkpoint) is PrefixCommitment and type(grant_authority) is ExternalHead,
            'Exact prospective plan, study checkpoint and fresh grant authority required')
    plan.__post_init__()
    require(mode == 'live' and plan.runtime.get('final_acceptance_financial_mode') == 'live'
            and plan.runtime.get('live_qualification_protocol') == issued.PROTOCOL
            and plan.runtime.get('qualified_study_protocol') == PROTOCOL,
            'Explicit prospectively pinned issued live lifecycle required')
    successor.validate_successor(plan, repository)
    _canonical(repository, 'execution repository')
    _canonical(output, 'study output')
    _canonical(existing_ledger_path, 'original ledger path')
    require(repository.is_dir() and callable(permit_provider), 'Actual repository and approved permit provider required')
    require(type(workers) is dict and set(workers) == {'mini', 'strong'}
            and all(type(worker) is OpenAIWorker and worker._transport is _HTTPS_TRANSPORT for worker in workers.values()),
            'Exact concrete live worker roster and transport required')
    require(digest({name: profile_manifest(worker) for name, worker in workers.items()})
            == plan.cohort.model_profiles_sha256, 'Actual worker profiles differ from the prospective study')
    require(plan.runtime.get('python_executable') == sys.executable
            and plan.runtime.get('python_version') == platform.python_version()
            and plan.runtime.get('platform') == platform.platform(), 'Actual process runtime differs from the live plan')
    require(type(evaluator) is runtime.DockerPublicChecks and evaluator.image == plan.runtime.get('image')
            and evaluator.timeout == plan.runtime.get('public_timeout_seconds'), 'Exact authored public evaluator required')
    require(type(plan.runtime.get('terminal_drain_seconds')) in (int, float)
            and 0 < plan.runtime['terminal_drain_seconds'] <= 300, 'Bounded actual terminal drain required')
    require(ledger_identity(existing_ledger_path) == expected_ledger_identity, 'Original cumulative ledger identity differs')
    require(type(checkpoint.authority) is ExternalHead, 'Independent exact study authority required')
    assert isinstance(checkpoint.authority, ExternalHead)
    checkpoint.validate_boundary(expected=expected_checkpoint)
    roots = [_canonical(grant_root, 'grant raw root'), _canonical(grant_delta_root, 'grant delta root'),
        _canonical(grant_authority.root, 'grant head root'), _canonical(checkpoint.raw_root, 'study raw root'),
        _canonical(checkpoint.delta_root, 'study delta root'), _canonical(checkpoint.authority.root, 'study head root'), output]
    roots.extend(_canonical(Path(value), 'final proof root') for value in plan.runtime['final_acceptance_roots'].values())
    require(all(not left.is_relative_to(right) and not right.is_relative_to(left)
            for index, left in enumerate(roots) for right in roots[index + 1:]),
            'Grant, study, final proof and runtime output roots must be disjoint')
    require(all(not existing_ledger_path.is_relative_to(root) and not root.is_relative_to(existing_ledger_path)
                and not repository.is_relative_to(root) for root in roots),
            'Owned roots cannot contain the original ledger or execution repository')
    require(grant_authority.journal_roots == (grant_root, grant_delta_root)
            and grant_authority.read() is None and not grant_root.exists() and not grant_delta_root.exists()
            and grant_root.parent.is_dir() and grant_delta_root.parent.is_dir(),
            'Fresh grant journal and durable parents required')
    children = plan.roster.children
    require(len(children) == 6 and sum(len(child.actors) for child in children) == 96,
            'Complete prospective six-child, 96-role roster required')
    for child in children:
        child_root = output / child.trajectory
        require(not checkpoint.has(Records.name('child.' + child.trajectory + '.begin'))
                and not child_root.exists() and not child_root.is_symlink(),
                'Qualification must precede every child deadline and runtime initialization')


def _validate_reference(reference: dict[str, Any], *, repository: Path,
                        expected_ledger_identity: dict[str, Any]) -> None:
    value = capsule.bound(reference)
    require(value.get('protocol') == capsule.PROTOCOL and value.get('policy') == capsule.POLICY
            and value.get('repository') == str(repository), 'Pinned issued capsule protocol/repository differs')
    wallet = value.get('wallet_authorization')
    require(type(wallet) is dict and wallet.get('ledger_identity') == expected_ledger_identity,
            'Pinned issued capsule names a different actual live wallet')


def run_qualified_study(plan: StudyPlan, *, qualification_reference: dict[str, Any],
                        grant_root: Path, grant_delta_root: Path, grant_authority: ExternalHead,
                        output: Path, repository: Path, checkpoint: CheckpointChain,
                        expected_checkpoint: PrefixCommitment, existing_ledger_path: Path,
                        expected_ledger_identity: dict[str, Any], workers: dict[str, OpenAIWorker], mode: str,
                        permit_provider: Callable[[dict[str, Any]], dict[str, Any]],
                        evaluator: Callable[[GitStore, Release], PublicResult]) -> dict[str, Any]:
    """Run the actual live public phase with one complete pre-deadline issuance.

    Validation does not transfer ownership. Beginning issuance transfers the
    supplied fresh grant authority to this entry, including failure cleanup.
    Recovery of individual financial owners requires separate fresh issuance;
    this entry never adopts an already-started whole-study runtime.
    """
    _validate_inputs(plan, grant_root=grant_root, grant_delta_root=grant_delta_root,
        grant_authority=grant_authority, output=output, repository=repository, checkpoint=checkpoint,
        expected_checkpoint=expected_checkpoint, existing_ledger_path=existing_ledger_path,
        expected_ledger_identity=expected_ledger_identity, workers=workers, mode=mode,
        permit_provider=permit_provider, evaluator=evaluator)
    _validate_reference(qualification_reference, repository=repository, expected_ledger_identity=expected_ledger_identity)
    session: issued.IssuedQualification | None = None
    failure: BaseException | None = None
    try:
        session = issued.IssuedQualification.issue(qualification_reference, root=grant_root,
            delta_root=grant_delta_root, authority=grant_authority, live_plan=plan, repository=repository)
        require(type(session) is issued.IssuedQualification, 'Exact issued qualification capability required')
        issued.IssuedQualification.check_study(session, plan, repository=repository,
            ledger_identity=expected_ledger_identity)
        checkpoint.validate_boundary(expected=expected_checkpoint)
        require(ledger_identity(existing_ledger_path) == expected_ledger_identity,
                'Original cumulative ledger changed during qualification issuance')
        plan.verify_sources(repository)
        return runtime.run_study(plan, output=output, repository=repository, checkpoint=checkpoint,
            expected_checkpoint=expected_checkpoint, existing_ledger_path=existing_ledger_path,
            expected_ledger_identity=expected_ledger_identity, workers=workers, mode=mode,
            permit_provider=permit_provider, evaluator=evaluator, qualification_session=session)
    except BaseException as error:
        failure = error
        raise
    finally:
        cleanup: list[BaseException] = []
        if session is not None:
            try:
                issued.IssuedQualification.revoke(session,
                    'qualified public study entry finished; further provider admissions closed')
            except BaseException as error:
                cleanup.append(error)
        try:
            grant_authority.close()
        except BaseException as error:
            cleanup.append(error)
        if cleanup:
            if failure is not None:
                cleanup.insert(0, failure)
            raise BaseExceptionGroup('Qualified study exit could not confirm grant cleanup', cleanup)
