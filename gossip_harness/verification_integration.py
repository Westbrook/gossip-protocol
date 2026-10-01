"""Trusted upstream policy merges for bounded verification experiments.

This component executes Git and compares text files. It never runs candidate
code, requests a model, or completes an agent task. Policy integration and the
scripted textual-conflict probe are explicitly separate kinds of evidence.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat

from .gitstore import GitStore


class PolicyIntegrationRejected(RuntimeError):
    """The fresh destination retains its previous accepted head and a receipt."""
    def __init__(self, receipt: dict):
        super().__init__(f"Trusted policy integration rejected: {receipt['status']}")
        self.receipt = receipt


def text_hash(files: dict[str, str]) -> str:
    return hashlib.sha256(json.dumps(files, sort_keys=True, ensure_ascii=True,
                                     separators=(',', ':')).encode()).hexdigest()


def receipt_path(destination: Path) -> Path:
    destination = Path(destination).resolve()
    return destination.with_name(destination.name + '.integration') / 'receipt.json'


def _save(path: Path, value: dict) -> None:
    temporary = path.with_suffix('.tmp')
    with temporary.open('w', encoding='utf-8') as handle:
        json.dump(value, handle, sort_keys=True, indent=2)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _exact_text_validator(expected: dict[str, str]):
    """Compare regular-file bytes without following candidate-created symlinks."""
    def validate(checkout: Path) -> tuple[bool, str]:
        observed = {}
        for parent, directories, filenames in os.walk(checkout, followlinks=False):
            parent = Path(parent)
            if parent == checkout and '.git' in directories:
                directories.remove('.git')
            for directory in directories:
                if (parent / directory).is_symlink():
                    return False, 'Candidate contains a directory symlink'
            for filename in filenames:
                source = parent / filename
                if source.name == '.git' and parent == checkout:
                    return False, 'Unexpected Git indirection file'
                if not stat.S_ISREG(source.lstat().st_mode):
                    return False, 'Candidate contains a symlink or special file'
                # The checkout is private to GitStore.prepare, but still avoid
                # following a replaced symlink during the read itself.
                descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(descriptor, 'rb') as handle:
                    if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                        return False, 'Candidate contains a special file'
                    observed[source.relative_to(checkout).as_posix()] = handle.read()
        wanted = {name: content.encode('utf-8') for name, content in expected.items()}
        if observed != wanted:
            return False, 'Merged text tree does not exactly preserve the current files plus trusted policy update'
        return True, 'Exact merged text tree verified; candidate code was not executed'
    return validate


def integrate_policy(initial: GitStore, current: GitStore, updates: dict[str, str],
                     destination: Path) -> tuple[GitStore, dict]:
    """Merge an initial-based policy branch into a fresh copy of current history.

    ``updates`` must contain exactly ``policy.json`` as complete UTF-8 text.
    ``current`` must descend from the current accepted head of ``initial``. The
    caller owns semantic validation of the trusted policy. Sources are read-only.
    Destination and its sibling evidence directory must both be absent; the
    caller may implement resume by verifying the durable receipt's bindings.
    """
    if (not isinstance(updates, dict) or set(updates) != {'policy.json'}
            or not isinstance(updates['policy.json'], str)):
        raise ValueError('Trusted integration accepts exactly policy.json with complete text contents')
    destination = Path(destination).resolve()
    receipt_file = receipt_path(destination)
    auxiliary = receipt_file.parent
    if destination.exists() or auxiliary.exists():
        raise FileExistsError('Policy integration requires fresh destination and evidence paths')
    initial_head, current_head = initial.head(), current.head()
    initial_files, current_files = initial.read_files(initial_head), current.read_files(current_head)
    if 'policy.json' not in initial_files or 'policy.json' not in current_files:
        raise ValueError('Both source histories must contain policy.json')
    if not current.is_ancestor(initial_head, current_head):
        raise ValueError('Current accepted history must descend from the initial accepted head')
    expected = {**current_files, **updates}
    auxiliary.mkdir(parents=True)
    incoming = GitStore.fork(initial, auxiliary / 'incoming.git')
    incoming_tip = incoming.propose(updates, base_sha=initial_head, message='Trusted upstream policy update')
    target = GitStore.fork(current, destination)

    # This is a real prepared commit (not a fabricated Candidate or a no-op
    # receipt), with the old tree and old accepted head. It is never authorized
    # to overwrite the trusted integration after the accepted head advances.
    evidence = GitStore.fork(current, auxiliary / 'old-evidence.git')
    evidence_tip = evidence.propose({}, base_sha=current_head, message='Pre-integration evidence snapshot')
    old_candidate = target.prepare(evidence, evidence_tip, current_head,
                                   _exact_text_validator(current_files), allowed_paths=('policy.json',))
    if old_candidate.status != 'prepared':
        raise RuntimeError('Could not prepare the old-head evidence candidate')
    candidate = target.prepare(incoming, incoming_tip, current_head,
                               _exact_text_validator(expected), allowed_paths=('policy.json',))
    receipt = dict(schema_version=1, component='trusted-upstream-policy-integration',
                   validation_kind='exact-text-tree-comparison-no-code-execution', api_calls=0,
                   initial_store=str(initial.path), current_store=str(current.path), destination=str(target.path),
                   initial_head=initial_head, current_head=current_head, incoming_tip=incoming_tip,
                   initial_files_sha256=text_hash(initial_files), current_files_sha256=text_hash(current_files),
                   updates_sha256=text_hash(updates), expected_files_sha256=text_hash(expected),
                   incoming_changed_paths=list(candidate.changed_paths), prepare_status=candidate.status,
                   old_evidence_candidate_sha=old_candidate.candidate_sha,
                   old_evidence_prepare_status=old_candidate.status)
    if candidate.status != 'prepared':
        receipt.update(status=candidate.status, detail=candidate.detail, merged_head=target.head(),
                       source_unchanged=initial.head()==initial_head and current.head()==current_head)
        _save(receipt_file, receipt)
        if target.head() != current_head: raise AssertionError('Rejected integration changed accepted history')
        raise PolicyIntegrationRejected(receipt)
    promoted = target.accept(candidate)
    if promoted.status != 'accepted' or promoted.new_head != candidate.candidate_sha:
        raise AssertionError('Trusted integration did not accept its exactly checked commit')
    merged_head = target.head()
    if target.read_files(merged_head) != expected:
        raise AssertionError('Accepted integration differs from the checked text tree')
    stale = target.accept(old_candidate)
    if stale.status != 'stale' or target.head() != merged_head:
        raise AssertionError('Old-head evidence was not rejected after trusted integration')
    if initial.head() != initial_head or current.head() != current_head:
        raise AssertionError('Trusted integration modified a source accepted ref')
    parents = target._git('rev-list', '--parents', '-n', '1', merged_head).split()[1:]
    if not target.is_ancestor(current_head, merged_head) or not target.is_ancestor(incoming_tip, merged_head):
        raise AssertionError('Accepted integration lost one of its source histories')
    receipt.update(status='accepted', merged_head=merged_head, exact_tested_sha=candidate.candidate_sha,
                   merged_files_sha256=text_hash(target.read_files(merged_head)), merge_parents=parents,
                   stale_probe_status=stale.status, stale_probe_head=stale.new_head, source_unchanged=True)
    _save(receipt_file, receipt)
    return target, receipt


def run_text_conflict_probe(destination: Path) -> dict:
    """Offline scripted conflicting policy branches; no model resolution claim."""
    root = Path(destination).resolve()
    if root.exists(): raise FileExistsError('Text-conflict probe requires a fresh directory')
    root.mkdir(parents=True)
    files = {'policy.json': '{"choice":"base"}\n', 'sentinel.txt': 'Preserve this file.\n'}
    initial = GitStore.create(root / 'initial.git', files)
    left = GitStore.fork(initial, root / 'left.git')
    right = GitStore.fork(initial, root / 'right.git')
    left_files = {**files, 'policy.json': '{"choice":"left"}\n'}
    left_tip = left.propose({'policy.json': left_files['policy.json']}, base_sha=initial.head(), message='Scripted left policy')
    left_candidate = left.prepare(left, left_tip, left.head(), _exact_text_validator(left_files), ('policy.json',))
    if left_candidate.status != 'prepared' or left.accept(left_candidate).status != 'accepted':
        raise AssertionError('Could not establish the scripted left branch')
    right_tip = right.propose({'policy.json': '{"choice":"right"}\n'}, base_sha=initial.head(), message='Scripted right policy')
    before = left.head()
    validation_calls = []
    def should_not_validate(checkout):
        validation_calls.append(str(checkout))
        return False, 'A textual conflict must be rejected before validation'
    conflict = left.prepare(right, right_tip, before, should_not_validate, ('policy.json',))
    rejected = left.accept(conflict)
    if conflict.status != 'text_conflict' or rejected.status != 'text_conflict' or left.head() != before or validation_calls:
        raise AssertionError('Real Git textual conflict was not rejected before validation/publication')
    receipt = dict(schema_version=1, component='scripted-offline-text-conflict-probe',
                   fixture_kind='deliberately_conflicting_policy_branches', model_calls=0,
                   claim='Tests Git conflict gating; does not test agent conflict resolution',
                   initial_head=initial.head(), left_tip=left_tip, right_tip=right_tip,
                   prepare_status=conflict.status, accept_status=rejected.status,
                   accepted_head_before=before, accepted_head_after=left.head(),
                   validator_calls=0, unchanged_files_sha256=text_hash(left.read_files()))
    _save(root / 'receipt.json', receipt)
    return receipt
