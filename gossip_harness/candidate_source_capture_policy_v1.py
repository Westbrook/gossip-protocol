"""Closed opt-in source capture; no callbacks, dispatch cache or receipt authority.

None preserves the release-v2 capture route. The sole opt-in policy selects the
reviewed batch helper with one fixed 60 second whole-capture deadline and a
separate five second cleanup reap allowance. Every invocation reads Git anew.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any

from . import candidate_git_source_batch_v1 as batch
from .candidate_release_execution_v2 import capture_git_source as legacy_capture
from .gitstore import GitStore

PROTOCOL = 'candidate-source-capture-policy-v1'
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
_BATCH_LOADED_SHA256 = hashlib.sha256(Path(batch.__file__).read_bytes()).hexdigest()


class SourceCaptureUnavailable(RuntimeError):
    """No complete timely source capture, including uncertain helper cleanup."""


@dataclass(frozen=True, slots=True)
class BatchCapturePolicy:
    timeout_seconds: int = 60
    cleanup_reap_seconds: int = 5

    def record(self) -> dict[str, Any]:
        if (type(self) is not BatchCapturePolicy or type(self.timeout_seconds) is not int
                or self.timeout_seconds != 60 or type(self.cleanup_reap_seconds) is not int
                or self.cleanup_reap_seconds != 5):
            raise ValueError('Exact batch60/cleanup5 capture policy required')
        fields = (batch.PROTOCOL, batch.DEADLINE_CONTRACT, batch.MAX_FILES, batch.MAX_FILE_BYTES,
                  batch.MAX_SOURCE_BYTES, batch.CLEANUP_REAP_SECONDS)
        if (tuple(type(value) for value in fields) != (str, str, int, int, int, float)
                or fields != ('candidate-git-source-batch-v1',
                    'whole-capture-monotonic-v1;default=60;range=1..120;cleanup-reap=5',
                    511, 2 * 1024 * 1024, 16 * 1024 * 1024, 5.0)):
            raise SourceCaptureUnavailable('Loaded batch capture contract changed')
        sources = evaluator_sources()
        return {'protocol': PROTOCOL, 'capture_protocol': batch.PROTOCOL,
            'deadline_contract': batch.DEADLINE_CONTRACT, 'timeout_seconds': self.timeout_seconds,
            'cleanup_reap_seconds': self.cleanup_reap_seconds, 'max_files': batch.MAX_FILES,
            'max_file_bytes': batch.MAX_FILE_BYTES, 'max_source_bytes': batch.MAX_SOURCE_BYTES,
            'cross_boundary_cache': False, 'sources': sources}

    def __post_init__(self) -> None:
        self.record()


def evaluator_sources() -> dict[str, str]:
    root = Path(__file__).resolve().parent
    expected = {'candidate_source_capture_policy_v1.py': LOADED_SOURCE_SHA256,
                'candidate_git_source_batch_v1.py': _BATCH_LOADED_SHA256}
    result = {'gossip_harness/' + name: hashlib.sha256((root / name).read_bytes()).hexdigest()
              for name in expected}
    if any(result['gossip_harness/' + name] != expected[name] for name in expected):
        raise SourceCaptureUnavailable('Loaded source capture helper changed')
    return result


def capture_registered_source(store: GitStore, commit_oid: str, *,
                              policy: BatchCapturePolicy | None = None) -> tuple[str, dict[str, bytes]]:
    if policy is None:
        return legacy_capture(store, commit_oid)
    if type(policy) is not BatchCapturePolicy:
        raise ValueError('Only the exact closed batch capture policy is supported')
    policy.record()
    try:
        return batch.capture_git_source_batch(store.path, commit_oid, timeout_seconds=policy.timeout_seconds)
    except (batch.CaptureError, OSError) as failure:
        raise SourceCaptureUnavailable('Complete batch source capture unavailable') from failure
