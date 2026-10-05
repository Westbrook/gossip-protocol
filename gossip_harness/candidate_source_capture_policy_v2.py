"""Explicit two-process source-capture opt-in; frozen v1 defaults are untouched."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path

from . import candidate_git_source_two_process_v1 as two_process
from . import candidate_git_source_batch_v1 as legacy
from .gitstore import GitStore

PROTOCOL = "candidate-source-capture-policy-v2"
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
_LOADED_HELPERS = {
    "candidate_git_source_two_process_v1.py": hashlib.sha256(Path(two_process.__file__).read_bytes()).hexdigest(),
    "candidate_git_source_batch_v1.py": hashlib.sha256(Path(legacy.__file__).read_bytes()).hexdigest(),
}


class SourceCaptureUnavailable(RuntimeError):
    """No complete capture can be attributed to the fixed declared policy."""


def evaluator_sources() -> dict[str, str]:
    root = Path(__file__).resolve().parent
    expected = {"candidate_source_capture_policy_v2.py": LOADED_SOURCE_SHA256, **_LOADED_HELPERS}
    result = {"gossip_harness/" + name: hashlib.sha256((root / name).read_bytes()).hexdigest()
              for name in expected}
    if any(result["gossip_harness/" + name] != value for name, value in expected.items()):
        raise SourceCaptureUnavailable("Loaded two-process capture helper changed")
    return result


@dataclass(frozen=True, slots=True)
class TwoProcessCapturePolicy:
    timeout_seconds: int = 60
    cleanup_reap_seconds: int = 5

    def __post_init__(self) -> None:
        self.record()

    def record(self) -> dict[str, object]:
        if (type(self) is not TwoProcessCapturePolicy or type(self.timeout_seconds) is not int
                or self.timeout_seconds != 60 or type(self.cleanup_reap_seconds) is not int
                or self.cleanup_reap_seconds != 5):
            raise ValueError("Exact two-process60/cleanup5 capture policy required")
        expected = ("candidate-git-source-two-process-v1",
                    "whole-capture-monotonic-v1;default=60;range=1..120;cleanup-reap=5",
                    511, 2 * 1024 * 1024, 16 * 1024 * 1024, 5.0)
        actual = (two_process.PROTOCOL, two_process.DEADLINE_CONTRACT, two_process.MAX_FILES,
                  two_process.MAX_FILE_BYTES, two_process.MAX_SOURCE_BYTES,
                  two_process.CLEANUP_REAP_SECONDS)
        legacy_actual = (legacy.DEADLINE_CONTRACT, legacy.MAX_FILES, legacy.MAX_FILE_BYTES,
                         legacy.MAX_SOURCE_BYTES, legacy.CLEANUP_REAP_SECONDS)
        wire = (two_process.MAX_TREE_BYTES, two_process.MAX_STDERR_BYTES,
                two_process.MAX_HEADER_BYTES, two_process.MAX_INFO_BYTES,
                two_process.MAX_REQUEST_BYTES, two_process.MAX_CONTENT_BYTES)
        wire_expected = (511 * 1200, 4096, 128, 513 * 128, 513 * 64 + 12,
                         16 * 1024 * 1024 + 511 * 128)
        if (tuple(type(value) for value in actual) != (str, str, int, int, int, float)
                or actual != expected or legacy_actual != expected[1:]
                or tuple(type(value) for value in legacy_actual) != (str, int, int, int, float)
                or any(type(value) is not int for value in wire) or wire != wire_expected
                or type(legacy.MAX_TREE_BYTES) is not int or legacy.MAX_TREE_BYTES != wire_expected[0]
                or type(legacy.MAX_STDERR_BYTES) is not int or legacy.MAX_STDERR_BYTES != wire_expected[1]):
            raise SourceCaptureUnavailable("Loaded capture contract changed")
        return {"protocol": PROTOCOL, "capture_protocol": two_process.PROTOCOL,
                "deadline_contract": two_process.DEADLINE_CONTRACT,
                "timeout_seconds": self.timeout_seconds,
                "cleanup_reap_seconds": self.cleanup_reap_seconds,
                "max_files": two_process.MAX_FILES,
                "max_file_bytes": two_process.MAX_FILE_BYTES,
                "max_source_bytes": two_process.MAX_SOURCE_BYTES,
                "max_tree_bytes": two_process.MAX_TREE_BYTES,
                "max_stderr_bytes_per_child": two_process.MAX_STDERR_BYTES,
                "max_request_bytes_per_phase": two_process.MAX_REQUEST_BYTES,
                "max_info_bytes": two_process.MAX_INFO_BYTES,
                "max_content_bytes": two_process.MAX_CONTENT_BYTES,
                "children_per_capture": 2, "cross_boundary_cache": False,
                "phase_order": ["ls-tree", "info-all", "contents-all", "EOF-exit-cleanup"],
                "sources": evaluator_sources()}


def capture_registered_source(store: GitStore, commit_oid: str, *,
                              policy: TwoProcessCapturePolicy) -> tuple[str, dict[str, bytes]]:
    """Required exact opt-in; no default path and no workflow adoption here."""
    if type(policy) is not TwoProcessCapturePolicy:
        raise ValueError("Only the exact closed two-process capture policy is supported")
    policy.record()
    try:
        return two_process.capture_git_source_two_process(
            store.path, commit_oid, timeout_seconds=policy.timeout_seconds)
    except (two_process.CaptureError, OSError) as failure:
        raise SourceCaptureUnavailable("Complete two-process source capture unavailable") from failure
