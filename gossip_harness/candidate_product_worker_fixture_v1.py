"""Inert, versioned hook overlay for authored physical qualification only.

Only source strings are processed here. No library module is imported or run on
the host. Hook seams add observation/pause points and expose the already-held
owner FD; they do not replace SQLite commit, kill, worker locks, jobs or receipts.
The existing corrected reference generator and its eight histories stay intact.
"""
from __future__ import annotations

import ast
import hashlib
from pathlib import Path

from .library_v2_json_reference_v2 import (
    corrected_v2_binary_files, corrected_v2_files, corrected_v2_source_inputs,
)

PROTOCOL = "candidate-product-worker-instrumented-fixture-v1"
HELPER = '''"""Evaluator-only rendezvous; disabled in uninstrumented process launches."""
def boundary(store, phase, claim=None):
    try:
        import gossip_worker_hook_v1 as hook
    except ModuleNotFoundError as error:
        if error.name == 'gossip_worker_hook_v1':
            return
        raise
    owner = claim or getattr(store, '_active_worker_claim', None) or store._worker_claim
    if owner is None:
        return
    hook.boundary(phase, lock_fd=store._evaluator_owner_fd,
        incarnation=owner['incarnation'], worker_generation=owner['generation'],
        job_id=owner.get('job_id'), epoch=owner.get('epoch'))
'''


def _once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise ValueError("Frozen reference hook seam differs")
    return source.replace(old, new)


def instrumented_files() -> dict[str, bytes]:
    """Produce an actual application source tree with a public instrumentation seam."""
    files = corrected_v2_files()
    files["library/_evaluator_worker_v1.py"] = HELPER
    name = "library/catalog/v2_maintenance.py"
    source = files[name]
    begin = source.index("    def _worker_reservation(self):")
    end = source.index("    @contextmanager\n    def worker_owner(self):", begin)
    reservation = source[begin:end]
    reservation = _once(reservation, "            yield\n", "            self._evaluator_owner_fd = descriptor\n            try:\n                yield\n            finally:\n                self._evaluator_owner_fd = None\n")
    source = source[:begin] + reservation + source[end:]
    source = _once(source, "            yield dict(owner)\n", "            from library._evaluator_worker_v1 import boundary\n            boundary(self, 'owner_acquired', owner)\n            yield dict(owner)\n")
    files[name] = source
    name = "library/catalog/worker.py"
    files[name] = _once(files[name], "    def _worker_claim_once(self, claim):\n        try:\n",
        "    def _worker_claim_once(self, claim):\n        from library._evaluator_worker_v1 import boundary\n        boundary(self, 'after_claim', claim)\n        try:\n")
    for name in ("library/catalog/control.py", "library/catalog/m4_control.py"):
        files[name] = _once(files[name], "                self.db.commit()\n",
            "                if getattr(self, '_active_worker_claim', None) is not None:\n"
            "                    from library._evaluator_worker_v1 import boundary\n"
            "                    boundary(self, 'before_commit')\n"
            "                self.db.commit()\n"
            "                if getattr(self, '_active_worker_claim', None) is not None:\n"
            "                    boundary(self, 'after_commit_before_output')\n")
    # Only syntax/data generation runs here. No generated code executes.
    for name, raw in files.items():
        if name.endswith(".py"):
            ast.parse(raw, filename=name)
    return {name: raw.encode() for name, raw in files.items()} | corrected_v2_binary_files()


def source_inputs() -> dict[str, str]:
    return corrected_v2_source_inputs() | {Path(__file__).name: hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
