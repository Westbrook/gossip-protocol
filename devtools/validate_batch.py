"""Run an explicit exact-source offline validation manifest.

    python -m devtools.validate_batch manifest.json --output /tmp/batch-unique

Manifests use schema ``validation-session-v1`` and contain image, optional
timeout_seconds/case_timeout_seconds/adapter_version, a resources object with
workers/cpus/memory_mib/outer_parallelism, and jobs. Each job has name, files
(safe relative names mapped to exact source TEXT), ordered cases (input,
expected, optional id/requirement), purpose (visible/final/repeatability),
deterministic (false by default), seed, protocol, and reason. Independent
observations require a reason. Seed records the oracle generation contract;
it does not seed candidate processes or establish determinism. No provider
calls are made. The pinned image must already be available locally.

Persisted reuse is optional via --cache and only trusts locally owned evidence.
For a future study, preregister a NEW protocol containing this adapter/support
digest, budget, purposes/reuse policy and changed timing/deadline contract;
prepare immutable jobs before submission and reduce results in input order.
Keep reviewer dependencies and final barriers in the controller. Never monkey
patch an existing study or reinterpret frozen observations through this CLI.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import signal

from .validation_session import (
    ADAPTER_VERSION, SESSION_PROTOCOL, ResourceBudget, ValidationJob, ValidationSession,
)


def load_manifest(path: Path) -> tuple[dict, list[ValidationJob]]:
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ValueError("Duplicate JSON keys are not allowed")
            result[key] = value
        return result

    manifest = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)
    allowed = {"schema", "image", "timeout_seconds", "case_timeout_seconds",
               "adapter_version", "resources", "jobs"}
    if (not isinstance(manifest, dict) or manifest.get("schema") != SESSION_PROTOCOL
            or set(manifest) - allowed):
        raise ValueError("Expected an explicit validation-session-v1 manifest")
    jobs = [ValidationJob(**job) for job in manifest["jobs"]]
    # Complete cheap shape/source validation before creating output or preflight.
    snapshots = [job.snapshot() for job in jobs]
    if not jobs or len({job["name"] for job in snapshots}) != len(jobs):
        raise ValueError("A batch needs nonempty, uniquely named jobs")
    options = dict(image=manifest["image"],
                   timeout_seconds=manifest.get("timeout_seconds", 60),
                   case_timeout_seconds=manifest.get("case_timeout_seconds", 2),
                   adapter_version=manifest.get("adapter_version", ADAPTER_VERSION),
                   budget=ResourceBudget(**manifest.get("resources", {})))
    options["budget"].capacity()
    return options, jobs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache", type=Path)
    args = parser.parse_args(argv)
    try:
        options, jobs = load_manifest(args.manifest)
        session = ValidationSession(args.output, cache=args.cache, **options)
    except (ValueError, KeyError, TypeError, OSError) as error:
        parser.error(str(error))
    previous = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, lambda *_: session.cancel())
        result = session.run(jobs)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    print(json.dumps({"passed": result["passed"], "counts": result["counts"],
                      "receipt": str(args.output.resolve() / "session.json")}, sort_keys=True))
    return 130 if result["cancelled"] else (0 if result["passed"] else 1)


if __name__ == "__main__":
    raise SystemExit(main())
