"""Four closed public worker histories; expected answers never enter inputs."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from . import candidate_product_worker_hooks_v1 as hooks

PROTOCOL = "candidate-product-worker-cases-v1"
CONTRACT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
ORIGINAL_PURPOSE = "public_product_definition"
DATABASE = "/tmp/library.sqlite"
CLI_PREFIX = ("python", "-m", "library", "--db", DATABASE, "--root", "/inputs", "--backup-dir", "/tmp/backups")


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def document(source: str, text: str) -> dict[str, str]:
    key = source.encode()
    return {"document_id": "doc-" + sha(b"document\0" + key),
            "source_id": "src-" + sha(b"source\0" + key), "source": source,
            "title": source.rsplit("/", 1)[-1], "text": text, "blob_id": "blob-" + sha(text.encode())}


def job(name: str = "recover", *, state: str = "running", epoch: int = 1) -> dict[str, Any]:
    return {"job_id": name, "epoch": epoch, "state": state, "total": 2,
            "completed": 2 if state == "completed" else 0, "error": None}


ENTRIES = ({"source": "inline/a.txt", "text": "first admitted text"},
           {"source": "inline/b.txt", "text": "second admitted text"})


def receipt(epoch: int = 1) -> dict[str, Any]:
    return {"job": job(state="completed", epoch=epoch),
            "documents": [document(row["source"], row["text"]) for row in ENTRIES]}


class _Builder:
    def __init__(self, identifier: str) -> None:
        self.value: dict[str, Any] = {"id": identifier, "protocol": PROTOCOL, "milestone": "M4",
            "original_definition_purpose": ORIGINAL_PURPOSE,
            "requirements": ["M3-WORKER-RECOVERY", "M3-DIAGNOSTICS", "M3-INTERFACES", "M4-COMPATIBILITY", "V2-WORKER-LIVENESS"],
            "input": {"files": {}, "actions": []}, "expected": [], "facets": [],
            "uncovered": ["Full M3-WORKER-RECOVERY acceptance", "one-second idle polling cadence",
                "independently instrumented matching application claim", "provisional writes before SQLite commit",
                "incarnation then worker-generation then epoch error precedence",
                "64-artifact cleanup bound, persisted cursor, interrupted cleanup and unknown-file conservation",
                "power-loss or host-restart durability", "study builder-agent restart"]}

    def add(self, operation: str, expected: dict[str, Any] | None = None, **values: Any) -> str:
        key = "s%03d" % len(self.value["input"]["actions"])
        self.value["input"]["actions"].append({"id": key, "op": operation, **values})
        self.value["expected"].append({"step_id": key, **(expected or {})})
        return key

    def cli(self, args: list[str], value: Any = None, *, subset: bool = False, exit_code: int = 0) -> str:
        return self.add("cli", {"exit_code": exit_code, "json_subset" if subset else "json": value}, args=args)

    def diag(self, generation: int, state: str) -> str:
        return self.cli(["diagnostics"], {"worker_generation": generation, "worker_state": state}, subset=True)

    def start(self, name: str, phase: str, *, once: bool = True, mode: str = "pause") -> str:
        return self.add("worker_start", worker=name, args=["worker", *(["--once"] if once else [])],
                        phase=phase, mode=mode)

    def hook(self, name: str) -> str:
        return self.add("hook_observe", worker=name)

    def snapshot(self, *, after: str | None = None, before: str | None = None,
                 graph_empty: bool = False, state: str | None = None, epoch: int = 1,
                 worker_generation: int | None = None) -> str:
        expected: dict[str, Any] = {"snapshot_complete": True}
        if worker_generation is not None:
            expected["worker_generation"] = worker_generation
        if graph_empty:
            expected["empty_graph"] = True
        if state:
            expected["job"] = job(state=state, epoch=epoch)
        if after:
            expected["same_receipt_as"] = after
        if before:
            expected["same_logical_as"] = before
        return self.add("snapshot", expected)

    def kill(self, name: str, *, no_stdout: bool = True) -> str:
        return self.add("worker_kill", {"abrupt_death": True, "stdout_empty": no_stdout}, worker=name)

    def admitted(self) -> str:
        self.value["input"]["files"]["manifest.json"] = encoded(list(ENTRIES)).decode()
        self.cli(["job-submit", "recover", "--kind", "json", "/inputs/manifest.json"], job(state="queued"))
        self.cli(["job-prepare", "recover"], {"job_id": "recover", "epoch": 1})
        self.cli(["worker-enqueue", "recover"], {"status": "enqueued", "job": job()})
        return self.snapshot(graph_empty=True, state="running", worker_generation=0)

    def facet(self, selector: str, clause: str, steps: list[str], *, limit: str | None = None) -> None:
        self.value["facets"].append({"selector": selector, "clause": clause, "steps": steps,
                                   "qualification_limit": limit})


def _idle() -> dict[str, Any]:
    h = _Builder("worker-lifecycle-daemon-contention-abrupt-restart")
    base = h.diag(0, "stopped")
    initial = h.snapshot(worker_generation=0)
    first = h.start("w1", "owner_acquired", once=False, mode="observe")
    owner = h.hook("w1")
    idle = h.diag(1, "idle")
    busy = h.cli(["worker", "--once"], {"error": "worker_busy"}, exit_code=2)
    unchanged = h.snapshot(worker_generation=1)
    killed = h.kill("w1")
    stopped = h.diag(1, "stopped")
    second = h.start("w2", "owner_acquired", once=False, mode="observe")
    owner2 = h.hook("w2")
    idle2 = h.diag(2, "idle")
    second_snapshot = h.snapshot(worker_generation=2)
    killed2 = h.kill("w2")
    stopped2 = h.diag(2, "stopped")
    h.facet("daemon-owner-contention", "M3-WORKER-RECOVERY/1", [initial, first, owner, idle, busy, unchanged],
            limit="Requires independently observed kernel owner lock; no polling-cadence credit.")
    h.facet("abrupt-owner-restart-generation", "M3-WORKER-RECOVERY/1", [first, killed, stopped, second, owner2, idle2, second_snapshot, killed2, stopped2])
    h.facet("stopped-idle-liveness", "V2-WORKER-LIVENESS", [base, owner, idle, killed, stopped, owner2, idle2, killed2, stopped2],
            limit="No matching-active-claim/running facet.")
    return h.value


def _precommit() -> dict[str, Any]:
    h = _Builder("worker-lifecycle-before-commit-abrupt-recovery")
    baseline = h.admitted()
    start = h.start("w1", "before_commit")
    hook = h.hook("w1")
    paused = h.snapshot(graph_empty=True, state="running", worker_generation=1)
    killed = h.kill("w1")
    stopped = h.diag(1, "stopped")  # Genuine fresh Store open may perform SQLite hot-journal recovery.
    recovered = h.snapshot(graph_empty=True, state="running", worker_generation=1)
    resumed = h.cli(["worker", "--once"], {"processed": "recover", "job": job(state="completed")})
    done = h.snapshot(state="completed", worker_generation=2)
    shown = h.cli(["job-show", "recover"], job(state="completed"))
    replay = h.cli(["job-commit", "recover", "1"], receipt())
    h.diag(2, "stopped")
    h.facet("precommit-abrupt-no-visible-graph", "M3-WORKER-RECOVERY/3",
            [baseline, start, hook, paused, killed, stopped, recovered],
            limit="Unavailable for active provisional-write atomicity until a separately qualified instrumentation mechanism proves provisional writes; hook + write-lock alone is insufficient.")
    h.facet("admitted-manifest-fresh-process-resume", "M3-WORKER-RECOVERY/3",
            [baseline, killed, recovered, resumed, done, shown, replay],
            limit="Root has manifest.json only; inline/a.txt and inline/b.txt never exist. Exact precommit phase remains separately unavailable.")
    return h.value


def _postcommit() -> dict[str, Any]:
    h = _Builder("worker-lifecycle-after-commit-original-receipt")
    baseline = h.admitted()
    start = h.start("w1", "after_commit_before_output")
    hook = h.hook("w1")
    committed = h.snapshot(state="completed", worker_generation=1)
    killed = h.kill("w1")
    stopped = h.diag(1, "stopped")
    same = h.snapshot(after=committed, state="completed", worker_generation=1)
    once = h.cli(["worker", "--once"], {"processed": None, "job": None})
    no_retry = h.snapshot(after=committed, state="completed", worker_generation=2)
    replay = h.cli(["job-commit", "recover", "1"], receipt())
    doc_id = document(ENTRIES[0]["source"], ENTRIES[0]["text"])["document_id"]
    # Only normative response subfields are selected here; the receipt remains exact.
    h.cli(["refresh", doc_id, "--expected-version", "1", "--text", "later text"],
          {"status": "refreshed"}, subset=True)
    h.cli(["delete", doc_id, "--expected-version", "2"], {"status": "deleted"}, subset=True)
    changed = h.snapshot(after=committed, state="completed", worker_generation=2)
    historical = h.cli(["job-commit", "recover", "1"], receipt())
    unchanged = h.snapshot(after=committed, before=changed, state="completed")
    wrong = h.cli(["job-commit", "recover", "2"], {"error": "stale_epoch"}, exit_code=2)
    unchanged2 = h.snapshot(after=committed, before=unchanged, state="completed")
    h.facet("postcommit-preoutput-abrupt-single-receipt", "M3-WORKER-RECOVERY/3",
            [baseline, start, hook, committed, killed, stopped, same, once, no_retry, replay])
    h.facet("historical-receipt-conservation", "M4-COMPATIBILITY/1", [committed, changed, historical, unchanged, wrong, unchanged2],
            limit="Exact stored receipt text equality and parsed public receipt equality are distinct assertions.")
    return h.value


def _epoch() -> dict[str, Any]:
    h = _Builder("worker-lifecycle-live-old-claim-epoch-fence")
    baseline = h.admitted()
    start = h.start("w1", "after_claim")
    hook = h.hook("w1")
    h.cli(["job-cancel", "recover"], job(state="cancelled", epoch=2))
    h.cli(["job-retry", "recover"], job(state="queued", epoch=3))
    stale = h.snapshot(graph_empty=True, state="queued", epoch=3, worker_generation=1)
    release = h.add("hook_release", worker="w1")
    old = h.add("worker_wait", {"exit_code": 2, "json": {"error": "stale_epoch"}}, worker="w1")
    preserved = h.snapshot(graph_empty=True, state="queued", epoch=3, worker_generation=1)
    fresh = h.cli(["worker", "--once"], {"processed": "recover", "job": job(state="completed", epoch=3)})
    done = h.snapshot(state="completed", epoch=3, worker_generation=2)
    replay = h.cli(["job-commit", "recover", "3"], receipt(3))
    h.facet("old-claim-epoch-fence", "M3-WORKER-RECOVERY/2", [baseline, start, hook, stale, release, old, preserved, fresh, done, replay],
            limit="Captured claim is an evaluator-hook claim, not independently qualified application-claim instrumentation; no incarnation/worker-generation precedence credit.")
    return h.value


_FACTORIES = (_idle, _precommit, _postcommit, _epoch)


def definitions() -> tuple[dict[str, Any], ...]:
    return tuple(factory() for factory in _FACTORIES)


@dataclass(frozen=True)
class WorkerCase:
    declaration_json: bytes

    def __post_init__(self) -> None:
        if type(self.declaration_json) is not bytes or self.declaration_json not in tuple(encoded(row) for row in definitions()):
            raise ValueError("Exact closed worker lifecycle factory required")

    @property
    def record(self) -> dict[str, Any]:
        return json.loads(self.declaration_json)

    @property
    def identifier(self) -> str:
        return str(self.record["id"])

    @property
    def sha256(self) -> str:
        return sha(self.declaration_json)

    def check_current(self) -> None:
        self.__post_init__()

    def inputs(self) -> dict[str, bytes]:
        files = {name: raw.encode() for name, raw in self.record["input"]["files"].items()}
        files["__evaluator__/gossip_worker_hook_v1.py"] = hooks.HOOK_SOURCE.encode()
        files["__evaluator__/worker_launcher_v1.py"] = hooks.LAUNCHER_SOURCE.encode()
        for action in self.record["input"]["actions"]:
            if action["op"] == "worker_start":
                name = action["worker"]
                files["__evaluator__/" + name + ".json"] = encoded({"phase": action["phase"], "mode": action["mode"],
                    "directory": hooks.ROOT + "/" + name, "maximum_hold_seconds": 90})
        return files

    @property
    def facet_ids(self) -> tuple[str, ...]:
        return tuple(self.identifier + ":" + row["selector"] for row in self.record["facets"])


def cases() -> tuple[WorkerCase, ...]:
    return tuple(WorkerCase(encoded(row)) for row in definitions())


def source_identity() -> dict[str, str]:
    return {Path(__file__).name: sha(Path(__file__).read_bytes()),
            Path(hooks.__file__).name: sha(Path(hooks.__file__).read_bytes())}
