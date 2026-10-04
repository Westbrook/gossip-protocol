"""Read original worker journals and keep product discrepancies apart from unknowns.

Only read_original() accepts physical authority: an exact live WorkerExecution
and its externally anchored current checkpoint. Pure helpers below confer no
physical, acceptance, scope, cohort, or independent-purpose authority.
"""
from __future__ import annotations

import base64
from dataclasses import asdict, dataclass
import re
from typing import Any

from . import candidate_client_process_v4 as engine
from . import candidate_product_worker_cases_v1 as cases
from . import candidate_product_worker_hooks_v1 as hooks
from . import candidate_checkpoint_chain_v1 as chain
from . import candidate_product_process_reader_v1 as original_reader
from . import candidate_product_process_observation_v1 as original_observer

PROTOCOL = "candidate-product-worker-observation-v1"


class DurableDiscrepancy(ValueError):
    """A complete trusted table contradicts a required product state."""


@dataclass(frozen=True)
class Facet:
    selector: str
    state: str
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class WorkerObservation:
    protocol: str
    execution_id: str
    binding_sha256: str
    checkpoint_sha256: str
    known_discrepancies: tuple[str, ...]
    unavailable: tuple[str, ...]
    facets: tuple[Facet, ...]
    cleanup_verified: bool
    physical: bool = True
    acceptance_authority: bool = False
    whole_product_acceptance: bool = False
    independent_purpose_credit: bool = False


def require(value: bool, message: str) -> None:
    if not value:
        raise ValueError(message)


def exact(left: Any, right: Any) -> bool:
    return cases.encoded(left) == cases.encoded(right)


def subset(actual: Any, expected: Any) -> bool:
    if type(expected) is dict:
        return type(actual) is dict and all(key in actual and subset(actual[key], value) for key, value in expected.items())
    return exact(actual, expected)


def strict_value(raw: bytes) -> Any:
    require(type(raw) is bytes, "Complete JSON bytes required")
    if len(raw) > 4 * 1024 * 1024:
        raise original_observer.ObservationLimit("Complete JSON exceeds observer byte allocation")
    return original_observer._strict_json(raw)


def product_json(raw: bytes) -> Any:
    """Known complete invalid product syntax is distinct from parser limits."""
    try:
        return strict_value(raw)
    except (original_observer.AmbiguousJSON, original_observer.ObservationLimit, RecursionError):
        raise
    except (ValueError, UnicodeError) as error:
        raise DurableDiscrepancy("Complete product JSON has invalid syntax") from error


def table(snapshot: dict[str, Any], name: str) -> list[dict[str, Any]]:
    require(snapshot.get("state") == "complete", "SQLite read transaction unavailable")
    row = snapshot["tables"][name]
    require(type(row) is dict and type(row["columns"]) is list and type(row["rows"]) is list,
            "Malformed trusted snapshot")
    columns = row["columns"]
    require(len(columns) == len(set(columns)), "Duplicate snapshot columns")
    require(all(type(item) is list and len(item) == len(columns) for item in row["rows"]), "Snapshot column mismatch")
    return [dict(zip(columns, values, strict=True)) for values in row["rows"]]


def controls(snapshot: dict[str, Any]) -> dict[str, Any]:
    rows = table(snapshot, "control")
    if any(type(row["key"]) is not str or type(row["value"]) is not str for row in rows):
        raise DurableDiscrepancy("Complete control table has non-text keys or values")
    if len({row["key"] for row in rows}) != len(rows):
        raise DurableDiscrepancy("Complete control table has duplicate keys")
    return {row["key"]: row["value"] for row in rows}


def selected_job(snapshot: dict[str, Any]) -> dict[str, Any]:
    matches = [row for row in table(snapshot, "jobs") if row["job_id"] == "recover"]
    if len(matches) != 1:
        raise DurableDiscrepancy("Complete jobs table lacks exactly one original recover job")
    return matches[0]


def public_job(row: dict[str, Any]) -> dict[str, Any]:
    return {key: row[key] for key in ("job_id", "epoch", "state", "total", "completed", "error")}


def graph_empty(snapshot: dict[str, Any]) -> bool:
    return all(not table(snapshot, name) for name in ("documents", "blobs", "document_revisions", "document_state"))


def completed_graph(snapshot: dict[str, Any], epoch: int) -> bool:
    job = selected_job(snapshot)
    if not exact(public_job(job), cases.job(state="completed", epoch=epoch)) or type(job["receipt"]) is not str:
        return False
    if not exact(product_json(job["receipt"].encode()), cases.receipt(epoch)):
        return False
    documents = table(snapshot, "documents")
    expected_documents = [{key: doc[key] for key in ("document_id", "source_id", "source", "blob_id", "title")}
                          for doc in (cases.document(row["source"], row["text"]) for row in cases.ENTRIES)]
    revisions, states, blobs = (table(snapshot, name) for name in ("document_revisions", "document_state", "blobs"))
    expected_revisions, expected_states, expected_blobs = [], [], []
    for row in cases.ENTRIES:
        doc = cases.document(row["source"], row["text"])
        revision_id = "rev-" + cases.sha(b"revision\0" + doc["document_id"].encode() + b"\0" + b"1" + b"\0" + doc["blob_id"].encode())
        expected_revisions.append({"revision_id": revision_id, "document_id": doc["document_id"], "revision": 1, "blob_id": doc["blob_id"]})
        expected_states.append({"document_id": doc["document_id"], "head_revision_id": revision_id,
            "edit_version": 1, "deleted": 0, "notes": "", "tags": "[]", "collections": "[]"})
        expected_blobs.append({"blob_id": doc["blob_id"], "content": {"blob_bytes": len(row["text"].encode()), "blob_sha256": cases.sha(row["text"].encode())}})
    def ordered(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(rows, key=cases.encoded)
    return (exact(ordered(documents), ordered(expected_documents)) and exact(ordered(revisions), ordered(expected_revisions))
            and exact(ordered(states), ordered(expected_states)) and exact(ordered(blobs), ordered(expected_blobs)))


def completed_graph_findings(snapshots: dict[str, dict[str, Any]], steps: tuple[str, ...],
                             epoch: int) -> tuple[tuple[str, str, str], ...]:
    """Grade each complete snapshot independently; confer no physical authority."""
    findings = []
    for step in steps:
        try:
            if not completed_graph(snapshots[step], epoch):
                findings.append((step, "failed", "complete durable receipt/documents/revisions differ"))
        except DurableDiscrepancy as error:
            findings.append((step, "failed", str(error)))
        except (ValueError, KeyError, TypeError, UnicodeError, OverflowError, RecursionError) as error:
            findings.append((step, "unavailable", str(error)))
    return tuple(findings)


def kernel_lock_evidence(value: dict[str, Any]) -> dict[str, Any]:
    """Conservative Linux POSIX/FLOCK attribution; unsupported forms unavailable.

    Do not turn a candidate's FD/path/event into ownership evidence. Require a
    complete independently executed proc reader, a live PID stat, fdinfo lock,
    and matching current kernel lock with the same device/inode/PID. OFDLCK
    owner=-1 is intentionally outside this qualification profile.
    """
    result: dict[str, Any] = {"exclusive_owner_lock": False, "sqlite_reserved_write_lock": False,
                              "matching_application_claim": False, "provisional_writes": False}
    try:
        require(value.get("state") == "complete", "kernel reader incomplete")
        raw = base64.b64decode(value["event_b64"], validate=True)
        require(len(raw) <= hooks.MAX_EVENT_BYTES, "hook event bound")
        event = strict_value(raw)
        require(type(event) is dict and set(event) == {"protocol", "phase", "pid", "lock_fd", "incarnation",
            "worker_generation", "job_id", "epoch"} and event["protocol"] == hooks.PROTOCOL
            and event["phase"] in hooks.PHASES, "hook interface differs")
        pid = event["pid"]
        require(type(pid) is int and pid == value["pid"] and pid == 1, "main worker PID namespace profile differs")
        require(type(event["lock_fd"]) is int and event["lock_fd"] == value["fd"], "hook FD differs")
        proc = value["proc_stat"]
        require(proc.startswith(str(pid) + " (") and ") " in proc, "kernel PID stat differs")
        require(proc.rsplit(") ", 1)[1].split()[0] not in ("Z", "X", "x"), "hook PID is not live")
        fd_identity, db_identity = value["fd_identity"], value["database_identity"]
        # A maintenance-local descriptor is a declared observation profile,
        # never a requirement that all conforming products choose this layout.
        require(value["fd_target"].startswith("/tmp/library.sqlite.maintenance/")
                and not value["fd_target"].endswith(" (deleted)"), "unsupported owner lock location")
        lock_pattern = re.compile(r"^(?:lock:\s*)?\d+:\s+(FLOCK|POSIX)\s+ADVISORY\s+WRITE\s+(\d+)\s+([0-9a-fA-F]+):([0-9a-fA-F]+):(\d+)\s+(\d+)\s+(EOF|\d+)$", re.MULTILINE)

        def locks(text: str) -> set[tuple[str, int, int, int, int, int, str]]:
            return {(m[0], int(m[1]), int(m[2], 16), int(m[3], 16), int(m[4]), int(m[5]), m[6])
                    for m in lock_pattern.findall(text)}

        current, fd_locks = locks(value["locks"]), locks(value["fdinfo"])
        owner_key = (fd_identity["major"], fd_identity["minor"], fd_identity["inode"])
        db_key = (db_identity["major"], db_identity["minor"], db_identity["inode"])
        result["exclusive_owner_lock"] = any(row[1] == pid and row[2:5] == owner_key and row in fd_locks
                                              and row[5] == 0 and row[6] == "EOF" for row in current)
        # This is rollback-journal SQLite's RESERVED byte, not a provisional-row
        # observation. WAL locks and unqualified byte profiles remain unknown.
        result["sqlite_reserved_write_lock"] = any(row[0] == "POSIX" and row[1] == pid and row[2:5] == db_key
            and row[5] <= 1073741825 and (row[6] == "EOF" or int(row[6]) >= 1073741825) for row in current)
        result["event"] = event
        result["database_identity"] = db_identity
        result["limitation"] = "Application matching claim and provisional writes are not independently instrumented."
    except (ValueError, KeyError, TypeError, UnicodeError, OverflowError) as error:
        result["limitation"] = type(error).__name__ + ":" + str(error)[:512]
    return result


class _Reader:
    def __init__(self, owner: Any):
        from . import candidate_product_worker_execution_v1 as execution
        self.owner, self.execution = owner, execution
        self._keeper: tuple[Any, dict[str, Any]] | None = None
        self.workers: dict[str, tuple[Any, dict[str, Any], dict[str, Any]]] = {}
        self._cli_exits: dict[str, int] = {}
        self._guarded_steps: set[str] = set()

    def raw(self, name: str) -> bytes:
        require(type(name) is str and re.fullmatch(r"[a-z][a-z0-9.-]{0,180}", name) is not None,
                "Original evidence name required")
        return self.owner.read_authenticated(name)

    def json(self, name: str) -> dict[str, Any]:
        value = strict_value(self.raw(name))
        require(type(value) is dict, "Original object required")
        return value

    def descriptor(self, value: dict[str, Any]) -> bytes:
        raw = self.raw(value["path"])
        require(type(value["bytes"]) is int and len(raw) == value["bytes"] and cases.sha(raw) == value["sha256"],
                "Original byte descriptor differs")
        return raw

    def control(self, label: str, cid: str, operation: str, method: str, status: int) -> bytes:
        require(self.raw(label + "-request.bin") == engine._request(method, "/containers/" + cid + "/" + operation),
                "Engine request differs from declared control")
        wire = original_reader._RetainedWire(self.raw(label + "-response.bin"))
        code, headers = wire.headers()
        body = wire.body(code, headers)
        require(code == status and not wire.buffer, "Original Engine response differs")
        return body

    def capture(self, prefix: str, cid: str, descriptors: dict[str, Any]) -> tuple[bytes, bytes]:
        require(self.raw(prefix + "-attach-request.bin") == engine._request("POST", "/containers/" + cid +
            "/attach?stream=1&stdout=1&stderr=1&stdin=0&logs=0", upgrade=True), "Original attach target differs")
        wire = original_reader._RetainedWire(self.raw(prefix + "-attach-response.bin"))
        status, headers = wire.headers()
        require(status == 101 and headers.get("upgrade", "").lower() == "tcp", "Original attach differs")
        decoder = engine.MultiplexDecoder(self.owner.policy.process)
        decoder.feed(bytes(wire.buffer))
        decoder.eof()  # Completeness authority was recorded by the original live reader, not this parse.
        for kind in ("stdout", "stderr"):
            raw = self.descriptor(descriptors[kind])
            require(raw == bytes(decoder.streams[kind]) and descriptors[kind].get("truncated") is False
                    and descriptors[kind].get("observed_bytes") == len(raw), "Complete original stream differs")
        return bytes(decoder.streams["stdout"]), bytes(decoder.streams["stderr"])

    def command(self, label: str, argv: list[str], *, maximum_timeout: float | None = None) -> bytes:
        expected = ["docker", "--host", "unix://" + self.owner.endpoint.socket_path, *argv[1:]]
        record, intent = self.json(label + ".json"), self.json(label + "-command-intent.json")
        require(argv[0] == "docker" and record.get("argv") == expected and intent.get("argv") == expected
                and intent.get("runtime_sha256") == self.execution.digest(self.owner.runtime),
                "Original command endpoint, argv or runtime differs")
        bound = self.owner.policy.transport_timeout_seconds if maximum_timeout is None else maximum_timeout
        require(type(intent.get("timeout_seconds")) in (int, float) and 0 < intent["timeout_seconds"] <= bound,
                "Original command deadline exceeds declared bound")
        require(record.get("exit_code") == 0 and record.get("timed_out") is False
                and record.get("capture_complete") is True and not record.get("control_errors"),
                "Original command completion unavailable")
        for channel in ("stdout", "stderr"):
            raw = self.descriptor(record[channel])
            require(record[channel].get("truncated") is False and record[channel].get("observed_bytes") == len(raw),
                    "Original command capture incomplete")
        return self.descriptor(record["stdout"])

    def spec(self, role: str, label: str, action: dict[str, Any] | None = None) -> Any:
        owner = self.owner
        intent = self.json("intent.json")
        require(intent["execution_id"] == owner.execution_id
                and intent["volume"] == "gossip-" + owner.execution_id + "-volume", "Original held volume name differs")
        labels = tuple(sorted({"gossip.execution": owner.execution_id, "gossip.role": role,
            "gossip.source": owner.binding.source_sha256, "gossip.fixture": owner.binding.fixture_sha256,
            "gossip.helper": owner.binding.helper_sha256, "gossip.step": "state-lifetime" if role == "keeper" else label}.items()))
        if role == "keeper":
            return self.execution.RoleSpec("keeper", "gossip-" + owner.execution_id + "-keeper",
                ("python", "-I", "-c", "import time;time.sleep(" + str(owner.policy.lifetime_seconds) + ")"),
                labels, (), intent["volume"])
        require(action is not None, "Exact action required for candidate role")
        assert action is not None
        staging = self.json("staging.json")
        require(exact(staging["source_manifest"], self.execution.source_manifest(owner.files))
                and exact(staging["input_manifest"], self.execution.source_manifest(owner.inputs)),
                "Original complete staging inventory differs")
        binds = (("/workspace", staging["workspace"]), ("/inputs", staging["inputs"]))
        if role == "worker":
            argv = ("python", "-B", "/inputs/__evaluator__/worker_launcher_v1.py",
                    "/inputs/__evaluator__/" + action["worker"] + ".json", *cases.CLI_PREFIX[3:], *action["args"])
            suffix = action["worker"]
        else:
            require(role == "cli", "Closed candidate role")
            argv, suffix = (*cases.CLI_PREFIX, *action["args"]), label
        return self.execution.RoleSpec("cli", "gossip-" + owner.execution_id + "-" + suffix,
            argv, labels, binds, intent["volume"])

    def inspect(self, label: str, cid: str) -> dict[str, Any]:
        result = strict_value(self.control(label, cid, "json", "GET", 200))
        require(type(result) is dict and result.get("Id") == cid, "Original inspection ID differs")
        return result

    def compare(self, label: str, before: dict[str, Any], after: dict[str, Any], spec: Any, phase: str) -> None:
        comparison = self.execution.role_identity_comparison(before, after, spec, self.owner.runtime, phase)
        require(comparison["matches"] is True and exact(comparison, self.json(label + ".json")),
                "Raw role identity or retained comparison differs")

    def created(self, label: str, spec: Any) -> dict[str, Any]:
        argv = self.execution.create_argv(spec, self.owner.policy.image_id)
        require(exact(self.json(label + "-create-intent.json"), {"spec": asdict(spec), "create_argv": argv}),
                "Original role declaration differs from frozen source/input history")
        require(not self.command(label + "-before", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"]).strip(),
                "Role was not absent before creation")
        cid = self.command(label + "-create", argv).strip().decode("ascii")
        require(re.fullmatch(r"[0-9a-f]{64}", cid) is not None, "Full created role ID required")
        created = self.inspect(label + "-created", cid)
        self.execution.validate_role(created, spec, self.owner.policy.image_id, self.owner.runtime)
        self.execution.validate_state(created, "created")
        return created

    def keeper(self) -> tuple[Any, dict[str, Any]]:
        if self._keeper is None:
            spec = self.spec("keeper", "keeper")
            created = self.created("keeper", spec)
            before = self.inspect("keeper-prestart", created["Id"])
            self.execution.validate_state(before, "created")
            self.compare("keeper-prestart-comparison", created, before, spec, "created-to-prestart")
            body = self.control("keeper-start", created["Id"], "start", "POST", 204)
            require(exact(self.json("keeper-start-completion.json"), {"status": 204, "body_sha256": cases.sha(body),
                "framing_complete": True, "eof_observed": True, "container_id": created["Id"]}), "Keeper start completion differs")
            running = self.inspect("keeper-running", created["Id"])
            self.execution.validate_state(running, "running")
            self.compare("keeper-startup-comparison", created, running, spec, "created-to-running")
            require(exact(running, self.json("keeper-running.json")), "Keeper baseline differs")
            self._keeper = spec, running
        return self._keeper

    def boundary(self, label: str, baseline: dict[str, Any], spec: Any) -> None:
        current = self.inspect(label, baseline["Id"])
        self.execution.validate_state(current, "running")
        self.compare(label + "-continuity", baseline, current, spec, "running-to-running")

    def keeper_boundary(self, label: str) -> None:
        spec, running = self.keeper()
        self.boundary(label + "-keeper", running, spec)
        name = self.json("intent.json")["volume"]
        baseline = self.json("volume-baseline.json")
        expected_labels = {"gossip.execution": self.owner.execution_id, "gossip.snapshot": self.execution.SNAPSHOT_PROTOCOL}
        require(baseline.get("Name") == name and baseline.get("Driver") == "local"
                and baseline.get("Options") == self.execution.VOLUME_OPTIONS and baseline.get("Labels") == expected_labels,
                "Held volume baseline is not the declared owned tmpfs")
        created = strict_value(self.command("volume-created", ["docker", "volume", "inspect", "--format", "{{json .}}", name]))
        current = strict_value(self.command(label + "-volume", ["docker", "volume", "inspect", "--format", "{{json .}}", name]))
        require(exact(created, baseline) and exact(current, baseline), "Raw held-volume identity/continuity differs")

    def trusted(self, label: str, source: str, args: tuple[str, ...] = (), *, worker: str | None = None,
                maximum_timeout: float | None = None) -> dict[str, Any]:
        self.keeper_boundary(label + "-before")
        if worker is None:
            cid = self.keeper()[1]["Id"]
        else:
            spec, _created, running = self.workers[worker]
            self.boundary(label + "-worker-before", running, spec)
            cid = running["Id"]
        argv = ["docker", "exec", "--user=65534:65534", cid, "python", "-I", "-c", source, *args]
        require(exact(self.json(label + "-trusted-input.json"), {"source_sha256": cases.sha(source.encode()), "argv": argv,
            "candidate_imports": False, "container_id": cid, "worker_pid_namespace": worker is not None}),
            "Trusted helper identity/input differs from fixed evaluator program")
        raw = self.command(label, argv, maximum_timeout=maximum_timeout)
        require(self.descriptor(self.json(label + ".json")["stderr"]) == b"", "Trusted helper stderr differs")
        self.keeper_boundary(label + "-after")
        if worker is not None:
            spec, _created, running = self.workers[worker]
            self.boundary(label + "-worker-after", running, spec)
        value = strict_value(raw)
        require(type(value) is dict, "Trusted helper object required")
        return value

    def worker_started(self, action: dict[str, Any], row: dict[str, Any]) -> None:
        label, worker = action["id"], action["worker"]
        require(self.trusted(label + "-hook-setup", hooks.SETUP_HOOK_SOURCE, (worker,)) == {"created": True},
                "Original hook directory setup differs")
        spec = self.spec("worker", label, action)
        created = self.created(label, spec)
        cid, prefix = created["Id"], label + "-worker"
        require(cid == row["container_id"] and prefix == row["start_label"], "Worker source role differs")
        before = self.inspect(prefix + "-prestart", cid)
        self.execution.validate_state(before, "created")
        self.compare(prefix + "-prestart", created, before, spec, "created-to-prestart")
        body = self.control(prefix + "-start", cid, "start", "POST", 204)
        require(exact(self.json(prefix + "-start-completion.json"), {"status": 204, "body_sha256": cases.sha(body),
            "container_id": cid, "framing_complete": True, "eof_observed": True}), "Worker start temporal evidence differs")
        running = self.inspect(prefix + "-running", cid)
        self.execution.validate_state(running, "running")
        self.compare(prefix + "-startup", created, running, spec, "created-to-running")
        require(exact(running, row["running"]) and exact(running, self.json(prefix + "-running.json")), "Worker running identity differs")
        self.workers[worker] = spec, created, running
        self.keeper_boundary(label + "-after")

    def removed(self, label: str, spec: Any, cid: str, *, force: bool) -> None:
        raw = self.command(label + "-inspect", ["docker", "inspect", "--format", "{{json .}}", spec.name])
        inspected = strict_value(raw)
        self.execution.validate_role(inspected, spec, self.owner.policy.image_id, self.owner.runtime)
        require(inspected["Id"] == cid, "Removed role source identity differs")
        require(exact(self.json(label + "-intent.json"), {"container_id": cid, "spec": asdict(spec), "force": force,
            "inspection_sha256": self.execution.digest(inspected)}), "Owned removal intent differs")
        self.command(label + "-remove", ["docker", "rm", *(["--force"] if force else []), cid])
        require(not self.command(label + "-absence", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + spec.name + "$"]).strip(),
                "Prior role remains before subsequent epoch")

    def cli_exit(self, row: dict[str, Any], action: dict[str, Any]) -> int:
        label = action["id"]
        if label in self._cli_exits:
            return self._cli_exits[label]
        spec = self.spec("cli", label, action)
        created = self.created(label, spec)
        cid, prefix, process = created["Id"], label + "-process", row["process"]
        require(cid == row["cli_id"] and exact(process, self.json(prefix + "-process.json"))
                and exact(process, self.json(label + "-process-result.json")), "Original CLI process copy differs")
        before = self.inspect(prefix + "-inspect-before", cid)
        self.execution.validate_state(before, "created")
        engine.validate_sandbox(before, self.owner.policy.process, expected_argv=list(spec.argv), runtime=self.owner.runtime)
        comparison = engine.identity_comparison(engine.immutable_inspection(created), engine.immutable_inspection(before),
            self.owner.runtime, phase="candidate-created-to-prestart", before_full_inspection=created, after_full_inspection=before)
        require(comparison["matches"] is True and exact(comparison, process["prestart_comparison"]), "Raw CLI prestart identity differs")
        declaration = self.json(prefix + "-intent.json")
        require(declaration["expected_argv"] == list(spec.argv) and exact(declaration["expected_runtime"], self.owner.runtime)
                and exact(declaration["policy"], asdict(self.owner.policy.process)) and declaration["container_id"] == cid
                and declaration["expected_full_inspection_sha256"] == cases.sha(engine._comparison_bytes(created)),
                "Original CLI declaration differs from exact source/runtime/argv")
        self.control(prefix + "-start", cid, "start", "POST", 204)
        wait = strict_value(self.control(prefix + "-wait", cid, "wait?condition=not-running", "POST", 200))
        final = self.inspect(prefix + "-inspect-final", cid)
        self.execution.validate_state(final, "exited")
        self.execution.validate_role(final, spec, self.owner.policy.image_id, self.owner.runtime)
        comparison = engine.startup_identity_comparison(engine.immutable_inspection(created), engine.immutable_inspection(final),
            self.owner.runtime, phase="candidate-created-to-exited", before_full_inspection=created, after_full_inspection=final)
        require(comparison["matches"] is True and exact(comparison, process["startup_comparison"]), "Raw CLI final source identity differs")
        completion = engine.completion_evidence(wait, final, started=True, killed=False, identity_ok=True)
        require(completion["natural"] is True and exact(completion, process["completion"])
                and process["exit_code"] == completion["inspect_exit_code"], "Original natural CLI exit is unavailable")
        start = process.get("start_response")
        require(type(start) is dict and start.get("status") == 204 and start.get("container_id") == cid
                and start.get("framing_complete") is True and start.get("eof_observed") is True,
                "Original CLI start temporal event unavailable")
        self._cli_exits[label] = process["exit_code"]
        return process["exit_code"]

    def cli(self, row: dict[str, Any], action: dict[str, Any]) -> tuple[int, bytes, bytes]:
        code = self.cli_exit(row, action)
        process, label = row["process"], action["id"]
        require(process.get("capture_complete") is True and process.get("attach_eof_after_start_confirmation") is True,
                "Original CLI capture unavailable")
        stdout, stderr = self.capture(label + "-process", row["cli_id"], process)
        self.keeper_boundary(label + "-before")
        return code, stdout, stderr

    def cli_after(self, row: dict[str, Any], action: dict[str, Any]) -> None:
        label = action["id"]
        self.keeper_boundary(label + "-after")
        self.removed(label + "-retire", self.spec("cli", label, action), row["cli_id"], force=True)

    def action_guards(self, action: dict[str, Any]) -> None:
        step = action["id"]
        if step in self._guarded_steps:
            return
        active, releases = {}, {}
        for item in self.owner.profile.case.record["input"]["actions"]:
            if item["id"] == step:
                break
            if item["op"] == "worker_start" and item["mode"] == "pause":
                active[item["worker"]] = item
            elif item["op"] in ("worker_kill", "worker_wait"):
                active.pop(item["worker"], None)
            elif item["op"] == "hook_release":
                releases[item["worker"]] = item
        phase_members = {"before": dict(active), "after": dict(active)}
        if action["op"] == "worker_start" and action["mode"] == "pause":
            phase_members["after"][action["worker"]] = action
        prior_position = self.owner.journal._chain.position(step + "-step-intent.json")
        prior_checked = None
        for phase, members in phase_members.items():
            name = step + "-" + phase + "-instrumentation.json"
            record = self.json(name)
            require(record.get("protocol") == "worker-hook-host-bound-v1" and record.get("clock") == "host-monotonic-ns"
                    and type(record.get("checked_ns")) is int and record["checked_ns"] > 0,
                    "Original host instrumentation clock unavailable")
            position = self.owner.journal._chain.position(name)
            require(position > prior_position and (prior_checked is None or record["checked_ns"] >= prior_checked),
                    "Instrumentation guard chronology differs")
            prior_position, prior_checked = position, record["checked_ns"]
            require([bound["worker"] for bound in record["bounds"]] == sorted(members),
                    "Instrumentation guard omits or adds an active paused worker")
            for bound in record["bounds"]:
                worker, declaration = bound["worker"], members[bound["worker"]]
                start = self.json(declaration["id"] + "-observation.json")
                if worker not in self.workers:
                    self.worker_started(declaration, start)
                prefix, cid = start["start_label"], start["container_id"]
                anchor = self.json(prefix + "-instrumentation-start.json")
                require(anchor.get("protocol") == record["protocol"] and anchor.get("clock") == record["clock"]
                        and anchor.get("container_id") == cid and anchor.get("hook_mode") == "pause"
                        and anchor.get("maximum_hold_seconds") == hooks.MAX_HOLD_SECONDS
                        and type(anchor.get("start_before_ns")) is int and anchor["start_before_ns"] > 0
                        and self.owner.journal._chain.position(prefix + "-instrumentation-start.json")
                            < self.owner.journal._chain.position(prefix + "-start-request.bin") < position,
                        "Original start-derived instrumentation anchor differs")
                released = releases.get(worker)
                release_label = None
                if released is not None:
                    self.action_guards(released)
                    release_label = released["id"] + "-release"
                    require(self.trusted(release_label, hooks.RELEASE_SOURCE, (hooks.ROOT + "/" + worker,))
                            == {"released": True, "acknowledgement": "released"}, "Prior release acknowledgement unavailable")
                valid = release_label is not None or 0 <= record["checked_ns"] - anchor["start_before_ns"] < hooks.MAX_HOLD_SECONDS * 1_000_000_000
                require(exact(bound, {"worker": worker, "container_id": cid, "start_label": prefix,
                    "start_before_ns": anchor["start_before_ns"], "maximum_hold_seconds": hooks.MAX_HOLD_SECONDS,
                    "release_acknowledgement_label": release_label, "within_bound": valid}) and valid,
                    "Evaluator hold expired or original instrumentation bound unavailable")
        self._guarded_steps.add(step)

    def worker_exit(self, row: dict[str, Any], start: dict[str, Any], *, killed: bool) -> int:
        prefix, cid = start["start_label"], start["container_id"]
        spec, _created, running = self.workers[start["worker"]]
        require(running["Id"] == cid, "Worker completion belongs to another source epoch")
        original = self.json(prefix + "-exit.json")
        require(all(exact(row.get(key), value) for key, value in original.items()) and row.get("container_id") == cid,
                "Original worker exit copy differs")
        self.control(prefix + "-start", cid, "start", "POST", 204)
        wait = strict_value(self.control(prefix + "-wait", cid, "wait?condition=not-running", "POST", 200))
        final = self.inspect(prefix + "-final", cid)
        self.execution.validate_state(final, "exited")
        self.compare(prefix + "-final", running, final, spec, "running-to-exited")
        completed = engine.completion_evidence(wait, final, started=True, killed=killed, identity_ok=True)
        require(exact(completed, row["completion"]), "Raw worker completion reconstruction differs")
        require(wait.get("StatusCode") == final["State"]["ExitCode"] == row["exit_code"]
                and final["State"]["Pid"] == 0 and final["State"].get("OOMKilled") is False
                and not wait.get("Error") and final["Id"] == cid, "Worker completion join differs")
        if killed:
            self.boundary(prefix + "-prekill", running, spec)
            require(exact(self.json(prefix + "-kill-intent.json"), {"controller_action": "abrupt-SIGKILL", "container_id": cid,
                "graceful_stop": False, "running_sha256": self.execution.digest(running)}), "Original abrupt intent differs")
            body = self.control(prefix + "-kill", cid, "kill?signal=SIGKILL", "POST", 204)
            require(exact(self.json(prefix + "-kill-completion.json"), {"status": 204, "body_sha256": cases.sha(body),
                "framing_complete": True, "eof_observed": True, "container_id": cid}), "Original SIGKILL completion differs")
            require(row.get("abrupt_death") is True and row["exit_code"] == 137
                    and row["completion"].get("killed_by_controller") is True
                    and row["completion"].get("natural") is False, "SIGKILL cause unavailable")
        else:
            require(row["completion"].get("natural") is True, "Natural worker completion unavailable")
        return row["exit_code"]

    def worker_finished(self, row: dict[str, Any], start: dict[str, Any], *, killed: bool) -> tuple[int, bytes, bytes]:
        code = self.worker_exit(row, start, killed=killed)
        prefix, cid = start["start_label"], start["container_id"]
        original = self.json(prefix + "-completion.json")
        require(all(exact(row.get(key), value) for key, value in original.items()), "Original worker completion copy differs")
        require(row.get("capture_complete") is True and row.get("attach_eof_after_start_confirmation") is True,
                "Worker original capture unavailable")
        stdout, stderr = self.capture(prefix, cid, row["capture"])
        return code, stdout, stderr


def read_original(owner: Any, checkpoint: chain.PrefixCommitment) -> WorkerObservation:
    # Late import avoids a cycle while binding both complete source files.
    from . import candidate_product_worker_execution_v1 as execution
    require(type(owner) is execution.WorkerExecution and type(checkpoint) is chain.PrefixCommitment,
            "Exact original worker owner and anchored checkpoint required")
    require(owner.checkpoint() == checkpoint and owner.sources == execution.evaluator_sources() == execution._LOADED_SOURCES,
            "Original checkpoint or loaded source changed")
    execution.admission.verify_loaded_sources(owner.sources)
    owner.admission.check_current(owner.actual_registration, owner.retained_freeze)
    reader = _Reader(owner)
    require(exact(reader.json("config.json"), owner.config), "Original physical config differs")
    terminal = reader.json("terminal.json")
    require(terminal["protocol"] == execution.PROTOCOL and terminal["execution_id"] == owner.execution_id,
            "Original terminal differs")
    declaration = owner.profile.case.record
    rows = {row["step_id"]: row for row in terminal["steps"]}
    require(len(rows) == len(terminal["steps"]), "Duplicate observed step")
    expected = {row["step_id"]: row for row in declaration["expected"]}
    discrepancies: list[str] = []
    unknown: list[str] = []
    failed_steps: set[str] = set()
    unknown_steps: set[str] = set()
    snapshots: dict[str, dict[str, Any]] = {}
    kernels: dict[str, dict[str, Any]] = {}
    starts: dict[str, dict[str, Any]] = {}
    finished: dict[str, dict[str, Any]] = {}
    last_position = owner.journal._chain.position("intent.json")

    def fail(step: str, reason: str) -> None:
        failed_steps.add(step)
        discrepancies.append(step + ":" + reason)

    def unavailable(step: str, reason: str) -> None:
        unknown_steps.add(step)
        unknown.append(step + ":" + reason)

    if rows:
        try:
            require(reader.trusted("initial-directories", hooks.INITIAL_DIRECTORY_SOURCE) == {"created": True},
                    "Initial owned-directory setup differs")
        except (ValueError, KeyError, TypeError, OSError) as error:
            unknown.append("initial-state:" + str(error))
    for action in declaration["input"]["actions"]:
        step, op = action["id"], action["op"]
        row, want = rows.get(step), expected[step]
        if row is None or row.get("state") != "observed":
            # A later continuity/cleanup failure must not erase an independently
            # authenticated wrong exit or pre-death stdout from the original.
            if row is not None:
                try:
                    require(exact(reader.json(step + "-observation.json"), row), "Partial original row differs")
                    require(exact(reader.json(step + "-step-intent.json"), action), "Partial ordered input differs")
                    reader.action_guards(action)
                    if op == "cli" and "process" in row:
                        code = reader.cli_exit(row, action)
                        if code != want["exit_code"]:
                            fail(step, "independent complete CLI exit differs before later infrastructure loss")
                        _code, stdout, stderr = reader.cli(row, action)
                        value = product_json(stdout if want["exit_code"] == 0 else stderr)
                        if "json" in want and not exact(value, want["json"]):
                            fail(step, "independent complete CLI JSON differs before later infrastructure loss")
                        if "json_subset" in want and not subset(value, want["json_subset"]):
                            fail(step, "independent normative CLI fields differ before later infrastructure loss")
                    elif op in ("worker_kill", "worker_wait"):
                        code = reader.worker_exit(row, starts[action["worker"]], killed=op == "worker_kill")
                        if "exit_code" in want and code != want["exit_code"]:
                            fail(step, "independent natural worker exit differs before later capture loss")
                        code, stdout, _stderr = reader.worker_finished(row, starts[action["worker"]], killed=op == "worker_kill")
                        if want.get("stdout_empty") and stdout:
                            fail(step, "independent worker stdout preceded abrupt termination")
                        if "json" in want and not exact(product_json(stdout if want.get("exit_code", 0) == 0 else _stderr), want["json"]):
                            fail(step, "independent complete worker JSON differs before later infrastructure loss")
                except DurableDiscrepancy as error:
                    fail(step, str(error))
                except (ValueError, KeyError, TypeError, UnicodeError, RecursionError):
                    pass
            unavailable(step, "no complete original observation")
            continue
        try:
            position = owner.journal._chain.position(step + "-step-intent.json")
            observed_position = owner.journal._chain.position(step + "-observation.json")
            require(last_position < position < observed_position, "Original ordered step chronology differs")
            last_position = observed_position
            require(exact(reader.json(step + "-observation.json"), row), "Original step row differs")
            require(exact(reader.json(step + "-step-intent.json"), action), "Ordered input differs")
            reader.action_guards(action)
            if op == "cli":
                exit_code = reader.cli_exit(row, action)
                if exit_code != want["exit_code"]:
                    fail(step, "complete CLI exit differs")
                _exit_code, stdout, stderr = reader.cli(row, action)
                raw = stdout if want["exit_code"] == 0 else stderr
                # A proven wrong exit remains a discrepancy if JSON later is ambiguous.
                try:
                    value = strict_value(raw)
                except (original_observer.AmbiguousJSON, original_observer.ObservationLimit, RecursionError):
                    raise
                except (ValueError, UnicodeError):
                    fail(step, "complete CLI response has invalid JSON syntax")
                    reader.cli_after(row, action)
                    continue
                if "json" in want and not exact(value, want["json"]):
                    fail(step, "complete CLI JSON differs")
                if "json_subset" in want and not subset(value, want["json_subset"]):
                    fail(step, "normative CLI fields differ")
                reader.cli_after(row, action)
            elif op == "worker_start":
                starts[action["worker"]] = row
                reader.worker_started(action, row)
            elif op in ("worker_kill", "worker_wait"):
                start = starts[action["worker"]]
                code = reader.worker_exit(row, start, killed=op == "worker_kill")
                if "exit_code" in want and code != want["exit_code"]:
                    fail(step, "resumed old worker exit differs")
                code, stdout, stderr = reader.worker_finished(row, start, killed=op == "worker_kill")
                raw = stdout if want.get("exit_code", 0) == 0 else stderr
                finished[action["worker"]] = row
                if want.get("stdout_empty") and stdout:
                    fail(step, "worker emitted data before abrupt termination")
                if "json" in want and not exact(product_json(raw), want["json"]):
                    fail(step, "resumed old worker error differs")
                spec, _created, _running = reader.workers[action["worker"]]
                reader.keeper_boundary(step + "-after")
                reader.removed(step + "-retire", spec, row["container_id"], force=False)
                require(row.get("removed") is True, "Worker removal summary unavailable")
            elif op == "snapshot":
                require(exact(row["record"], reader.json(step + "-snapshot.json")), "Snapshot process record differs")
                value = reader.trusted(step + "-snapshot", hooks.SNAPSHOT_SOURCE)
                require(exact(value, row["value"]) and value.get("state") == "complete" and value.get("opened_identity_verified") is True, "Independent SQLite capture unavailable")
                snapshots[step] = value
                if "worker_generation" in want and controls(value).get("worker_generation") != str(want["worker_generation"]):
                    fail(step, "observed durable worker generation differs")
                if want.get("empty_graph") and not graph_empty(value):
                    fail(step, "durable provisional graph is visible")
                if "job" in want:
                    selected = selected_job(value)
                    if not exact(public_job(selected), want["job"]):
                        fail(step, "durable public job differs")
                    if want["job"]["state"] != "completed" and selected["receipt"] is not None:
                        fail(step, "noncompleted job has a durable receipt")
                if "same_receipt_as" in want:
                    old = selected_job(snapshots[want["same_receipt_as"]])
                    if selected_job(value)["receipt"] != old["receipt"]:
                        fail(step, "original stored receipt text changed")
                if "same_logical_as" in want and not exact(value["tables"], snapshots[want["same_logical_as"]]["tables"]):
                    fail(step, "receipt replay or stale token changed durable tables")
            elif op == "hook_observe":
                directory = hooks.ROOT + "/" + action["worker"]
                require(exact(row["event"]["record"], reader.json(step + "-event.json")), "Hook reader process record differs")
                event_read = reader.trusted(step + "-event", hooks.EVENT_SOURCE,
                    (directory, str(owner.policy.hook_timeout_seconds)),
                    maximum_timeout=owner.policy.hook_timeout_seconds + owner.policy.transport_timeout_seconds)
                require(exact(event_read, row["event"]["value"]) and event_read.get("state") == "observed",
                        "Hook coordinate unavailable")
                require(row["kernel"] is not None, "Kernel observation missing")
                require(exact(row["kernel"]["record"], reader.json(step + "-kernel.json")), "Kernel reader process record differs")
                value = reader.trusted(step + "-kernel", hooks.KERNEL_SOURCE, (directory,), worker=action["worker"])
                require(exact(value, row["kernel"]["value"]) and value.get("event_b64") == event_read.get("event_b64"),
                        "Hook event changed between observations")
                fact = kernel_lock_evidence(value)
                kernels[step] = fact
                require(fact["exclusive_owner_lock"], "Independent current owner lock unavailable")
                require(fact["event"]["phase"] == starts[action["worker"]]["hook_phase"], "Hook phase differs from input")
            elif op == "hook_release":
                require(exact(row["record"], reader.json(step + "-release.json")), "Release process record differs")
                require(reader.trusted(step + "-release", hooks.RELEASE_SOURCE, (hooks.ROOT + "/" + action["worker"],))
                        == {"released": True, "acknowledgement": "released"}, "Hook release unavailable")
        except DurableDiscrepancy as error:
            fail(step, str(error))
        except (ValueError, KeyError, TypeError, UnicodeError, OverflowError, RecursionError) as error:
            unavailable(step, type(error).__name__ + ":" + str(error)[:384])

    # Hook coordinates must join independently captured committed controls and
    # the same held database inode. A phase marker alone never grants authority.
    actions = declaration["input"]["actions"]
    for index, action in enumerate(actions):
        if action["op"] != "hook_observe" or action["id"] not in kernels:
            continue
        step = action["id"]
        try:
            next_snapshot = next(a["id"] for a in actions[index + 1:] if a["op"] in ("snapshot", "worker_kill", "worker_wait"))
            snapshot = snapshots[next_snapshot]
            event, current = kernels[step]["event"], controls(snapshot)
            require(type(event["worker_generation"]) is int and str(event["worker_generation"]) == current["worker_generation"]
                    and event["incarnation"] == current["incarnation"], "Hook owner does not match independently read durable controls")
            require(all(kernels[step]["database_identity"][key] == snapshot["identity"][key] for key in ("device", "inode")),
                    "Kernel and SQLite database identity differ")
            if event["job_id"] is not None:
                require(event["job_id"] == "recover" and type(event["epoch"]) is int and event["epoch"] == 1,
                        "Hook claimed job/token differs from frozen admitted work")
        except (ValueError, KeyError, TypeError, StopIteration) as error:
            unavailable(step, "hook corroboration:" + str(error))
    # Exact admitted manifest/hash text survives all physical worker epochs.
    original_job: dict[str, Any] | None = None
    if any("job" in wanted for wanted in expected.values()):
        for step, value in snapshots.items():
            try:
                observed_job = selected_job(value)
                if original_job is None:
                    original_job = observed_job
                elif any(observed_job[key] != original_job[key] for key in ("manifest", "content_hashes")):
                    fail(step, "original admitted manifest/hash serialization changed")
            except DurableDiscrepancy as error:
                fail(step, str(error))
            except (ValueError, KeyError, TypeError) as error:
                unavailable(step, "manifest conservation:" + str(error))
    identifier = declaration["id"]
    if "daemon-contention" in identifier:
        try:
            before, after = (snapshots[key] for key in ("s001", "s006"))
            a, b = controls(before), controls(after)
            if a.get("worker_generation") != "0" or b.get("worker_generation") != "1":
                fail("s006", "observed owner generation publication differs")
            # The successful owner allocation is the only allowed baseline delta.
            a["worker_generation"] = b["worker_generation"]
            if a != b or any(not exact(before["tables"][name], after["tables"][name])
                             for name in before["tables"] if name != "control"):
                fail("s006", "competitor changed jobs, graph or non-worker controls")
            for key in ("s003", "s010"):
                event = kernels[key]["event"]
                require(event["job_id"] is None and event["epoch"] is None, "idle hook claims active job")
        except DurableDiscrepancy as error:
            fail("s006", str(error))
        except (ValueError, KeyError, TypeError) as error:
            unavailable("s006", str(error))
    elif "after-commit" in identifier:
        for step, state, reason in completed_graph_findings(snapshots, ("s006", "s009", "s011"), 1):
            (fail if state == "failed" else unavailable)(step, reason)
        try:
            require(finished["w1"]["abrupt_death"] is True, "No actual postcommit abrupt death")
        except DurableDiscrepancy as error:
            fail("s006", str(error))
        except (ValueError, KeyError, TypeError) as error:
            unavailable("s006", str(error))
    elif "before-commit" in identifier:
        try:
            if not completed_graph(snapshots["s011"], 1):
                fail("s011", "fresh process did not produce exactly one admitted batch")
        except DurableDiscrepancy as error:
            fail("s011", str(error))
        except (ValueError, KeyError, TypeError) as error:
            unavailable("s011", str(error))
    elif "old-claim" in identifier:
        try:
            if not completed_graph(snapshots["s013"], 3):
                fail("s013", "fresh worker epoch3 graph differs")
        except DurableDiscrepancy as error:
            fail("s013", str(error))
        except (ValueError, KeyError, TypeError) as error:
            unavailable("s013", str(error))

    facets: list[Facet] = []
    for facet in declaration["facets"]:
        steps = set(facet["steps"])
        reasons: list[str] = []
        # Unqualified application instrumentation never receives phase credit.
        if facet["selector"] in ("precommit-abrupt-no-visible-graph", "old-claim-epoch-fence"):
            reasons.append(str(facet["qualification_limit"]))
        if steps & unknown_steps:
            reasons.append("At least one original required observation is unavailable.")
        state = "failed" if steps & failed_steps else "unavailable" if reasons else "passed"
        facets.append(Facet(facet["selector"], state, tuple(reasons)))
    cleanup_verified = False
    try:
        cleanup = reader.json("cleanup-result.json")
        cleanup_verified = (cleanup.get("all_resources_absent") is True and not cleanup.get("uncertainties")
            and cleanup.get("main_journal_healed") is False and cleanup.get("acceptance_authority") is False
            and set(row["name"] for row in cleanup["dispositions"]) == {*owner.owned, owner.volume}
            and all(row["status"] in ("removed", "already-absent") for row in cleanup["dispositions"]))
        require(cleanup_verified == (terminal.get("cleanup_verified") is True), "Cleanup census differs")
    except (ValueError, KeyError, TypeError) as error:
        unknown.append("cleanup:" + str(error))
    mechanics = cleanup_verified and not terminal.get("infrastructure") and not any(item.startswith("initial-state:") for item in unknown)
    if not mechanics:
        unknown.append("mechanics:incomplete original lifecycle, capture or cleanup")
    facets.append(Facet("mechanics", "passed" if mechanics and not unknown_steps else "unavailable", ()))
    require(owner.checkpoint() == checkpoint, "Original prefix changed during read")
    return WorkerObservation(PROTOCOL, owner.execution_id, execution.digest(asdict(owner.binding)),
        execution.digest(asdict(checkpoint)), tuple(discrepancies), tuple(unknown), tuple(facets),
        cleanup_verified)
