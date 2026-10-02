"""Durable, source-bound local work decisions for trusted offline fixtures.

No provider, task scheduler, source selection, Git or acceptance authority lives
here. Checksums detect corrupt local state; they do not authenticate a malicious
host. A completed action is evidence, never completed project work.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import math
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterator

from .peer_store_v1 import Store, canonical_bytes, strict_loads

PROTOCOL = "peer-work-v1"
MAX_VISIBLE = 32
MAX_TEXT_BYTES = 2000
MAX_REQUEST_TEXT_BYTES = 4096
MAX_JOURNAL_BYTES = 256_000
CONFIG_FIELDS = {"run_id", "task_id", "role", "seed_producer", "builder_peer", "authority_sha256"}
_SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_PHASES = {
    "prepared": {"claimed", "denied", "unknown"},
    "claimed": {"dispatch_pending", "denied", "unknown"},
    "dispatch_pending": {"result", "denied", "unknown"},
    "result": {"published"},
    "published": set(), "denied": set(), "unknown": set(),
}


class WorkError(ValueError):
    """Invalid work evidence, conflicting action or corrupt durable state."""


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value, max_bytes=MAX_JOURNAL_BYTES)).hexdigest()


def text_sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha(value: Any) -> str:
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise WorkError("Invalid digest")
    return value


def _text(value: Any, maximum: int = MAX_TEXT_BYTES) -> str:
    if type(value) is not str:
        raise WorkError("Fixture text must be a string")
    try:
        if len(value.encode("utf-8")) > maximum:
            raise WorkError("Fixture text exceeds byte limit")
    except UnicodeError as error:
        raise WorkError("Invalid text encoding") from error
    return value


def _config(config: dict[str, Any], node_id: str) -> dict[str, Any]:
    if type(config) is not dict or set(config) != CONFIG_FIELDS:
        raise WorkError("Invalid work configuration fields")
    if any(type(value) is not str or _SAFE.fullmatch(value) is None
           for value in [node_id, *config.values()]):
        raise WorkError("Invalid work identity")
    if config["role"] not in {"builder", "reviewer"}:
        raise WorkError("Unsupported work role")
    if config["role"] == "builder" and config["builder_peer"] != node_id:
        raise WorkError("Builder role does not match configured producer")
    _sha(config["authority_sha256"])
    return dict(config)


def _copy(value: Any) -> Any:
    return strict_loads(canonical_bytes(value, max_bytes=MAX_JOURNAL_BYTES),
                        max_bytes=MAX_JOURNAL_BYTES)


def _same(left: Any, right: Any) -> bool:
    """JSON equality must preserve booleans versus numeric values."""
    return canonical_bytes(left, max_bytes=MAX_JOURNAL_BYTES) == canonical_bytes(right, max_bytes=MAX_JOURNAL_BYTES)


def _ids(value: Any) -> list[str]:
    if (type(value) is not list or len(value) > MAX_VISIBLE
            or value != sorted(set(_sha(item) for item in value))):
        raise WorkError("Invalid bounded event ID list")
    return value


def _validate_intent(intent: Any, config: dict[str, Any] | None = None,
                     node_id: str | None = None) -> dict[str, Any]:
    fields = {"protocol", "run_id", "task_id", "role", "node_id", "generation",
              "config", "policy_sha256", "authority_sha256", "slot_id", "action_id", "source_sha256",
              "seed_event_id", "visible_event_ids", "prerequisite_event_ids",
              "event_artifact_sha256", "request", "request_sha256", "intent_sha256"}
    if type(intent) is not dict or set(intent) != fields or intent["protocol"] != PROTOCOL:
        raise WorkError("Invalid action intent fields")
    policy = _config(intent["config"], intent["node_id"])
    if config is not None and policy != config or node_id is not None and intent["node_id"] != node_id:
        raise WorkError("Action belongs to a different configuration or node")
    if any(intent[key] != policy[key] for key in ("run_id", "task_id", "role", "authority_sha256")):
        raise WorkError("Action policy identity differs")
    if type(intent["generation"]) is not int or intent["generation"] != 0:
        raise WorkError("This phase supports only generation zero")
    visible, consumed = _ids(intent["visible_event_ids"]), _ids(intent["prerequisite_event_ids"])
    if not consumed or not set(consumed) <= set(visible) or intent["seed_event_id"] not in consumed:
        raise WorkError("Prerequisite was not in the local snapshot")
    artifacts = intent["event_artifact_sha256"]
    if type(artifacts) is not dict or set(artifacts) != set(visible):
        raise WorkError("Local artifact bindings differ from the visible snapshot")
    for sha in artifacts.values():
        _sha(sha)
    request = intent["request"]
    if (type(request) is not dict or set(request) != {"text", "context"}
            or type(request["context"]) is not dict
            or set(request["context"]) != {"source_sha256", "event_ids"}):
        raise WorkError("Invalid fixture request")
    _text(request["text"], MAX_TEXT_BYTES if intent["role"] == "builder" else MAX_REQUEST_TEXT_BYTES)
    canonical_bytes(request, max_bytes=16_000)
    source = text_sha(request["text"])
    if (intent["source_sha256"] != source
            or request["context"] != {"source_sha256": source, "event_ids": visible}
            or intent["request_sha256"] != digest(request)
            or intent["policy_sha256"] != digest({"protocol": PROTOCOL, "config": policy})):
        raise WorkError("Source, request or policy binding differs")
    slot = {key: intent[key] for key in ("protocol", "run_id", "task_id", "role", "node_id", "generation")}
    if intent["slot_id"] != digest(slot):
        raise WorkError("Invalid action slot")
    action_body = {key: value for key, value in intent.items() if key not in {"action_id", "intent_sha256"}}
    if intent["action_id"] != digest(action_body):
        raise WorkError("Invalid action identity")
    if intent["intent_sha256"] != digest({key: value for key, value in intent.items() if key != "intent_sha256"}):
        raise WorkError("Invalid intent checksum")
    return intent


def _clock(value: Any) -> bool:
    return type(value) in (int, float) and 0 <= value <= 1e30 and math.isfinite(value)


def _claim(intent: dict[str, Any], receipt: Any) -> None:
    if (type(receipt) is not dict
            or set(receipt) != {"status", "run_id", "config_sha256", "lease", "authority_time"}
            or receipt["status"] != "ok" or receipt["run_id"] != intent["run_id"]
            or receipt["config_sha256"] != intent["authority_sha256"] or not _clock(receipt["authority_time"])):
        raise WorkError("Invalid claim authority receipt")
    lease = receipt["lease"]
    if (type(lease) is not dict or set(lease) != {"task_id", "worker_id", "epoch", "expires_at"}
            or lease["task_id"] != intent["task_id"] or lease["worker_id"] != intent["node_id"]
            or type(lease["epoch"]) is not int or not 1 <= lease["epoch"] <= 2**63 - 1
            or not _clock(lease["expires_at"]) or lease["expires_at"] <= receipt["authority_time"]):
        raise WorkError("Claim lease binding differs")


def _completed(intent: dict[str, Any], receipt: Any, result: Any) -> None:
    fields = {"status", "run_id", "config_sha256", "task_id", "epoch", "action_id",
              "reservation_id", "request_sha256", "authority_time", "result", "usage_units"}
    if type(receipt) is not dict or set(receipt) != fields or receipt["status"] != "completed":
        raise WorkError("Missing completed authority receipt")
    if (any(receipt[key] != intent[key] for key in ("run_id", "task_id", "action_id", "request_sha256"))
            or receipt["config_sha256"] != intent["authority_sha256"]
            or type(receipt["epoch"]) is not int or not 1 <= receipt["epoch"] <= 2**63 - 1
            or type(receipt["reservation_id"]) is not str or not 1 <= len(receipt["reservation_id"]) <= 256
            or not _clock(receipt["authority_time"])
            or type(receipt["usage_units"]) is not int or receipt["usage_units"] != 0
            or type(result) is not dict or not _same(receipt["result"], result)):
        raise WorkError("Authority completion binding differs")
    if intent["role"] == "builder":
        if set(result) != {"text", "text_sha256"}:
            raise WorkError("Invalid build result fields")
        _text(result["text"], MAX_REQUEST_TEXT_BYTES)
        if result["text_sha256"] != text_sha(result["text"]):
            raise WorkError("Build result hash differs")
    else:
        if (set(result) != {"text_sha256", "nonempty", "is_uppercase", "decision"}
                or result["text_sha256"] != intent["source_sha256"]
                or type(result["nonempty"]) is not bool or type(result["is_uppercase"]) is not bool
                or result["decision"] not in {"approve", "reject"}):
            raise WorkError("Invalid review result fields or source binding")


def _outbox_bound(intent: dict[str, Any]) -> None:
    # Reserve 2KB for the bounded authority identity, envelope and variable clock
    # fields. This is capacity admission only, not execution or a review verdict.
    possible = ({"text": intent["request"]["text"].upper(), "text_sha256": "0" * 64}
                if intent["role"] == "builder" else
                {"text_sha256": "0" * 64, "nonempty": False, "is_uppercase": False, "decision": "approve"})
    if intent["role"] == "builder":
        _text(possible["text"], MAX_REQUEST_TEXT_BYTES)
    try:
        canonical_bytes({"intent": intent, "result": possible, "authority": {"result": possible}},
                        max_bytes=16_000 - 2048)
    except ValueError as error:
        raise WorkError("Work result would exceed bounded dissemination capacity") from error


def plan_action(store: Store, config: dict[str, Any]) -> dict[str, Any] | None:
    """Choose from one arrived local snapshot; never consult another peer.

    A caller must resume an existing journal action before asking for a new plan.
    Newly arrived, irrelevant events also bind the frozen context but cannot
    replace the one action already persisted for this slot.
    """
    config = _config(config, store.node_id)
    events = store.state()["events"]
    if len(events) > MAX_VISIBLE:
        raise WorkError("Local decision visibility exceeds bounded contract")
    visible = sorted(event["event_id"] for event in events)
    artifacts = {event["event_id"]: event["artifact_sha256"] for event in events}
    content = {event["event_id"]: store.artifact(event["artifact_sha256"]) for event in events}
    seeds = []
    for event in events:
        value = content[event["event_id"]]
        if (event["kind"] == "work_seed" and event["producer"] == config["seed_producer"]
                and value.get("run_id") == config["run_id"]):
            if (set(value) != {"protocol", "run_id", "generation", "text"}
                    or value["protocol"] != PROTOCOL or type(value["generation"]) is not int
                    or value["generation"] != 0):
                raise WorkError("Invalid seed or unsupported source supersession")
            _text(value["text"])
            seeds.append(event)
    if not seeds:
        return None
    if len(seeds) != 1:
        raise WorkError("Ambiguous seed for this source generation")
    seed = seeds[0]
    seed_id = seed["event_id"]
    text = content[seed_id]["text"]
    consumed = [seed_id]
    if config["role"] == "reviewer":
        results = []
        for event in events:
            value = content[event["event_id"]]
            if (event["kind"] != "work_result" or event["producer"] != config["builder_peer"]
                    or value.get("run_id") != config["run_id"]):
                continue
            if (set(value) != {"protocol", "run_id", "generation", "role", "intent", "result", "authority"}
                    or value["protocol"] != PROTOCOL or value["role"] != "builder"
                    or type(value["generation"]) is not int or value["generation"] != 0):
                raise WorkError("Invalid builder evidence envelope")
            prior = _validate_intent(value["intent"], node_id=config["builder_peer"])
            if (prior["role"] != "builder" or prior["run_id"] != config["run_id"]
                    or prior["config"]["seed_producer"] != config["seed_producer"]
                    or prior["seed_event_id"] != seed_id or prior["request"]["text"] != text
                    or prior["prerequisite_event_ids"] != [seed_id]
                    or prior["event_artifact_sha256"].get(seed_id) != artifacts[seed_id]):
                raise WorkError("Builder result is not bound to this seed")
            result = value["result"]
            if prior["authority_sha256"] != config["authority_sha256"]:
                raise WorkError("Builder used a different authority profile")
            _completed(prior, value["authority"], result)
            results.append((event, result))
        if not results:
            return None
        if len(results) != 1:
            raise WorkError("Ambiguous builder result for the source generation")
        event, result = results[0]
        consumed.append(event["event_id"])
        text = result["text"]
    source = text_sha(text)
    request = {"text": text, "context": {"source_sha256": source, "event_ids": visible}}
    intent = dict(protocol=PROTOCOL, run_id=config["run_id"], task_id=config["task_id"],
                  role=config["role"], node_id=store.node_id, generation=0, config=config,
                  authority_sha256=config["authority_sha256"],
                  policy_sha256=digest({"protocol": PROTOCOL, "config": config}),
                  source_sha256=source, seed_event_id=seed_id, visible_event_ids=visible,
                  prerequisite_event_ids=sorted(consumed), event_artifact_sha256=artifacts,
                  request=request, request_sha256=digest(request))
    intent["slot_id"] = digest({key: intent[key] for key in
        ("protocol", "run_id", "task_id", "role", "node_id", "generation")})
    intent["action_id"] = digest(intent)
    intent["intent_sha256"] = digest(intent)
    _validate_intent(intent, config, store.node_id)
    _outbox_bound(intent)
    return intent


class WorkJournal:
    """One immutable action slot with atomic durable progress and an outbox."""

    def __init__(self, path: Path, node_id: str, config: dict[str, Any]):
        self.path, self.node_id = Path(path), node_id
        self.config = _config(config, node_id)
        self.identity = {"protocol": PROTOCOL, "node_id": node_id, "config": self.config}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            if db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise WorkError("Work journal requires WAL")
            db.execute("BEGIN IMMEDIATE")
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if tables and tables != {"metadata", "action"}:
                raise WorkError("Foreign or incomplete work journal schema")
            if not tables:
                db.execute("CREATE TABLE metadata (id INTEGER PRIMARY KEY CHECK(id=1), payload BLOB NOT NULL)")
                db.execute("CREATE TABLE action (id INTEGER PRIMARY KEY CHECK(id=1), payload BLOB NOT NULL)")
                db.execute("INSERT INTO metadata VALUES (1, ?)", (canonical_bytes(self.identity),))
            self._load(db)
            db.commit()

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        db = None
        try:
            db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
            db.execute("PRAGMA synchronous=FULL")
            yield db
        except sqlite3.Error as error:
            raise WorkError("Work journal SQLite failure") from error
        finally:
            if db is not None:
                db.close()

    def _load(self, db: sqlite3.Connection) -> dict[str, Any] | None:
        for table in ("metadata", "action"):
            if (db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] > 1
                    or db.execute(f"SELECT 1 FROM {table} WHERE id!=1 OR typeof(payload)!='blob' OR length(payload)>?", (MAX_JOURNAL_BYTES,)).fetchone()):
                raise WorkError("Invalid work journal rows")
        meta = db.execute("SELECT payload FROM metadata WHERE id=1").fetchone()
        if meta is None or meta[0] != canonical_bytes(self.identity):
            raise WorkError("Work journal identity changed")
        row = db.execute("SELECT payload FROM action WHERE id=1").fetchone()
        if row is None:
            return None
        value = strict_loads(row[0], max_bytes=MAX_JOURNAL_BYTES)
        expected = {"intent", "phase", "claim", "response", "result", "outbox", "outbox_command_id", "event_id", "checksum"}
        if type(value) is not dict or set(value) != expected or value["phase"] not in _PHASES:
            raise WorkError("Invalid durable work record")
        if row[0] != canonical_bytes(value, max_bytes=MAX_JOURNAL_BYTES) or value["checksum"] != digest({k: v for k, v in value.items() if k != "checksum"}):
            raise WorkError("Work journal checksum or canonical encoding differs")
        _validate_intent(value["intent"], self.config, self.node_id)
        if value["outbox_command_id"] != digest([PROTOCOL, "outbox", value["intent"]["action_id"]]):
            raise WorkError("Invalid durable outbox command")
        for key in ("claim", "response", "result", "outbox"):
            if value[key] is not None and type(value[key]) is not dict:
                raise WorkError("Invalid durable receipt")
        phase = value["phase"]
        if phase in {"claimed", "dispatch_pending", "result", "published"} and value["claim"] is None:
            raise WorkError("Claim receipt is missing")
        if value["claim"] is not None:
            _claim(value["intent"], value["claim"])
        if phase in {"result", "published"} and (value["result"] is None or value["response"] is None):
            raise WorkError("Completed work lacks durable result or authority receipt")
        if phase in {"result", "published"}:
            _completed(value["intent"], value["response"], value["result"])
            if value["response"]["epoch"] != value["claim"]["lease"]["epoch"]:
                raise WorkError("Completion belongs to a different lease epoch")
        elif value["result"] is not None:
            raise WorkError("Result appears before authority completion")
        if phase in {"prepared", "claimed", "dispatch_pending"} and value["response"] is not None:
            raise WorkError("Response appears before a recorded outcome")
        if phase == "prepared" and value["claim"] is not None:
            raise WorkError("Claim appears before authorization")
        if phase == "published":
            _sha(value["event_id"])
            if value["outbox"] is None:
                raise WorkError("Published work lacks an outbox")
        elif value["event_id"] is not None:
            raise WorkError("Unexpected event publication")
        if value["outbox"] is not None and phase not in {"result", "published"}:
            raise WorkError("Outbox exists before a completed result")
        if value["outbox"] is not None:
            expected_outbox = {"protocol": PROTOCOL, "run_id": self.config["run_id"], "generation": 0,
                               "role": self.config["role"], "intent": value["intent"],
                               "result": value["result"], "authority": value["response"]}
            if not _same(value["outbox"], expected_outbox):
                raise WorkError("Outbox differs from the completed action")
        return value

    def _save(self, db: sqlite3.Connection, value: dict[str, Any]) -> dict[str, Any]:
        value["checksum"] = digest({key: item for key, item in value.items() if key != "checksum"})
        raw = canonical_bytes(value, max_bytes=MAX_JOURNAL_BYTES)
        db.execute("INSERT INTO action VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (raw,))
        checked = self._load(db)
        db.commit()
        assert checked is not None
        return checked

    def prepare(self, intent: dict[str, Any]) -> dict[str, Any]:
        intent = _copy(_validate_intent(intent, self.config, self.node_id))
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            value = self._load(db)
            if value is not None:
                if not _same(value["intent"], intent):
                    raise WorkError("This slot already has a different immutable action")
                return value
            value = dict(intent=intent, phase="prepared", claim=None, response=None, result=None,
                         outbox=None, outbox_command_id=digest([PROTOCOL, "outbox", intent["action_id"]]), event_id=None)
            return self._save(db, value)

    def read(self, action_id: str | None = None) -> dict[str, Any] | None:
        with self._db() as db:
            db.execute("BEGIN")
            value = self._load(db)
            if action_id is not None and (value is None or value["intent"]["action_id"] != action_id):
                raise WorkError("Unknown action")
            return value

    def record(self, action_id: str, phase: str, *, claim: dict | None = None,
               response: dict | None = None, result: dict | None = None) -> dict[str, Any]:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            value = self._load(db)
            if value is None or value["intent"]["action_id"] != action_id:
                raise WorkError("Unknown action")
            if phase not in _PHASES or phase != value["phase"] and phase not in _PHASES[value["phase"]]:
                raise WorkError("Invalid work phase transition")
            if phase == "published":
                raise WorkError("Use mark_published after durable outbox publication")
            for key, supplied in (("claim", claim), ("response", response), ("result", result)):
                if supplied is not None:
                    supplied = _copy(supplied)
                    if value[key] is not None and not _same(value[key], supplied):
                        raise WorkError("Durable receipt cannot be replaced")
                    value[key] = supplied
            value["phase"] = phase
            return self._save(db, value)

    def set_outbox(self, action_id: str, content: dict[str, Any]) -> dict[str, Any]:
        content = _copy(content)
        canonical_bytes({"protocol": "gossip-peer-v1", "content": content}, max_bytes=16_000)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            value = self._load(db)
            if (value is None or value["intent"]["action_id"] != action_id
                    or value["phase"] not in {"result", "published"}):
                raise WorkError("Outbox requires completed work")
            if value["outbox"] is not None and not _same(value["outbox"], content):
                raise WorkError("Durable outbox cannot be replaced")
            value["outbox"] = content
            return self._save(db, value)

    def mark_published(self, action_id: str, event_id: str) -> dict[str, Any]:
        _sha(event_id)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            value = self._load(db)
            if (value is None or value["intent"]["action_id"] != action_id
                    or value["phase"] not in {"result", "published"} or value["outbox"] is None):
                raise WorkError("Publication requires a durable completed outbox")
            if value["event_id"] is not None and value["event_id"] != event_id:
                raise WorkError("Publication event cannot be replaced")
            value.update(phase="published", event_id=event_id)
            return self._save(db, value)
