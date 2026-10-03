"""Prospectively authored public v2 development histories, never private acceptance.

Only the public v2 contract and existing observation adapter interfaces informed
these definitions. No v2 implementation, implementation tests, or execution
results were inspected. The child receives inputs only. Expectations and
relations remain in this host module. A caller owns sandboxing/provenance.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import inspect
import json
from typing import Any

PROTOCOL = "library-v2-public-development-histories-v1"
PURPOSE = "development_qualification"
CONTRACT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
COUNTER_MAX = 9223372036854775807
AMENDMENT_IDS = ("V2-MIGRATION-FENCE", "V2-MIGRATION-DIAGNOSTIC", "V2-COUNTER-DOMAIN",
                 "V2-M1-HASH-SERIALIZATION", "V2-WORKER-LIVENESS", "V2-BACKUP-ROOT")
LIMITATIONS = (
    "Public development qualification, not concealed independent final acceptance or statistical treatment evidence.",
    "Finite amendment examples do not cover every inherited requirement, every interface, crash boundary or concurrency interleaving.",
    "The observer executes inside caller isolation; caller must authenticate source, runtime, outputs, limits, protocol and purpose.",
    "Actual HTTP wire behavior and browser exact-integer transport require separate lanes; Service.request is routing only.",
    "Live matching active claim, stale claim dimensions and before/after-commit abrupt worker crash need separately synchronized histories; no timing-based claim is inferred.",
    "Legacy-handle history requires the declared prospective schema3 predecessor at /legacy-v2; historical unsupported binaries must be shut down separately.",
    "Adoption metadata-page/read-size bounds, no unowned reads, replacement race, concurrent authority and power-loss need separate instrumented/process lanes.",
    "Logical portable rows are compared on valid opens; implementation-owned indexes/triggers are not prescribed.",
    "File observations cover authored maintenance artifacts and backup-root entries, not implementation-specific lock/control files; sandboxing alone does not authenticate a hostile in-process observation.",
    "No held-out result may be sent to active builders; this module makes no cohort-freeze or receipt-authentication claim.",
)


def _bytes(value: Any, *, ascii: bool = True) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=ascii, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(_bytes(value)).hexdigest()


def _document(source: str, text: str) -> dict[str, Any]:
    return {"document_id": "doc-" + hashlib.sha256(b"document\0" + source.encode()).hexdigest(),
            "source_id": "src-" + hashlib.sha256(b"source\0" + source.encode()).hexdigest(),
            "source": source, "title": source.rsplit("/", 1)[-1],
            "blob_id": "blob-" + hashlib.sha256(text.encode()).hexdigest(), "text": text}


def _record(source: str = "a.txt", text: str = "alpha", version: int = 1) -> dict[str, Any]:
    return {"document": _document(source, text), "revision": 1, "edit_version": version,
            "deleted": False, "notes": "", "tags": [], "collections": []}


def _job(name: str, entries: list[dict[str, str]], *, epoch: int = 1,
         state: str = "queued", hashes: str | None = None) -> dict[str, Any]:
    entries = sorted(entries, key=lambda x: (x["source"], x["text"]))
    def digest(text: str) -> str | None:
        try:
            return hashlib.sha256(text.encode("utf-8", "strict")).hexdigest()
        except UnicodeEncodeError:
            return None
    job = {"job_id": name, "epoch": epoch, "state": state, "total": len(entries),
           "completed": len(entries) if state == "completed" else 0,
           "error": "invalid_utf8" if state == "failed" else None}
    receipt = {"job": job, "documents": [_document(x["source"], x["text"]) for x in entries]} if state == "completed" else None
    return {"job": job, "manifest_json": _bytes(entries).decode(),
            "content_hashes_json": hashes if hashes is not None else _bytes([digest(x["text"]) for x in entries]).decode(),
            "receipt_json": _bytes(receipt).decode() if receipt else None, "enrolled": False}


def _backup(records: list[dict[str, Any]], *, generation: int = 0,
            jobs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    blobs = {r["document"]["blob_id"]: r["document"]["text"] for r in records}
    for row in jobs or []:
        if row["receipt_json"]:
            for doc in json.loads(row["receipt_json"])["documents"]:
                blobs[doc["blob_id"]] = doc["text"]
    payload = {"schema": 3, "generation": generation,
               "documents": sorted(records, key=lambda r: (r["document"]["source"], r["document"]["document_id"])),
               "revisions": sorted([{"document_id": r["document"]["document_id"], "revision": 1,
                                      "blob_id": r["document"]["blob_id"]} for r in records], key=lambda r: r["document_id"]),
               "blobs": [{"blob_id": key, "text": blobs[key]} for key in sorted(blobs)],
               "collections": [], "jobs": sorted(jobs or [], key=lambda j: j["job"]["job_id"])}
    return {"format": "local-research-library-backup-v3", "payload": payload,
            "payload_sha256": hashlib.sha256(_bytes(payload, ascii=False)).hexdigest()}


def _fixture(*, schema: int = 4, generation: int = 0, records: list[dict[str, Any]] | None = None,
             jobs: list[dict[str, Any]] | None = None, control: dict[str, Any] | None = None,
             job_high: dict[str, int] | None = None, document_high: dict[str, int] | None = None,
             artifacts: list[list[Any]] | None = None, backups: list[list[Any]] | None = None,
             collections: list[str] | None = None) -> dict[str, Any]:
    controls = {"incarnation": "portable-v2-initial", "worker_generation": "3",
                "last_error": "null", "cleanup_cursor": "null"}
    controls.update(control or {})
    return {"schema": schema, "generation": str(generation), "records": records or [], "jobs": jobs or [],
            "control": controls, "job_high": job_high or {}, "document_high": document_high or {},
            "artifacts": artifacts or [], "backups": backups or [], "collections": collections or []}


def _call(method: str, *args: Any, target: str = "service", select: list[str] | None = None,
          observe: bool = True, **kwargs: Any) -> dict[str, Any]:
    action = {"op": "call", "target": target, "method": method, "args": list(args), "kwargs": kwargs, "observe": observe}
    if select is not None:
        action["select"] = select
    return action


def _cli(*args: str, backup_dir: str = "$BASE/backups", select: list[str] | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"op": "cli", "args": list(args), "backup_dir": backup_dir}
    if select is not None:
        result["value_select"] = select
    return result


def _cli_result(value: Any, code: int = 0) -> dict[str, Any]:
    return {"exit": code, "value": value, "other_stream_empty": True}


def _error(code: str) -> dict[str, str]:
    return {"error": code}


def _snap(*, exclude: list[str] | None = None) -> dict[str, Any]:
    return {"op": "snapshot", "exclude_controls": exclude or []}


def _digest() -> dict[str, str]:
    return {"$kind": "sha256"}


def _ref(index: int, *path: str | int) -> dict[str, Any]:
    return {"$ref": [index, *path]}


def _case(name: str, fixture: dict[str, Any], actions: list[dict[str, Any]], expected: list[Any],
          amendments: list[str], *, files: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {"id": "v2-" + name, "amendment_ids": amendments,
            "input": {"fixture": fixture, "files": files or [], "actions": actions},
            "expected": {"observations": expected}}


# This program has no expectations, correctness predicates or reference-source imports.
# SQLite writes construct authored portable fixtures BEFORE candidate opening only.
CHILD_ADAPTER = r'''import hashlib
import json
from pathlib import Path
import select
import sqlite3
import subprocess
import sys
import tempfile
import time

WORKSPACE = sys.argv[1] if len(sys.argv) > 1 else "/workspace"
LEGACY = sys.argv[2] if len(sys.argv) > 2 else "/legacy-v2"
raw = sys.stdin.buffer.read(262145)
if len(raw) > 262144: raise ValueError("input bound")
payload = json.loads(raw)
sys.path.insert(0, WORKSPACE)
from library.catalog.store import Store
from library.common import LibraryError
from library.ingestion.jobs import JobManager
from library.query.service import Service

def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode()

def strict_json(raw):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out: raise ValueError("duplicate JSON key")
            out[key] = value
        return out
    def constant(_): raise ValueError("nonfinite JSON")
    return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)

def expand(value):
    if type(value) is str: return value.replace("$BASE", str(base))
    if type(value) is list: return [expand(v) for v in value]
    if type(value) is dict: return {k: expand(v) for k, v in value.items()}
    return value

def normalized(value):
    if type(value) is str: return value.replace(str(base), "$BASE")
    if type(value) is list: return [normalized(v) for v in value]
    if type(value) is tuple: return [normalized(v) for v in value]
    if type(value) is dict: return {k: normalized(v) for k, v in value.items()}
    if type(value) is bytes: return {"blob_hex": value.hex()}
    return value

TABLES = {
 "metadata": "key TEXT PRIMARY KEY,value TEXT NOT NULL",
 "blobs": "blob_id TEXT PRIMARY KEY,content BLOB NOT NULL",
 "documents": "document_id TEXT PRIMARY KEY,source_id TEXT UNIQUE NOT NULL,source TEXT UNIQUE NOT NULL,blob_id TEXT NOT NULL,title TEXT NOT NULL",
 "jobs": "job_id TEXT PRIMARY KEY,epoch INTEGER NOT NULL,state TEXT NOT NULL,total INTEGER NOT NULL,completed INTEGER NOT NULL,error TEXT,manifest TEXT NOT NULL,content_hashes TEXT NOT NULL,receipt TEXT",
 "collections": "name TEXT PRIMARY KEY",
 "control": "key TEXT PRIMARY KEY,value TEXT NOT NULL",
 "job_control": "job_id TEXT PRIMARY KEY,epoch_high_water INTEGER NOT NULL,enrolled INTEGER NOT NULL",
 "document_control": "document_id TEXT PRIMARY KEY,edit_high_water INTEGER NOT NULL",
 "maintenance_artifacts": "artifact_id TEXT PRIMARY KEY,kind TEXT NOT NULL,root_kind TEXT NOT NULL,relative_path TEXT NOT NULL,owner_incarnation TEXT NOT NULL,owner_generation INTEGER NOT NULL,state TEXT NOT NULL",
 "backups": "name TEXT PRIMARY KEY,bytes INTEGER NOT NULL,payload_sha256 TEXT NOT NULL,generation INTEGER NOT NULL",
 "search_state": "singleton INTEGER PRIMARY KEY,published_generation INTEGER,target_generation INTEGER,processed INTEGER NOT NULL,total INTEGER NOT NULL,cursor TEXT",
 "search_entries": "generation INTEGER NOT NULL,document_id TEXT NOT NULL,source TEXT NOT NULL,text TEXT NOT NULL,PRIMARY KEY(generation,document_id)"}

def make_fixture(spec):
    connection = sqlite3.connect(db)
    try:
        tables = dict(TABLES)
        if spec["schema"] == 3:
            tables.update(lifecycle="document_id TEXT PRIMARY KEY REFERENCES documents(document_id),current_revision INTEGER NOT NULL,edit_version INTEGER NOT NULL,deleted INTEGER NOT NULL,notes TEXT NOT NULL,tags TEXT NOT NULL,collections TEXT NOT NULL",
                          revisions="document_id TEXT NOT NULL REFERENCES documents(document_id),revision INTEGER NOT NULL,blob_id TEXT NOT NULL REFERENCES blobs(blob_id),PRIMARY KEY(document_id,revision)")
        else:
            tables.update(document_revisions="revision_id TEXT PRIMARY KEY,document_id TEXT NOT NULL REFERENCES documents(document_id),revision INTEGER NOT NULL,blob_id TEXT NOT NULL REFERENCES blobs(blob_id),UNIQUE(document_id,revision)",
                          document_state="document_id TEXT PRIMARY KEY REFERENCES documents(document_id),head_revision_id TEXT NOT NULL REFERENCES document_revisions(revision_id),edit_version INTEGER NOT NULL,deleted INTEGER NOT NULL,notes TEXT NOT NULL,tags TEXT NOT NULL,collections TEXT NOT NULL")
        for name, columns in tables.items(): connection.execute("CREATE TABLE " + name + "(" + columns + ")")
        connection.executemany("INSERT INTO metadata VALUES (?,?)", [("schema",str(spec["schema"])),("catalog_generation",spec["generation"])])
        connection.executemany("INSERT INTO control VALUES (?,?)", sorted(spec["control"].items()))
        connection.execute("INSERT INTO search_state VALUES (1,NULL,NULL,0,0,NULL)")
        connection.executemany("INSERT INTO collections VALUES (?)", [(n,) for n in spec["collections"]])
        dh, jh = dict(spec["document_high"]), dict(spec["job_high"])
        for row in spec["records"]:
            doc = row["document"]
            connection.execute("INSERT OR IGNORE INTO blobs VALUES (?,?)", (doc["blob_id"],doc["text"].encode()))
            connection.execute("INSERT INTO documents VALUES (?,?,?,?,?)", tuple(doc[k] for k in ("document_id","source_id","source","blob_id","title")))
            tail = (row["edit_version"],int(row["deleted"]),row["notes"],encode(row["tags"]).decode(),encode(row["collections"]).decode())
            if spec["schema"] == 3:
                connection.execute("INSERT INTO lifecycle VALUES (?,?,?,?,?,?,?)", (doc["document_id"],1,*tail))
                connection.execute("INSERT INTO revisions VALUES (?,?,?)", (doc["document_id"],1,doc["blob_id"]))
            else:
                rid = "rev-" + hashlib.sha256(b"revision\0" + doc["document_id"].encode() + b"\0" + b"1\0" + doc["blob_id"].encode()).hexdigest()
                connection.execute("INSERT INTO document_revisions VALUES (?,?,?,?)", (rid,doc["document_id"],1,doc["blob_id"]))
                connection.execute("INSERT INTO document_state VALUES (?,?,?,?,?,?,?)", (doc["document_id"],rid,*tail))
            dh.setdefault(doc["document_id"],row["edit_version"])
        for row in spec["jobs"]:
            j = row["job"]
            connection.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?)", tuple(j[k] for k in ("job_id","epoch","state","total","completed","error")) + (row["manifest_json"],row["content_hashes_json"],row["receipt_json"]))
            if row["receipt_json"]:
                for doc in json.loads(row["receipt_json"])["documents"]:
                    connection.execute("INSERT OR IGNORE INTO blobs VALUES (?,?)",(doc["blob_id"],doc["text"].encode()))
            jh.setdefault(j["job_id"],j["epoch"])
        enrolled = {j["job"]["job_id"]: int(j["enrolled"]) for j in spec["jobs"]}
        connection.executemany("INSERT INTO job_control VALUES (?,?,?)", [(key,value,enrolled.get(key,0)) for key,value in sorted(jh.items())])
        connection.executemany("INSERT INTO document_control VALUES (?,?)", sorted(dh.items()))
        connection.executemany("INSERT INTO maintenance_artifacts VALUES (?,?,?,?,?,?,?)",spec["artifacts"])
        connection.executemany("INSERT INTO backups VALUES (?,?,?,?)",spec["backups"])
        connection.commit()
    finally: connection.close()

def read_rows(query, args=()):
    connection = sqlite3.connect("file:" + str(db) + "?mode=ro",uri=True)
    try: return [list(row) for row in connection.execute(query,args)]
    finally: connection.close()

def snapshot(excludes):
    names = [row[0] for row in read_rows("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    known = set(TABLES) | {"lifecycle","revisions","document_state","document_revisions"}
    rows = {}
    for name in names:
        if name not in known: continue
        values = read_rows("SELECT * FROM " + name)
        if name == "control": values = [v for v in values if v[0] not in excludes]
        rows[name] = sorted(normalized(values),key=encode)
    return hashlib.sha256(encode(rows)).hexdigest()

def command(arguments, selected):
    bootstrap = "import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module('library',run_name='__main__')"
    return [sys.executable,"-I","-B","-c",bootstrap,WORKSPACE,"--db",str(db),"--root",str(root),"--backup-dir",selected,*arguments]

def cli(action):
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        process = subprocess.run(command(action["args"],action["backup_dir"]),stdin=subprocess.DEVNULL,stdout=out,stderr=err,timeout=12,check=False)
        out.seek(0); err.seek(0); raw, error = out.read(262145), err.read(262145)
    if len(raw)>262144 or len(error)>262144: raise ValueError("CLI output bound")
    value = strict_json(raw if process.returncode == 0 else error)
    if "value_select" in action and type(value) is dict: value = {k:value[k] for k in action["value_select"]}
    return {"exit":process.returncode,"value":value,"other_stream_empty":not (error if process.returncode == 0 else raw)}

def invoke(action):
    receiver = store if action["target"] == "store" else jobs if action["target"] == "jobs" else service
    result = getattr(receiver,action["method"])(*action["args"],**action["kwargs"])
    if action["method"] == "request" and type(result) is tuple and len(result)==2 and type(result[1]) is bytes:
        result = (result[0],strict_json(result[1]))
    return result

with tempfile.TemporaryDirectory() as directory:
    base = Path(directory); root = base/"input"; root.mkdir(); (base/"backups").mkdir(); (base/"next").mkdir()
    db=base/"library.sqlite"; payload=expand(payload)
    make_fixture(payload["fixture"])
    for item in payload["files"]:
        path=Path(item["path"])
        if not path.is_relative_to(base): raise ValueError("authored path outside root")
        path.parent.mkdir(parents=True,exist_ok=True)
        if "symlink" in item: path.symlink_to(item["symlink"])
        else: path.write_text(item["text"],encoding="utf-8")
    store=service=jobs=None; worker=None; legacy=None; worker_streams=[]; observations=[]
    try:
        for raw_action in payload["actions"]:
            action=raw_action; op=action["op"]
            try:
                if op == "open":
                    if store is not None: store.close()
                    store=Store(db); service=Service(store,root,backup_dir=action.get("backup_dir",base/"backups")); jobs=JobManager(store); result={"opened":True}
                elif op == "call": result=invoke(action)
                elif op == "cli": result=cli(action)
                elif op == "snapshot": result=snapshot(action["exclude_controls"])
                elif op == "rows": result=read_rows(action["query"],action.get("args",[]))
                elif op == "file_digest": result=hashlib.sha256(Path(action.get("path",db)).read_bytes()).hexdigest()
                elif op == "files":
                    result={}
                    for name in ("backups","next"):
                        folder=base/name
                        if folder.exists():
                            for path in sorted(folder.rglob("*")):
                                if path.is_symlink(): result[str(path.relative_to(base))]={"symlink":str(path.readlink())}
                                elif path.is_file(): result[str(path.relative_to(base))]=hashlib.sha256(path.read_bytes()).hexdigest()
                    for item in payload["files"]:
                        path=Path(item["path"])
                        if path.parent == base/"library.sqlite.maintenance" and path.exists():
                            result[str(path.relative_to(base))]=hashlib.sha256(path.read_bytes()).hexdigest()
                elif op == "read_backup":
                    result=strict_json(Path(action["path"]).read_bytes())
                    for key in action.get("path_keys",[]): result=result[key]
                elif op == "legacy_open":
                    legacy_code = """import json,sys
sys.path.insert(0,sys.argv[1])
from library.catalog.store import Store
from library.query.service import Service
from library.common import LibraryError
store=Store(sys.argv[2]); service=Service(store,sys.argv[3],backup_dir=sys.argv[4])
print(json.dumps({"opened":True}),flush=True)
try:
    for raw in sys.stdin:
        action=json.loads(raw)
        try: value=getattr(service,action["method"])(*action["args"],**action["kwargs"])
        except LibraryError as error: value={"error":error.code}
        print(json.dumps(value,ensure_ascii=True,separators=(",",":")),flush=True)
finally: store.close()
"""
                    legacy_error=tempfile.TemporaryFile(); worker_streams.append(legacy_error)
                    legacy=subprocess.Popen([sys.executable,"-I","-B","-u","-c",legacy_code,LEGACY,str(db),str(root),str(base/"backups")],
                                            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=legacy_error)
                    if not select.select([legacy.stdout],[],[],8)[0]: raise TimeoutError("legacy open deadline")
                    result=strict_json(legacy.stdout.readline(262145))
                elif op == "legacy_call":
                    if legacy is None or legacy.poll() is not None: raise RuntimeError("missing live legacy process")
                    legacy.stdin.write(encode(action)+b"\n"); legacy.stdin.flush()
                    if not select.select([legacy.stdout],[],[],8)[0]: raise TimeoutError("legacy call deadline")
                    result=strict_json(legacy.stdout.readline(262145))
                elif op == "worker_start":
                    out=tempfile.TemporaryFile(); err=tempfile.TemporaryFile(); worker_streams += [out,err]
                    worker=subprocess.Popen(command(["worker"],str(base/"backups")),stdin=subprocess.DEVNULL,stdout=out,stderr=err)
                    deadline=time.monotonic()+5
                    while time.monotonic()<deadline:
                        if worker.poll() is not None: raise RuntimeError("worker exited before live observation")
                        state=service.diagnostics()["worker_state"]
                        if state != "stopped": break
                        time.sleep(.02)
                    else: raise TimeoutError("worker-owner observation deadline")
                    result={"worker_state":state}
                elif op == "worker_kill":
                    if worker is None or worker.poll() is not None: raise RuntimeError("missing live worker")
                    worker.kill(); worker.wait(timeout=5); result={"signal":-worker.returncode}; worker=None
                else: raise ValueError("unknown observation action")
            except LibraryError as error: result={"error":error.code}
            if "select" in action: result={k:result[k] for k in action["select"]}
            if action.get("observe",True): observations.append(normalized(result))
    finally:
        if legacy is not None:
            if legacy.poll() is None: legacy.kill(); legacy.wait(timeout=5)
            legacy.stdin.close(); legacy.stdout.close()
        if worker is not None and worker.poll() is None: worker.kill(); worker.wait(timeout=5)
        for stream in worker_streams: stream.close()
        if store is not None: store.close()
    result=encode({"observations":observations})
    if len(result)>262144: raise ValueError("observation bound")
    sys.stdout.buffer.write(result+b"\n")
'''


def _rows(query: str, *args: Any) -> dict[str, Any]:
    return {"op": "rows", "query": query, "args": list(args)}


def _diag(*keys: str) -> dict[str, Any]:
    return _call("diagnostics", select=list(keys))


def _migrations() -> list[dict[str, Any]]:
    cases = []
    fence, diagnostic = AMENDMENT_IDS[:2]
    missing_doc = _document("removed.txt", "")["document_id"]
    pending = ["owned-stage", "backup_stage", "backup", ".owned.partial", "historical-owner", 2, "staging"]
    for operation in ("migrate", "backup"):
        error = {"operation": operation, "code": "io_error"}
        fixture = _fixture(schema=3, generation=9, records=[_record(version=7)],
                           control={"last_error": _bytes(error).decode(), "backup_root": '"$BASE/backups"',
                                    "cleanup_cursor": '"owned-stage"'},
                           job_high={"gone-job": 18}, document_high={missing_doc: 21}, artifacts=[pending])
        q = "SELECT key,value FROM control WHERE key NOT IN ('incarnation','last_error') ORDER BY key"
        actions = [_rows(q), {"op": "files"}, {"op": "open"},
                   _rows("SELECT value FROM control WHERE key='incarnation'"), _rows(q),
                   _rows("SELECT * FROM job_control ORDER BY job_id"),
                   _rows("SELECT * FROM document_control ORDER BY document_id"),
                   _rows("SELECT * FROM maintenance_artifacts ORDER BY artifact_id"),
                   _diag("schema", "generation", "last_error", "recovery_action"),
                   {"op": "files"}, _snap(), {"op": "open"}, _snap(), _call("migrate", target="store"), _snap()]
        controls = [["backup_root", '"$BASE/backups"'],["cleanup_cursor",'"owned-stage"'],["worker_generation","3"]]
        dh = sorted([[missing_doc,21],[_document("a.txt","alpha")["document_id"],7]])
        expected = [controls, {"backups/.owned.partial": hashlib.sha256(b"owned evidence").hexdigest()},
                    {"opened": True}, [[{"$nonempty_not": "portable-v2-initial"}]], controls,
                    [["gone-job",18,0]], dh, [pending],
                    {"schema":4,"generation":9,"last_error":None if operation=="migrate" else error,
                     "recovery_action":"none" if operation=="migrate" else "choose_new_backup"},
                    _ref(1), _digest(), {"opened":True}, _ref(10),
                    {"from_schema":4,"to_schema":4,"migrated":False,"documents":1,"jobs":0}, _ref(10)]
        cases.append(_case("migration-preserves-controls-"+operation,fixture,actions,expected,[fence,diagnostic],
                           files=[{"path":"$BASE/backups/.owned.partial","text":"owned evidence"}]))
    for operation in ("migrate", "restore"):
        error = {"operation":operation,"code":"io_error"}
        fixture = _fixture(control={"last_error":_bytes(error).decode()})
        actions = [_snap(), {"op":"open"}, _snap(), _diag("last_error","recovery_action"),
                   _call("migrate",target="store"), _diag("last_error","recovery_action"),
                   _rows("SELECT value FROM control WHERE key='incarnation'")]
        expected = [_digest(),{"opened":True},_ref(0),
                    {"last_error":error,"recovery_action":"run_migration" if operation=="migrate" else "validate_backup"},
                    {"from_schema":4,"to_schema":4,"migrated":False,"documents":0,"jobs":0},
                    {"last_error":None if operation=="migrate" else error,
                     "recovery_action":"none" if operation=="migrate" else "validate_backup"},[["portable-v2-initial"]]]
        cases.append(_case("explicit-noop-diagnostic-"+operation,fixture,actions,expected,[fence,diagnostic]))
    fixture=_fixture(schema=3,generation=COUNTER_MAX,control={"worker_generation":str(COUNTER_MAX)})
    cases.append(_case("migration-at-maximum",fixture,[{"op":"open"},_diag("generation","worker_generation"),
        _rows("SELECT value FROM control WHERE key='incarnation'"),_call("migrate",target="store")],
        [{"opened":True},{"generation":COUNTER_MAX,"worker_generation":COUNTER_MAX},
         [[{"$nonempty_not":"portable-v2-initial"}]],
         {"from_schema":4,"to_schema":4,"migrated":False,"documents":0,"jobs":0}], [fence,AMENDMENT_IDS[2]]))
    record=_record(version=7); docid=record["document"]["document_id"]
    legacy_action: dict[str, Any]={"op":"legacy_call","method":"replace_annotations","args":[docid,7,"late",[],[]],"kwargs":{}}
    bad_action=deepcopy(legacy_action);bad_action["args"][1]=0
    cases.append(_case("supported-schema3-handle-fenced-before-removed-tables",_fixture(schema=3,generation=9,records=[record]),
        [{"op":"legacy_open"},_cli("migrate"),legacy_action,bad_action,{"op":"open"},_call("lifecycle_show",docid)],
        [{"opened":True},_cli_result({"from_schema":3,"to_schema":4,"migrated":True,"documents":1,"jobs":0}),
         _error("stale_instance"),_error("invalid_request"),{"opened":True},record],[fence]))
    return cases


def _counters() -> list[dict[str, Any]]:
    amendment=AMENDMENT_IDS[2]; record=_record(version=COUNTER_MAX); docid=record["document"]["document_id"]
    fixture=_fixture(generation=COUNTER_MAX,records=[record],collections=["kept"],
                     control={"last_error":'{"code":"io_error","operation":"backup"}'})
    actions=[{"op":"open"},_snap(),_call("replace_annotations",docid,COUNTER_MAX,"",[],[]),
             _call("refresh_document",docid,COUNTER_MAX,text="alpha"),_call("restore_document",docid,COUNTER_MAX),
             _call("create_collection","kept",COUNTER_MAX),_call("replace_annotations",docid,COUNTER_MAX,"changed",[],[]),
             _call("delete_document",docid,COUNTER_MAX),_call("create_collection","new",COUNTER_MAX),
             _call("replace_annotations",docid,COUNTER_MAX-1,"changed",[],[]),
             _call("create_collection","new",COUNTER_MAX-1),_snap()]
    unchanged={"status":"unchanged","record":record}
    expected=[{"opened":True},_digest(),unchanged,unchanged,unchanged,
              {"status":"unchanged","name":"kept","generation":COUNTER_MAX},
              _error("counter_exhausted"),_error("counter_exhausted"),_error("counter_exhausted"),
              _error("stale_version"),_error("stale_generation"),_ref(1)]
    cases=[_case("maximum-noops-atomic-exhaustion-and-stale-precedence",fixture,actions,expected,[amendment])]
    below=_record(version=COUNTER_MAX-1)
    changed=deepcopy(below);changed.update(edit_version=COUNTER_MAX,notes="one final edit")
    cases.append(_case("one-below-maximum",_fixture(generation=COUNTER_MAX-1,records=[below]),
        [{"op":"open"},_call("replace_annotations",docid,COUNTER_MAX-1,"one final edit",[],[]),
         _diag("generation"),_snap(),_call("replace_annotations",docid,COUNTER_MAX,"overflow",[],[]),_snap()],
        [{"opened":True},{"status":"updated","record":changed},{"generation":COUNTER_MAX},
         _digest(),_error("counter_exhausted"),_ref(3)],[amendment]))
    bad_tokens=[0,-1,COUNTER_MAX+1,True,1.0,"1",None]
    actions=[{"op":"open"},_snap()]; expected=[{"opened":True},_digest()]
    for value in bad_tokens:
        actions.append(_call("replace_annotations",docid,value,"",[],[]));expected.append(_error("invalid_request"))
    for value in [-1,COUNTER_MAX+1,True,1.0,"0",None]:
        actions.append(_call("create_collection","new",value));expected.append(_error("invalid_request"))
    for value in [0,-1,COUNTER_MAX+1,True,1.0,"1",None]:
        actions.append(_call("commit",{"job_id":"absent","epoch":value},target="jobs"));expected.append(_error("invalid_request"))
    actions.extend([_cli("annotate",docid,"--expected-version",str(COUNTER_MAX+1),"--notes",""),_snap()])
    expected.extend([_cli_result(_error("invalid_request"),2),_ref(1)])
    cases.append(_case("request-counter-domain-before-lookup",_fixture(records=[_record()]),actions,expected,[amendment]))
    for label, control, generation in (("generation-overflow",{},COUNTER_MAX+1),
                                       ("worker-negative",{"worker_generation":"-1"},0),
                                       ("worker-fraction",{"worker_generation":"1.5"},0)):
        cases.append(_case("refused-persisted-"+label,_fixture(schema=3,generation=generation,control=control),
                           [{"op":"file_digest"},{"op":"open"},{"op":"file_digest"}],
                           [_digest(),_error("invalid_database"),_ref(0)],[amendment,AMENDMENT_IDS[1]]))
    completed=_job("completed",[],epoch=COUNTER_MAX,state="completed")
    running=_job("running",[],epoch=COUNTER_MAX,state="running")
    cancelled=_job("cancelled",[],epoch=COUNTER_MAX,state="cancelled")
    fixture=_fixture(jobs=[completed,running,cancelled])
    cases.append(_case("maximum-job-replay-and-effective-fences",fixture,
        [{"op":"open"},_snap(),_call("prepare","running",target="jobs"),
         _call("commit",{"job_id":"completed","epoch":COUNTER_MAX},target="jobs"),
         _call("cancel","cancelled",target="jobs"),_call("cancel","running",target="jobs"),
         _call("retry","cancelled",target="jobs"),_snap()],
        [{"opened":True},_digest(),{"job_id":"running","epoch":COUNTER_MAX},json.loads(completed["receipt_json"]),
         cancelled["job"],_error("counter_exhausted"),_error("counter_exhausted"),_ref(1)],[amendment]))
    root={"backup_root":'"$BASE/backups"',"last_error":'{"code":"io_error","operation":"backup"}'}
    for label, incoming_records, incoming_jobs, dh, jh in (
        ("document-late-overflow",[_record("a.txt","alpha",2),_record("z.txt","zeta",COUNTER_MAX)],[],{},{}),
        ("job-late-overflow",[_record("a.txt","alpha",2)],[_job("z-job",[],epoch=COUNTER_MAX)],{},{}),
        ("retained-document-highwater",[_record("a.txt","alpha",1)],[],{docid:COUNTER_MAX},{}),
        ("retained-job-highwater",[],[_job("z-job",[])],{},{"z-job":COUNTER_MAX})):
        backup=_backup(incoming_records,generation=1,jobs=incoming_jobs)
        fixture=_fixture(generation=3,control=root,document_high=dh,job_high=jh)
        actions=[{"op":"open"},_call("restore_backup","incoming.json",2),_snap(),{"op":"files"},
                 _call("restore_backup","incoming.json",3),_snap(),{"op":"files"}]
        cases.append(_case("restore-atomic-"+label,fixture,actions,
            [{"opened":True},_error("stale_generation"),_digest(),{"backups/incoming.json":hashlib.sha256(_bytes(backup,ascii=False)).hexdigest()},
             _error("counter_exhausted"),_ref(2),_ref(3)],[amendment],
            files=[{"path":"$BASE/backups/incoming.json","text":_bytes(backup,ascii=False).decode()}]))
    backup=_backup([],generation=0,jobs=[completed]); fixture=_fixture(generation=3,control=root,job_high={"completed":COUNTER_MAX})
    cases.append(_case("restore-completed-maximum-no-increment",fixture,
        [{"op":"open"},_call("restore_backup","incoming.json",3),{"op":"open"},
         _call("commit",{"job_id":"completed","epoch":COUNTER_MAX},target="jobs"),
         _rows("SELECT epoch_high_water FROM job_control WHERE job_id='completed'"),
         _rows("SELECT receipt FROM jobs WHERE job_id='completed'")],
        [{"opened":True},{"restored":True,"generation":4,"documents":0,"jobs":1},{"opened":True},
         json.loads(completed["receipt_json"]),[[COUNTER_MAX]],[[completed["receipt_json"]]]],[amendment],
        files=[{"path":"$BASE/backups/incoming.json","text":_bytes(backup,ascii=False).decode()}]))
    return cases


def _hashes() -> list[dict[str, Any]]:
    amendment=AMENDMENT_IDS[3]; empty_digest=hashlib.sha256(b"").hexdigest()
    rows=[_job("empty",[],hashes=" [ ] "),_job("empty-text",[{"source":"empty.txt","text":""}],
           hashes=' [ "\\u0065'+empty_digest[1:]+'" ] '),
          _job("unicode",[{"source":"unicode.txt","text":"é\n"}]),
          _job("surrogate",[{"source":"bad.txt","text":"\ud800"}],hashes=" [ null ] "),
          _job("suffix",[{"source":"bad.pdf","text":"encoded"}])]
    completed=_job("completed-empty",[],state="completed",hashes=" [ ] ")
    completed["manifest_json"]=" [ ] "
    completed["receipt_json"]=json.dumps(json.loads(completed["receipt_json"]),indent=1,ensure_ascii=True)
    rows.append(completed)
    fixture=_fixture(schema=3,jobs=rows,control={"backup_root":'"$BASE/backups"'})
    serialized=[[r["job"]["job_id"],r["manifest_json"],r["content_hashes_json"],r["receipt_json"]] for r in sorted(rows,key=lambda r:r["job"]["job_id"])]
    query="SELECT job_id,manifest,content_hashes,receipt FROM jobs ORDER BY job_id"
    cases=[_case("portable-hash-spelling-preserved-migration-backup",fixture,
        [_rows(query),{"op":"open"},_rows(query),_call("backup","hashes.json",select=["name","generation"]),
         {"op":"read_backup","path":"$BASE/backups/hashes.json","path_keys":["payload","jobs"]},
         _call("restore_backup","hashes.json",0),{"op":"open"},_rows(query),
         _call("prepare","surrogate",target="jobs"),_call("prepare","suffix",target="jobs")],
        [serialized,{"opened":True},serialized,{"name":"hashes.json","generation":0},
         sorted(rows,key=lambda r:r["job"]["job_id"]),{"restored":True,"generation":1,"documents":0,"jobs":6},
         {"opened":True},serialized,_error("invalid_utf8"),_error("unsupported_type")],[amendment])]
    for label, hashes in (("null-for-utf8","[null]"),("prefixed",'["blob-'+empty_digest+'"]'),
                          ("uppercase",'["'+empty_digest.upper()+'"]'),("extra",'["'+empty_digest+'",null]'),
                          ("not-array","{}"),("nonfinite","[NaN]"),("trailing",'["'+empty_digest+'"] false')):
        row=_job("invalid",[{"source":"a.txt","text":""}],hashes=hashes)
        cases.append(_case("invalid-hash-"+label,_fixture(schema=3,jobs=[row]),
            [{"op":"file_digest"},{"op":"open"},{"op":"file_digest"}],
            [_digest(),_error("invalid_database"),_ref(0)],[amendment]))
    row=_job("invalid",[{"source":"a.txt","text":"\ud800"}],hashes='["'+empty_digest+'"]')
    cases.append(_case("digest-for-unencodable-text",_fixture(schema=3,jobs=[row]),
        [{"op":"file_digest"},{"op":"open"},{"op":"file_digest"}],
        [_digest(),_error("invalid_database"),_ref(0)],[amendment]))
    entries=[{"source":"bad.pdf","text":"é\n"},{"source":"surrogate.txt","text":"\ud800"}]
    row=_job("admitted",entries)
    cases.append(_case("new-admission-canonical-hash-format",_fixture(),
        [{"op":"open"},_call("submit","admitted",entries,target="jobs"),
         _rows("SELECT content_hashes,receipt FROM jobs WHERE job_id='admitted'")],
        [{"opened":True},row["job"],[[row["content_hashes_json"],None]]],[amendment]))
    return cases


def _liveness() -> list[dict[str, Any]]:
    amendment=AMENDMENT_IDS[4]
    running=_job("manual-running",[],state="running")
    return [_case("worker-owner-idle-abrupt-death",_fixture(jobs=[running]),
        [{"op":"open"},_diag("worker_state","worker_generation"),_snap(),
         _diag("worker_state"),_snap(),{"op":"worker_start"},_diag("worker_state"),
         {"op":"worker_kill"},_diag("worker_state"),_call("get","manual-running",target="jobs"),
         _cli("worker","--once"),_diag("worker_state")],
        [{"opened":True},{"worker_state":"stopped","worker_generation":3},_digest(),
         {"worker_state":"stopped"},_ref(2),{"worker_state":"idle"},{"worker_state":"idle"},
         {"signal":9},{"worker_state":"stopped"},running["job"],
         _cli_result({"processed":None,"job":None}),{"worker_state":"stopped"}],[amendment])]


def _adoptions() -> list[dict[str, Any]]:
    amendment=AMENDMENT_IDS[5]; bound={"backup_root":'"$BASE/backups"'}
    cases=[]
    actions=[{"op":"open"},_snap(exclude=["last_error"]),_call("list_backups"),_call("backup","new.json"),
             _call("restore_backup","missing.json",0),_snap(exclude=["last_error"]),_cli("backup-root-adopt","--expect-unbound"),
             _rows("SELECT value FROM control WHERE key='backup_root'"),
             _cli("backup-root-adopt","--expect-root","$BASE/backups"),
             _call("list_backups"),_cli("backup-root-adopt","--expect-unbound",backup_dir="$BASE/next")]
    cases.append(_case("explicit-initial-adoption-and-cas",_fixture(),actions,
        [{"opened":True},_digest(),_error("backup_root_unbound"),_error("backup_root_unbound"),
         _error("backup_root_unbound"),_ref(1),_cli_result({"adopted":True,"registered":0}),
         [['"$BASE/backups"']],_cli_result({"adopted":False,"registered":0}),
         {"backups":[],"total":0},_cli_result(_error("backup_root_mismatch"),2)],[amendment]))
    cases.append(_case("rebind-old-service-and-unowned-files",_fixture(control=bound),
        [{"op":"open"},_snap(exclude=["backup_root","last_error"]),{"op":"files"},
         _cli("backup-root-adopt","--expect-root","$BASE/backups",backup_dir="$BASE/next"),
         _call("list_backups"),_call("backup","forbidden.json"),_call("restore_backup","missing.json",0),
         _diag("generation"),_snap(exclude=["backup_root","last_error"]),{"op":"files"},
         {"op":"open","backup_dir":"$BASE/next"},_call("list_backups")],
        [{"opened":True},_digest(),{"next/unowned.json":hashlib.sha256(b"user evidence").hexdigest()},
         _cli_result({"adopted":True,"registered":0}),_error("backup_root_mismatch"),
         _error("backup_root_mismatch"),_error("backup_root_mismatch"),{"generation":0},_ref(1),_ref(2),
         {"opened":True},{"backups":[],"total":0}],[amendment],
        files=[{"path":"$BASE/next/unowned.json","text":"user evidence"}]))
    for root_kind in ("backup","maintenance"):
        artifact=["stranded","backup_stage" if root_kind=="backup" else "restore_stage",root_kind,
                  ".stranded.partial","old-owner",2,"staging"]
        location="backups" if root_kind=="backup" else "library.sqlite.maintenance"
        fixture=_fixture(schema=3,artifacts=[artifact])
        cases.append(_case("unbound-pending-"+root_kind,fixture,
            [{"op":"open"},_call("list_backups"),_snap(),{"op":"files"},
             _cli("backup-root-adopt","--expect-unbound"),_snap(),{"op":"files"},_diag("schema","generation")],
            [{"opened":True},_error("backup_root_unbound"),_digest(),{location+"/.stranded.partial":hashlib.sha256(b"stranded").hexdigest()},
             _cli_result(_error("maintenance_busy"),2),_ref(2),_ref(3),
             {"schema":4,"generation":0}],[amendment,AMENDMENT_IDS[0]],
            files=[{"path":"$BASE/"+location+"/.stranded.partial","text":"stranded"}]))
    backup=_backup([]); raw=_bytes(backup,ascii=False).decode(); registry=["registered.json",len(raw.encode()),backup["payload_sha256"],0]
    for label,text,code in (("corrupt","{}","invalid_backup"),("missing",None,"io_error"),("valid",raw,None)):
        files=[] if text is None else [{"path":"$BASE/next/registered.json","text":text}]
        fixture=_fixture(control=bound,backups=[registry])
        actions=[{"op":"open"},_snap(exclude=["backup_root"]),{"op":"files"},
                 _cli("backup-root-adopt","--expect-root","$BASE/backups",backup_dir="$BASE/next"),
                 _snap(exclude=["backup_root"]),{"op":"files"},_rows("SELECT value FROM control WHERE key='backup_root'")]
        expected=[{"opened":True},_digest(),{} if text is None else {"next/registered.json":hashlib.sha256(text.encode()).hexdigest()},
                  _cli_result(_error(code),2) if code else _cli_result({"adopted":True,"registered":1}),
                  _ref(1),_ref(2),[['"$BASE/backups"' if code else '"$BASE/next"']]]
        cases.append(_case("adoption-registered-"+label,fixture,actions,expected,[amendment],files=files))
    files=[{"path":"$BASE/backups/registered.json","text":"{}"}]
    cases.append(_case("same-root-validates-registry",_fixture(control=bound,backups=[registry]),
        [{"op":"open"},_snap(),_cli("backup-root-adopt","--expect-root","$BASE/backups"),_snap()],
        [{"opened":True},_digest(),_cli_result(_error("invalid_backup"),2),_ref(1)],[amendment],files=files))
    rows=[];files=[]
    for number in range(102):
        name=f"a{number:03}.json";rows.append([name,len(raw.encode()),backup["payload_sha256"],0])
        files.append({"path":"$BASE/next/"+name,"text":"{}" if number==101 else raw})
    cases.append(_case("adoption-validates-beyond-first-page",_fixture(control=bound,backups=rows),
        [{"op":"open"},_snap(),_cli("backup-root-adopt","--expect-root","$BASE/backups",backup_dir="$BASE/next"),_snap()],
        [{"opened":True},_digest(),_cli_result(_error("invalid_backup"),2),_ref(1)],[amendment],files=files))
    cases.append(_case("adoption-lexical-symlink-before-dotdot",_fixture(control=bound),
        [{"op":"open"},_snap(),_cli("backup-root-adopt","--expect-root","$BASE/backups",backup_dir="$BASE/link/../next"),_snap()],
        [{"opened":True},_digest(),_cli_result(_error("invalid_source"),2),_ref(1)],[amendment],
        files=[{"path":"$BASE/link","symlink":"$BASE/backups"}]))
    return cases


def acceptance_cases() -> list[dict[str, Any]]:
    """Return fresh input/host-expectation records; no product execution occurs."""
    return [*_migrations(), *_counters(), *_hashes(), *_liveness(), *_adoptions()]


def _equal(expected: Any, actual: Any, observations: list[Any]) -> bool:
    if type(expected) is dict and set(expected)=={"$ref"}:
        value: Any=observations
        for part in expected["$ref"]:
            try: value=value[part]
            except (IndexError,KeyError,TypeError): return False
        return _exact(value,actual)
    if type(expected) is dict and set(expected)=={"$kind"}:
        return expected["$kind"]=="sha256" and type(actual) is str and len(actual)==64 and all(x in "0123456789abcdef" for x in actual)
    if type(expected) is dict and set(expected)=={"$nonempty_not"}:
        return type(actual) is str and bool(actual) and actual!=expected["$nonempty_not"]
    if type(expected) is not type(actual): return False
    if type(expected) is dict:
        return expected.keys()==actual.keys() and all(_equal(v,actual[k],observations) for k,v in expected.items())
    if type(expected) is list:
        return len(expected)==len(actual) and all(_equal(a,b,observations) for a,b in zip(expected,actual))
    return bool(expected==actual)


def _exact(left: Any, right: Any) -> bool:
    if type(left) is not type(right): return False
    if type(left) is dict: return left.keys()==right.keys() and all(_exact(v,right[k]) for k,v in left.items())
    if type(left) is list: return len(left)==len(right) and all(_exact(a,b) for a,b in zip(left,right))
    return bool(left==right)


def score_case(case: dict[str, Any], actual: Any) -> bool:
    """Compare host observations after caller authenticates successful execution."""
    if type(actual) is not dict or set(actual)!={"observations"} or type(actual["observations"]) is not list:
        return False
    return _equal(case["expected"],actual,actual["observations"])


def registry_manifest() -> dict[str, Any]:
    cases=acceptance_cases()
    scorer=inspect.getsource(_equal)+inspect.getsource(_exact)+inspect.getsource(score_case)
    return {"protocol":PROTOCOL,"purpose":PURPOSE,"status":"unexecuted_public_development_definition",
            "contract_sha256":CONTRACT_SHA256,"case_ids":[c["id"] for c in cases],
            "ordered_inputs_sha256":_sha([{"id":c["id"],"input":c["input"]} for c in cases]),
            "ordered_expected_sha256":_sha([{"id":c["id"],"expected":c["expected"]} for c in cases]),
            "ordered_cases_sha256":_sha(cases),"adapter_sha256":hashlib.sha256(CHILD_ADAPTER.encode()).hexdigest(),
            "scorer_sha256":hashlib.sha256(scorer.encode()).hexdigest(),
            "amendment_cases":{a:[c["id"] for c in cases if a in c["amendment_ids"]] for a in AMENDMENT_IDS},
            "limitations":list(LIMITATIONS)}


CASE_IDS=tuple(c["id"] for c in acceptance_cases())
