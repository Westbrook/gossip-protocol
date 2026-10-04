"""Evaluator-only worker rendezvous and independently executed data readers.

This module does not import or run a candidate. The literal launcher/hook is a
prospectively frozen instrumentation interface under M3-INTERFACES clause 2.
Candidate-authored events are coordinates, never proof of a database boundary.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

PROTOCOL = "candidate-product-worker-hooks-v1"
ROOT = "/tmp/.gossip-worker-hooks-v1"
PHASES = ("owner_acquired", "after_claim", "before_commit", "after_commit_before_output")
MAX_EVENT_BYTES = 16384
MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024
MAX_HOLD_SECONDS = 90
RELEASE_ACK_SECONDS = 10

# Both publishers use the same complete-file, exclusive publication primitive.
# A reader of the final name can never observe the private file mid-write.
ATOMIC_PUBLICATION_SOURCE = r'''import os

def _publish_complete(directory, name, raw):
    temporary = "." + name + ".pending-" + os.urandom(16).hex()
    parent = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    descriptor = None
    owned = False
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=parent)
        owned = True
        offset = 0
        while offset < len(raw):
            written = os.write(descriptor, raw[offset:])
            if written <= 0:
                raise OSError("incomplete private publication write")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        # link() fails if the final name already exists, including a symlink.
        # Both names are relative to the same retained no-follow directory FD.
        os.link(temporary, name, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
        os.fsync(parent)
    finally:
        try:
            if descriptor is not None:
                os.close(descriptor)
        finally:
            try:
                if owned:
                    os.unlink(temporary, dir_fd=parent)
            finally:
                os.close(parent)
'''

# This helper is imported by an instrumented candidate, not injected into SQLite.
# No wrapper silently translates exception rollback into an abrupt process death.
HOOK_SOURCE = r'''"""Frozen evaluator hook. Disabled unless configured by the trusted launcher."""
import json
import time
''' + ATOMIC_PUBLICATION_SOURCE + r'''

_CONFIG = None
_PHASES = ("owner_acquired", "after_claim", "before_commit", "after_commit_before_output")

def configure(config):
    global _CONFIG
    if _CONFIG is not None:
        raise RuntimeError("hook already configured")
    _CONFIG = dict(config)

def boundary(phase, *, lock_fd, incarnation, worker_generation, job_id=None, epoch=None):
    if _CONFIG is None or phase != _CONFIG["phase"]:
        return
    if phase not in _PHASES or type(lock_fd) is not int or not 0 <= lock_fd <= 65535:
        raise ValueError("hook shape")
    if type(incarnation) is not str or not incarnation or len(incarnation) > 1024:
        raise ValueError("incarnation shape")
    if type(worker_generation) is not int or not 0 <= worker_generation < 2**63:
        raise ValueError("generation shape")
    if (job_id is None) != (epoch is None):
        raise ValueError("claim shape")
    if job_id is not None and (type(job_id) is not str or len(job_id) > 128 or
                               type(epoch) is not int or not 1 <= epoch < 2**63):
        raise ValueError("claim shape")
    event = {"protocol":"candidate-product-worker-hooks-v1", "phase":phase,
             "pid":os.getpid(), "lock_fd":lock_fd, "incarnation":incarnation,
             "worker_generation":worker_generation, "job_id":job_id, "epoch":epoch}
    raw = json.dumps(event,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()
    _publish_complete(_CONFIG["directory"], "event.json", raw)
    if _CONFIG["mode"] == "observe":
        return
    deadline = time.monotonic() + _CONFIG["maximum_hold_seconds"]
    release = _CONFIG["directory"] + "/release"
    while True:
        try:
            fd = os.open(release, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            if time.monotonic() >= deadline:
                _publish_complete(_CONFIG["directory"], "decision.json", b'{"decision":"expired"}')
                raise RuntimeError("evaluator hook hold expired")
            time.sleep(0.01)
            continue
        try:
            if os.read(fd, 2) != b"1":
                raise RuntimeError("invalid hook release")
        finally:
            os.close(fd)
        _publish_complete(_CONFIG["directory"], "decision.json", b'{"decision":"released"}')
        return
'''

LAUNCHER_SOURCE = r'''import json
import runpy
import sys
from pathlib import Path
import gossip_worker_hook_v1 as hook

config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
hook.configure(config)
# This restores normal `python -m library` module resolution from the source
# mount. It imports no candidate until this source-bound worker is started.
sys.path.insert(0, "/workspace")
sys.argv = ["library", *sys.argv[2:]]
runpy.run_module("library", run_name="__main__")
'''

# Run only as python -I -c in the source-free keeper. Snapshot uses a single
# read transaction and fixed public portable table/column names, not SQL from
# events or candidate output. Busy/hot-journal/corrupt input is reported unknown.
SNAPSHOT_SOURCE = r'''import base64
import hashlib
import json
import os
import sqlite3
import stat
import time

MAX = 4194304
TABLES = {
 "metadata":("key","value"), "control":("key","value"),
 "jobs":("job_id","epoch","state","total","completed","error","manifest","content_hashes","receipt"),
 "job_control":("job_id","epoch_high_water","enrolled"),
 "documents":("document_id","source_id","source","blob_id","title"),
 "blobs":("blob_id","content"),
 "document_revisions":("revision_id","document_id","revision","blob_id"),
 "document_state":("document_id","head_revision_id","edit_version","deleted","notes","tags","collections"),
 "document_control":("document_id","edit_high_water"),
 "maintenance_artifacts":("artifact_id","kind","root_kind","relative_path","owner_incarnation","owner_generation","state")}
def encode(value):
 return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False).encode()
def cell(value):
 if isinstance(value, bytes):
  return {"blob_bytes":len(value),"blob_sha256":hashlib.sha256(value).hexdigest()}
 if value is None or type(value) in (str,int,float):
  return value
 raise ValueError("unsupported SQLite cell")
result = {"protocol":"candidate-product-worker-sqlite-snapshot-v1", "state":"unavailable", "tables":{}}
connection = None
try:
 path = "/tmp/library.sqlite"
 info = os.lstat(path)
 if not stat.S_ISREG(info.st_mode):
  raise ValueError("database is not a regular non-symlink file")
 result["identity"] = {"device":info.st_dev,"inode":info.st_ino}
 connection = sqlite3.connect("file:/tmp/library.sqlite?mode=ro",uri=True,timeout=0.1)
 # Corroborate the actual newly opened database handle, not just a pathname
 # sampled before a candidate-controlled rename. This helper has no other DB.
 database_fds = []
 for name in os.listdir("/proc/self/fd"):
  try:
   opened = os.fstat(int(name))
  except (OSError,ValueError):
   continue
  if stat.S_ISREG(opened.st_mode) and (opened.st_dev,opened.st_ino)==(info.st_dev,info.st_ino):
   database_fds.append(int(name))
 if not database_fds:
  raise ValueError("opened database identity differs from pathname")
 connection.execute("PRAGMA query_only=ON")
 deadline = time.monotonic() + 2
 connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
 connection.execute("BEGIN")
 result["journal_mode"] = connection.execute("PRAGMA journal_mode").fetchone()[0]
 for name, columns in TABLES.items():
  rows = []
  cursor = connection.execute("SELECT " + ",".join(columns) + " FROM " + name + " ORDER BY 1")
  for row in cursor:
   rows.append([cell(value) for value in row])
   if len(rows) > 1024:
    raise ValueError("snapshot row bound")
  result["tables"][name] = {"columns":columns,"rows":rows}
  if len(encode(result)) > MAX - 8192:
   raise ValueError("snapshot byte bound")
 connection.rollback()
 after = os.lstat(path)
 if not stat.S_ISREG(after.st_mode) or (after.st_dev,after.st_ino)!=(info.st_dev,info.st_ino):
  raise ValueError("database path identity changed during read")
 for descriptor in database_fds:
  opened = os.fstat(descriptor)
  if (opened.st_dev,opened.st_ino)!=(info.st_dev,info.st_ino):
   raise ValueError("opened database identity changed during read")
 result["opened_identity_verified"] = True
 result["state"] = "complete"
except (OSError,sqlite3.Error,ValueError,TypeError,UnicodeError) as error:
 result["limitation"] = type(error).__name__ + ":" + str(error)[:512]
finally:
 if connection is not None:
  connection.close()
raw = encode(result)
if len(raw) > MAX:
 raw = encode({"protocol":result["protocol"],"state":"unavailable","limitation":"snapshot byte bound","tables":{}})
print(raw.decode())
'''

# Runs via fixed `docker exec <owned worker> python -I -c ...`, in its PID
# namespace. It never imports library or reads source/input modules. The event
# PID/FD is an untrusted selection, corroborated against kernel fdinfo/locks.
KERNEL_SOURCE = r'''import base64
import json
import os
import stat
import sys

root = sys.argv[1]
if not root.startswith("/tmp/.gossip-worker-hooks-v1/w") or "/" in root.removeprefix("/tmp/.gossip-worker-hooks-v1/"):
 raise ValueError("closed hook directory")
def read(path, limit):
 fd = os.open(path,os.O_RDONLY | os.O_NOFOLLOW)
 try:
  raw = os.read(fd,limit+1)
  if len(raw)>limit:
   raise ValueError("read bound")
  return raw
 finally:
  os.close(fd)
result = {"protocol":"candidate-product-worker-kernel-v1","state":"unavailable"}
try:
 raw = read(root + "/event.json",16384)
 result["event_b64"] = base64.b64encode(raw).decode()
 event = json.loads(raw)
 pid,fd = event["pid"],event["lock_fd"]
 if type(pid) is not int or not 1 <= pid <= 4194304 or type(fd) is not int or not 0 <= fd <= 65535:
  raise ValueError("PID/FD shape")
 proc = "/proc/"+str(pid)
 target = proc+"/fd/"+str(fd)
 identity = os.stat(target)
 result.update(pid=pid, fd=fd, fd_target=os.readlink(target),
               fd_identity={"device":identity.st_dev,"inode":identity.st_ino,"major":os.major(identity.st_dev),"minor":os.minor(identity.st_dev)},
               fdinfo=read(proc+"/fdinfo/"+str(fd),65536).decode(),
               proc_stat=read(proc+"/stat",65536).decode(),
               locks=read("/proc/locks",262144).decode())
 db = os.lstat("/tmp/library.sqlite")
 result["database_identity"] = {"device":db.st_dev,"inode":db.st_ino,"major":os.major(db.st_dev),"minor":os.minor(db.st_dev)}
 result["state"] = "complete"
except (OSError,ValueError,KeyError,TypeError,UnicodeError) as error:
 result["limitation"] = type(error).__name__ + ":" + str(error)[:512]
print(json.dumps(result,sort_keys=True,separators=(",",":"),ensure_ascii=False))
'''


EVENT_SOURCE = 'import base64,json,os,stat,sys,time\nroot=sys.argv[1]; deadline=time.monotonic()+float(sys.argv[2]); result={"state":"missing"}\nwhile time.monotonic()<deadline:\n try:\n  fd=os.open(root+"/event.json",os.O_RDONLY|os.O_NOFOLLOW)\n except FileNotFoundError:\n  time.sleep(0.05); continue\n try:\n  if not stat.S_ISREG(os.fstat(fd).st_mode): raise ValueError("regular event required")\n  raw=os.read(fd,16385)\n  if len(raw)>16384: raise ValueError("event bound")\n  result={"state":"observed","event_b64":base64.b64encode(raw).decode()}; break\n finally: os.close(fd)\nprint(json.dumps(result,sort_keys=True,separators=(",",":")))\n'

RELEASE_SOURCE = ATOMIC_PUBLICATION_SOURCE + r'''import json,stat,sys,time
root = sys.argv[1]
_publish_complete(root,"release",b"1")
deadline = time.monotonic() + 10
decision = "missing"
while time.monotonic() < deadline:
 try:
  fd = os.open(root+"/decision.json",os.O_RDONLY|os.O_NOFOLLOW)
 except FileNotFoundError:
  time.sleep(0.01)
  continue
 try:
  if not stat.S_ISREG(os.fstat(fd).st_mode): raise ValueError("regular decision required")
  raw = os.read(fd,129)
  if raw == b'{"decision":"released"}': decision = "released"
  elif raw == b'{"decision":"expired"}': decision = "expired"
  else: raise ValueError("invalid hook decision")
 finally:
  os.close(fd)
 break
print(json.dumps({"released":True,"acknowledgement":decision},sort_keys=True,separators=(",",":")))
'''

SETUP_HOOK_SOURCE = 'import json,os,sys\nroot="/tmp/.gossip-worker-hooks-v1"\nos.makedirs(root,mode=0o700,exist_ok=True)\nos.mkdir(root+"/"+sys.argv[1],0o700)\nprint(json.dumps({"created":True},sort_keys=True,separators=(",",":")))\n'

INITIAL_DIRECTORY_SOURCE = 'import json,os\nif os.listdir("/tmp"): raise ValueError("initial volume is not empty")\nos.mkdir("/tmp/backups",0o700)\nprint(json.dumps({"created":True},sort_keys=True,separators=(",",":")))\n'


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def interface() -> dict[str, Any]:
    return {"protocol": PROTOCOL, "phases": list(PHASES), "maximum_hold_seconds": MAX_HOLD_SECONDS,
            "hook_import": "gossip_worker_hook_v1.boundary", "root": ROOT,
            "candidate_claims_are_authority": False,
            "phase_semantics": {
                "owner_acquired": "After durable generation publication and live exclusive owner acquisition; no active claim.",
                "after_claim": "After selected claim capture/publication, outside any write transaction and before commit work.",
                "before_commit": "After provisional catalog work, immediately before SQLite commit while transaction is active.",
                "after_commit_before_output": "After SQLite commit returns, before any once response byte is written."},
            "unsupported_or_missing": "unavailable; never infer a boundary from elapsed time",
            "publication": "Write all bytes and fsync a private same-directory file; publish with exclusive hard link and fsync directory; never overwrite event or release.",
            "hold_expiry": "Each action is bounded by the original host monotonic timestamp before worker start plus 90 seconds until release is acknowledged. Expiry, missing acknowledgement or an exceeded host bound makes affected observations unavailable.",
            "release_acknowledgement_seconds": RELEASE_ACK_SECONDS,
            "sources": {name: hashlib.sha256(raw.encode()).hexdigest() for name, raw in
                        (("hook", HOOK_SOURCE), ("launcher", LAUNCHER_SOURCE),
                         ("sqlite_reader", SNAPSHOT_SOURCE), ("kernel_reader", KERNEL_SOURCE),
                         ("event_reader", EVENT_SOURCE), ("release", RELEASE_SOURCE),
                         ("hook_setup", SETUP_HOOK_SOURCE), ("initial_directories", INITIAL_DIRECTORY_SOURCE))},
            "limits": {"event_bytes": MAX_EVENT_BYTES, "snapshot_bytes": MAX_SNAPSHOT_BYTES},
            "unresolved": ["Markers do not independently prove a matching application claim.",
                           "A SQLite write lock alone does not prove provisional catalog rows were written.",
                           "Lock attribution only qualifies visible POSIX/FLOCK WRITE locks with the event PID."]}
