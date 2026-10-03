"""Independent, post-freeze M1 interface observations inside the existing sandbox.

Expected values live only on the host. The child exercises real application
modules, CLI processes and loopback HTTP; it never reports a ``passed`` flag.
This is a partial M1 acceptance surface, not browser or whole-project acceptance.
The caller owns the complete-cohort barrier and immutable Git source capture.
No source is imported or executed on the host, and no existing evaluator changes.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
from typing import Any, Callable

from .blackbox_validator import BlackboxValidator, SUPERVISOR_ADAPTER
from .sandbox import DockerValidator

PROTOCOL = "library-m1-independent-interfaces-v4"
PURPOSE = "independent_acceptance"
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_OID = re.compile(r"[0-9a-f]{40}\Z")
_ID = re.compile(r"[A-Za-z0-9_-]{1,96}\Z")

# Generic observations only: no oracle, expected values, score or verdict enters
# the sandbox. Child stdout remains untrusted even though this driver is authored.
# Separate CLI/server descendants remain in the supervisor's process group, so
# its unconditional process-group kill also removes descendants after failure.
CHILD_ADAPTER = r'''import contextlib
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import zipfile

WORKSPACE = "/workspace"
sys.path.insert(0, WORKSPACE)
payload = json.loads(sys.stdin.buffer.read(65537))
from library.catalog.store import Store
from library.common import LibraryError
from library.ingestion.jobs import JobManager

def call(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except LibraryError as error:
        return {"error": error.code}

def strict_json(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError('duplicate JSON key')
            value[key] = item
        return value
    def constant(_):
        raise ValueError('nonfinite JSON')
    value = json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_constant=constant)
    # Also rejects finite-looking exponent syntax that overflows to infinity.
    json.dumps(value,allow_nan=False)
    return value

def write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)

def cli(db, root, *arguments):
    # -c preserves a writable per-case cwd without setting candidate PYTHONPATH.
    bootstrap = "import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module('library',run_name='__main__')"
    return [sys.executable, "-I", "-c", bootstrap, WORKSPACE,
            "--db", str(db), "--root", str(root), *arguments]

def cli_data(db, root, *arguments):
    # File-backed output bounds resident memory. The outer sandbox bounds tmpfs;
    # the supervisor bounds time and kills this entire group on hangs/overflow.
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        result = subprocess.run(cli(db, root, *arguments), stdout=out, stderr=err,
                                stdin=subprocess.DEVNULL, timeout=4, check=False)
        out.seek(0); err.seek(0)
        raw, error = out.read(65537), err.read(65537)
    if len(raw) > 65536 or len(error) > 65536:
        raise ValueError("CLI output bound")
    value = strict_json(raw if result.returncode == 0 else error)
    return {"exit": result.returncode, "value": value,
            "other_stream_empty": not (error if result.returncode == 0 else raw)}

def intake(base):
    db, root = base / 'intake.sqlite', base / 'input'
    root.mkdir()
    write(root / 'folder/z.html', b'<b>literal</b>')
    write(root / 'folder/nested/a.md', 'caf\u00e9\n'.encode())
    with zipfile.ZipFile(root / 'bundle.zip', 'w') as archive:
        archive.writestr('empty/', '')
        archive.writestr('q.txt', 'shared')
        archive.writestr('deep/r.md', 'shared')
    (root / 'bundle.json').write_text(json.dumps({'entries':[
        {'source':'json/omega.txt','text':'\u03a9'}, {'source':'json/empty.txt','text':''}]}))
    store = Store(db); jobs = JobManager(store)
    submitted = [jobs.submit_directory('directory', root / 'folder', 'd'),
                 jobs.submit_zip('archive', root / 'bundle.zip', 'z'),
                 jobs.submit_json('json', root / 'bundle.json')]
    manifests = [store.job_manifest(name) for name in ('directory', 'archive', 'json')]
    receipts = [jobs.commit(jobs.prepare(name)) for name in ('directory', 'archive', 'json')]
    store.close(); store = Store(db)
    result = {'submitted':submitted, 'manifests':manifests, 'receipts':receipts,
              'jobs':store.list_jobs(), 'documents':store.documents()}
    store.close()
    return result

def rejected_intake(base):
    root = base / 'input'; root.mkdir()
    store = Store(base / 'reject.sqlite'); jobs = JobManager(store)
    store.insert('existing.txt', b'old')
    with zipfile.ZipFile(root / 'traversal.zip', 'w') as archive:
        archive.writestr('ok.txt', 'new')
        archive.writestr('x/../../escape.txt', 'bad')
    with zipfile.ZipFile(root / 'utf8.zip', 'w') as archive:
        archive.writestr('ok.txt', 'new')
        archive.writestr('bad.txt', b'\xff')
    write(root / 'directory/ok.txt', b'new')
    (root / 'directory/link.md').symlink_to(root / 'directory/ok.txt')
    (root / 'oversize.json').write_text(json.dumps({'entries':[
        {'source':'large.txt','text':'\u00e9' * 16385}]}))
    (root / 'extra.json').write_text('{"entries":[],"surprise":1}')
    errors = [call(jobs.submit_zip,'traversal',root/'traversal.zip','safe'),
              call(jobs.submit_zip,'utf8',root/'utf8.zip','safe'),
              call(jobs.submit_directory,'symlink',root/'directory','safe'),
              call(jobs.submit_json,'oversize',root/'oversize.json'),
              call(jobs.submit_json,'extra',root/'extra.json')]
    result = {'errors':errors, 'jobs':store.list_jobs(), 'documents':store.documents(),
              'escaped_exists':(root/'escape.txt').exists() or (base/'escape.txt').exists()}
    store.close()
    return result

def durability(base):
    db = base / 'durable.sqlite'
    store = Store(db); jobs = JobManager(store)
    store.insert('prior.txt', b'prior')
    submitted = jobs.submit('batch', [{'source':'b.txt','text':'same'},
                                     {'source':'a.md','text':'same'}])
    old = jobs.prepare('batch')
    other = Store(db)
    cancelled = other.cancel_job('batch')
    other.close(); store.close()
    store = Store(db); jobs = JobManager(store)
    stale_cancelled = call(jobs.commit, old)
    retried = jobs.retry('batch'); current = jobs.prepare('batch')
    failed_write = call(jobs.commit, current, fail_before_commit=True)
    running, prior = jobs.get('batch'), store.documents()
    store.close(); store = Store(db); jobs = JobManager(store)
    stale_retried = call(jobs.commit, old)
    committed = jobs.commit(current)
    store.close(); store = Store(db); jobs = JobManager(store)
    replay = jobs.commit(current)
    jobs.submit('conflict',[{'source':'new.txt','text':'new'},
                            {'source':'later.txt','text':'pending'}])
    conflict_token = jobs.prepare('conflict')
    other = Store(db); other.insert('later.txt', b'external'); other.close()
    conflict = call(jobs.commit, conflict_token)
    result = {'submitted':submitted, 'old_token':old, 'cancelled':cancelled,
              'stale_cancelled':stale_cancelled,'retried':retried,'current_token':current,
              'failed_write':failed_write,'running':running,'before_commit':prior,
              'stale_retried':stale_retried,'committed':committed,'replay':replay,
              'conflict':conflict,'jobs':store.list_jobs(),'documents':store.documents()}
    store.close()
    return result

def cli_restart(base):
    db, root = base / 'cli.sqlite', base / 'input'; root.mkdir()
    (root/'payload.json').write_text(json.dumps({'entries':[
        {'source':'cli/a.txt','text':'kept across processes'}]}))
    commands = [('job-submit','from_cli','--kind','json','payload.json'),
                ('job-prepare','from_cli'),('job-cancel','from_cli'),
                ('job-commit','from_cli','1'),('job-retry','from_cli'),
                ('job-prepare','from_cli'),('job-commit','from_cli','3'),
                ('job-show','from_cli'),('jobs',),('list',),('job-commit','from_cli','3')]
    return [cli_data(db, root, *command) for command in commands]

@contextlib.contextmanager
def server(db, root):
    # All traffic stays on the container's private loopback. No published port.
    with socket.socket() as probe:
        probe.bind(('127.0.0.1',0)); port = probe.getsockname()[1]
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen(cli(db,root,'serve','--port',str(port)),
            stdin=subprocess.DEVNULL,stdout=log,stderr=log)
        try:
            for _ in range(100):
                if process.poll() is not None:
                    raise RuntimeError('HTTP server stopped before readiness')
                try:
                    value = request_http(port,'GET','/health')
                    if value['status'] == 200:
                        break
                except (OSError,http.client.HTTPException):
                    pass
                time.sleep(.02)
            else:
                raise TimeoutError('HTTP readiness deadline')
            yield port
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait(timeout=1)

def request_http(port, method, path, body=None, content_type='application/json'):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
    try:
        raw = None if body is None else json.dumps(body).encode()
        connection.request(method,path,raw,{} if raw is None else {'Content-Type':content_type})
        response = connection.getresponse(); payload = response.read(65537)
        if len(payload) > 65536:
            raise ValueError('HTTP response bound')
        return {'status':response.status,'value':strict_json(payload),
                'json_content_type':response.getheader('Content-Type','').split(';',1)[0].strip().lower() == 'application/json'}
    finally:
        connection.close()

def http_restart(base):
    db, root = base / 'http.sqlite', base / 'input'; root.mkdir()
    (root/'http.json').write_text(json.dumps({'entries':[
        {'source':'http/literal.html','text':'<script>never execute</script>'}]}))
    result = []
    with server(db,root) as port:
        result.extend([request_http(port,'GET','/health'),
            request_http(port,'POST','/api/jobs',{'job_id':'web','json':'http.json'}),
            request_http(port,'POST','/api/jobs/web/prepare',{}),
            request_http(port,'POST','/api/jobs/web/cancel',{}),
            request_http(port,'POST','/api/jobs/web/commit',{'epoch':1})])
    # A genuinely new server process must use persisted cancelled state.
    with server(db,root) as port:
        result.extend([request_http(port,'GET','/api/jobs/web'),
            request_http(port,'POST','/api/jobs/web/retry',{}),
            request_http(port,'POST','/api/jobs/web/prepare',{}),
            request_http(port,'POST','/api/jobs/web/commit',{'epoch':3,'fail_before_commit':True}),
            request_http(port,'POST','/api/jobs/web/commit',{'epoch':True}),
            request_http(port,'POST','/api/jobs/web/commit',{'epoch':3}),
            request_http(port,'GET','/api/jobs'),request_http(port,'GET','/api/documents'),
            request_http(port,'POST','/api/jobs/web/cancel',{},'text/plain')])
    result.append(cli_data(db,root,'job-show','web'))
    return result

functions = {'intake':intake,'rejected_intake':rejected_intake,'durability':durability,
             'cli_restart':cli_restart,'http_restart':http_restart}
if type(payload) is not dict or set(payload) != {'scenario'} or payload['scenario'] not in functions:
    raise ValueError('Unknown closed acceptance scenario')
with tempfile.TemporaryDirectory(prefix='acceptance-') as directory:
    answer = functions[payload['scenario']](Path(directory).resolve())
sys.stdout.write(json.dumps(answer,ensure_ascii=True,allow_nan=False,separators=(',',':'))+'\n')
'''


def _bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True,
                      separators=(",", ":")).encode()


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def source_sha256(files: dict[str, str]) -> str:
    """Exactly the immutable source encoding used by BlackboxValidator."""
    return _sha(_bytes(files))


def _document(source: str, text: str) -> dict[str, str]:
    # Independently derived from the written identity contract, not an app import.
    return {"document_id": "doc-" + _sha(b"document\0" + source.encode()),
            "source_id": "src-" + _sha(b"source\0" + source.encode()),
            "source": source, "blob_id": "blob-" + _sha(text.encode()),
            "title": source.rsplit("/", 1)[-1], "text": text}


def _documents(entries: list[tuple[str, str]]) -> list[dict[str, str]]:
    return [_document(source, text) for source, text in sorted(entries)]


def _job(name: str, state: str = "queued", epoch: int = 1, total: int = 2,
         error: str | None = None) -> dict[str, Any]:
    return {"job_id": name, "epoch": epoch, "state": state, "total": total,
            "completed": total if state == "completed" else 0, "error": error}


def _receipt(name: str, entries: list[tuple[str, str]], epoch: int = 1) -> dict[str, Any]:
    return {"job": _job(name, "completed", epoch, len(entries)),
            "documents": _documents(entries)}


def acceptance_cases() -> list[dict[str, Any]]:
    """Fresh closed independent assertions, withheld from active role prompts.

    This intentionally does not call the public dictionary oracle or authored
    reference. It covers additional interfaces; it is not a held-out task family.
    """
    directory = [("d/nested/a.md", "café\n"), ("d/z.html", "<b>literal</b>")]
    archive = [("z/q.txt", "shared"), ("z/deep/r.md", "shared")]
    bundle = [("json/omega.txt", "Ω"), ("json/empty.txt", "")]
    batch = [("a.md", "same"), ("b.txt", "same")]
    cli_entries = [("cli/a.txt", "kept across processes")]
    web_entries = [("http/literal.html", "<script>never execute</script>")]
    cli_values = [_job("from_cli", total=1), {"job_id":"from_cli","epoch":1},
        _job("from_cli","cancelled",2,1), {"error":"stale_epoch"},
        _job("from_cli",epoch=3,total=1), {"job_id":"from_cli","epoch":3},
        _receipt("from_cli",cli_entries,3), _job("from_cli","completed",3,1),
        {"jobs":[_job("from_cli","completed",3,1)]},
        {"documents":_documents(cli_entries),"total":1}, _receipt("from_cli",cli_entries,3)]
    web_values = [(200,{"status":"ok","schema":0}), (200,_job("web",total=1)),
        (200,{"job_id":"web","epoch":1}), (200,_job("web","cancelled",2,1)),
        (409,{"error":"stale_epoch"}), (200,_job("web","cancelled",2,1)),
        (200,_job("web",epoch=3,total=1)), (200,{"job_id":"web","epoch":3}),
        (400,{"error":"invalid_request"}), (400,{"error":"invalid_request"}),
        (200,_receipt("web",web_entries,3)), (200,{"jobs":[_job("web","completed",3,1)]}),
        (200,{"documents":_documents(web_entries),"total":1}), (400,{"error":"invalid_request"})]
    observations = [
        ("intake", "Real directory/ZIP/JSON admission, identity, manifest and reopen", {
            "submitted":[_job(name) for name in ("directory","archive","json")],
            "manifests":[[{"source":s,"text":t} for s,t in sorted(entries)]
                         for entries in (directory,archive,bundle)],
            "receipts":[_receipt(name,entries) for name,entries in
                        (("directory",directory),("archive",archive),("json",bundle))],
            "jobs":[_job(name,"completed") for name in ("archive","directory","json")],
            "documents":_documents(directory+archive+bundle)}),
        ("rejected_intake", "Discovery rejection precedes durable admission and catalog writes", {
            "errors":[{"error":code} for code in ("invalid_source","invalid_utf8",
                       "invalid_source","too_large","invalid_json")],
            "jobs":[], "documents":_documents([("existing.txt","old")]),"escaped_exists":False}),
        ("durability", "Cancellation, rollback, replay and cross-connection conflict fencing", {
            "submitted":_job("batch"),"old_token":{"job_id":"batch","epoch":1},
            "cancelled":_job("batch","cancelled",2),"stale_cancelled":{"error":"stale_epoch"},
            "retried":_job("batch",epoch=3),"current_token":{"job_id":"batch","epoch":3},
            "failed_write":{"error":"injected_failure"},"running":_job("batch","running",3),
            "before_commit":_documents([("prior.txt","prior")]),"stale_retried":{"error":"stale_epoch"},
            "committed":_receipt("batch",batch,3),"replay":_receipt("batch",batch,3),
            "conflict":{"error":"source_changed"},
            "jobs":[_job("batch","completed",3),_job("conflict","failed",error="source_changed")],
            "documents":_documents(batch+[("prior.txt","prior"),("later.txt","external")])}),
        ("cli_restart", "Real isolated CLI invocations share durable state and error conventions",
            [{"exit":2 if i == 3 else 0,"value":value,"other_stream_empty":True}
             for i,value in enumerate(cli_values)]),
        ("http_restart", "Real loopback HTTP restart, body validation and CLI-visible persistence",
            [{"status":status,"value":value,"json_content_type":True} for status,value in web_values] +
            [{"exit":0,"value":_job("web","completed",3,1),"other_stream_empty":True}]),
    ]
    return [{"id":"m1-independent-"+name,"requirement":requirement,
             "input":{"scenario":name},"expected":expected}
            for name,requirement,expected in observations]


@dataclass(frozen=True, slots=True)
class AcceptancePolicy:
    image_id: str
    timeout_seconds: int = 120
    case_timeout_seconds: int = 30

    def __post_init__(self) -> None:
        if (type(self.image_id) is not str or not self.image_id.startswith("sha256:")
                or not _HASH.fullmatch(self.image_id[7:])):
            raise ValueError("An immutable local image ID is required")
        if (type(self.timeout_seconds) is not int or not 30 <= self.timeout_seconds <= 300
                or type(self.case_timeout_seconds) is not int
                or not 5 <= self.case_timeout_seconds <= 60):
            raise ValueError("Acceptance limits must be bounded integer seconds")


@dataclass(frozen=True, slots=True)
class FrozenSubject:
    cohort_id: str
    trajectory_id: str
    execution_contract_sha256: str
    commit_oid: str
    tree_oid: str
    source_sha256: str

    def __post_init__(self) -> None:
        for value in (self.cohort_id, self.trajectory_id):
            if type(value) is not str or not _ID.fullmatch(value):
                raise ValueError("Invalid frozen subject identity")
        for value in (self.execution_contract_sha256, self.source_sha256):
            if type(value) is not str or not _HASH.fullmatch(value):
                raise ValueError("Invalid frozen SHA256")
        for value in (self.commit_oid, self.tree_oid):
            if type(value) is not str or not _OID.fullmatch(value):
                raise ValueError("Immutable Git commit and tree required")


def make_validator(policy: AcceptancePolicy) -> BlackboxValidator:
    validator = BlackboxValidator(policy.image_id, timeout_seconds=policy.timeout_seconds,
                                  case_timeout_seconds=policy.case_timeout_seconds)
    # Deliberately compose the existing bounded executor without mutating its
    # module globals or any public evaluator instance.
    validator._sandbox = DockerValidator(policy.image_id,
        {"supervisor.py":SUPERVISOR_ADAPTER,"child.py":CHILD_ADAPTER},
        command=("python","-I","/checks/supervisor.py"), timeout_seconds=policy.timeout_seconds)
    return validator


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _save_raw(path: Path, raw: bytes) -> None:
    with path.open("xb") as output:
        output.write(raw); output.flush(); os.fsync(output.fileno())
    _sync_directory(path.parent)


def _save(path: Path, value: Any) -> None:
    _save_raw(path, _bytes(value))


def _runtime(validator: BlackboxValidator) -> dict[str, Any]:
    environment = validator._sandbox._environment()
    version = subprocess.run(["docker","version","--format","{{json .Server}}"],
        capture_output=True, timeout=15, env=environment, check=False)
    image = subprocess.run(["docker","image","inspect","--format","{{json .}}",validator.image],
        capture_output=True, timeout=15, env=environment, check=False)
    if (version.returncode or image.returncode or len(version.stdout)>65536
            or len(image.stdout)>1_048_576):
        raise ValueError("Exact Docker runtime observation failed")
    server, inspected = json.loads(version.stdout), json.loads(image.stdout)
    if (type(server) is not dict or type(server.get("Version")) is not str
            or not server["Version"] or type(inspected) is not dict
            or inspected.get("Id") != validator.image):
        raise ValueError("Docker runtime/image mismatch")
    return {"server":server,"image":{key:inspected.get(key)
            for key in ("Id","Os","Architecture","Variant")}}


_LOADED_SOURCES = {name:_sha(Path(__file__).with_name(name+".py").read_bytes())
                   for name in ("library_m1_acceptance_v4","blackbox_validator","sandbox")}


def _unchanged_sources() -> dict[str, str]:
    sources = {name:_sha(Path(__file__).with_name(name+".py").read_bytes())
               for name in _LOADED_SOURCES}
    if sources != _LOADED_SOURCES:
        raise ValueError("Loaded acceptance evaluator sources changed")
    return sources


def run_once(output: Path, files: dict[str, str], subject: FrozenSubject,
             freeze_receipt: bytes, verify_freeze: Callable[[FrozenSubject, bytes], bool],
             policy: AcceptancePolicy) -> dict[str, Any]:
    """Execute one fresh independent interface observation after an exact barrier.

    ``verify_freeze`` is a TRUSTED controller capability, not candidate evidence:
    it must verify the durable whole-cohort freeze and immutable Git source/tree
    association. This module validates the supplied source bytes against the
    subject. It does not manufacture a freeze barrier or authorize public release.
    Existing output is never resumed, replaced or re-executed. An interrupted
    intent remains unknown; the study controller owns the single-attempt policy.
    """
    if type(subject) is not FrozenSubject or type(policy) is not AcceptancePolicy:
        raise ValueError("Typed acceptance subject and policy required")
    sources = _unchanged_sources()
    cases = acceptance_cases()
    files, cases, _ = BlackboxValidator._inputs(files, cases)
    if source_sha256(files) != subject.source_sha256:
        raise ValueError("Source bytes differ from frozen subject")
    if type(freeze_receipt) is not bytes or not 1 <= len(freeze_receipt) <= 1_048_576:
        raise ValueError("Bounded durable freeze receipt required")
    if verify_freeze(subject, freeze_receipt) is not True:
        raise ValueError("Complete-cohort source freeze is not verified")
    output = Path(output)
    if not output.is_absolute() or output != output.resolve() or output.exists():
        raise ValueError("A new canonical absolute acceptance output is required")
    validator = make_validator(policy)
    runtime = _runtime(validator)
    arguments = validator._sandbox._arguments("ACCEPTANCE",Path("/WORKSPACE"),Path("/CHECKS"))
    arguments.insert(2,"--interactive")
    config = {"protocol":PROTOCOL,"purpose":PURPOSE,"subject":asdict(subject),
        "freeze_receipt_sha256":_sha(freeze_receipt),"suite_sha256":_sha(_bytes(cases)),
        "policy":asdict(policy),"runtime":runtime,"evaluator_sources":sources,
        "adapter_sha256":validator._sandbox.checks_sha256,
        "environment_sha256":_sha(_bytes(validator._sandbox._environment())),
        "host_python":{"implementation":platform.python_implementation(),"version":platform.python_version()},
        "sandbox_arguments":arguments,
        "qualified_dimensions":["intake","durable_jobs","cli_processes","http_restart"],
        "unqualified_dimensions":["browser","full_M1_requirements","M2","M3","M4"],
        "whole_project_acceptance":False,"reuse_allowed":False}
    output.mkdir(parents=False, exist_ok=False)
    _sync_directory(output.parent)
    _save_raw(output/"freeze-receipt.bin",freeze_receipt)
    _save(output/"intent.json",config)
    if verify_freeze(subject, freeze_receipt) is not True:
        raise ValueError("Complete-cohort freeze changed before execution")
    _unchanged_sources()
    receipt = validator.evaluate(files,cases)
    _save(output/"validator-receipt.json",receipt)
    if verify_freeze(subject, freeze_receipt) is not True:
        raise ValueError("Complete-cohort freeze changed during execution")
    _unchanged_sources()
    # The genuine validator owns host-side comparison. Binding sanity checks
    # prevent accidental use of an unrelated mutable instance's last result.
    if (receipt.get("source_sha256") != subject.source_sha256
            or receipt.get("suite_sha256") != config["suite_sha256"]
            or receipt.get("adapter_sha256") != config["adapter_sha256"]
            or receipt.get("image_id") != policy.image_id):
        raise ValueError("Acceptance receipt differs from exact execution intent")
    answer = {"protocol":PROTOCOL,"purpose":PURPOSE,"intent_sha256":_sha(_bytes(config)),
        "validator_receipt_sha256":_sha(_bytes(receipt)),"physical_execution_requested":True,
        "evaluation_completed":receipt["status"] in ("passed","failed"),
        "reused":False,"interface_checks_passed":receipt["passed"],"status":receipt["status"],
        "whole_project_acceptance":False,"browser_qualified":False,
        "case_count":len(cases),"source_sha256":subject.source_sha256}
    _save(output/"receipt.json",answer)
    return answer
