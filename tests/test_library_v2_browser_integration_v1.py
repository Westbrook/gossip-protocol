"""Physical browser qualification of the authored, freshly built prospective-v2 release.

Every app process imports the output of the actual release command. These are
explicit prospective-v2 adaptations of M4/M2/M3 regression workflows, not unchanged inherited
receipts and not arbitrary-candidate or independent study acceptance. Raw HTTP
bodies, downloads, CLI results, source identities and the release manifest remain
available after the owned processes have stopped. The host Python is recorded;
qualification of the pinned Python 3.12 image belongs to the Docker lane.
"""

from contextlib import contextmanager
import hashlib
import inspect
import http.client
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import threading
import unittest
from unittest.mock import Mock, patch

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.library_v2_reference_v1 import write_v2_project
from tests.test_library_m2_browser_reference_v1 import (
    MAX_LOG_BYTES,
    ROOT,
    _cli,
    _environment,
    _runtime,
    _stop,
)

DRIVER = ROOT / "devtools/browser/library_v2_reference.cjs"
IMAGE = "sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f"
CONTRACT = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
RELEASE_BOOTSTRAP = r'''
import resource,runpy,sys
resource.setrlimit(resource.RLIMIT_CPU,(30,30))
resource.setrlimit(resource.RLIMIT_FSIZE,(33554432,33554432))
resource.setrlimit(resource.RLIMIT_NOFILE,(96,96))
sys.path.insert(0,sys.argv.pop(1))
runpy.run_module('library.clients.release',run_name='__main__')
'''


def _publish_port_record(target, value):
    """Publish one complete bounded startup record from the owned child."""
    temporary = target.with_name(target.name + ".partial")
    if target.exists() or target.is_symlink():
        raise AssertionError("Server readiness output already exists")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    if target.exists() or target.is_symlink():
        raise AssertionError("Server readiness output appeared before publication")
    os.replace(temporary, target)


# This trusted bootstrap includes the exact helper above, not an independent
# reimplementation. Application imports resolve only from the actual release.
_SERVER_BOOTSTRAP = r'''
import json,os,resource,signal,sys
from pathlib import Path
resource.setrlimit(resource.RLIMIT_CPU,(60,60))
resource.setrlimit(resource.RLIMIT_FSIZE,(33554432,33554432))
resource.setrlimit(resource.RLIMIT_NOFILE,(128,128))
''' + inspect.getsource(_publish_port_record) + r'''
root=Path(sys.argv[1]); sys.path.insert(0,str(root/'app'))
from library.catalog.store import Store
from library.query.service import Service
from library.clients.http import handler_for
from http.server import HTTPServer
store=Store(root/'catalog.sqlite3')
def stopped(_signum,_frame): raise SystemExit(0)
signal.signal(signal.SIGTERM,stopped)
try:
    with HTTPServer(('127.0.0.1',0),handler_for(Service(store,root/'input'))) as server:
        _publish_port_record(Path(sys.argv[2]), {'port':server.server_port,
            'pid':os.getpid(),'incarnation':int(sys.argv[3])})
        server.serve_forever(poll_interval=0.05)
finally:
    store.close()
'''


def _wait_server_port(child, port_file, incarnation, *, timeout=10):
    deadline = time.monotonic() + timeout
    while not port_file.exists():
        if child.poll() is not None:
            raise AssertionError("Owned server exited before readiness publication")
        if time.monotonic() >= deadline:
            raise AssertionError("Owned server readiness publication timed out")
        time.sleep(0.01)
    if child.poll() is not None or not port_file.is_file() or port_file.is_symlink():
        raise AssertionError("Invalid owned server readiness source")
    with port_file.open("rb") as stream:
        raw = stream.read(513)
    if not raw or len(raw) > 512:
        raise AssertionError("Owned server readiness record exceeds bound or is empty")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate readiness field")
            result[key] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, UnicodeError) as error:
        raise AssertionError("Owned server published malformed readiness JSON") from error
    if (type(value) is not dict or set(value) != {"port", "pid", "incarnation"}
            or type(value["port"]) is not int or not 1 <= value["port"] <= 65535
            or type(value["pid"]) is not int or value["pid"] != child.pid
            or type(value["incarnation"]) is not int or value["incarnation"] != incarnation):
        raise AssertionError("Owned server readiness identity or port mismatch")
    return value


@contextmanager
def _server(root, incarnation):
    port_file = root / f"server-{incarnation}-port.json"
    log = root / f"server-{incarnation}.log"
    with log.open("xb") as output:
        child = subprocess.Popen([sys.executable, "-I", "-c", _SERVER_BOOTSTRAP,
                                  str(root), str(port_file), str(incarnation)],
                                 cwd=root, stdin=subprocess.DEVNULL, stdout=output,
                                 stderr=output, env=_environment(root), start_new_session=True)
        try:
            value = _wait_server_port(child, port_file, incarnation)
            connection = http.client.HTTPConnection("127.0.0.1", value["port"], timeout=3)
            try:
                connection.request("GET", "/health")
                response = connection.getresponse()
                raw = response.read(513)
                if response.status != 200 or len(raw) > 512 or json.loads(raw) != {"status": "ok", "schema": 4}:
                    raise AssertionError("Owned released server did not pass bounded schema4 readiness")
            finally:
                connection.close()
            if child.poll() is not None:
                raise AssertionError("Owned server exited after publishing readiness")
            with (root / f"server-{incarnation}-readiness.json").open("x") as retained:
                json.dump({"identity": value, "health_raw_hex": raw.hex(), "health_sha256": _sha(raw),
                           "bootstrap_sha256": _sha(_SERVER_BOOTSTRAP.encode()),
                           "served_directory": "app", "staging_source_served": False}, retained, indent=2)
                retained.write("\n")
            yield value["port"]
        finally:
            _stop(child)
            if log.stat().st_size > MAX_LOG_BYTES:
                raise AssertionError(f"Server log exceeded observation bound: {log}")


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _records(directory):
    return [{"path": path.relative_to(directory).as_posix(), "sha256": _sha(path.read_bytes()),
             "bytes": path.stat().st_size}
            # Manifest order is the complete relative POSIX path string, not
            # pathlib's component tuple (library-* sorts before library/... ).
            for path in sorted(directory.rglob("*"), key=lambda item: item.relative_to(directory).as_posix())
            if path.is_file() and "__pycache__" not in path.parts]


def _fixture(root):
    root.mkdir()
    staging = root / "source-staging"
    write_v2_project(staging)
    source_records = _records(staging)
    # Actual release invocation; the app below is never served from staging.
    stdout, stderr = root / "release-command.json", root / "release-command.stderr"
    with stdout.open("xb") as out, stderr.open("xb") as err:
        result = subprocess.run([sys.executable, "-I", "-c", RELEASE_BOOTSTRAP, str(staging),
                                 "--output", str(root / "app")], cwd=root,
                                stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                env=_environment(root), timeout=45, check=False)
    if result.returncode or stderr.stat().st_size or stdout.stat().st_size > MAX_LOG_BYTES:
        raise AssertionError(f"Actual release command failed; inspect {stdout} and {stderr}")
    returned = json.loads(stdout.read_bytes())
    if set(returned) != {"format", "manifest", "files", "source_sha256"}:
        raise AssertionError("Release command result is not the declared closed shape")
    manifest_path = root / "app/release-manifest.json"
    manifest_raw = manifest_path.read_bytes()
    manifest = json.loads(manifest_raw)
    actual = [record for record in _records(root / "app") if record["path"] != "release-manifest.json"]
    if manifest["files"] != actual:
        raise AssertionError("Release manifest does not match actual packaged bytes")
    source_sha256 = _sha(_canonical(actual))
    if returned != {"format": "local-research-library-release-v1", "manifest": "release-manifest.json",
                    "files": len(actual), "source_sha256": source_sha256}:
        raise AssertionError("Release result is not bound to its actual file records")
    if manifest["source_sha256"] != source_sha256 or manifest["product_contract_sha256"] != CONTRACT:
        raise AssertionError("Release source or product-contract binding mismatch")
    if manifest["runtime"] != {"image": IMAGE, "python": "3.12"} or manifest["storage_version"] != 4:
        raise AssertionError("Release runtime/storage declaration mismatch")
    for name in ("INSTALL.md", "API.md", "RECOVERY.md", "USER-GUIDE.md"):
        if not (root / "app/release" / name).is_file():
            raise AssertionError(f"Released operator documentation missing: {name}")
    for name in ("input", "backups", "control"):
        (root / name).mkdir()
    for name, text in {
        "alpha.txt": '<img src=x onerror="window.__m2Injected=true"> alpha original',
        "beta.txt": "beta original",
        "gamma.txt": "gamma original",
        "refresh.txt": '<svg onload="window.__m2Injected=true"> alpha path refresh',
        "later.txt": "created after the backup snapshot",
    }.items():
        (root / "input" / name).write_text(text, encoding="utf-8")
    (root / "input/bundle.json").write_text(json.dumps({"entries": [
        {"source": "jobs/one.txt", "text": "job original"}]}), encoding="utf-8")
    dataset = root / "app/release/dataset"
    for name in ("welcome.txt", "notes.md", "literal.html"):
        (root / "input" / name).write_bytes((dataset / name).read_bytes())
    (root / "input/dataset.json").write_text(json.dumps({"entries": [
        {"source": "literal.html", "text": (dataset / "literal.html").read_text(encoding="utf-8")}]}), encoding="utf-8")
    (root / "source-bindings.json").write_text(json.dumps({
        "purpose": "authored_reference_browser_qualification", "whole_project_acceptance": False,
        "host_python": sys.version, "pinned_python_qualification": "separate Docker lane",
        "staged_source_records": source_records, "staged_source_sha256": _sha(_canonical(source_records)),
        "actual_release_result": returned, "release_manifest_sha256": _sha(manifest_raw),
        "released_source_sha256": source_sha256, "driver_sha256": _sha(DRIVER.read_bytes()),
        "served_directory": "app", "served_staging_source": False,
        "product_contract_sha256": CONTRACT, "adaptation": "prospective-v2; frozen M4 receipt not reused",
    }, indent=2) + "\n")


def _adopt(root):
    value = _cli(root, "root-adoption", "--backup-dir", str((root / "backups").resolve()),
                 "backup-root-adopt", "--expect-unbound")
    if value != {"adopted": True, "registered": 0}:
        raise AssertionError("Explicit empty-root adoption did not match its contract")


def _browser(root, port, workflow, layout="mixed"):
    node, modules = _runtime()
    environment = _environment(root)
    environment["NODE_PATH"] = str(modules)
    cache = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    environment["PLAYWRIGHT_BROWSERS_PATH"] = str(Path(cache).expanduser().resolve()) if cache else str(
        Path.home() / ("Library/Caches/ms-playwright" if sys.platform == "darwin" else ".cache/ms-playwright"))
    command = [str(node), str(DRIVER), "--origin", f"http://127.0.0.1:{port}",
               "--output", str(root / "browser"), "--worker-control", str(root / "control"),
               "--workflow", workflow, "--layout", layout]
    worker = None
    with (root / "driver.stdout").open("xb") as out, (root / "driver.stderr").open("xb") as err:
        child = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                 env=environment, start_new_session=True)
        try:
            deadline = time.monotonic() + 180
            while child.poll() is None:
                if time.monotonic() > deadline:
                    raise AssertionError(f"Owned browser timed out; retained evidence: {root}")
                request = root / "control/worker-request.json"
                if workflow == "maintenance" and worker is None and request.exists():
                    if request.stat().st_size > 256 or json.loads(request.read_bytes()) != {"job_id": "m4_browser_job"}:
                        raise AssertionError("Unexpected worker command request")
                    # Fixed authored process command; driver data cannot select args.
                    worker = _cli(root, "worker", "worker", "--once")
                    result = root / "control/worker-result.json"
                    temporary = root / "control/worker-result.partial"
                    with temporary.open("x") as stream:
                        json.dump(worker, stream)
                        stream.flush()
                        os.fsync(stream.fileno())
                    temporary.rename(result)
                time.sleep(0.025)
            result_code = child.returncode
        finally:
            _stop(child)
    for stream in (root / "driver.stdout", root / "driver.stderr"):
        if stream.stat().st_size > MAX_LOG_BYTES:
            raise AssertionError(f"Browser log exceeded observation limit: {stream}")
    if result_code != 0:
        raise AssertionError(f"Inspect retained v2 browser receipt and logs in {root}")
    receipt_path = root / "browser/receipt.json"
    if receipt_path.stat().st_size > 2 * MAX_LOG_BYTES:
        raise AssertionError("Browser receipt exceeded observation limit")
    receipt = json.loads(receipt_path.read_bytes())
    if not receipt["passed"] or receipt["browserLaunches"] != 1 or receipt["freshContexts"] != 2:
        raise AssertionError(f"V2 browser qualification failed: {receipt_path}")
    if workflow == "maintenance" and worker is None:
        raise AssertionError("V2 did not invoke its real fixed CLI worker")
    return receipt


def _raw_http(root, port, name, target):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    try:
        connection.request("GET", target)
        response = connection.getresponse()
        raw = response.read(MAX_LOG_BYTES + 1)
        if len(raw) > MAX_LOG_BYTES:
            raise AssertionError("HTTP reopen observation exceeded bound")
        relative = f"reopen-http-{name}.bin"
        with (root / relative).open("xb") as stream:
            stream.write(raw)
        evidence = {"target": target, "status": response.status, "headers": response.getheaders(),
                    "raw_file": relative, "bytes": len(raw), "sha256": _sha(raw), "value": json.loads(raw)}
        if response.status != 200:
            raise AssertionError(f"HTTP reopen failed; inspect {relative}")
        return evidence
    finally:
        connection.close()


def _persistence(root, receipt, workflow):
    expected = receipt["finalState"]
    commands = {"documents": ("documents-v1", "--deleted", "all"), "collections": ("collections",),
                "jobs": ("jobs",), "history": ("revisions-v1", expected["alphaID"]),
                "legacyDocuments": ("documents", "--deleted", "all"),
                "legacyHistory": ("revisions", expected["alphaID"])}
    targets = {"documents": "/api/v1/documents?deleted=all", "collections": "/api/lifecycle/collections",
               "jobs": "/api/jobs", "history": "/api/v1/documents/" + expected["alphaID"] + "/revisions",
               "legacyDocuments": "/api/lifecycle/documents?deleted=all",
               "legacyHistory": "/api/lifecycle/documents/" + expected["alphaID"] + "/revisions"}
    if workflow in {"maintenance", "dataset"}:
        commands.update({"backups": ("backups",), "diagnostics": ("diagnostics",)})
        targets.update({"backups": "/api/maintenance/backups", "diagnostics": "/api/maintenance/diagnostics"})
    observed = {key: _cli(root, index, *arguments) for index, (key, arguments) in enumerate(commands.items(), 1)}
    with _server(root, 2) as port:
        reopened = {key: _raw_http(root, port, key, target) for key, target in targets.items()}
    comparisons = {key: {"cli_matches_browser": observed[key] == expected[key],
                         "http_matches_cli": reopened[key]["value"] == observed[key]} for key in commands}
    result = {"passed": all(all(values.values()) for values in comparisons.values()),
              "physically_executed": True, "purpose": "authored_reference_browser_qualification",
              "whole_project_acceptance": False, "browser_final_state": {key: expected[key] for key in commands},
              "fresh_cli_observations": observed, "fresh_http_process_observations": reopened,
              "comparisons": comparisons,
              "checks": ["fresh_cli_from_release_output", "fresh_http_process_from_release_output",
                         "actual_raw_response_bytes_retained", "all_owned_processes_stopped"]}
    (root / "persistence-receipt.json").write_text(json.dumps(result, indent=2) + "\n")
    if not result["passed"]:
        raise AssertionError(f"Persistence mismatch: {root / 'persistence-receipt.json'}")


# These names/columns are the PUBLIC schema4 + preserved schema3 interchange
# tables, not a query of implementation-private helpers for expected outcomes.
PUBLIC_TABLES = (
    "metadata", "blobs", "documents", "jobs", "collections", "control",
    "job_control", "document_control", "maintenance_artifacts", "backups",
    "search_state", "search_entries", "document_state", "document_revisions",
)
HIGH = 9007199254740993
COUNTER_MAX = 9223372036854775807


def _counter_fixture(root, maximum):
    _fixture(root)
    _adopt(root)
    _cli(root, "seed-alpha", "import", "alpha.txt")
    _cli(root, "seed-beta", "import", "beta.txt")
    _cli(root, "seed-collection", "collection-create", "stable", "--expected-generation", "2")
    (root / "input/empty.json").write_text('{"entries":[]}', encoding="utf-8")
    _cli(root, "seed-job", "job-submit", "counter_empty", "--kind", "json", "empty.json")
    value = COUNTER_MAX if maximum else HIGH
    # Fixed portable SQL input authored before the application HTTP process
    # starts. Both current counters AND durable high-waters are set together.
    # Only contract-public columns are changed; no internal API is an oracle.
    with sqlite3.connect(root / "catalog.sqlite3") as connection:
        if connection.execute("SELECT value FROM metadata WHERE key='schema'").fetchone() != ("4",):
            raise AssertionError("Counter fixture requires actual schema4 released initialization")
        connection.execute("UPDATE metadata SET value=? WHERE key='catalog_generation'", (str(value),))
        connection.execute("UPDATE document_state SET edit_version=?", (value,))
        connection.execute("UPDATE document_control SET edit_high_water=?", (value,))
        connection.execute("UPDATE jobs SET epoch=? WHERE job_id='counter_empty'", (value,))
        connection.execute("UPDATE job_control SET epoch_high_water=? WHERE job_id='counter_empty'", (value,))
        connection.execute("UPDATE control SET value=? WHERE key='worker_generation'", (str(value),))
    (root / "counter-fixture.json").write_text(json.dumps({
        "purpose": "authored_reference_browser_qualification",
        "whole_project_acceptance": False, "product_contract_sha256": CONTRACT,
        "fixture_authority": "public schema4/schema3 column contract; counters fixed before HTTP startup",
        "observation_protocol": COUNTER_FIXTURE_PROTOCOL,
        "comparison_window": "after explicit released diagnostics initialization; full rows/files retained",
        "counter": value, "source_names": ["alpha.txt", "beta.txt"],
        "job": {"job_id": "counter_empty", "state": "queued", "epoch": value, "total": 0},
        "high_waters_equal_live": True, "worker_generation": value,
        "expected_initial_generation": value,
    }, indent=2) + "\n")


def _durable_snapshot(root):
    def cell(value):
        return {"blob_hex": value.hex()} if isinstance(value, bytes) else value

    tables = {}
    with sqlite3.connect(f"file:{root / 'catalog.sqlite3'}?mode=ro", uri=True) as connection:
        for table in PUBLIC_TABLES:
            rows = [[cell(value) for value in row] for row in connection.execute('SELECT * FROM "' + table + '"')]
            tables[table] = sorted(rows, key=lambda value: _canonical(value))
    # Retain all candidate-created data-directory bytes, including ownership and
    # backup artifacts. Exclude only test-owned app/staging/browser/control dirs;
    # SQL snapshots above cover the database's logical durable state.
    files = {}
    for directory in sorted(root.iterdir()):
        if not directory.is_dir() or directory.name in {"app", "source-staging", "browser", "control"}:
            continue
        for path in sorted(directory.rglob("*")):
            if path.is_file():
                files[path.relative_to(root).as_posix()] = {"bytes": path.stat().st_size, "sha256": _sha(path.read_bytes())}
    return {"public_tables": tables, "data_files": files}


COUNTER_FIXTURE_PROTOCOL = "library-v2-browser-counter-fixture-v2"


def _prime_counter_observer(root, maximum):
    """Initialize read synchronization before the unchanged-file action window.

    This is an explicit authored-reference fixture profile, not a generic
    exclusion of lock files. The initial read may create exactly the declared
    inert synchronization file; all later no-op/exhausted transitions still
    compare every file and public row against the complete primed baseline.
    """
    initial = _durable_snapshot(root)
    (root / "durable-unprimed.json").write_text(json.dumps(initial, indent=2) + "\n")
    diagnostics = _cli(root, "counter-observer-initialization", "diagnostics")
    primed = _durable_snapshot(root)
    (root / "durable-primed.json").write_text(json.dumps(primed, indent=2) + "\n")
    coordination = "catalog.sqlite3.maintenance/worker-observe.lock"
    permitted = {"bytes": 0, "sha256": _sha(b"")}
    comparisons = {
        "all_public_tables_unchanged": initial["public_tables"] == primed["public_tables"],
        "coordination_file_initially_absent": coordination not in initial["data_files"],
        "exact_single_empty_coordination_file_added": primed["data_files"] == {
            **initial["data_files"], coordination: permitted},
    }
    value = COUNTER_MAX if maximum else HIGH
    comparisons["diagnostics_has_expected_unchanged_state"] = (
        type(diagnostics["generation"]) is int and diagnostics["generation"] == value
        and type(diagnostics["worker_generation"]) is int and diagnostics["worker_generation"] == value
        and diagnostics["worker_state"] == "stopped" and diagnostics["last_error"] is None)
    evidence = {"protocol": COUNTER_FIXTURE_PROTOCOL, "physically_executed": True,
                "purpose": "authored_reference_browser_qualification", "whole_project_acceptance": False,
                "command": "released CLI diagnostics; before browser/server action window",
                "initial_snapshot": "durable-unprimed.json", "primed_snapshot": "durable-primed.json",
                "initial_snapshot_sha256": _sha((root / "durable-unprimed.json").read_bytes()),
                "primed_snapshot_sha256": _sha((root / "durable-primed.json").read_bytes()),
                "permitted_initial_read_delta": {coordination: permitted}, "comparisons": comparisons,
                "additional_data_file_exclusions": [],
                "complementary_cold_exhaustion_control": "tests/test_library_v2_maintenance_reference_v1.py::"
                    "LibraryV2MaintenanceReferenceTests.test_worker_owner_exhaustion_precedes_liveness_publication",
                "limitation": "Synchronized-read initialization is outside the subsequent maximum-action window; "
                              "this does not permit file creation during an exhausted transition.",
                "passed": all(comparisons.values())}
    (root / "observer-initialization.json").write_text(json.dumps(evidence, indent=2) + "\n")
    if not evidence["passed"]:
        raise AssertionError("Released diagnostics initialization changed more than its declared inert coordination file")
    return primed


def _counter_persistence(root, receipt, maximum):
    initial = COUNTER_MAX if maximum else HIGH
    expected = {"generation": initial if maximum else initial + 3,
                "alpha_version": initial if maximum else initial + 2,
                "beta_version": initial, "notes": "" if maximum else "draft explicitly rebased",
                "epoch": initial, "job_state": "queued" if maximum else "completed"}
    observed = {"documents": _cli(root, "counter-final-documents", "documents-v1", "--deleted", "all"),
                "jobs": _cli(root, "counter-final-jobs", "jobs"),
                "diagnostics": _cli(root, "counter-final-diagnostics", "diagnostics"),
                "collections": _cli(root, "counter-final-collections", "collections")}
    documents = observed["documents"]
    if type(documents["generation"]) is not int or documents["generation"] != expected["generation"]:
        raise AssertionError("Fresh CLI exact final generation mismatch")
    records = {row["source"]: row for row in documents["records"]}
    if set(records) != {"alpha.txt", "beta.txt"} or documents["total"] != 2:
        raise AssertionError("Counter workflow changed the wrong document set")
    for name, version in (("alpha.txt", expected["alpha_version"]), ("beta.txt", expected["beta_version"])):
        if type(records[name]["edit_version"]) is not int or records[name]["edit_version"] != version or records[name]["deleted"] is not False:
            raise AssertionError("Exact document version/deletion mismatch")
    if records["alpha.txt"]["notes"] != expected["notes"] or records["beta.txt"]["notes"] != "":
        raise AssertionError("Stale/exhausted annotation escaped into durable state")
    jobs = observed["jobs"]["jobs"]
    if len(jobs) != 1 or jobs[0]["epoch"] != expected["epoch"] or type(jobs[0]["epoch"]) is not int or jobs[0]["state"] != expected["job_state"]:
        raise AssertionError("Legacy job exact epoch or transition mismatch")
    diagnostic = observed["diagnostics"]
    if diagnostic["generation"] != expected["generation"] or diagnostic["worker_generation"] != initial or diagnostic["worker_state"] != "stopped" or diagnostic["last_error"] is not None:
        raise AssertionError("Counter workflow unexpectedly changed diagnostics")
    with _server(root, 2) as port:
        reopened = {key: _raw_http(root, port, "counter-" + key, target)
                    for key, target in {"documents": "/api/v1/documents?deleted=all", "jobs": "/api/jobs",
                                        "diagnostics": "/api/maintenance/diagnostics",
                                        "collections": "/api/lifecycle/collections"}.items()}
    comparisons = {key: value["value"] == observed[key] for key, value in reopened.items()}
    if not all(comparisons.values()):
        raise AssertionError("Fresh HTTP process and CLI disagree on exact persisted counters")
    exported = json.loads((root / "browser/counter-export.json").read_bytes())
    if type(exported["generation"]) is not int or exported["generation"] != expected["generation"]:
        raise AssertionError("Downloaded export substituted a string/float/rounded generation")
    if receipt["counterExpectation"]["generation"] != str(expected["generation"]):
        raise AssertionError("Browser receipt declared an unexpected plan")
    result = {"passed": True, "physically_executed": True,
              "purpose": "authored_reference_browser_qualification", "whole_project_acceptance": False,
              "expected_from_public_fixture": expected, "fresh_cli_observations": observed,
              "fresh_http_process_observations": reopened, "comparisons": comparisons,
              "numeric_type_check": "Python json integer values AND independent browser raw-token checks",
              "maximum_fixture": maximum}
    (root / "counter-persistence-receipt.json").write_text(json.dumps(result, indent=2) + "\n")


class LibraryV2BrowserReadinessTests(unittest.TestCase):
    def test_private_partial_is_not_observed_before_atomic_publication(self):
        with ArtifactDirectory(type(self).__name__, retain_success=True) as artifacts:
            artifacts.output.mkdir()
            target = artifacts.output / "server-1-port.json"
            expected = {"port": 54321, "pid": 4242, "incarnation": 1}
            child = Mock(pid=4242)
            child.poll.return_value = None
            partial = threading.Event()
            release = threading.Event()
            observed, errors = [], []

            def delayed_dump(value, stream, **kwargs):
                encoded = json.dumps(value, **kwargs)
                stream.write(encoded[:1])
                stream.flush()
                partial.set()
                if not release.wait(2):
                    raise AssertionError("Forced partial publication control timed out")
                stream.write(encoded[1:])

            def publish():
                try:
                    _publish_port_record(target, expected)
                except Exception as error:
                    errors.append(error)

            def read():
                try:
                    observed.append(_wait_server_port(child, target, 1, timeout=2))
                except Exception as error:
                    errors.append(error)

            publisher = threading.Thread(target=publish)
            reader = threading.Thread(target=read)
            with patch.object(json, "dump", side_effect=delayed_dump):
                try:
                    publisher.start()
                    self.assertTrue(partial.wait(1))
                    self.assertEqual(target.with_name(target.name + ".partial").read_bytes(), b"{")
                    self.assertFalse(target.exists())
                    reader.start()
                    # Deliberately expose the formerly failing partial-write
                    # window; the reader must keep waiting for the final name.
                    time.sleep(0.03)
                    self.assertTrue(reader.is_alive())
                    self.assertEqual(observed, [])
                finally:
                    release.set()
                    publisher.join(timeout=2)
                    if reader.ident is not None:
                        reader.join(timeout=2)
            self.assertFalse(publisher.is_alive())
            self.assertFalse(reader.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(observed, [expected])
            self.assertFalse(target.with_name(target.name + ".partial").exists())
            self.assertEqual(json.loads(target.read_bytes()), expected)
            original = target.read_bytes()
            with self.assertRaises(AssertionError):
                _publish_port_record(target, expected)
            self.assertEqual(target.read_bytes(), original)

    def test_published_readiness_refuses_invalid_identity_payload_and_missing_owner(self):
        with ArtifactDirectory(type(self).__name__, retain_success=True) as artifacts:
            artifacts.output.mkdir()
            child = Mock(pid=4242)
            child.poll.return_value = None
            invalid = [b"", b"{" + b" " * 512, b"{", b"[]",
                       b'{"port":true,"pid":4242,"incarnation":1}',
                       b'{"port":65536,"pid":4242,"incarnation":1}',
                       b'{"port":54321,"pid":7,"incarnation":1}',
                       b'{"port":54321,"pid":4242,"incarnation":2}',
                       b'{"port":54321,"pid":4242,"incarnation":1,"extra":0}',
                       b'{"port":1,"port":54321,"pid":4242,"incarnation":1}']
            for index, raw in enumerate(invalid):
                with self.subTest(index=index):
                    target = artifacts.output / f"invalid-{index}.json"
                    target.write_bytes(raw)
                    with self.assertRaises(AssertionError):
                        _wait_server_port(child, target, 1, timeout=0.05)
            missing = artifacts.output / "never-published.json"
            with self.assertRaisesRegex(AssertionError, "timed out"):
                _wait_server_port(child, missing, 1, timeout=0.025)
            child.poll.return_value = 1
            with self.assertRaisesRegex(AssertionError, "exited"):
                _wait_server_port(child, missing, 1, timeout=0.025)


class LibraryV2BrowserIntegrationTests(unittest.TestCase):
    def test_exact_signed64_browser_tokens_stale_drafts_and_maximum_atomicity(self):
        with ArtifactDirectory(type(self).__name__, retain_success=True) as artifacts:
            artifacts.output.mkdir()
            receipts = []
            for maximum in (False, True):
                workflow = "counter-max" if maximum else "counter-high"
                for layout in ("desktop", "mobile"):
                    root = artifacts.output / (workflow + "-" + layout)
                    _counter_fixture(root, maximum)
                    before = _prime_counter_observer(root, maximum)
                    (root / "durable-before.json").write_text(json.dumps(before, indent=2) + "\n")
                    with _server(root, 1) as port:
                        receipt = _browser(root, port, workflow, layout)
                    _counter_persistence(root, receipt, maximum)
                    after = _durable_snapshot(root)
                    (root / "durable-after.json").write_text(json.dumps(after, indent=2) + "\n")
                    if maximum:
                        self.assertEqual(before, after, "Maximum no-ops/reads/exhaustion must preserve all public rows and data files")
                    receipts.append({"fixture": workflow, "layout": layout,
                                     "maximum_durable_state_unchanged": before == after if maximum else None,
                                     "observation_protocol": COUNTER_FIXTURE_PROTOCOL,
                                     "observer_initialization_sha256": _sha((root / "observer-initialization.json").read_bytes()),
                                     "source_bindings_sha256": _sha((root / "source-bindings.json").read_bytes()),
                                     "browser_receipt_sha256": _sha((root / "browser/receipt.json").read_bytes()),
                                     "persistence_receipt_sha256": _sha((root / "counter-persistence-receipt.json").read_bytes()),
                                     "passed": True})
            (artifacts.output / "qualification-receipt.json").write_text(json.dumps({
                "passed": True, "physically_executed": True, "purpose": "authored_reference_browser_qualification",
                "whole_project_acceptance": False, "release_builds": 4, "browser_launches": 4,
                "fresh_contexts": 8, "counter_values": [HIGH, COUNTER_MAX], "fixtures": receipts,
                "observation_protocol": COUNTER_FIXTURE_PROTOCOL, "additional_data_file_exclusions": [],
            }, indent=2) + "\n")

    def test_v2_released_lifecycle_recovery_dataset_and_reopened_backend(self):
        with ArtifactDirectory(type(self).__name__, retain_success=True) as artifacts:
            artifacts.output.mkdir()
            receipts = []
            for workflow in ("lifecycle", "maintenance", "dataset"):
                root = artifacts.output / workflow
                _fixture(root)
                _adopt(root)
                with _server(root, 1) as port:
                    receipt = _browser(root, port, workflow)
                _persistence(root, receipt, workflow)
                receipts.append({"fixture": workflow, "source_bindings_sha256": _sha((root / "source-bindings.json").read_bytes()),
                                 "browser_receipt_sha256": _sha((root / "browser/receipt.json").read_bytes()),
                                 "persistence_receipt_sha256": _sha((root / "persistence-receipt.json").read_bytes()),
                                 "passed": True})
            (artifacts.output / "qualification-receipt.json").write_text(json.dumps({
                "passed": True, "physically_executed": True, "purpose": "authored_reference_browser_qualification",
                "whole_project_acceptance": False, "release_builds": 3, "browser_launches": 3,
                "fresh_contexts": 6, "fixtures": receipts,
            }, indent=2) + "\n")


if __name__ == "__main__":
    unittest.main()
