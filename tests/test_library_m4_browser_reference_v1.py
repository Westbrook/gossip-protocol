"""Physical browser qualification of the authored, freshly built M4 release.

Every app process imports the output of the actual release command. These are
explicit M4 adaptations of M2/M3 regression workflows, not unchanged inherited
receipts and not arbitrary-candidate or independent study acceptance. Raw HTTP
bodies, downloads, CLI results, source identities and the release manifest remain
available after the owned processes have stopped. The host Python is recorded;
qualification of the pinned Python 3.12 image belongs to the Docker lane.
"""

import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.library_m4_reference_v1 import write_m4_project
from tests.test_library_m2_browser_reference_v1 import (
    MAX_LOG_BYTES,
    ROOT,
    _cli,
    _environment,
    _runtime,
    _server,
    _stop,
)

DRIVER = ROOT / "devtools/browser/library_m4_reference.cjs"
IMAGE = "sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f"
CONTRACT = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
RELEASE_BOOTSTRAP = r'''
import resource,runpy,sys
resource.setrlimit(resource.RLIMIT_CPU,(30,30))
resource.setrlimit(resource.RLIMIT_FSIZE,(33554432,33554432))
resource.setrlimit(resource.RLIMIT_NOFILE,(96,96))
sys.path.insert(0,sys.argv.pop(1))
runpy.run_module('library.clients.release',run_name='__main__')
'''


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _records(directory):
    return [{"path": path.relative_to(directory).as_posix(), "sha256": _sha(path.read_bytes()),
             "bytes": path.stat().st_size}
            for path in sorted(directory.rglob("*")) if path.is_file() and "__pycache__" not in path.parts]


def _fixture(root):
    root.mkdir()
    staging = root / "source-staging"
    write_m4_project(staging)
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
    }, indent=2) + "\n")


def _browser(root, port, workflow):
    node, modules = _runtime()
    environment = _environment(root)
    environment["NODE_PATH"] = str(modules)
    cache = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    environment["PLAYWRIGHT_BROWSERS_PATH"] = str(Path(cache).expanduser().resolve()) if cache else str(
        Path.home() / ("Library/Caches/ms-playwright" if sys.platform == "darwin" else ".cache/ms-playwright"))
    command = [str(node), str(DRIVER), "--origin", f"http://127.0.0.1:{port}",
               "--output", str(root / "browser"), "--worker-control", str(root / "control"),
               "--workflow", workflow]
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
        raise AssertionError(f"Inspect retained M4 browser receipt and logs in {root}")
    receipt_path = root / "browser/receipt.json"
    if receipt_path.stat().st_size > 2 * MAX_LOG_BYTES:
        raise AssertionError("Browser receipt exceeded observation limit")
    receipt = json.loads(receipt_path.read_bytes())
    if not receipt["passed"] or receipt["browserLaunches"] != 1 or receipt["freshContexts"] != 2:
        raise AssertionError(f"M4 browser qualification failed: {receipt_path}")
    if workflow == "maintenance" and worker is None:
        raise AssertionError("M4 did not invoke its real fixed CLI worker")
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


class LibraryM4BrowserReferenceTests(unittest.TestCase):
    def test_authored_release_browser_lifecycle_recovery_and_reopened_backend(self):
        with ArtifactDirectory(type(self).__name__, retain_success=True) as artifacts:
            artifacts.output.mkdir()
            receipts = []
            for workflow in ("lifecycle", "maintenance", "dataset"):
                root = artifacts.output / workflow
                _fixture(root)
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
