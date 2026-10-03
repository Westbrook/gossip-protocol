"""Physical UI qualification of fixed authored M3 source, never arbitrary agents.

The M2 driver is intentionally unchanged and runs on its own fresh M3 fixture.
The M3 driver observes actual bounded browser downloads and maintenance controls.
Every server, CLI and Chromium child belongs to this test; retained evidence
includes all observed CLI and HTTP values, not merely a post-reopen pass label.
"""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.library_m3_reference_v1 import m3_files
from tests.test_library_m2_browser_reference_v1 import (
    DRIVER as M2_DRIVER,
    MAX_LOG_BYTES,
    ROOT,
    _cli,
    _environment,
    _http,
    _runtime,
    _server,
    _stop,
)

DRIVER = ROOT / "devtools/browser/library_m3_reference.cjs"


def _fixture(root):
    root.mkdir()
    source = m3_files()
    for name, contents in source.items():
        target = root / "app" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(contents, encoding="utf-8")
    (root / "input").mkdir()
    (root / "backups").mkdir()
    (root / "control").mkdir()
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
    (root / "source-bindings.json").write_text(json.dumps({
        "purpose": "authored_reference_browser_qualification",
        "whole_project_acceptance": False,
        "python": sys.version,
        "sources": {name: hashlib.sha256(contents.encode()).hexdigest()
                    for name, contents in sorted(source.items())},
        "drivers": {str(driver.relative_to(ROOT)): hashlib.sha256(driver.read_bytes()).hexdigest()
                    for driver in (M2_DRIVER, DRIVER)},
    }, indent=2) + "\n")


def _browser(root, port, *, inherited):
    node, modules = _runtime()
    environment = _environment(root)
    environment["NODE_PATH"] = str(modules)
    cache = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    environment["PLAYWRIGHT_BROWSERS_PATH"] = str(Path(cache).expanduser().resolve()) if cache else str(
        Path.home() / ("Library/Caches/ms-playwright" if sys.platform == "darwin" else ".cache/ms-playwright"))
    command = [str(node), str(M2_DRIVER if inherited else DRIVER), "--origin", f"http://127.0.0.1:{port}",
               "--output", str(root / "browser")]
    if not inherited:
        command.extend(["--worker-control", str(root / "control")])
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
                if not inherited and worker is None and request.exists():
                    if request.stat().st_size > 256 or json.loads(request.read_bytes()) != {"job_id": "m3_browser_job"}:
                        raise AssertionError("Unexpected worker command request")
                    # This is the single fixed process command authorized by this
                    # authored test. Driver data cannot choose executable/args.
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
        raise AssertionError(f"Inspect retained browser receipt and logs in {root}")
    receipt_path = root / "browser/receipt.json"
    if receipt_path.stat().st_size > 2 * MAX_LOG_BYTES:
        raise AssertionError("Browser receipt exceeded observation limit")
    receipt = json.loads(receipt_path.read_bytes())
    if not receipt["passed"] or receipt["browserLaunches"] != 1 or receipt["freshContexts"] != 2:
        raise AssertionError(f"Browser qualification failed: {receipt_path}")
    if not inherited and worker is None:
        raise AssertionError("M3 did not invoke its real fixed CLI worker")
    return receipt


def _persistence(root, receipt, *, inherited):
    expected = receipt["finalState"]
    commands = {
        "documents": ("documents", "--deleted", "all"),
        "collections": ("collections",),
        "jobs": ("jobs",),
        "history": ("revisions", expected["alphaID"]),
    }
    targets = {
        "documents": "/api/lifecycle/documents?deleted=all",
        "collections": "/api/lifecycle/collections",
        "jobs": "/api/jobs",
        "history": "/api/lifecycle/documents/" + expected["alphaID"] + "/revisions",
    }
    if not inherited:
        commands.update({"backups": ("backups",), "diagnostics": ("diagnostics",)})
        targets.update({"backups": "/api/maintenance/backups", "diagnostics": "/api/maintenance/diagnostics"})
    observed = {key: _cli(root, index, *arguments) for index, (key, arguments) in enumerate(commands.items(), 1)}
    with _server(root, 2) as port:
        reopened = {key: _http(port, target) for key, target in targets.items()}
    comparisons = {key: {"cli_matches_browser": observed[key] == expected[key],
                         "http_matches_cli": reopened[key] == observed[key]} for key in commands}
    # Retain actual reopened HTTP values even if a comparison fails. This is
    # stronger than the old M2 receipt's label alone and preserves diagnosis.
    result = {
        "passed": all(all(values.values()) for values in comparisons.values()),
        "physically_executed": True,
        "purpose": "authored_reference_browser_qualification",
        "whole_project_acceptance": False,
        "browser_final_state": {key: expected[key] for key in commands},
        "fresh_cli_observations": observed,
        "fresh_http_process_observations": reopened,
        "comparisons": comparisons,
        "checks": ["fresh_cli_shared_sqlite", "fresh_http_process_reopen", "all_owned_processes_stopped"],
    }
    (root / "persistence-receipt.json").write_text(json.dumps(result, indent=2) + "\n")
    if not result["passed"]:
        raise AssertionError(f"Persistence mismatch: {root / 'persistence-receipt.json'}")


class LibraryM3BrowserReferenceTests(unittest.TestCase):
    def test_authored_browser_maintenance_and_reopened_shared_backend(self):
        with ArtifactDirectory(type(self).__name__, retain_success=True) as artifacts:
            artifacts.output.mkdir()
            receipts = []
            for name, inherited in (("inherited-m2", True), ("m3-maintenance", False)):
                root = artifacts.output / name
                _fixture(root)
                with _server(root, 1) as port:
                    receipt = _browser(root, port, inherited=inherited)
                _persistence(root, receipt, inherited=inherited)
                receipts.append({"fixture": name, "browser_receipt_sha256": hashlib.sha256(
                    (root / "browser/receipt.json").read_bytes()).hexdigest(),
                    "persistence_receipt_sha256": hashlib.sha256((root / "persistence-receipt.json").read_bytes()).hexdigest(),
                    "passed": True})
            (artifacts.output / "qualification-receipt.json").write_text(json.dumps({
                "passed": True, "physically_executed": True,
                "purpose": "authored_reference_browser_qualification", "whole_project_acceptance": False,
                "browser_launches": 2, "fresh_contexts": 4, "fixtures": receipts,
            }, indent=2) + "\n")


if __name__ == "__main__":
    unittest.main()
