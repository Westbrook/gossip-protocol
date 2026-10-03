"""Physical Chromium checks of the trusted authored M2 app, not candidate evidence.

Run only in the explicit browser lane after the combined offline gate. Every
application process executes the repository's authored m2_files(), with a new
SQLite database and owned loopback socket. This is deliberately not an adapter
that accepts arbitrary candidate source, nor independent study acceptance.
"""

from contextlib import contextmanager
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import unittest

from devtools.test_artifacts import ArtifactDirectory
from gossip_harness.library_m2_reference_v1 import m2_files

ROOT = Path(__file__).resolve().parents[1]
BUNDLED_NODE = Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node"
DRIVER = ROOT / "devtools/browser/library_m2_reference.cjs"
MAX_LOG_BYTES = 262144

# This fixed trusted bootstrap applies limits before importing authored modules.
# Process stdout/stderr are file-backed and RLIMIT_FSIZE bounded, not PIPEs that
# could deadlock. The parent only terminates this exact Popen child/process group.
BOOTSTRAP = r'''
import json,os,resource,signal,sys
from pathlib import Path
resource.setrlimit(resource.RLIMIT_CPU,(60,60))
resource.setrlimit(resource.RLIMIT_FSIZE,(33554432,33554432))
resource.setrlimit(resource.RLIMIT_NOFILE,(128,128))
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
        Path(sys.argv[2]).write_text(json.dumps({'port':server.server_port}))
        server.serve_forever(poll_interval=0.05)
finally:
    store.close()
'''
CLI_BOOTSTRAP = r'''
import resource,runpy,sys
resource.setrlimit(resource.RLIMIT_CPU,(10,10))
resource.setrlimit(resource.RLIMIT_FSIZE,(33554432,33554432))
resource.setrlimit(resource.RLIMIT_NOFILE,(96,96))
sys.path.insert(0,sys.argv.pop(1))
runpy.run_module('library',run_name='__main__')
'''


def _environment(root):
    """No inherited credentials, PYTHONPATH, NODE_OPTIONS or application state."""
    return {"PATH": os.defpath, "HOME": str(root), "TMPDIR": str(root),
            "LANG": "en_US.UTF-8", "LC_ALL": "en_US.UTF-8"}


def _runtime():
    selected = os.environ.get("GOSSIP_BROWSER_NODE")
    node = Path(selected).expanduser() if selected else BUNDLED_NODE / "bin/node"
    if not node.is_file() and not selected:
        executable = shutil.which("node")
        node = Path(executable) if executable else node
    modules_value = os.environ.get("GOSSIP_BROWSER_NODE_MODULES")
    if modules_value:
        modules = Path(modules_value).expanduser()
    elif (ROOT / "devtools/browser/node_modules/playwright").is_dir():
        modules = ROOT / "devtools/browser/node_modules"
    else:
        modules = BUNDLED_NODE / "node_modules"
    if not node.is_absolute() or not node.is_file():
        raise AssertionError("Pinned browser Node is unavailable; set GOSSIP_BROWSER_NODE")
    if not modules.is_absolute() or not (modules / "playwright").is_dir():
        raise AssertionError("Pinned Playwright is unavailable; set GOSSIP_BROWSER_NODE_MODULES")
    return node, modules


def _stop(child):
    if child.poll() is not None:
        return
    os.killpg(child.pid, signal.SIGTERM)
    try:
        child.wait(timeout=3)
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait(timeout=3)


@contextmanager
def _server(root, incarnation):
    port_file = root / f"server-{incarnation}-port.json"
    log = root / f"server-{incarnation}.log"
    with log.open("xb") as output:
        child = subprocess.Popen([sys.executable, "-I", "-c", BOOTSTRAP, str(root), str(port_file)],
                                 cwd=root, stdin=subprocess.DEVNULL, stdout=output,
                                 stderr=output, env=_environment(root), start_new_session=True)
        try:
            deadline = time.monotonic() + 10
            while not port_file.exists():
                if child.poll() is not None:
                    raise AssertionError(f"Authored server exited during startup; inspect {log}")
                if time.monotonic() >= deadline:
                    raise AssertionError(f"Authored server startup timed out; inspect {log}")
                time.sleep(0.025)
            value = json.loads(port_file.read_bytes())
            port = value["port"]
            if type(port) is not int or not 1 <= port <= 65535:
                raise AssertionError("Invalid owned loopback port")
            yield port
        finally:
            _stop(child)
            if log.stat().st_size > MAX_LOG_BYTES:
                raise AssertionError(f"Server log exceeded observation bound: {log}")


def _http(port, target):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    try:
        connection.request("GET", target)
        response = connection.getresponse()
        raw = response.read(MAX_LOG_BYTES + 1)
        if response.status != 200 or len(raw) > MAX_LOG_BYTES:
            raise AssertionError("Bounded reopen observation failed")
        return json.loads(raw)
    finally:
        connection.close()


def _cli(root, index, *arguments):
    stdout = root / f"cli-{index}.json"
    stderr = root / f"cli-{index}.stderr"
    with stdout.open("xb") as out, stderr.open("xb") as err:
        result = subprocess.run([sys.executable, "-I", "-c", CLI_BOOTSTRAP, str(root / "app"),
                                 "--db", str(root / "catalog.sqlite3"),
                                 "--root", str(root / "input"), *arguments],
                                cwd=root, stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                env=_environment(root), timeout=15, check=False)
    if result.returncode != 0 or stderr.stat().st_size or stdout.stat().st_size > MAX_LOG_BYTES:
        raise AssertionError(f"CLI persistence observation failed: {stdout}, {stderr}")
    return json.loads(stdout.read_bytes())


class LibraryM2BrowserReferenceTests(unittest.TestCase):
    def test_authored_browser_lifecycle_and_reopened_shared_backend(self):
        with ArtifactDirectory(type(self).__name__, retain_success=True) as artifacts:
            root = artifacts.output
            root.mkdir()
            source = m2_files()
            for name, contents in source.items():
                target = root / "app" / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(contents, encoding="utf-8")
            inputs = root / "input"
            inputs.mkdir()
            for name, text in {
                "alpha.txt": '<img src=x onerror="window.__m2Injected=true"> alpha original',
                "beta.txt": "beta original",
                "gamma.txt": "gamma original",
                "refresh.txt": '<svg onload="window.__m2Injected=true"> alpha path refresh',
            }.items():
                (inputs / name).write_text(text, encoding="utf-8")
            # JSON intake uses the frozen closed entries-envelope shape.
            (inputs / "bundle.json").write_text(json.dumps({"entries": [
                {"source": "jobs/one.txt", "text": "job original"}]}), encoding="utf-8")
            bindings = {name: hashlib.sha256(contents.encode()).hexdigest()
                        for name, contents in sorted(source.items())}
            (root / "source-bindings.json").write_text(json.dumps({
                "purpose": "authored_reference_browser_qualification",
                "whole_project_acceptance": False,
                "python": sys.version,
                "sources": bindings,
                "driver_sha256": hashlib.sha256(DRIVER.read_bytes()).hexdigest(),
            }, indent=2) + "\n")
            node, modules = _runtime()
            environment = _environment(root)
            environment["NODE_PATH"] = str(modules)
            # Browser discovery needs the installed cache, not the isolated app HOME.
            cache = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
            if cache:
                environment["PLAYWRIGHT_BROWSERS_PATH"] = str(Path(cache).expanduser().resolve())
            else:
                environment["PLAYWRIGHT_BROWSERS_PATH"] = str(
                    Path.home() / ("Library/Caches/ms-playwright" if sys.platform == "darwin" else ".cache/ms-playwright"))
            with _server(root, 1) as port:
                with (root / "driver.stdout").open("xb") as out, (root / "driver.stderr").open("xb") as err:
                    child = subprocess.Popen([str(node), str(DRIVER), "--origin", f"http://127.0.0.1:{port}",
                                              "--output", str(root / "browser")],
                                             cwd=ROOT, stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                             env=environment, start_new_session=True)
                    try:
                        result = child.wait(timeout=180)
                    finally:
                        _stop(child)
                self.assertLessEqual((root / "driver.stdout").stat().st_size, MAX_LOG_BYTES)
                self.assertLessEqual((root / "driver.stderr").stat().st_size, MAX_LOG_BYTES)
                self.assertEqual(result, 0, f"Inspect retained browser receipt and driver logs in {root}")
                receipt = json.loads((root / "browser/receipt.json").read_bytes())
                self.assertTrue(receipt["passed"], receipt)
                self.assertEqual(receipt["browserLaunches"], 1)
                self.assertEqual(receipt["freshContexts"], 2)
            expected = receipt["finalState"]
            observed = {
                "documents": _cli(root, 1, "documents", "--deleted", "all"),
                "collections": _cli(root, 2, "collections"),
                "jobs": _cli(root, 3, "jobs"),
                "history": _cli(root, 4, "revisions", expected["alphaID"]),
            }
            self.assertEqual(observed, {key: expected[key] for key in observed})
            with _server(root, 2) as port:
                reopened = {
                    "documents": _http(port, "/api/lifecycle/documents?deleted=all"),
                    "collections": _http(port, "/api/lifecycle/collections"),
                    "jobs": _http(port, "/api/jobs"),
                    "history": _http(port, "/api/lifecycle/documents/" + expected["alphaID"] + "/revisions"),
                }
            self.assertEqual(reopened, observed)
            (root / "persistence-receipt.json").write_text(json.dumps({
                "passed": True, "physically_executed": True,
                "purpose": "authored_reference_browser_qualification",
                "whole_project_acceptance": False,
                "observations": observed,
                "checks": ["fresh_cli_shared_sqlite", "fresh_http_process_reopen", "all_owned_processes_stopped"],
            }, indent=2) + "\n")


if __name__ == "__main__":
    unittest.main()
