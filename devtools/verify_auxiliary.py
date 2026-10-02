"""Read-only adapters for explicitly selected independent report/browser lanes.

Inventory parses test declarations without importing the report. Only the
isolated worker may import those tests. Source fingerprints deliberately omit
canonical user feedback, review checkpoints, and mutable progress state.
"""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


AUXILIARY_LANES = frozenset({"report", "browser"})
BROWSER_PAGES = ("report", "research", "investigation", "pilot", "experiments", "swarm", "swarm-pilot", "sustained-pilot", "verification-pilot", "continuation-comparison", "benchmark-comparison", "investigation-roadmap")
BROWSER_FILES = ("run.cjs", "lifecycle.cjs", "lifecycle.test.cjs", "check_syntax.cjs", "fixture_server.py", "package.json", "package-lock.json")


def _selected(lanes: list[str] | None) -> set[str]:
    return set(lanes or []) & AUXILIARY_LANES


def _locator(root: Path) -> tuple[Path, dict[str, Any]]:
    source = root / ".progress-report" / "project.json"
    try:
        document = json.loads(source.read_text())
    except (OSError, ValueError) as error:
        raise ValueError(f"Selected report/browser lane needs a readable locator: {source}: {error}") from error
    if not isinstance(document, dict) or document.get("schemaVersion") != 1:
        raise ValueError("Invalid progress report locator schema")
    for key in ("projectId", "reportWorkspace", "stateLocation", "reportUrl"):
        if not isinstance(document.get(key), str) or not document[key]:
            raise ValueError(f"Progress report locator needs {key}")
    workspace = Path(document["reportWorkspace"])
    workspace = (workspace if workspace.is_absolute() else root / workspace).resolve()
    if not workspace.is_dir():
        raise ValueError(f"Selected report/browser lane cannot find report workspace: {workspace}")
    parsed = urlsplit(document["reportUrl"])
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("Progress report URL must identify the server root")
    _required_file(workspace, "report.py")
    _required_file(workspace, "test_report.py")
    _required_file(workspace, "index.html")
    return workspace, document


def _required_file(directory: Path, relative: str) -> Path:
    path = directory / relative
    if not path.is_file() or not path.resolve().is_relative_to(directory.resolve()):
        raise ValueError(f"Selected auxiliary lane needs an existing source inside its workspace: {path}")
    return path


def _report_inventory(workspace: Path) -> list[dict[str, Any]]:
    path = _required_file(workspace, "test_report.py")
    module = ast.parse(path.read_bytes(), filename=str(path))
    if any(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "load_tests" for node in module.body):
        raise ValueError("Report load_tests hook requires an explicit inventory adapter")
    tests: list[dict[str, Any]] = []
    seen: set[str] = set()
    classes = {node.name for node in module.body if isinstance(node, ast.ClassDef)
               and any(ast.unparse(base) in {"unittest.TestCase", "TestCase"} for base in node.bases)}
    for node in module.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if any(ast.unparse(base) in classes for base in node.bases):
            raise ValueError(f"Inherited report test class requires an explicit inventory: {node.name}")
        if any(isinstance(statement, (ast.Assign, ast.AnnAssign)) and any(
                isinstance(child, ast.Name) and child.id.startswith("test") for child in ast.walk(statement))
                for statement in node.body):
            raise ValueError(f"Dynamic report test assignment requires an explicit inventory: {node.name}")
        methods = [item for item in node.body if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                   and item.name.startswith("test")]
        if not methods:
            continue
        if node.name not in classes:
            raise ValueError(f"Report tests must directly inherit unittest.TestCase: {node.name}")
        for method in sorted(methods, key=lambda item: item.name):
            if isinstance(method, ast.AsyncFunctionDef):
                raise ValueError("Async report tests require an explicit inventory adapter")
            key = f"report/test_report.py::{node.name}"
            identifier = f"{key}.{method.name}"
            if identifier in seen:
                raise ValueError(f"Duplicate report test method: {identifier}")
            seen.add(identifier)
            tests.append({
                "id": identifier, "class": key, "path": "test_report.py", "name": node.name,
                "method": method.name, "lane": "report", "weight": 8, "timeout_seconds": 180,
                "root": str(workspace), "module": "test_report", "reuse": False,
                # Reserve the peak eight concurrent writers for this class.
                # The controller must account for intentional inner parallelism.
                "nested_processes": 8 if method.name == "test_concurrent_writers_preserve_all_events" else 0,
            })
    if not tests:
        raise ValueError("Selected report lane has no discovered test methods")
    return tests


def inventory(root: Path, lanes: list[str]) -> list[dict[str, Any]]:
    """Return selected auxiliary tests; offline calls never require a locator."""
    selected = _selected(lanes)
    if not selected:
        return []
    root = root.resolve()
    workspace, _ = _locator(root)
    tests = _report_inventory(workspace) if "report" in selected else []
    if "browser" in selected:
        directory = root / "devtools" / "browser"
        for name in BROWSER_FILES:
            _required_file(directory, name)
        tests.append({
            "id": "browser/report-contracts::BrowserBatch.test_all_pages",
            "class": "browser/report-contracts::BrowserBatch", "path": "devtools/browser/run.cjs",
            "name": "BrowserBatch", "method": "test_all_pages", "lane": "browser", "weight": 1,
            "timeout_seconds": 240, "reuse": False, "pages": list(BROWSER_PAGES),
            "command": ["node", str(directory / "run.cjs"), "--pages", "all"],
            "receipt_kind": "browser", "server_health": "validated by browser worker before browser launch",
        })
    return tests


def fingerprint(root: Path, lanes: list[str] | None = None) -> dict[str, str]:
    """Bind selected auxiliary source bytes, never mutable canonical user data."""
    selected = _selected(lanes)
    if not selected:
        return {}
    root = root.resolve()
    workspace, _ = _locator(root)
    sources = {"auxiliary:locator": root / ".progress-report" / "project.json"}
    for name in ("report.py", "test_report.py", "index.html"):
        sources[f"report:{name}"] = _required_file(workspace, name)
    if "browser" in selected:
        for name in BROWSER_FILES:
            sources[f"browser:{name}"] = _required_file(root / "devtools" / "browser", name)
        for path in sorted(root.glob("*.html")):
            sources[f"page:{path.name}"] = _required_file(root, path.name)
    hashes = {label: hashlib.sha256(path.read_bytes()).hexdigest() for label, path in sorted(sources.items())}
    hashes["auxiliary:inventory"] = hashlib.sha256(json.dumps(inventory(root, sorted(selected)), sort_keys=True).encode()).hexdigest()
    return hashes


def validate_browser_receipt(path: Path) -> dict[str, Any]:
    """Require a complete physical browser observation before reporting green."""
    try:
        receipt = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise ValueError(f"Missing or unreadable browser execution receipt: {path}: {error}") from error
    if not isinstance(receipt, dict) or receipt.get("schemaVersion") != 1 or receipt.get("passed") is not True:
        raise ValueError("Browser receipt does not establish a successful execution")
    if receipt.get("physicalExecution") is not True or type(receipt.get("browserLaunches")) is not int or receipt.get("browserLaunches") != 1:
        raise ValueError("Browser receipt requires exactly one fresh browser launch")
    if receipt.get("playwright") != "1.62.1" or receipt.get("chromiumRevision") != "1234" or not isinstance(receipt.get("browserVersion"), str) or not receipt["browserVersion"]:
        raise ValueError("Browser receipt does not match the pinned Playwright/Chromium runtime")
    if receipt.get("pages") != list(BROWSER_PAGES):
        raise ValueError("Browser receipt does not cover every declared page")
    results = receipt.get("results")
    if not isinstance(results, list) or not all(isinstance(result, dict) for result in results) or [result.get("page") for result in results] != list(BROWSER_PAGES):
        raise ValueError("Browser receipt has missing, duplicate, or reordered page results")
    if any(result.get("passed") is not True for result in results):
        raise ValueError("Browser receipt contains unsuccessful page checks")
    if receipt.get("canonicalReviewPreserved") is not True or receipt.get("borrowedServerStopped") is not False:
        raise ValueError("Browser receipt does not establish canonical review/server preservation")
    if receipt.get("cleanup") != {"browser": "complete", "api": "complete", "ownedServer": "complete"}:
        raise ValueError("Browser receipt does not establish complete resource cleanup")
    if any(not isinstance(receipt.get(key), dict) or not receipt[key] for key in ("sourceBindings", "verificationBindings", "serverIdentity")):
        raise ValueError("Browser receipt lacks source/tool/server bindings")
    return receipt
