"""Fast, import-free syntax/configuration gate followed by parallel lint/types.

Run with the persistent development interpreter. A failed prerequisite starts
zero downstream tooling; no application or candidate module is ever imported.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, replace
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tomllib
from typing import Any


EXCLUDED_DIRECTORIES = frozenset({"runs", "results", ".venv", ".cache", "__pycache__", ".git", "node_modules", "build", "dist"})
REQUIRED_LINT_RULES = frozenset({"E9", "F63", "F7", "F82"})
# The continuation follow-up newly traverses these historical dependencies.
# Exact immutable identities permit recording their existing diagnostics without
# permitting new verification modules (or changed copies) to acquire type debt.
FROZEN_TYPE_BASELINE_EXCEPTIONS = {
    "analysis/qualify_benchmark.py": "481fb19a5dc555846ec339f9f501d7dd1c06c36bbbb64d4a8efd3033941855d9",
    "gossip_harness/continuation_transport.py": "de79df63641a14e947eba6896d4ffd52f450c47ec4993e61a49d5797d6cb8498",
    "gossip_harness/verification_buildgraph.py": "9a6f175c7af265221afa98051fb555abbafb08fa28e447fbf3245c63580dd717",
    "gossip_harness/verification_calendar.py": "6b9a39daf5256e1f3ea440847dc847b00af4b1169640d62a3f92b21d87e68c62",
    "gossip_harness/verification_experiment.py": "b10c646b60108fbc23b16de345034f9dccb635aa53cacc5c7c6ead07f99569bc",
    "gossip_harness/verification_integration.py": "bb0d33a085f27c0100874a6062d8d258438117ee4a1867200825625e762b7992",
    "gossip_harness/verification_journal.py": "0072f4b5a0f4dda68a58608e7a51f89d6bfa4a4213d7b760fbec36eec1b036b9",
    "gossip_harness/verification_stage.py": "21752f84506d4417fc07a7b7047ad2632b7afef0a98984a9009ad53043897d71",
}


@dataclass(frozen=True)
class TypeBaseline:
    path: str
    diagnostics: Counter[str]
    source_sha256: dict[str, str]
    sha256: str


@dataclass(frozen=True)
class Configuration:
    roots: tuple[str, ...]
    versions: dict[str, str]
    timeout_seconds: float
    javascript_syntax: str | None = None
    type_roots: tuple[str, ...] = ()
    type_baseline: TypeBaseline | None = None


@dataclass(frozen=True)
class Check:
    name: str
    status: str
    duration_seconds: float
    diagnostics: str = ""
    command: tuple[str, ...] = ()
    returncode: int | None = None
    baseline_count: int = 0
    baseline_path: str | None = None
    blocking_diagnostics: str | None = None


class ConfigurationError(ValueError):
    """An unusable or accidentally disabled static gate."""


def _diagnostic_key(diagnostic: dict[str, Any]) -> str:
    return json.dumps(diagnostic, sort_keys=True, separators=(",", ":"))


def _read_type_baseline(root: Path, path: str, version: str) -> TypeBaseline:
    root = root.resolve()
    location = root / path
    if not location.is_file() or not location.resolve().is_relative_to(root):
        raise ConfigurationError("Missing or external type baseline")
    document = json.loads(location.read_text())
    if not isinstance(document, dict) or document.get("schema_version") != 1 or document.get("mypy_version") != version:
        raise ConfigurationError("Type baseline schema/mypy version mismatch; explicit review required")
    if not isinstance(document.get("files"), list):
        raise ConfigurationError("Type baseline must list source files")
    diagnostics: Counter[str] = Counter()
    source_sha256: dict[str, str] = {}
    seen: set[str] = set()
    for entry in document["files"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str) or not isinstance(entry.get("diagnostics"), list):
            raise ConfigurationError("Malformed type baseline source")
        relative = Path(entry["path"])
        legacy_source = (
            relative.parts[:1] == ("gossip_harness",) and len(relative.parts) == 2
            and not relative.name.startswith(("verification_", "continuation_", "benchmark_"))
            and not relative.stem.endswith("_v2")
        ) or relative.as_posix() == "analysis/run_sustained_review_probes.py"
        frozen_identity = FROZEN_TYPE_BASELINE_EXCEPTIONS.get(relative.as_posix())
        frozen_source = frozen_identity is not None and entry.get("sha256") == frozen_identity
        if not legacy_source and not frozen_source:
            raise ConfigurationError("Type debt is restricted to unchanged legacy scientific files")
        if entry["path"] in seen:
            raise ConfigurationError("Duplicate type baseline source")
        seen.add(entry["path"])
        source = root / relative
        if not source.resolve().is_relative_to(root) or hashlib.sha256(source.read_bytes()).hexdigest() != entry["sha256"]:
            raise ConfigurationError(f"Type baseline source changed: {relative}; fix debt or explicitly review a new baseline")
        source_sha256[entry["path"]] = entry["sha256"]
        for diagnostic in entry["diagnostics"]:
            if not isinstance(diagnostic, dict) or diagnostic.get("file") != entry["path"] or diagnostic.get("severity") != "error":
                raise ConfigurationError("Type baseline diagnostics must bind to their exact source")
            diagnostics[_diagnostic_key(diagnostic)] += 1
    return TypeBaseline(path, diagnostics, source_sha256, hashlib.sha256(location.read_bytes()).hexdigest())


def _read_configuration(root: Path) -> Configuration:
    with (root / "pyproject.toml").open("rb") as handle:
        document = tomllib.load(handle)
    try:
        configuration = document["tool"]["devtools"]["static"]
        roots = configuration["roots"]
        timeout = configuration["timeout_seconds"]
        dependencies = document["dependency-groups"]["dev"]
        lint = document["tool"]["ruff"]["lint"]
        mypy = document["tool"]["mypy"]
    except (KeyError, TypeError) as error:
        raise ConfigurationError(f"Missing static gate configuration: {error}") from error
    if not isinstance(roots, list) or not roots or not all(isinstance(item, str) and item for item in roots):
        raise ConfigurationError("tool.devtools.static.roots must be a nonempty list of paths")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 600:
        raise ConfigurationError("timeout_seconds must be positive and at most 600")
    if not isinstance(dependencies, list):
        raise ConfigurationError("dependency-groups.dev must pin Ruff and mypy")
    versions: dict[str, str] = {}
    for dependency in dependencies:
        match = re.fullmatch(r"(ruff|mypy)==([0-9]+(?:\.[0-9]+)+)", str(dependency))
        if match:
            versions[match[1]] = match[2]
    if set(versions) != {"ruff", "mypy"}:
        raise ConfigurationError("Pin exactly one Ruff and one mypy version in dependency-groups.dev")
    if not isinstance(lint, dict) or not REQUIRED_LINT_RULES.issubset(set(lint.get("select", []))):
        raise ConfigurationError("Ruff correctness baseline must select E9, F63, F7 and F82")
    if any(lint.get(key) for key in ("ignore", "extend-ignore", "per-file-ignores", "extend-per-file-ignores")):
        raise ConfigurationError("Static correctness baseline must not be disabled by lint ignores")
    if not isinstance(mypy, dict) or not mypy.get("files") or mypy.get("check_untyped_defs") is not True:
        raise ConfigurationError("Mypy needs explicit files and check_untyped_defs=true")
    if mypy.get("ignore_errors") or mypy.get("ignore_missing_imports") or mypy.get("disable_error_code") or mypy.get("follow_imports") in {"skip", "silent"}:
        raise ConfigurationError("Mypy baseline must report errors rather than hide imports/errors")
    if mypy.get("incremental") is not True:
        raise ConfigurationError("Mypy incremental cache must remain enabled")
    for override in mypy.get("overrides", []):
        if override.get("ignore_errors") or override.get("ignore_missing_imports") or override.get("disable_error_code") or override.get("follow_imports") in {"skip", "silent"}:
            raise ConfigurationError("Mypy overrides must not hide errors/imports")
    type_roots = mypy["files"]
    if not isinstance(type_roots, list) or not all(isinstance(item, str) for item in type_roots):
        raise ConfigurationError("Mypy files must be an explicit list of authored paths")
    source_files(root, tuple(type_roots))
    javascript_syntax = configuration.get("javascript_syntax")
    if javascript_syntax is not None:
        if not isinstance(javascript_syntax, str):
            raise ConfigurationError("javascript_syntax must name a project script")
        script = root / javascript_syntax
        if not script.is_file() or not script.resolve().is_relative_to(root.resolve()):
            raise ConfigurationError("Missing or external JavaScript syntax checker")
    baseline_path = configuration.get("type_baseline")
    if baseline_path is not None and not isinstance(baseline_path, str):
        raise ConfigurationError("type_baseline must be a project path")
    baseline = _read_type_baseline(root, baseline_path, versions["mypy"]) if baseline_path else None
    return Configuration(tuple(roots), versions, float(timeout), javascript_syntax, tuple(type_roots), baseline)


def source_files(root: Path, roots: tuple[str, ...]) -> list[Path]:
    """Enumerate only declared authored paths and reject symlink escapes."""
    resolved_root = root.resolve()
    files: set[Path] = set()
    for entry in roots:
        relative = Path(entry)
        if relative.is_absolute() or ".." in relative.parts or relative == Path("."):
            raise ConfigurationError(f"Source root must be a project-relative authored path: {entry}")
        if any(part in EXCLUDED_DIRECTORIES for part in relative.parts):
            raise ConfigurationError(f"Generated/historical source root is forbidden: {entry}")
        path = root / relative
        if not path.exists():
            raise ConfigurationError(f"Missing source root: {entry}")
        if not path.resolve().is_relative_to(resolved_root):
            raise ConfigurationError(f"Source path escapes project: {entry}")
        candidates: list[Path] = []
        if path.is_file():
            candidates.append(path)
        else:
            for directory, subdirectories, names in os.walk(path, followlinks=False):
                subdirectories[:] = sorted(name for name in subdirectories if name not in EXCLUDED_DIRECTORIES)
                candidates.extend(Path(directory) / name for name in names if name.endswith(".py"))
        for candidate in candidates:
            if candidate.suffix != ".py" or any(part in EXCLUDED_DIRECTORIES for part in candidate.relative_to(root).parts):
                continue
            if not candidate.resolve().is_relative_to(resolved_root):
                raise ConfigurationError(f"Source path escapes project: {candidate}")
            files.add(candidate)
    if not files:
        raise ConfigurationError("No authored Python sources found")
    return sorted(files)


def check_syntax(root: Path, files: list[Path]) -> Check:
    started = time.monotonic()
    diagnostics: list[str] = []
    for path in files:
        try:
            # compile() also catches errors AST parsing alone permits, e.g.
            # a return outside a function. It neither imports nor writes pyc.
            compile(path.read_bytes(), str(path.relative_to(root)), "exec", dont_inherit=True)
        except (SyntaxError, ValueError, OSError) as error:
            diagnostics.append(f"{path.relative_to(root)}: {error}")
    return Check("syntax", "failed" if diagnostics else "passed", time.monotonic() - started,
                 "\n".join(diagnostics), returncode=1 if diagnostics else 0)


def _installed_versions(expected: dict[str, str]) -> tuple[Check, dict[str, str]]:
    started = time.monotonic()
    diagnostics: list[str] = []
    installed: dict[str, str] = {}
    for package, wanted in sorted(expected.items()):
        try:
            actual = metadata.version(package)
        except metadata.PackageNotFoundError:
            diagnostics.append(f"Missing {package}=={wanted}")
            continue
        installed[package] = actual
        if actual != wanted:
            diagnostics.append(f"Expected {package}=={wanted}; installed {actual}")
    if diagnostics:
        diagnostics.append("Run scripts/bootstrap-dev, then use .venv/bin/python.")
    return Check("tools", "error" if diagnostics else "passed", time.monotonic() - started,
                 "\n".join(diagnostics)), installed


def _run_tool(name: str, command: tuple[str, ...], root: Path, timeout: float) -> Check:
    started = time.monotonic()
    try:
        process = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=timeout,
                                 env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        status = "passed" if process.returncode == 0 else "failed"
        return Check(name, status, time.monotonic() - started, process.stdout + process.stderr,
                     command, process.returncode)
    except (OSError, subprocess.TimeoutExpired) as error:
        return Check(name, "error", time.monotonic() - started, str(error), command)


def _run_type_tool(command: tuple[str, ...], root: Path, timeout: float, baseline: TypeBaseline | None) -> Check:
    check = _run_tool("types", command, root, timeout)
    if baseline is None or check.status == "error":
        return check
    if check.returncode not in {0, 1}:
        return replace(check, status="error")
    try:
        emitted = [json.loads(line) for line in check.diagnostics.splitlines() if line.strip()]
        observed = Counter(_diagnostic_key(item) for item in emitted if item["severity"] == "error")
    except (ValueError, KeyError, TypeError) as error:
        return replace(check, status="error", diagnostics=f"Invalid mypy JSON: {error}\n{check.diagnostics}")
    if check.returncode == 1 and not observed:
        return replace(check, status="error", diagnostics="Mypy failed without a structured error\n" + check.diagnostics)
    matched = sum((observed & baseline.diagnostics).values())
    unexpected = observed - baseline.diagnostics
    summary = f"{matched} exact legacy diagnostics accepted from unchanged source; baseline: {baseline.path}\n"
    blocking = "\n".join(
        f"{item['file']}:{item['line']}: {item['message']} [{item['code']}]"
        for key in unexpected.elements() for item in [json.loads(key)]
    )
    return replace(check, status="failed" if unexpected else "passed", baseline_count=matched,
                   baseline_path=baseline.path, diagnostics=summary + check.diagnostics, blocking_diagnostics=blocking)


def run_checks(root: Path) -> dict[str, Any]:
    from devtools.telemetry import ProcessTelemetry
    resources = ProcessTelemetry("static gate controller; child CPU includes checker processes")
    root = root.resolve()
    started = time.monotonic()
    checks: list[Check] = []
    versions: dict[str, str] = {}
    files: list[Path] = []
    try:
        configuration = _read_configuration(root)
        files = source_files(root, configuration.roots)
        checks.append(Check("configuration", "passed", time.monotonic() - started))
    except (OSError, ValueError, TypeError, KeyError) as error:
        checks.append(Check("configuration", "error", time.monotonic() - started, str(error)))
        configuration = None
    if configuration:
        checks.append(check_syntax(root, files))
        if checks[-1].status == "passed" and configuration.javascript_syntax:
            checks.append(_run_tool("javascript_syntax", ("node", configuration.javascript_syntax),
                                    root, configuration.timeout_seconds))
        if checks[-1].status == "passed":
            tool_check, versions = _installed_versions(configuration.versions)
            checks.append(tool_check)
            if tool_check.status == "passed":
                lint_command = (sys.executable, "-m", "ruff", "check", "--config", "pyproject.toml",
                                *(str(path.relative_to(root)) for path in files))
                type_command = (
                    sys.executable, "-m", "mypy", "--config-file", "pyproject.toml", "--output", "json",
                    *(str(path.relative_to(root)) for path in source_files(root, configuration.type_roots)),
                )
                with ThreadPoolExecutor(max_workers=2, thread_name_prefix="static") as pool:
                    futures = [
                        pool.submit(_run_tool, "lint", lint_command, root, configuration.timeout_seconds),
                        pool.submit(_run_type_tool, type_command, root, configuration.timeout_seconds, configuration.type_baseline),
                    ]
                    checks.extend(future.result() for future in futures)
    status = "error" if any(item.status == "error" for item in checks) else (
        "failed" if any(item.status == "failed" for item in checks) else "passed")
    return {
        "schema_version": 1,
        "resources": resources.snapshot(),
        "status": status,
        "duration_seconds": time.monotonic() - started,
        "source_count": len(files),
        "type_roots": list(configuration.type_roots) if configuration else [],
        "type_baseline": {
            "path": configuration.type_baseline.path if configuration and configuration.type_baseline else None,
            "recorded_debt": sum(configuration.type_baseline.diagnostics.values()) if configuration and configuration.type_baseline else 0,
            "active_debt": sum(item.baseline_count for item in checks),
            "baseline_sha256": configuration.type_baseline.sha256 if configuration and configuration.type_baseline else None,
            "source_sha256": configuration.type_baseline.source_sha256 if configuration and configuration.type_baseline else {},
            "scope": "Explicit roots and their imports; all other authored files receive syntax/correctness lint checks",
        },
        "python": sys.version,
        "executable": sys.executable,
        "tool_versions": versions,
        "checks": [asdict(check) for check in checks],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Write the complete JSON check receipt")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    arguments = parser.parse_args()
    receipt = run_checks(arguments.root)
    output = json.dumps(receipt, indent=2) + "\n"
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = arguments.output.with_suffix(arguments.output.suffix + f".{os.getpid()}.tmp")
        temporary.write_text(output)
        temporary.replace(arguments.output)
    for check in receipt["checks"]:
        print(f"{check['name']}: {check['status']} ({check['duration_seconds']:.3f}s)")
        if check["baseline_count"]:
            print(f"  Existing unchanged type debt: {check['baseline_count']} exact diagnostics ({check['baseline_path']})")
        if check["status"] != "passed":
            print(check["blocking_diagnostics"] or check["diagnostics"], file=sys.stderr)
    return int({"passed": 0, "failed": 1, "error": 2}[receipt["status"]])


if __name__ == "__main__":
    raise SystemExit(main())
