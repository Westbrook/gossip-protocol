"""Run a known historical audit with an independently pinned complete runtime.

The runtime is trusted executable code supplied by the caller, not by a study
manifest. The caller must pin its attestation digest separately from the archive.
Candidate sources and archive metadata remain data. This never runs a study,
candidate, provider, Docker job, or an archive-selected entrypoint.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from typing import Any, Sequence

from retain_experiments import InputSnapshot


PROTOCOL = "historical-audit-runtime-v1"
AUDITORS = {"sustained": "gossip_harness.sustained_audit",
            "verification": "gossip_harness.verification_audit"}
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_NAME = re.compile(r"[A-Za-z_][A-Za-z_0-9]*\.py\Z")
_BOOTSTRAP = ("import runpy,sys; sys.path.insert(0,sys.argv.pop(1)); "
              "runpy.run_module(sys.argv.pop(1),run_name='__main__')")


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=True,
                      allow_nan=False, separators=(",", ":")).encode()


def _decode(data: bytes) -> Any:
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("Duplicate JSON member")
            value[key] = item
        return value
    def constant(value):
        raise ValueError("Nonfinite JSON value: " + value)
    return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)


def _identity() -> dict[str, str]:
    executable = Path(sys.executable).resolve()
    git = shutil.which("git")
    if git is None:
        raise ValueError("Historical audit requires existing Git")
    git_path = Path(git).resolve()
    return {"python_sha256": _hash(executable.read_bytes()),
            "python_version": sys.version, "git_path": str(git_path),
            "git_sha256": _hash(git_path.read_bytes())}


def _sources(runtime: Path, snapshot: InputSnapshot) -> dict[str, bytes]:
    package = runtime / "gossip_harness"
    if package.is_symlink() or not package.is_dir():
        raise ValueError("Runtime needs a real gossip_harness package")
    result = {}
    for path in sorted(package.glob("*.py")):
        if _NAME.fullmatch(path.name) is None:
            raise ValueError("Invalid runtime module name")
        result[f"gossip_harness/{path.name}"] = snapshot.read(path, runtime)
    if "gossip_harness/__init__.py" not in result:
        raise ValueError("Runtime is incomplete: missing package initializer")
    return result


def _complete(files: dict[str, bytes], module: str) -> list[str]:
    """Verify the audit's local import closure without importing runtime code."""
    pending = [module, "gossip_harness"]
    seen: set[str] = set()
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        relative = name.replace(".", "/") + ("/__init__.py" if name == "gossip_harness" else ".py")
        if relative not in files:
            raise ValueError("Runtime is incomplete: " + relative)
        parsed = ast.parse(files[relative], filename=relative)
        dynamic_names = {"__import__"}
        for node in ast.walk(parsed):
            if isinstance(node, ast.ImportFrom) and node.module == "importlib":
                dynamic_names.update(item.asname or item.name for item in node.names
                                     if item.name == "import_module")
        for node in ast.walk(parsed):
            if isinstance(node, ast.Call) and (
                    isinstance(node.func, ast.Name) and node.func.id in dynamic_names
                    or isinstance(node.func, ast.Attribute) and node.func.attr == "import_module"):
                raise ValueError("Dynamic audit imports require a new explicit runtime contract")
            imported = []
            if isinstance(node, ast.Import):
                imported = [item.name for item in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level > 1:
                    raise ValueError("Audit import escapes its known package")
                imported = [("gossip_harness." if node.level else "") + (node.module or "")]
                if node.level and not node.module:
                    imported = ["gossip_harness." + item.name for item in node.names]
                elif not node.level and node.module == "gossip_harness":
                    imported = ["gossip_harness." + item.name for item in node.names]
            for target in imported:
                if target.split(".")[0] == "gossip_harness":
                    pending.append(target)
                elif target.split(".")[0] not in sys.stdlib_module_names:
                    raise ValueError("Audit runtime has an unbound external dependency: " + target)
    return sorted(seen)


def _execute(command: list[str], cwd: Path, stdout: Any, stderr: Any,
             timeout: float, environment: dict[str, str]) -> int:
    def cancelled(signum, _frame):
        raise KeyboardInterrupt("Historical audit cancelled by signal " + str(signum))
    handlers = {}
    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            handlers[signum] = signal.signal(signum, cancelled)
    process = None
    try:
        process = subprocess.Popen(command, cwd=cwd, stdout=stdout, stderr=stderr,
                                   env=environment, start_new_session=True)
        return process.wait(timeout=timeout)
    except BaseException:
        # Stop only this owned audit group, including any active read-only Git
        # child; preserve original study/controller processes and all evidence.
        if process is not None:
            # Repeated cancellation cannot interrupt this bounded cleanup.
            for signum in handlers:
                signal.signal(signum, signal.SIG_IGN)
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
            # A child may ignore TERM after its leader exits. Always finish the
            # entire group; process.wait() alone only establishes leader exit.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)
        raise
    finally:
        for signum, handler in handlers.items():
            signal.signal(signum, handler)


def attest_runtime(runtime: Path, auditor: str) -> dict[str, Any]:
    """Describe caller-trusted code; creating this document is not an approval."""
    if auditor not in AUDITORS:
        raise ValueError("Unknown historical auditor")
    runtime = runtime.resolve()
    snapshot = InputSnapshot()
    files = _sources(runtime, snapshot)
    closure = _complete(files, AUDITORS[auditor])
    snapshot.verify()
    return dict(protocol=PROTOCOL, auditor=auditor, entrypoint=AUDITORS[auditor],
                complete_runtime=True, import_closure=closure,
                runtime=_identity(), source_sha256={key: _hash(raw) for key, raw in files.items()})


def run_audit(run: Path, runtime: Path, attestation: Path, trusted_runtime_sha256: str,
              output: Path, *, auditor: str, accounting_ledger: Path | None = None,
              timeout_seconds: float = 1800) -> dict[str, Any]:
    if auditor not in AUDITORS or _SHA.fullmatch(trusted_runtime_sha256) is None:
        raise ValueError("Known auditor and independently pinned runtime SHA-256 required")
    if type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 3600:
        raise ValueError("Audit timeout must be positive and at most one hour")
    run, runtime, output = run.resolve(), runtime.resolve(), output.absolute()
    if output.exists() or output.is_symlink() or output.resolve().is_relative_to(run):
        raise ValueError("Historical audit output must be fresh and outside the original run")
    snapshot = InputSnapshot()
    raw_attestation = snapshot.read(attestation.absolute(), attestation.absolute().parent)
    if _hash(raw_attestation) != trusted_runtime_sha256:
        raise ValueError("Runtime attestation differs from the independently pinned digest")
    trusted = _decode(raw_attestation)
    files = _sources(runtime, snapshot)
    identity = _identity()
    actual = dict(protocol=PROTOCOL, auditor=auditor, entrypoint=AUDITORS[auditor],
                  complete_runtime=True, import_closure=_complete(files, AUDITORS[auditor]),
                  runtime=identity, source_sha256={key: _hash(raw) for key, raw in files.items()})
    if _json(trusted) != _json(actual):
        raise ValueError("Runtime does not match the complete trusted attestation")
    report = _decode(snapshot.read(run / "results.json", run))
    contract = report.get("contract")
    if (not isinstance(contract, dict) or not isinstance(contract.get("sources"), dict)
            or not contract["sources"] or report.get("contract_sha") != _hash(_json(contract))):
        raise ValueError("Archived run has no valid exact core contract")
    for name, expected in contract["sources"].items():
        if not isinstance(name, str) or _NAME.fullmatch(name) is None:
            raise ValueError("Invalid archived core source name")
        relative = "gossip_harness/" + name
        raw = snapshot.read(run / "source-snapshot" / relative, run)
        if not isinstance(expected, str) or _hash(raw) != expected or files.get(relative) != raw:
            raise ValueError("Trusted runtime and frozen core differ: " + name)
    for name in ("preregistered.json", "manifest.json"):
        path = run / name
        if path.exists() or path.is_symlink():
            document = _decode(snapshot.read(path, run))
            if document.get("contract", contract) != contract:
                raise ValueError("Archived manifest and results contracts differ")
            if "sources" in document and document["sources"] != contract["sources"]:
                raise ValueError("Archived manifest and results source contracts differ")
    snapshot.verify()
    output.mkdir(parents=True, exist_ok=False)
    copied = output / "runtime"
    (copied / "gossip_harness").mkdir(parents=True)
    for relative, raw in files.items():
        (copied / relative).write_bytes(raw)
    (output / "trusted-runtime.json").write_bytes(raw_attestation)
    receipt: dict[str, Any] = dict(protocol=PROTOCOL, auditor=auditor,
        trusted_runtime_sha256=trusted_runtime_sha256, contract_sha256=report["contract_sha"],
        complete_runtime=True, executed=False, passed=False, source_sha256=actual["source_sha256"],
        runtime=actual["runtime"], candidate_execution=False, provider_requests=0)
    command = [sys.executable, "-I", "-B", "-S", "-c", _BOOTSTRAP, str(copied),
               AUDITORS[auditor], "--run", str(run), "--output", str(output / "audit.json")]
    if accounting_ledger is not None:
        command.extend(["--accounting-ledger", str(accounting_ledger.resolve())])
    began = time.monotonic()
    try:
        snapshot.verify()
        if _identity() != identity:
            raise ValueError("Audit executable runtime changed during execution")
        receipt["executed"] = True
        with (output / "stdout.log").open("wb") as stdout, (output / "stderr.log").open("wb") as stderr:
            returncode = _execute(command, copied, stdout, stderr, timeout_seconds,
                {"PATH": str(Path(identity["git_path"]).parent) + os.pathsep + os.defpath, "LANG": "C.UTF-8"})
        receipt["returncode"] = returncode
        snapshot.verify()
        if _identity() != identity:
            raise ValueError("Audit executable runtime changed during execution")
        if _sources(runtime, InputSnapshot()) != files:
            raise ValueError("Trusted runtime module inventory changed during execution")
        if any((copied / name).read_bytes() != raw for name, raw in files.items()):
            raise ValueError("Owned audit runtime changed during execution")
        if returncode == 0:
            audit = output / "audit.json"
            audit_result = _decode(audit.read_bytes())
            receipt["audit_sha256"] = _hash(audit.read_bytes())
            if not isinstance(audit_result, dict) or audit_result.get("passed") is not True:
                raise ValueError("Historical auditor did not certify a complete passing audit")
            receipt["passed"] = True
        receipt["inputs_unchanged"] = True
    except BaseException as error:
        receipt["failure"] = str(error) or type(error).__name__
        raise
    finally:
        receipt["duration_seconds"] = time.monotonic() - began
        for name in ("stdout.log", "stderr.log"):
            if (output / name).is_file():
                receipt[name.replace(".", "_") + "_sha256"] = _hash((output / name).read_bytes())
        (output / "receipt.json").write_bytes(_json(receipt) + b"\n")
    return receipt


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--auditor", choices=sorted(AUDITORS), required=True)
    parser.add_argument("--attestation", type=Path, required=True)
    parser.add_argument("--attest", action="store_true", help="Describe trusted runtime; does not execute or approve it")
    parser.add_argument("--trusted-runtime-sha256")
    parser.add_argument("--run", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--accounting-ledger", type=Path)
    args = parser.parse_args(argv)
    if args.attest:
        data = _json(attest_runtime(args.runtime, args.auditor)) + b"\n"
        with args.attestation.open("xb") as stream:
            stream.write(data)
        print(json.dumps({"attestation_sha256": _hash(data), "executed": False}))
    else:
        if not args.run or not args.output or not args.trusted_runtime_sha256:
            parser.error("Execution needs --run, --output and independently trusted --trusted-runtime-sha256")
        result = run_audit(args.run, args.runtime, args.attestation,
              args.trusted_runtime_sha256, args.output, auditor=args.auditor,
              accounting_ledger=args.accounting_ledger)
        print(json.dumps(result, indent=2))
        raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
