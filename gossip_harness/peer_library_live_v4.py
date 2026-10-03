"""Explicit operator entry for the qualified twenty-role developmental pilot.

Contract/preflight commands are read-only and do not discover credentials. Live
entry reads the already authorized credential only after qualification and the
existing cumulative wallet have passed; the authority checks them again before
each dispatch. This module cannot create funding, raise a cap, or retry a run.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time
from typing import Any, Mapping

from .peer_financial_authority_v2 import FinancialError
from .peer_financial_authority_v3 import digest, preflight_permit
from .peer_library_contract_v4 import MAX_OUTPUT_TOKENS
from .peer_library_project_v4 import (
    ProjectConfig, enrollment, execution_contract, profile_specs, run_project,
)
from .peer_store_v1 import strict_loads
from .worker import MODEL, STRONG_MODEL, OpenAIWorker

REPOSITORY = Path(__file__).resolve().parent.parent


def _canonical(path: Path, *, existing: bool) -> Path:
    path = Path(path)
    if not path.is_absolute() or path != path.resolve():
        raise FinancialError("A canonical absolute path is required")
    if existing and not path.is_file():
        raise FinancialError("Required operator input is not an existing regular file")
    return path


def read_permit(path: Path) -> dict[str, Any]:
    path = _canonical(path, existing=True)
    with path.open("rb") as stream:
        raw = stream.read(2_100_001)
    value = strict_loads(raw, max_bytes=2_100_000)
    if type(value) is not dict:
        raise FinancialError("Operator permit must be a bounded JSON object")
    return value


def opening_snapshot(ledger_path: Path, cohort_id: str) -> dict[str, int]:
    """Read a consistent existing ledger without bootstrap or mutation."""
    path = _canonical(ledger_path, existing=True)
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
        db.execute("BEGIN")
        cap = db.execute("SELECT value FROM settings WHERE key='budget'").fetchone()
        if cap is None or type(cap[0]) is not int:
            raise FinancialError("Existing cumulative budget is unavailable")
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        snapshot = {
            "global_cap": cap[0],
            "used_or_reserved": db.execute(
                "SELECT COALESCE(SUM(COALESCE(spent,amount)),0) FROM reservations").fetchone()[0],
            "unsettled_reservations": db.execute(
                "SELECT COUNT(*) FROM reservations WHERE spent IS NULL").fetchone()[0],
            "active_tasks": db.execute("SELECT COUNT(*) FROM tasks WHERE status='submitting' "
                "OR (status='claimed' AND expires>?)", (time.time(),)).fetchone()[0],
            "pending_promotions": db.execute("SELECT COUNT(*) FROM intents WHERE state='pending'").fetchone()[0],
            "nonterminal_financial_actions": 0, "halted_cohorts": 0, "existing_cohort": 0,
        }
        if "financial_actions_v2" in tables:
            snapshot["nonterminal_financial_actions"] = db.execute(
                "SELECT COUNT(*) FROM financial_actions_v2 WHERE state IN "
                "('pending','publication_pending','unknown')").fetchone()[0]
        if "financial_cohorts_v2" in tables:
            snapshot["halted_cohorts"] = db.execute(
                "SELECT COUNT(*) FROM financial_cohorts_v2 WHERE halted=1").fetchone()[0]
            snapshot["existing_cohort"] = db.execute(
                "SELECT COUNT(*) FROM financial_cohorts_v2 WHERE cohort=?", (cohort_id,)).fetchone()[0]
    return snapshot


def preflight(output: Path, ledger_path: Path, permit: dict[str, Any],
              expected_permit_sha256: str, config: ProjectConfig) -> dict[str, Any]:
    output = _canonical(output, existing=False)
    if output.exists():
        raise FinancialError("A new output is required; existing observations cannot be retried")
    ledger_path = _canonical(ledger_path, existing=True)
    contract = execution_contract(config)
    cohort = enrollment(output, contract, ledger_path, mode="live")
    preflight_permit(permit, expected_permit_sha256, cohort, profile_specs(config))
    if (permit["execution_design"] != contract["execution_design"]
            or permit["sources"] != contract["sources"]):
        raise FinancialError("Permit is not for the current project execution design")
    snapshot = opening_snapshot(ledger_path, cohort["cohort_id"])
    if (snapshot["global_cap"] != permit["expected_global_cap"]
            or snapshot["used_or_reserved"] != permit["expected_opening_usage"]
            or any(snapshot[field] for field in ("unsettled_reservations", "active_tasks",
                "pending_promotions", "nonterminal_financial_actions", "halted_cohorts", "existing_cohort"))):
        raise FinancialError("Fresh pilot requires the exact quiescent cumulative opening")
    return {"protocol": "peer-library-live-preflight-v4", "passed": True,
        "execution_contract_sha256": digest(contract),
        "execution_design_sha256": contract["execution_design_sha256"],
        "operator_permit_sha256": expected_permit_sha256, "opening": snapshot,
        "allocated_micro_usd": permit["incremental_cap_micro_usd"],
        "credentials_read": False, "api_calls": 0,
        "note": "Read-only snapshot; transactional enrollment and dispatch revalidate it."}


def _credential(value: Any) -> str:
    if (type(value) is not str or not 16 <= len(value) <= 1024
            or any(character.isspace() or ord(character) < 33 or ord(character) > 126 for character in value)):
        raise FinancialError("The authorized credential is absent or malformed")
    return value


def load_authorized_key(env_file: Path, *, environment: Mapping[str, str] | None = None) -> str:
    """Read only the previously authorized source, without export or logging."""
    environment = os.environ if environment is None else environment
    if "OPENAI_API_KEY" in environment:
        return _credential(environment["OPENAI_API_KEY"])
    path = _canonical(env_file, existing=True)
    if path != REPOSITORY / ".env.local":
        raise FinancialError("Only the existing project .env.local credential file is supported")
    tracked = subprocess.run(["git", "ls-files", "--error-unmatch", "--", ".env.local"],
        cwd=REPOSITORY, capture_output=True, timeout=10, check=False)
    ignored = subprocess.run(["git", "check-ignore", "--quiet", "--", ".env.local"],
        cwd=REPOSITORY, capture_output=True, timeout=10, check=False)
    if tracked.returncode != 1 or ignored.returncode != 0:
        raise FinancialError("Credential file must be untracked and ignored")
    with path.open("rb") as stream:
        raw = stream.read(65_537)
    if len(raw) > 65_536:
        raise FinancialError("Credential file exceeds the supported size")
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeError:
        raise FinancialError("Credential file must be UTF-8") from None
    matches = [match.group(1).strip() for line in lines
               if (match := re.fullmatch(r"\s*(?:export\s+)?OPENAI_API_KEY\s*=\s*(.*?)\s*", line))]
    if len(matches) != 1:
        raise FinancialError("Credential file needs exactly one OPENAI_API_KEY assignment")
    value = matches[0]
    if len(value) >= 2 and value[0] in ("'", '"') and value[-1] == value[0]:
        value = value[1:-1]
    # No shell interpolation, sourcing, comments, backslash escapes or subprocesses.
    if any(character in value for character in ("$", "`", "\\", "'", '"', "#")):
        raise FinancialError("Credential assignment must contain a literal value")
    return _credential(value)


def run_live(output: Path, ledger_path: Path, permit: dict[str, Any],
             expected_permit_sha256: str, config: ProjectConfig,
             env_file: Path = REPOSITORY / ".env.local") -> dict[str, Any]:
    preflight(output, ledger_path, permit, expected_permit_sha256, config)
    key = load_authorized_key(env_file)
    workers = {name: OpenAIWorker(key, model=model, max_output_tokens=MAX_OUTPUT_TOKENS,
                                timeout=config.worker_timeout)
               for name, model in (("mini", MODEL), ("strong", STRONG_MODEL))}
    del key
    return run_project(output, config, mode="live", workers=workers, ledger_path=ledger_path,
                       permit=permit, expected_permit_sha256=expected_permit_sha256)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("contract", "preflight", "live"))
    parser.add_argument("--deadline", type=int, default=5400)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--ledger", type=Path)
    parser.add_argument("--permit", type=Path)
    parser.add_argument("--permit-sha256")
    args = parser.parse_args(argv)
    try:
        config = ProjectConfig(deadline_seconds=args.deadline)
        if args.command == "contract":
            answer = execution_contract(config)
        else:
            if any(value is None for value in (args.output, args.ledger, args.permit, args.permit_sha256)):
                parser.error("preflight/live requires --output, --ledger, --permit and --permit-sha256")
            permit = read_permit(args.permit)
            operation = run_live if args.command == "live" else preflight
            answer = operation(args.output, args.ledger, permit, args.permit_sha256, config)
        print(json.dumps(answer, sort_keys=True, indent=2))
        return 0 if answer.get("passed", args.command == "contract") else 1
    except (ValueError, OSError, sqlite3.Error, FinancialError) as error:
        # Operator failures have stable messages; never print locals or arguments.
        print(json.dumps({"passed": False, "error_type": type(error).__name__,
                          "message": "Operator entry refused; retain the inputs and resolve the failed gate."}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
