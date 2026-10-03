"""Inert literal histories for the six HTTP persistence/listener catalog rows.

The shared builder records prospective requests, process boundaries, fixture
bytes and independently derived public state. Constructing these declarations
performs no I/O, candidate execution, admission, or acceptance. In particular,
a declared same-database handoff is not evidence that one physically occurred.
"""
from __future__ import annotations

import json

from gossip_harness import candidate_http_fixtures_v1 as f
from gossip_harness import candidate_http_semantics_v1 as s
from gossip_harness.candidate_http_cases_core_v1 import Builder, LiteralCase

FAMILY = "HTTP-PERSIST-LISTENER"
REQUIREMENT_IDS = ("V0-HTTP-01", "M1-HTTP-01")


def _builder(suffix: str, *, cli_interaction: bool = False,
             root: str = "/inputs") -> Builder:
    return Builder(
        f"{FAMILY}/{suffix}",
        REQUIREMENT_IDS,
        interaction_ids=("V0-CLI-03",) if cli_interaction else (),
        root=root,
    )


def _cancel_subject(b: Builder) -> None:
    """The original prepared token is epoch 1; cancellation advances to 2."""
    b.submit_job("subject", f.VALID_MANIFEST, label="subject-queued-epoch-one")
    b.action("prepare", "subject", label="subject-running-epoch-one")
    b.action("cancel", "subject", label="subject-cancelled-epoch-two")


def _show_documents(b: Builder, *, label: str) -> None:
    for index, document in enumerate(b.state.documents):
        expected = json.dumps(
            document.as_json(), ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")
        b.request(
            "GET", f"/api/documents/{document.document_id}",
            s.success("document", expected),
            label=f"{label}-{index}", check=False,
        )


def _same_database_obligations(b: Builder) -> None:
    b.note(
        "P06-SAME-DB-EPOCHS: every process epoch uses the same candidate source "
        "commit/tree, declared database path and owned volume. Its original "
        "keeper runs continuously; no copying, reseeding, database replacement "
        "or mutable-history reuse is allowed. The old server must stop and be "
        "removed before the next process starts. Physical source, volume, "
        "keeper and ordered full container-ID lineage remains unqualified."
    )


def _normal_restart() -> LiteralCase:
    b = _builder("http-normal-restart-cancel-retry")
    b.start()
    b.guard()
    _cancel_subject(b)
    b.census(label="cancelled-before-normal-stop")
    b.stop()
    b.start()
    b.census(label="cancelled-after-normal-restart")
    b.action("retry", "subject", label="subject-queued-epoch-three")
    b.action("prepare", "subject", label="subject-running-epoch-three")
    b.action("commit", "subject", epoch=3, label="subject-completed-epoch-three")
    b.census(label="completed-after-normal-restart")
    _show_documents(b, label="restart-final-document")
    _same_database_obligations(b)
    b.note(
        "Both server epochs retain the identical immutable input root. The "
        "post-restart census must independently establish cancelled epoch 2 "
        "before the known retry/prepare/commit epoch-3 history is attributed. "
        "Public jobs/documents/export do not prove hidden receipt/manifest/blob "
        "persistence, transaction atomicity or an unspecified HTTP wrapper."
    )
    return b.finish()


def _cli_reads(b: Builder, *, label: str) -> None:
    """Literal finite reads; CLI wrapper authority is deliberately unspecified."""
    b.cli(("jobs",), label=f"{label}-jobs")
    b.cli(("job-show", "sentinel"), label=f"{label}-sentinel")
    b.cli(("job-show", "subject"), label=f"{label}-subject")
    b.cli(("list", "--offset", "0", "--limit", "100"), label=f"{label}-documents")
    b.cli(("export",), label=f"{label}-export")
    for index, document in enumerate(b.state.documents):
        b.cli(("show", document.document_id), label=f"{label}-show-{index}")


def _http_cli_http() -> LiteralCase:
    b = _builder("http-cli-http-same-db", cli_interaction=True)
    b.start()
    b.guard()
    _cancel_subject(b)
    b.census(label="http-cancelled-before-cli")
    b.stop()
    _cli_reads(b, label="cli-cancelled-epoch-two")
    retry = f.apply_action(b.state, "retry", "subject")
    b.cli(("job-retry", "subject"), after=retry.state, label="cli-retry-epoch-three")
    _cli_reads(b, label="cli-queued-epoch-three")
    prepare = f.apply_action(b.state, "prepare", "subject")
    b.cli(("job-prepare", "subject"), after=prepare.state, label="cli-prepare-epoch-three")
    _cli_reads(b, label="cli-running-epoch-three")
    commit = f.apply_action(b.state, "commit", "subject", epoch=3)
    b.cli(("job-commit", "subject", "3"), after=commit.state, label="cli-commit-epoch-three")
    _cli_reads(b, label="cli-completed-epoch-three")
    b.start()
    b.census(label="http-completed-after-cli")
    b.action("commit", "subject", epoch=3, label="http-completed-epoch-three-replay")
    b.census(label="http-completed-after-replay")
    _show_documents(b, label="handoff-final-document")
    _same_database_obligations(b)
    b.note(
        "P07-CLI-APPROVAL-AND-BRIDGE: these argv declarations are inert. Every "
        "finite CLI invocation must reopen the same database with the same "
        "immutable root/source, exit naturally and be removed before the next "
        "invocation or replacement HTTP server. The server is absent for the "
        "whole finite-CLI interval, while the keeper/volume remain continuous. "
        "Existing pending CLI files are neither implemented nor replaced here."
    )
    b.note(
        "CLI exit 0 and one complete stdout JSON value are declared; their "
        "outer wrappers remain unspecified, with no recursive extraction. "
        "Expected CLI state is derived prospectively from the epoch-2 HTTP "
        "history, never from command output. Independently authenticated "
        "readback and qualified cross-interface state/receipt proofs remain "
        "required; declaration or unchanged HTTP public state supplies neither "
        "CLI wrapper authority nor receipt replay identity."
    )
    return b.finish()


def _root_case(*, changed: bool) -> LiteralCase:
    suffix = "root-change-source-changed" if changed else "root-independence-source-ids"
    b = _builder(suffix, root="/inputs/root-a")
    b.add_file("root-a/same.txt", b"first")
    b.add_file("root-b/same.txt", b"second" if changed else b"first")
    b.add_file("root-b/guard.txt", f.GUARD.text.encode("utf-8"))
    b.start()
    b.guard()
    b.import_source("same.txt", "first", label="first-root-import")
    b.census(label="first-root-complete-state")
    _show_documents(b, label="first-root-document")
    b.stop()
    b.start(root="/inputs/root-b")
    b.census(label="second-root-before-import")
    b.import_source(
        "same.txt", "second" if changed else "first",
        label="changed-source-rejected" if changed else "identical-source-reimport",
    )
    b.census(label="second-root-preserved-state")
    _show_documents(b, label="second-root-document")
    _same_database_obligations(b)
    b.note(
        "P04-VERSIONED-ROOT-RECIPE: root-a and root-b are distinct immutable "
        "fixture directories staged before any dispatch. Only the declared "
        "server root/argv changes at normal replacement; candidate source, "
        "database path, original keeper and owned volume retain their identity. "
        "Per-epoch root binding and staging integrity need physical qualification."
    )
    b.note(
        "same.txt document/source IDs derive only from the exact source key; "
        "its blob ID derives from UTF-8 bytes b'first'. Exact public readbacks "
        "retain this document after the second import. Root absolute paths and "
        "observed IDs cannot enter expected identities."
    )
    return b.finish()


def _listener_epochs() -> LiteralCase:
    b = _builder("listener-all-epochs", cli_interaction=True)
    b.start()
    b.guard()
    b.request("GET", "/health", s.success("health"),
              label="first-epoch-health", check=False)
    b.census(label="first-epoch-listener-census")
    b.stop()
    b.start()
    b.request("GET", "/health", s.success("health"),
              label="second-epoch-health", check=False)
    b.census(label="second-epoch-listener-census")
    _same_database_obligations(b)
    b.note(
        "P08-LISTENER-CONTROL: each declared HTTP request, including setup and "
        "census requests in both server epochs, requires actual IPv4 and IPv6 "
        "kernel listener tables from the server's shared network namespace at "
        "the declared service port. Bind connected pre-request and post-response "
        "snapshots to that request and source-bound server epoch. Every observed "
        "declared-port listener address must be exactly 127.0.0.1. A known "
        "wildcard or other address remains a failure if the sibling table is "
        "missing; incomplete clean-looking tables remain unavailable."
    )
    b.note(
        "The two literal server epochs and their authenticated request intervals "
        "bound this listener observation. Loopback reachability, Config fields "
        "or declared argv cannot substitute for kernel snapshots. No process "
        "socket ownership, continuous monitoring between snapshots, unobserved "
        "port coverage or all-future-behavior claim follows."
    )
    return b.finish()


def _root_page() -> LiteralCase:
    b = _builder("root-page-reachable", cli_interaction=True)
    b.start()
    b.guard()
    b.census(label="root-page-before")
    b.request("GET", "/", None, label="root-page-raw-facts", check=False)
    b.census(label="root-page-after")
    b.note(
        "R04: retain authenticated root-response status, headers, raw body and "
        "completion facts without applying an API JSON predicate. No exact "
        "markup, title, tag pattern, MIME, nonempty-body or HTTP-200 shortcut "
        "establishes the accessible local browser client. The required actual "
        "browser actions/accessibility/rendering/export checks remain separate "
        "and unqualified. Missing completion is an observation limitation, not "
        "a new product latency failure."
    )
    return b.finish()


def definitions() -> tuple[LiteralCase, ...]:
    """Return all six fixed rows; no caller can supply observed fixture values."""
    return (
        _normal_restart(),
        _http_cli_http(),
        _root_case(changed=False),
        _root_case(changed=True),
        _listener_epochs(),
        _root_page(),
    )
