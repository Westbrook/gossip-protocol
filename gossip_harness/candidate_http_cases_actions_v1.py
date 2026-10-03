"""Literal public-HTTP histories for all 58 M1 action-state catalog rows.

Expected states come only from prospectively authored fixture values. Requests
never interpolate observed jobs, epochs, tokens, wrappers or document IDs.
Declaring a restart or a receipt obligation does not establish physical execution
or acceptance authority; those require separately qualified runtime evidence.
"""
from __future__ import annotations

from . import candidate_http_fixtures_v1 as fixtures
from . import candidate_http_semantics_v1 as semantics
from .candidate_http_cases_core_v1 import Builder, LiteralCase

FAMILY = "HTTP-ACTION-STATE"
REQUIREMENTS = ("M1-HTTP-01", "M1-HTTP-02")
_CURRENT_EPOCH = {"queued": 1, "running": 1, "completed": 1, "cancelled": 2, "failed": 1}
_DIFFERENT_EPOCH = {"queued": 2, "running": 2, "completed": 2, "cancelled": 3, "failed": 2}
_WRAPPER_LIMIT = (
    "Successful submit, single-job lookup and action HTTP outer wrappers are unspecified. "
    "Status, complete JSON and independently derived public jobs/documents/export censuses "
    "are separate from token, receipt or hidden-manifest identity; never recursively mine "
    "a wrapper or infer those identities from unchanged public state."
)
_RECEIPT_OBLIGATION = (
    "The underlying completed-token operation must replay the same semantic receipt: "
    "its completed JOB and source-sorted batch documents, including identical existing "
    "sources once. Exact HTTP presentation is unspecified. A separately qualified "
    "method/CLI/storage interface with the same source, DB and epoch lineage must prove "
    "receipt identity; these HTTP censuses alone cannot discharge that obligation."
)
_TOKEN_OBLIGATION = (
    "The underlying running prepare operation must replay its same job_id/epoch token. "
    "The HTTP wrapper is unspecified, so equal public state and HTTP200 do not establish "
    "token semantic identity. Retain a separately qualified interface proof obligation."
)
_CONFLICT_MANIFEST = (fixtures.Entry("item.txt", "job text"), fixtures.Entry("z.txt", "new"))


def _new(suffix: str) -> Builder:
    builder = Builder(f"{FAMILY}/{suffix}", REQUIREMENTS)
    builder.note(_WRAPPER_LIMIT)
    builder.start()
    builder.guard()
    return builder


def _establish(builder: Builder, state: fixtures.JobState) -> None:
    """Create each nominal state through literal HTTP mutations, never DB seeding."""
    manifest = fixtures.FAILED_MANIFEST if state == "failed" else fixtures.VALID_MANIFEST
    builder.submit_job("subject", manifest, label="setup-submit-subject")
    if state in ("running", "completed", "failed"):
        builder.action("prepare", "subject", label="setup-prepare-subject")
    if state == "completed":
        builder.action("commit", "subject", epoch=1, label="setup-commit-epoch1")
    if state == "cancelled":
        builder.action("cancel", "subject", label="setup-cancel-to-epoch2")


def _base(suffix: str, state: fixtures.JobState) -> Builder:
    builder = _new(suffix)
    _establish(builder, state)
    return builder


def _matrix() -> tuple[LiteralCase, ...]:
    cases: list[LiteralCase] = []
    for state in fixtures.STATES:
        for action in fixtures.ACTIONS:
            builder = _base(f"{state}-{action}", state)
            builder.action(action, "subject", epoch=_CURRENT_EPOCH[state] if action == "commit" else None,
                           label="subject-action")
            if state == "running" and action == "prepare":
                builder.note(_TOKEN_OBLIGATION)
            if action == "commit" and state in ("running", "completed"):
                builder.note(_RECEIPT_OBLIGATION)
            cases.append(builder.finish())

        builder = _base(f"{state}-stale-commit", state)
        builder.action("commit", "subject", epoch=_DIFFERENT_EPOCH[state], label="different-epoch-commit")
        builder.note("Wrong generated integer epoch must report stale_epoch before state/replay checks, "
                     "including terminal states. The requested epoch is fixed before execution.")
        cases.append(builder.finish())

        manifest = fixtures.FAILED_MANIFEST if state == "failed" else fixtures.VALID_MANIFEST
        builder = _base(f"{state}-submit-same-canonical-manifest-reversed", state)
        builder.submit_job("subject", tuple(reversed(manifest)), label="same-manifest-reversed")
        builder.note("Canonical manifest replay must retain current state, epoch, progress and error. "
                     "Public behavior does not independently prove stored manifest bytes.")
        cases.append(builder.finish())

        builder = _base(f"{state}-submit-different-manifest", state)
        different = manifest[:-1] + (fixtures.Entry(manifest[-1].source, "changed"),)
        builder.submit_job("subject", different, label="different-manifest-conflict")
        cases.append(builder.finish())

        builder = _base(f"{state}-reopen", state)
        builder.census(label="before-reopen")
        builder.stop()
        builder.start()
        builder.census(label="after-reopen")
        builder.note("Restart requires the old server to be stopped and removed, a distinct server "
                     "epoch over the same source, root and DB, and a continuous keeper. "
                     "These lifecycle declarations and expected state identity are not physical proof.")
        cases.append(builder.finish())
    return tuple(cases)


def _missing() -> tuple[LiteralCase, ...]:
    cases: list[LiteralCase] = []
    for action in fixtures.ACTIONS:
        builder = _new(f"missing-{action}")
        builder.action(action, "missing", epoch=1 if action == "commit" else None, label="missing-job-action")
        cases.append(builder.finish())
    return tuple(cases)


def _old_tokens() -> tuple[LiteralCase, ...]:
    cases: list[LiteralCase] = []
    for suffix in ("cancelled", "retried-queued", "reprepared-running", "successor-completed"):
        builder = _new(f"old-token-after-{suffix}")
        builder.submit_job("subject", fixtures.VALID_MANIFEST, label="submit-valid-manifest")
        builder.action("prepare", "subject", label="prepare-epoch1")
        builder.action("cancel", "subject", label="cancel-to-epoch2")
        if suffix != "cancelled":
            builder.action("retry", "subject", label="retry-to-epoch3")
        if suffix in ("reprepared-running", "successor-completed"):
            builder.action("prepare", "subject", label="prepare-epoch3")
        if suffix == "successor-completed":
            builder.action("commit", "subject", epoch=3, label="complete-epoch3")
            builder.note(_RECEIPT_OBLIGATION)
        builder.action("commit", "subject", epoch=1, label="reject-old-prepared-epoch1")
        builder.note("The rejected epoch1 token is causally prepared by this history before cancel/retry. "
                     "No token or epoch is taken from candidate output; stale_epoch has priority.")
        cases.append(builder.finish())
    return tuple(cases)


def _additional_histories() -> tuple[LiteralCase, ...]:
    cases: list[LiteralCase] = []
    builder = _new("queued-invalid-prepare")
    builder.submit_job("subject", fixtures.FAILED_MANIFEST, label="admit-invalid-source-manifest")
    builder.action("prepare", "subject", label="deferred-invalid-source")
    cases.append(builder.finish())

    builder = _new("running-prepare-after-import")
    builder.submit_job("subject", (fixtures.Entry("item.txt", "job text"),), label="submit-item")
    builder.action("prepare", "subject", label="prepare-epoch1")
    builder.import_source("item.txt", "file text", label="intervening-direct-import")
    builder.action("prepare", "subject", label="replay-running-prepare")
    builder.note(_TOKEN_OBLIGATION)
    builder.note("Running prepare immediately replays before revalidation; the immutable input-file "
                 "import remains in the public catalog while subject stays running at epoch1.")
    cases.append(builder.finish())

    builder = _new("commit-rechecks-direct-import")
    builder.submit_job("subject", _CONFLICT_MANIFEST, label="submit-two-entry-manifest")
    builder.action("prepare", "subject", label="prepare-epoch1")
    builder.import_source("item.txt", "file text", label="intervening-direct-import")
    builder.action("commit", "subject", epoch=1, label="commit-rechecks-source-conflict")
    builder.note("Public rollback sensitivity requires failed/source_changed/completed0, guard plus "
                 "imported item, and no z.txt. Single-transaction, blob, manifest and receipt "
                 "conservation remain separate storage proof obligations.")
    cases.append(builder.finish())

    builder = _new("failed-retry-prepare-fails-again")
    builder.submit_job("subject", fixtures.FAILED_MANIFEST, label="admit-original-invalid-manifest")
    builder.action("prepare", "subject", label="fail-epoch1-invalid-source")
    builder.action("retry", "subject", label="retry-clears-error-at-epoch2")
    builder.action("prepare", "subject", label="same-invalid-manifest-fails-epoch2")
    builder.note("Retry must preserve the original invalid manifest: epoch2 queued/error-null "
                 "then failed/invalid_source/completed0 on prepare, with guard-only documents. "
                 "No repair manifest is substituted; hidden-byte manifest identity is separate.")
    cases.append(builder.finish())

    builder = _new("old-token-after-successor-failed")
    builder.submit_job("subject", _CONFLICT_MANIFEST, label="submit-original-valid-manifest")
    builder.action("prepare", "subject", label="prepare-epoch1")
    builder.action("cancel", "subject", label="cancel-to-epoch2")
    builder.action("retry", "subject", label="retry-to-epoch3")
    builder.action("prepare", "subject", label="prepare-epoch3")
    builder.import_source("item.txt", "file text", label="intervening-direct-import")
    builder.action("commit", "subject", epoch=3, label="current-commit-fails-source-changed")
    builder.action("commit", "subject", epoch=1, label="old-epoch-before-failed-state-check")
    builder.note("The original valid manifest genuinely reaches running at epochs1 and3. "
                 "An intervening immutable-file import causes current commit to fail source_changed; "
                 "then the previously prepared epoch1 must be stale_epoch rather than job_state. "
                 "Final failed epoch3 state and imported catalog are unchanged by that rejection.")
    cases.append(builder.finish())
    return tuple(cases)


def _single_job_reads() -> tuple[LiteralCase, ...]:
    cases: list[LiteralCase] = []
    for state in fixtures.STATES:
        builder = _base(f"{state}-get-single-job", state)
        builder.request("GET", "/api/jobs/subject", semantics.success("unspecified"),
                        label="single-job-lookup")
        builder.note("Single-job lookup must refer to subject's current state, but no exact HTTP "
                     "serialization is prescribed. The supported GET /api/jobs census separately "
                     "checks the full exact current job record and document conservation.")
        cases.append(builder.finish())
    return tuple(cases)


def definitions() -> tuple[LiteralCase, ...]:
    """Return the exact 58 prospective rows in retained proposal order."""
    return _matrix() + _missing() + _old_tokens() + _additional_histories() + _single_job_reads()
