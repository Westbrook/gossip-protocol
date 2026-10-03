"""V2 release derivative: frozen builder mechanics, explicit new inputs and docs.

Source generation validates exact normative source hashes. The rendered contract
is carried verbatim as CUMULATIVE-CONTRACT.md, with both linked registries included.
API.md adds the inherited v0/M1 interfaces with explicit current-v2 amendments. This is an
authored development reference and makes no whole-product acceptance claim.
"""
from __future__ import annotations

from collections.abc import Sequence
import hashlib
from pathlib import Path, PurePosixPath

from .library_m4_release_reference_v1 import (
    PUBLIC_FIXTURE_PATHS, _ASSETS as V1_ASSETS, _RELEASE,
)
from .library_project_fixture_v1 import IMMUTABLE_PATHS, RUNTIME_IMAGE

PRODUCT_CONTRACT_SHA256 = "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc"
V1_CONTRACT_SHA256 = "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c"
API_SOURCE_SHA256 = "0d35083497080a3317f4e6b2c5d71cff6abf478460914e0f0dc0f6a2c5584e11"

_INSTALL = '''# Install and run the cumulative v2 reference

This is an authored development reference, not a completed comparative study or
a claim of exhaustive v2 acceptance. Python 3.12 and its standard library suffice;
no provider account, API key, package installation or network retrieval is used.
The pinned qualification image is:

`sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f`

Build from this source root:

```sh
python -m library.clients.release --output ../library-release
```

The destination must not exist. Files, directories and symlinks already there
fail `already_exists` without replacement. Missing or symlinked source inputs
fail `invalid_source`. The compiled allowlist includes immutable seed inputs,
public compatibility snapshots, this v2 contract and public regression fixtures;
it excludes user databases, credentials and private acceptance banks. Identical
inputs produce identical sorted file records and canonical manifest bytes.
Compare every size/SHA256 and the canonical record-array digest in
`release-manifest.json` before using the package. A release can rebuild itself.

Use the already installed exact image (never substitute or download one) for the
public positive checks, from the release root:

```sh
docker image inspect sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f
docker run --pull=never --rm --network none --read-only --tmpfs /tmp:rw,nosuid,nodev,size=128m -v "$PWD:/app:ro" -w /app sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f python test_cumulative_public.py
```

These checks use isolated copies of frozen v0/M2 databases and real application
APIs. They are public positive development checks, not held-out acceptance of all
v2 amendments. They never migrate `compatibility/` originals.

For local use, put writable state outside the release, copy the demo inputs and
explicitly adopt the backup root before any backup operation:

```sh
mkdir -p ../library-state/inputs ../library-state/backups
cp release/dataset/welcome.txt release/dataset/notes.md release/dataset/literal.html ../library-state/inputs/
python -m library --db ../library-state/library.sqlite3 --root ../library-state/inputs --backup-dir ../library-state/backups backup-root-adopt --expect-unbound
python -m library --db ../library-state/library.sqlite3 --root ../library-state/inputs --backup-dir ../library-state/backups serve --port 8765
```

Open `http://127.0.0.1:8765/`; the server binds only loopback. Create the backup
directory explicitly. `--backup-dir` selects it in memory and never adopts or
rebinds it. Adoption is process-only, has no HTTP/browser route, and requires no
live worker or pending owned artifacts. The database parent must permit SQLite
journals and `<database basename>.maintenance`. Input roots are independent,
confined regular UTF-8 files; never use a whole home directory.

On Linux the already installed image can serve with `--network host`, read-only
`/app` and writable `/state` mounts, using paths under `/state`. Other hosts can
use their installed Python 3.12 for local browsing. Read RECOVERY.md before using
an existing database. CUMULATIVE-CONTRACT.md is the exact public v2 contract; API.md includes inherited interfaces; USER-GUIDE.md covers
the demonstration workflow. Frozen `README.md` and M1 seed files remain
historical inherited inputs: their cumulative amendments are specified in API.md.
'''

_RECOVERY = '''# V2 migration, recovery and backup ownership

Stop legacy v0/M1/M2 processes before migration and retain a separate closed
pre-migration database. Old binaries cannot open schema4. Supported v2 schema3
handles are fenced by a new incarnation on successful migration to schema4;
they return `stale_instance` before consulting removed tables. Migration preserves
all other control/high-water values, enrollment, ownership, backup registry and
root binding. Failure before activation preserves the old state. Normal schema4
opening and no-op migration do not rotate authority or run artifact cleanup.

Use the INSTALL.md command prefix and run `migrate`. An explicit successful
migration, including a no-op, clears only a `migrate` diagnostic. Ordinary open
preserves it; unrelated maintenance errors survive. Corrupt/unknown input and
counter exhaustion do not write a new diagnostic into the refused source.

Jobs run only after explicit `worker-enqueue JOB_ID` and `worker --once` or
`worker`. A worker owns an exclusive lifetime lock; second ownership fails
`worker_busy`. Writes check incarnation (`stale_instance`), then generation
(`stale_worker`), then epoch (`stale_epoch`). Persisted manifests resume without
rereading input files; completed receipts remain exact replay results. Diagnostics
reports `stopped` without a live current owner, even if a durable job says running;
a live owner with no matching claim is `idle`, and with a matching claim `running`.
An exited `--once` owner and a dead process are stopped. Diagnostics never starts
work, acquires ownership, clears an error or advances a counter.

All durable counters and optimistic tokens are exact signed64 integers through
9223372036854775807. Out-of-domain requests are `invalid_request`. A transition
needing an increment past the maximum fails `counter_exhausted` atomically,
including files, ownership and diagnostics. Unchanged writes, reads and completed
receipt replay remain possible at the maximum. Worker ownership and each later
job transition commit independently. Browser tokens above 2**53-1 remain exact;
never round a stale token to force a match. Reload and reconcile conflicts.

One durable backup root governs backup creation, listing, restore and backup-root
cleanup. An unbound installation returns `backup_root_unbound`; a differing
selected path returns `backup_root_mismatch` before backup I/O. Configuring or
opening a Service never binds, scans, clears or reinterprets a registry. Ordinary
catalog, diagnostics and migration remain available when roots differ.

Fresh installations explicitly adopt the existing directory with:

```sh
python -m library --db DATABASE --root INPUTS --backup-dir TARGET backup-root-adopt --expect-unbound
```

To move a bound registry, prepare the exact registered complete archives yourself
in an existing TARGET, stop workers, and use the current canonical root as a CAS:

```sh
python -m library --db DATABASE --root INPUTS --backup-dir TARGET backup-root-adopt --expect-root CURRENT
```

Use exactly one expectation. The process-only operation validates every registered
file, digest, length and generation in bounded pages while holding exclusive
maintenance authority. It rejects symlink paths and identity changes, does not
scan/register unowned files and does not copy, move or delete any file. All rows
are retained. A verified same-root adoption reports `adopted:false`; stale
expectations fail. Existing Services choosing the old root must be reconfigured.

Any pending owned artifact blocks adoption with `maintenance_busy`. Reconnect its
original bound root and perform separately authorized bounded recovery/cleanup;
never waive ownership or infer an original path from a relative name. An unbound
portable copy with pending artifacts can still expose its catalog, but safe
cleanup/adoption requires an intact original-root-bound copy with trustworthy
provenance. Lost ownership provenance is not promised recoverable. If a valid
complete BACKUP3 archive exists, a fresh empty installation may adopt that archive's
directory with an empty registry and restore its basename at expected_generation=0.
That does not recover or delete stranded artifacts in the old installation.

Create an unused basename using `backup checkpoint.json`; existing targets are
never replaced. Listing is registry metadata, not a new integrity check of each
archive. Restore validates a complete bounded envelope (including unregistered
complete basenames), takes exclusive authority and checks `expected_generation`.
It atomically rotates incarnation, increments live catalog generation and raises
incoming document tokens above target/snapshot high-water values. Only
noncompleted incoming jobs increment epochs; completed jobs retain exact original
epoch and receipt while target high-water takes the maximum without increment.
All increments are preflighted together; a later exhausted token cannot leave a
partial update. Reopen stale handles after restoration. A precommit crash leaves
the old state; a postcommit crash leaves the complete activated state.

`reindex --limit 64` advances bounded work and publishes complete generations.
Cleanup visits only registered owned artifacts in bounded separate transitions;
it never deletes a valid published backup or arbitrary input. Recovery actions
follow the retained last_error: none, restart_worker, retry_reindex,
choose_new_backup, validate_backup or run_migration. Read API.md for exact shapes,
limits, range/error precedence and serialization preservation.
'''


def _pinned_text(relative: str, digest: str) -> str:
    raw = (Path(__file__).resolve().parents[1] / relative).read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("Version the v2 release after a normative source change: " + relative)
    return raw.decode("utf-8")


def _api_text(normative: str) -> str:
    """Carry inherited endpoint/JOB details without retaining superseded rules."""
    previous = V1_ASSETS["release/API.md"]
    inherited = previous.split("## Frozen v0 contract\n", 1)[1].split("## Exact public interfaces\n", 1)[0]
    changes = {
        "GET /health returns {'status':'ok','schema':0}.":
            "GET /health returns {'status':'ok','schema':4} in this schema4 release.",
        "409 for source_changed, otherwise 400.":
            "409 for source_changed; other codes follow the consolidated mapping below.",
        "job_state, stale_epoch use HTTP409; not_found uses404; others400. Success200.":
            "job_state, stale_epoch use HTTP409; not_found uses404; additional v2 errors follow the consolidated mapping below. Success200.",
        "commit(token) checks epoch and state inside the SAME SQLite write transaction as":
            "commit(token) validates signed64 request range and the captured installation incarnation before checking epoch and state inside the SAME SQLite write transaction as",
    }
    for old, new in changes.items():
        if inherited.count(old) != 1:
            raise ValueError("Frozen inherited API text changed: " + old)
        inherited = inherited.replace(old, new)
    return ("# Current cumulative v2 API and inherited interfaces\n\n"
        "The [exact normative v2 rendering](CUMULATIVE-CONTRACT.md), copied below, and its "
        "linked machine registries govern this release. This document additionally carries "
        "the complete inherited v0/M1 interface descriptions. Their current release changes "
        "are explicit: health schema4; exact signed64 epoch/version/generation tokens; "
        "stale-instance fencing before job checks; the portable M1 content-hash rules; "
        "and the consolidated error mapping. Immutable seed histories retain their original meaning.\n\n"
        "All epochs and non-null expected versions are integers1..9223372036854775807, "
        "excluding booleans/floats; generations are integers0..9223372036854775807. "
        "No-op transitions/replays require no artificial increment. Required increments "
        "past the maximum fail counter_exhausted without changing state or diagnostics. "
        "Legacy v0 offsets retain their unbounded nonnegative domain. See the normative "
        "amendments below for exact identity/token/state/error precedence.\n\n"
        "## Inherited v0 and M1 interfaces, with current v2 changes\n\n"
        + inherited + "\n" + normative + "\n"
        + "## Consolidated error mapping and command process behavior\n\n"
        "HTTP success is200 application/json; errors are {\"error\":CODE}. not_found is404. "
        "source_changed, job_conflict, job_state, stale_epoch, stale_version, stale_generation, "
        "document_deleted, revision_capacity, collection_not_empty, worker_busy, stale_worker, "
        "stale_instance, counter_exhausted, maintenance_busy, already_exists, "
        "backup_root_unbound and backup_root_mismatch are409. Other domain errors, including "
        "invalid_request, invalid_source, invalid_utf8, unsupported_type, too_large, capacity, "
        "invalid_backup, invalid_database, unsupported_schema and io_error, are400. "
        "Unsupported routes are404. CLI data success writes one JSON value to stdout and "
        "exits0; domain/I/O errors write one error JSON to stderr and exit2; argparse usage "
        "errors exit2. Canonical export bytes have no BOM or trailing newline. Worker "
        "daemon writes no JSON until stopped.\n")


def release_files(source_paths: Sequence[str]) -> dict[str, str]:
    """Compile only explicitly supplied inputs, with exact normative provenance."""
    if isinstance(source_paths, (str, bytes)):
        raise ValueError("A sequence of explicit source paths is required")
    names = list(source_paths)
    if any(type(name) is not str for name in names) or len(names) != len(set(names)):
        raise ValueError("Invalid or duplicate release source path")
    assets = dict(V1_ASSETS)
    normative = _pinned_text("docs/library-cumulative-product-v2.md", API_SOURCE_SHA256)
    assets.update({
        "release/INSTALL.md": _INSTALL,
        "release/RECOVERY.md": _RECOVERY,
        "release/API.md": _api_text(normative),
        "release/CUMULATIVE-CONTRACT.md": normative,
        "library-cumulative-product-v1.json": _pinned_text("library-cumulative-product-v1.json", V1_CONTRACT_SHA256),
        "library-cumulative-product-v2.json": _pinned_text("library-cumulative-product-v2.json", PRODUCT_CONTRACT_SHA256),
    })
    guide = assets["release/USER-GUIDE.md"]
    marker = '6. Run `backup checkpoint.json`, or enter the name in Recovery and Create backup.'
    if guide.count(marker) != 1:
        raise ValueError("Frozen workflow documentation changed")
    guide = guide.replace(marker, '6. Confirm INSTALL.md\'s process-only `backup-root-adopt --expect-unbound`\n'
        '   completed with the explicitly selected `--backup-dir`. Constructing a Service\n'
        '   or opening a browser does not adopt a directory. Run `backup checkpoint.json`, or enter the name\n'
        '   in Recovery and Create backup.')
    assets["release/USER-GUIDE.md"] = guide + '''\nV2 tokens use exact signed64 integers. The browser preserves values above2**53-1;
never repair a conflict by rounding. At the counter maximum, effective changes
may fail `counter_exhausted` while reads, no-ops and completed receipt replay
remain usable. Recovery diagnostics distinguish a live idle owner from a matching
running claim and report stopped after owner exit. Moving or copying a database
does not rebind its backup root; follow RECOVERY.md's explicit adoption and
pending-artifact rules before backup I/O.\n'''
    all_paths = set(names) | set(assets) | {"library/clients/release.py"}
    allowed_fixed = set(IMMUTABLE_PATHS) | set(PUBLIC_FIXTURE_PATHS) | set(assets) | {"library/counters.py"}
    for name in all_paths:
        if (not name or "\\" in name or "\0" in name or str(PurePosixPath(name)) != name
                or name.startswith("/") or any(part.startswith(".") for part in PurePosixPath(name).parts)):
            raise ValueError("Invalid release source path")
        package_source = (name.startswith(("library/catalog/", "library/ingestion/", "library/query/", "library/clients/"))
                          and PurePosixPath(name).suffix in (".py", ".html"))
        if name not in allowed_fixed and not package_source:
            raise ValueError("Unowned or private release source path")
    required = set(IMMUTABLE_PATHS) | set(PUBLIC_FIXTURE_PATHS) | {"library/counters.py"}
    if not required.issubset(all_paths):
        raise ValueError("V2 release requires immutable seeds, public fixtures and counter adapter")
    assets["library/clients/release.py"] = (_RELEASE
        .replace("__SOURCE_PATHS__", repr(tuple(sorted(all_paths))))
        .replace("__RUNTIME_IMAGE__", repr(RUNTIME_IMAGE))
        .replace("__PRODUCT_CONTRACT_SHA256__", repr(PRODUCT_CONTRACT_SHA256)))
    return assets
