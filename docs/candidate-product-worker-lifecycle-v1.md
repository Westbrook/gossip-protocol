# Worker lifecycle observation draft v1

Status: isolated source draft. No Docker, Engine, provider, candidate application,
Git write, combined verification, independent acceptance, or physical qualification
was run while authoring this draft. The parent owns integration, source review,
manifest registration, the combined static gate, and any later physical dispatch.
The eight existing `candidate_product_process_*_v1` histories remain unchanged.

This is a new public observation family for the exact
`library-cumulative-product-v2.json` SHA256
`2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc`.
It uses M3-INTERFACES clause 2's separately frozen evaluator-only interface.
The interface must be held and disclosed before candidate execution. It adds no
HTTP field, ordinary user option, concealed expectation or expected answer.
Study builder-agent restart is a different event from every worker process below.

## Ordered histories and clause coverage

The four original histories contain **65 ordered actions**. The two additional
physical uncertainty controls execute the W01 plan against distinct declared
source variants, bringing prospective qualification to six histories / 95
planned actions. Unentered actions remain unentered. Fixed helper invocations,
Engine controls and creation/cleanup commands are separately retained; they are
not public CLI operations or HTTP requests. There is no HTTP request in this
worker family.

| History | Actions | Actual operation and public clause | Deliberate limit |
|---|---:|---|---|
| W01 daemon contention | 15 | A source-bound daemon stays alive with a kernel-attributed exclusive owner lock; a separate `worker --once` reports `worker_busy`; real SIGKILL, wait, disappearance, and a new daemon over the held database prove distinct owner epochs and generation changes. M3-WORKER-RECOVERY clause 1; stopped/idle portions of V2-WORKER-LIVENESS. | Does not measure the one-second polling cadence or a matching active claim. |
| W02 before-commit recovery | 15 | An admitted two-entry manifest is prepared/enrolled. A real once worker reaches the frozen rendezvous, a separate reader samples committed rows, Engine sends SIGKILL, a fresh normal CLI performs any genuine SQLite recovery, then a fresh worker resumes the same epoch and exact admitted text. M3-WORKER-RECOVERY clause 3. | The `precommit-abrupt-no-visible-graph` facet is always unavailable for full commit-boundary credit: a hook plus a SQLite reserved lock does not prove actual provisional row writes. The distinct fresh-process manifest-resumption facet can be observed. |
| W03 after-commit receipt | 20 | Independent SQLite capture sees completed job, exact original receipt, two documents/revisions/blobs while a once process is held before output; real SIGKILL plus complete empty stdout is required. A fresh once does not retry. Refresh/delete then receipt replay preserves exact stored receipt text and the historical parsed public receipt; wrong epoch remains stale. M3-WORKER-RECOVERY clause 3; M4-COMPATIBILITY clause 1. | This is application-process crash evidence over a held tmpfs volume, not power-loss or host-restart durability. |
| W04 old claim | 15 | A real live worker waits after claim/outside the write transaction; separate CLI cancel/retry advances 1→2→3; release lets the original process encounter the old epoch; a fresh once completes epoch 3. M3-WORKER-RECOVERY clause 2, selected epoch behavior. | The old-claim facet remains unavailable until matching application-claim instrumentation is independently qualified. No incarnation→worker-generation→epoch precedence claim. |

W01 is also executed with two distinct authored application sources: one actually
omits the hook; the other actually exits with code 23 at owner acquisition. Their
bounded missing/early-death captures must remain unavailable, with actual owned
cleanup. Tests never supply a prepared process result or use an assertion about
an intended artifact as evidence of executing it.

`candidate_product_worker_cases_v1.py` is a closed canonical factory. Inputs and
host expectations are separate. The candidate input mount contains only the
manifest and public hook implementation/configuration. `inline/a.txt` and
`inline/b.txt` do not exist there: recovery must use the admitted manifest. A
changed action, reordering, expected value or suite has a different definition
identity and is refused by the existing closed factory.

## Frozen instrumentation interface

The public import is `gossip_worker_hook_v1.boundary`. Its phases are:

* `owner_acquired`: after durable generation publication and exclusive ownership,
  before an active claim; observe mode emits once and returns.
* `after_claim`: after claim capture/publication, outside the write transaction;
  pause mode waits for a trusted release or its fixed hold deadline.
* `before_commit`: after intended provisional writes, immediately before SQLite
  commit. This phase is a coordinate, never independent proof of those writes.
* `after_commit_before_output`: after SQLite commit returns and before response
  bytes are emitted. Independent committed state plus the actual complete stream
  and kill chronology supply the meaningful corroboration.

The hook records only phase, actual process PID, already-held owner FD,
incarnation, worker generation and optional job/epoch. The fixed wrapper restores
normal `/workspace` module resolution and runs the actual application as
`__main__`; it does not emulate `Service`, call `Service.request`, bypass the
worker lock, fabricate SQLite results, or substitute exception rollback for
process death. A regular finite CLI remains a separate `python -m library`
process. Error JSON is interpreted on stderr under the declared nonzero branch;
success JSON is interpreted on stdout. Auxiliary CLI output is unspecified.

Hook events are candidate claims. The trusted kernel reader runs with `python
-I -c` via an owned worker exec, imports no candidate code, and reads original
`/proc/locks`, FD metadata/fdinfo and PID stat. The current qualification profile
requires main namespace PID 1 and a visible POSIX/FLOCK exclusive whole-file
lock beneath `/tmp/library.sqlite.maintenance/`. OFD locks, another path, an
unattributable owner, missing metadata or a missing hook produce unavailable
instrumentation; these are not invented product requirements or product failures.
Linux major/minor device numbers are captured inside Linux, avoiding host-OS
encoding assumptions. The selected FD, PID, lock inode and current kernel lock
must agree. Hook generation/incarnation and database inode join independently
captured committed SQLite controls. A reservation-byte lock is recorded separately
from the owner lock and remains insufficient proof of provisional row writes.

Event and release publishers write all bytes, handle short writes, and fsync a
private file before publishing its final name with an exclusive hard link. Both
names use the same retained no-follow directory descriptor. Existing final names
are never overwritten, and private names are cleaned on ordinary failures. The
hook publishes a separate complete decision file when release is consumed or its
hold expires; the trusted release helper waits at most ten seconds for that
acknowledgement. Missing or expired acknowledgement is instrumentation
unavailability, regardless of any candidate error message.

The controller retains `sNNN-worker-instrumentation-start.json` immediately before
the actual worker start request, with its original host monotonic nanoseconds.
Every action retains `sNNN-before-instrumentation.json` and
`sNNN-after-instrumentation.json`. For each active paused worker, both observations
must fit within 90 seconds of that earlier start timestamp until a prior action
has acknowledged release within the same bound. The reader reconstructs the
active worker roster, original timestamps, ordered journal positions, bound and
prior release acknowledgement. A missing/exceeded bound makes the affected action
unavailable; it cannot turn hook expiry into a product discrepancy. This is a
conservative instrumentation budget, distinct from a product latency requirement.

The SQLite helper runs in the source-free keeper and uses the fixed public
portable schema4 table/column inventory. It opens read-only, enables query-only,
and takes one bounded read transaction. It corroborates the actual opened file descriptor against the database inode and rechecks the path/descriptor after the read. It never repairs or resumes the app.
Busy/hot-journal/corrupt/oversized snapshots are unavailable. A subsequent fresh
normal CLI is the application process allowed to perform genuine recovery. BLOB
content is retained as length/SHA256; manifest/hash/receipt TEXT is retained exactly.
Parsed receipt equality and stored receipt-text equality are separate assertions.

The instrumented fixture generator processes inert corrected-reference source
strings only. Its exact source seams expose the already-held FD and place hook
calls around the real commit. It leaves actual locks, claims, job transitions,
SQLite commit and response generation in the application. Generated source is
committed by the later physical test to an isolated Git store and executed only
inside the pinned container. The precommit instrumentation limitation remains
explicit even for that authored source.

## Ownership, limits and evidence

`WorkerExecution` takes exact typed registration/profile/policy, a separately
admitted prospective registration, full Git commit/tree, pinned runtime image,
explicit local Engine endpoint, a live `ExternalHead`, and disjoint fresh raw,
delta and emergency-cleanup roots. Existing output roots are rejected; an
existing execution intent is never redispatched. Loaded evaluator definitions,
source bytes, complete input inventory, ordered history, original purpose,
profile, hook implementation, limits, environment, runtime and endpoint are bound.

The current draft permits `public_release` development observations only.
Independent acceptance/repeatability and cumulative-scope consumption remain
closed until a new family is explicitly integrated and globally frozen. Public
fixture qualification uses an explicitly disclosed fixture registration capability;
it is not a production prospective registry or full-cohort freeze. No observation
returned here grants whole-product, scope, cohort or promotion authority.

All containers use the existing pinned sandbox role policy: network none,
read-only root and source/input binds, unprivileged user, dropped capabilities,
no new privileges, no restart, no health check, bounded memory/PIDs/CPU, and no
host/Docker socket bind. The keeper mounts only the owned temporary database
volume. The worker and concurrent competitor mount the same volume, with distinct
full container IDs. At most three unretired containers coexist: keeper, worker,
and one finite competitor. The owned tmpfs is 32 MiB and is held across worker
removal/recreation. Root identity and exact keeper/volume continuity are checked
around trusted observations and restarts.

Prospective bounds: 900-second history, 300 seconds reserved for cleanup,
15-second Engine transport, 30-second finite CLI/wait, 10-second hook search,
90-second maximum hook hold, 1 MiB per candidate stdout/stderr, 8 MiB per trusted
control stream, 4 MiB SQLite snapshot, 1024 rows/table, 2-second SQLite VM budget,
16 KiB event and fixed frame/record/journal bounds from the pinned process and
checkpoint-chain profiles. Seed is 0 with the meaning fixed public input.
The limits are selected before execution and cannot be tuned after outcomes.

A worker attaches before start and retains the original upgraded stream. An
abrupt step requires a current running inspection, a retained exact
`POST .../kill?signal=SIGKILL`, complete 204 acknowledgement, independent wait,
exit inspection with matching code 137 and no OOM, immutable identity continuity,
and complete original attach EOF after start. Code 137 alone does not prove the
cause. The prior worker is removed and absence verified before a new epoch.
No graceful stop is substituted. Uncertain attachment is closed without claiming
EOF; retained partial bytes remain diagnostics. A failed socket close or partial
thread start cannot skip emergency cleanup. Command setup owns and reaps even a
Docker CLI whose second stream-drain thread did not start, and every subsequent
resource close is attempted after an earlier close fails. The first operation
error remains the primary failure.

The exclusive journal anchors every intent/raw response/output/inspection.
`read_original` accepts only the exact original live owner and current external
checkpoint, rechecks loaded sources, reconstructs controls/captures from original
bytes, and checks ordered step positions. Creation specs, command endpoint/argv/runtime, helper source/input, raw immutable role comparisons, prior-worker removal/absence and keeper/volume continuity are independently reconstructed from the originals. An arbitrary uploaded dict or prepared
artifact cannot enter that bridge. Known complete discrepancies survive alongside
later unavailable evidence. Missing rows and infrastructure are separate. Cleanup
uses the original claimed resource identities and the separately authenticated
emergency channel; it never heals an uncertain main journal or authorizes new
acceptance. Its retained disposition census is checked independently from the
terminal's boolean.

Worker wait and final inspection facts are retained in `sNNN-worker-exit.json`
before attach completeness is checked. Within a valid instrumentation bound,
an independently reconstructed wrong natural exit remains a discrepancy if the
stream later becomes unavailable. Complete malformed product JSON and wrong
durable column types remain discrepancies; ambiguous JSON and parser limits remain
unavailable. Each postcommit snapshot is checked independently, so a missing
earlier snapshot cannot suppress a later known wrong graph or receipt.

## Integration and unresolved obligations

The proposed manifest fragment lists four offline classes (30 focused tests) and
six separately gated Docker classes. The physical classes use the existing
`GOSSIP_RUN_DOCKER_TESTS=1` convention. They have not been run. A qualification
pass may include only the prospectively declared unavailable instrumentation
facets; it is never renamed a complete worker recovery or product pass.

Still required separately: independently proven provisional row writes before
commit; actual matching/stale claim transitions for the full worker-state matrix;
incarnation and worker-generation fence precedence; maintenance concurrency;
64-entry owned cleanup/cursor/interruption/unknown-file conservation; application
I/O/domain failure diagnostics; polling cadence; power-loss durability; a new
closed scope-consumer selector family and fresh independently admitted execution.
No old profile, selector, receipt or study source is edited to hide these gaps.
