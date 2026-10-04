# C03 HTTP execution v3

V3 extends the qualified v2 mechanics to represent every current literal HTTP
history intact: per-epoch input roots and argv, confined data symlinks, HTTP to
finite CLI to HTTP transitions, and up to 128 ordered steps. It remains an
infrastructure qualification executor. It does not award a product pass or
admit fresh independent acceptance.

The frozen v1/v2 executors, finite-process v4, wire transport v1 and their
historical evidence remain unchanged. The current 276-row catalog declares
8,203 steps, 7,602 HTTP requests, 29 finite CLI invocations and 8,193 total
processes including keepers. These are prospective allocation counts. The
largest history has 87 steps; the largest request count is 70. Nothing in this
document describes those histories as physically executed.

## Recipe and source identity

`recipe_from_case(core.LiteralCase)` produces a `HttpRecipe` with the original
row ID, complete canonical declaration bytes and exact definition digest. Each
`HttpStep` retains its complete declaration, epoch, root, argv and literal
request bytes. The recipe verifies that its executable projection matches the
full declaration. Safe artifact names derive separately from the row ID; IDs
containing slashes are preserved as data rather than rewritten into aliases.

Synthetic mechanics controls can retain the earlier explicit qualification
recipe constructor. Their resolved ordered mechanics record has its own
identity; it cannot claim a named catalog row without a complete matching
literal declaration. There is no fallback from an unsupported catalog history
to a shortened v2 history.

Registration binds the complete Git tree/source, fixture manifest, recipe,
definition, runtime, pinned image, endpoint, environment, helper, evaluator
sources, resource policy, seed, milestone and qualification purpose. Loaded
helper/core sources are included. Every candidate process still receives only
registered source and input data. Expected states, semantic assertions and
controller journals are not candidate mounts.

## One database history across interfaces

One owned tmpfs volume and one continuous source-free, input-free keeper hold
the database namespace for the whole history. The keeper mounts that volume
read-only. Server and finite CLI containers mount the same volume read-write
at `/tmp`, with `/workspace` and `/inputs` read-only. Their working directory
is `/workspace`; their network is `none`. No candidate is executed on the host.

A start step must name a real declared input directory, its literal argv and
the next server epoch. A request belongs to the current epoch and root. A stop
must prove the old server stopped and was removed before another server or
CLI can run. A CLI step requires no active server and the previous epoch/root
lineage. Each server and finite CLI gets a fresh full container ID.

The CLI branch calls frozen `candidate_client_process_v4.run_process` directly
with independently registered argv/runtime and the existing volume. It does
not construct a separate finite-history controller or create a second database.
It rechecks source, inputs, runtime, keeper and the volume around execution.
Raw finite results are saved before later continuity checks. A natural exit 2
can be an authenticated observation; the executor does not convert it into an
infrastructure failure or a product verdict. Missing natural completion or
complete capture remains unavailable and prevents dependent dispatch.

Exact role inspections retain all Config/HostConfig and complete mount rows.
Only already named runtime-gated startup rules apply: typed OOM default
false-to-null, and the probe-specific hostname transition to the independently
verified current server. The finite CLI receives no probe hostname permission.
Created and later volume views are retained as complete objects. Cleanup only
removes independently identified owned resources; failed removal is recorded
before it is attempted and is not retried by the final cleanup loop.

The trusted HTTP probe shares only `container:<full-current-server-ID>` network.
It has its fixed helper/request mount and private temporary filesystem, with
no source, input, database, candidate PID namespace or Engine socket. All roles
retain the pinned image, user 65534, read-only root, dropped capabilities,
no-new-privileges, CPU/memory/PID/descriptor limits, disabled logging and no
restart or health-check policy.

These identities establish continuity of the state namespace. They do not
prove a particular database inode, correct SQLite use, transaction atomicity,
crash recovery or hidden receipt contents. An orderly server replacement is
not a crash/partition test.

## Data links and request completeness

Only input data gains symlink support, through `candidate_http_inputs_v1`.
The immutable fixture tuple contains files, real directories and exact link
targets. Staging and subsequent verification use descriptor-relative no-follow
traversal, `lstat`/`readlink` semantics, explicit real ancestors and exact bytes.
Links cannot escape the owned input tree or traverse another link. They may
cross a selected Service.root into a declared sibling inside `/inputs`, which
is needed to observe the product's path boundary. Source and helper staging
continue to reject symlinks.

The unchanged HTTP wire transport already admits the catalog's largest body
of 65,537 bytes and largest request wire of 65,661 bytes. Those fit its 131,072-byte
request bound; the product's 65,536-byte body rule is a separate criterion.
Requests are checked against the selected wire policy before dispatch. No
hidden readiness request, body repair or post-send retry is introduced.

Content-Length and chunk framing can complete without socket EOF;
close-delimited responses require EOF. Complete request transmission does not
prove that the server consumed the body. Early oversized-input rejection must
not acquire a new obligation to drain the request: an incomplete send remains
unavailable under the current attribution contract. A trusted helper exit
alone does not make an incomplete HTTP exchange complete.

## Bounded journal and lifetime

V3 uses `candidate_http_journal_v3` for trusted journal bytes/JSON. It permits
32 MiB records with explicit depth/node limits and rejects duplicate keys,
nonfinite values and unsafe/unstable filesystem paths. Engine control JSON
keeps the frozen1 MiB parser. The reader supplies syntax and stable-file checks;
external controller checkpoints still supply provenance.

The total journal stays 512 MiB and 16,384 files. Normal writes cannot consume a
protected 96 MiB/512-file cleanup allowance. Before setup and each next step,
`_begin_operation` requires 128 MiB and 256 normal slots above that reserve, then
limits the operation's actual writes to that allocation. Exhaustion stops the
unobserved suffix as unavailable; it never becomes a candidate correctness or
latency failure.

The reserve calculation is retained in `quota_policy()`: at most three
unretired containers, up to 12 Docker cleanup commands with two 1 MiB raw streams
and one bounded metadata record each, three bounded Engine control envelopes,
ten further bounded metadata records, and a 1 MiB terminal. Cleanup JSON itself
is capped at1 MiB. The 512-file allowance exceeds the derived record census.
This is a bound on retained evidence, not a promise that a broken filesystem,
checkpoint sink or daemon can successfully retain or remove resources.

Terminal records contain ordered `{path, bytes, sha256}` references to retained
epoch, step and observation rows. Full argv and epoch state stay in their own
records, so many permitted long argv values cannot consume the compact terminal
allowance. Reauthentication reconstructs an exact-once ordered
prefix, including at most the current failed observation, and checks each
original row and raw stream. An observation is referenced only after its
retention/checkpoint succeeds. Full raw files are kept; histories are not split
to avoid a record cap. The terminal separately records entered steps, unentered
suffixes, finite transport invocation attempts, retained observations and
unavailable attempts. A lost observation-row write after a transport call is
counted as unavailable, while its raw artifacts remain discoverable through
the original checkpoint and recorded artifact prefix. Invoking a transport is
not evidence that a process started or an HTTP request was sent.

The default total history lifetime is 7,200 seconds, with 300 seconds reserved
as a bounded cleanup opportunity. Controller control calls, child reap and capture-drain waits use the remaining
phase budget; an unfinished reap or drain stays explicitly unproven. A frozen finite CLI is admitted only when its whole registered
command/control/teardown window fits; its command deadline is not silently
changed. Teardown and cleanup may still be unproven when the fixed opportunity
is exhausted. Resource deadlines are evaluator limits, not added product rules.

The aggregate journal limit does not guarantee room for 70 maximum-size legal
transport captures plus repeated evidence. It is a jointly registered limit;
large or slow otherwise-correct output can therefore remain unavailable. Future
quota changes must be declared before dispatch. The existing checkpoint
inventory behavior is preserved rather than introducing a new ledger.

## Qualification and later acceptance

The nearest checks are offline role, recipe, journal and inert Engine/socket
regressions, including the actual frozen finite transport on mocked boundaries.
They establish implementation behavior within that scope. Root owns the
combined static/offline gate, source freeze and any subsequent physical
qualification. No Docker or provider execution is authorized by constructing
a recipe or by this documentation.

A fresh v3 physical mechanics qualification must preserve M01–M09 and add
complete mixed-interface and long-request histories, per-epoch roots/confined
links, oversized body transmission and continuity/retention fault controls.
Historical v2 physical receipts remain historical; they do not qualify the new
composition by a protocol rename. Component receipts for unchanged exact
primitives can be reused only under the project's normal fingerprint and
purpose rules.

The next acceptance bridge must be an explicit later version of purpose
admission and evidence binding. Current `HttpBinding` only admits
`harness_qualification`; an adapter must not silently label one of these runs
`independent_acceptance`. A later version can retain the exact frozen wire,
finite-process, input/journal helper and role-comparison components, but must
register the full candidate/catalog/evaluator/runtime/limits/purpose before a
fresh acceptance dispatch and independently qualify the changed composition.
Old qualification observations cannot satisfy that fresh execution barrier.

The semantic/relation and CLI observation adapters, browser behavior, hidden
state/internal-layer obligations and full cumulative acceptance remain separate
work. This executor deliberately reports authenticated observations and
infrastructure uncertainty, not a swarm-quality result.
