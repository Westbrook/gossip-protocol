# Compact checkpoints and final-milestone profiles

This change implements three missing building blocks for the cumulative agent
study. It does not execute another agent cohort or establish a swarm advantage.
The [checkpoint](../analysis/compact-checkpoint-foundation-v1.json) binds the
source, checks, reviews and retained storage measurement.

## What now exists

`candidate_checkpoint_chain_v1` retains each original raw file once, followed by
one canonical delta containing its name, size, hash, context, sequence and prior
head. Raw and delta writes are exclusive and fsynced before a separately owned
head acknowledges the new prefix. Raw bytes, delta bytes, file counts and single
file sizes have explicit bounds, including capacity reserved for cleanup.

`candidate_checkpoint_head_v1` supplies that separate host-owned head. It uses a
private directory outside both journal roots, lifetime ownership exclusion,
atomic replacement and file/directory fsync. Reopen requires an independently
obtained expected commitment and re-establishes durability before returning.
Thread/process ownership and reentrant operation checks prevent competing
updates through the same owner. A lost acknowledgement leaves authority
uncertain even when newly written bytes are visible.

`cumulative_observation_profile_v1` describes prospective M4 applicability of the
complete existing CLI/HTTP declarations. It preserves 57 CLI histories, 305
steps and 900 diagnostic cells; 868 assertions are normative and 32 are expressly
unspecified by the original declarations. Projection preserves the complete
diagnostics and every required failure or unavailable result. The 276 HTTP
histories retain 8,203 steps and 7,602 requests. Only three health expectations,
across two histories, change from schema0 to schema4. Requests, fixtures and
original declarations are retained unchanged.

These profiles are inputs to independent semantic review. They grant no review,
dispatch or acceptance authority and cannot relabel an M1 observation as M4.
Raw-only HTTP steps remain unqualified for product acceptance; their execution,
capture, continuity and cleanup obligations remain mandatory. New M4 behavior
and other uncovered product obligations remain explicitly outside this inherited
profile and still need their own evidence.

## Integrity and recovery contract

The new checkpoint format deliberately changes when unrelated file tampering
is detected. `retain()` does not reread every previous file. `read()` authenticates
the bytes it consumes and checks the external head on both sides.
`validate_boundary()` reconstructs every delta, hashes every raw file and checks
the complete inventory and external head. Reopen performs the same complete
validation. Executors must call these checks at their declared dispatch,
observation and final-acceptance boundaries; that integration is still pending.

Tampering restored entirely between observations can escape detection. These
checks are not a continuously atomic filesystem snapshot. Full boundary scans
can still have growing-prefix costs, so storage improvement alone does not prove
acceptable whole-study runtime.

Any uncertain write, head acknowledgement or authenticated read closes current
authority. The chain preserves all artifacts and performs no implicit retry.
`read_prior()` can authenticate individual files from the last acknowledged
prefix, explicitly labelled prior-only, so a known failure need not disappear
when later infrastructure fails. It cannot authenticate an attempted unanchored
suffix or issue a complete-history validation. Cleanup after uncertainty needs
a separate owner channel using previously authenticated resource ownership;
the reserved normal cleanup capacity does not heal an uncertain journal.

The external expected head must come from a separately trusted owner. Rolling
back both the anchor and that independent expectation is outside this trusted-
host contract. The owner directory must remain outside actual candidate mounts;
merely constructing the class does not prove a mount policy. Long-lived fork
children can retain inherited advisory lock descriptors; workers must promptly
exec/exit or close inherited descriptors. PID guards prevent child API use and
child constructor cleanup cannot unlock the parent's lock.

## Verification

The central static/configuration gate and all 73 affected offline tests passed
fresh in four isolated class workers, using one resource slot. There were no
reused results, skips or source/runtime drift. The source fingerprint covers 545
inputs. Incremental type coverage retains 117 known legacy baseline diagnostics;
this is not a claim that every legacy function is type-clean.

Coverage includes delta/anchor interruption, corrupted raw bytes, rollback and
foreign suffixes, bounded inventories, cleanup reserves, strict canonical
decoding, process reopen, competing owners, actual fork rejection and fork-time
constructor cleanup, reentrancy and known-prior facts after a lost anchor
acknowledgement. Independent source review caught and resolved durability and
ownership defects before the combined gate. Physical power loss, mounted
candidate execution and full controller integration are not qualified here.

Reproduce the affected checks using the existing pinned environment:

```sh
.venv/bin/python -m devtools.verify --workers 1 \
  tests.test_candidate_checkpoint_chain_v1 \
  tests.test_candidate_checkpoint_head_v1 \
  tests.test_cumulative_observation_profile_v1
```

## Fixed-corpus storage measurement

A fresh storage-only replay copied all 4,109 original E01 journal files, including
582 empty files, in lexicographic filename order. Each input was checked against
the independently audited original inventory before retention. The new journal
passed full boundary validation, close/reopen validation and authenticated reads
of every file. Original candidate code and retained intent contents were never
executed. The original run and its evidence remain unchanged.

| Retained logical bytes | Original E01 | Compact replay |
| --- | ---: | ---: |
| Raw evidence | 17,513,379 | 17,513,379 |
| External checkpoint metadata | 886,042,635 | 1,575,633 |

The new metadata comprises 1,575,089 bytes of genesis/deltas and a 544-byte latest
head: **99.82% fewer retained external metadata bytes** for this corpus. The
separately retained expected prefix used for reopen adds 275 bytes, bringing
that total to 1,575,908 bytes with the same rounded reduction. These
counts exclude filesystem allocation, inode overhead, transient replaced heads
and measurement receipts. They are not physical disk-write measurements.

The replay took 21.64 seconds after output creation: input authentication plus
append took 13.00 seconds, final boundary validation 2.16 seconds, close/reopen
with complete validation 1.84 seconds, and authenticated point reads 4.48 seconds.
This measured interval excludes imports/preflight and final result-file fsync.
It is one host-storage measurement while the original HTTP qualification was
also running. It is **not comparable** to the original 49.66-minute test, which
performed candidate operations and more frequent integrity checks. There is no
whole-executor speedup or full-study runtime estimate yet.

The first driver failed preflight before creating an output because the strict
reader rejected the `/tmp` ancestor symlink on macOS. That script and failure log
are retained. The second driver uses canonical input paths and a distinct output
directory. The complete replay, input census and original-byte preservation are
independently audited; neither attempt adds a product-quality observation.

## Next implementation

Integrate the qualified compact chain and externally held commitments into the
CLI/HTTP owners and raw readers, including authentication before reopened config
is parsed, every consumed raw result, explicit decision boundaries and fallback
cleanup. Then consume the reviewed M4 profile in fresh execution and a versioned
health comparator; keep all diagnostic results alongside decisive assertions.

The durable financial terminal seal remains implementation work. The retained
design preserves six sequential child cohorts with 8/20 roles each, whose union
is 96 identities. It does not require a 96-principal RPC instance or a larger
simultaneous-owner limit. Admission, queued calls, RPC mutation and recovery must
all enforce the seal; pending or unknown charges cannot become a normal seal.

The complete four-milestone/six-trajectory controller, remaining acceptance
observers, exact complete rehearsal, live allocation/payload reconciliation and
fresh cohort evaluation remain required. Historical sources and observations
are preserved. No new model-quality sample, statistical ranking or whole-run
speedup is claimed by this foundation.
