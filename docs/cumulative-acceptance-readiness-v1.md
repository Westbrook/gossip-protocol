# Cumulative acceptance: observed limits and next implementation

The original HTTP v3 experiment has completed 19 of its 20 methods. Independent
raw audits are clear for those 19 completed methods, with their stated limits.
The final 87-step mixed CLI/HTTP history remains running at this checkpoint.
This is harness qualification, not product acceptance or new comparative agent
quality evidence. No new provider work was launched in this cycle.

The [machine-readable checkpoint](../analysis/cumulative-acceptance-readiness-v1.json)
records evidence identities, retained reviews, measured costs and remaining work.
The earlier [C06 observation bridge](candidate-observation-admission-v1.md) remains
an offline-qualified, limited adapter. Its six authored physical controls have
not run. Review found contract changes needed for the actual cumulative study,
so the next physical qualification should exercise the coherent successor.

## Completed extension evidence

E01–E05 retain 5,039 journal files and 5,039 external checkpoints. Independent
review authenticated all 77 original probes, 89 completed recipe steps, 88
container creations/starts/removals and five owned volumes. E01 includes all
70 requests; E02 checks real per-epoch roots, authored links and state continuity.
Those links escape the selected root while remaining inside `/inputs`; this
provides no product-confinement verdict. E03/E04 cover exact
65,536/65,537-byte bodies.

E05 sent its full 65,680-byte request. The server returned a complete 425-byte
413 response reporting zero consumed body bytes. That distinguishes transmission
from consumption; it does not establish physical incomplete-send coverage, which
remains unobserved. A consolidation-only audit script error was retained and
corrected without rerunning the experiment or raw component audits.

## A measured scaling problem

| E01 measurement | Observed value |
| --- | ---: |
| Requests | 70 |
| Test elapsed time, including verification | 2,979.76 seconds (49.66 minutes) |
| Journal files | 4,109 |
| Journal payload | 17,513,379 bytes |
| External checkpoint snapshots | 4,109 |
| External checkpoint bytes | 886,042,635 bytes |

Each retained file causes a full journal read/hash inventory and a newly stored
full prefix snapshot. The source therefore implies at least 8,443,995 journal
file visits for E01's per-write checkpoint path alone. This is a source-derived
operation count, not a measured syscall count or function-level time profile.
The whole five-method worker recorded 2,737.34 observer CPU seconds, including
2,166.13 system seconds; those totals do not isolate a particular routine.

External snapshots are outside the existing raw-journal quota. Their E01 bytes
are about 50.6 times the journal payload. The full HTTP catalog has 276 histories
and 7,602 requests before repeated purposes or trajectories. Full-catalog time,
storage and any optimization speedup remain unmeasured; multiplying E01's time
per request would ignore different history lengths and repeated-prefix costs.

The next checkpoint contract should retain one bounded delta per new raw file,
with its exact name, byte count, content hash, sequence, previous head and full
execution-context identity. An independently retained head anchors the prefix.
Raw artifacts, durable intents, original outcomes and cleanup evidence stay
intact. Full byte/inventory checks remain mandatory before side-effect dispatch,
before trusting observations, at reopen and at final acceptance. Every intervening
read must authenticate the actual bytes it consumes.

This is an explicit contract change. It moves detection of unrelated file tamper
from every intermediate write to mandatory boundaries or authenticated reads.
Tamper restored entirely between those observations may escape detection.
A local hash chain alone cannot prove freshness; independently retained anchors
and strict rollback/foreign-suffix checks remain necessary. Boundary scans can
still have growing-prefix costs. The change needs its own tamper, crash,
retention-failure, no-redispatch and resource qualification, with measured I/O;
no speedup or equivalence to the old contract is claimed yet.

## Concrete final-acceptance gaps

| Gap found in current sources | Required integration |
| --- | --- |
| CLI v5 and HTTP v4 bindings require M1; the complete compiler requires M4 | A versioned execution-subject and compatibility profile, followed by fresh M4-bound observations. Old M1 receipts cannot be relabelled. |
| Inherited expectations can change at M4, including health schema 0 becoming schema 4 | Independently review inherited and successor clauses, purpose and applicability before enabling the profile. A milestone parameter alone is insufficient. |
| CLI gates currently expose all 900 cells; 32 unspecified cells become skipped, which the Registry treats as incomplete | Keep all cells in diagnostics and explicitly identify normative decisive assertions. Unspecified cells must neither become passes nor permanently block legitimate acceptance; required unknowns still block it. |
| Release v2 and C06 use different complete-source digest domains | Adopt a common prospective identity in a versioned fresh execution, or verify an explicit complete Git/blob identity mapping without erasing original bindings or purposes. |
| The Registry permits 512 gates | Count the complete reviewed purpose/gate/case workload first. Duplicating all 57 CLI and 276 HTTP histories for two purposes alone would yield 666 gates; this is a conditional illustration, not the final required census. Qualify a sufficient bounded version if needed. |
| Financial close/recovery can resume work; the pilot's freeze is local and in memory | Implement a durable normal terminal admission seal, distinct from an integrity halt, enforced atomically at admission, queued invocation, RPC and recovery. |

The six trajectories contain 96 namespaced role identities in total. The current
RPC owner permits 64 principals. The original plan allows randomized sequential
trajectories on the cumulative ledger, avoiding a new simultaneous-cohort
capacity contract. Unique identities, per-trajectory envelopes and accounting
must still be retained; sequential execution does not weaken the all-six barrier.

## Next coherent implementation

1. Define the exact final-M4 profiles, independently reviewed applicability,
   compatibility and source identities. Count the complete required gates and
   preserve every unresolved obligation instead of inventing a complete mapping.
2. Implement and qualify compact externally anchored checkpoints and the narrow
   versioned profile/binding changes. Preserve the frozen v3 experiment and
   add6c41 sources/receipts. Reuse mechanics where valid; version changes do not
   require duplicating whole executors by default.
3. Add the irreversible financial admission seal and a controller-owned durable
   registration/cohort authority. Authenticate all six terminal trajectories,
   four milestone histories, final Git sources, unresolved work, financial
   originals and promotion checkpoints before any private evaluation.
4. Qualify the combined successor offline, then run its complete physical
   positive, defect, restart and boundary controls once and independently audit
   originals. The six old C06 controls remain explicitly unexecuted preparation;
   their needed behaviors must be represented in the successor's declared scope.

The full 123 product requirements, three prerequisites, 312 source obligations,
22 qualification authorities and 188 inherited gap notes remain in scope. Real
semantic scope review, remaining browser/storage/concurrency/recovery/release
observers and the four-milestone peer-local/central controllers are still required.
A durable authority that accurately reports those missing parts is useful, but
is not an accepted product or a completed study.

After implementation, the original six-trajectory contract still needs a fresh
complete matching zero-provider rehearsal, funding/payload reconciliation, all
live trajectories and independent final acceptance. The later placement-by-
transport comparison and held-out project-family confirmation remain separate
preserved obligations. This checkpoint adds no statistical sample or swarm
advantage claim.
