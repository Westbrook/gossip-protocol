# Cumulative M3 reference application

This is the trusted authored recovery/export milestone for the Local Research
Library benchmark. It makes the planned software project executable through M3;
it is **not a model-generated implementation, a comparative trial, or evidence
that swarm coordination is superior**. The full four-milestone project and six
matched trajectories remain open.

The machine-readable evidence checkpoint is
[`analysis/library-m3-reference-checkpoint-v1.json`](../analysis/library-m3-reference-checkpoint-v1.json).
The normative prospective contract remains
[`library-cumulative-product-v1.json`](../library-cumulative-product-v1.json),
with its existing SHA-256; this reference does not privately amend that contract.

## Result and qualification boundaries

The final combined run passed **82/82 checks** in 11 classes, including static,
offline, Docker and browser gates, with no skips, reused classes or stale input
bindings. All 352 retained input hashes matched current source.

`gossip_harness.library_m3_reference_v1.m3_files()` composes 32 generated files
from independently owned overlays. Frozen M1/M2 source generators and immutable
adapter inputs remain unchanged. The final Store combines backup, maintenance,
worker and control behavior, preserving inherited manual intake, original job
receipts, lifecycle editing and clients.

The central verification runner runs static checks and offline classes before
fresh pinned Docker and browser work. Exact final counts, source fingerprints,
execution paths and outcome hashes are in the checkpoint. All checks recorded
there as physical were actually executed; component checks are not substituted
for combined-tree qualification. Retained earlier runs are not deleted when a
later review requires another change.

Docker qualification exercises the same combined M3 tree against eight inherited
M1 histories, twelve independently authored M2 histories and ten independently
authored M3 histories. The M3 input/expected definitions were frozen before
reference execution, without their author reading the M3 implementation or its
tests. Deliberately failing variants omit the restore edit-token increment or
ignore worker enrollment; the relevant independent histories must reject them
as `wrong_answer`, rather than accepting infrastructure failure as detection.

Real desktop/mobile Playwright runs exercise the unchanged M2 workflow and M3
maintenance controls on separate fresh fixtures. They include seven canonical
browser downloads, empty-versus-all selection, deleted-document/history choices,
exact byte boundaries and failure without a partial download. They also cover
explicit worker enrollment, an actual worker CLI process, backup and restore,
stale generations, preserved dirty drafts for both retained and removed records,
and explicit discard. Actual reopened CLI and HTTP response values are retained,
not just a persistence pass label.

These observations are authored-reference qualification. The separate private
observer is not yet wired into authenticated whole-cohort candidate acceptance.
The exact generic candidate crash schedules, receipt adapters, coverage and
promotion barriers still require qualification before using this as a scientific
study endpoint.

## Recovery behavior implemented

- Schema 0/2 migration retains logical data and original serialized receipts,
  initializes schema 3 controls transactionally, and rolls back on failure.
- Durable incarnation, owner generation, job epoch and document high-water
  controls prevent old workers and old edit/job tokens from regaining authority.
  Restore retains fencing tombstones even for IDs removed from the logical set.
- Explicitly enrolled jobs execute in deterministic order under a lifetime owner
  lock. Fresh worker processes resume the persisted manifest without rereading
  input files. Terminal jobs are never automatically retried.
- Reindex steps visit at most 64 documents, persist their cursor, restart when
  the catalog generation changes, and atomically publish derived state. Queries
  continue to use the authoritative catalog throughout.
- Exports validate a complete selection from one snapshot and enforce canonical
  UTF-8 byte limits before clients emit a result.
- Backups validate the complete logical graph and immutable original receipts.
  Durable ownership precedes hidden unique `.partial` staging, fsync and
  no-overwrite publication. Recovery proves publication ownership using the
  retained stage and the final file; it never claims an unrelated final file.
- Restore validates before taking activation authority, checks the current
  catalog generation, and replaces the logical set in one SQLite transaction.
  It rotates incarnation and advances document/nonterminal-job fences, while
  preserving completed job receipt bytes.
- Bounded cleanup visits at most 64 durable artifact records per pass, retains
  a cursor, checks live owners, and leaves unknown files and published backups
  untouched. Registry recovery commits before the retained stage is removed.
- Diagnostics report one snapshot and actual live-worker lock state. A later
  successful operation clears only its own prior error. Stale owners cannot
  overwrite current diagnostics.

Authored subprocess tests actually terminate workers before/after the declared
public commit boundary and terminate backup/restore owners at publication and
activation boundaries. SQLite authorizer failures and reindex publication
failures exercise rollback. These use declared reference interposition; they
must not be described as independent crash acceptance of arbitrary candidates,
physical power-loss proof, or exhaustive filesystem-fault coverage.

## Explicit implementation policies and open contract decisions

The reference adds a local `control.backup_root` provenance value; it is excluded
from logical backups. `None` still means `backups` beside the database, including
`Service(..., backup_dir=None)`. No constructor executes jobs or runs cleanup.
A custom directory must be supplied before an owner starts recovery. If pending
artifacts belong to another directory, switching roots fails closed with
`maintenance_busy`; cleanup never guesses where an old relative path belongs.
Existing handles also check their root binding before backup metadata reads or
operations. Changing roots after pending artifacts are cleared resets the
root-relative metadata registry and preserves every file. Returning to an old
root does not automatically rediscover those files; a valid manually supplied
backup can still be restored. Multi-root registry retention is not specified by
the public contract and needs an explicit decision before broad deployment.

Ordinary handles that predate a restore reject writes with `stale_worker` until
reopened/adopted; that exact error is an authored strategy, not a new requirement
imposed on other implementations. Absent a live owner, this reference reports
`idle` for enrolled pending work and `stopped` otherwise. Independent histories
do not require this unspecified distinction.

Backup manifests retain their exact serialized bytes. Identical resubmission
compares canonical admitted entry values, so equivalent whitespace/Unicode escape
representations do not fabricate a job conflict. The frozen M1 fixture encodes
content hashes as bare SHA-256 hex strings (or null for deferred malformed
Unicode); the public interchange specification should state that representation
explicitly before general cross-implementation acceptance.

The public contract does not state a maximum for epoch/edit/generation integers,
while the declared SQLite INTEGER storage is signed 64-bit. This reference maps
activation overflow to `io_error` and rolls back atomically; it does not impose an
undocumented private input ceiling. A versioned counter-domain/exhaustion rule
remains required before claiming the full numerical contract is qualified.

## Next work toward the actual experiment

Finish the M4 cumulative migration and independent histories, close the public
fixture and trusted receipt-adapter gaps, and qualify the complete combined
application. Retain peer-local policy versus a matched central control,
generated distinguishing tests, real restart/partition faults, placement versus
transport separation, all six planned trajectories, the whole-cohort acceptance
barrier, and independent held-out confirmation. Quality and completion remain
the primary outcomes; development test duration is not an agent speed result.

No provider calls, key access or API spending are needed for this qualification.
The separately prepared M1 live pilot is still gated on the unanswered explicit
payload-transfer approval after automatic approval review rejected its launch.
Its fixed central phase controller cannot be relabeled as one of the comparative
peer-local versus central study cells.
