# B02 candidate intake and direct Store execution

`candidate_intake_store_driver_v1.py` executes each registered public development
history in one fresh pinned Docker container with an independently owned 32 MiB
storage volume. `candidate_intake_store_cases_v1.py` supplies the complete
prospective roster and host-side expected histories. The intake and direct-Store
qualification classes each retain their exact roster before execution, one unique
outcome file per case, and a final completion census including cases not run.
Each history has three paused storage captures: after setup, after the action
sequence, and after closing and reopening the Store.

This is infrastructure and observer qualification. It does not authenticate
production source review, establish a ScopePlan, close all 42 requirement IDs,
run independent final acceptance, or add model-comparison samples. There are no
provider calls. Cost and latency of these development controls are not evidence
about swarm performance.

## Execution boundary

The adapter accepts registered evaluator data, never a candidate-defined command
or shell script. Its finite operations call the required Store and JobManager
APIs, the real local file importer, and the offline commit signatures. Named
references preserve actual returned objects for mutation tests while a JSON copy
preserves each historical result before later mutation. References, fixture
paths, operation names, payload sizes and total operation count are validated
before creating an output or dispatching Docker.

Input fixtures are read back and bound to a retained manifest, then mounted
read-only at `/inputs`. The complete candidate source is independently hashed,
staged, read back and mounted read-only at `/workspace`; the evaluator adapter
and operation-only recipe are mounted at `/checks`. Expected answers, assertion
labels and decisions remain on the host. Fixtures can contain ordinary files,
directories, symlinks or FIFOs without following links while staging or verifying
those inputs. No candidate Python is imported on the host.

The sleeping container supervisor runs as root to support Docker pause/capture;
the candidate adapter runs as UID/GID 65534. It has no network, a read-only root
filesystem, dropped capabilities and no privilege escalation. Candidate calls
are untrusted observations. They cannot provide their own successful verdict or
replace the independently captured storage bytes.

Before/after/reopened frames, raw stdout/stderr, command arguments, exact image
and daemon inspection, paused-container inspections, complete tar captures and
cleanup commands are retained with lengths and hashes. Cleanup now retains both
`docker rm` output and an empty exact-name container listing, in addition to
volume ownership, removal and absence. A failed or truncated command, missing
capture, timeout or unverified cleanup cannot become reusable correctness evidence.
There is no automatic retry and an existing output directory is never overwritten.

## Controlled interference and unknown observations

The interference operation temporarily interposes only the required public
`Store.start_job` or `Store.fail_job` call. At that boundary an independently
opened Store performs its declared cancel/retry or prepare operation before the
original public call resumes. This is a reproducible ordering at a specified
boundary, not overlapping-process concurrency or an exhaustive race census.
Ordinary two-connection calls and imports remain separate histories.

A conforming Store can prevent instance-method interposition. Failure to install
or restore this evaluator hook is an observation limitation, not a product
failure. The adapter records the unavailable operation, stops its dependent
operations with explicit `not_run` records, and still captures the physical
state. The scorer preserves independent earlier failures and masks expectations
that require the unobserved transition. Unknown storage layouts or an unreviewed
schema likewise remain unavailable.

Reopening the same adapter's Store is not a process-death or power-loss test.
Pausing after a response is not an atomic observation at the exact return instant.
The candidate shares a process with the adapter's public API calls; method
results and signature/interposition observations are not authenticated
attestations. Their source review and host-captured storage checks are separate
parts of the qualification boundary.

## Bounds and representation profile

The new B02 session admits 16 MiB of total response output across its three
frames. The new B02 tar parser admits 32 MiB per captured storage file and 32 MiB
of total extracted file bytes, within a 40 MiB complete tar envelope. These are
explicit new implementations; frozen B01 globals and behavior are unchanged.
Source admission retains its separate 8 MiB/file and 16 MiB/complete-source
limits. Input fixtures allow 1,024 entries and 8 MiB; the recipe allows 12 MiB of
JSON data and 1,024 operations. All normative at-cap and one-over-cap intake
fixtures fit those input bounds. Session and observer controls check prospective
expected encodings, including legal control-character escaping at the aggregate
content limit, before physical qualification.

`candidate_intake_store_observer_v1.py` provides the versioned larger storage
bounds. `candidate_intake_store_profile_v1.py` derives the authored v2 profile's
schema and permitted auxiliary rows from source-reviewed literal declarations
and independent expected histories. It retains and checks all auxiliary tables,
including legitimate document/revision/control/counter writes. It does not apply
B01's unchanged-auxiliary-table assumption to document-creating intervals, ignore
unknown private state, or impose this authored SQLite representation on every
valid product. The profile metadata explicitly says qualification-only and does
not constitute production review authority.

## Physical qualification

The intake and direct Store classes can run concurrently under the root runner's
bounded worker budget; neither starts an inner pool. Every roster entry uses its
own mutable state and raw evidence. Five separately declared source defects test
archive rejection, directory-symlink rejection, stale-epoch admission, illegal
cancel and false success after controlled interference. Each control must exhibit
its intended wrong returned state or token, as well as fail the corresponding
host checks; a crash or unrelated exception does not count as sensitivity.

The exact final roster, source/definition hashes and measured durations belong
in the qualification receipt and completion census. Hundreds of containers and
three captures per history are expected; the original smaller-matrix estimate was
7–10 minutes and grows with the finalized scope. A census records physical
histories separately from the runner's unittest method count. Previously
successful unchanged B01 controls are not rerun to inflate this cycle's evidence.
