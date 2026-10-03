# Candidate intake and direct Store matrix v1

This public development evaluator declares 238 finite histories before candidate
execution: 121 directory/ZIP/JSON histories and 117 direct Store/manager histories.
It names a facet of each of the 42 B02 target IDs in the cumulative coverage map.
Those counts are design coverage, not 42 completed requirements, candidate passes,
independent final acceptance or comparative model samples.

The expected values were derived from the frozen `V0_REQUIREMENTS` and
`M1_REQUIREMENTS` strings, the M1 acceptance inventory and cumulative product v2.
The authors and normative reviewer did not inspect candidate/reference
implementations or candidate outputs to choose answers. Fixture generation uses
standard-library archive/JSON construction and independently specified text.
The source-reviewed auxiliary mapper is a separate qualification component; its
private-layout knowledge does not define the universal expected product outputs.

## Recipes and observations

`candidate_intake_store_cases_v1.py` exports `definitions`, `case_definition`,
`execution_recipe`, `definition_sha256`, `definition_sources` and `evaluate_case`.
`candidate_intake_fixtures_v1.py` declares the intake fixture bytes and histories.
The cache contains immutable encoded definitions; every accessor returns fresh
objects and refuses after a loaded definition dependency changes on disk.

Each recipe has explicit bounded fixtures and ordered `before`, `after` and
`reopened` operations. Only fixture bytes and invocation data enter the candidate
container. Expected states/results stay in the host. A call names a whitelisted
real Store or JobManager method; references retain actual return objects for
mutation, while historical observations copy their value at return time. The
reopened phase creates a new Store handle. It is not process death.

Every history seeds a known existing document and a separate queued job. Each
paused storage capture independently observes the complete declared document,
blob and job sets, immutable manifests/content hashes and receipts. A snapshot
with an orphan blob fails even if public counts or returned errors look correct.
New jobs and terminal receipts are compared with independently computed expected
values. Existing manifest/hash bytes and unchanged receipt bytes must survive.

The six ambiguously timed JSON classes described below place admission, an
independent second Store lookup and manifest lookup in the `before` phase. That
capture is explicitly **after admission and before prepare**. The `after` phase
then calls ordinary prepare. Other histories retain their declared before/action
meaning; `snapshot_semantics` makes this exception machine-readable.

The direct matrix contains all three methods (`start_job`, `fail_job`,
`commit_job`) × five states × lower/current/higher legal epochs: 45 histories.
Every state is reached through actual public operations at epoch 2, so the lower
value 1 tests stale fencing rather than invalid epoch 0. Separate input-domain
cases cover zero, negative, overflow, bool, float, string and null. Further cases
exercise cancellation/retry/prepare states, all required missing-job methods,
replay/conflict in every state, every required Store return category, original
input mutation, multiple token cycles, and a running prepare after a real
second-connection source conflict.

Five interference histories interpose only at the required public
`Store.start_job` or `Store.fail_job` call. The one-shot instrumentation restores the original method before
a second connection performs cancel/retry or prepare and the original call
resumes. `boundary_calls=1` counts that evaluator interposition event, not all
product method invocations. They establish those
controlled orderings, not a scheduler race census. Separate two-client
submission/cancellation histories are also explicitly ordered.

## Exact boundaries and ambiguity dispositions

The intake set includes empty formats, recursive discovery, literal HTML/newline/
Unicode data, parser-specific schema and syntax failures, namespace/member path
classes, filesystem and ZIP symlinks, accepted regular/missing ZIP type bits,
nonregular/encrypted ZIP members and duplicate source names. It exercises
64/65 files, 32768/32769 decoded member bytes, 524288/524289 total bytes, legal
1048576/1048577-byte ZIP archives, 16/17 combined segments, and inherited key and
segment byte limits. Each rejection isolates a decisive invalid class.

Two complete outcomes are prospectively permitted for six valid-schema JSON
classes: duplicate sources with equal/different text, excess count, excess member
bytes, excess total bytes, and escaped unencodable text. The frozen wording says
invalid discovery/archive/JSON performs no admission, but expressly defers
semantic **source** errors; it does not resolve those other classes' timing.
Following `CLARIFY-UNSPECIFIED-ERRORS`, these cases do not invent a uniquely
required phase:

- An immediate submit error must have the exact prescribed code, no admitted
  job, `not_found` from both lookups and the later prepare probe, and no catalog,
  blob or prior-job mutation.
- A submit success must return the exact queued job; the immediately captured
  state must contain its exact manifest/hash representation, and the independent
  lookup must agree. Prepare must then return the exact prescribed failure and
  persist failed/completed-zero/error while conserving catalog and blobs.

The host binds the branch to the observed submit result, then checks the complete
conjunction. Error-with-job, success-without-job, wrong later error and mutation
are explicitly rejected. These cases do not map the ambiguity to `M1-I29`.
Actual source-path/depth and unsupported-suffix JSON cases remain strictly
queued-then-failed. Raw invalid UTF-8, JSON syntax and JSON schema cases remain
strictly immediate. This disposition was declared before physical execution.

Directory FIFO discovery must reject, but the inherited text does not assign a
unique code to that filesystem class. Its assertion requires an actual domain
error and exact state conservation, without accepting an unexpected exception.
Nonstring namespace classification, duplicate JSON object-field parsing,
malformed ZIP filename encoding, arbitrary `fail_job` code validation and direct
Store hook argument types remain explicitly ungraded. Commit keyword-only/default
behavior is graded; actual injected rollback is separate retained B01 evidence.

ZIP ratio fixtures are valid deflate streams: 1199/12, 1200/12 and 1201/12
uncompressed/compressed bytes. Integer comparisons avoid floating-point boundary
ambiguity. Ignored directory payloads participate in resource accounting; empty
stored members have zero numerator and denominator, evaluated using max(1, C).
For these legal fixtures, every C=0 member also has U=0. Summing U_i <= 100 C_i
for positive-C members proves the aggregate bound, with an all-empty archive's
ratio 0/1. Therefore no aggregate-only excess can be isolated while every member
satisfies the bound under identical accounting. This is not a theorem about
arbitrary forged metadata with U>0 and C=0.

## Bounded scoring and qualification limits

A legal worst-case JSON fixture contains 524288 decoded U+0001 bytes. JSON escaping
makes its fixture about 3.1 MB and the largest phase's returned JSON about 6.3 MB;
all three phase result envelopes total 9,444,199 bytes. Expected case metadata is
larger and remains host-side. This case motivated a separately versioned B02
storage observer with sufficient row/normalized bounds; frozen B01 is unchanged.

Scoring uses strict typed JSON equality, so bool and integer are distinct. Extra
or missing results/jobs, orphan blobs, forged success flags, changed immutable
strings and illegal branch mixtures fail specific checks. Assertion selectors
name concrete result indices and storage snapshots; **all required case checks
form one conjunction**. A mutation operation returning null alone never proves
fresh-value behavior.

Unsupported public-method interposition is observation-unavailable, not a product
failure. The adapter stops dependent operations, retains earlier observations and
marks remaining calls not run. Local checks use true/false/null with reasons:
independent earlier failures remain false, while the unavailable action and its
dependent state expectations become null. A mixed or unavailable observation is
never accepted or reused as a correctness judgment. Registration/schema mismatch
is a precondition error. The independently reviewed auxiliary profile similarly
masks dependent checks; it derives permitted private control transitions from the
prospective history, never by copying observed post-state.

No result here closes browser/HTTP/CLI behavior, safe rendering or attempted
network reads, genuine process death, all concurrency timings, all Store
signatures, counter-exhausted durable-state fixtures, all later milestones,
production scope/review authority, cohort barriers or later held-out families.
Storage registration and interposition suitability require separate authority.
Source fingerprints, raw captures, protocol, purpose and physical execution must
still be bound before any production acceptance consumer can use these facets.
