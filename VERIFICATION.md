# Development verification

Run verification from this project directory. The workflow uses `unittest` and
keeps the scientific study runners independent from development tooling. Setup
requires `uv`, Git and Node 20+; Node supplies the JavaScript syntax gate as well
as the optional browser runner.

## Start once, reuse the environment

```sh
scripts/bootstrap-dev
.venv/bin/python -m devtools.verify --list
.venv/bin/python -m devtools.verify
```

Bootstrap creates Python 3.11.15 only if the environment is absent, then installs
the exact packages and transitive hashes in `requirements-dev.lock`. It reuses
both the environment and `.cache/uv`; ordinary verification never installs tools.
Python 3.11 is the primary development interpreter; candidate execution uses the
study's separately pinned Python Docker image. Dependencies remain development
dependencies, not a new application runtime requirement.

The default command runs the offline `fast`, `fixtures` and `git` lanes. Every
invocation performs the static gate before launching tests. Syntax and config
failures stop before tool/test setup; Ruff and mypy run concurrently after syntax
passes. Tool caches persist under `.cache/`.

`verification-manifest.json` explicitly assigns test classes to lanes, resource
weights and timeouts. Discovery includes `tests/`, `simulation/` and `analysis/`;
new or missing classes fail configuration rather than disappearing from the
count. Historical snapshots and generated repositories are excluded. The small
accepted-project fixture needed by `test_research_fixture` and the two committed
supplemental case files are fingerprinted explicitly.

## Select the work that changed

```sh
# Cheap gates and all fast tests.
.venv/bin/python -m devtools.verify --lanes fast

# One class or test, with the static gate still enforced.
.venv/bin/python -m devtools.verify tests.test_transport.TransportTests

# A consolidated offline batch; reuse still-valid passing class evidence.
.venv/bin/python -m devtools.verify --reuse

# Actual container checks only; mocks stay in their offline lane.
.venv/bin/python -m devtools.verify --lanes docker

# Independent report state tests and one consolidated browser batch.
.venv/bin/python -m devtools.verify --lanes report,browser --workers 8
```

Use `--list` to find exact selector names. Selectors accept module paths or
`path/to/test_file.py::Class.test_method`. An empty selection is an error.
`--lanes all` includes optional integrations; it does not run paid model calls or
launch scientific rehearsals. A targeted result is labeled targeted, not a full
project pass. Keep Docker running and the exact configured image already present
before requesting Docker; the tools never start Docker or pull images implicitly.

The report's deliberate eight-writer contention test reserves eight slots, so
use `--workers 8` with the report lane or `--lanes all`. This is a test of real
concurrent writes; it is not reduced to fit the offline default.

The default resource budget is four slots. `--workers N` changes that budget,
including declared inner parallelism. The legacy Git matrix needs four slots
and runs exclusively in the default budget. Selecting it with fewer slots fails
before execution; use narrower selectors or a budget of at least four. Trusted workflow, inventory, build-graph and calendar fixture classes also reserve
at least four slots and the full configured budget while active: their nested
CLI processes have fixed three- to four-second deadlines, and concurrent startup
contention caused real failures during qualification. New cross-version handoff
fixtures with five- to ten-second CLI deadlines use the same exclusive policy.
Ordinary classes run in separate processes, so patched globals and environments
cannot leak to peers. Class setup runs once for its selected methods. Offline checks
clear inherited Docker opt-in.

The scheduler fills available slots with fitting jobs from the current lane; a
large waiting job does not strand usable capacity. Explicit exclusive jobs and
lane boundaries remain barriers. Prior lanes complete before dependent costly
lanes start. On a failure, queued
work is not started; already running work retains its results and cleanup state.
Unexpected skips, missing results, stale inputs and incomplete coverage are
visible failures. There are no automatic retries to turn a failure green.

## Read and reuse evidence

Each command creates a unique session under `runs/verification/` (or the parent
specified by `--output`). Session files record selected/discovered counts, source
and runtime fingerprints, static output, class logs and results, timings and
observed direct subprocess starts. Phase measurements distinguish class setup,
test execution, cleanup and scheduler wait. They do not claim to measure child
processes that the observer cannot see.

`--reuse` reuses only intact successful offline class results with an exact
matching complete fingerprint and method selection. Intact passing classes from
a failed batch remain eligible; failed classes always need a new physical run.
Controller errors, changed inputs or unknown cleanup invalidate reuse. Source/configuration (including authored study-plan JSON),
runtime/tool environment, selection or evidence changes invalidate reuse.
Runtime identity is checked again after started work drains; a mismatch or an
unreadable final identity fails the session and makes its evidence ineligible
for reuse. Identity checks are boundary observations, not continuous monitoring
of transient changes that revert between checks. Executable bytes are hashed;
installed Python packages bind versions and RECORD metadata, assuming dependency
payloads are not manually altered behind that metadata.
Reused results retain a reference to their original execution and do not count
as new physical tests. Docker, browser and independent acceptance observations
are not silently replaced by this development cache. No cache hit converts
missing coverage into a full pass.

Expensive Git/Docker integrations retain their artifacts. Failures keep their
traceback and source/receipt evidence even when invoked directly with unittest.
The central runner supplies `GOSSIP_TEST_ARTIFACTS` so evidence lives with the
session. Existing `GOSSIP_LAB_TEST_OUTPUT` and `GOSSIP_DISCOVERY_OUTPUT` exact-path
overrides remain supported, but their destinations must be fresh. Small owned
successful fixtures can be cleaned; shared state and previous outputs are never
removed. No automatic historical-artifact cleanup policy is introduced.

Development-test reuse deliberately uses a conservative fingerprint of the
whole authored tree. An unrelated source edit can invalidate those receipts;
the runner does not guess dynamic imports, generated fixtures, or scientific
source-signature dependencies. Focused selection avoids unnecessary new work
while this dependency boundary remains conservative. Inventory supports directly
declared unittest methods; inherited/dynamically generated tests require an
explicit adapter rather than an assumed count.

## Static coverage and legacy type debt

Correctness lint and syntax cover all authored Python roots. Mypy covers new
`devtools` code, the explicitly listed legacy roots in `pyproject.toml`, and their
imports. `devtools/type-baseline.json` records individual preexisting diagnostics
from unchanged scientific sources. This is incremental coverage, not a claim
that every existing function is type-safe.

The gate validates each baseline source hash. A new diagnostic, a changed
diagnostic, any error in new tooling, or a changed baseline source fails. It does
not automatically regenerate the baseline or ignore a module. Fix type debt when
editing the affected legacy source; review any intentional baseline migration
as a source change. The receipt discloses the accepted diagnostic count and the
actual typed roots.

## Browser verification without server churn

Browser tests live in `devtools/browser/`; the independent report still owns its
server, UI, data and 13 state tests. Package and browser revision are pinned in
the browser package files. With a normal Node installation, prepare once:

```sh
npm ci --prefix devtools/browser
npm --prefix devtools/browser run install-browser
```

For CI or containers, provide the independent report workspace and configure its
location in `.progress-report/project.json`; it is intentionally outside this
project. Provision the pinned browser and its operating-system dependencies once,
then use the owned fixture server on its allocated port. The GitHub Actions
workflow is in `.github/workflows/verification.yml`; its independent report and
runner prerequisites are documented in [.github/README.md](.github/README.md).
Remote execution awaits the publication destination and its configuration.

The local Codex environment can reuse its already provisioned Playwright package
through `NODE_PATH`; no installation is required when the pinned package and
browser are present. Browser tests never download dependencies themselves.

```sh
node devtools/browser/run.cjs --pages report,research,experiments
node devtools/browser/run.cjs --pages all --fixture
```

By default the suite borrows the server identified by
`.progress-report/project.json`, verifies its project and content identity, and
leaves it running. `--fixture` creates an owned independent report snapshot on an
allocated port and stops only that owned process. A batch launches one Chromium
and uses separate contexts for each page. Common contracts are shared, while
page-specific assertions remain. Failures retain traces/screenshots with unique
names. Explicit signal handling closes owned resources, preserves a partial
failure receipt, bounds hung cleanup and leaves borrowed servers running. Six
Node lifecycle cases run through the default Python inventory without launching
a browser. Canonical feedback and explicit review state are never mutated by tests.

## Explicit validation batching and scientific boundaries

The optional `validation-session-v1` adapter is a new offline execution contract:

```sh
.venv/bin/python -m devtools.validate_batch \
  devtools/examples/validation-batch.json \
  --output runs/my-validation-batch \
  --cache runs/validation-cache
```

The manifest contains exact source text, ordered cases, pinned image, limits,
resource budget, purpose, protocol and seed. Cheap input validation precedes
Docker. One session preflight is shared, each physical job gets a separate
validator/container, and results are reduced in manifest order. Resource limits
account for declared outer concurrency. Completed and cancelled jobs remain
accounted for in the receipt.

Only explicitly deterministic `visible` jobs can share an identical in-flight
execution or reuse intact persisted evidence. Independent `final` and
`repeatability` jobs always execute and require a stated reason. Infrastructure
failures are never cached as correctness evidence. The key includes exact source,
ordered oracle, adapter/support digests, runtime, image, environment, limits,
seed, protocol and purpose. Source changes during the batch invalidate results.
The adapter evaluates source; it does not confer valid proposal or completion
status on a caller's control record.

Existing scientific protocols retain their original execution ordering and
independent observations. They are not monkey-patched to use this adapter. A new
study must explicitly register the adapter, concurrency/reuse policy and any
deadline effects, then run its new exact-contract rehearsal. The old studies
remain auditable using their retained source snapshots. Data-only frozen-source
preparation is available through `python -m devtools.frozen_sources --help`; it
does not execute archived candidate code or fabricate a new rehearsal.

## Evidence exports

`retain_experiments.py` now validates accounting and commit bindings before
writing an export. Exact accepted Git objects are read with one NUL-framed tree listing and one
size-framed blob batch, with each blob hash verified. It assembles a complete
sibling staging directory, checks
that source evidence stayed unchanged, and atomically publishes only to a fresh
destination. A failure removes only its owned staging directory and leaves the
final destination absent. Existing destinations and concurrent evidence remain
protected.

See [VERIFICATION_PLAN.md](VERIFICATION_PLAN.md) for the original audit and
[AGENTS.md](AGENTS.md) for the active development policy. Measured implementation
results are recorded separately under `runs/verification-implementation/` and in
the shared progress report; expected savings are not treated as measurements.

## Earlier development-tooling baseline and limits

Seven small offline Docker batches used the existing pinned image. Three
alternating samples per setting had medians of **7.234 seconds with one worker**
and **5.490 seconds with two**, about 24% lower elapsed time on this workload.
Each fresh batch had three logical checks, two physical executions and one
shared preflight. The warm batch reused both eligible visible checks and still
physically executed the independent final check (4.787 seconds). This small
fixture is not a project-wide speedup estimate.

One matched eight-file Git export sample reduced subprocesses from **10 to 2**
with identical output hashes; observed elapsed time was 2.619 versus 0.522 seconds.
That single serial-then-batch sample has uncontrolled cache effects. The
subprocess-count reduction is structural; the observed time ratio is not a
general benchmark.

The eight registered report/research pages passed in **9.8 seconds with one
Chromium launch**. The final owned-fixture lifecycle also passed. A real SIGTERM cancellation
exited 143 in 0.573 seconds, preserved a failed receipt and left none of its
recorded Chromium or fixture-server PIDs alive. Six lightweight Node cases and
the Python detached-child regression cover lifecycle failure paths. No borrowed
server was stopped.
New pages introduced by other workstreams need their own browser contract; the
eight-page receipt does not certify arbitrary future pages.

That earlier baseline covered wall time, first failure, setup/run/cleanup/queue
time, direct subprocess starts, declared resource tokens, reuse and cleanup.
It did not measure CPU/RSS, lock wait or byte counts. The continuation described
below adds explicitly scoped telemetry; those measurements are not retroactively
attributed to earlier receipts.

Shared legacy study runners retain their original scientific contracts. Applying
the new adapter to sustained/swarm/probe runs, changing independent observation
policy or relaxing integrity-scan timing requires an explicitly versioned study
contract and its new rehearsal. Those protected migrations are not silently
included in the development-tooling implementation.

The largest passing physical worker costs in the frozen offline qualification
were:

| Class/lane | Worker seconds | Observed direct child starts |
| --- | ---: | --- |
| Build-graph fixtures | 681.5 | 205 Python |
| Git-store boundary checks | 631.1 | 1,037 Git |
| Calendar fixtures | 497.9 | 881 Python |
| Recovery protocol, Git lane | 486.4 | 943 Git + 24 Python |
| Shared 16-scenario Git matrix | 459.5 | 1,696 Git |

The matrix spends 459.499 seconds in shared class setup and 0.011 seconds in its
seven assertions; preserving that shared setup is essential. The build-graph
counter excludes further CLI children inside its observed adapter processes.
Controller wall time can also include startup and collection overhead beyond
these worker times. The retained `frozen-offline-costs.json` profile links all
64 class/lane measurements to hashed physical evidence. These observations show
where future profiling matters; they do not justify deleting required boundary,
restart or persistence checks.

The main tooling qualification ran 26 checks (18 runner contracts, seven external
adapter contracts and one wrapper for six Node lifecycle cases). The cold
batch passed in 61.660 seconds. An unchanged rerun passed in 4.789 seconds with
**zero test workers started** and all three class receipts reused; the static
gate still ran. This measures the same frozen source and runtime, not a cache
that ignores edits.

A final queue correction then passed four focused scheduling regressions in
11.188 seconds: fitting-job selection, exclusive resource reservations, lane
barriers and failure draining. Those checks bind the final runner source; the
26-check cold/warm measurement above predates this small queue correction.

During implementation, the static gate caught a real new type error in
1.370 seconds, before dependent tests were launched. That gate passed with
42 explicitly recorded legacy diagnostics; these are disclosed type debt, not
new errors suppressed by module-wide ignores.

Qualification uses frozen source snapshots because a separate study workstream
is editing the same checkout. The broad snapshot contains 498 project methods
(489 offline and nine Docker); the discovery inventory at that handoff contained 522.
Final tooling changes have separate source-bound checks. The other workstream
reports its 19 new handoff checks passed, but those observations are not relabeled
as this workflow’s central execution receipts or a certificate for the entire
current combined tree. `qualification_at_registration` in the manifest is an
audit note; current execution status always comes from run receipts.

The mixed swarm Git group and post-hoc Git diagnostic passed in 147 and
145 seconds under their inherited 180-second worker watchdogs. Their current
Git-lane watchdogs are explicitly 600 seconds to leave room for measured host
contention; fast swarm controls retain 180 seconds. Candidate/container deadlines
and scientific acceptance timing are unchanged. This is an outer worker limit,
not a relaxation of any test assertion.

The complete frozen offline inventory reconciled at **489/489 passed**. Its
unchanged warm confirmation took **3.042 seconds**, ran the static gate, reused
64 intact class/lane receipts and launched **zero test workers**. The preceding
Git lane physically passed all 82 methods. The initial sweep’s two deadline
failures remain retained; only failed or missing work was executed during
recovery. This is an evidence-reuse measurement, not a controlled cold-versus-warm
end-to-end speed ratio.

A second-generation cache lookup exposed an original-duration display bug. It
now reads the already verified physical result, so successive warm runs preserve
that duration. All **19 final runner regressions passed in 57.542 seconds** after
both the queue and telemetry corrections, with unchanged source hashes. The
old physical receipts were intact throughout; the frozen warm result’s overall
3.042-second duration and coverage counts were unaffected.

The final integration batch passed all **nine real Docker checks and 13 report
checks** in 558.749 seconds, using five class workers, no reused integration
results, no skipped methods and unchanged frozen inputs. The four Docker groups
ran within eight resource slots; the report’s eight-writer contention check ran
only after Docker finished. Existing tools, the pinned image and the borrowed
report server stayed in place.

## Earlier development-tooling receipts

The machine-readable rollup reconciles source hashes and links the original
passing evidence. Failed and interrupted attempts remain retained alongside it.

- [Qualification and final source bindings](/Users/westbrook/Documents/repos/gossip-protocol/runs/verification-implementation/implementation-qualification.json)
- [Full offline warm confirmation](/Users/westbrook/Documents/repos/gossip-protocol/runs/verification-implementation/offline-final/20261001T012718-dcd6c6da/summary.json)
- [Docker and report verification](/Users/westbrook/Documents/repos/gossip-protocol/runs/verification-implementation/integrations-final/20261001T012730-5079e290/summary.json)
- [Final 19 runner regressions](/Users/westbrook/Documents/repos/gossip-protocol/runs/verification-implementation/cache-telemetry-final/receipt.json)
- [All 64 original class/lane cost observations](/Users/westbrook/Documents/repos/gossip-protocol/runs/verification-implementation/frozen-offline-costs.json)
- [Validation batching benchmark](/Users/westbrook/Documents/repos/gossip-protocol/runs/verification-implementation/validation-benchmark-v2/summary.json)
- [Matched Git extraction measurement](/Users/westbrook/Documents/repos/gossip-protocol/runs/verification-implementation/git-read-batch.json)
- [Actual browser cancellation and cleanup proof](/Users/westbrook/Documents/repos/gossip-protocol/runs/verification-implementation/browser-final-20261001T005603Z-1823bb21/proof-summary.json)


## Full-plan continuation

The new entry points preserve original scientific modules and make optimized
execution an explicit, separately qualified contract:

```sh
# Complete known-worker rehearsal; no provider calls.
.venv/bin/python -m gossip_harness.sustained_experiment_v2 run --output runs/sustained-v2-new
.venv/bin/python -m gossip_harness.swarm_experiment_v2 --output runs/swarm-v2-new

# Read the supplemental/early-ledger and historical compatibility interfaces.
.venv/bin/python -m analysis.run_sustained_review_probes_v2 --help
.venv/bin/python -m devtools.qualify_recovery_entry --help
.venv/bin/python -m devtools.historical_audit --help
```

Sustained v2 parses and hashes proposals before evaluation, retains them serially,
batches the independent initial portfolio, and reduces its receipts in order.
Repairs remain dependent singleton batches. Swarm v2 batches baseline, added-case,
and post-selection held-out matrices separately. Supplemental probe v2 snapshots
strictly validated retained inputs once, batches independent sources and verifies
the snapshot after execution. None of these changes permits model feedback from
private or supplemental observations.

The shared study contract binds the complete forward source closure, evaluator,
image, exact deadlines, resource budget, ordered suites, seeds, reuse declaration
and observation purposes. A caller dependency change is detected before passing
results or reusable proofs are published. Rehearsal qualification audits retained
session inputs, receipts and complete study-specific coverage; v1 summary flags
are insufficient. Visible reuse defaults to false because matching arbitrary
source does not establish determinism. `--deterministic-visible` is an explicit
new study-contract declaration, never an implicit optimization. Independent final
and repeatability jobs remain physical and record a reason.

Cancellation stops queued work, drains started validators, records interruption
and prevents later provider phases. Owned child processes are signalled and
reaped; borrowed servers and other study processes are untouched. Actual new-PID
checkpoints and fresh mutable candidate containers remain required isolation.

Recovery v2 inspects existing live-ledger ownership, limits and accounting
read-only before the unchanged legacy preparation path. Historical audits require
a complete independently pinned trusted runtime, not merely archived CORE files;
only the two known audit entrypoints are allowed. They reconcile frozen source,
runtime identity and the auditor's explicit passing result, retaining all output.
Recovery v2 exposes the Python API `run_recovery_experiment`; its entry guard
has a distinct qualification from the unchanged candidate execution. The
`devtools.qualify_recovery_entry` command accepts independently pinned original
proof, execution baseline and focused regression receipts. It checks the full
original import closure, all 24 original Docker executions and their exact
image/checker/cleanup receipts, then physically exercises nine read-only ledger
and entry guard cases. It refuses composition if original execution changes.

This completion used that **composed entry qualification**: 24 original candidate
executions reused, nine guard probes physically run, and zero new candidate,
container or provider executions. It is not a new 24-case rehearsal. The evidence
is [the composed entry receipt](/Users/westbrook/Documents/repos/gossip-protocol/runs/verification-completion/recovery-v2-composed/entry-guard.json).
A live v2 call still requires the original exact rehearsal gate as well as this
source-bound entry proof and its intact referenced evidence.

The historical compatibility run physically executed the fixed original auditor
against its independently pinned 38-file runtime. It reproduced the retained
certificate exactly: six rehearsal cases, 120 recorded requests and 24 billing
tasks, in 561.890 seconds. Python, Git, source hashes and auditor identity remained
unchanged. This performed zero candidate, container or provider executions.
[Historical compatibility reconciliation](/Users/westbrook/Documents/repos/gossip-protocol/runs/verification-completion/historical-actual/qualification-comparison.json).

The separate data-only `frozen_sources` command still does not execute a study.

The configured type gate now includes every new v2 entry point. Its 65 exact
legacy diagnostics bind unchanged imported v1 source bytes; 23 became visible
when adding these roots. New tools and v2 sources cannot enter that baseline.
During parallel edits contributors run scoped cheap checks, and one owner runs
the combined gate when the batch is coherent. This avoids repeatedly launching
the whole gate while another contributor is still editing an unrelated module.

## Resource measurements and inventory

Controller, isolated worker, static-gate and validation-session receipts now
record CPU deltas and platform-correct process-lifetime peak RSS. Child CPU is
separately scoped and can overlap worker observations; never add those fields.
A process-lifetime RSS high-water mark is not an interval-specific peak or a
simultaneous process-tree maximum. Peak CPU and process-tree RSS remain explicitly
unavailable because this lightweight observer does not sample them. It does not
claim to measure Docker daemon/container CPU or physical disk I/O.

Instrumented source/proof reads, retained payload writes, and controller lock
acquisitions record logical bytes and wait time with their exact scope. Unobserved
operations are disclosed rather than assigned zero. Resource tokens describe
scheduling limits, not measured consumption. Current receipt writes occur after
their own telemetry snapshot and are identified as such.

```sh
.venv/bin/python -m devtools.benchmark_validation --output runs/validation-benchmark-new
.venv/bin/python -m devtools.benchmark_validation --seeded-failure syntax --output runs/negative-syntax-new
.venv/bin/python -m devtools.benchmark_validation --seeded-failure types --output runs/negative-types-new
.venv/bin/python -m devtools.inventory_report --output runs/inventory-new.json
```

The benchmark performs three alternating cold samples per concurrency setting
(one and two workers), then one unchanged warm observation against the same
fixed deterministic fixture. Each cold sample has three logical checks, two
physical executions and one single-flight reuse; warm execution reuses the two
visible observations and independently executes the final one. Source or daemon
drift, failed observations, or incorrect accounting stop the benchmark. There
are no retries or provider calls. Three samples support a descriptive median;
p95 is omitted until at least twenty matching observations are available.

The generated inventory joins only explicitly selected receipts to manifest
invariants, expected outcomes, input bindings, setup/run/cleanup costs and retained
logs. Reused observations preserve original physical duration and do not add a
physical sample. Historical timeout failures stay visible. Matching source alone
does not establish current runtime validity or authorize receipt reuse. See
[.github/README.md](.github/README.md) for CI prerequisites and curated release
contents; optional unconfigured lanes are disclosed, not counted as passed.


The continuation benchmark passed all seven batches on unchanged sources. Median
elapsed time was **6.919 seconds at one worker** and **4.089 seconds at two**.
The warm batch took **3.672 seconds**, with two persisted visible reuses and one
fresh final execution. Direct Docker CLI starts were eight per cold batch and
six for warm; those include health/identity/cleanup operations, not eight
candidate containers. The controller measured 2.696 seconds of its own CPU and
a 36,847,616-byte lifetime RSS high-water mark across the complete 36.532-second
benchmark; child CPU is separately reported. External host load and cache warming
were uncontrolled, so this is descriptive workload evidence rather than a causal
project-wide speed estimate.

Seeded syntax failure returned in **0.0011 seconds** with zero child launches;
seeded type failure returned in **1.807 seconds** with two cheap checker launches.
Both produced **zero candidate executions**. These are upper bounds measured
when the cheap gate returned, not precision timestamps of individual diagnostics.
Receipts are under `runs/verification-completion/validation-benchmark-final/`,
`negative-syntax-final/` and `negative-types-final/`.
