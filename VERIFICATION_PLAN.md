# Verification efficiency audit and implementation plan

Prepared September 30, 2026; implementation and required qualification completed October 1, 2026. The full plan now has a central fast-first runner, explicit inventory, retained evidence and bounded scheduling, additive sustained/swarm/probe contracts, recovery entry qualification, historical compatibility, scoped telemetry, and source release/CI configuration. The frozen release passed all 665 project methods (656 offline and nine Docker), 13 independent report methods and source-bound browser checks. Its unchanged warm offline confirmation reused receipts without launching test workers. Complete sustained/swarm v2 rehearsals and the probe v2 diagnostic are qualified, with expected candidate failures distinguished from harness failures. Release packaging uses an exact export of main; external publication and hosted CI await a configured destination. [VERIFICATION.md](VERIFICATION.md#implementation-and-acceptance-status) maps each acceptance requirement to its implementation and evidence boundary; [AGENTS.md](AGENTS.md) contains the active working rules.

The sections below preserve the original audit and proposal. Their counts and present-tense descriptions refer to that audit snapshot, not the expanded implementation. Conditional optimizations are not missing acceptance requirements: deliberate independent observations and process restarts remain; no test was removed without evidence that its distinct defect coverage survives. Performance claims remain limited to the recorded workloads and measured scopes.

The main recommendation is to introduce one explicit verification workflow with cheap failure gates, classified test lanes, reusable infrastructure, retained evidence, and bounded parallel execution. Optimize repeated preparation and demonstrably redundant executions while preserving the checks that establish isolation, integration correctness, scientific independence, and exact-source acceptance.

This is a source audit, not a performance benchmark. The findings below identify actual control flow and configuration; savings remain hypotheses until measured. The concurrently developing adaptive study must be included in the inventory before implementation. This audit covers the existing sustained-study implementation and does not certify code added by that other workstream after inspection.

## 1. What was reviewed

| Surface | Current state | Required disposition |
| --- | --- | --- |
| Python development tests | 297 test methods across 31 modules: 278 in `tests/`, 11 in `simulation/`, 8 in `analysis/`. Nine methods are Docker opt-in; the other 288 are nominally offline, but many perform real Git or subprocess work. | Inventory each method/class by invariant, boundary, setup, resource needs, and cost. Count discovered, selected, passed, failed, skipped, and not run separately. |
| Static checks and tooling configuration | `pyproject.toml` contains packaging only. No project type/lint configuration, central test runner, dev dependency lock, CI workflow, or root Git repository was found at inspection. | Add a minimal verification entry point and pinned development configuration. Keep CI wiring conditional on the eventual repository/CI home. |
| Experiment verification | Local, integration, release, rehearsal, final, historical, diagnostic, and supplemental checks are embedded in the harness and analysis runners. | Give every execution a purpose and dependency boundary; do not equate all occurrences of a test with duplication. |
| Evidence confirmation | Git bindings, receipt schemas, source/suite hashes, selection reconstruction, ledger reconciliation, retention and inspection commands. | Keep these cheap integrity checks distinct from executing candidate code. Fail early before export, Docker, or provider work where possible. |
| Report and browser verification | Independent report has 13 Python test methods and five standalone Playwright scripts. Browser scripts repeat common layout, navigation, return-link and state-preservation checks. These are outside the 297-method inventory. | Keep the report independent, but give its tests a reproducible pinned environment and one parameterized browser suite. |
| Working rules | No physical AGENTS.md was found at the project root, in the inspected source/test directories, or along its ancestor chain. The supplied personal rules require a progress report and prefer Playwright, but prescribe no test ordering. | Add project-specific verification rules using the proposed text below; preserve the personal progress-report and browser preferences. |

Current source roots were inspected directly. Historical `runs/` and `results/` contain many frozen copies, generated repositories and archived tests; they must not enter ordinary test discovery, static checks, source search, or coverage denominators. One current test intentionally depends on a small retained artifact; preserve it explicitly rather than excluding all historical data blindly ([test_research_fixture.py:54](/Users/westbrook/Documents/repos/gossip-protocol/tests/test_research_fixture.py:54)).

## 2. Findings that should drive the work

### A. There is no fast-first development path

The README quickstart runs a real Git matrix before unit discovery and describes that matrix as taking several minutes ([README.md:31](/Users/westbrook/Documents/repos/gossip-protocol/README.md:31)). This is a useful demonstration, but a poor default development gate. Ordinary discovery also runs the full 16-case Git matrix inside `setUpClass`, before many cheap modules in alphabetical order ([test_experiment.py:13](/Users/westbrook/Documents/repos/gossip-protocol/tests/test_experiment.py:13)). Types and lint have no configured place in the workflow ([pyproject.toml:1](/Users/westbrook/Documents/repos/gossip-protocol/pyproject.toml:1)).

**Plan:** separate demonstration commands from development verification; run syntax, type and lint checks before environment setup or costly tests. Establish a usable type baseline first, with one type checker and one lint tool, pinned versions, explicit roots, persistent caches, and no blanket new ignores. Do not introduce two overlapping type checkers. Measure cold and warm cost; a fast focused regression may run alongside static checks, but expensive lanes wait for their prerequisites.

### B. Discovery is incomplete and the Docker command repeats offline tests

The documented discovery commands cover `tests/` and `simulation/`, omitting eight safety tests in `analysis/test_sustained_review_probes.py` ([README.md:33](/Users/westbrook/Documents/repos/gossip-protocol/README.md:33), [analysis test:23](/Users/westbrook/Documents/repos/gossip-protocol/analysis/test_sustained_review_probes.py:23)). The Docker example selects all of `test_blackbox_validator`, repeating its eleven mock tests as well as running six container tests ([README.md:409](/Users/westbrook/Documents/repos/gossip-protocol/README.md:409)).

**Plan:** discover every owned root explicitly and compare discovery against a generated inventory. Assign each test to one execution lane, unless a declared runtime matrix or repeatability study warrants another execution. Select the Docker class directly. Explicitly unset the Docker opt-in variable for offline lanes; a developer's inherited environment must not silently turn a cheap command into integration work. Missing required lanes and unexpected skips must not produce a complete-green result.

### C. Independent validators remain serial after parallel generation

Sustained portfolio generation is parallel, but four initial proposals are retained and validated serially ([sustained_stage.py:358](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/sustained_stage.py:358)). Swarm baseline, added-case, and held-out matrices are also sequential ([swarm_experiment.py:260](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/swarm_experiment.py:260), [287](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/swarm_experiment.py:287), [323](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/swarm_experiment.py:323)). Supplemental probes loop through independent final sources ([run_sustained_review_probes.py:214](/Users/westbrook/Documents/repos/gossip-protocol/analysis/run_sustained_review_probes.py:214)).

**Plan:** split immutable job preparation, bounded execution, and deterministic result reduction. Use one validation resource budget across nested work. Give each running job its own validator and output path: `BlackboxValidator` only promises sequential reuse and mutates `last_receipt` ([blackbox_validator.py:210](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/blackbox_validator.py:210)). Do not submit the existing state-mutating `proposal()` callback concurrently. Preserve reviewer dependencies, result order, deadlines, accounting and held-out barriers.

### D. Unchanged source is recognized only after another execution

The sustained stage parses the proposal, retains source and runs validation before calculating source identity and `same_source`. Empty or invalid proposals may therefore reevaluate the unchanged prior source ([sustained_stage.py:243](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/sustained_stage.py:243)).

**Plan:** compute identity before retention/execution and reuse eligible visible receipts under a deliberately versioned policy. Keep invalid source, invalid completion control, passing source, and a valid completion claim as separate facts. A passing old source does not make an invalid proposal acceptable. Changed valid source with malformed control can still need tests to produce repair feedback.

### E. Some apparent duplicates are independent observations by design

Sustained finalization executes final visible, final hidden, and the hidden suites on each historical stage. For a completed trajectory, final hidden and historical stage-three hidden use the same source and suite ([sustained_experiment.py:299](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/sustained_experiment.py:299)). The diagnostic explicitly allows those executions to differ under nondeterminism ([sustained_pool_diagnostic.py:295](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/sustained_pool_diagnostic.py:295)). Pilot local, integration and release gates may likewise overlap, but team integration changes the combined source tree ([pilot.py:250](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/pilot.py:250)).

**Plan:** make each repetition either an authoritative validation, an independent acceptance observation, or a named repeatability check. Record its reason. Future contracts may replace an unnecessary historical duplicate with a reference to the authoritative receipt. Preserve frozen protocols and results. Never substitute subsystem passes for a combined-tree check or present reused evidence as a new observation.

### F. Environment disposal and evidence disposal need different policies

The matrix and discovery integrations offer retained-output options; pilot and recovery integration tests always use temporary directories ([test_experiment.py:17](/Users/westbrook/Documents/repos/gossip-protocol/tests/test_experiment.py:17), [test_discovery_integration.py:105](/Users/westbrook/Documents/repos/gossip-protocol/tests/test_discovery_integration.py:105), [test_pilot_integration.py:48](/Users/westbrook/Documents/repos/gossip-protocol/tests/test_pilot_integration.py:48), [test_recovery_experiment.py:236](/Users/westbrook/Documents/repos/gossip-protocol/tests/test_recovery_experiment.py:236)). This can throw away the artifacts needed to diagnose a failed expensive run.

**Plan:** retain failure receipts, logs, exact input fingerprints, and relevant generated source in a consistent per-run directory. Re-run assertions against retained evidence when candidate reexecution is unnecessary. Reuse installed tools, pinned Docker images, trusted immutable fixtures and browser processes. Keep candidate containers, mutable databases, worker worktrees and browser contexts isolated. Fresh experiment directories preserve evidence; they do not imply the reusable tool environment must be rebuilt.

### G. There are concrete setup and subprocess hotspots to measure

The workflow fixture test source implies 142 Python startups across four methods (one initial call, 108 cumulative-stage calls and 33 mutant calls), with elapsed cost unmeasured ([test_sustained_queue.py:22](/Users/westbrook/Documents/repos/gossip-protocol/tests/test_sustained_queue.py:22)). A nominally fast heartbeat test contains a fixed 1.1-second sleep ([test_pilot_control.py:56](/Users/westbrook/Documents/repos/gossip-protocol/tests/test_pilot_control.py:56)). Git file extraction starts `cat-file` per path ([gitstore.py:333](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/gitstore.py:333)). Docker invocations perform staging and verified cleanup, including removal/probing around `--rm` ([sandbox.py:145](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/sandbox.py:145)).

**Plan:** measure first. Batch immutable file reads and trusted setup, keep expensive classes intact when sharding, and move ordinary clock-transition tests to controlled time/events while retaining one real heartbeat proof. Preserve fresh-process persistence coverage and fresh candidate execution. The blackbox runner already batches a whole suite in one container and uses fresh child processes per case; do not erase that isolation for a nominal speedup. Optimize cleanup only if absence and daemon responsiveness remain established.

### H. Browser setup and common assertions are duplicated across scripts

The independent report's `browser_check.cjs`, `experiments_check.cjs`, `swarm_check.cjs`, `swarm_pilot_check.cjs`, and `sustained_pilot_check.cjs` each launch Chromium. Four hardcode port 4178; all rely on the installed Playwright bundle rather than a colocated reproducible install manifest. They largely repeat desktop/mobile fit, anchor resolution, return-link flag, query preservation, no runtime errors, and untouched user feedback checks.

**Plan:** parameterize common page contracts in one suite; retain page-specific content assertions. Reuse one browser per worker and create isolated contexts/fixtures per test. Reuse a healthy report server whose project identity and serving directory match; otherwise launch an owned isolated fixture server on an allocated port. Do not restart or terminate the user's report server. Store artifacts by run, test and viewport rather than overwriting shared PNG names. Enable traces/screenshots on failure and selected changed-page visual checks; do not regenerate every historical screenshot for a text-only edit. Pin the browser revision as well as the package, and clean up resources in `finally` paths.

### I. Some confirmations fail later than necessary or repeatedly reread everything

Recovery checks the live ledger cap after Docker preflight ([recovery_experiment.py:507](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/recovery_experiment.py:507)). The older retainer creates and copies its destination before source/accounting gates ([retain_experiments.py:41](/Users/westbrook/Documents/repos/gossip-protocol/retain_experiments.py:41), [81](/Users/westbrook/Documents/repos/gossip-protocol/retain_experiments.py:81)). Supplemental probes reread every frozen input before preflight, before each source, and after the run ([run_sustained_review_probes.py:179](/Users/westbrook/Documents/repos/gossip-protocol/analysis/run_sustained_review_probes.py:179)).

**Plan:** perform cheap read-only shape, ownership, ledger, contract and path checks before costly preparation. Build exports transactionally after validation. Measure whole-input scans; consider execution from a verified immutable snapshot with explicit pre/post integrity checkpoints. That changes mutation-detection timing, so retain current fail-closed behavior until the new contract proves equivalent or declares the difference. Keep integrity assertions; eliminate redundant materialization, not their purpose.

### J. Optimization changes can invalidate existing scientific proofs

Rehearsal contracts hash shared runner, worker, sandbox and other sources; current-source checks also protect historical retention and probes ([sustained_experiment.py:33](/Users/westbrook/Documents/repos/gossip-protocol/gossip_harness/sustained_experiment.py:33), [retain_swarm.py:284](/Users/westbrook/Documents/repos/gossip-protocol/retain_swarm.py:284)). Swarm has a request-start deadline; making validation faster can change whether another provider request starts.

**Plan:** separate development tooling changes from experimental execution changes. Run historical audits with their verified frozen runner when supported, and add an explicit versioned replay path before modifying shared behavior. Reuse an exact valid rehearsal until its contract changes. Run one new exact-contract rehearsal after the changed implementation stabilizes and cheap gates pass. Record concurrency, reuse policy, timing limits and evaluator version in new experiment contracts; do not mix old and optimized runs as equivalent samples.

## 3. Proposed work process

These lane names describe the interface to implement; they are not commands that exist today. Keep `unittest` initially; changing test framework is not a prerequisite for scheduling and measurement.

| Stage | Run when | Scheduling and stop rule |
| --- | --- | --- |
| Plan the verification set | Before editing and after scope changes | Map changed surfaces to required checks and dependencies. Assign one execution owner so collaborating agents do not independently rerun the same suite. Read previous matching receipts first. |
| Static gate | First coherent edit, then as edits arrive | Syntax/import-shape, types, lint and configuration validation. Run independent cheap tools together if resource cost is small; preserve caches. Stop dependent expensive launches on failure. |
| Focused fast tests | Alongside or immediately after the cheap gate | Pure logic, mocked controls and the nearest regression checks. Fail fast during editing. A passing test only covers its actual source/dependency fingerprint. |
| Consolidated offline batch | Once a coherent change set is ready | Explicit discovery across all roots; process-isolated module/class shards, with shared class setup performed once. Separate pure tests from real Git and trusted fixture subprocess tests. Run independent affected shards together within one global resource limit. |
| Docker integration batch | After cheap gates and affected offline prerequisites | Preflight once per session/image/daemon identity, then execute selected real integration tests with one bounded scheduler. Preserve fresh candidate containers and unique mutable fixtures. Record all started work and cleanup on failure. |
| Browser batch | After changed HTML/JS or report behavior is stable | Run changed pages and common contracts once in a shared browser session; independent of unrelated Docker work if measured resource limits allow. Server-side review persistence tests use disposable report state. |
| Exact rehearsal | Only for changed execution contracts or missing/invalid proof | Wait until the contract is frozen; run its required offline experiment once, retain it, and reuse that proof for matching execution. A normal documentation edit does not require another study. |
| Evidence and completion | At the meaningful handoff | Check receipt/source/config bindings, required coverage, retained output, accounting and unresolved failures. Rerun only invalidated work. A sweeping change requires full applicable coverage, but an already completed check with an unchanged complete fingerprint need not run twice. |

Do not block a useful cheap regression merely to keep categories together. Batch expensive work at dependency boundaries, not by an arbitrary wall-clock schedule. A test→fix→test loop is appropriate after a real failure; an unchanged repeated full suite needs an explicit reason.

For a harness change, start with static and affected contract tests, then the consolidated offline batch, then the relevant Docker batch. For report-only content, use static link/content checks and changed-page browser verification; no Docker study. For shared sandbox/receipt/protocol changes, widen to all affected consumers, boundary tests and a new versioned rehearsal. Uncertain dependency mapping falls back to broader verification.

## 4. Reuse, parallelism and evidence rules

**Reuse key.** Bind reusable validation to exact source/tree bytes, ordered suite and oracle identity, evaluator/adapter/runner version, immutable image/runtime, relevant environment, seed, command/protocol, limits and evidence purpose. Store original execution ID, physical versus reused status, outcomes and provenance. Paths or modification times alone are insufficient. Never cache infrastructure failures as correctness results; negative behavioral reuse is permitted only for an explicitly deterministic, identical contract. Receipt reuse must be disabled for independent acceptance or repeatability observations.

**Environment lifecycle.** Maintain one session manifest recording owned and borrowed processes, tool versions, server identity, cache directories, selected lanes and run artifacts. Recheck health after an actual failure, daemon/image change, restart or contract mismatch—not before every candidate. Keep immutable seed preparation; reset or recreate mutable test state. Retain declared failures and bounded successful evidence under an explicit cleanup policy. No automatic deletion of historical study evidence or user state.

**Concurrency.** Start benchmarking at one and two validation workers; test four only if CPU, memory and I/O headroom support it. Current containers request one CPU and 256 MiB each. Reserve capacity across outer test shards and inner experiment pools; do not multiply their independent worker counts. Process isolation is required for test modules that patch `sys.modules`, environment or global fixture registries ([test_swarm_control.py:109](/Users/westbrook/Documents/repos/gossip-protocol/tests/test_swarm_control.py:109), [test_swarm_fixture.py:93](/Users/westbrook/Documents/repos/gossip-protocol/tests/test_swarm_fixture.py:93)). Serialize shared ledger publication and other conflicting resources. Use deterministic aggregation and preserve causal ordering even when executions finish out of order.

**Cancellation and retries.** Cancel superseded queued work; mark stale in-flight results by fingerprint rather than accepting them for newer files. Drain or safely stop owned jobs with verified cleanup, and account for every started request. No blanket retry-to-green: separate infrastructure failures from assertion failures and record every retry. Never terminate borrowed servers or another chat's active experiment.

**Keep existing good behavior.** Preserve `setUpClass` amortization, the recovery runner's bounded replay pool, exact broker-response replay, incremental reviewer/fuzz matrices, once-per-run preflight, receipt-bound promotion, host-side expected answers, Git scope checks/CAS and deliberate new-PID recovery tests. These are useful reuse or correctness mechanisms already present.

## 5. Confirm that each test earns its cost

Create an inventory record per test or expensive assertion group with: invariant/requirement; primary owning layer; distinct defect caught; trust boundary; fixture and isolation scope; affected inputs; expected result; cost/timeout; measured setup/run/cleanup; flake history; retained output; and reason for any overlap or repetition.

Use that inventory to classify each check as **keep, move earlier, batch, parallelize, reuse preparation, reuse evidence, consolidate, or remove**. Similar names or equal passing results are not enough to remove a test. Cheap mocks, real Docker boundaries, merged-tree integration, negative fixture mutants, scientific final scoring and receipt integrity have different jobs.

For a proposed consolidation, identify the unique defect it currently detects and demonstrate that the remaining check still catches that defect with a small controlled mutation or established regression case. Run targeted mutation probes only for these decisions, not a new whole-project mutation suite on every edit. The earlier supplemental probes found defects despite primary passes; strengthen relevant failure assertions while reducing wasted execution, rather than optimizing for fewer tests or a smaller green count.

## 6. Implementation sequence and acceptance

| Priority and work package | Deliverables | Acceptance |
| --- | --- | --- |
| P0 — inventory and baseline | One versioned verification manifest; explicit roots, lane ownership and resource tags; runtime fingerprints and timing receipts. Include newly added adaptive-study tests before freezing the inventory. | Every owned test accounted for exactly once per intended runtime; unexpected skips visible. Measure a clean run, a warm unchanged run, and a seeded early failure without provider calls. |
| P0 — fast gates and working rules | Project AGENTS.md, revised README ordering, pinned dev tooling, one verification entry point; types/lint baseline; Docker flag isolation. | Seeded syntax/type/config failures prevent dependent expensive setup. Analysis tests are included. The Docker lane runs only real integration tests and does not repeat mock methods. |
| P1 — session and artifact lifecycle | Retained failed integration outputs, immutable fixture reuse, explicit owned/borrowed process management, shared browser suite in the independent report workspace. | A failure is diagnosable from retained artifacts without rerunning candidate code. One browser launch per configured worker/batch. Existing report remains usable. No mutable state leaks between cases. |
| P1 — bounded scheduling | Process-isolated offline shards; shared Docker capacity budget; per-job validators; stable result merge; timeouts and cancellation. | Same required outcome set on deterministic fixtures at concurrency 1 and 2; no missing/duplicate receipts, races, oversubscription failures or leaked processes. Adopt higher concurrency only after measured benefit. |
| P2 — targeted runtime reuse | Pre-execution identity, explicit visible-evidence reuse, batched Git reads, measured scan/export improvements; versioned evaluator and replay boundary. | Reuse rejects changes to every key component; physical execution counts fall for repeated identical eligible requests. Independent final observations remain distinct. Old retained studies stay auditable. |
| P2 — remove proven redundancy and tune | Invariant coverage matrix; retained regression/mutation evidence for consolidations; final timing comparison and documentation. | No unique required defect detection lost; improved warm critical path and first-failure latency without worse timeout/flake behavior. No speedup claim unless measured on the same workload and hardware. |

Implement one package at a time and reuse passing receipts between packages when their complete fingerprints remain valid. Independent documentation/inventory work can proceed in parallel; scheduler changes and receipt-policy changes require separate validation so failures remain attributable.

The baseline should record wall and CPU time, p50/p95 where sample count supports them, first-negative latency, setup/run/cleanup/queue time, Git/Python/Docker/browser startups, physical executions versus logical checks, cache hits with reasons, bytes staged/read, peak CPU/RSS, lock wait, retries, skipped work and cleanup status. Use a small fixed repetition count on representative local offline fixtures; do not launch paid studies to benchmark the verification runner.

Set numerical budgets after the first baseline. Required initial guarantees are structural: an early static failure starts zero dependent expensive jobs; repeated mock tests are eliminated from the Docker lane; retained artifacts survive failure; all required tests remain covered; and browser or Docker reuse does not leak mutable state. A tuning change that improves average throughput but materially worsens failure latency or timeout rate should not be accepted by default.

## 7. Proposed AGENTS.md addition

This is proposed text for a new project rule file, not an active instruction or an applied change:

```text
Verification workflow

1. Before implementation, identify the affected verification lanes, dependency
   boundaries and one execution owner. Read existing matching receipts and the
   progress-report handoff. Do not rerun work solely because a chat changed.
2. Run cheap syntax, type, lint and configuration checks at the first coherent
   edit and keep them early throughout work. Reuse their tool caches. Do not
   launch dependent costly checks while a prerequisite is failing.
3. Run the nearest fast regression checks during editing. Once a coherent batch
   is ready, group affected offline, Git, Docker and browser checks by their
   prerequisites. Revisit a lane only for changed inputs, a failure, missing
   evidence, or a declared independent observation.
4. Use the central runner and its explicit test roots. Keep Docker opt-in unset
   for offline lanes. Do not treat missing required lanes, unexpected skips, or
   a partial run as a full pass.
5. Parallelize independent work through the shared resource budget. Isolate test
   modules that patch globals in worker processes; keep heavyweight class setup
   together. Give concurrent validators separate instances and unique outputs.
6. Reuse installed tools, pinned images, trusted immutable fixtures and healthy
   owned or borrowed servers. Use isolated browser contexts and fresh mutable
   candidate execution state. Do not stop/restart tools merely to change lanes.
7. Retain useful failure logs, receipts, exact source/config identities and
   fixtures. Preserve historical study evidence. Reuse eligible exact-bound
   receipts with explicit provenance; never call reuse a fresh execution.
8. Keep integration checks on the combined tree, sandbox boundaries, receipt
   integrity, final-evaluation barriers and deliberate restart tests. Version
   changes to scientific evaluation, reuse, concurrency or deadline policies.
9. Prefer reproducible Playwright tests for browser behavior. Pin package and
   browser versions; batch changed-page checks in shared browser workers with
   isolated contexts. Preserve canonical feedback and user review checkpoints.
10. At handoff, reconcile every required lane with current source/config
    fingerprints. For sweeping changes require full applicable coverage; reuse
    still-valid results and rerun only invalidated or missing checks. Report
    failures, skips, reused evidence and limitations separately. Do not repeat
    successful checks just to produce a longer validation list.
11. Keep the progress report current at meaningful work boundaries. Only the
    user marks items reviewed. An existing authorization does not need another
    confirmation merely because a test lane changes; ask only for a genuinely
    missing decision or authorization. A review receipt is not approval to spend,
    publish, or change a scientific protocol.
```

When implementing these rules, replace lane/tool placeholders with the actual supported entry point and keep README examples synchronized. Do not mandate unavailable type checks before their environment and baseline exist. Retain the supplied personal preferences unchanged.

## 8. Review boundary and next action

The useful next step is P0: make the verification inventory and measurements explicit, then add the cheap gates and scheduling rules. The more consequential changes—receipt reuse, parallel candidate validation, and scientific repeatability policy—follow that baseline and get their own versioned correctness checks.

Audit verification: direct source inspection; AST counts of current tests; three independent code reviews; review of the existing report handoff, browser scripts and empty feedback queue at audit boundaries. No full test suite, Docker experiment, provider request, tool installation or performance benchmark was run for this planning task. Timing and actual historical restart frequency therefore remain unmeasured. This plan does not claim that every source-level repetition caused wasted time in previous chats.

The canonical shared report remains at [Progress Report](http://127.0.0.1:4178/). This audit is a separate workstream; its completion does not mark the concurrently active adaptive study complete.
