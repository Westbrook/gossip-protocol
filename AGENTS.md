# Working in this project

## Progress and coordination

Use the `progress-report` skill at
`/Users/westbrook/.agents/skills/progress-report/SKILL.md` for substantive work.
Read `.progress-report/project.json`, the current handoff, and unresolved feedback
before planning. Reuse the independent report and its existing browser tab.
Update it at meaningful work boundaries; preserve other active workstreams and
explicit user review checkpoints. Only the user marks work reviewed.

Assign one verification owner for a change set. Collaborating agents may run
focused checks for their own edits, but must share results and fingerprints so
the owner can avoid redundant whole-suite runs. Do not edit another active
workstream's files without coordinating ownership. Do not stop its processes.

## Verification order

The central entry point is `.venv/bin/python -m devtools.verify`. Its manifest is
`verification-manifest.json`. Use `--list` to inspect coverage and supported lanes.

1. Identify changed surfaces and required lanes before implementation. Read
   matching retained receipts first; changing chats is not a reason to repeat
   completed checks.
2. Run syntax, types, lint and configuration checks at the first coherent edit.
   The runner executes the static gate before launching tests. Reuse the pinned
   environment and checker caches; do not reinstall tools on every invocation.
3. Run the nearest fast regression during editing. Once a coherent change set
   is ready, batch independent affected tests by dependency and resource needs.
   An actual failure justifies a fix-and-rerun loop; unchanged successful work
   needs an explicit reason to run again.
   During parallel edits, contributors run scoped cheap checks and report their
   source hashes. The verification owner runs the combined static gate once the
   batch is coherent. An unrelated contributor's unfinished type error does not
   justify repeating the global gate or blocking a nearest cheap regression;
   it does block dependent costly integration and final qualification.
4. Complete applicable offline checks before costly integration or rehearsal.
   Keep `GOSSIP_RUN_DOCKER_TESTS` unset in offline lanes. Select real Docker
   classes through the Docker lane rather than rerunning mixed mock/Docker
   modules. Missing required lanes and unexpected skips are not full success.
5. Run browser checks after the changed page or behavior is coherent. Prefer
   reproducible Playwright tests with pinned package/browser versions. Batch
   pages in shared browser workers with separate contexts and fixture state.
6. Reconcile the required checks with current source/configuration fingerprints
   at handoff. Sweeping changes require full applicable coverage. Reuse still
   valid results; rerun invalidated or missing checks. Report failures, skips,
   reused receipts and unverified surfaces separately.

Static tools have explicit coverage in `pyproject.toml`; do not imply that
incremental type coverage proves every legacy dynamic function type-safe. New
test classes must have an explicit manifest record. Keep `tests/`, `simulation/`
and `analysis/` discovery complete. Exclude generated `runs/`, historical
`results/`, dependencies and snapshots from ordinary discovery, except the
explicit small retained fixture dependencies named in the manifest.

## Parallelism and environment lifetime

Use the runner's bounded resource budget. Isolate modules that patch globals in
worker processes; keep expensive class setup together. Account for nested pools
instead of multiplying outer and inner worker counts. Give concurrent validators
their own instances and outputs, and aggregate results deterministically.

Reuse installed tools, pinned images, trusted immutable fixtures, and healthy
servers whose project identity matches. Distinguish owned from borrowed services;
never stop a borrowed report server or restart tools just to change test lanes.
Keep mutable databases, candidate execution state and browser contexts isolated.
Fresh candidate containers and intentional process-restart tests remain required.

Retain useful failure logs, receipts, source/config identities and fixtures. Keep
historical study evidence and failed integration artifacts. Do not reset the
cumulative ledger. Never delete or overwrite an existing output to make a rerun
succeed. Artifact cleanup requires an applicable retention policy or request.

## Evidence and experimental contracts

Receipt reuse requires exact source, ordered suite, evaluator, runtime/image,
environment, limits, seed, protocol and purpose binding. State whether a result
was physically executed or reused and link its original execution. Infrastructure
failures are not reusable correctness judgments. Do not reuse an observation
whose purpose is independent acceptance or repeatability.

Preserve checks on merged source, sandbox boundaries, receipt integrity, Git
scope/CAS, final-evaluation barriers and deliberate restart behavior. A local
subsystem pass does not validate a combined tree. Version changes to scientific
evaluation, concurrency, reuse or deadlines; preserve frozen study sources and
proofs. A matching rehearsal can be reused; a changed execution contract needs
one new complete rehearsal after cheaper gates pass.

Do not automatically retry failures until green or launch paid experiments to
benchmark the development workflow. Existing authorization does not need another
confirmation merely because a test lane changes; ask only for a genuinely
missing decision or authorization. Review receipts alone do not authorize
spending, publishing or changed scientific scope.
