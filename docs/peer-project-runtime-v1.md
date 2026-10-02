# Peer project runtime v1

This additive runtime connects one provided coding task to public Git promotion.
It composes the existing coding authority, payload mesh, candidate publisher and
receiver promotion bridge. It does not generate a project plan, call a live model,
provide independent final acceptance, or complete the registered scientific
comparison. All earlier v1 runtimes remain unchanged.

The CLI module is `gossip_harness.peer_project_runtime_v1`. Every process has
`root`, `node_id`, `mode`, `token`, `port`, `interval`, `fanout` and `role` fields.
The role determines the remaining trusted configuration:

| Role | Additional fields | Local work |
| --- | --- | --- |
| `peer` | `seed` (provided JSON or null) | Event and payload dissemination |
| `candidate-worker` | `coding`, `authority`, `candidate` | Coding, then durable proposal construction and publication |
| `promotion-service` | `authority`, `authority_port`, `task_specs`, `profiles`, `promotion` | Coding authority plus serial public validation and promotion |

`candidate` contains an exact `baseline_sha`, `allowed_paths` and `generation: 0`.
`promotion` contains the baseline, public host-oracle `cases`, pinned local image
ID, evaluation limits and an explicitly declared `initially_enabled` gate.
Repositories are fixed at each process's own `root/repository.git`; the candidate
and promotion journals are fixed private descendants. Bootstrap may create
independent Git clones before launch. The runtime checks baseline ancestry and
binds configuration, repository location and runtime identity across restarts.
Restoring an ephemeral TCP port does not change this identity.

The candidate timer continues after the coding peer's local step. It consumes a
completed, source-bound result only after the necessary bytes arrived locally,
constructs the exact proposal and publishes a real bundle payload followed by its
candidate offer. The wire contains hashes and manifests, never a sender path.
The service receives its own copy, imports it into its own quarantine, verifies
the authoritative dispatch binding, validates the actual merged source using
`BlackboxValidator`, and performs the existing fenced Git/ledger promotion.

The service owns two bounded TCP surfaces and a separate serial promotion thread.
Its event/payload timer continues during Git and Docker work. Observer state uses
small cached summaries: it never waits on the validator lock and omits source
files, case suites and large receipts. It reports up to eight recent attempt
summaries plus counts; full evidence remains in the durable promotion journal.
The `promotion_gate` observer operation changes only the declared timer's durable
enabled flag. It neither executes work synchronously nor resets failed or
terminal attempts. There is no observer action or promotion RPC.

The only coding transport remains the closed offline Responses fixture injected
into the real `OpenAIWorker` adapter. No API credential is discovered and no real
provider request is made. Candidate Python runs only inside the pinned Docker
container; the comparison oracle stays on the host. Docker prerequisites and the
image are supplied separately; the runtime neither starts Docker nor pulls an
image. Public promotion and task completion in this fixture remain distinct from
private project acceptance and a successful scientific study.

The dedicated Docker test class retains separate fixture directories for:

- Gossip and broker success through a divergent receiver branch. The public
  oracle requires both the generated change and the receiver's independently
  changed helper. Neither offered-only nor receiver-only fixture behavior meets
  the expected outputs. The sender process is killed and its repository renamed
  after the receiver has the full offer and bytes, before promotion is enabled.
- A wrong-answer proposal that the isolated host-oracle validation rejects,
  leaving the accepted Git head and task completion unchanged.
- A real service exit after Git CAS, followed by exact journal recovery. The
  retained validation receipt, container identity and offline coding entry must
  remain unchanged after restart and additional local timer rounds.

The service restart check occurs after successful evaluation and verified
container cleanup. It does not cover SIGKILL during an active `BlackboxValidator`
call: killing the parent can bypass its timeout and cleanup handlers. Durable
container tracking before launch, startup cleanup and a watchdog remain necessary
to qualify that stronger recovery guarantee.

Each fixture retains process configurations/logs, journal and quarantine evidence,
an offline transport entry trace, and a `pipeline-receipt.json` binding the offer,
bundle, exact merged source, validation, accepted commit and ledger state. These
are complete public-integration rehearsals, not the registered cohort's untouched
scientific rehearsal or independent final-project acceptance.

The largest topology has five owned daemon processes: service, worker, source and
two relays. The service has a mesh timer, a serial promotion timer, an authority
accept loop and bounded TCP handlers. At most one candidate container is active
in a fixture. Native Git, Docker CLI and in-container supervisor/child processes
add bounded work; the verification runner's conservative ten-slot exclusive
budget is a scheduling bound, not a literal thread count. Successful and failed
artifacts are retained. No test lane is implied to have passed merely by the
existence of this runtime or documentation.
