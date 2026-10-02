# Foundations for a twenty-role project

The prospective project has sixteen builders and four reviewers. These modules
provide its accounting and evidence boundaries. They do not yet implement or
qualify a twenty-role runtime. The existing studies and their frozen source
inventories remain unchanged.

## What the components do

| Component | Responsibility | Required host behavior |
| --- | --- | --- |
| `peer_project_contract_v2.py` | Immutable context, action, dispatch, local-view, candidate, selection, release and role records; strict bounded encoding and hashing | Authenticate actors, obtain evidence from the actual local store, and materialize the bound content |
| `peer_financial_authority_v2.py` | Asynchronous admission against one existing cumulative ledger and one immutable cohort allowance; durable result/accounting recovery | Supply the trusted ledger, owned payload store and registered worker profiles; keep the lifetime owner lock |
| `peer_review_recovery_v2.py` | Finite corrections of complete schema-invalid reviews, charged to the existing total reviewer allowance | Durably reserve the returned state before dispatch, retain the trusted current checkpoint, and provide known provider outcomes |
| `peer_review_release_v2.py` | Exact package selection, cumulative mandatory checks and eight explicit review scopes for one immutable public release | Authenticate selection, contribution, execution and local-view evidence, retain current history, and separately perform fenced Git promotion |

All financial dispatch in this version is restricted to offline fixture
transports. A user-approved budget alone does not qualify an execution contract.
Changing output directories cannot create a second cohort allowance.

## Accounting and persistence

Capacity waiting creates no reservation or paid backlog. An available executor
admits an exact action and lease under the joined ledger transaction, checking
the task, cohort and cumulative limits. Provider work runs outside the transaction
and caller's handler. All registered trajectories and milestones share the same
cohort allowance.

An admitted request retains its identity and reservation when the caller loses
the reply. Replaying that identity cannot invoke the provider again. A durable
known response can settle after its lease expires. A missing or indeterminate
response blocks new work and retains conservative accounting. The existing
request journal supplies local dispatch recovery; it does not claim
provider-side exactly-once execution.

The format-correction policy addresses the completed pilot's malformed reviewer
response without changing that historical result. A correction requires a
complete, known response that failed the declared whole-response schema. It
consumes both a correction allowance and a total reviewer call. Source, target,
local view, ordered suite and schema remain bound to the same immutable context.
Unknown provider outcomes and infrastructure failures do not permit correction.
Semantic rejection remains rejection.

## Evidence and review

Reviewers return a `review.json` data artifact through the existing worker's
patch protocol. The only writable path is `review.json`; candidate source and
execution evidence may appear as read-only context. The host reconstructs
verdict provenance from the actual retained and accounted worker response.
A model-authored claim about its own identity or tests is not a receipt.

The gate requires the registered pair of scopes from each of four distinct
reviewers. Two scope decisions in one response count as one reviewer invocation.
The eight slots cover the four packages and explicit cross-package requirements.
Missing evidence, rejected scopes, unresolved admitted refutations or a failed
mandatory check prevent eligibility. Production authors cannot approve their
own release.

The selected package contents plus registered inherited files must exactly
cover the target source. Named file hashes are SHA256 of their raw bytes.
Component identities use domain-separated canonical JSON; these are distinct
from the existing evaluator's source-digest encoding and the journal's receipt
encoding. Trusted adapters verify the original bytes and their correspondence.

Once a test is admitted, later suites must preserve its identity, contents and
ordering. Changing the target tree, protected base, suite or release generation
requires review of the new target. Unrelated movement on a provisional working
branch does not change an immutable release target. A gate receipt establishes
public-release eligibility; it neither updates Git nor substitutes for
independent final project acceptance.

## What remains before the practical experiment

The next integrated fixture must physically exercise twenty cognitive role
processes, private durable journals and Git stores, actual overlapping worker
calls, and continuing gossip during model and evaluator waits. Reviewers must
consume arrived source and tests, expose a cross-package defect, request a repair,
review the changed target and promote the exact tested commit. Partition,
restart, lost-reply and unknown-outcome cases must preserve these boundaries.
Infrastructure processes are counted separately.

Passing that fixture is only the first engineering milestone. The
[project plan](large-swarm-project-plan-v1.md) still requires four cumulative
Local Research Library releases and six trajectories: the smaller swarm,
twenty-role swarm and equally staffed orchestrated comparison, each under
healthy and compound-recovery conditions. Full rehearsal, explicit resource
limits and a complete cohort budget precede live execution. The separate
decision-placement by transport contrast and independent held-out confirmation
remain necessary before claiming a general advantage.
