# Peer coding candidate publisher, version 1

This additive sender adapter turns one locally completed coding action into a
real Git proposal and an immutable bundle offer. It consumes the existing
`CodingPeer` journal, locally arrived metadata and complete payload bytes. It
does not ask an observer to schedule the action, execute candidate code, advance
the sender's accepted branch, select a winner, or grant final acceptance.

```python
publisher = CandidatePublisher(
    worker.root / "candidate", worker, private_git_store,
    baseline_sha=provided_baseline,
    allowed_paths=("solution.py",),
)
publisher.tick()  # called by the owning peer's autonomous loop
state = publisher.state()  # detached, read-only snapshot
```

There is exactly one live publisher per `CodingPeer`. Construction registers a
weak owner reference under the peer's coding lock, and the journal must be a
private descendant of that peer's exclusively owned directory. The runtime
owns the peer/process lifetime; the publisher adds no thread, server or close
operation. It performs serial Git operations when its local prerequisites are
ready. A reopened process creates a new peer and publisher with the same exact
configuration. Concurrent arbitrary host writers are outside this ownership
contract.

The retained configuration binds this adapter and its relevant Git, bundle,
coding, payload, store and result-parser source files; Python version/platform;
the selected Python and Git executable paths and streamed file hashes; and a
digest of `PATH`, `HOME`, `DEVELOPER_DIR`, `SDKROOT` and the explicit native
library/preload selectors (`LD_LIBRARY_PATH`, `LD_PRELOAD`,
`DYLD_LIBRARY_PATH`, `DYLD_INSERT_LIBRARIES`). No executable version query is
needed. A changed selected identity rejects restart. This deliberately does not
claim to fingerprint all dynamic libraries or a delegated toolchain: for
example, macOS `/usr/bin/git` can be an Xcode launcher whose bytes do not identify
every underlying tool. The trusted, pinned qualification environment remains
part of the execution contract.

## Source and result binding

The initial immutable repository is a trusted fixture clone created before the
run, with the same baseline available to every compared arm. The publisher
requires the whole `WorkerRequest.files` mapping to equal the base commit's
actual file tree, including unchanged files. The request's base, task and
ordered allowed-path list must match its configured baseline and scope. Only
generation zero and the existing dispatch profile's full SHA-1 IDs are admitted.

A completed receipt alone cannot produce a proposal. The sender requires its
own published `coding_result`, the original seed producer's exact bound notice,
its own request notice, the configured coding service's exact bound result
notice, and all three canonical payloads locally complete. Repeated notices for
identical bytes do not replace the already bound seed/result event IDs. It
checks the retained coding claim and completion receipt, result usage, request
context, task/profile/action/epoch, authority and dispatch configuration, and
exact request/result digests. Up to 256 locally visible event IDs are frozen in
the candidate intent; arriving unrelated events do not rewrite that decision.

Changes may touch only the exact task-owned paths. The sender constructs the
full expected result tree from the authoritative result changes, applies those
changes through the existing `GitStore.propose`, then compares the full proposal
tree with that expected source. The deterministic proposal message binds the
immutable intent. The sender's accepted reference must remain at its baseline.
Recovery repeats the full source/mode check and requires the exact proposal ref,
direct baseline parent and deterministic commit headers/message before export
or publication. Repointing a checksummed journal at another locally valid
proposal cannot substitute source or borrow another parent.
The existing bundle exporter subsequently checks ancestry and its complete
historical repository profile before any offer is published.

`peer_source_sha256` and `peer_candidate_source_sha256` use the peer protocol's
UTF-8 canonical JSON (`ensure_ascii=False`). They are **not** the Blackbox
evaluator's canonical source digest, which escapes Unicode. A receiver must
compute and label each digest in its own namespace, even when ASCII fixtures
happen to produce equal values.

The supported repository profile and packed/expanded bounds are inherited from
[the bundle boundary](peer-git-bundle-v1.md). Baseline modes are checked before
proposal creation; unsupported historical trees are rejected by export. This
is a small text-fixture adapter, not admission for arbitrary repositories.

## Durable publication

The state machine is `prepared → proposal → bundle → published`:

1. Write the complete immutable input record before selecting the prepared
   action. The retained record contains the exact request, seed, service result,
   coding journal, candidate source and intent.
2. Create the peer-owned proposal from the fixed base and deterministic message,
   and persist the offered commit. A crash between Git creation and the journal
   write may reconstruct the same commit; it cannot pick another action.
3. Export actual bundle bytes and write them together with their manifest in a
   single immutable `bundle-record.json`, then persist its selected checksum.
   Recovery uses those retained bytes instead of regenerating a Git pack.
4. Publish the bytes through the existing payload protocol and publish the exact
   offer using stable, intent-derived command IDs. Persist both event IDs only
   after publication. Recovery after publication but before that write replays
   the same commands and obtains the same events.

Writes use fresh temporary files, file fsync, atomic replacement and directory
fsync. State envelopes have strict field validation and checksums. Reopening
revalidates retained input identities against the owning coding journal and
local arrived events/bytes, bundle hash/header/manifest, and published events.
A missing or changed durable artifact fails closed. An interrupted initial
identity write with only a partial temporary file also fails closed for manual
inspection. These checks detect corruption and substitution within the trusted
host contract; JSON checksums are not signatures against a malicious host.

The `candidate_offer` event is produced by the worker principal and contains
exactly:

```text
protocol = peer-candidate-v1
run_id, task_id, principal, epoch, action_id, request_id, profile_id
request_sha256, result_sha256, dispatch_config_sha256, generation = 0
bundle_sha256, bundle_manifest
```

`request_id` names the completed coding dispatch operation; `request_sha256`
identifies its complete request payload. No sender filesystem path crosses the
offer boundary. The receiver retrieves actual arrived bytes, validates receipt
ownership against its own authoritative dispatch journal, imports the bundle
into fresh retained quarantine, and compares both base and offered source with
that exact request/result. The sender's copied completion JSON alone is not
authority proof.

## Receiver and qualification boundaries

Publication means only that a source-bound proposal is available. The receiver
must still validate the exact merged candidate in a fresh isolated execution
environment, preserve scope and expected-head CAS, require the current task
lease at promotion, and retain the durable validation/promotion receipt. A
stale-head rejection or already-incorporated no-op must never become a new pass.
Public integration promotion remains separate from independent final evaluation
after the whole cohort is frozen.

`PeerCandidateTests` directly inherits `unittest.TestCase` and belongs in the
Git lane. Its offline fixtures exercise local result arrival gates, full-base
and scope rejection, real proposal/bundle bytes, Unicode digest distinctions,
four durable restart boundaries, Git/bundle side effects before journal writes,
corruption rejection, repeated notices and exclusive ownership. These use
valid-other-proposal/parent substitutions and execution-identity changes as
well as damaged bytes. The restart fixtures use
exception-injected stops and reconstruction of peer/journal objects; they are
**not actual process-kill evidence**. The separately composed process runtime
and integration tests must supply real network/process evidence. This document
does not claim those checks passed; retained central verification receipts are
the execution authority.

The publisher adds no model call, provider credential, Docker invocation,
candidate-code execution, new lease or promotion authority. Native Git parsing
has the existing explicit memory-isolation limitation; this cycle is restricted
to trusted harness-generated fixtures. Full coding source selection, iterative
repair, supersession/refutation, multi-generation reopening, deadlines and a
matched four-cell quality experiment remain separate work.
