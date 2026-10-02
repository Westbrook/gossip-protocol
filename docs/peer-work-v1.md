# Peer-local work foundation v1

Two independent processes now choose fixture work from their own arrived event
stores. A builder consumes a seed, obtains its own lease and requests a text
transformation. A reviewer waits until both the seed and the builder's result
have reached its store, then requests review of that actual result. The same
local decision policy runs over gossip and the two-broker transport adapter.

This establishes an executable decision and recovery boundary. It uses fixed
text fixtures, with zero model calls and no candidate-code execution. An
`approve` result concerns uppercase nonempty text; it is not software acceptance,
permission to merge, distributed completion, or evidence of superior quality.

## Local evidence and authority

`plan_action(store, config)` freezes the local event inventory, verifies required
artifact bindings and produces one immutable intent. It records the full visible
inventory separately from the prerequisite events consumed by the request.
Intent, request, source text, policy, role and authority configuration all have
explicit digest bindings. Arriving evidence cannot silently replace a prepared
request. The observer can bootstrap peers, publish the initial seed, configure
transport faults and inspect state; it has no `run_action` operation.

The work configuration has exactly these fields:

```json
{
  "run_id": "local-chain",
  "role": "builder",
  "task_id": "builder-slot",
  "seed_producer": "builder",
  "builder_peer": "builder",
  "authority_sha256": "SHA256 of the service configuration"
}
```

The reviewer uses `role: reviewer` and its own allowed task. A seed event has
kind `work_seed`, the configured producer, and exactly
`{protocol: "peer-work-v1", run_id, generation: 0, text}`. Seed text is bounded
at 2,000 UTF-8 bytes; the transformed text must also fit the authority's 4,096
byte request limit. At most 32 visible events are admitted. Ambiguous seeds,
unsupported generations and mismatched artifacts fail closed. There is no
policy yet for selecting a new source generation or reopening completed work.

The [authority](peer-authority-v1.md) authenticates each principal separately.
It authorizes only the requested allowed lease and exact fixed-fixture dispatch;
it does not select the task, recipient or context. The runtime's endpoint has
exact fields `{port, principal, key, config_sha256}`. Principal must equal the
peer's identity. A retained worker binds the capability hash across restarts;
the endpoint port may change. Plaintext keys are absent from work evidence.
These remain trusted processes on one host, without OS user/container isolation
or independently signed forwarded result receipts. Shared gossip credentials
do not prevent a malicious group member from impersonating an event producer.

## Durable action and outbox

`WorkJournal` persists and validates this state machine:

```text
prepared → claimed → dispatch_pending → result → published
                 rejection → denied
       unresolved dispatch → unknown
```

Claim and dispatch request IDs derive from the immutable action ID. A transport
timeout preserves the exact command and durable phase. Retrying that command
reconciles the authority's receipt; it does not grant another fixture invocation.
Unknown authority intent retains its entire reservation and becomes terminal
locally, including after restart. A stale historical lease cannot authorize a
new dispatch. There is no peer-controlled settlement or escape from ambiguity.

After a completed response, the worker persists its exact output envelope before
publishing with a stable store command ID. Death before publication recovers the
same output; death after publication but before the journal acknowledgment
recovers the same event. This is an idempotent outbox across two databases, not
one transaction spanning both databases.

The local timer selects and advances work. State observation does not advance
it. Graceful shutdown waits for work and request handlers before releasing node
ownership. Process death is handled by the retained journal and authority.

`work-telemetry.json` durably records boot count, RPC attempts, verified replies,
transport errors and whether the last request has an unresolved transport error.
An attempt does not prove a server commit; a missing reply does not prove
rollback. A crash can leave an attempt without a reply/error observation.
Successful recovery clears the pending flag while preserving cumulative counts.
Base transport byte counters still reset on boot. Permanent authority outage
remains visibly pending: bounded overall deadlines and retry backoff are future
work, not implemented availability guarantees.

## Qualification and remaining scope

Run the registered checks through the central verifier:

```sh
env -u GOSSIP_RUN_DOCKER_TESTS .venv/bin/python -m devtools.verify \
  tests/test_peer_authority_v1.py tests/test_peer_work_v1.py \
  tests/test_peer_work_runtime_v1.py --workers 6
```

The suite has 31 fast checks and 17 process checks. Process fixtures launch real
authority, builder and reviewer daemons; broker mode adds two relay daemons.
They cover local-only visibility during partition, healthy broker transfer,
authority outage/recovery, stale leases, worker death around result/publication,
lost completion acknowledgment, and unknown dispatch before/after fixture entry.
Private authority entry traces count entry attempts, not provider invocations
or proof that a fixture completed. The unknown tests observe further autonomous
ticks after restart and retain full reservations without duplicate entry.
Generated fixtures and failures are retained under `runs/`; subprocesses receive
an explicit environment without provider credentials.

This is targeted foundation qualification, not the full four-cell experiment.
Real provider/accounting adapters, sandboxed generated code, Git bundle and
merged-source CAS integration, source refutation/reopening, live cutoffs, complete
durable transport telemetry and matched worker/relay capacity remain required.
Independent held-out task-family sampling, autonomous tool use and multi-host
failure studies are separate outstanding phases. The existing controller study's
frozen sources and scientific contract remain unchanged.
