# Peer authority v1

This local service authorizes a caller's requested task lease and runs a fixed,
harmless text fixture. It does not select tasks, models, recipients, repairs or
winning sources. It exposes no global task list, candidate matrix, peer inbox,
settlement or publication operation. It contains no provider implementation and
executes no supplied code. Git scope, merged-source validation, exact Git CAS
and independent final acceptance remain required future integration.

Each principal has a distinct HMAC key and a fixed task allowlist. The service
binds its run ID, exact configuration, task fixtures and key hashes across
restart. Keys are never included in replies or the stored configuration digest.
This is authenticated loopback traffic on a trusted host, without TLS or
multi-host failover. Peer/model processes must not receive another principal's
key or access the private authority database. A peer runtime also needs its own
capability isolation; a shared gossip capability does not provide this.

## Exact interface

The signed request body is
`{protocol, run_id, principal, request_id, operation, payload}`. The protocol is
`peer-authority-v1`; its envelope is `{body, mac}`. HMAC-SHA256 covers canonical
strict JSON. Responses bind the run, principal, request ID and complete RPC
body hash. The client helper is:

```python
authority_request(port, principal, key, request_id, operation, payload,
                  *, run_id, timeout=2.0) -> dict
```

It performs one physical request and never retries automatically. Each returned
receipt includes `run_id` and `config_sha256`. A worker must verify both against
its frozen local contract. `config_digest(config)` calculates the latter from
the exact service configuration with keys replaced by their hashes.

| Operation | Exact payload | Receipt |
|---|---|---|
| `claim` | `{task_id}` | `status: ok`, historical `lease`, `authority_time` |
| `renew` | `{task_id, epoch}` | `status: ok`, renewed `lease`, `authority_time` |
| `validate` | `{task_id, epoch}` | `status: ok`, current `lease`, `authority_time` |
| `dispatch` | `{task_id, epoch, action_id, request_sha256, request}` | Own action's `dispatching`, `completed` or recovered `unknown` receipt |
| `lookup` | `{request_id}` | That principal's retained original receipt, or `status: missing` |

A lease is `{task_id, worker_id, epoch, expires_at}`. Authentication supplies the
worker identity; callers cannot submit a worker ID or clock. The service fixes
the lease duration and reservation size. A replay returns its historical receipt
and never renews a lease or grants a fresh invocation. A new dispatch validates
the actual current principal, epoch and expiry inside its transaction.
Use a fresh request ID for a new validation observation; replaying an earlier
`validate` response is a historical observation, not renewed permission.

An action receipt contains `task_id`, `epoch`, `action_id`, `reservation_id`,
`request_sha256` and `authority_time`. Only `completed` adds `result` and
`usage_units: 0`. Request IDs and action IDs are scoped to the authenticated
principal. Mutation/validation request IDs bind their complete RPC body; a different
payload under such an ID is rejected. The read-only `lookup` RPC does not create
a new durable request entry. An action
ID cannot move to another request ID within that principal. Reservation IDs are
service-derived hashes of protocol, run, principal and action, preserving one
common reservation namespace without revealing another principal's actions.

Authenticated deterministic denials return `status: rejected` with a bounded
reason such as `forbidden`, `malformed`, `unavailable`, `budget`,
`request_conflict`, `action_conflict`, `request_digest`, `source_digest` or
`capacity`. Unauthenticated/malformed envelopes receive no detailed reply.
`AuthorityError`, `OSError` or `TimeoutError` from the client describe transport,
authentication or protocol uncertainty, not a mutation rollback. Recover using
the original request ID and body, or look up that ID; do not manufacture a new
request or action ID to escape uncertainty.

## Fixed zero-provider fixtures

Both fixtures accept exactly:

```json
{
  "text": "arrived text",
  "context": {
    "source_sha256": "SHA256 of the exact UTF-8 text",
    "event_ids": ["an arrived immutable event ID"]
  }
}
```

Text is at most 4096 UTF-8 bytes. Context has at most 32 unique SHA256-shaped
event IDs. The service recomputes the text hash and the full canonical request
hash. Event IDs are caller assertions bound to that request; the authority does
not independently establish that those events arrived. The separate peer-local
journal and transport audit must prove that property.

`text_build` returns the uppercased text and its UTF-8 SHA256. `text_review`
computes the received text's SHA256, `nonempty`, `is_uppercase`, and an
`approve`/`reject` decision requiring both properties. These are data fixtures,
not coding-quality judgments. Their result depends on supplied text rather than
an injected success flag. No supplied path, code or command is executed.

## Atomicity, time and recovery

The frozen `Ledger` implementation is reused through a transaction-joining
subclass. Claim, renew and reserve mutations use the same SQLite connection as
the request journal. A savepoint rolls back rejected action mutations before
the rejected receipt is committed. No second ledger transaction can succeed
without its durable request binding. Receipt serialization is bounded before
commit. Inherited methods that open separate read sessions are not used inside
these transactions.

Dispatch first commits its reservation, exact request bytes and dispatch intent.
Only the initial owner enters the fixed fixture. It then commits the result and
trusted zero-usage settlement together in a later transaction. Generic test
credits are fully refunded after this zero-provider operation; they are not API
spend or a change to the project's cumulative financial ledger.

A service-private `authority_invocations` row is committed immediately before
fixture entry. It records principal, action, request digest, fixed fixture and
authority time. This is entry-attempt evidence, not proof of completed execution
or a count of provider calls. There is no peer RPC to list these rows.

A crash after intent without a durable result becomes `unknown` on restart.
Its full reservation remains held; the unresolved action fences task reclaim
and further dispatch even after lease expiry. There is no automatic replay,
refund, transfer or peer-controlled settlement. A completed action lost before
its acknowledgment replays the retained result without another fixture entry.
The engineering fault controls are trusted startup CLI arguments only.

Time is sampled inside the transaction after acquiring the SQLite write lock.
The persisted high-water mark clamps backward wall-clock movement. Already
expired epochs cannot revive; a backward host-clock jump can delay real-time
expiry until the clock catches up. This is not a fixed elapsed-time guarantee
under clock faults. The two-second socket deadline does not bound a mutation
waiting on SQLite's ten-second busy timeout. A client can time out before a
server commits.

One process owns the service directory via `flock`. Closing rejects new handlers
and retains that lock until active authority handlers finish. Restart preserves
the journal and epochs. The service does not offer multi-host consensus, disk
loss recovery or a replicated authority; all future study cells need the same
qualified availability and fault policy for this shared boundary.

## Configuration and qualification scope

The service configuration has exactly `protocol`, `run_id`, `budget_units`,
`lease_seconds`, `principals` and `tasks`. Each principal maps to `{key, tasks}`;
each task maps to `{fixture, reservation_units}`. There are at most 16 principals,
64 tasks and 256 durable requests per principal. These limits are an engineering
fixture contract, not a claim about capacity for a full software study.

Start `python -m gossip_harness.peer_authority_v1 --config CONFIG.json` with a
process configuration `{root, port, authority}`. Port zero binds a fresh loopback
endpoint; startup reports its PID, port, run and configuration hash. Optional
`--crash-point` and `--crash-request-id` inject a real process exit at one declared
boundary. Peers cannot arm faults or alter the catalog.

Offline tests cover request identity, fencing, atomic denial, ownership and real
localhost crash/restart behavior. Root owns registration, central verification
and combined-runtime qualification. This phase does not complete live provider
integration, exact Git acceptance, the four-cell placement/transport study,
independently held-out statistical confirmation, or autonomous tool-loop and
multi-host comparisons.
