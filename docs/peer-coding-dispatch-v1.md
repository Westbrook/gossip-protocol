# Peer coding dispatch v1

This additive backend joins authenticated peer requests to the existing
`OpenAIWorker`, `RequestJournal`, and the **same Ledger instance** owned by
`peer_authority_v1.Authority`. It admits bounded coding proposals and retains
request, usage, and recovery evidence. It does not select a task, profile, peer,
or winning proposal; run candidate code; validate Git provenance; publish a
release; or declare a project complete.

This version is **offline only**. It uses the real worker's request encoding,
Responses parsing, proposal validation, reservation estimate, and usage code
with an injected transport. Its authority has a private fixture ledger. A
qualified bridge to the cumulative financial ledger is still missing, so both
`allow_live=True` and any non-offline mode are rejected. Merely approving more
funding cannot enable live calls through this version. Tests use an explicit
dummy credential; the backend does not discover credentials.

## Trusted adapter boundary

Construct exactly one `CodingDispatch(authority, payloads, workers, task_specs,
transport_mode="offline", transport_identity=..., journal_root=...)` before
serving RPC. A second backend on the same Authority is rejected, including after
failed backend startup; close and reopen the Authority to retry startup. The
Authority's exclusive process owner, lifecycle counter and exact joined Ledger
are reused. Each backend permits at most **one invocation at a time**. This is an
engineering limit, not evidence about swarm parallel scaling or timing.

The authenticated wrapper must:

1. Authenticate the unchanged `peer-authority-v1` envelope before routing
   `operation="coding_dispatch"` to `backend.execute(body)`.
2. Call `backend.guard_claim(db, task_id)` inside the Authority's existing claim
   transaction. Unknown or pending coding actions fence ownership transfer, and
   a global halt fences all new coding work.
3. Reject the original text-fixture dispatch operation for coding service tasks.
   It must not bypass the coding guard or use a separate request namespace.
4. Expose only principal-scoped lookup and replies, verify run and configuration
   identities, and retain the authenticated receipt in its peer journal.
   No peer may settle, choose the budget, enumerate global work, or publish Git.

The shared `authority_requests` table binds `(principal, request_id)` to exact
RPC bytes across both old and new operations. Repeating an exact request returns
its retained receipt; changing any bound field is rejected. A historical claim
receipt is not current permission for a new dispatch. Admission validates the
current lease using the authority clock and `Ledger.reserve`.

`task_specs` fixes each task's ordered `allowed_paths` and allowed profile IDs.
`workers` maps those IDs to existing `OpenAIWorker` instances. Task, profile and
recipient choices come from the requester and trusted configuration, never from
this backend. A profile identity includes the existing profile manifest, timeout,
response cap, and caller-supplied immutable offline transport identity. The
combined service must bind that identity to actual fixture transport source and
response bytes; it is a trusted configuration assertion, not automatic introspection
of an arbitrary Python callable.

## Owned immutable request bytes

The payload adapter has two deliberately asymmetric methods:

```python
read_owned(principal: str, sha: str) -> bytes
put_owned(principal: str, data: bytes) -> str
```

`read_owned` must return only complete request bytes whose immutable ownership
notice has arrived locally for that principal. It may raise `PayloadUnavailable`,
`FileNotFoundError`, or `KeyError` while the bytes are absent. Such a request gets a
nonjournaled `waiting/payload_unavailable` receipt and consumes no reservation;
the same request can be retried after arrival. No caller paths or observer-wide
payload directory are accepted.

`put_owned` durably stores and publishes **service-owned result bytes** on behalf
of the requesting principal and returns their SHA-256. It must not forge a
requester-owned request notice. The backend verifies the returned content digest,
not `read_owned` visibility; later readers verify downloaded bytes independently.
A mere result payload notice is not a completed dispatch receipt.

The dispatch payload is exactly:

```json
{
  "task_id": "build",
  "epoch": 1,
  "action_id": "stable-principal-scoped-action-id",
  "profile_id": "mini",
  "request_sha256": "64 lowercase hex characters"
}
```

The referenced content is canonical JSON, at most `MAX_PAYLOAD_BYTES=600000`:

```json
{
  "worker_request": {
    "task_id": "build",
    "instructions": "Implement the requested change.",
    "allowed_paths": ["src/add.py"],
    "files": {"src/add.py": "def add(a, b): return a - b\n"},
    "base_sha": "40 lowercase hex characters",
    "attempt": 1,
    "feedback": ""
  },
  "context": {
    "source_sha256": "SHA256 of canonical files JSON",
    "event_ids": ["locally arrived immutable event ID"]
  }
}
```

Exported `canonical_payload` and `source_digest` implement the exact encoding.
The existing worker additionally enforces its 512000-byte encoded request limit.
There are at most 64 allowed paths, 8 configured profiles, 64 task specifications,
256 context event IDs, and 256 journaled requests per principal in this backend.
The base Authority has its own smaller v1 operation/request limits; this is not a
full-study capacity claim. Context IDs must be unique lowercase SHA-256 strings.
The source digest is recomputed from actual file contents. The base Git SHA is
shape-checked only: a separate trusted bundle/quarantine adapter must establish
Git provenance, allowed tree scope and source equality. The backend binds event
IDs as assertions; the local-view runtime and independent audit must establish
actual arrival and causal eligibility.

## Durable admission and recovery

The execution configuration pins the run, base Authority configuration, profiles,
task scopes, limits, microUSD accounting units, absolute resolved journal
namespace, and exact source bytes of this backend, canonicalizer, Authority,
Ledger, RequestJournal and worker. A changed configuration fails startup against
the existing database. Moving a journal is a contract change, not a transparent
restart.

One atomic joined transaction reserves through the existing Ledger, records the
coding action and writes its dispatching receipt. Stable call and reservation
IDs derive from the full bound action, request bytes, principal, profile and
configuration. The RequestJournal reserve callback verifies this existing
reservation and current lease; it never reserves a second time. No SQLite
transaction is held while invoking the worker or waiting for the journal lock.

| Retained boundary | Recovery behavior |
| --- | --- |
| Missing payload, before admission | Wait; no reservation and no provider intent |
| Admitted reservation without recoverable result | Unknown; retain full reserve and halt, no automatic invocation |
| Durable request intent without result | Unknown; retain full reserve and halt, no automatic retry |
| Durable result, before settlement | Replay the exact result and settle idempotently, even if the lease expired |
| Settlement committed, before settled file | Reconcile through the existing Ledger and journal; no second invocation |
| Result publication failed | Retain recoverable result; halt and retry publication only on service restart |
| Receipt committed, ACK lost | Return the original receipt; no new invocation or settlement |

Startup reconciliation runs before the combined service may serve RPC. Recovery
uses an invocation callback that is always forbidden. Known worker failures with
usage are charged and replayed. Unknown usage retains the full reservation and
sets a persistent global dispatch halt; worker metadata requesting a halt does
the same even when usage is known. Journal, storage or accounting exceptions
anywhere after admission also fail closed. The backend persists unknown/halt when
possible and always sets an in-memory fence first if durable recording fails.
A service whose storage cannot record state must not be treated as reconciled;
its retained pending action is examined on restart. There is no peer RPC to clear
the halt, refund usage, adopt an unknown action, or force another call.

Result publication happens only after the journal result is durable and, when
usage is known, settlement has been reconciled. An unknown-usage failure payload
may be published while the full reservation remains unsettled. Publication
precedes final receipt commit, so consumers must require
the bound completed receipt before treating the payload as an accepted dispatch
result. A `publication_pending` receipt is explicitly nonterminal: consumers
await a terminal bound receipt after service recovery. It does not authorize a
new invocation. Recovered publication does not automatically clear a previous halt.

Results are canonical JSON of at most `MAX_RESULT_BYTES=2100000`:

```json
{"kind":"result","payload":{"changes":{"src/add.py":"..."},"summary":"...","usage_units":166,"metadata":{}}}
```

Known or unknown worker failures use `kind="failure"` with sanitized `message`,
`usage_units` and `metadata` instead. A small receipt carries `run_id`, base
`config_sha256`, `dispatch_protocol`, `dispatch_config_sha256`, principal/request/
action/task/epoch/profile identities, `profile_sha256`, `payload_sha256`,
`worker_request_sha256`, `journal_request_sha256`, reservation/call IDs, reserved
units and authority time. Completed and known-failed receipts additionally bind
`result_sha256` and actual usage. `payload_sha256` equals the dispatch's
`request_sha256`; no second field with that name is required in the response.
The worker request digest uses JSON lists; the journal digest separately binds
its existing newline/ASCII encoding. If storage cannot record the terminal
outcome, the backend returns a nonjournaled `waiting/state_persistence_failure`
receipt with request and configuration identities. The peer keeps the original
request pending for lookup or exact replay; it must not create a replacement
dispatch. The backend's in-memory fence blocks new invocations throughout.

## Observations and remaining integration

Fault hooks are trusted startup controls, never peer RPCs: `after_admission`,
`before_invoke`, `after_result_persisted`, `after_settlement`,
`before_receipt_commit`, and `after_receipt_commit`. Each receives the outer RPC
request ID. `before_invoke` follows durable intent creation; it is not proof of a
network request. Action rows prove admission, journal files prove retained intent,
result and settlement. Actual transport-entry counts require a separately retained
transport trace, including across process restart.

The fast tests use real worker parsing and accounting with deterministic injected
HTTP responses, in-process exception boundaries, controlled duplicate threads and
fresh Authority reopening. They are not process-kill evidence. Separate combined
service/peer process tests must prove actual payload transport, local visibility,
service restart, publication and invocation counts. Static/fast receipts alone
cannot establish that integration.

Still outstanding are the cumulative live financial bridge; a true provider
adapter in the combined service; candidate execution in fresh sandboxes; Git
scope/CAS/merged-tree and independent acceptance gates; autonomous bounded coding
tool loops; equivalent placement/transport capacity; and the full preregistered
2x2 comparison on independently held-out task families. This backend is one
necessary engineering phase, not a quality, speed, cost or project-completion
result for gossip versus orchestration.
