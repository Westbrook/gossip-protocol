# Peer coding runtime v1

A coding peer now takes one provided task through the real bounded worker
interface. It waits for its configured seed event and complete request bytes,
forms a source-bound request from its own arrived context, obtains its own lease,
and sends the exact request digest to the common coding authority. Only its
local timer advances this state; observer reads do not execute work.

The seed is a provided `WorkerRequest`, not an agent-generated project plan.
The peer binds run, task, profile, seed producer/hash, service producer, authority
configuration and coding-dispatch configuration. Its durable journal records
stable action and RPC IDs before dispatch. The request's actual files and visible
event IDs are frozen; the authority cannot fill missing context from another
peer's directory.

The authority combines two independent local TCP surfaces in one trusted service
process: existing per-principal signed task/dispatch RPC and the group payload
mesh. `MeshPayloads.read_owned` requires a locally arrived notice from the
requesting principal plus complete verified bytes. A control request that arrives
before its payload is transiently unavailable, without creating a reservation or
cached denial. Exact retry can then proceed after ordinary data dissemination.

[CodingDispatch](peer-coding-dispatch-v1.md) shares the authority's original
request namespace and ledger, and reuses `RequestJournal` and `OpenAIWorker`.
The service fixes allowed paths and profiles. Peers cannot settle costs, alter
profiles, use the old text-dispatch operation to bypass coding gates, or clear
an unknown outcome. Accepted requests bind the complete source, request,
configuration, worker profile and accounting identity.

The CLI accepts only a server-configured offline Responses fixture. It injects
that fixture into the existing worker's real API-payload, response-parser and
usage-validation code; no API credential is loaded and no real provider request
is made. The library also rejects its default HTTPS transport and live mode.
Trusted injected Python transports are not a network sandbox. The existing
private authority ledger is test accounting; it cannot replace the cumulative
financial ledger. A separately qualified financial bridge remains required for
live dispatch, independently of additional funding approval.

The result is journaled before result payload publication. If publication fails,
`publication_pending` can recover the same persisted result without a new worker
invocation. A completed control receipt alone does not complete the peer's work:
the matching service-produced result notice and full payload must arrive locally.
Only then can the peer publish `coding_result` evidence through its stable outbox
identity. This is neither Git publication nor independent project acceptance.

Unknown provider usage or unresolved dispatch is terminal and conservative;
its reservation is retained and further dispatch is fenced. Known failures retain
their charged test usage and are replayed without a new invocation. Process tests
kill and restart actual workers and services around durable boundaries, inspect
private fixture-entry records only as output evidence, and check that one action
and one published result survive recovery. The tests also cover gossip/broker
source partitions and a control request overtaking its payload.

The service and worker remain trusted same-host processes. Producer labels on
gossip events are trusted-group provenance, not Byzantine signatures. The supplied
single-task policy does not yet select candidates, repair projects, reopen source
generations or detect global completion. Combining coding results with bundle
preparation, sandboxed candidate execution, final-evaluation barriers and the full
four-cell comparison remains required before claiming a software-project study.
