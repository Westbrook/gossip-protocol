# Peer runtime v1: process and transport foundation

This version exchanges immutable data between actual, independently running
local processes. It does not call models, run candidate code, claim tasks,
reserve money, integrate Git, or approve completion. The full future study in
`analysis/benchmark-live-gossip-followup-design-v2.json` remains outstanding.

Each node owns a private SQLite database, routing checkpoint and directory
lock. A published event binds its producer, origin sequence, kind and immutable
artifact hash. Repeated publication command IDs replay the same result; a
different command body under that ID is rejected. Incoming batches validate
completely and commit atomically. The receiver acknowledges only after commit.
An artifact is JSON data, never a path or executable program. Artifact content
is limited to 48 nested edges from its root, leaving wrapper headroom within
the 64-level JSON wire limit. This is checked on publication, receipt and reads;
the process suite exercises accepted boundary content through actual exchange.

The node's own timer chooses outbound contacts using only its local routing
configuration. Gossip uses a deterministic hash permutation of the membership
for each local turn, with bounded fanout. Broker clients contact the primary and
fall back on a failed exchange; broker replicas also exchange with one another.
Both adapters use the same bounded inventory, pull, push and durable merge
protocol. An acknowledgment confirms only the receiving node's local commit;
broker replication is asynchronous and has no quorum acknowledgment or
consensus guarantee. The failover check first confirms replication to both
brokers before killing the primary, then publishes through the surviving one. Actual transferred bytes and attempted/failed contacts are counted
per process boot. Future fault comparisons need durable telemetry or an external
accumulator that retains every boot's counters; the present counters cannot
measure total recovery traffic across restarts. There are no dummy contacts to
manufacture equality. Resource
and capacity equivalence for the future live study has not been established.
The four-process broker fixture uses two clients and two broker services; it is
a fault-contract check, not a matched four-worker coding comparison. Future
cells must explicitly account for relay services and worker placement.

The TCP protocol listens only on loopback. Frames are length-prefixed, capped
before reading the body, and share an absolute two-second socket/frame deadline.
That deadline bounds client observation and network I/O, not server mutation
completion: a handler may wait up to the SQLite ten-second busy timeout before
committing after the client has already timed out. Such a mutation has an
ambiguous client outcome. Stable command IDs and immutable batch identities make
later reconciliation safe; a timeout is never proof that nothing committed.
Accepted connections and handler threads are bounded. Messages and responses
carry HMAC authentication plus exact request/response identities. The ephemeral
shared cluster capability is a trusted-fixture mechanism, not TLS, individual
Byzantine provenance, or production authentication. Configuration is trusted
host input. The control sender can bootstrap membership, publish local fixture
data, inspect a node, and arm transport/crash faults. It cannot grant money or
release authority. Model-facing interfaces must never inherit this capability.

Partition checks deny both ingress and egress at the configured transport
boundary. They are real process/socket tests with an application-level fault
gate, not kernel packet-loss or geographically distributed network tests. Fault
gates are installed before publishing partition-specific data. Healing removes
the gate; peers reconcile through their own timers without observer routing.
The fixed roster and bounded corpus avoid claims about membership churn or an
unbounded production log. Process restart retains the database and routing
checkpoint. In-memory counters restart and are labeled accordingly. Disk loss,
multi-host authority, adaptive routing and Byzantine workers are unimplemented.

## Process configuration and boundaries

Start `python -m gossip_harness.peer_runtime_v1 --config CONFIG.json` using the
pinned project interpreter. The strict configuration contains `root`, `node_id`,
`mode` (`gossip` or `broker`), an ephemeral `token`, `port` (zero for a new bind),
`interval` (0.05–60 seconds), and `fanout` (1–4 for gossip, 2–4 for broker).
Broker mode requires exactly two configured brokers; fanout below two is
rejected so fallback never silently exceeds the declared contact allowance. The first stdout JSON record
reports the real PID and port. No credential discovery or environment API key
lookup is performed. Configure the complete membership via the authenticated
control operation before enabling autonomous ticks. Membership is immutable
after bootstrap. Restarts use the previous endpoint and local directory.

The Python `request()` helper implements one physical authenticated request
without automatic mutation retries. `publish` accepts a stable command ID,
kind and JSON content. `exchange` returns a bounded missing-event batch and
inventory; `merge` persists a supplied batch. Peer senders cannot invoke
control operations. `state` exposes only that node's retained local view,
process identity, routing and boot counters. It provides no global evidence
or completion judgment. Full RPC schemas, origin authentication and authoritative
service contracts must be frozen again before live coding integration.

Future adapters must keep task leases, budget dispatch, exact Git CAS and
independent acceptance outside this store. Convergence is neither permission to
spend nor evidence that a project is correct. A model/controller integration may
consume these local events, but no such decision loop is implemented here.

## Verification and retained evidence

The central verifier registers `PeerStoreTests`, `PeerProtocolTests` and
`PeerProcessTests`. The process class reserves five resource tokens: its test
worker plus up to four real daemons, including two durable brokers. It must be
serialized when another owner uses the global physical-resource budget.

The store class reserves three tokens for its worker and two transaction
threads and runs exclusively. The fast classes cover malformed content,
integrity, replay and transactions.
The process class exercises autonomous gossip across a backlog larger than a
single batch, partition/restart/heal, primary broker
death with standby service and catch-up, actual process exits before commit and
after commit but before acknowledgment, reordered duplicate traffic, bad frames,
and exclusive local ownership. The broker check also inspects one deliberate
local turn to confirm two attempted contacts (failed primary then standby).
Receive-crash atomicity uses a manual batch replay; reordered traffic reverses
entries within a batch. These do not establish autonomous recovery after an
incoming merge crash or arbitrary network packet reordering. State-predicate/readiness deadlines replace
fixed settle sleeps. All children are owned and cleaned up; fixture databases,
configuration, stderr and lifecycle timestamps are retained under
`runs/peer-runtime-fixtures`. They contain generated test capabilities only.

Use the central verifier with at least five tokens for the process class after
the coordinator permits it. Successful foundation checks qualify this bounded
transport contract only. They do not complete the four-cell live experiment,
establish coding quality, or satisfy the independently held-out statistical goal.
