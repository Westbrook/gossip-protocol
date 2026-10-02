# Peer payload transport v1

Coding requests, responses and Git bundles travel as immutable binary objects.
The existing event transport carries a `payload_available` descriptor; actual
bytes move through a separate bounded chunk exchange on the same authenticated
loopback connection protocol, membership and partition gates. A descriptor is
not proof that a peer possesses its payload.

Each node has its own `payloads.sqlite`. The store fixes 64 KiB chunks, an 8 MiB
object limit, 128 registered objects and 64 MiB of reserved declared content.
These are logical payload limits, not an exact SQLite/WAL disk-size bound.
Descriptors contain protocol, whole-content SHA-256, size, chunk size, ordered
chunk hashes and a media type. Supported media types are JSON, Git bundle and
opaque bytes. The root and node identity are bound across restart.

Registration reserves capacity once without claiming availability. Each chunk
is size/hash checked before durable insertion. Partial transfers survive process
death; duplicate chunks are idempotent. A full read requires all chunks and the
matching whole-content hash. Corrupt persisted records fail closed. Empty objects
are complete on registration only when their zero-length hash is correct.

`PayloadPeer` extends the existing event runtime without changing its v1 source.
The local producer calls `publish_payload(data, media_type, command_id)`. Other
peers first receive the immutable descriptor event through normal dissemination.
Only objects with an actually arrived descriptor enter bulk inventories. A
private object placed on disk without its event is not offered to the network.

One bulk exchange sends a bounded chunk inventory and returns at most one
missing chunk. The initiator can then push at most one chunk in the reverse
direction. This supports leaf-to-broker upload without giving brokers a local
path to a leaf repository. Every RPC uses the original authenticated frame and
response binding. Both incoming membership/partition checks and outgoing fault
gates apply to bulk traffic. Chunks without an arrived local descriptor are
rejected; the observer has no bulk-write or run-action RPC.

Gossip uses bounded rotating contacts. Broker clients try the primary and then
the standby on contact failure; brokers replicate with each other. Metadata and
payload transfers have separate contact counts. Equal fanout does not imply
matched byte, CPU or relay capacity, and these tests make no comparative speed
claim. The inherited receiver-local acknowledgments remain asynchronous rather
than quorum durable. Objects are retained; streaming eviction/backpressure and
multi-host transport remain future work.

The additive runtime persists observed event/bulk wire-byte increments, contact
counts, offered/validated chunk counts and boot count in `payload-telemetry.json`.
Per-boot counters remain separately available. These are retained observations,
not lossless crash accounting: a process can die after socket I/O but before its
counter is persisted. Offered chunks are not acknowledgments and validated
chunks include duplicates. The counters therefore cannot by themselves prove
unique delivery or exactly-once execution.

The process checks use real isolated daemon stores and actual TCP. They cover
payloads larger than a single frame, both transport adapters, a received notice
without its bytes, partition/heal, partial-transfer death/restart, and a recipient
obtaining bytes from the standby after both origin and primary have died. The
notice-only case deliberately bootstraps metadata with one explicit sync; its
subsequent bulk recovery is autonomous. No test driver supplies destination
payload bytes.

A separate integration check transfers a [Git bundle](peer-git-bundle-v1.md) and
its offer metadata, removes access to the original sender, imports only received
bytes into a fresh quarantine, validates the exact divergent merged tree and
checks CAS publication. Its validator inspects trusted JSON fixture data; it
executes no candidate code and does not establish independent final software
acceptance.

All services remain trusted processes on one host. Shared group HMAC credentials
authenticate membership, not Byzantine producer identity. The observer can
bootstrap event metadata and inject declared faults; model/worker processes must
not receive that control capability in a hostile deployment. Publication,
provider accounting and final project acceptance require their own authorities.
