# Cold observations join their original assigned cells

`reconstruct_enrolled` requires the actual probe state, its independently retained
execution checkpoint, the original cell locator, and an independently retained
controller checkpoint. It accepts no runner verdict or cached reader result.
The existing `reconstruct` remains a lower-level physical-record reader whose
result does not establish controller enrollment.

The joined reader verifies both journals against their supplied prefixes. It
then reconstructs the complete original cell assignment and verifies the actual
state journal, external head and cleanup paths. Execution protocol v7 retains
the exact enrollment slot and record digest in the physical intent. The joined
reader compares that reference with the reconstructed controller record, not a
caller-provided digest. A well-formed reference alone supplies no authority.

The original child clock is read independently. The recorded probe window and
clock domain must match that child and leave the configured cleanup allowance
inside its horizon. Cold reading remains possible after expiry; no clock is
renewed and no process or socket is opened.

The existing original-record reader reconstructs values, protocol order,
runtime identity and cleanup evidence. The join repeats original enrollment
authentication and rechecks both exact checkpoints before returning. Even a
legitimate new controller record during reconstruction invalidates the requested
snapshot; the reader does not silently follow it.

The result binds the cell, reservation, enrollment, both checkpoints, original
clock and nested observation. It preserves that observation's disposition:

| Observed evidence | Result |
| --- | --- |
| Complete good values and required mechanics | `pass` |
| Good values with missing completion or cleanup evidence | `unavailable` |
| Already-observed semantic defect with missing later cleanup | `fail` |
| Wrong cell, root, retained reference, or changed checkpoint | Exception; no joined observation |

An assigned cell is still not a qualified resource envelope, selection approval,
independent acceptance, or statistical sample. Those flags remain false. The
ranked controller must consume the complete set of appropriate joined results
under its frozen policy before choosing a candidate.

## Tests and limits

The new Git controls run the actual enrollment and physical-record readers
together, without mocking either function. They use real inert Git repositories,
state journals and external checkpoint heads. Financial and mesh evidence,
source reviews, and complete Engine responses are deliberately authored fixture
records. No candidate, Engine or provider runs; this is not a physical rehearsal.

Tests cover a complete read after expiry, root substitution, mismatched intent
references, missing cleanup, preserved known defects, stale execution prefixes,
controller appends during reading, and independent cleanup-headroom refusal.
Historical receipts retain their original source and protocol bindings.

Qualified aggregate time/byte/workspace accounting, complete ranked-controller
integration, exact-source independent reviews, named physical fault controls,
remaining acceptance routes and a matching rehearsal still precede a live pilot.
This change adds no comparative model-quality evidence.
