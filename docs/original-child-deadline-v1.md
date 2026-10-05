# Bind probe reservations to the original child clock

The prospective runtime can explicitly select
`original_child_deadline: cumulative_child_deadline_v1.POLICY`. This policy is
part of the study's resource-contract hash and its implementation is included
in the pinned source closure. An absent field retains the earlier wall-clock
policy; an unknown or altered field is refused. No frozen study is relabelled.

For the selected contract, the actual study controller records an absolute
monotonic window in `child.<trajectory>.begin`, before constructing the runtime.
It samples the wall clock once for the existing wall deadline. Both horizons
come from the same declared duration; the monotonic end is never recalculated
from a later wall-clock reading. A forward wall-clock jump may stop work early;
a rollback cannot extend the monotonic horizon.

The runtime rejoins the anchored original before source setup, finance startup
or role launch, and rejects a substituted wall deadline. Its owner-thread paths
can recheck the original journal. The financial RPC thread uses an immutable
projection of that authenticated record, with identity checks, so it does not
violate the journal's thread ownership. The controller refuses completion
credit for a public evaluation that finishes after the original horizon while
preserving the evaluation result.

`GossipChildRuntime.probe_capacity_window()` derives the declaration window from
that same original. Original-backed accounting checks exact start, end and clock
domain before any reservation. It also requires the clock record to precede
runtime work and the probe build freeze. Cold accounting inspection repeats the
original join after expiry without granting another attempt. A shifted start or
extended end cannot become valid by recomputing the request's own hashes.

Active use is restricted to the creating interpreter incarnation. Other
processes can inspect old evidence but cannot reinterpret its monotonic numbers
as a fresh horizon. This is a fail-closed continuation boundary, not a complete
controller-crash recovery implementation. Existing deliberate role-process
restart behavior is unchanged.

The new checks exercise the actual controller and original accounting paths,
anchored journals and inert Git. Runtime endpoints and role outcomes in the
controller fixture are synthetic. No live provider, Engine or candidate
execution, independent layout approval or new model-quality sample is supplied.

This binds the time authority, not physical executor capacity. The declared
capacity ledger still issues no executor leases, and the ranked-probe selection
policy still needs full controller activation. Shared model/evaluator capacity,
qualified resource envelopes, changed-source reviews, complete product
acceptance, host-fault controls and the matching six-trajectory rehearsal remain
required before interpreting a live comparison. In-flight API/OS operations are
not hard-preempted by an admission deadline; separate cleanup/drain duties remain.
