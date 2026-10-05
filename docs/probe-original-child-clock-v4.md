# Physical probes inherit the original child clock

The [v5 continuation](probe-cleanup-headroom-v5.md) also reserves the sequential
cleanup allowances before the original project deadline.

The physical `ProbeExecution` constructor now requires the prospective
`StudyPlan` as well as the real shared financial executor. It reads `contract`
and `child.<trajectory>.begin` from that executor's anchored checkpoint chain.
The plan digest, complete terminal roster and financial runtime policy must
match. Source pins are checked before the original clock can be used.

The registered probe window must lie inside the original child's monotonic
window and use its interpreter clock domain. A supplied timestamp cannot move
the start earlier, extend the end or survive a foreign interpreter incarnation.
Every non-cleanup effect boundary reauthenticates the original clock and owner.
Cheap nested deadline checks also apply the original monotonic and wall-clock
stop. A forward wall-clock jump may stop work earlier; a rollback cannot extend
the monotonic horizon. No clock is sampled to create a replacement horizon.

Capacity waiting remains within the registered probe deadline. On acquisition,
the original child is checked again before any durable probe intent or sandbox
setup. If the child expired while waiting, the unstarted slot is released and no
probe is dispatched. Cleanup retains its existing separate bounded allowance;
project expiry must not prevent removal and reaping of already-owned resources.

Execution protocol v4 changes the prospective environment identity. Earlier
source reviews and rehearsals remain historical. The regression fixtures use
real isolated financial journals/authorities, original child records and inert
Git with a deliberately refused Engine socket. They exercise no live candidate,
Docker Engine or provider. Synthetic review data is not independent approval.

This closes the physical owner's original-child deadline join. It does not
finish reservation-to-cell admission, qualified resource envelopes, ranked
controller activation, independent cold joins to the controller clock, remaining
product acceptance routes, physical controls or the complete matching rehearsal.
It provides no new statistical or model-quality observation.
