# Cleanup headroom inside the original project horizon

Physical probe admission now requires the active probe window to end before the
original child deadline by the sum of its sequential cleanup allowances:

| Existing cleanup path | Configured allowance |
| --- | ---: |
| Local pipe/process teardown | 5 seconds |
| Ordinary container and volume removal | 60 seconds |
| Fallback cleanup after ordinary cleanup fails | 60 seconds |
| Reserved total | 125 seconds |

The allowance is computed from the actual owner and pipe constants and is
included in the prospective execution environment. Both constructor admission
and subsequent non-cleanup effect boundaries check
`probe_deadline + cleanup_allowance <= original_child_deadline`. The exact
boundary is permitted; exceeding it by even one nanosecond is refused before a
shared executor slot or probe intent is acquired. The active window is never
silently shortened to make an invalid plan fit.

This closes a gap in v4: it constrained the active window to the project horizon
but allowed that window to consume all remaining time, leaving separately
bounded cleanup outside the declared whole-project schedule. Shared executor
capacity stays held through cleanup under the existing policy. Once resources
exist, cleanup is still allowed after project expiry; safety cleanup must not be
abandoned because the active deadline was exhausted.

This is a conservative scheduling guard over configured allowances. It is not a
hard bound on every operating-system call, a measured runtime, a qualified raw
byte/workspace envelope, or independent physical qualification. Unexpected
cleanup remains fail-closed and may leave capacity unavailable. Physical fault
controls, exact resource charges, the durable enrollment/admission connection
and a complete matching rehearsal remain necessary before the larger live study.

Execution protocol v5 changes the prospective environment identity. Older
reviews and qualifications retain their original sources. Offline tests use the
actual isolated financial owner and original clock, inert Git and a refused
Engine socket; they add no provider calls or model-quality observations.

The source audit also found these inputs for subsequent resource accounting.
They describe configured limits, not a complete qualified per-cell envelope:

| Component | Configured limit |
| --- | ---: |
| Main journal raw bytes (`Limits.max_raw_bytes`) | 512 MiB |
| Main journal deltas (`Limits.max_external_bytes`) | 64 MiB |
| Fallback diagnostics (`CleanupLimits.max_bytes`) | 128 MiB |
| Candidate writable tmpfs volume | 32 MiB |
| Container memory flags | `--memory=256m --memory-swap=256m` |

Host staging, source/control buffers, the external checkpoint head and other
runtime costs still need an explicit accounting boundary. The existing generic
capacity declarations must not be treated as sufficient merely because their
caller-supplied byte counts fit a budget.
