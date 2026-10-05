# One durable root assignment per reserved probe cell

`enroll_probe_cell` adds one immutable slot to the original controller journal.
Its key derives from the original reservation slot and complete cell identity.
The call first repeats the complete original reservation/probe/source join. It
then binds the complete typed observation registration, execution binding and
one canonical execution root, with fixed `raw`, `delta`, `head` and `cleanup`
children. It creates no directories and calls no candidate or executor.

The registered six-trajectory roster must match the prospective study. The
probe window must fit both the original child horizon and its history limit.
New enrollment requires a live original clock and an unexpired window. Roots
must be fresh and separate from the controller journal, external anchor,
protected source repository and candidate Git view.

Root ownership is checked against enrollment slots derived from the complete
reserved cell rosters, not a caller-supplied index. Different cells cannot share
or nest execution roots or overlap another cell's candidate repository. Every
second enrollment of a cell is refused, even with identical arguments. There is
no new-root retry route. The existing single-owner journal validates the expected
external checkpoint immediately before its single append; an uncertain append
latches the journal shut and retains its evidence.

`inspect_probe_enrollment` is read-only. It reconstructs the original source
join and registration, compares the complete retained assignment, reconstructs
the historical predecessor checkpoint from original deltas, and checks its
chronology and cross-cell root conflicts. Inspection can run after expiry; it
cannot renew a window or issue another root. Current Git source and original
financial/reviewer evidence still must be available.

The retained status is `enrolled_pending_qualification`. It grants no dispatch,
selection or acceptance authority and makes no claim that resource envelopes are
qualified. The typed registration is a prospective identity, not an independent
source review or runtime proof. [Physical execution v6](probe-cell-enrollment-adoption-v6.md)
now requires this enrollment as an additional ownership check. The successor
controller must still join qualified resource limits, independent source approval,
one-shot execution intent and physical observations before activation. An
enrollment alone must never become an admission callback.

Tests use inert Git, anchored journals and synthetic financial, mesh and
registration data. They include an actual anchored append whose acknowledgement
is deliberately lost; the retained uncertain state cannot enroll a replacement.
No candidate, Engine or provider executes and no statistical sample is added.
