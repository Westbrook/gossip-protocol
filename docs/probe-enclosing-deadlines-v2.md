# Probe source and callback deadlines

The generated-probe plan, state, pipe and execution contracts now carry the
`enclosing-deadline-v2` suffix. The previous contracts and source bytes remain
in Git and in the retained study worktrees. Existing source-layout reviews and
physical packets are not approvals for these changed bytes.

The source adapter supplies one deadline to both accepted-head reads, the
complete tree listing, and the interactive metadata/content capture. It takes
the earlier of the supplied absolute monotonic deadline and 60 seconds from
entry. It reuses the frozen source parser, byte limits and owned-child release
implementation, without invoking their fresh relative-clock entry point.
Neither metadata parsing nor a second Git child renews the allowance.

Active state checks pass their original window, capped by any shorter nested
control deadline. New state creation also uses the active window. Cold evidence
inspection and plan preparation outside active execution have separate audit
clocks; preparation accepts an explicit enclosing deadline for a live caller.
Neither preparation nor a cold read grants dispatch authority.

Every pipe step now enters a required owner deadline scope before frame
retention, source/runtime checks, optional database capture, or continuation.
The execution owner propagates that bound into nested source and Docker-control
operations. Nested scopes may shorten the deadline and restore their enclosing
scope on exit, including on failure. Each Docker-control command applies the
same rule to its own validation, retention and wait.

Normal and emergency cleanup keep their separately declared allowances. The
source helper retains its five-second owned-process reap allowance; failure to
reap remains an error. These are bounded process waits, not a promise to preempt
a stuck operating-system or filesystem call. Journal operations and evaluator
hashing still need their existing byte/resource bounds and post-operation clock
checks. The change does not prove a complete whole-child wall bound.

Verification covers real inert Git, original source/head checks, all four Git
child positions replaced in turn by an owned host-authored sleeper, actual
kill/reap/pipe closure, nested owner bounds and real local pipe handshakes.
The new source tests also check a subsecond parent allowance with controlled
clocks. Synthetic source-review/controller fixtures are not independent
approvals; no candidate or Engine execution is supplied by these checks.

Remaining launch gates include original controller deadline/financial admission,
shared executor integration and successor activation, changed-source reviews,
physical probe and host-fault qualification, incomplete final acceptance routes,
and the matching six-trajectory/four-milestone rehearsal. This fix creates no
new agent-quality sample and makes no comparative efficacy claim.
