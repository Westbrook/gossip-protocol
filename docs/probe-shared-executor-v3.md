# Generated probes require the shared executor

The [v4 continuation](probe-original-child-clock-v4.md) additionally enforces the
original child clock at physical admission. This document describes the v3 checkpoint.

`ProbeExecution` now requires the actual `CumulativeAuthorityV5` instance in its
constructor. There is no default executor and no separate probe semaphore.
Before admission it checks the financial owner/configuration identity and the
probe subject's cohort, trajectory and execution contract. The permit must
select the same explicit shared-evaluator policy as public evaluation.

`execute_once` acquires one slot from that financial authority before creating
the probe's durable execution intent or entering sandbox setup. It passes the
already registered absolute probe deadline; waiting cannot reset it. It checks
the state and executor again after waiting. The same slot stays occupied through
the physical owner and its cleanup. An uncertain dispatch or cleanup retains the
slot and halts the financial owner, as public evaluation already does. A refusal
before setup releases capacity without pretending that an execution occurred.
The one-shot owner still forbids a second attempt.

The existing container/volume cleanup facts are insufficient by themselves.
The physical terminal now also requires local cleanup: the local child has
terminated, all three streams are closed, and the pipe reports no cleanup errors.
This matters because `ProbePipe.close` records reap/close errors internally.
Those errors must prevent a qualified terminal even when container removal
succeeded. The cold reader requires the new local-cleanup fact. Execution and
reader protocol identities changed; historical packets retain their original
meaning and are not current-source approvals.

The offline integration uses actual isolated financial authorities, semaphores,
Git and anchored journals. Its Engine socket is deliberately not listening.
A separate cleanup check runs only a host-authored `pass` process. Complete cold
record chains are explicitly synthetic. No candidate, Engine or provider is run,
and no independent source approval or model-quality sample is created.

This connects the physical probe owner to actual shared concurrency. It does not
complete the controller's reservation-to-cell binding, original whole-child
clock join, qualified resource-envelope admission, source-context/selection
integration or full ranked policy activation. The existing state admission
capability must still authenticate those controller obligations. Capacity is
not selection or acceptance authority. Remaining product acceptance, exact
independent reviews, physical controls and the complete matching rehearsal stay
on the launch path.
