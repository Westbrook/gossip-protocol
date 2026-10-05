# Physical execution consumes the original cell assignment

`ProbeExecution` now requires an exact `ProbeCellEnrollment` locator in addition
to the actual financial executor, study plan and observation admission. There is
no default or legacy bypass. The locator points to the original controller,
reservation ledger, cell and assigned execution root; it is not an execution
permission or a caller-supplied success receipt.

Before physical setup, the owner verifies that the enrollment's controller uses
the same financial executor, study plan object, controller journal and child
index. The actual state journal and external checkpoint must occupy the assigned
`raw`, `delta` and `head` paths, and cleanup must use the assigned `cleanup` path.
All paths are canonical. The owner checks its own paths against the actual state
journal, so changing an owner attribute cannot redirect the check.

The locator reads the controller's current prefix through its independent
external head, then reconstructs the complete original enrollment. That repeats
the original reservation, matrix, source, registration, historical predecessor
and root-conflict checks. It never writes a replacement assignment. A later
controller append can be authenticated without changing the original cell record.

The physical owner retains the enrollment record identity and reauthenticates it
at every non-cleanup effect boundary, including after waiting for the shared
executor slot. An unavailable or changed original refuses execution before
intent/setup. An acquired but unused slot is released. Once cleanup is necessary,
loss of enrollment does not prevent the separately bounded safety cleanup path.

Execution protocol v6 includes the accounting source closure in its environment.
Existing reviews and qualifications remain bound to their historical sources.
The separate observation-admission check remains mandatory: pending enrollment
does not prove qualified aggregate resource limits, approve a source review, or
authorize selection or independent acceptance.

## Evidence boundaries

The accounting controls use actual inert Git, anchored controller records and
the full original-enrollment reader, with synthetic financial/mesh evidence.
They cover missing enrollment, changed registration/root, later controller
appends, and disagreement with the independent external head.

The execution-owner controls use real isolated financial authorities, state
journals and the shared semaphore. Their original-enrollment reader is explicitly
mocked through a declared decorator (`__wrapped__`), which the existing loaded-code
guard supports. No integrity checker is disabled; an undeclared substitution is
also tested and refused. These tests do not validate the wrapped reader's behavior.
All surrounding owner/root checks execute normally. They cover required
enrollment, substituted controller/locator, changed identity, rejection after a
capacity wait without intent or resource setup, and continued safety cleanup.
This split establishes the component contracts, not a complete physical run.
The refused Unix socket never reaches an Engine. No candidate or provider runs.

## Remaining work

The ranked controller still needs qualified aggregate byte/time/workspace charges
and complete controller adoption of joined observations. The
[joined cold reader](probe-enrolled-observations-v3.md) now independently
authenticates controller enrollment using both original checkpoints. Complete source
reviews, physical fault controls and a matching full rehearsal remain required
before the live pilot. Reconstruction checks expiry before and after the boundary;
this does not establish hard preemption of every filesystem or Git operation.
These engineering checks add no model-quality samples and prove no statistical
advantage for swarm coordination.
