# Public evaluation shares model executor capacity

The prospective runtime may select `shared_evaluator_capacity` with the exact
`EVALUATOR_CAPACITY_POLICY` from `peer_financial_authority_v5`. The study plan
requires the original-child clock policy as well. Both policies participate in
the resource-contract hash and independently pinned financial permit. Earlier
plans without this field retain their earlier behavior; no old study is
relabelled or requalified.

The real `GossipChildRuntime.evaluate` path now obtains one slot from the same
bounded semaphore used by `CumulativeAuthorityV5.submit`. This adds no pool and
no extra thread. Waiting uses the original child deadline. The runtime rejoins
the clock before waiting and checks its wall/monotonic guard again before
calling the evaluator. The evaluator holds its slot through synchronous
execution and cleanup. A valid sandbox cleanup confirmation releases it;
failed test outcomes can still release capacity when cleanup is confirmed.

The financial lifecycle counts evaluation while it runs. Shutdown waits for the
scope to exit, and terminal sealing refuses active or unresolved evaluation.
Waiting does not hold the terminal lock, so model completions can make progress.
After any wait, the owner rechecks closure, halt state and deadline before entry.
A replaced pool, substituted financial owner, altered policy, expired horizon
or sealed cohort cannot supply an evaluation slot.

If execution raises or returns without confirmed sandbox cleanup, the slot is
retained and the financial owner persists a halt. Runtime cleanup reports the
unresolved evaluator; this cannot become a successful final freeze. In the
selected child-clock policy, a controller-process restart cannot start a new
active horizon. This change is not a new crash recovery protocol or an
independent proof of sandbox cleanup. The existing sandbox receipt supplies the
cleanup fact. In-flight OS and Engine calls are not hard-preempted by admission.

Offline checks use the real joined financial ledger, actual semaphore and model
executor with an in-process synthetic transport. Runtime composition uses the
actual `evaluate` method with a synthetic evaluator and a constructed runtime
shell. These checks do not run Engine, candidate code or live providers, and do
not create model-quality observations or independent execution approvals.

This closes the public-evaluator capacity bypass. Generated-probe declaration
accounting still does not issue executor leases or bind qualified envelopes to
physical probe admission. The ranked comparison policy still needs complete
controller activation. Those launch gates, remaining product acceptance and
physical controls, and the full matched rehearsal remain outstanding.
