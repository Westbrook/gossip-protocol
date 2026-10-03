# B03 v4 core qualification checkpoint

The v4 process, execution controller and observer passed the combined offline gate: **159 methods**, with static checks passing, no reused class results, and no source or runtime drift. This qualifies the installed core at the recorded source identities. It does not qualify the complete CLI harness or add a comparative research result.

## What changed

The controller registers the intended command independently of Docker inspection. The process validates it against the pinned runtime before dispatch. An explicit `Config.Cmd: null` is allowed only for the exact singleton-entrypoint representation; missing fields, additional arguments and unrelated identity changes still reject. Raw null and empty-list identities remain distinct.

The transport records a completed start response only after it has fully observed and durably retained that response. A lost, malformed or unretained response cannot establish that Docker rejected the command. This record does not establish that a candidate ran or naturally exited. Versioned controller and observer bindings preserve that distinction and the existing product scoring.

The v1/v2/v3 implementations and their failed qualification evidence remain unchanged. The previous v3 missing-executable control failed before transport; this checkpoint does not retrospectively turn it into a pass.

## Verification and pending work

The combined active-worktree check covers 15 command-definition, 40 observer, 33 controller and 71 process methods. Static coverage follows the explicit roots in `pyproject.toml`; this is not a claim of complete type safety for all legacy code.

Three additional qualification files remain reviewed drafts outside the source tree. Their isolated snapshot passed **31 offline methods**, including the actual controller, transport and final qualifier operating over fake low-level IO. The tests distinguish a complete rejection from response bytes saved before a lost EOF, and check owned cleanup order. Independent source review maps all 19 planned regressions and reports no remaining material findings.

Automatic approval review rejected those three file writes under an earlier read-only assignment and a subsequent review was canceled. Root stopped retries and requested explicit human approval of the concrete additions. The 31-method draft result is separate from installed-tree qualification and does not override that pending approval.

After approval, install the exact reviewed files, register their classes and reconcile the combined source/configuration gate. Freeze the complete new version before fresh Docker qualification: one persistence history and all 11 unchanged controls. The remaining 56 histories require that first phase to pass. No v4 physical qualification has run at this checkpoint.

The independently reviewed HTTP specification supplement is prospective C03 design input only. It does not add requirements to the current finite CLI check or establish HTTP acceptance.

## Research meaning

This cycle made no provider calls, consumed no API budget, and added no independent project samples. It improves the reliability of later measurements. Swarm superiority, sustained whole-project acceptance and a causal advantage over orchestration remain unproven.

The [machine-readable checkpoint](../analysis/candidate-b03-v4-core-qualification-checkpoint.json) binds source hashes, retained executions, review evidence, open boundaries and the next action. The [existing v4 process](candidate-client-process-v4.md), [controller](candidate-client-execution-v4.md) and [observer](candidate-client-observation-v4.md) documents describe their contracts.
