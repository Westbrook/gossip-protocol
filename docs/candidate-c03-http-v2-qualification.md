# C03 HTTP v2: real-container mechanics qualified

The revised HTTP executor passed **94 targeted offline checks** and a fresh
**nine-control Docker run** in **170.17 seconds**. Independent review of the
retained evidence found no material discrepancy. This qualifies the bounded
execution mechanics; it adds **zero comparative project samples** and makes no
whole-product or swarm-quality claim.

V2 binds the complete inspection of the independently verified current server
before creating each probe. Under the pinned Docker runtime, only that probe's
startup hostname may change from its own short container ID to the verified
server's hostname. Domain values, source, epoch, network target and all remaining
role restrictions stay exact. The previous [v1 failure](candidate-c03-http-v1-qualification.md)
and its source remain unchanged.

## What ran

The fixture, nine histories, fourteen declared request rows, deadlines and
capture limits match the original prospective mechanics plan.

| Control | Observed behavior |
| --- | --- |
| M01 | Content-Length, chunked and close-delimited responses, plus a complete error response, retain their distinct framing rules. |
| M02 | A known synthetic mismatch survives an incomplete sibling response. |
| M03 | Missing close-delimited EOF remains unavailable after the observation deadline. |
| M04 | An exact wire-cap response completes; one extra byte produces unavailable capture. |
| M05 | Real server exit preserves diagnostic bytes and withholds server attribution. |
| M06 | Loopback reachability does not hide an actual wildcard listener. |
| M07 | A replacement server reopens the same SQLite database through the held state volume. |
| M08 | A real wrong-network probe is rejected before any helper start or HTTP request. |
| M09 | Conflicting Content-Length fields remain ambiguous and unavailable. |

All nine controls passed their declared expectations. The fourteen captured
probe records contain **thirteen authenticated complete request sends**,
**nine complete exchanges**, and **one unknown attributed outcome** from the
intentional server-death control. Incomplete/ambiguous responses are expected
negative controls, not failed software-project attempts. M08 contributes no
request record.

Fifteen probe containers were created and fourteen started. Ten server epochs
and nine keepers ran. All **34 containers and nine volumes** were removed with
retained ownership and absence evidence. M07 is an orderly same-volume server
replacement; it does not establish crash recovery or partition tolerance.

## Evidence and limits

The [qualification checkpoint](../analysis/candidate-c03-http-v2-qualification-checkpoint.json)
binds the source review, 24 source/configuration pins, 484 verifier input pins,
prospective runtime/environment identities, fresh offline and Docker receipts,
and remaining scope. The [public audit projection](../analysis/candidate-c03-http-v2-evidence-audit.json)
is a root-authored summary of the independently retained audit, with the
original audit's identity. It is not a second independent audit.

The audit independently reconstructed all fourteen donor transitions and raw
wire captures, reconciled 1,763 external checkpoints and 238 retained Docker
command records, and checked execution identities and cleanup. The runner's
239 direct Docker launches are a different counting scope. These integrity
checks are neither additional executions nor statistical project samples.

The static gate passed within its declared coverage and existing 117-diagnostic
legacy type baseline. All 94 offline methods and nine physical methods executed
fresh; none was reused. These are targeted checks, not a whole-repository test
or type-safety claim. No provider requests, credential reads, new API spending
or GitHub Pages refresh occurred in this cycle.

## Next required work

A reviewed pure semantic-observer draft has 39 component checks and is retained
but uninstalled. A separate adapter design has 37 proposed regression cases;
those cases have not executed. Integrating these requires original checkpoint
and execution provenance, raw response reconstruction and independent facet
judgments. An authenticated flag or matching digest alone does not supply
execution authority.

The full ten-family HTTP catalog, expected state transitions, confined path
fixtures, cross-interface evidence, production acceptance bridge, remaining
other interface gates and complete cumulative study remain open. Qualification
records cannot be relabeled as fresh independent acceptance. Quality and
sustained whole-project completion remain the primary comparative outcomes;
the prior live evidence still shows no demonstrated general swarm advantage.
