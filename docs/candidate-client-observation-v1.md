# Finite CLI observation, version 1

This host-only observer grades finite CLI process evidence from B03 C01/C02. It
never runs or imports a candidate, reads a candidate artifact from its path, or
treats candidate stdout as a supervisor envelope. It issues no Registry or C06
production acceptance authority. A local observed match is evidence for the
listed assertions in one invocation, not acceptance of a case, milestone,
complete application, or statistical hypothesis.

`process_observation(binding, transport, stdout, stderr)` freezes canonical JSON
metadata and both byte streams into a frozen `ProcessObservation`. No nested
mutable dictionaries are retained. Hashes, lengths, completeness flags, exact
binding fields and independent completion facts are structurally checked again
when scoring. The caller must authenticate the execution journal, retained raw
Docker responses and artifacts, staged source identity before/after execution,
container configuration and ownership. A well-formed fabricated dictionary is
not an authenticated observation.

The binding includes exact source, commit/tree, requirements, milestone, purpose,
definition, ordered-suite, fixture, runtime, environment, resource-limit and
evaluator identities, plus case/step identity, ordered step IDs and argv. The
ordered-suite digest identifies the intended suite; it does not say every case
was executed. This version admits `harness_qualification` only. The controller
must verify that the supplied expectation is the registered expectation for the
bound definition/case/step. Score output binds both the supplied expectation
hash and the observation hash.

`observe_cli_step(expected, observation)` returns an exact assertion census with
`True`, `False` or `None`, a disposition and a reason for every key. Passing only
supported assertions is reported separately from passing every local assertion.
Missing observations retain the expected key set as unavailable; explicit
unspecified facets remain separately unspecified. A known false assertion
survives unknown siblings. Incorrect expectation rosters and malformed raw
metadata raise evaluator errors instead of manufacturing a candidate verdict.

## Process and stream evidence

Actual natural completion requires consistent, source-bound Docker wait and
inspect evidence: the started candidate exited naturally, the identities match,
exit codes agree, no supervisor kill/OOM/state/wait error occurred and completion
time exists. The transport conservatively treats signal-like exits as ambiguous.
No candidate-produced marker supplies those facts. The process transport owns
raw Docker proof verification; the observer also checks the corresponding
structural proof flags.

For a shared-state history, the controller additionally supplies
`history_state_verified` after validating the state keeper before/after each
invocation. A false value leaves semantic output unavailable even if natural
completion and raw capture are proven. The independently observed exit comparison
is retained. The controller must classify a lost history precondition as
infrastructure/observation failure; a local wrong-exit comparison does not
establish that the candidate caused the failure. The optional field is absent
for isolated raw-process controls that make no shared-history claim.

A channel's whole-output semantics require both that natural completion evidence
and complete, untruncated raw bytes for that channel. EOF obtained by killing a
hung process proves only which bytes were retained. A correct-looking prefix
followed by a hang cannot pass final JSON assertions. Identity-unproven or
unnaturally terminated records produce no semantic pass or failure. Conversely,
a known wrong natural exit remains false when output capture is unavailable.

The transport's declared deadlines and default 4 MiB per-stream cap are evaluator
resource bounds, not product latency or output-size requirements. This observer
also limits total supplied raw stream allocation to 32 MiB and JSON nesting to
256 levels; those limits never create a product failure. A sibling channel's
unavailable capture does not erase a fully observed supported channel. The
controller must still report infrastructure/completeness limitations separately
from local assertion results.

## Supported semantic predicates

- Success: actual exit 0 and exactly one complete JSON value on stdout. Where the
  expectation has explicit semantic authority, compare that value recursively.
  Whitespace, object key order and lawful Unicode escaping are irrelevant. JSON
  numbers such as `1` and `1.0` compare by numeric value; booleans never equal
  numbers. Arrays retain their order and objects retain their complete key set.
- Domain/I/O error: actual exit 2, the prescribed `{"error": CODE}` on stderr,
  and an exact code only when the frozen specification assigns it. Entire stderr
  containing the error object is sufficient evidence. A known pure JSON object
  with the wrong shape or a wrong supported code fails the respective predicate.
  Empty stderr or complete UTF-8 prose with no object-opening `{` token proves
  the required JSON error object absent. This is an omission proof, never an
  extractor or substring-based success criterion. A correct code
  in an object with extra fields does not erase the independently wrong shape.
- Usage or a normatively unresolved usage-versus-rejection boundary: actual exit
  2. The fixture explicitly leaves presentation ungraded.

There is no blanket empty-other-stream assertion. Optional stderr diagnostics on
success or stdout diagnostics on a domain error do not invent another failure.
The frozen clauses do not explicitly prescribe a universal pure-stderr rule.
Mixed diagnostic/error framing is therefore ungraded; the observer never scans
for a desired error code or extracts an expected JSON substring to pass it.

JSON duplicate member names, excessive nesting, and unrepresentable numeric
parser cases are unqualified. No convenient last duplicate wins. Extra JSON
values, plain non-JSON stdout, empty stdout, invalid UTF-8 stdout and nonstandard
NaN/Infinity are known violations of the complete one-JSON-value success rule
when natural completion and stream capture are proven. Raw bytes remain bound
for every outcome.

The CLI import and jobs outer wrappers remain explicit unspecified assertions,
as directed by the source-only policy review. Satisfying generic JSON output
alone does not prove correct import or job semantics; the authored histories
also exercise separately supported persisted-state observations. Unknown exact
error codes remain null even when a candidate happens to use a familiar code.
No observed-code or observed-wrapper allowlist is introduced.

## Scope and checks

The offline controls exercise immutable/source/ordered-suite binding, forged
stdout supervision, natural completion, wrong exit with unavailable streams,
forced-kill prefix capture, source identity loss, semantic JSON and booleans,
usage, correct/wrong error shape and code, wrong channel, optional diagnostics,
mixed framing, explicit unknown wrappers/codes, and complete assertion censuses.
These are synthetic host-only observations, not physical candidate execution.
Root owns the central verification manifest, prospective freeze, combined checks
and any Docker qualification. The observer alone does not establish HTTP,
browser, listener ownership, physical storage conservation, process recovery,
production acceptance applicability or unseen statistical evidence.

Normative inputs are the frozen V0/M1 requirements in
`gossip_harness/library_project_fixture_v1.py`, the cumulative product contracts
and the mandatory-obligation inventory. The source-only client policy and CLI
wrapper supplement in `runs/candidate-b03-blind-client-policy-review-1.json` and
`runs/candidate-b03-cli-wrapper-policy-review-1.json` distinguish supported
predicates from inference; neither changes the frozen product contract.
