# B02 intake and direct Store definitions v3

V3 preserves the 241 v2 invocation recipes, their order, and every baseline
expected history. It changes the evaluation policy for exactly three cases:
`intake-directory-wrong-kind`, `intake-zip-wrong-kind`, and
`intake-json-wrong-kind`. The remaining 238 rows are byte-for-byte equivalent as
canonical JSON. No frozen v1/v2 source, definition, or execution is revised.

## Normative disposition

The M1 requirements require bounded directory, ZIP, and JSON intake and rejection
of invalid discovery without admission or catalog changes. They do not
unambiguously classify a top-level filesystem kind mismatch under a particular
error code. The ZIP rule for nonregular members applies inside the archive; it
does not assign an exact code when the archive path itself names a directory.

Sources are the frozen `M1_REQUIREMENTS` in
`gossip_harness/library_project_fixture_v1.py:97–111,181–198`, inventory items
M1-I01/M1-I26/M1-I28, and the precedence rules at
`library-cumulative-product-v1.json:1329` and
`library-cumulative-product-v2.json:1368`. The inventory's proposed coverage-gap
examples are not additional normative requirements. The compiled policy pins
these four source files, alongside the complete inherited definition source set.
It never loads a mutable diagnostic or review artifact as authority.

The previous `io_error` exact comparison was overconstrained. Its retained v2
failure remains a failed historical qualification; v3 does not relabel it.
Independent blind review is retained in
`runs/candidate-b02-v3-blind-policy-review-1.json`
(SHA-256 `4422728bde0c64ea9b0a102570533305cd4c143a6becf89b930568658df24d2b`).
This corrects the definition author's and earlier review's unsupported exact-code
assumption across all three kind mismatches, rather than admitting a particular
observed code.

An unspecified exact code is not a new product obligation or a permanent blocker
to full product acceptance. This legacy evaluator assertion receives no credit.
Acceptance must still establish the actual normative obligations, including
specified errors on grounded I/O cases and the remaining B02/production gaps.

## Supported judgment and transport scope

The host retains the original exact comparison as
`legacy_exact_error_diagnostic`, including its expected and observed frames and
whether it matched. Its replacement assertions are:

- `after.result.0.native-error-rejection`: supported rejection under the qualified
  normalizer representation, with a true/false/null result.
- `after.result.0.exact-error-code`: always null, explicitly unspecified, and
  never awarded M1-I28 exact-code credit—even when the old comparison matches.

An exact normalized frame `{"error": NONEMPTY_STRING}` supports rejection. Every
nonempty string receives the same substantive treatment, and the actual string
is retained; there is no observed-code allowlist. An exact `{"value": JOB}` with
valid six-field public JOB shape establishes a returned JOB and fails rejection,
including a zero-entry JOB. Types, job ID, positive epoch, state, counters and
state-dependent error shape are checked before recognizing that category.

Other native return forms, nulls, malformed frames, unexpected exception frames,
unavailable markers and not-run markers cannot qualify rejection. An unfamiliar
native return such as `{"value":{"error":"x"}}` is unknown applicability of this
normalizer profile, not proof that the product violated an invented exception
class. Direct methods do not universally promise the transport's error frame.
Unknown presentation cannot become a pass.

Every independent result-count, lookup/no-admission, document/blob/job state,
manifest/hash/receipt string and auxiliary-conservation check remains unchanged.
Unknown native presentation alone does not mask these checks or erase a known
failure. Explicit dependency-unavailable markers retain the frozen evaluator's
phase dependency masks and preserve failures observed before that boundary.
The driver separately checks its qualified auxiliary profile; this provider does
not broaden that profile or authorize ignoring private state changes.

`all_local_assertions_passed` remains false for all three policy cases.
`supported_assertions_passed` is the conjunction of supported checks;
`partial_qualification` identifies the limited policy even on failure.
`unspecified_assertions` lists the exact-code cell separately from
`observation_unavailable`. Native frame status is `native-error`, `job-success`,
`unqualified`, or `dependency-unavailable`. Raw frame and observed code are
retained. A supported partial observation is not an ordinary pass, authenticated
execution, full requirement verdict, production registration, or completed B02.

## Identity and qualification boundaries

Each changed row binds the frozen v2 definition, baseline case, unchanged recipe,
baseline expected history and compiled policy digests. All exported objects are
defensive copies; loaded-source guards reject inherited, normative or loaded
provider drift. No caller-supplied aliases or recipes can change the policy set.

`evaluation_policy_sha256()` binds the complete compiled policy.
`evaluation_contract_sha256(case_id, results)` also binds the v3 definition,
actual case, recipe and selected baseline expected history. This selection uses
the frozen immediate/deferred branch logic for the six inherited timing
ambiguities; the receipt never substitutes a default history for the branch
actually scored. The semantic definition digest includes the policy, inherited
identities and all 241 exported rows.

Focused offline controls cover recipe/history preservation, all three policy
cases, arbitrary nonempty codes, successful empty JOBs, unrecognized frames,
known failures under unknown presentation, state and auxiliary conservation,
prior failures before unavailability, valid empty input, source guards, defensive
copies, and selected-history bindings. They execute no candidate code, Docker,
provider call or physical replay. Root verification owns the combined gate and a
new prospective physical qualification after source review and freeze.
