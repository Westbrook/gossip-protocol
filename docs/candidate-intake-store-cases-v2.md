# B02 v2: complete JSON input and exact v1 preservation

This version retains all 238 public B02 v1 definition rows, recipes, expected
histories, assertion mappings and ordering **exactly**, then adds three finite
JSON-file histories. It defines 241 reference histories, not 241 successful
executions or comparative samples. The five inherited public-method interposition
histories retain their capability/unavailability semantics.

The new inputs are independently derived from the frozen contract. The earlier
failure on a legal, heavily escaped JSON bundle motivates stronger controls; its
observed outputs do not determine these expected values. No candidate source was
consulted to choose answers. The existing failed execution and frozen v1
expectations remain intact.

## Three additional histories

Each fixture starts with the small existing `intake-json-literal` JSON body and
surrounds it with 3,145,732 bytes of legal JSON whitespace on each side. The
whitespace repeats ASCII space, horizontal tab, carriage return and newline.
Its decoded entries remain unchanged.

| Actual case ID | Raw input bytes | Expected behavior |
| --- | ---: | --- |
| `intake-json-large-whitespace-valid` | 6,291,546 | Submit, inspect manifest, prepare, commit and reopen the same small decoded bundle. |
| `intake-json-large-whitespace-syntax` | 6,291,547 | The same bytes plus `!`: `invalid_json`, no admission and conserved storage. |
| `intake-json-large-whitespace-utf8` | 6,291,547 | The same bytes plus raw `0xff`: `invalid_utf8`, no admission and conserved storage. |

Each input stays below the harness's 8 MiB fixture budget, including the four-byte
seed fixture. The largest base64-bearing recipe is 8,389,463 encoded bytes,
below the separately declared 12 MiB recipe bound. These are evaluator resource
budgets and finite examples; they do not add a product JSON size limit.

The public M1 wording limits **archive bytes**, distinguishes
`submit_zip(archive_path)` from `submit_json(bundle_path)`, and specifies decoded
JSON text's UTF-8 byte accounting. Inventory `M1-I24.GAP-01` expressly preserves
equivalent decoded accounting across escaped/literal representations. No raw
JSON-bundle file limit is declared. The ordinary JSON grammar permits the four
whitespace bytes above. M1 expressly assigns malformed JSON to `invalid_json`
and malformed UTF-8 to `invalid_utf8`, with no job/catalog mutation for invalid
input. Relevant frozen sources are `library_project_fixture_v1.py` lines
97–111 and 181–187, and `library-m1-acceptance-inventory-v1.json` entries `M1-I17`
and `M1-I24`.

The syntax and UTF-8 controls require validation of the complete input. Accepting
a valid prefix is insufficient. The valid case exceeds both the previous 1 MiB
archive-style restriction and a hypothetical replacement cap just above the
existing 3.15 MB escaped fixture. It cannot prove the absence of every arbitrary
raw-input cap; source review and further independent inputs remain distinct work.

## Identity and scorer delegation

`candidate_intake_store_cases_v2.py` exposes the same provider API as v1 under
protocol `candidate-intake-store-cases-v2`. It pins the original semantic digest
and all four original source dependencies, plus its own loaded source. A changed
dependency refuses evaluation. Cached extension data is immutable encoded bytes;
callers receive fresh parsed objects.

Each extension has a fixed, source-declared scorer alias:

| New suffix | Frozen base case |
| --- | --- |
| `valid` | `intake-json-literal` |
| `syntax` | `intake-json-syntax` |
| `utf8` | `intake-json-invalid-utf8` |

The base expected history and ordered public calls are identical to the
extension's. Only the declared `batch` fixture bytes, actual case identity,
facet description and explicit provenance change. The caller cannot supply a
replacement alias or recipe. Before delegation, the provider verifies exact
expected-history/call-sequence equality and fixture isolation. No global
monkeypatch or hidden rebinding changes the frozen v1 scorer.

Each new row binds the actual case ID, base case ID/protocol/definition digest,
base case digest, old/new recipe digests, expected-history digest, call-sequence
digest, fixture path/hash and byte length. The normalized result identifies the
**actual** case and v2 protocol, full v2 definition digest, full case-definition
digest, actual recipe digest and used expected-history digest. An explicit
`scorer_alias` explains the delegation. For the six inherited timing ambiguities,
the result's expected-history digest identifies the actual selected frozen
immediate/deferred branch rather than always naming the default branch.

Delegation preserves strict typed comparisons, exact persisted strings,
conservation checks, complete-case conjunctions and true/false/null outcomes.
Unsupported observations cannot pass; independent earlier failures remain false
while genuinely dependent later expectations become unavailable. No alias turns
an unavailable observation into reusable correctness evidence.

## Verification and remaining scope

Focused checks assert byte-equivalent inheritance of all 238 rows, original
branch/recipe preservation, independently decoded fixture semantics, exact
syntax/UTF-8 distinctions, recipe resource bounds, immutable caller views,
rejected alias mutations and actual result identity. Scorer controls reject
arbitrary `too_large` refusal, swapped error codes, admission/orphans despite a
correct error and lost earlier failures under later unavailability.

Definition tests are not candidate executions. Root qualification must still bind
the actual driver, provider, observer, closed review profiles, source snapshot,
runtime, purpose and complete raw artifacts. The broader cumulative project,
production acceptance authority, matched model comparisons, six-trajectory
barrier, later held-out families and statistical conclusions remain unchanged.
