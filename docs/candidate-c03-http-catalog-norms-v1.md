# M1 HTTP catalog norms v1

This source review resolves R01–R04 in the retained 276-row C03 proposal. It
preserves all ten families, the eight existing HTTP target IDs, and the separately
labeled V0-CLI-03 and M1-A08 interactions. It supplies implementable expectation
dispositions; it grants no execution, product acceptance or scientific credit.
The paired JSON contains source fingerprints, exact case mappings and decisions.

The primary source is the frozen V0/M1 prose in
`gossip_harness/library_project_fixture_v1.py:35–212`. The interpretation follows
`analysis/candidate-b03-c03-policy-review-v1.json`. No candidate/reference
implementation, observed output or later M2/M4 parsing rule supplies an expected
answer. Observer source pins are rebound to held commit
`fced9d5bed99c3cc586b7d2ea1e585193d741c4f`; pins establish identity, not new authority.

## R01: closed requests, entry shape and validation timing

All following POST bodies are objects with exactly the listed top-level keys.
Missing a required key or adding another top-level key is `invalid_request`400,
with no action/admission and unchanged prior state. Isolate one defect. A present
empty array is not a missing field. The four submit forms are mutually exclusive.

| Route/form | Exact top-level keys | Supported field domain |
| --- | --- | --- |
| `/api/import` | `source` | Source-key string |
| `/api/export` | `ids` | Array of document-ID strings; empty selection is legal |
| `/api/jobs`, entries | `job_id`, `entries` | Valid job-ID string; entry array |
| `/api/jobs`, directory | `job_id`, `directory`, `namespace` | Valid job ID, confined directory path, valid key namespace |
| `/api/jobs`, ZIP | `job_id`, `zip`, `namespace` | Valid job ID, confined `.zip` path, valid key namespace |
| `/api/jobs`, JSON | `job_id`, `json` | Valid job ID, confined `.json` path; namespace forbidden |
| `/api/jobs/ID/commit` | `epoch` | Prospectively known generated integer epoch for the mandatory state/fencing cases |
| `/api/jobs/ID/prepare`, `/cancel`, `/retry` | none | Exactly `{}` |

Multiple selectors, absent selector, namespace on JSON submission and either
boolean value of HTTP `fail_before_commit` violate those closed top-level forms.
The hook is explicitly offline only. This proves transport nonexposure, not
transaction rollback or the complete M1-A08 obligation. Sources: fixture73–78,
153,177–179,194–199.

**Direct entries.** The supported core is an array of entry objects carrying
required `source` and `text` string fields. Empty entries and empty text are
legal. A non-array `entries`, nonobject member, missing required member field or
nonstring source/text is a structural shape error: `invalid_request`400 at submit,
with no job admission. These decisions resolve all six existing
`HTTP-POST-SHAPES/entries-shape-*` rows. Job-ID shape/domain errors have the same
explicit code and timing. Job IDs have1–64 ASCII alphanumeric, underscore or
hyphen characters. Sources: fixture38–48,101,110,115–116,165–167,194,203–204.

A **nested extra key in a direct entry object** is not expressly closed by the
route's top-level key declaration. The explicit nested “exactly” belongs to the
JSON bundle file schema. Acceptance/rejection, classification and timing for
extra direct-entry keys therefore remain unspecified. Do not automatically label
one an extra POST field. A strict evaluator `Entry(source: str, text: str)`
constructor is a sound authoring constraint; its refusal of other values creates
no additional product requirement.

**JSON bundle file.** This schema is expressly exact:
`{"entries":[{"source":KEY,"text":TEXT},...]}`. Wrong outer/member container,
missing or extra outer/nested fields, and nonstring source/text are schema errors:
`invalid_json`400, no admission. JSON syntax is also `invalid_json`; genuinely bad
file UTF-8 has the separately named `invalid_utf8` mapping. Do not transfer either
file error classification to HTTP JSON parser failures. Sources: fixture101,
183–188; the existing norm policy's body/parser disposition.

**Semantic validation.** Complete string fields can still contain an invalid
source, unsupported suffix or excessive content. Direct shape-valid entries are
validated at prepare/commit, not rejected merely for those semantic defects at
admission. An otherwise schema-valid JSON bundle with semantic source errors is
likewise admitted before prepare. Prepare then persists failed/error=CODE,
completed=0 without document/blob changes. In contrast, discovery file-count,
duplicate-key, expansion and content/archive byte errors retain the explicit
no-admission rules and named classifications. JSON65-file rejection is not
converted into deferred source validation. Do not generalize timing for a new
case without identifying its source category. Sources: fixture122–125,165–176,
183–188.

The failed→retry→prepare history retains its original invalid manifest. Retry
increments the epoch and clears the error while queued; preparing again restores
the same semantic failure at that new epoch. It does not replace the manifest
with a repaired one or prove exact hidden manifest bytes from public state alone.

## Field types without invented parser rules

| Isolated input | Required disposition | Exact code/status boundary |
| --- | --- | --- |
| Nonstring import `source` | Reject; catalog unchanged | Generic400; exact code unspecified |
| Non-array export `ids` | Reject; no partial successful export | Generic400; exact code unspecified |
| Nonstring/missing/duplicate selected ID | Reject; no partial successful export | Exact code and fixed status remain unspecified where classification is unresolved; observe the general reported-code/status relation separately |
| Invalid job-ID type/domain | Reject before admission | `invalid_request`400 |
| Nonstring directory/ZIP/JSON path or namespace | Reject before admission; previous jobs/catalog unchanged | Generic400; exact code unspecified |
| Complete nonobject HTTP JSON body | Reject; no action/admission | Generic400; exact parser/body code unspecified |
| Missing/extra top-level required fields | Reject; no action/admission | `invalid_request`400 |
| Current/different ordinary generated integer commit epoch | Apply state semantics / stale-epoch priority | Wrong epoch is `stale_epoch`409 before state checks |
| Epoch coercion/dialect (`"1"`, `1.0`, `true`, null, fraction, array/object) | Preserve as unspecified input-dialect facet | Do not invent `invalid_request`, `stale_epoch`, a coercion allowlist or a mandatory type-rejection predicate |

Source-key strings, document identities and selected export obligations come from
fixture38–63,73–78. Job shape/types come from115–116,165–167. The epoch contract
starts at1, increments on cancellation/retry, and fences wrong epochs
(fixture116,125–136,172). M1 does not prescribe a general JSON scalar
parser/coercion policy for `epoch`. The boolean exclusion at58–59 is specifically
about pagination offset/limit. Neither it nor later cumulative parsing/range
amendments may be generalized to commit. Required matrix/fencing cases use small,
nonboolean literal integer epochs known from the prospective history.

Unspecified scalar dialects do not hide failures on supported ordinary integer
requests. They also do not authorize a later observation to choose accepted input
forms or exact error codes. Duplicate JSON member handling, surrogate/parser
edge cases and unusual number spelling require their own source disposition;
they are not implicitly covered by a strict host parser.

## R02: paths, source keys and state conservation

Service PATH is relative to its declared root. The inherited source/key rules
require1–256 UTF-8 bytes, at most128 per segment, no backslash/NUL/empty/dot/parent
segment, no absolute path, and no direct or parent symlink. The M1 combined
namespace/member key retains the256-byte and16-segment limits. Directory means a
directory; bundle paths have the corresponding `.zip`/`.json` suffix. Sources:
fixture38–41,97–111,181–199.

A bad input PATH, namespace or directory/ZIP discovery rejects before admission,
with unchanged existing jobs/catalog. Valid nested roots, legal maximum lengths
and source depths must succeed in otherwise valid fixtures. A schema-valid source
string inside direct entries or a JSON bundle follows deferred validation:
queued admission, then failed/completed0 at prepare with no new documents.

Traversal and direct/ancestor symlinks have the explicit `invalid_source`400
classification. This includes the catalog's two namespace-parent representatives
`n/../m`: their prefixed member key contains the same literal parent traversal.
The paired JSON records that source-based classification for those two rows;
it is not an observed-code allowance. Remaining lexical representatives, key
byte/depth bounds, wrong filesystem kind and wrong suffix retain unspecified
exact codes while rejection, generic400 and the applicable state timing remain
supported. Never assign `too_large` to key syntax/length merely because it is the
code for content/archive byte bounds. Sources: fixture185–199; policy
`target_mapping[M1-I31]` and `error_code_dispositions`.

For a deferred source-key bound whose exact error code is unspecified, do not
invent a literal expected failed-job error. A prospectively declared relational
facet may compare the independently captured error response CODE with the later
JOB.error while checking job ID, epoch, failed state, total and completed=0
exactly. The relation is fixed before dispatch; it does not learn an expected
code or permit an observed-code allowlist. Classification remains unspecified.
If either response is unreadable, that relation is unavailable while other known
facets survive. The current whole-value comparator alone does not supply this
partial/relational expectation authority; P01/P03 must implement it explicitly.

Rejected symlink requests and unchanged public state do not by themselves prove
that no target was followed/read. Confined link fixtures still need a versioned
lstat/readlink construction and attribution contract. Physical blob/manifest/
receipt conservation, atomic transactions and internal delegation remain distinct
proof obligations.

## R03: receipt replay and HTTP presentation

Matching completed tokens must replay the same receipt. Receipt semantic values
contain the JOB and source-sorted batch documents, including existing identical
sources once. Wrong epoch takes priority even after completion. These are explicit
underlying obligations (fixture131–139,160–173), preserved by the catalog.

The literal outer HTTP serialization of an action receipt/token/JOB is
unspecified in the reviewed HTTP clause. Keep separate facets for HTTP200,
complete JSON, independently checked supported public state and receipt semantic
identity. Do not recursively mine an arbitrary wrapper, infer receipt equality
from unchanged public jobs/documents, or learn a wrapper from outputs. A
separately qualified method/CLI/storage interface may establish its own prescribed
receipt semantics; actual cross-interface identity requires the same source,
DB, history and epoch lineage. It does not manufacture an HTTP wrapper promise.
Compare semantic values, not whitespace/key order/escaping or exact JSON text.

Thus R03 is resolved as a supported obligation with a clearly limited HTTP
observation surface. It neither awards receipt credit nor adds an unsupported
serialization requirement as a permanent acceptance blocker.

## R04: root response and browser authority

GET `/` must serve the accessible local browser client (fixture70). Browser
list/search/import/show/export, text-safe rendering and visible job state/
cancel/retry remain required (fixture79,154). No exact markup, title, tag pattern,
response MIME or particular DOM is prescribed.

The HTTP-only root row retains authenticated status, headers, body and completion
facts. A200, nonempty body, HTML marker or MIME header cannot alone establish the
required client. Browser-page suitability, accessibility, actions and rendering
remain unqualified until the actual browser checks exercise them. Do not apply
the JSON shape for API success bodies to the root page. Truncation/timeouts are
observation limits rather than new product latency failures.

## Unfinished work remains explicit

R01–R04 are **resolved normative dispositions**, including facets resolved as
unspecified. They are not executed results. R05 still requires literal
materialization of each complete recipe, fixture, known epoch, setup check,
readback, restart and resource count before freeze. The276 rows and ten families
are preserved; source norms do not supply a smaller acceptance denominator.

P01–P10 remain: prospective definitions/expectation authority; fresh attributable
raw facts; independently derived state and relational predicates; versioned root
recipes; confined link fixtures; same-DB epochs; authorized and qualified CLI
handoff; listener controls; storage/architecture proof; and production registry/
whole-cohort authority. Current observer components supply pure comparisons or
historical qualification reanalysis, not product expectation registration.
The JSON prerequisite item statuses retain the earlier catalog proposal baseline;
this cycle is adding declaration and expected-fixture components under separate
qualification. Those components can advance P01/P03 without closing prospective
registry, derived-state or relational-predicate authority.
Pending CLI file approvals remain untouched. No code, verification configuration,
report, Git state, Engine or provider work is part of this source review.
