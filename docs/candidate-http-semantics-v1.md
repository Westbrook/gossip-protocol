# Pure M1 HTTP semantic comparisons v1

`gossip_harness.candidate_http_semantics_v1` is a pure host comparison component.
Its stable protocol is `candidate-http-semantics-v1`. It imports no candidate,
performs no execution, and supplies no journal authentication or acceptance
result. It is integrated from the retained C03 draft with typed immutable facts
and result records; frozen HTTP transport and execution modules remain separate.

`candidate_http_semantics_v1.py` offers immutable `ResponseFacts`, `Missing`,
`ListenerFacts` and validated `Expectation` values. `success`,
`classified_error`, and `unclassified_error` construct expectations;
`compare` returns separate `Facet` rows in a `Comparison`. Results always say
`authority=pure_value_comparison_only` and `product_verdict=not_evaluated`.
There is no authentication boolean, execution, receipt loader, or registry API.
Synthetic values can produce passing comparisons; that grants no authority.

The API separates final status, observed response Content-Type, complete JSON
syntax, exact supported body shape/value, exact assigned error code, general
code/status relation, and before/after listener snapshots. Facets retain
pass/fail/unavailable/unspecified separately and are never collapsed into one
product verdict. A wrong observed status survives an unavailable body; a known
wildcard address survives missing IPv6 coverage; an ambiguous duplicate field
does not erase an unambiguous wrong sibling.

Only the explicitly supported M1 response shapes are compared: health,
documents/list, document/show, legacy export, jobs/list, and errors. The caller
supplies independently derived expected document/job state; this module checks
its closed structural form and compares it, but does not prove expected-state
derivation, document identity mathematics, sorted catalog construction, or
request applicability. HTTP import, submit, single-job and action wrappers use
`success("unspecified")`. Their operations and effects must later be checked
through independently authenticated jobs/documents state reads. No arbitrary
wrapper is recursively searched.

Complete JSON is decoded with object pairs and numeric lexemes retained.
Whitespace, object ordering, Unicode escaping and numeric value-equivalent M1
response spellings are accepted; arrays, multiplicity, exact decoded strings,
newlines, and boolean-versus-number distinctions remain significant. Duplicate
fields stay unspecified, not last-key-wins. The standard byte decoder also
accepts UTF-8 BOM and recognized UTF-16/32 JSON, since frozen M1 does not impose
a response encoding spelling. Unsupported byte decoding, nesting, numerical
decoder limits and the 4 MiB allocation cap remain observation limits. NaN,
Infinity and complete trailing junk fail JSON syntax.

The body input is complete **transfer-decoded** bytes. Missing response headers
or non-identity Content-Encoding leaves body facets unavailable; this component
does not decompress content. That limitation does not grade a compressed
response as a product failure. Missing or non-JSON response Content-Type is
recorded as unspecified: frozen source explicitly requires JSON response
semantics, but only specifies literal application/json for POST requests.
Inventory suggestions to assert response content type do not create a norm.

`unclassified_error(404)` is reserved for unsupported routes. Their explicit
404 does not indirectly require a not_found code via the generic code/status
mapping. `unclassified_error(400)` represents isolated generic validation;
`unclassified_error()` retains an unresolved fixed status while checking the
general reported-code relation when readable. A relation pass does not prove
the reported error classification. `classified_error(code)` still needs a
source-justified isolated cause in the future frozen catalog.

Normative mapping (all paths below refer to the active worktree):

| Comparison | Source clauses |
| --- | --- |
| Six-field documents, list/search/export shape, exact content and order | `gossip_harness/library_project_fixture_v1.py:46–63`; policy `target_mapping[V0-HTTP-01]` |
| Success 200, error envelope and inherited mappings, unsupported route 404 | Fixture `72–79`; policy `target_mapping[V0-HTTP-02]` and `error_code_dispositions.status_policy` |
| Health status ok/schema 0 | Fixture `78`; cumulative-v2 `compatibility[2]`, with M4 schema 4 explicitly outside this M1 component |
| Exact jobs list and job state shape | Fixture `114–129`, `151–154`; policy `target_mapping[M1-HTTP-01]` |
| Assigned job errors and additive409 | Fixture `119–144`, `166–176`, `203–204`; policy `target_mapping[M1-HTTP-02]` |
| Unspecified import/action wrappers and unassigned error codes | Policy `response_wrapper_matrix`, `error_code_dispositions`, `decision_rule` |
| Representation and observation limits | Policy `representation_and_observation_policy`; cumulative-v2 future integer/counter amendments are not backported |
| Actual observed service-port 127.0.0.1 binding | Fixture `70`; policy `labeled_interactions[V0-CLI-03]`; inventory listener obligation |
| Remaining request bodies, intake effects and confinement | Fixture `194–199`; inventory M1-I30/M1-I31; **not discharged by this observer** |

Constructor checks reject malformed evaluator input before comparison. A final
status is an exact integer in 200..599 or `Missing`; informational 1xx is never
promoted into a final response fact. Headers are tuples of two-string tuples,
listener addresses are tuples of strings, and both listener slots require
`ListenerFacts` or `Missing`. Body and expectation bytes are immutable. Missing
reasons and listener limitations have explicit nonempty string types. Facets and
citations are immutable tuples. Comparison protocol, authority and product
verdict are fixed fields that cannot be supplied as constructor options.
These input constraints are not product failures or new product requirements;
the authenticated adapter must turn unsupported evidence into eligible facts or
`Missing`, with retained reasons, before comparison.

The focused offline class is `HttpSemanticFacetsV1Tests`. It covers normative
facets, legal serialization alternatives, ambiguity, limits, independently
known failures and constructor immutability. These synthetic controls supply
component evidence only. Root owns manifest registration, combined verification
and current source/configuration qualification.

`candidate_http_observation_v1` now provides original qualification-journal
fact reanalysis as a separate component. It admits the original externally
anchored history and reconstructs eligible response/listener facts from retained
evidence. This pure semantic module supplies none of that authority. A matching
digest or caller-supplied boolean remains insufficient. The import-time
`LOADED_SOURCE_SHA256` supports detection of already-imported source drift;
the adapter compares loaded and current identities at admission and observation.
Root's combined verification and actual historical reanalysis are still pending.

Prospective expectation registration must separately bind this observer version,
independently derived expectations, the ordered catalog and evaluation purpose.
The ten-family HTTP execution catalog, state/history effects, production
scope/registry mapping, request encoding variants, browser and export behavior,
full M1 acceptance, cumulative project completion, and new comparative
statistical samples remain separate unfinished obligations. Historical
qualification reanalysis is not fresh execution or independent acceptance.
