# Partial job expectations and fixed error-code relationships

Some source-key rejections must leave a failed job whose `error` equals the
reported error CODE, although the frozen specification does not assign one exact
code for that isolated fault. A whole-value expected body cannot express this
honestly: inserting a guessed code adds a requirement, and copying an observed
code into expected JSON learns the answer from the candidate.

`gossip_harness/candidate_http_relations_v1.py` instead declares equality between
two fixed fields before observations arrive. It separately compares the complete
ordered jobs census against literal expectations, including all unaffected jobs.
This is a pure supplied-value comparison. It neither authenticates observations
nor proves that a declaration existed before dispatch.

## Closed declaration and API

`JobsExpectation(step_id, jobs)` identifies the exact census step and contains
an immutable, ordered tuple of `JobRule` values. Every rule prescribes the six
JOB fields: `job_id`, `epoch`, `state`, `total`, `completed`, and `error`. The error
is a literal string/null, or `ErrorReference(step_id, expected_status=400)` for a
failed job. The reference always selects the source response's top-level
`error` and that literal subject's `JOB.error`; there is no arbitrary JSON pointer,
recursive wrapper search, inferred field name or observed-code allowlist.

The declaration requires unique sorted job IDs, exact known authoring integers,
source-supported state/counter relationships, at least one relation, distinct
source/census step IDs, and consistent source status expectations. Constructor
failures are authoring errors, not candidate product failures. In particular,
strict authoring epochs do not impose a new M1 wire-format integer rule.

`JobsExpectation.record()` serializes the fixed protocol, comparison kind,
census ID and full ordered job rules. A related error appears as:

```json
{"from_error_step":"prepare-1","expected_status":400}
```

The whole record's SHA256 binds those declarations. Status null/404 is available
only for separately source-justified unclassified errors, matching the existing
pure semantics vocabulary. Choosing a declared status is not evidence of its
applicability. The current key-bound failed-state cases prescribe generic400.

`compare(expectation, responses)` takes an immutable tuple of
`ObservedResponse(step_id, ResponseFacts)`. It accepts responses in any order,
rejects unknown or duplicated labels, and treats missing responses as unavailable
facts. It returns separate diagnostics for the jobs response, each error source,
and each subject's error equality. Step labels bind supplied values only; they
cannot establish actual request identity, chronological order or causation.

## What is compared

The jobs body must contain exactly the `jobs` array, with exact expected length,
order, fields and literal values. A related error slot still must be a string.
All other fields and all unrelated jobs remain fully prescribed. Numeric value
equality follows held M1 semantics: legal `1` and `1.0` representations may match;
a boolean is not a number. Exact code spelling is intentionally unspecified.
The error envelope prescribes a string, not a nonempty string, so equal empty
codes are not rejected by a newly invented rule.

Each source response independently receives the held unclassified-error
comparison: prescribed status when applicable, complete JSON, exact error
envelope, and the reported-code/status relationship. Thus a readable equal code
pair cannot hide a wrong status, extra envelope field, malformed response or
incorrect state. The output's `body_shape_value` for the census covers literal
structure/values and string type; `error_code_equality` is a separate required
facet for every declared relation.

The comparator never constructs expected JSON from a reported CODE. It compares
the two supplied strings directly at the declared locations. A mismatch fails
that equality; neither string becomes an accepted code for later observations.
Different explicit relations retain their own source/subject bindings, including
when several subjects deliberately reference the same source response.

## Missing and ambiguous evidence

Representation eligibility uses the unchanged semantic observer's JSON syntax,
content-coding and byte bounds. The caller must supply `Missing` for incomplete
or unqualified body captures. Supplied bytes are treated as complete: this pure
API cannot detect truncation that happens to leave syntactically complete JSON.
Only a future qualified observation bridge can authenticate capture completeness.
Missing bodies and unsupported coding remain unavailable. No permissive second
decoder or last-key-wins parser is used. Parsed object member pairs retain duplicates.

A missing or duplicate source error, missing subject, duplicate subject ID,
duplicate target error or nonstring value leaves the equality unavailable.
Known envelope/shape/type failures remain visible in their independent facets.
A duplicate member that prevents a supported value choice is unspecified in the
body comparison; it never erases a decisive mismatch in another literal field.

Subject lookup scans the complete observed jobs array once. A clean apparent
match plus another row with ambiguous `job_id` members that could designate the
same subject does not establish uniqueness. A duplicate-aware index preserves
that uncertainty without multiplying the scan by the number of relations. This
is evaluator work management, not a product job-count or latency requirement.

## Authority and integration requirements

All diagnostics have fixed `supplied_values_only` authority,
`acceptance_authority=False`, `fresh_execution=False` and
`chronology_verified=False`. The module performs no network, process, storage,
candidate, historical-journal or provider work. The held semantic module's source
identity capture remains part of its existing import behavior; it supplies no
execution authority here.

The source-backed meaning comes from frozen V0 HTTP error handling and M1 JOB,
prepare/commit failure and HTTP interface prose in
`gossip_harness/library_project_fixture_v1.py:72–79,114–144,146–176`, as resolved in
[the catalog norms](candidate-c03-http-catalog-norms-v1.md). The new type is an
explicit implementation of that document's partial-state predicate requirement;
it does not broaden the inventory or discharge an entire requirement ID.

A future qualified bridge must bind this exact declaration before dispatch,
authenticate each source/census response and complete request, prove correct
source/runtime/database/epoch lineage and applicability, and enforce the earlier
setup dependencies. It must consume every supported jobs, source and equality
facet. Considering only equality or only census structure is insufficient.
A pure comparison cannot prove that an earlier error caused a later failed job.

The full literal catalog, changed root/link/CLI mechanics, production mapping,
independent cohort barrier and complete project rehearsal remain separate work.
The old expectation-definition v1 is unchanged; it does not automatically accept
this new partial expectation. A versioned catalog/registration bridge must bind
it explicitly. No historical qualification is upgraded to fresh acceptance, and
this module adds no comparative model sample or evidence of swarm superiority.
