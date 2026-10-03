# Prospective finite CLI qualification, v1

This provider defines **57 histories in 19 families, with 305 finite CLI
invocations**. The longest chosen history has 16 invocations. Every invocation
must be a separate process over its declared persisted DB and input root. These
are development harness qualification fixtures, not independent acceptance or
statistical evidence about the swarm technique. They do not close the four G01
requirement IDs or all of B03.

## Contract and authoring provenance

Expected values are authored from `V0_REQUIREMENTS` and `M1_REQUIREMENTS` in
`gossip_harness/library_project_fixture_v1.py`, the frozen cumulative v1/v2
contracts, and the mandatory obligations distinguished from suggestions in the
M1 acceptance inventory. The provider reads their bytes only to verify pinned
SHA256 identities; it never imports candidate, reference or workflow-oracle code.
Document/source/blob identity formulas, source order, literal casefold matching,
JOB/TOKEN fields, transitions and receipts come from the specification prose.

The first prose read accidentally extended into the beginning of the immutable
seed `_COMMON`: `source_key` and the start of `identity`. No expected result was
derived from that snippet. No candidate/reference files or execution outputs
were inspected. **This authoring is not claimed fully source-blind.** Independent
policy review and exact source review remain separate evidence.

The independently authored supplement
`runs/candidate-b03-cli-wrapper-policy-review-1.json` is retained as historical
review provenance with SHA256
`ae42de0d0d107355868ac7a8820406d61306b26cf2a84a2c68d954909f0d61b5`.
Runtime definitions do not read this ignored receipt. The tracked provider
embeds `candidate-cli-semantic-policy-v1`, and its semantic definition digest
binds that policy plus the historical receipt identity. A fresh checkout needs
only the tracked normative files and provider. The embedded policy prevents
exact CLI import/jobs wrapper shapes from becoming invented
mandatory criteria. The provider records those cells as `stdout.wrapper`
unspecified, while requiring an actual exit 0 and one semantic stdout JSON value.
Subsequent supported show/list/export/job-show observations independently check
the resulting public state. They do not prove an unobserved import status field,
absence of extra durable jobs, or the complete sorted `jobs` census.

## Fixed families

| Family | Histories | Main observations |
| --- | ---: | --- |
| Empty | 1 | Empty list/search/export and pagination |
| Legacy | 1 | Import/reimport, Unicode/literal search, pagination, show, selected/all export |
| Configuration | 1 | Two explicit DBs and two roots, identity independent of absolute root |
| Changed source | 1 | `source_changed` and original document conservation |
| Selection rejection | 2 | Duplicate/missing selected IDs; exact error code unspecified |
| Missing show | 1 | `not_found` |
| Intake success | 3 | Directory/ZIP/JSON and persisted lifecycle/receipt replay |
| Job census | 1 | Out-of-order submissions, individual lookups, replay/conflict; jobs wrapper ungraded |
| Action matrix | 20 | Five states times prepare/commit/cancel/retry |
| Failed retry | 1 | Deferred invalid source, failed state, retained invalid manifest after retry |
| Prepare/import conflict | 1 | Real v0 import between prepare and commit |
| Intake I/O | 3 | Missing directory/ZIP/JSON input, `io_error`, no admitted requested job |
| Namespace | 3 | Required directory/ZIP namespace, forbidden JSON namespace |
| Invalid kind | 1 | Unsupported CLI intake kind |
| Grammar | 7 | Missing/unknown command, missing import/show argument, missing/noninteger epoch, unknown flag |
| Pagination/query | 4 | Negative offset, zero/101 limit, 257-character query |
| Syntax | 2 | Invalid ZIP/JSON syntax |
| UTF-8 | 3 | Invalid directory member/ZIP member/JSON bytes |
| Epoch fence | 1 | Cancel/retry, stale token rejection and current commit |

Input files are immutable fixture bytes. Distinct fixed roots containing the
same source key with different bytes exercise source changes without introducing
a setup mutation language. ZIP archives are inert, deterministic, stored members
with fixed metadata. A conflicting import uses an existing file whose bytes
differ from the already prepared JSON manifest. No internal DB writes initialize
product state. Independent CLI calls establish all job states.

## Shared provider and observer interface

`definitions()` returns a fresh ordered tuple of complete host-only case records.
`case_definition(case_id)` returns a fresh case. `execution_recipe(case_id)`
returns only `case_id`, base64 `fixtures`, explicit `directories`, and ordered
`steps`. Each step has a `step_id` and literal argv:

```
python -m library --db /tmp/db-a.sqlite --root /inputs/root-a COMMAND ...
```

The two-root/two-DB fixture also declares `root-b` and `/tmp/db-b.sqlite`.
All IDs embedded in argv are calculated from the normative identity formulas;
no argument depends on parsing an earlier candidate response. `case_id` and
`step_id` are host metadata. The controller must not mount the recipe, IDs or
expected values into the candidate container.

Host-only `expectations[step_id]` has `kind`, `exit_code`, `value`, `error_code`,
`semantic_value_supported`, `assertion_ids`, and `unspecified_assertion_ids`.
The semantic support flag distinguishes a supported expected JSON null from an
unsupported wrapper whose stored value is also null.

- `success`: `process.exit`, `stdout.json`, and `stdout.value` when supported.
  Unresolved import/jobs wrappers add explicit `stdout.wrapper = unspecified`.
- `domain_error`: `process.exit`, `stderr.error`, `stderr.error_code`. A missing
  normative code leaves the last facet unspecified, never an observed-code list.
- `usage_or_rejection`: `process.exit`; `streams.framing` explicitly unspecified.
  These include cases where the prose requires rejection but does not distinguish
  argparse usage presentation from domain presentation.

The observer owns scoring and exact assertion-census validation. The controller
prefixes these keys with `step_id`, collects independent real process completion,
retains both complete raw streams and validates source/purpose/order binding.
A printed answer without independently observed natural completion cannot pass
`process.exit`.

Successful stdout is one JSON value, compared semantically where supported.
Whitespace, key order and escaping are not canonical-byte requirements. No
blanket empty-other-stream criterion is defined. Whole stderr JSON of the
prescribed error shape is sufficient for domain presentation; mixed diagnostics
are retained as an unqualified framing observation rather than automatically
failed. No substring extraction manufactures a qualifying error frame. Usage
text is not fed through a mandatory domain JSON parser.

## Qualification and scope limits

The current physical execution proposal permits 64 steps/history, 8 MiB of
fixtures/1024 files, 4 MiB per stream and a bounded owned writable volume. These
are evaluator observation limits, not new product bounds. Timeout, truncation,
missing process completion or infrastructure failure cannot become correctness
passes or invented latency/output-size violations. Known independently observed
failures remain failures even if other facets are unavailable or unspecified.

`definition_sha256()` binds ordered recipes, expectations, normative/policy
identities, the embedded semantic policy, historical review identity and the
authoring disclosure. `definition_sources()` verifies exact tracked normative
bytes and adds the provider source hash; it does not require files in `runs/`. Execution must
additionally bind the observer/controller/transport, candidate source,
milestone, purpose, runtime/environment, limits and physical process provenance,
with expected values kept host-only.

Required observer/transport controls include legal JSON variants and auxiliary
diagnostics; wrong exit, stream and semantic value; extra stdout JSON;
stdout-only domain errors; usage prose; output with no completion; ignored DB or
root; wrong namespace routing; lost cross-process persistence; and limits reached
without product-failure reinterpretation. Definition tests perform no candidate,
subprocess, Docker or provider executions. Full combined qualification remains
the root verification owner's responsibility.

Remaining facets include `serve`, exact listener address ownership, HTTP and
browser workflows, real process-kill recovery, genuine concurrency, internal
physical state integrity and complete production acceptance authority. Separate
CLI processes demonstrate the observed persisted interactions, not crash recovery
or a simultaneous schedule. Broader four-milestone/six-trajectory experimental
scope and the final independent acceptance barrier remain unchanged.
