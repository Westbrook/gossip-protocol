# Product-v2 candidate delivery observations

`candidate_release_execution_v2` and `candidate_release_observer_v2` add an explicit
product-v2 delivery adapter for candidate Git trees. They preserve the existing
three-case scope: build acknowledgement, exact payload inventory/semantic manifest, and
four separate import/list/show/export CLI processes using the delivered package.
They do not establish the whole M4 release requirement, all v2 amendments,
HTTP/browser behavior, a complete cohort controller, or comparative model results.

## Admission and version boundary

The registered subject must be M4 and pin the exact cumulative-v2 contract:
`2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc`.
Registration admits only the partial association `M4-RELEASE-HANDOFF` and the
ordered cases `release-build`, `release-manifest`, `released-cli-roundtrip`.
This association is not a declaration of complete requirement coverage. A later
scope consumer must authenticate each declaration's real covered obligations.
Gate identity, cohort execution-contract digest, repetition and purpose remain
prospectively owned by the controller. Admission rejects v1 protocols, v1 typed
journal capabilities, wrong product pins, wrong milestones, foreign requirement
associations, and changed order before dispatch.

The executor and observer are explicit frozen forks rather than subclasses or
patches of shared v1 globals. The original executor closes over its module's
observer, protocol and dataclass types; subclassing it would silently retain v1
authority. V1 files and retained evidence are unchanged. V2 uses
`candidate-release-execution-v2`, `candidate-release-observer-v2` and
`complete-public-source-capsule-v2`. Its evaluator identity hashes its own two
modules plus the actual registry, sandbox and Git helpers. No v1 observer or
executor is imported by the v2 runtime.

The product's manifest and output **format names remain v1**, as prescribed by
the cumulative-v2 contract. API versions remain `v0`, `lifecycle-v2`,
`maintenance-v3`, `v1`; storage version is 4. The observer independently expects
the v2 product pin. A v1 product manifest fails even if all file hashes agree.
Actual manifest bytes must match their captured file size and SHA256. All other
package file records must match registered source bytes exactly. The manifest is
strictly parsed (duplicate keys and nonfinite values rejected) and compared by
meaning, allowing deterministic indentation, whitespace and reordered object
keys. The canonical-JSON rule binds the ordered file-record source digest; it
does not prescribe the manifest file's serialization. Wrong manifest fields,
changed record order or changed payload bytes still fail.
The fixed runtime is the existing Python 3.12 image
`sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f`.

## Source and execution authority

The controller captures every ordinary blob of the exact registered Git commit
and tree, including binary compatibility fixtures. This profile requires a
prospectively registered **deliverable-only tree**; it does not define universal
inclusion rules for an arbitrary development repository. It rejects private or
unsafe paths, links, oversized source trees, and a source-side reserved manifest.
Expected bytes come from captured Git blobs, never from candidate claims or the
authored reference generator. The source identity domain includes the v2 executor
protocol. It is distinct from the product manifest's canonical file-record-array
digest; neither should be relabelled as the other.

The original physical mechanics remain: two disposable isolated containers, no
network, read-only source and input mounts, unprivileged candidate commands,
32 MiB local tmpfs capture volume, paused builder before capture, bounded raw tar
capture (40 MiB), host path/type/size inspection without extraction, and a fresh
consumer running only the captured package. Source/package limits remain 16 MiB,
2 MiB per file, at most 511 source files plus the manifest. Four CLI commands run
as separate processes against one new consumer database and fixed UTF-8 input.

The controller retains original command arguments, bounded stdout/stderr bytes,
container and volume identities, cleanup observations, intent, terminal summary
and reconstructed verifier result. Reopening requires an independently retained
exact checkpoint, rejects appended or altered artifacts, and never redispatches
an existing intent. An interrupted intent remains unknown. A fixture journal
cannot authenticate physical evidence. Nonzero Docker exec, truncated transport,
timeouts and cleanup failures remain infrastructure uncertainty, not reusable
correctness results. Completed earlier failed cases survive later failures.

For independent acceptance or repeatability, the controller must authenticate an
irreversible six-trajectory terminal barrier before and after execution; passing
a fabricated callback is outside this trusted-operator model. This adapter does
not implement that cohort controller or authorize any model call. Hashes bind
local authority-owned evidence; they are not signatures or external attestation.

## Qualification plan and limits

`CandidateReleaseV2ObserverTests` exercises host scoring, exact binary delivery,
bounds, invalid/truncated capsules, original CLI shapes and wrong product/protocol
rejection. `CandidateReleaseV2ExecutionTests` exercises Git identity, immutable
registration, journals/checkpoints, cohort barriers, isolated volume plans and
conservative mocked-infrastructure normalization. Its mocked command records are
not physical evidence. `CandidateReleaseV2ExecutionCrashTests` actually exits a
child after durable intent and verifies that reopening cannot redispatch it.

`CandidateReleaseV2DockerTests` is the explicit real Docker lane. It creates one
positive candidate from the authored v2 reference, one legal alternate manifest
serialization control, and one fault control whose
builder corrupts only the emitted CLI after publication. The fault must pass the
build acknowledgement and fail both package and CLI checks; both legal packages
must pass all three observations. Each invocation
retains every external checkpoint, the physical raw journal and normalized
execution, authenticates it through `V2ReleaseObservationSource` using the
external checkpoint, and independently reopens the same journal without dispatch.
The bridge must preserve source, purpose, receipt and every normalized outcome;
it does not provide a scope qualification. These
are development qualification controls, not model-generated samples, independent
held-out product acceptance, or evidence that a larger swarm performs better.
The combined-cycle verification checkpoint records which controls actually ran.
