# Peer Git bundle quarantine, version 1

This additive boundary transfers **actual self-contained Git bundle bytes** from
a peer-owned proposal into a fresh receiver-owned quarantine. It does not read a
sender path during import, execute candidate code, validate project requirements,
select a winning proposal, or authorize publication. Existing Git, sandbox,
promotion, peer transport and text-work v1 modules remain unchanged.

## Public API

```python
payload, manifest = export_bundle(peer_store, offered_sha, base_sha)
quarantine = import_bundle(payload, manifest, fresh_receiver_root)
```

Both commit IDs must be full object IDs. The export must already be pinned at
`refs/harness/proposals/<offered_sha>`, as created by `GitStore.propose`. The base
must be an ancestor of the offered commit. Export copies no remote credentials
or path into its descriptor. It retains commands, bounded stdout/stderr, bundle,
manifest and source inspection in a fresh `peer-bundle-exports/<id>` directory
beside the sender store.

Import accepts immutable bytes and a detached strict manifest. Its output root
must not exist. It retains received bytes and manifest before Git processing,
then creates `quarantine.git`. Bundle v3 must advertise exactly the proposal ref
and object format; prerequisites, additional refs and filters are rejected. The
receiver verifies the bundle, fetches only that ref, checks strict object
connectivity, verifies exact base and offered commits, and reconstructs the
reachable-object and source inventories. Extra unreachable objects are rejected.
Only a fully inspected repository receives the harness-owned store marker.

The returned store has a proposal ref and **no accepted branch**. Read it using
`quarantine.read_files(manifest["offered_sha"])`, and pass it as the source of a
separate common integration gate. Do not call `quarantine.head()` as though
import had accepted its proposal.

The descriptor is bounded to 4 KiB and includes packed-byte identity, object
format, exact base/offer/tree, ref, object and file inventory digests, counts and
expanded sizes. Full inventories stay in retained local receipts. They bind
actual Git blob bytes and modes, avoiding a claim that a checkout or test staging
operation necessarily consumed every source object.

## Supported repository profile

The initial profile intentionally supports small text fixtures. Every reachable
historical commit is inspected, including subsequently reverted paths.

| Bound | Limit |
| --- | ---: |
| Packed bundle | 8 MiB |
| Sum of reachable expanded objects | 32 MiB |
| Individual text blob | 1 MiB |
| Reachable objects / commits | 4,096 / 256 |
| Files per historical tree | 256 |
| File path / depth | 240 ASCII characters / 16 components |
| Git command work per export or import | 60 seconds wall budget, plus bounded process/pipe cleanup |
| One Git process | At most 20 CPU seconds; 16 MiB per written file |
| Captured Git stdout / stderr | At most 2 MiB / 64 KiB |

Only mode `100644`, UTF-8 text without NUL bytes, and supported ASCII path
components are admitted. Symlinks, executable modes, gitlinks, binary files,
dot-prefixed components, unsafe paths and case-insensitive directory/file aliases
are rejected. The narrow profile must be prospectively expanded and qualified
before using ordinary projects that require those features. Task-owned path
scope is a separate integration policy; the caller must still supply it to
`GitStore.prepare`.

Git commands use a shared operation deadline and retained bounded pipe readers.
CPU and file limits are installed by a small isolated Python launcher before
executing Git. System/global Git configuration, replacement objects, hooks,
non-file protocols, automatic maintenance and automatic object unpacking are
disabled. Imports remain packed until object inspection.

Returning the imported store also calls the unchanged `GitStore` constructor,
whose final bare-repository identity check has its own existing 60-second
subprocess timeout. Thus the complete import API is not a strict 60-second
service deadline. A caller's transport timeout must not be treated as proof that
the import did not complete. The success receipt is written only after this
constructor check returns.

These are **not a hard parser-memory sandbox**. Compressed limits and an
expanded-object census are insufficient protection against every hostile native
Git parser input. This cycle covers trusted harness-generated fixture bundles.
Before unrestricted remote or model-influenced repository admission, qualify
the parser inside an appropriate bounded container or equivalent isolation
boundary. No candidate Python or other candidate code executes on the host.

## Integration and final acceptance remain separate

After transport delivers the exact bytes and descriptor, the authority-owned
integration service can call:

```python
candidate = accepted_store.prepare(
    quarantine, manifest["offered_sha"], expected_head,
    fresh_public_validator, allowed_paths=task_scope,
)
```

That gate merges and validates the actual candidate tree, preserves introduced
history scope (including reverted changes), and leaves the accepted reference
unchanged. A subsequent fenced promotion must bind the candidate, exact expected
head, lease epoch, run/task/source generation, validation contract and durable
receipt before delegating to `PromotionCoordinator`.

`GitStore.prepare` can return `noop` without invoking its validator. Preserve
this as already incorporated with no fresh validation; never relabel it as a
tested candidate. `PromotionCoordinator` already requires `prepared`. A
candidate pin alone is also insufficient authorization: `GitStore.accept` checks
the pin, ancestry and expected-head CAS, but knows no evaluator or receipt
identity. A new integration journal must bind those fields before any remote
publication API is enabled.

Public integration evidence belongs to the autonomous work loop. Independent
private acceptance remains behind the whole-cohort freeze barrier and must not
be returned as peer feedback. A published integration commit is not final
project acceptance. Source selection, supersession/reopening, paid provider
ownership and the complete four-cell study remain separate required work.

## Qualification scope

The new test classes are `BundleMetadataTests` (three fast cases),
`GitBundleTests` (nine real-Git cases), and `BundleProcessBoundaryTests` (one
controlled subprocess-cleanup case). The Git class retains a shared immutable
export fixture and individual receiver outputs. It checks supplied-byte import
with the sender unavailable, corruption and descriptor mismatch, historical
unsafe paths/modes (including empty subtrees), case collisions, reverted scope escapes, divergent merged
tree validation, stale expected heads and no-op validation accounting.

The semantic integration fixtures parse JSON with trusted host test code; they
do not execute candidate code. They do not qualify Docker evaluation or final
publication recovery. The verification owner runs the registered lanes after
static checks and independent source review, and records execution evidence
separately from this implementation description.
