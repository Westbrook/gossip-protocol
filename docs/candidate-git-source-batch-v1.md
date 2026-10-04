# Bounded Git source capture v1

This isolated adoption package contains production-named source and tests. Its
protocol is `candidate-git-source-batch-v1`, with no draft suffix. Nothing has
been installed into the frozen evaluator, and no call site, source registry,
active run, progress report, or product expectation was changed.

## Boundary and API

`capture_git_source_batch(path: Path, commit_oid: str, *, timeout_seconds: int = 60)`
returns `(tree_oid, dict[str, bytes])`. Each invocation performs four fresh Git
commands: exact commit/tree identity, complete NUL-framed recursive tree, all
blob type/size headers, and all blob content frames. The caller provides an exact
40-character lowercase SHA1 commit; symbolic refs and noncommit objects fail.

Every boundary recaptures the full tree and bytes. This is not a metadata cache,
content cache, sampled source check, or result-reuse mechanism. It preserves
ordinary-file mode admission, UTF-8 path scope, complete path census, 511-file,
2 MiB per-file and 16 MiB aggregate limits. It rejects symlinks, gitlinks,
private components, malformed paths and duplicate paths. Binary bytes and valid
Unicode, space, tab, newline and leading-dash filenames are supported. No checkout
or filters are involved. Every captured blob independently hashes to its listed
Git object identity. Exact object IDs and the returned byte map must still be
bound to the caller's independently registered source and immutable originals.

Git environment redirection and replacement objects are disabled. Hooks,
fsmonitor and lazy fetching are disabled, and network protocols are denied.
The helper invokes installed Git; it does not import candidate code. The existing
trusted Git store boundary still owns repository/configuration provenance. This
helper does not promise to sandbox a malicious replacement Git executable or
arbitrary repository-local Git configuration.

Each command has bounded stdin, stdout and stderr, with concurrent nonblocking
pipe handling. Size admission finishes before content dispatch. Identity and
size responses use exact LF framing; content frames have exact headers, byte
lengths, LF terminators and no trailing data. Malformed data raises `CaptureError`.

## Explicit deadline and cleanup contract

`DEADLINE_CONTRACT` is
`whole-capture-monotonic-v1;default=60;range=1..120;cleanup-reap=5`.
The integer capture budget is one monotonic deadline shared by all four commands
and host parsing/hash work. Successful command and capture returns check the
deadline, including after cleanup. Boolean, zero and out-of-range budgets fail
before dispatch. Unlike the frozen implementation's independent command budgets,
this is a changed execution contract; new adopter identities and qualification
are required. A clock budget cannot preempt an OS syscall that does not return.

Selector creation precedes child creation. A `finally` boundary surrounds all
owned process setup, selection and waiting. Cleanup independently attempts
selector close, child status/kill/reap, and every pipe close; failure of any one
operation cannot skip the rest. Reaping has a separate five-second maximum wait
budget beginning at cleanup entry. Cleanup failure raises `CaptureCleanupError`
with operation/exception-class names; it never certifies that an unreaped child
was released. The first body or cleanup interruption (`KeyboardInterrupt`, `SystemExit` or
`GeneratorExit`) is preserved through the remaining cleanup attempts and then
re-raised, with cleanup-failure notes when present. No
unrelated process or process group is signaled.

## Adoption seam

Add the new helper as an explicit dependency of a new storage executor, V5
runtime or versioned execution adapter. Those adopters may locally wrap
`capture_git_source_batch(store.path, commit_oid, timeout_seconds=60)` to preserve
the existing tuple interface. Bind the protocol, deadline, exact helper bytes,
ordered suite, runtime/environment, purpose and limits into their source/evidence
identities. Retain original intent bytes and independently expected source
identity. Keep all before/after and reopen captures, admission, CAS, cleanup,
barrier, scope and original-artifact checks at the same call boundaries.

Do not monkeypatch or replace the frozen `capture_git_source` function, change
an existing protocol's behavior without a new identity, or use a source capture
to authorize reuse of independent acceptance/repeatability purposes. Root owns
manifest/type integration, combined static and selected tests, and subsequent
complete matching physical qualification/rehearsal. No Docker, Engine, provider,
performance benchmark or active-run mutation was performed for this package.

## Evidence and limits

The real-Git tests compare the new returned tree and bytes to frozen release-v2
capture on binary, Unicode and path-edge fixtures. Other cases exercise missing,
oversized and disallowed objects, replacement-object/env isolation, exact framing,
aggregate bounds (including exact 511 files, 2 MiB and 16 MiB admission and
16 MiB + 1 rejection), deadline completion, partial setup, interruption and
independent cleanup faults. The production tests have no absolute checkout dependency; `run_scoped.py`
is only the isolated verification bootstrap with the frozen comparison checkout.

For a 61-file source, the frozen path structurally launches 124 Git children per
capture (two fixed commands plus two per file). A complete new capture launches
four, verified by a call counter. This is a 120-child reduction per capture,
not a measured speedup. Applying it at 46 already-identified P05 boundaries would
remove at least 5,520 Git process starts while still recapturing each boundary.
No filesystem/Git/Engine duration attribution follows from those counts.

Preserved original draft: `/tmp/gossip-git-batch-v1-draft-1`, source SHA256
`e115079bb1b43b6126ba25a643a692aa0ad61c7c78e87d46f0adb755822af841`.
Frozen comparison: `candidate_release_execution_v2.py`, SHA256
`6e57c6bd80a95870e95fdb4eb7c289b87a8055febacfe6b6e4c09a1b5ff8a242`.
