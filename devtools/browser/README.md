# Independent report browser verification

Run one batch after the affected HTML/JavaScript is coherent. Select changed pages
instead of invoking the five historical scripts separately. This development
tooling does not become part of the product or report runtime.

```sh
node devtools/browser/check_syntax.cjs
node devtools/browser/run.cjs --pages report,research,experiments
```

The default borrows the healthy server identified by `.progress-report/project.json`.
Before Chromium starts, the runner verifies its project/workspace identity and
compares the served index and selected HTML byte hashes with the expected files.
An identity mismatch fails; it never restarts or terminates a borrowed server.
Missing selected pages fail rather than silently skipping coverage.

`--pages all` covers report, research, investigation, pilot, experiments, swarm,
swarm-pilot, and sustained-pilot in one Chromium process. Each page has a fresh
browser context, shares the common responsive/navigation contract, and retains
its page-specific anchors and optional final-content assertions. Use
`--require-final` to reject draft result placeholders; the existing `REQUIRE_FINAL`
and `REQUIRE_PILOT_FINAL` flags also work. `--capture research,experiments` saves
selected desktop/mobile screenshots; screenshots and traces are always retained
on failure. `--output PATH` chooses the JSON receipt path. Unique artifact
directories avoid overwriting other batches.

The canonical server is read-only: context routing rejects all unmocked writes.
Report feedback/review UI assertions use an isolated routed fixture, including
exact payloads and review collapse after reload. Actual persistence, lock,
atomicity, and concurrent-writer behavior remain in the separate 13-test report
unit lane. The runner checks canonical feedback/checkpoints before and after; a
real concurrent user review can invalidate that consistency assertion and must
be distinguished from a test mutation.

For CI or an unavailable local server, use `--fixture` instead of `--url`:

```sh
node devtools/browser/run.cjs --fixture --pages report,research
```

This copies the report implementation, state, HTML, and immutable artifacts to
the run's isolated directory and uses the actual report handler on an allocated
localhost port. Trusted environment URLs are relocated in copied files. Source
and canonical state remain untouched. The runner closes its own server in
`finally`; failed snapshots and server logs are retained for diagnosis. A clean
checkout still needs the independent report workspace specified by the locator
(or an explicit `--locator PATH`); the report is deliberately not bundled into
the product. The syntax gate treats an absent independent workspace as optional.

Cancellation is owned by this runner. SIGINT/SIGTERM/SIGHUP stop new work, close
the browser/API and the owned fixture process, and preserve a failed receipt with
partial results. Borrowed servers are never stopped. Chromium keeps its normal
30-second launch allowance; resource cleanup is bounded to four seconds and cancellation to seven
seconds; the final fallback preserves failure evidence and invokes Playwright's
process-exit browser cleanup. Its competing signal handlers are disabled.
Six lightweight lifecycle contracts run through `tests/test_browser_lifecycle.py`
in the normal offline inventory without launching Chromium or a report server.

The pinned environment is Playwright **1.62.1**, Chromium revision **1234**. The
runner rejects other package/revision combinations and never installs tools.
Reuse an installed matching package through Node resolution / `NODE_PATH`. For
a new environment, provision once with `npm ci --ignore-scripts` in this folder
and `npm run install-browser`; keep that installation and the browser cache
between batches. Linux CI may additionally require Playwright system libraries.
The lockfile fixes package versions and official npm registry integrity digests.
Provisioning is a separate explicit step; normal verification never installs.

On the current workstation, the existing bundle can be reused with:

```sh
NODE_PATH=/Users/westbrook/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules \
  /Users/westbrook/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node \
  devtools/browser/run.cjs --pages report,research,experiments
```

No test result is called a fresh execution when reused by a higher-level runner.
Receipts record the actual pages, source bindings, runtime version, launch count,
timings, server ownership, and cleanup status. Timing is observational; one batch
does not establish a general performance improvement.
