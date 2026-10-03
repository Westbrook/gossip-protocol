# Public documentation publication

This branch contains a read-only, explicit projection of project documentation,
research outputs and progress. It is independent of the harness and local report
server. GitHub Pages publishes the root of `gh-pages`; `.nojekyll` skips Jekyll.

The site is a snapshot. It has no live API, shared feedback or review writes.
Publishing does not mark any local report card reviewed. Public HTML is a
derivative: the publication manifest records both original input and exported
hashes. Uncommitted documentation is labeled as a working-tree snapshot.

The build uses Python's standard library and an existing **pandoc 3.10**. Browser
verification uses an existing **Playwright 1.62.1 / Chromium revision 1234**.
Nothing installs dependencies or calls a model provider. Paths below are inputs
on the publishing operator's machine; they are not embedded in the public data.

```sh
python3 _tools/build_site.py --repo "$REPOSITORY_WORKSPACE" \
  --report "$REPORT_WORKSPACE" --report-state "$FROZEN_REPORT_JSON" \
  --output "$PAGES_CHECKOUT" --pandoc "$PANDOC_EXECUTABLE"
python3 _tools/check_site.py --source-root "$REPOSITORY_WORKSPACE" \
  --report-root "$REPORT_WORKSPACE" --report-state "$FROZEN_REPORT_JSON" \
  --require-sources
python3 _tools/preview.py --root "$PAGES_CHECKOUT" --port 4279
node _tools/browser_check.cjs "$PLAYWRIGHT_MODULE" \
  http://127.0.0.1:4279/gossip-protocol/ "$NEW_BROWSER_RECEIPT"
```

Use a separate deployment checkout. Never run the builder in the research
workspace or copy the report workspace recursively. Only authored root HTML,
linked immutable HTML report artifacts, documentation, and a narrow progress
projection are selected. Local operational artifacts remain text or links to
already published Git source. Raw report state, feedback, handoffs, database
ledgers, provider keys, runtime capabilities, candidate outputs and logs are
excluded. The canonical report stays local and writable.

Before pushing, run both checks, inspect the exact staged file list, then commit
and push this branch without rewriting history. An update is published only
after GitHub reports a successful Pages build and the public pages respond.
Rebuild after a new verified research commit to advance the source identity and
progress snapshot. A static export never certifies the scientific project itself.
