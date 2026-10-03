# Verification automation and release contents

`verification.yml` runs the same explicit inventory as local development. One
native Ubuntu 24.04 ARM64 job keeps its Python 3.11.15 environment, Node 24.16.0 runtime,
checker caches, Docker daemon, and image alive across the selected lanes. The
central runner starts with syntax/configuration/types/lint and does not start
class workers after a failed static gate. Offline classes finish before any
container download, independent report checkout, or Chromium provisioning.
All selected work uses the runner's bounded resource budget and retained outputs.
No provider credentials are needed or passed to the job.

The release build backend is pinned to setuptools 84.0.0, verified by the source/wheel build.
The Python package lock uses hashes; Playwright is 1.62.1 with Chromium revision
1234, installed once from `devtools/browser/package-lock.json`. Dependency and
checker caches are reusable; test receipts are uploaded rather than used as a
cross-run correctness cache. Hosted-runner environment identities can change,
so a cached dependency directory is not itself evidence of a passing test.
GitHub action major versions are explicit; their current upstream implementation
is not source-pinned. Pin them to reviewed commit SHAs when setting repository
supply-chain policy. The workflow has not been validated on a remote CI service
merely because its YAML exists.

## Required inputs for integration coverage

Offline and Docker checks are required. Docker uses the existing frozen image
`python@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f`
by default. Provisioning explicitly pulls `linux/arm64`, then checks its image ID,
OS, and architecture before tests. It does not substitute a new Python image or
change frozen test deadlines.

The default runner is `ubuntu-24.04-arm`. GitHub documents 4 CPUs for that runner
in public repositories and 2 CPUs in private repositories. This workflow checks
for native Linux ARM64 and at least 4 available logical CPUs before installing
tools. Its four scheduling slots reserve actual host capacity; setting
`--workers 4` does not create more CPUs. A private repository using the standard
two-CPU runner fails that prerequisite with an actionable message. Set
`GOSSIP_CI_RUNNER_LABEL` to an **already configured** native ARM runner with at
least 4 CPUs when needed. This configuration creates no runner and authorizes no
new paid capacity. Larger hosts retain the bounded four-slot offline/Docker
budget. The separate report contention test intentionally reserves eight tokens
for its eight mostly waiting writers; it is not eight CPU-bound validators.
See [GitHub-hosted runner specifications](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).

The job summary explicitly distinguishes unrequested report/browser lanes from
passed checks. Configure these repository variables for the independent report
and require the workflow in branch rules:

- `GOSSIP_CI_IMAGE_REFERENCE`: optional alternate registry location ending in
  `@sha256:<64 hex>`. It must resolve to the same frozen Linux ARM64 image ID,
  `gossip_harness.pilot.DEFAULT_IMAGE`. A mutable tag, different architecture,
  unrelated Python build, or missing image fails; an empty variable uses the
  verified default rather than omitting Docker coverage.
- `GOSSIP_CI_REPORT_REPOSITORY`: `owner/repository` for an independently published
  report implementation and sanitized fixture state. The repository must be
  readable by the workflow token; this project grants no additional access.
- `GOSSIP_CI_REPORT_REVISION`: its exact 40-character source commit. The checkout
  must contain `report.py`, `test_report.py`, `index.html`, `data/project.json`,
  and every immutable artifact referenced by that fixture state.

The report checkout moves outside the product checkout. Only its disposable CI
state and the disposable project locator are relocated. It remains an independent
runtime. Report persistence tests run before one Chromium batch; the batch uses
`--fixture` and closes its own server. A configured but incomplete report fails
instead of becoming an unexpected skip. Current machine-specific progress state
and user feedback must not be copied to a public repository as a setup shortcut.
The report-locator change is confined to the later report/browser observations;
offline receipts retain their original source/configuration binding. The generated
inventory therefore does not call earlier receipts a pass for the relocated tree.

The runner's static gate runs again at each independently selected integration
entry point because its selected inputs/environment differ. No offline class is
repeated by the integration commands. Docker containers and mutable browser
contexts remain isolated even though their installed tools and daemon are reused.

## Source release scope

`MANIFEST.in` includes authored sources, test/configuration files, source pages,
the browser lockfile, study plans, public cumulative contracts/documentation, and
explicitly retained fixtures. It excludes
runtime caches, node_modules, the virtual environment, runs, private environment
files, and local report state. Do not stage the entire `results/` tree. Its only
required test dependency is the seven files under
`results/practical-live-1/accepted/single` (6,235 bytes at audit time). These bind
the original accepted task-report source to `tests/test_research_fixture.py` and
must survive a fresh checkout/source distribution. The six public files under
`fixtures/library-m4-compatibility-v1` are also declared inputs: two SQLite
snapshots, their manifests, their constructor and README. These are small public
compatibility fixtures, not private study execution directories.

The source distribution is the developer verification artifact. Wheel package
selection is controlled separately by `pyproject.toml`; a wheel does not include
the research history or independent report. `devtools` and the two required top-level retention modules are included in
the wheel so its importable commands can resolve their support code. Verify a
built source archive from a fresh extracted directory before declaring it portable;
review the archive listing to ensure no secrets, generated Git repositories, or
unrequested historical outputs were included.

## Evidence inventory

Generate a complete declared inventory without tests or default history scanning:

```sh
.venv/bin/python -m devtools.inventory_report --output runs/inventory-new.json
```

Join explicitly chosen run directories (one level only) or exact summaries:

```sh
.venv/bin/python -m devtools.inventory_report \
  --receipts-root runs/verification \
  --receipt runs/verification-implementation/offline-final/20261001T003138-74431652/summary.json \
  --output runs/inventory-history-new.json
```

Each class/lane group includes its invariant, distinct failure boundary, overlap
rationale, expected outcome, ordered methods, input bindings, resource weight,
watchdog, physical setup/run/cleanup costs, failures, and observed timeouts. The
report checks physical result and log digests before using their costs. Reuse
observations keep the original physical duration and do not add another physical
sample. Missing or altered evidence remains visible without acquiring a passing
status. Historical receipt sources may differ from today's tree, including when
an old watchdog/configuration was the cause of a timeout.

By default even an exact source match remains historical evidence: runtime and
full execution identity were not compared. `--current-inputs <inputs.json>` accepts
an owner-produced current verification identity for that stronger comparison and
rejects it if it does not bind today's authored inputs. An inventory is an audit,
not authorization to reuse scientific observations. No flaky rate is invented
from convenience history, and p95 is omitted with fewer than 20 unique verified
physical samples, and never pooled across different execution identities or selections.
Costs also retain separate strata per exact identity and ordered selection. Outputs
must be new paths; old reports are never overwritten.
