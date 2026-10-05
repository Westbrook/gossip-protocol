# Local verification and release contents

GitHub Actions is disabled for this repository. Verification runs locally through
`devtools.verify`; pushes and pull requests do not run an authored CI workflow.
Retain receipts and reconcile their exact source/runtime identities before
completing a change. Historical receipts remain evidence for their original trees.

## Verification commands

Reuse the pinned development environment and installed tools:

```sh
.venv/bin/python -m devtools.verify --list
env -u GOSSIP_RUN_DOCKER_TESTS .venv/bin/python -m devtools.verify --workers 4
```

The runner starts with syntax/configuration/types/lint, then runs the offline
`fast`, `fixtures` and `git` lanes with a bounded resource budget. Select affected
modules during editing; run all applicable lanes for final qualification.
See [VERIFICATION.md](../VERIFICATION.md) for Docker, independent report and
Playwright commands, pinned dependencies and integration prerequisites. Complete
offline gates before costly integrations. An unrun integration is not a pass.
No GitHub runner, repository variables or Actions token are required.

## Publication

Commit verified checkpoints locally and push only `main` and `gh-pages` when
completing work. Do not publish to Sites. GitHub Pages remains configured from
`gh-pages` at `/`; removing verification automation does not delete its branch or
existing published content. GitHub-managed Pages builds are a hosting service
concern, not a project verification runner; no authored Pages workflow is supplied.

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
