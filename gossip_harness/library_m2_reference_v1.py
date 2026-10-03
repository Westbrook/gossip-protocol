"""Trusted authored cumulative M2 application, never a model-produced result.

The frozen M1 sources remain intact. This composition is for offline reference
qualification only; candidate starts and model prompts must never include it.
The prospective M2 contract and independently authored scenarios still require
separate execution and coverage qualification before a comparative study.
"""

from __future__ import annotations

from .library_m1_reference_v1 import m1_files
from .library_m2_browser_reference_v1 import browser_files
from .library_m2_catalog_reference_v1 import catalog_files
from .library_m2_clients_reference_v1 import clients_files
from .library_project_fixture_v1 import PACKAGE_SCOPES


def m2_files() -> dict[str, str]:
    """Return a fresh complete authored app with cumulative M1 intake support."""
    files = m1_files()
    overlaid: set[str] = set()
    for overlay in (catalog_files(), clients_files(), browser_files()):
        if overlaid.intersection(overlay):
            raise ValueError("M2 reference overlay ownership collides")
        files.update(overlay)
        overlaid.update(overlay)
    # Legacy visible documents omit tombstones at M2, but intake conflicts and
    # capacity must consider them. Keep the original ingestion file unchanged
    # and bind this narrowly scoped cumulative derivative to its exact seam.
    name = "library/ingestion/jobs.py"
    original = "validate_entries(self.store.job_manifest(job_id), self.store.documents())"
    replacement = "validate_entries(self.store.job_manifest(job_id), self.store.all_documents())"
    if files[name].count(original) != 1:
        raise ValueError("Frozen M1 intake validation seam changed")
    files[name] = files[name].replace(original, replacement)
    return files


def package_changes(package: str) -> dict[str, str]:
    """Cumulative authored package overlay relative to the exact M1 reference."""
    if package not in PACKAGE_SCOPES:
        raise ValueError("Unknown library package")
    previous = m1_files()
    return {path: source for path, source in m2_files().items()
            if path.startswith(PACKAGE_SCOPES[package]) and previous.get(path) != source}
