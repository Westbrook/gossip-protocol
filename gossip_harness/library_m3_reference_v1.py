"""Trusted authored cumulative M3 reference, never model or candidate input.

Composition is development qualification. It is not an experimental result,
complete acceptance coverage, or evidence that swarm coordination is superior.
Frozen M1/M2 modules remain immutable and retain their own source identities.
"""
from __future__ import annotations

from .library_m2_reference_v1 import m2_files
from .library_m3_backup_format_v1 import format_files
from .library_m3_backup_reference_v1 import backup_files
from .library_m3_browser_reference_v1 import browser_files
from .library_m3_clients_reference_v1 import clients_files
from .library_m3_control_reference_v1 import control_files
from .library_m3_maintenance_reference_v1 import maintenance_files
from .library_m3_worker_reference_v1 import worker_files
from .library_project_fixture_v1 import PACKAGE_SCOPES

_STORE = """from library.catalog.backup import BackupMixin
from library.catalog.maintenance import MaintenanceMixin
from library.catalog.worker import WorkerMixin
from library.catalog.control import CoreStore


class Store(BackupMixin, MaintenanceMixin, WorkerMixin, CoreStore):
    pass
"""


def m3_files() -> dict[str, str]:
    """Return complete independently owned overlays on the frozen M2 tree."""
    files = m2_files()
    owned: set[str] = set()
    for overlay in (control_files(), format_files(), backup_files(), worker_files(),
                    maintenance_files(), clients_files(), browser_files(),
                    {"library/catalog/store.py": _STORE}):
        if owned.intersection(overlay):
            raise ValueError("M3 reference overlay ownership collides")
        owned.update(overlay)
        files.update(overlay)
    return files


def package_changes(package: str) -> dict[str, str]:
    """Cumulative authored package changes relative to the exact M2 reference."""
    if package not in PACKAGE_SCOPES:
        raise ValueError("Unknown library package")
    previous = m2_files()
    return {path: source for path, source in m3_files().items()
            if path.startswith(PACKAGE_SCOPES[package]) and previous.get(path) != source}
