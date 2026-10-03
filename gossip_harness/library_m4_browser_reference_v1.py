"""Trusted authored M4 browser overlay, never independent acceptance evidence.

The maintained document lifecycle consumes the exact flattened v1 response
schema and displays stable revision identities beside their historical numbers.
Portable export defaults to the inherited v2 format. Selecting revision-aware
v4 explicitly downloads ``research-library-v4.json``. Legacy home/import/jobs, collection maintenance,
Recovery, and the legacy API/export adapters remain available unchanged.
"""

from __future__ import annotations

from .library_m3_browser_reference_v1 import browser_files as m3_browser_files


def _replace(source: str, old: str, new: str, *, count: int = 1) -> str:
    if source.count(old) != count:
        raise ValueError("Frozen M3 browser anchor changed")
    return source.replace(old, new, count)


def browser_files() -> dict[str, str]:
    """Add revision-aware screens without changing the frozen M3 source."""
    browser = m3_browser_files()["library/clients/index.html"]
    start = browser.index("const lifecycleUI = (() => {")
    end = browser.index("async function lifecycleRefresh()", start)
    lifecycle = browser[start:end]

    # Restrict schema changes to lifecycle code. The inherited home and job
    # clients continue to use their original document/receipt representations.
    lifecycle = _replace(
        lifecycle, "'/api/lifecycle/documents/'", "'/api/v1/documents/'"
    )
    lifecycle = _replace(
        lifecycle, "'/api/lifecycle/documents?'", "'/api/v1/documents?'"
    )
    lifecycle = _replace(
        lifecycle, "record.document.text", "record.current_revision.text", count=2
    )
    lifecycle = _replace(
        lifecycle, "record.document.document_id", "record.document_id"
    )
    # This includes the original token/source retained for a document removed
    # by backup restore. A refreshed server record must never bless that draft.
    lifecycle = _replace(
        lifecycle, "record.document.source", "record.source", count=2
    )
    lifecycle = _replace(lifecycle, "const doc = record.document;", "const doc = record;")
    lifecycle = _replace(
        lifecycle,
        "'Revision ' + current.revision + ', edit_version '",
        "'Revision ' + current.current_revision.revision + ' · ' + "
        "current.current_revision.revision_id + ', edit_version '",
    )
    lifecycle = _replace(
        lifecycle,
        "'Revision ' + revision.revision + ' · ' + revision.blob_id",
        "'Revision ' + revision.revision + ' · ' + revision.revision_id + "
        "' · ' + revision.blob_id",
    )
    lifecycle = _replace(
        lifecycle,
        "' · revision ' + record.revision + ' · ' + (record.deleted",
        "' · revision ' + record.current_revision.revision + ' · ' + "
        "record.current_revision.revision_id + ' · ' + (record.deleted",
    )
    # A restore performed by another client may remove a still-visible row.
    # Preserve its local draft on every ordinary open, not only the restoring
    # client's refresh path. Explicit Reload remains the discard boundary.
    lifecycle = _replace(
        lifecycle,
        "if (preserveMissing && drafts.has(id)) {",
        "if (!discard && drafts.has(id)) {",
    )
    lifecycle = _replace(
        lifecycle,
        "async function openRecord(id, discard = false, preserveMissing = false)",
        "async function openRecord(id, discard = false)",
    )
    lifecycle = _replace(
        lifecycle,
        "await openRecord(restoreID, false, true);",
        "await openRecord(restoreID, false);",
    )
    browser = browser[:start] + lifecycle + browser[end:]
    browser = _replace(
        browser,
        '<h2 id="lifecycle-heading">Document lifecycle</h2>',
        '<h2 id="lifecycle-heading">Document lifecycle</h2>\n'
        '  <p>Revision-aware views show the revision number and its stable revision ID. '
        'Edits still use the loaded document edit version.</p>',
    )
    browser = _replace(
        browser,
        '<h3 id="bundle-export-heading">Portable export</h3>',
        '<h3 id="bundle-export-heading">Portable export</h3>\n'
        '  <label>Export format <select id="bundle-format" '
        'aria-describedby="bundle-format-help">'
        '<option value="v2" selected>Portable v2 (legacy)</option>'
        '<option value="v4">Revision-aware v4</option></select></label>\n'
        '  <p id="bundle-format-help">Portable v2 downloads research-library-v2.json. '
        'Choose Revision-aware v4 to include revision IDs in '
        'research-library-v4.json.</p>',
    )
    for control in ("selection", "all"):
        browser = _replace(
            browser,
            'id="bundle-' + control + '"',
            'id="bundle-' + control + '" aria-describedby="bundle-format-help"',
        )
    browser = _replace(
        browser,
        "const body = {ids:all ? null : lifecycleUI.selectedIDs(),",
        "const format = el('format').value;\n"
        "    if (format !== 'v2' && format !== 'v4') { "
        "el('status').textContent = 'invalid_request'; return; }\n"
        "    const endpoint = format === 'v4' ? '/api/v1/export' : '/api/export-bundle';\n"
        "    const expectedFormat = 'local-research-library-export-' + format;\n"
        "    const filename = 'research-library-' + format + '.json';\n"
        "    const body = {ids:all ? null : lifecycleUI.selectedIDs(),",
    )
    browser = _replace(browser, "fetch('/api/export-bundle',", "fetch(endpoint,")
    browser = _replace(
        browser, "value?.format !== 'local-research-library-export-v2'",
        "value?.format !== expectedFormat",
    )
    browser = _replace(
        browser, "anchor.download = 'research-library-v2.json';",
        "anchor.download = filename;",
    )
    browser = _replace(
        browser, "'research-library-v2.json downloaded; ' + bytes.byteLength",
        "filename + ' downloaded; ' + bytes.byteLength",
    )
    browser = _replace(
        browser,
        "busy = true; el('selection').disabled = true; el('all').disabled = true;",
        "busy = true; el('format').disabled = true; "
        "el('selection').disabled = true; el('all').disabled = true;",
    )
    browser = _replace(
        browser,
        "busy = false; el('selection').disabled = false; el('all').disabled = false;",
        "busy = false; el('format').disabled = false; "
        "el('selection').disabled = false; el('all').disabled = false;",
    )
    return {"library/clients/index.html": browser}
