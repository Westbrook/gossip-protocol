"""Trusted authored M3 browser overlay, never independent acceptance evidence.

Compose recovery and bounded portability controls over the frozen M2 client.
No maintenance mutation or worker execution occurs while opening the page.
"""

from __future__ import annotations

from textwrap import dedent

from .library_m2_browser_reference_v1 import browser_files as m2_browser_files


def _source(value: str) -> str:
    return dedent(value).lstrip("\n").rstrip() + "\n"


_EXPORT_HTML = _source(r'''
    <section aria-labelledby="bundle-export-heading" id="bundle-export">
      <h3 id="bundle-export-heading">Portable export</h3>
      <p>Export selection uses the checked document IDs, including selections on other pages. An empty selection exports no documents.</p>
      <label><input type="checkbox" id="bundle-include-deleted"> Include deleted</label>
      <label><input type="checkbox" id="bundle-include-history"> Include history</label>
      <label>Export limit bytes <input id="bundle-limit" type="number" min="1" max="16777216" step="1" value="16777216" required></label>
      <button type="button" id="bundle-selection">Export selection</button>
      <button type="button" id="bundle-all">Export all</button>
      <p id="bundle-status" role="status" aria-live="polite"></p>
    </section>
''')

_RECOVERY_HTML = _source(r'''
    <section aria-labelledby="recovery-heading" id="recovery">
      <h2 id="recovery-heading">Recovery</h2>
      <p>Maintenance uses the same durable library as imports and document editing. Enqueue enrolls eligible jobs; a separately started worker processes them.</p>
      <p id="recovery-generation">Recovery catalog generation: loading</p>
      <label>Backup name <input id="recovery-name" autocomplete="off" placeholder="research.json"></label>
      <button type="button" id="recovery-backup">Create backup</button>
      <button type="button" id="recovery-restore" disabled>Restore backup</button>
      <p>Restore replaces the library with the named backup using the displayed recovery generation. Refresh diagnostics to load a current generation before submitting again.</p>
      <button type="button" id="recovery-reindex">Reindex</button>
      <span>Each reindex action processes at most 64 documents.</span>
      <button type="button" id="recovery-refresh">Refresh diagnostics</button>
      <p id="recovery-status" role="status" aria-live="polite"></p>
      <pre id="recovery-outcome" aria-label="Recovery outcome"></pre>
      <pre id="recovery-diagnostics" aria-label="Diagnostics"></pre>
      <h3>Backups</h3>
      <button type="button" id="recovery-backups-refresh">Refresh backups</button>
      <p id="recovery-backups-status" role="status" aria-live="polite"></p>
      <ul id="recovery-backups" aria-label="Backups"></ul>
      <button type="button" id="recovery-backups-previous" disabled>Previous backups</button>
      <button type="button" id="recovery-backups-next" disabled>Next backups</button>
    </section>
''')

_RECOVERY_STYLE = _source(r'''
    #recovery{margin-top:2rem;border-top:2px solid #ccd2ce;padding-top:1rem;overflow-wrap:anywhere}
    #recovery label{display:block}#recovery input,#recovery button,#bundle-export input,#bundle-export button{box-sizing:border-box;max-width:100%;min-width:0}
    #recovery pre{max-width:100%;white-space:pre-wrap;overflow-wrap:anywhere}
    #recovery-backups{padding-left:1.4rem}#recovery-backups li{margin-block:.6rem}
    #bundle-export{margin-top:1.5rem}#bundle-export input[type=number]{width:12rem}
    #bundle-export input[type=checkbox]{min-width:0;width:auto}
    #recovery button:disabled,#bundle-export button:disabled{cursor:default}
''')

_ENQUEUE_ROW = _source(r'''
    if (job.state === 'queued' || job.state === 'running') {
      const enqueue = document.createElement('button');
      enqueue.type = 'button'; enqueue.textContent = 'Enqueue';
      enqueue.setAttribute('aria-label', 'Enqueue ' + job.job_id);
      enqueue.onclick = () => recoveryUI.enqueue(job.job_id, enqueue);
      li.append(' ', enqueue);
    }
''')

_LIFECYCLE_EXTENSION = _source(r'''
    async function refreshAfterRestore() {
      // Restore changes both the logical catalog and every edit authority.
      // Reload the server view, retaining the saved draft and its old token.
      const restoreID = currentID;
      saveDraft();
      await refresh();
      if (restoreID && currentID === restoreID) await openRecord(restoreID, false, true);
    }
    return {refresh, refreshAfterRestore, selectedIDs:() => Array.from(selectedIDs)};
''')

_MISSING_RESTORE_DRAFT = _source(r'''
    if (preserveMissing && drafts.has(id)) {
      const draft = drafts.get(id);
      selectedIDs.delete(id);
      current = null; currentID = id; draftBase = draft.record; missingDraft = true;
      el('selected').hidden = false;
      el('selected-heading').textContent = draft.record.document.source;
      el('record-state').textContent = 'Document no longer exists after restore; draft edit_version ' + draftBase.edit_version;
      el('current-text').textContent = ''; el('history').replaceChildren();
      for (const field of draftFields) el(field).value = draft.values[field];
      el('draft-status').textContent = 'Your unsaved draft is preserved. Reload document explicitly to discard this missing document draft.';
      syncButtons();
      return;
    }
    missingDraft = false;
    selectedIDs.delete(id); drafts.delete(id);
''')

_RECOVERY_SCRIPT = _source(r'''
    const recoveryUI = (() => {
      const el = id => byId('recovery-' + id);
      let generation = null, busy = false, diagnosticsSequence = 0, backupSequence = 0;
      let backupOffset = 0, backupTotal = 0, backupShown = 0, backupsLoading = false;
      const status = text => { el('status').textContent = text; };
      const literal = value => JSON.stringify(value, null, 2);
      function syncButtons() {
        for (const id of ['backup', 'restore', 'reindex', 'refresh']) el(id).disabled = busy;
        el('restore').disabled = busy || generation === null;
        el('backups-refresh').disabled = busy || backupsLoading;
        el('backups-previous').disabled = busy || backupsLoading || backupOffset === 0;
        el('backups-next').disabled = busy || backupsLoading || backupOffset + backupShown >= backupTotal;
      }
      async function diagnostics() {
        const sequence = ++diagnosticsSequence;
        try {
          const value = await request('/api/maintenance/diagnostics');
          if (sequence !== diagnosticsSequence) return;
          if (!Number.isSafeInteger(value.generation) || value.generation < 0) throw new Error('invalid_response');
          generation = value.generation;
          el('generation').textContent = 'Recovery catalog generation: ' + generation;
          el('diagnostics').textContent = literal(value);
        } catch (error) {
          if (sequence === diagnosticsSequence) {
            generation = null; el('generation').textContent = 'Recovery catalog generation: unavailable';
            el('diagnostics').textContent = error.message;
          }
          throw error;
        } finally { syncButtons(); }
      }
      async function backups(reset = false) {
        if (reset) backupOffset = 0;
        const sequence = ++backupSequence;
        backupsLoading = true; syncButtons();
        try {
          const value = await request('/api/maintenance/backups?offset=' + backupOffset + '&limit=100');
          if (sequence !== backupSequence) return;
          backupTotal = value.total; backupShown = value.backups.length;
          el('backups').replaceChildren();
          for (const backup of value.backups) {
            const item = document.createElement('li');
            item.textContent = backup.name + ': ' + backup.bytes + ' bytes; generation ' + backup.generation + '; SHA-256 ' + backup.payload_sha256;
            el('backups').append(item);
          }
          el('backups-status').textContent = backupTotal + ' backups; page offset ' + backupOffset + '; showing ' + backupShown;
        } catch (error) {
          if (sequence === backupSequence) {
            backupShown = 0; backupTotal = 0; el('backups').replaceChildren();
            el('backups-status').textContent = error.message;
          }
          throw error;
        } finally {
          if (sequence === backupSequence) { backupsLoading = false; syncButtons(); }
        }
      }
      async function readRecovery() {
        const results = await Promise.allSettled([diagnostics(), backups(true)]);
        const failure = results.find(result => result.status === 'rejected');
        if (failure) throw failure.reason;
      }
      async function action(operation, body) {
        if (busy) return;
        busy = true; ++diagnosticsSequence; syncButtons(); status('');
        el('outcome').textContent = '';
        try {
          const endpoint = {backup:'backups', restore:'restore', reindex:'reindex'}[operation];
          const value = await request('/api/maintenance/' + endpoint, body);
          el('outcome').textContent = literal(value);
          status(operation + ' succeeded');
          // Read committed state. A refresh failure cannot retry the mutation.
          const refreshes = [readRecovery()];
          if (operation === 'restore') refreshes.push(lifecycleUI.refreshAfterRestore(), list(), showJobs());
          const results = await Promise.allSettled(refreshes);
          const failure = results.find(result => result.status === 'rejected');
          if (failure) status(operation + ' succeeded; refresh failed: ' + failure.reason.message);
        } catch (error) {
          status(error.message);
          // Expose persisted maintenance diagnostics after failure, without
          // retrying or replacing the generation the user deliberately used.
          const submittedGeneration = generation;
          try { await diagnostics(); } catch (_) { /* Keep the action error. */ }
          if (operation === 'restore' && error.message === 'stale_generation') {
            generation = null;
            el('generation').textContent = 'Recovery catalog generation: ' + submittedGeneration + ' (stale; refresh diagnostics)';
          }
          status(error.message);
        } finally { busy = false; syncButtons(); }
      }
      async function enqueue(id, button) {
        if (busy) return;
        busy = true; button.disabled = true; ++diagnosticsSequence; syncButtons();
        status(''); el('outcome').textContent = '';
        try {
          const value = await request('/api/maintenance/jobs/' + encodeURIComponent(id) + '/enqueue', {});
          el('outcome').textContent = literal(value);
          status(id + ': ' + value.status);
          byId('jobs-status').textContent = id + ': ' + value.status;
        } catch (error) {
          status(error.message); byId('jobs-status').textContent = id + ': ' + error.message;
        } finally {
          // Rendering hides enqueue controls for jobs that became terminal.
          await Promise.allSettled([showJobs(), diagnostics()]);
          busy = false; button.disabled = false; syncButtons();
        }
      }
      el('backup').onclick = () => action('backup', {name:el('name').value});
      el('restore').onclick = () => {
        if (generation !== null) action('restore', {name:el('name').value, expected_generation:generation});
      };
      el('reindex').onclick = () => action('reindex', {limit:64});
      el('refresh').onclick = async () => {
        if (busy) return;
        status('');
        try { await diagnostics(); status('Diagnostics refreshed'); }
        catch (error) { status(error.message); }
      };
      el('backups-refresh').onclick = () => backups(true).catch(() => {});
      for (const direction of ['previous', 'next']) el('backups-' + direction).onclick = () => {
        if (busy || backupsLoading) return;
        backupOffset = direction === 'previous' ? Math.max(0, backupOffset - 100) : backupOffset + 100;
        backups().catch(() => {});
      };
      // Page initialization performs reads only. Serve never starts a worker.
      readRecovery().catch(error => status(error.message));
      return {enqueue};
    })();

    (() => {
      const el = id => byId('bundle-' + id);
      let busy = false;
      async function exportBundle(all) {
        if (busy) return;
        el('status').textContent = '';
        const rawLimit = el('limit').value, limit = Number(rawLimit);
        if (!/^[0-9]+$/.test(rawLimit) || !Number.isSafeInteger(limit) || limit < 1 || limit > 16777216) {
          el('status').textContent = 'invalid_request'; return;
        }
        const body = {ids:all ? null : lifecycleUI.selectedIDs(), include_deleted:el('include-deleted').checked,
                      include_history:el('include-history').checked, max_bytes:limit};
        busy = true; el('selection').disabled = true; el('all').disabled = true;
        try {
          const response = await fetch('/api/export-bundle', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
          // Preserve the server's canonical UTF-8 bytes rather than serializing
          // parsed JSON with JavaScript's different key ordering/number rules.
          const bytes = await response.arrayBuffer();
          let value;
          try { value = JSON.parse(new TextDecoder('utf-8', {fatal:true}).decode(bytes)); }
          catch (_) { throw new Error('invalid_response'); }
          if (!response.ok) throw new Error(typeof value?.error === 'string' ? value.error : 'invalid_response');
          if (value?.format !== 'local-research-library-export-v2' || !Array.isArray(value.documents) ||
              !Number.isSafeInteger(value.generation) || value.generation < 0) throw new Error('invalid_response');
          if (bytes.byteLength > limit) throw new Error('too_large');
          const url = URL.createObjectURL(new Blob([bytes], {type:'application/json;charset=utf-8'}));
          const anchor = document.createElement('a');
          anchor.href = url; anchor.download = 'research-library-v2.json';
          document.body.append(anchor); anchor.click(); anchor.remove();
          setTimeout(() => URL.revokeObjectURL(url), 1000);
          el('status').textContent = 'research-library-v2.json downloaded; ' + bytes.byteLength + ' bytes; ' + value.documents.length + ' documents';
        } catch (error) { el('status').textContent = error.message; }
        finally { busy = false; el('selection').disabled = false; el('all').disabled = false; }
      }
      el('selection').onclick = () => exportBundle(false);
      el('all').onclick = () => exportBundle(true);
    })();
''')


def _replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise ValueError("Frozen M2 browser anchor changed")
    return source.replace(old, new, 1)


def browser_files() -> dict[str, str]:
    """Return the cumulative client with explicit M3 maintenance operations."""
    browser = m2_browser_files()["library/clients/index.html"]
    browser = _replace_once(browser, "</style>", _RECOVERY_STYLE + "</style>")
    browser = _replace_once(
        browser,
        '<section aria-labelledby="lifecycle-collections-heading">',
        _EXPORT_HTML + '<section aria-labelledby="lifecycle-collections-heading">',
    )
    browser = _replace_once(browser, "</main><script>", _RECOVERY_HTML + "</main><script>")
    browser = _replace_once(
        browser, "const drafts = new Map();", "const drafts = new Map(); let missingDraft = false;"
    )
    browser = _replace_once(
        browser,
        "el('reload').disabled = busy || !current;",
        "el('reload').disabled = busy || (!current && !missingDraft);",
    )
    browser = _replace_once(
        browser,
        "async function openRecord(id, discard = false)",
        "async function openRecord(id, discard = false, preserveMissing = false)",
    )
    browser = _replace_once(
        browser,
        "function displayRecord(record, preserveDraft) {",
        "function displayRecord(record, preserveDraft) { missingDraft = false;",
    )
    browser = _replace_once(
        browser, "selectedIDs.delete(id); drafts.delete(id);", _MISSING_RESTORE_DRAFT
    )
    browser = _replace_once(browser, "return {refresh};", _LIFECYCLE_EXTENSION)
    browser = _replace_once(browser, "byId('jobs').append(li);", _ENQUEUE_ROW + "byId('jobs').append(li);")
    browser = _replace_once(browser, "</script></body></html>", _RECOVERY_SCRIPT + "</script></body></html>")
    return {"library/clients/index.html": browser}
