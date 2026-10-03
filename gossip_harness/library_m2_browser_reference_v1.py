"""Trusted authored M2 browser overlay, never independent acceptance evidence.

This adds the cumulative contract's lifecycle and collection controls to the
unchanged v0/M1 client. Bounded export, backup and recovery controls belong to
M3, whose endpoints are deliberately not advertised by this M2 reference.
"""

from __future__ import annotations

from textwrap import dedent

from .library_m1_clients_reference_v1 import clients_files


def _source(value: str) -> str:
    return dedent(value).lstrip("\n").rstrip() + "\n"


_LIFECYCLE_HTML = _source(r'''
    <section aria-labelledby="lifecycle-heading" id="lifecycle">
      <h2 id="lifecycle-heading">Document lifecycle</h2>
      <form id="lifecycle-filters">
        <label>Query <input id="lifecycle-query" maxlength="256"></label>
        <label>Deletion filter <select id="lifecycle-deletion"><option value="active">Active</option><option value="deleted">Deleted</option><option value="all">All</option></select></label>
        <label>Tag filter <input id="lifecycle-tag"></label>
        <label>Collection filter <input id="lifecycle-collection"></label>
        <label>Page size <input id="lifecycle-limit" type="number" min="1" max="100" step="1" value="100" required></label>
        <button type="submit">Apply filters</button>
      </form>
      <p id="lifecycle-status" role="status" aria-live="polite"></p>
      <p id="lifecycle-page" role="status" aria-live="polite">Loading documents.</p>
      <ul id="lifecycle-records" aria-label="Lifecycle documents"></ul>
      <button type="button" id="lifecycle-previous" disabled>Previous page</button>
      <button type="button" id="lifecycle-next" disabled>Next page</button>
      <section aria-labelledby="lifecycle-selected-heading" id="lifecycle-selected" hidden>
        <h3 id="lifecycle-selected-heading">Selected document</h3>
        <p id="lifecycle-record-state"></p>
        <pre id="lifecycle-current-text"></pre>
        <p id="lifecycle-draft-status"></p>
        <fieldset id="lifecycle-annotations"><legend>Annotations</legend>
          <label>Notes <textarea id="lifecycle-notes" rows="4"></textarea></label>
          <label>Tags <textarea id="lifecycle-tags" rows="3" aria-describedby="lifecycle-tags-help"></textarea></label>
          <p id="lifecycle-tags-help">One normalized name per line. An empty field means no tags.</p>
          <label>Collections <textarea id="lifecycle-memberships" rows="3" aria-describedby="lifecycle-memberships-help"></textarea></label>
          <p id="lifecycle-memberships-help">One registered name per line. An empty field means no collections.</p>
          <button type="button" id="lifecycle-save">Save annotations</button>
        </fieldset>
        <fieldset id="lifecycle-refresh"><legend>Refresh content</legend>
          <label>Refresh text <textarea id="lifecycle-refresh-text" rows="5"></textarea></label>
          <button type="button" id="lifecycle-refresh-from-text">Refresh from text</button>
          <label>Refresh path <input id="lifecycle-refresh-path"></label>
          <button type="button" id="lifecycle-refresh-from-path">Refresh from path</button>
        </fieldset>
        <button type="button" id="lifecycle-delete">Delete document</button>
        <button type="button" id="lifecycle-restore" hidden>Restore document</button>
        <button type="button" id="lifecycle-reload">Reload document</button>
        <section aria-labelledby="lifecycle-history-heading"><h4 id="lifecycle-history-heading">Revision history</h4>
          <ol id="lifecycle-history"></ol>
        </section>
      </section>
      <section aria-labelledby="lifecycle-collections-heading"><h3 id="lifecycle-collections-heading">Collection registry</h3>
        <p id="lifecycle-generation">Catalog generation: loading</p>
        <ul id="lifecycle-collections" aria-label="Collections registry"></ul>
        <label>Collection name <input id="lifecycle-collection-name"></label>
        <button type="button" id="lifecycle-create-collection" disabled>Create collection</button>
        <button type="button" id="lifecycle-remove-collection" disabled>Remove collection</button>
      </section>
    </section>
''')

_LIFECYCLE_STYLE = _source(r'''
    #lifecycle{margin-top:2rem;border-top:2px solid #ccd2ce;padding-top:1rem;overflow-wrap:anywhere}
    #lifecycle label{display:block}#lifecycle textarea{display:block;box-sizing:border-box;width:100%;font:inherit;padding:.45rem}
    #lifecycle fieldset{margin:1rem 0;border:1px solid #ccd2ce;min-inline-size:0}
    #lifecycle input,#lifecycle select,#lifecycle button{box-sizing:border-box;max-width:100%}
    #lifecycle input{min-width:0}
    #lifecycle input[type=number]{min-width:4rem;width:6rem}#lifecycle button:disabled{cursor:default}
    #lifecycle-record-state,#lifecycle-draft-status{overflow-wrap:anywhere}
''')

_LIFECYCLE_SCRIPT = _source(r'''
    const lifecycleUI = (() => {
      const el = id => byId('lifecycle-' + id);
      const selectedIDs = new Set();
      const drafts = new Map();
      let pageOffset = 0, pageGeneration = null, collectionGeneration = null;
      let shown = 0, count = 0, pageSequence = 0, collectionSequence = 0, recordSequence = 0;
      let current = null, draftBase = null, currentID = null, busy = false, loadingPage = false;
      let filters = {q:'', tag:'', collection:'', deleted:'active', limit:100};
      const draftFields = ['notes', 'tags', 'memberships', 'refresh-text', 'refresh-path'];
      const status = text => { el('status').textContent = text; };
      const documentURL = id => '/api/lifecycle/documents/' + encodeURIComponent(id);
      const names = value => value === '' ? [] : value.split('\n');

      function saveDraft() {
        if (!currentID || !draftBase) return;
        const values = {};
        for (const field of draftFields) values[field] = el(field).value;
        drafts.set(currentID, {record:draftBase, values});
      }
      function baseline(record) {
        return {notes:record.notes, tags:record.tags.join('\n'), memberships:record.collections.join('\n'),
                'refresh-text':record.document.text, 'refresh-path':''};
      }
      function fillDraft(record) {
        const values = baseline(record);
        for (const field of draftFields) el(field).value = values[field];
        el('draft-status').textContent = '';
      }
      function unrelatedDraft(action, payload) {
        const submitted = new Set(action === 'annotations' ? ['notes', 'tags', 'memberships'] :
          action === 'refresh' ? [Object.hasOwn(payload, 'text') ? 'refresh-text' : 'refresh-path'] : []);
        const previous = baseline(draftBase), retained = {};
        for (const field of draftFields) {
          if (!submitted.has(field) && el(field).value !== previous[field]) retained[field] = el(field).value;
        }
        return retained;
      }
      function syncButtons() {
        el('previous').disabled = loadingPage || pageOffset === 0;
        el('next').disabled = loadingPage || pageOffset + shown >= count;
        el('create-collection').disabled = busy || collectionGeneration === null;
        el('remove-collection').disabled = busy || collectionGeneration === null;
        el('reload').disabled = busy || !current;
        el('annotations').disabled = busy || !current || current.deleted;
        el('refresh').disabled = busy || !current || current.deleted;
        el('delete').hidden = !current || current.deleted;
        el('restore').hidden = !current || !current.deleted;
        el('delete').disabled = busy || !current;
        el('restore').disabled = busy || !current;
        for (const button of el('records').querySelectorAll('button')) button.disabled = busy;
      }
      function displayRecordState() {
        el('record-state').textContent = 'Revision ' + current.revision + ', edit_version ' + current.edit_version + ', ' + (current.deleted ? 'Deleted' : 'Active');
        if (draftBase.edit_version !== current.edit_version) {
          el('record-state').textContent += '; draft edit_version ' + draftBase.edit_version;
        }
      }
      function displayRecord(record, preserveDraft) {
        current = record;
        currentID = record.document.document_id;
        if (!preserveDraft) draftBase = record;
        el('selected').hidden = false;
        el('selected-heading').textContent = record.document.source;
        displayRecordState();
        el('current-text').textContent = record.document.text;
        if (!preserveDraft) fillDraft(record);
        syncButtons();
      }
      async function history(id, sequence) {
        const value = await request(documentURL(id) + '/revisions');
        if (sequence !== recordSequence || id !== currentID) return;
        el('history').replaceChildren();
        for (const revision of value.revisions) {
          const item = document.createElement('li'), heading = document.createElement('p'), text = document.createElement('pre');
          heading.textContent = 'Revision ' + revision.revision + ' · ' + revision.blob_id;
          text.textContent = revision.text;
          item.append(heading, text); el('history').append(item);
        }
      }
      async function openRecord(id, discard = false) {
        if (busy) return;
        if (!discard) saveDraft();
        const sequence = ++recordSequence;
        try {
          const record = await request(documentURL(id));
          if (sequence !== recordSequence) return;
          const draft = discard ? null : drafts.get(id);
          displayRecord(record, false);
          if (draft) {
            // A stored draft retains its original token; a newer server token
            // never silently authorizes overwriting concurrent changes.
            draftBase = draft.record;
            for (const field of draftFields) el(field).value = draft.values[field];
            displayRecordState();
            el('draft-status').textContent = 'Saved draft retained. Reload document to discard it and use the latest version.';
            syncButtons();
          }
          if (discard) drafts.delete(id);
          el('history').replaceChildren();
          await history(id, sequence);
          if (sequence === recordSequence) status('Document loaded');
        } catch (error) {
          if (sequence !== recordSequence) return;
          status(error.message);
          if (error.message === 'not_found') {
            selectedIDs.delete(id); drafts.delete(id);
            if (currentID === id) {
              current = null; draftBase = null; currentID = null; el('selected').hidden = true;
            }
            syncButtons();
          }
        }
      }
      function renderRows(records) {
        el('records').replaceChildren();
        for (const record of records) {
          const doc = record.document;
          const item = document.createElement('li'), label = document.createElement('label');
          const checkbox = document.createElement('input'), button = document.createElement('button'), detail = document.createElement('span');
          checkbox.type = 'checkbox'; checkbox.checked = selectedIDs.has(doc.document_id);
          checkbox.setAttribute('aria-label', 'Select ' + doc.source);
          checkbox.onchange = () => {
            if (checkbox.checked) selectedIDs.add(doc.document_id); else selectedIDs.delete(doc.document_id);
          };
          label.append(checkbox, document.createTextNode(' ' + doc.source));
          button.type = 'button'; button.textContent = 'Open ' + doc.source;
          button.onclick = () => openRecord(doc.document_id);
          detail.textContent = ' · revision ' + record.revision + ' · ' + (record.deleted ? 'Deleted' : 'Active');
          item.append(label, button, detail); el('records').append(item);
        }
      }
      async function loadPage(reset = false, mayRestart = true) {
        if (reset) { pageOffset = 0; pageGeneration = null; }
        const sequence = ++pageSequence;
        loadingPage = true; syncButtons();
        const query = new URLSearchParams({q:filters.q, deleted:filters.deleted, offset:String(pageOffset), limit:String(filters.limit)});
        if (filters.tag !== '') query.set('tag', filters.tag);
        if (filters.collection !== '') query.set('collection', filters.collection);
        if (pageGeneration !== null) query.set('generation', String(pageGeneration));
        try {
          const value = await request('/api/lifecycle/documents?' + query);
          if (sequence !== pageSequence) return;
          pageGeneration = value.generation; shown = value.records.length; count = value.total;
          renderRows(value.records);
          el('page').textContent = count + ' documents; page offset ' + pageOffset + '; showing ' + shown + '; generation ' + pageGeneration;
        } catch (error) {
          if (sequence !== pageSequence) return;
          if (error.message === 'stale_generation' && mayRestart) {
            await loadPage(true, false);
            if (sequence + 1 === pageSequence) status('stale_generation');
            return;
          }
          el('records').replaceChildren(); shown = 0; count = 0;
          el('page').textContent = 'Page unavailable'; status(error.message);
          throw error;
        } finally {
          if (sequence === pageSequence) { loadingPage = false; syncButtons(); }
        }
      }
      async function loadCollections() {
        const sequence = ++collectionSequence;
        try {
          const value = await request('/api/lifecycle/collections');
          if (sequence !== collectionSequence) return;
          collectionGeneration = value.generation;
          el('generation').textContent = 'Catalog generation: ' + collectionGeneration;
          el('collections').replaceChildren();
          for (const collection of value.collections) {
            const item = document.createElement('li');
            item.textContent = collection.name + ': ' + collection.total;
            el('collections').append(item);
          }
        } catch (error) {
          if (sequence === collectionSequence) {
            collectionGeneration = null; el('generation').textContent = 'Catalog generation: unavailable';
            el('collections').replaceChildren(); status(error.message);
          }
          throw error;
        } finally { syncButtons(); }
      }
      async function refresh() {
        // A refresh updates lists, never an unsaved editor or its loaded token.
        const results = await Promise.allSettled([loadPage(true), loadCollections()]);
        const failure = results.find(result => result.status === 'rejected');
        if (failure) throw failure.reason;
      }
      async function mutateDocument(action, payload) {
        if (busy || !current) return;
        busy = true; syncButtons(); saveDraft();
        const id = currentID;
        const retained = unrelatedDraft(action, payload);
        const sequence = ++recordSequence;
        try {
          const value = await request(documentURL(id) + '/' + action, {expected_version:draftBase.edit_version, ...payload});
          drafts.delete(id); displayRecord(value.record, false);
          // Only submitted fields were saved. Keep unrelated edits visibly
          // unsaved, now based on this successful response's fresh CAS token.
          for (const [field, text] of Object.entries(retained)) el(field).value = text;
          if (Object.keys(retained).length) {
            saveDraft(); el('draft-status').textContent = 'Unsaved draft retained for fields not submitted.';
          }
          await Promise.all([history(id, sequence), refresh(), list()]);
          status(value.status);
        } catch (error) {
          status(error.message);
          if (error.message === 'stale_version' || error.message === 'document_deleted') {
            el('draft-status').textContent = 'Your draft is preserved. Reload document explicitly to discard it and load the current version.';
          }
        } finally { busy = false; syncButtons(); }
      }
      async function mutateCollection(remove) {
        if (busy || collectionGeneration === null) return;
        busy = true; syncButtons();
        try {
          const value = await request('/api/lifecycle/collections' + (remove ? '/remove' : ''), {name:el('collection-name').value, expected_generation:collectionGeneration});
          await refresh();
          // Collection membership does not change in these operations. Refresh
          // the selected server view while retaining its explicit draft token.
          if (currentID) {
            saveDraft();
            const record = await request(documentURL(currentID));
            displayRecord(record, true);
          }
          status(value.status);
        } catch (error) {
          status(error.message);
          if (error.message === 'stale_generation') {
            try { await refresh(); } catch (_) { /* Retain the mutation error. */ }
            status(error.message);
          }
        } finally { busy = false; syncButtons(); }
      }
      el('filters').onsubmit = async event => {
        event.preventDefault();
        const rawLimit = el('limit').value;
        if (!/^[0-9]+$/.test(rawLimit) || Number(rawLimit) < 1 || Number(rawLimit) > 100) { status('invalid_request'); return; }
        filters = {q:el('query').value, tag:el('tag').value, collection:el('collection').value, deleted:el('deletion').value, limit:Number(rawLimit)};
        status('');
        try { await loadPage(true); } catch (_) { /* Page loader exposes the error. */ }
      };
      for (const direction of ['previous', 'next']) el(direction).onclick = async () => {
        if (loadingPage) return;
        pageOffset = direction === 'previous' ? Math.max(0, pageOffset - filters.limit) : pageOffset + filters.limit;
        status('');
        try { await loadPage(); } catch (_) { /* Page loader exposes the error. */ }
      };
      for (const field of draftFields) el(field).oninput = () => {
        saveDraft(); el('draft-status').textContent = 'Unsaved draft';
      };
      el('save').onclick = () => mutateDocument('annotations', {notes:el('notes').value, tags:names(el('tags').value), collections:names(el('memberships').value)});
      el('refresh-from-text').onclick = () => mutateDocument('refresh', {text:el('refresh-text').value});
      el('refresh-from-path').onclick = () => mutateDocument('refresh', {path:el('refresh-path').value});
      el('delete').onclick = () => mutateDocument('delete', {});
      el('restore').onclick = () => mutateDocument('restore', {});
      el('reload').onclick = () => { if (currentID) openRecord(currentID, true); };
      el('create-collection').onclick = () => mutateCollection(false);
      el('remove-collection').onclick = () => mutateCollection(true);
      refresh().catch(error => status(error.message));
      return {refresh};
    })();
    async function lifecycleRefresh() {
      try { await lifecycleUI.refresh(); }
      catch (_) { /* The lifecycle loader displays the current error. */ }
    }
''')


def browser_files() -> dict[str, str]:
    """Return a fresh M2 browser overlay preserving the frozen legacy controls."""
    browser = clients_files()["library/clients/index.html"]
    browser = browser.replace("</style>", _LIFECYCLE_STYLE + "</style>")
    browser = browser.replace("</main><script>", _LIFECYCLE_HTML + "</main><script>")
    # The legacy import and job commit handlers already refresh their list.
    # Refresh lifecycle views after those writes without changing legacy search
    # and pagination behavior or discarding an unsaved lifecycle editor.
    browser = browser.replace("await list();", "await list(); await lifecycleRefresh();")
    browser = browser.replace("</script></body></html>", _LIFECYCLE_SCRIPT + "</script></body></html>")
    return {"library/clients/index.html": browser}
