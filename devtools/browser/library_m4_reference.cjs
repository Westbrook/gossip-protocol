#!/usr/bin/env node
// Physical M4 RELEASE-output qualification, explicitly adapted from the frozen
// M2/M3 workflows for flattened RECORD4 and /api/v1. Not independent acceptance.
// Every HTTP response and actual bounded download is retained as raw bytes.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const {setTimeout: delay} = require('node:timers/promises');
const {createLifecycle} = require('./lifecycle.cjs');
const {runtime, responsePolicy, INTAKE_CONTROL} = require('./library_m1_acceptance.cjs');
const {checkedOrigin, checkedRequest} = require('./library_m2_reference.cjs');
const digest = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
const LIMIT = 262144;
const canonical = value => {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).sort()
    .map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}';
  return JSON.stringify(value);
};

function loopbackHTTP(origin, request) {
  return new Promise((resolve, reject) => {
    const url = new URL(origin);
    const call = http.request({hostname: '127.0.0.1', port: Number(url.port), method: request.method,
      path: request.target, timeout: 4000, agent: false,
      headers: request.body === null ? {} : {'Content-Type': request.contentType, 'Content-Length': request.body.length}}, response => {
      let length = 0;
      const chunks = [];
      response.on('data', chunk => {
        length += chunk.length;
        if (length > LIMIT) call.destroy(Error('response observation bound exceeded'));
        else chunks.push(chunk);
      });
      response.on('error', reject);
      response.on('end', () => {
        try {
          const headers = [];
          for (let index = 0; index < response.rawHeaders.length; index += 2) headers.push(response.rawHeaders.slice(index, index + 2));
          resolve(responsePolicy({status: response.statusCode, headers, body: Buffer.concat(chunks).toString('base64')}));
        } catch (error) { reject(error); }
      });
    });
    call.on('timeout', () => call.destroy(Error('owned HTTP timeout')));
    call.on('error', reject);
    call.end(request.body);
  });
}

async function maintenanceWorkflows(pages, observe, receipt, output, control, allowedDownload) {
  const {expect} = require('playwright/test');
  const [page, peer] = pages;
  const life = target => target.getByRole('region', {name: 'Document lifecycle', exact: true});
  const recovery = target => target.getByRole('region', {name: 'Recovery', exact: true});
  const button = (target, name) => life(target).getByRole('button', {name, exact: true});
  const field = (target, name) => life(target).getByRole('textbox', {name, exact: true});
  const rbutton = (target, name) => recovery(target).getByRole('button', {name, exact: true});
  const error = (target, code) => life(target).getByRole('status').filter({hasText: new RegExp(`^${code}$`)});
  const action = async (target, locator, method, pathname, keyboard = false) => {
    const activate = async () => {
      if (keyboard) {
        await expect(locator).toBeEnabled();
        await locator.press('Enter');
        receipt.keyboardActions.push({method, pathname});
      } else await locator.click();
    };
    const [response] = await Promise.all([
      target.waitForResponse(response => response.request().method() === method && new URL(response.url()).pathname === pathname),
      activate(),
    ]);
    return {status: response.status(), value: await response.json(), response};
  };
  const apply = async (target, deleted) => {
    await life(target).getByRole('combobox', {name: /^Deletion filter(?:\s|$)/}).selectOption(deleted);
    const [response] = await Promise.all([
      target.waitForResponse(response => response.request().method() === 'GET' && new URL(response.url()).pathname === '/api/v1/documents'),
      field(target, 'Query').press('Enter'),
    ]);
    assert.equal(response.status(), 200);
    await expect(button(target, 'Next page')).toBeDisabled();
    return response.json();
  };
  const open = async (target, source) => {
    await Promise.all([
      target.waitForResponse(response => response.request().method() === 'GET' && new URL(response.url()).pathname.endsWith('/revisions')),
      button(target, `Open ${source}`).click(),
    ]);
    await expect(life(target).getByRole('heading', {name: source, exact: true})).toBeVisible();
    await expect(field(target, 'Notes')).toBeVisible();
  };
  const mutate = (target, name, id, operation) => action(target, button(target, name), 'POST', '/api/v1/documents/' + id + '/' + operation);
  const documentState = async () => (await observe('/api/v1/documents?deleted=all')).value;
  let exportVersion = 'v2';
  const exportRoute = () => exportVersion === 'v4' ? '/api/v1/export' : '/api/export-bundle';
  const expectedBundle = async (ids, includeDeleted, includeHistory) => {
    const state = await documentState();
    const documents = [];
    for (const record of state.records) {
      if ((ids !== null && !ids.includes(record.document_id)) || (record.deleted && !includeDeleted)) continue;
      const historyRoute = exportVersion === 'v4' ? '/api/v1/documents/' : '/api/lifecycle/documents/';
      const history = includeHistory ? (await observe(historyRoute + record.document_id + '/revisions')).value : [];
      const projected = exportVersion === 'v4' ? record : {
        document: {document_id: record.document_id, source_id: record.source_id, source: record.source,
          title: record.title, blob_id: record.current_revision.blob_id, text: record.current_revision.text},
        revision: record.current_revision.revision, edit_version: record.edit_version, deleted: record.deleted,
        notes: record.notes, tags: record.tags, collections: record.collections};
      documents.push({record: projected, revisions: includeHistory ? history.revisions : []});
    }
    return {format: 'local-research-library-export-' + exportVersion, generation: state.generation, documents};
  };
  const exportClick = async (name, ids, includeDeleted, includeHistory, maxBytes = 16777216) => {
    const expected = await expectedBundle(ids, includeDeleted, includeHistory);
    const bytes = Buffer.from(canonical(expected), 'utf8');
    assert(bytes.length <= maxBytes && bytes.length <= LIMIT);
    allowedDownload.active = true;
    let download;
    try {
      const result = await Promise.all([
        page.waitForEvent('download'),
        action(page, button(page, name), 'POST', exportRoute(), true),
      ]);
      download = result[0];
      const {response, status, value} = result[1];
      assert.equal(status, 200); assert.deepEqual(value, expected);
      assert.deepEqual(response.request().postDataJSON(), {ids, include_deleted: includeDeleted, include_history: includeHistory, max_bytes: maxBytes});
      const contentType = response.headers()['content-type'];
      assert(/^application\/json(?:;|$)/i.test(contentType));
      assert.deepEqual(await response.body(), bytes, 'HTTP export bytes must also be canonical');
      assert.equal(download.suggestedFilename(), 'research-library-' + exportVersion + '.json');
      assert(download.url().startsWith('blob:' + receipt.origin + '/'), 'only an owned application blob download');
      const stream = await download.createReadStream();
      assert(stream, 'actual browser download stream');
      const chunks = []; let size = 0;
      for await (const chunk of stream) {
        size += chunk.length;
        if (size > LIMIT) { stream.destroy(); await download.cancel(); throw Error('download observation bound exceeded'); }
        chunks.push(chunk);
      }
      const actual = Buffer.concat(chunks);
      assert.deepEqual(actual, bytes, 'actual downloaded raw bytes equal the declared canonical payload');
      assert.equal(await download.failure(), null);
      const basename = `download-${receipt.downloads.length + 1}.json`;
      fs.writeFileSync(path.join(output, basename), actual, {flag: 'wx'});
      receipt.downloads.push({file: basename, suggestedFilename: download.suggestedFilename(),
        contentType, format: exportVersion, ids, includeDeleted, includeHistory, maxBytes, bytes: actual.length,
        sha256: digest(actual), payload: expected, passed: true});
      await expect(button(page, name)).toBeEnabled();
      return actual.length;
    } finally {
      allowedDownload.active = false;
      if (download) await download.delete();
    }
  };
  const exportError = async (name, code) => {
    const before = receipt.downloadEvents;
    const result = await action(page, button(page, name), 'POST', exportRoute());
    assert.equal(result.status, code === 'not_found' ? 404 : 400);
    assert.deepEqual(result.value, {error: code});
    await expect(error(page, code)).toBeVisible();
    await expect(button(page, name)).toBeEnabled();
    assert.equal(receipt.downloadEvents, before, 'failed export has no download');
    receipt.exportErrors.push({code, request: result.response.request().postDataJSON(), downloadEventsBefore: before, downloadEventsAfter: receipt.downloadEvents});
  };
  await page.goto(receipt.origin + '/', {waitUntil: 'domcontentloaded'});
  await expect(recovery(page)).toBeVisible();
  await expect(rbutton(page, 'Refresh diagnostics')).toBeEnabled();
  assert.equal(receipt.requests.filter(row => row.method === 'POST').length, 0, 'page load performs no hidden writes');
  for (const source of ['alpha.txt', 'beta.txt']) {
    await page.getByLabel('Source path', {exact: true}).fill(source);
    assert.equal((await action(page, page.getByRole('button', {name: 'Import local file', exact: true}), 'POST', '/api/import')).status, 200);
    await expect(button(page, 'Open ' + source)).toBeVisible();
  }
  const initial = await documentState();
  const alphaID = initial.records.find(row => row.source === 'alpha.txt').document_id;
  const betaID = initial.records.find(row => row.source === 'beta.txt').document_id;
  await open(page, 'alpha.txt');
  await field(page, 'Refresh text').fill('<svg onload="window.__m3Injected=true"> café refreshed');
  assert.equal((await mutate(page, 'Refresh from text', alphaID, 'refresh')).status, 200);
  await expect(button(page, 'Save annotations')).toBeEnabled();
  await field(page, 'Notes').fill('snapshot annotation <script>window.__m3Injected=true</script>');
  assert.equal((await mutate(page, 'Save annotations', alphaID, 'annotations')).status, 200);
  await expect(button(page, 'Save annotations')).toBeEnabled();
  const includeDeleted = life(page).getByRole('checkbox', {name: 'Include deleted', exact: true});
  const includeHistory = life(page).getByRole('checkbox', {name: 'Include history', exact: true});
  const limit = life(page).getByRole('spinbutton', {name: 'Export limit bytes', exact: true});
  await expect(includeDeleted).not.toBeChecked(); await expect(includeHistory).not.toBeChecked();
  await expect(limit).toHaveValue('16777216');
  const formatControl = life(page).getByRole('combobox', {name: 'Export format', exact: true});
  await expect(formatControl).toHaveValue('v2');
  await exportClick('Export selection', [], false, false);
  await exportClick('Export all', null, false, false);
  receipt.completedChecks.push('default_v2_export_actual_empty_and_nonempty_canonical_bytes_and_legacy_filename');
  await formatControl.selectOption('v4');
  await expect(formatControl).toHaveValue('v4');
  exportVersion = 'v4';
  await exportClick('Export selection', [], false, false);
  await life(page).getByRole('checkbox', {name: 'Select alpha.txt', exact: true}).check();
  await exportClick('Export selection', [alphaID], false, false);
  await includeHistory.check();
  await exportClick('Export selection', [alphaID], false, true);
  await open(page, 'beta.txt');
  assert.equal((await mutate(page, 'Delete document', betaID, 'delete')).status, 200);
  await expect(button(page, 'Restore document')).toBeVisible();
  await apply(page, 'all');
  await life(page).getByRole('checkbox', {name: 'Select beta.txt', exact: true}).check();
  await exportError('Export selection', 'not_found');
  await includeDeleted.check();
  await exportClick('Export selection', [alphaID, betaID], true, true);
  await includeDeleted.uncheck(); await includeHistory.uncheck();
  const exactBytes = await exportClick('Export all', null, false, false);
  await limit.fill(String(exactBytes));
  await exportClick('Export all', null, false, false, exactBytes);
  await limit.fill(String(exactBytes - 1));
  await exportError('Export all', 'too_large');
  await limit.fill('16777216');
  await includeDeleted.check(); await includeHistory.check();
  await exportClick('Export all', null, true, true);
  receipt.completedChecks.push('actual_empty_selected_all_downloads_history_tombstones_exact_byte_cap_and_no_partial_result');

  await page.getByLabel('Job ID', {exact: true}).fill('m4_browser_job');
  await page.getByRole(INTAKE_CONTROL.role, {name: INTAKE_CONTROL.name}).selectOption('json');
  await page.getByLabel('Local path', {exact: true}).fill('bundle.json');
  assert.equal((await action(page, page.getByRole('button', {name: 'Submit job', exact: true}), 'POST', '/api/jobs')).status, 200);
  let result = await action(page, page.getByRole('button', {name: 'Enqueue m4_browser_job', exact: true}), 'POST', '/api/maintenance/jobs/m4_browser_job/enqueue', true);
  assert.equal(result.status, 200); assert.equal(result.value.status, 'enqueued');
  fs.writeFileSync(path.join(control, 'worker-request.partial'), JSON.stringify({job_id: 'm4_browser_job'}), {flag: 'wx'});
  fs.renameSync(path.join(control, 'worker-request.partial'), path.join(control, 'worker-request.json'));
  const until = Date.now() + 20000;
  while (!fs.existsSync(path.join(control, 'worker-result.json'))) {
    assert(Date.now() < until, 'owned CLI worker response timeout');
    await delay(30);
  }
  const workerBytes = fs.readFileSync(path.join(control, 'worker-result.json'));
  assert(workerBytes.length <= LIMIT);
  receipt.worker = JSON.parse(workerBytes);
  assert.equal(receipt.worker.processed, 'm4_browser_job');
  assert.equal(receipt.worker.job.state, 'completed');
  await page.reload({waitUntil: 'domcontentloaded'});
  await expect(page.getByRole('list', {name: 'Ingestion jobs', exact: true})).toContainText('m4_browser_job: completed');
  assert.equal(await page.getByRole('button', {name: 'Enqueue m4_browser_job', exact: true}).count(), 0);
  result = await action(page, rbutton(page, 'Reindex'), 'POST', '/api/maintenance/reindex');
  assert.equal(result.status, 200); assert.equal(result.value.state, 'completed');
  assert.equal(result.response.request().postDataJSON().limit, 64);
  receipt.reindexBeforeRestore = result.value;
  receipt.completedChecks.push('eligible_enqueue_control_real_cli_worker_completion_terminal_controls_and_reindex');

  const backupField = target => recovery(target).getByRole('textbox', {name: 'Backup name', exact: true});
  await backupField(page).fill('roundtrip.json');
  result = await action(page, rbutton(page, 'Create backup'), 'POST', '/api/maintenance/backups');
  assert.equal(result.status, 200);
  await expect(recovery(page).getByRole('list', {name: 'Backups', exact: true})).toContainText('roundtrip.json');
  receipt.backup = result.value;
  const snapshot = await documentState();
  result = await action(page, rbutton(page, 'Create backup'), 'POST', '/api/maintenance/backups');
  assert.equal(result.status, 409); assert.deepEqual(result.value, {error: 'already_exists'});
  let diagnostics = await action(page, rbutton(page, 'Refresh diagnostics'), 'GET', '/api/maintenance/diagnostics');
  assert.deepEqual(diagnostics.value.last_error, {operation: 'backup', code: 'already_exists'});
  await expect(recovery(page)).toContainText('already_exists');
  await expect(recovery(page)).toContainText('choose_new_backup');
  receipt.failedBackupDiagnostics = diagnostics.value;
  await backupField(page).fill('second.json');
  assert.equal((await action(page, rbutton(page, 'Create backup'), 'POST', '/api/maintenance/backups')).status, 200);
  diagnostics = await action(page, rbutton(page, 'Refresh diagnostics'), 'GET', '/api/maintenance/diagnostics');
  assert.equal(diagnostics.value.last_error, null);
  await expect(recovery(page)).not.toContainText('already_exists');
  receipt.clearedBackupDiagnostics = diagnostics.value;

  await peer.goto(receipt.origin + '/', {waitUntil: 'domcontentloaded'});
  await open(peer, 'alpha.txt');
  await field(peer, 'Notes').fill('old-client unsaved restore draft');
  await open(page, 'alpha.txt');
  await field(page, 'Notes').fill('after snapshot annotation');
  assert.equal((await mutate(page, 'Save annotations', alphaID, 'annotations')).status, 200);
  await expect(button(page, 'Save annotations')).toBeEnabled();
  await page.getByLabel('Source path', {exact: true}).fill('later.txt');
  assert.equal((await action(page, page.getByRole('button', {name: 'Import local file', exact: true}), 'POST', '/api/import')).status, 200);
  await expect(button(page, 'Open later.txt')).toBeVisible();
  await open(page, 'later.txt');
  await field(page, 'Notes').fill('removed document unsaved draft');
  await apply(peer, 'all');
  await open(peer, 'later.txt');
  await field(peer, 'Notes').fill('remote restore removed draft');
  const laterID = (await documentState()).records.find(row => row.source === 'later.txt').document_id;
  await backupField(peer).fill('roundtrip.json');
  receipt.preFailedRestoreState = await documentState();
  const writesBefore = receipt.requests.filter(row => row.method === 'POST' && row.path === '/api/maintenance/restore').length;
  result = await action(peer, rbutton(peer, 'Restore backup'), 'POST', '/api/maintenance/restore', true);
  assert.equal(result.status, 409); assert.deepEqual(result.value, {error: 'stale_generation'});
  await expect(recovery(peer)).toContainText('stale_generation');
  await expect(rbutton(peer, 'Restore backup')).toBeDisabled();
  assert.equal(receipt.requests.filter(row => row.method === 'POST' && row.path === '/api/maintenance/restore').length, writesBefore + 1);
  receipt.failedRestoreState = await documentState();
  assert.deepEqual(receipt.failedRestoreState, receipt.preFailedRestoreState, 'stale restore leaves the current lifecycle view unchanged');
  await action(page, rbutton(page, 'Refresh diagnostics'), 'GET', '/api/maintenance/diagnostics');
  await backupField(page).fill('roundtrip.json');
  result = await action(page, rbutton(page, 'Restore backup'), 'POST', '/api/maintenance/restore', true);
  assert.equal(result.status, 200); assert.equal(result.value.restored, true);
  assert.equal(result.value.documents, 3); assert.equal(result.value.jobs, 1);
  receipt.restore = result.value;
  await expect(recovery(page)).toContainText('restored');
  const restored = await documentState();
  assert.equal(restored.generation, receipt.failedRestoreState.generation + 1);
  assert.deepEqual(restored.records.map(row => row.document_id), snapshot.records.map(row => row.document_id));
  for (let index = 0; index < snapshot.records.length; index++) {
    const row = restored.records[index], old = snapshot.records[index];
    assert(row.edit_version > old.edit_version);
    assert.deepEqual({...row, edit_version: 0}, {...old, edit_version: 0});
  }
  assert.equal(restored.records.some(row => row.source === 'later.txt'), false);
  await expect(button(page, 'Open later.txt')).toBeHidden();
  await expect(field(page, 'Notes')).toHaveValue('removed document unsaved draft');
  await expect(button(page, 'Save annotations')).toBeDisabled();
  await expect(button(page, 'Reload document')).toBeEnabled();
  // Peer still displays the now-removed row. Opening it must not erase its
  // dirty draft merely because the other client restored an older snapshot.
  await expect(button(peer, 'Open later.txt')).toBeVisible();
  result = await action(peer, button(peer, 'Open later.txt'), 'GET', '/api/v1/documents/' + laterID);
  assert.equal(result.status, 404); assert.deepEqual(result.value, {error: 'not_found'});
  await expect(field(peer, 'Notes')).toHaveValue('remote restore removed draft');
  await expect(button(peer, 'Save annotations')).toBeDisabled();
  await expect(button(peer, 'Reload document')).toBeEnabled();
  result = await action(peer, button(peer, 'Reload document'), 'GET', '/api/v1/documents/' + laterID, true);
  assert.equal(result.status, 404); assert.deepEqual(result.value, {error: 'not_found'});
  await expect(field(peer, 'Notes')).toBeHidden();
  await open(peer, 'alpha.txt');
  await expect(field(peer, 'Notes')).toHaveValue('old-client unsaved restore draft');
  receipt.completedChecks.push('other_client_restore_removed_document_stale_row_open_retains_dirty_draft_until_explicit_reload');
  const annotationWrites = receipt.requests.filter(row => row.method === 'POST' && row.path.endsWith('/annotations')).length;
  result = await mutate(peer, 'Save annotations', alphaID, 'annotations');
  assert.equal(result.status, 409); assert.deepEqual(result.value, {error: 'stale_version'});
  await expect(error(peer, 'stale_version')).toBeVisible();
  await expect(field(peer, 'Notes')).toHaveValue('old-client unsaved restore draft');
  await expect(button(peer, 'Save annotations')).toBeEnabled();
  assert.equal(receipt.requests.filter(row => row.method === 'POST' && row.path.endsWith('/annotations')).length, annotationWrites + 1);
  await button(peer, 'Reload document').click();
  await expect(field(peer, 'Notes')).toHaveValue(snapshot.records.find(row => row.document_id === alphaID).notes);
  diagnostics = await action(page, rbutton(page, 'Refresh diagnostics'), 'GET', '/api/maintenance/diagnostics');
  assert.equal(diagnostics.value.last_error, null);
  await expect(recovery(page)).not.toContainText('stale_generation');
  result = await action(page, rbutton(page, 'Reindex'), 'POST', '/api/maintenance/reindex');
  assert.equal(result.status, 200); assert.equal(result.value.state, 'completed');
  receipt.reindexAfterRestore = result.value;
  await button(page, 'Reload document').click();
  await expect(field(page, 'Notes')).toBeHidden();
  receipt.completedChecks.push('backup_duplicate_error_success_clears_error_stale_restore_no_change_or_retry_snapshot_replacement_removed_draft_preservation_and_old_client_fencing');

  for (const target of pages) {
    assert.equal(await target.evaluate(() => window.__m3Injected === true || window.__m2Injected === true), false);
    assert.equal(await target.locator('img[src="x"],svg[onload]').count(), 0);
  }
  receipt.finalState = {alphaID, documents: await documentState(),
    collections: (await observe('/api/lifecycle/collections')).value,
    jobs: (await observe('/api/jobs')).value,
    history: (await observe('/api/v1/documents/' + alphaID + '/revisions')).value,
    backups: (await observe('/api/maintenance/backups')).value,
    diagnostics: (await observe('/api/maintenance/diagnostics')).value};
  receipt.deferred = ['abrupt worker/restore termination boundaries require dedicated process qualification',
    'migration is qualified separately against frozen independent snapshots',
    'candidate sandbox and independent post-freeze acceptance'];
}

async function lifecycleWorkflows(pages, observe, receipt) {
  const {expect} = require('playwright/test');
  const [page, peer] = pages;
  const region = target => target.getByRole('region', {name: 'Document lifecycle', exact: true});
  const button = (target, name) => region(target).getByRole('button', {name, exact: true});
  const field = (target, name) => region(target).getByRole('textbox', {name, exact: true});
  const rows = target => region(target).getByRole('list', {name: 'Lifecycle documents', exact: true});
  const pageStatus = target => region(target).getByRole('status').filter({hasText: /documents; page offset/});
  const errorStatus = (target, code) => region(target).getByRole('status').filter({hasText: new RegExp(`^${code}$`)});
  const action = async (target, locator, method, pathname) => {
    const [response] = await Promise.all([
      target.waitForResponse(response => response.request().method() === method && new URL(response.url()).pathname === pathname),
      locator.click(),
    ]);
    return {status: response.status(), value: await response.json()};
  };
  const apply = async (target, values = {}) => {
    for (const [name, value] of Object.entries(values)) {
      if (name === 'Deletion filter') await region(target).getByRole('combobox', {name: /^Deletion filter(?:\s|$)/}).selectOption(value);
      else if (name === 'Page size') await region(target).getByRole('spinbutton', {name, exact: true}).fill(String(value));
      else await field(target, name).fill(value);
    }
    // Keyboard activation verifies the normative filter controls, without
    // depending on the implementation's additional Apply filters label.
    const [response] = await Promise.all([
      target.waitForResponse(response => response.request().method() === 'GET' && new URL(response.url()).pathname === '/api/v1/documents'),
      field(target, 'Query').press('Enter'),
    ]);
    assert.equal(response.status(), 200);
    return response.json();
  };
  const open = async (target, source) => {
    await Promise.all([
      target.waitForResponse(response => response.request().method() === 'GET' && new URL(response.url()).pathname.endsWith('/revisions')),
      button(target, `Open ${source}`).click(),
    ]);
    await expect(region(target).getByRole('heading', {name: source, exact: true})).toBeVisible();
    await expect(field(target, 'Notes')).toBeVisible();
    await expect(region(target).getByRole('region', {name: 'Revision history', exact: true}).getByRole('listitem')).not.toHaveCount(0);
  };
  const current = async id => (await observe('/api/v1/documents/' + id)).value;
  const mutate = (target, name, id, operation) => action(target, button(target, name), 'POST', '/api/v1/documents/' + id + '/' + operation);
  await page.goto(receipt.origin + '/', {waitUntil: 'domcontentloaded'});
  await expect(page.getByRole('heading', {name: 'Local Research Library', exact: true})).toBeVisible();
  await expect(pageStatus(page)).toContainText('0 documents; page offset 0');
  for (const source of ['alpha.txt', 'beta.txt', 'gamma.txt']) {
    await page.getByLabel('Source path', {exact: true}).fill(source);
    assert.equal((await action(page, page.getByRole('button', {name: 'Import local file', exact: true}), 'POST', '/api/import')).status, 200);
    await expect(page.getByRole('list', {name: 'Documents', exact: true}).getByRole('button', {name: source, exact: true})).toBeVisible();
  }
  await page.getByLabel('Job ID', {exact: true}).fill('m2_browser_job');
  await page.getByRole(INTAKE_CONTROL.role, {name: INTAKE_CONTROL.name}).selectOption('json');
  await page.getByLabel('Local path', {exact: true}).fill('bundle.json');
  assert.equal((await action(page, page.getByRole('button', {name: 'Submit job', exact: true}), 'POST', '/api/jobs')).status, 200);
  for (const [operation, state, epoch] of [['prepare', 'running', 1], ['cancel', 'cancelled', 2],
    ['retry', 'queued', 3], ['prepare', 'running', 3], ['commit', 'completed', 3]]) {
    const result = await action(page, page.getByRole('button', {name: `${operation} m2_browser_job`, exact: true}), 'POST', '/api/jobs/m2_browser_job/' + operation);
    assert.equal(result.status, 200);
    await expect(page.getByRole('list', {name: 'Ingestion jobs', exact: true})).toContainText(new RegExp(`m2_browser_job: ${state}, epoch ${epoch}`));
  }
  await expect(pageStatus(page)).toContainText('4 documents; page offset 0');
  receipt.completedChecks.push('inherited_import_and_job_cancel_retry_commit_controls');

  const initial = await apply(page, {'Page size': 2});
  assert.equal(initial.total, 4);
  assert.deepEqual(initial.records.map(record => record.source), ['alpha.txt', 'beta.txt']);
  const alphaID = initial.records[0].document_id;
  await expect(button(page, 'Previous page')).toBeDisabled();
  await region(page).getByRole('checkbox', {name: 'Select beta.txt', exact: true}).check();
  await button(page, 'Next page').click();
  await expect(pageStatus(page)).toContainText('page offset 2');
  await expect(rows(page).getByRole('button')).toHaveText(['Open gamma.txt', 'Open jobs/one.txt']);
  await expect(button(page, 'Next page')).toBeDisabled();
  await button(page, 'Previous page').click();
  await expect(pageStatus(page)).toContainText('page offset 0');
  await expect(region(page).getByRole('checkbox', {name: 'Select beta.txt', exact: true})).toBeChecked();
  await open(page, 'alpha.txt');
  const history = region(page).getByRole('region', {name: 'Revision history', exact: true});
  await expect(history).toContainText('<img src=x onerror="window.__m2Injected=true"> alpha original');
  assert.equal(await page.locator('img[src="x"]').count(), 0);
  assert.equal(await page.evaluate(() => window.__m2Injected === true), false);
  receipt.completedChecks.push('accessible_keyboard_filters_pagination_and_id_selection');

  await field(page, 'Collection name').fill('  ReSeArCh  ');
  const created = await action(page, button(page, 'Create collection'), 'POST', '/api/lifecycle/collections');
  assert.equal(created.status, 200); assert.equal(created.value.name, 'research');
  await expect(region(page).getByRole('list', {name: 'Collections registry', exact: true})).toContainText('research: 0');
  const literalNotes = '<script>window.__m2Injected=true</script> exact annotation';
  await field(page, 'Notes').fill(literalNotes);
  await field(page, 'Tags').fill(' ExPeRiMent \nE\u0301');
  await field(page, 'Collections').fill(' RESEARCH ');
  let result = await mutate(page, 'Save annotations', alphaID, 'annotations');
  assert.equal(result.status, 200);
  assert.equal(result.value.record.edit_version, 2);
  assert.deepEqual(result.value.record.tags, ['experiment', 'é']);
  assert.deepEqual(result.value.record.collections, ['research']);
  await expect(field(page, 'Notes')).toHaveValue(literalNotes);
  await expect(button(page, 'Save annotations')).toBeEnabled();
  assert.equal((await apply(page, {'Tag filter': 'EXPERIMENT', 'Collection filter': ' research ', Query: 'alpha'})).total, 1);
  assert.equal((await apply(page, {Query: 'exact annotation'})).total, 0, 'notes do not leak into document-text search');
  await apply(page, {Query: '', 'Tag filter': '', 'Collection filter': ''});
  await field(page, 'Collection name').fill('research');
  result = await action(page, button(page, 'Remove collection'), 'POST', '/api/lifecycle/collections/remove');
  assert.equal(result.status, 409); assert.deepEqual(result.value, {error: 'collection_not_empty'});
  await expect(errorStatus(page, 'collection_not_empty')).toBeVisible();
  await field(page, 'Collection name').fill('empty');
  assert.equal((await action(page, button(page, 'Create collection'), 'POST', '/api/lifecycle/collections')).status, 200);
  await expect(button(page, 'Create collection')).toBeEnabled();
  assert.equal((await action(page, button(page, 'Remove collection'), 'POST', '/api/lifecycle/collections/remove')).status, 200);
  await expect(button(page, 'Remove collection')).toBeEnabled();
  receipt.completedChecks.push('normalized_annotations_collection_registry_and_nonempty_removal_guard');

  await peer.goto(receipt.origin + '/', {waitUntil: 'domcontentloaded'});
  await open(peer, 'alpha.txt');
  await field(peer, 'Notes').fill('unsaved losing draft');
  await field(page, 'Notes').fill('concurrent winning annotation');
  result = await mutate(page, 'Save annotations', alphaID, 'annotations');
  assert.equal(result.status, 200); assert.equal(result.value.record.edit_version, 3);
  await expect(button(page, 'Save annotations')).toBeEnabled();
  const writesBefore = receipt.requests.filter(request => request.method === 'POST' && request.path.endsWith('/annotations')).length;
  result = await mutate(peer, 'Save annotations', alphaID, 'annotations');
  assert.equal(result.status, 409); assert.deepEqual(result.value, {error: 'stale_version'});
  await expect(errorStatus(peer, 'stale_version')).toBeVisible();
  await expect(field(peer, 'Notes')).toHaveValue('unsaved losing draft');
  await expect(region(peer)).toContainText('draft is preserved');
  assert.equal((await current(alphaID)).notes, 'concurrent winning annotation');
  assert.equal(receipt.requests.filter(request => request.method === 'POST' && request.path.endsWith('/annotations')).length,
    writesBefore + 1, 'no automatic overwrite retry after stale_version');
  await button(peer, 'Reload document').click();
  await expect(field(peer, 'Notes')).toHaveValue('concurrent winning annotation');
  await field(peer, 'Notes').fill('explicitly resolved annotation');
  result = await mutate(peer, 'Save annotations', alphaID, 'annotations');
  assert.equal(result.status, 200); assert.equal(result.value.record.edit_version, 4);
  await expect(button(peer, 'Save annotations')).toBeEnabled();
  // Page one's captured generation became stale after the peer's write. It
  // must surface that conflict and restart at offset zero, never mix pages.
  const stalePage = await action(page, button(page, 'Next page'), 'GET', '/api/v1/documents');
  assert.equal(stalePage.status, 409); assert.deepEqual(stalePage.value, {error: 'stale_generation'});
  await expect(errorStatus(page, 'stale_generation')).toBeVisible();
  await expect(pageStatus(page)).toContainText('page offset 0');
  await button(page, 'Reload document').click();
  await expect(field(page, 'Notes')).toHaveValue('explicitly resolved annotation');
  receipt.completedChecks.push('two_context_stale_token_draft_preservation_explicit_reload_and_stale_pagination');

  const refreshText = '<img src=x onerror="window.__m2Injected=true"> alpha text refresh';
  await field(page, 'Notes').fill('draft preserved across content refresh');
  await field(page, 'Tags').fill(' Cross Action \nE\u0301');
  await field(page, 'Refresh path').fill('refresh.txt');
  await field(page, 'Refresh text').fill(refreshText);
  result = await mutate(page, 'Refresh from text', alphaID, 'refresh');
  assert.equal(result.status, 200); assert.equal(result.value.record.current_revision.revision, 2);
  assert.equal(result.value.record.edit_version, 5); assert.equal(result.value.record.source, 'alpha.txt');
  await expect(button(page, 'Refresh from text')).toBeEnabled();
  await expect(field(page, 'Notes')).toHaveValue('draft preserved across content refresh');
  await expect(field(page, 'Tags')).toHaveValue(' Cross Action \nE\u0301');
  await expect(field(page, 'Refresh path')).toHaveValue('refresh.txt');
  result = await mutate(page, 'Save annotations', alphaID, 'annotations');
  assert.equal(result.status, 200); assert.equal(result.value.record.edit_version, 6);
  await expect(button(page, 'Save annotations')).toBeEnabled();
  await expect(field(page, 'Refresh path')).toHaveValue('refresh.txt');
  assert.deepEqual(result.value.record.tags, ['cross action', 'é']);
  result = await mutate(page, 'Refresh from text', alphaID, 'refresh');
  assert.equal(result.status, 200); assert.equal(result.value.status, 'unchanged');
  assert.equal(result.value.record.edit_version, 6);
  await expect(button(page, 'Refresh from path')).toBeEnabled();
  await field(page, 'Refresh path').fill('refresh.txt');
  result = await mutate(page, 'Refresh from path', alphaID, 'refresh');
  assert.equal(result.status, 200); assert.equal(result.value.record.current_revision.revision, 3);
  assert.equal(result.value.record.edit_version, 7); assert.equal(result.value.record.source, 'alpha.txt');
  await expect(history.getByRole('listitem')).toHaveCount(3);
  await expect(history.getByRole('listitem').nth(0)).toContainText('Revision 1');
  await expect(history.getByRole('listitem').nth(1)).toContainText('Revision 2');
  await expect(history.getByRole('listitem').nth(2)).toContainText('Revision 3');
  await expect(history).toContainText(refreshText);
  await expect(history).toContainText('<svg onload="window.__m2Injected=true"> alpha path refresh');
  assert.equal(await page.locator('img[src="x"],svg[onload]').count(), 0);
  assert.equal(await page.evaluate(() => window.__m2Injected === true), false);
  receipt.completedChecks.push('text_path_refresh_noop_literal_history_and_cross_action_draft_preservation');

  await expect(button(page, 'Delete document')).toBeEnabled();
  result = await mutate(page, 'Delete document', alphaID, 'delete');
  assert.equal(result.status, 200); assert.equal(result.value.record.edit_version, 8);
  await expect(button(page, 'Restore document')).toBeVisible();
  await expect(field(page, 'Notes')).toBeDisabled();
  await expect(pageStatus(page)).toContainText('3 documents; page offset 0');
  assert.equal((await observe('/api/documents')).value.total, 3);
  const deleted = await apply(page, {'Deletion filter': 'deleted'});
  assert.equal(deleted.total, 1); assert.equal(deleted.records[0].document_id, alphaID);
  await expect(rows(page)).toContainText('Deleted');
  await expect(region(page).getByRole('list', {name: 'Collections registry', exact: true})).toContainText('research: 1');
  result = await mutate(page, 'Restore document', alphaID, 'restore');
  assert.equal(result.status, 200); assert.equal(result.value.record.edit_version, 9);
  await expect(button(page, 'Delete document')).toBeVisible();
  await apply(page, {'Deletion filter': 'all'});
  await expect(pageStatus(page)).toContainText('4 documents; page offset 0');
  // A locally saved draft cannot resurrect a remotely deleted record or
  // silently adopt its fresh CAS token when the row is reopened.
  await apply(page, {'Page size': 100});
  await open(page, 'gamma.txt');
  await field(page, 'Notes').fill('gamma unsaved draft');
  await open(page, 'alpha.txt');
  await apply(peer, {'Deletion filter': 'all'});
  await open(peer, 'gamma.txt');
  const gamma = (await observe('/api/v1/documents?deleted=all')).value.records.find(record => record.source === 'gamma.txt');
  result = await mutate(peer, 'Delete document', gamma.document_id, 'delete');
  assert.equal(result.status, 200);
  await expect(button(peer, 'Restore document')).toBeVisible();
  await open(page, 'gamma.txt');
  await expect(button(page, 'Restore document')).toBeVisible();
  await expect(button(page, 'Delete document')).toBeHidden();
  await expect(field(page, 'Notes')).toHaveValue('gamma unsaved draft');
  await expect(field(page, 'Notes')).toBeDisabled();
  result = await mutate(page, 'Restore document', gamma.document_id, 'restore');
  assert.equal(result.status, 409); assert.deepEqual(result.value, {error: 'stale_version'});
  await expect(errorStatus(page, 'stale_version')).toBeVisible();
  await button(page, 'Reload document').click();
  await expect(field(page, 'Notes')).toHaveValue('');
  result = await mutate(page, 'Restore document', gamma.document_id, 'restore');
  assert.equal(result.status, 200); assert.equal(result.value.record.edit_version, 3);
  await expect(button(page, 'Delete document')).toBeVisible();
  receipt.completedChecks.push('external_delete_reopened_draft_keeps_stale_token_and_latest_deleted_state');

  // Leave a real tombstone behind so both CLI and process-reopen observations
  // distinguish lifecycle all/active semantics and persisted deletion state.
  await open(page, 'beta.txt');
  const beta = (await observe('/api/v1/documents?deleted=all')).value.records.find(record => record.source === 'beta.txt');
  result = await mutate(page, 'Delete document', beta.document_id, 'delete');
  assert.equal(result.status, 200);
  await expect(button(page, 'Restore document')).toBeVisible();
  await page.reload({waitUntil: 'domcontentloaded'});
  await expect(pageStatus(page)).toContainText('3 documents; page offset 0');
  assert.equal(await button(page, 'Open beta.txt').count(), 0);
  await apply(page, {'Deletion filter': 'all'});
  await expect(button(page, 'Open beta.txt')).toBeVisible();
  receipt.completedChecks.push('delete_restore_tombstone_filters_and_reload_persistence');

  const finalDocuments = (await observe('/api/v1/documents?deleted=all')).value;
  const alpha = finalDocuments.records.find(record => record.document_id === alphaID);
  assert.equal(finalDocuments.total, 4);
  assert.equal(alpha.edit_version, 9); assert.equal(alpha.current_revision.revision, 3); assert.equal(alpha.deleted, false);
  assert.equal(alpha.notes, 'draft preserved across content refresh');
  assert.deepEqual(alpha.tags, ['cross action', 'é']);
  assert.deepEqual(alpha.collections, ['research']);
  assert.equal(finalDocuments.records.filter(record => record.deleted).length, 1);
  receipt.finalState = {alphaID, documents: finalDocuments,
    collections: (await observe('/api/lifecycle/collections')).value,
    jobs: (await observe('/api/jobs')).value,
    history: (await observe('/api/v1/documents/' + alphaID + '/revisions')).value};
  receipt.deferred = ['maintenance workflows are qualified in the separate fresh release fixture',
    'candidate sandbox and independent post-freeze acceptance'];
}


async function datasetWorkflows(pages, observe, receipt, output, control, allowedDownload) {
  const {expect} = require('playwright/test');
  const [page, peer] = pages;
  const life = target => target.getByRole('region', {name: 'Document lifecycle', exact: true});
  const recovery = page.getByRole('region', {name: 'Recovery', exact: true});
  const button = name => life(page).getByRole('button', {name, exact: true});
  const field = name => life(page).getByRole('textbox', {name, exact: true});
  const action = async (locator, method, pathname) => {
    await expect(locator).toBeEnabled();
    const [response] = await Promise.all([
      page.waitForResponse(response => response.request().method() === method && new URL(response.url()).pathname === pathname),
      locator.press('Enter'),
    ]);
    receipt.keyboardActions.push({method, pathname});
    return {status: response.status(), value: await response.json(), response};
  };
  const state = async () => (await observe('/api/v1/documents?deleted=all')).value;
  const open = async source => {
    await expect(button('Open ' + source)).toBeEnabled();
    await Promise.all([
      page.waitForResponse(response => response.request().method() === 'GET' && new URL(response.url()).pathname.endsWith('/revisions')),
      button('Open ' + source).press('Enter'),
    ]);
    await expect(field('Notes')).toBeVisible();
  };
  await page.goto(receipt.origin + '/', {waitUntil: 'domcontentloaded'});
  await expect(page.getByRole('heading', {name: 'Local Research Library', exact: true})).toBeVisible();
  assert.equal(receipt.requests.filter(row => row.method === 'POST').length, 0);
  for (const source of ['welcome.txt', 'notes.md']) {
    await page.getByLabel('Source path', {exact: true}).fill(source);
    const imported = await action(page.getByRole('button', {name: 'Import local file', exact: true}), 'POST', '/api/import');
    assert.equal(imported.status, 200);
    await expect(button('Open ' + source)).toBeVisible();
  }
  await page.getByLabel('Job ID', {exact: true}).fill('release-literal');
  await page.getByRole(INTAKE_CONTROL.role, {name: INTAKE_CONTROL.name}).selectOption('json');
  await page.getByLabel('Local path', {exact: true}).fill('dataset.json');
  assert.equal((await action(page.getByRole('button', {name: 'Submit job', exact: true}), 'POST', '/api/jobs')).status, 200);
  for (const operation of ['prepare', 'commit']) {
    const result = await action(page.getByRole('button', {name: `${operation} release-literal`, exact: true}),
      'POST', '/api/jobs/release-literal/' + operation);
    assert.equal(result.status, 200);
    await expect(page.getByRole('list', {name: 'Ingestion jobs', exact: true})).toContainText(
      'release-literal: ' + (operation === 'prepare' ? 'running' : 'completed'));
  }
  const original = await state();
  assert.equal(original.total, 3);
  const welcome = original.records.find(row => row.source === 'welcome.txt');
  const notes = original.records.find(row => row.source === 'notes.md');
  assert.equal(welcome.current_revision.blob_id, notes.current_revision.blob_id);
  assert.equal(welcome.current_revision.text, 'Café research — 雪\nShared bytes across two source keys.\n');
  const welcomeID = welcome.document_id;
  await open('welcome.txt');
  await field('Notes').fill('Reviewed café / 雪; literal <b>note</b>.');
  await field('Tags').fill('demo');
  let result = await action(button('Save annotations'), 'POST', '/api/v1/documents/' + welcomeID + '/annotations');
  assert.equal(result.status, 200); assert.equal(result.value.record.edit_version, 2);
  await expect(button('Refresh from text')).toBeEnabled();
  await field('Refresh text').fill('Café research — 雪\nSecond revision; preserved annotations.\n');
  result = await action(button('Refresh from text'), 'POST', '/api/v1/documents/' + welcomeID + '/refresh');
  assert.equal(result.status, 200); assert.equal(result.value.record.edit_version, 3);
  assert.equal(result.value.record.current_revision.revision, 2);
  assert.equal(result.value.record.notes, 'Reviewed café / 雪; literal <b>note</b>.');
  assert.deepEqual(result.value.record.tags, ['demo']);
  receipt.documentedWorkflow = {format: 'local-research-library-public-workflow-v1', operations: 7,
    inputSource: 'bytes copied from actual released release/dataset files',
    intermediateState: await state()};
  await expect(life(page).getByRole('region', {name: 'Revision history', exact: true})).toContainText('Shared bytes across two source keys.');
  await open('literal.html');
  const literal = original.records.find(row => row.source === 'literal.html');
  await expect(life(page).getByRole('region', {name: 'Revision history', exact: true})).toContainText(literal.current_revision.text.trim());
  assert.equal(await life(page).locator('img,script,svg').count(), 0, 'released HTML remains literal text');
  receipt.completedChecks.push('actual_released_dataset_seven_documented_operations_shared_bytes_unicode_literal_html_history_and_annotations');

  await recovery.getByRole('textbox', {name: 'Backup name', exact: true}).fill('documented.json');
  result = await action(recovery.getByRole('button', {name: 'Create backup', exact: true}), 'POST', '/api/maintenance/backups');
  assert.equal(result.status, 200);
  const snapshot = await state();
  await open('welcome.txt');
  await field('Notes').fill('post-backup change to remove');
  result = await action(button('Save annotations'), 'POST', '/api/v1/documents/' + welcomeID + '/annotations');
  assert.equal(result.status, 200);
  await expect(button('Save annotations')).toBeEnabled();
  await action(recovery.getByRole('button', {name: 'Refresh diagnostics', exact: true}), 'GET', '/api/maintenance/diagnostics');
  result = await action(recovery.getByRole('button', {name: 'Restore backup', exact: true}), 'POST', '/api/maintenance/restore');
  assert.equal(result.status, 200); assert.equal(result.value.restored, true);
  const restored = await state();
  assert.equal(restored.total, 3);
  for (let index = 0; index < snapshot.records.length; index++) {
    const row = restored.records[index], old = snapshot.records[index];
    assert(row.edit_version > old.edit_version);
    assert.deepEqual({...row, edit_version: 0}, {...old, edit_version: 0});
  }
  await field('Query').fill('Second revision');
  result = await action(field('Query'), 'GET', '/api/v1/documents');
  assert.equal(result.status, 200); assert.equal(result.value.total, 1);
  assert.equal(result.value.records[0].document_id, welcomeID);
  await field('Query').fill('');
  await action(field('Query'), 'GET', '/api/v1/documents');
  await life(page).getByRole('checkbox', {name: 'Select welcome.txt', exact: true}).check();
  await life(page).getByRole('checkbox', {name: 'Include history', exact: true}).check();
  await life(page).getByRole('combobox', {name: 'Export format', exact: true}).selectOption('v4');
  const current = (await state()).records.find(row => row.document_id === welcomeID);
  const history = (await observe('/api/v1/documents/' + welcomeID + '/revisions')).value;
  const expected = {format: 'local-research-library-export-v4', generation: restored.generation,
    documents: [{record: current, revisions: history.revisions}]};
  const expectedBytes = Buffer.from(canonical(expected), 'utf8');
  let download;
  allowedDownload.active = true;
  try {
    const responses = await Promise.all([page.waitForEvent('download'),
      action(button('Export selection'), 'POST', '/api/v1/export')]);
    download = responses[0]; const exported = responses[1];
    assert.equal(exported.status, 200); assert.deepEqual(exported.value, expected);
    assert.deepEqual(exported.response.request().postDataJSON(), {ids: [welcomeID], include_deleted: false,
      include_history: true, max_bytes: 16777216});
    assert.deepEqual(await exported.response.body(), expectedBytes);
    assert.equal(download.suggestedFilename(), 'research-library-v4.json');
    const stream = await download.createReadStream(); assert(stream);
    const chunks = []; let size = 0;
    for await (const chunk of stream) {
      size += chunk.length;
      if (size > LIMIT) { stream.destroy(); await download.cancel(); throw Error('download bound exceeded'); }
      chunks.push(chunk);
    }
    const bytes = Buffer.concat(chunks); assert.deepEqual(bytes, expectedBytes);
    assert.equal(await download.failure(), null);
    fs.writeFileSync(path.join(output, 'dataset-download.json'), bytes, {flag: 'wx'});
    receipt.downloads.push({file: 'dataset-download.json', bytes: bytes.length, sha256: digest(bytes), payload: expected, passed: true});
  } finally { allowedDownload.active = false; if (download) await download.delete(); }
  await peer.goto(receipt.origin + '/', {waitUntil: 'domcontentloaded'});
  receipt.finalState = {alphaID: welcomeID, documents: await state(),
    collections: (await observe('/api/lifecycle/collections')).value, jobs: (await observe('/api/jobs')).value,
    history: (await observe('/api/v1/documents/' + welcomeID + '/revisions')).value,
    backups: (await observe('/api/maintenance/backups')).value,
    diagnostics: (await observe('/api/maintenance/diagnostics')).value};
  receipt.completedChecks.push('documented_backup_explicit_restore_search_selected_canonical_v4_download_on_released_dataset');
  receipt.deferred = ['candidate sandbox and independent post-freeze acceptance'];
}

async function versionedEvidence(pages, observe, receipt) {
  const {expect} = require('playwright/test');
  const revisionID = (id, revision) => 'rev-' + digest(Buffer.concat([
    Buffer.from('revision\0' + id + '\0' + revision.revision + '\0' + revision.blob_id, 'ascii')]));
  const revision = (id, value) => {
    assert.deepEqual(Object.keys(value).sort(), ['blob_id', 'revision', 'revision_id', 'text']);
    assert.equal(value.revision_id, revisionID(id, value));
  };
  const legacyProjection = row => ({document: {document_id: row.document_id, source_id: row.source_id,
    source: row.source, title: row.title, blob_id: row.current_revision.blob_id, text: row.current_revision.text},
    revision: row.current_revision.revision, edit_version: row.edit_version, deleted: row.deleted,
    notes: row.notes, tags: row.tags, collections: row.collections});
  const state = receipt.finalState.documents;
  for (const row of state.records) {
    assert.deepEqual(Object.keys(row).sort(), ['collections', 'current_revision', 'deleted', 'document_id',
      'edit_version', 'notes', 'source', 'source_id', 'tags', 'title']);
    revision(row.document_id, row.current_revision);
    const history = (await observe('/api/v1/documents/' + row.document_id + '/revisions')).value;
    for (const item of history.revisions) revision(row.document_id, item);
  }
  const selected = state.records.find(row => row.document_id === receipt.finalState.alphaID);
  assert(selected);
  for (const target of pages) {
    const region = target.getByRole('region', {name: 'Document lifecycle', exact: true});
    await expect(region).toContainText(selected.current_revision.revision_id);
    await expect(region.getByRole('button', {name: 'Open ' + selected.source, exact: true})).toBeEnabled();
    await Promise.all([
      target.waitForResponse(response => response.request().method() === 'GET' &&
        new URL(response.url()).pathname === '/api/v1/documents/' + selected.document_id + '/revisions'),
      region.getByRole('button', {name: 'Open ' + selected.source, exact: true}).press('Enter'),
    ]);
    const historyRegion = region.getByRole('region', {name: 'Revision history', exact: true});
    for (const item of receipt.finalState.history.revisions) {
      await expect(historyRegion).toContainText('Revision ' + item.revision + ' · ' + item.revision_id);
    }
  }
  const health = (await observe('/health')).value;
  assert.deepEqual(health, {status: 'ok', schema: 4});
  const legacy = (await observe('/api/lifecycle/documents?deleted=all')).value;
  assert.deepEqual(legacy, {...state, records: state.records.map(legacyProjection)});
  const legacyHistory = (await observe('/api/lifecycle/documents/' + receipt.finalState.alphaID + '/revisions')).value;
  assert.deepEqual(legacyHistory, {document_id: receipt.finalState.alphaID,
    revisions: receipt.finalState.history.revisions.map(({revision_id, ...rest}) => rest)});
  const actualBrowser = receipt.requests.filter(row => row.actor === 'browser');
  assert(actualBrowser.some(row => row.path.startsWith('/api/v1/documents') && row.method === 'GET'));
  assert(actualBrowser.some(row => row.path.startsWith('/api/v1/documents/') && row.method === 'POST'));
  assert(!actualBrowser.some(row => row.path.startsWith('/api/lifecycle/documents')),
    'the maintained browser must really request v1 lifecycle data');
  assert(actualBrowser.some(row => row.path === '/api/import'));
  assert(actualBrowser.some(row => row.path === '/api/jobs'));
  receipt.versionedEvidence = {health, legacyProjection: legacy, legacyHistory,
    revisionFormula: 'sha256(revision\\0 + document_id + \\0 + revision + \\0 + blob_id)',
    actualBrowserV1Requests: actualBrowser.filter(row => row.path.startsWith('/api/v1/')).length};
  receipt.finalState.legacyDocuments = legacy;
  receipt.finalState.legacyHistory = legacyHistory;
  receipt.completedChecks.push('actual_browser_v1_routes_flattened_record4_deterministic_revision_ids_visible_ids_and_exact_legacy_adapters');
}

async function main(argv) {
  assert.deepEqual([argv[0], argv[2], argv[4], argv[6]], ['--origin', '--output', '--worker-control', '--workflow']);
  assert.equal(argv.length, 8);
  const workflow = argv[7]; assert(['lifecycle', 'maintenance', 'dataset'].includes(workflow));
  const origin = checkedOrigin(argv[1]), output = path.resolve(argv[3]), control = path.resolve(argv[5]);
  assert.equal(output, argv[3]); assert.equal(control, argv[5]);
  assert(fs.statSync(control).isDirectory()); fs.mkdirSync(output);
  const receipt = {protocol: 'library-m4-authored-release-browser-reference-v1', workflow, adaptedFrom: ['library_m2_reference.cjs', 'library_m3_reference.cjs'], purpose: 'authored_reference_browser_qualification',
    wholeProjectAcceptance: false, origin, passed: false, completedChecks: [], browserLaunches: 0,
    freshContexts: 0, keyboardActions: [], blockedRequests: [], pageErrors: [], downloadEvents: 0, downloads: [], exportErrors: [], requests: [], runtime: null};
  const handle = fs.openSync(path.join(output, 'receipt.json'), 'wx');
  let persisted = false;
  const persist = () => {
    if (persisted) return;
    fs.writeFileSync(handle, JSON.stringify(receipt, null, 2) + '\n'); fs.fsyncSync(handle); fs.closeSync(handle); persisted = true;
  };
  const resources = {}, lifecycle = createLifecycle(resources, receipt, persist);
  const contexts = [], pages = [], pending = new Set(), allowedDownload = {active: false};
  let requestCount = 0, closing = false;
  const send = async (request, actor = 'browser') => {
    lifecycle.check(); assert(!closing); assert(++requestCount <= 512, 'request count bound');
    const promise = loopbackHTTP(origin, request); pending.add(promise);
    try {
      const response = await promise;
      const rawFile = `http-${String(receipt.requests.length + 1).padStart(4, '0')}-response.bin`;
      fs.writeFileSync(path.join(output, rawFile), response.body, {flag: 'wx'});
      receipt.requests.push({actor, method: request.method, path: request.target, status: response.status,
        requestSha256: digest(request.body || ''), requestBodyBase64: request.body === null ? null : request.body.toString('base64'),
        responseSha256: digest(response.body), rawResponseFile: rawFile, bytes: response.body.length});
      return response;
    } finally { pending.delete(promise); }
  };
  const observe = async target => {
    const response = await send(checkedRequest(origin, origin + target, 'GET', null), 'observer');
    assert.equal(response.status, 200);
    return {status: response.status, value: JSON.parse(response.body.toString('utf8'))};
  };
  try {
    receipt.runtime = {...runtime(), driverSha256: digest(fs.readFileSync(__filename))};
    const home = path.join(output, 'browser-home'); fs.mkdirSync(home);
    resources.browser = await require('playwright').chromium.launch({headless: true, chromiumSandbox: true,
      executablePath: receipt.runtime.executable, timeout: 30000, handleSIGINT: false, handleSIGTERM: false, handleSIGHUP: false,
      env: {PATH: process.env.PATH || '', HOME: home, TMPDIR: home},
      proxy: {server: 'http://127.0.0.1:9', bypass: '<-loopback>'},
      args: ['--host-resolver-rules=MAP * ~NOTFOUND', '--force-webrtc-ip-handling-policy=disable_non_proxied_udp']});
    receipt.browserLaunches++; assert.equal(resources.browser.version(), receipt.runtime.browserVersion);
    for (let index = 0; index < 2; index++) {
      const context = await resources.browser.newContext({viewport: index ? {width: 430, height: 932} : {width: 1280, height: 980},
        serviceWorkers: 'block', permissions: [], acceptDownloads: true});
      contexts.push(context); receipt.freshContexts++;
      context.setDefaultTimeout(8000); context.setDefaultNavigationTimeout(15000);
      await context.routeWebSocket('**/*', socket => { receipt.blockedRequests.push('websocket'); socket.close(); });
      await context.route('**/*', async route => {
        try {
          const actual = route.request();
          await route.fulfill(await send(checkedRequest(origin, actual.url(), actual.method(), actual.postDataBuffer(), actual.headers()['content-type'] || '')));
        } catch (error) { receipt.blockedRequests.push(String(error).slice(0, 300)); await route.abort('blockedbyclient').catch(() => {}); }
      });
      context.on('page', page => {
        if (context.pages().length > 1) { receipt.blockedRequests.push('unexpected popup'); void page.close(); }
        page.on('pageerror', error => receipt.pageErrors.push(String(error).slice(0, 500)));
        page.on('download', download => {
          receipt.downloadEvents++;
          if (!allowedDownload.active || !download.url().startsWith('blob:' + origin + '/')) {
            receipt.blockedRequests.push('unexpected download'); void download.cancel();
          }
        });
        page.on('dialog', dialog => { receipt.blockedRequests.push('unexpected dialog'); void dialog.dismiss(); });
      });
      await context.tracing.start({screenshots: true, snapshots: true, sources: false});
      pages.push(await context.newPage());
    }
    if (workflow === 'lifecycle') await lifecycleWorkflows(pages, observe, receipt);
    else if (workflow === 'maintenance') await maintenanceWorkflows(pages, observe, receipt, output, control, allowedDownload);
    else await datasetWorkflows(pages, observe, receipt, output, control, allowedDownload);
    await versionedEvidence(pages, observe, receipt);
    receipt.viewportChecks = [];
    for (let index = 0; index < pages.length; index++) {
      const viewport = pages[index].viewportSize(); assert(viewport);
      const layout = await pages[index].evaluate(() => ({innerWidth: window.innerWidth,
        documentScrollWidth: document.documentElement.scrollWidth, bodyScrollWidth: document.body.scrollWidth}));
      const check = {contextIndex: index, viewportWidth: viewport.width, viewportHeight: viewport.height,
        ...layout, passed: layout.documentScrollWidth <= viewport.width + 1};
      receipt.viewportChecks.push(check); assert(check.passed, `context ${index}: viewport overflow`);
    }
    receipt.completedChecks.push('desktop_mobile_literal_content_and_viewport_fit_and_keyboard_m3_actions'); receipt.passed = true;
  } catch (error) { receipt.error = String(error).slice(0, 3000); process.exitCode = 1;
  } finally {
    for (let index = 0; index < contexts.length; index++) {
      try {
        if (pages[index]) await pages[index].screenshot({path: path.join(output, `page-${index}.png`), timeout: 3000, fullPage: true});
        await contexts[index].tracing.stop({path: path.join(output, `trace-${index}.zip`)});
      } catch (error) { receipt.passed = false; receipt.artifactError = String(error).slice(0, 500); }
    }
    closing = true; await lifecycle.cleanup(); await Promise.allSettled([...pending]); lifecycle.dispose();
    if (receipt.blockedRequests.length || receipt.pageErrors.length || receipt.downloadEvents !== receipt.downloads.length) receipt.passed = false;
    receipt.artifacts = Object.fromEntries(fs.readdirSync(output).filter(name => /\.(png|zip|json|bin)$/.test(name) && name !== 'receipt.json')
      .map(name => [name, digest(fs.readFileSync(path.join(output, name)))]));
    persist(); if (!receipt.passed) process.exitCode = 1;
    if (lifecycle.forceExitNeeded) process.exit(process.exitCode || 1);
  }
}

if (require.main === module) main(process.argv.slice(2)).catch(error => { console.error(String(error)); process.exitCode = 1; });
