#!/usr/bin/env node
// Fixed authored-reference UI qualification; not arbitrary candidate execution
// or independent study acceptance. Downloads are observed as bounded raw bytes.
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

async function workflows(pages, observe, receipt, output, control, allowedDownload) {
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
      target.waitForResponse(response => response.request().method() === 'GET' && new URL(response.url()).pathname === '/api/lifecycle/documents'),
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
  const mutate = (target, name, id, operation) => action(target, button(target, name), 'POST', '/api/lifecycle/documents/' + id + '/' + operation);
  const documentState = async () => (await observe('/api/lifecycle/documents?deleted=all')).value;
  const expectedBundle = async (ids, includeDeleted, includeHistory) => {
    const state = await documentState();
    const documents = [];
    for (const record of state.records) {
      if ((ids !== null && !ids.includes(record.document.document_id)) || (record.deleted && !includeDeleted)) continue;
      const history = includeHistory ? (await observe('/api/lifecycle/documents/' + record.document.document_id + '/revisions')).value : [];
      // The public revision-history result is a closed {document_id,revisions} envelope.
      documents.push({record, revisions: includeHistory ? history.revisions : []});
    }
    return {format: 'local-research-library-export-v2', generation: state.generation, documents};
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
        action(page, button(page, name), 'POST', '/api/export-bundle', true),
      ]);
      download = result[0];
      const {response, status, value} = result[1];
      assert.equal(status, 200); assert.deepEqual(value, expected);
      assert.deepEqual(response.request().postDataJSON(), {ids, include_deleted: includeDeleted, include_history: includeHistory, max_bytes: maxBytes});
      const contentType = response.headers()['content-type'];
      assert(/^application\/json(?:;|$)/i.test(contentType));
      assert.deepEqual(await response.body(), bytes, 'HTTP export bytes must also be canonical');
      assert.equal(download.suggestedFilename(), 'research-library-v2.json');
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
        contentType, ids, includeDeleted, includeHistory, maxBytes, bytes: actual.length,
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
    const result = await action(page, button(page, name), 'POST', '/api/export-bundle');
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
  const alphaID = initial.records.find(row => row.document.source === 'alpha.txt').document.document_id;
  const betaID = initial.records.find(row => row.document.source === 'beta.txt').document.document_id;
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

  await page.getByLabel('Job ID', {exact: true}).fill('m3_browser_job');
  await page.getByRole(INTAKE_CONTROL.role, {name: INTAKE_CONTROL.name}).selectOption('json');
  await page.getByLabel('Local path', {exact: true}).fill('bundle.json');
  assert.equal((await action(page, page.getByRole('button', {name: 'Submit job', exact: true}), 'POST', '/api/jobs')).status, 200);
  let result = await action(page, page.getByRole('button', {name: 'Enqueue m3_browser_job', exact: true}), 'POST', '/api/maintenance/jobs/m3_browser_job/enqueue', true);
  assert.equal(result.status, 200); assert.equal(result.value.status, 'enqueued');
  fs.writeFileSync(path.join(control, 'worker-request.partial'), JSON.stringify({job_id: 'm3_browser_job'}), {flag: 'wx'});
  fs.renameSync(path.join(control, 'worker-request.partial'), path.join(control, 'worker-request.json'));
  const until = Date.now() + 20000;
  while (!fs.existsSync(path.join(control, 'worker-result.json'))) {
    assert(Date.now() < until, 'owned CLI worker response timeout');
    await delay(30);
  }
  const workerBytes = fs.readFileSync(path.join(control, 'worker-result.json'));
  assert(workerBytes.length <= LIMIT);
  receipt.worker = JSON.parse(workerBytes);
  assert.equal(receipt.worker.processed, 'm3_browser_job');
  assert.equal(receipt.worker.job.state, 'completed');
  await page.reload({waitUntil: 'domcontentloaded'});
  await expect(page.getByRole('list', {name: 'Ingestion jobs', exact: true})).toContainText('m3_browser_job: completed');
  assert.equal(await page.getByRole('button', {name: 'Enqueue m3_browser_job', exact: true}).count(), 0);
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
  assert.deepEqual(restored.records.map(row => row.document.document_id), snapshot.records.map(row => row.document.document_id));
  for (let index = 0; index < snapshot.records.length; index++) {
    const row = restored.records[index], old = snapshot.records[index];
    assert(row.edit_version > old.edit_version);
    assert.deepEqual({...row, edit_version: 0}, {...old, edit_version: 0});
  }
  assert.equal(restored.records.some(row => row.document.source === 'later.txt'), false);
  await expect(button(page, 'Open later.txt')).toBeHidden();
  await expect(field(page, 'Notes')).toHaveValue('removed document unsaved draft');
  await expect(button(page, 'Save annotations')).toBeDisabled();
  await expect(button(page, 'Reload document')).toBeEnabled();
  const annotationWrites = receipt.requests.filter(row => row.method === 'POST' && row.path.endsWith('/annotations')).length;
  result = await mutate(peer, 'Save annotations', alphaID, 'annotations');
  assert.equal(result.status, 409); assert.deepEqual(result.value, {error: 'stale_version'});
  await expect(error(peer, 'stale_version')).toBeVisible();
  await expect(field(peer, 'Notes')).toHaveValue('old-client unsaved restore draft');
  await expect(button(peer, 'Save annotations')).toBeEnabled();
  assert.equal(receipt.requests.filter(row => row.method === 'POST' && row.path.endsWith('/annotations')).length, annotationWrites + 1);
  await button(peer, 'Reload document').click();
  await expect(field(peer, 'Notes')).toHaveValue(snapshot.records.find(row => row.document.document_id === alphaID).notes);
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
    history: (await observe('/api/lifecycle/documents/' + alphaID + '/revisions')).value,
    backups: (await observe('/api/maintenance/backups')).value,
    diagnostics: (await observe('/api/maintenance/diagnostics')).value};
  receipt.deferred = ['abrupt worker/restore termination boundaries require dedicated process qualification',
    'M4 migration and API v1', 'candidate sandbox and independent post-freeze acceptance'];
}

async function main(argv) {
  assert.deepEqual([argv[0], argv[2], argv[4]], ['--origin', '--output', '--worker-control']);
  assert.equal(argv.length, 6);
  const origin = checkedOrigin(argv[1]), output = path.resolve(argv[3]), control = path.resolve(argv[5]);
  assert.equal(output, argv[3]); assert.equal(control, argv[5]);
  assert(fs.statSync(control).isDirectory()); fs.mkdirSync(output);
  const receipt = {protocol: 'library-m3-authored-browser-reference-v1', purpose: 'authored_reference_browser_qualification',
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
  const send = async request => {
    lifecycle.check(); assert(!closing); assert(++requestCount <= 512, 'request count bound');
    const promise = loopbackHTTP(origin, request); pending.add(promise);
    try {
      const response = await promise;
      receipt.requests.push({method: request.method, path: request.target, status: response.status,
        requestSha256: digest(request.body || ''), responseSha256: digest(response.body), bytes: response.body.length});
      return response;
    } finally { pending.delete(promise); }
  };
  const observe = async target => {
    const response = await send(checkedRequest(origin, origin + target, 'GET', null));
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
    await workflows(pages, observe, receipt, output, control, allowedDownload);
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
    receipt.artifacts = Object.fromEntries(fs.readdirSync(output).filter(name => /\.(png|zip|json)$/.test(name) && name !== 'receipt.json')
      .map(name => [name, digest(fs.readFileSync(path.join(output, name)))]));
    persist(); if (!receipt.passed) process.exitCode = 1;
    if (lifecycle.forceExitNeeded) process.exit(process.exitCode || 1);
  }
}

if (require.main === module) main(process.argv.slice(2)).catch(error => { console.error(String(error)); process.exitCode = 1; });
