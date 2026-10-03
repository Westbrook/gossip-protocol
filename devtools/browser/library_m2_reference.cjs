#!/usr/bin/env node
// Physical browser qualification of the authored M2 reference only. This driver
// has no candidate-source input and is not a post-freeze study acceptance gate.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const {createLifecycle} = require('./lifecycle.cjs');
const {runtime, responsePolicy, INTAKE_CONTROL} = require('./library_m1_acceptance.cjs');
const digest = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
const LIMIT = 262144;

function checkedOrigin(value) {
  const url = new URL(value);
  assert.equal(url.protocol, 'http:');
  assert.equal(url.hostname, '127.0.0.1');
  assert.equal(url.username + url.password + url.search + url.hash, '');
  assert.equal(url.pathname, '/');
  assert(/^[0-9]+$/.test(url.port) && Number(url.port) > 0);
  return url.origin;
}

function checkedRequest(origin, urlValue, method, body, contentType = '') {
  const url = new URL(urlValue);
  assert.equal(url.origin, origin, 'only the owned loopback application');
  assert.equal(url.username + url.password, '');
  assert(['GET', 'POST'].includes(method));
  assert(url.pathname.startsWith('/') && (url.pathname + url.search).length <= 4096);
  assert(body === null || Buffer.isBuffer(body));
  assert(body === null || body.length <= 65536);
  assert(method !== 'GET' || body === null);
  if (method === 'POST') {
    assert(url.pathname.startsWith('/api/'));
    assert(/^application\/json(?:\s*;[^\r\n]*)?$/i.test(contentType));
  }
  return {method, target: url.pathname + url.search, body, contentType};
}

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
        if (length > LIMIT) call.destroy(Error('response bound exceeded'));
        else chunks.push(chunk);
      });
      response.on('error', reject);
      response.on('end', () => {
        try {
          const headers = [];
          for (let index = 0; index < response.rawHeaders.length; index += 2)
            headers.push(response.rawHeaders.slice(index, index + 2));
          resolve(responsePolicy({status: response.statusCode, headers,
            body: Buffer.concat(chunks).toString('base64')}));
        } catch (error) { reject(error); }
      });
    });
    call.on('timeout', () => call.destroy(Error('owned HTTP timeout')));
    call.on('error', reject);
    call.end(request.body);
  });
}

async function workflows(pages, observe, receipt) {
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
      target.waitForResponse(response => response.request().method() === 'GET' && new URL(response.url()).pathname === '/api/lifecycle/documents'),
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
  const current = async id => (await observe('/api/lifecycle/documents/' + id)).value;
  const mutate = (target, name, id, operation) => action(target, button(target, name), 'POST', '/api/lifecycle/documents/' + id + '/' + operation);
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
  assert.deepEqual(initial.records.map(record => record.document.source), ['alpha.txt', 'beta.txt']);
  const alphaID = initial.records[0].document.document_id;
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
  const stalePage = await action(page, button(page, 'Next page'), 'GET', '/api/lifecycle/documents');
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
  assert.equal(result.status, 200); assert.equal(result.value.record.revision, 2);
  assert.equal(result.value.record.edit_version, 5); assert.equal(result.value.record.document.source, 'alpha.txt');
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
  assert.equal(result.status, 200); assert.equal(result.value.record.revision, 3);
  assert.equal(result.value.record.edit_version, 7); assert.equal(result.value.record.document.source, 'alpha.txt');
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
  assert.equal(deleted.total, 1); assert.equal(deleted.records[0].document.document_id, alphaID);
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
  const gamma = (await observe('/api/lifecycle/documents?deleted=all')).value.records.find(record => record.document.source === 'gamma.txt');
  result = await mutate(peer, 'Delete document', gamma.document.document_id, 'delete');
  assert.equal(result.status, 200);
  await expect(button(peer, 'Restore document')).toBeVisible();
  await open(page, 'gamma.txt');
  await expect(button(page, 'Restore document')).toBeVisible();
  await expect(button(page, 'Delete document')).toBeHidden();
  await expect(field(page, 'Notes')).toHaveValue('gamma unsaved draft');
  await expect(field(page, 'Notes')).toBeDisabled();
  result = await mutate(page, 'Restore document', gamma.document.document_id, 'restore');
  assert.equal(result.status, 409); assert.deepEqual(result.value, {error: 'stale_version'});
  await expect(errorStatus(page, 'stale_version')).toBeVisible();
  await button(page, 'Reload document').click();
  await expect(field(page, 'Notes')).toHaveValue('');
  result = await mutate(page, 'Restore document', gamma.document.document_id, 'restore');
  assert.equal(result.status, 200); assert.equal(result.value.record.edit_version, 3);
  await expect(button(page, 'Delete document')).toBeVisible();
  receipt.completedChecks.push('external_delete_reopened_draft_keeps_stale_token_and_latest_deleted_state');

  // Leave a real tombstone behind so both CLI and process-reopen observations
  // distinguish lifecycle all/active semantics and persisted deletion state.
  await open(page, 'beta.txt');
  const beta = (await observe('/api/lifecycle/documents?deleted=all')).value.records.find(record => record.document.source === 'beta.txt');
  result = await mutate(page, 'Delete document', beta.document.document_id, 'delete');
  assert.equal(result.status, 200);
  await expect(button(page, 'Restore document')).toBeVisible();
  await page.reload({waitUntil: 'domcontentloaded'});
  await expect(pageStatus(page)).toContainText('3 documents; page offset 0');
  assert.equal(await button(page, 'Open beta.txt').count(), 0);
  await apply(page, {'Deletion filter': 'all'});
  await expect(button(page, 'Open beta.txt')).toBeVisible();
  receipt.completedChecks.push('delete_restore_tombstone_filters_and_reload_persistence');

  const finalDocuments = (await observe('/api/lifecycle/documents?deleted=all')).value;
  const alpha = finalDocuments.records.find(record => record.document.document_id === alphaID);
  assert.equal(finalDocuments.total, 4);
  assert.equal(alpha.edit_version, 9); assert.equal(alpha.revision, 3); assert.equal(alpha.deleted, false);
  assert.equal(alpha.notes, 'draft preserved across content refresh');
  assert.deepEqual(alpha.tags, ['cross action', 'é']);
  assert.deepEqual(alpha.collections, ['research']);
  assert.equal(finalDocuments.records.filter(record => record.deleted).length, 1);
  receipt.finalState = {alphaID, documents: finalDocuments,
    collections: (await observe('/api/lifecycle/collections')).value,
    jobs: (await observe('/api/jobs')).value,
    history: (await observe('/api/lifecycle/documents/' + alphaID + '/revisions')).value};
  receipt.deferred = ['M3 bounded export selection and download', 'M3 backup and recovery',
    'M4 migration and API v1', 'candidate sandbox and independent post-freeze acceptance'];
}

async function main(argv) {
  assert.deepEqual([argv[0], argv[2]], ['--origin', '--output']);
  assert.equal(argv.length, 4);
  const origin = checkedOrigin(argv[1]), output = path.resolve(argv[3]);
  assert.equal(output, argv[3]);
  fs.mkdirSync(output);
  const receipt = {protocol: 'library-m2-authored-browser-reference-v1', purpose: 'authored_reference_browser_qualification',
    wholeProjectAcceptance: false, origin, passed: false, completedChecks: [], browserLaunches: 0,
    freshContexts: 0, blockedRequests: [], pageErrors: [], downloads: 0, requests: [], runtime: null};
  const handle = fs.openSync(path.join(output, 'receipt.json'), 'wx');
  let persisted = false;
  const persist = () => {
    if (persisted) return;
    fs.writeFileSync(handle, JSON.stringify(receipt, null, 2) + '\n');
    fs.fsyncSync(handle); fs.closeSync(handle); persisted = true;
  };
  const resources = {}, lifecycle = createLifecycle(resources, receipt, persist);
  const contexts = [], pages = [];
  let requestCount = 0, closing = false;
  const pending = new Set();
  const send = async request => {
    lifecycle.check(); assert(!closing); assert(++requestCount <= 512, 'request count bound');
    const promise = loopbackHTTP(origin, request);
    pending.add(promise);
    try {
      const response = await promise;
      receipt.requests.push({method: request.method, path: request.target, status: response.status,
        requestSha256: digest(request.body || ''), responseSha256: digest(response.body), bytes: response.body.length});
      return response;
    } finally { pending.delete(promise); }
  };
  const observe = async target => {
    const response = await send(checkedRequest(origin, origin + target, 'GET', null));
    return {status: response.status, value: JSON.parse(response.body.toString('utf8'))};
  };
  try {
    receipt.runtime = {...runtime(), driverSha256: digest(fs.readFileSync(__filename))};
    const home = path.join(output, 'browser-home'); fs.mkdirSync(home);
    resources.browser = await require('playwright').chromium.launch({headless: true, chromiumSandbox: true,
      executablePath: receipt.runtime.executable, timeout: 30000,
      handleSIGINT: false, handleSIGTERM: false, handleSIGHUP: false,
      env: {PATH: process.env.PATH || '', HOME: home, TMPDIR: home},
      proxy: {server: 'http://127.0.0.1:9', bypass: '<-loopback>'},
      args: ['--host-resolver-rules=MAP * ~NOTFOUND', '--force-webrtc-ip-handling-policy=disable_non_proxied_udp']});
    receipt.browserLaunches++;
    assert.equal(resources.browser.version(), receipt.runtime.browserVersion);
    for (let index = 0; index < 2; index++) {
      const context = await resources.browser.newContext({viewport: index ? {width: 430, height: 932} : {width: 1280, height: 980},
        serviceWorkers: 'block', permissions: [], acceptDownloads: false});
      contexts.push(context); receipt.freshContexts++;
      context.setDefaultTimeout(8000); context.setDefaultNavigationTimeout(15000);
      await context.routeWebSocket('**/*', socket => { receipt.blockedRequests.push('websocket'); socket.close(); });
      await context.route('**/*', async route => {
        try {
          const actual = route.request();
          const request = checkedRequest(origin, actual.url(), actual.method(), actual.postDataBuffer(), actual.headers()['content-type'] || '');
          await route.fulfill(await send(request));
        } catch (error) {
          receipt.blockedRequests.push(String(error).slice(0, 300));
          await route.abort('blockedbyclient').catch(() => {});
        }
      });
      context.on('page', page => {
        if (context.pages().length > 1) { receipt.blockedRequests.push('unexpected popup'); void page.close(); }
        page.on('pageerror', error => receipt.pageErrors.push(String(error).slice(0, 500)));
        page.on('download', download => { receipt.downloads++; void download.cancel(); });
        page.on('dialog', dialog => { receipt.blockedRequests.push('unexpected dialog'); void dialog.dismiss(); });
      });
      await context.tracing.start({screenshots: true, snapshots: true, sources: false});
      pages.push(await context.newPage());
    }
    await workflows(pages, observe, receipt);
    receipt.viewportChecks = [];
    for (let index = 0; index < pages.length; index++) {
      const viewport = pages[index].viewportSize();
      assert(viewport, 'each browser context has an explicit viewport');
      const layout = await pages[index].evaluate(() => ({
        innerWidth: window.innerWidth,
        documentScrollWidth: document.documentElement.scrollWidth,
        bodyScrollWidth: document.body.scrollWidth,
      }));
      const check = {contextIndex: index, viewportWidth: viewport.width,
        viewportHeight: viewport.height, ...layout,
        passed: layout.documentScrollWidth <= viewport.width + 1};
      receipt.viewportChecks.push(check);
      assert(check.passed, `context ${index}: document width ${layout.documentScrollWidth} exceeds viewport ${viewport.width}`);
    }
    receipt.completedChecks.push('desktop_and_mobile_document_fit_viewport');
    receipt.passed = true;
  } catch (error) {
    receipt.error = String(error).slice(0, 3000);
    process.exitCode = 1;
  } finally {
    for (let index = 0; index < contexts.length; index++) {
      try {
        if (pages[index]) await pages[index].screenshot({path: path.join(output, `page-${index}.png`), timeout: 3000, fullPage: true});
        await contexts[index].tracing.stop({path: path.join(output, `trace-${index}.zip`)});
      } catch (error) { receipt.passed = false; receipt.artifactError = String(error).slice(0, 500); }
    }
    closing = true;
    await lifecycle.cleanup();
    await Promise.allSettled([...pending]);
    lifecycle.dispose();
    if (receipt.blockedRequests.length || receipt.pageErrors.length || receipt.downloads) receipt.passed = false;
    receipt.artifacts = Object.fromEntries(fs.readdirSync(output).filter(name => /\.(png|zip)$/.test(name))
      .map(name => [name, digest(fs.readFileSync(path.join(output, name)))]));
    persist();
    if (!receipt.passed) process.exitCode = 1;
    if (lifecycle.forceExitNeeded) process.exit(process.exitCode || 1);
  }
}

module.exports = {checkedOrigin, checkedRequest};
if (require.main === module) main(process.argv.slice(2)).catch(error => { console.error(String(error)); process.exitCode = 1; });
