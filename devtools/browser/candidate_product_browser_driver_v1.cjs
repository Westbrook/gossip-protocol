'use strict';
// Trusted Playwright host. Candidate scripts never receive this action catalog.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {once} = require('node:events');
const p = require('./candidate_product_browser_protocol_v1.cjs');
const {createLifecycle} = require('./lifecycle.cjs');

async function main(argv) {
  assert.deepEqual([argv[0], argv[2]], ['--input', '--output']); assert.equal(argv.length, 4);
  const input = path.resolve(argv[1]), output = path.resolve(argv[3]);
  assert.equal(input, argv[1]); assert.equal(output, argv[3]);
  const declaration = JSON.parse(fs.readFileSync(input));
  assert.equal(declaration.protocol, p.PROTOCOL);
  assert(Array.isArray(declaration.actions) && declaration.actions.length <= 64);
  fs.mkdirSync(output, {recursive: false, mode: 0o700});
  const runtime = p.runtime(); assert.deepEqual(runtime, declaration.runtime);
  const {chromium} = require('playwright');
  let sequence = 0, requestCount = 0, currentAction = null, closing = false;
  const pending = new Map();
  let outbound = Promise.resolve();
  function emit(kind, value) {
    const record = {protocol: p.PROTOCOL, seq: ++sequence, kind, ...value};
    const raw = Buffer.from(JSON.stringify(record) + '\n'); assert(raw.length <= p.MESSAGE_LIMIT);
    outbound = outbound.then(async () => { if (!process.stdout.write(raw)) await once(process.stdout, 'drain'); });
    return outbound;
  }
  const decoder = new p.LineDecoder();
  const failPending = error => { for (const item of pending.values()) item.reject(error); pending.clear(); };
  process.stdin.on('data', chunk => {
    try {
      for (const raw of decoder.feed(chunk)) {
        const row = JSON.parse(raw.toString('utf8'));
        assert.equal(row.protocol, p.PROTOCOL); assert.equal(row.kind, 'response');
        assert(Number.isInteger(row.request_id) && pending.has(row.request_id));
        const item = pending.get(row.request_id); pending.delete(row.request_id); item.resolve(row);
      }
    } catch (error) { failPending(error); process.exitCode = 2; }
  });
  process.stdin.on('end', () => { try { decoder.eof(); } catch (error) { failPending(error); } failPending(Error('owner pipe closed')); });
  let requestQueue = Promise.resolve();
  function request(request, source, actionId = currentAction) {
    assert(!closing && ++requestCount <= 256); const id = requestCount;
    const next = requestQueue.then(async () => {
      const reply = new Promise((resolve, reject) => pending.set(id, {resolve, reject}));
      await emit('request', {request_id: id, action_id: actionId, source, request});
      const result = await reply;
      if (result.available !== true) throw Error('bridge unavailable:' + String(result.reason).slice(0, 300));
      return {id, ...p.responsePolicy(result)};
    });
    requestQueue = next.catch(() => {}); return next;
  }
  const controls = [];
  async function control(target) {
    const result = await request(p.requestPolicy(p.ORIGIN + target, 'GET', null, [['Accept', 'application/json']]), 'control');
    controls.push({action_id: currentAction, request_id: result.id, target});
    return result;
  }
  const resources = {}, lifecycleState = {cleanup: {}};
  const lifecycle = createLifecycle(resources, lifecycleState, () => {}, {cleanupTimeoutMs: 5000});
  let context = null, page = null, contextNumber = 0, literalBefore = null;
  const diagnostics = [], artifacts = [], browserRequests = [];
  function diagnostic(kind, value) {
    if (diagnostics.length < 64) diagnostics.push({kind, action_id: currentAction, value: String(value).slice(0, 512)});
  }
  async function saveContext() {
    if (!context) return;
    const tag = 'context-' + String(contextNumber).padStart(2, '0');
    if (page) {
      try { await page.screenshot({path: path.join(output, tag + '.png'), timeout: 3000}); artifacts.push(tag + '.png'); }
      catch (error) { diagnostic('screenshot-unavailable', error); }
    }
    try { await context.tracing.stop({path: path.join(output, tag + '.zip')}); artifacts.push(tag + '.zip'); }
    catch (error) { diagnostic('trace-unavailable', error); }
    await context.close(); context = null; page = null;
  }
  async function freshContext() {
    await saveContext();
    context = await resources.browser.newContext({viewport: {width: 1280, height: 980}, serviceWorkers: 'block',
      acceptDownloads: false, permissions: [], locale: 'en-US', timezoneId: 'UTC'});
    contextNumber++;
    context.setDefaultTimeout(15000); context.setDefaultNavigationTimeout(15000);
    await context.routeWebSocket('**/*', socket => { diagnostic('blocked-websocket', socket.url()); socket.close(); });
    await context.route('**/*', async route => {
      const actionId = currentAction;
      try {
        const actual = route.request();
        const pairs = (await actual.headersArray()).map(x => [x.name, x.value]);
        const checked = p.requestPolicy(actual.url(), actual.method(), actual.postDataBuffer(), pairs);
        browserRequests.push({action_id: actionId, method: checked.method, target: checked.target,
          body_sha256: checked.body_sha256});
        const result = await request(checked, 'browser', actionId);
        await route.fulfill({status: result.status, headers: result.headers, body: result.body});
      } catch (error) {
        diagnostic('route-unavailable', error);
        await route.abort('blockedbyclient').catch(() => {});
      }
    });
    context.on('page', opened => {
      if (context.pages().length > 1) { diagnostic('blocked-extra-page', opened.url()); void opened.close(); }
      opened.on('download', download => { diagnostic('blocked-download', download.suggestedFilename()); void download.cancel(); });
      opened.on('pageerror', error => diagnostic('page-error', error));
      opened.on('dialog', dialog => { diagnostic('dialog', dialog.type()); void dialog.dismiss(); });
      opened.on('framenavigated', frame => {
        const url = frame.url();
        if (url !== 'about:blank' && !url.startsWith(p.ORIGIN + '/')) {
          diagnostic('unsupported-frame-origin', url); void opened.close();
        }
      });
    });
    await context.tracing.start({screenshots: true, snapshots: true, sources: false});
    page = await context.newPage();
  }
  function jobs() { return page.getByRole('list', {name: 'Ingestion jobs', exact: true}); }
  function row(id) { return jobs().getByRole('listitem').filter({hasText: p.jobIdentityPattern(id)}); }
  async function locatorFacts(locator) {
    const count = await locator.count(); const entries = [];
    if (count > 256) throw Error('DOM locator allocation exceeded');
    for (let index = 0; index < count; index++) {
      const item = locator.nth(index);
      const text = await item.innerText(); assert(Buffer.byteLength(text) <= 65536, 'DOM text allocation exceeded');
      entries.push({visible: await item.isVisible(), enabled: await item.isEnabled(), text});
    }
    return {count, entries};
  }
  function literalImages() {
    return page.getByRole('region', {name: 'Document details', exact: true})
      .locator('img[src="x"]').filter({visible: true});
  }
  async function literalTransition(action) {
    if (action.op !== 'search_open' || literalBefore === null) return null;
    const handles = await literalImages().elementHandles();
    assert(handles.length <= 256, 'Literal image allocation exceeded');
    let matching = 0, added = 0;
    try {
      for (const handle of handles) {
        const evidence = await handle.evaluate((node, before) => ({
          matching: node.getAttribute('onerror') === 'window.__candidate_browser_injected=true',
          existed: before.includes(node)}), literalBefore.handles);
        if (evidence.matching) { matching++; if (!evidence.existed) added++; }
      }
    } finally { for (const handle of handles) await handle.dispose(); }
    return {action_id: action.id, source: action.source, before_count: literalBefore.handles.length,
      after_matching_count: matching, new_matching_count: added};
  }
  async function snapshot(action) {
    const observedJobs = [], declaredJobs = action.jobs || (action.job ? [action.job] : []);
    for (const expected of declaredJobs) {
      const actions = {};
      for (const verb of ['prepare', 'commit', 'cancel', 'retry']) {
        actions[verb] = await locatorFacts(page.getByRole('button', {name: verb + ' ' + expected.job_id, exact: true}));
      }
      observedJobs.push({job_id: expected.job_id, row: await locatorFacts(row(expected.job_id)), actions});
    }
    const intake = page.getByRole('combobox', {name: /^Intake type(?:\s|$)/});
    const intakeOptions = await intake.evaluateAll(nodes => nodes.map(node => ({tag: node.tagName,
      values: Array.from(node.options || []).map(option => option.value), selected: node.value})));
    const controls = {};
    for (const label of ['Source path', 'Search', 'Job ID', 'Local path', 'Namespace']) {
      controls[label] = await locatorFacts(page.getByLabel(label, {exact: true}));
    }
    for (const label of ['Import local file', 'Search', 'Submit job']) {
      controls['button:' + label] = await locatorFacts(page.getByRole('button', {name: label, exact: true}));
    }
    const documentFacts = {};
    for (const source of action.visible_documents || action.documents.map(pair => pair[0])) {
      documentFacts[source] = await locatorFacts(page.getByRole('list', {name: 'Documents', exact: true})
        .getByRole('button', {name: source, exact: true}));
    }
    const value = {heading: await locatorFacts(page.getByRole('heading', {name: 'Local Research Library', exact: true})),
      status: await locatorFacts(page.getByRole('status')), job_list: await locatorFacts(jobs()),
      job_rows: await locatorFacts(jobs().getByRole('listitem')),
      document_list: await locatorFacts(page.getByRole('list', {name: 'Documents', exact: true})),
      document_buttons: await locatorFacts(page.getByRole('list', {name: 'Documents', exact: true}).getByRole('button')),
      controls, intake: await locatorFacts(intake), intake_options: intakeOptions, jobs: observedJobs, documents: documentFacts,
      details: await locatorFacts(page.getByRole('region', {name: 'Document details', exact: true})),
      literal_transition: await literalTransition(action), current_url: page.url(), context: contextNumber};
    assert(Buffer.byteLength(JSON.stringify(value)) <= 262144, 'DOM facts allocation exceeded'); return value;
  }
  function ready(action, value) {
    const one = fact => fact.count === 1 && fact.entries[0].visible;
    if (!value.heading.entries.some(x => x.visible) || !one(value.job_list) || !one(value.document_list) || !value.status.entries.some(x => x.visible)) return false;
    for (const expected of action.jobs || (action.job ? [action.job] : [])) {
      const found = value.jobs.find(x => x.job_id === expected.job_id);
      if (!found || !found.row.entries.some(entry => entry.visible
          && new RegExp('\\b' + expected.state + '\\b').test(entry.text)
          && new RegExp('epoch\\s+' + expected.epoch + '(?![0-9])').test(entry.text)
          && new RegExp('(?<![0-9])' + expected.completed + '\\s*/\\s*' + expected.total + '(?![0-9])').test(entry.text))) return false;
    }
    if (action.error && value.status.entries.filter(x => x.visible && x.text.includes(action.error)).length !== 1) return false;
    if (action.replaces_error && value.status.entries.some(x => x.visible && x.text.includes(action.replaces_error))) return false;
    if (Object.values(value.documents).some(x => !one(x))) return false;
    if (action.op === 'search_open' && !value.details.entries.some(x => x.visible && x.text.includes(action.literal))) return false;
    return true;
  }
  async function observe(action) {
    const deadline = Date.now() + 15000;
    let value = null, error = null, matched = false;
    do {
      lifecycle.check();
      try { value = await snapshot(action); matched = ready(action, value); if (matched) break; }
      catch (caught) { error = String(caught).slice(0, 512); }
      await new Promise(resolve => setTimeout(resolve, 50));
    } while (Date.now() < deadline);
    await requestQueue;
    const ids = [];
    for (const target of ['/api/jobs', '/api/documents']) { const reply = await control(target); ids.push(reply.id); }
    return {dom: value, observation_window_satisfied: matched, observation_error: error, control_request_ids: ids};
  }
  async function activate(name) {
    const locator = page.getByRole('button', {name, exact: true});
    const deadline = Date.now() + 15000;
    do {
      const count = await locator.count(); assert(count <= 256);
      for (let index = 0; index < count; index++) {
        const button = locator.nth(index);
        if (await button.isVisible() && await button.isEnabled()) { await button.click(); return; }
      }
      await new Promise(resolve => setTimeout(resolve, 50));
    } while (Date.now() < deadline);
    throw Error('Required actionable button unavailable: ' + name);
  }
  async function submit(action, id) {
    await page.getByLabel('Job ID', {exact: true}).fill(id);
    await page.getByRole('combobox', {name: /^Intake type(?:\s|$)/}).selectOption(action.kind);
    await page.getByLabel('Local path', {exact: true}).fill(action.path);
    if (action.namespace !== null) await page.getByLabel('Namespace', {exact: true}).fill(action.namespace);
    await activate('Submit job');
  }
  let failure = null, launched = false;
  try {
    await emit('runtime', {runtime, driver_sha256: p.sha(fs.readFileSync(__filename))});
    const home = path.join(output, 'browser-home'); fs.mkdirSync(home, {mode: 0o700});
    resources.browser = await chromium.launch({headless: true, chromiumSandbox: true,
      executablePath: runtime.browser_executable, handleSIGINT: false, handleSIGTERM: false, handleSIGHUP: false,
      timeout: 30000, env: {PATH: process.env.PATH || '', HOME: home, TMPDIR: home},
      proxy: {server: 'http://127.0.0.1:9', bypass: '<-loopback>'},
      args: ['--host-resolver-rules=MAP * ~NOTFOUND', '--force-webrtc-ip-handling-policy=disable_non_proxied_udp']});
    launched = true; assert.equal(resources.browser.version(), runtime.browser_version);
    await emit('launched', {browser_version: resources.browser.version(), browser_launches: 1});
    for (const action of declaration.actions) {
      currentAction = action.id; literalBefore = null; await emit('action_start', {action_id: action.id});
      try {
        if (['open', 'fresh_context'].includes(action.op)) { await freshContext(); await page.goto(p.ORIGIN + '/', {waitUntil: 'domcontentloaded'}); }
        else if (action.op === 'reload') await page.reload({waitUntil: 'domcontentloaded'});
        else if (action.op === 'submit') await submit(action, action.job.job_id);
        else if (action.op === 'submit_error') await submit(action, action.identifier);
        else if (action.op === 'action') await activate(action.action + ' ' + action.job.job_id);
        else if (action.op === 'import') {
          await page.getByLabel('Source path', {exact: true}).fill(action.path);
          await activate('Import local file');
        } else if (['search_open', 'clear_search'].includes(action.op)) {
          await page.getByLabel('Search', {exact: true}).fill(action.query || '');
          await activate('Search');
          if (action.op === 'search_open') {
            const handles = await literalImages().elementHandles();
            assert(handles.length <= 256, 'Literal baseline allocation exceeded');
            literalBefore = {handles};
            await page.getByRole('button', {name: action.source, exact: true}).click();
          }
        } else throw Error('unknown closed action');
        await emit('observation', {action_id: action.id, ...await observe(action)});
      } catch (error) {
        let dom = null; try { dom = await snapshot(action); } catch (_) {}
        await emit('observation', {action_id: action.id, dom, observation_window_satisfied: false,
          observation_error: String(error).slice(0, 1000), control_request_ids: []});
        throw error;
      } finally {
        if (literalBefore !== null) for (const handle of literalBefore.handles) await handle.dispose();
      }
    }
  } catch (error) { failure = String(error).slice(0, 1000); process.exitCode = 2; }
  finally {
    try { await saveContext(); } catch (error) { diagnostic('context-cleanup', error); }
    closing = true;
    await lifecycle.cleanup();
    await emit('terminal', {launched, browser_launches: launched ? 1 : 0, contexts: contextNumber,
      request_count: requestCount, diagnostics, failure, artifacts, cleanup: lifecycleState.cleanup,
      force_exit_needed: lifecycle.forceExitNeeded, cancelled: lifecycleState.cancelled === true});
    lifecycle.dispose(); await outbound;
    process.stdin.destroy();
    if (lifecycle.forceExitNeeded) process.exit(process.exitCode || 2);
  }
}
module.exports = {main};
if (require.main === module) main(process.argv.slice(2)).catch(error => { process.stderr.write(String(error).slice(0, 2000) + '\n'); process.exitCode = 2; });
