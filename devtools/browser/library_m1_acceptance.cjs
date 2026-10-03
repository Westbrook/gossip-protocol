#!/usr/bin/env node
// Trusted browser oracle. Candidate Python never runs on the host. Page content
// executes in a sandboxed fresh Chromium renderer, with closed outbound routing.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const {execFile} = require('node:child_process');
const {createLifecycle} = require('./lifecycle.cjs');

const ORIGIN = 'http://127.0.0.1:8765';
const LIMIT = 262144;
const digest = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
// A wrapping <label><select><option>… includes option descendant text in
// Playwright's label-text query. Use the actual control role and a bounded name
// prefix, keeping strict uniqueness without depending on option wording/IDs.
const INTAKE_CONTROL = {role: 'combobox', name: /^Intake type(?:\s|$)/};

function jobIdentityPattern(id) {
  assert(/^[A-Za-z0-9_-]{1,64}$/.test(id), 'bounded job identity');
  return new RegExp(`(?:^|[^A-Za-z0-9_-])${id}(?:$|[^A-Za-z0-9_-])`);
}

// Fixed endpoint and fixed stdlib program: no URL, method or response can become
// executable code or a Docker argument. -I excludes candidate import paths.
const HTTP_CLIENT = String.raw`import base64,http.client,json,sys
def pairs(values):
    result={}
    for key,value in values:
        if key in result: raise ValueError('duplicate JSON key')
        result[key]=value
    return result
def invalid_constant(value): raise ValueError('nonfinite JSON')
raw=sys.stdin.buffer.read(100001)
if len(raw)>100000: raise ValueError('input bound')
value=json.loads(raw)
if set(value)!={'method','path','body','content_type'}: raise ValueError('request shape')
if value['method'] not in ('GET','POST'): raise ValueError('method')
if not isinstance(value['path'],str) or not value['path'].startswith('/') or len(value['path'])>4096: raise ValueError('path')
body=None if value['body'] is None else base64.b64decode(value['body'],validate=True)
if body is not None and len(body)>65536: raise ValueError('body bound')
connection=http.client.HTTPConnection('127.0.0.1',8765,timeout=3)
try:
    headers={} if body is None else {'Content-Type':value['content_type']}
    connection.request(value['method'],value['path'],body,headers)
    response=connection.getresponse(); raw=response.read(262145)
    if len(raw)>262144: raise ValueError('response bound')
    headers=response.getheaders()
    if len(headers)>64 or sum(len(k)+len(v) for k,v in headers)>16384: raise ValueError('headers bound')
    if response.getheader('Content-Type','').split(';')[0].strip().lower()=='application/json':
        json.loads(raw.decode('utf-8','strict'),object_pairs_hook=pairs,parse_constant=invalid_constant)
    print(json.dumps({'status':response.status,'headers':headers,'body':base64.b64encode(raw).decode()}))
finally:
    connection.close()
`;

function requestPolicy(urlValue, method, rawBody, contentType = '') {
  const url = new URL(urlValue);
  assert.equal(url.origin, ORIGIN, 'only the fixed application origin is reachable');
  assert.equal(url.username + url.password, '', 'credentials forbidden');
  assert(['GET', 'POST'].includes(method), 'only GET and POST are admitted');
  assert(url.pathname.startsWith('/') && (url.pathname + url.search).length <= 4096, 'path bound');
  assert(!/[\r\n\0]/.test(url.pathname + url.search), 'no control characters');
  assert(rawBody === null || Buffer.isBuffer(rawBody), 'body must be bytes');
  assert(rawBody === null || rawBody.length <= 65536, 'body bound');
  assert(method !== 'GET' || rawBody === null, 'GET bodies forbidden');
  if (method === 'POST') {
    assert(url.pathname.startsWith('/api/'), 'writes only target application API');
    assert(/^application\/json(?:\s*;[^\r\n]*)?$/i.test(contentType), 'JSON writes only');
  }
  return {method, path: url.pathname + url.search,
    body: rawBody === null ? null : rawBody.toString('base64'), content_type: contentType};
}

function responsePolicy(value) {
  assert(value && typeof value === 'object' && Object.keys(value).sort().join() === 'body,headers,status');
  assert(Number.isInteger(value.status) && value.status >= 200 && value.status <= 599 && !(value.status >= 300 && value.status < 400), 'redirects and invalid status forbidden');
  assert(Array.isArray(value.headers) && value.headers.length <= 64, 'header count bound');
  assert(typeof value.body === 'string' && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value.body), 'canonical base64 body');
  const body = Buffer.from(value.body, 'base64');
  assert(body.length <= LIMIT, 'response body bound');
  const headers = {};
  let headerBytes = 0;
  for (const pair of value.headers) {
    assert(Array.isArray(pair) && pair.length === 2 && pair.every(x => typeof x === 'string'), 'header pair');
    const [key, val] = pair;
    headerBytes += Buffer.byteLength(key + val);
    assert(headerBytes <= 16384 && /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(key) && !/[\r\n\0]/.test(val), 'header syntax and size');
    // Preserve security/content semantics; hop-by-hop framing is reconstructed
    // by Playwright. Cookies/downloads/refresh are not accepted by this harness.
    if (['content-type', 'content-security-policy', 'x-content-type-options', 'cache-control'].includes(key.toLowerCase())) headers[key.toLowerCase()] = val;
    assert(!['location', 'refresh', 'set-cookie', 'content-disposition'].includes(key.toLowerCase()), 'navigation, cookies and downloads forbidden');
  }
  return {status: value.status, headers, body};
}

function dockerEnvironment() {
  return Object.fromEntries(['PATH', 'HOME', 'DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_CONFIG', 'XDG_RUNTIME_DIR']
    .filter(name => process.env[name] !== undefined).map(name => [name, process.env[name]]));
}

function finalVerdict(receipt) {
  if (receipt.blockedRequestCount || receipt.pageErrors.length || receipt.downloads) receipt.passed = false;
  if (!receipt.passed && receipt.status === 'passed') receipt.status = 'failed';
}

function dockerHTTP(container, request) {
  assert(/^gossip-browser-[0-9a-f]{32}$/.test(container), 'owned container name');
  return new Promise((resolve, reject) => {
    const child = execFile('docker', ['exec', '-i', container, 'python', '-I', '-c', HTTP_CLIENT],
      {timeout: 7000, maxBuffer: 524288, env: dockerEnvironment()}, (error, stdout) => {
        if (error) return reject(Error('Bounded container HTTP request failed'));
        try { resolve(responsePolicy(JSON.parse(stdout))); } catch (invalid) { reject(invalid); }
      });
    child.stdin.on('error', () => {});
    child.stdin.end(JSON.stringify(request));
  });
}

function runtime() {
  const playwright = require('playwright');
  const version = require('playwright/package.json').version;
  assert.equal(version, '1.62.1', 'pinned Playwright');
  const manifest = JSON.parse(fs.readFileSync(path.join(path.dirname(require.resolve('playwright-core/package.json')), 'browsers.json')));
  const chromium = manifest.browsers.find(item => item.name === 'chromium-headless-shell');
  assert.equal(chromium.revision, '1234', 'pinned Chromium');
  // Launch this exact exposed executable explicitly, rather than silently
  // selecting the separately installed headless-shell binary.
  const executable = playwright.chromium.executablePath();
  const chromiumMain = manifest.browsers.find(item => item.name === 'chromium');
  assert.equal(chromiumMain.revision, '1234');
  return {node: process.version, playwright: version, chromiumRevision: chromium.revision,
    nodeExecutable: process.execPath, nodeExecutableSha256: digest(fs.readFileSync(process.execPath)),
    browserVersion: chromium.browserVersion, executable,
    executableSha256: digest(fs.readFileSync(executable)),
    manifestSha256: digest(JSON.stringify(manifest)), driverSha256: digest(fs.readFileSync(__filename))};
}

async function workflows(page, observe, receipt) {
  const expect = require('playwright/test').expect;
  const jobs = page.getByRole('list', {name: 'Ingestion jobs', exact: true});
  const jobRow = id => jobs.getByRole('listitem').filter({hasText: jobIdentityPattern(id)});
  const state = async (id, value, epoch, total) => {
    await expect(jobRow(id)).toContainText(new RegExp(`\\b${value}\\b`));
    await expect(jobRow(id)).toContainText(`epoch ${epoch}`);
    await expect(jobRow(id)).toContainText(`${value === 'completed' ? total : 0}/${total}`);
    const response = await observe('GET', '/api/jobs/' + id);
    assert.equal(response.status, 200);
    assert.deepEqual(response.value, {job_id: id, epoch, state: value, total,
      completed: value === 'completed' ? total : 0, error: null});
  };
  const submit = async (id, kind, localPath, namespace) => {
    await page.getByLabel('Job ID', {exact: true}).fill(id);
    await page.getByRole(INTAKE_CONTROL.role, {name: INTAKE_CONTROL.name}).selectOption(kind);
    await page.getByLabel('Local path', {exact: true}).fill(localPath);
    if (namespace) await page.getByLabel('Namespace', {exact: true}).fill(namespace);
    await page.getByRole('button', {name: 'Submit job', exact: true}).click();
  };
  const action = async (id, name) => {
    const button = page.getByRole('button', {name: `${name} ${id}`, exact: true});
    await expect(button).toBeEnabled();
    await button.click();
  };
  await page.goto(ORIGIN + '/', {waitUntil: 'domcontentloaded'});
  await expect(page.getByRole('heading', {name: 'Local Research Library', exact: true})).toBeVisible();
  await expect(page.getByRole('status').filter({visible: true}).first()).toBeVisible();
  await expect(jobs).toHaveCount(1);
  await submit('browser_directory', 'directory', 'folder', 'directory');
  await state('browser_directory', 'queued', 1, 2);
  await action('browser_directory', 'prepare');
  await state('browser_directory', 'running', 1, 2);
  await action('browser_directory', 'cancel');
  await state('browser_directory', 'cancelled', 2, 2);
  assert.equal((await observe('GET', '/api/documents')).value.total, 0, 'cancelled batch leaves no documents');
  await page.reload({waitUntil: 'domcontentloaded'});
  await state('browser_directory', 'cancelled', 2, 2);
  const cancelledCommit = page.getByRole('button', {name: 'commit browser_directory', exact: true});
  if (await cancelledCommit.count()) await expect(cancelledCommit).toBeDisabled();
  await action('browser_directory', 'retry');
  await state('browser_directory', 'queued', 3, 2);
  await action('browser_directory', 'prepare');
  await state('browser_directory', 'running', 3, 2);
  await action('browser_directory', 'commit');
  await state('browser_directory', 'completed', 3, 2);
  receipt.completedChecks.push('directory_submit_prepare_cancel_reload_retry_commit_atomic_progress');

  for (const [id, kind, input, namespace] of [
    ['browser_zip', 'zip', 'bundle.zip', 'archive'], ['browser_json', 'json', 'bundle.json', null]]) {
    await submit(id, kind, input, namespace);
    await state(id, 'queued', 1, 1);
    await action(id, 'prepare'); await state(id, 'running', 1, 1);
    await action(id, 'commit'); await state(id, 'completed', 1, 1);
  }
  receipt.completedChecks.push('zip_and_json_browser_intake');
  await page.getByLabel('Source path', {exact: true}).fill('one.txt');
  await page.getByRole('button', {name: 'Import local file', exact: true}).click();
  await expect(page.getByRole('button', {name: 'one.txt', exact: true})).toBeVisible();
  await page.getByLabel('Search', {exact: true}).fill('literal browser text');
  await page.getByRole('button', {name: 'Search', exact: true}).click();
  await expect(page.getByRole('list', {name: 'Documents', exact: true}).getByRole('button')).toHaveCount(1);
  await page.getByRole('button', {name: 'json/literal.html', exact: true}).click();
  await expect(page.getByRole('region', {name: 'Document details', exact: true})).toContainText('<img src=x onerror="window.__injected=true"> literal browser text');
  assert.equal(await page.evaluate(() => window.__injected === true), false, 'document content is literal text');
  assert.equal(await page.locator('img[src="x"]').count(), 0, 'imported HTML is not inserted as DOM');
  const documents = (await observe('GET', '/api/documents')).value;
  assert.deepEqual(documents.documents.map(doc => [doc.source, doc.text]), [
    ['archive/z.txt', 'browser archive omega'], ['directory/a.txt', 'browser directory alpha'],
    ['directory/b.md', 'browser directory beta'],
    ['json/literal.html', '<img src=x onerror="window.__injected=true"> literal browser text'],
    ['one.txt', 'browser single-file import']]);
  assert.equal(documents.total, 5);
  const exported = await observe('GET', '/api/export');
  assert.equal(exported.status, 200);
  assert.deepEqual(exported.value, {format: 'local-research-library-v0', documents: documents.documents});
  receipt.completedChecks.push('single_import_search_literal_display_and_http_export');
  await submit('browser_invalid', 'json', 'missing.json', null);
  const intakeError = page.getByRole('status').filter({hasText: 'io_error'});
  await expect(intakeError).toHaveCount(1);
  await expect(intakeError).toBeVisible();
  const rejectedJobs = (await observe('GET', '/api/jobs')).value.jobs;
  assert.equal(rejectedJobs.length, 3, 'rejected intake is not durably admitted');
  assert.equal(rejectedJobs.some(job => job.job_id === 'browser_invalid'), false);
  assert.equal((await observe('GET', '/api/documents')).value.total, 5);
  receipt.completedChecks.push('accessible_intake_error_has_no_partial_state');
  // Make the persistence observation independent of browser form restoration:
  // a reload may preserve the previous search input's value.
  await page.getByLabel('Search', {exact: true}).fill('');
  await page.getByRole('button', {name: 'Search', exact: true}).click();
  await expect(page.getByRole('list', {name: 'Documents', exact: true}).getByRole('button')).toHaveCount(5);
  await page.reload({waitUntil: 'domcontentloaded'});
  await state('browser_directory', 'completed', 3, 2);
  await expect(page.getByRole('button', {name: 'one.txt', exact: true})).toBeVisible();
  receipt.completedChecks.push('completed_jobs_and_documents_survive_browser_reload');
}

async function main(argv) {
  if (argv.length === 1 && argv[0] === '--probe') { console.log(JSON.stringify(runtime())); return; }
  assert.deepEqual([argv[0], argv[2]], ['--container', '--output']);
  assert.equal(argv.length, 4);
  const container = argv[1], output = path.resolve(argv[3]);
  assert(/^gossip-browser-[0-9a-f]{32}$/.test(container));
  assert.equal(output, argv[3]);
  fs.mkdirSync(output, {recursive: false});
  const handle = fs.openSync(path.join(output, 'receipt.json'), 'wx');
  const receipt = {protocol: 'library-m1-browser-driver-v4', container, passed: false,
    completedChecks: [], blockedRequests: [], blockedRequestCount: 0, pageErrors: [], requests: [], runtime: null,
    browserLaunches: 0, freshContexts: 0, downloads: 0, evaluationStarted: false, evaluationCompleted: false};
  const resources = {};
  let persisted = false;
  const persist = () => {
    if (persisted) return;
    fs.writeFileSync(handle, JSON.stringify(receipt, null, 2) + '\n');
    fs.fsyncSync(handle); fs.closeSync(handle); persisted = true;
  };
  const lifecycle = createLifecycle(resources, receipt, persist);
  const blocked = value => { receipt.blockedRequestCount++; if (receipt.blockedRequests.length < 64) receipt.blockedRequests.push(value); };
  let context, page;
  let closing = false;
  let queue = Promise.resolve();
  let requests = 0;
  const request = (method, target, body = null, contentType = '') => {
    assert(++requests <= 256, 'request count bound');
    const validated = requestPolicy(ORIGIN + target, method, body, contentType);
    const result = queue.then(() => { lifecycle.check(); assert(!closing, 'browser observation is closing'); return dockerHTTP(container, validated); });
    queue = result.catch(() => {});
    return result.then(response => {
      receipt.requests.push({method, path: target, status: response.status,
        requestSha256: digest(JSON.stringify(validated)), bodySha256: digest(response.body), bytes: response.body.length});
      return response;
    });
  };
  const observe = async (method, target) => {
    const result = await request(method, target);
    return {status: result.status, value: JSON.parse(result.body.toString('utf8'))};
  };
  try {
    receipt.runtime = runtime();
    let ready = false;
    for (let attempt = 0; attempt < 20; attempt++) {
      lifecycle.check();
      try { const result = await observe('GET', '/health'); if (result.status === 200 && result.value.status === 'ok') { ready = true; break; } } catch (_) {}
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert(ready, 'actual container HTTP server is ready');
    const playwright = require('playwright');
    const home = path.join(output, 'browser-home'); fs.mkdirSync(home);
    resources.browser = await playwright.chromium.launch({headless: true, chromiumSandbox: true,
      executablePath: receipt.runtime.executable,
      handleSIGINT: false, handleSIGTERM: false, handleSIGHUP: false, timeout: 30000,
      env: {PATH: process.env.PATH || '', HOME: home, TMPDIR: home},
      proxy: {server: 'http://127.0.0.1:9', bypass: '<-loopback>'},
      args: ['--host-resolver-rules=MAP * ~NOTFOUND', '--force-webrtc-ip-handling-policy=disable_non_proxied_udp']});
    receipt.browserLaunches++;
    assert.equal(resources.browser.version(), receipt.runtime.browserVersion, 'actual pinned Chromium version');
    context = await resources.browser.newContext({viewport: {width: 1280, height: 980},
      serviceWorkers: 'block', acceptDownloads: false, permissions: []});
    receipt.freshContexts++;
    context.setDefaultTimeout(10000); context.setDefaultNavigationTimeout(15000);
    await context.routeWebSocket('**/*', socket => { blocked({kind: 'websocket'}); socket.close(); });
    await context.route('**/*', async route => {
      try {
        const actual = route.request();
        const checked = requestPolicy(actual.url(), actual.method(), actual.postDataBuffer(), actual.headers()['content-type'] || '');
        const response = await request(checked.method, checked.path, actual.postDataBuffer(), checked.content_type);
        await route.fulfill(response);
      } catch (error) {
        blocked({kind: 'http', reason: String(error).slice(0, 300)});
        await route.abort('blockedbyclient').catch(() => {});
      }
    });
    context.on('page', opened => {
      if (context.pages().length > 1) { blocked({kind: 'extra-page'}); void opened.close(); }
      opened.on('download', download => { receipt.downloads++; void download.cancel(); });
      opened.on('pageerror', error => { if (receipt.pageErrors.length < 20) receipt.pageErrors.push(String(error).slice(0, 500)); });
      opened.on('dialog', dialog => { void dialog.dismiss(); });
    });
    await context.tracing.start({screenshots: true, snapshots: true, sources: false});
    page = await context.newPage();
    receipt.evaluationStarted = true;
    await workflows(page, observe, receipt);
    assert.equal(receipt.blockedRequests.length, 0, 'application attempted no forbidden network');
    assert.equal(receipt.pageErrors.length, 0, 'browser scripts had no unhandled errors');
    assert.equal(receipt.downloads, 0, 'no downloads occurred');
    receipt.passed = true;
    receipt.status = 'passed'; receipt.evaluationCompleted = true;
  } catch (error) {
    receipt.error = String(error).slice(0, 2000);
    receipt.status = receipt.evaluationStarted && !lifecycle.cancelled ? 'failed' : 'infrastructure_failure';
    receipt.evaluationCompleted = receipt.status === 'failed';
    process.exitCode = 1;
  } finally {
    if (page) {
      try { await page.screenshot({path: path.join(output, 'final.png'), timeout: 3000}); }
      catch (error) { receipt.screenshotError = String(error).slice(0, 300); }
    }
    if (context) {
      try { await context.tracing.stop({path: path.join(output, 'trace.zip')}); }
      catch (error) { receipt.traceError = String(error).slice(0, 300); }
    }
    if (page && (receipt.screenshotError || receipt.traceError)) receipt.passed = false;
    closing = true;
    await lifecycle.cleanup();
    await queue;
    lifecycle.dispose();
    // Candidate scripts can still run while screenshots/traces are collected.
    // A late forbidden action must not coexist with a successful verdict.
    finalVerdict(receipt);
    receipt.artifacts = Object.fromEntries(['final.png', 'trace.zip'].filter(name => fs.existsSync(path.join(output, name)))
      .map(name => [name, digest(fs.readFileSync(path.join(output, name)))]));
    persist();
    if (!receipt.passed) process.exitCode = 1;
    if (lifecycle.forceExitNeeded) process.exit(process.exitCode || 1);
  }
}

module.exports = {requestPolicy, responsePolicy, dockerEnvironment, HTTP_CLIENT, runtime, finalVerdict, INTAKE_CONTROL, jobIdentityPattern};
if (require.main === module) main(process.argv.slice(2)).catch(error => { console.error(String(error)); process.exitCode = 1; });
