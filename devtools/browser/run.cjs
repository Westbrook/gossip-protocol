#!/usr/bin/env node
// One Chromium launch per batch; all pages receive isolated contexts.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const {spawn} = require('node:child_process');
const {once} = require('node:events');
const readline = require('node:readline');
const {createLifecycle} = require('./lifecycle.cjs');

const ROOT = path.resolve(__dirname, '../..');
const PAGES = {
  research: {start: 'design', link: 'Protocol sketch', target: 'protocol'},
  investigation: {start: 'design', link: 'Experiment matrix', target: 'matrix'},
  pilot: {start: 'pipeline', link: 'Three arms', target: 'arms'},
  experiments: {start: 'discovery', link: 'Coding results', target: 'discovery-results'},
  swarm: {start: 'protocol', link: 'Fair experiment', target: 'evaluation'},
  'swarm-pilot': {start: 'selection', link: 'Five comparisons', target: 'policies'},
  'sustained-pilot': {start: 'continuity', link: 'Three policies', target: 'policies'},
  'verification-pilot': {start: 'recovery', link: 'Matched reviewers', target: 'policies'},
};

function options(argv) {
  const opts = {pages: ['report', ...Object.keys(PAGES)], captures: [], fixture: false, requireFinal: false,
    locator: path.join(ROOT, '.progress-report/project.json')};
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    if (flag === '--fixture') opts.fixture = true;
    else if (flag === '--require-final') opts.requireFinal = true;
    else if (['--pages', '--capture', '--url', '--output', '--locator'].includes(flag)) {
      const value = argv[++i];
      assert(value && !value.startsWith('--'), `Missing value for ${flag}`);
      if (flag === '--pages') opts.pages = value === 'all' ? opts.pages : value.split(',');
      else if (flag === '--capture') opts.captures = value === 'all' ? ['report', ...Object.keys(PAGES)] : value.split(',');
      else opts[flag.slice(2)] = value;
    } else throw Error(`Unknown option ${flag}`);
  }
  for (const name of [...opts.pages, ...opts.captures]) assert(name === 'report' || PAGES[name], `Unknown page ${name}`);
  assert(opts.pages.length && new Set(opts.pages).size === opts.pages.length, 'Page selection must be unique and nonempty');
  assert(!opts.fixture || !opts.url, 'Choose --fixture or --url');
  return opts;
}

const digest = data => crypto.createHash('sha256').update(data).digest('hex');

function pageURL(name, base, fixture = false) {
  const overrides = {experiments: 'EXPERIMENT_URL', swarm: 'SWARM_URL',
    'swarm-pilot': 'SWARM_PILOT_URL', 'sustained-pilot': 'SUSTAINED_PILOT_URL',
    'verification-pilot': 'VERIFICATION_PILOT_URL'};
  const supplied = overrides[name] && process.env[overrides[name]];
  const url = new URL(supplied || `${name}.html`, base);
  if (fixture && supplied) {
    const relocated = new URL(base);
    url.protocol = relocated.protocol;
    url.host = relocated.host;
  }
  assert.equal(url.origin, new URL(base).origin, 'page belongs to the verified report server');
  return url;
}

async function fixtureServer(opts, artifacts, resources) {
  const directory = path.join(artifacts, 'fixture');
  const child = spawn(process.env.PYTHON || 'python3', [path.join(__dirname, 'fixture_server.py'),
    '--locator', opts.locator, '--directory', directory], {env: {...process.env, PYTHONDONTWRITEBYTECODE: '1'}, stdio: ['ignore', 'pipe', 'pipe']});
  resources.server = child;
  child.stderr.on('data', chunk => fs.appendFileSync(path.join(artifacts, 'fixture-server.log'), chunk));
  const lines = readline.createInterface({input: child.stdout});
  let timer;
  try {
    const line = await Promise.race([
      once(lines, 'line').then(([value]) => value),
      once(child, 'exit').then(([code]) => {throw Error(`Fixture server exited before ready (${code})`);}),
      new Promise((_, reject) => {timer = setTimeout(() => reject(Error('Fixture server startup timed out')), 10000);}),
    ]);
    return JSON.parse(line);
  } finally { clearTimeout(timer); lines.close(); }
}

async function get(api, url) {
  const response = await api.get(url);
  assert.equal(response.status(), 200, `${url} is available`);
  return response;
}

async function identity(api, base, locator, fixture, pages) {
  await get(api, base + 'health');
  const state = await (await get(api, base + 'api/state')).json();
  assert.equal(state.projectId, locator.projectId, 'report project identity');
  assert.equal(path.resolve(state.handoff.reportWorkspace), path.resolve(locator.reportWorkspace), 'canonical report workspace identity');
  assert.equal(path.resolve(state.handoff.canonicalState), path.resolve(locator.stateLocation), 'canonical report state identity');
  const serving = fixture ? fixture.directory : locator.reportWorkspace;
  const bindings = {};
  const sourceBindings = {};
  const files = [['', path.join(serving, 'index.html')], ['archive', path.join(serving, 'index.html')]];
  for (const name of pages.filter(name => name !== 'report')) {
    const url = pageURL(name, base, Boolean(fixture));
    let file;
    if (url.pathname === `/${name}.html`) file = path.join(fixture ? path.join(serving, 'pages') : ROOT, `${name}.html`);
    else {
      assert(url.pathname.startsWith('/artifacts/'), 'custom page must be an immutable report artifact');
      const artifactRoot = path.join(serving, 'artifacts');
      file = path.resolve(artifactRoot, decodeURIComponent(url.pathname.slice('/artifacts/'.length)));
      assert(file.startsWith(artifactRoot + path.sep), 'artifact remains in the expected report workspace');
    }
    files.push([url.pathname.slice(1) + url.search, file]);
  }
  const latest = pages.includes('report') && state.deliverables.at(-1);
  if (latest) {
    const url = new URL(latest.url);
    assert.equal(url.origin, new URL(base).origin, 'immutable snapshot belongs to the report');
    assert(url.pathname.startsWith('/artifacts/'), 'snapshot is an immutable report artifact');
    const artifactRoot = path.join(serving, 'artifacts');
    const file = path.resolve(artifactRoot, decodeURIComponent(url.pathname.slice('/artifacts/'.length)));
    assert(file.startsWith(artifactRoot + path.sep), 'snapshot remains in the report workspace');
    files.push([url.pathname.slice(1) + url.search, file]);
  }
  for (const [route, file] of files) {
    const actual = digest(await (await get(api, base + route)).body());
    const fileBytes = fs.readFileSync(file);
    const expected = digest(fileBytes);
    assert.equal(actual, expected, `server serves exact expected file: ${file}`);
    bindings[route || '/'] = actual;
    let source = file;
    let sourceBytes = fileBytes;
    if (fixture) {
      const relative = path.relative(serving, file);
      source = relative.startsWith('pages' + path.sep) ? path.join(ROOT, path.basename(file)) : path.join(locator.reportWorkspace, relative);
      sourceBytes = Buffer.from(fileBytes.toString('utf8').replaceAll(base, locator.reportUrl.replace(/\/?$/, '/')));
      assert.equal(digest(sourceBytes), digest(fs.readFileSync(source)), `fixture represents exact current source: ${source}`);
    }
    sourceBindings[source] = digest(sourceBytes);
  }
  return {state, bindings, sourceBindings};
}

async function fit(page, label) {
  assert.equal(await page.locator('body').evaluate(el => el.scrollWidth <= window.innerWidth), true, `${label} fits viewport`);
}

async function capture(page, directory, label, enabled) {
  if (enabled) await page.screenshot({path: path.join(directory, `${label}.png`), fullPage: false});
}

async function contentChecks(page, name, requireFinal) {
  if (!requireFinal) return;
  if (name === 'investigation') assert.equal(/pending|in progress|still running/i.test(await page.locator('#measured-results').innerText()), false, 'final results have no placeholder');
  if (name === 'pilot') {
    assert.equal(await page.locator('#live-results .warning').count(), 0, 'final pilot has no draft placeholder');
    assert.equal(await page.locator('#live-results table').count(), 3, 'pilot results, transport metrics and commit provenance retained');
  }
  const ids = name === 'experiments' ? ['overall-result', 'discovery-outcomes', 'recovery-outcomes', 'accounting-outcomes']
    : name === 'verification-pilot' ? ['overall-result', 'live-outcomes', 'cost-outcomes', 'verification-outcomes']
    : ['swarm-pilot', 'sustained-pilot'].includes(name) ? ['overall-result', 'live-outcomes', 'cost-outcomes'] : [];
  for (const id of ids) {
    assert.equal(await page.locator(`#${id}`).evaluate(el => el.classList.contains('warning')), false, `${id} has no draft placeholder`);
    if (name === 'experiments') assert.equal(/Pending live|Pending reconciliation|record in progress|pending results/i.test(await page.locator(`#${id}`).innerText()), false);
  }
}

async function pageContract(page, name, base, directory, opts) {
  const spec = PAGES[name];
  const url = pageURL(name, base, opts.fixture);
  url.searchParams.delete('progress-report');
  assert.equal((await page.goto(url.href)).status(), 200, 'page route available');
  await fit(page, 'desktop');
  assert.equal(await page.getByRole('link', {name: 'Progress Report', exact: true}).count(), 0, 'overlay absent without flag');
  assert.equal(await page.locator('nav a').evaluateAll(links => links.every(a => {
    const href = a.getAttribute('href');
    return href && href.startsWith('#') && Boolean(document.getElementById(href.slice(1)));
  })), true, 'all section anchors resolve');
  await capture(page, directory, 'desktop', opts.captures.includes(name));
  url.search = '?sample=keep&progress-report=untrusted';
  url.hash = spec.start;
  await page.goto(url.href);
  const back = page.locator('a.return').filter({hasText: /^Progress Report$/});
  assert.equal(await back.isVisible(), true, 'flag exposes return link');
  assert.equal(await back.getAttribute('href'), base, 'return uses trusted report target');
  await page.getByRole('link', {name: spec.link, exact: true}).click();
  const current = new URL(page.url());
  assert.equal(current.searchParams.get('sample'), 'keep', 'query preserved');
  assert.equal(current.searchParams.has('progress-report'), true, 'flag preserved');
  assert.equal(current.hash, `#${spec.target}`, 'fragment navigation');
  await page.setViewportSize({width: 390, height: 844});
  url.search = '?progress-report';
  url.hash = spec.start;
  await page.goto(url.href);
  await fit(page, 'mobile');
  await capture(page, directory, 'mobile', opts.captures.includes(name));
  if (name === 'sustained-pilot') {
    assert.equal(await page.locator('#results').count(), 1, 'sustained results section retained');
    assert.equal(await page.locator('#supplemental-quality').count(), 1, 'supplemental quality section retained');
    if (opts.captures.includes(name)) {
      await page.locator('#results').scrollIntoViewIfNeeded();
      await capture(page, directory, 'mobile-results', true);
      await page.setViewportSize({width: 1280, height: 980});
      await page.locator('#supplemental-quality').scrollIntoViewIfNeeded();
      await capture(page, directory, 'desktop-supplemental', true);
    }
  }
  const final = opts.requireFinal || (name === 'pilot' ? process.env.REQUIRE_PILOT_FINAL === '1' : process.env.REQUIRE_FINAL === '1');
  await contentChecks(page, name, final);
  await back.click();
  await page.getByRole('heading', {name: 'Gossip × Agents', exact: true}).waitFor();
}

async function reportContract(page, base, original, directory, opts) {
  await page.goto(base);
  await page.getByRole('heading', {name: 'Gossip × Agents', exact: true}).waitFor();
  assert.equal(await page.getByRole('heading', {name: 'Work plan', exact: true}).count(), 1);
  assert.equal(await page.locator('article.task').count(), original.tasks.filter(t => (t.inScope !== false && t.status !== 'complete') || t.tier !== 'archive').length);
  await fit(page, 'desktop report');
  await capture(page, directory, 'desktop', opts.captures.includes('report'));
  await page.setViewportSize({width: 390, height: 844});
  await fit(page, 'mobile report');
  await capture(page, directory, 'mobile', opts.captures.includes('report'));
  await page.getByRole('link', {name: /Open archive/}).click();
  await page.getByRole('heading', {name: 'Archive', exact: true}).waitFor();
  await page.getByRole('link', {name: '← Return to current work'}).click();
  await page.getByRole('heading', {name: 'Work plan', exact: true}).waitFor();
  const latest = original.deliverables.at(-1);
  if (latest) {
    const target = new URL(latest.url);
    assert.equal(target.origin, new URL(base).origin, 'immutable snapshot belongs to report server');
    await page.goto(target.href);
    const back = page.locator('a.return').filter({hasText: /^Progress Report$/});
    assert.equal(await back.isVisible(), true, 'immutable snapshot return link');
    assert.equal(await back.getAttribute('href'), base, 'immutable snapshot trusted return target');
    await back.click();
    await page.getByRole('heading', {name: 'Gossip × Agents', exact: true}).waitFor();
  }
  // These request fixtures exercise exact UI payloads and reload behavior. Real
  // persistence/locking is covered by the independent 13-test report unit lane.
  const fixture = structuredClone(original);
  fixture.revision = 100000;
  fixture.cards = [{id: 'fixture-card', itemId: 'research', version: 'fixture-v1', revision: 1, taskId: 'blueprint', title: 'Fixture memo', change: 'Isolated UI fixture', inspect: ['Inspect the claim'], url: base + 'research.html?progress-report', at: new Date().toISOString(), tier: 'current', eventIds: ['fixture-event']}];
  fixture.feedback = [];
  fixture.checkpoints = {};
  const posts = [];
  await page.route('**/api/state', route => route.fulfill({json: fixture}));
  await page.route('**/api/feedback', async route => {
    posts.push({type: 'feedback', body: route.request().postDataJSON()});
    await route.fulfill({json: {ok: true}});
  });
  await page.route('**/api/review', async route => {
    const body = route.request().postDataJSON();
    posts.push({type: 'review', body});
    fixture.checkpoints = {'local-viewer': {cards: {'fixture-card': body}}};
    fixture.revision++;
    await route.fulfill({json: {ok: true}});
  });
  await page.goto(base);
  await page.getByRole('heading', {name: 'Fixture memo'}).waitFor();
  await page.getByText('Feedback on this version', {exact: true}).click();
  await page.getByRole('textbox', {name: 'Feedback on Fixture memo version fixture-v1'}).fill('Check whether this claim is measured.');
  await page.getByRole('button', {name: 'Save feedback'}).click();
  await page.getByRole('status').filter({hasText: 'Feedback saved'}).waitFor();
  assert.deepEqual(posts[0], {type: 'feedback', body: {cardId: 'fixture-card', text: 'Check whether this claim is measured.'}});
  await page.getByRole('button', {name: 'Mark reviewed', exact: true}).click();
  await page.getByRole('status').filter({hasText: 'Review saved'}).waitFor();
  assert.deepEqual(posts[1], {type: 'review', body: {cardId: 'fixture-card', contentRevision: 1, eventIds: ['fixture-event']}});
  assert.equal(await page.getByRole('heading', {name: 'Fixture memo'}).isVisible(), false, 'reviewed card collapses');
  await page.reload();
  await page.getByRole('heading', {name: 'Work plan', exact: true}).waitFor();
  assert.equal(await page.getByRole('heading', {name: 'Fixture memo'}).isVisible(), false, 'checkpoint survives reload');
}

async function main(argv = process.argv.slice(2)) {
  const opts = options(argv);
  const locator = JSON.parse(fs.readFileSync(opts.locator, 'utf8'));
  const started = Date.now();
  const runId = `${new Date().toISOString().replace(/[:.]/g, '-')}-${crypto.randomUUID().slice(0, 8)}`;
  const output = path.resolve(opts.output || path.join(ROOT, 'runs', 'verification', 'browser', runId, 'receipt.json'));
  fs.mkdirSync(path.dirname(output), {recursive: true});
  // Reserve exclusively before any browser/server starts. O_EXCL also rejects
  // existing symlinks; the final receipt is written through this same handle.
  const outputHandle = fs.openSync(output, 'wx');
  const artifacts = path.join(path.dirname(output), `browser-${runId}`);
  fs.mkdirSync(artifacts);
  const receipt = {schemaVersion: 1, runId, passed: false, pages: opts.pages, startedAt: new Date(started).toISOString(),
    serverOwnership: opts.fixture ? 'owned-fixture' : 'borrowed', browserLaunches: 0, results: [], artifacts,
    snapshots: opts.captures, finalRequired: opts.requireFinal, physicalExecution: true};
  const resources = {};
  let persisted = false;
  function persistReceipt() {
    if (persisted) return;
    receipt.passed = receipt.passed && !receipt.cancelled;
    receipt.borrowedServerStopped = false;
    receipt.notRunPages = opts.pages.filter(name => !receipt.results.some(result => result.page === name));
    receipt.elapsedSeconds = (Date.now() - started) / 1000;
    receipt.finishedAt = new Date().toISOString();
    fs.writeFileSync(outputHandle, JSON.stringify(receipt, null, 2) + '\n');
    fs.closeSync(outputHandle);
    persisted = true;
    console.log(JSON.stringify({...receipt, receiptPath: output}));
  }
  const lifecycle = createLifecycle(resources, receipt, persistReceipt);
  try {
    receipt.verificationBindings = Object.fromEntries(['run.cjs', 'lifecycle.cjs', 'check_syntax.cjs', 'fixture_server.py', 'package.json', 'package-lock.json']
      .map(name => [name, digest(fs.readFileSync(path.join(__dirname, name)))]));
    receipt.reportImplementationSha256 = digest(fs.readFileSync(path.join(locator.reportWorkspace, 'report.py')));
    const playwright = require('playwright');
    receipt.playwright = require('playwright/package.json').version;
    assert.equal(receipt.playwright, '1.62.1', 'pinned Playwright version');
    const browserManifest = JSON.parse(fs.readFileSync(path.join(path.dirname(require.resolve('playwright-core/package.json')), 'browsers.json'), 'utf8'));
    const chromium = browserManifest.browsers.find(browser => browser.name === 'chromium');
    assert.equal(chromium.revision, '1234', 'pinned Chromium revision');
    receipt.chromiumRevision = chromium.revision;
    const fixture = opts.fixture ? await fixtureServer(opts, artifacts, resources) : null;
    lifecycle.check();
    receipt.fixtureServerPid = fixture ? fixture.pid : null;
    const base = new URL(fixture ? fixture.url : opts.url || process.env.REPORT_URL || locator.reportUrl).href;
    assert.equal(new URL(base).pathname, '/', 'report URL must use the server root');
    receipt.url = base;
    resources.api = await playwright.request.newContext({timeout: 10000});
    lifecycle.check();
    const {state, bindings, sourceBindings} = await identity(resources.api, base, locator, fixture, opts.pages);
    lifecycle.check();
    receipt.fileBindings = bindings;
    receipt.sourceBindings = sourceBindings;
    receipt.reportStateSha256 = digest(JSON.stringify(state));
    receipt.serverIdentity = {projectId: state.projectId, canonicalWorkspace: state.handoff.reportWorkspace, servingDirectory: fixture ? fixture.directory : locator.reportWorkspace};
    resources.browser = await playwright.chromium.launch({headless: true, timeout: 30000,
      handleSIGINT: false, handleSIGTERM: false, handleSIGHUP: false});
    receipt.browserLaunches++;
    lifecycle.check();
    receipt.browserVersion = resources.browser.version();
    assert.equal(receipt.browserVersion, chromium.browserVersion, 'actual Chromium binary matches pinned browser manifest');
    for (const name of opts.pages) {
      lifecycle.check();
      receipt.currentPage = name;
      const pageStarted = Date.now();
      const directory = path.join(artifacts, name);
      fs.mkdirSync(directory);
      const context = await resources.browser.newContext({viewport: {width: 1280, height: 980}, serviceWorkers: 'block'});
      lifecycle.check();
      context.setDefaultTimeout(10000);
      context.setDefaultNavigationTimeout(15000);
      let page;
      let failed = false;
      const errors = [];
      const forbiddenWrites = [];
      try {
        await context.tracing.start({screenshots: true, snapshots: true, sources: true});
        await context.route('**/*', async route => {
          if (!['GET', 'HEAD'].includes(route.request().method())) {
            forbiddenWrites.push({url: route.request().url(), method: route.request().method()});
            return route.abort('blockedbyclient');
          }
          return route.continue();
        });
        // Keep one consistent read-only snapshot even if another agent updates
        // progress during the batch. Interactions override it in their own page.
        await context.route('**/api/state', route => route.fulfill({json: state}));
        page = await context.newPage();
        page.on('pageerror', error => errors.push(error.message));
        if (name === 'report') await reportContract(page, base, state, directory, opts);
        else await pageContract(page, name, base, directory, opts);
        lifecycle.check();
        assert.deepEqual(errors, [], 'no JavaScript runtime errors');
        assert.deepEqual(forbiddenWrites, [], 'no unmocked writes to report server');
        receipt.results.push({page: name, passed: true, elapsedSeconds: (Date.now() - pageStarted) / 1000});
      } catch (error) {
        failed = true;
        if (page && !lifecycle.cancelled) await page.screenshot({path: path.join(directory, 'failure.png'), fullPage: true}).catch(() => {});
        receipt.results.push({page: name, passed: false, error: error.stack, elapsedSeconds: (Date.now() - pageStarted) / 1000});
      } finally {
        await context.tracing.stop(failed ? {path: path.join(directory, 'failure-trace.zip')} : {}).catch(() => {});
        await context.close();
      }
    }
    delete receipt.currentPage;
    lifecycle.check();
    const finalIdentity = await identity(resources.api, base, locator, fixture, opts.pages);
    lifecycle.check();
    assert.deepEqual(finalIdentity.bindings, bindings, 'served files unchanged during browser execution');
    assert.deepEqual(finalIdentity.sourceBindings, sourceBindings, 'source files unchanged during browser execution');
    for (const [name, expected] of Object.entries(receipt.verificationBindings)) assert.equal(digest(fs.readFileSync(path.join(__dirname, name))), expected, `${name} unchanged during browser execution`);
    assert.equal(digest(fs.readFileSync(path.join(locator.reportWorkspace, 'report.py'))), receipt.reportImplementationSha256, 'report implementation unchanged during browser execution');
    const after = finalIdentity.state;
    assert.deepEqual(after.feedback, state.feedback, 'canonical feedback preserved (concurrent external review changes invalidate this assertion)');
    assert.deepEqual(after.checkpoints, state.checkpoints, 'canonical review checkpoints preserved');
    receipt.passed = receipt.results.length === opts.pages.length && receipt.results.every(result => result.passed);
    receipt.canonicalReviewPreserved = true;
  } catch (error) { receipt.error = error.stack; }
  finally {
    await lifecycle.cleanup();
    receipt.passed = receipt.passed && !lifecycle.cancelled;
    if (receipt.passed && opts.fixture) fs.rmSync(path.join(artifacts, 'fixture'), {recursive: true});
    try { persistReceipt(); } finally { lifecycle.dispose(); }
    if (lifecycle.forceExitNeeded) process.exit(process.exitCode || 1);
  }
  if (!receipt.passed && !process.exitCode) process.exitCode = 1;
  return receipt;
}

module.exports = {main};
if (require.main === module) main().catch(error => {console.error(error.stack); process.exitCode = 1;});
