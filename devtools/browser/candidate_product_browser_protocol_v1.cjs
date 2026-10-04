'use strict';
// Data-only bridge profile. No Docker access and no product oracle in this module.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const PROTOCOL = 'candidate-product-browser-ipc-v1';
const ORIGIN = 'http://127.0.0.1:8765';
const MESSAGE_LIMIT = 8 * 1024 * 1024;
const sha = raw => crypto.createHash('sha256').update(raw).digest('hex');

function requestPolicy(urlValue, method, rawBody, headerPairs) {
  assert.equal(typeof urlValue, 'string');
  const url = new URL(urlValue);
  assert.equal(url.origin, ORIGIN); assert.equal(url.username, ''); assert.equal(url.password, '');
  assert.equal(url.hash, ''); assert(['GET', 'POST'].includes(method));
  const target = url.pathname + url.search;
  assert(Buffer.byteLength(target) <= 4096 && /^\/[!-~]*$/.test(target) && !/[\\#]/.test(target));
  const body = rawBody === null ? Buffer.alloc(0) : rawBody;
  assert(Buffer.isBuffer(body) && body.length <= 65536);
  assert(Array.isArray(headerPairs) && headerPairs.length <= 128);
  const seen = new Set(), headers = [];
  for (const pair of headerPairs) {
    assert(Array.isArray(pair) && pair.length === 2 && pair.every(x => typeof x === 'string'));
    const [name, value] = pair, key = name.toLowerCase();
    assert(/^[!#$%&'*+.^_`|~0-9a-z-]+$/.test(key));
    assert(/^[\t\x20-\x7e]*$/.test(value)); assert(!seen.has(key)); seen.add(key);
    assert(!['cookie', 'authorization', 'proxy-authorization', 'transfer-encoding', 'trailer', 'upgrade', 'expect'].includes(key));
    // The trusted HTTP/1.1 bridge supplies these three framing fields. This
    // declared projection confers no native Chromium network/framing credit.
    if (!['host', 'connection', 'content-length'].includes(key)) headers.push([name, value]);
  }
  if (method === 'GET') assert.equal(body.length, 0);
  if (method === 'POST') {
    assert(target.startsWith('/api/'));
    const type = headers.find(x => x[0].toLowerCase() === 'content-type');
    assert(type && /^application\/json(?:\s*;|$)/i.test(type[1]));
  }
  return {url: urlValue, method, target, headers, body_b64: body.toString('base64'), body_sha256: sha(body)};
}

function responsePolicy(value) {
  assert(value && value.available === true && Number.isInteger(value.status));
  assert(value.status >= 200 && value.status <= 599 && !(value.status >= 300 && value.status < 400));
  assert(Array.isArray(value.headers));
  const headers = Object.create(null);
  for (const [name, content] of value.headers) {
    assert.equal(typeof name, 'string'); assert.equal(typeof content, 'string');
    const key = name.toLowerCase();
    assert(!Object.hasOwn(headers, key));
    assert(!['set-cookie', 'location', 'content-disposition', 'content-encoding'].includes(key));
    assert(!['connection', 'content-length', 'transfer-encoding', 'trailer', 'upgrade'].includes(key));
    headers[key] = content;
  }
  assert.equal(typeof value.body_b64, 'string');
  const body = Buffer.from(value.body_b64, 'base64');
  assert.equal(body.toString('base64'), value.body_b64); assert(body.length <= 4194304);
  assert.equal(sha(body), value.body_sha256);
  return {status: value.status, headers, body};
}

function packageFiles(root) {
  const rows = [];
  function visit(directory, relative = '') {
    for (const name of fs.readdirSync(directory).sort()) {
      if (name === 'node_modules' || name === '.cache') continue;
      const absolute = path.join(directory, name), key = relative ? relative + '/' + name : name;
      const st = fs.lstatSync(absolute);
      assert(!st.isSymbolicLink(), 'runtime package symlink unsupported');
      if (st.isDirectory()) visit(absolute, key);
      else if (st.isFile()) {
        assert(rows.length < 8192 && st.size <= 64 * 1024 * 1024);
        rows.push([key, st.size, sha(fs.readFileSync(absolute))]);
      } else throw Error('runtime package special file unsupported');
    }
  }
  visit(root);
  return sha(Buffer.from(JSON.stringify(rows)));
}

function runtime() {
  const playwright = require('playwright');
  const root = path.dirname(require.resolve('playwright/package.json'));
  const core = path.dirname(require.resolve('playwright-core/package.json'));
  const version = require('playwright/package.json').version;
  assert.equal(version, '1.62.1');
  const manifestBytes = fs.readFileSync(path.join(core, 'browsers.json'));
  const manifest = JSON.parse(manifestBytes);
  const chromium = manifest.browsers.find(x => x.name === 'chromium');
  assert.equal(chromium.revision, '1234');
  const executable = playwright.chromium.executablePath();
  return {protocol: PROTOCOL, node_version: process.version, node_executable: process.execPath,
    node_sha256: sha(fs.readFileSync(process.execPath)), playwright_version: version,
    playwright_package_sha256: packageFiles(root), playwright_core_package_sha256: packageFiles(core),
    chromium_revision: chromium.revision, browser_version: chromium.browserVersion,
    browser_executable: executable, browser_sha256: sha(fs.readFileSync(executable)),
    browsers_manifest_sha256: sha(manifestBytes), protocol_sha256: sha(fs.readFileSync(__filename))};
}

function jobIdentityPattern(identifier) {
  assert(/^[A-Za-z0-9_-]{1,64}$/.test(identifier));
  return new RegExp('(?:^|[^A-Za-z0-9_-])' + identifier + '(?=$|[^A-Za-z0-9_-])');
}

class LineDecoder {
  constructor(limit = MESSAGE_LIMIT) { this.limit = limit; this.pending = Buffer.alloc(0); }
  feed(chunk) {
    assert(Buffer.isBuffer(chunk));
    this.pending = Buffer.concat([this.pending, chunk]);
    const result = [];
    while (true) {
      const end = this.pending.indexOf(10);
      if (end < 0) break;
      assert(end <= this.limit && end > 0);
      result.push(this.pending.subarray(0, end)); this.pending = this.pending.subarray(end + 1);
    }
    assert(this.pending.length <= this.limit); return result;
  }
  eof() { assert.equal(this.pending.length, 0, 'incomplete IPC line'); }
}
module.exports = {PROTOCOL, ORIGIN, MESSAGE_LIMIT, sha, requestPolicy, responsePolicy, runtime, jobIdentityPattern, LineDecoder};
if (require.main === module) {
  assert.deepEqual(process.argv.slice(2), ['--probe']);
  process.stdout.write(JSON.stringify(runtime()) + '\n');
}
