const assert = require('node:assert/strict');
const {EventEmitter, once} = require('node:events');
const test = require('node:test');
const {createLifecycle} = require('./lifecycle.cjs');

function fakeProcess() {
  const target = new EventEmitter();
  target.exit = code => target.emit('forced-exit', code);
  return target;
}

function fakeChild(kills, ignoreTerm = false) {
  const child = new EventEmitter();
  child.exitCode = null;
  child.signalCode = null;
  child.kill = signal => {
    kills.push(signal);
    if (signal === 'SIGTERM' && ignoreTerm) return true;
    child.signalCode = signal;
    child.emit('exit', null, signal);
    return true;
  };
  return child;
}

test('normal cleanup closes owned resources once and never operates borrowed servers', async () => {
  const calls = [];
  const receipt = {passed: true};
  const target = fakeProcess();
  const resources = {browser: {close: async () => calls.push('browser')}, api: {dispose: async () => calls.push('api')}, borrowedServer: {kill: () => assert.fail('borrowed server was stopped')}};
  const lifecycle = createLifecycle(resources, receipt, () => assert.fail('normal cleanup persisted early'), {process: target});
  await lifecycle.cleanup();
  await lifecycle.cleanup();
  lifecycle.dispose();
  assert.deepEqual(calls.sort(), ['api', 'browser']);
  assert.deepEqual(receipt.cleanup, {browser: 'complete', api: 'complete', ownedServer: 'complete'});
  assert.equal(receipt.passed, true);
  for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP']) assert.equal(target.listenerCount(signal), 0);
});

test('SIGTERM cancels, records failure, interrupts pending API and preserves signal exit code', async () => {
  const calls = [];
  const target = fakeProcess();
  const receipt = {passed: true};
  const lifecycle = createLifecycle({browser: {close: async () => calls.push('browser')}, api: {dispose: async () => calls.push('api')}}, receipt, () => {}, {process: target});
  target.emit('SIGTERM');
  await lifecycle.cleanup();
  assert.throws(() => lifecycle.check(), /SIGTERM/);
  lifecycle.dispose();
  assert.equal(target.exitCode, 143);
  assert.equal(receipt.cancelled, true);
  assert.equal(receipt.passed, false);
  assert.equal(receipt.cancellationSignal, 'SIGTERM');
  assert.deepEqual(calls.sort(), ['api', 'browser']);
});

test('SIGINT closes a browser acquired after cancellation without double cleanup', async () => {
  const resources = {};
  const target = fakeProcess();
  const receipt = {passed: true};
  let closed = 0;
  const lifecycle = createLifecycle(resources, receipt, () => {}, {process: target});
  target.emit('SIGINT');
  await lifecycle.cleanup();
  resources.browser = {close: async () => {closed++;}};
  assert.throws(() => lifecycle.check(), /SIGINT/);
  target.emit('SIGTERM');
  await lifecycle.cleanup();
  await lifecycle.cleanup();
  lifecycle.dispose();
  assert.equal(closed, 1);
  assert.equal(target.exitCode, 130);
});

test('owned fixture server is terminated and its exit awaited', async () => {
  const kills = [];
  const child = fakeChild(kills);
  const target = fakeProcess();
  const receipt = {passed: false};
  const lifecycle = createLifecycle({server: child}, receipt, () => {}, {process: target});
  target.emit('SIGTERM');
  await lifecycle.cleanup();
  lifecycle.dispose();
  assert.deepEqual(kills, ['SIGTERM']);
  assert.equal(child.signalCode, 'SIGTERM');
  assert.equal(receipt.cleanup.ownedServer, 'complete');
});

test('cleanup failure cannot prevent attempts to close other resources or become green', async () => {
  let apiClosed = false;
  const receipt = {passed: true};
  const lifecycle = createLifecycle({browser: {close: async () => {throw Error('close failed');}}, api: {dispose: async () => {apiClosed = true;}}}, receipt, () => {}, {process: fakeProcess()});
  await lifecycle.cleanup();
  lifecycle.dispose();
  assert.equal(apiClosed, true);
  assert.equal(receipt.passed, false);
  assert.match(receipt.cleanup.browser, /close failed/);
  assert.equal(lifecycle.forceExitNeeded, true);
});

test('cancellation deadline persists partial failure, force-kills only owned child and exits nonzero', async () => {
  const target = fakeProcess();
  const receipt = {passed: true};
  const kills = [];
  let persisted;
  const resources = {browser: {close: () => new Promise(() => {})}, server: fakeChild(kills, true), borrowedServer: {kill: () => assert.fail('borrowed server was stopped')}};
  const lifecycle = createLifecycle(resources, receipt, () => {persisted = structuredClone(receipt);}, {process: target, cleanupTimeoutMs: 10, cancellationTimeoutMs: 30});
  const exited = once(target, 'forced-exit');
  target.emit('SIGTERM');
  const [code] = await exited;
  lifecycle.dispose();
  assert.equal(code, 143);
  assert.equal(persisted.passed, false);
  assert.equal(persisted.cancellationDeadlineExceeded, true);
  assert.match(persisted.cleanup.browser, /exceeded/);
  assert.deepEqual(kills, ['SIGTERM', 'SIGKILL']);
  assert.equal(lifecycle.forceExitNeeded, true);
});
