// Own cancellation before Playwright starts. Browser launch disables its signal
// handlers, while Playwright retains its process-exit hook as a final kill guard.
const {once} = require('node:events');

async function stopOwnedServer(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, 'exit');
  child.kill('SIGTERM');
  const timer = setTimeout(() => child.kill('SIGKILL'), 2000);
  try { await exited; } finally { clearTimeout(timer); }
}

function createLifecycle(resources, receipt, persist, settings = {}) {
  const target = settings.process || process;
  const cleanupTimeoutMs = settings.cleanupTimeoutMs ?? 4000;
  const cancellationTimeoutMs = settings.cancellationTimeoutMs ?? 7000;
  const closing = new Map();
  let cancellation = null;
  let deadline;
  let forcedExitNeeded = false;
  let disposed = false;
  receipt.cleanup = {};
  const actions = {browser: resource => resource.close(), api: resource => resource.dispose(), ownedServer: stopOwnedServer};
  const resourceNames = {browser: 'browser', api: 'api', ownedServer: 'server'};

  function check() {
    if (cancellation) throw Error(`Browser batch cancelled by ${cancellation}`);
  }

  async function closeOne(name, resource) {
    const prior = closing.get(name);
    if (prior?.resource === resource) return prior.promise;
    receipt.cleanup[name] = 'in progress';
    const promise = (async () => {
      let timer;
      try {
        await Promise.race([
          Promise.resolve().then(() => actions[name](resource)),
          new Promise((_, reject) => {timer = setTimeout(() => {
            forcedExitNeeded = true;
            reject(Error(`${name} cleanup exceeded ${cleanupTimeoutMs}ms`));
          }, cleanupTimeoutMs);}),
        ]);
        receipt.cleanup[name] = 'complete';
      } catch (error) {
        forcedExitNeeded = true;
        receipt.cleanup[name] = String(error);
        receipt.passed = false;
      } finally { clearTimeout(timer); }
    })();
    closing.set(name, {resource, promise});
    return promise;
  }

  async function cleanup() {
    await Promise.all(Object.entries(resourceNames).map(([name, key]) => {
      const resource = resources[key];
      if (resource) return closeOne(name, resource);
      // Resource creation may still be in flight at cancellation. A later
      // cleanup call notices the newly acquired resource and closes it too.
      if (!(name in receipt.cleanup)) receipt.cleanup[name] = 'complete';
      return undefined;
    }));
  }

  function cancel(signal) {
    if (cancellation || disposed) return;
    cancellation = signal;
    target.exitCode = signal === 'SIGINT' ? 130 : signal === 'SIGHUP' ? 129 : 143;
    receipt.cancelled = true;
    receipt.cancellationSignal = signal;
    receipt.passed = false;
    receipt.error = `Browser batch cancelled by ${signal}`;
    // Bound even an in-flight launch or unresponsive third-party close call.
    // The final process exit invokes Playwright's registered browser kill hook.
    deadline = setTimeout(() => {
      receipt.passed = false;
      receipt.cancellationDeadlineExceeded = true;
      const child = resources.server;
      try {
        if (child && child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
      } catch (error) { receipt.cleanup.ownedServer = String(error); }
      try { persist(); } finally { target.exit(target.exitCode); }
    }, cancellationTimeoutMs);
    void cleanup();
  }

  const handlers = Object.fromEntries(['SIGINT', 'SIGTERM', 'SIGHUP'].map(signal => [signal, () => cancel(signal)]));
  for (const [signal, handler] of Object.entries(handlers)) target.on(signal, handler);
  return {
    check,
    cleanup,
    get cancelled() { return cancellation !== null; },
    get forceExitNeeded() { return forcedExitNeeded; },
    dispose() {
      disposed = true;
      clearTimeout(deadline);
      for (const [signal, handler] of Object.entries(handlers)) target.off(signal, handler);
    },
  };
}

module.exports = {createLifecycle, stopOwnedServer};
