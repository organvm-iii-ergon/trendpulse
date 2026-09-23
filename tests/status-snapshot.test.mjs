import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import ts from 'typescript';

const source = readFileSync(new URL('../src/status-snapshot.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
}).outputText;
const { withStatusSnapshot, STATUS_SNAPSHOT_KEY } = await import(
  `data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`,
);
const HOUR = 3600000;
const START = Date.UTC(2026, 8, 23);
function fixture() {
  const counts = { get: 0, put: 0, list: 0, cron: 0, delegated: 0 };
  const values = new Map();
  let now = START;
  const store = {
    async get(key) { counts.get++; return values.get(key) ?? null; },
    async put(key, value) { counts.put++; values.set(key, value); },
  };
  const context = { pending: [], waitUntil(promise) { this.pending.push(promise); } };
  const env = { store };
  const worker = {
    async fetch(request, receivedEnv, receivedCtx) {
      assert.equal(receivedEnv, env);
      assert.equal(receivedCtx, context);
      if (new URL(request.url).pathname === '/api/status') {
        counts.list++;
        return Response.json({ name: 'Fixture', subscriber_count: 7 });
      }
      counts.delegated++;
      return new Response('unchanged', { status: 401 });
    },
    async scheduled() { counts.cron++; },
  };
  const options = { name: 'Fixture', store: e => e.store, maxAgeMs: 2 * HOUR, now: () => now };
  let handler = withStatusSnapshot(worker, options);
  return {
    counts, values, store, worker, env, context,
    setNow(value) { now = value; },
    coldStart() { handler = withStatusSnapshot(worker, options); },
    request(path = '/api/status', method = 'GET') {
      return handler.fetch(new Request(`https://fixture.test${path}`, { method }), env, context);
    },
    async tick(time) {
      now = time;
      await handler.scheduled({ scheduledTime: time }, env, context);
      await Promise.all(context.pending.splice(0));
    },
  };
}
test('10,000 cold liveness requests perform zero storage operations', async () => {
  const f = fixture();
  for (let i = 0; i < 10000; i++) {
    f.coldStart();
    const response = await f.request('/healthz');
    assert.equal(response.status, 200);
  }
  assert.deepEqual(f.counts, { get: 0, put: 0, list: 0, cron: 0, delegated: 0 });
});
test('missing/corrupt/failed snapshots never cause request-time scans or writes', async () => {
  const f = fixture();
  for (const raw of [null, '{', '{}', '{"version":1,"body":[]}']) {
    if (raw === null) f.values.clear(); else f.values.set(STATUS_SNAPSHOT_KEY, raw);
    assert.equal((await f.request('/api/status?refresh=true')).status, 503);
  }
  f.store.get = async () => { throw new Error('quota'); };
  assert.equal((await f.request()).status, 503);
  assert.equal(f.counts.list, 0);
  assert.equal(f.counts.put, 0);
});
test('one day of one-minute cron preserves 1,440 jobs but refreshes only 24 times', async () => {
  const f = fixture();
  for (let minute = 0; minute < 1440; minute++) {
    f.coldStart();
    await f.tick(START + minute * 60000);
    for (let poll = 0; poll < 4; poll++) assert.equal((await f.request()).status, 200);
  }
  assert.equal(f.counts.cron, 1440);
  assert.equal(f.counts.list, 24);
  assert.equal(f.counts.put, 24);
});
test('four-hour and daily schedules are preserved without adding cron triggers', async () => {
  for (const hours of [[1, 5, 9, 13, 17, 21], [12]]) {
    const f = fixture();
    for (const hour of hours) await f.tick(START + hour * HOUR);
    assert.equal(f.counts.cron, hours.length);
    assert.equal(f.counts.list, hours.length);
    assert.equal(f.counts.put, hours.length);
  }
});
test('fresh metrics retain the existing response fields with honest sample time', async () => {
  const f = fixture();
  await f.tick(START);
  f.setNow(START + 60000);
  const body = await (await f.request()).json();
  assert.equal(body.subscriber_count, 7);
  assert.deepEqual(body._status_snapshot, {
    observed_at: new Date(START).toISOString(), age_seconds: 60, stale: false,
  });
});
test('stale/future snapshots are 503 and do not refresh on demand', async () => {
  const f = fixture();
  await f.tick(START);
  for (const time of [START + 2 * HOUR + 1, START - 1]) {
    f.setNow(time);
    assert.equal((await f.request()).status, 503);
  }
  assert.equal(f.counts.list, 1);
  assert.equal(f.counts.put, 1);
});
test('same/older scheduled event does not rewrite an already observed snapshot', async () => {
  const f = fixture();
  await f.tick(START);
  f.coldStart();
  await f.tick(START);
  await f.tick(START - HOUR);
  assert.equal(f.counts.cron, 3);
  assert.equal(f.counts.list, 1);
  assert.equal(f.counts.put, 1);
});
test('refresh failures remain visible without suppressing the product cron', async () => {
  const f = fixture();
  f.store.put = async () => { throw new Error('write quota exceeded'); };
  await assert.rejects(f.tick(START), /write quota exceeded/);
  assert.equal(f.counts.cron, 1);
  assert.equal((await f.request()).status, 503);
});
test('non-status paths preserve auth responses and exact environment/context', async () => {
  const f = fixture();
  const response = await f.request('/api/realtime');
  assert.equal(response.status, 401);
  assert.equal(await response.text(), 'unchanged');
  assert.equal(f.counts.delegated, 1);
  assert.equal(f.counts.get, 0);
});
test('HEAD has no body, unsafe status methods are rejected without storage', async () => {
  const f = fixture();
  const response = await f.request('/healthz', 'HEAD');
  assert.equal(response.status, 200);
  assert.equal(await response.text(), '');
  assert.equal((await f.request('/api/status', 'POST')).status, 405);
  assert.equal(f.counts.get, 0);
});

test('metrics sample after scheduled background writes settle', async () => {
  const f = fixture();
  let completed = false;
  const fetch = f.worker.fetch;
  f.worker.fetch = (...args) => {
    assert.equal(completed, true);
    return fetch(...args);
  };
  f.worker.scheduled = async (_event, _env, ctx) => {
    f.counts.cron++;
    ctx.waitUntil(new Promise(resolve => setTimeout(() => { completed = true; resolve(); }, 1)));
  };
  await f.tick(START);
  assert.equal(f.counts.list, 1);
});
test('finite but invalid dates are treated as corrupt snapshots', async () => {
  const f = fixture();
  f.values.set(STATUS_SNAPSHOT_KEY, JSON.stringify({
    version: 1, observedAt: 1e20, scheduledTime: START, body: {},
  }));
  assert.equal((await f.request()).status, 503);
});
