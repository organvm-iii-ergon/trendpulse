/** Public status must never enumerate KV. Only an existing cron may refresh it. */
interface SnapshotStore {
  get(key: string): Promise<string | null>;
  put(key: string, value: string): Promise<unknown>;
}
interface Context { waitUntil(promise: Promise<unknown>): void }
interface Event { scheduledTime: number }
interface Worker<E, S extends Event, C extends Context> {
  fetch(request: Request, env: E, ctx: C): Response | Promise<Response>;
  scheduled(event: S, env: E, ctx: C): void | Promise<void>;
}
interface Options<E> {
  name: string;
  store(env: E): SnapshotStore;
  maxAgeMs: number;
  now?: () => number;
}
interface Snapshot {
  version: 1;
  observedAt: number;
  scheduledTime: number;
  body: Record<string, unknown>;
}
export const STATUS_SNAPSHOT_KEY = '_ops:status-snapshot:v1';

function decode(raw: string | null): Snapshot | null {
  if (!raw) return null;
  try {
    const value = JSON.parse(raw) as Snapshot;
    if (value?.version !== 1 || !Number.isFinite(value.observedAt)
      || !Number.isFinite(new Date(value.observedAt).getTime())
      || !Number.isFinite(value.scheduledTime) || !value.body
      || typeof value.body !== 'object' || Array.isArray(value.body)) return null;
    return value;
  } catch { return null; }
}

export function withStatusSnapshot<E, S extends Event, C extends Context>(
  worker: Worker<E, S, C>, options: Options<E>,
): Worker<E, S, C> {
  const now = options.now ?? Date.now;
  const headers = { 'cache-control': 'no-store', 'retry-after': '300' };

  async function refresh(event: S, env: E, ctx: C): Promise<void> {
    // Existing schedules all fire on minute zero. EdgarFlash additionally fires
    // every minute: restrict only metrics, NOT its one-minute filing delivery.
    if (!Number.isFinite(event.scheduledTime)
      || new Date(event.scheduledTime).getUTCMinutes() !== 0) return;
    const store = options.store(env);
    const previous = decode(await store.get(STATUS_SNAPSHOT_KEY));
    if (previous && previous.scheduledTime >= event.scheduledTime) return;
    const response = await worker.fetch(new Request('https://status.internal/api/status'), env, ctx);
    if (!response.ok) throw new Error('status_snapshot_refresh_failed');
    const body: unknown = await response.json();
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      throw new Error('status_snapshot_invalid_response');
    }
    const snapshot = JSON.stringify({
      version: 1, observedAt: now(), scheduledTime: event.scheduledTime, body,
    });
    if (snapshot.length > 65536) throw new Error('status_snapshot_too_large');
    // No TTL: a failed refresh must not erase the last known evidence. Readers
    // report stale snapshots as 503, rather than claiming old metrics are live.
    await store.put(STATUS_SNAPSHOT_KEY, snapshot);
  }

  return {
    async fetch(request, env, ctx) {
      const path = new URL(request.url).pathname;
      if (path !== '/api/status' && path !== '/healthz') return worker.fetch(request, env, ctx);
      if (request.method !== 'GET' && request.method !== 'HEAD') {
        return new Response(null, { status: 405, headers: { allow: 'GET, HEAD' } });
      }
      if (path === '/healthz') {
        const response = Response.json({
          name: options.name, status: 'alive', check: 'liveness_only',
          revision: 'kv-list-status-snapshot-v1',
        }, { headers: { 'cache-control': 'no-store' } });
        return request.method === 'HEAD' ? new Response(null, response) : response;
      }
      let snapshot: Snapshot | null;
      try { snapshot = decode(await options.store(env).get(STATUS_SNAPSHOT_KEY)); }
      catch { snapshot = null; }
      if (!snapshot) {
        const response = Response.json({
          error: 'status_snapshot_unavailable',
          detail: 'Awaiting a scheduled metrics snapshot. /healthz checks liveness only.',
        }, { status: 503, headers });
        return request.method === 'HEAD' ? new Response(null, response) : response;
      }
      const ageMs = now() - snapshot.observedAt;
      const stale = ageMs < 0 || ageMs > options.maxAgeMs;
      const response = Response.json({
        ...snapshot.body,
        _status_snapshot: {
          observed_at: new Date(snapshot.observedAt).toISOString(),
          age_seconds: Math.max(0, Math.floor(ageMs / 1000)), stale,
        },
      }, { status: stale ? 503 : 200, headers });
      return request.method === 'HEAD' ? new Response(null, response) : response;
    },
    async scheduled(event, env, ctx) {
      if (!Number.isFinite(event.scheduledTime)
        || new Date(event.scheduledTime).getUTCMinutes() !== 0) {
        await worker.scheduled(event, env, ctx);
        return;
      }
      const pending: Promise<unknown>[] = [];
      const productContext = new Proxy(ctx, {
        get(target, property) {
          if (property === 'waitUntil') return (promise: Promise<unknown>) => {
            pending.push(promise);
            target.waitUntil(promise);
          };
          const value = Reflect.get(target, property, target);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      try {
        await worker.scheduled(event, env, productContext);
      } finally {
        // Observe after the product's registered background work settles. Its
        // failures stay registered with the real context; metrics never mask
        // or suppress delivery, and a failed refresh is visible independently.
        ctx.waitUntil(Promise.allSettled(pending).then(() => refresh(event, env, ctx)));
      }
    },
  };
}
