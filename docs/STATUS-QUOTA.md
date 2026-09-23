# Quota-safe status endpoints

Wrangler deploys `src/runtime.ts`; `src/index.ts` remains the unchanged product
implementation. `/healthz` checks only Worker liveness and performs zero KV
operations. `/api/status` reads one existing-namespace snapshot key and never
lists, writes, or refreshes KV in response to HTTP traffic. Original status fields
are preserved, with `_status_snapshot.observed_at`, `age_seconds`, and `stale`.
Missing, corrupt, unreadable, future-dated, or stale snapshots return 503; a
successful liveness check is not proof that ingestion or storage is healthy.

Only the existing scheduled handler refreshes metrics, after its registered
background work settles, and only on minute zero. Product scheduling and business
handlers are unchanged. With one invocation per configured cron event this adds:
EdgarFlash 24 status scans and 24 writes/day; TrendPulse 6 scans and 6 writes/day;
VulnPulse 2 lists per daily scan and 1 write/day. These are status-path budgets,
NOT account-wide usage totals or hard distributed exactly-once guarantees. KV
is eventually consistent, so concurrent duplicate events can repeat a refresh.
Business ingestion, delivery, paid/history queries, and other Workers remain
outside this budget and require separate authenticated usage attribution.

No new namespace, secret, paid plan, or cron trigger is required. A newly deployed
version may have no snapshot until its next eligible scheduled event (up to 1h,
4h, or 24h respectively); it must report 503 rather than scan on a public request.
Do not replace readiness checks with /healthz and call missing metrics healthy.

Validation: `node --test tests/status-snapshot.test.mjs` plus the existing lint,
typecheck, tests, and Wrangler dry build. Deploy only the reviewed commit using
the existing authorized Cloudflare deployment environment. Read back /healthz's
`kv-list-status-snapshot-v1` revision and a timestamped status snapshot after the
next eligible cron. Verify private Cloudflare analytics across a full UTC day:
this patch alone does not prove that the account-wide incident is resolved.
Rollback by redeploying the preceding verified version; no stored data is deleted.
