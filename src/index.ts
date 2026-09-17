/**
 * TrendPulse — continuous market/news research worker.
 *
 * Cron pulls from multiple sources, stores raw items, periodically synthesizes
 * a digest using Workers AI. Operator's intel; also sold as paid product.
 *
 * Sources:
 *   - Hacker News top stories
 *   - GitHub trending (proxied via search API: most-starred recent)
 *   - arxiv cs.AI new submissions
 *   - Reddit /r/MachineLearning, /r/programming
 */

export interface Env {
  AI: any;
  ASSETS: Fetcher;
  TP_DATA: KVNamespace;
  TP_DIGEST: KVNamespace;
  USER_AGENT: string;
  // --- Monetization (Lemon Squeezy) ---
  CHECKOUT_URL?: string;        // public LS checkout link, surfaced to the UI
  // --- Custom email delivery (optional; webhook delivery needs no provider) ---
  RESEND_API_KEY?: string;      // secret — if set, email delivery is enabled
  FROM_EMAIL?: string;          // e.g. "TrendPulse <digest@yourdomain.com>"
}

export type Source = 'hn' | 'github' | 'arxiv' | 'reddit-ml' | 'reddit-prog';

export interface Item {
  source: Source;
  title: string;
  url: string;
  score?: number;
  comments?: number;
  collected_at: string;
  summary?: string;
}

export interface Digest {
  generated_at: string;
  date_label: string;       // YYYY-MM-DD
  themes: { name: string; rationale: string; example_titles: string[]; signal_strength: 'rising' | 'steady' | 'spike' }[];
  one_line: string;
  source_counts: Record<string, number>;
}

const RAW_KEY_PREFIX = 'raw:';
const DIGEST_KEY_PREFIX = 'digest:';
const LATEST_KEY = 'digest:latest';
const SERVICE_NAME = 'trendpulse';
const MAX_URL_LENGTH = 4096;
const MAX_BODY_CHARS = 16 * 1024;
const MAX_LICENSE_KEY_CHARS = 256;
const MAX_EMAIL_CHARS = 254;
const MAX_WEBHOOK_URL_CHARS = 2048;
const MAX_WEBHOOK_SECRET_CHARS = 256;
const MAX_KEYWORD_CHARS = 80;

type LogLevel = 'info' | 'warn' | 'error';

interface RequestLogContext {
  request_id: string;
  method: string;
  path: string;
  start_ms: number;
}

class HttpError extends Error {
  status: number;
  code: string;
  details?: unknown;
  headers?: Record<string, string>;

  constructor(status: number, code: string, message: string, details?: unknown, headers?: Record<string, string>) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
    this.code = code;
    this.details = details;
    this.headers = headers;
  }
}

function logEvent(level: LogLevel, event: string, fields: Record<string, unknown> = {}): void {
  const record = {
    service: SERVICE_NAME,
    level,
    event,
    ts: new Date().toISOString(),
    ...fields,
  };
  const line = JSON.stringify(record);
  if (level === 'error') console.error(line);
  else if (level === 'warn') console.warn(line);
  else console.log(line);
}

function errorFields(err: unknown, includeStack = true): Record<string, unknown> {
  if (err instanceof Error) {
    const fields: Record<string, unknown> = { error_name: err.name, error_message: err.message };
    if (includeStack && err.stack) fields.error_stack = err.stack;
    return fields;
  }
  return { error_message: String(err) };
}

function logError(event: string, err: unknown, fields: Record<string, unknown> = {}): void {
  logEvent('error', event, { ...fields, ...errorFields(err) });
}

function requestIdFrom(req: Request): string {
  const raw = req.headers.get('X-Request-ID') ?? req.headers.get('CF-Ray') ?? '';
  const trimmed = raw.trim();
  if (/^[A-Za-z0-9._:-]{1,128}$/.test(trimmed)) return trimmed;
  const randomUUID = (crypto as Crypto & { randomUUID?: () => string }).randomUUID;
  if (typeof randomUUID === 'function') return randomUUID.call(crypto);
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function pathFromUrl(rawUrl: string): string {
  try { return new URL(rawUrl).pathname; } catch { return 'unknown'; }
}

function makeRequestContext(req: Request): RequestLogContext {
  return {
    request_id: requestIdFrom(req),
    method: req.method,
    path: pathFromUrl(req.url),
    start_ms: Date.now(),
  };
}

function errorResponse(err: unknown, ctx: RequestLogContext): Response {
  if (err instanceof HttpError) {
    const body: Record<string, unknown> = {
      error: err.message,
      code: err.code,
      request_id: ctx.request_id,
    };
    if (err.details != null) body.detail = err.details;
    return Response.json(body, { status: err.status, headers: err.headers });
  }

  return Response.json({
    error: 'internal server error',
    code: 'internal_error',
    request_id: ctx.request_id,
  }, { status: 500 });
}

function assertMethod(req: Request, allowed: string[]): void {
  if (allowed.includes(req.method)) return;
  throw new HttpError(405, 'method_not_allowed', 'method not allowed', {
    allowed_methods: allowed,
  }, { Allow: allowed.join(', ') });
}

// === Monetization: Lemon Squeezy license gate ===
//
// Premium features (custom filtering + custom delivery) are gated behind an
// active Lemon Squeezy subscription. We use LS *license keys*: enable "License
// keys" on the subscription product, the subscriber gets a key, and they pass
// it to TrendPulse. We validate it against the public LS license API — no API
// secret needed, since the key itself is the credential. Subscription status
// (active / past_due cancel / expired) is reflected in the key status, so a
// short-cached validate call is enough to keep the gate live.

const LS_VALIDATE_URL = 'https://api.lemonsqueezy.com/v1/licenses/validate';
const LIC_CACHE_PREFIX = 'lic:';        // cached validation result (TP_DATA)
const LIC_CACHE_TTL = 10 * 60;          // 10 min — long enough to cut API calls, short enough to honor cancellations
const DELIVERY_PREFIX = 'delivery:';    // one registration per license (TP_DATA)
const CUSTOM_PREFIX = 'custom:';        // cached custom digests (TP_DIGEST)

type Tier = 'free' | 'pro' | 'team';

interface LicenseResult {
  valid: boolean;
  status: string;          // active | inactive | expired | disabled | unknown
  tier: Tier;
  email?: string;
  expires_at?: string | null;
  error?: string;
}

interface DeliveryRegistration {
  license_hash: string;
  tier: Tier;
  email?: string;
  webhook?: string;
  webhook_secret?: string;          // optional — HMAC-signs the webhook payload
  filters?: { keywords?: string[]; sources?: Source[] };
  created_at: string;
}

async function sha256Hex(s: string): Promise<string> {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
}

// Map a Lemon Squeezy variant name to an internal tier. Anything active is at
// least Pro; variants mentioning "team" unlock Team-only features (webhooks).
export function tierFromVariant(variantName: unknown): Tier {
  const v = String(variantName ?? '').toLowerCase();
  if (v.includes('team') || v.includes('enterprise') || v.includes('business')) return 'team';
  return 'pro';
}

async function validateLicense(env: Env, rawKey: string | null | undefined): Promise<LicenseResult> {
  const key = (rawKey ?? '').trim();
  if (!key) return { valid: false, status: 'missing', tier: 'free', error: 'no license key provided' };
  if (key.length > MAX_LICENSE_KEY_CHARS) {
    return { valid: false, status: 'invalid', tier: 'free', error: 'license key is too long' };
  }
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(key)) {
    return { valid: false, status: 'invalid', tier: 'free', error: 'license key contains invalid characters' };
  }

  const hash = await sha256Hex(key);
  const cacheKey = `${LIC_CACHE_PREFIX}${hash}`;
  const cached = await env.TP_DATA.get(cacheKey);
  if (cached) {
    try { return JSON.parse(cached) as LicenseResult; } catch {}
  }

  let result: LicenseResult;
  try {
    const resp = await fetch(LS_VALIDATE_URL, {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': env.USER_AGENT,
      },
      body: new URLSearchParams({ license_key: key }).toString(),
    });
    const data: any = await resp.json().catch(() => ({}));
    const status = String(data?.license_key?.status ?? 'unknown');
    const valid = data?.valid === true && status === 'active';
    result = {
      valid,
      status,
      tier: valid ? tierFromVariant(data?.meta?.variant_name) : 'free',
      email: data?.meta?.customer_email ? String(data.meta.customer_email) : undefined,
      expires_at: data?.license_key?.expires_at ?? null,
      error: valid ? undefined : String(data?.error ?? `license ${status}`),
    };
  } catch (err) {
    // LS unreachable — don't grant access, but don't cache the failure.
    logError('license.validation_failed', err);
    return { valid: false, status: 'unreachable', tier: 'free', error: 'license service unavailable' };
  }

  // Cache both positive and negative (definitive) results briefly.
  await env.TP_DATA.put(cacheKey, JSON.stringify(result), { expirationTtl: LIC_CACHE_TTL });
  return result;
}

// Pull a license key from the request: Authorization: Bearer, X-License-Key
// header, ?key= query param, or a JSON/form body field `license_key`.
export function licenseFromRequest(req: Request, url: URL, body?: any): string | null {
  const auth = req.headers.get('Authorization');
  if (auth && /^Bearer\s+/i.test(auth)) return auth.replace(/^Bearer\s+/i, '').trim();
  const hdr = req.headers.get('X-License-Key');
  if (hdr) return hdr.trim();
  const q = url.searchParams.get('key') || url.searchParams.get('license_key');
  if (q) return q.trim();
  if (body && typeof body === 'object') {
    const b = body.license_key || body.key;
    if (b) return String(b).trim();
  }
  return null;
}

function paywall(env: Env, lic: LicenseResult, needed: Tier = 'pro'): Response {
  return Response.json({
    error: 'premium feature — active subscription required',
    detail: lic.error ?? `requires ${needed} tier`,
    your_status: lic.status,
    your_tier: lic.tier,
    upgrade: env.CHECKOUT_URL ?? null,
  }, { status: 402 });   // 402 Payment Required
}

export function tierMeets(have: Tier, need: Tier): boolean {
  const rank: Record<Tier, number> = { free: 0, pro: 1, team: 2 };
  return rank[have] >= rank[need];
}

// === Source fetchers ===

async function fetchHN(env: Env): Promise<Item[]> {
  // Top story IDs first, then top 30 details.
  const idsResp = await fetch('https://hacker-news.firebaseio.com/v0/topstories.json', {
    headers: { 'User-Agent': env.USER_AGENT },
  });
  if (!idsResp.ok) return [];
  const ids = (await idsResp.json() as number[]).slice(0, 30);
  const items: Item[] = [];
  for (const id of ids) {
    try {
      const r = await fetch(`https://hacker-news.firebaseio.com/v0/item/${id}.json`, {
        headers: { 'User-Agent': env.USER_AGENT },
      });
      if (!r.ok) continue;
      const it: any = await r.json();
      if (!it?.title) continue;
      items.push({
        source: 'hn',
        title: String(it.title),
        url: it.url ? String(it.url) : `https://news.ycombinator.com/item?id=${id}`,
        score: Number(it.score ?? 0),
        comments: Number(it.descendants ?? 0),
        collected_at: new Date().toISOString(),
      });
    } catch {}
  }
  return items;
}

async function fetchGithubTrending(env: Env): Promise<Item[]> {
  const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const r = await fetch(`https://api.github.com/search/repositories?q=created:>${since}&sort=stars&order=desc&per_page=30`, {
    headers: { 'User-Agent': env.USER_AGENT, 'Accept': 'application/vnd.github+json' },
  });
  if (!r.ok) return [];
  const data: any = await r.json();
  return (data.items ?? []).map((it: any): Item => ({
    source: 'github',
    title: `${it.full_name} — ${it.description ?? ''}`.slice(0, 240),
    url: it.html_url,
    score: it.stargazers_count,
    collected_at: new Date().toISOString(),
  }));
}

async function fetchArxiv(env: Env): Promise<Item[]> {
  // Use the query API instead of the daily RSS (which is empty on weekends).
  const url = 'https://export.arxiv.org/api/query?search_query=cat:cs.AI&start=0&max_results=25&sortBy=submittedDate&sortOrder=descending';
  const r = await fetch(url, {
    headers: { 'User-Agent': env.USER_AGENT, 'Accept': 'application/atom+xml' },
  });
  if (!r.ok) return [];
  const xml = await r.text();
  const out: Item[] = [];
  const entries = xml.split('<entry>').slice(1, 26);
  for (const e of entries) {
    const closeIdx = e.indexOf('</entry>');
    if (closeIdx < 0) continue;
    const block = e.slice(0, closeIdx);
    const title = match(block, /<title>([\s\S]*?)<\/title>/)?.replace(/\s+/g, ' ').trim();
    const link = match(block, /<id>([^<]+)<\/id>/);
    const summaryRaw = match(block, /<summary>([\s\S]*?)<\/summary>/)?.replace(/\s+/g, ' ').trim();
    if (!title || !link) continue;
    out.push({ source: 'arxiv', title, url: link, summary: summaryRaw?.slice(0, 400), collected_at: new Date().toISOString() });
  }
  return out;
}

async function fetchReddit(env: Env, sub: string, source: Source): Promise<Item[]> {
  // Reddit JSON API blocks Cloudflare IPs. Use RSS feed which is more open.
  const r = await fetch(`https://www.reddit.com/r/${sub}/top/.rss?t=day&limit=25`, {
    headers: {
      'User-Agent': env.USER_AGENT,
      'Accept': 'application/rss+xml,application/atom+xml',
    },
  });
  if (!r.ok) return [];
  const xml = await r.text();
  const out: Item[] = [];
  // Reddit RSS uses Atom <entry> blocks
  const entries = xml.split('<entry>').slice(1, 26);
  for (const e of entries) {
    const closeIdx = e.indexOf('</entry>');
    if (closeIdx < 0) continue;
    const block = e.slice(0, closeIdx);
    const title = match(block, /<title>([\s\S]*?)<\/title>/)?.replace(/<!\[CDATA\[|\]\]>/g, '').trim();
    const link = match(block, /<link[^>]+href="([^"]+)"/);
    if (!title || !link) continue;
    out.push({ source, title, url: link, collected_at: new Date().toISOString() });
  }
  return out;
}

function match(s: string, re: RegExp): string | undefined {
  return s.match(re)?.[1];
}

async function collectAll(env: Env): Promise<Record<string, Item[]>> {
  const sourceNames = ['hn', 'github', 'arxiv', 'reddit-ml', 'reddit-prog'] as const;
  const results = await Promise.allSettled([
    fetchHN(env),
    fetchGithubTrending(env),
    fetchArxiv(env),
    fetchReddit(env, 'MachineLearning', 'reddit-ml'),
    fetchReddit(env, 'programming', 'reddit-prog'),
  ]);
  results.forEach((result, index) => {
    if (result.status === 'rejected') {
      logError('collection.source_failed', result.reason, { source: sourceNames[index] });
    }
  });

  const [hn, gh, ax, rml, rprog] = results;

  return {
    hn: hn.status === 'fulfilled' ? hn.value : [],
    github: gh.status === 'fulfilled' ? gh.value : [],
    arxiv: ax.status === 'fulfilled' ? ax.value : [],
    'reddit-ml': rml.status === 'fulfilled' ? rml.value : [],
    'reddit-prog': rprog.status === 'fulfilled' ? rprog.value : [],
  };
}

// === Digest synthesis ===

const DIGEST_SYSTEM = `You are TrendPulse. Given a batch of titles from HN / GitHub trending / arxiv / Reddit, identify 3-7 cross-cutting themes that are gaining attention right now. A theme is something multiple sources agree on, OR something that's surprisingly absent given expectations.

Return JSON:
{
  "one_line": "<one sentence summary of what's hot today>",
  "themes": [
    {
      "name": "<short theme label>",
      "rationale": "<1-2 sentence why this is a theme>",
      "example_titles": ["<title 1>", "<title 2>", ...],
      "signal_strength": "rising|steady|spike"
    }
  ]
}

Prioritize: emerging tech, business model shifts, regulatory signals, market positioning changes. Skip celebrity / political / drama-driven items unless they have a structural product/market implication.

Return ONLY JSON.`;

export function tryParseJson(s: unknown): any | null {
  if (s == null) return null;
  if (typeof s === 'object') return s;
  const str = typeof s === 'string' ? s : String(s);
  // Strip code fences and any text before/after JSON object boundaries.
  let cleaned = str.replace(/^```json\s*|\s*```$/g, '').trim();
  const firstBrace = cleaned.indexOf('{');
  const lastBrace = cleaned.lastIndexOf('}');
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    cleaned = cleaned.slice(firstBrace, lastBrace + 1);
  }
  try { return JSON.parse(cleaned); } catch { return null; }
}

async function synthesizeDigest(items: Record<string, Item[]>, env: Env, focus?: string): Promise<Digest> {
  const allTitles: string[] = [];
  for (const [src, list] of Object.entries(items)) {
    for (const it of list) {
      const meta = it.score != null ? ` [${it.score}]` : '';
      allTitles.push(`(${src}) ${it.title}${meta}`);
    }
  }
  const corpus = allTitles.slice(0, 200).join('\n');

  const system = focus
    ? `${DIGEST_SYSTEM}\n\nThe reader has a specific focus: ${focus}. Weight themes toward this focus and skip items irrelevant to it.`
    : DIGEST_SYSTEM;

  let aiResp: any;
  try {
    aiResp = await env.AI.run('@cf/meta/llama-3.3-70b-instruct-fp8-fast', {
      messages: [
        { role: 'system', content: system },
        { role: 'user', content: `Titles from the last 24h:\n\n${corpus}\n\nRespond with the JSON object only, starting with { and ending with }.` },
      ],
      max_tokens: 2000,
    });
  } catch (err) {
    logError('digest.inference_failed', err);
    return {
      generated_at: new Date().toISOString(),
      date_label: new Date().toISOString().slice(0, 10),
      themes: [],
      one_line: 'inference failed; raw items below',
      source_counts: countSources(items),
    };
  }

  const raw = aiResp?.response ?? aiResp?.result ?? aiResp;
  const parsed = tryParseJson(raw);
  logEvent('info', 'digest.ai_response', {
    raw_type: typeof raw,
    raw_chars: typeof raw === 'string' ? raw.length : JSON.stringify(raw ?? null).length,
    parsed: parsed != null,
  });
  return {
    generated_at: new Date().toISOString(),
    date_label: new Date().toISOString().slice(0, 10),
    themes: Array.isArray(parsed?.themes) ? parsed.themes : [],
    one_line: String(parsed?.one_line ?? ''),
    source_counts: countSources(items),
  };
}

function countSources(items: Record<string, Item[]>): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [k, v] of Object.entries(items)) out[k] = v.length;
  return out;
}

// === Premium: custom filtering ===

const ALL_SOURCES: Source[] = ['hn', 'github', 'arxiv', 'reddit-ml', 'reddit-prog'];

// Most recent raw collection (cron stores one every 4h). Used to build
// personalized digests without re-fetching every source.
async function getLatestRaw(env: Env): Promise<Record<string, Item[]> | null> {
  // KV lists keys lexicographically; ISO timestamps sort chronologically, so
  // the last key is the newest. Pull a small window and pick the max.
  const list = await env.TP_DATA.list({ prefix: RAW_KEY_PREFIX });
  if (!list.keys.length) return null;
  const newest = list.keys.map(k => k.name).sort().at(-1)!;
  const v = await env.TP_DATA.get(newest);
  if (!v) return null;
  try { return JSON.parse(v) as Record<string, Item[]>; } catch { return null; }
}

interface CustomFilters { keywords: string[]; sources: Source[] }

interface HistorySearchParams {
  query: string;
  terms: string[];
  limit: number;
}

interface HistorySearchResult {
  date_label: string;
  generated_at: string;
  one_line: string;
  source_counts: Record<string, number>;
  matched_themes: Digest['themes'];
}

export function parseFilters(params: URLSearchParams): CustomFilters {
  const keywords = (params.get('keywords') ?? '')
    .split(',').map(s => s.trim().toLowerCase()).filter(Boolean).map(s => s.slice(0, MAX_KEYWORD_CHARS)).slice(0, 20);
  const reqSources = (params.get('sources') ?? '')
    .split(',').map(s => s.trim()).filter(Boolean) as Source[];
  const sources = reqSources.filter(s => ALL_SOURCES.includes(s));
  return { keywords, sources: sources.length ? sources : ALL_SOURCES };
}

export function applyFilters(raw: Record<string, Item[]>, f: CustomFilters): Record<string, Item[]> {
  const out: Record<string, Item[]> = {};
  for (const [src, list] of Object.entries(raw)) {
    if (!f.sources.includes(src as Source)) continue;
    const filtered = f.keywords.length
      ? list.filter(it => {
          const hay = `${it.title} ${it.summary ?? ''}`.toLowerCase();
          return f.keywords.some(k => hay.includes(k));
        })
      : list;
    if (filtered.length) out[src] = filtered;
  }
  return out;
}

function normalizeSearchText(raw: unknown): string {
  return String(raw ?? '').toLowerCase().replace(/\s+/g, ' ').trim();
}

export function normalizeSearchQuery(raw: unknown): string {
  return normalizeSearchText(raw).slice(0, 120);
}

export function parseHistorySearch(params: URLSearchParams): HistorySearchParams {
  const query = normalizeSearchQuery(params.get('q') || params.get('query'));
  const rawLimit = Number(params.get('limit') ?? '20');
  const limit = Math.min(50, Math.max(1, Number.isFinite(rawLimit) ? Math.floor(rawLimit) : 20));
  return { query, terms: query.split(' ').filter(Boolean).slice(0, 10), limit };
}

function textMatchesSearch(text: unknown, query: string, terms: string[]): boolean {
  const haystack = normalizeSearchText(text);
  return haystack.includes(query) || terms.every(term => haystack.includes(term));
}

function digestThemes(d: Digest): Digest['themes'] {
  return Array.isArray(d.themes) ? d.themes : [];
}

function themeExampleTitles(t: Digest['themes'][number]): string[] {
  return Array.isArray(t.example_titles) ? t.example_titles : [];
}

function themeSearchText(t: Digest['themes'][number]): string {
  return [t.name, t.rationale, ...themeExampleTitles(t)].join(' ');
}

function digestSearchText(d: Digest): string {
  return [
    d.one_line,
    ...digestThemes(d).map(themeSearchText),
  ].join(' ');
}

export function digestMatchesSearch(d: Digest, query: string): boolean {
  const normalized = normalizeSearchQuery(query);
  if (!normalized) return false;
  return textMatchesSearch(digestSearchText(d), normalized, normalized.split(' ').filter(Boolean));
}

function matchedThemes(d: Digest, search: HistorySearchParams): Digest['themes'] {
  return digestThemes(d)
    .filter(t => textMatchesSearch(themeSearchText(t), search.query, search.terms))
    .slice(0, 5)
    .map(t => ({ ...t, example_titles: themeExampleTitles(t).slice(0, 5) }));
}

async function listDigestArchiveKeys(env: Env): Promise<string[]> {
  const keys: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await env.TP_DIGEST.list({ prefix: DIGEST_KEY_PREFIX, limit: 1000, cursor });
    keys.push(...page.keys.map(k => k.name).filter(name => name !== LATEST_KEY));
    cursor = page.list_complete ? undefined : page.cursor;
  } while (cursor);
  return keys;
}

async function searchDigestHistory(env: Env, search: HistorySearchParams): Promise<{ total_matches: number; results: HistorySearchResult[] }> {
  const keys = await listDigestArchiveKeys(env);
  const matches: HistorySearchResult[] = [];

  for (const key of keys) {
    const value = await env.TP_DIGEST.get(key);
    if (!value) continue;
    let digest: Digest;
    try { digest = JSON.parse(value) as Digest; } catch { continue; }
    if (!digest.date_label || !digest.generated_at) continue;
    if (!textMatchesSearch(digestSearchText(digest), search.query, search.terms)) continue;

    matches.push({
      date_label: digest.date_label,
      generated_at: digest.generated_at,
      one_line: String(digest.one_line ?? ''),
      source_counts: digest.source_counts ?? {},
      matched_themes: matchedThemes(digest, search),
    });
  }

  matches.sort((a, b) => b.date_label.localeCompare(a.date_label));
  return { total_matches: matches.length, results: matches.slice(0, search.limit) };
}

// Build (and day-cache) a personalized digest for a given filter set.
async function buildCustomDigest(env: Env, f: CustomFilters): Promise<Digest & { filters: CustomFilters; matched: number }> {
  const dateKey = new Date().toISOString().slice(0, 10);
  const sig = await sha256Hex(JSON.stringify({ k: f.keywords, s: f.sources }));
  const cacheKey = `${CUSTOM_PREFIX}${dateKey}:${sig}`;
  const cached = await env.TP_DIGEST.get(cacheKey);
  if (cached) {
    try { return JSON.parse(cached); } catch {}
  }

  const raw = await getLatestRaw(env);
  if (!raw) {
    return { generated_at: new Date().toISOString(), date_label: dateKey, themes: [], one_line: 'no collection yet', source_counts: {}, filters: f, matched: 0 };
  }
  const filtered = applyFilters(raw, f);
  const matched = Object.values(filtered).reduce((n, l) => n + l.length, 0);

  const focus = f.keywords.length ? `topics matching: ${f.keywords.join(', ')}` : undefined;
  const digest = await synthesizeDigest(filtered, env, focus);
  const result = { ...digest, filters: f, matched };
  // Cache for the rest of the day so repeated calls don't re-run inference.
  await env.TP_DIGEST.put(cacheKey, JSON.stringify(result), { expirationTtl: 60 * 60 * 24 });
  return result;
}

async function handleCustomDigest(req: Request, env: Env, url: URL): Promise<Response> {
  const lic = await validateLicense(env, licenseFromRequest(req, url));
  if (!lic.valid) return paywall(env, lic, 'pro');

  const filters = parseFilters(url.searchParams);
  const digest = await buildCustomDigest(env, filters);
  return Response.json({ tier: lic.tier, ...digest });
}

async function handleDigestSearch(req: Request, env: Env, url: URL): Promise<Response> {
  const lic = await validateLicense(env, licenseFromRequest(req, url));
  if (!lic.valid) return paywall(env, lic, 'pro');

  const search = parseHistorySearch(url.searchParams);
  if (!search.query) {
    return Response.json({ error: 'provide a search query with `q`' }, { status: 400 });
  }

  const { total_matches, results } = await searchDigestHistory(env, search);
  return Response.json({
    tier: lic.tier,
    query: search.query,
    count: results.length,
    total_matches,
    results,
  });
}

// === Premium: custom delivery ===
//
// Subscribers register a delivery target (webhook and/or email) tied to their
// license. Each cron digest is pushed to active registrations, respecting any
// saved per-subscriber filters. Webhook delivery is a Team feature; email is
// available to any active subscription (requires RESEND_API_KEY).

async function readLimitedText(req: Request): Promise<string> {
  const lengthHeader = req.headers.get('Content-Length');
  if (lengthHeader) {
    const length = Number(lengthHeader);
    if (!Number.isFinite(length) || length < 0) {
      throw new HttpError(400, 'invalid_content_length', 'invalid Content-Length header');
    }
    if (length > MAX_BODY_CHARS) {
      throw new HttpError(413, 'payload_too_large', 'request body is too large');
    }
  }

  const text = await req.text();
  if (text.length > MAX_BODY_CHARS) {
    throw new HttpError(413, 'payload_too_large', 'request body is too large');
  }
  return text;
}

async function readBody(req: Request): Promise<Record<string, unknown>> {
  if (!req.body) return {};

  const ct = (req.headers.get('Content-Type') ?? '').toLowerCase();
  if (!ct) return {};

  if (ct.includes('application/json')) {
    const text = await readLimitedText(req);
    if (!text.trim()) return {};
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      throw new HttpError(400, 'invalid_json', 'invalid JSON request body');
    }
    if (parsed == null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new HttpError(400, 'invalid_body', 'request body must be a JSON object');
    }
    return parsed as Record<string, unknown>;
  }

  if (ct.includes('application/x-www-form-urlencoded')) {
    const text = await readLimitedText(req);
    return Object.fromEntries(new URLSearchParams(text));
  }

  throw new HttpError(415, 'unsupported_media_type', 'unsupported request Content-Type');
}

export function validWebhookUrl(u: string): boolean {
  try {
    const parsed = new URL(u);
    return (
      u.length <= MAX_WEBHOOK_URL_CHARS &&
      (parsed.protocol === 'https:' || parsed.protocol === 'http:') &&
      !!parsed.hostname &&
      !parsed.username &&
      !parsed.password
    );
  } catch { return false; }
}

async function handleDelivery(req: Request, env: Env, url: URL): Promise<Response> {
  const body = req.method === 'GET' ? undefined : await readBody(req);
  const lic = await validateLicense(env, licenseFromRequest(req, url, body));
  if (!lic.valid) return paywall(env, lic, 'pro');

  const hash = await sha256Hex((licenseFromRequest(req, url, body) ?? '').trim());
  const kvKey = `${DELIVERY_PREFIX}${hash}`;

  if (req.method === 'GET') {
    const existing = await env.TP_DATA.get(kvKey);
    let registration: DeliveryRegistration | null = null;
    if (existing) {
      try {
        registration = JSON.parse(existing) as DeliveryRegistration;
      } catch (err) {
        logError('delivery.registration_parse_failed', err);
      }
    }
    return Response.json({
      tier: lic.tier,
      email_delivery_available: !!env.RESEND_API_KEY,
      registration,
    });
  }

  if (req.method === 'DELETE') {
    await env.TP_DATA.delete(kvKey);
    return Response.json({ ok: true, removed: true });
  }

  // POST — register / update.
  const email = body?.email ? String(body.email).trim() : undefined;
  const webhook = body?.webhook ? String(body.webhook).trim() : undefined;
  if (!email && !webhook) {
    return Response.json({ error: 'provide an `email` and/or `webhook` target' }, { status: 400 });
  }
  if (email && email.length > MAX_EMAIL_CHARS) {
    return Response.json({ error: 'email address is too long' }, { status: 400 });
  }
  if (email && !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
    return Response.json({ error: 'invalid email address' }, { status: 400 });
  }
  if (email && !env.RESEND_API_KEY) {
    return Response.json({ error: 'email delivery is not configured on this instance; use a webhook target instead' }, { status: 503 });
  }
  if (webhook) {
    if (webhook.length > MAX_WEBHOOK_URL_CHARS) return Response.json({ error: 'webhook URL is too long' }, { status: 400 });
    if (!validWebhookUrl(webhook)) return Response.json({ error: 'invalid webhook URL' }, { status: 400 });
    if (!tierMeets(lic.tier, 'team')) return paywall(env, lic, 'team');
  }
  const webhookSecret = body?.webhook_secret ? String(body.webhook_secret) : undefined;
  if (webhookSecret && webhookSecret.length > MAX_WEBHOOK_SECRET_CHARS) {
    return Response.json({ error: 'webhook secret is too long' }, { status: 400 });
  }

  // Filters can be passed as arrays (JSON) or comma strings (form/query).
  const asList = (v: any): string[] =>
    Array.isArray(v) ? v.map(String) : typeof v === 'string' ? v.split(',') : [];
  const keywords = asList(body?.keywords).map(s => s.trim().toLowerCase()).filter(Boolean).map(s => s.slice(0, MAX_KEYWORD_CHARS)).slice(0, 20);
  const sources = asList(body?.sources).map(s => s.trim()).filter(s => ALL_SOURCES.includes(s as Source)) as Source[];

  const reg: DeliveryRegistration = {
    license_hash: hash,
    tier: lic.tier,
    email,
    webhook,
    webhook_secret: webhookSecret,
    filters: (keywords.length || sources.length) ? { keywords, sources } : undefined,
    created_at: new Date().toISOString(),
  };
  await env.TP_DATA.put(kvKey, JSON.stringify(reg));
  return Response.json({ ok: true, registered: { email: !!email, webhook: !!webhook, filters: reg.filters ?? null }, tier: lic.tier });
}

async function hmacHex(secret: string, message: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    'raw', new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'],
  );
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(message));
  return [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, '0')).join('');
}

export function digestToHtml(d: Digest): string {
  const themes = (d.themes ?? []).map(t =>
    `<li><strong>${escapeHtmlServer(t.name)}</strong> <em>(${escapeHtmlServer(t.signal_strength ?? 'steady')})</em><br>${escapeHtmlServer(t.rationale ?? '')}</li>`
  ).join('');
  return `<h2>TrendPulse — ${escapeHtmlServer(d.date_label)}</h2>
<p><strong>${escapeHtmlServer(d.one_line ?? '')}</strong></p>
<ul>${themes}</ul>
<p style="color:#888;font-size:12px">Generated ${escapeHtmlServer(d.generated_at ?? '')}</p>`;
}

export function escapeHtmlServer(s: unknown): string {
  return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));
}

async function sendWebhook(env: Env, reg: DeliveryRegistration, digest: Digest): Promise<boolean> {
  if (!reg.webhook) return false;
  const payload = JSON.stringify({ event: 'digest', generated_at: digest.generated_at, digest });
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'User-Agent': env.USER_AGENT,
    'X-TrendPulse-Event': 'digest',
  };
  if (reg.webhook_secret) headers['X-TrendPulse-Signature'] = await hmacHex(reg.webhook_secret, payload);
  try {
    const r = await fetch(reg.webhook, { method: 'POST', headers, body: payload });
    if (!r.ok) {
      logEvent('warn', 'delivery.webhook_non_2xx', { status: r.status });
    }
    return r.ok;
  } catch (err) {
    logError('delivery.webhook_failed', err);
    return false;
  }
}

async function sendEmail(env: Env, to: string, digest: Digest): Promise<boolean> {
  if (!env.RESEND_API_KEY) return false;
  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: env.FROM_EMAIL ?? 'TrendPulse <onboarding@resend.dev>',
        to,
        subject: `TrendPulse digest — ${digest.date_label}`,
        html: digestToHtml(digest),
      }),
    });
    if (!r.ok) {
      logEvent('warn', 'delivery.email_non_2xx', { status: r.status });
    }
    return r.ok;
  } catch (err) {
    logError('delivery.email_failed', err);
    return false;
  }
}

// Push a freshly-generated digest to every active subscriber. Standard digest
// is reused unless a subscriber has saved filters, in which case a personalized
// digest is built (and day-cached) for them.
async function deliverDigest(env: Env, standard: Digest): Promise<void> {
  const list = await env.TP_DATA.list({ prefix: DELIVERY_PREFIX });
  let delivered = 0, skipped = 0;
  for (const k of list.keys) {
    const v = await env.TP_DATA.get(k.name);
    if (!v) continue;
    let reg: DeliveryRegistration;
    try { reg = JSON.parse(v) as DeliveryRegistration; } catch { continue; }

    let digest = standard;
    if (reg.filters && (reg.filters.keywords?.length || reg.filters.sources?.length)) {
      const f: CustomFilters = {
        keywords: reg.filters.keywords ?? [],
        sources: (reg.filters.sources?.length ? reg.filters.sources : ALL_SOURCES),
      };
      try {
        digest = await buildCustomDigest(env, f);
      } catch (err) {
        logError('delivery.custom_digest_failed', err);
        digest = standard;
      }
    }

    const tasks: Promise<boolean>[] = [];
    if (reg.webhook && tierMeets(reg.tier, 'team')) tasks.push(sendWebhook(env, reg, digest));
    if (reg.email) tasks.push(sendEmail(env, reg.email, digest));
    if (!tasks.length) { skipped++; continue; }
    const results = await Promise.allSettled(tasks);
    if (results.some(r => r.status === 'fulfilled' && r.value)) delivered++; else skipped++;
  }
  logEvent('info', 'delivery.complete', { delivered, skipped });
}

async function handleLicenseCheck(req: Request, env: Env, url: URL): Promise<Response> {
  const body = await readBody(req);
  const lic = await validateLicense(env, licenseFromRequest(req, url, body));
  return Response.json({
    valid: lic.valid,
    tier: lic.tier,
    status: lic.status,
    expires_at: lic.expires_at ?? null,
    error: lic.error ?? null,
  }, { status: lic.valid ? 200 : 402 });
}

function handleConfig(env: Env): Response {
  return Response.json({
    checkout_url: env.CHECKOUT_URL ?? null,
    premium_enabled: !!env.CHECKOUT_URL,
    email_delivery: !!env.RESEND_API_KEY,
    features: {
      pro: ['custom filters', 'email delivery', 'historical search'],
      team: ['everything in pro', 'webhook delivery', 'custom sources'],
    },
  });
}

async function runCron(env: Env) {
  const started = Date.now();
  logEvent('info', 'cron.start');
  const items = await collectAll(env);
  const stamp = new Date().toISOString();
  await env.TP_DATA.put(`${RAW_KEY_PREFIX}${stamp}`, JSON.stringify(items), { expirationTtl: 60 * 60 * 24 * 14 });

  const digest = await synthesizeDigest(items, env);
  const dateKey = digest.date_label;
  await env.TP_DIGEST.put(`${DIGEST_KEY_PREFIX}${dateKey}`, JSON.stringify(digest));
  await env.TP_DIGEST.put(LATEST_KEY, JSON.stringify(digest));
  logEvent('info', 'digest.generated', {
    date_label: dateKey,
    themes: digest.themes.length,
    duration_ms: Date.now() - started,
  });

  // Push to paid subscribers' delivery targets (webhook / email).
  await deliverDigest(env, digest);
  logEvent('info', 'cron.complete', { duration_ms: Date.now() - started });
}

// === HTTP ===

async function handleLatest(_req: Request, env: Env): Promise<Response> {
  const v = await env.TP_DIGEST.get(LATEST_KEY);
  if (!v) {
    return Response.json({
      message: 'no digest yet — first cron run produces it within hours',
      hint: 'POST /api/run-now to trigger collection in the meantime (rate-limited)',
    }, { status: 202 });
  }
  return new Response(v, { headers: { 'Content-Type': 'application/json' } });
}

async function handleHistory(_req: Request, env: Env): Promise<Response> {
  const list = await env.TP_DIGEST.list({ prefix: DIGEST_KEY_PREFIX, limit: 30 });
  const out: Digest[] = [];
  for (const k of list.keys) {
    if (k.name === LATEST_KEY) continue;
    const v = await env.TP_DIGEST.get(k.name);
    if (v) try { out.push(JSON.parse(v) as Digest); } catch {}
  }
  return Response.json({
    count: out.length,
    digests: out.sort((a, b) => b.date_label.localeCompare(a.date_label)),
  });
}

async function handleRunNow(_req: Request, env: Env): Promise<Response> {
  // Manual trigger — useful for first-run.
  // Cheap rate-limit: only allow if we haven't run in last 30 min.
  const key = 'last_manual_run';
  const last = await env.TP_DATA.get(key);
  if (last && Date.now() - Number(last) < 30 * 60 * 1000) {
    return Response.json({ error: 'manual rate limit; try later' }, { status: 429 });
  }
  await env.TP_DATA.put(key, String(Date.now()), { expirationTtl: 60 * 60 });
  await runCron(env);
  return handleLatest(_req, env);
}

async function handleStatus(_req: Request, env: Env): Promise<Response> {
  const list = await env.TP_DATA.list({ prefix: RAW_KEY_PREFIX, limit: 5 });
  const recent = list.keys.map(k => k.name.replace(RAW_KEY_PREFIX, ''));
  return Response.json({
    name: 'TrendPulse',
    last_collections: recent,
    has_latest_digest: (await env.TP_DIGEST.get(LATEST_KEY)) != null,
  });
}

async function dispatchRequest(req: Request, env: Env): Promise<Response> {
  if (req.url.length > MAX_URL_LENGTH) {
    throw new HttpError(414, 'uri_too_long', 'request URL is too long');
  }

  const url = new URL(req.url);
  if (url.pathname === '/api/digest/latest') {
    assertMethod(req, ['GET']);
    return handleLatest(req, env);
  }
  if (url.pathname === '/api/digest/history') {
    assertMethod(req, ['GET']);
    return handleHistory(req, env);
  }
  if (url.pathname === '/api/run-now') {
    assertMethod(req, ['POST']);
    return handleRunNow(req, env);
  }
  if (url.pathname === '/api/status') {
    assertMethod(req, ['GET']);
    return handleStatus(req, env);
  }
  if (url.pathname === '/api/config') {
    assertMethod(req, ['GET']);
    return handleConfig(env);
  }
  // --- Premium (Lemon Squeezy subscription required) ---
  if (url.pathname === '/api/license/check') {
    assertMethod(req, ['POST']);
    return handleLicenseCheck(req, env, url);
  }
  if (url.pathname === '/api/digest/custom') {
    assertMethod(req, ['GET']);
    return handleCustomDigest(req, env, url);
  }
  if (url.pathname === '/api/digest/search') {
    assertMethod(req, ['GET']);
    return handleDigestSearch(req, env, url);
  }
  if (url.pathname === '/api/delivery') {
    assertMethod(req, ['GET', 'POST', 'DELETE']);
    return handleDelivery(req, env, url);
  }
  return env.ASSETS.fetch(req);
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const ctx = makeRequestContext(req);
    logEvent('info', 'request.start', {
      request_id: ctx.request_id,
      method: ctx.method,
      path: ctx.path,
    });

    try {
      const response = await dispatchRequest(req, env);
      logEvent('info', 'request.finish', {
        request_id: ctx.request_id,
        method: ctx.method,
        path: ctx.path,
        status: response.status,
        duration_ms: Date.now() - ctx.start_ms,
      });
      return response;
    } catch (err) {
      const response = errorResponse(err, ctx);
      const fields = {
        request_id: ctx.request_id,
        method: ctx.method,
        path: ctx.path,
        status: response.status,
        duration_ms: Date.now() - ctx.start_ms,
      };
      if (response.status >= 500) logError('request.failed', err, fields);
      else logEvent('warn', 'request.rejected', { ...fields, ...errorFields(err, false) });
      return response;
    }
  },

  async scheduled(_ev: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(runCron(env).catch(err => logError('cron.failed', err)));
  },
};
