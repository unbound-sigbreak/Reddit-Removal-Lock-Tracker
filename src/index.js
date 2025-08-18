#!/usr/bin/env node
"use strict";

/*
Reddit removal/lock tracker + heuristic reporting + karma-over-time series
- One-shot, cron-friendly (/10).
- SQLite always (better-sqlite3). Optional Postgres mirror (--pg-url / REDDIT_SCRAPER_PG_URL).
- OAuth via refresh token (Node 20+ global fetch).
- Scrapes /r/<sub>/new (limit=100, paginated), stops at cutoff, upserts posts, then (optionally) fetches comments.
- Recheck window: batch /api/info; comments recheck (confidence + new).
- Captures transitions (first_seen, removed_at, locked_at), outbound links, and score series (posts/comments).
- Idempotent, retry/backoff, timeouts. If SQLite window empty but PG has rows, recheck uses PG set.
- Removed moderator posts/comments tracking ability from main script to prevent stalking and abuse.
*/

const fs = require("fs");
const path = require("path");
const process = require("process");
const Database = require("better-sqlite3");
const { Client: PgClient } = require("pg");

const argv = process.argv.slice(2);
const opts = {
  clientId: process.env.REDDIT_CLIENT_ID || null,
  clientSecret: process.env.REDDIT_CLIENT_SECRET || null,
  refreshToken: process.env.REDDIT_SCRAPER_REFRESH_TOKEN || null,

  subreddit: process.env.REDDIT_SCRAPER_SUBREDDIT || null,
  daysBack: Math.max(1, Number(process.env.REDDIT_SCRAPER_DAYS_BACK || 3)),

  start: process.env.REDDIT_SCRAPER_START || null,
  end: process.env.REDDIT_SCRAPER_END || null,

  dbPath: process.env.REDDIT_SCRAPER_DB_PATH || null,
  pgUrl: process.env.REDDIT_SCRAPER_PG_URL || null,

  concurrency: Math.max(1, Number(process.env.REDDIT_SCRAPER_CONCURRENCY || 2)),
  maxPages: Math.max(0, Number(process.env.REDDIT_SCRAPER_MAX_PAGES || 0)),
  maxPosts: Math.max(0, Number(process.env.REDDIT_SCRAPER_MAX_POSTS || 0)),
  noComments: process.env.REDDIT_SCRAPER_NO_COMMENTS === "1",
  noRecheckComments: process.env.REDDIT_SCRAPER_NO_RECHECK_COMMENTS === "1",
  initialCommentLimit: Math.max(0, Number(process.env.REDDIT_SCRAPER_INITIAL_COMMENT_LIMIT || 0)),
  recheckCommentLimit: Math.max(0, Number(process.env.REDDIT_SCRAPER_RECHECK_COMMENT_LIMIT || 0)),
  fetchTimeoutMs: Math.max(1000, Number(process.env.REDDIT_SCRAPER_FETCH_TIMEOUT_MS || 20000)),
  report: process.env.REDDIT_SCRAPER_REPORT === "1",

  seriesMax: Math.max(0, Number(process.env.REDDIT_SCRAPER_SERIES_MAX || 0)),
  seriesDedupePosts: process.env.REDDIT_SCRAPER_SERIES_DEDUPE_POSTS === "0" ? false : true,
  commentSeriesMax: Math.max(0, Number(process.env.REDDIT_SCRAPER_COMMENT_SERIES_MAX || 0)),
  commentSeriesDedupe: process.env.REDDIT_SCRAPER_COMMENT_SERIES_DEDUPE === "1",

  verbose: !!process.env.REDDIT_SCRAPER_VERBOSE,
  ua: (process.env.REDDIT_SCRAPER_UA || "reddit-scraper/0.0.3"),
};

const pickNext = (flag, i) => {
  if (i + 1 >= argv.length) throw new Error(`Missing value for ${flag}`);
  return argv[i + 1];
};

const printHelpAndExit = (code = 0) => {
  console.log(`
Reddit Post/Comment Removal Tracker + Heuristics + Karma Series

Usage:
  index.js --client-id <id> --client-secret <secret> --refresh-token <tok> \\
           --subreddit <name> [--days-back 4] [--start <ISO|epoch>] [--end <ISO|epoch>] \\
           [--db <sqlite path>] [--pg-url <postgres dsn>] \\
           [--concurrency 2] [--max-pages N] [--max-posts N] \\
           [--no-comments] [--no-recheck-comments] \\
           [--initial-comment-limit N] [--recheck-comment-limit N] \\
           [--fetch-timeout-ms 20000] [--ua "reddit-crypt/3.1 by script"] \\
           [--series-max 288] [--no-series-dedupe-posts] \\
           [--comment-series-max 288] [--comment-series-dedupe] \\
           [--report] [--verbose] [--help|-h]
`);
  process.exit(code);
};

for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  switch (a) {
    case "--client-id": opts.clientId = pickNext(a, i++); break;
    case "--client-secret": opts.clientSecret = pickNext(a, i++); break;
    case "--refresh-token": opts.refreshToken = pickNext(a, i++); break;

    case "--subreddit": opts.subreddit = pickNext(a, i++); break;
    case "--days-back": opts.daysBack = Math.max(1, Number(pickNext(a, i++))); break;
    case "--start": opts.start = pickNext(a, i++); break;
    case "--end": opts.end = pickNext(a, i++); break;

    case "--db": opts.dbPath = pickNext(a, i++); break;
    case "--pg-url": opts.pgUrl = pickNext(a, i++); break;

    case "--concurrency": opts.concurrency = Math.max(1, Number(pickNext(a, i++))); break;
    case "--max-pages": opts.maxPages = Math.max(0, Number(pickNext(a, i++))); break;
    case "--max-posts": opts.maxPosts = Math.max(0, Number(pickNext(a, i++))); break;
    case "--no-comments": opts.noComments = true; break;
    case "--no-recheck-comments": opts.noRecheckComments = true; break;
    case "--initial-comment-limit": opts.initialCommentLimit = Math.max(0, Number(pickNext(a, i++))); break;
    case "--recheck-comment-limit": opts.recheckCommentLimit = Math.max(0, Number(pickNext(a, i++))); break;
    case "--fetch-timeout-ms": opts.fetchTimeoutMs = Math.max(1000, Number(pickNext(a, i++))); break;

    case "--series-max": opts.seriesMax = Math.max(0, Number(pickNext(a, i++))); break;
    case "--no-series-dedupe-posts": opts.seriesDedupePosts = false; break;
    case "--comment-series-max": opts.commentSeriesMax = Math.max(0, Number(pickNext(a, i++))); break;
    case "--comment-series-dedupe": opts.commentSeriesDedupe = true; break;
    case "--ua": opts.ua = pickNext(a, i++); break;
    case "--report": opts.report = true; break;
    case "--verbose": opts.verbose = true; break;

    case "--help":
    case "-h": printHelpAndExit(0); break;

    default:
      if (a.startsWith("-")) {
        console.error(`Unknown flag: ${a}`);
        printHelpAndExit(2);
      }
  }
}

if (!opts.clientId || !opts.clientSecret || !opts.refreshToken || !opts.subreddit) {
  console.error("Missing required auth or subreddit flags.");
  printHelpAndExit(2);
}

opts.dbPath = opts.dbPath || (opts.subreddit ? `./data/sqlite/${opts.subreddit}.db` : "./data/sqlite/reddit.db");
let pgConnected = false;

const parsePgDsn = (dsn) => {
  try {
    const u = new URL(dsn);
    return {
      user: decodeURIComponent(u.username || ""),
      host: u.hostname || "",
      port: u.port || "5432",
      db: (u.pathname || "").replace(/^\//, ""),
      sslmode: u.searchParams.get("sslmode") || "",
    };
  } catch {
    return null;
  }
};

const ensurePgDatabase = async () => {
  if (!opts.pgUrl) return null;

  const PG_MAINTENANCE_DB = process.env.POSTGRES_MAINTENANCE_DB || "postgres";
  const ident = (s) => `"${String(s).replace(/"/g, '""')}"`;

  const buildPgUrlWithDb = (dsn, dbName) => {
    const u = new URL(dsn);
    u.pathname = `/${encodeURIComponent(dbName)}`;
    return u.toString();
  };

  try {
    const c = new PgClient({ connectionString: opts.pgUrl });
    await c.connect();
    return c;
  } catch (e) {
    const msg = e?.message || "";
    const code = e?.code || "";
    if (code !== "3D000" && !/does not exist/i.test(msg)) {
      throw e;
    }
  }

  const target = new URL(opts.pgUrl);
  const targetDb = (target.pathname || "/").replace(/^\//, "");
  const targetUser = decodeURIComponent(target.username || "");

  const adminUrl = buildPgUrlWithDb(opts.pgUrl, PG_MAINTENANCE_DB);
  const admin = new PgClient({ connectionString: adminUrl });
  await admin.connect();
  try {
    try {
      await admin.query(`CREATE DATABASE ${ident(targetDb)} WITH OWNER ${ident(targetUser)}`);
      console.log(`[storage] created database ${targetDb} (owner=${targetUser}) via ${PG_MAINTENANCE_DB}`);
    } catch (ce) {
      if (ce?.code !== "42P04") throw ce;
      console.log(`[storage] database ${targetDb} already exists (race)`);
    }
  } finally {
    await admin.end().catch(() => {});
  }

  const client = new PgClient({ connectionString: opts.pgUrl });
  await client.connect();
  return client;
};

const pgDsnPretty = (dsn) => {
  const p = parsePgDsn(dsn);
  if (!p) return "(invalid DSN)";
  return `${p.user}@${p.host}:${p.port}/${p.db}${p.sslmode ? `?sslmode=${p.sslmode}` : ""}`;
};

const UA = opts.ua;
const TOKEN_URL = "https://www.reddit.com/api/v1/access_token";

const nowSec = () => Math.floor(Date.now() / 1000);
const iso = (s) => new Date(s * 1000).toISOString();
const commentsSeriesBumpedThisRun = new Set();

const parseWhen = (s) => {
  if (!s) return null;
  if (/^\d+$/.test(s)) return Number(s);
  const t = Date.parse(s);
  if (Number.isNaN(t)) throw new Error(`Invalid time: ${s}`);
  return Math.floor(t / 1000);
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const chunk = function* (arr, n) {
  for (let i = 0; i < arr.length; i += n) yield arr.slice(i, i + n);
};

const asInt = (b) => (b ? 1 : 0);
const oneLine = (s) => {
  return (s || "").replace(/\r?\n+/g, " ").replace(/\s+/g, " ").trim();
};

const normTitle = (s) => oneLine(String(s || "").toLowerCase());

const logv = (...args) => { if (opts.verbose) console.log(...args); };
const logKV = (obj) => console.log(Object.entries(obj).map(([k, v]) => `${k}=${v}`).join(" | "));

const safeParseJson = (t) => {
  try {
    return t ? JSON.parse(t) : [];
  } catch {
    return [];
  }
};

const appendSeriesString = (prevText, entry, dedupeKeys = [], max = 0, dedupe = true) => {
  let arr = safeParseJson(prevText);
  const last = arr[arr.length - 1];
  let same = false;
  if (dedupe && last) {
    same = dedupeKeys.every(k => {
      const a = last[k]; const b = entry[k];
      return (a === b) || (a == null && b == null);
    });
  }
  if (!same) {
    arr.push(entry);
    if (max > 0 && arr.length > max) arr = arr.slice(arr.length - max);
  }
  return JSON.stringify(arr);
};

const fetchWithTimeout = async (url, init = {}, timeoutMs = opts.fetchTimeoutMs) => {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: ctrl.signal });
  } finally {
    clearTimeout(t);
  }
};

let tokenState = { access_token: null, expires_at: 0 };

const fetchAccessToken = async () => {
  const form = new URLSearchParams();
  form.set("grant_type", "refresh_token");
  form.set("refresh_token", opts.refreshToken);

  const res = await fetchWithTimeout(TOKEN_URL, {
    method: "POST",
    headers: {
      "Authorization": "Basic " + Buffer.from(`${opts.clientId}:${opts.clientSecret}`).toString("base64"),
      "Content-Type": "application/x-www-form-urlencoded",
      "User-Agent": UA,
    },
    body: form,
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`Token refresh error ${res.status}: ${t}`);
  }

  const j = await res.json();
  tokenState.access_token = j.access_token;
  tokenState.expires_at = Date.now() + (j.expires_in || 3600) * 1000;

  return tokenState.access_token;
};

const getToken = async () => {
  if (tokenState.access_token && Date.now() < tokenState.expires_at - 10_000) {
    return tokenState.access_token;
  }
  return fetchAccessToken();
};

const redditJson = async (url, { method = "GET", body = null } = {}, retry = 0, maxRetries = 6) => {
  const backoff = Math.min(20000, 500 * 2 ** retry) + Math.floor(Math.random() * 400);

  const token = await getToken();
  let res;
  try {
    res = await fetchWithTimeout(url, {
      method,
      headers: {
        "Authorization": `Bearer ${token}`,
        "User-Agent": UA,
        "Accept": "application/json",
        ...(body ? { "Content-Type": "application/json" } : {}),
      },
      body,
    });
  } catch (e) {
    if (retry < maxRetries) {
      if (opts.verbose) console.warn(`Request failed (${e.name || e.message}); retry ${retry + 1}/${maxRetries} in ${backoff}ms: ${url}`);
      await sleep(backoff);
      return redditJson(url, { method, body }, retry + 1);
    }
    throw e;
  }

  if (res.status === 401 || res.status === 403) {
    let text = "";
    try { text = await res.text(); } catch {}
    if (/invalid|expired|unauthorized|not\s*authenticated/i.test(text)) {
      await fetchAccessToken();
      if (retry <= maxRetries) {
        return redditJson(url, { method, body }, retry + 1);
      }
    }
  }

  if (res.status === 429 || res.status >= 500) {
    if (retry < maxRetries) {
      if (opts.verbose) console.warn(`HTTP ${res.status} on ${url}; retry ${retry + 1}/${maxRetries} in ${backoff}ms`);
      await sleep(backoff);
      return redditJson(url, { method, body }, retry + 1);
    }
  }

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`Reddit fetch ${res.status} ${res.statusText} for ${url}: ${t}`);
  }
  return res.json();
};

const sqlite = new Database(opts.dbPath);
sqlite.pragma(`journal_mode = DELETE`);
sqlite.pragma(`synchronous = NORMAL`);
sqlite.pragma(`busy_timeout = 5000`);

sqlite.exec(`
CREATE TABLE IF NOT EXISTS posts (
  id TEXT PRIMARY KEY,
  name TEXT,
  subreddit TEXT,
  title TEXT,
  title_norm TEXT,
  author TEXT,
  distinguished TEXT,
  created_utc INTEGER,
  score INTEGER,
  upvote_ratio REAL,
  num_comments INTEGER,
  url TEXT,              -- subreddit permalink
  external_url TEXT,     -- outbound link if any
  selftext TEXT,
  domain TEXT,
  link_flair_text TEXT,
  is_self INTEGER,
  crosspost_parent TEXT,
  edited INTEGER,
  removed_by_category TEXT,
  locked INTEGER,
  first_seen INTEGER,
  removed_at INTEGER,
  locked_at INTEGER,
  last_checked INTEGER,
  score_series TEXT       -- JSON array of {ts, score, upvote_ratio, num_comments, locked, removed}
);
CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_utc);
CREATE INDEX IF NOT EXISTS idx_posts_flair ON posts(link_flair_text);
CREATE INDEX IF NOT EXISTS idx_posts_domain ON posts(domain);

CREATE TABLE IF NOT EXISTS comments (
  id TEXT PRIMARY KEY,
  name TEXT,
  post_id TEXT,
  parent_id TEXT,
  author TEXT,
  body TEXT,
  score INTEGER,
  created_utc INTEGER,
  edited INTEGER,
  removed_by_category TEXT,
  distinguished TEXT,
  is_submitter INTEGER,
  collapsed_reason TEXT,
  last_checked INTEGER,
  score_series TEXT       -- JSON array of {ts, score}
);
CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(post_id);
`);

let pg = null;
const initPg = async () => {
  if (!opts.pgUrl) {
    console.log("[storage] Postgres disabled (no REDDIT_SCRAPER_PG_URL)");
    return;
  }

  try {
    try {
      pg = await ensurePgDatabase();
    } catch (e1) {
      console.warn(`[storage] PG init first attempt failed: ${e1.message || e1}; retrying in 1000ms`);
      await sleep(1000);
      pg = await ensurePgDatabase();
    }
    
    pg = new PgClient({ connectionString: opts.pgUrl });
    await pg.connect();
    await pg.query(`
CREATE TABLE IF NOT EXISTS posts (
  id TEXT PRIMARY KEY,
  name TEXT,
  subreddit TEXT,
  title TEXT,
  title_norm TEXT,
  author TEXT,
  distinguished TEXT,
  created_utc BIGINT,
  score INT,
  upvote_ratio DOUBLE PRECISION,
  num_comments INT,
  url TEXT,
  external_url TEXT,
  selftext TEXT,
  domain TEXT,
  link_flair_text TEXT,
  is_self BOOLEAN,
  crosspost_parent TEXT,
  edited BIGINT,
  removed_by_category TEXT,
  locked BOOLEAN,
  first_seen BIGINT,
  removed_at BIGINT,
  locked_at BIGINT,
  last_checked BIGINT,
  score_series JSONB
);
CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_utc);
CREATE INDEX IF NOT EXISTS idx_posts_flair ON posts(link_flair_text);
CREATE INDEX IF NOT EXISTS idx_posts_domain ON posts(domain);

CREATE TABLE IF NOT EXISTS comments (
  id TEXT PRIMARY KEY,
  name TEXT,
  post_id TEXT,
  parent_id TEXT,
  author TEXT,
  body TEXT,
  score INT,
  created_utc BIGINT,
  edited BIGINT,
  removed_by_category TEXT,
  distinguished TEXT,
  is_submitter BOOLEAN,
  collapsed_reason TEXT,
  last_checked BIGINT,
  score_series JSONB
);
CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(post_id);
    `);
  } catch (e) {
    console.error("Postgres init failed:", e.message || e);
    pg = null;
    pgConnected = false;
  }
};

const selectPostByIdSql = sqlite.prepare(
  `SELECT removed_by_category, locked, first_seen, removed_at, locked_at, score_series FROM posts WHERE id=?`
);
const selectCommentByIdSql = sqlite.prepare(
  `SELECT score_series FROM comments WHERE id=?`
);

const upsertPostSql = sqlite.prepare(`
INSERT INTO posts (
  id,name,subreddit,title,title_norm,author,distinguished,created_utc,score,upvote_ratio,num_comments,
  url,external_url,selftext,domain,link_flair_text,is_self,crosspost_parent,
  edited,removed_by_category,locked,first_seen,removed_at,locked_at,last_checked,score_series
)
VALUES (
  @id,@name,@subreddit,@title,@title_norm,@author,@distinguished,@created_utc,@score,@upvote_ratio,@num_comments,
  @url,@external_url,@selftext,@domain,@link_flair_text,@is_self,@crosspost_parent,
  @edited,@removed_by_category,@locked,@first_seen,@removed_at,@locked_at,@last_checked,@score_series
)
ON CONFLICT(id) DO UPDATE SET
  title=excluded.title,
  title_norm=excluded.title_norm,
  author=excluded.author,
  distinguished=excluded.distinguished,
  score=excluded.score,
  upvote_ratio=excluded.upvote_ratio,
  num_comments=excluded.num_comments,
  url=excluded.url,
  external_url=excluded.external_url,
  selftext=excluded.selftext,
  domain=excluded.domain,
  link_flair_text=excluded.link_flair_text,
  is_self=excluded.is_self,
  crosspost_parent=excluded.crosspost_parent,
  edited=excluded.edited,
  removed_by_category=excluded.removed_by_category,
  locked=excluded.locked,
  first_seen=COALESCE(posts.first_seen, excluded.first_seen),
  removed_at=COALESCE(posts.removed_at, excluded.removed_at),
  locked_at=COALESCE(posts.locked_at, excluded.locked_at),
  last_checked=excluded.last_checked,
  score_series=excluded.score_series
`);

const upsertCommentSql = sqlite.prepare(`
INSERT INTO comments (id,name,post_id,parent_id,author,body,score,created_utc,edited,removed_by_category,distinguished,is_submitter,collapsed_reason,last_checked,score_series)
VALUES (@id,@name,@post_id,@parent_id,@author,@body,@score,@created_utc,@edited,@removed_by_category,@distinguished,@is_submitter,@collapsed_reason,@last_checked,@score_series)
ON CONFLICT(id) DO UPDATE SET
  author=excluded.author,
  body=excluded.body,
  score=excluded.score,
  edited=excluded.edited,
  removed_by_category=excluded.removed_by_category,
  distinguished=excluded.distinguished,
  is_submitter=excluded.is_submitter,
  collapsed_reason=excluded.collapsed_reason,
  last_checked=excluded.last_checked,
  score_series=excluded.score_series
`);

const mirrorPostPg = async (p) => {
  if (!pg) return;
  try {
    await pg.query(
      `INSERT INTO posts (
         id,name,subreddit,title,title_norm,author,distinguished,created_utc,score,upvote_ratio,num_comments,
         url,external_url,selftext,domain,link_flair_text,is_self,crosspost_parent,
         edited,removed_by_category,locked,first_seen,removed_at,locked_at,last_checked,score_series
       )
       VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
         $12,$13,$14,$15,$16,$17,$18,
         $19,$20,$21,$22,$23,$24,$25,$26::jsonb
       )
       ON CONFLICT (id) DO UPDATE SET
         title=EXCLUDED.title,
         title_norm=EXCLUDED.title_norm,
         author=EXCLUDED.author,
         distinguished=EXCLUDED.distinguished,
         score=EXCLUDED.score,
         upvote_ratio=EXCLUDED.upvote_ratio,
         num_comments=EXCLUDED.num_comments,
         url=EXCLUDED.url,
         external_url=EXCLUDED.external_url,
         selftext=EXCLUDED.selftext,
         domain=EXCLUDED.domain,
         link_flair_text=EXCLUDED.link_flair_text,
         is_self=EXCLUDED.is_self,
         crosspost_parent=EXCLUDED.crosspost_parent,
         edited=EXCLUDED.edited,
         removed_by_category=EXCLUDED.removed_by_category,
         locked=EXCLUDED.locked,
         first_seen=COALESCE(posts.first_seen, EXCLUDED.first_seen),
         removed_at=COALESCE(posts.removed_at, EXCLUDED.removed_at),
         locked_at=COALESCE(posts.locked_at, EXCLUDED.locked_at),
         last_checked=EXCLUDED.last_checked,
         score_series=EXCLUDED.score_series`,
      [
        p.id, p.name, p.subreddit, p.title, p.title_norm, p.author, p.distinguished, p.created_utc,
        p.score, p.upvote_ratio, p.num_comments, p.url, p.external_url, p.selftext, p.domain,
        p.link_flair_text, !!p.is_self, p.crosspost_parent, p.edited, p.removed_by_category, !!p.locked,
        p.first_seen, p.removed_at, p.locked_at, p.last_checked, p.score_series || "[]"
      ]
    );
  } catch (e) {
    console.error(`PG post upsert failed for ${p.id}:`, e.message || e);
  }
};

const mirrorCommentPg = async (c) => {
  if (!pg) return;
  try {
    await pg.query(
      `INSERT INTO comments (id,name,post_id,parent_id,author,body,score,created_utc,edited,removed_by_category,distinguished,is_submitter,collapsed_reason,last_checked,score_series)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)
       ON CONFLICT (id) DO UPDATE SET
         author=EXCLUDED.author,
         body=EXCLUDED.body,
         score=EXCLUDED.score,
         edited=EXCLUDED.edited,
         removed_by_category=EXCLUDED.removed_by_category,
         distinguished=EXCLUDED.distinguished,
         is_submitter=EXCLUDED.is_submitter,
         collapsed_reason=EXCLUDED.collapsed_reason,
         last_checked=EXCLUDED.last_checked,
         score_series=EXCLUDED.score_series`,
      [
        c.id, c.name, c.post_id, c.parent_id, c.author, c.body, c.score, c.created_utc, c.edited,
        c.removed_by_category, c.distinguished, !!c.is_submitter, c.collapsed_reason, c.last_checked,
        c.score_series || "[]"
      ]
    );
  } catch (e) {
    console.error(`PG comment upsert failed for ${c.id}:`, e.message || e);
  }
};

const upsertPostWithTransitions = (row) => {
  const prev = selectPostByIdSql.get(row.id);
  const nowts = nowSec();

  row.first_seen = prev?.first_seen || nowts;

  const prevRemoved = prev?.removed_by_category || null;
  const prevLocked = Number(prev?.locked || 0);

  const justRemoved = (!prevRemoved && row.removed_by_category) ? nowts : null;
  const justLocked = (!prevLocked && row.locked) ? nowts : null;

  row.removed_at = prev?.removed_at || justRemoved || null;
  row.locked_at  = prev?.locked_at  || justLocked  || null;

  const entry = {
    ts: nowts,
    score: row.score ?? null,
    upvote_ratio: row.upvote_ratio ?? null,
    num_comments: row.num_comments ?? null,
    locked: !!row.locked,
    removed: !!row.removed_by_category,
  };
  row.score_series = appendSeriesString(
    prev?.score_series || "[]",
    entry,
    ["score", "upvote_ratio", "num_comments", "locked", "removed"],
    opts.seriesMax,
    opts.seriesDedupePosts
  );

  upsertPostSql.run(row);
  return mirrorPostPg(row);
};

const upsertCommentWithSeries = (c, bumpSeries = true) => {
  const prev = selectCommentByIdSql.get(c.id);
  let seriesText = prev?.score_series || "[]";

  if (bumpSeries) {
    const entry = { ts: nowSec(), score: c.score ?? null };
    seriesText = appendSeriesString(
      seriesText,
      entry,
      ["score"],
      opts.commentSeriesMax,
      opts.commentSeriesDedupe
    );
  }

  c.score_series = seriesText;
  upsertCommentSql.run(c);
  return mirrorCommentPg(c);
};

const fetchNewPage = async (sub, after = null) => {
  const url = new URL(`https://oauth.reddit.com/r/${sub}/new.json?limit=100`);
  if (after) url.searchParams.set("after", after);
  return redditJson(url.toString());
};

const fetchApiInfoPostBatch = async (ids) => {
  const fullnames = ids.map((id) => `t3_${id}`).join(",");
  const url = `https://oauth.reddit.com/api/info?id=${encodeURIComponent(fullnames)}`;
  return redditJson(url);
};

const fetchCommentsForPost = async (id, sort = "confidence") => {
  const url = `https://oauth.reddit.com/comments/${id}.json?sort=${encodeURIComponent(sort)}&limit=500`;
  return redditJson(url);
};

const flattenComments = (listing, postIdBase36) => {
  const commentsRoot = (Array.isArray(listing) && listing[1] && listing[1].data && listing[1].data.children) ? listing[1].data.children : [];
  const out = [];

  const walk = (node) => {
    if (!node || node.kind !== "t1") return;
    const d = node.data || {};
    out.push({
      id: d.id,
      name: d.name || `t1_${d.id}`,
      post_id: postIdBase36,
      parent_id: d.parent_id || null,
      author: d.author || null,
      body: d.body || null,
      score: d.score ?? null,
      created_utc: d.created_utc ?? null,
      edited: (typeof d.edited === "number") ? d.edited : null,
      removed_by_category: d.removed_by_category ?? null,
      distinguished: d.distinguished ?? null,
      is_submitter: asInt(!!d.is_submitter),
      collapsed_reason: d.collapsed_reason || null,
      last_checked: nowSec(),
    });
    if (d.replies && d.replies.data && Array.isArray(d.replies.data.children)) {
      for (const child of d.replies.data.children) walk(child);
    }
  };

  for (const c of commentsRoot) walk(c);
  return out;
};

const fetchCommentsForPosts = async (ids, phaseLabel, secondSort = null) => {
  const idsArr = Array.from(new Set(ids.filter(Boolean)));
  let total = 0;

  const fetchAndMergeForPost = async (postId, sorts) => {
    const map = new Map();
    for (const s of sorts) {
      const listing = await fetchCommentsForPost(postId, s);
      const flat = flattenComments(listing, postId);
      for (const c of flat) map.set(c.id, c);
    }
    for (const c of map.values()) {
      const firstTimeThisRun = !commentsSeriesBumpedThisRun.has(c.id);
      upsertCommentWithSeries(c, firstTimeThisRun);
      commentsSeriesBumpedThisRun.add(c.id);
    }
    return map.size;
  };

  for (const grp of chunk(idsArr, opts.concurrency)) {
    logv(`comments:${phaseLabel} batch=${grp.length} remaining≈${idsArr.length - total}`);
    await Promise.all(grp.map(async (id) => {
      try {
        const sorts = ["confidence", ...(secondSort ? [secondSort] : [])];
        await fetchAndMergeForPost(id, sorts);
        total += 1;
      } catch (e) {
        console.error(`comments:${phaseLabel} fail id=${id}:`, e.message || e);
      }
    }));
  }
  logv(`comments:${phaseLabel} done targets=${idsArr.length}`);
  return { targets: idsArr.length };
};

const idsFromSqliteWindow = (windowStartSec) => {
  const rows = sqlite.prepare(`SELECT id FROM posts WHERE created_utc >= ?`).all(windowStartSec);
  return rows.map(r => r.id);
};

const idsFromPgWindow = async (windowStartSec) => {
  if (!pg) return [];
  try {
    const { rows } = await pg.query(`SELECT id FROM posts WHERE created_utc >= $1`, [windowStartSec]);
    return rows.map(r => r.id);
  } catch (e) {
    console.error("pg window ids fail:", e.message || e);
    return [];
  }
};

const scanNewAndUpsert = async () => {
  const maxBackCutoff = nowSec() - opts.daysBack * 86400;
  const startCutoff = Math.max(parseWhen(opts.start) ?? 0, maxBackCutoff);
  const endCutoff = parseWhen(opts.end) ?? Infinity;

  if (endCutoff < startCutoff) {
    logv("endCutoff < startCutoff; nothing to do");
    return { pages: 0, postsSeen: 0, commentTargets: [] };
  }

  let after = null;
  let stopPaging = false;
  let pages = 0;
  let postsSeen = 0;
  const commentTargets = [];

  while (true) {
    const page = await fetchNewPage(opts.subreddit, after);
    const children = (page && page.data && page.data.children) ? page.data.children : [];
    if (!children.length) break;

    for (const child of children) {
      if (child.kind !== "t3") continue;
      const d = child.data;

      const cu = Number(d.created_utc || 0);
      if (cu < startCutoff) { stopPaging = true; break; }
      if (cu > endCutoff) continue;

      const permalink = d.permalink ? `https://reddit.com${d.permalink}` : null;
      const isSelf = !!d.is_self;
      const external_url = !isSelf
        ? (d.url_overridden_by_dest || (d.url && !String(d.url).startsWith("/r/") ? d.url : null))
        : null;

      const row = {
        id: d.id,
        name: d.name || `t3_${d.id}`,
        subreddit: d.subreddit,
        title: d.title ?? null,
        title_norm: normTitle(d.title),
        author: d.author ?? null,
        distinguished: d.distinguished || null,
        created_utc: cu,
        score: Number.isFinite(d.score) ? d.score : null,
        upvote_ratio: (typeof d.upvote_ratio === "number") ? d.upvote_ratio : null,
        num_comments: Number.isFinite(d.num_comments) ? d.num_comments : null,

        url: permalink,
        external_url,

        selftext: d.selftext ?? null,
        domain: d.domain || null,
        link_flair_text: d.link_flair_text || null,
        is_self: asInt(isSelf),
        crosspost_parent: d.crosspost_parent || null,
        edited: (typeof d.edited === "number") ? d.edited : null,
        removed_by_category: d.removed_by_category ?? null,
        locked: asInt(!!d.locked),
        first_seen: null,
        removed_at: null,
        locked_at: null,
        last_checked: nowSec(),
        score_series: null,
      };

      await upsertPostWithTransitions(row);
      postsSeen++;
      commentTargets.push(d.id);

      console.log(
        `[${iso(cu)}] ${d.id} "${oneLine(d.title || "")}" flair="${d.link_flair_text || ""}" domain="${d.domain || ""}" removed=${row.removed_by_category} locked=${!!row.locked}`
      );

      if (opts.maxPosts && postsSeen >= opts.maxPosts) { stopPaging = true; break; }
    }

    pages++;
    if (opts.maxPages && pages >= opts.maxPages) break;
    if (stopPaging) break;
    after = page?.data?.after || null;
    if (!after) break;
  }

  if (!opts.noComments && commentTargets.length) {
    const targets = opts.initialCommentLimit ? commentTargets.slice(0, opts.initialCommentLimit) : commentTargets;
    await fetchCommentsForPosts(targets, "initial");
  }

  return { pages, postsSeen, commentTargets };
};

const recheckWindowAndUpsert = async () => {
  const windowStart = nowSec() - opts.daysBack * 86400;
  const sqliteIds = idsFromSqliteWindow(windowStart);
  const pgIds = await idsFromPgWindow(windowStart);
  const union = Array.from(new Set([...sqliteIds, ...pgIds]));
  const recheckIds = opts.maxPosts ? union.slice(0, opts.maxPosts) : union;

  logv(`recheck: sqlite=${sqliteIds.length} pg=${pgIds.length} union=${union.length} use=${recheckIds.length}`);

  let postUpdates = 0;
  let postBatches = 0;

  for (const grp of chunk(recheckIds, 100)) {
    try {
      const info = await fetchApiInfoPostBatch(grp);
      const posts = (info && info.data && info.data.children) ? info.data.children : [];
      for (const p of posts) {
        if (p.kind !== "t3") continue;
        const d = p.data;

        const permalink = d.permalink ? `https://reddit.com${d.permalink}` : null;
        const isSelf = !!d.is_self;
        const external_url = !isSelf
          ? (d.url_overridden_by_dest || (d.url && !String(d.url).startsWith("/r/") ? d.url : null))
          : null;

        const row = {
          id: d.id,
          name: d.name || `t3_${d.id}`,
          subreddit: d.subreddit,
          title: d.title ?? null,
          title_norm: normTitle(d.title),
          author: d.author ?? null,
          distinguished: d.distinguished || null,
          created_utc: Number(d.created_utc || 0),
          score: Number.isFinite(d.score) ? d.score : null,
          upvote_ratio: (typeof d.upvote_ratio === "number") ? d.upvote_ratio : null,
          num_comments: Number.isFinite(d.num_comments) ? d.num_comments : null,

          url: permalink,
          external_url,

          selftext: d.selftext ?? null,
          domain: d.domain || null,
          link_flair_text: d.link_flair_text || null,
          is_self: asInt(isSelf),
          crosspost_parent: d.crosspost_parent || null,
          edited: (typeof d.edited === "number") ? d.edited : null,
          removed_by_category: d.removed_by_category ?? null,
          locked: asInt(!!d.locked),
          first_seen: null,
          removed_at: null,
          locked_at: null,
          last_checked: nowSec(),
          score_series: null,
        };
        await upsertPostWithTransitions(row);
        postUpdates++;
      }
      postBatches++;
      logv(`recheck: batch=${postBatches} updated=${postUpdates}`);
    } catch (e) {
      console.error("recheck api/info batch fail:", e.message || e);
    }
  }

  if (!opts.noRecheckComments) {
    const targets = opts.recheckCommentLimit ? recheckIds.slice(0, opts.recheckCommentLimit) : recheckIds;
    if (targets.length) {
      await fetchCommentsForPosts(targets, "recheck", "new");
    }
  }

  return { postUpdates, postBatches, ids: recheckIds.length };
};

const reportHeuristics = () => {
  console.log("=== Heuristic Report (last window) ===");
  const windowStart = nowSec() - opts.daysBack * 86400;

  try {
    const rows = sqlite.prepare(`
      SELECT link_flair_text AS flair,
             COUNT(*) AS posts,
             SUM(removed_by_category IS NOT NULL) AS removed,
             ROUND(100.0 * SUM(removed_by_category IS NOT NULL)/COUNT(*), 1) AS pct_removed,
             SUM(locked=1) AS locked_cnt,
             ROUND(100.0 * SUM(locked=1)/COUNT(*), 1) AS pct_locked
      FROM posts
      WHERE created_utc >= ?
      GROUP BY link_flair_text
      HAVING posts >= 5
      ORDER BY pct_removed DESC, posts DESC
      LIMIT 20
    `).all(windowStart);
    console.log("--- Removal/Lock rate by flair (>=5 posts) ---");
    for (const r of rows) {
      console.log(`flair="${r.flair || ""}" posts=${r.posts} removed=${r.removed} (${r.pct_removed}%) locked=${r.locked_cnt} (${r.pct_locked}%)`);
    }
  } catch (e) { console.error("report flair:", e.message || e); }

  try {
    const rows = sqlite.prepare(`
      SELECT domain,
             COUNT(*) AS posts,
             SUM(removed_by_category IS NOT NULL) AS removed,
             ROUND(100.0 * SUM(removed_by_category IS NOT NULL)/COUNT(*), 1) AS pct_removed
      FROM posts
      WHERE created_utc >= ?
      GROUP BY domain
      HAVING posts >= 5
      ORDER BY pct_removed DESC, posts DESC
      LIMIT 20
    `).all(windowStart);
    console.log("--- Removal rate by domain (>=5 posts) ---");
    for (const r of rows) {
      console.log(`domain="${r.domain || ""}" posts=${r.posts} removed=${r.removed} (${r.pct_removed}%)`);
    }
  } catch (e) { console.error("report domain:", e.message || e); }

  try {
    const rows = sqlite.prepare(`
      SELECT link_flair_text AS flair,
             COUNT(*) AS removed_posts,
             ROUND(AVG(CASE WHEN removed_at IS NOT NULL THEN (removed_at - first_seen) END), 0) AS avg_latency_s
      FROM posts
      WHERE created_utc >= ? AND removed_at IS NOT NULL
      GROUP BY link_flair_text
      HAVING removed_posts >= 3
      ORDER BY avg_latency_s ASC
      LIMIT 20
    `).all(windowStart);
    console.log("--- Avg removal latency by flair (>=3 removed) ---");
    for (const r of rows) {
      console.log(`flair="${r.flair || ""}" removed_posts=${r.removed_posts} avg_latency_s=${r.avg_latency_s}`);
    }
  } catch (e) { console.error("report latency:", e.message || e); }

  try {
    const rows = sqlite.prepare(`
      SELECT p.id, p.title, p.link_flair_text AS flair,
             SUM(CASE WHEN c.created_utc BETWEEN p.created_utc AND (p.created_utc + 7200) AND c.removed_by_category IS NOT NULL THEN 1 ELSE 0 END) AS removed_2h,
             SUM(CASE WHEN c.created_utc BETWEEN p.created_utc AND (p.created_utc + 7200) THEN 1 ELSE 0 END) AS total_2h
      FROM posts p
      LEFT JOIN comments c ON c.post_id = p.id
      WHERE p.created_utc >= ?
      GROUP BY p.id
      HAVING total_2h >= 10
      ORDER BY (1.0 * removed_2h)/total_2h DESC
      LIMIT 20
    `).all(windowStart);
    console.log("--- Highest early (2h) comment removal rate (>=10 comments in 2h) ---");
    for (const r of rows) {
      const pct = r.total_2h ? Math.round(1000 * (r.removed_2h / r.total_2h)) / 10 : 0;
      console.log(`post=${r.id} flair="${r.flair || ""}" early_removed=${r.removed_2h}/${r.total_2h} (${pct}%) title="${oneLine(r.title || "")}"`);
    }
  } catch (e) { console.error("report comments:", e.message || e); }

  console.log("=== End Heuristic Report ===");
};

const shutdown = async () => {
  try { if (pg) await pg.end(); } catch {}
  try { if (sqlite && sqlite.open) sqlite.close(); } catch {}
};

const main = async () => {
  console.log(`[startup] sqlite=${opts.dbPath}`);
  console.log(`[startup] postgres=${opts.pgUrl ? pgDsnPretty(opts.pgUrl) : "disabled"}`);

  await initPg();

  let pages = 0, postsSeen = 0;
  let recheckPostUpdates = 0, recheckBatches = 0, recheckIds = 0;

  try {
    const s = await scanNewAndUpsert();
    pages = s.pages; postsSeen = s.postsSeen;

    const re = await recheckWindowAndUpsert();
    recheckPostUpdates = re.postUpdates;
    recheckBatches = re.postBatches;
    recheckIds = re.ids;

    if (opts.report) reportHeuristics();
  } catch (e) {
    console.error("fatal:", e.message || e);
    process.exitCode = 1;
  } finally {
    await shutdown();
  }

  logKV({
    Summary: "",
    pages,
    storage_mode: pgConnected ? "sqlite+pg" : "sqlite-only",
    posts_seen: postsSeen,
    recheck_ids: recheckIds,
    recheck_post_batches: recheckBatches,
    recheck_post_updates: recheckPostUpdates,
  });
};

process.on("SIGINT", async () => { await shutdown(); process.exit(130); });
process.on("SIGTERM", async () => { await shutdown(); process.exit(143); });

main().catch(async (e) => {
  console.error("unhandled:", e);
  await shutdown();
  process.exit(1);
});
