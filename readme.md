# Reddit Removal/Lock Tracker — Docker Quickstart

Tracks **post/comment removals**, **locks**, and **karma-over-time** for any subreddit. Always writes to **SQLite** and can **mirror to Postgres**.

---

## Features
- One-shot run (cron-friendly, e.g., every 10 minutes)
- OAuth (refresh token) with Node ≥ 22 (global `fetch`)
- Scrapes `/r/<sub>/new` + comments; back-checks recent days for removals/locks
- SQLite schema auto-migration; optional Postgres mirroring
- Heuristic summaries (flair/domain removal rates, latency, early comment removals)
- Score/time series for posts & comments (configurable)
- Removed subreddit moderator posts/comments and activity tracking ability from this script to prevent stalking and abuse.

---

## TL;DR (Docker)
```bash
# 1) Copy env template and fill it in
cp env.example .env

# 2) Get your Reddit refresh token (see section below), then paste into .env

# 3) Build images
docker compose build

# 4) (Optional) Start Postgres for mirroring
docker compose up -d postgres

# 5) Run the scraper once (writes SQLite; mirrors to PG if configured)
docker compose run --rm reddit-scraper

# 6) Schedule every 10 minutes (cron on the host)
# */10 * * * * cd /path/to/project && docker compose run --rm reddit-scraper >> /var/log/reddit-scraper.log 2>&1
```

> On startup the scraper logs which storage is used, e.g. `storage_mode=sqlite-only` or `sqlite+pg`.

---

## 1) Create a Reddit OAuth App
1. Go to <https://old.reddit.com/prefs/apps>
2. **Create app** → **web app**
3. Set **redirect uri**: `http://127.0.0.1:8910/callback`
4. Note the **client id** and **client secret**

---

## 2) Get a Refresh Token
The project contains `src/get_refresh_token.js`.

**Local (Node ≥ 22):**
```bash
REDDIT_CLIENT_ID=YOUR_ID \
REDDIT_CLIENT_SECRET=YOUR_SECRET \
node src/get_refresh_token.js
```
Open the printed URL, approve, then copy the **REFRESH TOKEN** from the terminal.

**Dockerized helper:**
```bash
docker run --rm -it -p 8910:8910 \
  -e REDDIT_CLIENT_ID=YOUR_ID \
  -e REDDIT_CLIENT_SECRET=YOUR_SECRET \
  -v "$PWD/src:/app/src" -w /app \
  node:22 node src/get_refresh_token.js
```

Paste the token into `.env` as `REDDIT_SCRAPER_REFRESH_TOKEN`.

---

## 3) Configure `.env`
Create it from the template and edit values:
```bash
cp env.example .env
```
Key entries (see `env.example` for the full list):
```dotenv
# Reddit
REDDIT_CLIENT_ID=...
REDDIT_CLIENT_SECRET=...
REDDIT_SCRAPER_REFRESH_TOKEN=...

# Scraper
REDDIT_SCRAPER_SUBREDDIT=all
REDDIT_SCRAPER_DAYS_BACK=3
REDDIT_SCRAPER_DB_PATH="/data/${REDDIT_SCRAPER_SUBREDDIT}.db"   # Ensure your compose volume maps /data
REDDIT_SCRAPER_CONCURRENCY=5

# Postgres (optional)
POSTGRES_USER=scraperadmin
POSTGRES_PASSWORD=change_me
POSTGRES_DB=${REDDIT_SCRAPER_SUBREDDIT}
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
PG_SSLMODE=disable
```
> Ensure your compose mounts `./data:/data` (or adjust to match your chosen path) so the SQLite file persists on the host.

---

## 4) Docker Compose Commands

**Build:**
```bash
docker compose build
```

**Start Postgres (optional):**
```bash
docker compose up -d postgres
```

**Run scraper once:**
```bash
docker compose run --rm reddit-scraper
```

**Follow logs:**
```bash
docker compose logs -f reddit-scraper
```

**pgAdmin (optional, if present):**
```bash
docker compose up -d pgadmin
# open http://localhost:8080
```

**Cron (every 10 minutes):**
```cron
*/10 * * * * cd /path/to/project && docker compose run --rm reddit-scraper >> /var/log/reddit-scraper.log 2>&1
```

---

## 5) Inspect Data

**SQLite (always written):**
```bash
sqlite3 ./data/${REDDIT_SCRAPER_SUBREDDIT}.db ".tables"
sqlite3 ./data/${REDDIT_SCRAPER_SUBREDDIT}.db "select count(*) from posts; select count(*) from comments;"
```

**Postgres (if enabled):**
```bash
docker compose exec postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "\\dt"
docker compose exec postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "select count(*) from public.posts; select count(*) from public.comments;"
```

---

## 6) Useful Overrides
These can be set via environment variables or CLI flags.

**Examples:**
```bash
# limit scope during testing
docker compose run --rm reddit-scraper \
  node index.js --subreddit all --days-back 4 --verbose \
  --max-posts 200 --initial-comment-limit 100 --recheck-comment-limit 200 \
  --report
```

**Full CLI (for reference):**
```
index.js --client-id <id> --client-secret <secret> --refresh-token <tok> \
         --subreddit <name> [--days-back 4] [--start <ISO|epoch>] [--end <ISO|epoch>] \
         [--db <sqlite path>] [--pg-url <postgres dsn>] \
         [--concurrency 2] [--max-pages N] [--max-posts N] \
         [--no-comments] [--no-recheck-comments] \
         [--initial-comment-limit N] [--recheck-comment-limit N] \
         [--fetch-timeout-ms 20000] [--ua "reddit-crypt/3.1 by script"] \
         [--series-max 288] [--no-series-dedupe-posts] \
         [--comment-series-max 288] [--comment-series-dedupe] \
         [--report] [--verbose] [--help|-h]
```

**Environment (the app reads):**
```js
REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_SCRAPER_REFRESH_TOKEN,
REDDIT_SCRAPER_SUBREDDIT, REDDIT_SCRAPER_DAYS_BACK,
REDDIT_SCRAPER_START, REDDIT_SCRAPER_END,
REDDIT_SCRAPER_DB_PATH, REDDIT_SCRAPER_PG_URL,
REDDIT_SCRAPER_CONCURRENCY, REDDIT_SCRAPER_MAX_PAGES, REDDIT_SCRAPER_MAX_POSTS,
REDDIT_SCRAPER_NO_COMMENTS, REDDIT_SCRAPER_NO_RECHECK_COMMENTS,
REDDIT_SCRAPER_INITIAL_COMMENT_LIMIT, REDDIT_SCRAPER_RECHECK_COMMENT_LIMIT,
REDDIT_SCRAPER_FETCH_TIMEOUT_MS, REDDIT_SCRAPER_REPORT,
REDDIT_SCRAPER_SERIES_MAX, REDDIT_SCRAPER_SERIES_DEDUPE_POSTS,
REDDIT_SCRAPER_COMMENT_SERIES_MAX, REDDIT_SCRAPER_COMMENT_SERIES_DEDUPE,
REDDIT_SCRAPER_VERBOSE, REDDIT_SCRAPER_UA
```

---

## Docker push (self hosted repo):
```
docker build .

docker tag reddit-scraper DOMAIN.xyz/YOUR_USERNAME/reddit-scraper:latest
docker tag reddit-scraper DOMAIN.xyz/YOUR_USERNAME/reddit-scraper:v0.0.X

docker push DOMAIN.xyz/YOUR_USERNAME/reddit-scraper:latest
docker push DOMAIN.xyz/YOUR_USERNAME/reddit-scraper:v0.0.X
```

---

## 7) Troubleshooting
- **No tables in Postgres**: verify `REDDIT_SCRAPER_PG_URL` (compose builds it from `POSTGRES_*`) and that `POSTGRES_DB` matches what you open in pgAdmin. Run the scraper once; it creates tables.
- **`relation "posts" does not exist`**: refresh pgAdmin (**Schemas → public → Tables**) after a successful run.
- **Password special chars**: if hand-writing a DSN, URL-encode the password. Using the compose-built DSN avoids this.
- **Rate limits**: the scraper backs off with jitter on 429/5xx; refreshes token on 401/403.

---

## 8) Notes on Behavior
- Stores permalink in `url` and outbound link (if any) in `external_url` with `domain` extracted
- Tracks `removed_by_category`, `locked`, and transition timestamps (`first_seen`, `removed_at`, `locked_at`)
- Post series dedupes adjacent identical points by default; comment series appends once **per run**
- Heuristics: flair/domain removal rates, avg removal latency, early comment removal ratio


# License:

MIT