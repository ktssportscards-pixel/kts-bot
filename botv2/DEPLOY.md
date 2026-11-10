# botv2 — Deploy guide (Railway)

> ## ⚠️ Current state — read this before anything below (updated 2026-11-10)
> **botv2 IS the live production bot.** Since the 2026-11-04 cutover the existing Railway
> `worker` service builds branch **`bot-v2-triumph`** with config `botv2/railway.json`
> (`python -m botv2`) on the same Discord bot v1 used. v1 (`kts_collectr_bot.py`, the root
> `Procfile`, branch `main`) is dormant rollback code — nothing below about "v1 keeps
> running" or "a second service with its own token" describes today.
>
> - **A push to `bot-v2-triumph` that touches `botv2/**` redeploys the live bot** — docs and
>   tests included (`watchPatterns`). The restart wipes the in-memory lots, and messages sent
>   while it restarts are not replayed. `.github/workflows/canary.yml` then smoke-tests it.
> - **`botv2/railway.json` must stay valid.** As noted further down, if Railway can't read it
>   the service falls back to the root `Procfile` — v1's code on the live bot's token.
> - The rest of this guide was written for the original two-service plan and is kept for
>   history. **Do not follow these parts of it on the live service:**
>   - *"set `BOTV2_PRICE_MOCK=1`, deploy … for a smoke test"* — on the live bot that sends real
>     customers FAKE quotes. (The bot now DMs Kevin at startup if it boots that way, and
>     `/status.flags.price_mock` shows it.) Test with `python -m botv2.selftest` instead.
>   - *"remove `DISCORD_TOKEN` to take v2 offline"* — `config.py` reads `DISCORD_BOT_TOKEN`
>     first, so that does nothing. Roll back by redeploying a previous good build, or by
>     pointing the service back at `main` (v1).
>   - *"New service … v2's own bot token … a different Discord application"* — do not create
>     a second service or a second bot.
>   - The `/owed`, `/tickets`, `/leaderboard`, `/links` endpoints no longer exist; see
>     `portal-interface/portal-calls-bot.md` for the real list (incl. `POST /void`).
> - Never run the bot locally with the production token: it would act in the live server.

v2 runs as a **second Railway service** off the **same repo** (`kts-bot`) that v1
already deploys from. It is a separate bot (its own Discord application + token).

## ⚠️ Read first — v1 and v2 are NOT automatically isolated

Build + git are cleanly separated (v1's root `Procfile`/`requirements.txt` are
untouched; `botv2/railway.json` is read only by the v2 service). **But two real
cross-effects must be handled before v1 and v2 ever run at the same time:**

1. **They share v1's live Google Drive folder, Apps Script, and template by default.**
   `config.py` defaults `PSA_FOLDER_ID`, `APPS_SCRIPT_URL`, and `TEMPLATE_SHEET_ID`
   to v1's exact values, and both bots create `"<user> discord <date>"` buying
   sheets in that one folder. v1's folder-wide sweeps (the boss CSV, the
   customer-lookup that runs in v1's **live** message handler, and the stalled-ticket
   requote) have **no owner filter**, so they would read v2's sheets as their own and
   corrupt live v1 operations.
   → **Fix (required before concurrent run):** give v2 its **own** `PSA_FOLDER_ID`
   (a new Drive folder, shared with the same service-account email). The bot passes
   `folder_id` to the Apps Script per call, so this env override alone fully isolates
   v2's output — no Apps Script change needed. **Safety net:** if `PSA_FOLDER_ID` is
   left at v1's folder, v2 **refuses** to create buying sheets (quoting still works)
   rather than contaminating v1 — so a forgotten env can't cause the corruption, it
   just disables sheets until you set it. (If instead you turn v1 **off** before v2
   goes live and never roll back, set `ALLOW_SHARED_PSA_FOLDER=1` to share v1's folder
   — your call.)

2. **Merging `bot-v2-triumph` → `main` force-redeploys the LIVE v1 service.** Both
   services deploy from `main`, and Railway redeploys every service on any push to
   its branch. v1 has no volume, so that restart wipes v1's in-memory/ephemeral state
   and rebuilds it on unpinned deps.
   → **Fix (required before the merge):** in the **v1** Railway service, set
   **Settings → Watch Paths** to exclude `botv2/**` (e.g. watch `!botv2/**`). v2's own
   `railway.json` already scopes its watch to `botv2/**`. (Separately worth doing for
   v1: add a volume + pin its `requirements.txt`, since any push to main restarts it
   today — pre-existing, not caused by v2.)

Everything below assumes you've decided how to handle those two.

---

## What's in this folder for deploy

| File | Purpose |
|------|---------|
| `railway.json` | v2's config-as-code: build + start command + health check + watch paths. |
| `requirements.txt` | v2's exact deps (`discord.py`, `aiohttp`, `gspread`, `google-auth`, `google-api-python-client`, `tzdata`). No `anthropic`/`pandas`. |
| `.env.example` | Every env var, marked required / important / optional. |
| `DEPLOY.md` | This file. |

v2 must run with the **repo root as the working directory** — it's a package
launched as `python -m botv2.bot` and uses relative imports (`from . import …`).
So the Railway service's Root Directory stays the repo root (the default), **not**
`botv2/`. `railway.json` lives in `botv2/` only as a per-service config path; the
build still happens from the root.

> **These files must be committed to the deployed branch.** Railway builds committed
> content only — if `botv2/railway.json` isn't in the commit it deploys, the v2
> service silently falls back to the root `Procfile` and runs **v1's** code. They are
> committed on `bot-v2-triumph`; make sure they reach whatever branch the v2 service
> builds from.

---

## One-time Railway setup

1. **New service → Deploy from GitHub repo →** `ktssportscards-pixel/kts-bot`.
2. **Settings → Config-as-code / "Railway Config File":** set to `botv2/railway.json`.
   (Leave Root Directory empty = repo root. Do **not** set it to `botv2`.)
3. **Settings → Branch:** `bot-v2-triumph` for a staged test, or `main` at go-live
   (after the Watch-Paths fix above).
4. **Add a Volume** (Settings → Volumes) mounted at `/data` so ticket / owed /
   tracking state survives redeploys.
5. **Variables →** add the env vars below (including v2's own `PSA_FOLDER_ID`).
6. **Generate a Domain** (Settings → Networking) so Railway can reach `/health`.
7. **Discord dev portal →** enable the **Message Content** and **Server Members**
   privileged intents for v2's bot app. If either is off, `bot.run()` raises and the
   service never goes green.
8. Deploy. The health check waits for the bot to log in to Discord and bind `$PORT`;
   a health-check timeout almost always means a bad `DISCORD_TOKEN` or a missing intent.

---

## Environment variables

**Required** (bot won't function correctly without these):

| Var | Notes |
|-----|-------|
| `DISCORD_TOKEN` | v2's **own** bot token — a different Discord application than v1. |
| `KEVIN_USER_ID` | Your numeric Discord user id (DMs + ticket detection; 0 → all Kevin DMs silently no-op). |
| `BOT_API_KEY` | `x-bot-key` secret for the portal's `/bot/price`. Must match the portal's value. Not the Triumph token. |
| `PSA_FOLDER_ID` | **Set to v2's OWN Drive folder** (see isolation note #1). If left at v1's folder, v2 refuses to create sheets (tripwire) rather than corrupting v1. |

**Required for buying-sheet creation** (the bot still boots + quotes without it; only
sheet output degrades, and it DMs you on failure):

| Var | Notes |
|-----|-------|
| `GOOGLE_CREDENTIALS_JSON` | Service-account creds as inline JSON (prod). Locally you may set `GOOGLE_CREDENTIALS_FILE` instead. |

**Important in prod:**

| Var | Notes |
|-----|-------|
| `DATA_DIR` | Set to `/data` (your mounted volume). Without it, state is wiped every deploy. |

**Optional** — `config.py` has working defaults; override only to change behavior:
`PORTAL_BASE_URL` (defaults to `https://portal.ktscollectibles.com`),
`GOOGLE_CREDENTIALS_FILE` (local-dev creds path), `APPS_SCRIPT_URL`,
`TEMPLATE_SHEET_ID`, `SPORTS_OP_MIN_LOT`, `POKEMON_SINGLES_MIN_LOT`, `MIN_LOT_VALUE`,
`MIN_SLAB_COUNT`, `POKEMON_LANE_OPEN`, `BOTV2_PRICE_MOCK`, `ALLOW_SHARED_PSA_FOLDER`
(set to `1` only to intentionally share v1's folder once v1 is retired). **`PORT` is
injected by Railway — never set it.**

---

## Verify a deploy

- **Health:** open `https://<your-domain>/health` → should return `ok`.
  Other read endpoints: `/owed`, `/tickets`, `/leaderboard`, `/links`.
- **Logs:** you should see `[web] listening on 0.0.0.0:<port>` and the Discord
  ready line.
- **Pricing smoke test without the live endpoint:** set `BOTV2_PRICE_MOCK=1`,
  deploy, drop a few cert numbers in a test ticket — you'll get mock quotes,
  proving the Discord↔sheet↔DM flow end to end. **Unset it before go-live.**
- If `/bot/price` is reachable but returns a 404/HTML (e.g. mid-rollout), the bot
  now tells the customer it's "briefly unavailable" and pages you — it won't silently
  drop the quote.

---

## Go-live sequence (when the portal's `/bot/price` is ready)

1. Portal side: `/bot/price` live, `BOT_API_KEY` identical on both sides.
2. v2 service: `BOTV2_PRICE_MOCK` unset, `BOT_API_KEY` set, volume mounted, v2's own
   `PSA_FOLDER_ID` set.
3. v1 service: Watch Paths exclude `botv2/**` (so the merge won't restart v1).
4. Merge `bot-v2-triumph` → `main` (gated on Kevin's OK), or point the v2 service at
   `bot-v2-triumph` for a final staged test.
5. Watch the health check go green, then run one real cert list through a ticket.
6. v1 keeps running untouched; retire it only once v2 is proven.

## Rollback

Railway → the v2 service → Deployments → **Redeploy** a previous good build, or
remove `DISCORD_TOKEN` to take v2 offline instantly. v1 is unaffected either way.
