# botv2 — Triumph-sourced KTS Collectr bot (ground-up rebuild)

Fresh rebuild of the Discord bot. **One architectural change** from v1
(`../kts_collectr_bot.py`): prices come from the **KTS Portal**, not the CardLadder
scraper.

- **v1:** Discord → CardLadder comp helper (Brave + a vision model, ~tens of
  seconds/cert) → bot's own `classify_psa_comp` + three hand-synced band tables.
- **v2:** Discord → **`POST /bot/price`** on the portal → priced rows back
  (sub-second/cert). The portal runs Triumph's scanner API **and the same
  `offer.js` the app uses**, so each row carries comp value, CardLadder
  confidence, **and** the BUY/PASS decision + offer.

**The bot holds no Triumph token, no CardLadder client, and no rules engine.**
The Triumph refresh token never leaves the portal; the bot authenticates to
`/bot/price` with a separate `BOT_API_KEY` shared secret. Kevin's payout rate
(`payoutCents`/`memberMultiplier`) is dropped at the portal and defensively
stripped again here, so it can never reach a customer sheet or message.

## Why this is better than v1
- **Instant** — a 50–100 cert lot returns in seconds, not minutes.
- **1:1 with the app** — identical comp values *and* identical rules (same
  `offer.js`), so bot and portal can't drift. The v1 "rules mirror" is gone.
- **Confidence gating** — the bot finally has `clConfidence`, so it honors the
  same `minConf` the app does instead of disclaiming it.
- **Much simpler** — the entire scraper/retry/cache/health-monitor subsystem and
  the three band tables are deleted.

## Run / test
```
# from the kts-bot/ directory
python -m botv2.bot                 # live (needs DISCORD_TOKEN + BOT_API_KEY + portal /bot/price)
BOTV2_PRICE_MOCK=1 python -m botv2.bot   # offline: mock prices, exercise the Discord flow
```
`portal_client.price_certs([...])` is the single price call; `BOTV2_PRICE_MOCK=1`
makes it return deterministic fake rows so the bot is testable before the portal
endpoint is deployed.

## `/bot/price` contract (owned by the portal side)
```
POST {PORTAL_BASE_URL}/bot/price   header x-bot-key: <BOT_API_KEY>
body {"certs":[...≤200], "customer"?: "<discord_username>", "rateOverride"?: {...}}
  -> {"rows":[ row, ... ] (one per cert, in order), "customerMatched": bool}
row = {certId, grader, grade, cardTitle, imageUrl, category, marketValue, lastSale,
       status, rejectionReason, clConfidence, decision, offer, reason, vip}
```
VIP is single-source in the portal: `customer` = the raw Discord username (Kevin
requires it at signup); the portal matches its VIP section case-insensitively and
applies that seller's rates (per-row `vip`). Precedence: explicit `rateOverride`
> `customer` VIP > public. `payoutCents`/`memberMultiplier` never leave the portal.

## Module layout (all ✅ — slab flow feature-complete)
| module | role |
|---|---|
| `config.py` | env + tunables + confidential-field guard |
| `portal_client.py` | **the price source** — `price_certs()` → `PricedRow`s (customer/VIP/override) |
| `pricing.py` | aggregate rows → quote + customer/Kevin messages (no band logic) |
| `certs.py` | cert + tracking-number extraction (verbatim v1 parser) |
| `sheets.py` | buying-sheet create + fill (portal offer as a number) + tracking save |
| `tickets.py` | overflow-proof ticket-channel registry |
| `welcome.py` | one-time greeting |
| `lots.py` | lot state + minimums + lane totals + redeploy rebuild-from-sheets |
| `shipping.py` | VA/Doral routing + ship/proceed gate |
| `vip.py` | rare manual override (VIP is portal-sourced by username) |
| `tracking.py` | record accepted lots → owed/tickets stores, tracking #, mark paid |
| `payout_digest.py` | Friday 9am ET unpaid-owed digest DM to Kevin |
| `commands.py` | `!ping` / `!help` / `!adopt` / `!paid` |
| `webserver.py` | /health + /owed /tickets /leaderboard /links (binds $PORT) |
| `state.py` | atomic JSON persistence |
| `bot.py` | Discord client, `on_message` router, entry |
| `selftest.py` | the test suite — `python -m botv2.selftest` |

Rebuilt fresh using v1 as the reference so no battle-tested edge case was lost
(state recovery, CGC, overflow-proof tickets, min-lot holds, cert-parser guards).

**Dropped (Kevin, Nov 4):** Pokémon-singles Collectr CSV (not buying them now) and
`!summary` (he exports a CSV from the app). Pokémon slot quotas are moot — the
portal does allocation.

## Decisions (settled)
- **Sheet payout**: the portal's number written straight in (H = offer rate); no
  local band formula — the portal is the source of truth. Editing a comp = re-quote.
- **VIP**: single-source in the portal, keyed by the raw Discord username the bot
  sends; the portal applies it. A local manual override remains for a one-off.

## Still open
- Integration-test against the live `/bot/price` (needs the portal endpoint built
  on Kevin's direct go). Until then v2 runs on the mock.
- Deploy wiring (Procfile/requirements) when Kevin moves v2 off local.

## Backups of v1 (the months of proven work)
- git tag `v1-stable-2026-11-04`, branch `v1-backup-2026-11-04`
- full copy: `../../kts-bot-BACKUP-v1-2026-11-04/`
- `../kts_collectr_bot.py` stays the LIVE bot on `main`; v2 lives on branch
  `bot-v2-triumph` and does not touch the live deploy until Kevin says go.
