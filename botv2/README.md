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
body {"certs":[...]}  (<=200)   ->   {"rows":[ row, ... ]}  (one per cert, in order)
row = {certId, grader, grade, cardTitle, imageUrl, category, marketValue,
       lastSale, status, rejectionReason, clConfidence, decision, offer, reason}
```

## Module layout
| module | status | role |
|---|---|---|
| `config.py` | ✅ | env + tunables + confidential-field guard |
| `portal_client.py` | ✅ | **the price source** — `price_certs()` → `PricedRow`s |
| `pricing.py` | ⏳ | aggregate rows into a quote (group by category, totals) — no band logic |
| `sheets.py` | ⏳ | buying-sheet create + fill (writes the portal's values/offer) |
| `tickets.py` | ⏳ | ticket-channel registry |
| `welcome.py` | ⏳ | welcome / rate-card copy |
| `lots.py` | ⏳ | lot state + buying minimums + lane totals |
| `shipping.py` | ⏳ | dual-address (VA / Doral) routing |
| `vip.py` | ⏳ | per-customer rate overrides (see Open decisions) |
| `quotas.py` | ⏳ | Pokémon slot quotas |
| `payments.py` | ⏳ | payout packets / digest |
| `summary.py` | ⏳ | `!summary` customer buying sheets |
| `webserver.py` | ⏳ | /owed /tickets /leaderboard /links /health |
| `state.py` | ⏳ | JSON persistence stores |
| `bot.py` | ⏳ | Discord client, `on_message` router, commands (entry) |

All ⏳ modules are rebuilt fresh, using v1 as the reference so no battle-tested
edge case is lost (state recovery, CGC, overflow-proof tickets, min-lot holds…).

## Open decisions (settle while building, flag to Kevin)
1. **Sheet offer**: v1 used a formula that re-priced from bands. v2 writes the
   portal's value + offer as numbers; editing a value means a re-quote (no local
   band formula — the portal is the source of truth). → going with numbers.
2. **VIP**: per-Discord-customer overrides aren't known to the portal. Either the
   bot post-adjusts the offer for VIP customers, or `/bot/price` accepts an
   optional per-request rate override. → scope with the portal side.

## Backups of v1 (the months of proven work)
- git tag `v1-stable-2026-11-04`, branch `v1-backup-2026-11-04`
- full copy: `../../kts-bot-BACKUP-v1-2026-11-04/`
- `../kts_collectr_bot.py` stays the LIVE bot on `main`; v2 lives on branch
  `bot-v2-triumph` and does not touch the live deploy until Kevin says go.
