# Bot ↔ Portal interface — handoff

Everything that connects the **Discord bot** (this repo, `kts-bot/botv2/`, deployed on
Railway as the `worker` service, branch `bot-v2-triumph`) and the **portal / seller app**
(`portal.ktscollectibles.com`, Fly.io — the "Triumph Partners pricing API" session's domain).

Owner split in one line: **the portal owns all pricing, bands, allocation, rates, margin, and
the Triumph credential; the bot owns Discord, tickets, shipping, the owed ledger, and delivery
posts.** The bot has NO pricing/band/allocation logic — it relays the portal's decision.

## The wiring
| Direction | Endpoint | Auth | Purpose |
|---|---|---|---|
| **bot → portal** | `POST /bot/price` | `x-bot-key` | price certs against Triumph (BUY/PASS + offer) |
| bot → portal | `POST /bot/claim` · `POST /bot/unclaim` | `x-bot-key` | reserve / release a Triumph allocation slot (auto-claim) |
| bot → portal | `GET /bot/allocation` | `x-bot-key` | live band fullness (feeds `!bands` + the lanes board) |
| bot → portal | `POST /bot/register-tracking` | `x-bot-key` | register a UPS # with the EasyPost delivery watcher |
| **portal → bot** | `GET /ops` | `x-bot-key` | per-ticket lifecycle for the admin Ops dashboard |
| portal → bot | `GET /status` | `x-bot-key` | health, counts, last error, feature-flag states |
| portal → bot | `GET /analytics` | `x-bot-key` | volume + payout aggregates (no margin) |
| portal → bot | `POST /delivery` | `x-bot-key` | carrier "out for delivery" / "delivered" events |
| public | `GET /health` | none | liveness (Railway health-check + canary) |

Bot base URL: `https://worker-production-1175.up.railway.app`
Portal base URL: `https://portal.ktscollectibles.com` (bot reads `PORTAL_BASE_URL`)

## Files in this folder
- `bot-calls-portal.md` — the outbound contracts (what the bot sends the portal + reads back).
- `portal-calls-bot.md` — the inbound contracts (what the bot exposes for the portal).
- `conventions.md` — shared rules both sides must honor (auth, snowflakes-as-strings, confidential fields, forbidden words).
- `open-items.md` — the live coordination list (who owes what next).

Authoritative source (this repo): outbound = `botv2/portal_client.py` + `botv2/config.py`;
inbound = `botv2/webserver.py`. If a doc here and the code disagree, the code wins — update the doc.
