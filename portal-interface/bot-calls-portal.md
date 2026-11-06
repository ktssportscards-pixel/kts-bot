# Outbound — the bot calls the portal

Source of truth: `botv2/portal_client.py` (+ endpoint URLs in `botv2/config.py`). All calls send
`x-bot-key: <BOT_API_KEY>` + `content-type: application/json` and request gzip. Non-2xx/non-JSON →
`PortalUnavailable` (graceful, no silent drop); a 401 → `PortalAuthError` (bad key, never retried).

---
## POST /bot/price
Prices certs against Triumph's scanner. The decision is authoritative (already reflects live Triumph allocation).

**Request:** `{ "certs": ["<cert>", …], "customer"?: "<discord_username>", "rateOverride"?: <rare manual %> }`
(certs are chunked; `customer` is the raw Discord username used for VIP matching.)

**Response:** `{ "rows": [ <row>, … ], "customerMatched": <bool> }`

Each **row** the bot reads (`PricedRow`): `certId`, `grader`, `grade`, `cardTitle`, `imageUrl`,
`category`, `categoryLabel`, `band`, `marketValue`, `lastSale`, `value`, `valueBasis`, `offerPct`, `status`,
`rejectionReason`, `clConfidence`, `decision` (`BUY`|`PASS`), `offer`, `reason`, `error`, `vip`,
`nearFull` (bool), `pctFull` (number).

**⚠️ MUST NOT appear in rows:** `payoutCents`, `memberMultiplier`, margin — confidential, portal-side only.
The bot's offer basis is `value` (lastSale for a CGC with a sale, else marketValue); `offerPct` = round(offer/value×100).

---
## POST /bot/claim · POST /bot/unclaim
Reserve / release a Triumph allocation slot for BUY cards (auto-claim). Double-gated: the portal's own
kill switch AND the bot's `BOTV2_AUTO_CLAIM` must both be on, else every row comes back `skipped`.

**Request:** `{ "certs": ["<cert>", …] (≤200), "idempotencyKey"?: "<channel id>" }`
**Response:** `{ "enabled": <bool>, "results": [ { "certId", "claimed", "alreadyClaimed", "removed", "skipped", "error", "idempotent" }, … ] }`
Idempotent on (key, certId). PASS certs → `skipped`. `/bot/unclaim` is the kill-switch undo.
**Note:** the portal's actual Triumph claim WRITE (`pricing.claimCert`) is still stubbed/inert pending the
Saturday capture — so even fully gated-on, no real reservation fires yet (the "reserve a slot" behavior is
not live; see `open-items.md` → auto-submit).

---
## GET /bot/allocation
Live band fullness → feeds the Kevin-only `!bands` board AND the customer lanes board.

**Response:** `{ "period": …, "spendHidden": <bool>, "reliability": "<note>",
"categories": [ { "category", "categoryLabel", "ranges": [ { "pctFull": <number|null> }, … ] }, … ] }`
Lanes board maps peak `pctFull` → 🟢 <85 / 🟡 85–99 / 🔴 ≥100; `spendHidden` or null → 🟢 (never invents a number).

---
## POST /bot/register-tracking
On a customer tracking drop, asks the portal to start the EasyPost watch. Fail-soft (never blocks the ack).

**Request:** `{ "channel_id": <id>, "tracking": "<1Z…>" }`
Gated by `BOTV2_REGISTER_TRACKING`. Portal persists `{tracking_code, channel_id, easypost_id, status, delivered_at, notified_at}` on its own Fly volume. On delivery the portal calls the bot's `POST /delivery` (see `portal-calls-bot.md`).
