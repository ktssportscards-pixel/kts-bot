# Inbound — the portal calls the bot

Source of truth: `botv2/webserver.py`. The bot's web server runs on the Railway `worker`
(`https://worker-production-1175.up.railway.app`), started inside the gateway's `on_ready`.
Every endpoint except `/health` requires `x-bot-key` and **fails closed** (401) if `BOT_API_KEY`
is unset. Snowflake ids (`channel_id`, `message_id`) are always emitted as **JSON strings**.

**Actually called by the portal today: only `/ops` + `/delivery`.** `/status` + `/analytics` are authed and
**available** but not called portal-side right now (monitoring/handy — e.g. the portal could read
`/status.flags` later). `/health` is for Railway's health-check + the bot's own canary, not the portal.

---
## GET /health  — PUBLIC
Liveness for the Railway health-check + the canary. Returns text `KTS botv2 — OK · started <ISO>`.
The `· started <ISO>` stamp is how a deploy is verified from outside (it advances on each redeploy).

## GET /ops  — x-bot-key
Per-ticket lifecycle (quote → claim → ship → owed/paid) for the admin Ops dashboard.
`{ "tickets": [ { channel_id (STRING), name, user, status, comp, payout, cards, tracking,
delivered, delivered_at, claimed, … }, … ] }`
`?cards=1` adds `card_detail: [ { cert, value, offer, offer_pct, category, grader, grade, name } ]`
per ticket (the Triumph-value reconcile feed). Default omits `card_detail`.

## GET /status  — x-bot-key
`{ "ok", "started", "uptime_seconds", "counts": { tickets_total, tickets_open, owed_open, owed_total },
"last_error": { key, text, at } | null, "flags": { delivery_tracking, register_tracking, lanes_board,
lanes_schedule, lanes_channel_id, stale_nudges, ship_all_doral, auto_claim, pokemon_lane_open } }`
`flags` is the non-secret feature-toggle state (added 2026-11-06 so a Railway toggle is verifiable from outside).

## GET /analytics  — x-bot-key
Volume + payout aggregates. `?days=N` optional (clamped 1–3650). **Margin is NOT included** — stays portal-side.

## POST /delivery  — x-bot-key  (the carrier webhook SINK)
The portal/EasyPost adapter POSTs carrier events here.
**Request:** `{ "channel_id"? | "tracking"?, "status"?, "delivered_at"?, "out_for_delivery_at"?, "carrier"? }`
- Resolves by `channel_id`, else by `tracking` number (both case-insensitive; a numeric tracking is coerced to string).
- `status`: **`out_for_delivery`** → posts the customer "🚚 out for delivery today" heads-up (gated by `BOTV2_DELIVERY_TRACKING`, no Kevin DM); **`delivered`** → flips the ticket + ALWAYS DMs Kevin + posts "📦 arrived" (gated). A **missing / blank / unknown** status is acked + **ignored** (NOT treated as delivered). Send an explicit status string.
- Always 200 on the delivered path (even no-match) so a webhook can't retry-storm; a mark failure also returns 200.

**Removed 2026-11-06:** the old `/owed` `/tickets` `/leaderboard` `/links` reads (retired v1 local tracker; portal uses `/ops`). If anything portal-side still hits those, switch it to `/ops`.
