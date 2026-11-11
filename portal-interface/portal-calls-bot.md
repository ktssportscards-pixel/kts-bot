# Inbound — the portal calls the bot

Source of truth: `botv2/webserver.py`. The bot's web server runs on the Railway `worker`
(`https://worker-production-1175.up.railway.app`), started inside the gateway's `on_ready`.
Every endpoint except `/health` requires `x-bot-key` and **fails closed** (401) if `BOT_API_KEY`
is unset. Snowflake ids (`channel_id`, `message_id`) are always emitted as **JSON strings**.

**Actually called by the portal today: only `/ops` + `/delivery`.** `/status` + `/analytics` are authed and
**available** but not called portal-side right now (monitoring/handy — e.g. the portal could read
`/status.flags` later). `/health` is for Railway's health-check + the bot's own canary, not the portal.
`POST /void` (below) is the bot-side half of the Ops board's "cancel this quote" control — this file did
not list it before 2026-11-10; confirm on the portal side whether it is wired up yet.

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
lanes_schedule, lanes_channel_id, stale_nudges, ship_all_doral, auto_claim, pokemon_lane_open,
price_mock, kevin_dm } }`
`flags` is the non-secret feature-toggle state (added 2026-11-06 so a Railway toggle is verifiable from outside).
`price_mock: true` on the live bot means it is quoting FAKE mock prices; `kevin_dm: false` means `KEVIN_USER_ID`
is unset (no admin commands, no alerts). `last_error` is the most recent fault since boot and is never cleared —
judge it by its `at` timestamp (the canary ignores one older than 90 minutes).

## GET /analytics  — x-bot-key
Volume + payout aggregates. `?days=N` optional (clamped 1–3650). **Margin is NOT included** — stays portal-side.

## POST /delivery  — x-bot-key  (the carrier webhook SINK)
The portal/EasyPost adapter POSTs carrier events here.
**Request:** `{ "channel_id"? | "tracking"?, "status"?, "delivered_at"?, "out_for_delivery_at"?, "carrier"? }`
- Resolves by `channel_id` when that id has a ticket; otherwise by `tracking` number (case-insensitive; a numeric tracking is coerced to string). So a request whose `channel_id` matches no ticket but whose `tracking` does is applied to the ticket that holds that number (changed 2026-11-10 — it used to answer `found:false`), and **the `channel_id` in the response is the RESOLVED ticket's, which can differ from the one sent**.
- `status`: **`out_for_delivery`** → posts the customer "🚚 out for delivery today" heads-up (gated by `BOTV2_DELIVERY_TRACKING`, no Kevin DM); **`delivered`** → flips the ticket + ALWAYS DMs Kevin + posts "📦 arrived" (gated). A **missing / blank / unknown** status is acked + **ignored** (NOT treated as delivered). Send an explicit status string.
- Always 200 on the delivered path (even no-match) so a webhook can't retry-storm; a mark failure also returns 200.

## POST /void  — x-bot-key  (true cancel of a quote; the HTTP twin of Kevin's `!void`)
Removes the ticket's bot-side state so the quote can never be accepted, shipped or paid: the ticket row
(ledger, tracking, delivery, review flag), any owed row, the sheet-cache entry, the in-memory lot + last
quote — and best-effort moves the buying sheet to Drive trash (recoverable). The bot only checks the bot
key; WHO may void is the portal's responsibility.

**Request:** `{ "user": "<discord username>" }` **or** `{ "channel_id": "<snowflake STRING>" }` — exactly one —
plus optional `"force": true`.
- `channel_id` must be a JSON **string** holding an integer (surrounding spaces and one leading `-` are tolerated; a JSON number, or any other string, → 400).
- `user` is matched case-insensitively (leading `@` ignored) against **open (quoted)** tickets only. A blank
  name (`""`, `" "`, `"@"`) → 400; it never matches "every ticket".
- `force` must be a real JSON boolean (a string like `"false"` → 400). It is required to void a ticket that
  has shipped (`accepted`) — that deletes the whole ticket, including an owed balance and paid history.

**Responses (all HTTP 200 unless noted):**
| body | meaning |
|---|---|
| `{voided:true, channel_id, user, status, cards, payout, had_owed, had_sheet, sheet_trashed, sheet_url?, other_sheet_urls, cache_cleanup_pending}` | Voided. `had_sheet` = a buying sheet was on record; `sheet_trashed` = the **latest saved** one was moved to trash. **`sheet_url` is present only when a sheet still needs trashing by hand** — it is the first such link (Kevin is DMed it). Normally that is the latest sheet whose trash failed; rarely it is a second sheet the bot only held in memory, in which case `sheet_trashed` can be `true` at the same time. `other_sheet_urls` = the OTHER buying sheets on record for the same ticket (one is built per cert message; recorded since 2026-11-10); they are **never trashed automatically** — listed so they can be dealt with (Kevin is DMed them too). A link given as `sheet_url` is not repeated there. `cache_cleanup_pending:true` = only a trailing cache write failed; the quote is dead — retry **by `channel_id`** to tidy. |
| `{voided:false, reason:"needs_force", status}` | The ticket has shipped; resend with `force:true` if intended. |
| `{voided:false, reason:"ambiguous", matches:[{channel_id, channel_name, cards}]}` | The user has several open quotes; resend with one `channel_id`. |
| `{voided:false, reason:"persist_failed", channel_id, ticket_intact:true, owed_removed, ship_blocked}` | A storage write failed and the **ticket is still live**; nothing was cleaned up or trashed. Safe to retry. `owed_removed:true` = the owed row had already gone; `ship_blocked:true` = the cancel guard got set, so a customer `ship` is ignored until the retry or a re-quote. |
| `{voided:false, reason:"persist_failed", channel_id, ticket_intact:false, …}` | No live ticket, but cleaning up leftover state failed to persist. Safe to retry. |
| `{voided:false, reason:"not_found", residual_cleaned, had_sheet, sheet_trashed, sheet_url?, other_sheet_urls}` | No ticket for that target. `residual_cleaned:true` = this call DID remove leftover state from an earlier partial void (and tried to trash its sheet); `false` = nothing existed. A `user` lookup can only find an open ticket, never leftovers: it answers with the same keys, always `residual_cleaned:false, had_sheet:false, sheet_trashed:false, other_sheet_urls:[]` — also when that seller's only tickets have already shipped. To finish a partial void use `channel_id`. |
| HTTP 400 `{error}` | Bad JSON / not an object / both or neither target / bad `channel_id` / blank `user` / non-boolean `force`. |
| HTTP 401 | Missing/wrong `x-bot-key` (or `BOT_API_KEY` unset on the bot). |

Idempotent: repeating a void is safe. The call waits for the Drive trash attempt: about 20 s for the Apps
Script call, and up to a minute more if that fails and the direct Drive fallback runs — size the caller's
timeout for that. Code wins on drift: `botv2/webserver.py::_void` + `botv2/tracking.py::void_ticket`.

**Removed 2026-11-06:** the old `/owed` `/tickets` `/leaderboard` `/links` reads (retired v1 local tracker; portal uses `/ops`). If anything portal-side still hits those, switch it to `/ops`.
