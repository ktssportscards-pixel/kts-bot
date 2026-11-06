# Open coordination items (bot ↔ portal)

As of 2026-11-06. "Owner" = whose next action it is. All pricing/rate/allocation changes need
Kevin's DIRECT word to the portal session (the bot relay is a heads-up, not authorization).

## Open
1. **Basketball $1–$300 unlimited — this weekend** · owner: **portal (+ Kevin's direct confirm)**
   Only the **$1–$300 per-card band** is uncapped; everything **> $300** follows the normal/exact
   Triumph allocation. Portal to confirm the exact **window + rate** with Kevin, then set it portal-side.
   Once set, `/bot/price` + `/bot/allocation` reflect it automatically — **no bot change**.

2. **Auto-submit (the Triumph loop)** · owner: **bot (Saturday capture) → portal (build) → bot (trigger)**
   Approval read-back is already live. Remaining: capture the Triumph **submit/draft-create + un-draft**
   write-endpoint SHAPE (gated until Sat 3 AM ET — see `botv2/SATURDAY_CAPTURE.md`). Auth is already
   solved server-side, so only the write shape is needed. Then portal builds `/bot/submit`, bot triggers
   it like `/bot/price` with the kill switch. Bot side (`claim_certs`/`unclaim_certs`, double-gated) is ready + inert.

3. **Delivery "out for delivery" forwarding** · owner: **portal**
   The bot's `/delivery` handles both `out_for_delivery` and `delivered`, but the EasyPost adapter
   currently forwards **delivered-only**. To light up the "🚚 out for delivery today" customer post,
   forward EasyPost `out_for_delivery` events to `/delivery` with `status: "out_for_delivery"`.

## Recently closed (FYI — no action)
- **channel_id snowflake → strings** (bot, done): `/ops` etc. now emit `channel_id` as a string;
  portal stopgap can be dropped (or kept as belt-and-suspenders).
- **Public-endpoint lockdown** (bot, done): `/owed /tickets /leaderboard /links` were leaking
  seller names/payouts/tracking/offer_pct — now removed; portal confirmed it only uses `/ops` + `/delivery`.
- **`/delivery` hardening** (bot, done): numeric tracking coerced, missing status fails closed, always-200.
  → when wiring OFD forwarding, send an explicit `status` string.
- **Flags readout on `/status`** (bot, done): the portal can read live feature-flag states from `/status.flags`.
