# Open coordination items (bot ↔ portal)

As of 2026-11-06. "Owner" = whose next action it is. All pricing/rate/allocation changes need
Kevin's DIRECT word to the portal session (the bot relay is a heads-up, not authorization).

## Open
1. **Auto-submit (the Triumph loop)** · owner: **bot (Saturday capture) → portal (build) → bot (trigger)**
   Approval read-back is already live. Remaining: capture the Triumph **submit/draft-create + un-draft**
   write-endpoint SHAPE (gated until Sat 3 AM ET — see `botv2/SATURDAY_CAPTURE.md`). Auth is already
   solved server-side, so only the write shape is needed. Then portal builds `/bot/submit`, bot triggers
   it like `/bot/price` with the kill switch. Bot side (`claim_certs`/`unclaim_certs`, double-gated) is ready + inert;
   the portal's actual Triumph claim write (`pricing.claimCert`) is also stubbed pending this capture.

2. **Delivery "out for delivery" forwarding** · owner: **portal**
   The bot's `/delivery` handles both `out_for_delivery` and `delivered`, but the EasyPost adapter
   currently forwards **delivered-only**. To light up the "🚚 out for delivery today" customer post,
   forward EasyPost `out_for_delivery` events to `/delivery` with `status: "out_for_delivery"`.

## Recently closed (FYI — no action)
- **Basketball ≤ $300 uncapped — DONE + LIVE** (portal; Kevin confirmed directly, 2026-11-06).
  This weekend only — **auto-expires Mon 2026-11-09 00:00 ET**. Basketball cards **≤ $300 ignore the
  Triumph allocation cap**; everything **> $300 follows normal Triumph allocation UNCHANGED** (verified: a
  $400 alloc-rejected card still PASSes "Allocation Exceeded"). Rate unchanged (Kevin's usual %); the
  $240–$250 rate-band gap was filled at 95% per his follow-up. Portal rule:
  `rules.basketballUncapped = { maxValue: 300, activeUntil: '2026-11-09T05:00:00Z' }`.
  `/bot/price` + `/bot/allocation` reflect it automatically — no bot change.
- **channel_id snowflake → strings** (bot): `/ops` etc. now emit `channel_id` as a string;
  portal stopgap can be dropped (or kept as belt-and-suspenders).
- **Public-endpoint lockdown** (bot): `/owed /tickets /leaderboard /links` were leaking
  seller names/payouts/tracking/offer_pct — now removed; portal confirmed it only uses `/ops` + `/delivery`.
- **`/delivery` hardening** (bot): numeric tracking coerced, missing status fails closed, always-200.
  → when wiring OFD forwarding, send an explicit `status` string.
- **Flags readout on `/status`** (bot): live feature-flag states are readable at `/status.flags`
  (available to the portal; not called portal-side today).
