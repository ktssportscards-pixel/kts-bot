# Shared conventions — both sides must honor these

## Auth
- Every bot endpoint except `/health` requires header `x-bot-key: <BOT_API_KEY>` and **fails closed**
  (401) when the key is unset — an endpoint can never be accidentally public.
- Every bot→portal call sends the same `x-bot-key`. The key is a Railway/Fly secret — never commit it, never log it, never put it in a URL.

## Discord snowflakes → JSON strings
- `channel_id`, `message_id`, and any Discord snowflake id are emitted as **strings** in JSON, never
  numbers. A 19-digit snowflake exceeds JS's 2^53 safe-integer range, so a JSON *number* is silently
  corrupted by `JSON.parse` (…896533 → …896500) and breaks the app's decode. The bot now stringifies
  them on every id-carrying response; the portal keeps a parse-time stopgap as belt-and-suspenders.

## Confidential — never cross the boundary toward the bot / customer
- `payoutCents`, `memberMultiplier`, and **margin** are portal-only. They must NOT appear in any
  `/bot/price` row, any bot state, any customer-facing text, or any bot endpoint (incl. `/analytics`,
  which is volume + payout only).
- The bot NEVER holds the Triumph credential and NEVER moves money.

## Customer-facing text
- `CUSTOMER_FORBIDDEN_WORDS = ("triumph", "poketrader", "pixel", "allocation", "doral")` must never
  appear on a customer surface (ticket posts, the lanes board, customer DMs, acks, error messages).
  The lanes board drops any category label containing a forbidden word and shows no numbers/%/$.
- The bot messages customers in Discord ONLY for the deliberate, flag-gated delivery updates
  (`BOTV2_DELIVERY_TRACKING`). Everything else customer-facing is the quote/ship/welcome flow.

## Webhook / event hygiene (`POST /delivery`)
- Send an **explicit** `status` string (`delivered` or `out_for_delivery`). A missing/blank/unknown
  status is acked + ignored — it is NEVER defaulted to delivered (that would fire a false "arrived").
- Send `tracking` as a string. The bot coerces a numeric one, but strings are the contract.
- The bot always returns 200 on the delivered path so a 5xx can't trigger a carrier retry-storm.
