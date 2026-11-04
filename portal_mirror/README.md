# portal_mirror — KTS Portal rules → Discord bot

Mirrors the **KTS Portal** seller app's buying rules into the Discord bot so both
channels quote the **same offer on the same card**. The portal
(portal.ktscollectibles.com) is the **source of truth**; this module re-implements
its decision engine (`offer.js`) and is driven by the portal's own rules board.

**Status: LIVE (Nov 4).** The bot's own pricing path (`classify_psa_comp`,
`get_psa_payout_rate`, `build_sheet_h_formula`, welcome card) was aligned to the
Nov 4 portal board and deployed to `main`. Sports + One Piece are HELD until the
Triumph window (Sat Nov 7, 3 AM EST = 08:00 UTC, `SPORTS_OP_GO_LIVE_UTC`); Pokémon
is off — so nothing is takeable until Saturday and the band alignment is latent
behind the hold. `portal_pricing.py`/`rules.json` are NOT imported by the bot at
runtime (the bot re-implements the bands in its own classify + sheet formula);
they remain the verification oracle the bot is checked against.

Go-live rail decisions (Kevin, Nov 4): PSA 7+ grade floor and sale-age gate DROPPED
on sports/OP (the portal has neither); the "must show a visible sale" data-quality
check and the curated NBA/WNBA/collegiate bans KEPT as safe-stricter overlays; a
live-allocation disclaimer attached to every sports/OP quote.

## Files
- `rules.json` — the portal rules board, mirrored from the live Fly.io config
  (`updated_at` 2026-11-04). Re-mirror = replace this file, then re-verify.
- `portal_pricing.py` — the engine. `evaluate(comp)` takes the bot's own CardLadder
  comp (`clValue`, `grade`, `sport`, `cert`) and returns accept/reject + offer,
  matching `offer.js` to the cent. `rate_card_lines()` builds the welcome rate card.
- `make_oracle.js` — runs the REAL `offer.js` (from `~/kts-seller-app`) over a value
  grid and writes `oracle_grid.json` (ground truth).
- `verify_parity.py` — asserts `portal_pricing.py` == the oracle grid.
- `oracle_grid.json` — generated; 116,433 points.
- `verify_bot_parity.py` — imports the REAL bot (heavy deps stubbed) and asserts
  `classify_psa_comp` + `get_psa_payout_rate` == the portal across the value grid.
- `verify_hformula.py` — evaluates the REAL sheet H-formula string (a tiny
  self-tested Sheets interpreter) and asserts it == the portal across the grid.

## Verify
```
cd portal_mirror
node make_oracle.js        # needs ~/kts-seller-app present (reads offer.js only; no tokens/network)
python3 verify_parity.py   # portal_pricing.py == offer.js  -> "PARITY OK"
python3 verify_bot_parity.py  # bot classify + quote rate == portal -> "BOT PARITY OK"
python3 verify_hformula.py    # sheet payout formula == portal -> "H-FORMULA PARITY OK"
```
Last run: parity **116,433 pts**; bot + H-formula **20,788 pts each**, 0 mismatches.

## What this does NOT do
- **No price source change.** It never calls Triumph's scanner API. The bot keeps
  its own CardLadder comp; this module only decides/offers from that comp.
- **No CardLadder confidence.** The portal gates on CL confidence (minConf 1–5); the
  bot's comp has no CL score, so `minConf` is NOT enforced here (quotes in-range).
- **No live Triumph allocation.** Sports + One Piece are Triumph-routed in the portal
  (takeability from live allocation). The bot can't ask, so it quotes the rate and
  attaches `ALLOCATION_DISCLAIMER` on those categories.

## Rules currently mirrored (Nov 4 board, 21:10 — exact Triumph allocation bands, gaps preserved)
- Pokémon PSA: **PARKED / empty** — buyer undecided (Triumph vs Pixel), so the track is `enabled:false`. Prior Trade-lane rates are kept in rules.json (PSA 7+, cert ≥70M: $1–100 93% · $100–400 82% · $400–650 85% · $650–1,000 82%) for one-flag re-enable once Kevin picks the buyer.
- One Piece PSA: $0–100 89% · $100.01–240 84% · $250–1,000 84% · $1,000–1,200 85%  (gap $240–250)
- NBA PSA: $0.60–240 95% · $250–800 95% · $900–1,000 95%  (gaps $240–250 and $800–900)
- MLB PSA: $1–30 100% · $35–100 90% · $100–220 90% · $250–450 90% · $500–600 89%  (gaps $30–35, $220–250, $450–500; tops at $600)
- NFL PSA: $0.40–30 100% · $30–500 90% · $500–800 89%  (no gaps; tops at $800)
- CGC: all tracks off.

## Went live (Nov 4)
Done a sturdier way than "import portal_pricing": the bot keeps its Google-Sheets
payout formula, so the Nov 4 bands were written directly into
`build_sheet_h_formula`, `classify_psa_comp`, the per-card rate tiers, the welcome
card, and `SPORTS_OP_GO_LIVE_UTC`, then verified against `portal_pricing`/the oracle
(see Verify) and merged to `main` (= Railway deploy). Re-mirror = pull the current
board into `rules.json`, mirror the changed bands into those same bot spots, and
re-run all three verifiers until clean. Also re-arm `SPORTS_OP_GO_LIVE_UTC` each
period (the portal's window is AUTO; the bot's is a hard-coded instant).

## Staying in sync
The "Triumph Partners pricing API" session pings the bot session when it changes a
rule while both are running. A change Kevin makes directly in the portal admin will
NOT auto-reach the bot — tell the bot session "re-mirror" and it re-pulls `rules.json`
and re-runs the parity check.
