# portal_mirror — KTS Portal rules → Discord bot

Mirrors the **KTS Portal** seller app's buying rules into the Discord bot so both
channels quote the **same offer on the same card**. The portal
(portal.ktscollectibles.com) is the **source of truth**; this module re-implements
its decision engine (`offer.js`) and is driven by the portal's own rules board.

**Status: STAGED, NOT WIRED IN.** Nothing in `kts_collectr_bot.py` imports this yet.
The live bot's pricing path is unchanged. Kevin isn't buying right now, so this is a
staged mirror ready to switch on — no lanes are opened by anything on this branch.

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

## Verify
```
cd portal_mirror
node make_oracle.js        # needs ~/kts-seller-app present (reads offer.js only; no tokens/network)
python3 verify_parity.py   # prints "PARITY OK"
```
Last run: **116,433 points, 0 mismatches.**

## What this does NOT do
- **No price source change.** It never calls Triumph's scanner API. The bot keeps
  its own CardLadder comp; this module only decides/offers from that comp.
- **No CardLadder confidence.** The portal gates on CL confidence (minConf 1–5); the
  bot's comp has no CL score, so `minConf` is NOT enforced here (quotes in-range).
- **No live Triumph allocation.** Sports + One Piece are Triumph-routed in the portal
  (takeability from live allocation). The bot can't ask, so it quotes the rate and
  attaches `ALLOCATION_DISCLAIMER` on those categories.

## Rules currently mirrored (Nov 4 board, 21:10 — exact Triumph allocation bands, gaps preserved)
- Pokémon PSA (manual "Trade", PSA 7+, cert ≥70M): $1–100 93% · $100–400 82% · $400–650 85% · $650–1,000 82%  [unchanged]
- One Piece PSA: $0–100 89% · $100.01–240 84% · $250–1,000 84% · $1,000–1,200 84%  (gap $240–250; top band % is a PLACEHOLDER pending Kevin)
- NBA PSA: $0.60–240 95% · $250–800 95% · $900–1,000 95%  (gaps $240–250 and $800–900)
- MLB PSA: $1–30 100% · $35–100 90% · $100–220 90% · $250–450 90% · $500–600 89%  (gaps $30–35, $220–250, $450–500; tops at $600)
- NFL PSA: $0.40–30 100% · $30–500 90% · $500–800 89%  (no gaps; tops at $800)
- CGC: all tracks off.

## Going live (when Kevin is buying again)
Wire `portal_pricing.evaluate()` into the bot's quote path (replacing the relevant
`classify_psa_comp` branches), regenerate the welcome rate card from
`rate_card_lines()`, and rebuild the sheet H-formula from `rules.json`. Review the
diff on this branch before merging to `main` (merge to `main` = Railway deploy).

## Staying in sync
The "Triumph Partners pricing API" session pings the bot session when it changes a
rule while both are running. A change Kevin makes directly in the portal admin will
NOT auto-reach the bot — tell the bot session "re-mirror" and it re-pulls `rules.json`
and re-runs the parity check.
