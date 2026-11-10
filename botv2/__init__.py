"""
botv2 — the Triumph-sourced rebuild of the KTS Collectr Discord bot.

Run:  python -m botv2        (from the kts-bot/ directory — see __main__.py for why not
                              `python -m botv2.bot`)
Test: python -m botv2.selftest

botv2 has been the LIVE bot since the 2026-11-04 cutover; v1 (../kts_collectr_bot.py) is
the dormant rollback copy and the reference for every battle-tested edge case. v2 is
priced entirely through the KTS Portal's /bot/price endpoint — no CardLadder scraper, no
Triumph token, no local rules engine. See README.md and DEPLOY.md ("Current state").
"""
__version__ = "2.0.0-dev"
