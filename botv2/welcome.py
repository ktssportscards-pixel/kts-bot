"""
welcome — the one-time greeting posted when a ticket opens.

v1 listed specific $ bands in the welcome, which meant hand-editing it every week
(another copy of the rules that could drift). v2 leans into the new capability
instead: pricing is instant, so the greeting just invites certs and the per-cert
quote is the authoritative answer on what's taken. (Live ranges can later be
pulled from the portal if Kevin wants them shown — see README open decisions.)
"""

WELCOME_MSG = (
    "👋 **Welcome to KTS Collectibles!**\n\n"
    "We buy **PSA graded slabs** — Pokémon, One Piece, and sports "
    "(Baseball/MLB, Basketball/NBA, Football/NFL) — plus **CGC Pokémon**.\n\n"
    "📨 **Drop your cert numbers** (just the numbers — space or comma separated) "
    "and I'll price them for you right away. ⚡\n\n"
    "⚠️ Slabs only — no raw cards.\n\n"
    "What are you looking to sell?"
)


def welcome_msg() -> str:
    return WELCOME_MSG
