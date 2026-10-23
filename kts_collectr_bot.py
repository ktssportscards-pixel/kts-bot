"""
KTS Collectibles — Full Discord Bot
=====================================
Handles TWO types of customers automatically:

1. PSA SLAB sellers:
   - Customer sends cert numbers
   - Bot creates a Google Sheet copy with cert numbers + CardLadder links
   - Pings Kevin with sheet link

2. RAW CARD sellers (Collectr) — ONE PIECE + POKÉMON (Jul 31 2026):
   - Customer uploads their Collectr CSV export in their ticket channel
   - Bot reads it, validates per game, calculates per-game market value
   - One Piece: English NM singles, $1-$150 per card
       under $10k     → 85%
       $10k+          → 88%
   - Pokémon: English NM UNGRADED singles, $40-$80 per card, 2022+ sets only,
     no trainer cards, no Master Ball versions (see pokemon_species.py)
       under $5k      → 85%
       $5k+           → 88%
   - Any rule-breaking rows BLOCK the quote until the re-export is clean
   - Sends customer their offer (per-game portions priced separately)
   - Pings Kevin with breakdown

SETUP:
1. pip install discord.py anthropic gspread google-auth google-api-python-client pandas
2. Fill in config values below
3. python kts_collectr_bot.py
"""

import discord
import anthropic
import gspread
import asyncio
import re
import io
import time
import os
import json
import pandas as pd
import pokemon_species
from datetime import datetime, date, timezone
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ── CONFIGURATION ────────────────────────────────────────────────────────────────
DISCORD_BOT_TOKEN        = os.environ.get("DISCORD_BOT_TOKEN", "")
ANTHROPIC_API_KEY        = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_CREDENTIALS_FILE  = os.environ.get("GOOGLE_CREDENTIALS_FILE", os.path.expanduser("~/Downloads/google_credentials.json"))
TEMPLATE_SHEET_ID        = "1y_jis_knml_UIVWxxtEHrKs_vjkuX97x8Q7u3pVsQVM"
KTS_FOLDER_ID            = "1Ib1XgsCt9yc8B7EkppSEd97xTWlW8S2y"   # Parent folder
PSA_FOLDER_ID            = "1ayHilGpXqNQA8RDRSw1igTsCxBEI4hvm"   # PSA Slabs
COLLECTR_FOLDER_ID       = "1nAUPg7QW7tRzzdiHxG7UYUSPCa8MDZq3"   # Collectr Singles
APPS_SCRIPT_URL          = "https://script.google.com/macros/s/AKfycbxPenrARSCnPZ6Ddwaokcz24Fwvcobgp0ybvvzJR49cCJ_DUcNoprRXDPpyTJA0rJ71Cg/exec"
YOUR_DISCORD_USER_ID     = 1120958174036500480  # Kevin's Discord user ID

# Helper API — Cardladder comp lookup service
HELPER_URL               = os.environ.get("HELPER_URL", "https://helper.ktscollectibles.com")
HELPER_API_KEY           = os.environ.get("HELPER_API_KEY", "")

# Raw card payout percentages by lot size — ONE PIECE singles (flyer Jun 2026).
# 85% base, 88% on 10k+ lots. Always quote the low end of the range. Singles-only
# lots still have a $3,000 minimum (see MIN_LOT_VALUE); sub-minimum lots are held.
RAW_PAYOUT_TIERS = [
    (0,     10000,        0.85),   # under $10k lot → 85%
    (10000, float('inf'), 0.88),   # $10k+ lot      → 88%
]

# Raw card per-card price limits (One Piece)
RAW_MIN_PRICE = 1
RAW_MAX_PRICE = 150

# Raw Pokémon singles: OFF for the Aug 7 weekend — absent from the flyer (same
# convention as MLB). Flip BUYING_POKEMON_RAW to True to resume with the rules
# below; everything (validation, rates, minimums) stays wired up behind the flag.
BUYING_POKEMON_RAW = False
# Aug 28 weekend: One Piece raw singles ALSO off (One Piece absent from the
# flyer entirely — slabs and raws, confirmed by Kevin). Flip to resume.
BUYING_ONE_PIECE_RAW = False
# Pokémon raw singles rules (Jul 31 2026 weekend spec): $40-$80 per card, English
# Near Mint UNGRADED, 2022+ sets only, no trainers, no Master Ball versions
# (rules data + helpers live in pokemon_species.py). 85% base, 88% when the
# POKÉMON portion of the lot is $5k+. Pokémon-singles lots need $1,500+
# (POKEMON_SINGLES_MIN_LOT); One Piece singles keep their own $3,000 minimum.
POKEMON_RAW_MIN_PRICE = 40
POKEMON_RAW_MAX_PRICE = 80
POKEMON_RAW_PAYOUT_TIERS = [
    (0,    5000,         0.85),   # under $5k Pokémon portion → 85%
    (5000, float('inf'), 0.88),   # $5k+ Pokémon portion      → 88%
]
POKEMON_SINGLES_MIN_LOT = 1500

# PSA slab buying criteria
PSA_MIN_PRICE = 1
PSA_MIN_GRADE = 7
PSA_MAX_AGE_DAYS = 30
# CGC slab buying criteria (Kevin, Oct 18 — Pixel takes CGC): Pokémon only,
# CardLadder value $1-$100 at a flat 83%; over $100 or any non-Pokémon CGC is
# rejected. No other CGC lanes, no grade / sale-age gates, VIP rates don't
# apply. CGC cards do NOT count against the PSA Pokémon quota bands (separate
# buyer lane). Inside the bot a CGC cert is the token "cgc:<digits>" (exactly
# what the comp helper takes and echoes back), so a CGC and a PSA cert with the
# same digits can never collide in lots, retries or by-cert maps; sheets carry
# the bare digits in column B with "CGC" in column A.
CGC_PREFIX = "cgc:"
CGC_MIN_PRICE = 1
CGC_MAX_PRICE = 100
CGC_POKEMON_RATE = 0.83
# Per-sport price ceilings — sports not listed here are rejected outright, and any
# slab priced ABOVE its ceiling is rejected (Sep 26 weekend flyer):
# Pokémon $1-100 / $400-700 / $3,750+ (big band, NO ceiling), NBA $1-$250,
# NFL $1-$220 ($500 for QB GOATs — enforced in classify). One Piece and MLB
# absent from the flyer — both OFF.
# Oct 2 weekend flyer. Ceilings here are the MAX-POSSIBLE per sport (select
# players included); non-select caps are enforced in classify via the
# *_GENERAL_MAX constants + the Triumph select lists (triumph_lists.py).
# Oct 16: Pokémon (Pixel) is live nightly now; Sports + One Piece (Triumph)
# don't open until Sat 3 AM ET — hold them with a friendly message until then
# (flip True + wire poketrader address/min-lot before Saturday).
# Oct 16: Sports + One Piece (Triumph/Doral) auto-open Sat Oct 17, 3 AM ET.
# Time-gated so the go-live needs NO second deploy — before this instant the
# hold message fires exactly as before; at/after it, sports/OP classify
# normally. 3 AM EDT (UTC-4, DST still on through Nov 1 2026) = 07:00 UTC.
# Oct 23 (Kevin): same hold for the Oct 23-25 weekend — re-armed for Sat Oct 24.
SPORTS_OP_GO_LIVE_UTC = datetime(2026, 10, 24, 7, 0, tzinfo=timezone.utc)
def sports_op_live():
    return datetime.now(timezone.utc) >= SPORTS_OP_GO_LIVE_UTC
SPORTS_OP_HOLD_MSG = ("🗓️ Sports & One Piece open **Saturday 3 AM ET** — Pokémon "
                      "is live right now, so send those through, and resubmit "
                      "your sports/One Piece Saturday!")
# Sports + One Piece (slabs) ship to the Doral address and require the sports/OP
# slab portion to total $1,500+ (combined across MLB/NBA/NFL/One Piece) — that's
# what justifies the separate address. Pokémon has NO minimum and ships to VA.
SPORTS_OP_MIN_LOT = 1500
SPORTSOP_SPORTS = ('one piece', 'basketball', 'mlb', 'football')
PSA_SPORT_MAX_PRICE = {
    'pokemon': 5000,      # Oct 17-18 weekend: continuous $1-$5,000, fat pools every band
    'one piece': 1000,    # Oct 17-18: $1-$700 + $900-$1,000 (gap 700-900 in classify)
    'mlb': 800,           # Oct 23-25: $1-$240 + $250-$800 (gap 240-250 in classify)
    'basketball': 1400,   # Oct 17-18: $1-$800 + $900-$1,400 (gap 800-900 via bands)
    'football': 800,      # Oct 23-25: $1-$800 continuous
}
MLB_GENERAL_MAX = 800    # Oct 23-25 weekend ceilings — no select-player lanes this week
NBA_GENERAL_MAX = 1400
NFL_GENERAL_MAX = 800
# Select-player bands ABOVE the general ceilings (inclusive; from the flyer's
# SPECIALTY rows). Values between bands are rejected even for listed players.
MLB_SLUGGER_BANDS = []   # Oct 10-11: no slugger lanes above the $220 ceiling
# Sluggers $1,400-$2,000 SUSPENDED Oct 4 (~$253 left in the app lane; Kevin:
# no backlog above $300 (was $600 until Oct 4 12:53 PM), so app-full = instant reject).
NFL_GOAT_BANDS = []      # Oct 10-11: QB Sheeps lane is $0-$100, inside the general range
# NBA general is BANDED (Kevin, Oct 4 12:53 PM #sports-slabs: backlog covers
# sports $1-$300 only now; above that we take ONLY what the Triumph app can
# absorb right now — $300-$1,000 general is app-full and $2,000-$2,500 is an
# app gap => reject):
NBA_GENERAL_BANDS = [(1, 800), (900, 1400)]   # Oct 17-18: $240-$800 pool is back
# 2000s-list players additionally ride the app's 2000s specialty lanes with
# real room: $0-$1,000 and $3,000-$3,750 (the $1,400-$2,500 lane is ~$850
# left => nothing fits => not offered).
NBA_2000S_BANDS = [(1, 800), (900, 1400)]     # Oct 17-18: NO 2000s specialty — same as general
NFL_MIN_PRICE = 1     # dormant — no NFL floor
MLB_MIN_PRICE = 1     # dormant since Oct 12 ($1-$300 greenlight; was 25 for the Oct 10-11 app floor)
# Select-player name lists captured from the Triumph Partners app (Oct 2) —
# the customer-facing checker page mirrors these (tinyurl.com/2awv494h).
# QB_GOATS now comes from there too (6 names — includes Dan Marino).
from triumph_lists import (TRIUMPH_MLB_SLUGGERS, TRIUMPH_NBA_2000S,
                           TRIUMPH_QB_GOATS, TRIUMPH_POKEMON_NVD)
QB_GOATS = TRIUMPH_QB_GOATS
SELECT_CHECK_URL = "tinyurl.com/2awv494h"
# Pokémon buy map (Oct 10-11 weekend): $1-$2,400 and $2,500-$5,000,
# NO personal caps (boss: $1-$100 unlimited for the team, rest straight
# FCFS against the shared Triumph pools — the app is the oracle).
# Gates: PSA 7+ everywhere; 7-series certs (# ≥70M) ≤$1,000;
# sale ≤60d at ≤$100, ≤30d above (CL 4+).
POKEMON_QUOTA_BANDS = [
    (1, 5000, None, None),      # Oct 17-18: continuous $1-$5,000, no personal caps —
]                               # straight FCFS against ~$522k of shared team pools
# Oct 17-18: NO specialty/NVD lanes — everything rides the general bands:
POKEMON_NVD_BANDS = []
def is_pokemon_nvd(comp):
    return _name_on_list(comp, TRIUMPH_POKEMON_NVD)
# Caps are DEFAULTS — override at runtime with "!quota cap <band> <n|off>"
# (stored as cap:<band> in the quota store), so count changes never need a
# deploy.
POKEMON_BUY_BANDS = [(lo, hi) for lo, hi, _k, _c in POKEMON_QUOTA_BANDS]
POKEMON_BIG_BAND_MIN = 2000           # Pixel $2,000-$5,000 = conf-5 FIRM: any grade, flag for Kevin to verify 5/5
POKEMON_CERT_GATE_MAX = 100           # Triumph "7+ CERTS" gate — now only the ≤$100 lane
# PIXEL LANE (Oct 16, Kevin): pokemon $100.01-$1,999.99 sells to Pixel's Cards
# (pixelscardsllc) instead of Triumph — UNLIMITED depth, ANY grade, ANY cert
# EXCEPT certs starting with 0, sale ≤60d (CL 3+), Kevin gets flat 90% of CLV.
PIXEL_LANE_LO, PIXEL_LANE_HI = 17, 1999.99       # conf 3+ tier (accepted) — ALL pokemon $17+ is Pixel now
# Pixel also takes $2,000-$5,000 at conf 5 FIRM — handled as the big-band flag
# (POKEMON_BIG_BAND_MIN) so Kevin eyeballs the 5/5 bars before locking.
PIXEL_WHOLE_LO, PIXEL_WHOLE_HI = 17, 5000         # full Pixel range ($17-$5,000): any grade, any cert except leading-0
POKEMON_MIN_PRICE = 17            # Pixel floor — Pokémon under $17 declined (Option A, Oct 16)
POKEMON_MIN_CERT_SERIES = 70_000_000  # cert NUMBER must be ≥ 70,000,000 — the "7 series"
                                      # (8-digit certs starting 7-9, plus every 9-digit cert).
                                      # Kevin Oct 4: Triumph REJECTS Pokémon below a 7 cert;
                                      # "7+ certs" means the cert series, NOT digit count
                                      # (the old 7-digit-length reading never fired — bug).
# (Pikachu lane REMOVED Aug 11 per Kevin — Pikachus follow standard Pokémon
# rules; $5k-$20k Pikachus land in the big-ticket review band like everything
# else.)
# Pokémon grades (Sep 18): every band is PSA 7+ (generic PSA_MIN_GRADE floor).
# One Piece slabs (Sep 18): PSA 7+ across the whole (only) $1-$100 band —
# the generic PSA_MIN_GRADE floor covers it; nothing sells above $100.
# Per-sport max age of last sale (days). pokemon / basketball / football / mlb are
# value-dependent and handled directly in classify_psa_comp; the rest use this dict.
PSA_SPORT_MAX_AGE_DAYS = {
    'one piece': 60,   # sale within the past 2 months
}
# (Jul 17: Pokémon now has a hard $160 ceiling, so nothing reaches this manual-review
# threshold — anything over $160 is rejected outright before this check. Kept only so
# the code path below stays valid; effectively dead while the ceiling is $160.)
PSA_POKEMON_MANUAL_REVIEW_OVER = 1000
# CardLadder/helper may return One Piece under various names — normalize them all
# to 'one piece' before lookup. Note: the helper returns 'other' for One Piece
# slabs (and possibly other non-mainstream TCGs), so we map 'other' → 'one piece'
# per Kevin's call. If we ever start seeing rogue Magic/Yu-Gi-Oh/Lorcana slabs
# slip through under 'other', revisit this mapping.
PSA_SPORT_ALIASES = {
    'onepiece': 'one piece',
    'one piece': 'one piece',
    'popculture': 'one piece',
    'pop culture': 'one piece',
    'tcg': 'one piece',
    'other': 'one piece',
    'nfl': 'football',
    'football': 'football',
    'baseball': 'mlb',
    'mlb': 'mlb',
}

def normalize_sport(sport_raw):
    """Map various CardLadder sport strings to our canonical sport keys."""
    s = (sport_raw or '').lower().strip()
    if not s:
        return s
    # Direct match first
    if s in PSA_SPORT_MAX_PRICE:
        return s
    # Alias lookup
    if s in PSA_SPORT_ALIASES:
        return PSA_SPORT_ALIASES[s]
    return s


def apply_avg3_value(comp, threshold=0):
    """
    Set comp['clValue'] to the AVERAGE OF THE LAST 3 SALES, except when that
    average is HIGHER than the CardLadder value — then keep the CardLadder value.
    i.e. value = min(avg3Sales, clValue). Only applied when the CardLadder value
    is ABOVE `threshold`; at/below threshold the CardLadder value is kept as-is.
        NBA -> threshold=PSA_SPORT_MAX_PRICE['basketball'] (in-band cards keep the
               direct CardLadder value; over-ceiling cards get the avg-3 discount
               so a stale-high CL value doesn't auto-reject a card whose actual
               recent sales sit inside the buy band)
        MLB no longer uses this (removed Jul 26 — MLB prices on the direct
        CardLadder value).
    Mutates comp['clValue'] so every downstream step (price caps, payout, the
    sheet's "Our comp" column G) uses this value. The original CardLadder value is
    kept in comp['clValueRaw']. If avg3Sales isn't available, the CardLadder value
    is left in place.
    """
    try:
        cl = float(comp.get('clValue')) if comp.get('clValue') is not None else None
    except (TypeError, ValueError):
        cl = None
    try:
        avg3 = float(comp.get('avg3Sales')) if comp.get('avg3Sales') is not None else None
    except (TypeError, ValueError):
        avg3 = None
    comp['clValueRaw'] = cl
    if avg3 is not None and avg3 > 0:
        if cl is None or cl <= 0:
            comp['clValue'] = avg3
        elif cl > threshold:
            comp['clValue'] = min(avg3, cl)
        # else cl <= threshold: keep the direct CardLadder value
    return comp

# ── PER-CARD PAYOUT TIERS BY SPORT (Sep 18 weekend flyer) ─────────────────────────
# Low end of each flyer range. Grade, reject-zones, and the Pokémon manual bucket
# live in classify_psa_comp, so only in-band cards reach these blends. CL-confidence
# requirements can't be enforced (no CL score in the data) — priced on value+grade.
# Pokémon (Sep 26): $1-$100 → 91%; $400-$700 → 86%. The $3,750+ big band is
# NEVER auto-priced (classify returns 'review' — Kevin makes his own offer),
# so it has no tier here. Gaps between bands are rejected in classify.
# All rates are pure value lookups (Oct 2 flyer) — the select-player name
# lists only gate WHICH values are reachable (classify), never the rate.
PSA_POKEMON_PER_CARD_TIERS = [
    (0, 100.01, 0.90),             # $17-$100 → 90% (Pixel, break-even — volume bait)
    (100.01, float('inf'), 0.87),  # $100-$5,000 → 87% (Pixel, flat 90% CLV - 3pt)
]
# NBA (Oct 2): $1-$1,000 → 95% (ANY grade); $1,000-$3,500 → 92% (PSA 7+);
# 2000s-list $3,500-$3,750 → 93% (PSA 7+).
PSA_BASKETBALL_PER_CARD_TIERS = [
    (0, 1000.01, 0.95),
    (1000.01, 3500.01, 0.92),
    (3500.01, float('inf'), 0.93),
]
# MLB (Oct 23-25 flyer): $1-$30 → 100%, $30-$240 + $250-$500 → 90%,
# $500-$800 → 89%. PSA 7+; over $100 needs a sale ≤60d (CL 3+).
PSA_MLB_PER_CARD_TIERS = [
    (0, 30.01, 1.00),              # $1-$30 → 100%
    (30.01, 500.01, 0.90),         # $30-$500 → 90% (gap $240-$250 in classify)
    (500.01, float('inf'), 0.89),  # $500-$800 → 89% (Oct 23-25)
]
# One Piece (Oct 2): $1-$100 → 88%; $100-$900 → 83% (CL 4+ = sale ≤30d).
# PSA 7+.
PSA_ONE_PIECE_PER_CARD_TIERS = [
    (0, 100.01, 0.89),
    (100.01, float('inf'), 0.84),
]
# NFL (Oct 23-25 flyer): $1-$30 → 100%, $30-$500 → 90%, $500-$800 → 89%.
# PSA 7+; over $100 needs a sale ≤60d (CL 3+).
PSA_FOOTBALL_PER_CARD_TIERS = [
    (0, 30.01, 1.00),              # $1-$30 → 100% (Kevin, Oct 15)
    (30.01, 500.01, 0.90),         # $30-$500 → 90%
    (500.01, float('inf'), 0.89),  # $500-$800 → 89% (Oct 23-25, ceiling $800)
]

# ── BASKETBALL-SPECIFIC REJECTION RULES ──────────────────────────────────────────
# Players we won't buy AT ALL regardless of price.
BBALL_PLAYERS_REJECT_ALWAYS = [
    "karl malone",
    "ja morant",
]
# Players we won't buy if the comp is over $200.
BBALL_PLAYERS_REJECT_OVER_200 = [
    "trae young",
    "jaren jackson jr",
    "jaren jackson",     # catches "Jaren Jackson Jr." after normalization too
    "zion williamson",
]
BBALL_PLAYER_PRICE_CAP = 200
# WNBA players treated as a separate sport (rejected). CardLadder reports them
# under sport='basketball', so we match by player name.
WNBA_PLAYERS = [
    "caitlin clark",
    "a'ja wilson",
    "aja wilson",         # alternate spelling without apostrophe
    "sabrina ionescu",
    "angel reese",
    "paige bueckers",
    "juju watkins",
    "breanna stewart",
    "diana taurasi",
    "sue bird",
]
# Brand/set keywords that indicate unlicensed product (no NBA logo). These flag
# the lot for Kevin's manual review rather than auto-reject — sometimes legit
# sub-brands collide. He'll eyeball before paying.
UNLICENSED_BRAND_KEYWORDS = [
    "sage",
    "leaf",
    "press pass",
    "chronicles draft picks",
    "bowman u now",
    "bowman university now",
]
# Collegiate set keywords — cards featuring NBA players in college/USA Basketball
# jerseys. Hard-rejected because Kevin doesn't buy the entire product line.
COLLEGIATE_SET_KEYWORDS = [
    "bowman university",
    "bowman u ",            # trailing space avoids matching "Bowman Update"
    "bowman u'",            # catches "Bowman U's" / possessive forms
    "topps chrome university",
    "topps university",
    "panini prizm draft picks",
    "prizm draft picks",
    "panini chronicles draft",
    "chronicles draft",
    "stars & stripes",
    "stars and stripes",
    "usa basketball",
]


def check_basketball_rejections(comp):
    """
    Apply basketball-specific buying rules. Returns (status, reason) where status
    is 'accepted', 'rejected', or 'flag' (passes through but Kevin should verify).
    Called after the standard PSA checks pass.
    """
    name = (comp.get('cardName') or '').lower()
    set_name = (comp.get('setName') or comp.get('set') or '').lower()
    try:
        cv = float(comp.get('clValue') or 0)
    except (TypeError, ValueError):
        cv = 0.0

    # Always-reject players (any price)
    for player in BBALL_PLAYERS_REJECT_ALWAYS:
        if player in name:
            return ('rejected', f"{player.title()} (not buying)")

    # WNBA players
    for player in WNBA_PLAYERS:
        if player in name:
            return ('rejected', f"WNBA ({player.title()})")

    # Over-$200 player cap
    if cv > BBALL_PLAYER_PRICE_CAP:
        for player in BBALL_PLAYERS_REJECT_OVER_200:
            if player in name:
                return ('rejected',
                        f"{player.title()} over ${BBALL_PLAYER_PRICE_CAP} (${cv:,.0f})")

    # Collegiate sets — hard reject (NBA players in college jerseys, draft picks lines, USA Basketball)
    combined = name + " " + set_name
    for keyword in COLLEGIATE_SET_KEYWORDS:
        if keyword in combined:
            return ('rejected', f"collegiate set ('{keyword.strip()}') — not buying")

    # Unlicensed brand → flag for manual review, don't auto-reject
    for brand in UNLICENSED_BRAND_KEYWORDS:
        if brand in combined:
            return ('flag', f"possibly unlicensed brand ('{brand}') — verify before payment")

    return ('accepted', None)


def _fmt_pct(rate):
    """Format a rate as a percent, trimming a trailing .0 (e.g. 0.915 -> '91.5%')."""
    s = f"{rate * 100:.1f}".rstrip('0').rstrip('.')
    return f"{s}%"


def _sport_label(sport):
    """Display name for a canonical sport key (e.g. 'mlb' -> 'MLB'). 'cgc' is
    the quote breakdown's CGC Pokémon group (price_and_send_psa_offer)."""
    return {'mlb': 'MLB', 'one piece': 'One Piece',
            'cgc': 'CGC Pokemon'}.get(sport, (sport or '').title())


def _blended_per_card_rate(tiers, card_values):
    """Blend a per-card tiered rate across a list of card values."""
    if not card_values:
        return tiers[0][2]
    total = sum(card_values)
    if total <= 0:
        return tiers[0][2]
    payout = 0.0
    for cv in card_values:
        for low, high, rate in tiers:
            if low <= cv < high:
                payout += cv * rate
                break
        else:
            payout += cv * tiers[-1][2]
    return payout / total


# ── POKÉMON SLOT QUOTAS (Kevin, Oct 3) ───────────────────────────────────────────
# Lazy-loaded store (DATA_DIR is defined later in the file; fully resolved
# before any Discord event runs). Counts accepted cards per quota band.
_POKEMON_QUOTA = None

# Deploy-wipe safety (Oct 5): DATA_DIR has NO Railway volume, so every deploy
# deletes the store and counts restart at 0 — which silently REOPENED full
# bands and made Kevin re-send `!quota set ...` after every deploy (he did it
# 3x on Oct 4 alone). A fresh store now seeds the week's capped bands as FULL:
# wrong only in the safe direction (over-reject), and `!quota set <band>
# <used>` reopens slots instantly. UPDATE THIS SEED when the weekly flyer
# resets allocations.
POKEMON_QUOTA_SEED_USED = {}  # Oct 10-11: no capped bands this weekend

def _pokemon_quota_load():
    global _POKEMON_QUOTA
    if _POKEMON_QUOTA is None:
        try:
            with open(os.path.join(DATA_DIR, "pokemon_quota_store.json")) as f:
                _POKEMON_QUOTA = {str(k): int(v) for k, v in json.load(f).items()}
        except Exception:
            _POKEMON_QUOTA = dict(POKEMON_QUOTA_SEED_USED)
            _pokemon_quota_save()   # persist so `!quota` shows the seeded state
    return _POKEMON_QUOTA

def _pokemon_quota_save():
    try:
        p = os.path.join(DATA_DIR, "pokemon_quota_store.json")
        with open(p + ".tmp", "w") as f:
            json.dump(_pokemon_quota_load(), f)
        os.replace(p + ".tmp", p)
    except Exception as e:
        print(f"quota store save failed (non-critical): {e}")

def pokemon_band_for(cv):
    """The quota band tuple (lo, hi, key, cap) containing cv, or None."""
    for band in POKEMON_QUOTA_BANDS:
        if band[0] <= cv <= band[1]:
            return band
    return None

def pokemon_quota_cap(key, default_cap):
    """Effective cap: runtime override (cap:<key> in the store) beats the
    default from POKEMON_QUOTA_BANDS. None = uncapped."""
    ov = _pokemon_quota_load().get(f"cap:{key}")
    return ov if ov is not None else default_cap

def pokemon_quota_left(key, cap):
    eff = pokemon_quota_cap(key, cap)
    if eff is None:
        return None   # uncapped
    return max(0, eff - _pokemon_quota_load().get(key, 0))

def pokemon_quota_take(key):
    """Count one accepted card against a quota band; returns the new count."""
    q = _pokemon_quota_load()
    q[key] = q.get(key, 0) + 1
    _pokemon_quota_save()
    return q[key]

def handle_quota_command(content):
    """!quota — show Pokémon slot usage. !quota set <band> <used> adjusts the
    used-count; !quota cap <band> <n|off> changes a band's cap at runtime
    (no deploy needed). Band keys: 100-200, 200-300, 400-700, 1400-1800."""
    toks = content.strip().split()
    q = _pokemon_quota_load()
    keys = [b[2] for b in POKEMON_QUOTA_BANDS if b[2]]
    if len(toks) >= 4 and toks[1].lower() in ('set', 'cap'):
        key = toks[2]
        if key not in keys:
            return f"Unknown band `{key}` — valid: {', '.join(keys)}"
        if toks[1].lower() == 'set':
            try:
                q[key] = max(0, int(toks[3]))
            except ValueError:
                return "Usage: `!quota set 100-200 <used-count>`"
            _pokemon_quota_save()
            return f"🧮 Set **{key}** used-count to **{q[key]}**."
        # cap
        if toks[3].lower() in ('off', 'none', 'unlimited'):
            q.pop(f"cap:{key}", None)
            _pokemon_quota_save()
            _d = next(b[3] for b in POKEMON_QUOTA_BANDS if b[2] == key)
            return f"🧮 Cap override removed for **{key}** (back to default: {_d if _d is not None else 'uncapped'})."
        try:
            q[f"cap:{key}"] = max(0, int(toks[3]))
        except ValueError:
            return "Usage: `!quota cap 400-700 <n>` or `!quota cap 400-700 off`"
        _pokemon_quota_save()
        return f"🧮 Cap for **{key}** set to **{q[f'cap:{key}']}**."
    lines = ["🧮 **Pokémon slot quotas (used/cap):**"]
    for lo, hi, key, cap in POKEMON_QUOTA_BANDS:
        if key:
            used = q.get(key, 0)
            eff = pokemon_quota_cap(key, cap)
            if eff is None:
                lines.append(f"• ${lo:g}–${hi:g}: **{used} used** · no cap")
            else:
                lines.append(f"• ${lo:g}–${hi:g}: **{used}/{eff}** ({max(0, eff - used)} left)")
        else:
            lines.append(f"• ${lo:g}–${hi:g}: unlimited")
    lines.append("`!quota set <band> <used>` · `!quota cap <band> <n|off>` (bands: " + ", ".join(keys) + ")")
    return "\n".join(lines)


# ── SHOP OPEN/CLOSED SWITCH (Kevin, Oct 5) ───────────────────────────────────────
# Master off-switch for buying (Oct 3-4 weekend: sheets closed 12 AM Sunday but
# the bot kept quoting overnight tickets). While CLOSED, cert submissions and
# Collectr CSVs in ticket channels get SHOP_CLOSED_MSG instead of a
# sheet/comps/quote. Everything else keeps working: all ! commands, and the
# proceed/shipping/tracking flow for lots that were quoted before the close.
# Toggle live with "!shop closed" / "!shop open" (persisted to DATA_DIR like the
# quota store) — but DATA_DIR has NO volume on Railway, so a deploy wipes the
# toggle and the bot wakes up on the SEED: the SHOP_CLOSED env var if set
# (1/true/yes/on/closed = closed), else SHOP_CLOSED_DEFAULT below. For a close
# that must survive deploys, set SHOP_CLOSED=1 in the Railway env (changing an
# env var redeploys, so it takes effect on its own) or flip the constant.
SHOP_CLOSED_DEFAULT = False

def _shop_closed_seed():
    """What a fresh deploy wakes up as (env var beats the code constant)."""
    v = os.environ.get("SHOP_CLOSED")
    if v is None or not v.strip():
        return SHOP_CLOSED_DEFAULT
    return v.strip().lower() in ("1", "true", "yes", "on", "closed")

SHOP_CLOSED_MSG = (
    "🔒 **Buying is closed until next week** — Kevin will announce when sheets "
    "reopen. Hang onto your list and drop it here then! 🙏"
)

# Lazy-loaded like the quota store (DATA_DIR is defined later in the file;
# fully resolved before any Discord event runs).
_SHOP = None

def _shop_load():
    global _SHOP
    if _SHOP is None:
        try:
            with open(os.path.join(DATA_DIR, "shop_store.json")) as f:
                _SHOP = json.load(f)
        except Exception:
            _SHOP = {}
    return _SHOP

def _shop_save():
    try:
        p = os.path.join(DATA_DIR, "shop_store.json")
        with open(p + ".tmp", "w") as f:
            json.dump(_shop_load(), f)
        os.replace(p + ".tmp", p)
    except Exception as e:
        print(f"shop store save failed (non-critical): {e}")

def shop_is_closed():
    """A !shop toggle in the store wins; with no store entry (fresh deploy,
    wiped DATA_DIR) the seed decides."""
    v = _shop_load().get("closed")
    return _shop_closed_seed() if v is None else bool(v)

def handle_shop_command(content):
    """!shop — show buying status. !shop closed / !shop open flips it
    immediately, no deploy needed. The reply always says what the deploy seed
    is, because a deploy wipes the toggle back to it (no DATA_DIR volume)."""
    toks = content.strip().lower().split()
    sub = toks[1] if len(toks) > 1 else ""
    seed_label = "CLOSED" if _shop_closed_seed() else "OPEN"
    if sub in ("closed", "close"):
        _shop_load()["closed"] = True
        _shop_save()
        return ("🔒 **Buying is now CLOSED** — cert/CSV submissions in tickets "
                "get the closed message instead of a quote. Already-quoted lots "
                "can still proceed/ship, and all commands keep working.\n"
                f"⚠️ A deploy resets this to the seed (**{seed_label}**) — for a "
                "close that survives deploys, set `SHOP_CLOSED=1` in Railway.")
    if sub == "open":
        _shop_load()["closed"] = False
        _shop_save()
        reply = "✅ **Buying is now OPEN** — submissions quote normally."
        if _shop_closed_seed():
            reply += ("\n⚠️ The deploy seed is **CLOSED** (`SHOP_CLOSED` env "
                      "var / constant) — any deploy re-closes buying until you "
                      "clear it.")
        return reply
    status = "🔒 **CLOSED**" if shop_is_closed() else "✅ **OPEN**"
    return (f"Buying is {status} (deploy seed: {seed_label}).\n"
            "`!shop closed` · `!shop open`")


def _name_on_list(comp, names):
    """True if the card name contains any name from `names` (lowercase
    substring, same semantics as the NBA ban lists)."""
    card = (comp.get('cardName') or '').lower()
    return any(n in card for n in names)


def is_qb_goat(comp):
    return _name_on_list(comp, QB_GOATS)


def is_mlb_slugger(comp):
    return _name_on_list(comp, TRIUMPH_MLB_SLUGGERS)


def is_nba_2000s(comp):
    return _name_on_list(comp, TRIUMPH_NBA_2000S)


def select_list_for_sport(sport):
    """The select-player list that unlocks a sport's extended bands, or None."""
    return {'mlb': TRIUMPH_MLB_SLUGGERS, 'football': QB_GOATS,
            'basketball': TRIUMPH_NBA_2000S,
            'pokemon': TRIUMPH_POKEMON_NVD}.get(sport)


def _pokemon_card_rate(c, vip_tiers=None):
    """Per-card Pokémon rate from the band tiers (standard or VIP). Cards that
    failed the band rules (big-ticket review lane) never reach pricing."""
    cv = float(c['clValue'])
    tiers = vip_tiers or PSA_POKEMON_PER_CARD_TIERS
    if c.get('pokemon_band_ok', True):
        for low, high, r in tiers:
            if low <= cv < high:
                return r
    return tiers[-1][2]


def get_psa_payout_rate(sport, sport_lot_total, card_values=None):
    """
    Return the effective (blended) per-card payout rate for a sport's accepted cards.
    Sep 18 weekend rates:
    - pokemon: $1-100 → 91%; $450-600 → 86%; $1,201-1,600 → 86% (gaps rejected).
    - basketball: $1-350 → 94% PSA 7+ (ceiling $350).
    - mlb: $26-400 → 89% PSA 7+ (floor $26, ceiling $400).
    - one piece: $1-100 → 88% PSA 7+ (ceiling $100).
    - football (NFL): NOT BUYING (absent from PSA_SPORT_MAX_PRICE).
    Reject-zones / ceilings / grade floors are filtered in classify_psa_comp, so
    only in-band cards reach these blends.
    """
    if sport == 'pokemon':
        return _blended_per_card_rate(PSA_POKEMON_PER_CARD_TIERS, card_values)
    if sport == 'basketball':
        return _blended_per_card_rate(PSA_BASKETBALL_PER_CARD_TIERS, card_values)
    if sport == 'mlb':
        return _blended_per_card_rate(PSA_MLB_PER_CARD_TIERS, card_values)
    if sport == 'one piece':
        return _blended_per_card_rate(PSA_ONE_PIECE_PER_CARD_TIERS, card_values)
    if sport == 'football':
        return _blended_per_card_rate(PSA_FOOTBALL_PER_CARD_TIERS, card_values)
    return PSA_POKEMON_PER_CARD_TIERS[0][2]

# VIP rates PAUSED while we're buying One Piece raws (May 2026).
# Standard raw tiers are 85% (88% on $10k+ lots), so legacy VIP rates of 87/89%
# would exceed margin on sub-$10k lots.
# Keep lists here for easy re-enable later; treated as standard tier for now.
VIP_CLIENTS = []
VIP_CLIENTS_89 = []

# ── PAYOUT CALCULATOR ────────────────────────────────────────────────────────────
def get_raw_rate(game, game_total, username):
    """Payout % for ONE game's singles portion. Each game's tier is judged on
    its OWN portion of the lot: One Piece hits 88% at $10k+ of One Piece,
    Pokémon hits 88% at $5k+ of Pokémon (Jul 31 2026)."""
    username_lower = username.lower()
    if username_lower in VIP_CLIENTS_89:
        return 0.89, "VIP rate"
    if username_lower in VIP_CLIENTS:
        return 0.87, "VIP rate"
    tiers = POKEMON_RAW_PAYOUT_TIERS if game == 'pokemon' else RAW_PAYOUT_TIERS
    for low, high, rate in tiers:
        if low <= game_total < high:
            return rate, f"${low:,}–{'$'+str(int(high)//1000)+'k' if high != float('inf') else '+'} tier"
    return 0.80, "standard rate"


def _has_non_latin_script(text):
    """True if text contains a character from a non-Latin writing system
    (Japanese kana/kanji, Korean hangul, Chinese, Cyrillic, etc.) — i.e. a
    genuinely foreign-language card. Typographic punctuation and accented Latin
    letters (curly apostrophes in "Luffy's", en-/em-dashes, ™, é) are NOT treated
    as foreign: they show up in normal ENGLISH Collectr exports and must not trip
    the non-English filter. (The old check rejected on *any* byte > 127, which
    started false-flagging every English card the moment Collectr switched to
    curly apostrophes.)"""
    for c in text:
        o = ord(c)
        if o < 0x80:
            continue  # plain ASCII
        if (0x3040 <= o <= 0x30FF or   # Hiragana + Katakana
                0x31F0 <= o <= 0x31FF or   # Katakana phonetic extensions
                0x3400 <= o <= 0x4DBF or   # CJK Extension A
                0x4E00 <= o <= 0x9FFF or   # CJK Unified Ideographs
                0xF900 <= o <= 0xFAFF or   # CJK Compatibility Ideographs
                0xAC00 <= o <= 0xD7AF or   # Hangul syllables
                0x1100 <= o <= 0x11FF or   # Hangul Jamo
                0x3130 <= o <= 0x318F or   # Hangul Compatibility Jamo
                0xFF00 <= o <= 0xFFEF or   # Halfwidth/Fullwidth forms
                0x0400 <= o <= 0x04FF):    # Cyrillic
            return True
    return False


def parse_collectr_csv(content_bytes):
    """
    Parse a Collectr CSV export and validate it per game (Jul 31 2026):
      ONE PIECE singles: English, $1-$150 per card (unchanged).
      POKÉMON singles:   English, Near Mint, UNGRADED, $40-$80 per card,
                         2022+ sets only, no trainer cards, no Master Ball
                         versions (data + helpers in pokemon_species.py).
      Any other game is rejected.
    BLOCK UNTIL CLEAN: any violating row lands in `issues` and the caller
    refuses to quote until a clean re-export arrives — same handling for both
    games. Totals/counts come back per game (game_totals / game_counts) so
    mixed lots are priced per portion.
    """
    df = pd.read_csv(io.BytesIO(content_bytes))

    # Find the market price column (Collectr includes the date in the column name)
    price_col = None
    for col in df.columns:
        if "Market Price" in col:
            price_col = col
            break
    if not price_col:
        return None, "Couldn't find market price column in this CSV."

    df[price_col] = pd.to_numeric(df[price_col], errors='coerce').fillna(0)

    qty_col = 'Quantity' if 'Quantity' in df.columns else None
    if qty_col:
        df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(1)
        df['_line_total'] = df[price_col] * df[qty_col]
    else:
        df['_line_total'] = df[price_col]

    # ── VALIDATION ──────────────────────────────────────────────────────────────

    # Detect game/category — Collectr uses 'Game' or 'Category' column
    game_col = None
    for candidate in ['Game', 'TCG', 'Category']:
        if candidate in df.columns:
            game_col = candidate
            break

    def _game_of(row):
        """'pokemon' / 'one piece' / 'other'. Pokémon must match EXACTLY —
        "Pokemon Japan" / "Pokémon (JP)" must NOT ride in under English
        Pokémon rules (they fall to 'other' and get blocked, matching the old
        code where every pokemon-ish row was declined). A blank category in a
        modern export is 'other' too (fail closed — the old code blocked NaN
        categories; treating them as One Piece would let Pokémon rows dodge
        every Pokémon rule). No game column at all = legacy export = One Piece."""
        if not game_col:
            return 'one piece'
        v = pokemon_species.canon(str(row.get(game_col, '')).strip())
        if v == 'pokemon':
            return 'pokemon'
        if not v or v == 'nan':
            return 'other'
        if 'one piece' in v:
            return 'one piece'
        return 'other'

    df['_game'] = df.apply(_game_of, axis=1)

    non_english_tags = ('(jp)', '(kr)', '(cn)', '(tw)', '(kor)', '(jpn)', '(chn)')
    other_game_cards = []
    non_english = []
    op_over_max, op_under_min = [], []
    pk_price, pk_condition, pk_graded = [], [], []
    pk_master_ball, pk_trainer, pk_old_set, pk_sealed = [], [], [], []
    pokemon_off = []
    onepiece_off = []

    def _cell(row, col):
        """Cell as a clean string: pandas turns blanks into NaN and
        str(nan) == 'nan', which must read as empty, not as a value."""
        v = str(row.get(col, '')).strip()
        return '' if v.lower() == 'nan' else v

    for _, row in df.iterrows():
        name = str(row.get('Product Name', 'Unknown'))
        set_name = _cell(row, 'Set')
        game = row['_game']
        price = float(row[price_col])
        label = f"• {name} ({set_name})"

        if game == 'other':
            cat = _cell(row, game_col) if game_col else ''
            other_game_cards.append(f"{label} — {cat or '(no category)'}")
            continue

        # Raw Pokémon toggled OFF (absent from this week's flyer) — decline the
        # rows without running the per-card rules.
        if game == 'pokemon' and not BUYING_POKEMON_RAW:
            pokemon_off.append(label)
            continue
        # Raw One Piece toggled OFF (Aug 28: One Piece fully off).
        if game == 'one piece' and not BUYING_ONE_PIECE_RAW:
            onepiece_off.append(label)
            continue

        # English only (both games). Collectr marks foreign cards two ways:
        #   1. Language tag in the product name: "(JP)", "(KR)", "(CN)", ...
        #   2. Non-Latin script (kana/kanji/hangul/...) in name or set. We do
        #      NOT reject on any non-ASCII byte — English exports contain curly
        #      apostrophes / ™ / é (see _has_non_latin_script).
        if (any(tag in name.lower() for tag in non_english_tags)
                or _has_non_latin_script(name + set_name)):
            non_english.append(label)

        if game == 'one piece':
            # One Piece: per-card price $1-$150 (unchanged rules)
            if price > RAW_MAX_PRICE:
                op_over_max.append(f"• {name} — ${price:.2f}")
            elif price < RAW_MIN_PRICE:
                op_under_min.append(f"• {name} — ${price:.2f}")
            continue

        # ── Pokémon rules (Jul 31 2026) ─────────────────────────────────────
        variance = _cell(row, 'Variance')
        condition = _cell(row, 'Card Condition')
        grade = _cell(row, 'Grade')

        if not (POKEMON_RAW_MIN_PRICE <= price <= POKEMON_RAW_MAX_PRICE):
            pk_price.append(f"• {name} — ${price:.2f}")
        if pokemon_species.canon(condition) != 'near mint':
            pk_condition.append(f"{label} — {condition or 'no condition given'}")
        # Graded cards don't belong in a raw-singles export — they go through
        # the PSA slab flow (cert numbers) instead. Blank Grade = ungraded.
        if grade and pokemon_species.canon(grade) != 'ungraded':
            pk_graded.append(f"{label} — {grade}")
        if pokemon_species.is_master_ball_variant(name, variance):
            pk_master_ball.append(label)
        # Japanese-only sets carry Latin names ("VSTAR Universe", "... sv2a
        # Japanese") that the script/tag checks above can't see.
        if pokemon_species.is_non_english_set(set_name) and label not in non_english:
            non_english.append(label)
        # Sealed product titled after its featured Pokémon ("Pikachu V Box",
        # "... UPC Promo Sealed") would pass the species filter — veto first.
        if pokemon_species.is_sealed_product_name(name):
            pk_sealed.append(label)
        # FAIL-CLOSED trainer filter: accept only if the name contains a
        # recognized Pokémon species. Trainers/items/energy don't.
        elif (pokemon_species.find_species(name) is None
                or pokemon_species.is_trainer_with_species_name(name)):
            pk_trainer.append(label)
        if pokemon_species.is_pre_2022_set(set_name, _cell(row, 'Card Number'), name):
            pk_old_set.append(label)

    issues = []
    if pokemon_off:
        issues.append(("pokemon_off", pokemon_off))
    if onepiece_off:
        issues.append(("onepiece_off", onepiece_off))
    if other_game_cards:
        issues.append(("other_game", other_game_cards))
    if non_english:
        issues.append(("non_english", non_english))
    if op_over_max:
        issues.append(("over_max", op_over_max))
    if op_under_min:
        issues.append(("under_min", op_under_min))
    if pk_price:
        issues.append(("pk_price", pk_price))
    if pk_condition:
        issues.append(("pk_condition", pk_condition))
    if pk_graded:
        issues.append(("pk_graded", pk_graded))
    if pk_master_ball:
        issues.append(("pk_master_ball", pk_master_ball))
    if pk_sealed:
        issues.append(("pk_sealed", pk_sealed))
    if pk_trainer:
        issues.append(("pk_trainer", pk_trainer))
    if pk_old_set:
        issues.append(("pk_old_set", pk_old_set))

    total = df['_line_total'].sum()
    card_count = int(df[qty_col].sum()) if qty_col else len(df)

    game_totals, game_counts = {}, {}
    for g in ('one piece', 'pokemon'):
        sub = df[df['_game'] == g]
        game_totals[g] = float(sub['_line_total'].sum())
        game_counts[g] = int(sub[qty_col].sum()) if qty_col else len(sub)

    top_cards = df.nlargest(5, '_line_total')[['Product Name', 'Set', price_col, '_line_total']].copy()
    top_list = []
    for _, row in top_cards.iterrows():
        name = str(row.get('Product Name', 'Unknown'))
        set_name = str(row.get('Set', ''))
        price = row['_line_total']
        top_list.append(f" • {name} ({set_name}) — ${price:.2f}")

    return {
        "total": total,
        "card_count": card_count,
        "game_totals": game_totals,
        "game_counts": game_counts,
        "top_cards": top_list,
        "issues": issues,
        "df": df
    }, None


# ── GOOGLE SHEETS ────────────────────────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def get_credentials():
    """Load Google credentials from env var (Railway) or file (local)."""
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        return Credentials.from_service_account_info(info, scopes=SCOPES)
    return Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)

def get_gspread_client():
    return gspread.authorize(get_credentials())

def get_drive_service():
    return build("drive", "v3", credentials=get_credentials())

def _find_fresh_psa_sheet(username, since_dt, folder_id=None):
    """Drive fallback for create_psa_sheet: the Apps Script usually FINISHES
    creating the sheet server-side even when our HTTP call to it dies (Aug 2
    morning: a transient Google 404 and a client timeout — both sheets existed
    in Drive anyway). Find a '<username> discord ...' sheet created after we
    started calling, so a retry doesn't make a duplicate. The name must be
    EXACTLY '<username> discord …' — a bare startswith(username) let 'mark'
    adopt markandjheyson's fresh sheet (and 'tr' trrev's)."""
    try:
        drive = get_drive_service()
        safe = username.replace("\\", "\\\\").replace("'", "\\'")
        since_iso = since_dt.strftime("%Y-%m-%dT%H:%M:%S")
        q = (f"name contains '{safe} discord' and '{folder_id or PSA_FOLDER_ID}' in parents "
             f"and trashed=false and createdTime > '{since_iso}'")
        files = drive.files().list(q=q, orderBy="createdTime desc", pageSize=5,
                                   fields="files(id,name,createdTime)").execute().get("files", [])
        want = username.lower() + " discord"
        for f in files:
            fname = f["name"].lower()
            if (fname == want or fname.startswith(want + " ")) \
                    and not f["name"].endswith(SUMMARY_NAME_SUFFIX):
                return f
    except Exception as e:
        print(f"Drive fallback search failed: {e}")
    return None

def create_psa_sheet(username, cert_numbers, folder_id=None):
    """Create a buying sheet by calling the Google Apps Script web app.
    folder_id: Drive folder for the new file (default: the PSA Slabs folder);
    the !summary sheets pass their own folder so they never sit among the
    buying sheets that Kevin's nightly reconcile sweeps.
    Google gets slow/flaky under weekend-morning load: a 103-cert sheet took
    54s on a HEALTHY run (the old 30s timeout could never survive it), and
    failed calls usually still created their sheet server-side. So: generous
    timeout; on failure poll Drive for the sheet Google may have finished
    anyway (returns it with no duplicate); only then retry the call once."""
    import urllib.request
    import urllib.parse
    import time as _time
    from datetime import timedelta as _td
    # Column B carries bare numbers — a CGC token 'cgc:<digits>' goes in as its
    # digits (fill_buying_sheet then stamps "CGC" in column A).
    certs_str = ",".join([cert_digits(c) for c in cert_numbers])
    folder_id = folder_id or PSA_FOLDER_ID
    params = urllib.parse.urlencode({"username": username, "certs": certs_str, "folder_id": folder_id})
    url = f"{APPS_SCRIPT_URL}?{params}"
    started = datetime.utcnow() - _td(seconds=30)   # slack for clock skew vs Drive
    last_err = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "KTS-Bot/1.0"})
            with urllib.request.urlopen(req, timeout=150) as resp:
                data = json.loads(resp.read().decode())
            if not data.get("success"):
                raise Exception(data.get("error", "Unknown error from Apps Script"))
            return data["url"], data["name"], data
        except Exception as e:
            last_err = e
            print(f"Apps Script sheet call failed (attempt {attempt}/2): {e}")
            # Server-side run may still be finishing — poll Drive up to ~60s.
            for _ in range(6):
                _time.sleep(10)
                f = _find_fresh_psa_sheet(username, started, folder_id)
                if f:
                    sheet_url = f"https://docs.google.com/spreadsheets/d/{f['id']}/edit"
                    print(f"Apps Script call failed but the sheet WAS created — recovered '{f['name']}' from Drive")
                    return sheet_url, f["name"], {"sheet_id": f["id"], "url": sheet_url,
                                                  "name": f["name"], "recovered": True}
    raise last_err


# ── HELPER API (CARDLADDER COMPS) ────────────────────────────────────────────────
def extract_sheet_id(url):
    """Pull the sheet ID out of a Google Sheets URL."""
    m = re.search(r'/d/([a-zA-Z0-9_-]+)', url or "")
    return m.group(1) if m else None


# ── CGC CERT TOKENS (Oct 18) ─────────────────────────────────────────────────────
# A PSA cert travels through the bot as its bare digits ("12345678"); a CGC cert
# as "cgc:<digits>" — the exact string the comp helper takes (CardLadder lookup
# with grader=cgc) and echoes back in each result's "cert" field. Sheets get the
# bare digits (column B) plus "CGC" in column A; Discord shows "CGC <digits>".
def is_cgc_cert(cert):
    """True for a CGC cert token ('cgc:6193949247')."""
    return str(cert if cert is not None else '').strip().lower().startswith(CGC_PREFIX)

def cert_digits(cert):
    """The bare cert number (sheet column B / CardLadder link) of a PSA or CGC token."""
    s = str(cert if cert is not None else '').strip()
    return s[len(CGC_PREFIX):].strip() if is_cgc_cert(s) else s

def cgc_cert(digits):
    return CGC_PREFIX + str(digits).strip()

def cert_label(cert):
    """How a cert reads in Discord: '12345678' (PSA, unchanged) or 'CGC 6193949247'."""
    return (f"CGC {cert_digits(cert)}" if is_cgc_cert(cert)
            else str(cert if cert is not None else '').strip())

def _comp_is_cgc(c):
    return bool(c) and is_cgc_cert(c.get('cert'))

def _cgc_is_pokemon(c):
    """CardLadder files CGC Pokémon under sport 'pokemon'; a blank sport counts
    when the card name says Pokémon."""
    sp = normalize_sport((c or {}).get('sport'))
    if sp:
        return sp == 'pokemon'
    return 'pokemon' in str((c or {}).get('cardName') or '').lower().replace('é', 'e')

def _slab_grade_label(c):
    """'PSA 10' (exactly the old Kevin-line text) or 'CGC 9.5'."""
    if _comp_is_cgc(c):
        g = re.sub(r'(?i)^\s*(cgc|psa)\s*', '', str(c.get('grade') or '')).strip()
        return f"CGC {g}".strip()
    return f"PSA {str((c or {}).get('grade') or '').replace('PSA ', '').strip()}"

def _sheet_cert_token(a_val, digits, e_val=''):
    """A buying-sheet row's cert as the bot's token: CGC when column A says CGC,
    the grade (E) reads 'CGC …', or the number is CGC-length (10+ digits — PSA
    certs are 7-9; a CGC row whose comps never loaded still shows the template's
    default 'PSA' in A)."""
    d = str(digits if digits is not None else '').strip()
    a = str(a_val if a_val is not None else '').strip().upper()
    e = str(e_val if e_val is not None else '').strip().upper()
    if a == 'CGC' or e.startswith('CGC') or len(d) >= 10:
        return cgc_cert(d)
    return d


# Cloudflare tunnels cut HTTP responses at ~100s. A COLD chunk of 4 sales-heavy
# scrapes (4 concurrent × 60-90s each) regularly blows past that, so every cold
# chunk burned its 180s client timeout and only the cache-warm second pass
# recovered it — a 59-cert lookup crawled for ~50 min. Chunks of 2 keep cold
# chunks comfortably under the ceiling; warm (cached) chunks are instant anyway.
HELPER_CHUNK_SIZE = 2

# One lookup at a time, across ALL tickets. A 241-slab lot split over 5 Discord
# messages used to run 5 lookup_comps concurrently, slamming the helper's single
# Brave with ~5 parallel chunk streams: scrapes half-failed under the load, those
# empty results got CACHED (poisoning every lookup for 24h), and CardLadder
# rate-limited the account. lookup_comps runs in asyncio.to_thread threads, so a
# threading.Lock is the right primitive.
import threading
_HELPER_LOCK = threading.Lock()

def _comp_is_bad(c):
    """A scrape that carries no usable value — missing entirely, found=false, or
    found=true with clValue None (the signature of a scrape that half-loaded
    under load, or of a poisoned cache entry)."""
    return (not c) or (not c.get('found')) or (c.get('clValue') is None)

# The helper answers found=false for TWO very different things: CardLadder truly
# has no such card, or the scrape itself broke (CardLadder page.goto timeout, the
# page never opened, vision JSON unparseable). Only the first is a real "no comp".
# Oct 18: a lone cert hit a CardLadder timeout and jon.mobile was told "no comp on
# CardLadder" for a $44k Skyridge Houndoom that loaded fine a minute later.
_HELPER_INFRA_NOTE_PREFIXES = ("lookup error", "page open failed", "parse error")

def _comp_is_infra_error(c):
    """True when we have no real answer for a cert: no entry at all (its chunk
    failed) or a found=false whose note is one of the helper's infrastructure
    errors. A genuine found=false (no such note) is a real answer — False."""
    if not c:
        return True
    if c.get('found'):
        return False
    note = str(c.get('note') or '').strip().lower()
    return note.startswith(_HELPER_INFRA_NOTE_PREFIXES)

def _uncovered_certs(certs, comps):
    """Certs with no usable helper answer yet (missing or infra error). Genuine
    not-founds count as covered — the card really isn't on CardLadder."""
    by_cert = {str(c.get('cert', '')).strip(): c for c in comps or []}
    return [str(c).strip() for c in certs if _comp_is_infra_error(by_cert.get(str(c).strip()))]

# When EVERY cert came back bad, the inline re-check only runs for a ticket that
# fits in ONE helper chunk (a lone timed-out cert like jon.mobile's): exactly one
# extra chunk (~90s max) under _HELPER_LOCK. Bigger all-bad tickets go to the
# background retry instead — re-scraping 20 timing-out certs inline held the lock
# ~16 extra min while every other ticket queued behind it.
HELPER_ALL_BAD_RETRY_CAP = HELPER_CHUNK_SIZE

# Customer-facing reason for a cert whose comp didn't load (customers read it).
COMP_NOT_LOADED_MSG = "CardLadder didn't load this one — resend it in a few minutes and I'll re-check"
# Column G marker on the (customer-visible) buying sheet for the same case. Text,
# not blank: rebuild_lot_from_sheets reads a blank G as $0 and would drop a cert
# accepted on an earlier sheet; text is skipped. !requoteall treats it as unfilled.
SHEET_NOT_LOADED_MARK = "didn't load — resend"

def _purge_helper_cache(certs):
    """Best-effort DELETE /cache/<cert> on the helper so a retry re-scrapes fresh
    instead of being served the same poisoned cache entry."""
    import urllib.request as urlreq
    for c in certs:
        try:
            req = urlreq.Request(
                f"{HELPER_URL}/cache/{c}", method="DELETE",
                headers={"X-API-Key": HELPER_API_KEY, "User-Agent": "KTS-Bot/1.0"})
            urlreq.urlopen(req, timeout=10).close()
        except Exception:
            pass

def _match_helper_results(chunk, results):
    """Point every helper result's 'cert' at the token we sent, so CGC answers
    land on their 'cgc:<digits>' cert. The CGC-aware helper echoes the prefixed
    string verbatim; a differently-cased prefix, or a bare-digits echo that
    matches exactly one sent cert, is mapped back too — except a bare echo whose
    CardLadder url says grader=psa (a PSA lookup must never answer for a CGC
    slab). Mutates and returns results."""
    sent = [str(c).strip() for c in chunk]
    exact = set(sent)
    by_lower = {s.lower(): s for s in sent}
    by_digits = {}
    for s in sent:
        by_digits.setdefault(cert_digits(s), []).append(s)
    for r in results or []:
        if not isinstance(r, dict):
            continue
        e = str(r.get('cert', '')).strip()
        if e in exact:
            continue
        tok = by_lower.get(e.lower())
        if tok is None:
            cands = by_digits.get(cert_digits(e)) or []
            if len(cands) == 1:
                tok = cands[0]
                if (is_cgc_cert(tok) and not is_cgc_cert(e)
                        and 'grader=psa' in str(r.get('url') or '').lower()):
                    tok = None
        if tok is not None:
            r['cert'] = tok
    return results

def _lookup_comps_once(cert_list):
    """Single pass over the helper in chunks; merged results list. Per-chunk
    failures are logged; if EVERY chunk fails the last error is raised.
    CGC certs are already 'cgc:<digits>' tokens — sent as-is; results are
    mapped back onto the sent tokens (_match_helper_results)."""
    import urllib.request as urlreq
    import time as _time
    chunks = [cert_list[i:i + HELPER_CHUNK_SIZE] for i in range(0, len(cert_list), HELPER_CHUNK_SIZE)]
    all_results = []
    last_error = None
    for idx, chunk in enumerate(chunks, 1):
        if idx > 1 and len(cert_list) > 40:
            _time.sleep(1.0)   # big lots: pace CardLadder instead of firehosing it
        payload = json.dumps({"certs": chunk}).encode("utf-8")
        req = urlreq.Request(
            f"{HELPER_URL}/comp/batch",
            data=payload,
            headers={"Content-Type": "application/json", "X-API-Key": HELPER_API_KEY, "User-Agent": "KTS-Bot/1.0"},
            method="POST",
        )
        try:
            with urlreq.urlopen(req, timeout=180) as resp:
                data = json.loads(resp.read().decode())
            all_results.extend(_match_helper_results(chunk, data.get("results", [])))
            print(f"Helper chunk {idx}/{len(chunks)} ok ({len(chunk)} certs)")
        except Exception as e:
            last_error = e
            print(f"Helper chunk {idx}/{len(chunks)} failed: {e}")
    if not all_results and last_error:
        raise last_error
    return all_results

def lookup_comps(certs, recheck_all_bad=True):
    """
    Call helper.ktscollectibles.com/comp/batch and return the merged results list.
    Each result: {cert, found, clValue, cardName, grade, recentSales, avg3Sales, salesCount, note}
    Returns [] if the helper key isn't set; raises only if every chunk fails.
    Serialized across tickets via _HELPER_LOCK. Bad results (missing / found=false /
    no value) from a PARTIALLY successful pass get one purge-cache-and-retry pass.
    When EVERYTHING came back bad, a re-check runs only if recheck_all_bad, the
    ticket fits in HELPER_ALL_BAD_RETRY_CAP certs (one chunk), and the bad results
    could plausibly recover (helper infra error / found-but-no-value — a genuine
    found=false would just be re-scraped to the same answer). That re-checks a
    1-cert ticket that hit a CardLadder timeout (Oct 18) without hammering a
    down / rate-limited helper; bigger all-bad tickets go to the background retry,
    which is itself the retry loop and passes recheck_all_bad=False.
    """
    if not HELPER_API_KEY or not certs:
        return []
    import time as _time
    cert_list = list(dict.fromkeys(str(c).strip() for c in certs))
    with _HELPER_LOCK:
        results = _lookup_comps_once(cert_list)
        by_cert = {str(c.get('cert', '')).strip(): c for c in results}
        bad = [c for c in cert_list if _comp_is_bad(by_cert.get(c))]
        if bad and results and len(bad) == len(cert_list):
            if recheck_all_bad and len(cert_list) <= HELPER_ALL_BAD_RETRY_CAP:
                bad = [c for c in bad if _comp_is_infra_error(by_cert.get(c))
                       or (by_cert.get(c) or {}).get('found')]
            else:
                bad = []
        if bad and results:
            print(f"Comp second pass: {len(bad)}/{len(cert_list)} bad results — purging cache, retrying")
            _purge_helper_cache(bad)
            _time.sleep(5)
            try:
                for c in _lookup_comps_once(bad[:100]):
                    k = str(c.get('cert', '')).strip()
                    # Take a good answer; also let a REAL answer (genuine
                    # not-found / no value) replace a first-pass timeout.
                    if k and (k not in by_cert or not _comp_is_bad(c)
                              or (_comp_is_infra_error(by_cert[k])
                                  and not _comp_is_infra_error(c))):
                        by_cert[k] = c
            except Exception as e:
                print(f"Comp second pass failed (keeping first-pass results): {e}")
        return [by_cert[k] for k in cert_list if k in by_cert]


def classify_cgc_comp(comp):
    """Kevin's CGC rules (Oct 18): Pokémon with a CardLadder value of $1-$100 is
    bought at CGC_POKEMON_RATE; over $100 and every non-Pokémon CGC slab are
    rejected; not-found / timeouts read exactly like PSA. Reasons are
    customer-visible. Returns ('accepted', None) or ('rejected', reason)."""
    if _comp_is_infra_error(comp):
        return ('rejected', COMP_NOT_LOADED_MSG)
    if not comp.get('found'):
        return ('rejected', 'no comp on CardLadder')
    cv = comp.get('clValue')
    try:
        cv = float(cv) if cv is not None else None
    except (TypeError, ValueError):
        cv = None
    if cv is None:
        return ('rejected', 'no CL value')
    if not _cgc_is_pokemon(comp):
        sp = str(comp.get('sport') or '').lower().strip() or 'not Pokémon'
        return ('rejected', f"{sp} (we only take Pokémon in CGC)")
    if cv > CGC_MAX_PRICE:
        return ('rejected', f"${cv:,.2f} (over our ${CGC_MAX_PRICE:,} max for CGC slabs)")
    if cv < CGC_MIN_PRICE:
        return ('rejected', f"${cv:,.2f} (under our ${CGC_MIN_PRICE} minimum for CGC slabs)")
    return ('accepted', None)


def classify_psa_comp(comp):
    """
    Decide whether a helper comp result fits KTS PSA buying criteria.
    Returns (status, reason): 'accepted'/None or 'rejected'/<short reason>.
    CGC comps (cert 'cgc:<digits>') are routed to classify_cgc_comp.
    """
    # Helper timeout / missing entry is NOT "no comp" — never tell a customer a
    # card isn't on CardLadder when the page simply didn't load (customer-visible).
    if _comp_is_infra_error(comp):
        return ('rejected', COMP_NOT_LOADED_MSG)
    # CGC slabs have their own (short) rulebook — none of the PSA lanes apply.
    if _comp_is_cgc(comp):
        return classify_cgc_comp(comp)
    if not comp.get('found'):
        return ('rejected', 'no comp on CardLadder')
    cv = comp.get('clValue')
    try:
        cv = float(cv) if cv is not None else None
    except (TypeError, ValueError):
        cv = None
    if cv is None:
        return ('rejected', 'no CL value')
    sport = normalize_sport(comp.get('sport'))
    if not sports_op_live() and sport in SPORTSOP_SPORTS:
        return ('rejected', SPORTS_OP_HOLD_MSG)
    max_price = PSA_SPORT_MAX_PRICE.get(sport)
    if max_price is None:
        sport_label = sport or 'unknown sport'
        return ('rejected',
                f"{sport_label} (we buy Pokemon, One Piece, MLB, NBA and NFL slabs)")
    if cv > max_price:
        return ('rejected', f"${cv:,.2f} (over our ${max_price:,} {sport} max)")
    if cv < PSA_MIN_PRICE:
        return ('rejected', f"${cv:.2f} (under ${PSA_MIN_PRICE} min)")
    # Oct 10-11 weekend floors/gaps straight from the Triumph allocation page:
    if sport == 'mlb' and cv < MLB_MIN_PRICE:
        return ('rejected',
                f"${cv:,.2f} (under our ${MLB_MIN_PRICE} baseball minimum this weekend)")
    # Oct 17-18 weekend gaps straight from the Triumph allocation page:
    if sport == 'mlb' and 240 < cv < 250:
        return ('rejected',
                f"${cv:,.2f} ($240-$250 is a gap in our MLB ranges this weekend — "
                f"taking $1-$240 and $250-$800)")
    if sport == 'one piece' and 700 < cv < 900:
        return ('rejected',
                f"${cv:,.2f} (our One Piece ranges are $1-$700 and $900-$1,000 "
                f"this weekend)")

    # ── SELECT-PLAYER lanes (Oct 2): above each sport's general ceiling,
    # only listed players qualify, and only inside the flyer's specialty
    # bands. Non-listed cards point sellers at the public checker page.
    if sport == 'mlb' and cv > MLB_GENERAL_MAX:
        if not is_mlb_slugger(comp):
            return ('rejected',
                    f"${cv:,.2f} (over our ${MLB_GENERAL_MAX} MLB max — select "
                    f"Sluggers only at $600+, check {SELECT_CHECK_URL})")
        if not any(lo <= cv <= hi for lo, hi in MLB_SLUGGER_BANDS):
            return ('rejected',
                    f"${cv:,.2f} (outside Slugger bands — $600-$1,000 / "
                    f"$3,750-$5,000 only right now)")
    if sport == 'football' and cv > NFL_GENERAL_MAX:
        if not is_qb_goat(comp):
            return ('rejected',
                    f"${cv:,.2f} (over our ${NFL_GENERAL_MAX} NFL max — QB "
                    f"GOATs only at $600+, check {SELECT_CHECK_URL})")
        if not any(lo <= cv <= hi for lo, hi in NFL_GOAT_BANDS):
            return ('rejected',
                    f"${cv:,.2f} (between QB GOAT bands — $600-$1,000 / "
                    f"$1,400-$2,000 only)")
    if sport == 'basketball':
        _in_gen = any(lo <= cv <= hi for lo, hi in NBA_GENERAL_BANDS)
        _in_2k = is_nba_2000s(comp) and any(lo <= cv <= hi
                                            for lo, hi in NBA_2000S_BANDS)
        if not _in_gen and not _in_2k:
            if cv > NBA_GENERAL_MAX:
                return ('rejected',
                        f"${cv:,.2f} (over our ${NBA_GENERAL_MAX:,} NBA max this weekend)")
            return ('rejected',
                    f"${cv:,.2f} (our NBA ranges are $1-$800 and $900-$1,400 "
                    f"this weekend)")

    # Pokémon (Oct 3 allocation update): unlimited ≤$100, slot-limited
    # $100-$200 / $200-$300 / $1,400-$1,800, everything else rejected.
    if sport == 'pokemon':
        if cv < POKEMON_MIN_PRICE:
            return ('rejected',
                    f"${cv:,.2f} (under our ${POKEMON_MIN_PRICE} Pokémon minimum)")
        if cv > PIXEL_WHOLE_HI:
            return ('rejected',
                    f"${cv:,.2f} (over our ${PIXEL_WHOLE_HI:,} Pokémon max this weekend)")
    pokemon_big = (sport == 'pokemon' and cv >= POKEMON_BIG_BAND_MIN)
    pokemon_pixel = (sport == 'pokemon' and PIXEL_LANE_LO <= cv <= PIXEL_LANE_HI)
    # Pixel: ALL Pokémon $17-$5,000 — any cert EXCEPT ones starting with 0
    # (fail closed on blanks). No 7-series gate (that was Triumph; Pokémon is
    # all-Pixel now, Option A Oct 16).
    if sport == 'pokemon' and PIXEL_WHOLE_LO <= cv <= PIXEL_WHOLE_HI:
        _cert_s = str(comp.get('cert', '')).strip()
        if not _cert_s or _cert_s.startswith('0'):
            return ('rejected',
                    f"cert {comp.get('cert')} — we can't take certs starting "
                    f"with 0 on Pokémon ${PIXEL_WHOLE_LO:,.0f}-${PIXEL_WHOLE_HI:,.0f}")
    # Grade floor PSA 7+ — EXCEPT Pokémon $3,000+ (flyer: any grade) and
    # NBA ≤ $1,000 (flyer row carries no PSA badge; mirrors Triumph).
    any_grade = pokemon_big or pokemon_pixel or (sport == 'basketball' and cv <= 1000)
    if not any_grade:
        grade_raw = str(comp.get('grade') or '').replace('PSA', '').strip()
        try:
            g = float(grade_raw)
        except ValueError:
            return ('rejected', f"grade '{grade_raw}' unrecognized")
        if g < PSA_MIN_GRADE:
            return ('rejected', f"PSA {grade_raw} (we buy {PSA_MIN_GRADE}-10 only)")
    last_sale = comp.get('lastSaleDate')
    if not last_sale:
        return ('rejected', 'no recent sale visible')
    try:
        last_d = datetime.strptime(str(last_sale).strip(), '%Y-%m-%d').date()
    except (ValueError, AttributeError):
        return ('rejected', f"unparseable sale date '{last_sale}'")
    # Sale-age gates (Kevin's decode: CL 4+ ≈ sale ≤30d, CL 3+ ≈ ≤60d):
    #   pokemon: ≤$100 60d · $100-$3,000 30d (CL 4+) · $3,000+ none (any conf)
    #   one piece: ≤$100 60d · >$100 30d (CL 4+)
    #   NBA: ≤$1,000 any sale ever · >$1,000 60d (CL 3+)
    #   MLB: ≤$100 any sale ever · >$100 60d (CL 3+)
    #   NFL: ≤$100 any sale ever · >$100 60d (CL 3+, GOAT bands included)
    if sport == 'pokemon':
        max_age = float('inf') if pokemon_big else 60   # ≤$100 + Pixel conf-3 ($100-$2k) = 60d; $2k+ conf-5 flagged (manual)
    elif sport == 'one piece':
        max_age = 60 if cv <= 100 else 30
    elif sport == 'basketball':
        max_age = float('inf') if cv <= 1000 else 60
    elif sport in ('mlb', 'football'):
        max_age = float('inf') if cv <= 100 else 60
    else:
        max_age = PSA_SPORT_MAX_AGE_DAYS.get(sport, PSA_MAX_AGE_DAYS)
    if (date.today() - last_d).days > max_age:
        return ('rejected', f"last sale {last_sale} (>{max_age:g}d ago)")

    # NBA player/set bans still apply (all values).
    if sport == 'basketball':
        return check_basketball_rejections(comp)

    # Pokémon $3,000+: auto-quoted at 87% per the flyer, but still ⚠️ flag
    # Kevin — big tickets deserve a look before paying.
    if pokemon_big:
        return ('flag', f"${cv:,.0f} Pokémon $2k-$5k → Pixel conf-5 FIRM — "
                        f"auto-quoted 87% (any grade); VERIFY 5/5 bars on "
                        f"CardLadder before you lock")

    return ('accepted', None)


def fill_buying_sheet(sheet_id, comps, sheet_name="Form. Put Date Here."):
    """
    Populate the freshly-created buying sheet with everything the user normally sees:
      A = "PSA", B = cert (already there), C = HYPERLINK to Cardladder,
      D = card name, E = grade, F = sport, G = CL Value, J = last sale date.
    Replicates the existing onEdit + cardladder-comp behavior since simple onEdit
    doesn't fire on programmatic writes (so the sheet copy lands empty otherwise).
    CGC rows (cert token 'cgc:<digits>', bare digits in B): A = "CGC", the
    CardLadder link uses grader=cgc, E = "CGC <grade>" (so the downstream buyer
    list can't mistake it for PSA), F = 'pokemon' for a Pokémon card, and H gets
    the bot's standard H formula (build_sheet_h_formula — its CGC arm pays
    CGC_POKEMON_RATE on Pokémon $1-$100, else 0) instead of the template's
    70% non-PSA branch; a formula, not a number, so a hand-corrected G
    re-prices the row. Column I (the template's ARRAYFORMULA G*H) is never
    written, so it keeps computing.
    Returns the cert tokens that have NO row in column B (nothing was written
    for them) — the caller alerts Kevin about quoted CGC certs that didn't
    make it onto the sheet.
    """
    if not comps:
        return []
    gc = get_gspread_client()
    ss = gc.open_by_key(sheet_id)
    try:
        sheet = ss.worksheet(sheet_name)
    except Exception:
        sheet = ss.sheet1
    cert_col = sheet.col_values(2)  # column B, 1-indexed
    cert_to_row = {}
    for i, val in enumerate(cert_col):
        v = str(val).strip()
        if v.isdigit() and 6 <= len(v) <= 13:   # 13: the longest CGC certs
            cert_to_row[v] = i + 1
    # A 10-13 digit CGC number could DISPLAY as 6.19395E+09 in an Automatic-
    # format cell; re-read column B unformatted for any CGC cert not matched.
    _cgc_missing = {cert_digits(c.get('cert')) for c in comps
                    if _comp_is_cgc(c) and cert_digits(c.get('cert')) not in cert_to_row}
    if _cgc_missing:
        try:
            for i, val in enumerate(sheet.col_values(2, value_render_option='UNFORMATTED_VALUE')):
                v = _summary_cert(val)
                if v in _cgc_missing and v not in cert_to_row:
                    cert_to_row[v] = i + 1
        except Exception as e:
            print(f"fill_buying_sheet: unformatted cert re-read failed ({e})")
    updates = []
    missing = []
    for c in comps:
        cert = str(c.get('cert', '')).strip()
        cgc = _comp_is_cgc(c)
        digits = cert_digits(cert)
        row = cert_to_row.get(digits)
        if not row:
            missing.append(cert)
            continue
        link_url = f"https://app.cardladder.com/search?cert={digits}&grader={'cgc' if cgc else 'psa'}"
        updates.append({'range': f'A{row}', 'values': [["CGC" if cgc else "PSA"]]})
        updates.append({'range': f'C{row}', 'values': [[f'=HYPERLINK("{link_url}","🔗 CardLadder")']]})
        if c.get('cardName'):
            updates.append({'range': f'D{row}', 'values': [[c['cardName']]]})
        if cgc:
            updates.append({'range': f'E{row}', 'values': [[_slab_grade_label(c)]]})
        elif c.get('grade'):
            grade_clean = str(c['grade']).replace('PSA ', '').replace('PSA', '').strip()
            updates.append({'range': f'E{row}', 'values': [[grade_clean]]})
        if cgc and c.get('found') and _cgc_is_pokemon(c):
            updates.append({'range': f'F{row}', 'values': [["pokemon"]]})
        elif c.get('sport'):
            updates.append({'range': f'F{row}', 'values': [[str(c['sport']).lower().strip()]]})
        if c.get('found') and c.get('clValue') is not None:
            # Manual-review cards (Pokémon $3,750+): do NOT show the comp on
            # the customer-visible sheet — Kevin market-checks and writes his
            # own number (Kevin, Sep 26). His DM still carries the CL value.
            if classify_psa_comp(c)[0] == 'review':
                updates.append({'range': f'G{row}', 'values': [["Kevin will quote"]]})
            else:
                updates.append({'range': f'G{row}', 'values': [[c['clValue']]]})
        elif _comp_is_infra_error(c):
            # Page timed out / never opened — NOT "not found" (customer-visible).
            updates.append({'range': f'G{row}', 'values': [[SHEET_NOT_LOADED_MARK]]})
        else:
            updates.append({'range': f'G{row}', 'values': [["not found"]]})
        if cgc:
            updates.append({'range': f'H{row}', 'values': [[build_sheet_h_formula(row)]]})
        if c.get('lastSaleDate'):
            updates.append({'range': f'J{row}', 'values': [[c['lastSaleDate']]]})
        # SELECT-list stamp (Oct 2): column L carries "Y" when the card's
        # player is on the sport's select list (Sluggers / 2000s / QB GOATs).
        # The H formula's specialty bands require it — this is how the
        # 1,001-name list reaches the sheet without living in a formula.
        # (PSA only — CGC has no select lanes.)
        _sp = normalize_sport((c.get('sport') or '').lower().strip())
        _sel_list = None if cgc else select_list_for_sport(_sp)
        if _sel_list and _name_on_list(c, _sel_list):
            updates.append({'range': f'L{row}', 'values': [["Y"]]})
    if updates:
        sheet.batch_update(updates, value_input_option="USER_ENTERED")
    return missing


def highlight_certs_review(sheet_id, certs, sheet_name="Form. Put Date Here."):
    """Shade the rows of the given certs bright orange so Kevin can't miss them and
    knows to price them by hand (cards classify_psa_comp returns as 'review').
    Best-effort: a failure to format must never block the quote."""
    if not certs:
        return
    gc = get_gspread_client()
    ss = gc.open_by_key(sheet_id)
    try:
        sheet = ss.worksheet(sheet_name)
    except Exception:
        sheet = ss.sheet1
    cert_set = {cert_digits(c) for c in certs}   # column B holds bare digits (CGC too)
    orange = {"backgroundColor": {"red": 1.0, "green": 0.6, "blue": 0.0}}
    for i, val in enumerate(sheet.col_values(2)):  # column B = cert
        if str(val).strip() in cert_set:
            sheet.format(f"A{i + 1}:J{i + 1}", orange)


# ── CERT EXTRACTION ──────────────────────────────────────────────────────────────
# One pass finds every 7-13 digit number, remembering which grader label (if any)
# sits right in front of it ON THE SAME LINE ("CGC 6193949247", "cgc:6193949247",
# "CGC #…", "BGS 0012345678"). The label may not reach across a line break: in
# "PSA + CGC\n12345678\n6193949247" the CGC label must not claim the PSA cert.
_CERT_NUMBER_RE = re.compile(
    r'(?:\b(cgc|psa|bgs|sgc|tag|beckett)[ \t]*[:#\-]?[ \t]*|\b)(\d{7,13})\b', re.I)
# Discord timestamps (<t:1729300000:R>) carry 10-digit numbers — never certs.
_DISCORD_TS_RE = re.compile(r'<t:\d+(?::[a-zA-Z])?>')
# Context that means a bare 10-13 digit number is NOT a CGC cert: tracking
# numbers, phone numbers, payment / bank details. Only gates BARE (unlabelled)
# 10-13 digit numbers — PSA's 7-9 digit certs are never affected, and in a
# message that says "CGC" a CGC-shaped number (see _cgc_shaped) still counts.
# ("number", "order", "pay" and "app" are deliberately NOT here: "cert number
# 6193949247", "in order", "what do you pay" are how sellers write certs. A lone
# bare number is confirmed on CardLadder before any sheet is made —
# cgc_needs_confirm — which is what stops a bare phone / payment number.)
_NOT_A_CERT_WORDS = {
    'tracking', 'track', 'tracker', 'fedex', 'ups', 'usps', 'dhl', 'shipped',
    'shipping', 'label', 'phone', 'cell', 'mobile', 'call', 'text', 'tel',
    'whatsapp', 'zelle', 'venmo', 'paypal', 'cashapp', 'cash', 'apple',
    'payment', 'invoice', 'acct', 'account', 'routing', 'wire', 'ach', 'bank',
}

def _cgc_shaped(digits):
    """The shape of every real CGC card cert seen so far (94 of 94 on Oct 18):
    10 digits starting 6 (60…/61…), or 13 digits starting 14 (1401…)."""
    d = str(digits)
    return (len(d) == 10 and d.startswith('6')) or (len(d) == 13 and d.startswith('14'))

def _strip_links(text):
    # Strip out any URLs FIRST. GIF links (Tenor, Giphy, etc.) end in long numeric
    # IDs (e.g. tenor.com/view/funny-cat-gif-25649837) that otherwise get mistaken
    # for cert numbers. A real cert is always sent as a plain number, never in a link.
    text = re.sub(r'https?://\S+', ' ', text)
    return re.sub(r'www\.\S+', ' ', text)

def _strip_cert_noise(text):
    """Links and Discord timestamps out — what's left is where certs can be."""
    return _DISCORD_TS_RE.sub(' ', _strip_links(text))

def extract_certs(text):
    """Cert tokens in a message, in order, deduped. PSA certs are bare 7-9 digit
    strings, taken exactly as before CGC support (whatever label precedes them).
    CGC certs come back as 'cgc:<digits>' tokens:
      - a 10-13 digit number with "CGC" right in front of it on the same line;
      - a bare CGC-shaped number (6 + 9 digits, or 14 + 11 digits), unless the
        message has tracking / phone / payment words and doesn't say CGC;
      - any other bare 10 or 13 digit number only when the message says CGC
        and has no such words.
    Never a CGC cert: a 10-13 digit number labelled PSA/BGS/SGC/TAG/Beckett, or
    a bare 11-12 digit number (a 12-digit number is a FedEx tracking number and
    still falls through to the tracking handler). A lone bare 10-digit number is
    only a candidate — see cgc_needs_confirm."""
    if not text:
        return []
    text = _strip_links(text)
    scan = _DISCORD_TS_RE.sub(lambda m: ' ' * len(m.group(0)), text)   # same offsets
    found = [(m.group(2), (m.group(1) or '').lower(), m.start(2))
             for m in _CERT_NUMBER_RE.finditer(scan)]
    if not found:
        return []
    # The chatter guard counts words on the link-stripped text exactly as the
    # pre-CGC bot did (timestamps included), so PSA messages extract unchanged.
    stripped = re.sub(r'\d', '', text).strip()
    words = [w for w in stripped.split() if re.search(r'[a-zA-Z]', w)]
    plain = [re.sub(r'[^a-z]', '', w.lower()) for w in words]
    # "CGC" labelling a cert isn't chatter — "CGC 1… CGC 2… CGC 3… CGC 4…" is
    # four certs, not four words.
    if sum(1 for p in plain if p != 'cgc') > 3:
        return []
    not_cert_ctx = any(p in _NOT_A_CERT_WORDS for p in plain)
    says_cgc = bool(re.search(r'cgc', text, re.I))
    seen = set()
    unique = []
    for digits, label, pos in found:
        n = len(digits)
        if n <= 9:
            # PSA — the pre-CGC rule exactly: a standalone 7-9 digit number. A
            # label glued on ("psa12345678") was never a match before; keep it so.
            if pos > 0 and (scan[pos - 1].isalnum() or scan[pos - 1] == '_'):
                continue
            tok = digits
        elif label == 'cgc':
            tok = cgc_cert(digits)
        elif label:
            continue          # another grader's serial (BGS 0012345678, …)
        elif n in (11, 12):
            continue          # never a bare CGC cert (12 = FedEx tracking)
        elif _cgc_shaped(digits) and (says_cgc or not not_cert_ctx):
            tok = cgc_cert(digits)
        elif says_cgc and not not_cert_ctx:
            tok = cgc_cert(digits)
        else:
            continue
        if tok not in seen:
            seen.add(tok)
            unique.append(tok)
    return unique

def cgc_needs_confirm(text, certs):
    """True when a message's ONLY cert is one bare 10-digit number and nothing
    in it says CGC — as likely a phone number ("6195551234" answering "what's
    your Zelle?") as a slab. on_message looks it up on CardLadder first and
    makes a sheet / quote only when CardLadder knows it as a CGC slab."""
    if len(certs or []) != 1 or not is_cgc_cert(certs[0]):
        return False
    if len(cert_digits(certs[0])) != 10:
        return False
    return not re.search(r'cgc', _strip_cert_noise(text or ''), re.I)

# A 13-digit number shaped like a CGC cert (14…) is never a tracking number (no
# carrier uses 13-digit numeric tracking) — the tracking handler skips it.
def _looks_like_cgc_13(num):
    return len(num) == 13 and num.startswith('14')


# ── PSA OFFER (shared by the live path and the background comp retry) ─────────────
# channel_id -> {"task": asyncio.Task, "certs": [...], "sheet_id", "sheet_url",
#                "username", "spawned_at": float}. One retry per ticket; a newer
# submission supersedes it (merging outstanding certs) via start_comp_retry, and
# a covering live quote cancels it via cancel_comp_retry. Persisted (minus the
# task handle) to PENDING_RETRY_FILE so redeploys don't orphan promised quotes.
pending_comp_retries = {}

def start_comp_retry(channel, channel_id, username, certs, sheet_id, sheet_url,
                     max_attempts=15, spawned_at=None):
    """Spawn (or supersede) the background comp retry for a ticket. If a retry is
    already pending, its task is cancelled and any of its certs missing from the
    new submission are merged in, so a second helper-down submission never gets
    silently dropped. The newest sheet is used for filling/linking."""
    old = pending_comp_retries.get(channel_id)
    merged = list(certs)
    if old:
        merged = [c for c in old.get("certs", []) if c not in set(certs)] + merged
        t = old.get("task")
        if t is not None:
            t.cancel()
    entry = {
        "certs": merged, "sheet_id": sheet_id, "sheet_url": sheet_url,
        "username": username,
        "spawned_at": spawned_at if spawned_at is not None else datetime.now().timestamp(),
    }
    entry["task"] = asyncio.create_task(retry_psa_offer_when_helper_back(
        channel, channel_id, username, merged, sheet_id, sheet_url,
        max_attempts=max_attempts))
    pending_comp_retries[channel_id] = entry
    _save_pending_retries()

def cancel_comp_retry(channel_id, reason=""):
    """Cancel and forget the pending comp retry for a ticket (if any).
    Returns True if one existed."""
    entry = pending_comp_retries.pop(channel_id, None)
    if entry is None:
        return False
    t = entry.get("task")
    if t is not None:
        t.cancel()
    _save_pending_retries()
    if reason:
        print(f"comp retry cancelled for {channel_id}: {reason}")
    return True

async def price_and_send_psa_offer(channel, channel_id, username, certs, comps,
                                   sheet_id, sheet_url, delayed=False):
    """Apply avg-3 value adjustments, fill the buying sheet, classify every cert,
    and post the offer (customer message + Kevin ping). Shared by the live path in
    on_message and retry_psa_offer_when_helper_back (delayed=True swaps in a
    thanks-for-your-patience opener)."""
    _loaded_head = ("✅ Comps are in — thanks for your patience! Here's your quote:"
                    if delayed else "✅ All comps loaded!")
    # ONE snapshot of the VIP state for this whole quote: the sheet stamp and the
    # quoted rates must never disagree (a !vip add/remove mid-quote would other-
    # wise desynchronize them across the awaits below). BOTH the pokemon tiers
    # and the per-sport rates snapshot here — no later re-reads.
    vip_tiers = vip_pokemon_tiers(username)
    vip_srates = vip_sport_rates(username)
    # Value adjustments BEFORE filling the sheet or classifying, so the sheet's
    # "Our comp" and all pricing use the adjusted value.
    # MLB uses the DIRECT CardLadder value (the min(avg-3-sales, CL) discount was
    # removed Jul 26 per Kevin — it was underpricing MLB slabs).
    for _c in comps:
        _sp = normalize_sport(_c.get('sport'))
        if _sp == 'basketball' and not _comp_is_cgc(_c):   # CGC: straight CL value
            # Over-ceiling NBA cards get the avg-3 discount so a
            # stale-high CL value doesn't auto-reject a card whose
            # actual recent sales sit inside the buy band.
            # (Was hardcoded 250 — left a $200-250 gap where cards
            # were rejected on raw CL value while identical cards
            # above $250 got discounted and accepted.)
            # .get with an inf default: basketball may be absent from
            # PSA_SPORT_MAX_PRICE while sports are off (Aug 12) — a stray
            # basketball-classified cert in a lot must not crash the quote
            # (classify rejects it right after anyway).
            apply_avg3_value(_c, threshold=PSA_SPORT_MAX_PRICE.get('basketball', float('inf')))
    if sheet_id and comps:
        _not_on_sheet = []
        try:
            _not_on_sheet = await asyncio.to_thread(fill_buying_sheet, sheet_id, comps) or []
        except Exception as e:
            print(f"Sheet fill error (offer continues): {e}")
        # A quoted CGC cert with no row in column B (the Apps Script that builds
        # the sheet dropped / rewrote a 10-13 digit number) would be quoted here
        # but never paid off the sheet, rebuilt on 'ship', or put on the buyer
        # list — say so loudly instead of skipping it silently.
        _cgc_off = [c for c in _not_on_sheet if is_cgc_cert(c)]
        if _cgc_off:
            await ping_kevin(
                f"⚠️ **{len(_cgc_off)} quoted CGC cert(s) aren't on {username}'s buying "
                f"sheet** — the quote includes them but the sheet has no row for "
                f"them, so it won't pay them. Add the rows by hand: "
                + ", ".join(f"`{cert_label(c)}`" for c in _cgc_off) + f"\n{sheet_url}",
                channel)
    # VIP customers get their negotiated rates baked into THEIR sheet's
    # payout formulas (the template copy carries standard rates). A silent failure
    # here would make the sheet compute LESS than the quoted number and Kevin pays
    # off the sheet — so retry once, then alert him loudly.
    if sheet_id and (vip_tiers or vip_srates):
        for _attempt in (1, 2):
            try:
                await asyncio.to_thread(write_custom_sheet_formulas, sheet_id,
                                        vip_tiers, vip_srates)
                break
            except Exception as e:
                if _attempt == 1:
                    print(f"VIP formula write failed (retrying in 20s): {e}")
                    await asyncio.sleep(20)
                else:
                    print(f"VIP formula write FAILED twice: {e}")
                    await ping_kevin(
                        f"⚠️ **VIP formula stamp FAILED for {username}** — their sheet's "
                        f"H column still computes STANDARD rates but the quote below uses "
                        f"their VIP rates. Fix column H before paying!\n{sheet_url}",
                        channel)
    by_cert = {str(c.get('cert', '')).strip(): c for c in comps}
    accepted = []
    rejected = []
    flagged = []   # accepted cards that need Kevin's manual review
    review = []    # 'review' status — priced by hand, not auto-quoted
    review_certs = []
    unloaded = []  # helper timeout / missing — NOT a reject; resend to re-check
    kevin_lines = []
    quota_filled_notes = []   # Pokémon slot bands that filled during THIS quote
    for cert in certs:
        c = by_cert.get(str(cert).strip())
        status, reason = classify_psa_comp(c)
        # Pokémon slot quotas: count each accepted card IMMEDIATELY so later
        # certs in this same lot see the updated count (a 40-card $150 lot
        # must stop at the 25th slot, not accept all 40). classify rejects
        # once a band's slots hit 0 left. CGC cards are a separate buyer lane
        # and never count against (or get blocked by) the PSA Pokémon bands.
        if (status in ('accepted', 'flag') and c and not _comp_is_cgc(c)
                and normalize_sport(c.get('sport')) == 'pokemon'):
            try:
                _qb = pokemon_band_for(float(c['clValue']))
                if _qb and _qb[2] is not None:
                    _qn = pokemon_quota_take(_qb[2])
                    _qcap = pokemon_quota_cap(_qb[2], _qb[3])
                    if _qcap is not None and _qn == _qcap:
                        quota_filled_notes.append(
                            f"${_qb[0]:g}-${_qb[1]:g} Pokémon slots now FULL "
                            f"({_qcap}/{_qcap}) — announce it!")
            except Exception as _qe:
                print(f"quota count error (non-critical): {_qe}")
        # Kevin's lines show 'CGC 6193949247' / '(CGC 10)' for CGC slabs; PSA
        # lines read exactly as before ('12345678', '(PSA 10)').
        _lbl = cert_label(cert)
        if status == 'review':
            # Manual-review cards: keep out of the auto-priced lot (0%),
            # highlight orange on the sheet for Kevin to quote by hand.
            review.append((cert, reason))
            review_certs.append(cert)
            cv = float(c['clValue'])
            name = (c.get('cardName') or '')[:50]
            kevin_lines.append(f"• `{_lbl}` — 🟠 **${cv:.2f}** — {name} ({_slab_grade_label(c)}) — {reason}")
        elif status == 'accepted':
            accepted.append(c)
            cv = float(c['clValue'])
            name = (c.get('cardName') or '')[:50]
            _cgc_tag = f" — CGC {_fmt_pct(CGC_POKEMON_RATE)}" if _comp_is_cgc(c) else ""
            kevin_lines.append(f"• `{_lbl}` — **${cv:.2f}** — {name} ({_slab_grade_label(c)}){_cgc_tag}")
        elif status == 'flag':
            # Still accept the card so the lot is priced, but warn Kevin.
            accepted.append(c)
            flagged.append((cert, c, reason))
            cv = float(c['clValue'])
            name = (c.get('cardName') or '')[:50]
            kevin_lines.append(f"• `{_lbl}` — ⚠️ **${cv:.2f}** — {name} ({_slab_grade_label(c)}) — {reason}")
        elif _comp_is_infra_error(c):
            unloaded.append((cert, reason))
            kevin_lines.append(f"• `{_lbl}` — ⚠️ comp didn't load (helper timeout) — re-check")
        else:
            rejected.append((cert, reason))
            kevin_lines.append(f"• `{_lbl}` — ❌ {reason}")

    # Shade manual-review cards orange so Kevin knows to price them by hand.
    if review_certs and sheet_id:
        try:
            await asyncio.to_thread(highlight_certs_review, sheet_id, review_certs)
        except Exception as e:
            print(f"Review-highlight error: {e}")

    sport_groups = {}
    for c in accepted:
        # CGC slabs are their own group ('cgc' → "CGC Pokemon" at a flat
        # CGC_POKEMON_RATE) — never blended into the PSA Pokémon bands or VIP.
        sp = 'cgc' if _comp_is_cgc(c) else normalize_sport(c.get('sport'))
        sport_groups.setdefault(sp, []).append(c)
    sport_breakdown = []
    for sp, comps_in_sport in sport_groups.items():
        card_values = [float(c['clValue']) for c in comps_in_sport]
        sport_total = sum(card_values)
        if sp == 'cgc':
            rate = CGC_POKEMON_RATE
            is_vip = False
        elif sp == 'pokemon':
            # Per-card: band rate (standard or VIP) vs Pikachu lane — better wins.
            _payout = sum(float(c['clValue']) * _pokemon_card_rate(c, vip_tiers)
                          for c in comps_in_sport)
            rate = (_payout / sport_total) if sport_total else PSA_POKEMON_PER_CARD_TIERS[0][2]
            is_vip = bool(vip_tiers)
        elif sp in vip_srates:
            # Flat VIP rate for this sport (grade/ceiling/age rules stay standard).
            rate = vip_srates[sp]
            is_vip = True
        else:
            rate = get_psa_payout_rate(sp, sport_total, card_values)
            is_vip = False
        sport_breakdown.append({
            'sport': sp,
            'count': len(comps_in_sport),
            'total': sport_total,
            'rate': rate,
            'payout': sport_total * rate,
            'vip': is_vip,
        })
    total_comp = sum(s['total'] for s in sport_breakdown)
    total_payout = sum(s['payout'] for s in sport_breakdown)
    n_accepted = len(accepted)
    n_rejected = len(rejected)

    if n_accepted:
        last_offer[channel_id] = {
            "payout": total_payout,
            "total": total_comp,
            "rate": (total_payout / total_comp) if total_comp else PSA_POKEMON_PER_CARD_TIERS[0][2],
        }

    # Record accepted slab comp values toward the $3k combined-lot minimum.
    slab_value_map = {
        str(c.get('cert', '')).strip(): float(c['clValue'])
        for c in accepted if c.get('clValue') is not None
    }
    # CGC slabs ride the Pokémon (VA) lane — only CGC Pokémon is ever accepted.
    slab_sport_map = {
        str(c.get('cert', '')).strip(): ('pokemon' if _comp_is_cgc(c)
                                         else normalize_sport(c.get('sport')))
        for c in accepted if c.get('clValue') is not None
    }
    add_slab_values(channel_id, slab_value_map, slab_sport_map)

    breakdown_lines = [
        f"• {_sport_label(s['sport'])}: {s['count']} card{'s' if s['count'] != 1 else ''}, "
        f"${s['total']:,.2f} → ${s['payout']:,.2f} ({_fmt_pct(s['rate'])})"
        + (" ✨ VIP" if s.get('vip') else "")
        for s in sport_breakdown
    ]

    # Kevin DM
    n_flagged = len(flagged)
    n_review = len(review)
    flag_warning = ""
    if n_flagged:
        flag_warning = (
            f"\n⚠️ **{n_flagged} card{'s' if n_flagged != 1 else ''} flagged for manual review** — check each reason before paying!\n"
            + "\n".join([f"   - `{cert_label(cert)}`: {reason}" for cert, _, reason in flagged])
            + "\n"
        )
    review_warning = ""
    if n_review:
        review_warning = (
            f"\n🟠 **{n_review} card{'s' if n_review != 1 else ''} to price by hand** (highlighted orange on the sheet, not in the auto-quote):\n"
            + "\n".join([f"   - `{cert_label(cert)}`: {reason}" for cert, reason in review])
            + "\n"
        )
    n_unloaded = len(unloaded)
    unloaded_warning = ""
    if n_unloaded:
        unloaded_warning = (
            f"\n⚠️ **{n_unloaded} comp{'s' if n_unloaded != 1 else ''} didn't load (helper timeout)** — "
            f"NOT rejected — the customer was asked to re-send it in the ticket "
            f"(if you type the cert yourself, the new sheet/rates use YOUR username):\n"
            + "\n".join([f"   - `{cert_label(cert)}`" for cert, _ in unloaded])
            + "\n"
        )
    n_cgc = sum(1 for _ct in certs if is_cgc_cert(_ct))
    summary = (
        f"📋 **PSA sheet — {username}**\n"
        f"{len(certs)} certs{f' ({n_cgc} CGC)' if n_cgc else ''} | Accepted **{n_accepted}** | Comp **${total_comp:,.2f}** | Payout **${total_payout:,.2f}**"
        f"{f' | {n_rejected} rejected' if n_rejected else ''}"
        f"{f' | {n_unloaded} ⚠️ not loaded' if n_unloaded else ''}"
        f"{f' | {n_flagged} ⚠️ flagged' if n_flagged else ''}"
        f"{f' | {n_review} 🟠 manual' if n_review else ''}\n"
        f"{sheet_url}\n"
        + ("\n".join(breakdown_lines) + "\n\n" if breakdown_lines else "\n")
        + unloaded_warning
        + flag_warning
        + review_warning
    )
    if n_accepted and not lot_qualifies(channel_id):
        _sc, _sv, _si, _cb = lot_summary(channel_id)
        summary = f"⏳ **[BELOW MINIMUM — HOLD]** {_sc} slab(s), combined ${_cb:,.2f}\n" + summary
    if quota_filled_notes:
        summary = "🧮 **" + " · ".join(quota_filled_notes) + "**\n" + summary
    await ping_kevin(summary + "\n".join(kevin_lines), channel)

    # Customer follow-up
    if n_unloaded and not delayed:
        _loaded_head = "✅ Comps loaded!"   # not "All" — some need a re-check
    customer_parts = [_loaded_head, ""]
    if n_accepted > 0:
        customer_parts += [
            f"**Total comp:** ${total_comp:,.2f}",
            f"**Total payout:** ${total_payout:,.2f}",
            f"**Number of cards:** {n_accepted}",
            "",
            "**Breakdown:**",
            *breakdown_lines,
        ]
    if rejected:
        customer_parts.append("")
        if n_accepted > 0:
            customer_parts.append(
                f"⚠️ {n_rejected} card{'s' if n_rejected != 1 else ''} don't fit our current buying criteria:"
            )
        elif len(certs) - n_unloaded == 1:
            customer_parts.append(
                "⚠️ Unfortunately, this card doesn't fit our current buying criteria:"
            )
        else:
            customer_parts.append(
                f"⚠️ Unfortunately, none of these {len(certs) - n_unloaded} cards fit our current buying criteria:"
            )
        customer_parts += [f"• `{cert_label(cert)}` — {reason}" for cert, reason in rejected]
    if n_unloaded:
        # Kept apart from the rejects: these were never judged, just not loaded.
        customer_parts += [
            "",
            f"🔄 {n_unloaded} card{'s' if n_unloaded != 1 else ''} need{'' if n_unloaded != 1 else 's'} a re-check:",
            *[f"• `{cert_label(cert)}` — {reason}" for cert, reason in unloaded],
        ]
    if n_review:
        customer_parts += [
            "",
            f"💎 {n_review} card{'s' if n_review != 1 else ''} we'll quote by hand and get right back to you:",
            *[f"• `{cert_label(cert)}`" for cert, _ in review],
        ]
    if n_accepted > 0:
        hold_tail = proceed_or_hold_tail(channel_id)
        if hold_tail:
            customer_parts += ["", hold_tail]
        else:
            if unloaded:
                _proceed = (f"Resend the re-check card{'s' if n_unloaded != 1 else ''} first "
                            f"if you'd like {'them' if n_unloaded != 1 else 'it'} included — otherwise "
                            f"let me know if you want to proceed with the {n_accepted} we can take.")
            elif rejected:
                _proceed = f"Let me know if you want to proceed with the {n_accepted} we can take."
            else:
                _proceed = "Let me know if you'd like to proceed!"
            customer_parts += ["", _proceed]
    customer_parts += [
        "",
        "If you want to see where I'm at on each individual card, click the sheet link above and request access.",
    ]
    customer_msg = "\n".join(customer_parts)
    for chunk in _split_for_discord(customer_msg):
        await channel.send(chunk)
    # Big-ticket review cards: TAG Kevin in the ticket itself (Kevin, Sep 26 —
    # "make sure it tags me") on top of the DM, so he market-checks and makes
    # his own offer. No comp value here — the customer can read this channel.
    if n_review:
        try:
            await channel.send(
                f"<@{YOUR_DISCORD_USER_ID}> — {n_review} big-ticket "
                f"card{'s' if n_review != 1 else ''} above need your manual offer."
            )
        except Exception as e:
            print(f"Review tag failed (DM already sent): {e}")
    # Ticket has a customer summary sheet (!summary)? Refresh it in the background
    # with this quote's sheet. Fire-and-forget: never delays or breaks the quote.
    schedule_summary_refresh(channel, channel_id)

async def retry_psa_offer_when_helper_back(channel, channel_id, username, certs,
                                           sheet_id, sheet_url,
                                           max_attempts=15, interval_s=180):
    """The comp helper was down (or every CardLadder page timed out) when these
    certs arrived. Keep retrying in the background (every 3 min, 15 tries — the VM
    watchdog usually revives the helper within one cycle); when the comps load,
    post the quote automatically so the customer never has to re-send anything.
    Managed exclusively through start_comp_retry / cancel_comp_retry; the identity
    checks below make a superseded or cancelled task a no-op even mid-lookup.

    Lock-friendly (Oct 18): real answers (good comps AND genuine not-founds) are
    kept across attempts, so each attempt only looks up certs that still haven't
    loaded; a probe chunk goes first and the rest is skipped while it still times
    out; and lookup_comps runs without its inline all-bad re-check (this loop IS
    the retry). One cert that never loads doesn't hold the whole lot hostage:
    from the 2nd attempt a lot missing only a few certs is quoted (the stragglers
    show as "need a re-check"), and at give-up whatever loaded is quoted."""
    def _mine():
        e = pending_comp_retries.get(channel_id)
        return e is not None and e.get("task") is asyncio.current_task()
    cert_keys = list(dict.fromkeys(str(c).strip() for c in certs))
    answers = {}   # cert -> latest helper entry (real answers are final)
    t0 = time.monotonic()

    def _merge(got):
        for c in got or []:
            k = str(c.get('cert', '')).strip()
            if k in cert_keys and _comp_is_infra_error(answers.get(k)):
                answers[k] = c

    def _uncovered():
        return _uncovered_certs(cert_keys, list(answers.values()))

    async def _quote():
        comps = [answers[k] for k in cert_keys if k in answers]
        try:
            await price_and_send_psa_offer(channel, channel_id, username, certs,
                                           comps, sheet_id, sheet_url, delayed=True)
        except (discord.NotFound, discord.Forbidden):
            print(f"Comp retry: ticket for {username} closed before comps returned — dropping")
        except Exception as e:
            print(f"Delayed quote failed — {username}: {e}")
            await ping_kevin(
                f"⚠️ Auto-quote after helper recovery FAILED for **{username}** "
                f"({len(certs)} certs) — the customer may not have seen a quote. "
                f"Quote manually: {sheet_url}\nError: {e}",
                channel
            )

    def _cert_list(cs):
        return ", ".join(f"`{cert_label(c)}`" for c in cs[:20]) + (" …" if len(cs) > 20 else "")

    try:
        for attempt in range(1, max_attempts + 1):
            await asyncio.sleep(interval_s)
            if not _mine():
                return   # superseded or cancelled
            todo = _uncovered()
            probe, rest = todo[:HELPER_CHUNK_SIZE], todo[HELPER_CHUNK_SIZE:]
            try:
                got = await asyncio.to_thread(lookup_comps, probe, recheck_all_bad=False)
            except Exception:
                continue
            _merge(got)
            # Probe still all timeouts / missing → CardLadder is still broken;
            # don't scrape the rest of the lot under the global lock this round.
            if rest and len(_uncovered_certs(probe, got or [])) < len(probe):
                try:
                    _merge(await asyncio.to_thread(lookup_comps, rest, recheck_all_bad=False))
                except Exception as e:
                    print(f"Comp retry: rest-of-lot lookup failed ({e}) — next attempt")
            # Genuine no-comp answers come back as found=False ENTRIES; a missing
            # cert, or a found=False whose note is a helper error ("lookup error:
            # page.goto: Timeout…", Oct 18), is infrastructure, not a missing card.
            uncovered = _uncovered()
            n_loaded = len(cert_keys) - len(uncovered)
            if uncovered:
                few = len(uncovered) <= max(2, len(cert_keys) // 10)
                if not (n_loaded and attempt >= 2 and few):
                    print(f"Comp retry {attempt}/{max_attempts}: {n_loaded}/{len(cert_keys)} "
                          f"certs loaded — waiting")
                    continue
                print(f"Comp retry {attempt}/{max_attempts}: {n_loaded}/{len(cert_keys)} loaded — "
                      f"quoting now, {len(uncovered)} shown as re-check")
            if not _mine():
                return   # cancelled while the lookup was in flight
            await _quote()
            return
        if not _mine():
            return
        uncovered = _uncovered()
        n_loaded = len(cert_keys) - len(uncovered)
        elapsed = int((time.monotonic() - t0) // 60)
        print(f"Comp retry gave up after {max_attempts} attempts (~{elapsed} min) — "
              f"{username}, {n_loaded}/{len(cert_keys)} loaded")
        if n_loaded:
            # Close the "I'll post your quote" promise with what DID load; the
            # stragglers show in the quote as "need a re-check".
            await _quote()
            await ping_kevin(
                f"🚨 Comp auto-retry gave up after ~{elapsed} min — posted **{username}**'s "
                f"quote for the {n_loaded}/{len(cert_keys)} that loaded. Still not loading "
                f"(customer asked to re-send): {_cert_list(uncovered)}\n{sheet_url}",
                channel
            )
        else:
            await ping_kevin(
                f"🚨 Comp auto-retry gave up after ~{elapsed} min ({max_attempts} tries) — "
                f"**{username}**'s {len(cert_keys)} certs never got comps. Quote manually: {sheet_url}"
                + (f"\n(Still not loading: {_cert_list(uncovered)})" if uncovered else ""),
                channel
            )
    finally:
        # Only clean up our own entry — a superseding task has already replaced it.
        e = pending_comp_retries.get(channel_id)
        if e is not None and e.get("task") is asyncio.current_task():
            pending_comp_retries.pop(channel_id, None)
            _save_pending_retries()

async def comps_pending_auto_retry(channel, channel_id, username, certs, sheet_id,
                                   sheet_url, err_note=""):
    """No usable comps for this submission (helper down, or every CardLadder page
    timed out) — tell the customer, gate "proceed" (_lot_entry), ping Kevin, and
    keep retrying in the background: when the helper comes back (the VM watchdog
    usually revives it within ~3 min) the quote posts itself."""
    await channel.send(
        "⚠️ Comps are taking a little longer than usual — I'll "
        "keep trying and post your quote here as soon as they're in!"
    )
    _lot_entry(channel_id)
    cert_list = "\n".join([f"• `{cert_label(c)}`" for c in certs])
    await ping_kevin(
        f"📋 **PSA sheet — {username}** (comps pending, auto-retrying — "
        f"type `!cancelretry` in the ticket to take over manually)\n"
        f"{len(certs)} certs | {sheet_url}{err_note}\n\n"
        f"{cert_list}",
        channel
    )
    # Always (re)spawn: a second helper-down submission supersedes the old
    # retry and MERGES its outstanding certs, so no batch is silently dropped.
    start_comp_retry(channel, channel_id, username, certs, sheet_id, sheet_url)


async def quote_or_pend_live(channel, channel_id, username, certs, comps, comp_error,
                             sheet_id, sheet_url):
    """Live cert path, after the lookup: post the quote, or — when the helper is
    down or answered but EVERY cert is a timeout / missing entry (Oct 18) — hand
    off to the background retry instead of a quote that rejects them all."""
    all_timeouts = bool(comps) and len(_uncovered_certs(certs, comps)) == len(certs)
    if comps and not all_timeouts:
        # Re-check for a pending retry NOW, not just at submission: one spawned
        # by a concurrent submission while our lookup was queued on the helper
        # lock would otherwise post a second quote for these same certs. A retry
        # for a DIFFERENT batch (not covered by ours) keeps running.
        _pending = pending_comp_retries.get(channel_id)
        if _pending and set(_pending.get("certs", [])) <= set(certs):
            cancel_comp_retry(channel_id, "covered by a live quote")
        await price_and_send_psa_offer(channel, channel_id, username, certs, comps,
                                       sheet_id, sheet_url)
        return
    if all_timeouts:
        err_note = "\n\n⚠️ Helper is up but every CardLadder page timed out"
    else:
        err_note = f"\n\n⚠️ Helper offline ({comp_error})" if comp_error else ""
    await comps_pending_auto_retry(channel, channel_id, username, certs, sheet_id,
                                   sheet_url, err_note)


# ── DISCORD BOT ──────────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members = True  # needed to read server members + assign tier roles (enable "Server Members Intent" in the Dev Portal)
bot = discord.Client(intents=intents)

welcomed_tickets = set()
last_offer = {}
channel_sheet = {}

# ── BUYING MINIMUMS ───────────────────────────────────────────────────────────
# Qualifying paths for a lot (Jul 31 2026):
#   1. Lot CONTAINS PSA slabs   -> need >=15 accepted slabs AND >=$3,000 combined
#      value (slab CardLadder comp + any raw singles market value).
#   2. One Piece singles alone  >=$3,000  -> lot qualifies.
#   3. Pokémon singles alone    >=$1,500  -> lot qualifies (POKEMON_SINGLES_MIN_LOT).
# Hitting EITHER singles threshold qualifies the whole lot (slabs then taken
# regardless of count, matching the long-standing One Piece behavior).
# Slabs and singles can arrive in either order / separate messages, so we keep a
# running per-ticket total and only let a seller proceed/ship once it qualifies.
MIN_LOT_VALUE = 0      # Kevin, Oct 3: "no minimum lots this week"
MIN_SLAB_COUNT = 0     # (standing values to restore after: $1,000 / 10)

# Per-ticket running lot.
#   channel_id -> {"singles": {game: market_value}, "slab_certs": {cert: comp_value}}
#   singles    : per-game market value of the latest valid Collectr CSV
#                (whole dict replaced on re-upload — latest CSV is the truth)
#   slab_certs : accepted PSA certs -> comp value, accumulated & deduped by cert
lot_state = {}

def _lot_entry(channel_id):
    return lot_state.setdefault(channel_id, {"singles": {}, "slab_certs": {}, "slab_sport": {}})

def set_singles_value(channel_id, totals_by_game):
    """Latest Collectr CSV is the current truth for singles — replace, don't add.
    Replaces the ENTIRE per-game dict: a re-export with no Pokémon rows zeroes
    the Pokémon portion too."""
    _lot_entry(channel_id)["singles"] = {
        g: float(v or 0) for g, v in (totals_by_game or {}).items() if float(v or 0) > 0
    }

def singles_by_game(channel_id):
    entry = lot_state.get(channel_id) or {}
    singles = entry.get("singles") or {}
    return {
        "one piece": float(singles.get("one piece", 0)),
        "pokemon": float(singles.get("pokemon", 0)),
    }

def add_slab_values(channel_id, cert_value_map, cert_sport_map=None):
    """Accumulate accepted slab certs (deduped by cert so re-sends don't double-count).
    cert_sport_map (cert -> normalized sport) feeds the per-lane split that routes
    Pokémon → VA and Sports/One Piece → Doral and enforces the sports/OP minimum."""
    entry = _lot_entry(channel_id)
    entry.setdefault("slab_sport", {})
    for cert, val in cert_value_map.items():
        entry["slab_certs"][str(cert)] = float(val or 0)
    for cert, sp in (cert_sport_map or {}).items():
        entry["slab_sport"][str(cert)] = sp

def rebuild_lot_from_sheets(channel_id, username, days=4, created_after=None, extra_sheet_id=None):
    """A restart/redeploy wipes lot_state, and compose_shipping would then fail open
    to the VA (Pokémon) address — wrong for Sports/One Piece. Rebuild this ticket's
    lot from the customer's buying sheets instead: every row with a payout > 0
    (so Kevin's hand edits — e.g. zeroing what Triumph won't take — count), with
    its sport. Later sheets win per cert. Returns True if anything was rebuilt.
    created_after (tz-aware datetime — the ticket channel's created_at): ignore
    sheets from before this ticket opened. A repeat seller's PREVIOUS deal (often
    already shipped) is still inside the `days` window and must not be merged in —
    same guard as _drive_find_sheet. A few minutes of slack absorbs clock skew.
    extra_sheet_id: this ticket's cached channel_sheet (or a list: the cache plus
    every sheet Kevin's own cert pastes created in this ticket — see
    record_ticket_sheet). Counted even when it isn't named after the customer —
    certs KEVIN pasted in the ticket land on a sheet named after him, and
    they're still part of this lot."""
    from datetime import timedelta as _td
    since_dt = datetime.utcnow() - _td(days=days)
    if created_after is not None:
        ca = (created_after.astimezone(timezone.utc).replace(tzinfo=None)
              if created_after.tzinfo else created_after)
        since_dt = max(since_dt, ca - _td(minutes=10))
    since = since_dt.strftime('%Y-%m-%dT%H:%M:%S')
    safe = username.replace("'", "\\'")
    drive = get_drive_service()
    files = drive.files().list(
        q=(f"name contains '{safe} discord' and '{PSA_FOLDER_ID}' in parents "
           f"and trashed=false and createdTime > '{since}'"),
        orderBy='createdTime asc', pageSize=50,
        fields='files(id,name,createdTime)').execute().get('files', [])
    files = [f for f in files if f['name'].split(' discord')[0].strip() == username]
    extra_ids = ([extra_sheet_id] if isinstance(extra_sheet_id, str)
                 else list(extra_sheet_id or []))
    for xid in dict.fromkeys(x for x in extra_ids if x):
        if any(f['id'] == xid for f in files):
            continue
        try:
            meta = drive.files().get(fileId=xid,
                                     fields='id,name,createdTime,trashed').execute()
            ct = meta.get('createdTime') or ''
            # No timestamp: still this ticket's sheet (it's the cache) — keep it, last.
            if not meta.get('trashed') and (not ct or ct[:19] > since):
                files.append({'id': xid, 'name': meta.get('name', ''),
                              'createdTime': ct or '~'})
                files.sort(key=lambda f: f.get('createdTime') or '')   # later sheets win
        except Exception as e:
            print(f"lot rebuild: cached sheet {xid} unreadable ({e}) — skipped")
    vals, sports = {}, {}
    gc = get_gspread_client()
    for f in files:
        rows = gc.open_by_key(f['id']).get_worksheet(0).get_all_values(
            value_render_option='UNFORMATTED_VALUE')
        for r in rows[1:]:
            r = list(r) + [''] * 10
            cert = str(r[1]).strip()
            if cert.endswith('.0'):
                cert = cert[:-2]
            if not cert.isdigit():
                continue
            # CGC rows (A="CGC" / grade "CGC 10" / 10+ digit cert) key as the
            # same 'cgc:<digits>' token the live quote used.
            cert = _sheet_cert_token(r[0], cert, r[4])
            try:
                cv, pay = float(r[6] or 0), float(r[8] or 0)
            except (TypeError, ValueError):
                continue
            if cv > 0 and pay > 0:
                sp = normalize_sport(str(r[5]))
                if is_cgc_cert(cert) and not sp:
                    sp = 'pokemon'   # CGC = Pokémon-only lane (VA)
                vals[cert], sports[cert] = cv, ('basketball' if sp == 'nba' else sp)
            else:
                vals.pop(cert, None); sports.pop(cert, None)
    if not vals:
        return False
    entry = _lot_entry(channel_id)
    entry["slab_certs"].update(vals)
    entry.setdefault("slab_sport", {}).update(sports)
    entry["rebuilt"] = True
    return True

def lot_value_by_lane(channel_id):
    """(pokemon_slab_value, sportsop_slab_value) for the running lot.
    Unknown/missing sport falls into the Pokémon (VA, no-minimum) lane — the
    least-disruptive default that never misroutes to Doral or wrongly holds."""
    entry = lot_state.get(channel_id) or {}
    certs = entry.get("slab_certs", {}) or {}
    sports = entry.get("slab_sport", {}) or {}
    pk = so = 0.0
    for cert, val in certs.items():
        if sports.get(str(cert)) in SPORTSOP_SPORTS:
            so += float(val or 0)
        else:
            pk += float(val or 0)
    return pk, so

def lot_summary(channel_id):
    """Return (slab_count, slab_value, singles_value, combined_value).
    singles_value is the sum across games — use singles_by_game for the split."""
    entry = lot_state.get(channel_id) or {"singles": {}, "slab_certs": {}}
    slab_value = sum(entry["slab_certs"].values())
    slab_count = len(entry["slab_certs"])
    singles = sum((entry.get("singles") or {}).values())
    return slab_count, slab_value, singles, slab_value + singles

def lot_qualifies(channel_id):
    """True if the running lot meets the buying minimums for its path."""
    slab_count, slab_value, singles, combined = lot_summary(channel_id)
    games = singles_by_game(channel_id)
    # An EMPTY lot never qualifies, even with minimums at 0 — the CSV-rejected
    # and comps-pending paths create empty entries precisely so a premature
    # "proceed"/"ship" hits the hold message instead of the shipping address.
    # (With MIN_LOT_VALUE=0 the one-piece shortcut below would otherwise
    # return True on 0 >= 0.)
    if slab_count == 0 and singles == 0:
        return False
    # A singles portion hitting its own threshold qualifies the whole lot —
    # we'll take any slabs included regardless of slab count.
    # One Piece $3,000+, Pokémon $1,500+ (Jul 31 2026).
    if games["one piece"] >= MIN_LOT_VALUE:
        return True
    if games["pokemon"] >= POKEMON_SINGLES_MIN_LOT:
        return True
    # Otherwise, any lot containing slabs needs 15+ slabs AND $3,000+ combined.
    if slab_count > 0:
        return combined >= MIN_LOT_VALUE and slab_count >= MIN_SLAB_COUNT
    # Singles-only and below every singles threshold.
    return False

def proceed_or_hold_tail(channel_id):
    """
    Return None if the lot qualifies (caller shows its normal 'proceed' prompt).
    Otherwise return a customer-facing message listing exactly what's missing,
    to use INSTEAD of the proceed prompt.
    """
    # Sports/One Piece lane: needs $1,500+ (ships to the separate FL address).
    # Pokémon has no minimum, so a mixed lot can still proceed with its Pokémon.
    pk_lane, so_lane = lot_value_by_lane(channel_id)
    if 0 < so_lane < SPORTS_OP_MIN_LOT:
        short = SPORTS_OP_MIN_LOT - so_lane
        if pk_lane > 0:
            return (f"Let me know if you'd like to proceed with your **Pokémon**! "
                    f"⏳ Your **Sports/One Piece** (**${so_lane:,.2f}**) is held — that lane needs "
                    f"**${SPORTS_OP_MIN_LOT:,}+** (it ships to a separate address). "
                    f"You're **${short:,.2f}** short, so add more to include it.")
        return (f"📊 Your **Sports & One Piece** slabs total **${so_lane:,.2f}** — the minimum for "
                f"that lane is **${SPORTS_OP_MIN_LOT:,}** (it ships to a separate address, so we batch it). "
                f"You're **${short:,.2f}** short. Add more and resubmit, or send **Pokémon** (no minimum)!")
    if lot_qualifies(channel_id):
        return None
    slab_count, slab_value, singles, combined = lot_summary(channel_id)
    games = singles_by_game(channel_id)
    op, pk = games["one piece"], games["pokemon"]
    # Empty entry (created on the CSV-rejected / helper-down paths just to gate
    # "proceed"): there's no priced lot to itemize, so don't tell a slab customer
    # they're "$3,000 of singles short" — their quote simply isn't done.
    if slab_count == 0 and singles == 0:
        return ("⏳ **Your quote isn't finalized yet** — Kevin is reviewing it and "
                "will confirm shortly. Hang tight!")
    lines = ["📊 **Heads up — this lot doesn't meet our buying minimums yet.**", ""]
    parts = []
    if slab_count:
        parts.append(f"PSA slabs: **{slab_count}** (${slab_value:,.2f})")
    if op:
        parts.append(f"One Piece singles: **${op:,.2f}**")
    if pk:
        parts.append(f"Pokémon singles: **${pk:,.2f}**")
    if len(parts) > 1:
        parts.append(f"Combined: **${combined:,.2f}**")
    lines.append("  •  ".join(parts))
    lines.append("")
    singles_routes = (
        f"get your **One Piece singles alone to ${MIN_LOT_VALUE:,}+** or your "
        f"**Pokémon singles alone to ${POKEMON_SINGLES_MIN_LOT:,}+** (upload your Collectr CSV)"
    )
    if slab_count > 0:
        if slab_count < MIN_SLAB_COUNT:
            lines.append(
                f"• We require at least **{MIN_SLAB_COUNT} PSA slabs** — you have **{slab_count}**, "
                f"so **{MIN_SLAB_COUNT - slab_count}** more needed."
            )
        if combined < MIN_LOT_VALUE:
            lines.append(
                f"• Combined value must be **${MIN_LOT_VALUE:,}+** — you're **${MIN_LOT_VALUE - combined:,.2f}** short."
            )
        lines += [
            "",
            "We prioritize **quantity** — we don't take just a few big slabs. Either add more **PSA slab certs** "
            f"to reach {MIN_SLAB_COUNT}, **or** {singles_routes} and "
            "we'll take the slabs regardless of count. Then I'll get you set up to ship! 🙌",
        ]
    else:
        if op:
            lines.append(
                f"• One Piece singles need **${MIN_LOT_VALUE:,}+** on their own — "
                f"you're **${MIN_LOT_VALUE - op:,.2f}** short."
            )
        if pk:
            lines.append(
                f"• Pokémon singles need **${POKEMON_SINGLES_MIN_LOT:,}+** on their own — "
                f"you're **${POKEMON_SINGLES_MIN_LOT - pk:,.2f}** short."
            )
        lines += [
            "",
            "Add more raw singles and re-upload your Collectr CSV — hitting **either** singles minimum "
            "qualifies the whole lot (or add **PSA slabs** — note that any lot containing slabs needs "
            f"**{MIN_SLAB_COUNT}+** of them). 🙌",
        ]
    return "\n".join(lines)
_POKEMON_RAW_WELCOME_LINE = (
    "• **Pokémon raw singles** (English, Near Mint, ungraded, **$40–$80 per card**, **2022 or newer sets only**, "
    "Pokémon character cards only — **no trainers**, **no Master Ball versions**) → upload your Collectr CSV export\n"
) if BUYING_POKEMON_RAW else ""
_POKEMON_RAW_MIN_LINE = (
    "• **Pokémon singles only:** your Collectr export must be **$1,500+** in value.\n"
) if BUYING_POKEMON_RAW else ""
_POKEMON_RAW_OFF_LINE = (
    "" if BUYING_POKEMON_RAW else "⚠️ We are **not** buying Pokémon raw cards this weekend.\n\n"
)
WELCOME_MSG = (
    "👋 Welcome to KTS Collectibles!\n\n"
    "We're currently buying (PSA graded slabs — send your cert numbers):\n"
    "• **Pokémon** — $17–$5,000 🔥 ANY grade · ANY cert — 90% up to $100, 87% above\n"
    "• **One Piece** — $1–$700 and $900–$1,000\n"
    "• **Baseball / MLB** — $1–$240 and $250–$800\n"
    "• **Basketball / NBA** — $1–$800 and $900–$1,400\n"
    "• **Football / NFL** — $1–$800\n"
    + _POKEMON_RAW_WELCOME_LINE +
    "\n"
    f"**CGC slabs — Pokémon only:** ${CGC_MIN_PRICE}–${CGC_MAX_PRICE} at "
    f"{int(round(CGC_POKEMON_RATE * 100))}% (send the cert numbers with \"CGC\" in front)\n\n"
    "⚠️ We are **not** buying raw cards — PSA slabs (plus CGC Pokémon) only.\n\n"
    "📊 **Pokémon:** no minimum — send what you've got!\n"
    "<<SPORTS_OP_LINE>>"
    + _POKEMON_RAW_MIN_LINE +
    "\nWhat are you looking to sell?"
)
_SPORTS_OP_TAIL = ("— those need **$1,500+** (combined) and ship to a "
                   "**separate address** from your Pokémon.\n")

def welcome_msg():
    """WELCOME_MSG with the sports/One Piece line matching the go-live gate, so
    it stops saying "open Saturday 3 AM ET" once they're actually open."""
    line = ("🏈 **Sports & One Piece** are open " if sports_op_live()
            else "🗓️ **Sports & One Piece** open **Saturday 3 AM ET** ")
    return WELCOME_MSG.replace("<<SPORTS_OP_LINE>>", line + _SPORTS_OP_TAIL)

SHIPPING_MSG = (
    "📦 **Awesome, let's do it!** Ship your cards to the address below — you'll be paid out once your package arrives and is processed.\n\n"
    "**Ship to:**\n"
    "Kevin Smith\n"
    "3630 S Plaza Trail\n"
    "Suite 150-E\n"
    "Virginia Beach, VA 23452\n\n"
    "🚚 **Shipping method — required:** **UPS Next Day Air A.M.** (overnight, morning delivery) — "
    "nothing slower (no standard overnight, 2-Day, or ground). Packages **must be in hand by "
    "Tuesday 9 AM**, so ship **Monday at the latest**.\n\n"
    "📥 **Packaging:**\n"
    "• **Slabs:** ship as-is — no sleeves or stickers.\n"
    "• **Raw cards:** penny sleeve only — no top loaders.\n\n"
    "🗓️ **Payouts run Fridays.** First come, first serve — you join the queue the moment your package is scanned in on arrival.\n"
    "• In hand **Tuesday** → usually paid that **Friday**\n"
    "• Arrives later in the week → rolls into the **next** Friday payout.\n\n"
    "👉 Best move: **overnight it Monday so it lands Tuesday morning** to make that week's payout. Payment via wire or ACH ⚡\n\n"
    "Once you've shipped, **drop your tracking number here** so Kevin can keep an eye out!"
)

# ── DUAL-ADDRESS SHIPPING (Oct 17) ────────────────────────────────────────────
# Pokémon → Virginia Beach (Pixel), NO minimum. Sports + One Piece → Doral
# (Triumph), only if that portion totals $1,500+. A mixed lot ships as TWO
# packages. SHIPPING_MSG above stays the fail-open default (VA) for the
# redeploy-lost-state path.
_VA_BLOCK = (
    "Kevin Smith\n"
    "3630 S Plaza Trail\n"
    "Suite 150-E\n"
    "Virginia Beach, VA 23452"
)
_DORAL_BLOCK = (
    "Kevin Smith\n"
    "8266 NW 14th St\n"
    "Doral, FL 33126-1703"
)
_SHIP_METHOD_TAIL = (
    "🚚 **Shipping method — required:** **UPS Next Day Air A.M.** (overnight, morning delivery) — "
    "nothing slower (no standard overnight, 2-Day, or ground). Packages **must be in hand by "
    "Tuesday 9 AM**, so ship **Monday at the latest**.\n\n"
    "📥 **Packaging:**\n"
    "• **Slabs:** ship as-is — no sleeves or stickers.\n"
    "• **Raw cards:** penny sleeve only — no top loaders.\n\n"
    "🗓️ **Payouts run Fridays.** First come, first serve — you join the queue the moment your package is scanned in on arrival.\n"
    "• In hand **Tuesday** → usually paid that **Friday**\n"
    "• Arrives later in the week → rolls into the **next** Friday payout.\n\n"
    "👉 Best move: **overnight it Monday so it lands Tuesday morning** to make that week's payout. Payment via wire or ACH ⚡\n\n"
    "Once you've shipped, **drop your tracking number here** so Kevin can keep an eye out!"
)

def compose_shipping(channel_id):
    """Return (message, gave_address, kevin_note) for a lot that has cleared the
    ship gate. Routes Pokémon → VA and qualifying Sports/OP → Doral; a mixed lot
    gets both as two packages. Sports/OP under $1,500 is held (no FL address)
    while any Pokémon still ships to VA."""
    pk, so = lot_value_by_lane(channel_id)
    # A lot rebuilt from the sheets after a restart reflects what Kevin already
    # agreed to take (he zeroes what he won't), so the $1,500 gate isn't re-applied.
    rebuilt = bool((lot_state.get(channel_id) or {}).get("rebuilt"))
    so_ok = so >= SPORTS_OP_MIN_LOT or (rebuilt and so > 0)
    has_pk, has_so = pk > 0, so > 0
    # Fail open (redeploy lost state): no recorded lanes -> default VA message.
    if not has_pk and not has_so:
        return SHIPPING_MSG, True, "VA (default/no lot on record)"
    # Sports/OP present but short AND no Pokémon -> pure hold, no address.
    if has_so and not so_ok and not has_pk:
        short = SPORTS_OP_MIN_LOT - so
        msg = (
            f"📊 **Almost there!** Your Sports & One Piece slabs total **${so:,.2f}** — "
            f"our minimum for that lane is **${SPORTS_OP_MIN_LOT:,}** (Sports/One Piece ship to a "
            f"separate address, so we batch them). You're **${short:,.2f}** short.\n\n"
            f"Add more Sports/One Piece slabs to hit **${SPORTS_OP_MIN_LOT:,}+** and resubmit — "
            f"or send any **Pokémon** (no minimum) and I'll get you shipping right away! 🙌"
        )
        return msg, False, f"HELD — sports/OP ${so:,.2f} < ${SPORTS_OP_MIN_LOT:,}, no pokemon"
    lanes = []
    kparts = []
    if has_pk:
        lanes.append(("🟡 **Pokémon → ship to:**", _VA_BLOCK))
        kparts.append(f"pokemon ${pk:,.2f} → VA")
    if has_so and so_ok:
        lanes.append(("🔵 **Sports & One Piece → ship to:**", _DORAL_BLOCK))
        kparts.append(f"sports/OP ${so:,.2f} → Doral")
    if len(lanes) == 2:
        head = ("📦 **Awesome, let's do it!** Your lot splits across two lanes — please ship as "
                "**two separate packages** to the addresses below (you're paid once each arrives and is processed).\n\n")
        body = "\n\n".join(f"{label}\n{block}" for label, block in lanes)
    else:
        label, block = lanes[0]
        head = ("📦 **Awesome, let's do it!** Ship your cards to the address below — "
                "you'll be paid out once your package arrives and is processed.\n\n")
        body = f"{label}\n{block}"
    tail_note = ""
    if has_so and not so_ok:  # pokemon ships, sports/OP held
        short = SPORTS_OP_MIN_LOT - so
        tail_note = (
            f"\n\n⏳ **Heads up:** your Sports/One Piece (**${so:,.2f}**) is under the "
            f"**${SPORTS_OP_MIN_LOT:,}** minimum for that lane (it ships separately to our FL address). "
            f"You're **${short:,.2f}** short — add more and I'll send that address too. "
            f"Your Pokémon above is good to ship now!"
        )
        kparts.append(f"sports/OP ${so:,.2f} HELD (<${SPORTS_OP_MIN_LOT:,})")
    msg = head + body + tail_note + "\n\n" + _SHIP_METHOD_TAIL
    if rebuilt:
        kparts.append("lot rebuilt from their sheets after a restart — $1,500 min not re-checked")
    return msg, True, " · ".join(kparts)

def ship_blocked_sportsop_only(channel_id):
    """True when the ONLY thing on the lot is sports/OP under the $1,500 minimum
    (nothing can ship yet). Pokémon on the lot always clears this."""
    pk, so = lot_value_by_lane(channel_id)
    return (so > 0 and so < SPORTS_OP_MIN_LOT and pk <= 0)

AGREE_KEYWORDS = ["ship", "proceed"]

DISCORD_MAX_LEN = 2000

def _split_for_discord(text, limit=DISCORD_MAX_LEN):
    """Split text at line boundaries into chunks <= limit chars (Discord's hard cap)."""
    if len(text) <= limit:
        return [text]
    chunks, current = [], ""
    for line in text.split("\n"):
        if len(line) > limit:
            if current:
                chunks.append(current)
                current = ""
            for i in range(0, len(line), limit):
                chunks.append(line[i:i + limit])
            continue
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) > limit:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks

async def ping_kevin(msg, channel=None):
    """DM Kevin. Returns True if delivered, False otherwise (never raises) —
    callers that must not lose an alert (helper_health_monitor) retry on False."""
    try:
        kevin = await bot.fetch_user(YOUR_DISCORD_USER_ID)
        channel_link = f"\n**Ticket:** <#{channel.id}>" if channel else ""
        for chunk in _split_for_discord(msg + channel_link):
            await kevin.send(chunk)
        return True
    except Exception as e:
        print(f"Could not ping Kevin: {e}")
        return False

def is_agreeing(text):
    # Trigger shipping when the message is just an agree word by itself
    # ("ship" or "proceed"), tolerating trailing punctuation/spaces.
    cleaned = text.strip().lower().strip(".!?, ")
    return cleaned in AGREE_KEYWORDS

import aiohttp.web as _ow_web

# Persistent data dir: set env DATA_DIR to a mounted Railway volume (e.g. /data) so
# owed/leaderboard/link stores survive redeploys. Falls back to the app folder (ephemeral).
DATA_DIR = os.environ.get("DATA_DIR") or os.path.dirname(os.path.abspath(__file__))
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception as _e:
    print(f"DATA_DIR not writable ({DATA_DIR}): {_e}; using app folder")
    DATA_DIR = os.path.dirname(os.path.abspath(__file__))

# ── OWED / PACKAGES — the bot stores deals and serves them to the local tracker ──
OWED_FILE = os.path.join(DATA_DIR, "owed_store.json")
try:
    with open(OWED_FILE) as _f:
        OWED_STORE = json.load(_f)
except Exception:
    OWED_STORE = []

def _save_owed():
    try:
        with open(OWED_FILE, "w") as _f:
            json.dump(OWED_STORE, _f)
    except Exception as e:
        print(f"owed store save failed (non-critical): {e}")

def upsert_owed(rec):
    for i, o in enumerate(OWED_STORE):
        if o.get("id") == rec["id"]:
            OWED_STORE[i] = {**o, **rec}
            _save_owed()
            return
    OWED_STORE.append(rec)
    _save_owed()

# ── ACCEPTED SHIP-LISTS (per ticket) ──────────────────────────────────────────────
# Recorded when a seller agrees to the offer; served at GET /tickets so the local
# ticket-tracker HTML can sync them (same pattern as OWED_STORE -> /owed).
TICKETS_FILE = os.path.join(DATA_DIR, "tickets_store.json")
try:
    with open(TICKETS_FILE) as _f:
        TICKETS_STORE = json.load(_f)
except Exception:
    TICKETS_STORE = []

def _save_tickets():
    try:
        with open(TICKETS_FILE, "w") as _f:
            json.dump(TICKETS_STORE, _f)
    except Exception as e:
        print(f"tickets store save failed (non-critical): {e}")

def upsert_ticket(rec):
    for i, t in enumerate(TICKETS_STORE):
        if t.get("id") == rec["id"]:
            TICKETS_STORE[i] = {**t, **rec}
            _save_tickets()
            return
    TICKETS_STORE.append(rec)
    _save_tickets()

def record_accepted_ship_list(channel_id, username):
    """Snapshot the accepted lot for this ticket (certs + CL comp values, singles
    totals, offer rate). lot_state is memory-only, so after a redeploy between
    quote and 'ship' there may be nothing to record — skip silently; the tracker's
    manual paste path covers that rare case."""
    entry = lot_state.get(channel_id) or {}
    certs = entry.get("slab_certs") or {}
    singles = entry.get("singles") or {}
    if not certs and not singles:
        return None
    offer = last_offer.get(channel_id) or {}
    rec = {
        "id": f"tk_{channel_id}",
        "user": username,
        "channel_id": channel_id,
        "date": date.today().isoformat(),
        "certs": {str(c): float(v or 0) for c, v in certs.items()},
        "singles": {g: float(v or 0) for g, v in singles.items()},
        "rate": offer.get("rate"),
        "status": "accepted",
    }
    upsert_ticket(rec)
    return rec

class _CsvDone(Exception):
    """Control-flow sentinel: 'done with the Collectr CSV block, skip its remainder'.
    Raised on the early-exit paths (parse error, validation issues) inside the CSV
    branch so execution still reaches the fall-through to the PSA-cert handler —
    a message can carry BOTH a CSV and cert numbers, and returning early used to
    silently drop the certs. Caught before the generic `except Exception`."""
    pass

# ── PERSISTED CHANNEL STATE — survives redeploys ──────────────────────────────────
# channel_sheet (channel -> buying sheet) and welcomed_tickets used to be memory-only,
# so every Railway deploy wiped them: tracking numbers fell back to a fuzzy Drive
# search (which never matched old "ticket-" channels and could match the WRONG
# customer on short usernames) and long tickets risked re-welcomes. Persist both.
CHANNEL_SHEET_FILE = os.path.join(DATA_DIR, "channel_sheet_store.json")
WELCOMED_FILE = os.path.join(DATA_DIR, "welcomed_store.json")
try:
    with open(CHANNEL_SHEET_FILE) as _f:
        channel_sheet.update({int(k): v for k, v in json.load(_f).items()})
except Exception:
    pass
try:
    with open(WELCOMED_FILE) as _f:
        welcomed_tickets.update(int(x) for x in json.load(_f))
except Exception:
    pass

def _save_channel_sheet():
    try:
        with open(CHANNEL_SHEET_FILE, "w") as _f:
            json.dump({str(k): v for k, v in channel_sheet.items()}, _f)
    except Exception as e:
        print(f"channel_sheet store save failed (non-critical): {e}")

def _save_welcomed():
    try:
        with open(WELCOMED_FILE, "w") as _f:
            json.dump(list(welcomed_tickets), _f)
    except Exception as e:
        print(f"welcomed store save failed (non-critical): {e}")

def remember_channel_sheet(channel_id, sheet_id):
    channel_sheet[channel_id] = sheet_id
    _save_channel_sheet()

# Every buying sheet a cert submission CREATED in a ticket, in order. channel_sheet
# keeps only the latest one (each new submission overwrites it), so a sheet of
# certs KEVIN pasted in the ticket (named after him, not the customer) used to drop
# out of everything keyed on the cache as soon as the customer sent more. Only the
# cert path writes here — never a Drive lookup — so an entry is proof the sheet
# was made in THIS ticket. {channel_id: [{"sid", "owner", "kevin", "ts"}]}
TICKET_SHEETS_FILE = os.path.join(DATA_DIR, "ticket_sheets_store.json")
ticket_sheets = {}
try:
    with open(TICKET_SHEETS_FILE) as _f:
        ticket_sheets.update({int(k): v for k, v in json.load(_f).items()})
except Exception:
    pass

def record_ticket_sheet(channel_id, sheet_id, owner, by_kevin=False):
    """Cert path only: remember that this ticket's submission created sheet_id."""
    try:
        hist = ticket_sheets.setdefault(channel_id, [])
        if not any(h.get("sid") == sheet_id for h in hist):
            hist.append({"sid": sheet_id, "owner": owner or "", "kevin": bool(by_kevin),
                         "ts": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')})
            del hist[:-200]
        with open(TICKET_SHEETS_FILE, "w") as _f:
            json.dump({str(k): v for k, v in ticket_sheets.items()}, _f)
    except Exception as e:
        print(f"ticket sheet history save failed (non-critical): {e}")

def kevin_pasted_sheet_ids(channel_id):
    """Sheets Kevin's own cert pastes created in this ticket (oldest first)."""
    return [h["sid"] for h in ticket_sheets.get(channel_id, []) if h.get("kevin") and h.get("sid")]

# Pending comp retries survive redeploys: the JSON store holds everything except
# the task handle; on_ready re-spawns young entries (see _resume_pending_retries).
PENDING_RETRY_FILE = os.path.join(DATA_DIR, "pending_retries_store.json")

def _save_pending_retries():
    try:
        snap = {str(cid): {k: v for k, v in e.items() if k != "task"}
                for cid, e in pending_comp_retries.items()}
        with open(PENDING_RETRY_FILE, "w") as _f:
            json.dump(snap, _f)
    except Exception as e:
        print(f"pending-retry store save failed (non-critical): {e}")

_PENDING_RETRIES_RESUMED = False

async def _resume_pending_retries():
    """After a redeploy, re-spawn comp retries that were promised to customers
    ("I'll keep trying...") before the restart. Entries older than the retry
    window — or whose ticket channel no longer resolves — are dropped with a
    ping so Kevin can quote manually instead of the promise silently dying."""
    try:
        with open(PENDING_RETRY_FILE) as _f:
            saved = json.load(_f)
    except Exception:
        return
    if not saved:
        return
    now = datetime.now().timestamp()
    for cid_str, e in saved.items():
        cid = int(cid_str)
        age_s = max(0, now - float(e.get("spawned_at", 0)))
        remaining = 15 - int(age_s // 180)
        chan = None
        try:
            chan = bot.get_channel(cid) or await bot.fetch_channel(cid)
        except Exception:
            pass
        if chan is None or remaining < 1:
            await ping_kevin(
                f"⚠️ Pending comp auto-retry for **{e.get('username')}** "
                f"({len(e.get('certs', []))} certs) expired across a redeploy — "
                f"quote manually: {e.get('sheet_url')}")
            continue
        start_comp_retry(chan, cid, e.get("username"), e.get("certs", []),
                         e.get("sheet_id"), e.get("sheet_url"),
                         max_attempts=remaining, spawned_at=e.get("spawned_at"))
    _save_pending_retries()   # drops the expired/unresolvable entries

# ── VIP / PREMIUM RATES (Pokémon slabs only) ─────────────────────────────────────
# Kevin negotiates per-customer Pokémon rates. Managed entirely in Discord:
#   !vip add <username or @mention> 91            -> 91% on ALL Pokémon bands
#   !vip add <username> 91 88 85                  -> per band (≤$100 / mid / high)
#   !vip remove <username>    !vip list
# Keyed by lowercase Discord username; persisted to DATA_DIR. Rates are PINNED —
# weekly flyer changes don't move a VIP's promised rate until Kevin re-adds them.
# Only the payout % changes: grade floors, ceilings and sale-age stay standard.
VIP_FILE = os.path.join(DATA_DIR, "vip_store.json")
_VIP_LOAD_FAILED = False
try:
    with open(VIP_FILE) as _f:
        _raw_vip = json.load(_f)
    VIP_RATES = {}
    for _k, _v in _raw_vip.items():
        # Valid: a non-empty numeric 'pokemon' rate list, and/or flat numeric
        # per-sport rates under other keys (multi-sport VIP, Sep 21).
        _ok = False
        if isinstance(_v, dict):
            _pok = _v.get("pokemon")
            _pok_valid = (isinstance(_pok, list) and _pok
                          and all(isinstance(_x, (int, float)) for _x in _pok))
            _sports_valid = any(_sk != "pokemon" and isinstance(_sv, (int, float))
                                for _sk, _sv in _v.items())
            _ok = _pok_valid or (_pok in (None, []) and _sports_valid)
        if _ok:
            VIP_RATES[str(_k).lower()] = _v
        else:
            print(f"vip store: dropping malformed entry {_k!r}")
except FileNotFoundError:
    VIP_RATES = {}
except Exception as _e:
    # A corrupt store must NOT silently revert VIPs to standard rates: preserve
    # the bad file (the next _save_vip would clobber it) and alert Kevin from
    # on_ready. Money is quoted off this dict.
    VIP_RATES = {}
    _VIP_LOAD_FAILED = True
    print(f"VIP STORE FAILED TO LOAD ({_e}) — VIPs quote at STANDARD rates until re-added")
    try:
        os.replace(VIP_FILE, VIP_FILE + ".corrupt")
    except Exception:
        pass

def _save_vip():
    # Atomic write: a crash mid-save must never leave a truncated store behind.
    try:
        with open(VIP_FILE + ".tmp", "w") as _f:
            json.dump(VIP_RATES, _f)
        os.replace(VIP_FILE + ".tmp", VIP_FILE)
    except Exception as e:
        print(f"vip store save failed (non-critical): {e}")

# Wired-in VIP roster (Kevin, Sep 21). DATA_DIR has NO volume on Railway, so
# the JSON store wipes on every deploy — these seed entries survive because
# they live in code. A !vip store entry for the same username OVERRIDES its
# seed ('!vip clear' does NOT remove seed entries — edit this dict for that).
# Schema per user: 'pokemon' -> list of band rates (like the store);
# other keys -> flat VIP rate for that canonical sport.
VIP_SEED = {
    # cwilk_sportscards REMOVED Oct 5 (Kevin: "dont give cwilk vip rates") —
    # he quotes at standard rates now. If he's re-VIP'd, remember the ticket
    # channel spelling ends in '.': seed BOTH 'cwilk_sportscards' and
    # 'cwilk_sportscards.'.
}

def _vip_entry(username):
    """The user's VIP entry: store entry if present, else the code seed."""
    key = (username or "").lower()
    return VIP_RATES.get(key) or VIP_SEED.get(key)

def vip_sport_rates(username):
    """Non-Pokémon flat VIP rates for the user, e.g. {'basketball': 0.95}.
    Empty dict if none. Sports keys are canonical (normalize_sport output)."""
    entry = _vip_entry(username) or {}
    return {k: v for k, v in entry.items()
            if k != 'pokemon' and isinstance(v, (int, float))}

def vip_pokemon_tiers(username):
    """The user's custom Pokémon tier list — band boundaries from the CURRENT
    standard tiers, rates from their VIP entry. If they were given FEWER rates
    than there are bands, the remaining bands keep the STANDARD rate (a VIP
    negotiated on the low band must not silently get a premium rate on a
    $1,500 slab). None if the user isn't a VIP."""
    entry = _vip_entry(username)
    if not entry:
        return None
    rates = entry.get("pokemon") or []
    if not rates:
        return None
    return [(low, high, rates[i] if i < len(rates) else std)
            for i, (low, high, std) in enumerate(PSA_POKEMON_PER_CARD_TIERS)]

def _pokemon_band_labels():
    """Human labels for the Pokémon RATE tiers — must always return exactly
    one label per entry of PSA_POKEMON_PER_CARD_TIERS (the !vip summary zips
    them; a count mismatch is an IndexError that kills every !vip command)."""
    ceiling = PSA_SPORT_MAX_PRICE.get('pokemon', 0)
    labels = []
    for i, (low, high, _r) in enumerate(PSA_POKEMON_PER_CARD_TIERS):
        top = ceiling if high == float('inf') else round(high - 0.01)
        labels.append(f"≤${top:,.0f}" if i == 0 else f"${round(low - 0.01):,.0f}-${top:,.0f}")
    return labels

def _vip_parse_rates(tokens):
    """Parse 1-3 rate tokens -> (rates, err). Accepts 91, 91%, 0.91."""
    rates = []
    for t in tokens[:len(PSA_POKEMON_PER_CARD_TIERS)]:
        try:
            v = float(t.replace('%', ''))
        except ValueError:
            return None, f"'{t}' isn't a number — usage: `!vip add <username> 91 88 85`"
        if v > 1.5:
            v = v / 100.0
        if not (0.5 <= v <= 1.2):
            return None, f"{t} is outside the sane range (50-120%) — not saved."
        rates.append(round(v, 4))
    if not rates:
        return None, "Usage: `!vip add <username> 91` or `!vip add <username> 91 88 85`"
    return rates, None

def _vip_resolve_target(token, mentions, guild, for_remove=False):
    """Resolve one target token (mention / user ID / username) -> (name, err, note).
    Safety rules: mention tokens matched by id; pasted 15-21 digit IDs resolved
    via the member list; usernames validated against real members. A typed name
    that isn't an exact username AUTO-MATCHES when exactly ONE member fits
    (their display/global name matches exactly, or exactly one member's names
    contain it) — note explains the substitution so it's always visible in the
    reply. Multiple or zero candidates still refuse: never guess on money.
    for_remove skips validation so raw keys can be cleaned up."""
    m = re.match(r'^<@!?(\d+)>$', token)
    if m:
        uid = int(m.group(1))
        user = next((u for u in (mentions or []) if getattr(u, 'id', None) == uid), None)
        if user is None:
            return None, "Couldn't resolve that @mention — try the plain username instead.", None
        return user.name.lower(), None, None
    name = token.lstrip('@').lower()
    if re.match(r'^\d{15,21}$', name):
        mb = next((mm for mm in (guild.members if guild else [])
                   if getattr(mm, 'id', None) == int(name)), None)
        if mb is None:
            if for_remove:
                return name, None, None
            return None, (f"`{name}` looks like a Discord user ID, but no server "
                          f"member matched it — use **Copy Username** instead, "
                          f"or @mention them."), None
        return mb.name.lower(), None, None
    if for_remove:
        return name, None, None
    try:
        float(name)
        return None, (f"'{token}' looks like a rate, not a username — "
                      f"usage: `!vip add <username> 91 [88 85]`"), None
    except ValueError:
        pass
    if not re.match(r'^[a-z0-9._]{2,32}$', name):
        return None, f"'{token}' doesn't look like a Discord username — not saved.", None
    if guild is not None:
        exact = next((mb for mb in guild.members if mb.name.lower() == name), None)
        if exact is None:
            # Tier 1: their display/global name matches exactly.
            t1 = [mb for mb in guild.members
                  if (mb.display_name or '').lower() == name
                  or ((getattr(mb, 'global_name', '') or '')).lower() == name]
            # Tier 2: the token appears inside any of their names.
            t2 = [mb for mb in guild.members
                  if name in mb.name.lower()
                  or name in (mb.display_name or '').lower()
                  or name in ((getattr(mb, 'global_name', '') or '')).lower()]
            pick = t1[0] if len(t1) == 1 else (t2[0] if len(t2) == 1 else None)
            if pick is not None:
                return pick.name.lower(), None, f"matched from `{name}`"
            cands = [mb.name for mb in (t1 or t2)][:3]
            hint = (" Did you mean: " + ", ".join(f"`{c}`" for c in cands) + "?") if cands else ""
            return None, (f"No server member has the exact username `{name}` — not saved "
                          f"(VIP keys must match their login username, not display name).{hint}"), None
    return name, None, None

def _vip_rate_summary(name):
    parts = []
    tiers = vip_pokemon_tiers(name)
    if tiers:
        labels = _pokemon_band_labels()
        parts.append(", ".join(f"{labels[i]} → {t[2]*100:g}%" for i, t in enumerate(tiers)))
    srates = vip_sport_rates(name)
    if srates:
        parts.append(", ".join(f"{_sport_label(s)} → {r*100:g}%"
                               for s, r in sorted(srates.items())))
    return " | ".join(parts) if parts else "(no rates?)"

def handle_vip_command(content, mentions=None, guild=None):
    """Parse and apply a !vip command; returns the reply text (sync, testable).
    Subcommands: add/set (one user), bulk/batch/all (rates on the first line,
    then one username/ID per line), remove, list. Mention targets are resolved
    from the token IN the text matched by id — raw message.mentions is never
    trusted on its own (Discord's reply-ping injects the replied-to user)."""
    first_line = content.strip().split("\n")[0]
    toks = first_line.split()[1:]   # drop '!vip'
    sub = (toks[0].lower() if toks else "list")

    if sub == "list":
        names = sorted(set(VIP_RATES) | set(VIP_SEED))
        if not names:
            return "No VIP users set. Add one with `!vip add <username> 91` (or `91 88 85` per band)."
        lines = ["✨ **VIP rates:**"]
        for name in names:
            tag = " 🔒 (wired-in)" if name in VIP_SEED and name not in VIP_RATES else ""
            lines.append(f"• **{name}**: {_vip_rate_summary(name)}{tag}")
        return "\n".join(lines)

    if sub == "clear":
        # Full reset, two-step: bare `!vip clear` previews the roster (so Kevin
        # can review before nuking), `!vip clear yes` wipes it.
        if not VIP_RATES:
            return "VIP list is already empty."
        if len(toks) > 1 and toks[1].lower() in ("yes", "confirm"):
            n = len(VIP_RATES)
            names = ", ".join(sorted(VIP_RATES))
            VIP_RATES.clear()
            _save_vip()
            return (f"🧹 VIP list cleared — removed {n} user{'s' if n != 1 else ''}: {names}.\n"
                    f"Re-add with `!vip add <username> <rates>` or `!vip bulk`.")
        preview = "\n".join(f"• **{name}**: {_vip_rate_summary(name)}" for name in sorted(VIP_RATES))
        return (f"⚠️ **This removes ALL {len(VIP_RATES)} VIP entries:**\n{preview}\n\n"
                f"Type `!vip clear yes` to confirm.")

    if sub in ("bulk", "batch", "all"):
        rates, err = _vip_parse_rates(toks[1:])
        if err:
            return err + "\nBulk usage: first line `!vip bulk 91 90 85`, then one username/ID per line."
        target_tokens = []
        for line in content.strip().split("\n")[1:]:
            target_tokens += [t for t in re.split(r'[,\s]+', line.strip()) if t]
        if not target_tokens:
            return ("Bulk usage — rates on the first line, then the list:\n"
                    "`!vip bulk 91 90 85`\n`icevyy`\n`meta`\n`...` (usernames or user IDs, one per line)")
        results = []
        applied = 0
        for t in target_tokens:
            name, terr, note = _vip_resolve_target(t, mentions, guild)
            if terr:
                results.append(f"• {t} ✗ {terr}")
                continue
            _sw = (" ⚠️ replaces wired-in rates (non-Pokémon sports drop to standard)"
                   if name in VIP_SEED and vip_sport_rates(name) else "")
            VIP_RATES[name] = {"pokemon": list(rates)}
            applied += 1
            results.append(f"• **{name}** ✓" + (f" ({note})" if note else "") + _sw)
        if applied:
            _save_vip()
        pretty = "/".join(f"{r*100:g}" for r in rates)
        return (f"✨ VIP Pokémon rates **{pretty}** applied to {applied} of "
                f"{len(target_tokens)}:\n" + "\n".join(results)
                + ("\n(pinned until you change them — weekly flyer updates won't move them)" if applied else ""))

    if sub in ("add", "set"):
        rest = toks[1:]
        target_tok = None
        for i, t in enumerate(rest):
            target_tok = t
            rest = rest[:i] + rest[i+1:]
            break
        if target_tok is None:
            return "Usage: `!vip add <username> 91` or `!vip add <username> 91 88 85`"
        name, err, note = _vip_resolve_target(target_tok, mentions, guild)
        if err:
            return err
        rates, err = _vip_parse_rates(rest)
        if err:
            return err
        _seed_warn = ""
        if name in VIP_SEED and vip_sport_rates(name):
            _dropped = ", ".join(f"{_sport_label(s)} {r*100:g}%"
                                 for s, r in sorted(vip_sport_rates(name).items()))
            _seed_warn = (f"\n⚠️ **{name}** has wired-in rates ({_dropped}) — this "
                          f"override REPLACES all of them: their non-Pokémon sports "
                          f"now quote at STANDARD rates. `!vip remove {name}` brings "
                          f"the wired-in rates back.")
        VIP_RATES[name] = {"pokemon": rates}
        _save_vip()
        _tag = f" ({note})" if note else ""
        return (f"✨ VIP set for **{name}**{_tag}: Pokémon {_vip_rate_summary(name)}\n"
                f"(pinned until you change it — weekly flyer updates won't move it; "
                f"grade/ceiling/sale-age rules stay standard)" + _seed_warn)

    if sub in ("remove", "delete", "del"):
        if len(toks) < 2:
            return "Usage: `!vip remove <username>`"
        name, err, _note = _vip_resolve_target(toks[1], mentions, guild, for_remove=True)
        if err:
            return err
        if name in VIP_RATES:
            del VIP_RATES[name]
            _save_vip()
            if name in VIP_SEED:
                return (f"Removed the **{name}** override — their WIRED-IN rates "
                        f"apply again: {_vip_rate_summary(name)}\n"
                        f"(wired-in rates live in the bot code — ask Claude to "
                        f"edit VIP_SEED to change or drop them)")
            return f"Removed **{name}** from VIP — they get standard rates now."
        if name in VIP_SEED:
            return (f"**{name}** has WIRED-IN rates ({_vip_rate_summary(name)}) — "
                    f"those live in the bot code and can't be removed by command. "
                    f"Ask Claude to edit VIP_SEED.")
        return f"**{name or '?'}** isn't on the VIP list. `!vip list` to see who is."

    return ("Commands: `!vip add <username> 91 [88 85]` · `!vip bulk 91 90 85` + list "
            "of usernames on following lines · `!vip remove <username>` · `!vip list`")

def build_sheet_h_formula(r, pokemon_tiers=None, sport_rates=None):
    """The per-row payout formula written into buying sheets — generated from the
    bot's CURRENT rate constants so bot and sheet can't disagree. pokemon_tiers
    overrides only the Pokémon band RATES (VIP sheets); sport_rates replaces
    EVERY band rate of that sport with one flat VIP rate (gates/bans stay).
    Oct 2 structure: pokemon continuous $1-$5,000 (90/87, any grade+any cert
    at $3,000+, cert 7+ only ≤$1,000); one piece $1-$900 (88/83); MLB 100/90/90
    to $300 + Slugger bands; NBA 95/92 (backlog to $300; app lanes above) +
    2000s band; NFL 90/90/88 to $300 + GOAT bands. SELECT-list cards are stamped
    "Y" in column L by fill_buying_sheet — the formula trusts that stamp (the
    1,001-name NBA list can't live in a cell formula); Kevin can hand-type Y
    to override. If a weekly flyer changes the band COUNT, update this builder.
    Column F holds the HELPER's raw sport string: 'pokemon', 'basketball',
    'football'/'nfl', 'baseball'/'mlb', 'other'/'one piece'/aliases."""
    def _n(v):
        return f"{v:g}"   # 1.0 -> "1", 0.85 -> "0.85" (matches the template's style)
    pt = pokemon_tiers or PSA_POKEMON_PER_CARD_TIERS
    sr = sport_rates or {}
    p1 = _n(pt[0][2])                                   # pokemon ≤$100
    p2 = _n(pt[1][2] if len(pt) > 1 else pt[-1][2])     # pokemon $100-$3,000
    p3 = _n(pt[2][2] if len(pt) > 2 else (pt[1][2] if len(pt) > 1 else pt[-1][2]))  # $3,000-$5,000
    def _sport(tier_list, key):
        flat = sr.get(key)
        if flat is not None:
            return [_n(flat)] * len(tier_list)
        return [_n(t[2]) for t in tier_list]
    o1, o2 = _sport(PSA_ONE_PIECE_PER_CARD_TIERS, 'one piece')
    b1, b2, b3 = _sport(PSA_BASKETBALL_PER_CARD_TIERS, 'basketball')
    m1, m2, m3 = _sport(PSA_MLB_PER_CARD_TIERS, 'mlb')
    f1, f2, f3 = _sport(PSA_FOOTBALL_PER_CARD_TIERS, 'football')
    g = PSA_MIN_GRADE
    cs = POKEMON_MIN_CERT_SERIES
    is_op = (f'OR(F{r}="other",F{r}="one piece",F{r}="onepiece",'
             f'F{r}="tcg",F{r}="popculture",F{r}="pop culture")')
    is_mlb = f'OR(F{r}="baseball",F{r}="mlb")'
    is_nfl = f'OR(F{r}="football",F{r}="nfl")'
    sel = f'L{r}="Y"'   # bot-stamped select-list marker
    # NBA player + collegiate-set bans, mirrored from the bot's constants.
    ban_always = '|'.join(BBALL_PLAYERS_REJECT_ALWAYS + WNBA_PLAYERS
                          + COLLEGIATE_SET_KEYWORDS)
    ban_over = '|'.join(BBALL_PLAYERS_REJECT_OVER_200)
    bans = (f'OR(REGEXMATCH(LOWER(D{r}&""),"{ban_always}"),'
            f'AND(G{r}>{BBALL_PLAYER_PRICE_CAP},'
            f'REGEXMATCH(LOWER(D{r}&""),"{ban_over}")))')
    # Sale-age gates (100000 ≈ none): pokemon 60/30/none; OP 60/30;
    # NBA none ≤$1,000 else 60; MLB+NFL none ≤$100 else 60; default 30.
    maxage = (f'IFS(F{r}="pokemon",IF(N(G{r})<=2000,60,100000),'
              f'{is_op},IF(N(G{r})<=100,60,30),'
              f'F{r}="basketball",IF(N(G{r})<=1000,100000,60),'
              f'{is_mlb},IF(N(G{r})<=100,100000,60),'
              f'{is_nfl},IF(N(G{r})<=100,100000,60),'
              f'TRUE,30)')
    # Pokémon bands (Oct 3): $1-100, $100-300, $1,400-1,800 — slot quotas are
    # bot-side only (a formula can't count), so the sheet shows the band rate.
    pok = (f'IF(G{r}<17,0,'
           f'IF(AND(G{r}>=17,G{r}<=5000,LEFT(B{r}&"",1)="0"),0,'
           f'IFS(AND(G{r}>=17,G{r}<=100),{p1},'
           f'AND(G{r}>100,G{r}<=5000),{p2},'
           f'TRUE,0)))')
    op = (f'IF(N(E{r})<{g},0,'
          f'IFS(AND(G{r}>=1,G{r}<=100),{o1},'
          f'AND(G{r}>100,G{r}<=700),{o2},'
          f'AND(G{r}>=900,G{r}<=1000),{o2},'
          f'TRUE,0))')
    nba = (f'IF({bans},0,'
           f'IFS(AND(G{r}>=1,G{r}<=800),{b1},'
           f'AND(G{r}>=900,G{r}<=1000),{b1},'
           f'AND(G{r}>1000,G{r}<=1400,N(E{r})>={g}),{b2},'
           f'TRUE,0))')
    mlb = (f'IF(N(E{r})<{g},0,'
           f'IFS(AND(G{r}>=1,G{r}<=30),{m1},'
           f'AND(G{r}>30,G{r}<=240),{m2},'
           f'AND(G{r}>=250,G{r}<=500),{m2},'
           f'AND(G{r}>500,G{r}<=800),{m3},'
           f'TRUE,0))')
    nfl = (f'IF(N(E{r})<{g},0,'
           f'IFS(AND(G{r}>=1,G{r}<=30),{f1},'
           f'AND(G{r}>30,G{r}<=500),{f2},'
           f'AND(G{r}>500,G{r}<=800),{f3},'
           f'TRUE,0))')
    rate = (f'IFS('
            f'F{r}="pokemon",{pok},'
            f'{is_op},{op},'
            f'F{r}="basketball",{nba},'
            f'{is_mlb},{mlb},'
            f'{is_nfl},{nfl},'
            f'TRUE,0)')
    too_old = (f'IF(ISNUMBER(J{r}),(TODAY()-J{r})>{maxage},'
               f'IFERROR((TODAY()-DATEVALUE(J{r}))>{maxage},TRUE))')
    # CGC rows (Oct 18): flat CGC rate on Pokémon $1-$100, else 0 — never the
    # template's 70% non-PSA branch, and never a VIP rate. fill_buying_sheet
    # writes this formula into each CGC row's H; a VIP H-column rewrite keeps it.
    cgc = (f'IF(AND(F{r}="pokemon",G{r}>={CGC_MIN_PRICE},G{r}<={CGC_MAX_PRICE}),'
           f'{_n(CGC_POKEMON_RATE)},0)')
    return (f'=IF(OR(NOT(ISNUMBER(G{r})),G{r}=0),"",IF(A{r}="CGC",{cgc},'
            f'IF(A{r}<>"PSA",0.7,IF({too_old},0,{rate}))))')

def write_custom_sheet_formulas(sheet_id, pokemon_tiers, sport_rates=None,
                                sheet_name="Form. Put Date Here."):
    """Rewrite H2:H1000 in one customer's sheet with their VIP rates (Pokémon
    band tiers and/or flat per-sport rates). Blocking — run via asyncio.to_thread."""
    gc = get_gspread_client()
    ss = gc.open_by_key(sheet_id)
    try:
        sheet = ss.worksheet(sheet_name)
    except Exception:
        sheet = ss.sheet1
    sheet.update(values=[[build_sheet_h_formula(r, pokemon_tiers, sport_rates)]
                         for r in range(2, 1001)],
                 range_name="H2:H1000", value_input_option="USER_ENTERED")


# ── PAYMENT PACKETS / PAYOUT ORGANIZATION ────────────────────────────────────────
# Kevin sends every wire/ACH HIMSELF from his bank — the bot never moves money.
# It just makes the manual send instant: store each seller's payment details
# (!payinfo, Kevin-only, DM-routed — bank details never post in channels), DM a
# copy-paste "payment packet" whenever a debt is logged with !owe, mark payments
# done with !paid, and DM a queue digest on payout days (Thu/Fri).
PAYINFO_FILE = os.path.join(DATA_DIR, "payinfo_store.json")
try:
    with open(PAYINFO_FILE) as _f:
        PAYINFO = {str(k).lower(): v for k, v in json.load(_f).items()}
except Exception:
    PAYINFO = {}

def _save_payinfo():
    try:
        with open(PAYINFO_FILE + ".tmp", "w") as _f:
            json.dump(PAYINFO, _f)
        os.replace(PAYINFO_FILE + ".tmp", PAYINFO_FILE)
    except Exception as e:
        print(f"payinfo store save failed (non-critical): {e}")

def handle_payinfo_command(content):
    """!payinfo set <user> <details...> (multi-line ok) · !payinfo <user> ·
    !payinfo remove <user> · !payinfo list.  Sync + testable; caller routes the
    reply to Kevin's DM."""
    body = content.strip()
    lines = body.split("\n")
    toks = lines[0].split()[1:]   # drop '!payinfo'
    sub = (toks[0].lower() if toks else "list")
    if sub == "set":
        if len(toks) < 2:
            return "Usage: `!payinfo set <username> <their wire/ACH details...>` (details can span lines)"
        name = toks[1].lstrip('@').lower()
        details = " ".join(toks[2:])
        if len(lines) > 1:
            details = (details + "\n" if details else "") + "\n".join(l.strip() for l in lines[1:] if l.strip())
        if not details:
            return "No details given — paste their wire/ACH info after the username."
        PAYINFO[name] = {"details": details, "updated": date.today().isoformat()}
        _save_payinfo()
        return f"💾 Payment details saved for **{name}** (updated {PAYINFO[name]['updated']})."
    if sub in ("remove", "delete", "del"):
        name = (toks[1].lstrip('@').lower() if len(toks) > 1 else "")
        if name in PAYINFO:
            del PAYINFO[name]; _save_payinfo()
            return f"Removed payment details for **{name}**."
        return f"No payment details on file for **{name or '?'}**."
    if sub == "list":
        if not PAYINFO:
            return "No payment details on file yet. `!payinfo set <username> <details>` to add."
        return "💾 **Payment details on file:**\n" + "\n".join(
            f"• **{n}** (updated {v.get('updated','?')})" for n, v in sorted(PAYINFO.items()))
    # bare "!payinfo <user>" -> view
    name = sub.lstrip('@')
    v = PAYINFO.get(name)
    if not v:
        return f"No payment details on file for **{name}**. `!payinfo set {name} <details>` to add."
    return f"💸 **{name}** (updated {v.get('updated','?')}):\n{v.get('details','')}"

def build_payment_packet(seller, amount, method, note=""):
    """The copy-paste DM Kevin gets when a debt is logged: everything his bank's
    send screen needs, plus safety flags. Never posted in channels."""
    key = (seller or "").lower()
    info = PAYINFO.get(key)
    flags = []
    dup = [o for o in OWED_STORE
           if o.get("status") != "paid"
           and str(o.get("discord", "")).lower() == key
           and abs(float(o.get("amount", 0)) - float(amount)) < 0.005]
    if len(dup) > 1:
        flags.append(f"⚠️ DUPLICATE? {len(dup)} unpaid entries for {seller} at this exact amount — make sure this isn't logged twice.")
    if info:
        try:
            upd = datetime.strptime(info.get("updated", ""), "%Y-%m-%d").date()
            if (date.today() - upd).days <= 3:
                flags.append(f"⚠️ Their payment details were changed {info['updated']} — double-check before sending.")
        except ValueError:
            pass
    lines = [f"💸 **Payment packet — {seller}**",
             f"Amount: **${float(amount):,.2f}** · Method: **{method}**",
             f"Memo: KTS payout — {seller} — {date.today().isoformat()}"]
    if note:
        lines.append(f"Note: {note}")
    if info:
        lines.append(f"── payment details on file (updated {info.get('updated','?')}) ──")
        lines.append(info.get("details", ""))
    else:
        flags.append(f"⚠️ No payment details on file — `!payinfo set {key} <their wire/ACH info>` for next time.")
    lines += flags
    lines.append("_Review at the bank before sending — packets are prep, not payment._")
    return "\n".join(lines)

def mark_paid(seller, amount=None):
    """Mark the OLDEST matching unpaid entry paid. Returns (rec, remaining_unpaid_for_seller)."""
    key = (seller or "").lower()
    match = [o for o in OWED_STORE
             if o.get("status") != "paid" and str(o.get("discord", "")).lower() == key
             and (amount is None or abs(float(o.get("amount", 0)) - float(amount)) < 0.005)]
    if not match:
        return None, [o for o in OWED_STORE
                      if o.get("status") != "paid" and str(o.get("discord", "")).lower() == key]
    match.sort(key=lambda o: o.get("date", ""))
    rec = match[0]
    rec["status"] = "paid"
    rec["date_paid"] = date.today().isoformat()
    _save_owed()
    rest = [o for o in OWED_STORE
            if o.get("status") != "paid" and str(o.get("discord", "")).lower() == key]
    return rec, rest

def build_payout_digest():
    """Everything unpaid, oldest first (first-come-first-serve), with aging and
    missing-payinfo flags. None if the queue is empty."""
    unpaid = [o for o in OWED_STORE if o.get("status") != "paid"]
    if not unpaid:
        return None
    unpaid.sort(key=lambda o: o.get("date", ""))
    total = sum(float(o.get("amount", 0)) for o in unpaid)
    lines = [f"🗓️ **Payout day — {len(unpaid)} in the queue, ${total:,.2f} total** (oldest first):"]
    for o in unpaid:
        key = str(o.get("discord", "")).lower()
        age = ""
        try:
            days = (date.today() - datetime.strptime(o.get("date", ""), "%Y-%m-%d").date()).days
            age = f" · {days}d" + (" ⚠️ AGING" if days > 7 else "")
        except ValueError:
            pass
        pin = "" if key in PAYINFO else " · ❓ no payment info on file"
        lines.append(f"• **{o.get('discord')}** — ${float(o.get('amount',0)):,.2f} {o.get('method','')}"
                     f"{age}{pin}")
    lines.append("`!payinfo <name>` for details · `!paid <name> [amount]` after each send.")
    return "\n".join(lines)

_PAYOUT_DIGEST_MARKER = os.path.join(DATA_DIR, "payout_digest_last.txt")

async def payout_digest_loop():
    """DM the payout digest once on Friday mornings (~8am Central) — payouts
    are Fridays only (Kevin, Aug 28). Marker survives redeploys."""
    from zoneinfo import ZoneInfo
    tz = ZoneInfo("America/Chicago")
    while True:
        try:
            now = datetime.now(tz)
            today = now.date().isoformat()
            last = ""
            try:
                with open(_PAYOUT_DIGEST_MARKER) as f:
                    last = f.read().strip()
            except Exception:
                pass
            if now.weekday() == 4 and now.hour >= 8 and last != today:
                digest = build_payout_digest()
                if digest is None or await ping_kevin(digest):
                    try:
                        with open(_PAYOUT_DIGEST_MARKER, "w") as f:
                            f.write(today)
                    except Exception:
                        pass
        except Exception as e:
            print(f"payout digest loop error: {e}")
        await asyncio.sleep(1800)



def _drive_find_sheet(lookup_names, created_after=None):
    """Sync helper: search the PSA folder for a buying sheet whose name STARTS WITH
    one of the lookup names (the Apps Script names sheets '<username> <source> <date>',
    e.g. 'cwilk_sportscards. discord 7-13'). Exact-prefix match only — Drive's
    `contains` is a loose token match, so a short username like 'mark' would happily
    return markandjheyson's sheet and we'd write tracking to the wrong customer.
    created_after (tz-aware datetime): skip sheets older than it — a repeat seller's
    sheet from a PREVIOUS deal predates the current ticket channel and must never be
    matched (we'd write this deal's tracking onto their old completed sheet).
    Returns a sheet id or None. Blocking network call — run via asyncio.to_thread."""
    drive = get_drive_service()
    for lookup in lookup_names:
        lookup = (lookup or "").strip().lower()
        if not lookup:
            continue
        try:
            results = drive.files().list(
                q=f"'{PSA_FOLDER_ID}' in parents and name contains '{lookup}' and trashed=false",
                fields="files(id,name,createdTime)",
                orderBy="createdTime desc",
                pageSize=10,
            ).execute()
        except Exception as e:
            print(f"Drive lookup error for '{lookup}': {e}")
            continue
        for f in results.get("files", []):
            fname = (f.get("name") or "").lower()
            if not (fname == lookup or fname.startswith(lookup + " ")):
                continue
            if _is_summary_sheet_name(f.get("name")):
                continue   # '<user> — summary (KTS Collectibles)' is not a buying sheet
            if created_after is not None:
                try:
                    ct = datetime.fromisoformat(
                        (f.get("createdTime") or "").replace("Z", "+00:00"))
                    if ct < created_after:
                        continue   # older than this ticket — a previous deal's sheet
                except ValueError:
                    pass   # unparseable timestamp: don't block on the guard
            return f["id"]
    return None

async def find_sheet_for_channel(channel_id, channel_name, username, channel_created_at=None):
    """Resolve the buying sheet for a ticket: channel_sheet cache first, then an
    exact-prefix Drive search by the channel name (minus any legacy 'ticket-'
    prefix — sheets are named from the bare username, never with the prefix) and
    by the author's username (covers characters Discord strips from channel names,
    like the trailing period in 'cwilk_sportscards.'). Pass the ticket channel's
    created_at so a repeat customer's sheet from a previous deal (older than this
    ticket) can never match. Never guesses: returns None rather than someone
    else's — or some other deal's — sheet."""
    sheet_id = channel_sheet.get(channel_id)
    if sheet_id:
        return sheet_id
    bare = re.sub(r'^ticket-?', '', (channel_name or '').lower())
    sheet_id = await asyncio.to_thread(_drive_find_sheet, [bare, username], channel_created_at)
    if sheet_id:
        remember_channel_sheet(channel_id, sheet_id)
        print(f"Found sheet for #{channel_name} via Drive lookup: {sheet_id}")
    return sheet_id

def ticket_customer_from_overwrites(channel):
    """The ticket opener's RAW username from the channel's member permission
    overwrites — Discord's own record, no network call. on_guild_channel_create
    names the channel _sanitize_channel_name(opener.name) off this same overwrite,
    and sheets are named from message.author.name, the same raw string
    ('.yungturtle' -> #yungturtle). A legacy 'ticket-<name>' channel also matches
    on the bare name; an un-renamed 'ticket-0712' falls back to its sole non-bot,
    non-Kevin member overwrite (how on_guild_channel_create picks the opener).
    Anything else — no match, or two members sanitizing alike — returns None."""
    try:
        members = [t for t in channel.overwrites
                   if isinstance(t, discord.Member) and not t.bot
                   and t.id != YOUR_DISCORD_USER_ID and t.name]
    except Exception:
        return None
    cname = (getattr(channel, 'name', '') or '').lower()
    names = {cname}
    if cname.startswith('ticket-'):
        names.add(cname[len('ticket-'):])
    hits = {t.name for t in members if _sanitize_channel_name(t.name) in names}
    if len(hits) == 1:
        return hits.pop()
    if not hits and cname.startswith('ticket-') and len({t.name for t in members}) == 1:
        return members[0].name
    return None

def _drive_customer_username(channel_id, channel_name, days=4, created_after=None):
    """Sync helper (run via asyncio.to_thread): FALLBACK for when the channel's
    overwrites can't name the customer (ticket_customer_from_overwrites). The RAW
    Discord username read off their buying-sheet names ('<username> discord <date>').
      1. The channel_sheet cache (a sheet created in this ticket), if its name
         maps back to this channel — a sheet from certs KEVIN pasted is named
         after him and is skipped.
      2. Otherwise every PSA-folder sheet from the last `days` days — and not
         from before this ticket opened (created_after) — whose name prefix
         sanitizes to the channel name.
    Two different raw usernames that sanitize alike ('cwilk.' vs 'cwilk'), or no
    match at all, return None."""
    cname = (channel_name or '').lower()
    targets = {_sanitize_channel_name(cname)}
    if cname.startswith('ticket-'):   # legacy prefix only — not 'ticketmaster99'
        targets.add(_sanitize_channel_name(cname[len('ticket-'):]))
    targets -= {''}

    def _owner(fname):
        fname = fname or ''
        if ' discord' not in fname or _is_summary_sheet_name(fname):
            return None
        raw = fname.split(' discord')[0].strip()
        return raw if raw and _sanitize_channel_name(raw) in targets else None

    drive = get_drive_service()
    sid = channel_sheet.get(channel_id)
    if sid:
        try:
            raw = _owner(drive.files().get(fileId=sid, fields='name').execute().get('name'))
            if raw:
                return raw
        except Exception as e:
            print(f"customer lookup: cached sheet {sid} unreadable ({e}) — scanning Drive")
    from datetime import timedelta as _td
    since_dt = datetime.utcnow() - _td(days=days)
    if created_after is not None:
        ca = (created_after.astimezone(timezone.utc).replace(tzinfo=None)
              if created_after.tzinfo else created_after)
        since_dt = max(since_dt, ca - _td(minutes=10))
    since = since_dt.strftime('%Y-%m-%dT%H:%M:%S')
    found, token = set(), None
    for _ in range(10):   # paginate; Drive caps pageSize at 1000
        kw = {'pageToken': token} if token else {}
        resp = drive.files().list(
            q=f"'{PSA_FOLDER_ID}' in parents and trashed=false and createdTime > '{since}'",
            pageSize=1000, fields='nextPageToken,files(id,name)', **kw).execute()
        found.update(o for o in (_owner(f.get('name')) for f in resp.get('files', [])) if o)
        token = resp.get('nextPageToken')
        if not token:
            break
    if len(found) == 1:
        return found.pop()
    if found:
        print(f"customer lookup: #{channel_name} matches {sorted(found)} — ambiguous, not guessing")
    return None

async def customer_username_for_channel(channel, days=4, errors=None):
    """Drive fallback for the ticket's customer (raw username, e.g. '.yungturtle'),
    or None if it can't be pinned to exactly one. Bounded by the channel's
    created_at so a sheet from before this ticket never counts. Never raises; a
    Drive failure is appended to `errors` (if given) so the caller can say so."""
    try:
        return await asyncio.to_thread(_drive_customer_username, channel.id, channel.name,
                                       days, getattr(channel, 'created_at', None))
    except Exception as e:
        print(f"customer lookup failed for #{getattr(channel, 'name', '?')}: {e}")
        if errors is not None:
            errors.append(f"{type(e).__name__}: {str(e)[:80]}")
        return None

# ── CUSTOMER SUMMARY SHEET (!summary — Kevin only, inside the ticket) ─────────────
# Oct 18: caspar asked for ONE organized sheet of everything he sent. Every cert
# submission makes a NEW buying sheet and customers re-send certs across sheets,
# so a weekend ends as a pile of overlapping sheets. !summary merges this deal's
# buying sheets (the customer's since the ticket opened, at most SUMMARY_MAX_DAYS
# back, plus the sheets Kevin's own cert pastes made in this ticket) into one
# spreadsheet, posts its link in the ticket, and keeps it fresh: after every
# later quote in the ticket and every SUMMARY_TICK_S while it's active.
# CUSTOMER-VISIBLE: it only ever carries the customer's own card data plus fixed
# customer-safe strings — never the resale buyers, allocations, margins or resale.
SUMMARY_NAME_SUFFIX = " — summary (KTS Collectibles)"
# The Apps Script creator always names its sheet "<seed> discord <M-DD>". The
# seed has spaces, which a Discord username can't, so nothing keyed on
# '<customer> discord' (lot rebuild, ship lookup, customer scan) can take the
# not-yet-renamed file for a buying sheet; the sweeps skip both name shapes.
SUMMARY_SEED_PREFIX = "KTS summary "
# FOLDER — summaries never sit among the buying sheets: Kevin's nightly
# paying-vs-sending reconcile (and other sweeps) read EVERY spreadsheet directly
# in the PSA folder, keep each cert's latest occurrence and read column F as the
# sport — on a summary F is the rate, so every summarized cert would show as
# "on Pixel but not being paid for". Summaries go in SUMMARY_FOLDER_ID (env) if
# set, else in a 'KTS customer summaries' folder INSIDE the PSA folder (found by
# name; made once if missing). Sweeps of the PSA folder's direct children never
# see it.
SUMMARY_FOLDER_ID = (os.environ.get("SUMMARY_FOLDER_ID") or "").strip()
SUMMARY_FOLDER_NAME = "KTS customer summaries"
# LINK SHARING — Kevin's call (asked Oct 18). True: whenever the link is POSTED
# in the ticket (never before the sheet is fully written), that one Drive file
# is checked and, if needed, given "anyone with the link can VIEW" (permission
# type 'anyone', role 'reader') so the customer opens it without an access
# request. False: the bot never adds sharing (the customer clicks "request
# access" like on buying sheets) — but it also never REMOVES an 'anyone'
# permission a summary already has. Never touches the sharing of any other file.
SUMMARY_LINK_SHARING = True
SUMMARY_MAX_DAYS = 4               # same window as rebuild_lot_from_sheets: this deal, not last weekend's
SUMMARY_SKEW_MIN = 10              # slack before the ticket's created_at (clock skew)
SUMMARY_STALE_SALE_DAYS = 60       # last sale older than this -> "no sales" reason
SUMMARY_AUTO_REFRESH_DELAY_S = 90  # coalesce a burst of quotes into one refresh
SUMMARY_READ_PAUSE_S = 2.0         # between real sheet reads (shared 60 reads/min quota)
SUMMARY_BACKOFF_S = (15, 30, 45, 60, 60)   # 429/5xx back-off (Sheets quota resets per minute)
SUMMARY_HTTP_TIMEOUT_S = 60        # a stalled socket raises instead of holding the ticket's lock
SUMMARY_LOCK_WAIT_S = 300          # !summary gives up waiting for a running build after this
SUMMARY_TICK_S = 900               # background refresh of active summaries (team edits, hand prices)
SUMMARY_TICK_MAX_H = 72            # ...for this long after Kevin's last !summary
SUMMARY_FORBIDDEN_WORDS = ('triumph', 'poketrader', 'pixel', 'allocation',
                           'margin', 'resale', 'resell')
# Column-M notes are copied onto the customer's sheet ONLY when they are one of
# the team's customer-safe templates (apply_triumph_to_sheet.py REASONS / NO_FULL).
# Anything else — internal wording, app reasons, buyer names — reads "❌ Not
# buying this weekend".
SUMMARY_SAFE_NOTES = (
    "❌ NO — our limit for this range is full this weekend",
    "❌ NO — not enough reliable sales data to price it",
    "❌ NO — this cert was already submitted (duplicate)",
    "❌ NO — we're not buying this sport in this price range this weekend",
)
_SUMMARY_SAFE_NOTE_RE = re.compile(
    r"❌ NO — our (?:One Piece|Pokémon|Pokemon|sports|Sports) limit is full this weekend")
SUMMARY_CHANNEL_PROP = "kts_ticket_channel"   # Drive appProperties (private to the bot's app)
SUMMARY_POSTED_PROP = "kts_posted"            # '1' once the link went into the ticket
SUMMARY_EXTRA_PROP = "kts_x"                  # kts_x1.. = Kevin-pasted sheets a build included
SUMMARY_EXTRA_MAX = 25
SUMMARY_META_FIELDS = ("id,name,trashed,createdTime,modifiedTime,parents,appProperties,"
                       "lastModifyingUser(me)")
summary_sheets = {}             # channel_id -> {"sid", "customer", "channel", "posted", ...}
_summary_none_checked = set()   # channels whose clean Drive lookup found no summary
_summary_refresh_pending = set()
_summary_cmd_running = set()    # channels with a !summary in progress
_summary_bg_tasks = set()       # strong refs: un-referenced asyncio tasks can be GC'd
_summary_locks = {}             # channel_id -> (loop, asyncio.Lock)
_summary_global = {}            # 'sem' -> (loop, asyncio.Semaphore(1)): one build bot-wide
_summary_folder_cache = {}
from collections import OrderedDict as _OrderedDict
_summary_rows_cache = _OrderedDict()   # (sheet id, Drive modifiedTime) -> rows
SUMMARY_ROWS_CACHE_MAX = 500
_SUMMARY_TICKER = {}            # 'task' -> the background ticker
_SUMMARY_EPOCH = date(1899, 12, 30)   # Sheets date serial 0
_SUMMARY_TRANSIENT_RE = re.compile(
    r"\b(429|500|502|503|504)\b|quota|rate ?limit|ratelimit|backenderror|"
    r"internal error|timed out|timeout|temporarily unavailable", re.I)

def _is_summary_sheet_name(name):
    """A customer summary sheet (or its pre-rename seed) — never a buying sheet."""
    n = (name or '').strip()
    return n.endswith(SUMMARY_NAME_SUFFIX) or n.startswith(SUMMARY_SEED_PREFIX)

def _summary_q(s):
    """Escape a value for a Drive query string literal."""
    return (s or '').replace('\\', '\\\\').replace("'", "\\'")

def _summary_url(sid):
    return f"https://docs.google.com/spreadsheets/d/{sid}/edit"

def _summary_retry(fn, what=""):
    """Run a blocking Drive/Sheets call, backing off on 429 / quota / 5xx / timeout.
    Anything else (404, permission, bad request) raises straight away."""
    waits = list(SUMMARY_BACKOFF_S)
    while True:
        try:
            return fn()
        except Exception as e:
            if not waits or not _SUMMARY_TRANSIENT_RE.search(f"{type(e).__name__} {e}"):
                raise
            w = waits.pop(0)
            print(f"summary: {what} — {str(e)[:80]} — backing off {w}s")
            time.sleep(w)

def _summary_set_timeout(gc):
    """gspread's HTTP client has NO timeout by default; give the summary's one."""
    for obj in (getattr(gc, 'http_client', None), gc):
        fn = getattr(obj, 'set_timeout', None)
        if callable(fn):
            try:
                fn(SUMMARY_HTTP_TIMEOUT_S)
                return
            except Exception:
                pass

def _summary_et(dt):
    """datetime (aware, or naive UTC) -> US Eastern: 'Sent' days are Kevin's days."""
    from datetime import timedelta as _td
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        return dt.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        return dt.astimezone(timezone(_td(hours=-4)))   # no tzdata: EDT

def _summary_today():
    return _summary_et(datetime.now(timezone.utc)).date()

def _summary_stamp():
    """'Sun 10/18, 3:42 PM ET' — row 3 of the sheet."""
    t = _summary_et(datetime.now(timezone.utc))
    return f"{t:%a} {t.month}/{t.day}, {t.hour % 12 or 12}:{t:%M} {'AM' if t.hour < 12 else 'PM'} ET"

def _summary_day(created_iso):
    """Drive createdTime ('2026-10-17T15:04:05.000Z') -> its Eastern date."""
    try:
        dt = datetime.strptime((created_iso or '')[:19], '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        return _summary_today()
    return _summary_et(dt).date()

def _summary_num(v):
    """A sheet cell as a number, or None ('not found', "didn't load — resend",
    blank, '#VALUE!'). Accepts '$1,234.50'."""
    import math
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        f = float(v)
    else:
        s = re.sub(r'[$,\s]', '', str(v or ''))
        if not s:
            return None
        try:
            f = float(s)
        except ValueError:
            return None
    return f if math.isfinite(f) else None

def _summary_sale_date(v):
    """Column J (last sale): a Sheets date serial, '2026-08-01', or '8/1/2026'."""
    from datetime import timedelta as _td
    if v is None or isinstance(v, bool) or str(v).strip() == '':
        return None
    try:
        f = float(v)
        return _SUMMARY_EPOCH + _td(days=int(f)) if 20000 < f < 80000 else None
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    for fmt, part in (('%Y-%m-%d', s[:10]), ('%m/%d/%Y', s.split()[0]), ('%m/%d/%y', s.split()[0])):
        try:
            return datetime.strptime(part, fmt).date()
        except ValueError:
            continue
    return None

def _summary_cert(v):
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    c = str(v if v is not None else '').strip()
    if c.endswith('.0'):
        c = c[:-2]
    return c if c.isdigit() and len(c) >= 6 else None

def _summary_row_is_cgc(r):
    """A buying-sheet row for a CGC slab (A="CGC", grade "CGC 10", or a
    CGC-length cert) — see _sheet_cert_token."""
    return is_cgc_cert(_sheet_cert_token(r[0], _summary_cert(r[1]) or '', r[4]))

def _summary_text(v):
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    return str(v if v is not None else '').strip()

def _summary_customer_safe(text):
    t = (text or '').lower()
    return not any(w in t for w in SUMMARY_FORBIDDEN_WORDS)

def _summary_safe_note(note):
    """The team's column-M note, if it is one of the customer-safe templates."""
    t = re.sub(r'\s+', ' ', _summary_text(note)).replace('’', "'")
    t = t.replace(' - ', ' — ').replace(' – ', ' — ')
    if (t in SUMMARY_SAFE_NOTES or _SUMMARY_SAFE_NOTE_RE.fullmatch(t)) and _summary_customer_safe(t):
        return t
    return None

def _summary_day_label(d):
    return f"{d:%a} {d.month}/{d.day}"

def _summary_range_label(d0, d1):
    if d0 == d1:
        return f"{d0:%b} {d0.day}"
    if (d0.year, d0.month) == (d1.year, d1.month):
        return f"{d0:%b} {d0.day}–{d1.day}"
    return f"{d0:%b} {d0.day}–{d1:%b} {d1.day}"

def _summary_classify(r):
    """One buying-sheet row (padded) -> what it says about the card:
      buy       payout I > 0 on a numeric comp G (Kevin's hand-typed I wins)
      buy_hand  payout I > 0 with no numeric comp ('Kevin will quote', 'agreed',
                'not found' + a hand price…) — priced by hand, comp left blank
      decline   numeric comp with an explicit 0 (H or I), or a ❌ note in M
      dup       a decline whose note says the cert was already submitted
      unpriced  nothing decided yet: comp blank / didn't load / not found /
                'Kevin will quote' with no payout
    A broken I ('#REF!' / blank — a number typed into the ARRAYFORMULA column
    breaks the whole array) under a live rate 0 < H <= 1 is priced as G × H, the
    value the formula would show, and flagged (pay_err) for Kevin."""
    comp, pay, rate = _summary_num(r[6]), _summary_num(r[8]), _summary_num(r[7])
    note = _summary_text(r[12])
    out = {'kind': 'unpriced', 'comp': comp, 'pay': 0.0, 'rate': 0, 'pay_err': False}
    if pay is not None and pay > 0:
        if comp is not None and comp > 0:
            out.update(kind='buy', pay=round(pay, 2), rate=round(pay / comp, 4))
        else:
            out.update(kind='buy_hand', comp=None, pay=round(pay, 2), rate='')
    elif pay is None and comp is not None and comp > 0 and rate is not None and rate > 0:
        if rate <= 1:
            out.update(kind='buy', pay=round(comp * rate, 2), rate=round(rate, 4), pay_err=True)
        else:
            out['pay_err'] = True   # rate isn't a fraction — don't guess a payout
    elif comp is not None and (pay == 0 or rate == 0):
        out['kind'] = 'decline'
    elif note.startswith('❌'):
        out['kind'] = 'decline'
    if out['kind'] == 'decline' and re.search(r'duplicate|already submitted', note, re.I):
        out['kind'] = 'dup'
    return out

def _summary_reason(r, k, today):
    """Customer-facing status for a card we're NOT (or not yet) buying."""
    g = str(r[6] if r[6] is not None else '').strip().lower()
    if k['kind'] == 'unpriced':
        if 'load' in g:
            return "⏳ Didn't load — resend it in the ticket"
        if 'kevin will quote' in g:      # manual-review card, not priced yet
            return "💎 Pricing this one by hand — we'll get back to you"
        if not g or k['comp'] is not None:
            return "⏳ Still pricing this one"
        return "❌ Not buying — no CardLadder comp"
    if _summary_row_is_cgc(r):
        # CGC has its own short rulebook (Pokémon $1-$100) and no sale-age gate.
        safe = _summary_safe_note(r[12])
        if safe:
            return safe
        sp = normalize_sport(_summary_text(r[5]))
        if sp and sp != 'pokemon':
            return "❌ Not buying — we only take Pokémon in CGC"
        if k['comp'] is not None and k['comp'] > CGC_MAX_PRICE:
            return f"❌ Not buying — over our ${CGC_MAX_PRICE:,} max for CGC slabs"
        if k['comp'] is not None and k['comp'] < CGC_MIN_PRICE:
            return f"❌ Not buying — under our ${CGC_MIN_PRICE} minimum for CGC slabs"
        if k['comp'] is None and g and 'load' not in g and 'kevin will quote' not in g:
            return "❌ Not buying — no CardLadder comp"
        return "❌ Not buying this weekend"
    sd = _summary_sale_date(r[9])
    if sd and (today - sd).days > SUMMARY_STALE_SALE_DAYS:
        return f"❌ Not buying — no sales in the last {SUMMARY_STALE_SALE_DAYS} days"
    safe = _summary_safe_note(r[12])
    if safe:
        return safe
    if k['comp'] is None and g and 'load' not in g and 'kevin will quote' not in g:
        return "❌ Not buying — no CardLadder comp"
    return "❌ Not buying this weekend"

def build_summary_data(customer, sheets, today=None, stamp=None):
    """Pure: merge buying-sheet rows into the summary layout.
    sheets: [{'createdTime': Drive iso, 'rows': sheet rows, header first,
    'name': optional}]. In createdTime order, the LATEST row that decided the
    card (buy / hand price / decline — see _summary_classify) wins per cert. A
    later row that decided nothing (a re-send whose comp didn't load, came back
    'not found', or isn't priced yet) never erases an earlier quote, and a $0
    'already submitted (duplicate)' decline on a re-sent copy never erases an
    earlier buy. 'Sent' is the first day the cert came in, with '(re-sent <Day>)'
    when it came again on a later day. Payout is that row's I value (Kevin's
    hand edits win), comp is G, rate = I/G. Buying rows first, then still-pricing,
    then not-buying (with a reason); only buying rows count in the totals."""
    import hashlib
    today = today or _summary_today()
    first_day, days_seen, hist = {}, {}, {}
    for s in sorted(sheets, key=lambda s: s.get('createdTime') or ''):
        day = _summary_day(s.get('createdTime'))
        for r in (s.get('rows') or [])[1:]:
            r = list(r) + [''] * 14
            c = _summary_cert(r[1])
            if not c:
                continue
            if _summary_row_is_cgc(r):
                c = cert_label(cgc_cert(c))   # 'CGC 6193949247': own key + Cert # cell
            first_day.setdefault(c, day)
            days_seen.setdefault(c, set()).add(day)
            hist.setdefault(c, []).append((r, _summary_classify(r), s.get('name') or ''))
    buy, pend, nobuy, zeroed, pay_err = [], [], [], [], []
    n_hand = 0
    for c, entries in hist.items():
        strong = [e for e in entries if e[1]['kind'] in ('buy', 'buy_hand', 'decline')]
        weak = [e for e in entries if e[1]['kind'] == 'dup']
        r, k, sname = (strong or weak or entries)[-1]
        if k['pay_err'] and sname and sname not in pay_err:
            pay_err.append(sname)
        if k['kind'] == 'decline' and any(e[1]['kind'] in ('buy', 'buy_hand') for e in strong[:-1]):
            zeroed.append(c)    # an earlier sheet was buying it; a later one says $0
        base = [first_day[c], c, _summary_text(r[3]), _summary_text(r[4]), k['comp']]
        if k['kind'] == 'buy':
            buy.append(base + [k['rate'], k['pay'], '✅ Buying'])
        elif k['kind'] == 'buy_hand':
            n_hand += 1
            buy.append(base[:4] + [None, '', k['pay'], '✅ Buying (priced by hand)'])
        else:
            why = _summary_reason(r, k, today)
            (pend if why[:1] in ('⏳', '💎') else nobuy).append(base + [0, 0.0, why])
    buy.sort(key=lambda x: (x[0], -x[6]))
    pend.sort(key=lambda x: (x[0], x[1]))
    nobuy.sort(key=lambda x: (x[0], -(x[4] or 0)))
    tot_pay = round(sum(x[6] for x in buy), 2)
    tot_comp = round(sum(x[4] or 0 for x in buy), 2)
    per_day = {}
    for x in buy:   # buy is day-sorted, so insertion order is chronological
        k = per_day.setdefault(x[0], [0, 0.0])
        k[0] += 1
        k[1] += x[6]
    listed = buy + pend + nobuy
    n = len(listed)
    rng = ''
    if first_day:
        d0 = min(first_day.values())
        d1 = max(max(s) for s in days_seen.values())
        rng = _summary_range_label(d0, d1)
    counts = (f"{n} cards sent · {len(buy)} buying"
              + (f" · {len(pend)} still pricing" if pend else '')
              + f" · {len(nobuy)} not buying")
    comp_label = ("Total comp (cards we're buying)" if not n_hand else
                  f"Total comp (cards we're buying, not counting {n_hand} priced by hand)")
    rows = [[f"{customer} — everything you sent us {rng}"],
            [counts, '', f"{comp_label} ${tot_comp:,.2f}", '', '',
             f"YOUR TOTAL PAYOUT ${tot_pay:,.2f}"],
            [''],
            ['Sent', 'Cert #', 'Card', 'Grade', 'Comp', 'Rate', 'You get', 'Status']]
    for x in listed:
        later = sorted(d for d in days_seen[x[1]] if d != x[0])
        sent = _summary_day_label(x[0]) + (f" (re-sent {later[-1]:%a})" if later else '')
        rows.append([sent, x[1], x[2], x[3], x[4] if x[4] is not None else '', x[5], x[6], x[7]])
    day_block = ([['Sent', 'Cards buying', 'Payout']]
                 + [[_summary_day_label(k), v[0], round(v[1], 2)] for k, v in per_day.items()]
                 + [['Total', len(buy), tot_pay]])
    digest = hashlib.sha1(json.dumps([rows, day_block], default=str).encode()).hexdigest()
    if stamp:
        rows[2] = [stamp]
    return {'customer': customer, 'range': rng, 'rows': rows, 'day_block': day_block,
            'n': n, 'n_buy': len(buy), 'n_pend': len(pend), 'n_nobuy': len(nobuy),
            'n_hand': n_hand, 'tot_comp': tot_comp, 'tot_pay': tot_pay, 'n_days': len(per_day),
            'statuses': [x[7] for x in listed], 'buy_certs': sorted(x[1] for x in buy),
            'zeroed': zeroed, 'pay_err_sheets': pay_err, 'hash': digest}

def _summary_formats(data):
    n = len(data['rows'])
    nd = data['n_days']
    grey = {'red': 0.9, 'green': 0.9, 'blue': 0.9}
    green = {"backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}}
    amber = {"backgroundColor": {"red": 0.99, "green": 0.95, "blue": 0.80}}
    red = {"backgroundColor": {"red": 0.98, "green": 0.86, "blue": 0.86}}
    cur = {'numberFormat': {'type': 'CURRENCY', 'pattern': '$#,##0.00'}}
    fm = [{'range': 'A1', 'format': {'textFormat': {'bold': True, 'fontSize': 14}}},
          {'range': 'A2:H2', 'format': {'textFormat': {'bold': True}}},
          {'range': 'F2', 'format': {'textFormat': {'bold': True, 'foregroundColor':
                                                    {'red': 0.0, 'green': 0.45, 'blue': 0.15}}}},
          {'range': 'A3', 'format': {'textFormat': {'italic': True, 'fontSize': 9, 'foregroundColor':
                                                    {'red': 0.4, 'green': 0.4, 'blue': 0.4}}}},
          {'range': 'A4:H4', 'format': {'textFormat': {'bold': True}, 'backgroundColor': grey}},
          {'range': 'J4:L4', 'format': {'textFormat': {'bold': True}, 'backgroundColor': grey}},
          {'range': f'J{5 + nd}:L{5 + nd}', 'format': {'textFormat': {'bold': True}}},
          {'range': f'E5:E{n}', 'format': cur},
          {'range': f'G5:G{n}', 'format': cur},
          {'range': f'F5:F{n}', 'format': {'numberFormat': {'type': 'PERCENT', 'pattern': '0%'}}},
          {'range': f'K5:K{5 + nd}', 'format': {'numberFormat': {'type': 'NUMBER', 'pattern': '0'}}},
          {'range': f'L5:L{5 + nd}', 'format': cur},
          {'range': f'B5:B{n}', 'format': {'numberFormat': {'type': 'TEXT'}}},
          {'range': f'H5:H{n}', 'format': {'horizontalAlignment': 'LEFT'}}]
    for i, st in enumerate(data['statuses']):
        fm.append({'range': f'H{5 + i}',
                   'format': green if st.startswith('✅') else amber if st[:1] in ('⏳', '💎') else red})
    return fm

SUMMARY_COL_WIDTHS = [180, 95, 470, 55, 95, 70, 95, 330, 20, 110, 95, 110]

def _summary_write(ss, data):
    """Rebuild the summary on a BRAND-NEW tab, then drop every older tab. A sheet
    from the Apps Script creator carries the buying template's conditional
    formats, data validation and colours — clearing values leaves all of that
    behind, a fresh worksheet carries none of it. The old tab stays intact until
    the new one is fully written, and ONE atomic batchUpdate then deletes the old
    tabs and gives the new tab its final name, frozen header and widths — a
    failure or redeploy at any step never leaves the customer a blank sheet or a
    tab named 'building …' (the next build sweeps any leftover tab)."""
    old = _summary_retry(lambda: ss.worksheets(), 'list tabs')
    titles = {getattr(w, 'title', '') for w in old}
    n = int(time.time() * 1000) % 10**9
    while f"building {n}" in titles:   # a leftover tab from a failed build
        n += 1
    tmp = f"building {n}"
    ws = _summary_retry(lambda: ss.add_worksheet(
        title=tmp, rows=max(40, len(data['rows']) + 8), cols=12), 'add tab')
    _summary_retry(lambda: ws.update(values=data['rows'], range_name='A1',
                                     value_input_option='RAW'), 'write rows')
    _summary_retry(lambda: ws.update(values=data['day_block'], range_name='J4',
                                     value_input_option='RAW'), 'write per-day block')
    _summary_retry(lambda: ws.batch_format(_summary_formats(data)), 'format')
    reqs = [{'deleteSheet': {'sheetId': w.id}} for w in old if getattr(w, 'id', None) != ws.id]
    reqs.append({'updateSheetProperties': {
        'properties': {'sheetId': ws.id, 'title': data['range'] or 'Summary',
                       'gridProperties': {'frozenRowCount': 4}},
        'fields': 'title,gridProperties.frozenRowCount'}})
    reqs += [{'updateDimensionProperties': {
        'range': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
        'properties': {'pixelSize': w}, 'fields': 'pixelSize'}}
        for i, w in enumerate(SUMMARY_COL_WIDTHS)]
    _summary_retry(lambda: ss.batch_update({'requests': reqs}), 'finish tab')
    return ws

def _summary_window(created_after, max_days=SUMMARY_MAX_DAYS):
    """(list_since, window_since) Drive createdTime floors. window_since: the
    ticket's open time minus clock-skew slack, but never more than max_days back
    (a repeat seller's long-open ticket must not merge last weekend's deal, which
    is usually shipped and paid). list_since reaches back to the ticket's open
    time, so sheets left OUT by the day cap are counted and Kevin is told."""
    from datetime import timedelta as _td
    cap = datetime.utcnow() - _td(days=max_days)
    if created_after is None:
        s = cap.strftime('%Y-%m-%dT%H:%M:%S')
        return s, s
    ca = (created_after.astimezone(timezone.utc).replace(tzinfo=None)
          if created_after.tzinfo else created_after)
    floor = ca - _td(minutes=SUMMARY_SKEW_MIN)
    return floor.strftime('%Y-%m-%dT%H:%M:%S'), max(floor, cap).strftime('%Y-%m-%dT%H:%M:%S')

def _summary_folder(drive, create=False):
    """The Drive folder summaries live in (never the PSA folder itself)."""
    if SUMMARY_FOLDER_ID:
        return SUMMARY_FOLDER_ID
    if _summary_folder_cache.get('id'):
        return _summary_folder_cache['id']
    q = (f"name = '{_summary_q(SUMMARY_FOLDER_NAME)}' and "
         f"mimeType = 'application/vnd.google-apps.folder' and "
         f"'{PSA_FOLDER_ID}' in parents and trashed=false")
    files = _summary_retry(lambda: drive.files().list(
        q=q, orderBy='createdTime', pageSize=10, fields='files(id,name,createdTime)').execute(),
        'find summary folder').get('files', [])
    if files:
        _summary_folder_cache['id'] = files[0]['id']
        return files[0]['id']
    if not create:
        return None
    try:
        f = _summary_retry(lambda: drive.files().create(
            body={'name': SUMMARY_FOLDER_NAME, 'mimeType': 'application/vnd.google-apps.folder',
                  'parents': [PSA_FOLDER_ID]}, fields='id').execute(), 'make summary folder')
    except Exception as e:
        raise RuntimeError(f"couldn't make the '{SUMMARY_FOLDER_NAME}' Drive folder "
                           f"({str(e)[:80]}) — make a folder with that name inside the PSA "
                           f"Slabs folder (or set SUMMARY_FOLDER_ID) and run !summary again")
    _summary_folder_cache['id'] = f['id']
    return f['id']

def _summary_meta(drive, sid):
    """The summary file's metadata, or None if it was trashed or deleted."""
    try:
        meta = _summary_retry(lambda: drive.files().get(
            fileId=sid, fields=SUMMARY_META_FIELDS).execute(), 'check summary')
    except Exception as e:
        if re.search(r"\b404\b|not ?found", str(e), re.I):
            return None
        raise
    return None if meta.get('trashed') else meta

def _summary_targets(channel_name):
    cname = (channel_name or '').lower()
    targets = {_sanitize_channel_name(cname)}
    if cname.startswith('ticket-'):
        targets.add(_sanitize_channel_name(cname[len('ticket-'):]))
    return targets - {''}

def _summary_find_existing(drive, folder, channel_id, channel_name, customer, created_after,
                           adopt_seed=False):
    """This ticket's summary in the summaries folder, created since the ticket
    opened (a repeat seller's summary from an earlier ticket is never reused):
    '<customer> — summary (KTS Collectibles)', or with no customer one whose
    owner sanitizes to the channel name. A file tagged (appProperties) for THIS
    channel wins; one tagged for another ticket (same seller, two tickets open)
    is never adopted; untagged ones only when they are unambiguous. adopt_seed:
    also take an orphaned 'KTS summary <customer> discord …' seed (a redeploy or
    a failed rename between the creator call and the rename).
    Returns (file id, owner username, metadata, is_seed) — newest — or None."""
    if not folder:
        return None
    since = _summary_window(created_after)[0]
    files, token = [], None
    for _ in range(10):   # paginate
        kw = {'pageToken': token} if token else {}
        resp = _summary_retry(lambda: drive.files().list(
            q=f"'{folder}' in parents and trashed=false and createdTime > '{since}'",
            orderBy='createdTime desc', pageSize=100,
            fields=f'nextPageToken,files({SUMMARY_META_FIELDS})', **kw).execute(), 'find summary')
        files += resp.get('files', [])
        token = resp.get('nextPageToken')
        if not token:
            break
    targets = _summary_targets(channel_name)
    mine, untagged, seeds = [], [], []
    for f in sorted(files, key=lambda f: f.get('createdTime') or '', reverse=True):
        name = f.get('name') or ''
        if name.endswith(SUMMARY_NAME_SUFFIX):
            owner = name[:-len(SUMMARY_NAME_SUFFIX)]
            if not ((owner == customer) if customer else (_sanitize_channel_name(owner) in targets)):
                continue
            tag = (f.get('appProperties') or {}).get(SUMMARY_CHANNEL_PROP)
            if tag is None:
                untagged.append((f['id'], owner, f, False))
            elif tag == str(channel_id):
                mine.append((f['id'], owner, f, False))
            # tagged for another ticket: never this one's
        elif adopt_seed and customer and name.startswith(f"{SUMMARY_SEED_PREFIX}{customer} discord"):
            seeds.append((f['id'], customer, f, True))
    if mine:
        return mine[0]
    if untagged:
        if len({o for _, o, _, _ in untagged}) == 1:
            return untagged[0]
        print(f"summary lookup: #{channel_name} matches {sorted({o for _, o, _, _ in untagged})} — ambiguous")
        return None
    return seeds[0] if seeds else None

def _summary_finalize(drive, sid, customer, channel_id, folder, parents=None):
    """Rename a fresh (or orphaned) seed to '<customer> — summary (KTS
    Collectibles)' — no ' discord ' in it, so rebuild_lot_from_sheets,
    requote_stalled_tickets and the other sweeps ignore it — tag it with its
    ticket's channel id (best-effort), and if the creator didn't put it in the
    summaries folder, move it there in the same call."""
    title = customer + SUMMARY_NAME_SUFFIX
    kw = {}
    if folder and parents is not None and folder not in parents:
        kw = {'addParents': folder}
        if parents:
            kw['removeParents'] = ','.join(parents)
    tagged = {'name': title, 'appProperties': {SUMMARY_CHANNEL_PROP: str(channel_id)}}
    try:
        try:
            _summary_retry(lambda: drive.files().update(
                fileId=sid, body=tagged, fields='id,name,parents', **kw).execute(), 'rename summary')
        except Exception as e:
            print(f"summary {sid}: rename+tag failed ({str(e)[:80]}) — renaming without the tag")
            _summary_retry(lambda: drive.files().update(
                fileId=sid, body={'name': title}, fields='id,name,parents', **kw).execute(),
                'rename summary')
    except Exception as e:
        if kw:   # still sitting among the buying sheets
            raise RuntimeError(f"new summary is stuck in the PSA Slabs folder — move it OUT by hand "
                               f"(tonight's reconcile reads that folder) and rename it '{title}': "
                               f"{_summary_url(sid)} ({str(e)[:60]})")
        raise RuntimeError(f"new summary couldn't be renamed — rename it '{title}' by hand: "
                           f"{_summary_url(sid)} ({str(e)[:60]})")

def _summary_create(drive, customer, channel_id, folder):
    """New summary spreadsheet via the same Apps Script creator as buying sheets
    (the service account's own Drive is quota-full), made in the summaries
    folder, then renamed + tagged. Returns the file id."""
    url, _name, data = create_psa_sheet(SUMMARY_SEED_PREFIX + customer, ["00000000"], folder_id=folder)
    sid = (data or {}).get("sheet_id") or extract_sheet_id(url)
    if not sid:
        raise RuntimeError("sheet creator returned no sheet id")
    parents = _summary_retry(lambda: drive.files().get(
        fileId=sid, fields='id,parents').execute(), 'check summary folder').get('parents')
    _summary_finalize(drive, sid, customer, channel_id, folder, parents or [])
    return sid

def _summary_is_cached(f):
    return bool(f.get('modifiedTime')) and (f['id'], f['modifiedTime']) in _summary_rows_cache

def _summary_read_rows(gc, f):
    """A buying sheet's first-tab rows (A:M, raw values: numbers stay numbers, J
    dates come back as serials) in ONE Sheets read — or none at all when Drive's
    modifiedTime says the sheet hasn't changed since we last read it."""
    key = (f['id'], f.get('modifiedTime') or '')
    if key[1] and key in _summary_rows_cache:
        _summary_rows_cache.move_to_end(key)
        return _summary_rows_cache[key]
    params = {'valueRenderOption': 'UNFORMATTED_VALUE'}
    def _get():
        hc = getattr(gc, 'http_client', None)
        if hc is not None and hasattr(hc, 'values_get'):
            return hc.values_get(f['id'], 'A1:M', params=params)   # no tab name = first tab
        return gc.open_by_key(f['id']).values_get('A1:M', params=params)
    rows = (_summary_retry(_get, f"read {f.get('name')}") or {}).get('values', [])
    if key[1]:
        for k in [k for k in _summary_rows_cache if k[0] == key[0]]:
            del _summary_rows_cache[k]
        _summary_rows_cache[key] = rows
        while len(_summary_rows_cache) > SUMMARY_ROWS_CACHE_MAX:
            _summary_rows_cache.popitem(last=False)
    return rows

def _summary_owner(name):
    return (name or '').split(' discord')[0].strip()

def _summary_collect(drive, gc, customer, created_after, trusted_ids=(), cached_id=None):
    """This deal's buying sheets, oldest first, with their rows:
      - every '<customer> discord …' sheet (exact owner) inside the window;
      - trusted_ids: sheets Kevin's cert pastes made in THIS ticket (the
        cert-path history, plus the ones earlier builds recorded on the summary
        file itself so they survive a redeploy);
      - cached_id (channel_sheet) only if it is the customer's own sheet (or
        one of the trusted ones). The cache is also written by Drive lookups
        (!requoteall maps 'cwilk.' sheets to #cwilk; older code cached Kevin's
        newest sheet from a tracking-number lookup), so a sheet named after
        anyone else — Kevin included — needs the cert-path proof above.
    Any other sheet is never merged onto the customer's sheet (it is reported). Unchanged sheets
    are served from the (id, modifiedTime) cache. A sheet that can't be read
    raises: a customer-visible total must never silently miss a sheet."""
    list_since, win_since = _summary_window(created_after)
    safe = _summary_q(customer)
    files, token = [], None
    for _ in range(10):   # paginate
        kw = {'pageToken': token} if token else {}
        resp = _summary_retry(lambda: drive.files().list(
            q=(f"name contains '{safe} discord' and '{PSA_FOLDER_ID}' in parents "
               f"and trashed=false and createdTime > '{list_since}'"),
            orderBy='createdTime asc', pageSize=100,
            fields='nextPageToken,files(id,name,createdTime,modifiedTime)', **kw).execute(), 'list sheets')
        files += resp.get('files', [])
        token = resp.get('nextPageToken')
        if not token:
            break
    mine = [f for f in files
            if _summary_owner(f.get('name')) == customer and not _is_summary_sheet_name(f.get('name'))]
    use = [f for f in mine if (f.get('createdTime') or '')[:19] > win_since]
    older = [f for f in mine if (f.get('createdTime') or '')[:19] <= win_since]
    trusted = set(x for x in trusted_ids if x)
    skipped, extra_ids = [], []
    for xid in dict.fromkeys([*trusted_ids, cached_id]):
        if not xid or any(f['id'] == xid for f in mine):
            continue
        try:
            meta = _summary_retry(lambda: drive.files().get(
                fileId=xid, fields='id,name,createdTime,modifiedTime,trashed').execute(), 'ticket sheet')
        except Exception as e:
            print(f"summary: ticket sheet {xid} unreadable ({e}) — skipped")
            skipped.append(f"{xid[:10]}… (unreadable)")
            continue
        name = meta.get('name') or ''
        if meta.get('trashed') or _is_summary_sheet_name(name):
            continue
        owner = _summary_owner(name)
        if not (owner == customer or xid in trusted):
            skipped.append(name or xid)   # someone else's sheet cached on this ticket
            continue
        ct = meta.get('createdTime') or ''
        if ct and ct[:19] <= win_since:
            older.append(meta)
            continue
        use.append({'id': xid, 'name': name, 'createdTime': ct or '~',
                    'modifiedTime': meta.get('modifiedTime') or ''})
        if owner != customer:
            extra_ids.append(xid)
    use.sort(key=lambda f: f.get('createdTime') or '')
    out, read_any = [], False
    for f in use:
        if not _summary_is_cached(f):
            if read_any and SUMMARY_READ_PAUSE_S:
                time.sleep(SUMMARY_READ_PAUSE_S)
            read_any = True
        out.append({'id': f['id'], 'name': f.get('name', ''),
                    'createdTime': f.get('createdTime') or '~', 'rows': _summary_read_rows(gc, f)})
    oldest = min((f.get('createdTime') or '' for f in older), default='')
    return out, {'older': len(older), 'older_from': oldest[:10], 'skipped': skipped,
                 'extra_ids': extra_ids}

def _summary_build_sync(channel_id, channel_name, customer, created_after, *, known_sid=None,
                        allow_create=True, auto=False, cached_id=None,
                        prev_hash=None, prog=None):
    """Blocking (run via asyncio.to_thread): build or refresh this ticket's summary.
      1. Which file: known_sid (if not trashed) -> lookup in the summaries
         folder -> otherwise create one, only when allow_create (an auto-refresh
         never creates).
      2. auto: a summary someone edited by hand is left alone ('hand_edited'),
         and an unchanged result isn't rewritten ('unchanged').
      3. Read this deal's sheets, write a fresh tab. Sharing/posting is the
         caller's job (_summary_publish_sync), only after the write succeeded.
    prog (dict) gets 'sid' as soon as the file is known, so a failure after that
    point is reported. Returns {'status': 'ok' | 'unchanged' | 'hand_edited' |
    'no_summary' | 'no_customer' | 'no_sheets', ...}."""
    prog = prog if prog is not None else {}
    drive = get_drive_service()
    folder = _summary_folder(drive, create=False)
    sid, meta, adopted = None, None, False
    if known_sid:
        meta = _summary_meta(drive, known_sid)
        if meta:
            sid = known_sid
        else:
            print(f"summary {known_sid} for #{channel_name} is gone (trashed/deleted)")
    if sid is None:
        found = _summary_find_existing(drive, folder, channel_id, channel_name, customer,
                                       created_after, adopt_seed=allow_create)
        if found:
            sid, owner, meta, is_seed = found
            customer = customer or owner
            if is_seed:
                _summary_finalize(drive, sid, customer, channel_id, folder, meta.get('parents'))
                adopted = True
    if sid:
        prog.update(sid=sid, customer=customer)
    if sid is None and not allow_create:
        return {'status': 'no_summary'}
    if not customer:
        return {'status': 'no_customer'}
    props = (meta or {}).get('appProperties') or {}
    hand_edited = bool(meta) and not adopted and (meta.get('lastModifyingUser') or {}).get('me') is False
    base = {'sid': sid, 'customer': customer, 'url': _summary_url(sid) if sid else None,
            'posted_before': props.get(SUMMARY_POSTED_PROP) == '1', 'hand_edited': hand_edited}
    if auto and hand_edited:
        return {**base, 'status': 'hand_edited'}
    prev_extra = [props[k] for k in sorted(props, key=lambda k: (len(k), k))
                  if k.startswith(SUMMARY_EXTRA_PROP) and props[k]]
    trusted = list(dict.fromkeys(prev_extra + kevin_pasted_sheet_ids(channel_id)))
    gc = get_gspread_client()
    _summary_set_timeout(gc)
    sheets, info = _summary_collect(drive, gc, customer, created_after, trusted, cached_id)
    data = build_summary_data(customer, sheets, stamp=f"Last updated {_summary_stamp()}")
    base.update(n_sheets=len(sheets), sheet_names=[s['name'] for s in sheets], **info)
    if not data['n']:
        return {**base, 'status': 'no_sheets'}
    stats = {k: data[k] for k in ('n', 'n_buy', 'n_pend', 'n_nobuy', 'n_hand', 'tot_comp',
                                  'tot_pay', 'range', 'buy_certs', 'zeroed', 'pay_err_sheets', 'hash')}
    if auto and prev_hash and data['hash'] == prev_hash:
        return {**base, **stats, 'status': 'unchanged', 'created': False}
    created = False
    if sid is None:
        folder = folder or _summary_folder(drive, create=True)
        sid = _summary_create(drive, customer, channel_id, folder)
        created = True
        prog.update(sid=sid, customer=customer)
        base.update(sid=sid, url=_summary_url(sid))
    ss = _summary_retry(lambda: gc.open_by_key(sid), 'open summary')
    _summary_write(ss, data)
    extra = list(dict.fromkeys(prev_extra + info['extra_ids']))[-SUMMARY_EXTRA_MAX:]
    if extra != prev_extra:
        try:   # remembered on the file: survives a redeploy that wipes DATA_DIR
            _summary_retry(lambda: drive.files().update(fileId=sid, body={'appProperties': {
                f"{SUMMARY_EXTRA_PROP}{i + 1}": v for i, v in enumerate(extra)}},
                fields='id').execute(), 'record ticket sheets')
        except Exception as e:
            print(f"summary {sid}: couldn't record ticket sheets on the file ({str(e)[:80]})")
    return {**base, **stats, 'status': 'ok', 'created': created}

def _summary_publish_sync(sid):
    """Right before the link is posted: make sure the file's real sharing matches
    SUMMARY_LINK_SHARING (checked every time — a share that failed or never ran
    is retried). Returns {'link_open', 'shared_now', 'share_err'}."""
    drive = get_drive_service()
    out = {'link_open': False, 'shared_now': False, 'share_err': None}
    try:
        perms = _summary_retry(lambda: drive.permissions().list(
            fileId=sid, fields='permissions(id,type,role)').execute(), 'read sharing').get('permissions', [])
        out['link_open'] = any(p.get('type') == 'anyone' for p in perms)
        if not out['link_open'] and SUMMARY_LINK_SHARING:
            _summary_retry(lambda: drive.permissions().create(
                fileId=sid, body={'type': 'anyone', 'role': 'reader'}, fields='id').execute(),
                'share summary')
            out['link_open'] = out['shared_now'] = True
    except Exception as e:
        out['share_err'] = f"{type(e).__name__}: {str(e)[:80]}"
        print(f"summary {sid}: link sharing check failed ({e})")
    return out

def _summary_mark_posted_sync(sid):
    try:
        _summary_retry(lambda: get_drive_service().files().update(
            fileId=sid, body={'appProperties': {SUMMARY_POSTED_PROP: '1'}}, fields='id').execute(),
            'mark posted')
    except Exception as e:
        print(f"summary {sid}: couldn't mark as posted ({str(e)[:80]})")

def _summary_lock(channel_id):
    """One build at a time per ticket (a !summary and an auto-refresh racing
    would delete each other's fresh tab). Re-made if the event loop changed."""
    loop = asyncio.get_running_loop()
    ent = _summary_locks.get(channel_id)
    if ent is None or ent[0] is not loop:
        ent = (loop, asyncio.Lock())
        _summary_locks[channel_id] = ent
    return ent[1]

def _summary_global_sem():
    """One summary build at a time BOT-WIDE: they share the Sheets quota with
    every ticket's quote."""
    loop = asyncio.get_running_loop()
    ent = _summary_global.get('sem')
    if ent is None or ent[0] is not loop:
        ent = (loop, asyncio.Semaphore(1))
        _summary_global['sem'] = ent
    return ent[1]

async def _summary_acquire(channel_id, timeout=None):
    """Take the ticket's lock, then the bot-wide slot. Returns a release()
    callable, or None if `timeout` seconds passed first (nothing held)."""
    lk, sem = _summary_lock(channel_id), _summary_global_sem()
    async def both():
        await lk.acquire()
        try:
            await sem.acquire()
        except BaseException:
            lk.release()
            raise
    def release():
        sem.release()
        lk.release()
    if timeout is None:
        await both()
        return release
    task = asyncio.ensure_future(both())
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if task not in done:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return None
    task.result()
    return release

def _summary_register(channel_id, channel, res, confirmed=None, posted=None, requested=False):
    """Remember a built/found summary for this ticket (auto-refresh + ticker)."""
    ent = summary_sheets.get(channel_id)
    if ent is None or ent.get('sid') != res.get('sid'):
        ent = {}
    ent.update(sid=res['sid'], customer=res['customer'], channel=channel)
    for k in ('hash', 'buy_certs', 'tot_pay'):
        if res.get(k) is not None:
            ent[k] = res[k]
    ent['posted'] = bool(ent.get('posted') or res.get('posted_before') or posted)
    if confirmed is not None:
        ent['confirmed'] = bool(ent.get('confirmed') or confirmed)
    ent.setdefault('confirmed', ent['posted'])
    if requested or 'active_since' not in ent:
        ent['active_since'] = time.time()
    if requested:
        ent.pop('paused', None)
    summary_sheets[channel_id] = ent
    _summary_none_checked.discard(channel_id)
    return ent

def summary_customer_message(url, first, link_open, rng=''):
    """The ticket post (customer-visible) — fixed wording, nothing internal."""
    span = f" {rng}" if rng else ""
    head = (f"📊 Here's one sheet with everything you've sent us{span}: " if first
            else f"📊 Updated — here's one sheet with everything you've sent us{span}: ")
    lines = [head + url,
             "Every card, what we're buying and your total payout in one place — "
             "it keeps updating as we price your cards."]
    if not link_open:
        lines.append("Click the link and request access to open it.")
    return "\n".join(lines)

def _summary_list(items, n=5):
    items = list(items)
    return ", ".join(items[:n]) + (f" +{len(items) - n} more" if len(items) > n else "")

def _summary_kevin_notes(res):
    """Kevin-only notes about what a build left out / found odd."""
    notes = ""
    if res.get('older'):
        notes += (f" ℹ️ Left out {res['older']} older sheet{'s' if res['older'] != 1 else ''} "
                  f"of theirs from this ticket (from {res.get('older_from') or '?'}) — over "
                  f"{SUMMARY_MAX_DAYS} days old, treated as an earlier deal.")
    if res.get('skipped'):
        notes += (f" ⚠️ Left out sheet(s) that aren't theirs or from your pastes in this ticket: "
                  f"{_summary_list(res['skipped'])}.")
    if res.get('zeroed'):
        notes += (f" ℹ️ {len(res['zeroed'])} card(s) bought on an earlier sheet are $0 on a later "
                  f"one ({_summary_list(res['zeroed'])}) — shown as not buying; check that's intended.")
    if res.get('pay_err_sheets'):
        notes += (f" ⚠️ Broken payout cells (column I) on {_summary_list(res['pay_err_sheets'])} — "
                  f"used comp × rate for those cards; fix the sheet.")
    return notes

async def run_summary_command(message, post=False):
    """Kevin typed !summary (or `!summary post`) in a ticket. Never raises; every
    problem goes to Kevin's DMs only — the ticket gets the link or nothing."""
    channel = message.channel
    channel_id = channel.id
    cname = getattr(channel, 'name', '?')
    try:
        if channel_id in _summary_cmd_running:
            await ping_kevin(f"📊 Still building #{cname}'s summary from your last `!summary` — "
                             f"the link goes in the ticket when it's done.", channel)
            return
        _summary_cmd_running.add(channel_id)
        try:
            try:
                await message.add_reaction('👀')   # ack: a first build can take a minute or two
            except Exception:
                pass
            await _run_summary(channel, channel_id, cname, post)
        finally:
            _summary_cmd_running.discard(channel_id)
    except Exception as e:
        print(f"!summary failed for #{cname}: {e}")
        await ping_kevin(f"⚠️ `!summary` in #{cname} failed ({type(e).__name__}: {str(e)[:300]}) — "
                         f"nothing was posted in the ticket.", channel)

async def _run_summary(channel, channel_id, cname, post):
    ent = summary_sheets.get(channel_id) or {}
    customer = ticket_customer_from_overwrites(channel)
    errs, from_sheets = [], False
    if not customer and ent.get('customer') and ent.get('confirmed'):
        customer = ent['customer']   # Kevin already confirmed this ticket's customer
    if not customer:
        customer = await customer_username_for_channel(channel, days=SUMMARY_MAX_DAYS, errors=errs)
        from_sheets = customer is not None
    if not customer:
        why = f"Drive/Sheets error: {errs[0]}" if errs else "couldn't tell whose ticket this is"
        await ping_kevin(f"📊 `!summary` in #{cname}: no summary made ({why}). "
                         f"Nothing was posted in the ticket.", channel)
        return
    if from_sheets and post and ent.get('customer') not in (None, customer):
        await ping_kevin(f"📊 `!summary post` in #{cname}: the sheet names now point at **{customer}**, "
                         f"not **{ent['customer']}** you checked — nothing posted. Run `!summary` again.",
                         channel)
        return
    confirmed = (not from_sheets) or post   # `!summary post` = Kevin checked who it is
    release = await _summary_acquire(channel_id, timeout=SUMMARY_LOCK_WAIT_S)
    if release is None:
        await ping_kevin(f"📊 `!summary` in #{cname}: another summary build is still running "
                         f"(Google is slow or rate-limiting) — try again in a few minutes. "
                         f"Nothing was posted.", channel)
        return
    pub = None
    try:
        ent = summary_sheets.get(channel_id) or {}
        known = ent.get('sid') if ent.get('customer') == customer else None
        res = await asyncio.to_thread(
            _summary_build_sync, channel_id, cname, customer, getattr(channel, 'created_at', None),
            known_sid=known, allow_create=True, auto=False, cached_id=channel_sheet.get(channel_id))
        if res.get('status') == 'ok':
            if confirmed:
                pub = await asyncio.to_thread(_summary_publish_sync, res['sid'])
            ent = _summary_register(channel_id, channel, res, confirmed=confirmed, requested=True)
    finally:
        release()
    if res.get('status') != 'ok':
        await ping_kevin(f"📊 `!summary` for **{customer}**: no buying sheets with certs from this "
                         f"deal in this ticket — nothing posted in the ticket."
                         + _summary_kevin_notes(res), channel)
        return
    counts = (f"{res['n']} cards · {res['n_buy']} buying"
              + (f" · {res['n_pend']} still pricing" if res.get('n_pend') else '')
              + f" · {res['n_nobuy']} not buying")
    notes = _summary_kevin_notes(res)
    if res.get('hand_edited'):
        notes += " ℹ️ Someone had edited the summary itself by hand — it was rebuilt from the buying sheets."
    if not confirmed:
        await ping_kevin(
            f"📊 Summary built for **{res['customer']}** ({res['range']}) but NOT posted: this channel has "
            f"no ticket-opener overwrite, so I matched the customer by sheet name. Check it's them: "
            f"{res['url']} — {counts}, payout **${res['tot_pay']:,.2f}**, from "
            f"{_summary_list(res.get('sheet_names') or [])}. If it's right, type `!summary post` "
            f"in the ticket.{notes}", channel)
        return
    first = not ent.get('posted')
    link_open = bool(pub and pub['link_open'])
    try:
        await channel.send(summary_customer_message(res['url'], first, link_open, res['range']))
        posted = "Link posted in the ticket."
        if first:
            await asyncio.to_thread(_summary_mark_posted_sync, res['sid'])
        ent['posted'] = True
    except Exception as e:
        print(f"summary: ticket post failed for #{cname}: {e}")
        posted = "⚠️ Couldn't post the link in the ticket — send it yourself."
    if pub and pub['shared_now']:
        notes += " 🔗 Anyone with the link can view it."
    elif not link_open:
        notes += (f" ⚠️ Link isn't shared ({pub['share_err']}) — they'll have to request access."
                  if pub and pub['share_err'] else " ⚠️ Link isn't shared — they'll have to request access.")
    if from_sheets:
        notes += " ℹ️ Customer matched by sheet name (you confirmed with `!summary post`)."
    await ping_kevin(
        f"📊 Summary {'created' if res['created'] else 'refreshed'} for **{res['customer']}** "
        f"({res['range']}): {counts} from {res['n_sheets']} sheet{'s' if res['n_sheets'] != 1 else ''} — "
        f"comp ${res['tot_comp']:,.2f} → payout **${res['tot_pay']:,.2f}** — {res['url']} {posted}"
        f"{notes} It refreshes itself after each quote here and every {SUMMARY_TICK_S // 60} min for "
        f"{SUMMARY_TICK_MAX_H}h; hand edits on the summary itself pause that.", channel)

def schedule_summary_refresh(channel, channel_id):
    """Called after every quote (price_and_send_psa_offer) and by the ticker: if
    this ticket has a summary, refresh it in the background. Never raises and
    never awaits, so the quote path can't be delayed or broken. A ticket with no
    summary in memory gets ONE background Drive lookup per process (restart
    recovery); never auto-creates. Returns True if a refresh was scheduled."""
    try:
        if channel_id in _summary_refresh_pending:
            return False   # one already waiting — it will read this quote's sheet too
        ent = summary_sheets.get(channel_id)
        if ent is None and channel_id in _summary_none_checked:
            return False
        if ent is not None and ent.get('paused'):
            return False   # hand-edited summary: Kevin was told, waits for his !summary
        _summary_refresh_pending.add(channel_id)
        try:
            task = asyncio.get_running_loop().create_task(_summary_auto_refresh(channel, channel_id))
        except Exception:
            _summary_refresh_pending.discard(channel_id)
            raise
        _summary_bg_tasks.add(task)
        task.add_done_callback(_summary_bg_tasks.discard)
        return True
    except Exception as e:
        print(f"summary auto-refresh not scheduled for {channel_id}: {e}")
        return False

async def _summary_auto_refresh(channel, channel_id):
    cname = getattr(channel, 'name', '?')
    known = channel_id in summary_sheets
    started = False
    prog = {}
    try:
        if SUMMARY_AUTO_REFRESH_DELAY_S:
            await asyncio.sleep(SUMMARY_AUTO_REFRESH_DELAY_S)
        release = await _summary_acquire(channel_id)
        try:
            started = True
            _summary_refresh_pending.discard(channel_id)   # later quotes schedule anew
            ent = summary_sheets.get(channel_id)
            known = ent is not None
            if ent is None and channel_id in _summary_none_checked:
                return
            if ent is not None and ent.get('paused'):
                return
            customer = (ent or {}).get('customer') or ticket_customer_from_overwrites(channel)
            res = await asyncio.to_thread(
                _summary_build_sync, channel_id, cname, customer, getattr(channel, 'created_at', None),
                known_sid=(ent or {}).get('sid'), allow_create=False, auto=True,
                cached_id=channel_sheet.get(channel_id),
                prev_hash=(ent or {}).get('hash'), prog=prog)
        finally:
            release()
        st = res.get('status')
        if st in ('ok', 'unchanged'):
            prev = dict(summary_sheets.get(channel_id) or {})   # snapshot: register mutates it
            same_file = prev.get('sid') == res['sid']
            ent = _summary_register(channel_id, channel, res)
            if st == 'ok' and same_file and ent.get('posted') and prev.get('buy_certs') is not None:
                dropped = sorted(set(prev['buy_certs']) - set(res.get('buy_certs') or ()))
                was, now = prev.get('tot_pay') or 0.0, res['tot_pay']
                if dropped or now < was - 0.005:
                    await ping_kevin(
                        f"📊 #{cname} summary refreshed — the customer now sees payout "
                        f"${was:,.2f} → **${now:,.2f}**"
                        + (f"; no longer buying: {_summary_list(dropped)}" if dropped else "")
                        + f". If that's wrong, fix the buying sheet. {res['url']}", channel)
            print(f"summary auto-refresh #{cname}: {st}, {res.get('n')} cards, "
                  f"${res.get('tot_pay') or 0:,.2f} payout")
        elif st == 'hand_edited':
            ent = _summary_register(channel_id, channel, res)
            if not ent.get('paused'):
                ent['paused'] = True
                await ping_kevin(
                    f"✋ Someone edited #{cname}'s summary sheet by hand, so I stopped auto-refreshing "
                    f"it (a rebuild would wipe that edit). Edit the buying sheets instead and type "
                    f"`!summary` in the ticket to rebuild it. {res['url']}", channel)
        elif st in ('no_summary', 'no_customer'):
            summary_sheets.pop(channel_id, None)
            _summary_none_checked.add(channel_id)
            print(f"summary auto-refresh #{cname}: no summary for this ticket — off until !summary")
        else:
            print(f"summary auto-refresh for #{cname}: {st} — left as is")
    except Exception as e:
        print(f"summary auto-refresh failed for #{cname}: {e}")
        if prog.get('sid') and channel_id not in summary_sheets and prog.get('customer'):
            _summary_register(channel_id, channel, {'sid': prog['sid'], 'customer': prog['customer']})
        if known or prog.get('sid'):   # only nag about a summary that's known to exist
            try:
                await ping_kevin(f"⚠️ Summary sheet auto-refresh failed for #{cname} "
                                 f"({type(e).__name__}: {str(e)[:100]}) — type `!summary` "
                                 f"in the ticket to retry.", channel)
            except Exception:
                pass
    finally:
        if not started:   # once started, the flag belongs to the NEXT quote's refresh
            _summary_refresh_pending.discard(channel_id)

def _summary_recover_sync():
    """After a restart: summaries made in the last SUMMARY_TICK_MAX_H hours,
    tagged with their ticket — [(channel_id, sid, owner, posted, created epoch)]."""
    drive = get_drive_service()
    folder = _summary_folder(drive, create=False)
    if not folder:
        return []
    from datetime import timedelta as _td
    since = (datetime.utcnow() - _td(hours=SUMMARY_TICK_MAX_H)).strftime('%Y-%m-%dT%H:%M:%S')
    resp = _summary_retry(lambda: drive.files().list(
        q=f"'{folder}' in parents and trashed=false and createdTime > '{since}'",
        orderBy='createdTime desc', pageSize=200,
        fields='files(id,name,createdTime,appProperties)').execute(), 'recover summaries')
    out = []
    for f in resp.get('files', []):
        name, props = f.get('name') or '', f.get('appProperties') or {}
        tag = str(props.get(SUMMARY_CHANNEL_PROP) or '')
        if not (name.endswith(SUMMARY_NAME_SUFFIX) and tag.isdigit()):
            continue
        try:
            ct = datetime.strptime((f.get('createdTime') or '')[:19], '%Y-%m-%dT%H:%M:%S')
            ts = ct.replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            ts = time.time()
        out.append((int(tag), f['id'], name[:-len(SUMMARY_NAME_SUFFIX)],
                    props.get(SUMMARY_POSTED_PROP) == '1', ts))
    return out

async def _summary_recover():
    """Re-register active summaries after a restart so the ticker keeps them
    fresh even if no new quote lands in their ticket."""
    try:
        found = await asyncio.to_thread(_summary_recover_sync)
    except Exception as e:
        print(f"summary recovery scan failed: {e}")
        return 0
    n = 0
    for cid, sid, owner, posted, ts in found:   # newest first: first one per ticket wins
        if cid in summary_sheets:
            continue
        ch = bot.get_channel(cid)
        if ch is None:
            continue   # ticket closed
        summary_sheets[cid] = {'sid': sid, 'customer': owner, 'channel': ch, 'posted': posted,
                               'confirmed': posted, 'active_since': ts}
        n += 1
    if n:
        print(f"summary: re-registered {n} active summar{'y' if n == 1 else 'ies'} after restart")
    return n

async def _summary_tick_once():
    """Queue a refresh for every active summary (cheap when nothing changed:
    Drive metadata only, no Sheets reads or writes)."""
    n, now = 0, time.time()
    for cid, ent in list(summary_sheets.items()):
        if ent.get('paused') or now - ent.get('active_since', now) > SUMMARY_TICK_MAX_H * 3600:
            continue
        try:
            ch = bot.get_channel(cid)
        except Exception:
            ch = ent.get('channel')
        if ch is None:   # ticket closed (channel deleted): stop refreshing its summary
            summary_sheets.pop(cid, None)
            continue
        if schedule_summary_refresh(ch, cid):
            n += 1
    return n

async def summary_ticker():
    await _summary_recover()
    while True:
        await asyncio.sleep(SUMMARY_TICK_S)
        try:
            await _summary_tick_once()
        except Exception as e:
            print(f"summary ticker error: {e}")

def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

async def _owed_get(request):
    show_all = request.query.get("all") == "1"
    data = OWED_STORE if show_all else [o for o in OWED_STORE if o.get("status") != "paid"]
    return _cors(_ow_web.json_response(data))

async def _owed_paid(request):
    oid = request.query.get("id")
    for o in OWED_STORE:
        if o.get("id") == oid:
            o["status"] = "paid"
            o["date_paid"] = date.today().isoformat()
            _save_owed()
            return _cors(_ow_web.json_response({"ok": True}))
    return _cors(_ow_web.json_response({"ok": False, "error": "not found"}, status=404))

async def _tickets_get(request):
    """Accepted ship-lists for the ticket tracker. Filters: ?user=<name>
    (case-insensitive substring) and ?since=YYYY-MM-DD."""
    data = TICKETS_STORE
    user_q = (request.query.get("user") or "").strip().lower()
    if user_q:
        data = [t for t in data if user_q in (t.get("user") or "").lower()]
    since = (request.query.get("since") or "").strip()
    if since:
        data = [t for t in data if (t.get("date") or "") >= since]
    return _cors(_ow_web.json_response(data))

async def _owed_root(request):
    return _cors(_ow_web.Response(text="KTS bot owed endpoint OK"))

# ── LEADERBOARD (public top-suppliers board, tier-only — no dollars exposed) ──
LEADERBOARD_FILE = os.path.join(DATA_DIR, "leaderboard_store.json")
LEADERBOARD_KEY = os.environ.get("LEADERBOARD_KEY", "kts-lb-2026")
try:
    with open(LEADERBOARD_FILE) as _f:
        LEADERBOARD = json.load(_f)
except Exception:
    LEADERBOARD = {"updated": None, "entries": []}
def _save_leaderboard():
    try:
        with open(LEADERBOARD_FILE, "w") as _f:
            json.dump(LEADERBOARD, _f)
    except Exception as e:
        print(f"leaderboard save failed (non-critical): {e}")
TIER_EMOJI = {"Diamond": "💎", "Gold": "🥇", "Silver": "🥈", "Bronze": "🥉"}
TIER_RANK = {"Bronze": 1, "Silver": 2, "Gold": 3, "Diamond": 4}

# ── SUPPLIER TIER ROLES (auto-assigned to the top-10 board members) ──
LINK_FILE = os.path.join(DATA_DIR, "supplier_links.json")
try:
    with open(LINK_FILE) as _f:
        SUPPLIER_LINKS = json.load(_f)   # {normalized board name: discord user_id}
except Exception:
    SUPPLIER_LINKS = {}
def _save_links():
    try:
        with open(LINK_FILE, "w") as _f:
            json.dump(SUPPLIER_LINKS, _f)
    except Exception as e:
        print(f"links save failed (non-critical): {e}")
TIER_ROLE_NAMES = {"Diamond": "💎 Diamond Supplier", "Gold": "🥇 Gold Supplier", "Silver": "🥈 Silver Supplier"}
TIER_ROLE_COLORS = {"Diamond": 0x4DD0E1, "Gold": 0xF1C40F, "Silver": 0xBDC3C7}
def _norm_name(s):
    import re as _re
    return _re.sub(r'[^a-z0-9]', '', (s or '').lower())
def _resolve_member(guild, name):
    """Find a guild member for a board display name: explicit link first, else username/nick match."""
    key = _norm_name(name)
    uid = SUPPLIER_LINKS.get(key)
    if uid:
        m = guild.get_member(int(uid))
        if m:
            return m
    for m in guild.members:
        if key in (_norm_name(m.name), _norm_name(getattr(m, 'global_name', '') or ''), _norm_name(m.display_name)):
            return m
    return None
async def _ensure_tier_roles(guild):
    roles = {}
    for tier, rname in TIER_ROLE_NAMES.items():
        role = discord.utils.get(guild.roles, name=rname)
        if role is None:
            try:
                role = await guild.create_role(name=rname, colour=discord.Colour(TIER_ROLE_COLORS[tier]),
                                                hoist=True, mentionable=False, reason="KTS supplier tier role")
            except Exception as e:
                print(f"could not create role {rname}: {e}")
        roles[tier] = role
    return roles
async def sync_supplier_roles():
    """Give each top-10 board member their tier role; strip tier roles from anyone who dropped off."""
    gid = LEADERBOARD.get("guild_id")
    guild = bot.get_guild(int(gid)) if gid else (bot.guilds[0] if bot.guilds else None)
    if guild is None:
        return {"ok": False, "error": "bot isn't in a server yet", "unmatched": []}
    roles = await _ensure_tier_roles(guild)
    all_tier_roles = {r for r in roles.values() if r}
    desired = {}   # member.id -> tier role they should have
    unmatched = []
    for e in (LEADERBOARD.get("entries") or []):   # roles for EVERYONE over the tier bar, not just top 10
        role = roles.get(e.get("tier"))
        if role is None:
            continue
        m = _resolve_member(guild, e.get("name", ""))
        if m is None:
            unmatched.append(e.get("name", ""))
            continue
        desired[m.id] = role
    for m in guild.members:
        has = all_tier_roles & set(m.roles)
        want = desired.get(m.id)
        try:
            if want and want not in has:
                await m.add_roles(want, reason="KTS supplier tier")
            for r in has:
                if r != want:
                    await m.remove_roles(r, reason="KTS supplier tier update")
        except Exception as ex:
            print(f"role update failed for {m}: {ex}")
    return {"ok": True, "assigned": len(desired), "unmatched": unmatched}
def _move_arrow(m):
    if m is None: return ""
    if m == "new": return "  🆕"
    if isinstance(m, int) and m > 0: return f"  ▲{m}"
    if isinstance(m, int) and m < 0: return f"  ▼{abs(m)}"
    return "  —"
def format_leaderboard():
    entries = LEADERBOARD.get("entries") or []
    if not entries:
        return "No leaderboard yet — check back soon."
    lines = ["🏆 **KTS Top Suppliers** 🏆"]
    cur = None
    for e in entries[:10]:   # public board shows top 10 only (roles still go to everyone over the tier bar)
        t = e.get("tier", "")
        if t != cur:
            cur = t
            lines.append(f"\n{TIER_EMOJI.get(t, '•')} **{t}**")
        lines.append(f"`#{e.get('rank',0):>2}`  {e.get('name','')}{_move_arrow(e.get('move'))}")
    if LEADERBOARD.get("updated"):
        lines.append(f"\n_Updated {LEADERBOARD['updated']} · ▲▼ vs last week_")
    return "\n".join(lines)
async def _leaderboard_get(request):
    return _cors(_ow_web.json_response(LEADERBOARD))
async def _links_get(request):
    return _cors(_ow_web.json_response(SUPPLIER_LINKS))
async def _leaderboard_set(request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if body.get("key") != LEADERBOARD_KEY:
        return _cors(_ow_web.json_response({"ok": False, "error": "bad key"}, status=403))
    # restore baked-in supplier→Discord links so they survive Railway redeploys
    incoming_links = body.get("links")
    if isinstance(incoming_links, dict):
        for _k, _v in incoming_links.items():
            SUPPLIER_LINKS[str(_k)] = _v
        _save_links()
    new_entries = body.get("entries", [])
    # detect tier-ups vs the previously stored board
    old_tier = {e.get("name"): e.get("tier") for e in (LEADERBOARD.get("entries") or [])}
    promos = [(e.get("name"), old_tier[e.get("name")], e.get("tier")) for e in new_entries
              if e.get("name") in old_tier and TIER_RANK.get(e.get("tier"), 0) > TIER_RANK.get(old_tier[e.get("name")], 0)]
    LEADERBOARD["entries"] = new_entries
    LEADERBOARD["updated"] = body.get("updated") or date.today().isoformat()
    _save_leaderboard()
    cid = LEADERBOARD.get("channel_id")
    if cid and promos:
        ch = bot.get_channel(cid)
        if ch:
            for nm, ot, nt in promos:
                try:
                    await ch.send(f"🎉 **{nm}** just climbed to {TIER_EMOJI.get(nt,'')} **{nt}** tier — up from {ot}! 🔥")
                except Exception as ex:
                    print(f"tier-up announce failed: {ex}")
    role_res = {}
    try:
        role_res = await sync_supplier_roles()
        if role_res.get("unmatched"):
            print(f"role sync — unmatched (need !link): {role_res['unmatched']}")
    except Exception as ex:
        print(f"role sync failed: {ex}")
    # auto-update the live pinned board message, if one exists
    await _update_live_board()
    return _cors(_ow_web.json_response({"ok": True, "count": len(new_entries), "promos": len(promos),
                                        "roles_assigned": role_res.get("assigned", 0),
                                        "roles_unmatched": role_res.get("unmatched", [])}))

async def _update_live_board():
    bmid = LEADERBOARD.get("board_message_id"); bcid = LEADERBOARD.get("board_channel_id")
    if not (bmid and bcid):
        return
    try:
        ch = bot.get_channel(bcid)
        if ch:
            msg = await ch.fetch_message(bmid)
            await msg.edit(content=format_leaderboard())
    except Exception as ex:
        print(f"live board update failed: {ex}")

async def start_owed_webserver():
    app = _ow_web.Application()
    app.add_routes([
        _ow_web.get("/", _owed_root),
        _ow_web.get("/health", _owed_root),
        _ow_web.get("/owed", _owed_get),
        _ow_web.get("/owed/paid", _owed_paid),
        _ow_web.get("/tickets", _tickets_get),
        _ow_web.get("/leaderboard", _leaderboard_get),
        _ow_web.post("/leaderboard", _leaderboard_set),
        _ow_web.get("/links", _links_get),
    ])
    runner = _ow_web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", "8080"))
    await _ow_web.TCPSite(runner, "0.0.0.0", port).start()
    print(f"✅ Owed web endpoint listening on 0.0.0.0:{port}  (GET /owed)")

_OWED_WEB_STARTED = False
_HELPER_MONITOR_STARTED = False

def _helper_is_healthy():
    """Blocking health probe of the comp helper (run via asyncio.to_thread)."""
    import urllib.request as urlreq
    req = urlreq.Request(f"{HELPER_URL}/health", headers={"User-Agent": "KTS-Bot/1.0"})
    with urlreq.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    return bool(data.get("ok")) and bool(data.get("braveAttached"))

async def helper_health_monitor(interval_s=300):
    """DM Kevin when the comp helper goes down (2 consecutive failed checks, to
    skip momentary blips — the VM watchdog usually self-heals within ~3 min) and
    again when it recovers, so an outage never goes unnoticed."""
    fails = 0
    alerted_down = False
    while True:
        await asyncio.sleep(interval_s)
        try:
            healthy = await asyncio.to_thread(_helper_is_healthy)
        except Exception:
            healthy = False
        if healthy:
            if alerted_down:
                await ping_kevin("✅ **Comp helper is back up** — cert lookups working again.")
                alerted_down = False
            fails = 0
        else:
            fails += 1
            if fails >= 2 and not alerted_down:
                # Only mark alerted if the DM actually went through — otherwise
                # retry the alert on the next cycle instead of going silent.
                alerted_down = await ping_kevin(
                    "🚨 **Comp helper looks DOWN** (2 checks in a row). The VM "
                    "watchdog should revive it within a few minutes and cert "
                    "quotes will auto-retry — but if this doesn't clear in "
                    "~10 min, check the VM."
                )

def build_recent_tickets_csv(days=3, chan_links=None):
    """Collect every buying sheet created in the last N days into one CSV —
    a row per card with a nonzero payout rate (what Kevin is actually buying),
    spreadsheet-ready for the send-to-boss tracker. chan_links maps sanitized
    channel names -> Discord jump URLs so every row and summary line links
    straight to the ticket. Returns (csv_text, summary).
    Blocking — run via asyncio.to_thread."""
    chan_links = chan_links or {}
    from datetime import timedelta as _td
    since = (datetime.utcnow() - _td(days=days)).strftime('%Y-%m-%dT%H:%M:%S')
    files = get_drive_service().files().list(
        q=f"'{PSA_FOLDER_ID}' in parents and trashed=false and createdTime > '{since}'",
        orderBy='createdTime asc', pageSize=200,
        fields='files(name,id,createdTime)').execute().get('files', [])
    files = [f for f in files if not _is_summary_sheet_name(f.get('name'))]   # !summary sheets
    gcl = get_gspread_client()
    header = "Date,Customer,Cert,Card,Grade,Value,Rate,Payout,Ticket"
    by_cert = {}   # cert -> csv row (dedupes resubmitted lots; last sheet wins)
    n_skipped = n_dupes = 0
    tickets = []
    error_rows = []
    for f in files:
        username = f['name'].split(' discord')[0].strip()
        day = f['createdTime'][:10]
        ticket_url = chan_links.get(_sanitize_channel_name(username), '')
        rows = None
        for _attempt in (1, 2, 3):
            try:
                ws = gcl.open_by_key(f['id']).get_worksheet(0)
                rows = ws.get_values("A2:H1000")
                break
            except Exception as e:
                _err = e
                if _attempt < 3:
                    time.sleep(35)   # Sheets read quota resets per minute
        if rows is None:
            error_rows.append(f'{day},{username},SHEET READ FAILED,"{str(_err)[:60]}",,,,,')
            continue
        time.sleep(1.2)   # pace reads to stay under the per-minute quota
        t_cards = 0
        t_value = 0.0
        for row in rows:
            row += [''] * (8 - len(row))
            cert, name, grade, value, rate = row[1], row[3], row[4], row[6], row[7]
            if not str(cert).strip().isdigit():
                continue
            try:
                v = float(str(value).replace('$', '').replace(',', ''))
                rt = float(str(rate).replace('%', '') or 0)
            except ValueError:
                continue
            if rt > 1.5:
                rt = rt / 100.0   # sheet renders H as "86", not 0.86
            if rt <= 0:
                n_skipped += 1
                continue
            safe_name = str(name).replace('"', "'")
            cert_key = str(cert).strip()
            if cert_key in by_cert:
                n_dupes += 1
            by_cert[cert_key] = (f'{day},{username},{cert_key},"{safe_name}",{grade},'
                                 f'{v:.2f},{rt*100:g}%,{v*rt:.2f},{ticket_url}', v, v*rt)
            t_cards += 1
            t_value += v
        if t_cards:
            tickets.append(f"{username} ({day[5:]}): {t_cards} cards ${t_value:,.2f}"
                           + (f" — <{ticket_url}>" if ticket_url else ""))
    total_value = sum(x[1] for x in by_cert.values())
    total_payout = sum(x[2] for x in by_cert.values())
    lines = [header] + [x[0] for x in by_cert.values()] + error_rows
    summary = (f"🧾 **Last {days} day{'s' if days != 1 else ''}:** {len(files)} sheets, "
               f"{len(by_cert)} accepted cards, ${total_value:,.2f} comp → ${total_payout:,.2f} payout"
               + (f" ({n_skipped} zero-rate rows left out)" if n_skipped else "")
               + (f" ({n_dupes} duplicate certs collapsed)" if n_dupes else "")
               + (f" ⚠️ {len(error_rows)} sheet(s) unreadable — see CSV bottom" if error_rows else "") + "\n"
               + "\n".join(f"• {t}" for t in tickets[:25]))
    return "\n".join(lines), summary

async def requote_stalled_tickets(days=7):
    """One-shot recovery sweep (!requoteall): find sheets from the last N days
    that have certs but NO comps (a crashed or interrupted quote), match each
    back to its ticket channel by username (channels are named after their
    opener), re-run the comps, and post the quote — reusing the EXISTING sheet.
    Sequential; helper lookups are already serialized/paced. Returns a summary."""
    guild = bot.guilds[0] if bot.guilds else None
    if guild is None:
        return "Bot isn't in a server yet."
    from datetime import timedelta as _td
    since = (datetime.utcnow() - _td(days=days)).strftime('%Y-%m-%dT%H:%M:%S')
    def _list_sheets():
        return get_drive_service().files().list(
            q=f"'{PSA_FOLDER_ID}' in parents and trashed=false and createdTime > '{since}'",
            orderBy='createdTime asc', pageSize=100,
            fields='files(name,id)').execute().get('files', [])
    try:
        files = await asyncio.to_thread(_list_sheets)
    except Exception as e:
        return f"Drive scan failed: {e}"
    done = skipped = failed = 0
    quoted_channels = set()
    for f in files:
        if _is_summary_sheet_name(f.get('name')):
            continue   # a !summary sheet, not a buying sheet — never requote it
        def _read(fid=f['id']):
            ws = get_gspread_client().open_by_key(fid).get_worksheet(0)
            # A stalled sheet never got its column A stamped, so CGC is told
            # apart by length (10+ digits) — same token the live quote uses.
            certs = [_sheet_cert_token('', str(c).strip(), '')
                     for c in ws.col_values(2)[1:] if str(c).strip().isdigit()]
            filled = [v for v in ws.col_values(7)[1:]
                      if str(v).strip() not in ('', 'not found', SHEET_NOT_LOADED_MARK)]
            return certs, filled
        try:
            certs, filled = await asyncio.to_thread(_read)
        except Exception as e:
            failed += 1
            await ping_kevin(f"🔁 {f['name']}: sheet read failed ({str(e)[:80]})")
            continue
        if not certs or filled:
            continue   # healthy quote or empty sheet — not stalled
        username = f['name'].split(' discord')[0].strip()
        channel = discord.utils.get(guild.text_channels, name=_sanitize_channel_name(username))
        if channel is None:
            skipped += 1
            await ping_kevin(f"🔁 **{f['name']}**: stalled ({len(certs)} certs) but no ticket "
                             f"channel named after the customer — re-send certs there manually.")
            continue
        if channel.id in quoted_channels:
            skipped += 1
            await ping_kevin(f"🔁 **{f['name']}**: second stalled sheet for #{channel.name} — "
                             f"skipped (already requoted the first; handle by hand if both were real).")
            continue
        if channel.id in pending_comp_retries:
            # The background comp retry still owes this ticket its quote (its
            # sheet's G column stays empty until then, so it LOOKS stalled).
            # Quoting here too would post a duplicate when the retry lands.
            skipped += 1
            await ping_kevin(f"🔁 **{f['name']}**: auto-retry still pending for #{channel.name} — "
                             f"skipped (type `!cancelretry` in the ticket first if you want "
                             f"!requoteall to take it).")
            continue
        remember_ticket_channel(channel.id)
        remember_channel_sheet(channel.id, f['id'])
        sheet_url = f"https://docs.google.com/spreadsheets/d/{f['id']}/edit"
        try:
            # No inline all-bad re-check: a sweep runs many tickets back to back
            # under the helper lock; a no-load ticket is just reported for later.
            comps = await asyncio.to_thread(lookup_comps, certs, recheck_all_bad=False)
            returned = {str(c.get('cert', '')).strip() for c in comps or []}
            n_loaded = len(certs) - len(_uncovered_certs(certs, comps))
            # Missing certs, or EVERY cert a CardLadder timeout (Oct 18) — same
            # as helper-down: don't post a quote that rejects them all.
            if not comps or any(str(c).strip() not in returned for c in certs) or not n_loaded:
                failed += 1
                await ping_kevin(f"🔁 **{username}**: partial comp coverage "
                                 f"({n_loaded}/{len(certs)} loaded) — not quoting, run !requoteall again later.")
                continue
            await price_and_send_psa_offer(channel, channel.id, username, certs,
                                           comps, f['id'], sheet_url, delayed=True)
            quoted_channels.add(channel.id)
            done += 1
        except Exception as e:
            failed += 1
            await ping_kevin(f"🔁 **{username}**: requote FAILED — {str(e)[:120]}\n{sheet_url}", channel)
    return (f"🔁 **Requote sweep done** — {done} ticket{'s' if done != 1 else ''} quoted, "
            f"{skipped} skipped, {failed} failed.")

@bot.event
async def on_ready():
    global _OWED_WEB_STARTED, _HELPER_MONITOR_STARTED
    print(f"✅ KTS Collectibles Bot online as {bot.user}")
    if not _OWED_WEB_STARTED:
        _OWED_WEB_STARTED = True
        try:
            await start_owed_webserver()
        except Exception as e:
            print(f"owed web server failed to start: {e}")
    if not _HELPER_MONITOR_STARTED:
        _HELPER_MONITOR_STARTED = True
        asyncio.create_task(helper_health_monitor())
        asyncio.create_task(payout_digest_loop())
    if not _SUMMARY_TICKER.get('task'):
        # Customer summary sheets: restart recovery + periodic refresh (team edits,
        # hand prices) of every active summary.
        _SUMMARY_TICKER['task'] = asyncio.create_task(summary_ticker())
    global _PENDING_RETRIES_RESUMED
    if not _PENDING_RETRIES_RESUMED:
        _PENDING_RETRIES_RESUMED = True
        asyncio.create_task(_resume_pending_retries())
    global _VIP_LOAD_FAILED
    if _VIP_LOAD_FAILED:
        _VIP_LOAD_FAILED = False
        await ping_kevin(
            "⚠️ **VIP store failed to load** — every VIP is quoting at STANDARD "
            "rates until you re-add them with `!vip add`. The unreadable file "
            "was kept as vip_store.json.corrupt.")
    # Pick up overflow tickets (category was full) that opened while the bot
    # was down or that a store wipe forgot — runs on every (re)connect, cheap
    # and idempotent.
    await _sweep_overflow_tickets()

async def handle_owe(message):
    """!owe <@seller|name> <amount> [ach] [note...] — log a package Kevin owes (default Wire)."""
    import re as _re
    body = message.content.strip()[4:].strip()  # drop '!owe'
    if message.mentions:
        seller = message.mentions[0].display_name or message.mentions[0].name
        body = _re.sub(r'<@!?\d+>', '', body).strip()
    else:
        toks = body.split()
        seller = toks[0] if toks else ''
        body = ' '.join(toks[1:])
    m = _re.search(r'\$?\s*([\d,]+(?:\.\d{1,2})?)', body)
    if not seller or not m:
        await message.channel.send("Usage: `!owe @seller 2400 [ach] [note]`  (defaults to Wire)")
        return
    amount = float(m.group(1).replace(',', ''))
    method = 'ACH' if 'ach' in body.lower() else 'Wire'
    note = _re.sub(r'\b(ach|wire)\b', '', body.replace(m.group(0), '', 1), flags=_re.I).strip(' -')
    rec = {"id": f"owed_{message.id}", "discord": seller, "amount": amount,
           "method": method, "date": date.today().isoformat(), "note": note,
           "status": "owed", "date_paid": None, "source": "bot"}
    upsert_owed(rec)
    await message.channel.send(
        f"✅ Logged: owe **{seller}** ${amount:,.2f} via {method}"
        + (f" — {note}" if note else "")
        + ".  Open the tracker → **Sync from bot**.  💸 Packet in your DMs.")
    # Copy-paste payment packet with the seller's stored details -> Kevin's DMs
    # only (bank info never posts in channels).
    await ping_kevin(build_payment_packet(seller, amount, method, note))

def _sanitize_channel_name(name):
    """Discord channel names: lowercase letters/digits/dash/underscore. Usernames
    with periods (e.g. 'cwilk_sportscards.') lose them — the Drive sheet lookup
    already tries both the channel name and the author's raw username."""
    s = re.sub(r'[^a-z0-9_-]', '', (name or '').lower().replace(' ', '-'))
    return s[:90]

# ── TICKET CHANNEL REGISTRY — survives category overflow ─────────────────────
# Discord caps categories at 50 channels. When "open ticket" is full, Ticket
# Tool drops new tickets OUTSIDE the category (they float below the previous
# category, e.g. under announcements). After the rename to the customer's
# username those channels carry no "ticket" marker anywhere, so the
# name/category gate can't see them and the bot goes silent. Fix: remember
# every ticket channel's ID at CREATION (they're always born "ticket-XXXX")
# and let the gate match on the remembered ID too. `!adopt` (Kevin, in-channel)
# registers a channel manually — the rescue for tickets opened before this
# existed or after a redeploy wiped the store (DATA_DIR volume still pending).
TICKET_CHANNELS_FILE = os.path.join(DATA_DIR, "ticket_channels_store.json")
try:
    with open(TICKET_CHANNELS_FILE) as _f:
        TICKET_CHANNELS = set(json.load(_f))
except Exception:
    TICKET_CHANNELS = set()

def _save_ticket_channels():
    try:
        with open(TICKET_CHANNELS_FILE + ".tmp", "w") as _f:
            json.dump(sorted(TICKET_CHANNELS), _f)
        os.replace(TICKET_CHANNELS_FILE + ".tmp", TICKET_CHANNELS_FILE)
    except Exception as e:
        print(f"ticket-channel store save failed (non-critical): {e}")

def remember_ticket_channel(channel_id):
    if channel_id not in TICKET_CHANNELS:
        TICKET_CHANNELS.add(channel_id)
        _save_ticket_channels()

def _ticket_fingerprint(channel_name, everyone_can_view, member_names):
    """Pure Ticket-Tool fingerprint check: a ticket is PRIVATE to @everyone and
    named after a member who holds an explicit access overwrite (that's exactly
    what our rename-at-creation produces). Public channels and private channels
    named anything else (staff rooms etc.) don't match."""
    if everyone_can_view:
        return False
    return any(_sanitize_channel_name(n) == channel_name for n in member_names if n)

def _looks_like_ticket_channel(channel):
    """Discord-side wrapper for _ticket_fingerprint. Best-effort: any API
    weirdness returns False (fail closed — !adopt still exists)."""
    try:
        eo = channel.overwrites_for(channel.guild.default_role)
        everyone_can_view = eo.view_channel is not False
        member_names = [t.name for t in channel.overwrites
                        if isinstance(t, discord.Member) and not t.bot]
        return _ticket_fingerprint(channel.name, everyone_can_view, member_names)
    except Exception:
        return False

async def _sweep_overflow_tickets():
    """Startup sweep: register every channel that walks and quacks like a
    ticket but sits outside the (full) ticket category — so overflow tickets
    opened while the bot was down, or forgotten in a store wipe, work again
    without Kevin touching them. Registering is silent (no welcome blast)."""
    found = 0
    try:
        for ch in bot.get_all_channels():
            if not isinstance(ch, discord.TextChannel) or ch.id in TICKET_CHANNELS:
                continue
            cat = (ch.category.name.lower() if ch.category else "")
            if "ticket" in ch.name.lower() or "ticket" in cat:
                continue   # normal gate already sees these
            if _looks_like_ticket_channel(ch):
                remember_ticket_channel(ch.id)
                found += 1
        if found:
            print(f"Ticket sweep: auto-registered {found} overflow ticket channel(s)")
    except Exception as e:
        print(f"Ticket sweep failed (non-critical): {e}")

@bot.event
async def on_guild_channel_create(channel):
    """Rename a freshly-opened ticket channel to the opener's Discord username
    (e.g. #ticket-0712 -> #cwilk_sportscards). The opener is the one non-bot
    MEMBER with a permission overwrite — Ticket Tool grants the opener access
    that way when it creates the channel. Overwrites can land a moment after
    creation, so poll briefly. Best-effort: a failed rename (missing Manage
    Channels permission, rate limit) is logged and never blocks anything —
    the ticket still works under its original name via the category check."""
    if not isinstance(channel, discord.TextChannel):
        return
    cat = (channel.category.name.lower() if channel.category else "")
    if "ticket" not in channel.name.lower() and "ticket" not in cat:
        return
    # Remember the ID for good: once renamed (and if it sits outside a FULL
    # "open ticket" category), nothing else identifies this as a ticket.
    remember_ticket_channel(channel.id)
    opener = None
    for _ in range(10):
        fresh_ch = bot.get_channel(channel.id) or channel
        for target in fresh_ch.overwrites:
            if isinstance(target, discord.Member) and not target.bot:
                opener = target
                break
        if opener:
            channel = fresh_ch
            break
        await asyncio.sleep(3)
    if opener is None:
        print(f"Ticket rename: no opener overwrite found on #{channel.name} — leaving as-is")
        return
    new_name = _sanitize_channel_name(opener.name)
    if not new_name or channel.name == new_name:
        return
    try:
        await channel.edit(name=new_name, reason="KTS: ticket renamed to opener's username")
        print(f"Ticket renamed: #{channel.name} -> #{new_name} (opener {opener.name})")
    except Exception as e:
        print(f"Ticket rename failed for #{channel.name} (bot needs Manage Channels?): {e}")

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    # ── !recent [days] (Kevin only, DM ok): CSV of all cards from tickets in
    # the last N days (default 3) — for the send-to-boss spreadsheet ──
    if message.content.strip().lower().startswith('!recent'):
        if message.author.id == YOUR_DISCORD_USER_ID:
            _toks = message.content.split()
            _days = int(_toks[1]) if len(_toks) > 1 and _toks[1].isdigit() else 3
            await message.channel.send(f"🧾 Pulling tickets from the last {_days} day{'s' if _days != 1 else ''}...")
            try:
                _g = message.guild or (bot.guilds[0] if bot.guilds else None)
                _links = ({ch.name: f"https://discord.com/channels/{_g.id}/{ch.id}"
                           for ch in _g.text_channels} if _g else {})
                _csv, _summary = await asyncio.to_thread(build_recent_tickets_csv, _days, _links)
                _fp = io.BytesIO(_csv.encode('utf-8'))
                await message.channel.send(
                    _summary,
                    file=discord.File(_fp, filename=f"tickets_last_{_days}d.csv"))
            except Exception as e:
                await message.channel.send(f"⚠️ Couldn't build the list: {str(e)[:150]}")
        return

    # ── !requoteall [days] (Kevin only, DM ok): auto-requote every stalled
    # sheet (certs but no comps) from the last N days — no pasting needed ──
    if message.content.strip().lower().startswith('!requoteall'):
        if message.author.id == YOUR_DISCORD_USER_ID:
            _toks = message.content.split()
            _days = int(_toks[1]) if len(_toks) > 1 and _toks[1].isdigit() else 7
            await message.channel.send(f"🔁 Requote sweep started (last {_days} days) — per-ticket updates land in your DMs.")
            _summary = await requote_stalled_tickets(_days)
            await message.channel.send(_summary)
        return

    # ── !payinfo (Kevin only) — sellers' wire/ACH details, DM-routed ──
    if message.content.strip().lower().startswith('!payinfo'):
        if message.author.id == YOUR_DISCORD_USER_ID:
            try:
                reply = handle_payinfo_command(message.content)
                if message.guild is None:
                    await message.channel.send(reply)
                else:
                    # The command itself may contain bank details — scrub it and
                    # DM the reply. Never leave payment info in a channel.
                    try:
                        await message.delete()
                    except Exception:
                        pass
                    if not await ping_kevin(reply):
                        await message.channel.send("Couldn't DM you — check DM settings.")
            except Exception as e:
                print(f"payinfo command error: {e}")
        return

    # ── !paid (Kevin only) — mark the oldest matching owed entry paid ──
    if message.content.strip().lower().startswith('!paid'):
        if message.author.id == YOUR_DISCORD_USER_ID:
            toks = message.content.strip().split()[1:]
            if not toks:
                await message.channel.send("Usage: `!paid <username> [amount]`")
                return
            seller = toks[0].lstrip('@')
            amt = None
            if len(toks) > 1:
                try:
                    amt = float(toks[1].replace('$', '').replace(',', ''))
                except ValueError:
                    pass
            rec, remaining = mark_paid(seller, amt)
            if rec is None:
                if remaining:
                    listing = ", ".join(f"${float(o.get('amount',0)):,.2f} ({o.get('date','?')})" for o in remaining)
                    await message.channel.send(f"No unpaid entry matched that amount for **{seller}** — unpaid: {listing}")
                else:
                    await message.channel.send(f"Nothing unpaid on file for **{seller}**.")
            else:
                left = f"  ({len(remaining)} still unpaid for them)" if remaining else ""
                await message.channel.send(
                    f"✅ Paid: **{rec.get('discord')}** ${float(rec.get('amount',0)):,.2f} "
                    f"{rec.get('method','')} (logged {rec.get('date','?')}).{left}  Tracker syncs on next open.")
        return

    # ── !quota command (Kevin only) — Pokémon slot usage ──
    if message.content.strip().lower().startswith('!quota'):
        if message.author.id == YOUR_DISCORD_USER_ID:
            try:
                await message.channel.send(handle_quota_command(message.content))
            except Exception as e:
                await message.channel.send(f"Quota command error: {e}")
        return

    # ── !shop command (Kevin only, DM ok) — buying open/closed master switch ──
    if message.content.strip().lower().startswith('!shop'):
        if message.author.id == YOUR_DISCORD_USER_ID:
            try:
                await message.channel.send(handle_shop_command(message.content))
            except Exception as e:
                await message.channel.send(f"Shop command error: {e}")
        return

    # ── !vip command (Kevin only) — per-customer premium Pokémon rates ──
    if message.content.strip().lower().startswith('!vip'):
        if message.author.id == YOUR_DISCORD_USER_ID:
            try:
                # DM commands still validate typed usernames against the KTS
                # server's member list (typo protection works everywhere).
                _g = message.guild or next(iter(bot.guilds), None)
                reply = handle_vip_command(message.content, message.mentions, _g)
                if message.guild is None:
                    await message.channel.send(reply)
                else:
                    # Negotiated rates are private — never post them in a server
                    # channel (tickets are customer-readable). DM the reply and
                    # scrub Kevin's own command message from the channel too.
                    try:
                        await message.delete()
                    except Exception:
                        pass   # needs Manage Messages; harmless if missing
                    if not await ping_kevin(reply):
                        await message.channel.send(
                            "Couldn't DM you — check your DM settings. "
                            "(Not posting VIP rates here.)")
            except Exception as e:
                print(f"vip command error: {e}")
        return

    # ── !owe command (Kevin only) — log a package he owes, anywhere ──
    if message.content.strip().lower().startswith('!owe'):
        if message.author.id == YOUR_DISCORD_USER_ID:
            await handle_owe(message)
        return

    # ── suppliers board (open to anyone) — renamed off !leaderboard/!top to avoid Carl-bot ──
    if message.content.strip().lower() in ('!suppliers', '!topguys', '!plugs'):
        await message.channel.send(format_leaderboard())
        return

    # ── post a LIVE leaderboard message here that auto-updates (Kevin only) ──
    if message.content.strip().lower() == '!postboard':
        if message.author.id == YOUR_DISCORD_USER_ID:
            sent = await message.channel.send(format_leaderboard())
            LEADERBOARD["board_channel_id"] = message.channel.id
            LEADERBOARD["board_message_id"] = sent.id
            if message.guild:
                LEADERBOARD["guild_id"] = message.guild.id
            _save_leaderboard()
            try:
                await sent.pin()
            except Exception:
                pass
            await message.channel.send("✅ That's now the **live leaderboard** — it auto-updates on every refresh. (You can delete this line.)")
        return

    # ── set this channel for tier-up announcements (Kevin only) ──
    if message.content.strip().lower() == '!setboardchannel':
        if message.author.id == YOUR_DISCORD_USER_ID:
            LEADERBOARD["channel_id"] = message.channel.id
            if message.guild:
                LEADERBOARD["guild_id"] = message.guild.id
            _save_leaderboard()
            await message.channel.send("✅ Tier-up announcements will post in this channel.")
        return

    # ── link a board name to a Discord member, for role assignment (Kevin only) ──
    if message.content.strip().lower().startswith('!link'):
        if message.author.id == YOUR_DISCORD_USER_ID:
            if message.guild:
                LEADERBOARD["guild_id"] = message.guild.id; _save_leaderboard()
            if not message.mentions:
                await message.channel.send("Usage: `!link @user <board name>`  e.g. `!link @someone Valcon Vault`")
            else:
                import re as _re
                nm = _re.sub(r'<@!?\d+>', '', message.content.strip()[5:]).strip()
                if not nm:
                    await message.channel.send("Add the board name: `!link @user Valcon Vault`")
                else:
                    SUPPLIER_LINKS[_norm_name(nm)] = message.mentions[0].id
                    _save_links()
                    await message.channel.send(f"🔗 Linked **{nm}** → {message.mentions[0].mention}. Run `!syncroles` to apply.")
        return

    # ── (re)assign supplier tier roles now (Kevin only) ──
    if message.content.strip().lower() == '!syncroles':
        if message.author.id == YOUR_DISCORD_USER_ID:
            if message.guild:
                LEADERBOARD["guild_id"] = message.guild.id; _save_leaderboard()
            res = await sync_supplier_roles()
            if not res.get("ok"):
                await message.channel.send(f"⚠️ {res.get('error')}")
            else:
                out = f"✅ Tier roles applied to **{res.get('assigned',0)}** suppliers."
                if res.get("unmatched"):
                    out += "\n❓ Couldn't auto-match (tag with `!link @user <name>`): " + ", ".join(res["unmatched"])
                await message.channel.send(out)
        return

    # A channel counts as a buying ticket if its OWN name contains "ticket" (the old
    # naming) OR it sits under a category whose name contains "ticket". The new ticket
    # system names each channel after the customer's Discord username (e.g.
    # #markandjheyson) and files them all under the "open ticket" category, so the old
    # channel-name check silently ignored every new ticket. Gating on the category
    # keeps the bot working no matter what the individual channel is named.
    _is_text = isinstance(message.channel, discord.TextChannel)
    _cat_name = message.channel.category.name.lower() if _is_text and message.channel.category else ""
    is_ticket = _is_text and (message.channel.id in TICKET_CHANNELS
                              or "ticket" in message.channel.name.lower()
                              or "ticket" in _cat_name)
    # Lazy auto-adopt: an unregistered channel that matches the Ticket-Tool
    # fingerprint (private + named after its opener) is an overflow ticket the
    # sweep hasn't seen yet — register it and process normally.
    if _is_text and not is_ticket and _looks_like_ticket_channel(message.channel):
        remember_ticket_channel(message.channel.id)
        is_ticket = True

    # ── !adopt (Kevin only): force-register THIS channel as a buying ticket —
    # rescue for overflow tickets that predate the ID registry (or lost it to
    # a redeploy while the DATA_DIR volume is still missing) ──
    if _is_text and message.content.strip().lower() == '!adopt':
        if message.author.id == YOUR_DISCORD_USER_ID:
            remember_ticket_channel(message.channel.id)
            await message.channel.send(
                "✅ This channel now counts as a **buying ticket** — cert numbers "
                "and Collectr CSVs will process here."
            )
        return

    # ── !summary (Kevin only, inside the ticket): ONE customer-visible sheet with
    # everything this ticket's customer sent this deal (latest priced row wins per
    # cert); link posted in the ticket, refreshed after every later quote and on a
    # timer. Anyone else's !summary is ignored. ──
    _cmd = message.content.strip().lower()
    if _cmd == '!summary' or _cmd.startswith('!summary '):
        if message.author.id == YOUR_DISCORD_USER_ID:
            if is_ticket:
                # `!summary post`: Kevin checked a summary whose customer was matched
                # by sheet name (no ticket-opener overwrite) — now post it.
                await run_summary_command(message, post=_cmd.split()[1:2] == ['post'])
            else:
                await ping_kevin("📊 `!summary` only works inside a customer's ticket channel.")
        return

    if not is_ticket:
        return

    # ── cancel the pending comp auto-retry in this ticket (Kevin only) ──
    if message.content.strip().lower() == '!cancelretry':
        if message.author.id == YOUR_DISCORD_USER_ID:
            if cancel_comp_retry(message.channel.id, "cancelled by Kevin via !cancelretry"):
                await message.channel.send("🛑 Auto-retry cancelled for this ticket — it's all yours.")
            else:
                await message.channel.send("No pending auto-retry for this ticket.")
        return

    channel_id = message.channel.id
    username = message.author.name
    text = message.content.strip()
    print(f"[{message.channel.name}] {username}: {text[:60]}{' (+attachment)' if message.attachments else ''}")

    csv_attachment = None
    for att in message.attachments:
        if att.filename.lower().endswith('.csv'):
            csv_attachment = att
            break

    certs = extract_certs(text) if text else []

    # A lone bare 10-digit number with no "CGC" anywhere is as likely a phone /
    # Zelle number as a CGC slab. Ask CardLadder BEFORE making a sheet — a junk
    # sheet would be customer-visible AND replace this ticket's cached sheet (so
    # the 'ship' / tracking rows would land on it). Not a CGC slab on CardLadder
    # -> the message is treated as having no cert at all (exactly what the
    # pre-CGC bot did with it); Kevin gets a quiet heads-up, the customer nothing.
    prefetched_comps = None
    if certs and cgc_needs_confirm(text, certs):
        _probe, _probe_err = [], None
        try:
            async with message.channel.typing():
                _probe = await asyncio.to_thread(lookup_comps, certs)
        except Exception as e:
            _probe_err = f"{type(e).__name__}: {str(e)[:80]}"
        if any(c.get('found') for c in _probe or []):
            prefetched_comps = _probe
        else:
            _num = cert_digits(certs[0])
            if _probe_err or not _probe or any(_comp_is_infra_error(c) for c in _probe):
                _why = _probe_err or "CardLadder page didn't load"
                await ping_kevin(
                    f"⚠️ **{username}** sent a lone number `{_num}` that might be a CGC cert, "
                    f"but the comp helper couldn't check it ({_why}). No sheet made — if it's "
                    f"a cert, have them resend it with \"CGC\" in front once the helper's back.",
                    message.channel)
            else:
                await ping_kevin(
                    f"ℹ️ **{username}** sent a lone number `{_num}` — CardLadder has no CGC "
                    f"slab with that cert, so the bot ignored it (phone / payment number?). "
                    f"No sheet made; if it IS a CGC cert, quote it by hand.",
                    message.channel)
            certs = []

    # ── WELCOME ──────────────────────────────────────────────────────────────────
    # Send the welcome EXACTLY ONCE, at the very start of the ticket. welcomed_tickets
    # is persisted to DATA_DIR (survives redeploys); the oldest-25 history scan is a
    # fallback for channels welcomed before the store existed. We also count PRIOR
    # human messages: tickets opened while the old channel-name gate ignored them
    # (username-only channels, pre-fix) already have a whole conversation and no
    # welcome — blasting WELCOME_MSG mid-negotiation and swallowing their message
    # (which is likely "ship" or a tracking number) would be wrong, so in-flight
    # channels skip the welcome and fall straight through to normal processing.
    if channel_id not in welcomed_tickets:
        already_welcomed = False
        prior_human_msgs = 0
        kevin_replied = False
        try:
            async for msg in message.channel.history(limit=25, oldest_first=True):
                if msg.author == bot.user and "Welcome to KTS Collectibles" in (msg.content or ""):
                    already_welcomed = True
                    break
                if msg.author.id == YOUR_DISCORD_USER_ID:
                    # Kevin already talking in here = genuinely in-flight ticket.
                    kevin_replied = True
                # Count only messages STRICTLY OLDER than the one being handled
                # (snowflake ids are time-ordered). Two rapid first messages would
                # otherwise each count the other as "prior" and BOTH skip the
                # welcome — permanently, since the channel gets persisted below.
                # Filter ALL bots (ticket-tool posts its own embed at open).
                if not msg.author.bot and msg.id < message.id:
                    prior_human_msgs += 1
        except Exception:
            pass
        welcomed_tickets.add(channel_id)
        _save_welcomed()
        # In-flight = Kevin already replied, or 2+ earlier customer messages.
        # A SINGLE earlier customer message with no Kevin reply is most likely a
        # message the bot missed during a deploy restart (Discord doesn't replay
        # them) — that ticket never got greeted, so still welcome it.
        if not already_welcomed and not kevin_replied and prior_human_msgs < 2:
            await asyncio.sleep(1)
            await message.channel.send(welcome_msg())
            # If they already sent certs or a CSV with their first message,
            # don't make them re-send — fall through to processing below.
            if not (certs or csv_attachment):
                return

    # ── SHOP CLOSED GATE (Kevin, Oct 5) ──────────────────────────────────────────
    # While buying is closed (!shop closed, or the SHOP_CLOSED deploy seed),
    # swallow BOTH quote paths — cert numbers and Collectr CSVs — with the
    # closed notice instead of a sheet/comps/offer. Sits AFTER the welcome
    # (ticket bookkeeping stays intact) and BEFORE the quote paths; everything
    # below the quote paths (proceed/shipping/tracking for lots quoted before
    # the close) is deliberately NOT gated.
    if (certs or csv_attachment) and shop_is_closed():
        await message.channel.send(SHOP_CLOSED_MSG)
        _what = (f"{len(certs)} cert(s)" if certs else "a Collectr CSV") + \
                (" + a Collectr CSV" if certs and csv_attachment else "")
        await ping_kevin(
            f"🔒 Shop closed — **{username}** submitted {_what}; no quote sent. "
            f"They'll need a fresh submission when you reopen (`!shop open`).",
            message.channel
        )
        return

    # ── COLLECTR CSV (ONE PIECE + POKÉMON singles) ───────────────────────────────
    if csv_attachment:
        async with message.channel.typing():
            try:
                csv_bytes = await csv_attachment.read()
                result, error = parse_collectr_csv(csv_bytes)
                if error:
                    await message.channel.send(f"Couldn't read that file — {error}. Try re-exporting from Collectr.")
                    raise _CsvDone()

                # BLOCK UNTIL CLEAN: any violating row below stops the quote for
                # the WHOLE export — the seller fixes their Collectr list and
                # re-uploads; we never silently skip rows or partially quote.
                issues = result.get("issues", [])
                for issue_type, cards in issues:
                    card_list = "\n".join(cards[:5])
                    if len(cards) > 5:
                        card_list += f"\n• ...and {len(cards)-5} more"
                    if issue_type == "onepiece_off":
                        await message.channel.send(
                            f"❌ **We're not buying raw singles this weekend — PSA graded slabs only.**\n\n"
                            f"Detected One Piece cards in your CSV:\n{card_list}\n\n"
                            f"We're buying **PSA slabs** (Pokémon, Basketball, Baseball & Football) — "
                            f"drop your cert numbers here! 🙏"
                        )
                    elif issue_type == "pokemon_off":
                        await message.channel.send(
                            f"❌ **We're not buying Pokémon raw cards this weekend.**\n\n"
                            f"Detected Pokémon cards in your CSV:\n{card_list}\n\n"
                            f"We're currently buying **One Piece raw singles** (English, NM, $1–$150). "
                            f"We're still happy to look at any **PSA graded Pokémon slabs** you have — "
                            f"just drop your cert numbers here! 🙏"
                        )
                    elif issue_type == "other_game":
                        await message.channel.send(
                            f"❌ **We only buy One Piece raw singles right now:**\n{card_list}\n\n"
                            f"Please remove these and re-export."
                        )
                    elif issue_type == "non_english":
                        await message.channel.send(
                            f"❌ **Non-English cards — we only buy English cards:**\n{card_list}\n\n"
                            f"Please remove these and re-export."
                        )
                    elif issue_type == "over_max":
                        await message.channel.send(
                            f"❌ **One Piece cards over ${RAW_MAX_PRICE} — we can't buy these as raws:**\n{card_list}\n\n"
                            f"Our One Piece limit is **${RAW_MIN_PRICE}–${RAW_MAX_PRICE} per card**. Remove these and re-export."
                        )
                    elif issue_type == "under_min":
                        await message.channel.send(
                            f"❌ **One Piece cards under ${RAW_MIN_PRICE} — we can't buy these:**\n{card_list}\n\n"
                            f"Our One Piece minimum is **${RAW_MIN_PRICE} per card**. Remove these and re-export."
                        )
                    elif issue_type == "pk_price":
                        await message.channel.send(
                            f"❌ **Pokémon cards outside our ${POKEMON_RAW_MIN_PRICE}–${POKEMON_RAW_MAX_PRICE} range:**\n{card_list}\n\n"
                            f"We're only buying Pokémon singles priced **${POKEMON_RAW_MIN_PRICE}–${POKEMON_RAW_MAX_PRICE} per card** "
                            f"this weekend. Remove these and re-export."
                        )
                    elif issue_type == "pk_condition":
                        await message.channel.send(
                            f"❌ **Pokémon cards not marked Near Mint:**\n{card_list}\n\n"
                            f"We only buy **Near Mint** Pokémon singles. Remove these and re-export."
                        )
                    elif issue_type == "pk_graded":
                        await message.channel.send(
                            f"❌ **Graded cards in your raw-singles export:**\n{card_list}\n\n"
                            f"Graded cards go through our **PSA slab** flow instead — remove them from the CSV "
                            f"and drop the **cert numbers** here so we can price them as slabs! 🙏"
                        )
                    elif issue_type == "pk_master_ball":
                        await message.channel.send(
                            f"❌ **Master Ball versions — we're not buying these:**\n{card_list}\n\n"
                            f"Please remove them and re-export."
                        )
                    elif issue_type == "pk_sealed":
                        await message.channel.send(
                            f"❌ **Sealed products — we only buy raw singles through this flow:**\n{card_list}\n\n"
                            f"Boxes, tins, bundles, decks, and other sealed items can't go in your "
                            f"Collectr singles export. Remove these and re-export."
                        )
                    elif issue_type == "pk_trainer":
                        await message.channel.send(
                            f"❌ **These aren't Pokémon-character cards we can buy (trainers/items/energy):**\n{card_list}\n\n"
                            f"We're only buying **Pokémon character cards** — no trainer cards at all. "
                            f"If something here IS actually a Pokémon card, let us know and Kevin will take a look! 🙏\n"
                            f"Otherwise, remove these and re-export."
                        )
                    elif issue_type == "pk_old_set":
                        await message.channel.send(
                            f"❌ **Pokémon cards from pre-2022 sets:**\n{card_list}\n\n"
                            f"We're only buying Pokémon singles from **2022 or newer sets** this weekend. "
                            f"Remove these and re-export."
                        )
                if issues:
                    await ping_kevin(
                        f"⚠️ **Collectr rejected — {username}**\n" +
                        "\n".join([f"• {t}: {len(c)} cards" for t, c in issues]),
                        message.channel
                    )
                    # Make sure a lot_state entry exists so a later "proceed" hits
                    # the below-minimum gate instead of failing open and sending
                    # the shipping address for a lot we just rejected. setdefault
                    # only — never clobbers a previously recorded valid lot.
                    _lot_entry(channel_id)
                    raise _CsvDone()

                total = result["total"]
                card_count = result["card_count"]
                game_totals = result.get("game_totals") or {'one piece': float(total)}
                game_counts = result.get("game_counts") or {'one piece': card_count}

                # Price each game's portion on its OWN tier: One Piece hits 88%
                # at $10k+ of One Piece, Pokémon at $5k+ of Pokémon.
                GAME_LABELS = {'one piece': 'One Piece', 'pokemon': 'Pokémon'}
                portions = []   # (game, count, value, rate, payout)
                for _g in ('one piece', 'pokemon'):
                    _val = float(game_totals.get(_g, 0))
                    if _val <= 0:
                        continue
                    _rate, _ = get_raw_rate(_g, _val, username)
                    portions.append((_g, int(game_counts.get(_g, 0)), _val, _rate, _val * _rate))
                payout = sum(p[4] for p in portions)
                # rate is only meaningful for a single-game lot; None when mixed.
                rate = portions[0][3] if len(portions) == 1 else None
                last_offer[channel_id] = {"payout": payout, "total": total, "rate": rate}

                # Record per-game singles value toward the buying minimums.
                set_singles_value(channel_id, game_totals)
                _sc, _sv, combined_singles, combined = lot_summary(channel_id)

                # Save CSV to Google Drive
                try:
                    import urllib.request as urlreq
                    csv_text = csv_bytes.decode('utf-8', errors='replace')
                    post_data = json.dumps({
                        "username": username,
                        "csv": csv_text,
                        "folder_id": COLLECTR_FOLDER_ID
                    }).encode('utf-8')
                    req = urlreq.Request(
                        APPS_SCRIPT_URL,
                        data=post_data,
                        headers={"Content-Type": "application/json"}
                    )
                    # Off the event loop — a slow Apps Script cold start would
                    # otherwise freeze every other ticket for up to 15s.
                    await asyncio.to_thread(urlreq.urlopen, req, timeout=15)
                except Exception as e:
                    print(f"CSV Drive save error (non-critical): {e}")

                hold_tail = proceed_or_hold_tail(channel_id)
                if len(portions) <= 1:
                    # Single-game lot — keep the long-standing message format.
                    _pct = int(round((rate or 0) * 100))
                    offer_body = (
                        f"✅ **Your offer:**\n\n"
                        f"📦 **{card_count} cards** | Market value: **${total:,.2f}**\n"
                        f"💰 **Payout: ${payout:,.2f}** ({_pct}%)\n\n"
                    )
                else:
                    # Mixed lot — each game's portion is priced on its own tier.
                    _lines = ["✅ **Your offer:**", ""]
                    for _g, _cnt, _val, _rate, _pay in portions:
                        _lines.append(
                            f"📦 **{GAME_LABELS[_g]}** — {_cnt} cards | Market value: **${_val:,.2f}** | "
                            f"**${_pay:,.2f}** ({int(round(_rate*100))}%)"
                        )
                    _lines += ["", f"💰 **Total payout: ${payout:,.2f}**", ""]
                    offer_body = "\n".join(_lines)
                offer_body += hold_tail if hold_tail else "Let me know if you'd like to proceed!"
                for chunk in _split_for_discord(offer_body):
                    await message.channel.send(chunk)
                kevin_prefix = "" if lot_qualifies(channel_id) else f"⏳ **[BELOW MINIMUM — HOLD]** "
                games_label = " + ".join(GAME_LABELS[p[0]] for p in portions) or "Collectr"
                if len(portions) <= 1:
                    kevin_body = (
                        f"{card_count} cards | ${total:,.2f} market | {int(round((rate or 0)*100))}% | **${payout:,.2f}**"
                    )
                else:
                    kevin_body = " • ".join(
                        f"{GAME_LABELS[_g]}: {_cnt} cards | ${_val:,.2f} @ {int(round(_rate*100))}% = ${_pay:,.2f}"
                        for _g, _cnt, _val, _rate, _pay in portions
                    ) + f" • **total ${payout:,.2f}**"
                kevin_msg = (
                    f"{kevin_prefix}💚 **Collectr offer sent — {username}** ({games_label})\n"
                    + kevin_body
                    + (f" | combined ${combined:,.2f}" if combined != total else "")
                )
                top = "\n".join(result["top_cards"][:3]) if result["top_cards"] else ""
                if top:
                    kevin_msg += f"\n{top}"
                await ping_kevin(kevin_msg, message.channel)

            except _CsvDone:
                pass   # early exit from the CSV block — not an error
            except Exception as e:
                print(f"Collectr error: {e}")
                await message.channel.send("Had an issue with that file — Kevin will take a look!")
                await ping_kevin(f"⚠️ Collectr error — **{username}**: {str(e)}", message.channel)
        # A message can carry BOTH a Collectr CSV and PSA cert numbers. The CSV was
        # handled above; fall through so the certs still get a sheet + comps (they
        # used to be silently dropped, and the lot then mis-gated as singles-only).
        if not certs:
            return

    # ── PSA CERT NUMBERS ─────────────────────────────────────────────────────────
    if certs:
        async with message.channel.typing():
            try:
                # If a background comp retry is pending and this submission covers
                # its certs (a re-send / correction), cancel it NOW — before our
                # own lookup even starts — so the retry can't race us and post a
                # duplicate quote. A submission of DIFFERENT certs leaves the
                # retry running: that batch is additive and still owed its quote.
                _pending = pending_comp_retries.get(channel_id)
                if _pending and set(_pending.get("certs", [])) <= set(certs):
                    cancel_comp_retry(channel_id, "covered by a live re-submission")
                await message.channel.send(
                    f"Got it! Setting up your buying sheet for {len(certs)} cert{'s' if len(certs) > 1 else ''}... ⏳"
                )
                # Off the event loop — Apps Script sheet creation can take many
                # seconds and would freeze every other ticket while it runs.
                sheet_url, sheet_name, data = await asyncio.to_thread(create_psa_sheet, username, certs)
                sheet_id = data.get("sheet_id") or extract_sheet_id(sheet_url)
                if sheet_id:
                    remember_channel_sheet(channel_id, sheet_id)
                    record_ticket_sheet(channel_id, sheet_id, username,
                                        message.author.id == YOUR_DISCORD_USER_ID)
                    # Stamp VIP formulas at CREATION time too: if the helper is
                    # down and Kevin ends up quoting this sheet by hand (or the
                    # retry exhausts), the sheet must already carry their rates.
                    _vt = vip_pokemon_tiers(username)
                    _vs = vip_sport_rates(username)
                    if _vt or _vs:
                        try:
                            await asyncio.to_thread(write_custom_sheet_formulas, sheet_id, _vt, _vs)
                        except Exception as e:
                            print(f"VIP creation-time stamp failed (quote-time stamp retries): {e}")

                # Tell the customer the sheet is ready BEFORE the slow helper call.
                # Helper lookup for 50 certs can be 1-2 min; don't make them wait.
                await message.channel.send(
                    f"✅ Sheet ready! Pulling CardLadder comps now... ⏳\n\n"
                    f"📊 {sheet_url}"
                )

                # Now look up comps and price the lot (slow path).
                comps = []
                comp_error = None
                if prefetched_comps is not None:
                    comps = prefetched_comps   # the lone-number CardLadder check above
                else:
                    try:
                        comps = await asyncio.to_thread(lookup_comps, certs)
                    except Exception as e:
                        comp_error = str(e)
                        print(f"Helper comp lookup error: {e}")

                await quote_or_pend_live(message.channel, channel_id, username, certs,
                                         comps, comp_error, sheet_id, sheet_url)
            except Exception as e:
                print(f"Sheet error: {e}")
                await message.channel.send("Small hiccup — Kevin will set this up manually and be right with you!")
                await ping_kevin(
                    f"⚠️ Sheet failed — **{username}**\nCerts: {', '.join(cert_label(c) for c in certs)}\nError: {str(e)}",
                    message.channel
                )
        return

    # ── AGREED / SHIPPING ─────────────────────────────────────────────────────────
    if is_agreeing(text):
        # Whose deal is this? When KEVIN types "ship" in a customer's ticket,
        # username is HIS name — resolve the ticket's customer (channel overwrites
        # first, sheet names as a fallback) so the lot rebuild, ship-list record,
        # sheet lookup and pings are about the customer (Oct 18: yungturtle's NBA
        # lot got the VA address).
        kevin_typed = message.author.id == YOUR_DISCORD_USER_ID
        lot_wiped = channel_id not in lot_state
        cust_errs, cust_from_sheets, drive_tried = [], False, False
        if not kevin_typed:
            customer = username
        else:
            # Discord first: the ticket opener's member overwrite (no network call).
            customer = ticket_customer_from_overwrites(message.channel)
            # The sheet-name scan costs Drive calls — only spend them BEFORE the
            # address when a wiped lot needs a name to rebuild from.
            if customer is None and lot_wiped:
                drive_tried = True
                customer = await customer_username_for_channel(message.channel, errors=cust_errs)
                cust_from_sheets = customer is not None
        who = customer or f"#{message.channel.name}"
        typed_by = f"You typed 'ship' for {who}" if kevin_typed else f"{username} typed 'ship'"
        # A restart wiped this ticket's lot: rebuild it from this ticket's sheets
        # (the customer's, plus the cached one even if Kevin pasted it) so
        # Sports/One Piece still route to Doral.
        rebuild_err = None
        if lot_wiped and customer:
            try:
                await asyncio.to_thread(
                    rebuild_lot_from_sheets, channel_id, customer,
                    created_after=getattr(message.channel, 'created_at', None),
                    extra_sheet_id=[channel_sheet.get(channel_id)] + kevin_pasted_sheet_ids(channel_id))
            except Exception as e:
                rebuild_err = f"{type(e).__name__}: {str(e)[:80]}"
                print(f"lot rebuild from sheets failed for {customer}: {e}")
        # Nothing rebuilt -> compose_shipping fails open to the VA address. A
        # customer-typed ship already says so in knote; when Kevin typed it, say
        # why and that the address needs a second look.
        rebuild_warn = ""
        if kevin_typed and lot_wiped and channel_id not in lot_state:
            _err = rebuild_err or (cust_errs[0] if cust_errs else None)
            if _err:
                _why = f"Drive/Sheets error: {_err}"
            elif customer:
                _why = f"no priced rows on {customer}'s sheets from this ticket"
            else:
                _why = "couldn't tell whose ticket this is"
            rebuild_warn = (f" ⚠️ Lot was wiped by a restart and couldn't be rebuilt ({_why})"
                            " — they got the default VA address; double-check it "
                            "(Sports/One Piece ship to Doral).")
        # Legacy combined-lot minimum (currently MIN_LOT_VALUE=0, so inert) — still
        # honored if ever re-enabled. Fail OPEN with no recorded lot (redeploy wiped
        # in-memory state) so we never stonewall a seller who already got an offer.
        if channel_id in lot_state and not lot_qualifies(channel_id):
            _sc, _sv, _si, _cb = lot_summary(channel_id)
            await message.channel.send(proceed_or_hold_tail(channel_id))
            await ping_kevin(
                f"⏳ **{typed_by} but lot is below minimum** "
                f"({_sc} slab(s), combined ${_cb:,.2f}) — address NOT sent.",
                message.channel
            )
            return
        # Lane-aware: Pokémon → VA (no min); Sports/One Piece → Doral ($1,500+).
        ship_msg, gave_addr, knote = compose_shipping(channel_id)
        await message.channel.send(ship_msg)
        if kevin_typed and customer is None and not drive_tried:
            # The address is out — now a Drive scan can name the customer for the
            # ping, ship-list record and tracking row without delaying it.
            customer = await customer_username_for_channel(message.channel)
            cust_from_sheets = customer is not None
            who = customer or f"#{message.channel.name}"
            typed_by = f"You typed 'ship' for {who}"
        if cust_from_sheets:
            rebuild_warn += (" ℹ️ Customer matched by sheet name (no ticket-opener "
                             "overwrite on the channel) — double-check it's them.")
        if not gave_addr:
            # Sports/OP-only under the $1,500 minimum — held, no address sent.
            await ping_kevin(
                f"⏳ **{typed_by} — {knote}.** Address NOT sent.{rebuild_warn}",
                message.channel
            )
            return
        # Snapshot the accepted ship-list for the local ticket tracker (GET /tickets).
        try:
            _rec = record_accepted_ship_list(channel_id, customer or message.channel.name)
        except Exception as e:
            _rec = None
            print(f"ship-list record failed (non-critical): {e}")
        _rec_note = (f" 📦 Ship-list recorded ({len(_rec['certs'])} certs) — tracker → Sync from bot."
                     if _rec else " ⚠️ No lot on record to snapshot (redeploy?) — add this one to the tracker manually.")
        _by_note = " (you typed 'ship')" if kevin_typed else ""
        await ping_kevin(f"✅ **{who} agreed**{_by_note} — shipping sent ({knote}).{_rec_note}{rebuild_warn}",
                         message.channel)
        # Resolve the sheet through the cache + Drive fallback (channel_sheet used
        # to be memory-only, so a redeploy between quote and "ship" silently
        # skipped the tracking row). Never search by Kevin's own name — it could
        # match a sheet of certs he pasted elsewhere.
        try:
            ship_sheet_id = await find_sheet_for_channel(
                channel_id, message.channel.name, customer or "", message.channel.created_at)
        except Exception as e:
            ship_sheet_id = None
            print(f"ship: sheet lookup failed for {who} (non-critical, no tracking row): {e}")
        if ship_sheet_id:
            try:
                import urllib.request as urlreq
                post_data = json.dumps({
                    "action": "add_tracking",
                    "sheet_id": ship_sheet_id,
                    "username": customer or message.channel.name
                }).encode("utf-8")
                req = urlreq.Request(
                    APPS_SCRIPT_URL,
                    data=post_data,
                    headers={"Content-Type": "application/json"}
                )
                await asyncio.to_thread(urlreq.urlopen, req, timeout=15)
                print(f"Added tracking row for {who}")
            except Exception as e:
                print(f"Tracking row error (non-critical): {e}")
        return

    # ── TRACKING NUMBER ───────────────────────────────────────────────────────────
    # A bare 1401… 13-digit number is a CGC cert (one extract_certs passed over,
    # e.g. in a chatty message) — never save it as the ticket's tracking number.
    tracking_match = next(
        (m for m in re.finditer(r'\b([0-9]{20,22}|1Z[A-Z0-9]{16}|[0-9]{12,15})\b', text)
         if not _looks_like_cgc_13(m.group(1))), None)
    if tracking_match:
        tracking_num = tracking_match.group(1)
        # Cache + exact-prefix Drive fallback. The old inline fallback searched
        # `name contains '<channel name>'`: it never matched legacy ticket-<name>
        # channels (sheets are named from the bare username) and on short usernames
        # could match — and silently write tracking into — the WRONG customer's
        # sheet. find_sheet_for_channel handles both; if it can't find a confident
        # match it returns None and we ping Kevin instead of guessing.
        # Kevin typing a customer's tracking number: search by the CUSTOMER, never
        # by his own name — that matched (and cached for this ticket) his newest
        # pasted sheet, possibly from another customer's ticket.
        _track_user = username
        if message.author.id == YOUR_DISCORD_USER_ID:
            _track_user = ticket_customer_from_overwrites(message.channel) or ""
        sheet_id = await find_sheet_for_channel(
            channel_id, message.channel.name, _track_user, message.channel.created_at)
        if sheet_id:
            try:
                import urllib.request as urlreq
                post_data = json.dumps({
                    "action": "update_tracking",
                    "sheet_id": sheet_id,
                    "tracking": tracking_num
                }).encode("utf-8")
                req = urlreq.Request(
                    APPS_SCRIPT_URL,
                    data=post_data,
                    headers={"Content-Type": "application/json"}
                )
                await asyncio.to_thread(urlreq.urlopen, req, timeout=15)
                print(f"Saved tracking {tracking_num} for {username}")
            except Exception as e:
                print(f"Tracking save error (non-critical): {e}")
        else:
            await ping_kevin(
                f"⚠️ Tracking number `{tracking_num}` from **{username}** in "
                f"#{message.channel.name} — couldn't find their buying sheet, "
                f"add it to the sheet manually.",
                message.channel
            )
        return

    # ── STAY SILENT ───────────────────────────────────────────────────────────────

# ── RUN ──────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Starting KTS Collectibles Bot...")
    bot.run(DISCORD_BOT_TOKEN)
