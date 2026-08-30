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
from datetime import datetime, date
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
# Per-sport price ceilings — sports not listed here are rejected outright, and any
# slab priced ABOVE its ceiling is rejected (Aug 28 weekend flyer).
# pokemon $5,000 · NBA $500 · MLB $600 · NFL $1,600 (NFL also has a $300
# FLOOR — see NFL_MIN_PRICE). ONE PIECE fully OFF this weekend (slabs AND
# raw singles — absent from the flyer, confirmed by Kevin).
PSA_SPORT_MAX_PRICE = {
    'pokemon': 5000,
    'basketball': 500,
    'mlb': 600,
    'football': 1600,
}
NFL_MIN_PRICE = 300   # NFL floor (Aug 28): under $300 rejected
# Pokémon buy map (Aug 28 flyer): $1-$140 (PSA 7+, cert# must be 7+ DIGITS,
# CL 4+ eyeballed) and $3,500-$5,000 (ANY grade, CL 3+ eyeballed — quoted at
# the same rate but FLAGGED to Kevin as big-ticket). Dead zone between.
POKEMON_GAP = (140, 3500)     # exclusive bounds: 140 < value < 3500 → rejected
POKEMON_BIG_BAND_MIN = 3500   # $3,500-$5,000: any grade, quoted + ⚠️ flagged
POKEMON_MIN_CERT_DIGITS = 7   # flyer "Cert #s 7+": 7+ digit certs only ($1-140 band)
# (Pikachu lane REMOVED Aug 11 per Kevin — Pikachus follow standard Pokémon
# rules; $5k-$20k Pikachus land in the big-ticket review band like everything
# else.)
# Pokémon grades (Kevin, Aug 11): the whole $1-$200 band is PSA 7+.
# Big-ticket $5k-$20k = any grade (Kevin reviews).
# One Piece slabs (Aug 12): PSA 7+ across the whole (only) $1-$100 band —
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

# ── PER-CARD PAYOUT TIERS BY SPORT (Aug 7 weekend flyer) ──────────────────────────
# Low end of each flyer range. Grade, reject-zones, and the Pokémon manual bucket
# live in classify_psa_comp, so only in-band cards reach these blends. CL-confidence
# requirements can't be enforced (no CL score in the data) — priced on value+grade.
# Pokémon (Aug 28): $1-$140 → 85% (flyer 85-90 — pay the low end),
# $3,500-$5,000 → 85% (flyer 85-90). Gap enforced in classify_psa_comp.
PSA_POKEMON_PER_CARD_TIERS = [
    (0,    140.01,        0.85),   # $1-$140 → 85%  (.01 so exactly $140 is 85%)
    (3500, float('inf'),  0.85),   # $3,500-$5,000 → 85%  ($5,000 ceiling)
]
# Basketball (NBA, Aug 7): $1-$30 → 100% (flyer 100-105), $30-$200 → 95%.
# ANY grade, one sale ever. Ceiling $200.
# NBA (Aug 28): $1-$500 flat 95%, PSA 7+ (any-grade waiver OFF), one sale
# ever. $100+ "CL 3+" is eyeballed. Ceiling $500.
PSA_BASKETBALL_PER_CARD_TIERS = [
    (0, float('inf'), 0.95),   # $1-$500 → 95%  ($500 ceiling rejects above)
]
# MLB (Aug 28 — BACK for the first time since July): $1-$600 flat 90%,
# PSA 7+, direct CardLadder value (no avg-3). $100+ "CL 3+" eyeballed.
PSA_MLB_PER_CARD_TIERS = [
    (0, float('inf'), 0.90),   # $1-$600 → 90%  ($600 ceiling rejects above)
]
# One Piece slabs (Aug 12): $1-$100 → 86% ("86-87 as well" — bot quotes the
# low end). Nothing above $100. PSA 7+ (generic floor), sale ≤2mo.
PSA_ONE_PIECE_PER_CARD_TIERS = [
    (0, 100.01, 0.86),   # $1-$100 → 86%  (.01 so exactly $100 is 86%)
]
# NFL (Aug 28): $300-$1,600 flat 90% (CL 3+ eyeballed). FLOOR $300
# (NFL_MIN_PRICE), ceiling $1,600.
PSA_FOOTBALL_PER_CARD_TIERS = [
    (0, float('inf'), 0.90),   # $300-$1,600 → 90% (floor+ceiling in classify)
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
    """Display name for a canonical sport key (e.g. 'mlb' -> 'MLB')."""
    return {'mlb': 'MLB', 'one piece': 'One Piece'}.get(sport, (sport or '').title())


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
    Jul 31 weekend rates:
    - pokemon: $1-100 → 89%, $100-150 → 87%, $150-750 → 84% PSA 8-10 (ceiling $750).
    - basketball: $1-200 → 95% any grade (ceiling $200).
    - one piece (PSA 8-10): $1-100 → 88%, $101-750 → 83% (ceiling $750).
    - football (NFL): $1-30 → 100%, $30-100 → 92%, $100-200 → 90% (ceiling $200).
    - mlb: NOT BUYING (rejected in classify_psa_comp).
    Reject-zones / ceilings / grade floors are filtered in classify_psa_comp, so
    only in-band cards reach these blends.
    """
    if sport == 'pokemon':
        return _blended_per_card_rate(PSA_POKEMON_PER_CARD_TIERS, card_values)
    if sport == 'basketball':
        return _blended_per_card_rate(PSA_BASKETBALL_PER_CARD_TIERS, card_values)
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

def _find_fresh_psa_sheet(username, since_dt):
    """Drive fallback for create_psa_sheet: the Apps Script usually FINISHES
    creating the sheet server-side even when our HTTP call to it dies (Aug 2
    morning: a transient Google 404 and a client timeout — both sheets existed
    in Drive anyway). Find a '<username> discord ...' sheet created after we
    started calling, so a retry doesn't make a duplicate."""
    try:
        drive = get_drive_service()
        safe = username.replace("\\", "\\\\").replace("'", "\\'")
        since_iso = since_dt.strftime("%Y-%m-%dT%H:%M:%S")
        q = (f"name contains '{safe} discord' and '{PSA_FOLDER_ID}' in parents "
             f"and trashed=false and createdTime > '{since_iso}'")
        files = drive.files().list(q=q, orderBy="createdTime desc", pageSize=5,
                                   fields="files(id,name,createdTime)").execute().get("files", [])
        for f in files:
            if f["name"].lower().startswith(username.lower()):
                return f
    except Exception as e:
        print(f"Drive fallback search failed: {e}")
    return None

def create_psa_sheet(username, cert_numbers):
    """Create a buying sheet by calling the Google Apps Script web app.
    Google gets slow/flaky under weekend-morning load: a 103-cert sheet took
    54s on a HEALTHY run (the old 30s timeout could never survive it), and
    failed calls usually still created their sheet server-side. So: generous
    timeout; on failure poll Drive for the sheet Google may have finished
    anyway (returns it with no duplicate); only then retry the call once."""
    import urllib.request
    import urllib.parse
    import time as _time
    from datetime import timedelta as _td
    certs_str = ",".join([str(c).strip() for c in cert_numbers])
    params = urllib.parse.urlencode({"username": username, "certs": certs_str, "folder_id": PSA_FOLDER_ID})
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
                f = _find_fresh_psa_sheet(username, started)
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

def _lookup_comps_once(cert_list):
    """Single pass over the helper in chunks; merged results list. Per-chunk
    failures are logged; if EVERY chunk fails the last error is raised."""
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
            all_results.extend(data.get("results", []))
            print(f"Helper chunk {idx}/{len(chunks)} ok ({len(chunk)} certs)")
        except Exception as e:
            last_error = e
            print(f"Helper chunk {idx}/{len(chunks)} failed: {e}")
    if not all_results and last_error:
        raise last_error
    return all_results

def lookup_comps(certs):
    """
    Call helper.ktscollectibles.com/comp/batch and return the merged results list.
    Each result: {cert, found, clValue, cardName, grade, recentSales, avg3Sales, salesCount, note}
    Returns [] if the helper key isn't set; raises only if every chunk fails.
    Serialized across tickets via _HELPER_LOCK. Bad results (missing / found=false /
    no value) from a PARTIALLY successful pass get one purge-cache-and-retry pass —
    if everything came back bad the helper itself is down or rate-limited, and a
    second hammering would only make that worse.
    """
    if not HELPER_API_KEY or not certs:
        return []
    import time as _time
    cert_list = list(dict.fromkeys(str(c).strip() for c in certs))
    with _HELPER_LOCK:
        results = _lookup_comps_once(cert_list)
        by_cert = {str(c.get('cert', '')).strip(): c for c in results}
        bad = [c for c in cert_list if _comp_is_bad(by_cert.get(c))]
        if bad and len(bad) < len(cert_list):
            print(f"Comp second pass: {len(bad)}/{len(cert_list)} bad results — purging cache, retrying")
            _purge_helper_cache(bad)
            _time.sleep(5)
            try:
                for c in _lookup_comps_once(bad[:100]):
                    k = str(c.get('cert', '')).strip()
                    if k and (k not in by_cert or not _comp_is_bad(c)):
                        by_cert[k] = c
            except Exception as e:
                print(f"Comp second pass failed (keeping first-pass results): {e}")
        return [by_cert[k] for k in cert_list if k in by_cert]


def classify_psa_comp(comp):
    """
    Decide whether a helper comp result fits KTS PSA buying criteria.
    Returns (status, reason): 'accepted'/None or 'rejected'/<short reason>.
    """
    if not comp or not comp.get('found'):
        return ('rejected', 'no comp on CardLadder')
    cv = comp.get('clValue')
    try:
        cv = float(cv) if cv is not None else None
    except (TypeError, ValueError):
        cv = None
    if cv is None:
        return ('rejected', 'no CL value')
    sport = normalize_sport(comp.get('sport'))
    max_price = PSA_SPORT_MAX_PRICE.get(sport)
    if max_price is None:
        sport_label = sport or 'unknown sport'
        return ('rejected', f"{sport_label} (we only buy pokemon, basketball, baseball, and football slabs right now)")
    if cv > max_price:
        # Two decimals so a $520.40 NBA card reads "over our $500 max" sensibly.
        return ('rejected', f"${cv:,.2f} (over our ${max_price:,} {sport} max)")
    if cv < PSA_MIN_PRICE:
        return ('rejected', f"${cv:.2f} (under ${PSA_MIN_PRICE} min)")
    # NFL has a $300 floor this weekend.
    if sport == 'football' and cv < NFL_MIN_PRICE:
        return ('rejected', f"${cv:,.2f} (under our ${NFL_MIN_PRICE} NFL minimum)")
    # Pokémon dead zone between the two bands.
    if sport == 'pokemon' and POKEMON_GAP[0] < cv < POKEMON_GAP[1]:
        return ('rejected',
                f"${cv:,.2f} (outside our Pokémon buy ranges — "
                f"$1-${POKEMON_GAP[0]} and ${POKEMON_GAP[1]:,}-${max_price:,} only)")
    # Pokémon $1-$140 band: cert number must be 7+ digits (flyer "Cert #s 7+").
    pokemon_big = (sport == 'pokemon' and cv >= POKEMON_BIG_BAND_MIN)
    if (sport == 'pokemon' and not pokemon_big
            and len(str(comp.get('cert', '')).strip()) < POKEMON_MIN_CERT_DIGITS):
        return ('rejected',
                f"cert {comp.get('cert')} ({len(str(comp.get('cert', '')).strip())} digits — "
                f"we need {POKEMON_MIN_CERT_DIGITS}+ digit cert numbers under $140)")
    grade_raw = str(comp.get('grade') or '').replace('PSA', '').strip()
    try:
        g = float(grade_raw)
    except ValueError:
        return ('rejected', f"grade '{grade_raw}' unrecognized")
    # Grade floor PSA 7+ everywhere EXCEPT the Pokémon $3,500-$5,000 band
    # (flyer: any grade there).
    if not pokemon_big and g < PSA_MIN_GRADE:
        return ('rejected', f"PSA {grade_raw} (we buy {PSA_MIN_GRADE}-10 only)")
    last_sale = comp.get('lastSaleDate')
    if not last_sale:
        return ('rejected', 'no recent sale visible')
    try:
        last_d = datetime.strptime(str(last_sale).strip(), '%Y-%m-%d').date()
    except (ValueError, AttributeError):
        return ('rejected', f"unparseable sale date '{last_sale}'")
    # Sale-age limit (days). pokemon: 2 months (carried forward — flyer silent).
    # NBA: one sale ever. MLB: $1-100 one sale ever, else 90d. NFL: 90d.
    if sport == 'pokemon':
        max_age = 60
    elif sport == 'basketball':
        max_age = float('inf')
    elif sport in ('football', 'mlb'):
        max_age = float('inf') if cv <= 100 else 90
    else:
        max_age = PSA_SPORT_MAX_AGE_DAYS.get(sport, PSA_MAX_AGE_DAYS)
    if (date.today() - last_d).days > max_age:
        return ('rejected', f"last sale {last_sale} (>{max_age:g}d ago)")

    # NBA player/set bans still apply.
    if sport == 'basketball':
        return check_basketball_rejections(comp)

    # Pokémon $3,500-$5,000: quoted at the band rate but FLAGGED so Kevin
    # eyeballs CL 3+ before paying (bot can't read CL confidence).
    if pokemon_big:
        return ('flag', f"${cv:,.0f} big-ticket Pokémon — eyeball CL 3+ before paying")


    return ('accepted', None)


def fill_buying_sheet(sheet_id, comps, sheet_name="Form. Put Date Here."):
    """
    Populate the freshly-created buying sheet with everything the user normally sees:
      A = "PSA", B = cert (already there), C = HYPERLINK to Cardladder,
      D = card name, E = grade, F = sport, G = CL Value, J = last sale date.
    Replicates the existing onEdit + cardladder-comp behavior since simple onEdit
    doesn't fire on programmatic writes (so the sheet copy lands empty otherwise).
    """
    if not comps:
        return
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
        if v.isdigit() and 6 <= len(v) <= 12:
            cert_to_row[v] = i + 1
    updates = []
    for c in comps:
        cert = str(c.get('cert', '')).strip()
        row = cert_to_row.get(cert)
        if not row:
            continue
        link_url = f"https://app.cardladder.com/search?cert={cert}&grader=psa"
        updates.append({'range': f'A{row}', 'values': [["PSA"]]})
        updates.append({'range': f'C{row}', 'values': [[f'=HYPERLINK("{link_url}","🔗 CardLadder")']]})
        if c.get('cardName'):
            updates.append({'range': f'D{row}', 'values': [[c['cardName']]]})
        if c.get('grade'):
            grade_clean = str(c['grade']).replace('PSA ', '').replace('PSA', '').strip()
            updates.append({'range': f'E{row}', 'values': [[grade_clean]]})
        if c.get('sport'):
            updates.append({'range': f'F{row}', 'values': [[str(c['sport']).lower().strip()]]})
        if c.get('found') and c.get('clValue') is not None:
            updates.append({'range': f'G{row}', 'values': [[c['clValue']]]})
        else:
            updates.append({'range': f'G{row}', 'values': [["not found"]]})
        if c.get('lastSaleDate'):
            updates.append({'range': f'J{row}', 'values': [[c['lastSaleDate']]]})
    if updates:
        sheet.batch_update(updates, value_input_option="USER_ENTERED")


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
    cert_set = {str(c).strip() for c in certs}
    orange = {"backgroundColor": {"red": 1.0, "green": 0.6, "blue": 0.0}}
    for i, val in enumerate(sheet.col_values(2)):  # column B = cert
        if str(val).strip() in cert_set:
            sheet.format(f"A{i + 1}:J{i + 1}", orange)


# ── CERT EXTRACTION ──────────────────────────────────────────────────────────────
def extract_certs(text):
    if not text:
        return []
    # Strip out any URLs FIRST. GIF links (Tenor, Giphy, etc.) end in long numeric
    # IDs (e.g. tenor.com/view/funny-cat-gif-25649837) that otherwise get mistaken
    # for cert numbers. A real cert is always sent as a plain number, never in a link.
    text = re.sub(r'https?://\S+', ' ', text)
    text = re.sub(r'www\.\S+', ' ', text)
    numbers = re.findall(r'\b\d{7,9}\b', text)
    if not numbers:
        return []
    stripped = re.sub(r'\d', '', text).strip()
    words = [w for w in stripped.split() if re.search(r'[a-zA-Z]', w)]
    if len(words) > 3:
        return []
    seen = set()
    unique = []
    for n in numbers:
        if n not in seen:
            seen.add(n)
            unique.append(n)
    return unique


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
    # wise desynchronize them across the awaits below).
    vip_tiers = vip_pokemon_tiers(username)
    # Value adjustments BEFORE filling the sheet or classifying, so the sheet's
    # "Our comp" and all pricing use the adjusted value.
    # MLB uses the DIRECT CardLadder value (the min(avg-3-sales, CL) discount was
    # removed Jul 26 per Kevin — it was underpricing MLB slabs).
    for _c in comps:
        _sp = normalize_sport(_c.get('sport'))
        if _sp == 'basketball':
            # Over-ceiling NBA cards get the avg-3 discount so a
            # stale-high CL value doesn't auto-reject a card whose
            # actual recent sales sit inside the $1-200 buy band.
            # (Was hardcoded 250 — left a $200-250 gap where cards
            # were rejected on raw CL value while identical cards
            # above $250 got discounted and accepted.)
            # .get with an inf default: basketball may be absent from
            # PSA_SPORT_MAX_PRICE while sports are off (Aug 12) — a stray
            # basketball-classified cert in a lot must not crash the quote
            # (classify rejects it right after anyway).
            apply_avg3_value(_c, threshold=PSA_SPORT_MAX_PRICE.get('basketball', float('inf')))
    if sheet_id and comps:
        try:
            await asyncio.to_thread(fill_buying_sheet, sheet_id, comps)
        except Exception as e:
            print(f"Sheet fill error (offer continues): {e}")
    # VIP customers get their negotiated Pokémon rates baked into THEIR sheet's
    # payout formulas (the template copy carries standard rates). A silent failure
    # here would make the sheet compute LESS than the quoted number and Kevin pays
    # off the sheet — so retry once, then alert him loudly.
    if sheet_id and vip_tiers:
        for _attempt in (1, 2):
            try:
                await asyncio.to_thread(write_custom_sheet_formulas, sheet_id, vip_tiers)
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
    kevin_lines = []
    for cert in certs:
        c = by_cert.get(str(cert).strip())
        status, reason = classify_psa_comp(c)
        if status == 'review':
            # Manual-review cards: keep out of the auto-priced lot (0%),
            # highlight orange on the sheet for Kevin to quote by hand.
            review.append((cert, reason))
            review_certs.append(cert)
            cv = float(c['clValue'])
            name = (c.get('cardName') or '')[:50]
            grade = str(c.get('grade') or '').replace('PSA ', '').strip()
            kevin_lines.append(f"• `{cert}` — 🟠 **${cv:.2f}** — {name} (PSA {grade}) — {reason}")
        elif status == 'accepted':
            accepted.append(c)
            cv = float(c['clValue'])
            name = (c.get('cardName') or '')[:50]
            grade = str(c.get('grade') or '').replace('PSA ', '').strip()
            kevin_lines.append(f"• `{cert}` — **${cv:.2f}** — {name} (PSA {grade})")
        elif status == 'flag':
            # Still accept the card so the lot is priced, but warn Kevin.
            accepted.append(c)
            flagged.append((cert, c, reason))
            cv = float(c['clValue'])
            name = (c.get('cardName') or '')[:50]
            grade = str(c.get('grade') or '').replace('PSA ', '').strip()
            kevin_lines.append(f"• `{cert}` — ⚠️ **${cv:.2f}** — {name} (PSA {grade}) — {reason}")
        else:
            rejected.append((cert, reason))
            kevin_lines.append(f"• `{cert}` — ❌ {reason}")

    # Shade manual-review cards orange so Kevin knows to price them by hand.
    if review_certs and sheet_id:
        try:
            await asyncio.to_thread(highlight_certs_review, sheet_id, review_certs)
        except Exception as e:
            print(f"Review-highlight error: {e}")

    sport_groups = {}
    for c in accepted:
        sp = normalize_sport(c.get('sport'))
        sport_groups.setdefault(sp, []).append(c)
    sport_breakdown = []
    for sp, comps_in_sport in sport_groups.items():
        card_values = [float(c['clValue']) for c in comps_in_sport]
        sport_total = sum(card_values)
        if sp == 'pokemon':
            # Per-card: band rate (standard or VIP) vs Pikachu lane — better wins.
            _payout = sum(float(c['clValue']) * _pokemon_card_rate(c, vip_tiers)
                          for c in comps_in_sport)
            rate = (_payout / sport_total) if sport_total else PSA_POKEMON_PER_CARD_TIERS[0][2]
        else:
            rate = get_psa_payout_rate(sp, sport_total, card_values)
        sport_breakdown.append({
            'sport': sp,
            'count': len(comps_in_sport),
            'total': sport_total,
            'rate': rate,
            'payout': sport_total * rate,
            'vip': sp == 'pokemon' and bool(vip_tiers),
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
    add_slab_values(channel_id, slab_value_map)

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
            f"\n⚠️ **{n_flagged} card{'s' if n_flagged != 1 else ''} flagged for manual review** — verify licensing before paying!\n"
            + "\n".join([f"   - `{cert}`: {reason}" for cert, _, reason in flagged])
            + "\n"
        )
    review_warning = ""
    if n_review:
        review_warning = (
            f"\n🟠 **{n_review} card{'s' if n_review != 1 else ''} to price by hand** (highlighted orange on the sheet, not in the auto-quote):\n"
            + "\n".join([f"   - `{cert}`: {reason}" for cert, reason in review])
            + "\n"
        )
    summary = (
        f"📋 **PSA sheet — {username}**\n"
        f"{len(certs)} certs | Accepted **{n_accepted}** | Comp **${total_comp:,.2f}** | Payout **${total_payout:,.2f}**"
        f"{f' | {n_rejected} rejected' if n_rejected else ''}"
        f"{f' | {n_flagged} ⚠️ flagged' if n_flagged else ''}"
        f"{f' | {n_review} 🟠 manual' if n_review else ''}\n"
        f"{sheet_url}\n"
        + ("\n".join(breakdown_lines) + "\n\n" if breakdown_lines else "\n")
        + flag_warning
        + review_warning
    )
    if n_accepted and not lot_qualifies(channel_id):
        _sc, _sv, _si, _cb = lot_summary(channel_id)
        summary = f"⏳ **[BELOW MINIMUM — HOLD]** {_sc} slab(s), combined ${_cb:,.2f}\n" + summary
    await ping_kevin(summary + "\n".join(kevin_lines), channel)

    # Customer follow-up
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
        else:
            customer_parts.append(
                f"⚠️ Unfortunately, none of these {len(certs)} cards fit our current buying criteria:"
            )
        customer_parts += [f"• `{cert}` — {reason}" for cert, reason in rejected]
    if n_review:
        customer_parts += [
            "",
            f"💎 {n_review} card{'s' if n_review != 1 else ''} we'll quote by hand and get right back to you:",
            *[f"• `{cert}`" for cert, _ in review],
        ]
    if n_accepted > 0:
        hold_tail = proceed_or_hold_tail(channel_id)
        if hold_tail:
            customer_parts += ["", hold_tail]
        else:
            customer_parts += [
                "",
                "Let me know if you'd like to proceed!" if not rejected
                else f"Let me know if you want to proceed with the {n_accepted} we can take.",
            ]
    customer_parts += [
        "",
        "If you want to see where I'm at on each individual card, click the sheet link above and request access.",
    ]
    customer_msg = "\n".join(customer_parts)
    for chunk in _split_for_discord(customer_msg):
        await channel.send(chunk)

async def retry_psa_offer_when_helper_back(channel, channel_id, username, certs,
                                           sheet_id, sheet_url,
                                           max_attempts=15, interval_s=180):
    """The comp helper was down when these certs arrived. Keep retrying in the
    background (every 3 min, ~45 min total — the VM watchdog usually revives the
    helper within one cycle); when it answers, post the quote automatically so the
    customer never has to re-send anything. Managed exclusively through
    start_comp_retry / cancel_comp_retry; the identity checks below make a
    superseded or cancelled task a no-op even mid-lookup."""
    def _mine():
        e = pending_comp_retries.get(channel_id)
        return e is not None and e.get("task") is asyncio.current_task()
    try:
        for _ in range(max_attempts):
            await asyncio.sleep(interval_s)
            if not _mine():
                return   # superseded or cancelled
            try:
                comps = await asyncio.to_thread(lookup_comps, certs)
            except Exception:
                continue
            if not comps:
                continue
            # Require FULL coverage before quoting. lookup_comps returns partial
            # results when only some chunks succeed — exactly what a mid-recovery
            # helper produces — and quoting then would falsely reject every cert
            # in the failed chunks ("no comp on CardLadder"). Genuine no-comp
            # answers still come back as found=False ENTRIES, so a missing cert
            # means infrastructure failure, not a missing card.
            returned = {str(c.get('cert', '')).strip() for c in comps}
            if any(str(c).strip() not in returned for c in certs):
                print(f"Comp retry: {len(returned)}/{len(certs)} certs returned — "
                      f"helper still recovering, waiting for full coverage")
                continue
            if not _mine():
                return   # cancelled while the lookup was in flight
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
            return
        print(f"Comp retry gave up after {max_attempts} attempts — {username}, {len(certs)} certs")
        await ping_kevin(
            f"🚨 Helper stayed down ~{max_attempts * interval_s // 60} min — "
            f"**{username}**'s {len(certs)} certs never got comps. Quote manually: {sheet_url}",
            channel
        )
    finally:
        # Only clean up our own entry — a superseding task has already replaced it.
        e = pending_comp_retries.get(channel_id)
        if e is not None and e.get("task") is asyncio.current_task():
            pending_comp_retries.pop(channel_id, None)
            _save_pending_retries()


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
MIN_LOT_VALUE = 3000   # standard minimum (the Aug 14 weekend's $1,000 exception expired)
MIN_SLAB_COUNT = 15

# Per-ticket running lot.
#   channel_id -> {"singles": {game: market_value}, "slab_certs": {cert: comp_value}}
#   singles    : per-game market value of the latest valid Collectr CSV
#                (whole dict replaced on re-upload — latest CSV is the truth)
#   slab_certs : accepted PSA certs -> comp value, accumulated & deduped by cert
lot_state = {}

def _lot_entry(channel_id):
    return lot_state.setdefault(channel_id, {"singles": {}, "slab_certs": {}})

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

def add_slab_values(channel_id, cert_value_map):
    """Accumulate accepted slab certs (deduped by cert so re-sends don't double-count)."""
    entry = _lot_entry(channel_id)
    for cert, val in cert_value_map.items():
        entry["slab_certs"][str(cert)] = float(val or 0)

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
    "We're currently buying:\n"
    "• **PSA graded slabs** (Pokémon, Basketball, Baseball & Football) → send your cert numbers\n"
    + _POKEMON_RAW_WELCOME_LINE +
    "\n⚠️ We are **not** buying raw cards or One Piece this weekend — PSA slabs only.\n\n"
    "📊 **Minimum lot requirements:**\n"
    f"• At least **{MIN_SLAB_COUNT} slabs** AND **${MIN_LOT_VALUE:,}+** total value.\n"
    + _POKEMON_RAW_MIN_LINE +
    "\n🔢 We prioritize **quantity** — we won't take a lot that's just a few big-ticket slabs.\n\n"
    "What are you looking to sell?"
)

SHIPPING_MSG = (
    "📦 **Awesome, let's do it!** Ship your cards to Kevin and you'll be paid out once your package arrives and is processed.\n\n"
    "**Ship to:**\n"
    "Kevin Smith\n"
    "1223 Dellwood Drive\n"
    "Westlake OH 44145\n\n"
    "🚚 **Shipping method — required:** All lots must be sent **UPS Next Day Air A.M.** (overnight, "
    "morning delivery) — no standard overnight, 2-Day, ground, or anything slower. Packages must be "
    "**in hand Tuesday morning**, so ship **Monday at the latest**.\n\n"
    "📝 **Please include a note inside your package with:**\n"
    "• Your Discord username\n"
    "• Amount owed\n"
    "• Preferred payment method (Wire or ACH)\n\n"
    "📥 **Packaging requirements — please read, these affect your payout:**\n"
    "• **Raw cards:** penny sleeve only — **no top loaders.** Cards shipped in top loaders = **2% deducted** from payout.\n"
    "• **Slabs:** ship as-is — **no sleeves or stickers.** Slabs shipped in sleeves or with stickers = **2% deducted** from payout.\n"
    "• **No note** (or missing required info above) = **2% deducted** from payout.\n\n"
    "⚠️ Without a note we also won't know who the package is from, so payment may be delayed on top of the deduction.\n\n"
    "🗓️ **Payouts run Fridays only.**\n"
    "It's **first come, first serve** — you join the payout queue the moment your package is scanned in on arrival.\n"
    "• Package in **Tuesday** → usually paid that **Friday**\n"
    "• Arrives later in the week → rolls into the **next** Friday payout.\n\n"
    "👉 Best move: **overnight it Monday so it lands Tuesday morning** to make that week's payout. Payment via wire or ACH ⚡\n\n"
    "Once you've shipped, **drop your tracking number here** so Kevin can keep an eye out!"
)

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
        if (isinstance(_v, dict) and isinstance(_v.get("pokemon"), list) and _v["pokemon"]
                and all(isinstance(_x, (int, float)) for _x in _v["pokemon"])):
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

def vip_pokemon_tiers(username):
    """The user's custom Pokémon tier list — band boundaries from the CURRENT
    standard tiers, rates from their VIP entry (last rate repeats if they gave
    fewer rates than there are bands). None if the user isn't a VIP."""
    entry = VIP_RATES.get((username or "").lower())
    if not entry:
        return None
    rates = entry.get("pokemon") or []
    if not rates:
        return None
    return [(low, high, rates[i] if i < len(rates) else rates[-1])
            for i, (low, high, _std) in enumerate(PSA_POKEMON_PER_CARD_TIERS)]

def _pokemon_band_labels():
    """Human labels for the current Pokémon bands, e.g. ['≤$100', '$100-$150', '$150-$750']."""
    labels = []
    for i, (low, high, _r) in enumerate(PSA_POKEMON_PER_CARD_TIERS):
        top = PSA_SPORT_MAX_PRICE['pokemon'] if high == float('inf') else int(high)
        labels.append(f"≤${top}" if i == 0 else f"${int(low)}-${top}")
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
    labels = _pokemon_band_labels()
    tiers = vip_pokemon_tiers(name)
    return ", ".join(f"{labels[i]} → {t[2]*100:g}%" for i, t in enumerate(tiers))

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
        if not VIP_RATES:
            return "No VIP users set. Add one with `!vip add <username> 91` (or `91 88 85` per band)."
        lines = ["✨ **VIP Pokémon rates:**"]
        for name in sorted(VIP_RATES):
            lines.append(f"• **{name}**: {_vip_rate_summary(name)}")
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
            VIP_RATES[name] = {"pokemon": list(rates)}
            applied += 1
            results.append(f"• **{name}** ✓" + (f" ({note})" if note else ""))
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
        VIP_RATES[name] = {"pokemon": rates}
        _save_vip()
        _tag = f" ({note})" if note else ""
        return (f"✨ VIP set for **{name}**{_tag}: Pokémon {_vip_rate_summary(name)}\n"
                f"(pinned until you change it — weekly flyer updates won't move it; "
                f"grade/ceiling/sale-age rules stay standard)")

    if sub in ("remove", "delete", "del"):
        if len(toks) < 2:
            return "Usage: `!vip remove <username>`"
        name, err, _note = _vip_resolve_target(toks[1], mentions, guild, for_remove=True)
        if err:
            return err
        if name in VIP_RATES:
            del VIP_RATES[name]
            _save_vip()
            return f"Removed **{name}** from VIP — they get standard rates now."
        return f"**{name or '?'}** isn't on the VIP list. `!vip list` to see who is."

    return ("Commands: `!vip add <username> 91 [88 85]` · `!vip bulk 91 90 85` + list "
            "of usernames on following lines · `!vip remove <username>` · `!vip list`")

def build_sheet_h_formula(r, pokemon_tiers=None):
    """The per-row payout formula written into buying sheets — generated from the
    bot's CURRENT rate constants so bot and sheet can't disagree. pokemon_tiers
    overrides only the Pokémon band RATES (VIP sheets). NOTE: assumes the current
    band structure (Aug 28: pokemon $1-140 [PSA 7+, 7+ digit cert] and
    $3,500-5,000 [any grade]; NBA $1-500; MLB $1-600; NFL $300-1,600; one
    piece OFF) — if a weekly flyer changes the band COUNT, update this
    builder with it."""
    def _n(v):
        return f"{v:g}"   # 1.0 -> "1", 0.85 -> "0.85" (matches the template's style)
    pt = pokemon_tiers or PSA_POKEMON_PER_CARD_TIERS
    p1, p2 = _n(pt[0][2]), _n(pt[1][2])
    p_low_top = int(pt[0][1])            # 140 (band 1 top, inclusive)
    p_big_low = int(pt[1][0])            # 3500 (any-grade band start)
    pok_max = PSA_SPORT_MAX_PRICE['pokemon']
    nba_max = PSA_SPORT_MAX_PRICE['basketball']
    mlb_max = PSA_SPORT_MAX_PRICE['mlb']
    nfl_max = PSA_SPORT_MAX_PRICE['football']
    b1 = _n(PSA_BASKETBALL_PER_CARD_TIERS[0][2])
    m1 = _n(PSA_MLB_PER_CARD_TIERS[0][2])
    f1 = _n(PSA_FOOTBALL_PER_CARD_TIERS[0][2])
    g = PSA_MIN_GRADE
    cd = POKEMON_MIN_CERT_DIGITS
    maxage = (f'IF(F{r}="pokemon",60,'
              f'IF(F{r}="basketball",99999,'
              f'IF(OR(F{r}="mlb",F{r}="baseball"),IF(G{r}<=100,99999,90),'
              f'IF(F{r}="football",90,30))))')
    # Pokémon band 1 needs PSA 7+ AND a 7+ digit cert (column B); the
    # $3,500-$5,000 band is any grade (flagged to Kevin bot-side).
    pok_band = (f'IF(G{r}<={p_low_top},IF(OR(N(E{r})<{g},LEN(B{r})<{cd}),0,{p1}),'
                f'IF(AND(G{r}>={p_big_low},G{r}<={pok_max}),{p2},0))')
    rate = (f'IFS('
            f'F{r}="pokemon",{pok_band},'
            f'F{r}="basketball",IF(AND(G{r}>=1,G{r}<={nba_max}),IF(N(E{r})<{g},0,{b1}),0),'
            f'OR(F{r}="mlb",F{r}="baseball"),IF(AND(G{r}>=1,G{r}<={mlb_max}),IF(N(E{r})<{g},0,{m1}),0),'
            f'F{r}="football",IF(AND(G{r}>={NFL_MIN_PRICE},G{r}<={nfl_max}),IF(N(E{r})<{g},0,{f1}),0),'
            f'F{r}="other",0,'
            f'TRUE,0)')
    too_old = (f'IF(ISNUMBER(J{r}),(TODAY()-J{r})>{maxage},'
               f'IFERROR((TODAY()-DATEVALUE(J{r}))>{maxage},TRUE))')
    return f'=IF(OR(NOT(ISNUMBER(G{r})),G{r}=0),"",IF(A{r}<>"PSA",0.7,IF({too_old},0,{rate})))'

def write_custom_sheet_formulas(sheet_id, pokemon_tiers, sheet_name="Form. Put Date Here."):
    """Rewrite H2:H1000 in one customer's sheet with their VIP Pokémon rates.
    Blocking — run via asyncio.to_thread."""
    gc = get_gspread_client()
    ss = gc.open_by_key(sheet_id)
    try:
        sheet = ss.worksheet(sheet_name)
    except Exception:
        sheet = ss.sheet1
    sheet.update(values=[[build_sheet_h_formula(r, pokemon_tiers)] for r in range(2, 1001)],
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
        def _read(fid=f['id']):
            ws = get_gspread_client().open_by_key(fid).get_worksheet(0)
            certs = [c for c in ws.col_values(2)[1:] if str(c).strip().isdigit()]
            filled = [v for v in ws.col_values(7)[1:] if str(v).strip() not in ('', 'not found')]
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
        remember_ticket_channel(channel.id)
        remember_channel_sheet(channel.id, f['id'])
        sheet_url = f"https://docs.google.com/spreadsheets/d/{f['id']}/edit"
        try:
            comps = await asyncio.to_thread(lookup_comps, certs)
            returned = {str(c.get('cert', '')).strip() for c in comps or []}
            if not comps or any(str(c).strip() not in returned for c in certs):
                failed += 1
                await ping_kevin(f"🔁 **{username}**: partial comp coverage "
                                 f"({len(returned)}/{len(certs)}) — not quoting, run !requoteall again later.")
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
            await message.channel.send(WELCOME_MSG)
            # If they already sent certs or a CSV with their first message,
            # don't make them re-send — fall through to processing below.
            if not (certs or csv_attachment):
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
                    # Stamp VIP formulas at CREATION time too: if the helper is
                    # down and Kevin ends up quoting this sheet by hand (or the
                    # retry exhausts), the sheet must already carry their rates.
                    _vt = vip_pokemon_tiers(username)
                    if _vt:
                        try:
                            await asyncio.to_thread(write_custom_sheet_formulas, sheet_id, _vt)
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
                try:
                    comps = await asyncio.to_thread(lookup_comps, certs)
                except Exception as e:
                    comp_error = str(e)
                    print(f"Helper comp lookup error: {e}")

                if comps:
                    # (Any retry this submission covers was already cancelled at
                    # submission time, before our lookup — a still-pending retry
                    # here is for a DIFFERENT batch and keeps running.)
                    await price_and_send_psa_offer(
                        message.channel, channel_id, username, certs, comps,
                        sheet_id, sheet_url)
                else:
                    # Helper unavailable — tell the customer, gate "proceed"
                    # (_lot_entry), ping Kevin, and keep retrying in the
                    # background: when the helper comes back (the VM watchdog
                    # usually revives it within ~3 min) the quote posts itself.
                    await message.channel.send(
                        "⚠️ Comps are taking a little longer than usual — I'll "
                        "keep trying and post your quote here as soon as they're in!"
                    )
                    _lot_entry(channel_id)
                    cert_list = "\n".join([f"• `{c}`" for c in certs])
                    err_note = f"\n\n⚠️ Helper offline ({comp_error})" if comp_error else ""
                    await ping_kevin(
                        f"📋 **PSA sheet — {username}** (comps pending, auto-retrying — "
                        f"type `!cancelretry` in the ticket to take over manually)\n"
                        f"{len(certs)} certs | {sheet_url}{err_note}\n\n"
                        f"{cert_list}",
                        message.channel
                    )
                    # Always (re)spawn: a second helper-down submission supersedes
                    # the old retry and MERGES its outstanding certs, so no batch
                    # is silently dropped.
                    start_comp_retry(message.channel, channel_id, username, certs,
                                     sheet_id, sheet_url)
            except Exception as e:
                print(f"Sheet error: {e}")
                await message.channel.send("Small hiccup — Kevin will set this up manually and be right with you!")
                await ping_kevin(
                    f"⚠️ Sheet failed — **{username}**\nCerts: {', '.join(certs)}\nError: {str(e)}",
                    message.channel
                )
        return

    # ── AGREED / SHIPPING ─────────────────────────────────────────────────────────
    if is_agreeing(text):
        # Block shipping unless the recorded lot meets the buying minimums.
        # Fail OPEN when we have no recorded lot (e.g. bot was redeployed and lost
        # in-memory state) so we never stonewall a seller who already got an offer.
        if channel_id in lot_state and not lot_qualifies(channel_id):
            _sc, _sv, _si, _cb = lot_summary(channel_id)
            await message.channel.send(proceed_or_hold_tail(channel_id))
            await ping_kevin(
                f"⏳ **{username} typed 'ship' but lot is below minimum** "
                f"({_sc} slab(s), combined ${_cb:,.2f}) — address NOT sent.",
                message.channel
            )
            return
        await message.channel.send(SHIPPING_MSG)
        # Snapshot the accepted ship-list for the local ticket tracker (GET /tickets).
        try:
            _rec = record_accepted_ship_list(channel_id, username)
        except Exception as e:
            _rec = None
            print(f"ship-list record failed (non-critical): {e}")
        _rec_note = (f" 📦 Ship-list recorded ({len(_rec['certs'])} certs) — tracker → Sync from bot."
                     if _rec else " ⚠️ No lot on record to snapshot (redeploy?) — add this one to the tracker manually.")
        await ping_kevin(f"✅ **{username} agreed** — shipping address sent.{_rec_note}", message.channel)
        # Resolve the sheet through the cache + Drive fallback (channel_sheet used
        # to be memory-only, so a redeploy between quote and "ship" silently
        # skipped the tracking row).
        ship_sheet_id = await find_sheet_for_channel(
            channel_id, message.channel.name, username, message.channel.created_at)
        if ship_sheet_id:
            try:
                import urllib.request as urlreq
                post_data = json.dumps({
                    "action": "add_tracking",
                    "sheet_id": ship_sheet_id,
                    "username": username
                }).encode("utf-8")
                req = urlreq.Request(
                    APPS_SCRIPT_URL,
                    data=post_data,
                    headers={"Content-Type": "application/json"}
                )
                await asyncio.to_thread(urlreq.urlopen, req, timeout=15)
                print(f"Added tracking row for {username}")
            except Exception as e:
                print(f"Tracking row error (non-critical): {e}")
        return

    # ── TRACKING NUMBER ───────────────────────────────────────────────────────────
    tracking_match = re.search(r'\b([0-9]{20,22}|1Z[A-Z0-9]{16}|[0-9]{12,15})\b', text)
    if tracking_match:
        tracking_num = tracking_match.group(1)
        # Cache + exact-prefix Drive fallback. The old inline fallback searched
        # `name contains '<channel name>'`: it never matched legacy ticket-<name>
        # channels (sheets are named from the bare username) and on short usernames
        # could match — and silently write tracking into — the WRONG customer's
        # sheet. find_sheet_for_channel handles both; if it can't find a confident
        # match it returns None and we ping Kevin instead of guessing.
        sheet_id = await find_sheet_for_channel(
            channel_id, message.channel.name, username, message.channel.created_at)
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
