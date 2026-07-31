"""
KTS Collectibles — Full Discord Bot
=====================================
Handles TWO types of customers automatically:

1. PSA SLAB sellers:
   - Customer sends cert numbers
   - Bot creates a Google Sheet copy with cert numbers + CardLadder links
   - Pings Kevin with sheet link

2. RAW CARD sellers (Collectr) — ONE PIECE ONLY (May 2026):
   - Customer uploads their Collectr CSV export in their ticket channel
   - Bot reads it, calculates total market value
   - One Piece English NM singles, $1-$150 per card
   - Applies correct % based on lot size (low end of the range):
       under $10k     → 85%
       $10k+          → 88%
   - Pokémon raws are politely declined (PSA slabs still accepted)
   - Sends customer their offer
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
import os
import json
import pandas as pd
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

# PSA slab buying criteria
PSA_MIN_PRICE = 1
PSA_MIN_GRADE = 7
PSA_MAX_AGE_DAYS = 30
# NBA slabs are bought at ANY grade with just one sale on record (any date), across
# the whole $1-$200 band (Jul 31 weekend). Set to the $200 ceiling so both the
# any-grade waiver AND the "one sale ever" age (inf) apply to every in-range NBA card.
NBA_ANY_GRADE_MAX_PRICE = 200
# Per-sport price ceilings — sports not listed here are rejected outright, and any
# slab priced ABOVE its ceiling is rejected (Jul 31 weekend flyer).
# pokemon: $750. basketball (NBA): $200. one piece: $750. football (NFL): $200.
# MLB REMOVED Jul 31 — baseball slabs are rejected (absent from the flyer).
PSA_SPORT_MAX_PRICE = {
    'pokemon': 750,
    'basketball': 200,
    'one piece': 750,
    'football': 200,   # NFL slabs
}
# Pokémon $150+ requires PSA 8-10 (the $1-$150 bands stay PSA 7+).
POKEMON_HIGH_BAND_MIN = 150
POKEMON_HIGH_BAND_MIN_GRADE = 8
# One Piece slabs require PSA 8-10 across the board (Jul 31 weekend).
ONE_PIECE_MIN_GRADE = 8
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

# ── PER-CARD PAYOUT TIERS BY SPORT (Jul 10 weekend flyer) ─────────────────────────
# Low end of each flyer range. Grade, reject-zones, and the Pokémon manual bucket
# live in classify_psa_comp, so only in-band cards reach these blends. CL-confidence
# requirements can't be enforced (no CL score in the data) — priced on value+grade.
# Pokémon (Jul 31 weekend): $1-$100 → 89% (flyer says 89-90 — pay the low end),
# $100-$150 → 87%, $150-$750 → 84% (PSA 8-10 only — enforced in classify_psa_comp).
# Ceiling $750.
PSA_POKEMON_PER_CARD_TIERS = [
    (0,      100.01,       0.89),   # $1-$100 → 89%  (.01 so exactly $100 is 89%)
    (100.01, 150,          0.87),   # $100-$150 → 87%
    (150,    float('inf'), 0.84),   # $150-$750 → 84%  ($750 ceiling rejects above)
]
# Basketball (NBA, Jul 31 weekend): $1-$200 → 95%, ANY grade, one sale ever.
# Ceiling $200.
PSA_BASKETBALL_PER_CARD_TIERS = [
    (0, float('inf'), 0.95),   # $1-$200 → 95%  ($200 ceiling rejects above)
]
PSA_ONE_PIECE_PER_CARD_TIERS = [
    (0,      100.01,       0.88),   # $1-$100 → 88%  (.01 so exactly $100 is 88%)
    (100.01, float('inf'), 0.83),   # $101-$750 → 83%  ($750 ceiling rejects above)
]
# NFL / football (Jul 31 weekend): $1-30 → 100%, $30-100 → 92%, $100-200 → 90%.
# Ceiling $200. (The flyer's "CL 3+" on the $100-200 band is a CardLadder
# confidence requirement the bot cannot read — Kevin eyeballs it.)
PSA_FOOTBALL_PER_CARD_TIERS = [
    (0,     30.01,        1.00),   # $1-$30 → 100%  (.01 so exactly $30 is 100%)
    (30.01, 100,          0.92),   # $30-$100 → 92%
    (100,   float('inf'), 0.90),   # $100-$200 → 90%  ($200 ceiling rejects above)
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
def get_payout_rate(total, username):
    """Return the payout percentage for a given lot total."""
    username_lower = username.lower()
    if username_lower in VIP_CLIENTS_89:
        return 0.89, "VIP rate"
    if username_lower in VIP_CLIENTS:
        return 0.87, "VIP rate"
    for low, high, rate in RAW_PAYOUT_TIERS:
        if low <= total < high:
            return rate, f"${low:,}–{'$'+str(high//1000)+'k' if high != float('inf') else '+'} tier"
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
    Parse a Collectr CSV export and return total market value + card list.
    Validates cards against KTS One Piece buying requirements:
      - Must be One Piece TCG (Pokémon politely declined)
      - English only
      - $1-$150 per card
      - Near Mint only
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

    pokemon_cards = []
    other_game_cards = []
    if game_col:
        for _, row in df.iterrows():
            game_val = str(row.get(game_col, '')).strip().lower()
            name = str(row.get('Product Name', 'Unknown'))
            set_name = str(row.get('Set', ''))
            if 'pokemon' in game_val or 'pokémon' in game_val:
                pokemon_cards.append(f"• {name} ({set_name})")
            elif game_val and 'one piece' not in game_val:
                other_game_cards.append(f"• {name} ({set_name}) — {row.get(game_col, '')}")

    # Check for non-English cards. Collectr marks them two ways:
    #   1. Language tag in the product name: "(JP)", "(KR)", "(CN)", "(TW)", "(KOR)"
    #   2. Characters from a non-Latin script (Japanese/Korean/Chinese/etc.) in the
    #      name or set. We deliberately do NOT reject on any non-ASCII byte —
    #      English exports contain curly apostrophes / dashes / ™ that are not a
    #      foreign language (see _has_non_latin_script).
    non_english_tags = ('(jp)', '(kr)', '(cn)', '(tw)', '(kor)', '(jpn)', '(chn)')
    non_english = []
    for _, row in df.iterrows():
        name = str(row.get('Product Name', ''))
        set_name = str(row.get('Set', ''))
        combined = name + set_name
        name_lower = name.lower()
        if any(tag in name_lower for tag in non_english_tags):
            non_english.append(f"• {name} ({set_name})")
        elif _has_non_latin_script(combined):
            non_english.append(f"• {name} ({set_name})")

    # Per-card price range: $1-$150
    over_max = []
    under_min = []
    for _, row in df.iterrows():
        price = float(row[price_col])
        name = str(row.get('Product Name', 'Unknown'))
        if price > RAW_MAX_PRICE:
            over_max.append(f"• {name} — ${price:.2f}")
        elif price < RAW_MIN_PRICE:
            under_min.append(f"• {name} — ${price:.2f}")

    issues = []
    if pokemon_cards:
        issues.append(("pokemon", pokemon_cards))
    if other_game_cards:
        issues.append(("other_game", other_game_cards))
    if non_english:
        issues.append(("non_english", non_english))
    if over_max:
        issues.append(("over_max", over_max))
    if under_min:
        issues.append(("under_min", under_min))

    total = df['_line_total'].sum()
    card_count = int(df[qty_col].sum()) if qty_col else len(df)

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

def create_psa_sheet(username, cert_numbers):
    """Create a buying sheet by calling the Google Apps Script web app."""
    import urllib.request
    import urllib.parse
    certs_str = ",".join([str(c).strip() for c in cert_numbers])
    params = urllib.parse.urlencode({"username": username, "certs": certs_str, "folder_id": PSA_FOLDER_ID})
    url = f"{APPS_SCRIPT_URL}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "KTS-Bot/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    if not data.get("success"):
        raise Exception(data.get("error", "Unknown error from Apps Script"))
    return data["url"], data["name"], data


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
        return ('rejected', f"{sport_label} (we only buy pokemon, basketball, football, and one piece right now)")
    if cv > max_price:
        # Two decimals so a $200.40 NBA card reads "over our $200 max" sensibly
        # instead of the contradictory "$200 (over our $200 max)".
        return ('rejected', f"${cv:,.2f} (over our ${max_price} {sport} max)")
    if cv < PSA_MIN_PRICE:
        return ('rejected', f"${cv:.2f} (under ${PSA_MIN_PRICE} min)")
    grade_raw = str(comp.get('grade') or '').replace('PSA', '').strip()
    try:
        g = float(grade_raw)
    except ValueError:
        return ('rejected', f"grade '{grade_raw}' unrecognized")
    # Grade floor. NBA (whole $1-$200 band) → ANY grade accepted; everything else
    # (all Pokémon, football, mlb, one piece) → PSA_MIN_GRADE+.
    nba_any_grade = (sport == 'basketball' and cv <= NBA_ANY_GRADE_MAX_PRICE)
    if not nba_any_grade and g < PSA_MIN_GRADE:
        return ('rejected', f"PSA {grade_raw} (we buy {PSA_MIN_GRADE}-10 only)")
    # Pokémon $200+ band is PSA 8-10 only (the $1-$200 bands stay PSA 7+).
    if (sport == 'pokemon' and cv >= POKEMON_HIGH_BAND_MIN
            and g < POKEMON_HIGH_BAND_MIN_GRADE):
        return ('rejected',
                f"PSA {grade_raw} (${cv:,.0f} Pokémon needs PSA "
                f"{POKEMON_HIGH_BAND_MIN_GRADE}-10 at ${POKEMON_HIGH_BAND_MIN}+)")
    # One Piece slabs are PSA 8-10 only (whole $1-$750 range, Jul 31 weekend).
    if sport == 'one piece' and g < ONE_PIECE_MIN_GRADE:
        return ('rejected',
                f"PSA {grade_raw} (One Piece slabs are PSA {ONE_PIECE_MIN_GRADE}-10 only)")
    last_sale = comp.get('lastSaleDate')
    if not last_sale:
        return ('rejected', 'no recent sale visible')
    try:
        last_d = datetime.strptime(str(last_sale).strip(), '%Y-%m-%d').date()
    except (ValueError, AttributeError):
        return ('rejected', f"unparseable sale date '{last_sale}'")
    # Sale-age limit (days). Value-dependent for pokemon / basketball / football / mlb.
    if sport == 'pokemon':
        max_age = 60 if cv < 100 else 30                                    # $1-100 within 2 months, else 1 month
    elif sport == 'basketball':
        max_age = float('inf') if cv <= NBA_ANY_GRADE_MAX_PRICE else 90     # whole $1-200 band: one sale ever
    elif sport in ('football', 'mlb'):
        max_age = float('inf') if cv < 100 else 90                         # $1-100 just needs one sale on record
    else:
        max_age = PSA_SPORT_MAX_AGE_DAYS.get(sport, PSA_MAX_AGE_DAYS)
    if (date.today() - last_d).days > max_age:
        return ('rejected', f"last sale {last_sale} (>{max_age:g}d ago)")

    # NBA: reject the $400-$1000 dead zone (not a buy band), then apply player/set bans.
    if sport == 'basketball':
        if 400 < cv < 1000:
            return ('rejected', f"${cv:,.0f} — outside our NBA buy ranges ($400-$1,000)")
        return check_basketball_rejections(comp)

    # Pokémon manual bucket — slabs over $1,000 are priced by hand (highlighted orange).
    if sport == 'pokemon' and cv > PSA_POKEMON_MANUAL_REVIEW_OVER:
        return ('review',
                f"${cv:,.0f} Pokémon — over ${PSA_POKEMON_MANUAL_REVIEW_OVER:,.0f}, price manually")

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
            apply_avg3_value(_c, threshold=PSA_SPORT_MAX_PRICE['basketball'])
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
        if sp == 'pokemon' and vip_tiers:
            # Negotiated per-customer Pokémon rates (see VIP_RATES / !vip).
            rate = _blended_per_card_rate(vip_tiers, card_values)
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
# Two qualifying paths for a lot:
#   1. Lot CONTAINS PSA slabs  -> need >=15 accepted slabs AND >=$3,000 combined
#      value (slab CardLadder comp + any One Piece singles market value).
#   2. One Piece singles ONLY  -> Collectr export must be >=$3,000 in value.
# Slabs and singles can arrive in either order / separate messages, so we keep a
# running per-ticket total and only let a seller proceed/ship once it qualifies.
MIN_LOT_VALUE = 3000
MIN_SLAB_COUNT = 15

# Per-ticket running lot.  channel_id -> {"singles": float, "slab_certs": {cert: comp_value}}
#   singles    : market value of the latest valid Collectr CSV (replaced on re-upload)
#   slab_certs : accepted PSA certs -> comp value, accumulated & deduped by cert
lot_state = {}

def _lot_entry(channel_id):
    return lot_state.setdefault(channel_id, {"singles": 0.0, "slab_certs": {}})

def set_singles_value(channel_id, value):
    """Latest Collectr CSV is the current truth for singles — replace, don't add."""
    _lot_entry(channel_id)["singles"] = float(value or 0)

def add_slab_values(channel_id, cert_value_map):
    """Accumulate accepted slab certs (deduped by cert so re-sends don't double-count)."""
    entry = _lot_entry(channel_id)
    for cert, val in cert_value_map.items():
        entry["slab_certs"][str(cert)] = float(val or 0)

def lot_summary(channel_id):
    """Return (slab_count, slab_value, singles_value, combined_value)."""
    entry = lot_state.get(channel_id) or {"singles": 0.0, "slab_certs": {}}
    slab_value = sum(entry["slab_certs"].values())
    slab_count = len(entry["slab_certs"])
    singles = entry["singles"]
    return slab_count, slab_value, singles, slab_value + singles

def lot_qualifies(channel_id):
    """True if the running lot meets the buying minimums for its path."""
    slab_count, slab_value, singles, combined = lot_summary(channel_id)
    # One Piece singles alone at $3,000+ qualifies the whole lot — we'll take any
    # slabs included regardless of slab count.
    if singles >= MIN_LOT_VALUE:
        return True
    # Otherwise, any lot containing slabs needs 15+ slabs AND $3,000+ combined.
    if slab_count > 0:
        return combined >= MIN_LOT_VALUE and slab_count >= MIN_SLAB_COUNT
    # Singles-only and under $3,000.
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
    # Empty entry (created on the CSV-rejected / helper-down paths just to gate
    # "proceed"): there's no priced lot to itemize, so don't tell a slab customer
    # they're "$3,000 of One Piece singles short" — their quote simply isn't done.
    if slab_count == 0 and singles == 0:
        return ("⏳ **Your quote isn't finalized yet** — Kevin is reviewing it and "
                "will confirm shortly. Hang tight!")
    lines = ["📊 **Heads up — this lot doesn't meet our buying minimums yet.**", ""]
    if slab_count and singles:
        lines.append(
            f"PSA slabs: **{slab_count}** (${slab_value:,.2f})  •  "
            f"One Piece singles: **${singles:,.2f}**  •  Combined: **${combined:,.2f}**"
        )
    elif slab_count:
        lines.append(f"PSA slabs: **{slab_count}** (${slab_value:,.2f})")
    else:
        lines.append(f"One Piece singles: **${singles:,.2f}**")
    lines.append("")
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
            "to reach 15, **or** get your **One Piece singles alone to $3,000+** (upload your Collectr CSV) and "
            "we'll take the slabs regardless of count. Then I'll get you set up to ship! 🙌",
        ]
    else:
        lines += [
            f"• For a One Piece singles-only lot, your Collectr export must be **${MIN_LOT_VALUE:,}+** in value — "
            f"you're **${MIN_LOT_VALUE - singles:,.2f}** short.",
            "",
            "Add more **One Piece raw singles** and re-upload your Collectr CSV (or add **PSA slabs** — "
            f"note that any lot containing slabs needs **{MIN_SLAB_COUNT}+** of them). 🙌",
        ]
    return "\n".join(lines)
WELCOME_MSG = (
    "👋 Welcome to KTS Collectibles!\n\n"
    "We're currently buying:\n"
    "• **PSA graded slabs** (Pokémon, Basketball, Football, MLB & One Piece) → send your cert numbers\n"
    "• **One Piece raw singles** (English, Near Mint, $1–$150) → upload your Collectr CSV export\n\n"
    "⚠️ We are **not** buying Pokémon raw cards at this time.\n\n"
    "📊 **Minimum lot requirements:**\n"
    "• Lots **with PSA slabs:** at least **15 slabs** AND **$3,000+** total value (slabs + any One Piece singles combined).\n"
    "• **One Piece singles only:** your Collectr export must be **$3,000+** in value.\n\n"
    "🔢 We prioritize **quantity** — we won't take a lot that's just a few big-ticket slabs.\n\n"
    "What are you looking to sell?"
)

SHIPPING_MSG = (
    "📦 **Awesome, let's do it!** Ship your cards to Kevin and you'll be paid out once your package arrives and is processed.\n\n"
    "**Ship to:**\n"
    "Kevin Smith\n"
    "1363 Boylston St\n"
    "Unit 368\n"
    "Boston MA 02215\n\n"
    "🚚 **Shipping method — required:** All lots must be sent **UPS Overnight** (strongly preferred) "
    "or **UPS 2-Day** at the latest. Please **do not** use ground or any slower service.\n\n"
    "📝 **Please include a note inside your package with:**\n"
    "• Your Discord username\n"
    "• Amount owed\n"
    "• Preferred payment method (Wire or ACH)\n\n"
    "📥 **Packaging requirements — please read, these affect your payout:**\n"
    "• **Raw cards:** penny sleeve only — **no top loaders.** Cards shipped in top loaders = **2% deducted** from payout.\n"
    "• **Slabs:** ship as-is — **no sleeves or stickers.** Slabs shipped in sleeves or with stickers = **2% deducted** from payout.\n"
    "• **No note** (or missing required info above) = **2% deducted** from payout.\n\n"
    "⚠️ Without a note we also won't know who the package is from, so payment may be delayed on top of the deduction.\n\n"
    "🗓️ **Payouts run Thursdays & Fridays only.**\n"
    "It's **first come, first serve** — you join the payout queue the moment your package is scanned in on arrival.\n"
    "• Package in **Monday** → usually paid **Thursday**\n"
    "• Package in **Tuesday** → usually paid **Friday**\n"
    "• Arrives later in the week → rolls into the **next** Thursday/Friday payout.\n\n"
    "👉 Best move: **overnight it Monday so it lands Tuesday** to make that week's queue. Payment via wire or ACH ⚡\n\n"
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

def handle_vip_command(content, mentions=None, guild=None):
    """Parse and apply a !vip command; returns the reply text (sync, testable).
    Target resolution: a mention TOKEN present in the command text wins, matched
    to the resolved user by id — raw message.mentions is NEVER trusted on its
    own, because Discord's reply-ping injects the replied-to user into it in
    arbitrary order (that could assign a VIP rate to the wrong customer). A
    typed username is validated against real server members when possible."""
    toks = content.strip().split()[1:]   # drop '!vip'
    sub = (toks[0].lower() if toks else "list")

    def _mention_target():
        m = re.search(r'<@!?(\d+)>', content)
        if not m:
            return None, None
        uid = int(m.group(1))
        user = next((u for u in (mentions or []) if getattr(u, 'id', None) == uid), None)
        if user is None:
            return None, "Couldn't resolve that @mention — try the plain username instead."
        return user.name.lower(), None

    if sub == "list":
        if not VIP_RATES:
            return "No VIP users set. Add one with `!vip add <username> 91` (or `91 88 85` per band)."
        lines = ["✨ **VIP Pokémon rates:**"]
        labels = _pokemon_band_labels()
        for name, entry in sorted(VIP_RATES.items()):
            tiers = vip_pokemon_tiers(name)
            parts = ", ".join(f"{labels[i]} → {t[2]*100:g}%" for i, t in enumerate(tiers))
            lines.append(f"• **{name}**: {parts}")
        return "\n".join(lines)

    if sub in ("add", "set"):
        rest = toks[1:]
        name, err = _mention_target()
        if err:
            return err
        if name:
            rest = [t for t in rest if not re.match(r'^<@!?\d+>$', t)]
        else:
            if not rest:
                return "Usage: `!vip add <username> 91` or `!vip add <username> 91 88 85`"
            name = rest[0].lstrip('@').lower()
            rest = rest[1:]
            try:
                float(name)
                return (f"'{toks[1]}' looks like a rate, not a username — "
                        f"usage: `!vip add <username> 91 [88 85]`")
            except ValueError:
                pass
            if not re.match(r'^[a-z0-9._]{2,32}$', name):
                return f"'{toks[1]}' doesn't look like a Discord username — not saved."
            if guild is not None:
                exact = next((mb for mb in guild.members if mb.name.lower() == name), None)
                if exact is None:
                    cands = [mb.name for mb in guild.members
                             if name in mb.name.lower()
                             or name in (mb.display_name or '').lower()
                             or name in ((getattr(mb, 'global_name', '') or '')).lower()][:3]
                    hint = (" Did you mean: " + ", ".join(f"`{c}`" for c in cands) + "?") if cands else ""
                    return (f"No server member has the exact username `{name}` — not saved "
                            f"(VIP keys must match their login username, not display name).{hint}")
        if not rest:
            return "Usage: `!vip add <username> 91` or `!vip add <username> 91 88 85`"
        rates = []
        for t in rest[:len(PSA_POKEMON_PER_CARD_TIERS)]:
            try:
                v = float(t.replace('%', ''))
            except ValueError:
                return f"'{t}' isn't a number — usage: `!vip add <username> 91 88 85`"
            if v > 1.5:
                v = v / 100.0
            if not (0.5 <= v <= 1.2):
                return f"{t} is outside the sane range (50-120%) — not saved."
            rates.append(round(v, 4))
        VIP_RATES[name] = {"pokemon": rates}
        _save_vip()
        labels = _pokemon_band_labels()
        tiers = vip_pokemon_tiers(name)
        parts = ", ".join(f"{labels[i]} → {t[2]*100:g}%" for i, t in enumerate(tiers))
        return (f"✨ VIP set for **{name}**: Pokémon {parts}\n"
                f"(pinned until you change it — weekly flyer updates won't move it; "
                f"grade/ceiling/sale-age rules stay standard)")

    if sub in ("remove", "delete", "del"):
        name, err = _mention_target()
        if err:
            return err
        if not name:
            # Raw typed key on purpose (no charset/guild checks): lets Kevin clean
            # up any legacy/odd key exactly as `!vip list` shows it.
            name = (toks[1].lstrip('@').lower() if len(toks) > 1 else "")
        if name in VIP_RATES:
            del VIP_RATES[name]
            _save_vip()
            return f"Removed **{name}** from VIP — they get standard rates now."
        return f"**{name or '?'}** isn't on the VIP list. `!vip list` to see who is."

    return "Commands: `!vip add <username> 91 [88 85]` · `!vip remove <username>` · `!vip list`"

def build_sheet_h_formula(r, pokemon_tiers=None):
    """The per-row payout formula written into buying sheets — generated from the
    bot's CURRENT rate constants so bot and sheet can't disagree. pokemon_tiers
    overrides only the Pokémon band RATES (VIP sheets). NOTE: assumes the current
    band structure (3 pokemon bands, 3 football bands, 2 one piece bands) — if a
    weekly flyer changes the band COUNT, update this builder with it."""
    def _n(v):
        return f"{v:g}"   # 1.0 -> "1", 0.89 -> "0.89" (matches the template's style)
    pt = pokemon_tiers or PSA_POKEMON_PER_CARD_TIERS
    p1, p2, p3 = _n(pt[0][2]), _n(pt[1][2]), _n(pt[2][2])
    p_mid_top = int(pt[1][1])
    pok_max = PSA_SPORT_MAX_PRICE['pokemon']
    nba_max = PSA_SPORT_MAX_PRICE['basketball']
    op_max = PSA_SPORT_MAX_PRICE['one piece']
    nfl_max = PSA_SPORT_MAX_PRICE['football']
    f1, f2, f3 = (_n(PSA_FOOTBALL_PER_CARD_TIERS[0][2]), _n(PSA_FOOTBALL_PER_CARD_TIERS[1][2]),
                  _n(PSA_FOOTBALL_PER_CARD_TIERS[2][2]))
    op1, op2 = _n(PSA_ONE_PIECE_PER_CARD_TIERS[0][2]), _n(PSA_ONE_PIECE_PER_CARD_TIERS[1][2])
    g = PSA_MIN_GRADE
    gp = POKEMON_HIGH_BAND_MIN_GRADE
    gop = ONE_PIECE_MIN_GRADE
    maxage = (f'IF(F{r}="pokemon",IF(G{r}<100,60,30),IF(F{r}="basketball",IF(G{r}<={nba_max},99999,90),'
              f'IF(OR(F{r}="football",F{r}="mlb",F{r}="baseball"),IF(G{r}<100,99999,90),'
              f'IF(F{r}="other",60,30))))')
    rate = (f'IFS('
            f'F{r}="pokemon",IF(G{r}<=100,IF(N(E{r})<{g},0,{p1}),IF(G{r}<{p_mid_top},IF(N(E{r})<{g},0,{p2}),IF(G{r}<={pok_max},IF(N(E{r})<{gp},0,{p3}),0))),'
            f'F{r}="basketball",IF(G{r}<={nba_max},0.95,0),'
            f'F{r}="football",IF(N(E{r})<{g},0,IF(G{r}>{nfl_max},0,IF(G{r}<=30,{f1},IF(G{r}<100,{f2},{f3})))),'
            f'OR(F{r}="mlb",F{r}="baseball"),0,'
            f'F{r}="other",IF(N(E{r})<{gop},0,IF(AND(G{r}>=1,G{r}<=100),{op1},IF(G{r}<={op_max},{op2},0))),'
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
        + ".  Open the tracker → **Sync from bot**.")

def _sanitize_channel_name(name):
    """Discord channel names: lowercase letters/digits/dash/underscore. Usernames
    with periods (e.g. 'cwilk_sportscards.') lose them — the Drive sheet lookup
    already tries both the channel name and the author's raw username."""
    s = re.sub(r'[^a-z0-9_-]', '', (name or '').lower().replace(' ', '-'))
    return s[:90]

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
    is_ticket = _is_text and ("ticket" in message.channel.name.lower() or "ticket" in _cat_name)
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

    # ── COLLECTR CSV (ONE PIECE) ─────────────────────────────────────────────────
    if csv_attachment:
        async with message.channel.typing():
            try:
                csv_bytes = await csv_attachment.read()
                result, error = parse_collectr_csv(csv_bytes)
                if error:
                    await message.channel.send(f"Couldn't read that file — {error}. Try re-exporting from Collectr.")
                    raise _CsvDone()

                issues = result.get("issues", [])
                for issue_type, cards in issues:
                    card_list = "\n".join(cards[:5])
                    if len(cards) > 5:
                        card_list += f"\n• ...and {len(cards)-5} more"
                    if issue_type == "pokemon":
                        await message.channel.send(
                            f"❌ **We're not buying Pokémon raw cards right now.**\n\n"
                            f"Detected Pokémon cards in your CSV:\n{card_list}\n\n"
                            f"We're currently only buying **One Piece raw singles** (English, NM, $1–$150). "
                            f"We're still happy to take a look at any **PSA graded Pokémon slabs** you have — "
                            f"just drop your cert numbers here! 🙏"
                        )
                    elif issue_type == "other_game":
                        await message.channel.send(
                            f"❌ **We only buy One Piece raw singles right now:**\n{card_list}\n\n"
                            f"Please remove these and re-export."
                        )
                    elif issue_type == "non_english":
                        await message.channel.send(
                            f"❌ **Non-English cards — we only buy English One Piece:**\n{card_list}\n\n"
                            f"Please remove these and re-export."
                        )
                    elif issue_type == "over_max":
                        await message.channel.send(
                            f"❌ **Cards over ${RAW_MAX_PRICE} — we can't buy these as raws:**\n{card_list}\n\n"
                            f"Our limit is **${RAW_MIN_PRICE}–${RAW_MAX_PRICE} per card**. Remove these and re-export."
                        )
                    elif issue_type == "under_min":
                        await message.channel.send(
                            f"❌ **Cards under ${RAW_MIN_PRICE} — we can't buy these:**\n{card_list}\n\n"
                            f"Our minimum is **${RAW_MIN_PRICE} per card**. Remove these and re-export."
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
                rate, tier_label = get_payout_rate(total, username)
                payout = total * rate
                last_offer[channel_id] = {"payout": payout, "total": total, "rate": rate}

                # Record singles value toward the buying minimums.
                set_singles_value(channel_id, total)
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
                offer_body = (
                    f"✅ **Your offer:**\n\n"
                    f"📦 **{card_count} cards** | Market value: **${total:,.2f}**\n"
                    f"💰 **Payout: ${payout:,.2f}** ({int(rate*100)}%)\n\n"
                )
                offer_body += hold_tail if hold_tail else "Let me know if you'd like to proceed!"
                for chunk in _split_for_discord(offer_body):
                    await message.channel.send(chunk)
                kevin_prefix = "" if lot_qualifies(channel_id) else f"⏳ **[BELOW MINIMUM — HOLD]** "
                kevin_msg = (
                    f"{kevin_prefix}💚 **Collectr offer sent — {username}** (One Piece)\n"
                    f"{card_count} cards | ${total:,.2f} market | {int(rate*100)}% | **${payout:,.2f}**"
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
        await ping_kevin(f"✅ **{username} agreed** — shipping address sent.", message.channel)
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
