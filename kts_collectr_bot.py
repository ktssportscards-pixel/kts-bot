"""
KTS Collectibles — Full Discord Bot
=====================================
Handles TWO types of customers automatically:

1. PSA SLAB sellers:
   - Customer sends cert numbers
   - Bot creates a Google Sheet copy with cert numbers + CardLadder links
   - Pings Kevin with sheet link

2. RAW CARD sellers (Collectr) — ONE PIECE ONLY (May 2026):
   - Customer uploads their Collectr CSV export in DMs
   - Bot reads it, calculates total market value
   - One Piece English NM singles, $1-$99 per card
   - Applies correct % based on lot size:
       $3000 - $4000  → 86%
       $4000 - $5000  → 87%
       $5000+         → 88%
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

# Raw card payout percentages by lot size — ONE PIECE singles.
# Singles-only lots have a $3,000 minimum (see MIN_LOT_VALUE), so the $0–$4k band
# is really the "$3,000–$4,000" tier; it only shows on sub-minimum (held) lots,
# which aren't actually paid out.
RAW_PAYOUT_TIERS = [
    (0,    4000,         0.86),   # $3,000–$4,000 → 86%
    (4000, 5000,         0.87),   # $4,000–$5,000 → 87%
    (5000, float('inf'), 0.88),   # $5,000+       → 88%
]

# Raw card per-card price limits (One Piece)
RAW_MIN_PRICE = 1
RAW_MAX_PRICE = 99

# PSA slab buying criteria
PSA_MIN_PRICE = 1
PSA_MIN_GRADE = 7
PSA_MAX_AGE_DAYS = 30
# Pokémon priced under $100 are bought at ANY grade (grade floor waived). Pokémon
# $100–$175, plus all basketball and one piece, still require PSA_MIN_GRADE+.
# The $100 boundary mirrors the Pokémon payout-tier split so a card's grade rule
# and its payout rate stay consistent.
POKEMON_ANY_GRADE_MAX_PRICE = 100
# Per-sport price ceilings — sports not listed here are rejected outright.
PSA_SPORT_MAX_PRICE = {
    'pokemon': 175,
    'basketball': 900,
    'one piece': 200,
    'mlb': 1000,
}
# Per-sport max age of last sale (days). Default is PSA_MAX_AGE_DAYS; MLB allows
# sales within the last 3 months.
PSA_SPORT_MAX_AGE_DAYS = {
    'mlb': 90,
    'basketball': 90,
}
# MLB cards at/above this value are accepted but FLAGGED for Kevin's manual review.
MLB_MANUAL_REVIEW_PRICE = 500
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
        MLB        -> threshold=0   (always use the avg-3 logic)
        NBA $250+  -> threshold=250 ($1-$250 keep direct CardLadder value)
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

# Pokemon payout:
#   $1-$100 cards  : rate scales with the TOTAL value of the $1-$100 Pokemon bucket
#                    in this lot (lot-size tiers below). Any grade (see grade rules).
#   $100-$175 cards: flat 87%, PSA 7+ (grade enforced in classify_psa_comp).
# Basketball: flat rate.  One Piece: tiered per individual card value.
PSA_POKEMON_LOW_MAX = 100        # cards under $100 use the lot-size tiers; $100+ → high rate
PSA_POKEMON_HIGH_RATE = 0.87     # $100-$175 Pokemon

# Lot-size tiers for the $1-$100 Pokemon bucket — rate is chosen by the TOTAL
# value of just the $1-$100 Pokemon cards in the lot.
PSA_POKEMON_LOW_LOT_TIERS = [
    (0,     1500,         0.90),   # under $1,500    → 90% (base)
    (1500,  3000,         0.91),   # $1,500-$3,000   → 91%
    (3000,  5000,         0.915),  # $3,000-$5,000   → 91.5%
    (5000,  10000,        0.92),   # $5,000-$10,000  → 92%
    (10000, 15000,        0.925),  # $10,000-$15,000 → 92.5%
    (15000, float('inf'), 0.93),   # $15,000+        → 93%
]
# Basketball (NBA): $1-$250 → 95%, $250-$900 → 90%, by individual card value.
PSA_BASKETBALL_LOW_MAX = 250
PSA_BASKETBALL_LOW_RATE = 0.95
PSA_BASKETBALL_HIGH_RATE = 0.90
PSA_MLB_PER_CARD_TIERS = [
    (0,    100,          0.95),   # $1-$100   → 95%
    (100,  float('inf'), 0.90),   # $100-$1000 → 90%
]
PSA_ONE_PIECE_PER_CARD_TIERS = [
    (0,    100,          0.87),  # $1-$100 → 87%
    (100,  float('inf'), 0.84),  # $100-$200 → 84%
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


def pokemon_low_lot_rate(low_bucket_total):
    """Rate for the $1-$100 Pokemon bucket, chosen by that bucket's TOTAL value."""
    for low, high, rate in PSA_POKEMON_LOW_LOT_TIERS:
        if low <= low_bucket_total < high:
            return rate
    return PSA_POKEMON_LOW_LOT_TIERS[-1][2]


def _pokemon_effective_rate(card_values):
    """
    Blend the Pokemon payout: the $1-$100 bucket gets a rate based on its TOTAL
    value (lot-size tiers); $100-$175 cards get the flat high rate. Returns the
    effective blended rate so sport_total * rate == total payout.
    """
    if not card_values:
        return PSA_POKEMON_LOW_LOT_TIERS[0][2]
    low_total = sum(cv for cv in card_values if cv < PSA_POKEMON_LOW_MAX)
    high_total = sum(cv for cv in card_values if cv >= PSA_POKEMON_LOW_MAX)
    total = low_total + high_total
    if total <= 0:
        return PSA_POKEMON_LOW_LOT_TIERS[0][2]
    low_rate = pokemon_low_lot_rate(low_total)
    payout = low_total * low_rate + high_total * PSA_POKEMON_HIGH_RATE
    return payout / total


def _basketball_effective_rate(card_values):
    """NBA: $1-$250 → 95%, $250-$900 → 90% by card value. Returns blended rate."""
    if not card_values:
        return PSA_BASKETBALL_LOW_RATE
    total = sum(card_values)
    if total <= 0:
        return PSA_BASKETBALL_LOW_RATE
    payout = 0.0
    for cv in card_values:
        payout += cv * (PSA_BASKETBALL_LOW_RATE if cv <= PSA_BASKETBALL_LOW_MAX else PSA_BASKETBALL_HIGH_RATE)
    return payout / total


def get_psa_payout_rate(sport, sport_lot_total, card_values=None):
    """
    Return the effective (blended) payout rate for a sport's accepted cards.
    - pokemon: $1-$100 cards use a lot-size tier (rate scales with that bucket's
      total value); $100-$175 cards are flat 87%.
    - basketball: flat rate.
    - one piece: tiered per individual card; returns the effective blended rate.
    """
    if sport == 'pokemon':
        return _pokemon_effective_rate(card_values)
    if sport == 'basketball':
        return _basketball_effective_rate(card_values)
    if sport == 'one piece':
        return _blended_per_card_rate(PSA_ONE_PIECE_PER_CARD_TIERS, card_values)
    if sport == 'mlb':
        return _blended_per_card_rate(PSA_MLB_PER_CARD_TIERS, card_values)
    return PSA_POKEMON_LOW_LOT_TIERS[0][2]

# VIP rates PAUSED while we're buying One Piece raws (May 2026).
# Top of standard tier is 85%, so legacy VIP rates of 87/89% would exceed margin.
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


def parse_collectr_csv(content_bytes):
    """
    Parse a Collectr CSV export and return total market value + card list.
    Validates cards against KTS One Piece buying requirements:
      - Must be One Piece TCG (Pokémon politely declined)
      - English only
      - $1-$99 per card
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
    #   2. Non-ASCII characters in the name or set (for cards that weren't tagged)
    non_english_tags = ('(jp)', '(kr)', '(cn)', '(tw)', '(kor)', '(jpn)', '(chn)')
    non_english = []
    for _, row in df.iterrows():
        name = str(row.get('Product Name', ''))
        set_name = str(row.get('Set', ''))
        combined = name + set_name
        name_lower = name.lower()
        if any(tag in name_lower for tag in non_english_tags):
            non_english.append(f"• {name} ({set_name})")
        elif any(ord(c) > 127 for c in combined):
            non_english.append(f"• {name} ({set_name})")

    # Per-card price range: $1-$99
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


HELPER_CHUNK_SIZE = 4  # Matches helper CONCURRENCY=4. CardLadder under load + Cloudflare challenge can push per-cert time to 60-90s; chunks of 4 (one parallel batch) keep total under the ~100s tunnel timeout.

def lookup_comps(certs):
    """
    Call helper.ktscollectibles.com/comp/batch in chunks and return the merged results list.
    Each result: {cert, found, clValue, cardName, grade, recentSales, avg3Sales, salesCount, note}
    Returns [] if the helper key isn't set. Per-chunk failures are logged; if every chunk
    fails the last error is re-raised so the caller can surface it.
    """
    if not HELPER_API_KEY or not certs:
        return []
    import urllib.request as urlreq
    cert_list = [str(c).strip() for c in certs]
    chunks = [cert_list[i:i + HELPER_CHUNK_SIZE] for i in range(0, len(cert_list), HELPER_CHUNK_SIZE)]
    all_results = []
    last_error = None
    for idx, chunk in enumerate(chunks, 1):
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
        return ('rejected', f"{sport_label} (we only buy pokemon, basketball, mlb, and one piece)")
    if cv > max_price:
        return ('rejected', f"${cv:,.0f} (over our ${max_price} {sport} max)")
    if cv < PSA_MIN_PRICE:
        return ('rejected', f"${cv:.2f} (under ${PSA_MIN_PRICE} min)")
    grade_raw = str(comp.get('grade') or '').replace('PSA', '').strip()
    try:
        g = float(grade_raw)
    except ValueError:
        return ('rejected', f"grade '{grade_raw}' unrecognized")
    # Grade floor. Pokémon under $100 → ANY grade accepted; everything else
    # (Pokémon $100+, basketball, one piece) → PSA_MIN_GRADE+.
    pokemon_any_grade = (sport == 'pokemon' and cv < POKEMON_ANY_GRADE_MAX_PRICE)
    if not pokemon_any_grade and g < PSA_MIN_GRADE:
        return ('rejected', f"PSA {grade_raw} (we buy {PSA_MIN_GRADE}-10 only)")
    last_sale = comp.get('lastSaleDate')
    if not last_sale:
        return ('rejected', 'no recent sale visible')
    try:
        last_d = datetime.strptime(str(last_sale).strip(), '%Y-%m-%d').date()
    except (ValueError, AttributeError):
        return ('rejected', f"unparseable sale date '{last_sale}'")
    max_age = PSA_SPORT_MAX_AGE_DAYS.get(sport, PSA_MAX_AGE_DAYS)
    if (date.today() - last_d).days > max_age:
        return ('rejected', f"last sale {last_sale} (>{max_age}d ago)")

    # Basketball-specific rejection rules (player bans, WNBA, unlicensed flag)
    if sport == 'basketball':
        return check_basketball_rejections(comp)

    # MLB: $500+ is accepted but flagged for Kevin's manual review.
    if sport == 'mlb' and cv >= MLB_MANUAL_REVIEW_PRICE:
        return ('flag', f"${cv:,.0f} — $500+ MLB, verify sales before paying")

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


# ── DISCORD BOT ──────────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
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
    "• **PSA graded slabs** (Pokémon, Basketball, MLB & One Piece) → send your cert numbers\n"
    "• **One Piece raw singles** (English, Near Mint, $1–$99) → upload your Collectr CSV export\n\n"
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
    try:
        kevin = await bot.fetch_user(YOUR_DISCORD_USER_ID)
        channel_link = f"\n**Ticket:** <#{channel.id}>" if channel else ""
        for chunk in _split_for_discord(msg + channel_link):
            await kevin.send(chunk)
    except Exception as e:
        print(f"Could not ping Kevin: {e}")

def is_agreeing(text):
    # Trigger shipping when the message is just an agree word by itself
    # ("ship" or "proceed"), tolerating trailing punctuation/spaces.
    cleaned = text.strip().lower().strip(".!?, ")
    return cleaned in AGREE_KEYWORDS

import aiohttp.web as _ow_web

# ── OWED / PACKAGES — the bot stores deals and serves them to the local tracker ──
OWED_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "owed_store.json")
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

async def start_owed_webserver():
    app = _ow_web.Application()
    app.add_routes([
        _ow_web.get("/", _owed_root),
        _ow_web.get("/health", _owed_root),
        _ow_web.get("/owed", _owed_get),
        _ow_web.get("/owed/paid", _owed_paid),
    ])
    runner = _ow_web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", "8080"))
    await _ow_web.TCPSite(runner, "0.0.0.0", port).start()
    print(f"✅ Owed web endpoint listening on 0.0.0.0:{port}  (GET /owed)")

_OWED_WEB_STARTED = False

@bot.event
async def on_ready():
    global _OWED_WEB_STARTED
    print(f"✅ KTS Collectibles Bot online as {bot.user}")
    if not _OWED_WEB_STARTED:
        _OWED_WEB_STARTED = True
        try:
            await start_owed_webserver()
        except Exception as e:
            print(f"owed web server failed to start: {e}")

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

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    # ── !owe command (Kevin only) — log a package he owes, anywhere ──
    if message.content.strip().lower().startswith('!owe'):
        if message.author.id == YOUR_DISCORD_USER_ID:
            await handle_owe(message)
        return

    is_ticket = isinstance(message.channel, discord.TextChannel) and "ticket" in message.channel.name.lower()
    if not is_ticket:
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
    # Send the welcome EXACTLY ONCE, at the very start of the ticket. We look at the
    # OLDEST messages (where the welcome always lives) instead of the most recent —
    # so the check never scrolls off no matter how long the ticket gets, and it
    # still works after a bot restart (in-memory set would be empty then).
    if channel_id not in welcomed_tickets:
        already_welcomed = False
        try:
            async for msg in message.channel.history(limit=25, oldest_first=True):
                if msg.author == bot.user and "Welcome to KTS Collectibles" in (msg.content or ""):
                    already_welcomed = True
                    break
        except Exception:
            pass
        welcomed_tickets.add(channel_id)
        if not already_welcomed:
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
                    return

                issues = result.get("issues", [])
                for issue_type, cards in issues:
                    card_list = "\n".join(cards[:5])
                    if len(cards) > 5:
                        card_list += f"\n• ...and {len(cards)-5} more"
                    if issue_type == "pokemon":
                        await message.channel.send(
                            f"❌ **We're not buying Pokémon raw cards right now.**\n\n"
                            f"Detected Pokémon cards in your CSV:\n{card_list}\n\n"
                            f"We're currently only buying **One Piece raw singles** (English, NM, $1–$99). "
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
                    return

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
                    urlreq.urlopen(req, timeout=15)
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

            except Exception as e:
                print(f"Collectr error: {e}")
                await message.channel.send("Had an issue with that file — Kevin will take a look!")
                await ping_kevin(f"⚠️ Collectr error — **{username}**: {str(e)}", message.channel)
        return

    # ── PSA CERT NUMBERS ─────────────────────────────────────────────────────────
    if certs:
        async with message.channel.typing():
            try:
                await message.channel.send(
                    f"Got it! Setting up your buying sheet for {len(certs)} cert{'s' if len(certs) > 1 else ''}... ⏳"
                )
                sheet_url, sheet_name, data = create_psa_sheet(username, certs)
                sheet_id = data.get("sheet_id") or extract_sheet_id(sheet_url)
                if sheet_id:
                    channel_sheet[channel_id] = sheet_id

                # Tell the customer the sheet is ready BEFORE the slow helper call.
                # Helper lookup for 50 certs can be 1-2 min; don't make them wait.
                await message.channel.send(
                    f"✅ Sheet ready! Pulling CardLadder comps now... ⏳\n\n"
                    f"📊 {sheet_url}"
                )

                # Now look up comps and fill the sheet (slow path).
                comps = []
                comp_error = None
                try:
                    comps = await asyncio.to_thread(lookup_comps, certs)
                    # MLB: replace CL value with min(avg-of-last-3-sales, CL value)
                    # BEFORE filling the sheet or classifying, so the sheet's
                    # "Our comp" and all pricing use that value.
                    for _c in comps:
                        _sp = normalize_sport(_c.get('sport'))
                        if _sp == 'mlb':
                            apply_avg3_value(_c, threshold=0)        # MLB: always
                        elif _sp == 'basketball':
                            apply_avg3_value(_c, threshold=250)      # NBA: $250+ only
                    if sheet_id and comps:
                        await asyncio.to_thread(fill_buying_sheet, sheet_id, comps)
                except Exception as e:
                    comp_error = str(e)
                    print(f"Helper comp lookup error: {e}")

                if comps:
                    by_cert = {str(c.get('cert', '')).strip(): c for c in comps}
                    accepted = []
                    rejected = []
                    flagged = []   # accepted cards that need Kevin's manual review
                    kevin_lines = []
                    for cert in certs:
                        c = by_cert.get(str(cert).strip())
                        status, reason = classify_psa_comp(c)
                        if status == 'accepted':
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

                    sport_groups = {}
                    for c in accepted:
                        sp = normalize_sport(c.get('sport'))
                        sport_groups.setdefault(sp, []).append(c)
                    sport_breakdown = []
                    for sp, comps_in_sport in sport_groups.items():
                        card_values = [float(c['clValue']) for c in comps_in_sport]
                        sport_total = sum(card_values)
                        rate = get_psa_payout_rate(sp, sport_total, card_values)
                        sport_breakdown.append({
                            'sport': sp,
                            'count': len(comps_in_sport),
                            'total': sport_total,
                            'rate': rate,
                            'payout': sport_total * rate,
                        })
                    total_comp = sum(s['total'] for s in sport_breakdown)
                    total_payout = sum(s['payout'] for s in sport_breakdown)
                    n_accepted = len(accepted)
                    n_rejected = len(rejected)

                    if n_accepted:
                        last_offer[channel_id] = {
                            "payout": total_payout,
                            "total": total_comp,
                            "rate": (total_payout / total_comp) if total_comp else PSA_POKEMON_LOW_LOT_TIERS[0][2],
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
                        for s in sport_breakdown
                    ]

                    # Kevin DM
                    n_flagged = len(flagged)
                    flag_warning = ""
                    if n_flagged:
                        flag_warning = (
                            f"\n⚠️ **{n_flagged} card{'s' if n_flagged != 1 else ''} flagged for manual review** — verify licensing before paying!\n"
                            + "\n".join([f"   - `{cert}`: {reason}" for cert, _, reason in flagged])
                            + "\n"
                        )
                    summary = (
                        f"📋 **PSA sheet — {username}**\n"
                        f"{len(certs)} certs | Accepted **{n_accepted}** | Comp **${total_comp:,.2f}** | Payout **${total_payout:,.2f}**"
                        f"{f' | {n_rejected} rejected' if n_rejected else ''}"
                        f"{f' | {n_flagged} ⚠️ flagged' if n_flagged else ''}\n"
                        f"{sheet_url}\n"
                        + ("\n".join(breakdown_lines) + "\n\n" if breakdown_lines else "\n")
                        + flag_warning
                    )
                    if n_accepted and not lot_qualifies(channel_id):
                        _sc, _sv, _si, _cb = lot_summary(channel_id)
                        summary = f"⏳ **[BELOW MINIMUM — HOLD]** {_sc} slab(s), combined ${_cb:,.2f}\n" + summary
                    await ping_kevin(summary + "\n".join(kevin_lines), message.channel)

                    # Customer follow-up
                    customer_parts = ["✅ All comps loaded!", ""]
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
                        await message.channel.send(chunk)
                else:
                    # Helper unavailable — fall back to old behavior
                    cert_list = "\n".join([f"• `{c}`" for c in certs])
                    err_note = f"\n\n⚠️ Helper offline ({comp_error})" if comp_error else ""
                    await ping_kevin(
                        f"📋 **PSA sheet — {username}**\n"
                        f"{len(certs)} certs | {sheet_url}{err_note}\n\n"
                        f"{cert_list}",
                        message.channel
                    )
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
        if channel_id in channel_sheet:
            try:
                import urllib.request as urlreq
                post_data = json.dumps({
                    "action": "add_tracking",
                    "sheet_id": channel_sheet[channel_id],
                    "username": username
                }).encode("utf-8")
                req = urlreq.Request(
                    APPS_SCRIPT_URL,
                    data=post_data,
                    headers={"Content-Type": "application/json"}
                )
                urlreq.urlopen(req, timeout=15)
                print(f"Added tracking row for {username}")
            except Exception as e:
                print(f"Tracking row error (non-critical): {e}")
        return

    # ── TRACKING NUMBER ───────────────────────────────────────────────────────────
    tracking_match = re.search(r'\b([0-9]{20,22}|1Z[A-Z0-9]{16}|[0-9]{12,15})\b', text)
    if tracking_match:
        tracking_num = tracking_match.group(1)
        sheet_id = channel_sheet.get(channel_id)
        if not sheet_id:
            try:
                creds = get_credentials()
                from googleapiclient.discovery import build
                drive = build("drive", "v3", credentials=creds)
                channel_name = message.channel.name
                results = drive.files().list(
                    q=f"'{PSA_FOLDER_ID}' in parents and name contains '{channel_name}' and trashed=false",
                    fields="files(id,name)",
                    orderBy="createdTime desc",
                    pageSize=1
                ).execute()
                files = results.get("files", [])
                if files:
                    sheet_id = files[0]["id"]
                    channel_sheet[channel_id] = sheet_id
                    print(f"Found sheet for {channel_name} via Drive lookup: {sheet_id}")
            except Exception as e:
                print(f"Drive lookup error: {e}")
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
                urlreq.urlopen(req, timeout=15)
                print(f"Saved tracking {tracking_num} for {username}")
            except Exception as e:
                print(f"Tracking save error (non-critical): {e}")
        return

    # ── STAY SILENT ───────────────────────────────────────────────────────────────

# ── RUN ──────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Starting KTS Collectibles Bot...")
    bot.run(DISCORD_BOT_TOKEN)
