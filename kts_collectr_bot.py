"""
KTS Collectibles — Full Discord Bot
=====================================
Handles TWO types of customers automatically:

1. PSA SLAB sellers:
   - Customer sends cert numbers
   - Bot creates a Google Sheet copy with cert numbers + CardLadder links
   - Pings Kevin with sheet link

2. RAW CARD sellers (Collectr):
   - Customer uploads their Collectr CSV export in DMs
   - Bot reads it, calculates total market value
   - Applies correct % based on lot size:
       $1 - $500    → 84%
       $500 - $1000 → 85%
       $1000 - $2000→ 86%
       $2000+ → 87%
       Bulk 87% → up to 87% (Kevin decides)
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

# Raw card payout percentages by lot size
RAW_PAYOUT_TIERS = [
    (0,    500,          0.84),
    (500,  1000,         0.85),
    (1000, 2000,         0.86),
    (2000, float('inf'), 0.87),
]

# PSA slab buying criteria
PSA_MIN_PRICE = 1
PSA_MIN_GRADE = 7
PSA_MAX_AGE_DAYS = 30
# Per-sport price ceilings — sports not listed here are rejected outright.
PSA_SPORT_MAX_PRICE = {
    'pokemon': 200,
    'basketball': 250,
}
# Pokemon: flat rate. Basketball: tiered by the basketball-only lot total.
PSA_POKEMON_PAYOUT_RATE = 0.87
PSA_BASKETBALL_PAYOUT_TIERS = [
    (0,    1000,         0.93),
    (1000, 3000,         0.95),
    (3000, float('inf'), 0.96),
]


def get_psa_payout_rate(sport, sport_lot_total):
    """Return the payout rate for a given sport's accepted lot total."""
    if sport == 'basketball':
        for low, high, rate in PSA_BASKETBALL_PAYOUT_TIERS:
            if low <= sport_lot_total < high:
                return rate
        return PSA_BASKETBALL_PAYOUT_TIERS[-1][2]
    return PSA_POKEMON_PAYOUT_RATE

# VIP clients who always get 87% regardless of lot size
VIP_CLIENTS = ["nickj1234", "gbywby"]
VIP_CLIENTS_89 = ["icevyy"]  # Gets 89% — mod in server

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
    return 0.84, "standard rate"


def parse_collectr_csv(content_bytes):
    """
    Parse a Collectr CSV export and return total market value + card list.
    Also validates cards against KTS buying requirements.
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
    TAG_TEAM_KEYWORDS = [" &", "tag team", "gx tag"]

    PRE_2020_SETS = {
        "base set", "jungle", "fossil", "base set 2", "team rocket",
        "gym heroes", "gym challenge", "neo genesis", "neo discovery",
        "neo revelation", "neo destiny", "legendary collection", "expedition",
        "aquapolis", "skyridge", "ruby & sapphire", "sandstorm", "dragon",
        "team magma vs team aqua", "hidden legends", "firered & leafgreen",
        "team rocket returns", "deoxys", "emerald", "unseen forces",
        "delta species", "legend maker", "holon phantoms", "crystal guardians",
        "dragon frontiers", "power keepers", "diamond & pearl",
        "mysterious treasures", "secret wonders", "great encounters",
        "majestic dawn", "legends awakened", "stormfront", "platinum",
        "rising rivals", "supreme victors", "arceus", "heartgold & soulsilver",
        "unleashed", "undaunted", "triumphant", "call of legends",
        "black & white", "emerging powers", "noble victories", "next destinies",
        "dark explorers", "dragons exalted", "boundaries crossed",
        "plasma storm", "plasma freeze", "plasma blast", "legendary treasures",
        "xy", "flashfire", "furious fists", "phantom forces", "primal clash",
        "double crisis", "roaring skies", "ancient origins", "breakthrough",
        "breakpoint", "generations", "fates collide", "steam siege",
        "evolutions", "sun & moon", "guardians rising", "burning shadows",
        "shining legends", "crimson invasion", "ultra prism", "forbidden light",
        "celestial storm", "dragon majesty", "lost thunder", "team up",
        "detective pikachu", "unbroken bonds", "unified minds", "hidden fates",
        "cosmic eclipse",
    }

    # Check for trainer cards — not buying any trainers for now
    trainer_cards = []
    if 'Category' in df.columns:
        for _, row in df.iterrows():
            if str(row.get('Category', '')).strip().lower() == 'trainer':
                trainer_cards.append(f"• {str(row.get('Product Name', 'Unknown'))}")

    # Check for non-English cards (Japanese, Korean, Chinese characters in name or set)
    non_english = []
    for _, row in df.iterrows():
        name = str(row.get('Product Name', ''))
        set_name = str(row.get('Set', ''))
        combined = name + set_name
        if any(ord(c) > 127 for c in combined):
            non_english.append(f"• {name} ({set_name})")

    over_100 = []
    for _, row in df.iterrows():
        price = float(row[price_col])
        name = str(row.get('Product Name', 'Unknown'))
        if price > 100:
            over_100.append(f"• {name} — ${price:.2f}")

    pre_2020_found = []
    set_col = 'Set' if 'Set' in df.columns else None
    if set_col:
        for _, row in df.iterrows():
            set_name = str(row.get('Set', '')).lower().strip()
            name = str(row.get('Product Name', 'Unknown'))
            is_tag_team = any(t in name.lower() for t in TAG_TEAM_KEYWORDS)
            if is_tag_team:
                continue
            if set_name in PRE_2020_SETS:
                pre_2020_found.append(f"• {name} ({row.get('Set', '')})")

    issues = []
    if trainer_cards:
        issues.append(("trainer", trainer_cards))
    if non_english:
        issues.append(("non_english", non_english))
    if over_100:
        issues.append(("over_100", over_100))
    if pre_2020_found:
        issues.append(("pre_2020", pre_2020_found))

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


def check_low_value_cards(df):
    """
    For cards under $5, check against KTS buying list.
    Returns list of rejected card names.
    """
    VSTAR_EXCLUDED = ["drapion", "simisear", "mawile", "regidrago"]
    RADIANT_EXCLUDED = ["chargabug", "tsareena", "sealeo"]

    price_col = None
    for col in df.columns:
        if "Market Price" in col:
            price_col = col
            break
    if not price_col:
        return []

    rejected = []

    for _, row in df.iterrows():
        try:
            price = float(str(row.get(price_col, 0)).replace("$", "").replace(",", "") or 0)
        except:
            price = 0

        if price >= 5:
            continue

        name = str(row.get("Product Name", "")).lower()
        rarity = str(row.get("Rarity", "")).lower()
        set_name = str(row.get("Set", "")).lower()
        display_name = str(row.get("Product Name", "unknown card"))

        is_vmax = "vmax" in name
        is_vstar = "vstar" in name
        is_ex_or_v = " ex" in name or name.endswith(" ex") or " v " in name or name.endswith(" v")
        is_full_art_trainer = "full art" in name and rarity == "ultra rare" and not any(x in name for x in ["ex", " v ", "vmax", "vstar"])
        is_illustration_rare = "illustration rare" in rarity or "special illustration rare" in rarity
        is_trainer_gallery = "trainer gallery" in set_name
        is_galarian_gallery = "galarian gallery" in set_name
        is_rainbow = "hyper rare" in rarity
        is_gold = ("secret rare" in rarity or "hyper rare" in rarity) and "gold" in name and "item" not in name and "energy" not in name
        is_radiant = "radiant rare" in rarity or "radiant" in name
        is_amazing_rare = "amazing rare" in rarity

        if is_vstar:
            if any(x in name for x in VSTAR_EXCLUDED):
                rejected.append(display_name)
            continue

        if is_radiant:
            if any(x in name for x in RADIANT_EXCLUDED):
                rejected.append(display_name)
            continue

        if is_gold:
            continue

        if any([is_vmax, is_ex_or_v, is_full_art_trainer,
                is_illustration_rare, is_trainer_gallery, is_galarian_gallery,
                is_rainbow, is_amazing_rare]):
            continue

        rejected.append(display_name)

    return rejected


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


HELPER_CHUNK_SIZE = 5  # Helper page-load + vision can hit 30-60s per cert under load; chunks of 5 keep each request under Cloudflare tunnel's ~100s timeout.

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
    sport = (comp.get('sport') or '').lower().strip()
    max_price = PSA_SPORT_MAX_PRICE.get(sport)
    if max_price is None:
        sport_label = sport or 'unknown sport'
        return ('rejected', f"{sport_label} (we only buy pokemon and basketball)")
    if cv > max_price:
        return ('rejected', f"${cv:,.0f} (over our ${max_price} {sport} max)")
    if cv < PSA_MIN_PRICE:
        return ('rejected', f"${cv:.2f} (under ${PSA_MIN_PRICE} min)")
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
    if (date.today() - last_d).days > PSA_MAX_AGE_DAYS:
        return ('rejected', f"last sale {last_sale} (>{PSA_MAX_AGE_DAYS}d ago)")
    return ('accepted', None)


def fill_buying_sheet(sheet_id, comps, sheet_name="Form. Put Date Here."):
    """
    Populate the freshly-created buying sheet with everything the user normally sees:
      A = "PSA", B = cert (already there), C = HYPERLINK to Cardladder,
      D = card name, E = grade, F = sport, G = CL Value.
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
    if updates:
        sheet.batch_update(updates, value_input_option="USER_ENTERED")


# ── CERT EXTRACTION ──────────────────────────────────────────────────────────────
def extract_certs(text):
    if not text:
        return []
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

WELCOME_MSG = (
    "👋 Welcome to KTS Collectibles! We buy Pokémon cards — PSA graded slabs and raw singles.\n\n"
    "What are you looking to sell?\n"
    "• **PSA slabs** → send your cert numbers\n"
    "• **Raw cards** → upload your Collectr CSV export"
)

SHIPPING_MSG = (
    "📦 **Awesome, let's do it!** Ship your cards to Kevin and he'll pay you out instantly upon arrival.\n\n"
    "**Ship to:**\n"
    "Kevin Smith\n"
    "1363 Boylston St\n"
    "Unit 368\n"
    "Boston MA 02215\n\n"
    "📝 **Please include a note inside your package with:**\n"
    "• Your Discord username\n"
    "• Amount owed\n"
    "• Preferred payment method (PayPal F&F or Wire)\n\n"
    "⚠️ If no note is included, payment may be delayed as we won't know who the package is from.\n\n"
    "Payment via PayPal F&F or wire once received ⚡\n\n"
    "Once you've shipped, **drop your tracking number here** so Kevin can keep an eye out!"
)

FIRM_KEYWORDS = [
    "counter", "lower", "less", "more money", "higher", "better offer",
    "negotiate", "can you do", "how about", "what about", "discount",
    "too low", "not enough", "worth more", "offer more", "come up",
    "go up", "budge", "flexible", "room", "bump"
]

AGREE_KEYWORDS = ["ship"]

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

def is_negotiating(text):
    t = text.lower()
    return any(kw in t for kw in FIRM_KEYWORDS)

def is_agreeing(text):
    return text.strip().lower() == "ship"

@bot.event
async def on_ready():
    print(f"✅ KTS Collectibles Bot online as {bot.user}")

@bot.event
async def on_message(message):
    if message.author.bot:
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
    bot_already_spoke = False
    try:
        async for msg in message.channel.history(limit=50):
            if msg.author == bot.user:
                bot_already_spoke = True
                break
    except Exception:
        bot_already_spoke = channel_id in welcomed_tickets

    if not bot_already_spoke:
        welcomed_tickets.add(channel_id)
        await asyncio.sleep(1)
        await message.channel.send(WELCOME_MSG)
        return

    # ── COLLECTR CSV ─────────────────────────────────────────────────────────────
    if csv_attachment:
        async with message.channel.typing():
            try:
                csv_bytes = await csv_attachment.read()
                result, error = parse_collectr_csv(csv_bytes)
                if error:
                    await message.channel.send(f"Couldn't read that file — {error}. Try re-exporting from Collectr.")
                    return

                # Check low value cards against buying list
                rejected_cards = check_low_value_cards(result["df"])
                if rejected_cards:
                    card_list = "\n".join(f"• {c}" for c in rejected_cards[:25])
                    overflow = f"\n*...and {len(rejected_cards) - 25} more*" if len(rejected_cards) > 25 else ""
                    await message.channel.send(
                        f"❌ **I can't accept this lot as submitted.**\n\n"
                        f"The following card(s) are under $5 and don't fit our current buying criteria:\n\n"
                        f"{card_list}{overflow}\n\n"
                        f"Please remove these from your Collectr portfolio, re-export the CSV, and re-upload it here. "
                        f"Once removed I'll make you an offer! 🙏"
                    )
                    return

                issues = result.get("issues", [])
                for issue_type, cards in issues:
                    card_list = "\n".join(cards[:5])
                    if len(cards) > 5:
                        card_list += f"\n• ...and {len(cards)-5} more"
                    if issue_type == "trainer":
                        await message.channel.send(
                            f"❌ **Trainer cards — we're not buying trainers at this time:**\n{card_list}\n\n"
                            f"Please remove these and re-export."
                        )
                    elif issue_type == "non_english":
                        await message.channel.send(
                            f"❌ **Non-English cards — we only buy English cards:**\n{card_list}\n\n"
                            f"Please remove these and re-export."
                        )
                    elif issue_type == "over_100":
                        await message.channel.send(
                            f"❌ **Cards over $100 — we can't buy these:**\n{card_list}\n\n"
                            f"Our limit is **$1–$100 per card**. Remove these and re-export."
                        )
                    elif issue_type == "pre_2020":
                        await message.channel.send(
                            f"❌ **Pre-2020 cards — we can't buy these:**\n{card_list}\n\n"
                            f"We only buy **2020-present + Tag Teams**, Near Mint only. Remove these and re-export."
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

                await message.channel.send(
                    f"✅ **Your offer:**\n\n"
                    f"📦 **{card_count} cards** | Market value: **${total:,.2f}**\n"
                    f"💰 **Payout: ${payout:,.2f}** ({int(rate*100)}%)\n\n"
                    f"Let me know if you'd like to proceed!"
                )
                kevin_msg = (
                    f"💚 **Collectr offer sent — {username}**\n"
                    f"{card_count} cards | ${total:,.2f} market | {int(rate*100)}% | **${payout:,.2f}**"
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
                    if sheet_id and comps:
                        await asyncio.to_thread(fill_buying_sheet, sheet_id, comps)
                except Exception as e:
                    comp_error = str(e)
                    print(f"Helper comp lookup error: {e}")

                if comps:
                    by_cert = {str(c.get('cert', '')).strip(): c for c in comps}
                    accepted = []
                    rejected = []
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
                        else:
                            rejected.append((cert, reason))
                            kevin_lines.append(f"• `{cert}` — ❌ {reason}")

                    sport_groups = {}
                    for c in accepted:
                        sp = (c.get('sport') or '').lower().strip()
                        sport_groups.setdefault(sp, []).append(c)
                    sport_breakdown = []
                    for sp, comps_in_sport in sport_groups.items():
                        sport_total = sum(float(c['clValue']) for c in comps_in_sport)
                        rate = get_psa_payout_rate(sp, sport_total)
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
                            "rate": (total_payout / total_comp) if total_comp else PSA_POKEMON_PAYOUT_RATE,
                        }

                    breakdown_lines = [
                        f"• {s['sport'].title()}: {s['count']} card{'s' if s['count'] != 1 else ''}, "
                        f"${s['total']:,.2f} → ${s['payout']:,.2f} ({int(s['rate']*100)}%)"
                        for s in sport_breakdown
                    ]

                    # Kevin DM
                    summary = (
                        f"📋 **PSA sheet — {username}**\n"
                        f"{len(certs)} certs | Accepted **{n_accepted}** | Comp **${total_comp:,.2f}** | Payout **${total_payout:,.2f}**"
                        f"{f' | {n_rejected} rejected' if n_rejected else ''}\n"
                        f"{sheet_url}\n"
                        + ("\n".join(breakdown_lines) + "\n\n" if breakdown_lines else "\n")
                    )
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

    # ── NEGOTIATION ──────────────────────────────────────────────────────────────
    if channel_id in last_offer and is_negotiating(text):
        offer = last_offer[channel_id]
        await message.channel.send(
            f"We're firm on **${offer['payout']:,.2f}** ({int(offer['rate']*100)}% of market). "
            f"Our rates are based on live market data and we pay instantly! 🙏"
        )
        await ping_kevin(
            f"🔴 **{username} negotiating** — offered ${offer['payout']:,.2f}\nSaid: \"{text[:100]}\"",
            message.channel
        )
        return

    # ── AGREED / SHIPPING ─────────────────────────────────────────────────────────
    if is_agreeing(text):
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
