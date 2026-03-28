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
       $2000+       → 87%
       Bulk 87%     → up to 87% (Kevin decides)
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
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ── CONFIGURATION ──────────────────────────────────────────────────────────────
DISCORD_BOT_TOKEN       = os.environ.get("DISCORD_BOT_TOKEN", "")
ANTHROPIC_API_KEY       = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_CREDENTIALS_FILE = os.environ.get("GOOGLE_CREDENTIALS_FILE", os.path.expanduser("~/Downloads/google_credentials.json"))
TEMPLATE_SHEET_ID       = "1y_jis_knml_UIVWxxtEHrKs_vjkuX97x8Q7u3pVsQVM"
KTS_FOLDER_ID           = "1Ib1XgsCt9yc8B7EkppSEd97xTWlW8S2y"   # Parent folder
PSA_FOLDER_ID           = "1ayHilGpXqNQA8RDRSw1igTsCxBEI4hvm"   # PSA Slabs
COLLECTR_FOLDER_ID      = "1nAUPg7QW7tRzzdiHxG7UYUSPCa8MDZq3"   # Collectr Singles
APPS_SCRIPT_URL         = "https://script.google.com/macros/s/AKfycbz89bFS6eMJTbDF8CxVTyQem69jyaGvzey5OtnwUQP55NOtgT1vCSrTekcQmtSwJVWNGA/exec"
YOUR_DISCORD_USER_ID    = 1120958174036500480  # Kevin's Discord user ID

# Raw card payout percentages by lot size
RAW_PAYOUT_TIERS = [
    (0,     500,   0.84),
    (500,   1000,  0.85),
    (1000,  2000,  0.86),
    (2000,  float('inf'), 0.87),
]

# VIP clients who always get 87% regardless of lot size
# Add Discord usernames here (lowercase)
VIP_CLIENTS = ["nickj1234"]

# ── PAYOUT CALCULATOR ──────────────────────────────────────────────────────────
def get_payout_rate(total, username):
    """Return the payout percentage for a given lot total."""
    username_lower = username.lower()
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

    # ── VALIDATION ────────────────────────────────────────────────────────────
    # Tag Team keywords — always allowed
    TAG_TEAM_KEYWORDS = [" &", "tag team", "gx tag"]

    # Known PRE-2020 sets to reject (anything NOT in modern era)
    # We use a blacklist of confirmed old sets rather than guessing by year
    PRE_2020_SETS = {
        "base set", "jungle", "fossil", "base set 2", "team rocket",
        "gym heroes", "gym challenge", "neo genesis", "neo discovery",
        "neo revelation", "neo destiny", "legendary collection",
        "expedition", "aquapolis", "skyridge",
        "ruby & sapphire", "sandstorm", "dragon", "team magma vs team aqua",
        "hidden legends", "firered & leafgreen", "team rocket returns",
        "deoxys", "emerald", "unseen forces", "delta species",
        "legend maker", "holon phantoms", "crystal guardians",
        "dragon frontiers", "power keepers",
        "diamond & pearl", "mysterious treasures", "secret wonders",
        "great encounters", "majestic dawn", "legends awakened",
        "stormfront", "platinum", "rising rivals", "supreme victors",
        "arceus", "heartgold & soulsilver", "unleashed", "undaunted",
        "triumphant", "call of legends",
        "black & white", "emerging powers", "noble victories",
        "next destinies", "dark explorers", "dragons exalted",
        "boundaries crossed", "plasma storm", "plasma freeze",
        "plasma blast", "legendary treasures",
        "xy", "flashfire", "furious fists", "phantom forces",
        "primal clash", "double crisis", "roaring skies",
        "ancient origins", "breakthrough", "breakpoint",
        "generations", "fates collide", "steam siege", "evolutions",
        "sun & moon", "guardians rising", "burning shadows",
        "shining legends", "crimson invasion", "ultra prism",
        "forbidden light", "celestial storm", "dragon majesty",
        "lost thunder", "team up", "detective pikachu",
        "unbroken bonds", "unified minds", "hidden fates",
        "cosmic eclipse",
    }

    # Check for cards over $100
    over_100 = []
    for _, row in df.iterrows():
        price = float(row[price_col])
        name = str(row.get('Product Name', 'Unknown'))
        if price > 100:
            over_100.append(f"• {name} — ${price:.2f}")

    # Check for pre-2020 sets
    pre_2020_found = []
    set_col = 'Set' if 'Set' in df.columns else None
    if set_col:
        for _, row in df.iterrows():
            set_name = str(row.get('Set', '')).lower().strip()
            name = str(row.get('Product Name', 'Unknown'))
            # Tag teams are always allowed
            is_tag_team = any(t in name.lower() for t in TAG_TEAM_KEYWORDS)
            if is_tag_team:
                continue
            # Check against known pre-2020 set list
            if set_name in PRE_2020_SETS:
                pre_2020_found.append(f"• {name} ({row.get('Set', '')})")

    # Return validation issues if any
    issues = []
    if over_100:
        issues.append(("over_100", over_100))
    if pre_2020_found:
        issues.append(("pre_2020", pre_2020_found))

    total = df['_line_total'].sum()
    card_count = int(df[qty_col].sum()) if qty_col else len(df)

    # Build a summary of top cards by value
    top_cards = df.nlargest(5, '_line_total')[['Product Name', 'Set', price_col, '_line_total']].copy()
    top_list = []
    for _, row in top_cards.iterrows():
        name = str(row.get('Product Name', 'Unknown'))
        set_name = str(row.get('Set', ''))
        price = row['_line_total']
        top_list.append(f"  • {name} ({set_name}) — ${price:.2f}")

    return {
        "total": total,
        "card_count": card_count,
        "top_cards": top_list,
        "issues": issues,
        "df": df
    }, None


# ── GOOGLE SHEETS ──────────────────────────────────────────────────────────────
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
    """Create a buying sheet by calling the Google Apps Script web app.
    This runs under Kevin's Google account so no storage quota issues."""
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

    return data["url"], data["name"]

# ── CERT EXTRACTION ────────────────────────────────────────────────────────────
def extract_certs(text):
    """
    Only treat a message as cert numbers if it's MOSTLY numbers.
    If someone types "i'll pass on 135540599" we ignore it.
    A cert-only message is one where numbers make up most of the content.
    """
    if not text:
        return []
    numbers = re.findall(r'\b\d{7,9}\b', text)
    if not numbers:
        return []
    # Count non-whitespace, non-number characters
    stripped = re.sub(r'\d', '', text).strip()
    words = [w for w in stripped.split() if re.search(r'[a-zA-Z]', w)]
    # If there are more than 3 regular words, this is a sentence — ignore certs
    if len(words) > 3:
        return []
    seen = set()
    unique = []
    for n in numbers:
        if n not in seen:
            seen.add(n)
            unique.append(n)
    return unique

# ── DISCORD BOT ────────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True

bot = discord.Client(intents=intents)

# Track which tickets have already been welcomed
welcomed_tickets = set()

# Track last offer per channel for negotiation detection
last_offer = {}  # channel_id -> {"payout": float, "total": float, "rate": float}

WELCOME_MSG = (
    "👋 Welcome to KTS Collectibles! We buy Pokémon cards — PSA graded slabs and raw singles.\n\n"
    "What are you looking to sell?\n"
    "• **PSA slabs** → send your cert numbers\n"
    "• **Raw cards** → upload your Collectr CSV export"
)

SHIPPING_MSG = (
    "📦 **Paid Upon Arrival** — ship your cards to Kevin first, then payment goes out instantly once received.\n\n"
    "**Ship to:**\n"
    "Kevin Smith\n"
    "1363 Boylston St\n"
    "Unit 368\n"
    "Boston MA 02215\n\n"
    "Payment via PayPal F&F or wire ⚡"
)

FIRM_KEYWORDS = [
    "counter", "lower", "less", "more money", "higher", "better offer",
    "negotiate", "can you do", "how about", "what about", "discount",
    "too low", "not enough", "worth more", "offer more", "come up",
    "go up", "budge", "flexible", "room", "bump"
]

AGREE_KEYWORDS = [
    "deal", "agreed", "sounds good", "let's do it", "i'll take it",
    "i accept", "proceed", "move forward", "ship", "send address",
    "where do i send", "shipping address", "what's your address",
    "how do i send", "where to send"
]

async def ping_kevin(msg, channel=None):
    try:
        kevin = await bot.fetch_user(YOUR_DISCORD_USER_ID)
        channel_link = f"\n**Ticket:** <#{channel.id}>" if channel else ""
        await kevin.send(msg + channel_link)
    except Exception as e:
        print(f"Could not ping Kevin: {e}")

def is_negotiating(text):
    t = text.lower()
    return any(kw in t for kw in FIRM_KEYWORDS)

def is_agreeing(text):
    t = text.lower()
    return any(kw in t for kw in AGREE_KEYWORDS)

@bot.event
async def on_ready():
    print(f"✅ KTS Collectibles Bot online as {bot.user}")

@bot.event
async def on_message(message):
    # Ignore own messages and other bots (Ticket Tool etc)
    if message.author.bot:
        return

    # Only in ticket channels
    is_ticket = isinstance(message.channel, discord.TextChannel) and "ticket" in message.channel.name.lower()
    if not is_ticket:
        return

    channel_id = message.channel.id
    username = message.author.name
    text = message.content.strip()

    print(f"[{message.channel.name}] {username}: {text[:60]}{' (+attachment)' if message.attachments else ''}")

    # ── CHECK FOR CSV ─────────────────────────────────────────────────────────
    csv_attachment = None
    for att in message.attachments:
        if att.filename.lower().endswith('.csv'):
            csv_attachment = att
            break

    # ── CHECK FOR CERT NUMBERS ────────────────────────────────────────────────
    certs = extract_certs(text) if text else []

    # ── WELCOME: send once on first human message ─────────────────────────────
    if channel_id not in welcomed_tickets:
        welcomed_tickets.add(channel_id)
        await asyncio.sleep(1)
        await message.channel.send(WELCOME_MSG)
        return

    # ── COLLECTR CSV ──────────────────────────────────────────────────────────
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
                    if issue_type == "over_100":
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

                # Save CSV to Google Drive Collectr Singles folder
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

    # ── PSA CERT NUMBERS ──────────────────────────────────────────────────────
    if certs:
        async with message.channel.typing():
            try:
                await message.channel.send(
                    f"Got it! Setting up your buying sheet for {len(certs)} cert{'s' if len(certs) > 1 else ''}... ⏳"
                )
                sheet_url, sheet_name = create_psa_sheet(username, certs)
                await message.channel.send(
                    f"✅ Sheet ready! Kevin will check comps on CardLadder and get back to you.\n\n"
                    f"📊 {sheet_url}"
                )
                cert_list = "\n".join([f"• {c}" for c in certs])
                await ping_kevin(
                    f"📋 **PSA sheet — {username}**\n{len(certs)} certs | {sheet_url}\n\n{cert_list}",
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

    # ── NEGOTIATION ───────────────────────────────────────────────────────────
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

    # ── AGREED / SHIPPING REQUEST ─────────────────────────────────────────────
    if is_agreeing(text):
        await message.channel.send(SHIPPING_MSG)
        await ping_kevin(f"✅ **{username} agreed** — shipping address sent.", message.channel)
        return

    # ── EVERYTHING ELSE: STAY SILENT ─────────────────────────────────────────

# ── RUN ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Starting KTS Collectibles Bot...")
    bot.run(DISCORD_BOT_TOKEN)
