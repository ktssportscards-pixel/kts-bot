"""
botv2 configuration — every env var and tunable in one place.

v2 is the ground-up rebuild of the KTS Collectr Discord bot. The one architectural
change from v1 (kts_collectr_bot.py): prices no longer come from the CardLadder
scraper (Brave + vision). They come from the KTS Portal's server-to-server
endpoint POST /bot/price, which prices against Triumph's scanner API and returns
the SAME decision the portal/app uses. The bot therefore holds NO Triumph token,
NO CardLadder helper, and NO rules engine of its own.
"""
import os

# ── Discord ───────────────────────────────────────────────────────────────────
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
# Kevin's Discord user id — used to DM him quote summaries and to detect an
# in-flight ticket (if he's already talking in a channel, skip the welcome).
KEVIN_USER_ID = int(os.environ.get("KEVIN_USER_ID", "0") or "0")

# ── Price source: KTS Portal /bot/price proxy (Triumph lives behind it) ────────
# BOT_API_KEY is a NEW shared secret for the x-bot-key header — it is NOT the
# Triumph refresh token (that never leaves the portal).
PORTAL_BASE_URL = os.environ.get("PORTAL_BASE_URL", "https://portal.ktscollectibles.com")
BOT_API_KEY = os.environ.get("BOT_API_KEY", "")
PRICE_ENDPOINT = PORTAL_BASE_URL.rstrip("/") + "/bot/price"
PRICE_BATCH_CAP = 200          # portal's hard per-request cap (over → 400)
PRICE_TIMEOUT_S = 90           # generous; the portal parallelizes server-side
PRICE_MAX_RETRIES = 2          # retry 5xx / network only (endpoint is idempotent)
PRICE_RETRY_BACKOFF_S = 1.5    # base backoff between retries

# Dev/offline mode: when the real /bot/price isn't reachable yet, a local mock
# can answer so the bot is testable before the portal endpoint is deployed.
PRICE_MOCK = os.environ.get("BOTV2_PRICE_MOCK", "").lower() in ("1", "true", "yes")

# ── Google Sheets (buying sheets) ──────────────────────────────────────────────
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")
GOOGLE_CREDENTIALS_FILE = os.environ.get(
    "GOOGLE_CREDENTIALS_FILE", os.path.expanduser("~/Downloads/google_credentials.json"))

# ── Persistence (set DATA_DIR to a mounted volume in prod so state survives) ───
DATA_DIR = os.environ.get("DATA_DIR") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data")

# ── Web server (tracker sync: /owed /tickets /leaderboard /links /health) ──────
WEB_PORT = int(os.environ.get("PORT", "8080"))

# ── Confidential-field guard ───────────────────────────────────────────────────
# The portal already drops payoutCents + memberMultiplier at the source, so they
# never reach the bot. This list is a belt-and-suspenders denylist: if any of
# these ever appear on a row, the bot strips them before anything persists or is
# shown, so Kevin's payout math can never leak to a customer sheet or message.
CONFIDENTIAL_ROW_FIELDS = ("payout", "payoutCents", "memberMultiplier", "source")

# Words that must never appear on customer-facing surfaces (buyer identities).
CUSTOMER_FORBIDDEN_WORDS = ("triumph", "poketrader", "pixel", "allocation", "doral")
