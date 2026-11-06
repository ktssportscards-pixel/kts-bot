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
# Accept the existing v1 Railway var name (DISCORD_BOT_TOKEN) first, so the same
# bot token works unchanged on the shared service; DISCORD_TOKEN is a fallback.
DISCORD_TOKEN = os.environ.get("DISCORD_BOT_TOKEN") or os.environ.get("DISCORD_TOKEN") or ""
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

# ── Auto-claim (reserve Triumph allocation via the portal's /bot/claim) ─────────
# DOUBLE-GATED: this bot-side flag AND the portal's own kill switch (default OFF,
# flipped only on Kevin's word) must BOTH be on before anything is claimed. With
# this off, the bot never calls /bot/claim at all. The portal re-prices each cert
# and only claims decision==BUY; the claim is idempotent on (idempotencyKey,certId).
AUTO_CLAIM = os.environ.get("BOTV2_AUTO_CLAIM", "").lower() in ("1", "true", "yes")
CLAIM_ENDPOINT = PORTAL_BASE_URL.rstrip("/") + "/bot/claim"
UNCLAIM_ENDPOINT = PORTAL_BASE_URL.rstrip("/") + "/bot/unclaim"

# ── Allocation awareness (read-only "band getting full" heads-up) ──────────────
# Threshold for a near-full nudge. The portal also flags per-card nearFull on
# /bot/price rows at this same %; the board read gives the fuller picture.
ALLOCATION_ENDPOINT = PORTAL_BASE_URL.rstrip("/") + "/bot/allocation"
NEAR_FULL_PCT = int(os.environ.get("NEAR_FULL_PCT", "85"))

# ── Delivery tracking (auto "your package arrived" update) ─────────────────────
# A customer drops a tracking number in their ticket (or the app); when the carrier
# marks it delivered, the bot flips the ticket to delivered and — ONLY if this flag
# is on — posts a customer-safe "arrived!" note in their own ticket, so they don't
# check the carrier or DM Kevin "did you get it?". This is a DELIBERATE, NARROW
# exception to the "bot never messages customers" rule: factual delivery/status
# updates to that one ticket, scrubbed of any buyer name — nothing else.
# DEFAULT OFF: with it off, delivery status is still recorded (ops + Kevin's DM see
# it) but NOTHING is posted to the customer. The carrier integration itself
# (EasyPost / Shippo / AfterShip account + webhook) lives OUTSIDE the bot: that
# service calls the bot's authenticated POST /delivery, so this side is
# provider-agnostic and holds no carrier credentials.
DELIVERY_TRACKING = os.environ.get("BOTV2_DELIVERY_TRACKING", "").lower() in ("1", "true", "yes")

# ── Delivery-tracking registration (bot → portal) ──────────────────────────────
# When a customer drops a UPS tracking number, the bot asks the portal to start
# WATCHING it for delivery (POST /bot/register-tracking): the portal registers an
# EasyPost tracker and, on delivery, relays to the bot's /delivery. This is the
# "shadow mode" switch — ON = the bot registers tracking numbers (so deliveries flip
# tickets + DM Kevin), with NOTHING customer-facing (the customer "arrived!" post is
# the SEPARATE DELIVERY_TRACKING flag). DEFAULT OFF so a deploy is inert until Kevin
# activates. Shadow mode = REGISTER_TRACKING on + DELIVERY_TRACKING off; customer-live
# = both on. The portal dedups, so a re-register is a no-op (no re-charge).
REGISTER_TRACKING = os.environ.get("BOTV2_REGISTER_TRACKING", "").lower() in ("1", "true", "yes")
REGISTER_TRACKING_ENDPOINT = PORTAL_BASE_URL.rstrip("/") + "/bot/register-tracking"

# ── Customer self-service status (!status in a ticket) ─────────────────────────
# When ON, a customer can run `!status` in their own ticket to see their order's stage
# (quoted → confirmed → in transit → delivered → paid) — CUSTOMER-SAFE (no buyer name,
# no amounts; the app shows the rest). DEFAULT OFF so Kevin controls availability; with
# it off the bot ignores `!status` entirely.
CUSTOMER_STATUS = os.environ.get("BOTV2_CUSTOMER_STATUS", "").lower() in ("1", "true", "yes")

# ── Live "lanes" board (customer-safe, auto-refreshing) ────────────────────────
# An auto-updating pinned message in LANES_CHANNEL_ID showing which buying lanes are
# open / filling / full during the rush — CUSTOMER-SAFE: no numbers, no dollars, no
# percentages, no buyer name (the Kevin-only !bands board shows the real figures; this
# one must NOT). DEFAULT OFF, and a no-op unless BOTV2_LANES_BOARD is on AND a channel
# id is set. Refreshes every LANES_REFRESH_MIN minutes (floored at 1).
LANES_BOARD = os.environ.get("BOTV2_LANES_BOARD", "").lower() in ("1", "true", "yes")
LANES_CHANNEL_ID = int(os.environ.get("BOTV2_LANES_CHANNEL_ID", "0") or "0")
LANES_REFRESH_MIN = int(os.environ.get("BOTV2_LANES_REFRESH_MIN", "10") or "10")

# ── Stale-ticket nudges (Kevin-facing daily "stuck tickets" digest) ────────────
# When on, the bot DMs Kevin once a day (8 AM ET) a digest of tickets stuck too long
# in a stage, so nothing slips with many simultaneous clients: quoted-but-never-shipped,
# shipped-but-not-delivered, and delivered-but-still-unpaid, each past its threshold.
# KEVIN-ONLY — the digest never reaches a customer. DEFAULT OFF (the loop only starts
# when this is on).
STALE_NUDGES = os.environ.get("BOTV2_STALE_NUDGES", "").lower() in ("1", "true", "yes")
STALE_QUOTED_DAYS = int(os.environ.get("BOTV2_STALE_QUOTED_DAYS", "5"))    # quoted, never shipped
STALE_TRANSIT_DAYS = int(os.environ.get("BOTV2_STALE_TRANSIT_DAYS", "5"))  # shipped, not delivered
STALE_UNPAID_DAYS = int(os.environ.get("BOTV2_STALE_UNPAID_DAYS", "3"))    # delivered, still unpaid

# ── Google Sheets (buying sheets) ──────────────────────────────────────────────
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")
GOOGLE_CREDENTIALS_FILE = os.environ.get(
    "GOOGLE_CREDENTIALS_FILE", os.path.expanduser("~/Downloads/google_credentials.json"))
# Buying sheets are created by the same Apps Script web app v1 uses (so the format
# Kevin pays off, and the downstream !summary / payments / nightly reconcile that
# read these sheets, stay identical). v2 fills them from the portal's rows.
APPS_SCRIPT_URL = os.environ.get(
    "APPS_SCRIPT_URL",
    "https://script.google.com/macros/s/AKfycbxPenrARSCnPZ6Ddwaokcz24Fwvcobgp0ybvvzJR49cCJ_DUcNoprRXDPpyTJA0rJ71Cg/exec")
# v1's shared PSA buying-sheet folder. v2 must NOT create sheets here while v1 is
# live: v1's folder-wide sweeps (boss CSV, customer lookup, stalled requote) have no
# owner filter and would ingest v2's sheets.
V1_PSA_FOLDER_ID = "1ayHilGpXqNQA8RDRSw1igTsCxBEI4hvm"
# v2's buying-sheet output folder. The default is v1's folder ONLY as a tripwire —
# if it's still this at sheet-creation time, create_psa_sheet() refuses (so v2 can't
# silently corrupt v1). Set this to v2's OWN Drive folder id (shared with the
# service-account email) before taking cards. To intentionally share v1's folder
# (only safe once v1 is retired), set ALLOW_SHARED_PSA_FOLDER=1.
PSA_FOLDER_ID = os.environ.get("PSA_FOLDER_ID", V1_PSA_FOLDER_ID)
ALLOW_SHARED_PSA_FOLDER = os.environ.get("ALLOW_SHARED_PSA_FOLDER", "").lower() in ("1", "true", "yes")
TEMPLATE_SHEET_ID = os.environ.get("TEMPLATE_SHEET_ID", "1y_jis_knml_UIVWxxtEHrKs_vjkuX97x8Q7u3pVsQVM")
SHEET_FORM_NAME = "Form. Put Date Here."   # the worksheet tab the template ships with
SHEET_NOT_LOADED_MARK = "didn't load — resend"

# ── Persistence (set DATA_DIR to a mounted volume in prod so state survives) ───
DATA_DIR = os.environ.get("DATA_DIR") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data")

# ── Lot minimums + lane status ─────────────────────────────────────────────────
SPORTS_OP_MIN_LOT = int(os.environ.get("SPORTS_OP_MIN_LOT", "1500"))       # Doral lane min
POKEMON_SINGLES_MIN_LOT = int(os.environ.get("POKEMON_SINGLES_MIN_LOT", "1500"))
MIN_LOT_VALUE = int(os.environ.get("MIN_LOT_VALUE", "0"))                  # combined min (0 = off)
MIN_SLAB_COUNT = int(os.environ.get("MIN_SLAB_COUNT", "0"))               # slab-count min (0 = off)
# Whether the Pokémon (Pixel, no-minimum) lane is open — controls only customer
# COPY (the "send Pokémon, no minimum" invites); actual acceptance is the portal's
# call per cert. Kevin flips this when the Pokémon lane opens.
POKEMON_LANE_OPEN = os.environ.get("POKEMON_LANE_OPEN", "").lower() in ("1", "true", "yes")

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


def psa_folder_unsafe() -> bool:
    """True when v2 would write buying sheets into v1's shared folder without the
    explicit ALLOW_SHARED_PSA_FOLDER opt-in — i.e. PSA_FOLDER_ID hasn't been pointed
    at v2's own folder yet. Used to disable sheet creation and warn at startup."""
    return PSA_FOLDER_ID == V1_PSA_FOLDER_ID and not ALLOW_SHARED_PSA_FOLDER
