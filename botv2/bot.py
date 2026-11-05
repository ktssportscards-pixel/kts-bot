"""
bot — Discord entry point + on_message router for botv2 (the core quote loop).

Flow: a ticket channel → welcome once → extract certs → price via the portal →
post the customer quote + DM Kevin the summary. Priced entirely through
portal_client (Triumph behind it); no CardLadder, no local rules engine.

Run:  python -m botv2.bot          (needs DISCORD_TOKEN + BOT_API_KEY + a live /bot/price)
      BOTV2_PRICE_MOCK=1 python -m botv2.bot   (offline: mock prices)

Deferred to later modules (tracked in README, not lost from v1): buying-sheet
create/fill, dual-address shipping + proceed/ship handling, CSV lot ingestion,
VIP override lookup, Pokémon quotas, and the command set (!summary, !payinfo,
!adopt, …). Each hooks into this loop without changing it.
"""
import asyncio

import discord

from . import (certs, commands, config, lots, payout_digest, portal_client,
               pricing, sheets, shipping, state, tickets, tracking, vip,
               webserver, welcome)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True       # ticket detection reads member permission overwrites
bot = discord.Client(intents=intents)

# Welcomed ticket channels (persisted; survives redeploys with a DATA_DIR volume).
_welcomed: set = set(int(x) for x in state.load("welcomed", list))
# Per-channel snapshot of the latest accepted quote, used to record the lot on ship.
_last_quote: dict = {}
# Per-channel buying-sheet URL (persisted) — used to save a tracking number later.
_channel_sheet: dict = state.load("channel_sheet", dict)


def _remember_welcomed(cid: int) -> None:
    if cid not in _welcomed:
        _welcomed.add(cid)
        state.save("welcomed", sorted(_welcomed))


async def ping_kevin(text: str) -> bool:
    """DM Kevin. Best-effort — a failed DM never blocks a customer quote."""
    if not config.KEVIN_USER_ID:
        return False
    try:
        user = bot.get_user(config.KEVIN_USER_ID) or await bot.fetch_user(config.KEVIN_USER_ID)
        await user.send(text)
        return True
    except Exception as e:
        print(f"[bot] ping_kevin failed (non-fatal): {e}")
        return False


# ── events ────────────────────────────────────────────────────────────────────
async def _handle_ship(channel) -> None:
    """Customer said 'ship'/'proceed' — give the routed address(es) or the hold."""
    # If a redeploy wiped the in-memory lot, rebuild it from the customer's buying
    # sheets first so sports/OP route correctly (else shipping fails open to VA).
    if not lots.has_lot(channel.id):
        uname = tickets.sanitize_channel_name(channel.name) or channel.name
        try:
            await asyncio.to_thread(lots.rebuild_from_sheets, channel.id, uname, 4,
                                    getattr(channel, "created_at", None))
        except Exception as e:
            print(f"[bot] lot rebuild failed (non-fatal): {e}")
    msg, gave, note = shipping.compose_shipping(channel.id)
    await channel.send(msg)
    if gave:
        tracking.record_accepted_ship(channel.id, _last_quote.get(channel.id))
    await ping_kevin(f"🚚 #{channel.name} — ship: {note}")


async def _handle_tracking(channel, tnum) -> None:
    """Customer dropped a tracking number — acknowledge, note it, save to the sheet."""
    uname = tickets.sanitize_channel_name(channel.name) or channel.name
    await channel.send(f"📦 Got it — tracking `{tnum}` noted. Kevin will keep an eye out! 🙌")
    tracking.record_tracking(channel.id, tnum)
    sheet_url = _channel_sheet.get(str(channel.id))
    saved = ""
    if sheet_url:
        try:
            await asyncio.to_thread(sheets.save_tracking, sheet_url, tnum)
            saved = " (saved to sheet)"
        except Exception as e:
            print(f"[bot] tracking sheet-save failed (non-fatal): {e}")
    await ping_kevin(f"📦 Tracking from **{uname}** (#{channel.name}): `{tnum}`{saved}"
                     + ("" if sheet_url else " — no sheet cached, add by hand"))


def _seconds_until_friday_9am_et() -> float:
    from datetime import datetime, timedelta, timezone
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        # no tz database on the image (tzdata should prevent this) — fall back to a
        # fixed -5h offset so the digest still fires ~Friday morning rather than dying.
        now = datetime.now(timezone(timedelta(hours=-5)))
    target = now.replace(hour=9, minute=0, second=0, microsecond=0) + timedelta(
        days=(4 - now.weekday()) % 7)             # Friday = weekday 4
    if target <= now:
        target += timedelta(days=7)
    return (target - now).total_seconds()


async def _payout_digest_loop():
    """DM Kevin the unpaid-owed digest every Friday 9 AM ET (payouts run Fridays).
    The whole body is guarded so no single failure (scheduling, build, or send) can
    permanently kill the task — it logs, backs off, and re-arms."""
    while True:
        try:
            await asyncio.sleep(_seconds_until_friday_9am_et())
            digest = payout_digest.build_digest()
            if digest:
                await ping_kevin(digest)
            await asyncio.sleep(90)                # clear the 9:00 minute, no re-fire
        except Exception as e:
            print(f"[bot] payout digest failed (non-fatal): {e}")
            await asyncio.sleep(3600)              # back off an hour, then re-arm


_web_runner = None
_digest_started = False


@bot.event
async def on_ready():
    global _web_runner, _digest_started
    print(f"[bot] logged in as {bot.user} (botv2)")
    if config.psa_folder_unsafe():
        print("[bot] ⚠️ PSA_FOLDER_ID is still v1's shared folder — buying-sheet "
              "creation will be REFUSED (it would corrupt v1). Set PSA_FOLDER_ID to "
              "v2's own folder before taking cards (or ALLOW_SHARED_PSA_FOLDER=1).")
    if _web_runner is None:                       # start once; on_ready re-fires on reconnect
        try:
            _web_runner = await webserver.start()
        except Exception as e:
            print(f"[bot] web server start failed (non-fatal): {e}")
    if not _digest_started:
        _digest_started = True
        bot.loop.create_task(_payout_digest_loop())
    try:
        await tickets.sweep_overflow(bot)
    except Exception as e:
        print(f"[bot] startup sweep failed (non-fatal): {e}")


@bot.event
async def on_guild_channel_create(channel):
    try:
        await tickets.rename_new_ticket(bot, channel)
    except Exception as e:
        print(f"[bot] ticket rename failed (non-fatal): {e}")


@bot.event
async def on_message(message):
    try:
        await _handle_message(message)
    except Exception as e:
        print(f"[bot] on_message error (non-fatal): {e}")


# ── core loop ─────────────────────────────────────────────────────────────────
async def _handle_message(message) -> None:
    if message.author.bot or message.author == bot.user:
        return
    channel = message.channel
    if not isinstance(channel, discord.TextChannel):
        return
    # Commands first — so !adopt works on a channel the gate doesn't yet know.
    if await commands.handle(bot, message):
        return
    if not tickets.is_ticket_channel(channel):
        return

    text = message.content or ""
    cert_list = certs.extract_certs(text)

    # Welcome exactly once; a fresh greet with no certs ends here.
    if channel.id not in _welcomed:
        welcomed_now = await _welcome_if_new(message, channel)
        if welcomed_now and not cert_list:
            return

    if not cert_list:
        if shipping.looks_like_agree(text):
            await _handle_ship(channel)
        else:
            tnum = certs.extract_tracking(text)
            if tnum:
                await _handle_tracking(channel, tnum)
        return

    # A lone bare 10-digit CGC candidate might be a phone/payment number — confirm
    # it resolves to a real slab on the portal before quoting (v1 behavior).
    if certs.cgc_needs_confirm(text, cert_list):
        try:
            pre = await portal_client.price_certs(cert_list)
        except portal_client.PortalError:
            pre = []
        if not pre or not pre[0].priced:
            await ping_kevin(
                f"ℹ️ **#{channel.name}** sent a lone number "
                f"`{certs.cert_digits(cert_list[0])}` that didn't resolve as a slab — "
                f"ignored (phone/payment?). No quote made.")
            return

    username = tickets.sanitize_channel_name(channel.name) or message.author.name
    # VIP is portal-sourced. Kevin requires every KTS-app user to enter their Discord
    # username, so the portal's VIP section is keyed by it — send the RAW Discord
    # username (message.author.name) as the match key (the sanitized channel name
    # strips dots and could miss). `username` stays the sheet/display identity.
    customer = message.author.name or username
    rate_override = vip.override_for(username)   # rare manual one-off; usually None

    async with channel.typing():
        try:
            rows = await portal_client.price_certs(cert_list, customer=username,
                                                   rate_override=rate_override)
        except portal_client.PortalAuthError as e:
            await ping_kevin(f"🚨 **botv2 can't authenticate to pricing** — {e}. "
                             f"Check BOT_API_KEY / the portal.")
            await channel.send("⚠️ Pricing is briefly unavailable — hang tight, I'll "
                               "get your quote shortly.")
            return
        except portal_client.PortalError as e:
            await ping_kevin(f"🚨 **botv2 pricing error** in #{channel.name}: {e}")
            await channel.send("⚠️ Pricing is briefly unavailable — please resend in a minute.")
            return

    quote = pricing.build_quote(rows)
    # Track accepted slabs for the ship/minimum gate, then tailor the proceed line.
    lots.add_slab_values(
        channel.id,
        {r.cert: r.market_value for r in quote.accepted if r.market_value is not None},
        {r.cert: r.category for r in quote.accepted})
    _last_quote[channel.id] = {
        "username": username, "comp": quote.comp_total, "payout": quote.payout_total,
        "cards": quote.n_accepted,
        "certs": {r.cert: r.market_value for r in quote.accepted if r.market_value is not None},
    }
    proceed = lots.proceed_or_hold_tail(channel.id)
    await channel.send(pricing.customer_message(quote, proceed_override=proceed))  # instant

    # Buying sheet for Kevin — deliberately OFF the customer's critical path (the
    # Apps Script is slow). The customer already has their quote; Kevin gets the
    # sheet link + summary once it's built. A sheet failure never blocks the DM.
    sheet_url = ""
    try:
        sheet_url, _name, _ = await asyncio.to_thread(sheets.create_psa_sheet, username, cert_list)
        _channel_sheet[str(channel.id)] = sheet_url          # cache for later tracking-save
        state.save("channel_sheet", _channel_sheet)
        missing = await asyncio.to_thread(sheets.fill_buying_sheet, sheet_url, rows)
        if missing:
            await ping_kevin(f"⚠️ {len(missing)} cert(s) didn't land on {username}'s sheet: "
                             + ", ".join(certs.cert_label(c) for c in missing))
    except Exception as e:
        await ping_kevin(f"⚠️ buying sheet failed for #{channel.name} ({e}) — "
                         f"summary below, add rows by hand if needed.")
    await ping_kevin(pricing.kevin_summary(quote, username, sheet_url))


async def _welcome_if_new(message, channel) -> bool:
    """Welcome EXACTLY ONCE, skipping genuinely in-flight tickets (Kevin already
    talking, or 2+ prior customer messages = a conversation already underway, where
    blasting the welcome would swallow a 'ship'/tracking reply). Returns True iff we
    sent the welcome. Faithful port of v1's logic."""
    already = False
    prior_human = 0
    kevin_replied = False
    try:
        async for msg in channel.history(limit=25, oldest_first=True):
            if msg.author == bot.user and "Welcome to KTS Collectibles" in (msg.content or ""):
                already = True
                break
            if config.KEVIN_USER_ID and msg.author.id == config.KEVIN_USER_ID:
                kevin_replied = True
            if not msg.author.bot and msg.id < message.id:   # strictly older only
                prior_human += 1
    except Exception:
        pass
    _remember_welcomed(channel.id)
    if not already and not kevin_replied and prior_human < 2:
        await asyncio.sleep(1)
        await channel.send(welcome.welcome_msg())
        return True
    return False


def run() -> None:
    if not config.DISCORD_TOKEN:
        raise SystemExit("DISCORD_TOKEN not set (export it, or run with BOTV2_PRICE_MOCK=1 "
                         "only exercises pricing, not Discord).")
    bot.run(config.DISCORD_TOKEN)


if __name__ == "__main__":
    run()
