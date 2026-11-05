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

from . import (alerts, certs, commands, config, lots, payout_digest,
               portal_client, pricing, sheets, shipping, state, tickets,
               tracking, vip, webserver, welcome)

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


async def alert_kevin(key: str, text: str) -> bool:
    """A failure alert for Kevin: record it for /status, then DM him — RATE-LIMITED,
    so an error storm (portal down → every quote errors) can't flood his DMs. The same
    `key` re-DMs at most once per alerts.ALERT_WINDOW_S; the /status record always
    updates. Use this (not a bare print) for any fault Kevin would want to know about;
    the print stays for the container logs."""
    alerts.record_error(key, text)
    if alerts.should_alert(key):
        return await ping_kevin(text)
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
            await alert_kevin("lot_rebuild",
                              f"⚠️ lot rebuild failed on ship (#{channel.name}): {e} — "
                              f"shipping may route to the wrong hub; check this ticket.")
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
            await alert_kevin("tracking_save",
                              f"⚠️ tracking sheet-save failed (#{channel.name}): {e} — "
                              f"the number is noted below; add it to the sheet by hand.")
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


def _seconds_until_daily_et(hour: int = 8) -> float:
    """Seconds until the next HH:00 America/New_York (same tz fallback as the digest)."""
    from datetime import datetime, timedelta, timezone
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        now = datetime.now(timezone(timedelta(hours=-5)))
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
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
            await alert_kevin("digest", f"⚠️ payout-digest loop failed: {e} — "
                              f"the Friday owed digest may not fire; check the bot.")
            await asyncio.sleep(3600)              # back off an hour, then re-arm


async def _heartbeat_loop():
    """Once-daily 'bot alive' DM (8 AM ET) with the key counts, so a silent/dead bot is
    a *visible* problem rather than an unnoticed one — if this DM stops arriving, Kevin
    knows to look. Guarded exactly like the digest so no single failure kills the task."""
    while True:
        try:
            await asyncio.sleep(_seconds_until_daily_et(8))
            c = tracking.counts()
            await ping_kevin(
                f"✅ botv2 heartbeat — {c['tickets_open']} open ticket(s), "
                f"{c['owed_open']} unpaid (${c['owed_total']:,.2f} owed). Still running.")
            await asyncio.sleep(90)                # clear the minute, no re-fire
        except Exception as e:
            print(f"[bot] heartbeat failed (non-fatal): {e}")
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
            await alert_kevin("webserver", f"🚨 botv2 web server failed to start: {e} — "
                              f"/health, /status and the owed tracker are down.")
    if not _digest_started:                        # start the scheduled loops once
        _digest_started = True
        bot.loop.create_task(_payout_digest_loop())
        bot.loop.create_task(_heartbeat_loop())
    try:
        await tickets.sweep_overflow(bot)
    except Exception as e:
        print(f"[bot] startup sweep failed (non-fatal): {e}")
        await alert_kevin("sweep", f"⚠️ startup ticket sweep failed: {e}")


@bot.event
async def on_guild_channel_create(channel):
    try:
        await tickets.rename_new_ticket(bot, channel)
    except Exception as e:
        print(f"[bot] ticket rename failed (non-fatal): {e}")
        await alert_kevin("rename",
                          f"⚠️ ticket rename failed (#{getattr(channel, 'name', '?')}): {e}")


@bot.event
async def on_message(message):
    try:
        await _handle_message(message)
    except Exception as e:
        print(f"[bot] on_message error (non-fatal): {e}")
        await alert_kevin(f"on_message:{type(e).__name__}",
                          f"⚠️ botv2 on_message error (non-fatal): {e}")


# ── core loop ─────────────────────────────────────────────────────────────────
async def _handle_message(message) -> None:
    if message.author.bot or message.author == bot.user:
        return
    channel = message.channel
    # Commands FIRST — before the channel-type gate — so admin commands (!bands,
    # !paid, …) also work in Kevin's DMs with the bot, not only in a guild channel.
    if await commands.handle(bot, message):
        return
    if not isinstance(channel, discord.TextChannel):
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
            await alert_kevin("price_auth",
                              f"🚨 **botv2 can't authenticate to pricing** — {e}. "
                              f"Check BOT_API_KEY / the portal.")
            await channel.send("⚠️ Pricing is briefly unavailable — hang tight, I'll "
                               "get your quote shortly.")
            return
        except portal_client.PortalError as e:
            await alert_kevin("price_error", f"🚨 **botv2 pricing error** in #{channel.name}: {e}")
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
    tracking.record_quote(
        channel.id, _last_quote[channel.id], channel_name=channel.name,
        card_detail=[{"cert": r.cert, "value": r.comp_value, "offer": r.offer,
                      "offer_pct": r.offer_pct, "category": r.category,
                      "grader": r.grader, "grade": r.grade, "name": (r.card_name or "")[:60]}
                     for r in quote.accepted])
    proceed = lots.proceed_or_hold_tail(channel.id)
    await channel.send(pricing.customer_message(quote, proceed_override=proceed))  # instant

    # Auto-claim: reserve Triumph allocation for the BUYs the instant we quote them,
    # to lock a slot before the team pools fill. DOUBLE-GATED — the portal's own kill
    # switch AND config.AUTO_CLAIM must BOTH be on, else claim_certs comes back all-
    # skipped and nothing is reserved. Off the customer's path; Kevin gets the outcome.
    if config.AUTO_CLAIM and quote.accepted:
        try:
            results = await portal_client.claim_certs(
                [r.cert for r in quote.accepted], idempotency_key=str(channel.id))
            locked = [r for r in results if r.claimed or r.already_claimed]
            errs = [r for r in results if r.error]
            tracking.record_claim(channel.id, len(locked), len(results))
            if locked or errs:
                msg = f"🔒 Auto-claim #{channel.name}: **{len(locked)}** locked of {len(results)}"
                if errs:
                    msg += f" | ⚠️ {len(errs)} errored"
                await ping_kevin(msg)
        except portal_client.PortalError as e:
            await alert_kevin(f"autoclaim:{type(e).__name__}",
                              f"⚠️ auto-claim error #{channel.name}: {e}")

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
        await alert_kevin(f"sheet_fail:{type(e).__name__}",
                          f"⚠️ buying sheet failed for #{channel.name} ({e}) — "
                          f"summary below, add rows by hand if needed.")
    await ping_kevin(pricing.kevin_summary(quote, username, sheet_url,
                                           ticket_link=f"<#{channel.id}>"))


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
