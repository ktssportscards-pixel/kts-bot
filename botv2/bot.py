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
        # _last_quote is in-memory and is wiped by a redeploy; fall back to the PERSISTED
        # quoted ticket so a cert-less "ship" right after a redeploy still books the owed
        # entry (otherwise the shipped lot silently never enters the payout queue).
        snap = _last_quote.get(channel.id) or tracking.quoted_snapshot(channel.id)
        tracking.record_accepted_ship(channel.id, snap)
    await ping_kevin(f"🚚 #{channel.name} — ship: {note}")


async def _handle_tracking(channel, tnum) -> None:
    """Customer dropped a tracking number — acknowledge, note it, save to the sheet."""
    uname = tickets.sanitize_channel_name(channel.name) or channel.name
    ack = f"📦 Got it — tracking `{tnum}` noted. Kevin will keep an eye out! 🙌"
    if config.DELIVERY_TRACKING:                       # auto-arrival updates are on
        ack = (f"📦 Got it — tracking `{tnum}` noted. I'll let you know right here the "
               f"moment it arrives — no need to check it yourself. 🙌")
    await channel.send(ack)
    tracking.record_tracking(channel.id, tnum)
    if config.REGISTER_TRACKING:                        # ask the portal to watch it for delivery
        try:
            await portal_client.register_tracking(channel.id, tnum)
        except Exception as e:                          # fail-soft: never block the ack/sheet-save
            print(f"[bot] register-tracking failed (non-fatal): {e}")
            await alert_kevin(f"register_track:{type(e).__name__}",
                              f"⚠️ couldn't register tracking `{tnum}` (#{channel.name}) with the "
                              f"delivery watcher: {e} — the auto delivery update may not fire for this one.")
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


async def notify_delivered(channel_id, user: str = "") -> bool:
    """On a package delivery: ALWAYS DM Kevin a heads-up (his own DM — always safe, and
    it's how he knows to go verify the lot), and — ONLY if config.DELIVERY_TRACKING is
    on (the narrow, deliberate exception to 'the bot never messages customers') — post a
    customer-safe 'arrived!' note in that ticket. Best-effort; never raises into the web
    handler. Returns True iff the customer note actually posted."""
    try:
        number = tracking.delivery_status(int(channel_id)).get("number", "")
    except Exception:
        number = ""
    posted = False
    if config.DELIVERY_TRACKING:                       # customer-facing: gated exception
        try:
            channel = bot.get_channel(int(channel_id)) or await bot.fetch_channel(int(channel_id))
            await channel.send(tracking.delivery_customer_message(user))
            posted = True
        except Exception as e:
            print(f"[bot] notify_delivered: customer post failed in {channel_id}: {e}")
            await alert_kevin("delivery_post",
                              f"⚠️ delivery: couldn't post the 'arrived' note to channel "
                              f"{channel_id}: {e} — let the customer know by hand if needed.")
    await ping_kevin(tracking.delivery_kevin_message(user, number))   # always (Kevin's own DM)
    return posted


async def notify_out_for_delivery(channel_id, user: str = "") -> bool:
    """Post a customer-safe 'out for delivery today' heads-up in a ticket — ONLY if
    config.DELIVERY_TRACKING is on (same gate as the 'arrived' post). Best-effort; never
    raises into the web handler. No Kevin DM here (his delivery DM fires on ARRIVAL, not
    on every out-for-delivery). Returns True iff the note actually posted."""
    if not config.DELIVERY_TRACKING:
        return False
    try:
        channel = bot.get_channel(int(channel_id)) or await bot.fetch_channel(int(channel_id))
        await channel.send(tracking.delivery_ofd_message(user))
        return True
    except Exception as e:
        print(f"[bot] notify_out_for_delivery: post failed in {channel_id}: {e}")
        await alert_kevin("delivery_ofd",
                          f"⚠️ delivery: couldn't post the 'out for delivery' note to channel "
                          f"{channel_id}: {e}")
        return False


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


async def _render_lanes_board():
    """Fetch the lanes data + build the CUSTOMER-SAFE board, then create+pin (first time)
    or edit-in-place a single pinned message in LANES_CHANNEL_ID. The message id is
    persisted so restarts edit the same message instead of re-posting. No-op unless the
    board is enabled + a channel is set."""
    if not (config.LANES_BOARD and config.LANES_CHANNEL_ID):
        return
    data = await portal_client.fetch_allocation()
    text = pricing.lanes_board(data)
    channel = bot.get_channel(config.LANES_CHANNEL_ID) or await bot.fetch_channel(config.LANES_CHANNEL_ID)
    st = state.load("lanes_board", dict)
    msg = None
    if st.get("message_id") and st.get("channel_id") == config.LANES_CHANNEL_ID:
        try:
            msg = await channel.fetch_message(int(st["message_id"]))
        except Exception:
            msg = None                                # deleted/unpinned → re-create below
    if msg is None:
        msg = await channel.send(text)
        try:
            await msg.pin()
        except Exception as e:
            print(f"[bot] lanes board pin failed (non-fatal): {e}")
        state.save("lanes_board", {"message_id": msg.id, "channel_id": config.LANES_CHANNEL_ID})
    else:
        await msg.edit(content=text)


# Lanes board weekly window (ET), Python weekday(): Mon=0 … Sun=6.
_LANES_OPEN_HOUR = 3     # Saturday 3 AM ET — board appears
_LANES_CLOSE_HOUR = 3    # Monday 3 AM ET — board comes down


def _now_et():
    from datetime import datetime, timedelta, timezone
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        return datetime.now(timezone(timedelta(hours=-5)))


def _lanes_in_window(now=None) -> bool:
    """True when the lanes board should be shown: Saturday 3 AM ET → Monday 3 AM ET."""
    now = now or _now_et()
    wd, hr = now.weekday(), now.hour
    if wd == 5:                      # Saturday — open from 3 AM
        return hr >= _LANES_OPEN_HOUR
    if wd == 6:                      # Sunday — all day
        return True
    if wd == 0:                      # Monday — until 3 AM
        return hr < _LANES_CLOSE_HOUR
    return False                     # Tue–Fri — closed


def _seconds_until_lanes_open() -> float:
    from datetime import timedelta
    now = _now_et()
    days_ahead = (5 - now.weekday()) % 7         # days to the next Saturday
    target = (now + timedelta(days=days_ahead)).replace(
        hour=_LANES_OPEN_HOUR, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=7)
    return max(60.0, (target - now).total_seconds())


async def _remove_lanes_board():
    """Delete the pinned board (if posted) + clear its saved id, so the board disappears
    when the weekly window is closed and reposts fresh when it reopens."""
    st = state.load("lanes_board", dict)
    mid = st.get("message_id")
    if not mid:
        return
    try:
        channel = bot.get_channel(config.LANES_CHANNEL_ID) or await bot.fetch_channel(config.LANES_CHANNEL_ID)
        msg = await channel.fetch_message(int(mid))
        await msg.delete()
    except Exception as e:
        print(f"[bot] lanes board remove (non-fatal): {e}")
    state.save("lanes_board", {})                    # clear → reposts fresh next window


async def _lanes_board_loop():
    """Keep the customer-safe 'lanes' board current during the weekly buying window
    (config.LANES_SCHEDULE: Sat 3 AM → Mon 3 AM ET). Inside the window it posts/refreshes
    every LANES_REFRESH_MIN min; outside it removes the board and sleeps until the next
    open. Guarded: a failure alerts Kevin + backs off, never crashing the task. No-op
    unless enabled + a channel is set."""
    if not (config.LANES_BOARD and config.LANES_CHANNEL_ID):
        return
    while True:
        try:
            if (not config.LANES_SCHEDULE) or _lanes_in_window():
                await _render_lanes_board()
                await asyncio.sleep(max(60, config.LANES_REFRESH_MIN * 60))
            else:
                await _remove_lanes_board()                       # window closed → take it down
                await asyncio.sleep(min(_seconds_until_lanes_open(), 3600))
        except Exception as e:
            print(f"[bot] lanes board failed (non-fatal): {e}")
            await alert_kevin("lanes_board", f"⚠️ lanes board loop failed: {e} — the live "
                              f"board in the lanes channel may be stale; check the bot.")
            await asyncio.sleep(600)                  # back off 10 min, then re-arm

def _stale_digest() -> str:
    """Build Kevin's stale-ticket digest (or '' when nothing is stale). KEVIN-ONLY —
    names + channels are fine here; this never reaches a customer."""
    from datetime import date
    s = tracking.stale_tickets(date.today())
    sections = []

    def block(title, items, verb):
        if items:
            sections.append("\n".join(
                [title] + [f"  • **{i['user'] or '?'}** — {verb} {i['days']}d · <#{i['channel_id']}>"
                           for i in items]))

    block(f"🕒 **Quoted, never shipped** (>{config.STALE_QUOTED_DAYS}d):", s["quoted_stale"], "quoted")
    block(f"🚚 **Shipped, not delivered** (>{config.STALE_TRANSIT_DAYS}d):", s["transit_stale"], "in transit")
    block(f"💸 **Delivered, still unpaid** (>{config.STALE_UNPAID_DAYS}d):", s["unpaid_stale"], "sitting")
    if not sections:
        return ""
    total = sum(len(v) for v in s.values())
    return f"⚠️ **Stale-ticket check — {total} need a look:**\n\n" + "\n\n".join(sections)


async def _stale_nudge_loop():
    """Once-daily (8 AM ET) Kevin-ONLY DM of tickets stuck too long in a stage, so nothing
    slips with many simultaneous clients. Guarded like the other loops; gated by
    config.STALE_NUDGES (the loop is only started when that is on)."""
    while True:
        try:
            await asyncio.sleep(_seconds_until_daily_et(8))
            digest = _stale_digest()
            if digest:
                await ping_kevin(digest)
            await asyncio.sleep(90)                # clear the minute, no re-fire
        except Exception as e:
            print(f"[bot] stale-nudge loop failed (non-fatal): {e}")
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
        if config.LANES_BOARD and config.LANES_CHANNEL_ID:
            bot.loop.create_task(_lanes_board_loop())

        if config.STALE_NUDGES:                     # Kevin-only stuck-ticket digest (gated, default off)
            bot.loop.create_task(_stale_nudge_loop())
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
        "cert_offers": {r.cert: r.offer for r in quote.accepted},   # per-cert, for owed accumulation
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
    # Safety net for `python -m botv2.bot`: that loads this file as `__main__`, but the
    # package (webserver, …) imports it as `botv2.bot` — a separate module with its own,
    # never-logged-in client, so delivery posts would hit a dead client. Delegate to the
    # package module's run() so exactly ONE client exists. Preferred entrypoint is
    # `python -m botv2` (see botv2/__main__.py); this just keeps the old command safe.
    from botv2.bot import run as _run

    _run()
