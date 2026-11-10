"""
bot — Discord entry point + on_message router for botv2 (the core quote loop).

Flow: a ticket channel → welcome once → extract certs → price via the portal →
post the customer quote + DM Kevin the summary. Priced entirely through
portal_client (Triumph behind it); no CardLadder, no local rules engine.

Run:  python -m botv2              (needs a Discord token + BOT_API_KEY + a live /bot/price;
                                    see __main__.py for why not `python -m botv2.bot`)
      BOTV2_PRICE_MOCK=1 python -m botv2   (mock prices — ONLY with a TEST Discord app)
Test: python -m botv2.selftest     (offline; never touches Discord, the portal or Drive)

The buying sheet, shipping, tracking, void and command flows live in their own modules
(sheets / shipping / tracking / commands) and hook into this loop.
"""
import asyncio

import discord

from . import (alerts, certs, commands, config, lots, payout_digest,
               portal_client, pricing, sheets, shipping, state, textsplit, tickets,
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
    """DM Kevin. Best-effort — a failed DM never blocks a customer quote. Discord rejects
    a message over 2,000 characters outright, so long text (a big lot's summary, the
    Friday digest, a stale-ticket digest) goes out as several messages instead of being
    dropped. True only if there was something to send and every part was delivered."""
    if not config.KEVIN_USER_ID:
        return False
    try:
        user = bot.get_user(config.KEVIN_USER_ID) or await bot.fetch_user(config.KEVIN_USER_ID)
        return await textsplit.send_chunked(user, text) > 0
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
        ok = await ping_kevin(text)
        if not ok:
            alerts.forget(key)       # never delivered → don't let it use up the quiet window
        return ok
    return False


# ── persistence faults → Kevin ──────────────────────────────────────────────────
_bg_tasks: set = set()          # strong refs to fire-and-forget alert tasks (never GC'd mid-flight)


def _state_problem_text(kind: str, name: str, detail: str) -> str:
    if kind == "load":
        return (f"🚨 **Data file `{name}.json` could not be read** ({detail}). I'm running on an "
                f"EMPTY `{name}` until it's fixed, so anything that depends on it (owed balances, "
                f"tickets, payment info, void guards…) may be wrong or missing. The unreadable file "
                f"is kept next to it in the data folder — restore it before trusting `{name}` again.")
    return (f"🚨 **Couldn't save `{name}`** ({detail}). The latest change to it is NOT on disk "
            f"(quotes / shipments / payments recorded since may be lost on a restart). "
            f"Check the data volume — full or read-only?")


def _spawn(coro) -> None:
    task = asyncio.get_running_loop().create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def _on_state_problem(kind: str, name: str, detail: str) -> None:
    """state.py could not read or write a store. Record it for /status and DM Kevin
    (rate-limited per store by alert_kevin) — from whichever thread it happened on.
    Never raises: persistence must not fail harder because the alert couldn't go out."""
    key, text = f"state_{kind}:{name}", _state_problem_text(kind, name, detail)
    alerts.record_error(key, text)                       # visible on /status even with no loop
    try:
        try:
            asyncio.get_running_loop()
            _spawn(alert_kevin(key, text))               # on the bot's loop already
        except RuntimeError:                             # a worker thread (or not started yet)
            bot.loop.call_soon_threadsafe(lambda: _spawn(alert_kevin(key, text)))
    except Exception:
        pass                                             # no loop yet → /status + the log have it


state.add_listener(_on_state_problem)


def _startup_storage_problems() -> list:
    """(key, text) alerts about configuration that silently loses or misplaces data — checked
    once at startup. Everything money-related (tickets, owed balances, payment info, void
    guards) is a JSON file under DATA_DIR; if that isn't a persistent, writable volume it is
    lost silently. And a blank PSA_FOLDER_ID sends buying sheets to the Apps Script's default
    folder while skipping the v1-folder safety check."""
    out = []
    if config.psa_folder_blank():
        out.append(("psa_folder_blank",
                    "🚨 `PSA_FOLDER_ID` is set but BLANK — buying sheets are being created with an "
                    "empty folder id (the Apps Script picks its own default folder) and the "
                    "\"never write into v1's folder\" safety check is skipped. Set it to the real "
                    "buying-sheet folder id."))
    if config.DATA_DIR_IS_DEFAULT:
        out.append(("data_dir_default",
                    "🚨 `DATA_DIR` is not set on this service — tickets, owed balances, payment info "
                    "and void guards are being kept INSIDE the container and will be wiped by the next "
                    "redeploy. Point `DATA_DIR` at the mounted volume."))
    if not state.writable():
        out.append(("data_dir_write",
                    f"🚨 The data folder (`{config.DATA_DIR}`) is not writable — nothing the bot "
                    f"records right now (quotes, shipments, payments, voids) is being saved. "
                    f"Check the volume."))
    return out


# ── events ────────────────────────────────────────────────────────────────────
async def _handle_ship(channel) -> None:
    """Customer said 'ship'/'proceed' — give the routed address(es) or the hold."""
    # A voided-and-not-re-quoted channel is cancelled: never rebuild OR book it — a stale in-memory
    # lot or an un-trashed buying sheet must not resurrect the cancelled quote. Alert Kevin (not the
    # customer); a fresh quote clears the marker, so a legitimate re-submit ships normally.
    if tracking.is_voided(channel.id):
        await ping_kevin(f"🛑 #{channel.name} — ship attempt on a VOIDED channel; ignored (no rebuild, no booking).")
        return
    # If a redeploy wiped the in-memory lot, rebuild it from the customer's buying sheets first
    # so sports/OP route correctly (else shipping fails open to VA).
    if not lots.has_lot(channel.id):
        uname = tickets.sanitize_channel_name(channel.name) or channel.name
        try:
            await asyncio.to_thread(lots.rebuild_from_sheets, channel.id, uname, 4,
                                    getattr(channel, "created_at", None))
        except Exception as e:
            print(f"[bot] lot rebuild failed (non-fatal): {e}")
            await alert_kevin(f"lot_rebuild:{channel.id}",
                              f"⚠️ lot rebuild failed on ship (#{channel.name}): {e} — "
                              f"shipping may route to the wrong hub; check this ticket.")
        # The rebuild awaited for seconds: re-check the cancel guard, so a void that landed
        # meanwhile still wins (and drop any lot the rebuild thread just re-created).
        if tracking.is_voided(channel.id):
            lots.drop(channel.id)
            await ping_kevin(f"🛑 #{channel.name} — ship attempt on a VOIDED channel; ignored (no rebuild, no booking).")
            return
    msg, gave, note = shipping.compose_shipping(channel.id)
    await channel.send(msg)
    booked_note = ""
    if gave:
        # _last_quote is in-memory and is wiped by a redeploy; fall back to the PERSISTED
        # quoted ticket so a cert-less "ship" right after a redeploy still books the owed
        # entry (otherwise the shipped lot silently never enters the payout queue).
        snap = _last_quote.get(channel.id) or tracking.quoted_snapshot(channel.id)
        booked = tracking.record_accepted_ship(channel.id, snap)
        # Kevin's ship DM used to read the same whether or not anything was booked. Say what
        # actually happened to the payout queue.
        if booked is None:
            booked_note = (" · ⚠️ NOTHING BOOKED — no accepted cards on record for this ticket "
                           "(the address was still posted).")
        elif not booked.get("persisted", True):
            booked_note = (" · 🚨 the lot did NOT save (storage write failed) — it is NOT reliably in "
                           "the owed queue. Fix storage, then have `ship` sent again in the ticket.")
            alerts.record_error(f"ship_book:{channel.id}",
                                f"ship booking did not persist for #{channel.name}")
        else:
            booked_note = (f" · booked — owed on this ticket is now ${float(booked.get('owed') or 0):,.2f} "
                           f"({int(booked.get('owed_cards') or 0)} card(s)).")
    await ping_kevin(f"🚚 #{channel.name} — ship: {note}{booked_note}")


async def _handle_tracking(channel, tnum) -> None:
    """Customer dropped a tracking number — acknowledge, note it, save to the sheet."""
    uname = tickets.sanitize_channel_name(channel.name) or channel.name
    # Record FIRST, so the ack never promises an arrival update that cannot happen. The
    # "I'll let you know…" wording is used only when all three hold: customer updates are on
    # (BOTV2_DELIVERY_TRACKING), the delivery watcher is on (BOTV2_REGISTER_TRACKING), and the
    # number was actually stored on a ticket (a delivery event can only find a stored number).
    noted = tracking.record_tracking(channel.id, tnum)
    ack = f"📦 Got it — tracking `{tnum}` noted. Kevin will keep an eye out! 🙌"
    if config.DELIVERY_TRACKING and config.REGISTER_TRACKING and noted:
        ack = (f"📦 Got it — tracking `{tnum}` noted. I'll let you know right here the "
               f"moment it arrives — no need to check it yourself. 🙌")
    await channel.send(ack)
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
            await alert_kevin(f"tracking_save:{channel.id}",
                              f"⚠️ tracking sheet-save failed (#{channel.name}): {e} — "
                              f"the number is noted below; add it to the sheet by hand.")
    if noted:
        # Booking as owed happens only on a ship/proceed message. A tracking number for cards
        # that are still merely QUOTED means a package is coming for a lot that is not in the
        # payout queue (no digest line, no `!owe` packet) — tell Kevin; never auto-book here.
        n_q, amt_q = tracking.unbooked_quoted(channel.id)
        warn = (f" — ⚠️ {n_q} quoted card(s) (${amt_q:,.2f}) on this ticket are NOT booked as owed "
                f"(no ship/proceed was handled). `ship` in the ticket books them."
                if n_q else "")
    elif tracking.is_voided(channel.id):
        warn = (" — ⚠️ this channel's quote was VOIDED: no ticket/owed on record, so the number "
                "is NOT stored and nothing is booked.")
    else:
        warn = (" — ⚠️ no ticket on record for this channel, so the number is NOT stored "
                "(a delivery update can't fire for it).")
    await ping_kevin(f"📦 Tracking from **{uname}** (#{channel.name}): `{tnum}`{saved}"
                     + ("" if sheet_url else " — no sheet cached, add by hand") + warn)


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
    kmsg = tracking.delivery_kevin_message(user, number)
    try:
        n_q, amt_q = tracking.unbooked_quoted(int(channel_id))
    except Exception:
        n_q, amt_q = 0, 0.0
    if n_q:                                            # delivered, but never booked as owed
        kmsg += (f" ⚠️ {n_q} quoted card(s) (${amt_q:,.2f}) on this ticket are NOT booked as owed — "
                 f"no ship/proceed was handled, so they're not in the payout queue.")
    await ping_kevin(kmsg)                             # always (Kevin's own DM)
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


def _real_seconds_until(now, target) -> float:
    """Real elapsed seconds from `now` to `target` (both timezone-aware). Subtracting two
    datetimes that share one tzinfo ignores the UTC offset, so across a daylight-saving
    change the answer is an hour off — the 8 AM DMs fired at 7 AM and again at 8 AM on the
    fall-back Sunday (an hour late in spring), and the next Friday digest an hour early
    and then again on time. Converting both to UTC first gives the true gap."""
    from datetime import timezone
    return (target.astimezone(timezone.utc) - now.astimezone(timezone.utc)).total_seconds()


def _seconds_until_friday_9am_et(now=None) -> float:
    """Seconds until the next Friday 09:00 America/New_York. `now` is injectable for tests;
    _now_et() falls back to a fixed -5h offset if the image has no tz database, so the
    digest still fires ~Friday morning rather than dying."""
    from datetime import timedelta
    now = now or _now_et()
    target = now.replace(hour=9, minute=0, second=0, microsecond=0) + timedelta(
        days=(4 - now.weekday()) % 7)             # Friday = weekday 4
    if target <= now:
        target += timedelta(days=7)
    return _real_seconds_until(now, target)


def _seconds_until_daily_et(hour: int = 8, now=None) -> float:
    """Seconds until the next HH:00 America/New_York (same tz fallback as the digest)."""
    from datetime import timedelta
    now = now or _now_et()
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return _real_seconds_until(now, target)


async def _send_digest(kind: str, digest: str) -> bool:
    """DM Kevin a scheduled digest and CHECK that it went out. ping_kevin is best-effort and
    used to be called blind here, so a digest Discord rejected (or closed DMs) simply never
    arrived — no alert, nothing on /status. Nothing to send counts as success. On failure:
    a short alert (it may fail the same way, but it is short and it is recorded for /status)."""
    if not digest:
        return True
    if await ping_kevin(digest):
        return True
    await alert_kevin(f"digest_send:{kind}",
                      f"⚠️ This {kind} digest could NOT be delivered to your DMs "
                      f"({len(digest):,} characters). " +
                      ("Run `!owe` for the unpaid queue." if kind == "payout"
                       else "Run `!stalecheck` to see it."))
    return False


async def _payout_digest_loop():
    """DM Kevin the unpaid-owed digest every Friday 9 AM ET (payouts run Fridays).
    The whole body is guarded so no single failure (scheduling, build, or send) can
    permanently kill the task — it logs, backs off, and re-arms."""
    while True:
        try:
            await asyncio.sleep(_seconds_until_friday_9am_et())
            await _send_digest("payout", payout_digest.build_digest())
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
        except discord.NotFound:
            msg = None                                # really deleted → re-create below
        # Any OTHER error (403 / 5xx / network blip) propagates to the loop's handler, which
        # skips this refresh and alerts Kevin. Treating it as "gone" posted + pinned a second
        # board next to the first and orphaned the old one for good.
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
    return max(60.0, _real_seconds_until(now, target))


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
    except discord.NotFound:
        pass                                         # already gone → just forget the id
    # Any other error propagates (the loop alerts + retries) and the saved id is KEPT: clearing
    # it after a failed delete left the board pinned all week and posted a second one next window.
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
            await _send_digest("stale-ticket", _stale_digest())
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
    # A store that was already unreadable when the bot booted was reported before the loop
    # (and Kevin's DM) existed — say so now. Rate-limited, so reconnects don't repeat it.
    for name, why in state.load_failures().items():
        await alert_kevin(f"state_load:{name}", _state_problem_text("load", name, why))
    if not _digest_started:                        # start the scheduled loops once
        _digest_started = True
        if not config.KEVIN_USER_ID:
            print("[bot] ⚠️ KEVIN_USER_ID is unset — admin commands and ALL Kevin DMs/alerts are disabled.")
        for key, text in _startup_storage_problems():
            print(f"[bot] {text}")
            await alert_kevin(key, text)
        if config.PRICE_MOCK:                      # a live Discord connection + mock pricing = fake quotes
            print("[bot] ⚠️ BOTV2_PRICE_MOCK is ON — quotes are MOCK prices, not real ones.")
            await alert_kevin("price_mock", "🚨 botv2 is running with `BOTV2_PRICE_MOCK=1` — every quote "
                              "is a FAKE mock price. Turn it off unless this is a test bot.")
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
        # Name the ticket and rate-limit per ticket: a bare "on_message error" keyed only by
        # exception type told Kevin nothing and hid every other ticket's failure for 15 minutes.
        ch = getattr(message, "channel", None)
        where = f" in #{ch.name}" if getattr(ch, "name", None) else ""
        print(f"[bot] on_message error{where} (non-fatal): {e}")
        await alert_kevin(f"on_message:{type(e).__name__}:{getattr(ch, 'id', '')}",
                          f"⚠️ botv2 on_message error{where} (non-fatal): {e}")


def _lot_values(rows) -> dict:
    """cert → value for the running lot and the ledger's comp column. The portal's
    marketValue when it sent one (unchanged). For a BUY priced off a last sale with NO
    marketValue, the value its offer was priced against — such a card used to be left out
    of the lot altogether: quoted alone, the customer was told "your quote isn't finalized
    yet"; in a mixed lot its lane/address was missing from the ship reply."""
    return {r.cert: (r.market_value if r.market_value is not None else r.comp_value)
            for r in rows if r.comp_value is not None}


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
        # A short "Shipped! 1Z…" is BOTH agree-shaped and a tracking drop. It used to take the
        # agree branch only, so the number was never acked, stored, saved to the sheet or
        # registered. Do both — ship first (unchanged), so the owed row exists when the
        # number is recorded. Messages that are only one or the other behave as before.
        tnum = certs.extract_tracking(text)
        if shipping.looks_like_agree(text):
            await _handle_ship(channel)
        if tnum:
            await _handle_tracking(channel, tnum)
        return

    # A lone bare 10-digit CGC candidate might be a phone/payment number — confirm
    # it resolves to a real slab on the portal before quoting (v1 behavior).
    if certs.cgc_needs_confirm(text, cert_list):
        try:
            pre = await portal_client.price_certs(cert_list)
        except portal_client.PortalError as e:
            # Pricing is down, so we can't tell a slab from a phone number — say THAT, not
            # "didn't resolve as a slab" (which sent Kevin looking for a typo during an outage).
            await alert_kevin("price_error",
                              f"🚨 **botv2 pricing error** in #{channel.name} while checking a lone "
                              f"number `{certs.cert_digits(cert_list[0])}`: {e} — no quote made. If it "
                              f"was a real CGC cert, have them resend it.")
            return
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
            await alert_kevin(f"price_auth:{channel.id}",
                              f"🚨 **botv2 can't authenticate to pricing** in #{channel.name} — {e}. "
                              f"Check BOT_API_KEY / the portal. Nothing retries on its own: this "
                              f"ticket needs a re-quote once it's fixed.")
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
        channel.id, _lot_values(quote.accepted),
        {r.cert: r.category for r in quote.accepted})
    # A card this re-quote declined leaves the running lot too (the ledger already drops it),
    # so its lane no longer shows up in the ship instructions.
    lots.remove_certs(channel.id, [r.cert for r in quote.rejected])
    _last_quote[channel.id] = {
        "username": username, "comp": quote.comp_total, "payout": quote.payout_total,
        "cards": quote.n_accepted,
        "certs": _lot_values(quote.accepted),
        "cert_offers": {r.cert: r.offer for r in quote.accepted},   # per-cert, for owed accumulation
    }
    retracted = tracking.record_quote(
        channel.id, _last_quote[channel.id], channel_name=channel.name,
        card_detail=[{"cert": r.cert, "value": r.comp_value, "offer": r.offer,
                      "offer_pct": r.offer_pct, "category": r.category,
                      "grader": r.grader, "grade": r.grade, "name": (r.card_name or "")[:60]}
                     for r in quote.accepted],
        passed=[r.cert for r in quote.rejected])   # explicit PASS drops a re-quoted card (combine-all)
    # The buying sheet is built further down, off the customer's path, and can take a minute
    # or more. Remember the channel's cancellation cutoff as of THIS quote: it only ever moves
    # when a void runs, so a different value after the build means the quote was voided meanwhile.
    void_mark = tracking.void_cutoff_of(channel.id)
    proceed = lots.proceed_or_hold_tail(channel.id)
    # instant. A long quote (many declined cards listed one per line) is split across
    # messages — one over Discord's 2,000-char cap is rejected whole, which used to abort
    # this handler here: no quote for the customer, no sheet, no summary for Kevin.
    await textsplit.send_chunked(channel, pricing.customer_message(quote, proceed_override=proceed))
    if retracted:
        # The customer was just told these don't fit — but they HAD an earlier BUY quote on
        # them. Make the retraction visible to Kevin (the summary only shows "rejected").
        await ping_kevin(f"ℹ️ #{channel.name}: this re-quote REMOVED {len(retracted)} previously quoted "
                         f"card(s) from the lot (now declined): "
                         + ", ".join(certs.cert_label(c) for c in retracted)
                         + ". They won't be booked if the customer ships on the earlier quote.")

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
    if tracking.void_cutoff_of(channel.id) != void_mark:
        # Voided between the quote and here (the customer post / claim step awaited): the
        # quote is already dead, so don't build a sheet for it at all.
        await ping_kevin(f"🛑 #{channel.name} was VOIDED right after its quote posted — "
                         f"no buying sheet was built for it.")
        return
    try:
        sheet_url, _name, _ = await asyncio.to_thread(sheets.create_psa_sheet, username, cert_list)
        if tracking.void_cutoff_of(channel.id) != void_mark:
            # A void hit this channel while the sheet was building. It found no sheet to trash,
            # so don't hand the dead quote a live one: no cache entry, no fill, no summary —
            # just give Kevin the link. (No automatic trash from here.)
            await ping_kevin(f"🛑 #{channel.name} was VOIDED while its buying sheet was still building. "
                             f"I did NOT fill or link that sheet — trash it by hand: {sheet_url}")
            return
        _channel_sheet[str(channel.id)] = sheet_url          # cache for later tracking-save
        state.save("channel_sheet", _channel_sheet)
        tracking.remember_sheet(channel.id, sheet_url)       # history: a void reports the earlier ones
        missing = await asyncio.to_thread(sheets.fill_buying_sheet, sheet_url, rows)
        if missing:
            await ping_kevin(f"⚠️ {len(missing)} cert(s) didn't land on {username}'s sheet: "
                             + ", ".join(certs.cert_label(c) for c in missing))
    except Exception as e:
        await alert_kevin(f"sheet_fail:{channel.id}",
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
