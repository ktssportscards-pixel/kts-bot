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

from . import (certs, config, lots, portal_client, pricing, sheets, shipping,
               state, tickets, vip, webserver, welcome)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True       # ticket detection reads member permission overwrites
bot = discord.Client(intents=intents)

# Welcomed ticket channels (persisted; survives redeploys with a DATA_DIR volume).
_welcomed: set = set(int(x) for x in state.load("welcomed", list))


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
    msg, _gave, note = shipping.compose_shipping(channel.id)
    await channel.send(msg)
    await ping_kevin(f"🚚 #{channel.name} — ship: {note}")


_web_runner = None


@bot.event
async def on_ready():
    global _web_runner
    print(f"[bot] logged in as {bot.user} (botv2)")
    if _web_runner is None:                       # start once; on_ready re-fires on reconnect
        try:
            _web_runner = await webserver.start()
        except Exception as e:
            print(f"[bot] web server start failed (non-fatal): {e}")
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
        return  # CSV / tracking / commands handled by later modules

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
    # VIP is portal-sourced: pass the username and the portal applies that seller's
    # VIP rates from its own VIP section (single source). rate_override is only a
    # rare manual bot-side override for a one-off non-portal customer (usually None).
    rate_override = vip.override_for(username)

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
    proceed = lots.proceed_or_hold_tail(channel.id)
    await channel.send(pricing.customer_message(quote, proceed_override=proceed))  # instant

    # Buying sheet for Kevin — deliberately OFF the customer's critical path (the
    # Apps Script is slow). The customer already has their quote; Kevin gets the
    # sheet link + summary once it's built. A sheet failure never blocks the DM.
    sheet_url = ""
    try:
        sheet_url, _name, _ = await asyncio.to_thread(sheets.create_psa_sheet, username, cert_list)
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
