"""
tickets — overflow-proof ticket-channel detection (ported faithfully from v1).

Discord caps a category at 50 channels. When the "open ticket" category is full,
Ticket Tool drops new tickets OUTSIDE it, and after the rename-to-username those
channels carry no "ticket" marker — so a plain name/category gate goes blind and
the bot falls silent. Fix (v1, preserved): remember every ticket channel's ID at
creation, and additionally fingerprint-match (private to @everyone + named after a
member who holds an access overwrite). `!adopt` registers one by hand as the last
resort. Every path fails CLOSED — any API weirdness just means "not a ticket," so
the bot never spams a non-ticket channel.
"""
import asyncio
import re

import discord

from . import state

_STORE = "ticket_channels"
_known: set = set(int(x) for x in state.load(_STORE, list))


def remember(channel_id: int) -> None:
    cid = int(channel_id)
    if cid not in _known:
        _known.add(cid)
        state.save(_STORE, sorted(_known))


def known(channel_id: int) -> bool:
    return int(channel_id) in _known


def sanitize_channel_name(name: str) -> str:
    """Discord channel-name rules: lowercase [a-z0-9_-], spaces→dash, cap 90."""
    return re.sub(r"[^a-z0-9_-]", "", (name or "").lower().replace(" ", "-"))[:90]


def fingerprint(channel_name: str, everyone_can_view: bool, member_names) -> bool:
    """Pure Ticket-Tool fingerprint: PRIVATE to @everyone and named after a member
    who holds an access overwrite (exactly what rename-at-creation produces)."""
    if everyone_can_view:
        return False
    return any(sanitize_channel_name(n) == channel_name for n in member_names if n)


def looks_like_ticket(channel) -> bool:
    """Discord wrapper for fingerprint(); fails closed on any API error."""
    try:
        eo = channel.overwrites_for(channel.guild.default_role)
        everyone_can_view = eo.view_channel is not False
        member_names = [t.name for t in channel.overwrites
                        if isinstance(t, discord.Member) and not t.bot]
        return fingerprint(channel.name, everyone_can_view, member_names)
    except Exception:
        return False


def is_ticket_channel(channel) -> bool:
    """The on_message gate: a known ID, a 'ticket' marker in name/category, or a
    fingerprint match — which we then remember so it's instant next time."""
    try:
        cat = channel.category.name.lower() if channel.category else ""
    except Exception:
        cat = ""
    if known(channel.id) or "ticket" in channel.name.lower() or "ticket" in cat:
        return True
    if looks_like_ticket(channel):
        remember(channel.id)
        return True
    return False


async def sweep_overflow(bot) -> int:
    """Startup sweep: register overflow tickets opened while the bot was down (or
    lost to a store wipe) so they work again without Kevin touching them."""
    found = 0
    try:
        for ch in bot.get_all_channels():
            if not isinstance(ch, discord.TextChannel) or known(ch.id):
                continue
            cat = ch.category.name.lower() if ch.category else ""
            if "ticket" in ch.name.lower() or "ticket" in cat:
                continue  # the normal gate already sees these
            if looks_like_ticket(ch):
                remember(ch.id)
                found += 1
    except Exception as e:
        print(f"[tickets] sweep failed (non-fatal): {e}")
    if found:
        print(f"[tickets] swept {found} overflow ticket channel(s)")
    return found


async def rename_new_ticket(bot, channel) -> None:
    """on_guild_channel_create handler: remember + rename #ticket-XXXX to the
    opener's username (the one non-bot member with an access overwrite). Overwrites
    can land a moment after creation, so poll briefly. Best-effort — a failed
    rename never blocks; the ticket still works under its original name."""
    if not isinstance(channel, discord.TextChannel):
        return
    cat = channel.category.name.lower() if channel.category else ""
    if "ticket" not in channel.name.lower() and "ticket" not in cat:
        return
    remember(channel.id)  # once renamed + outside a full category, only the ID identifies it
    opener = None
    for _ in range(10):
        fresh = bot.get_channel(channel.id) or channel
        for target in fresh.overwrites:
            if isinstance(target, discord.Member) and not target.bot:
                opener = target
                channel = fresh
                break
        if opener:
            break
        await asyncio.sleep(3)
    if opener is None:
        print(f"[tickets] no opener overwrite on #{channel.name} — leaving as-is")
        return
    new_name = sanitize_channel_name(opener.name)
    if not new_name or channel.name == new_name:
        return
    try:
        await channel.edit(name=new_name, reason="KTS: ticket renamed to opener's username")
        print(f"[tickets] renamed #{channel.name} -> #{new_name}")
    except Exception as e:
        print(f"[tickets] rename failed for #{channel.name} (needs Manage Channels?): {e}")
