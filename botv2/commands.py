"""
commands — `!` command dispatch. Admin commands are Kevin-only.

Handled BEFORE the ticket gate so `!adopt` works in a channel the gate doesn't yet
recognize (an overflow ticket). Unknown commands fall through to normal processing
(which simply ignores non-cert text), so this never swallows a real message.
"""
from . import config, tickets, tracking


async def handle(bot, message) -> bool:
    """Handle a `!` command. Returns True if one was handled (caller should stop)."""
    text = (message.content or "").strip()
    if not text.startswith("!"):
        return False
    parts = text[1:].split()
    if not parts:
        return False
    cmd = parts[0].lower()
    channel = message.channel
    is_kevin = bool(config.KEVIN_USER_ID) and message.author.id == config.KEVIN_USER_ID

    if cmd in ("ping", "health"):
        await channel.send("🏓 botv2 is up.")
        return True

    if cmd == "help":
        base = ("**KTS bot**\n"
                "• Drop your PSA/CGC cert numbers → instant quote\n"
                "• `ship` or `proceed` → shipping address once your lot's set")
        if is_kevin:
            base += ("\n\n**Admin (you):**\n"
                     "• `!adopt` — treat this channel as a ticket\n"
                     "• `!paid` — mark this ticket's lot paid\n"
                     "• `!bands` — DM you the live allocation board\n"
                     "• VIP is managed in the KTS app's VIP section (portal is the source of truth)")
        await channel.send(base)
        return True

    if cmd == "adopt":
        if not is_kevin:
            return True   # silently ignore admin commands from non-Kevin
        tickets.remember(channel.id)
        await channel.send("✅ Adopted — I'll treat this channel as a ticket now.")
        return True

    if cmd == "paid":
        if not is_kevin:
            return True
        ok = tracking.mark_paid(channel.id)
        await channel.send("✅ Marked paid." if ok else "⚠️ No owed entry for this channel.")
        return True

    if cmd == "bands":
        if not is_kevin:
            return True
        from . import pricing, portal_client
        try:
            board = pricing.allocation_board(await portal_client.fetch_allocation())
        except portal_client.PortalError as e:
            board = f"⚠️ Couldn't load the allocation board: {e}"
        # DM Kevin the figures — NEVER post the real numbers into a (customer) channel.
        try:
            await message.author.send(board)
        except Exception as e:
            print(f"[cmd] !bands DM failed: {e}")
            try:
                await channel.send("⚠️ Couldn't DM you the board — open your DMs to the bot and retry.")
            except Exception:
                pass
            return True
        # In a guild channel, note it went to DMs (no numbers leak). In a DM the board's already here.
        if getattr(channel, "guild", None) is not None:
            try:
                await channel.send("📊 Allocation board sent to your DMs.")
            except Exception:
                pass
        return True

    return False   # unknown command → fall through (normal processing ignores it)
