"""
commands — `!` command dispatch. Admin commands are Kevin-only.

Handled BEFORE the ticket gate so `!adopt` works in a channel the gate doesn't yet
recognize (an overflow ticket). Unknown commands fall through to normal processing
(which simply ignores non-cert text), so this never swallows a real message.
"""
import asyncio

from . import config, tickets, tracking


async def _dm_kevin_result(message, channel, text) -> None:
    """DM Kevin a command result. If typed in a guild channel, delete the command first
    (it may name a seller / carry $ figures) and stay silent there — no customer-facing
    ack. Falls back to a terse channel notice only if the DM itself fails."""
    in_guild = getattr(channel, "guild", None) is not None
    if in_guild:
        try:
            await message.delete()
        except Exception:
            pass
    try:
        await message.author.send(text)
    except Exception as e:
        print(f"[cmd] result DM failed: {e}")
        if in_guild:
            try:
                await channel.send("⚠️ Couldn't DM you — open your DMs to the bot and retry.")
            except Exception:
                pass


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
                     "• `!resolve` — clear a ticket's ⚠️ review flag after you've checked it (money unchanged)\n"
                     "• `!void <user | id:channel>` — kill a quote so it can't be accepted/shipped/paid (off-hours/error); add `force` for a shipped ticket\n"
                     "• `!bands` — DM you the live allocation board\n"
                     "• `!lanes` — DM you the customer-safe lanes board (preview before it goes live)\n"
                     "• `!stats [days]` — DM you volume + payout analytics\n"
                     "• `!stalecheck` — DM you the stuck-ticket digest on demand (same as the daily 8am check)\n"
                     "• `!owe [user]` — DM you copy-paste payment packets for the unpaid queue\n"
                     "• `!payinfo` — store/view a seller's wire info (use in a DM; I scrub it from channels)\n"
                     "• `!synctracking` — register open tickets' tracking #s with the delivery watcher\n"
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

    if cmd == "resolve":
        if not is_kevin:
            return True
        cleared = tracking.resolve_review(channel.id)
        if cleared:
            shown = ", ".join(str(c) for c in cleared)
            await channel.send(f"✅ Review flag cleared for this ticket ({shown}). "
                               f"The owed amount is unchanged — pay it when you're ready.")
        else:
            await channel.send("ℹ️ Nothing to resolve — this ticket has no review flag.")
        return True

    if cmd == "void":
        # Kill a quote so it can never be accepted/shipped/paid — the bot-side twin of the
        # portal's remove-submission control. Works by username (the seller's OPEN quote)
        # or an explicit `id:<channel_id>`, runnable from a DM even after the ticket
        # channel is deleted. A shipped/paid ticket (a real balance) needs `force`.
        if not is_kevin:
            return True
        from . import lots, sheets, bot as botmod
        a = parts[1:]
        force = any(w.lower() == "force" for w in a)
        target = next((w for w in a if w.lower() != "force"), None)
        if not target:
            await _dm_kevin_result(message, channel,
                "**!void** — kill a quote so it can't be accepted, shipped, or paid.\n"
                "• `!void <username>` — voids that seller's OPEN (quoted) ticket\n"
                "• `!void id:<channel_id>` — voids one specific ticket\n"
                "• add `force` to void a shipped/paid ticket (drops a real balance)\n"
                "Best run in a DM with me.")
            return True
        # Resolve the target → a channel_id (or send a disambiguation / error and stop).
        cid = None
        if target.lower().startswith("id:"):
            raw = target[3:]
            if not raw.lstrip("-").isdigit():
                await _dm_kevin_result(message, channel, f"⚠️ `{target}` isn't a valid channel id.")
                return True
            cid = int(raw)
        elif target.lstrip("-").isdigit():
            cid = int(target)
        else:
            uname = target.lstrip("@")
            quoted = tracking.find_tickets(user=uname, status="quoted")
            if len(quoted) == 1:
                cid = quoted[0].get("channel_id")
            elif not quoted:
                others = tracking.find_tickets(user=uname)
                if others:
                    await _dm_kevin_result(message, channel,
                        f"ℹ️ No OPEN (quoted) ticket for **{uname}** — their {len(others)} ticket(s) are "
                        f"already shipped/paid. To void one of those, use `!void id:<channel_id> force`.")
                else:
                    await _dm_kevin_result(message, channel, f"ℹ️ No ticket on record for **{uname}**.")
                return True
            else:
                lines = [f"⚠️ **{uname}** has {len(quoted)} open quotes — pick one with `!void id:<channel_id>`:"]
                for r in quoted:
                    lines.append(f"• `id:{r.get('channel_id')}` — {r.get('cards', 0)} cards, "
                                 f"${float(r.get('payout', 0) or 0):,.2f}, {r.get('date', '')}")
                await _dm_kevin_result(message, channel, "\n".join(lines))
                return True
        # Status / force gate.
        rows = tracking.find_tickets(channel_id=cid)
        if not rows:
            await _dm_kevin_result(message, channel, f"ℹ️ No ticket on record for channel `{cid}`.")
            return True
        st = rows[0].get("status", "quoted")
        if st in ("accepted", "paid") and not force:
            await _dm_kevin_result(message, channel,
                f"🛑 Channel `{cid}` is **{st}** — a real balance of "
                f"${float(rows[0].get('payout', 0) or 0):,.2f}. Voiding it drops that balance. "
                f"Re-run as `!void id:{cid} force` if you really mean it.")
            return True
        # Void it, then clear the in-memory lot + last-quote so a still-open channel can't
        # re-book it, and (best-effort) trash the Drive buying sheet.
        res = tracking.void_ticket(cid)
        if not res.get("found"):
            await _dm_kevin_result(message, channel, f"ℹ️ Nothing to void for channel `{cid}` (already gone).")
            return True
        vcid = res.get("channel_id", cid)
        lots.drop(vcid)
        botmod._last_quote.pop(vcid, None)
        botmod._last_quote.pop(cid, None)
        try:
            botmod._channel_sheet.pop(str(vcid), None)
            botmod._channel_sheet.pop(str(cid), None)
        except Exception:
            pass
        trashed = None
        if res.get("sheet_url"):
            try:
                trashed = await asyncio.to_thread(sheets.trash_sheet, res["sheet_url"])
            except Exception as e:
                print(f"[cmd] !void sheet trash failed: {e}")
                trashed = False
        out = [f"🗑️ **Voided** {res.get('user') or '?'}'s ticket "
               f"(`{vcid}`, was **{res.get('status')}**): {res.get('cards', 0)} cards, "
               f"${float(res.get('payout', 0) or 0):,.2f}.",
               "Removed: quote record" + (" + owed balance" if res.get("had_owed") else "")
               + " + sheet cache + running lot."]
        if res.get("sheet_url"):
            out.append("🗑️ Buying sheet moved to Drive trash (recoverable)." if trashed
                       else f"⚠️ Couldn't auto-trash the buying sheet — do it by hand: {res['sheet_url']}")
        out.append("It can no longer be accepted, shipped, or paid.")
        await _dm_kevin_result(message, channel, "\n".join(out))
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

    if cmd == "lanes":
        # DM Kevin a PREVIEW of the CUSTOMER-SAFE lanes board (the same render the pinned
        # auto-board posts), so he can eyeball/tweak the look before pointing it at a
        # customer channel. Kevin-only; the board itself carries no numbers/names.
        if not is_kevin:
            return True
        from . import pricing, portal_client
        try:
            board = pricing.lanes_board(await portal_client.fetch_allocation())
        except Exception as e:
            print(f"[cmd] !lanes fetch failed: {e}")
            board = f"⚠️ Couldn't load the lanes board: {e}"
        preview = "👁️ **Lanes board preview** — what customers would see:\n\n" + board
        if not (config.LANES_BOARD and config.LANES_CHANNEL_ID):
            preview += ("\n\n(Preview only — the live auto-board is OFF. Set "
                        "`BOTV2_LANES_BOARD=1` + `BOTV2_LANES_CHANNEL_ID=<channel id>` to post it.)")
        try:
            await message.author.send(preview)
        except Exception as e:
            print(f"[cmd] !lanes DM failed: {e}")
            try:
                await channel.send("⚠️ Couldn't DM you the lanes preview — open your DMs to the bot and retry.")
            except Exception:
                pass
            return True
        if getattr(channel, "guild", None) is not None:
            try:
                await channel.send("👁️ Lanes board preview sent to your DMs.")
            except Exception:
                pass
        return True

    if cmd == "stats":
        if not is_kevin:
            return True
        from . import analytics
        days = int(parts[1]) if (len(parts) > 1 and parts[1].isdigit()) else None
        txt = analytics.summary_text(days)
        # DM Kevin the figures — payout/customer numbers NEVER go into a (customer) channel.
        try:
            await message.author.send(txt)
        except Exception as e:
            print(f"[cmd] !stats DM failed: {e}")
            try:
                await channel.send("⚠️ Couldn't DM you the stats — open your DMs to the bot and retry.")
            except Exception:
                pass
            return True
        if getattr(channel, "guild", None) is not None:
            try:
                await channel.send("📈 Stats sent to your DMs.")
            except Exception:
                pass
        return True

    if cmd in ("stalecheck", "stale"):
        # On-demand version of the daily 8am-ET stuck-ticket digest (so Kevin can test /
        # check it any time). Kevin-only; NOT gated by BOTV2_STALE_NUDGES (that flag only
        # controls the automatic daily loop). The digest carries seller names + channels,
        # so it's DM-only, never a customer channel.
        if not is_kevin:
            return True
        from . import bot as botmod                      # reuse the daily loop's digest builder
        digest = botmod._stale_digest() or \
            "✅ Stale-ticket check — nothing stuck past its thresholds right now."
        # Backtick the env var (its underscores would otherwise be eaten by Discord's
        # `_italics_`), and positively confirm whether the daily auto-nudge is scheduled.
        if config.STALE_NUDGES:
            digest += "\n\n_(Daily auto-nudge is ON — you also get this automatically at 8 AM ET.)_"
        else:
            digest += ("\n\n(Daily auto-nudge is OFF — this ran on demand only. Set "
                       "`BOTV2_STALE_NUDGES=1` on Railway to also get it automatically at 8 AM ET.)")
        try:
            await message.author.send(digest)
        except Exception as e:
            print(f"[cmd] !stalecheck DM failed: {e}")
            try:
                await channel.send("⚠️ Couldn't DM you the stale check — open your DMs to the bot and retry.")
            except Exception:
                pass
            return True
        if getattr(channel, "guild", None) is not None:
            try:
                await channel.send("🕒 Stale-ticket check sent to your DMs.")
            except Exception:
                pass
        return True

    if cmd == "payinfo":
        # Wire/ACH details are CONFIDENTIAL — never leave them sitting in a channel.
        # If typed in a guild channel, delete the command first (it may carry details
        # on `!payinfo set`), then DM Kevin. Best used in a DM with the bot.
        in_guild = getattr(channel, "guild", None) is not None
        if in_guild:
            try:
                await message.delete()
            except Exception as e:
                print(f"[cmd] !payinfo couldn't delete in-channel message: {e}")
        if not is_kevin:
            return True   # swallow admin command (already scrubbed if it was in a channel)
        reply = tracking.payinfo_command(message.content)
        try:
            await message.author.send(reply)
        except Exception as e:
            print(f"[cmd] !payinfo DM failed: {e}")
            if in_guild:
                try:   # never echo the details into the channel
                    await channel.send("⚠️ Couldn't DM you — open your DMs to the bot, then retry `!payinfo` (I deleted the message).")
                except Exception:
                    pass
            return True
        if in_guild:
            try:
                await channel.send("🔒 Handled in your DMs — I deleted the message so details don't sit in the channel.")
            except Exception:
                pass
        return True

    if cmd == "owe":
        if not is_kevin:
            return True
        user = parts[1].lstrip("@") if len(parts) > 1 else None
        packet = tracking.build_owe_packets(user)
        # Packets carry wire details → DM only, never a channel.
        try:
            await message.author.send(packet)
        except Exception as e:
            print(f"[cmd] !owe DM failed: {e}")
            if getattr(channel, "guild", None) is not None:
                try:
                    await channel.send("⚠️ Couldn't DM you — open your DMs to the bot and retry.")
                except Exception:
                    pass
            return True
        if getattr(channel, "guild", None) is not None:
            try:
                await message.delete()
            except Exception:
                pass
            try:
                await channel.send("💸 Payment packet(s) sent to your DMs.")
            except Exception:
                pass
        return True

    if cmd == "synctracking":
        # Backfill: register any open tickets' tracking numbers with the delivery watcher
        # (for ones dropped before registration was on, or to re-sync). Kevin-only; the
        # portal dedups, so it's safe to re-run. Registers even if the auto flag is off.
        if not is_kevin:
            return True
        from . import portal_client                     # config is already module-level
        if getattr(channel, "guild", None) is not None:
            try:
                await message.delete()
            except Exception:
                pass
        pending = tracking.tickets_with_open_tracking()
        if not pending:
            try:
                await message.author.send("📦 No open tickets with a tracking number to register.")
            except Exception:
                pass
            return True
        ok = fail = 0
        for t in pending:
            try:
                await portal_client.register_tracking(t["channel_id"], t["tracking"])
                ok += 1
            except Exception as e:
                fail += 1
                print(f"[cmd] synctracking failed for {t['tracking']}: {e}")
        gate = "" if config.REGISTER_TRACKING else \
            "  ⚠️ BOTV2_REGISTER_TRACKING is OFF, so NEW drops won't auto-register yet."
        try:
            await message.author.send(
                f"📦 Registered {ok}/{len(pending)} open tracking number(s) with the delivery watcher"
                + (f" — {fail} failed (see logs)" if fail else "") + "." + gate)
        except Exception:
            pass
        return True

    return False   # unknown command → fall through (normal processing ignores it)
