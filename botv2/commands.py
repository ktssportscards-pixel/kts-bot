"""
commands — `!` command dispatch. Admin commands are Kevin-only.

Handled BEFORE the ticket gate so `!adopt` works in a channel the gate doesn't yet
recognize (an overflow ticket). Unknown commands fall through to normal processing
(which simply ignores non-cert text), so this never swallows a real message.
"""
import asyncio

from . import config, textsplit, tickets, tracking


async def _dm_kevin_result(message, channel, text, delete: bool = True) -> None:
    """DM Kevin a command result. If typed in a guild channel, delete the command first
    (it may name a seller / carry $ figures) and stay silent there — no customer-facing
    ack. Falls back to a terse channel notice only if the DM itself fails. Pass
    delete=False when the caller already removed the command message."""
    in_guild = getattr(channel, "guild", None) is not None
    if in_guild and delete:
        try:
            await message.delete()
        except Exception:
            pass
    try:
        await textsplit.send_chunked(message.author, text)
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
                     "• `!bands` — DM you the live bands board\n"
                     "• `!lanes` — DM you the customer-safe lanes board (preview before it goes live)\n"
                     "• `!stats [days]` — DM you volume + payout analytics\n"
                     "• `!stalecheck` — DM you the stuck-ticket digest on demand (same as the daily 8am check)\n"
                     "• `!owe [user]` — DM you copy-paste payment packets for the unpaid queue\n"
                     "• `!payinfo` — store/view a seller's wire info (use in a DM; I scrub it from channels)\n"
                     "• `!synctracking` — register open tickets' tracking #s with the delivery watcher\n"
                     "• VIP is managed in the KTS app's VIP section (portal is the source of truth)")
        await textsplit.send_chunked(channel, base)
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
        if ok:
            reply = "✅ Marked paid."
        elif tracking.has_owed_entry(channel.id):
            # mark_paid returns False for a failed write too — don't call that "no owed entry".
            reply = ("⚠️ Couldn't save that — a storage write failed, so this is NOT marked paid yet. "
                     "Try `!paid` again.")
        else:
            reply = "⚠️ No owed entry for this channel."
        await channel.send(reply)
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
        # The command names a seller — remove it from a guild channel BEFORE any slow work
        # (the Drive trash below can take 20s+; the message used to sit there meanwhile).
        if getattr(channel, "guild", None) is not None:
            try:
                await message.delete()
            except Exception:
                pass

        async def reply(text):
            await _dm_kevin_result(message, channel, text, delete=False)
        a = parts[1:]
        force = any(w.lower() == "force" for w in a)
        target = next((w for w in a if w.lower() != "force"), None)
        if not target:
            await reply(
                "**!void** — kill a quote so it can't be accepted, shipped, or paid.\n"
                "• `!void <username>` — voids that seller's OPEN (quoted) ticket\n"
                "• `!void id:<channel_id>` — voids one specific ticket\n"
                "• add `force` to void a shipped/paid ticket (drops a real balance)\n"
                "Best run in a DM with me.")
            return True
        # Resolve the target → a channel_id (or send a disambiguation / error and stop).
        cid = None
        is_id = target.lower().startswith("id:")
        raw = target[3:] if is_id else target
        if is_id or raw.lstrip("-").isdigit():
            cid = tracking.parse_channel_id(raw)
            if cid is None:
                await reply(f"⚠️ `{target}` isn't a valid channel id.")
                return True
        else:
            uname = target.strip().lstrip("@").strip()
            if not uname:                 # "@" / "@ bob" — never let a blank name match every ticket
                await reply(
                    f"⚠️ `{target}` isn't a username. Use `!void <username>` (no space after the @) "
                    f"or `!void id:<channel_id>`.")
                return True
            quoted = tracking.find_tickets(user=uname, status="quoted")
            if len(quoted) == 1:
                cid = quoted[0].get("channel_id")
            elif not quoted:
                others = tracking.find_tickets(user=uname)
                if others:
                    await reply(
                        f"ℹ️ No OPEN (quoted) ticket for **{uname}** — their {len(others)} ticket(s) are "
                        f"already shipped/paid. To void one of those, use `!void id:<channel_id> force`.")
                else:
                    await reply(
                        f"ℹ️ No ticket on record for **{uname}**. (To finish cleaning up after an earlier "
                        f"partial void, use `!void id:<channel_id>` — a username can't find a ticket "
                        f"that's already gone.)")
                return True
            else:
                lines = [f"⚠️ **{uname}** has {len(quoted)} open quotes — pick one with `!void id:<channel_id>`:"]
                for r in quoted:
                    lines.append(f"• `id:{r.get('channel_id')}` — {r.get('cards', 0)} cards, "
                                 f"${float(r.get('payout', 0) or 0):,.2f}, {r.get('date', '')}")
                await reply("\n".join(lines))
                return True
        # Status / force gate — applies ONLY when a ticket actually exists (a real balance needs
        # force). With no ticket we still call the idempotent purge below, so a re-run clears
        # residual state left by a prior partial void instead of stalling at "nothing to void".
        rows = tracking.find_tickets(channel_id=cid)
        if rows and rows[0].get("status", "quoted") in ("accepted", "paid") and not force:
            # Say what is actually OWED. The ticket total is every card ever quoted on it
            # (still-quoted + shipped + already paid), which read as "a real balance" even for
            # a ticket that was fully paid.
            total = float(rows[0].get("payout", 0) or 0)
            o = tracking.owed_entry(cid)
            if o is None:
                money = f"ticket total ${total:,.2f}"
            elif o.get("paid"):
                money = f"already PAID (nothing outstanding; ticket total ${total:,.2f})"
            else:
                money = (f"**${float(o.get('payout', 0) or 0):,.2f} still owed (unpaid)**; "
                         f"ticket total ${total:,.2f}")
            await reply(
                f"🛑 Channel `{cid}` has **shipped** — {money}. Voiding it deletes the whole ticket: "
                f"the owed balance, the paid history and any newer open quote on it. "
                f"Re-run as `!void id:{cid} force` if you really mean it.")
            return True
        res = tracking.void_ticket(cid)          # idempotent full purge (runs even with no ticket)
        vcid = res.get("channel_id", cid)
        removed = bool(res.get("ticket_removed"))
        # found-but-not-removed = the void BAILED on a storage write with the ticket still live.
        # Then NOTHING may be cleaned up or trashed: the lot, the quote and the sheet all still
        # belong to a live ticket (void_ticket still returns its sheet_url). Only a dead quote
        # (removed) or a channel with no ticket at all (residue of an earlier partial void) is
        # purged — in memory and in Drive — so nothing can reconstruct a cancelled quote.
        bailed = bool(res.get("found")) and not removed
        trashed, had_mem, mem_sheet = None, False, ""
        sheet_url = res.get("sheet_url") or ""
        if not bailed:
            try:                                   # the in-memory cache normally mirrors the disk copy
                mem_sheet = (botmod._channel_sheet.pop(str(vcid), None)
                             or botmod._channel_sheet.pop(str(cid), None) or "")
                botmod._channel_sheet.pop(str(cid), None)
            except Exception:
                pass
            had_mem = bool(lots.drop(vcid)) | bool(lots.drop(cid))
            had_mem |= botmod._last_quote.pop(vcid, None) is not None
            had_mem |= botmod._last_quote.pop(cid, None) is not None
            if sheet_url:
                try:
                    trashed = await asyncio.to_thread(sheets.trash_sheet, sheet_url)
                except Exception as e:
                    print(f"[cmd] !void sheet trash failed: {e}")
                    trashed = False
        sheet_lines = []
        if sheet_url and not bailed:
            sheet_lines.append("🗑️ Buying sheet moved to Drive trash (recoverable)." if trashed
                               else f"⚠️ Couldn't auto-trash the buying sheet — do it by hand: {sheet_url}")
        if mem_sheet and mem_sheet != sheet_url:
            # Only the in-memory cache knew this sheet (its cache-file write had failed). The
            # automatic trash acts on the persisted pointer only, so hand Kevin the link.
            sheet_lines.append(f"⚠️ I also had a buying sheet on record for this channel that wasn't saved "
                               f"to disk — trash it by hand: {mem_sheet}")
        # Every cert message builds its own sheet; only the latest is trashed automatically.
        # Never let Kevin believe the ticket's sheets are gone when earlier ones are still live.
        others = [] if bailed else [u for u in (res.get("other_sheet_urls") or []) if u != mem_sheet]
        if others:
            sheet_lines.append(
                f"📄 {len(others)} EARLIER buying sheet(s) were built for this ticket and are still live "
                f"in Drive (only the latest is auto-trashed) — trash them by hand if they're dead too:\n"
                + "\n".join(f"• {u}" for u in others))
        by_hand = bool((sheet_url and not bailed and not trashed) or (mem_sheet and mem_sheet != sheet_url)
                       or others)
        if removed:
            out = [f"🗑️ **Voided** {res.get('user') or '?'}'s ticket "
                   f"(`{vcid}`, was **{res.get('status')}**): {res.get('cards', 0)} cards, "
                   f"${float(res.get('payout', 0) or 0):,.2f}.",
                   "Removed: quote record" + (" + owed balance" if res.get("had_owed") else "")
                   + " + sheet cache + running lot."]
            out += sheet_lines
            out.append("It can no longer be accepted, shipped, or paid.")
            if not res.get("ok", True):
                out.append(f"(Minor: the sheet-cache entry didn't clear on the first write — harmless; "
                           f"re-run `!void id:{vcid}` once to tidy it. The quote is fully dead regardless.)")
            await reply("\n".join(out))
            return True
        if bailed:
            # An owed/ticket/marker write failed → the ticket is still live. Say exactly what
            # did and didn't happen; nothing in memory or Drive was touched.
            owed_note = (" Its owed balance WAS already removed before the failure — re-run now so the "
                         "ticket doesn't sit on record as shipped with nothing owed."
                         if res.get("owed_removed") else " Nothing was removed.")
            ship_note = (" (The cancel guard did get set, so a customer `ship` is ignored on this "
                         "channel until you re-run or they re-quote.)" if tracking.is_voided(cid) else "")
            await reply(
                f"⚠️ Couldn't void channel `{cid}` — a storage write failed, so the ticket is STILL LIVE."
                f"{owed_note} The buying sheet and the running lot were left alone.{ship_note} "
                f"Re-run the same command to retry.")
            return True
        # No ticket on record.
        residual = bool(res.get("sheet_url") or res.get("had_owed") or res.get("other_sheet_urls"))
        if residual or had_mem or mem_sheet:
            cleared = [w for w, hit in (("sheet cache", bool(res.get("sheet_url"))),
                                        ("owed balance", bool(res.get("owed_removed"))),
                                        ("in-memory quote / lot", had_mem)) if hit]
            out = [f"🧹 Channel `{cid}` had no live ticket, but I cleared leftover state"
                   + (f" ({', '.join(cleared)})" if cleared else "") + "."]
            out += sheet_lines
            if not res.get("ok", True):
                out.append(f"⚠️ A storage write failed during that cleanup — something is still on disk. "
                           f"Re-run `!void id:{cid}` to finish.")
            elif by_hand:
                out.append("Everything else is clean; only the sheet(s) above still need trashing.")
            else:
                out.append("It's fully clean now.")
            await reply("\n".join(out))
        else:
            await reply(f"ℹ️ Nothing to void for channel `{cid}` (already gone).")
        return True

    if cmd == "bands":
        if not is_kevin:
            return True
        from . import pricing, portal_client
        try:
            board = pricing.allocation_board(await portal_client.fetch_allocation())
        except Exception as e:      # PortalError, but also raw network / timeout errors (as !lanes does)
            print(f"[cmd] !bands fetch failed: {e}")
            board = f"⚠️ Couldn't load the allocation board: {e}"
        # DM Kevin the figures — NEVER post the real numbers into a (customer) channel.
        try:
            await textsplit.send_chunked(message.author, board)
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
                await channel.send("📊 Board sent to your DMs.")   # no forbidden word in a channel
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
            await textsplit.send_chunked(message.author, preview)
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
            await textsplit.send_chunked(message.author, txt)
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
            await textsplit.send_chunked(message.author, digest)
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
        deleted = False
        if in_guild:
            try:
                await message.delete()
                deleted = True
            except Exception as e:
                print(f"[cmd] !payinfo couldn't delete in-channel message: {e}")
        if not is_kevin:
            return True   # swallow admin command (already scrubbed if it was in a channel)
        # Never claim the message is gone when the delete failed — the details would sit in
        # the channel while Kevin believes they were scrubbed.
        not_deleted = ("⚠️ I could NOT delete your `!payinfo` message here (missing Manage Messages?) — "
                       "delete it yourself so the details don't sit in the channel.")
        reply = tracking.payinfo_command(message.content)
        try:
            await textsplit.send_chunked(message.author, reply)
        except Exception as e:
            print(f"[cmd] !payinfo DM failed: {e}")
            if in_guild:
                try:   # never echo the details into the channel
                    await channel.send("⚠️ Couldn't DM you — open your DMs to the bot, then retry `!payinfo`"
                                       + (" (I deleted the message)." if deleted else ". " + not_deleted))
                except Exception:
                    pass
            return True
        if in_guild:
            try:
                await channel.send("🔒 Handled in your DMs — I deleted the message so details don't sit in the channel."
                                   if deleted else "🔒 Handled in your DMs. " + not_deleted)
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
            await textsplit.send_chunked(message.author, packet)
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
