"""
webserver — authenticated JSON endpoints for the portal + monitoring, plus /health.

Binds $PORT so Railway's health-check passes (the bot holds the port). Public:
/health only. Authenticated (x-bot-key, fails closed): /ops (portal ops dashboard),
/status (monitoring + flags), /analytics (volume/payout), /delivery (carrier webhook
sink). The old v1 /owed /tickets /leaderboard /links reads were removed — their only
consumer was a retired local tracker, and the portal reads /ops instead.
"""
import datetime
import hmac

import aiohttp.web as web

from . import config, state

# Captured at process start, so /health reveals which deploy is live: a recent
# timestamp = a fresh redeploy (lets a deploy be verified from outside).
_STARTED = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


def _authed(req) -> bool:
    """x-bot-key gate, shared by every non-public endpoint. Fails CLOSED when BOT_API_KEY
    is unset (no key configured → deny everyone) so an endpoint can never be accidentally
    public. Only /health is intentionally unauthenticated (Railway health-check + canary)."""
    key, got = config.BOT_API_KEY, req.headers.get("x-bot-key")
    if not key or not isinstance(got, str):
        return False
    # constant-time compare (bytes, so a non-ASCII header can't raise) — no timing oracle on the key
    return hmac.compare_digest(got.encode("utf-8", "backslashreplace"),
                               key.encode("utf-8", "backslashreplace"))


def _unauthorized():
    return web.json_response({"error": "unauthorized"}, status=401)


_SNOWFLAKE_KEYS = ("channel_id", "message_id")


def _stringify_snowflakes(obj):
    """Emit Discord snowflake ids (channel_id/message_id) as JSON STRINGS, recursively.
    A snowflake (e.g. 1492170401608896533) exceeds JS's safe-integer range (2**53), so as
    a JSON *number* it is silently corrupted by JSON.parse (…896533 → …896500) and the
    native app — which decodes channel_id as a String — fails the whole decode. Non-
    snowflake ints (counts, uptime, offer_pct, …) are left untouched."""
    if isinstance(obj, dict):
        return {k: (str(v) if (k in _SNOWFLAKE_KEYS and isinstance(v, int))
                    else _stringify_snowflakes(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_stringify_snowflakes(x) for x in obj]
    return obj


def _json(payload):
    """CORS JSON response with snowflake ids stringified (see _stringify_snowflakes).
    Use this for any payload that can carry a channel_id / snowflake id."""
    return _cors(web.json_response(_stringify_snowflakes(payload)))


async def _health(_req):
    return _cors(web.Response(
        text=f"KTS botv2 — OK · started {_STARTED}"))


async def _ops(req):
    """Consolidated per-ticket lifecycle (quote → claim → ship → owed) for the portal's
    ops dashboard. AUTHENTICATED with x-bot-key — unlike the local-tracker reads above,
    this is the full picture and must not be public."""
    if not _authed(req):
        return _unauthorized()
    from . import tracking
    detail = req.query.get("cards", "").lower() in ("1", "true", "yes")   # ?cards=1 → per-cert
    return _json({"tickets": tracking.ops_view(detail=detail)})


async def _status(req):
    """Machine-readable health for monitoring: uptime (from the process-start stamp),
    live store counts (open tickets / unpaid owed), and the last failure the bot saw.
    AUTHENTICATED with x-bot-key (same gate as /ops) — it exposes internal counts, so
    it must not be public. Additive: /health stays the public liveness check."""
    if not _authed(req):
        return _unauthorized()
    from . import tracking, alerts, state
    try:
        started = datetime.datetime.fromisoformat(_STARTED)
        uptime = int((datetime.datetime.now(datetime.timezone.utc) - started).total_seconds())
    except Exception:
        uptime = None
    return _cors(web.json_response({
        "ok": True,
        "started": _STARTED,
        "uptime_seconds": uptime,
        "counts": tracking.counts(),
        "last_error": alerts.last_error(),
        "flags": {                                   # non-secret toggle states (NO keys/payout/margin)
            "delivery_tracking": config.DELIVERY_TRACKING,
            "register_tracking": config.REGISTER_TRACKING,
            "lanes_board": config.LANES_BOARD,
            "lanes_schedule": config.LANES_SCHEDULE,
            "lanes_channel_id": str(config.LANES_CHANNEL_ID) if config.LANES_CHANNEL_ID else "",
            "stale_nudges": config.STALE_NUDGES,
            "ship_all_doral": config.SHIP_ALL_DORAL,
            "auto_claim": config.AUTO_CLAIM,
            "pokemon_lane_open": config.POKEMON_LANE_OPEN,
            "price_mock": config.PRICE_MOCK,                 # True on a live bot = FAKE quotes
            "kevin_dm": bool(config.KEVIN_USER_ID),          # False = no admin commands / alerts
            # Standing conditions: true until someone fixes them. They are alerted once, at
            # startup, so last_error alone would let them age out of sight — the canary reads these.
            "data_dir_default": config.data_dir_in_container(),  # True = state lives in the container
            "psa_folder_blank": config.psa_folder_blank(),   # True = v1-folder safety check skipped
            "unreadable_stores": sorted(state.load_failures()),  # failing to read right now
            "stuck_stores": sorted(state.stuck()),           # empty in memory until a RESTART
            "unsaved_stores": sorted(state.save_failures()),  # most recent save failed
            "data_dir_writable": state.write_ok(),           # False = the last write attempt failed
        },
    }))


async def _analytics(req):
    """Volume + payout aggregates for the portal's analytics view. AUTHENTICATED with
    x-bot-key. VOLUME + PAYOUT ONLY — margin stays portal-side. Optional ?days=N window."""
    if not _authed(req):
        return _unauthorized()
    from . import analytics
    try:
        days = int(req.query["days"]) if req.query.get("days") else None
        if days is not None:
            days = max(1, min(days, 3650))           # clamp: a huge ?days would OverflowError in timedelta
    except (ValueError, OverflowError):
        days = None
    return _cors(web.json_response(analytics.summary(days)))


async def _delivery(req):
    """Delivery webhook SINK (provider-agnostic). AUTHENTICATED with x-bot-key. The
    portal / carrier aggregator (EasyPost · Shippo · AfterShip) POSTs here when a
    tracked package is delivered; the bot flips the ticket to delivered and — ONLY if
    config.DELIVERY_TRACKING is on — posts a customer-safe 'arrived!' note in that
    ticket + DMs Kevin. Body: {channel_id? | tracking?, status? (out_for_delivery |
    delivered — a missing/blank/unknown status is acked + ignored, never defaulted to
    delivered), delivered_at?, out_for_delivery_at?, carrier?};
    resolves by channel_id when that id has a ticket, else by tracking number (so the
    channel_id in the reply is the RESOLVED ticket's). status 'out_for_delivery' posts a
    customer-safe 'out for delivery today' heads-up (gated, no Kevin DM); 'delivered'
    posts the 'arrived' note + DMs Kevin; any other status is acked + ignored. The bot
    holds NO carrier credentials and makes no outbound carrier calls. Always 200 on the
    delivered path (even when no ticket matches) so a webhook can't retry-storm."""
    if not _authed(req):
        return _unauthorized()
    try:
        body = await req.json()
    except Exception:
        return web.json_response({"error": "bad json"}, status=400)
    if not isinstance(body, dict):                   # valid JSON but not an object (e.g. [] / null)
        return web.json_response({"error": "body must be a JSON object"}, status=400)
    from . import tracking
    # Fail CLOSED on a missing/blank status (and tolerate hyphens): a status-less webhook
    # must NOT default to the terminal 'delivered' (that would fire a false customer
    # 'arrived' post); unknown statuses fall through to ack+ignore below.
    status = str(body.get("status") or "").lower().replace(" ", "_").replace("-", "_")
    channel_id = body.get("channel_id")
    tracking_num = body.get("tracking") or body.get("tracking_num") or body.get("trackingNumber")
    if tracking_num is not None:                      # a carrier may serialize an all-digit number as a JSON int
        tracking_num = str(tracking_num).strip() or None
    if channel_id is None and not tracking_num:
        return web.json_response({"error": "need channel_id or tracking"}, status=400)

    if status == "out_for_delivery":                # intermediate: the 'on the truck today' heads-up
        try:
            res = tracking.mark_out_for_delivery(channel_id=channel_id, tracking_num=tracking_num,
                                                 at=body.get("out_for_delivery_at"))
        except Exception as e:                       # never 500 → a webhook would retry-storm
            print(f"[web] out-for-delivery mark failed (non-fatal): {e}")
            return _cors(web.json_response({"ok": True, "stage": "out_for_delivery", "error": "mark_failed"}))
        posted = False
        if res.get("found") and not res.get("already"):
            try:
                from . import bot as botmod
                posted = await botmod.notify_out_for_delivery(res["channel_id"], res.get("user", ""))
            except Exception as e:
                print(f"[web] out-for-delivery notify failed (non-fatal): {e}")
        return _json(
            {**res, "stage": "out_for_delivery", "posted": posted,
             "messaging_enabled": config.DELIVERY_TRACKING})

    if status != "delivered":                       # only out_for_delivery + delivered act; ack the rest
        return _cors(web.json_response({"ok": True, "ignored": status}))
    try:
        res = tracking.mark_delivered(channel_id=channel_id, tracking_num=tracking_num,
                                      delivered_at=body.get("delivered_at"),
                                      carrier=body.get("carrier"))
    except Exception as e:                           # never 500 → a webhook would retry-storm
        print(f"[web] delivery mark failed (non-fatal): {e}")
        return _cors(web.json_response({"ok": True, "stage": "delivered", "error": "mark_failed"}))
    posted = False
    if res.get("found") and not res.get("already"):   # notify_delivered gates the customer post itself
        try:
            from . import bot as botmod
            posted = await botmod.notify_delivered(res["channel_id"], res.get("user", ""))
        except Exception as e:
            print(f"[web] delivery notify failed (non-fatal): {e}")
    return _json(
        {**res, "stage": "delivered", "posted": posted, "messaging_enabled": config.DELIVERY_TRACKING})


async def _void(req):
    """Void-from-app: the authenticated HTTP twin of the Kevin-only !void command, so the
    portal Ops board can trigger a TRUE cancel (not just a dismiss). AUTHENTICATED with
    x-bot-key (fails closed when BOT_API_KEY unset). Body: {user | channel_id, force?}.
    Removes the ticket row (ledger/tracking/delivery/review), any owed row, the
    channel_sheet cache, the in-memory lot + _last_quote, and best-effort trashes the
    Drive buying sheet (recoverable). Idempotent; 'quoted' voids freely, 'accepted'/'paid'
    need force. WHO may call is the portal's responsibility; this endpoint only checks the
    bot key. Contract: portal-interface/portal-calls-bot.md § POST /void (code wins on drift)."""
    if not _authed(req):
        return _unauthorized()
    try:
        body = await req.json()
    except Exception:
        return web.json_response({"error": "bad json"}, status=400)
    if not isinstance(body, dict):
        return web.json_response({"error": "body must be a JSON object"}, status=400)
    import asyncio
    from . import tracking, lots, sheets, bot as botmod
    # STRICT force: only a real JSON boolean true forces. A non-boolean (e.g. the string
    # "false", which is truthy via bool()) is rejected so it can never authorize a destructive
    # void of a real balance.
    if "force" in body and not isinstance(body["force"], bool):
        return web.json_response({"error": "force must be a boolean"}, status=400)
    force = body.get("force") is True
    raw_cid = body.get("channel_id")
    user = body.get("user")
    # EXACTLY ONE target.
    if (raw_cid is not None) == bool(user):
        return web.json_response({"error": "provide exactly one of user or channel_id"}, status=400)
    # Resolve the target → a channel_id (mirrors the !void command's resolution).
    cid = None
    if raw_cid is not None:
        # channel_id must be a STRING snowflake (a JSON number loses 64-bit precision in JS).
        cid = tracking.parse_channel_id(raw_cid) if isinstance(raw_cid, str) else None
        if cid is None:
            return web.json_response({"error": "channel_id must be a numeric string"}, status=400)
    else:
        uname = str(user).strip().lstrip("@").strip()
        if not uname:                    # " " / "@" — never let a blank name match every ticket
            return web.json_response({"error": "user must be a non-empty username"}, status=400)
        quoted = tracking.find_tickets(user=uname, status="quoted")
        if len(quoted) == 1:
            cid = quoted[0].get("channel_id")
        elif not quoted:
            # Same keys as the channel_id not_found below, so a strict decoder written against
            # the contract doesn't choke on this path (a `user` lookup never purges leftovers).
            return _json({"voided": False, "reason": "not_found", "residual_cleaned": False,
                          "ship_blocked": False, "had_sheet": False, "sheet_trashed": False,
                          "other_sheet_urls": []})
        else:
            return _json({"voided": False, "reason": "ambiguous",
                          "matches": [{"channel_id": str(r.get("channel_id")),
                                       "channel_name": r.get("channel_name", ""),
                                       "cards": r.get("cards", 0)} for r in quoted]})
    # Status / force gate — applies ONLY when a ticket actually exists (a real balance needs
    # force). With no ticket we still fall through to the idempotent purge below, so a repeat
    # call after a partial void clears residual owed/sheet-cache instead of stalling at not_found.
    rows = tracking.find_tickets(channel_id=cid)
    if rows and rows[0].get("status", "quoted") in ("accepted", "paid") and not force:
        return _json({"voided": False, "reason": "needs_force", "status": rows[0].get("status", "quoted")})
    # Idempotent full purge (runs even with no ticket → clears residual). Mirrors the !void
    # caller: when the quote is (now) dead, or there was no ticket at all, clear the in-memory
    # lot + last-quote + sheet cache and best-effort trash the Drive sheet. When the void
    # BAILED on a storage write with the ticket still live (found, not removed), touch nothing —
    # the lot and the sheet still belong to a live ticket.
    res = tracking.void_ticket(cid)
    vcid = res.get("channel_id", cid)
    removed = bool(res.get("ticket_removed"))
    bailed = bool(res.get("found")) and not removed
    sheet_url, sheet_trashed, had_mem, mem_sheet, by_hand, others = "", False, False, "", [], []
    if not bailed:
        try:                                 # the in-memory cache normally mirrors the disk copy
            mem_sheet = (botmod._channel_sheet.pop(str(vcid), None)
                         or botmod._channel_sheet.pop(str(cid), None) or "")
            botmod._channel_sheet.pop(str(cid), None)
        except Exception:
            pass
        had_mem = bool(lots.drop(vcid)) | bool(lots.drop(cid))
        had_mem |= botmod._last_quote.pop(vcid, None) is not None
        had_mem |= botmod._last_quote.pop(cid, None) is not None
        # The automatic trash acts on the PERSISTED pointer only (what void_ticket returned). A
        # sheet only the in-memory cache knew (its cache-file write had failed) is reported for
        # trashing by hand, never trashed from here.
        sheet_url = res.get("sheet_url") or ""
        if sheet_url:
            try:
                sheet_trashed = bool(await asyncio.to_thread(sheets.trash_sheet, sheet_url))
            except Exception as e:
                print(f"[web] /void sheet trash failed (non-fatal): {e}")
                sheet_trashed = False
            if not sheet_trashed:
                by_hand.append(sheet_url)
        if mem_sheet and mem_sheet != sheet_url:
            by_hand.append(mem_sheet)
        # Every cert message builds its own sheet; only the latest is trashed automatically.
        others = [u for u in (res.get("other_sheet_urls") or []) if u not in by_hand]
        if by_hand or others:
            # The cache entry is already purged, so this response and Kevin's DM are the only
            # places the links still exist — hand them to both (mirrors !void's "do it by hand").
            # ping_kevin, not alert_kevin: this is a to-do for Kevin, not a bot fault for /status.
            lines = [f"⚠️ Void from the app (channel `{vcid}`, {res.get('user') or '?'}): the quote is dead, but:"]
            if by_hand:
                lines.append("• its buying sheet was NOT auto-trashed — do it by hand: " + " , ".join(by_hand))
            if others:
                lines.append(f"• {len(others)} OTHER buying sheet(s) are on record for this ticket and were NOT "
                             f"auto-trashed (only the latest is) — trash them by hand if they're dead too; "
                             f"ignore any you already removed:")
                lines += [f"   {u}" for u in others]
            try:
                await botmod.ping_kevin("\n".join(lines))
            except Exception as e:
                print(f"[web] /void sheet DM failed (non-fatal): {e}")
    # had_sheet tells "trash failed" apart from "there was no sheet"; sheet_url is the first
    # sheet that still needs trashing by hand (absent when none does).
    # other_sheet_urls = the other sheets on record for the ticket (never auto-trashed); a link
    # already given as sheet_url is not repeated here.
    # (Every link that still needs a hand is returned exactly once: the first as sheet_url, any
    # further one — e.g. a memory-only sheet when the latest sheet's trash also failed — here.)
    returned = by_hand[0] if by_hand else ""
    listed = [] if bailed else list(dict.fromkeys(by_hand[1:] + list(res.get("other_sheet_urls") or [])))
    sheet_info = {"had_sheet": bool(sheet_url or mem_sheet), "sheet_trashed": sheet_trashed,
                  "other_sheet_urls": [u for u in listed if u != returned]}
    if by_hand:
        sheet_info["sheet_url"] = by_hand[0]
    if removed:
        return _json({"voided": True, "channel_id": str(vcid), "user": res.get("user", ""),
                      "status": res.get("status", "quoted"), "cards": res.get("cards", 0),
                      "payout": round(float(res.get("payout", 0) or 0), 2),
                      "had_owed": bool(res.get("had_owed")), **sheet_info,
                      "cache_cleanup_pending": not res.get("ok", True)})
    if bailed:
        # an owed/ticket/marker write failed with the ticket intact → nothing cleaned; retry.
        return _json({"voided": False, "reason": "persist_failed", "channel_id": str(vcid),
                      "ticket_intact": True, "owed_removed": bool(res.get("owed_removed")),
                      "ship_blocked": bool(tracking.is_voided(vcid))})
    residual = bool(res.get("sheet_url") or res.get("had_owed") or res.get("other_sheet_urls")
                    or had_mem or mem_sheet)
    if not res.get("ok", True):
        # the residual purge itself didn't persist → safe to retry.
        return _json({"voided": False, "reason": "persist_failed", "channel_id": str(vcid),
                      "ticket_intact": False, **sheet_info})
    # No ticket. residual_cleaned=True means this call DID remove leftover state from an
    # earlier partial void (so "not_found" isn't mistaken for "nothing happened").
    # If what it removed was a quote that only existed in memory (its ticket never saved), set
    # the cancel guard void_ticket had no ticket to set it on; ship_blocked reports the result.
    if had_mem or mem_sheet:
        tracking.mark_cancelled(vcid)
    return _json({"voided": False, "reason": "not_found", "residual_cleaned": residual,
                  "ship_blocked": bool(tracking.is_voided(vcid)), **sheet_info})


async def start():
    """Start the web server on $PORT. Returns the AppRunner (keep a reference)."""
    app = web.Application()
    app.add_routes([
        web.get("/", _health),
        web.get("/health", _health),
        web.get("/ops", _ops),
        web.get("/status", _status),
        web.get("/analytics", _analytics),
        web.post("/delivery", _delivery),
        web.post("/void", _void),
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", config.WEB_PORT).start()
    print(f"[web] listening on 0.0.0.0:{config.WEB_PORT}")
    return runner
