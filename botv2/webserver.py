"""
webserver — tracker sync (/owed /tickets /leaderboard /links) + /health.

Binds $PORT so Railway's `web` process health-check passes (the bot must hold the
port). Serves JSON from the state stores, which the payments / ship-accept flow
populate. Read-only; CORS wide open (the local ticket/owed tracker is the only
consumer). Ported from v1's owed web server.
"""
import datetime

import aiohttp.web as web

from . import config, state

# Captured at process start, so /health reveals which deploy is live: a recent
# timestamp = a fresh redeploy (lets a deploy be verified from outside).
_STARTED = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


async def _health(_req):
    return _cors(web.Response(
        text=f"KTS botv2 — owed/tickets endpoint OK · started {_STARTED}"))


async def _owed(_req):
    return _cors(web.json_response(state.load("owed", list)))


async def _tickets(_req):
    return _cors(web.json_response(state.load("tickets", list)))


async def _leaderboard(_req):
    return _cors(web.json_response(state.load("leaderboard", dict)))


async def _links(_req):
    return _cors(web.json_response(state.load("supplier_links", dict)))


async def _ops(req):
    """Consolidated per-ticket lifecycle (quote → claim → ship → owed) for the portal's
    ops dashboard. AUTHENTICATED with x-bot-key — unlike the local-tracker reads above,
    this is the full picture and must not be public."""
    if not config.BOT_API_KEY or req.headers.get("x-bot-key") != config.BOT_API_KEY:
        return web.json_response({"error": "unauthorized"}, status=401)
    from . import tracking
    detail = req.query.get("cards", "").lower() in ("1", "true", "yes")   # ?cards=1 → per-cert
    return _cors(web.json_response({"tickets": tracking.ops_view(detail=detail)}))


async def _status(req):
    """Machine-readable health for monitoring: uptime (from the process-start stamp),
    live store counts (open tickets / unpaid owed), and the last failure the bot saw.
    AUTHENTICATED with x-bot-key (same gate as /ops) — it exposes internal counts, so
    it must not be public. Additive: /health stays the public liveness check."""
    if not config.BOT_API_KEY or req.headers.get("x-bot-key") != config.BOT_API_KEY:
        return web.json_response({"error": "unauthorized"}, status=401)
    from . import tracking, alerts
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
    }))


async def _analytics(req):
    """Volume + payout aggregates for the portal's analytics view. AUTHENTICATED with
    x-bot-key. VOLUME + PAYOUT ONLY — margin stays portal-side. Optional ?days=N window."""
    if not config.BOT_API_KEY or req.headers.get("x-bot-key") != config.BOT_API_KEY:
        return web.json_response({"error": "unauthorized"}, status=401)
    from . import analytics
    try:
        days = int(req.query["days"]) if req.query.get("days") else None
    except ValueError:
        days = None
    return _cors(web.json_response(analytics.summary(days)))


async def _delivery(req):
    """Delivery webhook SINK (provider-agnostic). AUTHENTICATED with x-bot-key. The
    portal / carrier aggregator (EasyPost · Shippo · AfterShip) POSTs here when a
    tracked package is delivered; the bot flips the ticket to delivered and — ONLY if
    config.DELIVERY_TRACKING is on — posts a customer-safe 'arrived!' note in that
    ticket + DMs Kevin. Body: {channel_id? | tracking?, status?='delivered',
    delivered_at?, carrier?}; resolves by channel_id, else by tracking number. The bot
    holds NO carrier credentials and makes no outbound carrier calls. Always 200 on the
    delivered path (even when no ticket matches) so a webhook can't retry-storm."""
    if not config.BOT_API_KEY or req.headers.get("x-bot-key") != config.BOT_API_KEY:
        return web.json_response({"error": "unauthorized"}, status=401)
    try:
        body = await req.json()
    except Exception:
        return web.json_response({"error": "bad json"}, status=400)
    from . import tracking
    status = str(body.get("status", "delivered")).lower()
    channel_id = body.get("channel_id")
    tracking_num = body.get("tracking") or body.get("tracking_num") or body.get("trackingNumber")
    if channel_id is None and not tracking_num:
        return web.json_response({"error": "need channel_id or tracking"}, status=400)
    if status != "delivered":                       # only 'delivered' acts; ack the rest
        return _cors(web.json_response({"ok": True, "ignored": status}))
    res = tracking.mark_delivered(channel_id=channel_id, tracking_num=tracking_num,
                                  delivered_at=body.get("delivered_at"),
                                  carrier=body.get("carrier"))
    posted = False
    if res.get("found") and not res.get("already"):   # notify_delivered gates the customer post itself
        try:
            from . import bot as botmod
            posted = await botmod.notify_delivered(res["channel_id"], res.get("user", ""))
        except Exception as e:
            print(f"[web] delivery notify failed (non-fatal): {e}")
    return _cors(web.json_response(
        {**res, "posted": posted, "messaging_enabled": config.DELIVERY_TRACKING}))


async def start():
    """Start the web server on $PORT. Returns the AppRunner (keep a reference)."""
    app = web.Application()
    app.add_routes([
        web.get("/", _health),
        web.get("/health", _health),
        web.get("/owed", _owed),
        web.get("/tickets", _tickets),
        web.get("/leaderboard", _leaderboard),
        web.get("/links", _links),
        web.get("/ops", _ops),
        web.get("/status", _status),
        web.get("/analytics", _analytics),
        web.post("/delivery", _delivery),
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", config.WEB_PORT).start()
    print(f"[web] listening on 0.0.0.0:{config.WEB_PORT}")
    return runner
