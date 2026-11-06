"""
webserver — authenticated JSON endpoints for the portal + monitoring, plus /health.

Binds $PORT so Railway's health-check passes (the bot holds the port). Public:
/health only. Authenticated (x-bot-key, fails closed): /ops (portal ops dashboard),
/status (monitoring + flags), /analytics (volume/payout), /delivery (carrier webhook
sink). The old v1 /owed /tickets /leaderboard /links reads were removed — their only
consumer was a retired local tracker, and the portal reads /ops instead.
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


def _authed(req) -> bool:
    """x-bot-key gate, shared by every non-public endpoint. Fails CLOSED when BOT_API_KEY
    is unset (no key configured → deny everyone) so an endpoint can never be accidentally
    public. Only /health is intentionally unauthenticated (Railway health-check + canary)."""
    return bool(config.BOT_API_KEY) and req.headers.get("x-bot-key") == config.BOT_API_KEY


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
    resolves by channel_id, else by tracking number. status 'out_for_delivery' posts a
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
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", config.WEB_PORT).start()
    print(f"[web] listening on 0.0.0.0:{config.WEB_PORT}")
    return runner
