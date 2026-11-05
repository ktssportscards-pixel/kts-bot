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
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", config.WEB_PORT).start()
    print(f"[web] listening on 0.0.0.0:{config.WEB_PORT}")
    return runner
