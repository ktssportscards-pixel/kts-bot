"""
canary — a live smoke test of the deployed bot. Catches a deploy that builds but is
broken at runtime, a boot-time fault, or a broken pricing path, BEFORE a customer does.

    python -m botv2.canary

Env:
  BOT_BASE_URL  the bot's web host (default: prod worker on Railway)
  BOT_API_KEY   x-bot-key — enables the authed /status + the pricing check
  CANARY_CERT   a known PSA/CGC cert number to price-check (e.g. a sports slab)

Exit 0 = all green · 1 = a check failed (printed). SIDE-EFFECT-FREE: /bot/price is a
read; the canary never claims, registers, or writes anything.
"""
import json
import os
import sys
import urllib.error
import urllib.request

BOT = os.environ.get("BOT_BASE_URL", "https://worker-production-1175.up.railway.app").rstrip("/")
KEY = os.environ.get("BOT_API_KEY", "")
CERT = os.environ.get("CANARY_CERT", "")

_fails = []


def check(cond, msg):
    print(("  OK  " if cond else "  !!  ") + msg)
    if not cond:
        _fails.append(msg)


def _get(path, key=None, timeout=25):
    req = urllib.request.Request(BOT + path)
    if key:
        req.add_header("x-bot-key", key)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def _price_check():
    """The core function: price a known cert through the bot's own client against the
    LIVE portal (not the mock) and confirm a clean, valid decision comes back. Reuses
    the exact PricedRow parsing the bot uses, so this tests the real quote path."""
    os.environ.pop("BOTV2_PRICE_MOCK", None)
    import asyncio

    from . import config, portal_client
    config.BOT_API_KEY, config.PRICE_MOCK = KEY, False
    rows = asyncio.run(portal_client.price_certs([CERT]))
    check(len(rows) == 1, f"/bot/price: exactly one row (got {len(rows)})")
    row = rows[0] if rows else None
    check(bool(row) and not row.error,
          "/bot/price: no row error" + (f" — {row.error}" if row and row.error else ""))
    check(bool(row) and row.decision.upper() in ("BUY", "PASS"),
          f"/bot/price: decision BUY/PASS (got {getattr(row, 'decision', '?')!r})")
    if row and row.is_buy:
        check(isinstance(row.offer, (int, float)) and row.offer > 0,
              f"/bot/price: a BUY carries a positive offer (got {row.offer})")


def main():
    print(f"canary → {BOT}")
    try:
        s, b = _get("/health")
        check(s == 200 and "botv2" in b, f"/health 200 + botv2 banner (got {s})")
    except Exception as e:
        check(False, f"/health reachable — {e}")

    if KEY:
        try:
            s, b = _get("/status", KEY)
            d = json.loads(b) if s == 200 else {}
            check(s == 200 and d.get("ok") is True, f"/status ok (got {s})")
            le = d.get("last_error")
            check(le is None, "/status: no last_error" + (f" — {le.get('key')}" if le else ""))
        except Exception as e:
            check(False, f"/status — {e}")
        if CERT:
            try:
                _price_check()
            except Exception as e:
                check(False, f"/bot/price — {e}")
        else:
            print("  ··  CANARY_CERT unset — skipping the pricing check")
    else:
        print("  ··  BOT_API_KEY unset — skipping authed /status + pricing checks")

    if _fails:
        print(f"\nCANARY FAILED ({len(_fails)}):")
        for f in _fails:
            print("   -", f)
        sys.exit(1)
    print("\nCANARY OK")


if __name__ == "__main__":
    main()
