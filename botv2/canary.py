"""
canary — a live smoke test of the deployed bot. Catches a deploy that builds but is
broken at runtime, a boot-time fault, or a broken pricing path, BEFORE a customer does.

    python -m botv2.canary

Env:
  BOT_BASE_URL  the bot's web host (default: prod worker on Railway)
  BOT_API_KEY   x-bot-key — enables the authed /status + the pricing check
  CANARY_CERT   a known PSA/CGC cert number to price-check (e.g. a sports slab)
  CANARY_ERROR_MAX_AGE_MIN  how old /status's last_error may be and still fail the
                run (default 150 = two hourly runs plus slack, so a fault is seen by at
                least two scheduled runs even if one starts late or is dropped). The bot
                never clears last_error, so without a window one transient alert kept every
                hourly run red until the next redeploy — which would hide any later, real
                breakage behind a permanently red check.

Exit 0 = all green · 1 = a check failed (printed). SIDE-EFFECT-FREE: /bot/price is a
read; the canary never claims, registers, or writes anything.
"""
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

BOT = os.environ.get("BOT_BASE_URL", "https://worker-production-1175.up.railway.app").rstrip("/")
KEY = os.environ.get("BOT_API_KEY", "")
CERT = os.environ.get("CANARY_CERT", "")
try:
    ERROR_MAX_AGE_MIN = float(os.environ.get("CANARY_ERROR_MAX_AGE_MIN") or 150)
except ValueError:
    ERROR_MAX_AGE_MIN = 150.0

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


def error_age_minutes(last_error, now=None):
    """Minutes since /status's last_error was recorded, or None when it carries no usable
    timestamp (the caller then treats it as fresh — fail closed)."""
    try:
        at = datetime.fromisoformat(str((last_error or {}).get("at")).replace("Z", "+00:00"))
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        return ((now or datetime.now(timezone.utc)) - at).total_seconds() / 60.0
    except Exception:
        return None


def is_fresh_error(last_error, now=None, max_age_min=None) -> bool:
    """True if `last_error` should fail the canary: it exists and is recent (or undatable)."""
    if not last_error:
        return False
    age = error_age_minutes(last_error, now)
    limit = ERROR_MAX_AGE_MIN if max_age_min is None else max_age_min
    return age is None or age <= limit


def standing_problems(flags) -> list:
    """Conditions /status reports that stay true until someone fixes them. The bot alerts
    each of them once, at startup — so judging only by last_error's age would turn the canary
    green on an unfixed fault 150 minutes after boot. A bot older than a flag reports nothing
    for it, which is not a problem."""
    f = flags if isinstance(flags, dict) else {}
    out = []
    if f.get("price_mock") is True:
        out.append("mock pricing is ON — customers are being quoted FAKE prices")
    if f.get("data_dir_default") is True:
        out.append("DATA_DIR is not set — tickets / owed balances live in the container and are "
                   "wiped by every redeploy")
    if f.get("psa_folder_blank") is True:
        out.append("PSA_FOLDER_ID is blank — the 'never write into v1's folder' check is skipped")
    stores = f.get("unreadable_stores")
    if isinstance(stores, list) and stores:
        out.append("data file(s) could not be read: " + ", ".join(str(s) for s in stores[:10]))
    return out


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
            fresh = is_fresh_error(le)
            check(not fresh, f"/status: no last_error in the last {ERROR_MAX_AGE_MIN:g} min"
                  + (f" — {le.get('key')} at {le.get('at')}" if fresh and isinstance(le, dict) else ""))
            if le and not fresh:
                print(f"  ··  older last_error on record ({le.get('key')} at {le.get('at')}) — not failing on it")
            # Standing misconfiguration (mock pricing on a live bot, state kept in the container,
            # an unreadable data file…): red until fixed, however long ago the bot booted.
            standing = standing_problems(d.get("flags"))
            check(not standing, "/status: no standing problem" + (" — " + "; ".join(standing) if standing else ""))
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
