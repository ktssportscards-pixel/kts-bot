"""alerts — in-memory rate limiter + last-error record for Kevin-facing failure
alerts.

A failure should reach Kevin as a DM, not just the container logs — but an error
storm (the portal goes down, every quote fails) must not flood his DMs. So an
alert is keyed, and the same key fires at most once per window. Pure/leaf module
(no bot or discord import), so the web server can read last_error() for /status
without a circular import. State is in-memory and resets on redeploy, which is
fine: it only gates how often an alert repeats, never whether a fault is handled.
"""
import time
from datetime import datetime, timezone

ALERT_WINDOW_S = 900           # 15 min: suppress an identical (keyed) alert within this
_last_sent: dict = {}          # error-key -> monotonic timestamp of its last alert
_last_error: dict = None       # the most recent failure, for /status


def should_alert(key: str, now: float = None) -> bool:
    """True if an alert for `key` hasn't fired within ALERT_WINDOW_S (and records it
    firing now). False suppresses a duplicate. `now` (monotonic seconds) is injectable
    for tests; in production it defaults to time.monotonic()."""
    t = time.monotonic() if now is None else now
    last = _last_sent.get(key)
    if last is not None and (t - last) < ALERT_WINDOW_S:
        return False
    _last_sent[key] = t
    return True


def record_error(key: str, text: str) -> None:
    """Record the most recent failure (shown by /status), regardless of whether the
    alert itself was rate-limited — so /status always reflects the latest fault."""
    global _last_error
    _last_error = {"key": key, "text": (text or "")[:300],
                   "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}


def last_error() -> dict:
    return _last_error


def reset() -> None:
    """Clear all state (tests)."""
    global _last_error
    _last_sent.clear()
    _last_error = None
