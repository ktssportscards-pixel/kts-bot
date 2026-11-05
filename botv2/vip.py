"""
vip — RARE manual bot-side rate override.

The source of truth for who's VIP is Kevin's **KTS Portal VIP section** — the bot
passes the customer's Discord username to /bot/price and the portal applies that
seller's VIP rates itself (and flags each row vip=True). So the two stay in sync
with zero bot-side list to maintain, and all offer math stays in offer.js.

This module is only a fallback for a ONE-OFF VIP who isn't a portal seller: a local
override map, merged in as /bot/price's optional `rateOverride`. Normally empty.

Store shape (keyed by lowercased Discord username):
  {"someuser": {"basketball": 97, "pokemon": [{"min":0,"max":100,"rate":93}]}}
each category → a flat percent OR [{min,max,rate}] bands (the portal's rateOverride).
"""
from . import state

_STORE = "vip_rates"
_vip: dict = state.load(_STORE, dict)


def override_for(username: str):
    """The rateOverride map for a customer, or None for public rates."""
    if not username:
        return None
    return _vip.get(username.strip().lower()) or None


def is_vip(username: str) -> bool:
    return override_for(username) is not None


def set_override(username: str, override: dict) -> None:
    _vip[username.strip().lower()] = override
    state.save(_STORE, _vip)


def clear_override(username: str) -> bool:
    removed = _vip.pop(username.strip().lower(), None) is not None
    if removed:
        state.save(_STORE, _vip)
    return removed


def all_vips() -> dict:
    return dict(_vip)
