"""
vip — per-customer rate overrides.

A VIP customer's rates are applied by the PORTAL, not the bot: the bot just looks
up the customer's override map and passes it to /bot/price, which runs it through
offer.js's own {rateOverrides} path (capped at 100%). So all offer math stays in
offer.js and the bot never prices — v2 reintroduces zero rate logic.

Store shape (keyed by lowercased Discord username):
  {"someuser": {"basketball": 97, "pokemon": [{"min":0,"max":100,"rate":93}]}}
where each category maps to a flat percent OR a list of {min,max,rate} bands —
exactly the portal's rateOverride contract.
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
