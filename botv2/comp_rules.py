"""
comp_rules — per-sport buying rules for comp-driven (CardLadder direct) pricing.

Loaded from {DATA_DIR}/comp_rules.json and editable via GET/POST /admin.
Provides rate, price range, grade, and sale-recency gates per sport + CGC.
"""
from __future__ import annotations

import threading
from typing import Optional

from . import state

_LOCK = threading.Lock()
_CACHE: Optional[dict] = None

SPORTS = ['pokemon', 'basketball', 'baseball', 'football', 'soccer', 'hockey', 'other']

SPORT_LABELS = {
    'pokemon': 'Pokémon',
    'basketball': 'Basketball',
    'baseball': 'Baseball',
    'football': 'Football',
    'soccer': 'Soccer',
    'hockey': 'Hockey',
    'other': 'Other',
}

DEFAULTS: dict = {
    'sports': {
        'pokemon':    {'rate': 0.87, 'minValue': 1.0,  'maxValue': 200.0,  'minGrade': 7, 'maxSaleAgeDays': 30, 'minConf': 0},
        'basketball': {'rate': 0.85, 'minValue': 1.0,  'maxValue': 300.0,  'minGrade': 7, 'maxSaleAgeDays': 30, 'minConf': 0},
        'baseball':   {'rate': 0.85, 'minValue': 1.0,  'maxValue': 250.0,  'minGrade': 7, 'maxSaleAgeDays': 30, 'minConf': 0},
        'football':   {'rate': 0.85, 'minValue': 1.0,  'maxValue': 250.0,  'minGrade': 7, 'maxSaleAgeDays': 30, 'minConf': 0},
        'soccer':     {'rate': 0.85, 'minValue': 1.0,  'maxValue': 250.0,  'minGrade': 7, 'maxSaleAgeDays': 30, 'minConf': 0},
        'hockey':     {'rate': 0.85, 'minValue': 1.0,  'maxValue': 250.0,  'minGrade': 7, 'maxSaleAgeDays': 30, 'minConf': 0},
        'other':      {'rate': 0.85, 'minValue': 1.0,  'maxValue': 250.0,  'minGrade': 7, 'maxSaleAgeDays': 30, 'minConf': 0},
    },
    'cgc': {'rate': 0.80, 'minValue': 1.0, 'maxValue': 500.0, 'maxSaleAgeDays': 30, 'minConf': 0},
}


def load() -> dict:
    global _CACHE
    with _LOCK:
        if _CACHE is None:
            raw = state.load('comp_rules', {})
            _CACHE = _merge_defaults(raw)
        return dict(_CACHE)


def save(rules: dict) -> bool:
    global _CACHE
    merged = _merge_defaults(rules)
    ok = state.save('comp_rules', merged)
    if ok:
        with _LOCK:
            _CACHE = merged
    return ok


def reset_cache():
    """Force reload from disk on next access (for tests)."""
    global _CACHE
    with _LOCK:
        _CACHE = None


def _merge_defaults(raw: dict) -> dict:
    """Merge saved rules over DEFAULTS so new sports/fields auto-appear."""
    if not isinstance(raw, dict):
        raw = {}
    result: dict = {'sports': {}, 'cgc': {}}
    cgc_raw = raw.get('cgc') if isinstance(raw.get('cgc'), dict) else {}
    result['cgc'] = {**DEFAULTS['cgc'], **cgc_raw}
    sports_raw = raw.get('sports') if isinstance(raw.get('sports'), dict) else {}
    for sport in SPORTS:
        sport_raw = sports_raw.get(sport) if isinstance(sports_raw.get(sport), dict) else {}
        result['sports'][sport] = {**DEFAULTS['sports'][sport], **sport_raw}
    return result


def get_sport_rules(sport: str) -> dict:
    rules = load()
    s = (sport or 'other').lower().strip()
    return rules['sports'].get(s, rules['sports']['other'])


def get_cgc_rules() -> dict:
    return load()['cgc']
