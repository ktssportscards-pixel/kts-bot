"""
portal_pricing.py — mirror of the KTS Portal buying engine for the Discord bot.
================================================================================
SELF-CONTAINED. Nothing in the live bot imports this yet — it's staged on the
`portal-rules-mirror` branch so the bot's current pricing path is untouched.

What it is: a faithful re-implementation of the portal's offer.js DECISION logic
(rate tiers, value ranges/gaps, grade & cert gates) driven by the portal's own
rules board (rules.json, mirrored from portal.ktscollectibles.com). The portal is
the source of truth; re-mirror by replacing rules.json and re-running verify_parity.

What it is NOT: it does not fetch prices. The bot feeds its OWN CardLadder comp
(clValue/grade/sport/cert) into evaluate(). We never call Triumph's scanner API.

Two things this cannot reproduce (documented divergences, both disclaimed to the
seller on sports/One Piece):
  1. CardLadder CONFIDENCE (portal minConf 1-5). The bot's comp source returns no
     CL confidence score, so minConf is NOT enforced here (quotes in-range instead).
  2. Triumph LIVE ALLOCATION. Sports + One Piece are Triumph-routed in the portal;
     takeability comes from Triumph in real time. The bot can't ask, so it quotes
     the rate and flags that final acceptance depends on live allocation.

Offer math matches offer.js exactly: offer = round_half_up(value * rate_pct) / 100.
"""
import json
import os
import re

_HERE = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(_HERE, 'rules.json')) as _f:
    RULES = json.load(_f)

# Bot's CardLadder sport strings -> portal category keys.
BOT_SPORT_TO_CAT = {
    'pokemon': 'pokemon',
    'one piece': 'onepiece', 'onepiece': 'onepiece',
    'basketball': 'basketball', 'nba': 'basketball',
    'mlb': 'baseball', 'baseball': 'baseball',
    'football': 'football', 'nfl': 'football',
}
# Categories the portal routes to Triumph (live-allocation takeability we can't see).
TRIUMPH_CATS = {'basketball', 'baseball', 'football', 'onepiece'}
CATEGORY_LABELS = {'pokemon': 'Pokémon', 'onepiece': 'One Piece',
                   'basketball': 'Basketball', 'baseball': 'Baseball', 'football': 'Football'}

ALLOCATION_DISCLAIMER = ("final acceptance depends on live allocation — may be full "
                         "even though it's in range")


def _num(x):
    try:
        if x is None or x == '':
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _grade_num(g):
    return _num(re.sub(r'[^0-9.]', '', str(g if g is not None else '')))


def _round_half_up_cents(value, rate_pct):
    # Mirrors JS Math.round(value * rate) / 100 for value*rate >= 0.
    return int(value * rate_pct + 0.5) / 100.0


def evaluate(comp, rules=RULES):
    """Decide one CardLadder comp against the portal rules board.
    comp: {clValue, grade, sport, cert}. Returns a dict:
      {decision: 'accept'|'reject', offer: float|None, rate: int|None,
       category, categoryLabel, band: (min,max)|None, disclaimer: str, reason: str}
    """
    raw_sport = str(comp.get('sport') or '').lower().strip()
    cat = BOT_SPORT_TO_CAT.get(raw_sport)
    v = _num(comp.get('clValue'))
    cert_raw = str(comp.get('cert') or '').strip()
    cert_digits = re.sub(r'\D', '', cert_raw)
    out = {'decision': 'reject', 'offer': None, 'rate': None,
           'category': cat, 'categoryLabel': CATEGORY_LABELS.get(cat, 'Unknown'),
           'band': None, 'disclaimer': '', 'reason': ''}

    if cat is None:
        out['reason'] = f"{raw_sport or 'unknown'} isn't a category we price here"
        return out
    if v is None:
        out['reason'] = 'no CardLadder value'
        return out

    track = rules.get('tracks', {}).get(cat + ':PSA')
    if not track or not track.get('enabled') or not track.get('tiers'):
        out['reason'] = f"{out['categoryLabel']} isn't open right now"
        return out

    # Pokémon cert cannot start with 0 (fail-closed on blanks), per offer.js.
    if cat == 'pokemon' and (not cert_raw or cert_raw.startswith('0')):
        out['reason'] = "Pokémon certs can't start with 0"
        return out

    # Tier: FIRST tier (array order) whose [min,max] contains v.
    tier = None
    for t in track['tiers']:
        lo, hi = _num(t.get('min')), _num(t.get('max'))
        if lo is not None and v < lo:
            continue
        if hi is not None and v > hi:
            continue
        tier = t
        break
    if tier is None:
        out['reason'] = f"${v:,.2f} is outside the posted ranges for {out['categoryLabel']}"
        return out
    out['band'] = (_num(tier.get('min')), _num(tier.get('max')))

    # Grade gate: tier.minGrade else track.minGrade (Pokémon = 7; others none).
    mg = _num(tier.get('minGrade'))
    if mg is None:
        mg = _num(track.get('minGrade'))
    if mg is not None:
        g = _grade_num(comp.get('grade'))
        if g is None or g < mg:
            out['reason'] = f"needs PSA {mg:g}+"
            return out

    # Cert-number floor (only Pokémon: >= 70,000,000).
    mc = _num(track.get('minCert'))
    if mc is not None and (not cert_digits or float(cert_digits) < mc):
        out['reason'] = 'cert graded too long ago (below the current cert floor)'
        return out

    # minConf is intentionally NOT enforced — the bot has no CL confidence score.
    rate = _num(tier.get('rate'))
    out['decision'] = 'accept'
    out['rate'] = int(rate) if rate == int(rate) else rate
    out['offer'] = _round_half_up_cents(v, rate)
    if cat in TRIUMPH_CATS:
        out['disclaimer'] = ALLOCATION_DISCLAIMER
    return out


def rate_card_lines(rules=RULES):
    """Human-readable rate card from the live rules (for the welcome message)."""
    lines = []
    for cat in ('pokemon', 'onepiece', 'basketball', 'baseball', 'football'):
        tr = rules.get('tracks', {}).get(cat + ':PSA')
        if not tr or not tr.get('enabled') or not tr.get('tiers'):
            continue
        parts = []
        for t in tr['tiers']:
            parts.append(f"${_num(t['min']):,g}-${_num(t['max']):,g} → {int(_num(t['rate']))}%")
        gates = []
        if _num(tr.get('minGrade')) is not None:
            gates.append(f"PSA {_num(tr['minGrade']):g}+")
        if _num(tr.get('minCert')) is not None:
            gates.append("recent cert")
        gate_s = f"  ({', '.join(gates)})" if gates else ''
        lines.append(f"{CATEGORY_LABELS[cat]}{gate_s}: " + '  |  '.join(parts))
    return lines
