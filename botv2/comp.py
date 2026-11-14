"""
comp — CardLadder comp helper client.

Calls the local PSA comp helper (psa-comp-helper/helper.js) which drives a
real Brave window via CDP and reads prices via Claude vision. The helper must
be running and reachable at COMP_HELPER_URL.

Usage from commands.py:
  rows = await comp.lookup(['47967337', '73293014'], grader='psa')
  text = comp.format_results(rows, grader='psa')

Usage for comp-pricing mode (called by portal_client.price_certs):
  priced = rows_to_priced_rows(rows, grader='psa')
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

import aiohttp

from . import config

if TYPE_CHECKING:
    from .portal_client import PricedRow


class CompError(Exception):
    """Helper unreachable, auth failure, or unexpected response shape."""


def parse_certs(raw: str) -> list[str]:
    """Extract 6–12 digit cert numbers from a raw string (deduped, order-preserved)."""
    seen: dict[str, None] = {}
    for m in re.finditer(r'\b(\d{6,12})\b', raw or ''):
        seen.setdefault(m.group(1), None)
    return list(seen)


async def lookup(certs: list[str], grader: str = 'psa',
                 session: Optional[aiohttp.ClientSession] = None) -> list[dict]:
    """POST /comp/batch to the helper. Returns raw result dicts (one per cert)."""
    if not certs:
        return []
    if not config.COMP_HELPER_URL:
        raise CompError('COMP_HELPER_URL not set — start the comp helper and configure the env var')

    url = config.COMP_HELPER_URL.rstrip('/') + '/comp/batch'
    headers = {'content-type': 'application/json', 'accept': 'application/json',
               'accept-encoding': 'gzip, deflate'}
    if config.COMP_HELPER_KEY:
        headers['x-api-key'] = config.COMP_HELPER_KEY

    own = session is None
    if own:
        session = aiohttp.ClientSession()
    try:
        async with session.post(
            url,
            json={'certs': certs, 'grader': grader},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as resp:
            if resp.status == 401:
                raise CompError('helper rejected API key (401) — check COMP_HELPER_KEY')
            if resp.status != 200:
                body = (await resp.text())[:200]
                raise CompError(f'helper returned HTTP {resp.status}: {body}')
            try:
                data = await resp.json(content_type=None)
            except (aiohttp.ClientError, ValueError) as e:
                raise CompError(f'helper returned non-JSON body: {e}')
        rows = data.get('results') if isinstance(data, dict) else None
        if not isinstance(rows, list):
            raise CompError(f'unexpected helper response shape: {str(data)[:200]}')
        return rows
    finally:
        if own:
            await session.close()


# ── formatting ────────────────────────────────────────────────────────────────

def format_results(rows: list[dict], grader: str = 'psa') -> str:
    """Format comp results for Discord. Returns one or more ≤1900-char blocks."""
    if not rows:
        return '(no results)'

    lines: list[str] = []
    total_buyable = 0
    total_found = 0

    for r in rows:
        cert = r.get('cert', '?')
        if not r.get('found'):
            note = r.get('note') or ''
            lines.append(f'`{cert}` — no data{(" — " + note[:60]) if note else ""}')
            continue

        price = (r.get('clValue')
                 or r.get('avg3Sales')
                 or (r.get('recentSales') or [None])[0])
        tag = ('CL' if r.get('clValue') is not None
               else 'avg3' if r.get('avg3Sales') is not None
               else 'last')
        price_str = f'${price:.2f} ({tag})' if price is not None else 'no price'

        buy, skip_reason = _buyable(r, grader)
        badge = '✅' if buy else '❌'
        total_found += 1
        if buy:
            total_buyable += 1

        sales = r.get('recentSales') or []
        sales_str = (' · ' + '/'.join(f'${s:.0f}' for s in sales[:3])) if sales else ''

        last_sale = r.get('lastSaleDate') or ''
        date_str = f' · last {last_sale}' if last_sale else ''

        card = r.get('cardName') or ''
        grade = r.get('grade') or ''
        name_str = ''
        if card:
            name_str = card[:50] + (f' {grade}' if grade else '')

        skip_str = f' ({skip_reason})' if not buy and skip_reason else ''

        line = f'{badge} `{cert}` **{price_str}**{sales_str}{date_str}{skip_str}'
        if name_str:
            line += f'\n    ↳ {name_str}'
        lines.append(line)

    total = len(rows)
    footer = f'\n**{total_buyable} buyable / {total_found} found / {total} total**'

    # Chunk into ≤1900-char blocks (Discord limit is 2000; leave room for the header)
    blocks: list[str] = []
    current: list[str] = []
    current_len = 0
    for ln in lines:
        need = len(ln) + 1  # +1 for '\n'
        if current and current_len + need > 1900:
            blocks.append('\n'.join(current))
            current = []
            current_len = 0
        current.append(ln)
        current_len += need
    if current:
        blocks.append('\n'.join(current))

    if not blocks:
        return footer.lstrip('\n')
    blocks[-1] += footer  # attach summary to the last block
    return '\n---\n'.join(blocks)


def _buyable(r: dict, grader: str) -> tuple[bool, str]:
    """Mirror the buyability logic from psa-comp-helper/public/index.html."""
    price = (r.get('clValue')
             or r.get('avg3Sales')
             or (r.get('recentSales') or [None])[0])
    if price is None or price < 1:
        return False, 'price < $1'

    if grader.lower() != 'psa':
        return True, ''  # CGC: any positive price is buyable

    # PSA-specific gates
    grade_m = re.search(r'\d+', str(r.get('grade') or ''))
    grade = int(grade_m.group()) if grade_m else 0
    if grade < 7:
        return False, f'grade {r.get("grade") or "?"} < 7'

    sport = (r.get('sport') or '').lower()
    cap = 200 if sport == 'pokemon' else 250
    if price > cap:
        return False, f'${price:.0f} > ${cap} {sport} cap'

    last_sale = r.get('lastSaleDate')
    if not last_sale:
        return False, 'no last-sale date'
    try:
        last_dt = datetime.strptime(last_sale, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - last_dt).days
        if age_days > 30:
            return False, f'last sale {age_days}d ago (>30)'
    except ValueError:
        return False, 'bad last-sale date'

    return True, ''


# ── comp-driven pricing (COMP_PRICING_MODE) ───────────────────────────────────

def rows_to_priced_rows(comp_rows: list[dict], grader: str = 'psa',
                         original_certs: Optional[list[str]] = None) -> 'list[PricedRow]':
    """Map comp helper results to PricedRow objects using comp_rules.

    PSA: value = clValue (CL market value), rate = per-sport rule.
    CGC: value = last sale price, rate = CGC rule.
    Gates: minValue/maxValue, minGrade (PSA), maxSaleAgeDays.
    original_certs: the input cert strings (preserve cgc: prefix for CGC rows).
    """
    from .portal_client import PricedRow
    from . import comp_rules

    is_cgc = grader.lower() == 'cgc'
    out: list[PricedRow] = []

    for i, r in enumerate(comp_rows):
        raw_cert = r.get('cert', '')
        cert_id = re.sub(r'\D', '', str(raw_cert))
        cert = (f'cgc:{cert_id}' if is_cgc else cert_id)
        if original_certs and i < len(original_certs):
            cert = original_certs[i]

        if not r.get('found'):
            note = r.get('note') or ''
            out.append(PricedRow(
                cert=cert, cert_id=cert_id,
                grader=grader.upper(),
                status='no match', decision='PASS',
                reason=note or 'no CardLadder data for this cert',
            ))
            continue

        card_name = r.get('cardName') or ''
        grade_str = str(r.get('grade') or '')
        sport = (r.get('sport') or '').lower()
        last_sale_price = (r.get('recentSales') or [None])[0]
        cl_value = r.get('clValue')
        avg3 = r.get('avg3Sales')

        # Pick value basis: CGC uses last sale; PSA uses CL market value
        if is_cgc and last_sale_price is not None:
            value = last_sale_price
            value_basis = 'lastSold'
            rules = comp_rules.get_cgc_rules()
        else:
            value = cl_value or avg3 or last_sale_price
            value_basis = 'market'
            rules = (comp_rules.get_cgc_rules() if is_cgc
                     else comp_rules.get_sport_rules(sport))

        ok, skip_reason = _apply_rules(r, grader, rules, value)

        common = dict(cert=cert, cert_id=cert_id, grader=grader.upper(),
                      grade=grade_str, card_name=card_name, category=sport,
                      market_value=cl_value, last_sale=last_sale_price,
                      value=value, value_basis=value_basis)

        if not ok or value is None:
            out.append(PricedRow(**common, status='rejected', decision='PASS',
                                 reason=skip_reason or 'does not meet buying criteria'))
            continue

        rate = float(rules.get('rate', 0.85))
        offer = round(value * rate, 2)
        out.append(PricedRow(**common, offer_pct=round(rate * 100, 1),
                             status='priced', decision='BUY', offer=offer))

    return out


def _apply_rules(r: dict, grader: str, rules: dict,
                 value: Optional[float]) -> tuple[bool, str]:
    """Gate a comp result against the configured rules dict."""
    if value is None or value < 1:
        return False, 'no price found'

    min_val = float(rules.get('minValue', 1))
    max_val = float(rules.get('maxValue', 9999))
    max_age = int(rules.get('maxSaleAgeDays', 30))

    if value < min_val:
        return False, f'${value:.2f} below ${min_val:.0f} min'
    if value > max_val:
        return False, f'${value:.2f} above ${max_val:.0f} max'

    if grader.lower() == 'psa':
        min_grade = int(rules.get('minGrade', 7))
        m = re.search(r'\d+', str(r.get('grade') or ''))
        grade = int(m.group()) if m else 0
        if grade < min_grade:
            return False, f'grade {r.get("grade") or "?"} < {min_grade}'

    last_sale = r.get('lastSaleDate')
    if not last_sale:
        # CGC without a sale date but has a value → allow (clValue path)
        return (True, '') if grader.lower() == 'cgc' else (False, 'no last-sale date')
    try:
        last_dt = datetime.strptime(last_sale, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - last_dt).days
        if age > max_age:
            return False, f'last sale {age}d ago (>{max_age})'
    except ValueError:
        return False, 'bad last-sale date'

    return True, ''
