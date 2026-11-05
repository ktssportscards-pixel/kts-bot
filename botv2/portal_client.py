"""
portal_client — the ONLY price source for botv2.

Calls the KTS Portal's server-to-server endpoint POST /bot/price with the
x-bot-key shared secret. The portal prices each cert against Triumph's scanner
API and runs the SAME offer.js the app uses, so every row comes back with the
comp value, the CardLadder confidence, AND the BUY/PASS decision + offer. The
bot holds no Triumph token, no CardLadder client, and no rules engine of its own.

Contract (agreed with the portal side):
  POST {PORTAL_BASE_URL}/bot/price
  headers: x-bot-key: <BOT_API_KEY>, content-type: application/json
  body:    {"certs": ["123", "cgc:456", ...]}   (or {"cert": "123"})
  cap:     200 certs / request (over -> HTTP 400)
  resp:    {"rows": [ <row>, ... ]}  -- one row per input cert, IN INPUT ORDER
  row:     {certId, grader, grade, cardTitle, imageUrl, category, marketValue,
            lastSale, status, rejectionReason, clConfidence, decision, offer, reason}
  Kevin's payout rate (payoutCents / memberMultiplier) is dropped at the source.
  Idempotent; safe to retry on 5xx. Per-row `status` lets us retry only misses.
"""
from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from typing import Optional

import aiohttp

from . import config


# ── errors ─────────────────────────────────────────────────────────────────────
class PortalError(Exception):
    """Base for portal pricing failures."""


class PortalAuthError(PortalError):
    """401 — bad/missing x-bot-key. A config problem, never retried."""


class PortalBadRequest(PortalError):
    """400 — malformed request (e.g. over the 200 cap). Never retried."""


class PortalUnavailable(PortalError):
    """Repeated 5xx / network / timeout. The caller should hold + retry later."""


# ── the priced row ──────────────────────────────────────────────────────────────
@dataclass
class PricedRow:
    """One cert's result from /bot/price. This is botv2's price model — the whole
    bot consumes PricedRow, never a raw CardLadder comp. Confidential fields
    (payout/multiplier) are never populated; they are dropped at the portal."""
    cert: str                      # as sent (digits, or "cgc:<digits>")
    cert_id: str                   # digits only
    grader: str = ""               # "PSA" | "CGC"
    grade: str = ""
    card_name: str = ""
    image_url: str = ""
    category: str = ""             # raw portal category, e.g. "pokemon", "basketball"
    market_value: Optional[float] = None   # dollars (CardLadder comp)
    last_sale: Optional[float] = None       # dollars (CGC valued off this)
    status: str = ""               # portal status: accepted|rejected|priced|no match|error|...
    rejection_reason: str = ""
    confidence: Optional[float] = None       # CardLadder match confidence (clConfidence)
    decision: str = ""             # "BUY" | "PASS"
    offer: Optional[float] = None            # dollars (from the portal's offer.js)
    reason: str = ""               # PASS reason / note
    error: str = ""                # transport/portal error for THIS cert, if any

    @property
    def priced(self) -> bool:
        """True when the portal returned a usable comp value for this cert."""
        return self.market_value is not None and not self.error

    @property
    def is_buy(self) -> bool:
        return self.decision.upper() == "BUY"

    @property
    def is_miss(self) -> bool:
        """No value AND no hard decision — worth retrying (vs a clean PASS)."""
        return not self.priced and self.decision.upper() not in ("BUY", "PASS")

    @classmethod
    def from_api(cls, sent_cert: str, row: dict) -> "PricedRow":
        """Map a /bot/price row to PricedRow, stripping confidential fields and
        coercing types defensively (never trust the wire shape blindly)."""
        row = {k: v for k, v in (row or {}).items()
               if k not in config.CONFIDENTIAL_ROW_FIELDS}

        def num(v):
            try:
                return None if v is None else float(v)
            except (TypeError, ValueError):
                return None

        return cls(
            cert=sent_cert,
            cert_id=str(row.get("certId") or _digits(sent_cert)),
            grader=str(row.get("grader") or "").upper(),
            grade=str(row.get("grade") or ""),
            card_name=str(row.get("cardTitle") or ""),
            image_url=str(row.get("imageUrl") or ""),
            category=str(row.get("category") or "").lower(),
            market_value=num(row.get("marketValue")),
            last_sale=num(row.get("lastSale")),
            status=str(row.get("status") or ""),
            rejection_reason=str(row.get("rejectionReason") or ""),
            confidence=num(row.get("clConfidence")),
            decision=str(row.get("decision") or "").upper(),
            offer=num(row.get("offer")),
            reason=str(row.get("reason") or ""),
            error=str(row.get("error") or ""),
        )


def _digits(cert: str) -> str:
    return re.sub(r"\D", "", str(cert or ""))


# ── the client ───────────────────────────────────────────────────────────────
async def price_certs(certs: list[str],
                       rate_override: Optional[dict] = None,
                       session: Optional[aiohttp.ClientSession] = None) -> list[PricedRow]:
    """Price a list of certs via the portal. Returns one PricedRow per input cert,
    in input order. Chunks at the portal's 200 cap. Retries only transient failures
    (5xx / network / timeout); raises PortalAuthError / PortalBadRequest on a 4xx
    config error, PortalUnavailable if transient failures exhaust retries.

    rate_override: optional per-customer VIP rates, applied by the portal through
    offer.js's own {rateOverrides} path (so ALL offer math stays in offer.js, and
    offer.js caps it at 100%). Shape: {category: flat_pct | [{min,max,rate}], ...}.
    Omitted -> public rates. One call = one customer's lot = one override map."""
    certs = [str(c).strip() for c in certs if str(c).strip()]
    if not certs:
        return []
    if config.PRICE_MOCK:
        return [_mock_row(c) for c in certs]

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()
    try:
        out: list[PricedRow] = []
        for i in range(0, len(certs), config.PRICE_BATCH_CAP):
            chunk = certs[i:i + config.PRICE_BATCH_CAP]
            out.extend(await _price_chunk(session, chunk, rate_override))
        return out
    finally:
        if own_session:
            await session.close()


async def _price_chunk(session: aiohttp.ClientSession, chunk: list[str],
                       rate_override: Optional[dict] = None) -> list[PricedRow]:
    headers = {"x-bot-key": config.BOT_API_KEY,
               "content-type": "application/json",
               "accept": "application/json"}
    payload: dict = {"certs": chunk}
    if rate_override:
        payload["rateOverride"] = rate_override
    last_exc: Optional[Exception] = None

    for attempt in range(config.PRICE_MAX_RETRIES + 1):
        try:
            async with session.post(config.PRICE_ENDPOINT, json=payload, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=config.PRICE_TIMEOUT_S)) as resp:
                if resp.status == 401:
                    raise PortalAuthError("portal rejected x-bot-key (401)")
                if resp.status == 400:
                    body = (await resp.text())[:300]
                    raise PortalBadRequest(f"portal 400: {body}")
                if resp.status >= 500:
                    raise PortalUnavailable(f"portal {resp.status}")
                data = await resp.json(content_type=None)
            rows = data.get("rows") if isinstance(data, dict) else None
            if not isinstance(rows, list):
                raise PortalUnavailable(f"portal returned no rows array: {str(data)[:200]}")
            # Map in input order; tolerate a short/misaligned array by cert matching.
            return _align_rows(chunk, rows)
        except (PortalAuthError, PortalBadRequest):
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, PortalUnavailable) as e:
            last_exc = e
            if attempt < config.PRICE_MAX_RETRIES:
                await asyncio.sleep(config.PRICE_RETRY_BACKOFF_S * (attempt + 1))
            continue
    raise PortalUnavailable(f"portal unavailable after {config.PRICE_MAX_RETRIES + 1} tries: {last_exc}")


def _align_rows(chunk: list[str], rows: list[dict]) -> list[PricedRow]:
    """Prefer positional mapping (contract: rows in input order). If the counts
    disagree, fall back to matching by certId digits so one bad/missing row can't
    shift every downstream cert."""
    if len(rows) == len(chunk):
        return [PricedRow.from_api(c, r) for c, r in zip(chunk, rows)]
    by_id = {}
    for r in rows:
        by_id.setdefault(_digits(str((r or {}).get("certId", ""))), r)
    out = []
    for c in chunk:
        r = by_id.get(_digits(c))
        out.append(PricedRow.from_api(c, r) if r is not None else
                   PricedRow(cert=c, cert_id=_digits(c), status="error",
                             error="no row returned for this cert"))
    return out


# ── mock (dev/offline: lets the bot be exercised before /bot/price is deployed) ─
_MOCK_CATS = ["basketball", "baseball", "football", "one piece", "pokemon"]


def _mock_row(cert: str) -> PricedRow:
    h = int(hashlib.sha1(_digits(cert).encode() or b"0").hexdigest(), 16)
    cat = _MOCK_CATS[h % len(_MOCK_CATS)]
    value = round(((h >> 8) % 120000) / 100.0 + 1, 2)        # ~$1..$1200
    is_cgc = cert.lower().startswith("cgc:") or len(_digits(cert)) >= 10
    buy = (value <= 1000) and (h % 7 != 0)
    return PricedRow(
        cert=cert, cert_id=_digits(cert),
        grader="CGC" if is_cgc else "PSA", grade=str(7 + (h % 4)),
        card_name=f"[MOCK] {cat.title()} Card {_digits(cert)[-4:]}",
        category=cat, market_value=value,
        last_sale=round(value * 0.95, 2) if is_cgc else None,
        status="priced" if buy else "rejected",
        confidence=1 + (h % 5),
        decision="BUY" if buy else "PASS",
        offer=round(value * 0.9, 2) if buy else None,
        reason="" if buy else "[mock] out of band",
    )
