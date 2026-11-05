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
  resp:    {"rows": [ <row>, ... ], "customerMatched": bool}  -- one row per input
           cert, IN INPUT ORDER
  row:     {certId, grader, grade, cardTitle, imageUrl, category, categoryLabel,
            marketValue, lastSale, value, valueBasis, clConfidence, status,
            rejectionReason, decision, offer, offerPct, reason, band, vip, error}
           `value` is the base the offer was priced against (lastSale for a CGC with
           a sale, else marketValue); `offerPct` = round(offer/value*100). The app
           shows `value` + `valueBasis`, so the bot's comp/rate use `value` too.
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
    market_value: Optional[float] = None   # dollars (CardLadder market value)
    last_sale: Optional[float] = None       # dollars (last sold; CGC valued off this)
    value: Optional[float] = None           # dollars — the base the offer was priced
                                            # against (lastSale for a CGC w/ a sale,
                                            # else marketValue). What the app shows.
    value_basis: str = ""          # "lastSold" | "market" — which basis `value` used
    offer_pct: Optional[float] = None       # portal offer %, 0-100 (round(offer/value*100))
    status: str = ""               # portal status: accepted|rejected|priced|no match|error|...
    rejection_reason: str = ""
    confidence: Optional[float] = None       # CardLadder match confidence (clConfidence)
    decision: str = ""             # "BUY" | "PASS"
    offer: Optional[float] = None            # dollars (from the portal's offer.js)
    reason: str = ""               # PASS reason / note
    error: str = ""                # transport/portal error for THIS cert, if any
    vip: bool = False              # portal applied this customer's VIP rate to THIS card
    customer_matched: bool = False  # the `customer` matched a portal seller row at all
    near_full: bool = False        # allocation: this card's band is ≥ the near-full % (soft urgency)
    pct_full: Optional[float] = None  # allocation: this card's band %-full (None if spendHidden)

    @property
    def comp_value(self) -> Optional[float]:
        """The figure the offer is priced against and the app displays: the portal's
        `value` (lastSale basis for a CGC with a sale, market otherwise), falling back
        to marketValue when the portal didn't send `value` (older portal / mock). The
        sheet's comp column and the shown rate are both built from this, so CGC cards
        valued off a last sale read the same way they do in the app."""
        return self.value if self.value is not None else self.market_value

    @property
    def priced(self) -> bool:
        """True when the portal returned a usable comp value for this cert."""
        return self.comp_value is not None and not self.error

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
            value=num(row.get("value")),
            value_basis=str(row.get("valueBasis") or ""),
            offer_pct=num(row.get("offerPct")),
            status=str(row.get("status") or ""),
            rejection_reason=str(row.get("rejectionReason") or ""),
            confidence=num(row.get("clConfidence")),
            decision=str(row.get("decision") or "").upper(),
            offer=num(row.get("offer")),
            reason=str(row.get("reason") or ""),
            error=str(row.get("error") or ""),
            vip=bool(row.get("vip", False)),
            near_full=bool(row.get("nearFull", False)),
            pct_full=num(row.get("pctFull")),
        )


def _digits(cert: str) -> str:
    return re.sub(r"\D", "", str(cert or ""))


# ── the client ───────────────────────────────────────────────────────────────
async def price_certs(certs: list[str],
                       customer: Optional[str] = None,
                       rate_override: Optional[dict] = None,
                       session: Optional[aiohttp.ClientSession] = None) -> list[PricedRow]:
    """Price a list of certs via the portal. Returns one PricedRow per input cert,
    in input order. Chunks at the portal's 200 cap. Retries only transient failures
    (5xx / network / timeout); raises PortalAuthError / PortalBadRequest on a 4xx
    config error, PortalUnavailable if transient failures exhaust retries.

    customer: the Discord username — the portal matches it against its own VIP
    section (single source of truth for who's VIP) and auto-applies that seller's
    rates, flagging each row vip=True. This is the primary VIP path.
    rate_override: an optional MANUAL override map ({category: flat_pct |
    [{min,max,rate}]}) for a one-off non-portal customer; the portal applies it via
    offer.js's {rateOverrides} (100%-capped). Omitted -> customer's VIP, else public.
    Either way ALL offer math stays in offer.js — the bot never prices."""
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
            out.extend(await _price_chunk(session, chunk, customer, rate_override))
        return out
    finally:
        if own_session:
            await session.close()


async def _price_chunk(session: aiohttp.ClientSession, chunk: list[str],
                       customer: Optional[str] = None,
                       rate_override: Optional[dict] = None) -> list[PricedRow]:
    headers = {"x-bot-key": config.BOT_API_KEY,
               "content-type": "application/json",
               "accept": "application/json",
               # Ask for gzip only — these payloads are tiny, and this removes any
               # dependency on a brotli ('br') decoder being present/consistent in
               # the runtime (a mismatched brotli wheel otherwise fails the decode).
               "accept-encoding": "gzip, deflate"}
    payload: dict = {"certs": chunk}
    if customer:
        payload["customer"] = customer
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
                if resp.status != 200:
                    # 403/404/405/429/… — endpoint not live yet, rate-limited, or an
                    # edge/CDN interstitial. Transient-shaped: retry, then the caller
                    # gets the graceful "briefly unavailable" path (never a silent drop).
                    body = (await resp.text())[:200]
                    raise PortalUnavailable(f"portal {resp.status}: {body}")
                try:
                    data = await resp.json(content_type=None)
                except (aiohttp.ClientError, ValueError) as e:
                    # 200 with a non-JSON body (HTML page, truncated proxy response).
                    # json.JSONDecodeError is a ValueError. Funnel into the retry/
                    # unavailable path instead of leaking an unhandled parse error.
                    raise PortalUnavailable(f"portal returned a non-JSON body ({e})")
            rows = data.get("rows") if isinstance(data, dict) else None
            if not isinstance(rows, list):
                raise PortalUnavailable(f"portal returned no rows array: {str(data)[:200]}")
            # Map in input order; tolerate a short/misaligned array by cert matching.
            matched = bool(data.get("customerMatched"))
            out = _align_rows(chunk, rows)
            for r in out:
                r.customer_matched = matched
            return out
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
    market = round(((h >> 8) % 120000) / 100.0 + 1, 2)       # ~$1..$1200
    is_cgc = cert.lower().startswith("cgc:") or len(_digits(cert)) >= 10
    last_sale = round(market * 0.95, 2) if is_cgc else None
    basis = last_sale if (is_cgc and last_sale) else market  # mirror offer.js `value`
    buy = (basis <= 1000) and (h % 7 != 0)
    offer = round(basis * 0.9, 2) if buy else None
    return PricedRow(
        cert=cert, cert_id=_digits(cert),
        grader="CGC" if is_cgc else "PSA", grade=str(7 + (h % 4)),
        card_name=f"[MOCK] {cat.title()} Card {_digits(cert)[-4:]}",
        category=cat, market_value=market, last_sale=last_sale,
        value=basis, value_basis="lastSold" if (is_cgc and last_sale) else "market",
        offer_pct=round(offer / basis * 100) if (buy and basis) else None,
        status="priced" if buy else "rejected",
        confidence=1 + (h % 5),
        decision="BUY" if buy else "PASS",
        offer=offer,
        reason="" if buy else "[mock] out of band",
    )


# ── claim client (reserve Triumph allocation via /bot/claim; UNDO via /bot/unclaim) ─
@dataclass
class ClaimResult:
    """One cert's result from /bot/claim or /bot/unclaim. The portal re-prices and
    claims only decision==BUY; a PASS/ineligible cert rides as `skipped`, never an
    error. `claimed`/`removed` are the success flags; `idempotent`/`already_claimed`
    mean a prior identical call already did it (so a retry can't double-claim)."""
    cert_id: str
    claimed: bool = False
    already_claimed: bool = False
    removed: bool = False           # from /bot/unclaim
    skipped: str = ""               # e.g. "Auto-claim is turned off." or a PASS reason
    error: str = ""
    idempotent: bool = False

    @property
    def ok(self) -> bool:
        return self.claimed or self.already_claimed or self.removed

    @classmethod
    def from_api(cls, row: dict) -> "ClaimResult":
        row = row or {}
        return cls(
            cert_id=str(row.get("certId") or ""),
            claimed=bool(row.get("claimed", False)),
            already_claimed=bool(row.get("alreadyClaimed", False)),
            removed=bool(row.get("removed", False)),
            skipped=str(row.get("skipped") or ""),
            error=str(row.get("error") or ""),
            idempotent=bool(row.get("idempotent", False)),
        )


async def claim_certs(certs: list, idempotency_key: Optional[str] = None,
                      session: Optional[aiohttp.ClientSession] = None) -> list:
    """Reserve Triumph allocation for BUY certs via the portal's /bot/claim. DOUBLE-
    GATED: the portal claims only when ITS kill switch is on AND config.AUTO_CLAIM is
    on here; otherwise every BUY comes back skipped. Idempotent on (idempotencyKey,
    certId) so a retry can't double-claim. Per-cert issues ride as the row's
    skipped/error; raises PortalAuthError/PortalBadRequest on a 4xx, PortalUnavailable
    when transient failures exhaust retries."""
    return await _claim_call(config.CLAIM_ENDPOINT, certs, idempotency_key, session)


async def unclaim_certs(certs: list, idempotency_key: Optional[str] = None,
                        session: Optional[aiohttp.ClientSession] = None) -> list:
    """The UNDO — remove drafted claims before the weekly submit, via /bot/unclaim.
    Lets the kill switch actively reverse a wrongly-drafted claim, not just halt."""
    return await _claim_call(config.UNCLAIM_ENDPOINT, certs, idempotency_key, session)


async def _claim_call(endpoint, certs, idempotency_key, session):
    certs = [str(c).strip() for c in certs if str(c).strip()]
    if not certs:
        return []
    if config.PRICE_MOCK:
        return [_mock_claim(c) for c in certs]
    own = session is None
    if own:
        session = aiohttp.ClientSession()
    try:
        out = []
        for i in range(0, len(certs), config.PRICE_BATCH_CAP):
            out.extend(await _claim_chunk(session, endpoint,
                                          certs[i:i + config.PRICE_BATCH_CAP], idempotency_key))
        return out
    finally:
        if own:
            await session.close()


async def _claim_chunk(session, endpoint, chunk, idempotency_key):
    headers = {"x-bot-key": config.BOT_API_KEY,
               "content-type": "application/json",
               "accept": "application/json",
               "accept-encoding": "gzip, deflate"}
    payload = {"certs": chunk}
    if idempotency_key:
        payload["idempotencyKey"] = str(idempotency_key)
    last_exc = None
    for attempt in range(config.PRICE_MAX_RETRIES + 1):
        try:
            async with session.post(endpoint, json=payload, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=config.PRICE_TIMEOUT_S)) as resp:
                if resp.status == 401:
                    raise PortalAuthError("portal rejected x-bot-key on claim (401)")
                if resp.status == 400:
                    body = (await resp.text())[:300]
                    raise PortalBadRequest(f"claim 400: {body}")
                if resp.status >= 500:
                    raise PortalUnavailable(f"claim {resp.status}")
                if resp.status != 200:
                    body = (await resp.text())[:200]
                    raise PortalUnavailable(f"claim {resp.status}: {body}")
                try:
                    data = await resp.json(content_type=None)
                except (aiohttp.ClientError, ValueError) as e:
                    raise PortalUnavailable(f"claim non-JSON body ({e})")
            rows = data.get("results") if isinstance(data, dict) else None
            if not isinstance(rows, list):
                raise PortalUnavailable(f"claim returned no results array: {str(data)[:200]}")
            return [ClaimResult.from_api(r) for r in rows]
        except (PortalAuthError, PortalBadRequest):
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, PortalUnavailable) as e:
            last_exc = e
            if attempt < config.PRICE_MAX_RETRIES:
                await asyncio.sleep(config.PRICE_RETRY_BACKOFF_S * (attempt + 1))
            continue
    raise PortalUnavailable(f"claim endpoint unavailable after {config.PRICE_MAX_RETRIES + 1} tries: {last_exc}")


def _mock_claim(cert: str) -> "ClaimResult":
    # Offline/selftest: mirror the kill-switch-off default (nothing claims).
    return ClaimResult(cert_id=_digits(cert), skipped="[mock] auto-claim off")


# ── allocation board (read-only "band getting full" data) ───────────────────────
async def fetch_allocation(session: Optional[aiohttp.ClientSession] = None) -> dict:
    """Read the team allocation board via GET /bot/allocation (read-only, x-bot-key).
    Returns the raw board {period, spendHidden, categories:[{category, categoryLabel,
    leftCents, pctFull?, ranges:[{minCents,maxCents,leftCents,pctFull?}]}], reliability}.
    Values are draftCents-correct per sub-range (not the overestimating band total) but
    are still Triumph's own figures — use only as a conservative near-full signal."""
    if config.PRICE_MOCK:
        return _mock_allocation()
    headers = {"x-bot-key": config.BOT_API_KEY, "accept": "application/json",
               "accept-encoding": "gzip, deflate"}
    own = session is None
    if own:
        session = aiohttp.ClientSession()
    try:
        async with session.get(config.ALLOCATION_ENDPOINT, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 401:
                raise PortalAuthError("portal rejected x-bot-key on allocation (401)")
            if resp.status != 200:
                raise PortalUnavailable(f"allocation {resp.status}")
            try:
                data = await resp.json(content_type=None)
            except (aiohttp.ClientError, ValueError) as e:
                raise PortalUnavailable(f"allocation non-JSON body ({e})")
        return data if isinstance(data, dict) else {}
    finally:
        if own:
            await session.close()


def _mock_allocation() -> dict:
    return {
        "period": {"name": "[mock] weekend"},
        "spendHidden": False,
        "categories": [
            {"category": "onepiece", "categoryLabel": "One Piece", "leftCents": 45000,
             "pctFull": 66.0, "ranges": [
                 {"minCents": 10000, "maxCents": 100000, "leftCents": 45000, "pctFull": 66.0}]},
            {"category": "basketball", "categoryLabel": "Basketball / NBA", "leftCents": 900000,
             "pctFull": 20.0, "ranges": [
                 {"minCents": 0, "maxCents": 100000, "leftCents": 900000, "pctFull": 20.0}]},
        ],
        "reliability": "approximate — Triumph's figures can be off; treat as a near-full signal, not exact.",
    }
