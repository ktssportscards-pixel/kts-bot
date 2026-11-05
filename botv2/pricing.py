"""
pricing — turn PricedRows from the portal into a customer quote + a Kevin summary.

In v1 this was ~500 lines of classify + three band tables + blended-rate math. In
v2 the portal already returns the BUY/PASS decision AND the per-cert offer (the
same offer.js the app runs), so this module only PARTITIONS and PRESENTS:
  - no band logic, no rate blending (payout is just the sum of the portal's offers)
  - no "final acceptance depends on allocation" disclaimer — the portal's decision
    already reflects live Triumph allocation, so a BUY here is authoritative.
"""
from dataclasses import dataclass, field

from . import config
from .portal_client import PricedRow

CATEGORY_LABELS = {
    "pokemon": "Pokémon",
    "onepiece": "One Piece", "one piece": "One Piece", "tcg": "One Piece",
    "basketball": "Basketball / NBA",
    "baseball": "Baseball / MLB", "mlb": "Baseball / MLB",
    "football": "Football / NFL", "nfl": "Football / NFL",
}


def category_label(cat: str) -> str:
    return CATEGORY_LABELS.get((cat or "").lower(), (cat or "Other").title())


def money(v) -> str:
    return f"${(v or 0):,.2f}"


def reject_reason(r: PricedRow) -> str:
    """CUSTOMER-safe reason a row wasn't taken. Scrubs any portal reason that names
    the backend — e.g. Triumph's "Allocation Exceeded." carries a forbidden word — so
    supplier/backend details never reach a customer. Kevin's DM uses kevin_reason()
    for the true text."""
    raw = (r.reason or r.rejection_reason or "").strip()
    low = raw.lower()
    if "allocation" in low:                         # band/pool is full — don't name it
        return "we're at our buying limit on this one right now"
    if raw and not any(w in low for w in config.CUSTOMER_FORBIDDEN_WORDS):
        return raw
    if not r.priced:
        return "no comp found for this cert"
    return "doesn't fit our current buying criteria"


def kevin_reason(r: PricedRow) -> str:
    """The TRUE reason, for Kevin's DM only (he may see backend terms the customer
    must never see, e.g. 'Allocation Exceeded.')."""
    raw = (r.reason or r.rejection_reason or "").strip()
    if raw:
        return raw
    return "no comp found" if not r.priced else "out of current buying criteria"


# ── the quote model ─────────────────────────────────────────────────────────────
@dataclass
class CategoryBreakdown:
    category: str
    label: str
    count: int
    comp_total: float
    payout_total: float

    @property
    def rate(self) -> float:
        return (self.payout_total / self.comp_total) if self.comp_total else 0.0


@dataclass
class Quote:
    accepted: list = field(default_factory=list)    # PricedRow, decision BUY
    rejected: list = field(default_factory=list)     # PricedRow, PASS / no comp
    unpriced: list = field(default_factory=list)     # transport error → resend
    breakdown: list = field(default_factory=list)    # CategoryBreakdown (accepted)
    comp_total: float = 0.0
    payout_total: float = 0.0
    vip: bool = False               # portal applied this customer's VIP rates

    @property
    def n_accepted(self) -> int:
        return len(self.accepted)

    @property
    def effective_rate(self) -> float:
        return (self.payout_total / self.comp_total) if self.comp_total else 0.0


def build_quote(rows: list[PricedRow]) -> Quote:
    """Partition portal rows into accepted / rejected / unpriced and total the
    portal's own per-cert offers. No pricing math happens here."""
    q = Quote()
    groups: dict[str, dict] = {}
    for r in rows:
        if r.error:                      # transport/portal error for this cert
            q.unpriced.append(r)
        elif r.is_buy:
            q.accepted.append(r)
            comp = r.comp_value or 0.0       # portal's `value` (the offer basis)
            pay = r.offer if r.offer is not None else 0.0
            q.comp_total += comp
            q.payout_total += pay
            g = groups.setdefault(r.category or "other",
                                  {"count": 0, "comp": 0.0, "pay": 0.0})
            g["count"] += 1
            g["comp"] += comp
            g["pay"] += pay
        else:                            # PASS, or priced-but-not-buy, or no match
            q.rejected.append(r)
    q.breakdown = [
        CategoryBreakdown(cat, category_label(cat), g["count"], g["comp"], g["pay"])
        for cat, g in sorted(groups.items())
    ]
    q.vip = any(r.vip for r in q.accepted)
    return q


# ── presentation ────────────────────────────────────────────────────────────────
def breakdown_lines(q: Quote) -> list[str]:
    out = []
    for b in q.breakdown:
        cards = "card" if b.count == 1 else "cards"
        out.append(f"• {b.label}: {b.count} {cards}, {money(b.comp_total)} → "
                   f"{money(b.payout_total)} ({b.rate * 100:.1f}%)")
    return out


def customer_message(q: Quote, head: str = "✅ All comps loaded!",
                     proceed_override: str = None) -> str:
    parts = [head, ""]
    if q.n_accepted:
        parts += [
            f"**Total comp:** {money(q.comp_total)}",
            f"**Total payout:** {money(q.payout_total)}",
            f"**Number of cards:** {q.n_accepted}",
            "",
            "**Breakdown:**",
            *breakdown_lines(q),
        ]
        if q.vip:
            parts += ["", "✨ *VIP rates applied — thanks for being a top supporter!*"]
        if any(r.near_full for r in q.accepted):
            parts += ["", "⏳ *Heads up — a few of these are in ranges filling up fast; "
                          "get them submitted soon to lock them in!*"]
    if q.rejected:
        parts.append("")
        n = len(q.rejected)
        if q.n_accepted:
            parts.append(f"⚠️ {n} card{'s' if n != 1 else ''} "
                         f"don't fit our current buying criteria:")
        elif n == 1:
            parts.append("⚠️ Unfortunately, this card doesn't fit our current "
                         "buying criteria:")
        else:
            parts.append(f"⚠️ Unfortunately, none of these {n} cards fit our "
                         f"current buying criteria:")
        parts += [f"• `{r.cert}` — {reject_reason(r)}" for r in q.rejected]
    if q.unpriced:
        n = len(q.unpriced)
        parts += ["",
                  f"🔄 {n} card{'s' if n != 1 else ''} need{'s' if n == 1 else ''} "
                  f"a re-check — resend {'it' if n == 1 else 'them'}:",
                  *[f"• `{r.cert}`" for r in q.unpriced]]
    if q.n_accepted:
        parts += ["", proceed_override or _proceed_line(q)]
    return "\n".join(parts)


def _proceed_line(q: Quote) -> str:
    if q.unpriced:
        return ("Resend the re-check card(s) if you'd like them included — "
                "otherwise let me know if you want to proceed with the "
                f"{q.n_accepted} we can take.")
    if q.rejected:
        return f"Let me know if you want to proceed with the {q.n_accepted} we can take."
    return "Let me know if you'd like to proceed!"


def kevin_summary(q: Quote, username: str, sheet_url: str = "", ticket_link: str = "") -> str:
    """Kevin's DM: totals + per-cert detail. Shows comp values only — never the
    portal's payout rate (it never reaches the bot). `ticket_link` is a Discord
    channel mention ("<#id>") so Kevin can jump straight to the ticket from the DM."""
    n = q.n_accepted + len(q.rejected) + len(q.unpriced)
    header = (f"📋 **{username}**{' ✨VIP' if q.vip else ''} — {n} cert{'s' if n != 1 else ''} | "
              f"Accepted **{q.n_accepted}** | Comp **{money(q.comp_total)}** | "
              f"Payout **{money(q.payout_total)}**")
    if q.rejected:
        header += f" | {len(q.rejected)} rejected"
    if q.unpriced:
        header += f" | {len(q.unpriced)} ⚠️ re-check"
    lines = [header]
    if ticket_link:
        lines.append(f"**Ticket:** {ticket_link}")
    if sheet_url:
        lines.append(sheet_url)
    lines += breakdown_lines(q)
    lines.append("")
    for r in q.accepted:
        basis = " _(last sold)_" if r.value_basis == "lastSold" else ""
        full = (f" ⚡{r.pct_full:.0f}% full" if (r.near_full and r.pct_full is not None)
                else (" ⚡near full" if r.near_full else ""))
        lines.append(f"• `{r.cert}` — **{money(r.comp_value)}**{basis} → "
                     f"{money(r.offer)} — {(r.card_name or '')[:50]} "
                     f"({r.grader} {r.grade}){full}")
    for r in q.rejected:
        lines.append(f"• `{r.cert}` — ❌ {kevin_reason(r)}")
    for r in q.unpriced:
        lines.append(f"• `{r.cert}` — ⚠️ re-check ({r.error})")
    return "\n".join(lines)


def allocation_board(data: dict) -> str:
    """Kevin-facing allocation board (he may see the real figures). Sorted by the
    fullest sub-range; flags ranges at/over the near-full threshold. Category-level
    %-full is often absent, so per-range %-full drives the ⚠️ marks."""
    cats = data.get("categories") or []
    if not cats:
        return "📊 No allocation data available right now."
    hidden = data.get("spendHidden")
    period = (data.get("period") or {}).get("name") or ""
    out = [f"📊 **Allocation board**{(' · ' + period) if period else ''}"
           + (" _(spend hidden — room left only)_" if hidden else "")]

    def mark(p):
        if p is None:
            return ""
        return f" — {p:.0f}% full" + (" ⚠️" if p >= config.NEAR_FULL_PCT else "")

    def peak(c):
        return max([(r.get("pctFull") or 0) for r in (c.get("ranges") or [])] or [0])

    for c in sorted(cats, key=peak, reverse=True):
        label = c.get("categoryLabel") or c.get("category") or "?"
        out.append(f"**{label}** — {money((c.get('leftCents') or 0) / 100)} left{mark(c.get('pctFull'))}")
        for r in c.get("ranges") or []:
            lo = money((r.get("minCents") or 0) / 100)
            hi = money((r.get("maxCents") or 0) / 100)
            out.append(f"  {lo}–{hi}: {money((r.get('leftCents') or 0) / 100)} left{mark(r.get('pctFull'))}")
    note = data.get("reliability")
    if note:
        out.append(f"_{note}_")
    return "\n".join(out)
