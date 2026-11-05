"""
analytics — volume + payout aggregates over the ticket/owed stores, for the portal's
analytics view and a Kevin-only `!stats` summary.

VOLUME + PAYOUT ONLY. "Comp" is the CardLadder/quote value and "payout" is what Kevin
pays the customer (the offer) — both non-confidential. MARGIN (what Triumph pays Kevin)
is confidential and computed PORTAL-SIDE off Kevin's payout rate; the bot never sees it,
so nothing here references it. Everything is computed in Python over the stored rows.
"""
from datetime import date, datetime, timedelta

from . import state


def _as_date(s):
    """Parse an ISO timestamp ('2026-11-05T06:10:33') or date ('2026-11-05') to a date."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s)).date()
    except ValueError:
        try:
            return date.fromisoformat(str(s)[:10])
        except ValueError:
            return None


def _ticket_date(t):
    return _as_date(t.get("quoted_at")) or _as_date(t.get("date"))


def _rnd(d):
    return {k: (round(v, 2) if isinstance(v, float) else v) for k, v in d.items()}


def summary(days: int = None) -> dict:
    """Aggregate volume + payout over the tickets store, optionally limited to the last
    `days` (by quote/record date). Returns totals, lifecycle status counts, per-category
    and top-customer breakdowns. No margin — that stays portal-side."""
    tickets = state.load("tickets", list)
    owed = {r.get("id"): r for r in state.load("owed", list)}
    cutoff = (date.today() - timedelta(days=days)) if days else None

    totals = {"tickets": 0, "cards": 0, "comp": 0.0, "payout": 0.0}
    by_category = {}        # category -> {cards, comp, payout}
    by_customer = {}        # user -> {tickets, cards, comp, payout}
    open_n = shipped_n = paid_n = 0

    for t in tickets:
        td = _ticket_date(t)
        if cutoff and (td is None or td < cutoff):
            continue
        cid = t.get("channel_id")
        o = owed.get(f"owed_{cid}", {})
        comp = float(t.get("comp") or 0)
        payout = float(t.get("payout") or 0)
        cards = int(t.get("cards") or 0)
        user = t.get("user") or t.get("channel_name") or "(unknown)"

        totals["tickets"] += 1
        totals["cards"] += cards
        totals["comp"] += comp
        totals["payout"] += payout

        c = by_customer.setdefault(user, {"tickets": 0, "cards": 0, "comp": 0.0, "payout": 0.0})
        c["tickets"] += 1
        c["cards"] += cards
        c["comp"] += comp
        c["payout"] += payout

        if o:                       # an owed entry exists => the lot shipped / was accepted
            shipped_n += 1
            if o.get("paid"):
                paid_n += 1
        elif t.get("status") == "quoted":
            open_n += 1

        for cd in t.get("card_detail") or []:
            cat = (cd.get("category") or "other").lower()
            g = by_category.setdefault(cat, {"cards": 0, "comp": 0.0, "payout": 0.0})
            g["cards"] += 1
            g["comp"] += float(cd.get("value") or 0)
            g["payout"] += float(cd.get("offer") or 0)

    by_category = {k: _rnd(v) for k, v in
                   sorted(by_category.items(), key=lambda x: -x[1]["payout"])}
    custs = [{"user": u, **_rnd(v)} for u, v in by_customer.items()]

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "window_days": days,
        "totals": _rnd(totals),
        "status": {"open": open_n, "shipped": shipped_n, "paid": paid_n},
        "by_category": by_category,
        "top_customers_by_payout": sorted(custs, key=lambda c: -c["payout"])[:10],
        "top_customers_by_cards": sorted(custs, key=lambda c: -c["cards"])[:10],
    }


def summary_text(days: int = None) -> str:
    """Compact Kevin-facing summary for the `!stats` DM."""
    s = summary(days)
    t, st = s["totals"], s["status"]
    win = f"last {days}d" if days else "all time"

    def money(v):
        return f"${(v or 0):,.2f}"

    lines = [
        f"📈 **Stats ({win})** — {t['tickets']} tickets | {t['cards']} cards | "
        f"comp {money(t['comp'])} → payout {money(t['payout'])}",
        f"open {st['open']} · shipped {st['shipped']} · paid {st['paid']}",
    ]
    if s["by_category"]:
        lines.append("**By category:**")
        for cat, g in list(s["by_category"].items())[:8]:
            lines.append(f"• {cat}: {g['cards']} cards, {money(g['comp'])} → {money(g['payout'])}")
    if s["top_customers_by_payout"]:
        lines.append("**Top customers (payout):**")
        for c in s["top_customers_by_payout"][:5]:
            lines.append(f"• {c['user']}: {money(c['payout'])} "
                         f"({c['cards']} cards, {c['tickets']} tix)")
    return "\n".join(lines)
