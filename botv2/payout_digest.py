"""
payout_digest — the weekly "who's owed" reminder DM to Kevin.

Payouts run Fridays, so once a week the bot compiles the unpaid `owed` entries
(written by tracking.record_accepted_ship on ship) into a single digest — name,
amount, card count, date, and whether tracking has arrived — so nobody slips
through. Kevin clears each with `!paid` in their ticket.
"""
from . import state


def build_digest() -> str:
    """Compile unpaid owed entries into a digest, or '' if nobody's owed."""
    owed = state.load("owed", list)
    pending = [r for r in owed if not r.get("paid")]
    if not pending:
        return ""
    pending.sort(key=lambda r: r.get("date", ""))
    total = sum(float(r.get("payout", 0) or 0) for r in pending)
    payinfo = state.load("payinfo", dict)   # load once; flag entries with no details on file
    lines = [f"💰 **Payout digest** — {len(pending)} unpaid, **${total:,.2f}** total:", ""]
    missing = 0
    needs_review = 0
    for r in pending:
        track = f" · 📦 `{r['tracking']}`" if r.get("tracking") else " · ⏳ no tracking yet"
        pin = "" if (r.get("user") or "").lstrip("@").strip().lower() in payinfo else " · ❓ no payment info"
        if pin:
            missing += 1
        rev = " · ⚠️ REVIEW before paying" if r.get("review_overlaps") else ""
        if rev:
            needs_review += 1
        lines.append(f"• **{r.get('user', '?')}** — ${float(r.get('payout', 0) or 0):,.2f} "
                     f"({r.get('cards', 0)} cards, {r.get('date', '')}){track}{pin}{rev}")
    tail = ["", "`!owe` for copy-paste payment packets · mark each paid with `!paid` in their ticket."]
    if needs_review:
        tail.insert(1, f"⚠️ {needs_review} need REVIEW before paying (a legacy un-itemised "
                       f"balance that couldn't be auto-verified) — see `!owe`, then `!resolve`.")
    if missing:
        tail.insert(1, f"❓ {missing} missing payment info — `!payinfo set <name> <details>` to add.")
    lines += tail
    return "\n".join(lines)
