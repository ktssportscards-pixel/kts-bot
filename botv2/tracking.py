"""
tracking — snapshot accepted lots to the owed / tickets stores when a customer ships.

Feeds the web server's /tickets + /owed (the local tracker on Kevin's Mac) and sets
up the payout flow. Writes are keyed by channel so a re-ship updates in place rather
than duplicating. State-store backed, so it survives redeploys with a DATA_DIR volume.
"""
from datetime import date, datetime

from . import state


def _upsert(store_name: str, rec: dict) -> None:
    data = state.load(store_name, list)
    for i, r in enumerate(data):
        if r.get("id") == rec["id"]:
            data[i] = {**r, **rec}
            state.save(store_name, data)
            return
    data.append(rec)
    state.save(store_name, data)


def record_accepted_ship(channel_id, snapshot: dict):
    """Record an accepted lot at ship time. `snapshot` = {username, comp, payout,
    cards, certs}. Writes an 'accepted' ticket + an unpaid owed entry."""
    if not snapshot:
        return None
    base = {
        "user": snapshot.get("username", ""),
        "channel_id": channel_id,
        "date": date.today().isoformat(),
        "certs": snapshot.get("certs", {}),
        "comp": round(float(snapshot.get("comp", 0) or 0), 2),
        "payout": round(float(snapshot.get("payout", 0) or 0), 2),
        "cards": int(snapshot.get("cards", 0) or 0),
        "status": "accepted",
    }
    _upsert("tickets", {**base, "id": f"tk_{channel_id}"})
    _upsert("owed", {**base, "id": f"owed_{channel_id}", "paid": False})
    return base


def record_tracking(channel_id, tracking_num: str) -> bool:
    """Attach a tracking number to this channel's ticket + owed entries (if any)."""
    hit = False
    for store, key in (("tickets", f"tk_{channel_id}"), ("owed", f"owed_{channel_id}")):
        data = state.load(store, list)
        changed = False
        for r in data:
            if r.get("id") == key:
                r["tracking"] = tracking_num
                changed = hit = True
        if changed:
            state.save(store, data)
    return hit


def mark_paid(channel_id) -> bool:
    """Flip the owed entry for a channel to paid. Returns True if found."""
    data = state.load("owed", list)
    hit = False
    for r in data:
        if r.get("id") == f"owed_{channel_id}":
            r["paid"] = True
            r["paid_date"] = date.today().isoformat()
            hit = True
    if hit:
        state.save("owed", data)
    return hit


def record_quote(channel_id, snapshot: dict, channel_name: str = "",
                 card_detail: list = None) -> None:
    """Record/refresh a ticket at QUOTE time (status 'quoted') so OPEN tickets show on
    the ops dashboard before they ship. Upserts the same id ship uses, so a later ship
    just flips it to 'accepted'; never downgrades an already-accepted ticket.
    `card_detail` = per-cert [{cert,value,offer,offer_pct,category,grader,grade,name}]
    (the quote value + what was paid, per card) — feeds the dashboard drill-down AND
    the Triumph-value reconciliation (portal diffs each against the approved value)."""
    if not snapshot:
        return
    cur = next((r for r in state.load("tickets", list)
                if r.get("id") == f"tk_{channel_id}"), None)
    status = "accepted" if (cur and cur.get("status") == "accepted") else "quoted"
    _upsert("tickets", {
        "id": f"tk_{channel_id}",
        "channel_id": channel_id,
        "channel_name": channel_name or (cur or {}).get("channel_name", ""),
        "user": snapshot.get("username", ""),
        "date": (cur or {}).get("date") or date.today().isoformat(),
        "certs": snapshot.get("certs", {}),
        "card_detail": card_detail if card_detail is not None else (cur or {}).get("card_detail", []),
        "comp": round(float(snapshot.get("comp", 0) or 0), 2),
        "payout": round(float(snapshot.get("payout", 0) or 0), 2),
        "cards": int(snapshot.get("cards", 0) or 0),
        "status": status,
        "quoted_at": (cur or {}).get("quoted_at") or datetime.now().isoformat(timespec="seconds"),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    })


def record_claim(channel_id, locked: int, total: int) -> None:
    """Record the auto-claim outcome on this ticket (allocation slots locked)."""
    _upsert("tickets", {"id": f"tk_{channel_id}",
                        "claimed": {"locked": int(locked), "total": int(total),
                                    "at": datetime.now().isoformat(timespec="seconds")}})


def ops_view(channel_sheet: dict = None, detail: bool = False) -> list:
    """Consolidated per-ticket rows for the ops dashboard: merges the tickets store
    (quoted→accepted lifecycle + claim) with the owed store (paid status) and the
    channel→sheet map. One row per ticket; open/unpaid first, newest first.
    detail=True adds `card_detail` (the per-cert quote value + offer) for the dashboard
    drill-down + the Triumph-value reconciliation; default off keeps the summary light."""
    channel_sheet = channel_sheet if channel_sheet is not None else state.load("channel_sheet", dict)
    owed = {r.get("id"): r for r in state.load("owed", list)}
    rows = []
    for t in state.load("tickets", list):
        cid = t.get("channel_id")
        o = owed.get(f"owed_{cid}", {})
        row = {
            "channel_id": cid,
            "name": t.get("channel_name") or t.get("user") or "",
            "user": t.get("user", ""),
            "status": t.get("status", "quoted"),
            "comp": t.get("comp"), "payout": t.get("payout"), "cards": t.get("cards"),
            "sheet_url": channel_sheet.get(str(cid), ""),
            "claimed": t.get("claimed"),
            "shipped": bool(o),
            "tracking": t.get("tracking") or o.get("tracking") or "",
            "owed": o.get("payout") if o else None,
            "paid": bool(o.get("paid")) if o else None,
            "paid_date": o.get("paid_date"),
            "quoted_at": t.get("quoted_at"),
            "updated_at": t.get("updated_at") or t.get("quoted_at"),
        }
        if detail:
            row["card_detail"] = t.get("card_detail", [])
        rows.append(row)
    rows.sort(key=lambda r: r.get("updated_at") or "", reverse=True)   # newest first
    rows.sort(key=lambda r: r.get("paid") is True)                     # open/unpaid first (stable)
    return rows
