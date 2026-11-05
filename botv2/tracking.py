"""
tracking — snapshot accepted lots to the owed / tickets stores when a customer ships.

Feeds the web server's /tickets + /owed (the local tracker on Kevin's Mac) and sets
up the payout flow. Writes are keyed by channel so a re-ship updates in place rather
than duplicating. State-store backed, so it survives redeploys with a DATA_DIR volume.
"""
from datetime import date

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
