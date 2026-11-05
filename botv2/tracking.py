"""
tracking — snapshot accepted lots to the owed / tickets stores when a customer ships.

Feeds the web server's /tickets + /owed (the local tracker on Kevin's Mac) and sets
up the payout flow. Writes are keyed by channel so a re-ship updates in place rather
than duplicating. State-store backed, so it survives redeploys with a DATA_DIR volume.
"""
from datetime import date, datetime

from . import config, state


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


def carrier_of(tracking_num: str) -> str:
    """Best-effort carrier from the tracking-number shape (display only — the real
    carrier comes from the aggregator). UPS `1Z…`, USPS 20-22 digits, FedEx 12-15."""
    t = (tracking_num or "").strip().upper()
    if t.startswith("1Z"):
        return "UPS"
    if t.isdigit() and 20 <= len(t) <= 22:
        return "USPS"
    if t.isdigit() and 12 <= len(t) <= 15:
        return "FedEx"
    return ""


def record_tracking(channel_id, tracking_num: str) -> bool:
    """Attach a tracking number to this channel's ticket + owed entries (if any), and
    open a `delivery` sub-record on the ticket (status 'in_transit') so the carrier
    'delivered' event can flip it later. Keeps the flat `tracking` string too — the
    local tracker + ops dashboard already read that. Never clobbers a 'delivered'
    state when the same number is re-dropped."""
    hit = False
    for store, key in (("tickets", f"tk_{channel_id}"), ("owed", f"owed_{channel_id}")):
        data = state.load(store, list)
        changed = False
        for r in data:
            if r.get("id") == key:
                r["tracking"] = tracking_num
                if store == "tickets":
                    d = r.get("delivery") or {}
                    if not (d.get("status") == "delivered" and d.get("number") == tracking_num):
                        r["delivery"] = {
                            "number": tracking_num,
                            "carrier": carrier_of(tracking_num),
                            "status": "in_transit",
                            "noted_at": datetime.now().isoformat(timespec="seconds"),
                            "delivered_at": None,
                        }
                changed = hit = True
        if changed:
            state.save(store, data)
    return hit


def find_channel_by_tracking(tracking_num: str):
    """Resolve a tracking number to its ticket's channel_id (scans the tickets store),
    so the delivery webhook can act on a number alone. None if no ticket matches."""
    num = (tracking_num or "").strip()
    if not num:
        return None
    for r in state.load("tickets", list):
        if r.get("tracking") == num or (r.get("delivery") or {}).get("number") == num:
            return r.get("channel_id")
    return None


def mark_delivered(channel_id=None, tracking_num=None, delivered_at=None,
                   carrier=None) -> dict:
    """Flip a ticket's delivery sub-record to 'delivered' (idempotent). Resolve by
    channel_id, else by tracking number. Returns {found, channel_id, already, user,
    number} so the caller can decide whether to post the customer 'arrived' note. The
    delivered state is recorded even when customer messaging is off (ops + Kevin see
    it); only the customer-facing post is gated by config.DELIVERY_TRACKING upstream."""
    if channel_id is None and tracking_num:
        channel_id = find_channel_by_tracking(tracking_num)
    miss = {"found": False, "channel_id": channel_id, "already": False,
            "user": "", "number": (tracking_num or "")}
    if channel_id is None:
        return miss
    data = state.load("tickets", list)
    for r in data:
        if r.get("id") == f"tk_{channel_id}":
            d = r.get("delivery") or {}
            number = d.get("number") or r.get("tracking") or (tracking_num or "")
            if d.get("status") == "delivered":
                return {"found": True, "channel_id": channel_id, "already": True,
                        "user": r.get("user", ""), "number": number}
            r["delivery"] = {
                "number": number,
                "carrier": carrier or d.get("carrier") or carrier_of(number),
                "status": "delivered",
                "noted_at": d.get("noted_at"),
                "delivered_at": delivered_at or datetime.now().isoformat(timespec="seconds"),
            }
            state.save("tickets", data)
            return {"found": True, "channel_id": channel_id, "already": False,
                    "user": r.get("user", ""), "number": number}
    return miss


def delivery_status(channel_id) -> dict:
    """The delivery sub-record for a channel's ticket, or {} if none."""
    for r in state.load("tickets", list):
        if r.get("id") == f"tk_{channel_id}":
            return r.get("delivery") or {}
    return {}


def delivery_customer_message(user: str = "") -> str:
    """Customer-safe 'your package arrived' note for the ticket. Deliberately generic —
    no buyer name, no payout, no internal terms — and scrubbed against the forbidden
    list as a belt-and-suspenders guard before it can ever post."""
    msg = "📦 Your package just arrived — got it safely, thank you! We'll take it from here. 🙌"
    if any(w in msg.lower() for w in config.CUSTOMER_FORBIDDEN_WORDS):
        return "📦 Your package just arrived — got it safely, thank you! 🙌"
    return msg


def delivery_kevin_message(user: str = "", number: str = "") -> str:
    """Kevin-side heads-up when a package is delivered (his DM; may name the number)."""
    who = f"**{user}**'s" if user else "a"
    tail = f" (`{number}`)" if number else ""
    return f"📦 Delivered — {who} package just arrived{tail}. It's in; verify the lot when you get a sec."


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


def counts() -> dict:
    """Quick store counts for the heartbeat DM + /status: how many tickets are still
    open (not yet paid) and how much is owed. A cheap read of the two stores — a ticket
    counts as open until its owed entry is marked paid (a quoted-but-not-shipped ticket
    has no owed entry yet, so it is open too)."""
    owed = state.load("owed", list)
    paid_ids = {r.get("id") for r in owed if r.get("paid")}
    tickets = state.load("tickets", list)
    open_tickets = sum(1 for t in tickets
                       if f"owed_{t.get('channel_id')}" not in paid_ids)
    unpaid = [r for r in owed if not r.get("paid")]
    return {
        "tickets_total": len(tickets),
        "tickets_open": open_tickets,
        "owed_open": len(unpaid),
        "owed_total": round(sum(float(r.get("payout", 0) or 0) for r in unpaid), 2),
    }


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
            "delivered": (t.get("delivery") or {}).get("status") == "delivered",
            "delivered_at": (t.get("delivery") or {}).get("delivered_at"),
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


# ── payment info ("payinfo" store) ───────────────────────────────────────────
# A seller's wire/ACH details, keyed by lowercase username: {details, updated}.
# CONFIDENTIAL — this is Kevin-only, DM-only. It must NEVER reach a customer
# channel or the ops dashboard. The bot only organizes payouts; it never moves
# money, so these details are prep for a manual Mercury wire, nothing more.

def _payinfo_key(user) -> str:
    """Normalize a username to the payinfo store key (strip @, lowercase)."""
    return (user or "").lstrip("@").strip().lower()


def get_payinfo(user):
    """Return the stored {details, updated} for a seller, or None."""
    return state.load("payinfo", dict).get(_payinfo_key(user))


def set_payinfo(user: str, details: str) -> str:
    """Store/replace a seller's payment details. Returns the key used."""
    key = _payinfo_key(user)
    data = state.load("payinfo", dict)
    data[key] = {"details": (details or "").strip(), "updated": date.today().isoformat()}
    state.save("payinfo", data)
    return key


def payinfo_command(content: str) -> str:
    """Parse a `!payinfo …` command and return the reply text for Kevin's DM.
    Forms (multi-line details ok on `set`):
      !payinfo set <user> <wire/ACH details…>   store/replace
      !payinfo <user>                            view
      !payinfo remove <user>                     delete
      !payinfo list                              names + last-updated only
    The caller DMs the return value — details must never post in a channel."""
    raw_lines = (content or "").strip().split("\n")
    toks = raw_lines[0].split()[1:]          # drop the '!payinfo' token
    sub = (toks[0].lower() if toks else "list")
    data = state.load("payinfo", dict)

    if sub == "set":
        if len(toks) < 2:
            return ("Usage: `!payinfo set <username> <their wire/ACH details…>`\n"
                    "(details can span multiple lines — paste the whole thing)")
        key = _payinfo_key(toks[1])
        details = " ".join(toks[2:])
        if len(raw_lines) > 1:                # fold in any continuation lines
            extra = "\n".join(l.rstrip() for l in raw_lines[1:] if l.strip())
            details = (details + "\n" + extra) if details.strip() else extra
        if not details.strip():
            return f"No details given — paste {key}'s wire/ACH info after the username."
        set_payinfo(key, details)
        return f"💾 Payment details saved for **{key}** (updated {date.today().isoformat()})."

    if sub in ("remove", "delete", "del", "rm"):
        key = _payinfo_key(toks[1]) if len(toks) > 1 else ""
        if key in data:
            del data[key]
            state.save("payinfo", data)
            return f"🗑️ Removed payment details for **{key}**."
        return f"No payment details on file for **{key or '?'}**."

    if sub == "list":
        if not data:
            return "No payment details on file yet. `!payinfo set <username> <details>` to add."
        return ("💾 **Payment details on file** (names only — `!payinfo <name>` to view):\n"
                + "\n".join(f"• **{n}** — updated {v.get('updated', '?')}"
                            for n, v in sorted(data.items())))

    # bare "!payinfo <user>" → view the details
    key = _payinfo_key(sub)
    v = data.get(key)
    if not v:
        return f"No payment details on file for **{key}**. `!payinfo set {key} <details>` to add."
    return f"💸 **{key}** — payment details (updated {v.get('updated', '?')}):\n{v.get('details', '')}"


def build_payment_packet(entry: dict, all_unpaid: list = None) -> str:
    """A copy-paste payment packet for ONE unpaid owed entry: amount, memo, and the
    seller's stored wire/ACH details, with safety flags. DM-only — carries details.
    Flags mirror v1: duplicate amount (double-logged?), details changed in the last
    3 days, and no payment info on file. The bot never sends money — this is prep."""
    user = entry.get("user", "")
    key = _payinfo_key(user)
    amount = float(entry.get("payout", 0) or 0)
    info = state.load("payinfo", dict).get(key)
    flags = []

    if all_unpaid is None:
        all_unpaid = [o for o in state.load("owed", list) if not o.get("paid")]
    dup = [o for o in all_unpaid
           if _payinfo_key(o.get("user")) == key
           and abs(float(o.get("payout", 0) or 0) - amount) < 0.005]
    if len(dup) > 1:
        flags.append(f"⚠️ DUPLICATE? {len(dup)} unpaid entries for {user} at "
                     f"${amount:,.2f} — check it isn't logged twice before paying.")
    if info:
        try:
            upd = datetime.strptime(info.get("updated", ""), "%Y-%m-%d").date()
            if 0 <= (date.today() - upd).days <= 3:
                flags.append(f"⚠️ Their payment details changed {info['updated']} "
                             f"(≤3 days ago) — double-check before sending.")
        except ValueError:
            pass

    lines = [f"💸 **Payment packet — {user}**",
             f"Amount: **${amount:,.2f}**",
             f"Memo: KTS payout — {user} — {entry.get('date', date.today().isoformat())} "
             f"({entry.get('cards', 0)} cards)"]
    if info:
        lines.append(f"── details on file (updated {info.get('updated', '?')}) ──")
        lines.append(info.get("details", ""))
    else:
        flags.append(f"⚠️ No payment details on file for {user} — "
                     f"`!payinfo set {key} <their wire/ACH info>`.")
    lines += flags
    lines.append("_Review at the bank before sending — packets are prep, not payment._")
    return "\n".join(lines)


def build_owe_packets(user: str = None) -> str:
    """The DM for `!owe [user]`: a payment packet per unpaid owed entry (all sellers,
    or just the one named), oldest first, with a running total header. DM-only."""
    unpaid = [o for o in state.load("owed", list) if not o.get("paid")]
    if user:
        key = _payinfo_key(user)
        unpaid = [o for o in unpaid if _payinfo_key(o.get("user")) == key]
    if not unpaid:
        who = f" for **{_payinfo_key(user)}**" if user else ""
        return f"✅ Nothing unpaid{who} right now."
    unpaid.sort(key=lambda o: o.get("date", ""))
    total = sum(float(o.get("payout", 0) or 0) for o in unpaid)
    header = f"💸 **{len(unpaid)} to pay — ${total:,.2f} total** (oldest first):"
    packets = [build_payment_packet(o, all_unpaid=unpaid) for o in unpaid]
    return header + "\n\n" + "\n\n".join(packets)
