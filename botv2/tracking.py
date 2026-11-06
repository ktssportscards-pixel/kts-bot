"""
tracking — snapshot accepted lots to the owed / tickets stores when a customer ships.

Feeds the web server's /tickets + /owed (the local tracker on Kevin's Mac) and sets
up the payout flow. Writes are keyed by channel so a re-ship updates in place rather
than duplicating. State-store backed, so it survives redeploys with a DATA_DIR volume.
"""
import logging
from datetime import date, datetime

from . import config, state

log = logging.getLogger("botv2.tracking")


def _upsert(store_name: str, rec: dict) -> None:
    data = state.load(store_name, list)
    for i, r in enumerate(data):
        if r.get("id") == rec["id"]:
            data[i] = {**r, **rec}
            state.save(store_name, data)
            return
    data.append(rec)
    state.save(store_name, data)


# ── per-cert money ledger (the source of truth for the owed balance) ─────────
# The TICKET carries a per-cert ledger so every card has its own lifecycle:
#       quoted  → accepted (shipped, unpaid) → paid
#   * quoted   — priced on the ticket, not shipped yet          → NOT owed
#   * accepted — shipped, unpaid                                → OWED
#   * paid     — shipped + settled; KEPT in the ledger so a re-ship of the same
#                card (or a later lot that re-lists it) can NEVER re-queue it    → NOT owed
# It is accumulated at QUOTE time, so two quote messages before one ship still
# book every card (the in-memory _last_quote in bot.py only holds the last msg).
# The OWED store row is a pure PROJECTION of this ledger (the outstanding balance),
# so every existing consumer (ops dashboard, counts, portal, iOS tile) is unchanged.
# A legacy owed row that shipped before this ledger existed has no per-cert offers,
# so its EXACT total is carried verbatim in a reserved "_carried" bucket — never
# split into invented per-card prices — and a new card that collides with that
# un-itemised set is flagged for review rather than silently merged or re-counted.

_CARRIED = "_carried"   # reserved ledger key: a legacy, un-itemised shipped balance
_UNKNOWN = "(legacy balance of unknown composition — verify no double-count)"


def _entry(offer, comp, status):
    return {"offer": round(float(offer or 0), 2),
            "comp": round(float(comp or 0), 2), "status": status}


def _carried_lots(ledger: dict) -> list:
    """The carried bucket is a LIST of un-itemised legacy/fallback lots, each with its
    own cert set + amount + status, so a re-ship of ONE sub-lot can be recognised
    instead of folded into an anonymous running total (which would double-count it)."""
    return list((ledger or {}).get(_CARRIED) or [])


def _carried_certs(ledger: dict) -> set:
    """Union of the cert ids across every carried lot (for overlap detection)."""
    out = set()
    for lot in _carried_lots(ledger):
        out |= set(lot.get("certs") or [])
    return out


def _quote_entries(certs: dict, cert_offers: dict) -> dict:
    """Per-cert ledger entries for a quote (status 'quoted'). Keyed by cert; the
    offer is what we pay, the comp is the market value (display/analytics)."""
    certs = certs or {}
    return {cert: _entry(offer, certs.get(cert, 0), "quoted")
            for cert, offer in (cert_offers or {}).items()}


def _merge_quote(ledger: dict, entries: dict) -> tuple:
    """Accumulate freshly-quoted certs into the ledger across quote messages. Returns
    (ledger, review). A cert already accepted or paid is LOCKED (never re-priced or
    downgraded); a cert that sits inside the un-itemised carried amount is NOT itemised
    (that would double-count the same card) and is flagged for review; a still-quoted
    cert takes the latest offer (a genuine re-quote at a new price)."""
    led = dict(ledger or {})
    carried_certs = _carried_certs(led)
    review = []
    for cert, e in entries.items():
        if cert in carried_certs:
            review.append(cert)            # already inside the carried amount → don't itemise
            continue
        cur = led.get(cert)
        if cur and cur.get("status") in ("accepted", "paid"):
            continue                       # locked once shipped
        led[cert] = e
    return led, review


def _sum_ledger(ledger: dict, statuses: tuple) -> tuple:
    """(payout, comp, cards) over ledger entries whose status is in `statuses`,
    including any carried legacy lots whose status qualifies."""
    payout = comp = 0.0
    cards = 0
    for cert, e in (ledger or {}).items():
        if cert == _CARRIED:
            for lot in (e or []):
                if lot.get("status") in statuses:
                    payout += float(lot.get("offer", 0) or 0)
                    comp += float(lot.get("comp", 0) or 0)
                    cards += len(lot.get("certs") or []) or 1
            continue
        if e.get("status") not in statuses:
            continue
        payout += float(e.get("offer", 0) or 0)
        comp += float(e.get("comp", 0) or 0)
        cards += 1
    return round(payout, 2), round(comp, 2), cards


def _project_owed(channel_id, ledger: dict, existing_owed: dict, base: dict, ticket: dict = None) -> dict:
    """Build the owed store row from the ledger: payout = the OUTSTANDING balance
    (accepted + unpaid, plus any still-unpaid carried legacy lots). paid=True once
    nothing is outstanding. Keeps the tracking + ship date already on the row, falling
    back to the ticket's tracking when none was carried (a number dropped before ship)."""
    payout, comp, cards = _sum_ledger(ledger, ("accepted",))
    accepted = {c: e for c, e in (ledger or {}).items()
                if c != _CARRIED and e.get("status") == "accepted"}
    eo = existing_owed or {}
    tk = ticket or {}
    row = {
        "id": f"owed_{channel_id}", "channel_id": channel_id,
        "user": base.get("user") or eo.get("user", ""),
        "date": eo.get("date") or base.get("date"),
        "status": "accepted",
        "paid": payout == 0,
        "certs": {c: e["comp"] for c, e in accepted.items()},
        "cert_payout": {c: e["offer"] for c, e in accepted.items()},
        "payout": payout, "comp": comp, "cards": cards,
        "tracking": eo.get("tracking") or tk.get("tracking") or (tk.get("delivery") or {}).get("number"),
    }
    row["paid_date"] = eo.get("paid_date") if row["paid"] else None
    return row


def _carried_from_legacy(existing_owed: dict) -> dict:
    """A ticket that shipped before the ledger existed has no per-cert offers. Carry
    its EXACT owed total verbatim as a single carried lot (never split into invented
    per-card prices); its status mirrors whether that legacy balance was already paid."""
    return {_CARRIED: [{
        "offer": round(float(existing_owed.get("payout", 0) or 0), 2),
        "comp": round(float(existing_owed.get("comp", 0) or 0), 2),
        "status": "paid" if existing_owed.get("paid") else "accepted",
        "certs": list((existing_owed.get("certs") or {}).keys()),
    }]}


def _migrate_ledger(ledger: dict, existing_owed: dict) -> dict:
    """Seed a legacy balance into the ledger BEFORE any new cert is written, so a quote
    that lands before the ship can't skip migration and drop the old owed total. No-op
    once a ledger (incl. a carried bucket) already exists, or when there is no prior
    owed row. Called at the top of BOTH record_quote and record_accepted_ship."""
    if ledger:
        return dict(ledger)
    if existing_owed:
        return _carried_from_legacy(existing_owed)
    return {}


def _carried_add(ledger: dict, amount: float, certs: list, comp: float) -> list:
    """Fold an UN-ITEMISED lot total (a snapshot with no per-cert offers — a legacy/
    fallback shape) into the carried list, resolving by CERT IDENTITY so no card is ever
    paid twice or silently dropped. Un-itemised lots are inherently ambiguous without
    per-cert offers, so anything that can't be resolved cleanly is FLAGGED, never guessed:
      * shares ANY cert with a PAID lot        → no-op (a settled card is never re-queued)
      * exact cert set of an UNPAID lot        → no-op if same total, else replace (corrected)
      * partially overlaps an UNPAID lot       → flag for review, leave the balance as-is
      * disjoint from every lot, certs known   → a genuinely new lot, appended as accepted
      * NO certs at all (bare total, no id)     → appended as accepted AND flagged for review
    Returns a list of review tokens for the caller to surface. This is what makes the
    un-itemised path correct-by-construction instead of a per-edge patch."""
    amount = round(float(amount or 0), 2)
    nc = set(certs or [])
    lots = _carried_lots(ledger)
    review = []
    if nc:
        for i, lot in enumerate(lots):
            lc = set(lot.get("certs") or [])
            if not (lc & nc):
                continue                               # no shared card → keep looking
            if lot.get("status") == "paid":
                return review                          # a paid card is NEVER re-queued (full or subset overlap)
            if lc == nc:                               # same unpaid cards
                if round(float(lot.get("offer", 0) or 0), 2) != amount:
                    lots[i] = {"offer": amount, "comp": round(float(comp or 0), 2),
                               "status": "accepted", "certs": sorted(nc)}   # corrected total
                    ledger[_CARRIED] = lots
                return review                          # same total → idempotent no-op
            review.append(_UNKNOWN)                    # partial overlap with an unpaid lot → ambiguous
            return review                              # don't guess which cards are new; flag, leave as-is
        lots.append({"offer": amount, "comp": round(float(comp or 0), 2),
                     "status": "accepted", "certs": sorted(nc)})   # disjoint → genuinely new lot
        ledger[_CARRIED] = lots
        return review
    # no cert identity at all → can't reconcile against anything; book it (never silently
    # drop a seller's money) and flag so a possible duplicate is checked by hand
    lots.append({"offer": amount, "comp": round(float(comp or 0), 2),
                 "status": "accepted", "certs": []})
    ledger[_CARRIED] = lots
    review.append(_UNKNOWN)
    return review


def record_accepted_ship(channel_id, snapshot: dict):
    """Record an accepted/shipped lot. `snapshot` = {username, comp, payout, cards,
    certs (cert->market value), cert_offers (cert->offer)}. Promotes the ticket's
    quoted cards to accepted, writes the 'accepted' TICKET (latest snapshot + ledger),
    and PROJECTS the OWED row (the outstanding balance) from the ledger.

    The owed balance ACCUMULATES per cert across unpaid lots and NEVER re-queues a
    paid card: a second lot shipped before the first is paid adds to the balance; a
    re-ship of a paid card, or a later lot that re-lists one, leaves it paid. A legacy
    owed row (no ledger) keeps its exact prior total as a carried amount and flags any
    overlapping new card for review."""
    if not snapshot:
        return None
    certs = dict(snapshot.get("certs") or {})
    cert_offers = dict(snapshot.get("cert_offers") or {})
    base = {
        "user": snapshot.get("username", ""),
        "channel_id": channel_id,
        "date": date.today().isoformat(),
        "certs": certs,
        "comp": round(float(snapshot.get("comp", 0) or 0), 2),
        "payout": round(float(snapshot.get("payout", 0) or 0), 2),
        "cards": int(snapshot.get("cards", 0) or 0),
        "status": "accepted",
    }
    ticket = next((r for r in state.load("tickets", list)
                   if r.get("id") == f"tk_{channel_id}"), None)
    existing = next((r for r in state.load("owed", list)
                     if r.get("id") == f"owed_{channel_id}"), None)
    new_entries = _quote_entries(certs, cert_offers)
    review = list((ticket or {}).get("review_overlaps") or [])

    # seed any legacy balance BEFORE touching the ledger (a quote may already have done it)
    ledger = _migrate_ledger((ticket or {}).get("ledger") or {}, existing)
    carried_certs = _carried_certs(ledger)

    # ship accepts every card quoted so far on the ticket (accumulated across quote msgs);
    # a quoted cert that sits inside the carried amount is dropped + flagged, never promoted
    # (overlap exclusion has to happen AT promotion, not only against this ship's new cards)
    for cert, e in list(ledger.items()):
        if cert == _CARRIED or e.get("status") != "quoted":
            continue
        if cert in carried_certs:
            review.append(cert)
            del ledger[cert]
            continue
        ledger[cert] = {**e, "status": "accepted"}

    # fold in this ship's own itemised cards; a card already shipped stays locked
    for cert, e in new_entries.items():
        if cert in carried_certs:                     # inside an un-itemised carried amount → flag, never double-count
            review.append(cert)
            continue
        cur = ledger.get(cert)
        if cur and cur.get("status") in ("accepted", "paid"):
            continue                                  # locked once shipped — no re-queue, no re-price
        ledger[cert] = {**e, "status": "accepted"}

    # a snapshot with NO per-cert offers carries only a lot TOTAL → book it un-itemised,
    # unless the quote already itemised these cards on the ledger (then it's booked
    # already). Guard: a bare total (no certs) must NOT open a carried lot on top of
    # itemised cards already accepted/paid on this ticket — that would double the lot.
    itemised_present = any(c != _CARRIED and e.get("status") in ("accepted", "paid")
                           for c, e in ledger.items())
    if (not new_entries and base["payout"] and not any(c in ledger for c in certs)
            and not (not certs and itemised_present)):
        review.extend(_carried_add(ledger, base["payout"], list(certs.keys()), base["comp"]))

    # a carried lot with NO cert list can't be overlap-checked against itemised cards,
    # so a shared card could be paid twice → flag the ticket for a manual check
    if any(lot.get("status") != "paid" and not (lot.get("certs") or [])
           for lot in _carried_lots(ledger)) \
            and any(c != _CARRIED and e.get("status") == "accepted" for c, e in ledger.items()):
        review.append(_UNKNOWN)

    review = sorted(set(review))
    tk = {**base, "id": f"tk_{channel_id}", "ledger": ledger}
    if review:
        tk["review_overlaps"] = review
    _upsert("tickets", tk)                            # ticket = latest snapshot (payout stays the quote) + ledger

    owed_rec = _project_owed(channel_id, ledger, existing, base, ticket)
    if review:
        owed_rec["review_overlaps"] = review
        log.warning("owed ledger on channel %s needs review (overlap with a legacy "
                    "un-itemised balance — not re-counted): %s", channel_id, review)
    _upsert("owed", owed_rec)
    return base


def quoted_snapshot(channel_id):
    """Rebuild a ship snapshot ({username,comp,payout,cards,certs}) from the PERSISTED
    ticket — a fallback for when the in-memory _last_quote was wiped by a redeploy, so a
    cert-less "ship" after a redeploy still books the owed entry. None if no ticket."""
    r = next((r for r in state.load("tickets", list)
              if r.get("id") == f"tk_{channel_id}"), None)
    if not r:
        return None
    cert_offers = {c.get("cert"): c.get("offer") for c in (r.get("card_detail") or [])
                   if c.get("cert") is not None and c.get("offer") is not None}
    return {"username": r.get("user", ""), "comp": r.get("comp", 0),
            "payout": r.get("payout", 0), "cards": r.get("cards", 0),
            "certs": r.get("certs", {}), "cert_offers": cert_offers}


def carrier_of(tracking_num: str) -> str:
    """Best-effort carrier from the tracking-number shape (display only — the real
    carrier comes from the aggregator). UPS `1Z…`, USPS 20-22 digits, FedEx 12-15."""
    t = str(tracking_num or "").strip().upper()          # tolerate a non-str (e.g. JSON number)
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
                    # Only (re)open an in_transit record when the NUMBER actually changes.
                    # A re-drop of the SAME number must NOT regress an out_for_delivery or
                    # delivered sub-record back to in_transit (that would let a later scan
                    # re-post the customer 'out for delivery'/'arrived' note a second time).
                    if d.get("number") != tracking_num:
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
    num = str(tracking_num or "").strip().upper()     # case-insensitive; tolerate a non-str
    if not num:
        return None
    for r in state.load("tickets", list):
        stored = str(r.get("tracking") or "").upper()
        dnum = str((r.get("delivery") or {}).get("number") or "").upper()
        if num in (stored, dnum):
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


def mark_out_for_delivery(channel_id=None, tracking_num=None, at=None) -> dict:
    """Flip a ticket's delivery sub-record to 'out_for_delivery' (idempotent; NEVER
    regresses a 'delivered'). Resolve by channel_id, else by tracking number. Returns
    {found, channel_id, already, user, number} — `already` is True when it's already
    out_for_delivery OR already delivered, so the caller won't re-post."""
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
            if d.get("status") in ("out_for_delivery", "delivered"):
                return {"found": True, "channel_id": channel_id, "already": True,
                        "user": r.get("user", ""), "number": number}
            r["delivery"] = {
                "number": number,
                "carrier": d.get("carrier") or carrier_of(number),
                "status": "out_for_delivery",
                "noted_at": d.get("noted_at"),
                "delivered_at": d.get("delivered_at"),
                "out_for_delivery_at": at or datetime.now().isoformat(timespec="seconds"),
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


def tickets_with_open_tracking() -> list:
    """Open tickets (not yet delivered) that carry a tracking number — for a backfill
    registration with the delivery watcher. Returns [{channel_id, tracking, user}]."""
    out = []
    for r in state.load("tickets", list):
        tnum = r.get("tracking") or (r.get("delivery") or {}).get("number")
        if not tnum or (r.get("delivery") or {}).get("status") == "delivered":
            continue
        out.append({"channel_id": r.get("channel_id"), "tracking": tnum, "user": r.get("user", "")})
    return out


def delivery_customer_message(user: str = "") -> str:
    """Customer-safe 'your package arrived' note for the ticket. Deliberately generic —
    no buyer name, no payout, no internal terms — and scrubbed against the forbidden
    list as a belt-and-suspenders guard before it can ever post."""
    msg = "📦 Your package just arrived — got it safely, thank you! We'll take it from here. 🙌"
    if any(w in msg.lower() for w in config.CUSTOMER_FORBIDDEN_WORDS):
        return "📦 Your package just arrived — got it safely, thank you! 🙌"
    return msg


def delivery_ofd_message(user: str = "") -> str:
    """Customer-safe 'out for delivery today' note for the ticket — the heads-up the day
    the carrier has it on the truck. Generic (no buyer name / payout / internal terms),
    scrubbed against the forbidden list as a backstop before it can post."""
    msg = "🚚 Your package is out for delivery today — it should arrive soon!"
    if any(w in msg.lower() for w in config.CUSTOMER_FORBIDDEN_WORDS):
        return "🚚 Your package is out for delivery today!"
    return msg


def delivery_kevin_message(user: str = "", number: str = "") -> str:
    """Kevin-side heads-up when a package is delivered (his DM; may name the number)."""
    who = f"**{user}**'s" if user else "a"
    tail = f" (`{number}`)" if number else ""
    return f"📦 Delivered — {who} package just arrived{tail}. It's in; verify the lot when you get a sec."


def mark_paid(channel_id) -> bool:
    """Flip the owed entry for a channel to paid. Returns True if found. Also marks
    the ticket ledger's accepted cards as paid, so they are KEPT but can never be
    re-queued by a later re-ship or an overlapping new lot (per-cert paid state)."""
    # 1) ledger: accepted → paid (new; retains the cards so a re-ship stays settled),
    #    including every accepted carried lot
    for t in state.load("tickets", list):
        if t.get("id") == f"tk_{channel_id}" and t.get("ledger"):
            led = {}
            for c, e in t["ledger"].items():
                if c == _CARRIED:
                    led[c] = [{**lot, "status": "paid"} if lot.get("status") == "accepted" else lot
                              for lot in (e or [])]
                elif e.get("status") == "accepted":
                    led[c] = {**e, "status": "paid"}
                else:
                    led[c] = e
            _upsert("tickets", {"id": f"tk_{channel_id}", "ledger": led})
            break
    # 2) owed row: unchanged external behaviour (also covers legacy rows with no ledger)
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
    existing_owed = next((r for r in state.load("owed", list)
                          if r.get("id") == f"owed_{channel_id}"), None)
    status = "accepted" if (cur and cur.get("status") == "accepted") else "quoted"
    # Accumulate the per-cert money ledger across quote messages. Seed any legacy balance
    # FIRST (so a quote landing before the ship can't skip migration and drop the old
    # total), then merge this message's cards (latest offer per still-quoted cert;
    # accepted/paid and carried cards are locked). Offers come from card_detail
    # (authoritative per-cert) or the snapshot's cert_offers.
    cert_offers = dict(snapshot.get("cert_offers") or {}) or {
        c.get("cert"): c.get("offer") for c in (card_detail or [])
        if c.get("cert") is not None and c.get("offer") is not None}
    ledger = _migrate_ledger((cur or {}).get("ledger") or {}, existing_owed)
    ledger, review = _merge_quote(ledger, _quote_entries(snapshot.get("certs", {}), cert_offers))
    review = sorted(set(list((cur or {}).get("review_overlaps") or []) + review))
    rec = {
        "id": f"tk_{channel_id}",
        "channel_id": channel_id,
        "channel_name": channel_name or (cur or {}).get("channel_name", ""),
        "user": snapshot.get("username", ""),
        "date": (cur or {}).get("date") or date.today().isoformat(),
        "certs": snapshot.get("certs", {}),
        "card_detail": card_detail if card_detail is not None else (cur or {}).get("card_detail", []),
        "ledger": ledger,
        "comp": round(float(snapshot.get("comp", 0) or 0), 2),
        "payout": round(float(snapshot.get("payout", 0) or 0), 2),
        "cards": int(snapshot.get("cards", 0) or 0),
        "status": status,
        "quoted_at": (cur or {}).get("quoted_at") or datetime.now().isoformat(timespec="seconds"),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    if review:
        rec["review_overlaps"] = review
    _upsert("tickets", rec)


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


def _as_date(s):
    """Parse an ISO date or datetime string to a date (first 10 chars); None if blank/bad."""
    if not s:
        return None
    try:
        return date.fromisoformat(str(s)[:10])
    except ValueError:
        return None


def stale_tickets(today) -> dict:
    """PURE — takes `today` (a date) so it's deterministic for tests (never calls
    date.today() itself). Flags tickets stuck too long in a stage, for Kevin's daily
    nudge. Three MUTUALLY-EXCLUSIVE buckets, each item {user, channel_id, days}, oldest
    (most-stuck) first:
      quoted_stale  — quoted but never shipped (no owed entry) > config.STALE_QUOTED_DAYS
      transit_stale — shipped (owed + unpaid) but not delivered  > config.STALE_TRANSIT_DAYS
      unpaid_stale  — delivered but still unpaid                 > config.STALE_UNPAID_DAYS
    Age is measured from each stage's own timestamp: the quote date, the ship date, and
    the delivered_at respectively. KEVIN-ONLY data (names + channels) — never customer-facing."""
    owed = {r.get("channel_id"): r for r in state.load("owed", list)}
    quoted_stale, transit_stale, unpaid_stale = [], [], []
    for t in state.load("tickets", list):
        cid = t.get("channel_id")
        user = t.get("user", "")
        o = owed.get(cid)
        delivered = (t.get("delivery") or {}).get("status") == "delivered"
        if o is None:                                  # never shipped → age from the quote
            qd = _as_date(t.get("quoted_at")) or _as_date(t.get("date"))
            days = (today - qd).days if qd else None
            if days is not None and days > config.STALE_QUOTED_DAYS:
                quoted_stale.append({"user": user, "channel_id": cid, "days": days})
        elif o.get("paid"):
            continue                                   # paid → nothing stuck
        elif not delivered:                            # in transit too long → age from ship date
            sd = _as_date(o.get("date")) or _as_date(t.get("date"))
            days = (today - sd).days if sd else None
            if days is not None and days > config.STALE_TRANSIT_DAYS:
                transit_stale.append({"user": user, "channel_id": cid, "days": days})
        else:                                          # delivered, unpaid → age from delivered_at
            dd = _as_date((t.get("delivery") or {}).get("delivered_at"))
            days = (today - dd).days if dd else None
            if days is not None and days > config.STALE_UNPAID_DAYS:
                unpaid_stale.append({"user": user, "channel_id": cid, "days": days})
    for b in (quoted_stale, transit_stale, unpaid_stale):
        b.sort(key=lambda x: x["days"], reverse=True)  # most-stuck first
    return {"quoted_stale": quoted_stale, "transit_stale": transit_stale,
            "unpaid_stale": unpaid_stale}


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
