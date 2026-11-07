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


def _upsert(store_name: str, rec: dict) -> bool:
    """Insert or merge `rec` (matched by id) into a store. Returns the save result (True on
    persist, False on a failed write) so a caller that must gate on durability can check it."""
    data = state.load(store_name, list)
    for i, r in enumerate(data):
        if r.get("id") == rec["id"]:
            data[i] = {**r, **rec}
            return state.save(store_name, data)
    data.append(rec)
    return state.save(store_name, data)


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
    fallback shape) into the carried list, resolving by CERT IDENTITY against EVERY cert
    already booked on the ticket (itemised entries AND carried lots), so no card is ever
    paid twice or silently dropped. Without per-cert offers a bare total can't be split,
    so anything that can't be resolved cleanly is FLAGGED (never guessed, never dropped):
      * cert set already fully booked (⊆ known):
          - exact match to an UNPAID carried lot → no-op if same total, else replace (corrected)
          - otherwise (covered by a PAID lot and/or itemised certs) → no-op (settled/booked)
      * shares some known certs AND adds genuinely-new cards → flag (can't value the new ones)
      * fully disjoint, certs known → a genuinely new lot, appended accepted
          (also flagged if any existing bare lot has unknown composition — can't be sure)
      * NO certs at all (bare total): flag only if itemised cards already carry the ticket
          (likely a dup → no overpay); else book it + flag (never silently underpay)
    Returns a list of review tokens for the caller to surface. Correct-by-construction:
    every outcome is a clean identity resolution or an explicit review flag."""
    amount = round(float(amount or 0), 2)
    nc = set(certs or [])
    lots = _carried_lots(ledger)
    itemised = {c for c in ledger if c != _CARRIED and ledger[c].get("status") in ("accepted", "paid")}
    known = itemised | _carried_certs(ledger)          # every cert already booked on this ticket
    review = []

    if not nc:                                         # a bare total with NO cert identity
        if itemised:                                   # itemised cards already carry this ticket's money →
            review.append(_UNKNOWN)                    #   likely a dup; flag, don't book (avoid overpay)
            return review
        lots.append({"offer": amount, "comp": round(float(comp or 0), 2),
                     "status": "accepted", "certs": []})
        ledger[_CARRIED] = lots                        # otherwise book it + flag (never silently drop)
        review.append(_UNKNOWN)
        return review

    if nc & known:                                     # overlaps cards already on the ticket
        if nc - known:                                 # ...AND adds genuinely-new cards → can't value the
            review.append(_UNKNOWN)                    #   new ones from a bare total → flag, don't book/drop
            return review
        for i, lot in enumerate(lots):                 # nc ⊆ known: a re-ship of already-booked cards
            if set(lot.get("certs") or []) == nc and lot.get("status") != "paid":
                if round(float(lot.get("offer", 0) or 0), 2) != amount:
                    lots[i] = {"offer": amount, "comp": round(float(comp or 0), 2),
                               "status": "accepted", "certs": sorted(nc)}   # corrected total, same unpaid cards
                    ledger[_CARRIED] = lots
                return review                          # exact unpaid match → idempotent/corrected
        return review                                  # covered by a paid lot and/or itemised certs → settled

    # fully disjoint from every known cert → a genuinely new un-itemised lot
    if any(not (lot.get("certs") or []) for lot in lots):
        review.append(_UNKNOWN)                        # an existing bare lot's composition is unknown →
    lots.append({"offer": amount, "comp": round(float(comp or 0), 2),   #   can't be sure this is really new
                 "status": "accepted", "certs": sorted(nc)})
    ledger[_CARRIED] = lots
    return review


def record_accepted_ship(channel_id, snapshot: dict):
    """Record an accepted/shipped lot. `snapshot` = {username, comp, payout, cards,
    certs (cert->market value), cert_offers (cert->offer)}. Shipping accepts the FULL
    union of still-quoted BUY cards the ticket accumulated across EVERY quote message
    (not just this snapshot), writes the 'accepted' TICKET (latest snapshot + ledger),
    and PROJECTS the OWED row (the outstanding balance) from the ledger.

    The owed balance ACCUMULATES per cert across unpaid lots and NEVER re-queues a
    paid card: a second lot shipped before the first is paid adds to the balance; a
    re-ship of a paid card, or a later lot that re-lists one, leaves it paid. A card
    quoted BUY and never explicitly PASSed is owed even if it's absent from the final
    message — a plain omission is NOT a withdrawal (its allocation slot was claimed at
    quote time; an explicit re-quoted PASS already dropped it at quote time). A legacy
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

    # self-heal a partial mark_paid: if the owed row was acknowledged PAID but the ticket
    # ledger's paid-flag write didn't persist (owed saved, ledger save failed — see
    # mark_paid's ordering note), the certs that paid row covered are settled. Restore
    # their paid status so this re-ship can't reopen them. A no-op in the normal case
    # (a fully-persisted payment already left them 'paid'); genuinely new certs below
    # still open a fresh unpaid lot.
    if existing and existing.get("paid"):
        for cert in (existing.get("cert_payout") or existing.get("certs") or {}):
            e = ledger.get(cert)
            if isinstance(e, dict) and e.get("status") == "accepted":
                ledger[cert] = {**e, "status": "paid"}
        carried = ledger.get(_CARRIED)               # legacy carried lots the paid owed covered are settled too
        if isinstance(carried, list):
            ledger[_CARRIED] = [{**lot, "status": "paid"} if lot.get("status") == "accepted" else lot
                                for lot in carried]

    # COMBINE-ALL-QUOTES: shipping the lot accepts the FULL union of still-quoted BUY cards the
    # ticket accumulated across EVERY quote message — not just this message's snapshot. A card
    # merely absent from the final message is NOT withdrawn: its allocation slot was claimed the
    # instant it was first quoted, so a plain omission keeps it. An explicit re-quoted PASS (the
    # portal declined a re-listed card) already removed its card at quote time. (Already-ACCEPTED
    # certs persist across ships — a ship is a commitment.) A quoted cert sitting inside an
    # un-itemised carried amount is dropped + flagged, since itemising it would double-count money.
    for cert, e in list(ledger.items()):
        if cert == _CARRIED or e.get("status") != "quoted":
            continue
        if cert in carried_certs:
            review.append(cert)
            del ledger[cert]
            continue
        ledger[cert] = {**e, "status": "accepted"}       # every quoted BUY in the lot → accepted/owed

    # fold in this ship's own itemised cards; a card already shipped stays locked
    for cert, e in new_entries.items():
        if cert in carried_certs:                     # inside an un-itemised carried amount → flag, never double-count
            review.append(cert)
            continue
        cur = ledger.get(cert)
        if cur and cur.get("status") in ("accepted", "paid"):
            continue                                  # locked once shipped — no re-queue, no re-price
        ledger[cert] = {**e, "status": "accepted"}

    # a snapshot with NO per-cert offers carries only a lot TOTAL → hand it to _carried_add,
    # which resolves it against every cert already booked on the ticket (itemised + carried)
    # by identity, or flags it when a bare total can't be reconciled (see its docstring)
    if not new_entries and base["payout"]:
        review.extend(_carried_add(ledger, base["payout"], list(certs.keys()), base["comp"]))

    # a carried lot with NO cert list can't be overlap-checked against itemised cards,
    # so a shared card could be paid twice → flag the ticket for a manual check
    if any(lot.get("status") != "paid" and not (lot.get("certs") or [])
           for lot in _carried_lots(ledger)) \
            and any(c != _CARRIED and e.get("status") == "accepted" for c, e in ledger.items()):
        review.append(_UNKNOWN)

    review = sorted(set(review))
    # ticket totals = the WHOLE accumulated lot (ledger union), so the ops dashboard + analytics
    # match the owed union rather than only this ship message's snapshot.
    t_payout, t_comp, t_cards = _sum_ledger(ledger, ("quoted", "accepted", "paid"))
    tk = {**base, "id": f"tk_{channel_id}", "ledger": ledger,
          "comp": t_comp, "payout": t_payout, "cards": t_cards}
    if review:
        tk["review_overlaps"] = review
    _upsert("tickets", tk)                            # ticket = latest snapshot + union totals + ledger

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
    """Flip the owed entry for a channel to paid. Returns True only on a PERSISTED
    payment. Also marks the ticket ledger's accepted cards as paid, so they are KEPT
    but can never be re-queued by a later re-ship or an overlapping new lot.

    Write ORDER is a safety property, not incidental: persist the OWED row FIRST — it
    is the payment-driving projection Kevin actually pays off (/owed, packets) — and
    BAIL if it doesn't persist, changing nothing else. That guarantees we never end
    with the ticket ledger reading 'paid' while owed still reads 'unpaid', the mismatch
    that risks a DOUBLE-PAY off the owed store. Only after owed is safely saved do we
    mark the ledger paid (the secondary projection that keeps the cards from being
    re-queued). A failure on that second write leaves owed=paid — the safe direction:
    record_accepted_ship preserves an already-paid owed row, so the payment is neither
    lost nor silently reopened."""
    # 1) owed row FIRST (also covers legacy rows with no ledger). If there's no owed
    #    row, or the write doesn't persist, change nothing else and report failure.
    data = state.load("owed", list)
    hit = False
    for r in data:
        if r.get("id") == f"owed_{channel_id}":
            r["paid"] = True
            r["paid_date"] = date.today().isoformat()
            hit = True
    if not hit:
        return False
    if not state.save("owed", data):
        return False
    # 2) ledger: accepted → paid (retains the cards so a re-ship stays settled),
    #    including every accepted carried lot. Secondary to the owed write above.
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
    return True


def resolve_review(channel_id) -> list:
    """Kevin has eyeballed an ambiguous ticket — clear its review flag from BOTH the ticket
    and the owed row. Returns the certs/markers that were cleared (empty if none), so the
    command can confirm what it resolved. The money is untouched; this only clears the flag."""
    cleared = []
    for store, key in (("tickets", f"tk_{channel_id}"), ("owed", f"owed_{channel_id}")):
        data = state.load(store, list)
        changed = False
        for r in data:
            if r.get("id") == key and r.get("review_overlaps"):
                cleared = cleared or list(r["review_overlaps"])
                r.pop("review_overlaps", None)
                changed = True
        if changed:
            state.save(store, data)
    return cleared


def find_tickets(channel_id=None, user=None, status=None) -> list:
    """Find ticket rows by channel_id (exact) or username (case-insensitive, @-stripped),
    optionally restricted to a status ('quoted' | 'accepted' | 'paid'). Read-only — used by
    the !void command to resolve a target and gate on status."""
    uname = (user or "").lstrip("@").strip().lower() or None
    out = []
    for r in state.load("tickets", list):
        if channel_id is not None and str(r.get("channel_id")) != str(channel_id):
            continue
        if uname is not None and (r.get("user") or "").lstrip("@").strip().lower() != uname:
            continue
        if status is not None and r.get("status", "quoted") != status:
            continue
        out.append(r)
    return out


def void_ticket(channel_id) -> dict:
    """Completely remove a ticket's bot-side state so its quote can never be accepted,
    shipped, or paid: delete the `tickets` row (which carries the per-cert ledger,
    tracking, delivery sub-record and review flag — all gone with the row), any `owed`
    row, and the `channel_sheet` cache entry. Returns a summary of what was removed —
    {found, channel_id, user, channel_name, status, cards, payout, had_owed, sheet_url} —
    or {found: False} if there was no ticket. `ok` is True only when every store write
    persisted; ok=False means a write failed partway — the function BAILS with the ticket
    row left intact so a re-run recovers.

    ORDER is a safety property: remove `owed` FIRST and the `tickets` row LAST, so a
    persistence failure can never orphan an owed row behind an already-deleted ticket (the
    ticket is the anchor a retry keys on). The CALLER drops the in-memory lot, clears
    bot._last_quote, and (best-effort) trashes the Drive sheet at sheet_url — but ONLY on
    ok=True; this function only touches the persisted stores. Idempotent (a second call
    finds nothing)."""
    tk_id = f"tk_{channel_id}"
    tickets = state.load("tickets", list)
    row = next((r for r in tickets if r.get("id") == tk_id), None)
    # Do NOT early-return when the ticket is absent: a prior void may have deleted the ticket
    # but failed on a later write, leaving residual owed/channel_sheet. This is an idempotent
    # FULL PURGE — it clears whatever bot-side state remains for the channel, so a retry (even
    # with the ticket already gone) completes the cleanup rather than stalling at not_found.
    cid_val = (row or {}).get("channel_id", channel_id)
    keys = {str(channel_id), str(cid_val)}
    cs = state.load("channel_sheet", dict)
    sheet_url = next((cs[k] for k in keys if cs.get(k)), "")   # READ now; delete LAST (step 3)
    summary = {
        "found": row is not None,                    # a ticket existed at call time
        "ok": True,
        "ticket_removed": False,                     # set True once THIS call deletes the ticket row
        "channel_id": cid_val,
        "user": (row or {}).get("user", ""),
        "channel_name": (row or {}).get("channel_name", ""),
        "status": (row or {}).get("status", "quoted"),
        "cards": (row or {}).get("cards", 0),
        "payout": round(float((row or {}).get("payout", 0) or 0), 2),
        "had_owed": False, "sheet_url": sheet_url,
    }
    # 0) cancellation intent FIRST — persist the voided marker BEFORE deleting any reconstruction
    #    anchor (ticket ledger / in-memory lot / Drive sheet), and bail if it can't persist, so we
    #    never remove the anchors while the durable guard is missing. Only when a real ticket exists.
    if row is not None and not mark_voided(channel_id):
        summary["ok"] = False
        return summary
    # 1) owed — removing it before the ticket means a failed write never leaves an orphaned
    #    owed row behind a deleted ticket. Bail (everything intact) if it won't persist.
    owed = state.load("owed", list)
    had_owed = any(r.get("id") == f"owed_{channel_id}" for r in owed)
    summary["had_owed"] = had_owed
    if had_owed and not state.save("owed", [r for r in owed if r.get("id") != f"owed_{channel_id}"]):
        summary["ok"] = False
        return summary                               # ticket + owed + cache all intact → retry recovers
    # 2) tickets row NEXT (the anchor). Bail if it won't persist: owed already gone is
    #    idempotent, and the channel_sheet (hence sheet_url) is still intact for a retry. Once
    #    this succeeds the quote is DEAD — ticket_removed tells the caller to clear in-memory
    #    state + trash the sheet NOW (even if the cache write below later fails), so a ship can
    #    never reconstruct the cancelled quote.
    if row is not None:
        if not state.save("tickets", [r for r in tickets if r.get("id") != tk_id]):
            summary["ok"] = False
            return summary
        summary["ticket_removed"] = True
    # 3) channel_sheet cache LAST — only once the ticket is gone. A cache-write failure is still
    #    an incomplete void (ok=False), but it is recoverable: a retry re-enters here (even with
    #    the ticket already gone) and removes the entry. The ticket/owed are already consistent.
    if any(k in cs for k in keys):
        for k in keys:
            cs.pop(k, None)
        if not state.save("channel_sheet", cs):
            summary["ok"] = False
    return summary


# ── durable void marker ─────────────────────────────────────────────────────────
# The Drive sheet trash on void is best-effort; if it fails (or a crash/concurrent ship
# races it), rebuild_from_sheets could resurrect the cancelled lot from the un-trashed
# buying sheet (it queries trashed=false sheets by username). This persisted marker is the
# belt-and-suspenders: void sets it, a fresh quote clears it, and _handle_ship refuses to
# rebuild a voided channel — so a cancelled quote can't reconstruct through ANY boundary.
_VOIDED = "voided"


def mark_voided(channel_id) -> bool:
    """Persist cancellation intent. Returns True if the marker is durably set (including already
    present), False if a needed save did not persist. void_ticket records this BEFORE deleting any
    reconstruction anchor (ticket ledger / lot / sheet) and bails if it fails — so a cancelled
    quote never loses its durable guard while its anchors are being removed."""
    try:
        data = state.load(_VOIDED, list)
        key = str(channel_id)
        if key in data:
            return True
        data.append(key)
        return bool(state.save(_VOIDED, data))
    except Exception as e:
        print(f"[tracking] mark_voided failed (non-fatal): {e}")
        return False


def clear_voided(channel_id) -> None:
    """A fresh quote reactivates the channel — drop any voided marker so a later rebuild works."""
    try:
        data = state.load(_VOIDED, list)
        key = str(channel_id)
        if key in data:
            state.save(_VOIDED, [x for x in data if x != key])
    except Exception as e:
        print(f"[tracking] clear_voided failed (non-fatal): {e}")


def is_voided(channel_id) -> bool:
    try:
        return str(channel_id) in state.load(_VOIDED, list)
    except Exception:
        return False


def record_quote(channel_id, snapshot: dict, channel_name: str = "",
                 card_detail: list = None, passed: list = None) -> None:
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
    # An explicit PASS on a re-quote (the portal declined a card we'd quoted BUY) drops a
    # STILL-QUOTED card from the lot. A card merely OMITTED from a later message is NOT a
    # withdrawal (its allocation slot was claimed at quote time) — only an explicit PASS
    # removes it; accepted/paid cards are LOCKED (a shipped card can't be un-shipped by a
    # later PASS, and it stays in card_detail for the margin reconcile).
    dropped = []
    for cert in (passed or []):
        e = ledger.get(cert)
        if isinstance(e, dict) and e.get("status") == "quoted":
            del ledger[cert]
            dropped.append(cert)
    review = sorted(set(list((cur or {}).get("review_overlaps") or []) + review))
    # card_detail is the UNION of BUY cards across quote messages, each cert exactly ONCE: the
    # portal reconcile sums it 1:1 with NO dedup, so a cert emitted twice double-counts the card
    # (count, paid total, and row list). Latest row wins per re-quoted cert; an explicitly-passed
    # card is removed; a card merely omitted from this message stays booked (combine-all).
    merged_cd = {c.get("cert"): c for c in ((cur or {}).get("card_detail") or [])
                 if c.get("cert") is not None}
    for c in (card_detail or []):
        cert = c.get("cert")
        if cert is None:
            continue
        led = ledger.get(cert)
        if isinstance(led, dict) and led.get("status") in ("accepted", "paid"):
            continue                          # a shipped/paid card is LOCKED — don't re-price its row
        merged_cd[cert] = c
    for cert in dropped:
        merged_cd.pop(cert, None)
    # The ticket's comp/payout/cards reflect the WHOLE accumulated lot (the ledger union across
    # every quote message), not just this message — so the ops dashboard + !stats analytics match
    # the owed union instead of under-counting a multi-message lot by its earlier messages.
    t_payout, t_comp, t_cards = _sum_ledger(ledger, ("quoted", "accepted", "paid"))
    if not t_payout and not t_cards:        # a bare-total quote (no per-cert ledger yet) keeps its total
        t_payout = round(float(snapshot.get("payout", 0) or 0), 2)
        t_comp = round(float(snapshot.get("comp", 0) or 0), 2)
        t_cards = int(snapshot.get("cards", 0) or 0)
    rec = {
        "id": f"tk_{channel_id}",
        "channel_id": channel_id,
        "channel_name": channel_name or (cur or {}).get("channel_name", ""),
        "user": snapshot.get("username", ""),
        "date": (cur or {}).get("date") or date.today().isoformat(),
        "certs": snapshot.get("certs", {}),
        "card_detail": list(merged_cd.values()),
        "ledger": ledger,
        "comp": t_comp,
        "payout": t_payout,
        "cards": t_cards,
        "status": status,
        "quoted_at": (cur or {}).get("quoted_at") or datetime.now().isoformat(timespec="seconds"),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    if review:
        rec["review_overlaps"] = review
    if _upsert("tickets", rec):                       # only reactivate the channel (clear the voided
        clear_voided(channel_id)                      # guard) once the replacement quote DURABLY persists


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
        _rev = list(t.get("review_overlaps") or o.get("review_overlaps") or [])
        row["needs_review"] = bool(_rev)       # ambiguous ticket — Kevin must eyeball before payout
        row["review_certs"] = _rev
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
    review = list(entry.get("review_overlaps") or [])
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
    if review:                                    # ambiguous ticket — make it UNMISSABLE before paying
        certs = [str(c) for c in review if c != _UNKNOWN]
        bits = []
        if certs:
            bits.append("card(s) that may already sit inside a legacy un-itemised balance "
                        "(couldn't auto-verify — check they aren't double-counted): " + ", ".join(certs))
        if _UNKNOWN in review:
            bits.append("a legacy un-itemised balance of unknown composition")
        lines.insert(1, "⚠️ **NEEDS REVIEW before paying** — " + "; ".join(bits)
                     + ". Make sure the amount above isn't double-counting a card already paid "
                       "or already inside this balance, then `!resolve` in the ticket to clear it.")
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
    n_review = sum(1 for o in unpaid if o.get("review_overlaps"))
    if n_review:
        header += (f"\n⚠️ **{n_review} need review before paying** — flagged below "
                   f"(a legacy un-itemised balance that couldn't be auto-verified). "
                   f"Check for no double-count, then `!resolve` in each ticket.")
    packets = [build_payment_packet(o, all_unpaid=unpaid) for o in unpaid]
    return header + "\n\n" + "\n\n".join(packets)
