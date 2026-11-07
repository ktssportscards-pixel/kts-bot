"""
lots — the per-ticket running lot: accepted slab values by lane + buying minimums.

Lane routing: Pokémon → VA; sports + One Piece → Doral (separate address). NO minimum
lot size — everything drop-ships (the $1,500 floor was removed 2026-11-06). An
unknown/blank category falls into the VA (Pokémon) lane — the least-disruptive default
that never misroutes to Doral.

In-memory per channel (survives a session, not a redeploy). Collectr-CSV singles
are stubbed empty until the CSV module lands; the slab lane logic is complete.
"""
from . import config

# Sports/One Piece categories (Doral lane). Everything else → VA (Pokémon) lane.
SPORTSOP_CATEGORIES = {
    "one piece", "onepiece", "basketball", "baseball", "mlb",
    "football", "nfl", "tcg", "popculture", "pop culture", "other",
}

# channel_id -> {"slab_certs": {cert: value}, "slab_sport": {cert: category},
#                "singles": {game: value}, "rebuilt": bool}
_lots: dict = {}


def _entry(cid):
    return _lots.setdefault(cid, {"slab_certs": {}, "slab_sport": {}, "singles": {}, "rebuilt": False})


def is_sportsop(category: str) -> bool:
    return (category or "").lower().strip() in SPORTSOP_CATEGORIES


def add_slab_values(cid, cert_value: dict, cert_category: dict = None) -> None:
    """Accumulate accepted slab certs (deduped by cert so re-sends don't double-count)."""
    e = _entry(cid)
    e["slab_certs"].update({str(c): float(v or 0) for c, v in (cert_value or {}).items()})
    e["slab_sport"].update({str(c): (cat or "").lower() for c, cat in (cert_category or {}).items()})


def set_singles(cid, totals_by_game: dict) -> None:
    """Latest Collectr CSV is the truth — replace the whole per-game dict."""
    _entry(cid)["singles"] = {g: float(v or 0) for g, v in (totals_by_game or {}).items()
                              if float(v or 0) > 0}


def mark_rebuilt(cid) -> None:
    _entry(cid)["rebuilt"] = True


def drop(cid) -> bool:
    """Forget a channel's running lot entirely (used by the !void command so a killed
    quote can't be rebuilt/shipped). Returns True if there was a lot to drop; pops both
    the raw key and its str form defensively (callers pass channel.id as an int)."""
    gone = _lots.pop(cid, None) is not None
    if _lots.pop(str(cid), None) is not None:
        gone = True
    return gone


def singles_by_game(cid) -> dict:
    s = (_lots.get(cid) or {}).get("singles") or {}
    return {"one piece": float(s.get("one piece", 0)), "pokemon": float(s.get("pokemon", 0))}


def value_by_lane(cid):
    """(pokemon_VA_value, sportsop_Doral_value) for the running lot."""
    e = _lots.get(cid) or {}
    certs = e.get("slab_certs", {}) or {}
    sports = e.get("slab_sport", {}) or {}
    pk = so = 0.0
    for cert, val in certs.items():
        if is_sportsop(sports.get(str(cert))):
            so += float(val or 0)
        else:
            pk += float(val or 0)
    return pk, so


def summary(cid):
    """(slab_count, slab_value, singles_value, combined_value)."""
    e = _lots.get(cid) or {"slab_certs": {}, "singles": {}}
    slab_value = sum(e.get("slab_certs", {}).values())
    slab_count = len(e.get("slab_certs", {}))
    singles = sum((e.get("singles") or {}).values())
    return slab_count, slab_value, singles, slab_value + singles


def is_rebuilt(cid) -> bool:
    return bool((_lots.get(cid) or {}).get("rebuilt"))


def qualifies(cid) -> bool:
    """True if the running lot is non-empty. There is NO minimum lot size anymore
    (MIN_LOT_VALUE / MIN_SLAB_COUNT / POKEMON_SINGLES_MIN_LOT all default 0), so any lot
    with a slab or a single qualifies; shipping no longer holds small sports/OP lots."""
    slab_count, slab_value, singles, combined = summary(cid)
    games = singles_by_game(cid)
    if slab_count == 0 and singles == 0:
        return False
    if games["one piece"] >= config.MIN_LOT_VALUE:
        return True
    if games["pokemon"] >= config.POKEMON_SINGLES_MIN_LOT:
        return True
    if slab_count > 0:
        return combined >= config.MIN_LOT_VALUE and slab_count >= config.MIN_SLAB_COUNT
    return False


def has_lot(cid) -> bool:
    """True if this channel has any accepted slabs on record (survives a session,
    not a redeploy — rebuild_from_sheets restores it after a restart)."""
    e = _lots.get(cid) or {}
    return bool(e.get("slab_certs")) or bool(e.get("singles"))


def rebuild_from_sheets(cid, username, days=4, created_after=None) -> bool:
    """A redeploy wipes the in-memory lot, and shipping would then fail open to the
    VA address — wrong for Sports/One Piece. Rebuild from the customer's recent
    buying sheet(s): every row with comp>0 AND payout>0 (so Kevin's hand-edits —
    e.g. zeroing what Triumph won't take — count), with its category. Later sheets
    win per cert. Returns True if anything was rebuilt."""
    from datetime import datetime, timedelta, timezone
    from . import certs, sheets
    try:
        since_dt = datetime.utcnow() - timedelta(days=days)
        if created_after is not None:
            ca = (created_after.astimezone(timezone.utc).replace(tzinfo=None)
                  if getattr(created_after, "tzinfo", None) else created_after)
            since_dt = max(since_dt, ca - timedelta(minutes=10))
        since = since_dt.strftime("%Y-%m-%dT%H:%M:%S")
        safe = username.replace("'", "\\'")
        drive = sheets.get_drive_service()
        files = drive.files().list(
            q=(f"name contains '{safe} discord' and '{config.PSA_FOLDER_ID}' in parents "
               f"and trashed=false and createdTime > '{since}'"),
            orderBy="createdTime asc", pageSize=50,
            fields="files(id,name,createdTime)").execute().get("files", [])
        files = [f for f in files if f["name"].split(" discord")[0].strip() == username]
        from . import tracking                        # lazy import to avoid a circular import at load
        cutoff = tracking.void_cutoff_of(cid)          # Fix B: after a cancellation, rebuild ONLY from sheets
        if cutoff:                                     # created strictly AFTER it, so a cancelled sheet whose
            try:                                       # Drive trash failed (created before) can't resurrect it
                cut = datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
            except Exception:
                cut = None
            if cut is not None:
                def _after_cutoff(f):
                    try:
                        return datetime.fromisoformat(
                            str(f.get("createdTime", "")).replace("Z", "+00:00")) > cut
                    except Exception:
                        return True                    # undatable sheet → keep (never drop a legit one)
                files = [f for f in files if _after_cutoff(f)]
        if not files:
            return False
        gc = sheets.get_gspread_client()
        vals, cats = {}, {}
        for f in files:
            rows = gc.open_by_key(f["id"]).get_worksheet(0).get_all_values(
                value_render_option="UNFORMATTED_VALUE")
            for r in rows[1:]:
                r = list(r) + [""] * 10
                cert = str(r[1]).strip()
                if cert.endswith(".0"):
                    cert = cert[:-2]
                if not cert.isdigit():
                    continue
                is_cgc = str(r[0]).strip().upper() == "CGC" or str(r[4]).upper().startswith("CGC")
                tok = certs.cgc_cert(cert) if is_cgc else cert
                try:
                    cv, pay = float(r[6] or 0), float(r[8] or 0)   # G=comp, I=payout
                except (TypeError, ValueError):
                    continue
                if cv > 0 and pay > 0:
                    vals[tok] = cv
                    cats[tok] = (str(r[5]).lower().strip() or ("pokemon" if is_cgc else ""))
                else:
                    vals.pop(tok, None)
                    cats.pop(tok, None)
        if not vals:
            return False
        e = _entry(cid)
        e["slab_certs"].update(vals)
        e["slab_sport"].update(cats)
        e["rebuilt"] = True
        return True
    except Exception as ex:
        print(f"[lots] rebuild_from_sheets failed (non-fatal): {ex}")
        return False


def proceed_or_hold_tail(cid):
    """None if the lot qualifies (caller shows its normal 'proceed' prompt), else a
    customer-facing message listing what's missing. NO minimum lot size (everything
    drop-ships), so a non-empty lot always qualifies — this now only catches an empty /
    not-yet-finalized lot."""
    if qualifies(cid):
        return None
    slab_count, slab_value, singles, combined = summary(cid)
    if slab_count == 0 and singles == 0:
        return ("⏳ **Your quote isn't finalized yet** — Kevin is reviewing it and "
                "will confirm shortly. Hang tight!")
    games = singles_by_game(cid)
    parts = []
    if slab_count:
        parts.append(f"PSA slabs: **{slab_count}** (${slab_value:,.2f})")
    if games["one piece"]:
        parts.append(f"One Piece singles: **${games['one piece']:,.2f}**")
    if games["pokemon"]:
        parts.append(f"Pokémon singles: **${games['pokemon']:,.2f}**")
    if len(parts) > 1:
        parts.append(f"Combined: **${combined:,.2f}**")
    return ("📊 **Heads up — this lot doesn't meet our buying minimums yet.**\n\n"
            + "  •  ".join(parts))
