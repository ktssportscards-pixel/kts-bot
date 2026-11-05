"""
lots — the per-ticket running lot: accepted slab values by lane + buying minimums.

Ported from v1. Lane routing: Pokémon → VA (no minimum); sports + One Piece →
Doral (needs $1,500+ combined, batched to the separate address). An unknown/blank
category falls into the VA (Pokémon) lane — the least-disruptive default that never
misroutes to Doral or wrongly holds.

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
    """True if the running lot meets the buying minimums for its path (verbatim v1
    logic). With MIN_LOT_VALUE/MIN_SLAB_COUNT at 0 this means 'non-empty'; the
    sports/OP $1,500 LANE gate is enforced separately in shipping."""
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
    customer-facing message listing exactly what's missing (used instead)."""
    pk_lane, so_lane = value_by_lane(cid)
    pk_invite = ", or send **Pokémon** (no minimum)!" if config.POKEMON_LANE_OPEN else "!"
    if 0 < so_lane < config.SPORTS_OP_MIN_LOT:
        short = config.SPORTS_OP_MIN_LOT - so_lane
        if pk_lane > 0:
            return (f"Let me know if you'd like to proceed with your **Pokémon**! "
                    f"⏳ Your **Sports/One Piece** (**${so_lane:,.2f}**) is held — that lane needs "
                    f"**${config.SPORTS_OP_MIN_LOT:,}+** (it ships to a separate address). "
                    f"You're **${short:,.2f}** short, so add more to include it.")
        return (f"📊 Your **Sports & One Piece** slabs total **${so_lane:,.2f}** — the minimum for "
                f"that lane is **${config.SPORTS_OP_MIN_LOT:,}** (it ships to a separate address, so we "
                f"batch it). You're **${short:,.2f}** short. Add more and resubmit" + pk_invite)
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
