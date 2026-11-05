"""
shipping — dual-address (VA / Doral) routing + the ship/proceed gate.

Ported from v1. Pokémon → Virginia Beach (no minimum); qualifying Sports/One Piece
→ Doral (needs $1,500+). A mixed lot ships as two packages. Sports/OP under the
minimum is held (no FL address) while any Pokémon still ships to VA. Fails OPEN
(default VA message) when no lot is on record — e.g. after a redeploy wiped the
in-memory lot — so a 'ship' never dead-ends.
"""
from . import config, lots

AGREE_KEYWORDS = ("ship", "proceed")

_VA_BLOCK = (
    "Kevin Smith\n"
    "3630 S Plaza Trail\n"
    "Suite 150-E\n"
    "Virginia Beach, VA 23452"
)
_DORAL_BLOCK = (
    "Kevin Smith\n"
    "8266 NW 14th St\n"
    "Doral, FL 33126-1703"
)
_SHIP_METHOD_TAIL = (
    "🚚 **Shipping method — required:** **UPS Next Day Air A.M.** (overnight, morning delivery) — "
    "nothing slower (no standard overnight, 2-Day, or ground). Packages **must be in hand by "
    "Tuesday 9 AM**, so ship **Monday at the latest**.\n\n"
    "📥 **Packaging:**\n"
    "• **Slabs:** ship as-is — no sleeves or stickers.\n"
    "• **Raw cards:** penny sleeve only — no top loaders.\n\n"
    "🗓️ **Payouts run Fridays.** First come, first serve — you join the queue the moment your "
    "package is scanned in on arrival.\n"
    "• In hand **Tuesday** → usually paid that **Friday**\n"
    "• Arrives later in the week → rolls into the **next** Friday payout.\n\n"
    "👉 Best move: **overnight it Monday so it lands Tuesday morning** to make that week's payout. "
    "Payment via wire or ACH ⚡\n\n"
    "Once you've shipped, **drop your tracking number here** so Kevin can keep an eye out!"
)
# Fail-open default (no lot on record): single VA address.
SHIPPING_MSG = ("📦 **Awesome, let's do it!** Ship your cards to the address below — you'll be paid "
                "out once your package arrives and is processed.\n\n"
                "🟡 **Ship to:**\n" + _VA_BLOCK + "\n\n" + _SHIP_METHOD_TAIL)


def looks_like_agree(text: str) -> bool:
    """A short 'ship'/'proceed' message (not buried in a longer sentence of chatter)."""
    t = (text or "").lower().strip()
    return any(k in t for k in AGREE_KEYWORDS) and len(t.split()) <= 4


def ship_blocked_sportsop_only(cid) -> bool:
    """True when the ONLY thing on the lot is sports/OP under the $1,500 minimum
    (nothing can ship yet). Any Pokémon on the lot clears this."""
    pk, so = lots.value_by_lane(cid)
    return so > 0 and so < config.SPORTS_OP_MIN_LOT and pk <= 0


def compose_shipping(cid):
    """(message, gave_address, kevin_note) for a lot that cleared the ship gate."""
    pk, so = lots.value_by_lane(cid)
    rebuilt = lots.is_rebuilt(cid)
    so_ok = so >= config.SPORTS_OP_MIN_LOT or (rebuilt and so > 0)
    has_pk, has_so = pk > 0, so > 0
    if not has_pk and not has_so:                      # fail open
        return SHIPPING_MSG, True, "VA (default/no lot on record)"
    if has_so and not so_ok and not has_pk:            # sports/OP short, no pokemon → pure hold
        short = config.SPORTS_OP_MIN_LOT - so
        pk_invite = (" — or send any **Pokémon** (no minimum) and I'll get you shipping right away! 🙌"
                     if config.POKEMON_LANE_OPEN else "! 🙌")
        msg = (f"📊 **Almost there!** Your Sports & One Piece slabs total **${so:,.2f}** — our minimum "
               f"for that lane is **${config.SPORTS_OP_MIN_LOT:,}** (Sports/One Piece ship to a separate "
               f"address, so we batch them). You're **${short:,.2f}** short.\n\n"
               f"Add more Sports/One Piece slabs to hit **${config.SPORTS_OP_MIN_LOT:,}+** and resubmit"
               + pk_invite)
        return msg, False, f"HELD — sports/OP ${so:,.2f} < ${config.SPORTS_OP_MIN_LOT:,}, no pokemon"

    lanes, kparts = [], []
    if has_pk:
        lanes.append(("🟡 **Pokémon → ship to:**", _VA_BLOCK))
        kparts.append(f"pokemon ${pk:,.2f} → VA")
    if has_so and so_ok:
        lanes.append(("🔵 **Sports & One Piece → ship to:**", _DORAL_BLOCK))
        kparts.append(f"sports/OP ${so:,.2f} → Doral")
    if len(lanes) == 2:
        head = ("📦 **Awesome, let's do it!** Your lot splits across two lanes — please ship as "
                "**two separate packages** to the addresses below (you're paid once each arrives and "
                "is processed).\n\n")
        body = "\n\n".join(f"{label}\n{block}" for label, block in lanes)
    else:
        label, block = lanes[0]
        head = ("📦 **Awesome, let's do it!** Ship your cards to the address below — you'll be paid out "
                "once your package arrives and is processed.\n\n")
        body = f"{label}\n{block}"
    tail = ""
    if has_so and not so_ok:                           # pokemon ships, sports/OP held
        short = config.SPORTS_OP_MIN_LOT - so
        tail = (f"\n\n⏳ **Heads up:** your Sports/One Piece (**${so:,.2f}**) is under the "
                f"**${config.SPORTS_OP_MIN_LOT:,}** minimum for that lane (it ships separately to our FL "
                f"address). You're **${short:,.2f}** short — add more and I'll send that address too. "
                f"Your Pokémon above is good to ship now!")
        kparts.append(f"sports/OP ${so:,.2f} HELD (<${config.SPORTS_OP_MIN_LOT:,})")
    if rebuilt:
        kparts.append("lot rebuilt from sheets after a restart — $1,500 min not re-checked")
    return head + body + tail + "\n\n" + _SHIP_METHOD_TAIL, True, " · ".join(kparts)
