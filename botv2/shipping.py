"""
shipping — dual-address (VA / Doral) routing + the ship/proceed gate.

Pokémon → Virginia Beach, Sports/One Piece → Doral; a mixed lot ships as two packages.
NO minimum lot size — everything drop-ships (Kevin removed the $1,500 floor 2026-11-06),
so any non-empty lot ships. Fails OPEN (default VA message) when no lot is on record —
e.g. after a redeploy wiped the in-memory lot — so a 'ship' never dead-ends.
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


def compose_shipping(cid):
    """(message, gave_address, kevin_note) for a lot that cleared the ship gate.
    NO minimum lot size (everything drop-ships): any Pokémon → VA, any Sports/One Piece
    → Doral, a mixed lot as two packages. Fails OPEN (VA) when no lot is on record.
    When config.SHIP_ALL_DORAL is on, EVERYTHING goes to the single Doral address instead."""
    pk, so = lots.value_by_lane(cid)
    if config.SHIP_ALL_DORAL:                          # consolidate everything → one Doral package
        head = ("📦 **Awesome, let's do it!** Ship everything to the address below — you'll be paid "
                "out once your package arrives and is processed.\n\n")
        return (head + "📍 **Ship to:**\n" + _DORAL_BLOCK + "\n\n" + _SHIP_METHOD_TAIL, True,
                f"ALL→Doral (consolidated) · pk ${pk:,.2f} + so ${so:,.2f}")
    has_pk, has_so = pk > 0, so > 0
    if not has_pk and not has_so:                      # fail open (e.g. lot wiped by a redeploy)
        return SHIPPING_MSG, True, "VA (default/no lot on record)"
    lanes, kparts = [], []
    if has_pk:
        lanes.append(("🟡 **Pokémon → ship to:**", _VA_BLOCK))
        kparts.append(f"pokemon ${pk:,.2f} → VA")
    if has_so:
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
    return head + body + "\n\n" + _SHIP_METHOD_TAIL, True, " · ".join(kparts)
