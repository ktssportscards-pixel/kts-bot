"""
selftest — botv2's test suite. No pytest needed: run it directly.

    python -m botv2.selftest        # mock pricing + a temp DATA_DIR
    (exit 0 = all pass, 1 = a failure, printed with its section)

Covers every pure/offline path: cert + tracking parsing, quote building + messages,
VIP flag, lots/lanes/minimums, VA/Doral shipping, tracking record + payout digest,
commands, and the portal client's mock + param plumbing. The live Discord + Google
+ /bot/price paths are exercised separately (they need real creds/endpoint).
"""
import asyncio
import os
import tempfile

# Must be set before importing any botv2 module that reads config at import time.
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="botv2-selftest-"))
os.environ["BOTV2_PRICE_MOCK"] = "1"

from botv2 import (certs, commands, lots, payout_digest, portal_client,  # noqa: E402
                   pricing, shipping, tracking, vip)
from botv2.portal_client import PricedRow                                # noqa: E402

_fails = []


def check(cond, msg):
    if not cond:
        _fails.append(msg)


def section(name):
    print(f"  · {name}")


# ── certs ────────────────────────────────────────────────────────────────────
def test_certs():
    section("certs")
    ec = certs.extract_certs
    check(ec("12345678") == ["12345678"], "single PSA")
    check(ec("12345678 23456789 34567890") == ["12345678", "23456789", "34567890"], "PSA list")
    check(ec("CGC 6193949247") == ["cgc:6193949247"], "labeled CGC")
    check(ec("my cgc card 6193949247") == ["cgc:6193949247"], "CGC-shaped in CGC ctx")
    check(ec("tracking 6193949247") == [], "CGC-shaped tracking ctx excluded")
    check(ec("BGS 0012345678") == [], "other grader excluded")
    check(ec("way too many random words here now ok 12345678") == [], "chatter guard")
    check(ec("psa12345678") == [], "glued label not a cert")
    check(ec("12345678 12345678 23456789") == ["12345678", "23456789"], "dedup order")
    et = certs.extract_tracking
    check(et("1Z999AA10123456784") == "1Z999AA10123456784", "UPS tracking")
    check(et("here 9400111899223817200000 ok") == "9400111899223817200000", "USPS tracking")
    check(et("123456789012") == "123456789012", "FedEx 12 tracking")
    check(et("12345678") == "", "8-digit cert not tracking")
    check(et("1401234567890") == "", "CGC-13 skipped")


# ── pricing / quote ───────────────────────────────────────────────────────────
def test_pricing():
    section("pricing")
    rows = [PricedRow(cert="1", cert_id="1", category="basketball", market_value=300, decision="BUY", offer=285),
            PricedRow(cert="2", cert_id="2", category="baseball", market_value=200, decision="BUY", offer=180, vip=True),
            PricedRow(cert="3", cert_id="3", category="football", market_value=50, decision="PASS", reason="out of band"),
            PricedRow(cert="4", cert_id="4", category="", market_value=None, decision="", status="error", error="timeout")]
    q = pricing.build_quote(rows)
    check(q.n_accepted == 2, "2 accepted")
    check(abs(q.payout_total - 465) < 0.01, "payout sums offers")
    check(len(q.rejected) == 1 and len(q.unpriced) == 1, "reject + unpriced split")
    check(q.vip, "vip flag set from a vip row")
    cm = pricing.customer_message(q)
    check("Total payout" in cm and "VIP rates applied" in cm and "out of band" in cm, "customer msg")
    ks = pricing.kevin_summary(q, "topguy")
    check("✨VIP" in ks and "$285.00" in ks, "kevin summary")


# ── lots + shipping ───────────────────────────────────────────────────────────
def test_lots_shipping():
    section("lots + shipping")
    def fresh(cid): lots._lots.pop(cid, None)

    fresh(1); lots.add_slab_values(1, {"a": 800}, {"a": "basketball"})
    _, gave, note = shipping.compose_shipping(1)
    check(not gave and shipping.ship_blocked_sportsop_only(1), "sportsOP<1500 held")

    fresh(2); lots.add_slab_values(2, {"a": 1200, "b": 900}, {"a": "baseball", "b": "football"})
    _, gave, note = shipping.compose_shipping(2)
    check(gave and "Doral" in note, "sportsOP>=1500 -> Doral")

    fresh(3); lots.add_slab_values(3, {"a": 500, "b": 800}, {"a": "pokemon", "b": "basketball"})
    _, gave, note = shipping.compose_shipping(3)
    check(gave and "VA" in note and "HELD" in note, "mixed: VA ships, sports held")

    fresh(4); lots.add_slab_values(4, {"a": 500}, {"a": "pokemon"})
    _, gave, note = shipping.compose_shipping(4)
    check(gave and "VA" in note, "pokemon only -> VA")

    fresh(5)
    _, gave, note = shipping.compose_shipping(5)
    check(gave and "default" in note, "empty -> fail open VA")

    fresh(6); lots.add_slab_values(6, {"a": 800}, {"a": "basketball"})
    check(lots.proceed_or_hold_tail(6), "proceed tail holds when short")
    fresh(7); lots.add_slab_values(7, {"a": 2000}, {"a": "basketball"})
    check(lots.proceed_or_hold_tail(7) is None, "proceed tail clears when qualified")
    check(lots.has_lot(7) and not lots.has_lot(999), "has_lot")


# ── tracking + digest ─────────────────────────────────────────────────────────
def test_tracking_digest():
    section("tracking + digest")
    tracking.record_accepted_ship(10, {"username": "topguy", "comp": 1500, "payout": 1350, "cards": 3, "certs": {"a": 1500}})
    tracking.record_accepted_ship(11, {"username": "bob", "comp": 800, "payout": 720, "cards": 2, "certs": {"b": 800}})
    check(tracking.record_tracking(10, "1Z999AA10123456784"), "record_tracking")
    d = payout_digest.build_digest()
    check("Payout digest" in d and "topguy" in d and "1Z999AA10123456784" in d, "digest content")
    check("2,070.00" in d, "digest total")
    tracking.record_accepted_ship(10, {"username": "topguy", "payout": 1400})  # idempotent re-ship
    check(len(tracking.state.load("tickets", list)) == 2, "re-ship updates in place")
    check(tracking.mark_paid(10) and "topguy" not in payout_digest.build_digest(), "mark_paid drops from digest")


# ── vip + commands + portal client ────────────────────────────────────────────
def test_vip_commands_client():
    section("vip + commands + client")
    vip.set_override("voxy", {"basketball": 97})
    check(vip.override_for("VOXY") == {"basketball": 97} and vip.override_for("x") is None, "vip lookup")
    vip.clear_override("voxy"); check(vip.override_for("voxy") is None, "vip clear")

    class Chan:
        async def send(self, t): self.sent = t
    class Auth:
        def __init__(s, k): s.id = (0 if k else 9); s.name = "x"; s.bot = False
    class Msg:
        def __init__(s, t, k=False): s.content = t; s.author = Auth(k); s.channel = Chan()
    check(asyncio.run(commands.handle(None, Msg("hello"))) is False, "non-command passes through")
    check(asyncio.run(commands.handle(None, Msg("!ping"))) is True, "!ping handled")
    check(asyncio.run(commands.handle(None, Msg("!adopt", k=False))) is True, "!adopt non-kevin swallowed")

    rows = asyncio.run(portal_client.price_certs(["12345678", "99999999"],
                                                 customer="topguy", rate_override={"basketball": 97}))
    check(len(rows) == 2 and all(r.cert for r in rows), "mock client w/ customer+override")


def main():
    print("botv2 selftest:")
    for t in (test_certs, test_pricing, test_lots_shipping, test_tracking_digest, test_vip_commands_client):
        t()
    if _fails:
        print(f"\nFAILED ({len(_fails)}):")
        for f in _fails:
            print("  ✗", f)
        raise SystemExit(1)
    print("\nALL TESTS PASS")


if __name__ == "__main__":
    main()
