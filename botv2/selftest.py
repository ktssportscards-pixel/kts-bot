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
    ksl = pricing.kevin_summary(q, "topguy", ticket_link="<#555>")
    check("**Ticket:** <#555>" in ksl, "kevin summary includes ticket jump link")

    # CGC priced off lastSale: comp uses the portal's `value` (not marketValue), so
    # the shown comp + rate match the app; marketValue stays supplementary.
    cgc = PricedRow(cert="cgc:9", cert_id="9", grader="CGC", grade="9.5",
                    category="pokemon", market_value=100, last_sale=80,
                    value=80, value_basis="lastSold", offer_pct=90,
                    decision="BUY", offer=72)
    check(cgc.comp_value == 80 and cgc.priced, "comp_value prefers portal value")
    qc = pricing.build_quote([cgc])
    check(abs(qc.comp_total - 80) < 0.01, "quote comp_total uses value, not marketValue")
    kc = pricing.kevin_summary(qc, "cgcguy")
    check("$80.00" in kc and "last sold" in kc, "kevin summary shows value + basis")
    # a CGC with only a lastSale-based value and no marketValue is still priced
    # (used to be dropped as 'not found' when priced keyed off marketValue).
    novm = PricedRow(cert="cgc:8", cert_id="8", value=60, value_basis="lastSold",
                     decision="BUY", offer=54)
    check(novm.priced and novm.comp_value == 60, "value-only CGC not dropped")
    # "Allocation Exceeded." must NOT leak the backend word to the customer, but
    # Kevin's DM keeps the real reason.
    alloc = PricedRow(cert="9", cert_id="9", category="basketball", decision="PASS",
                      reason="Allocation Exceeded.")
    qa = pricing.build_quote([alloc])
    check("allocation" not in pricing.customer_message(qa).lower(),
          "customer msg scrubs the 'allocation' reason")
    check("Allocation Exceeded" in pricing.kevin_summary(qa, "someone"),
          "kevin DM keeps the real allocation reason")
    # near-full allocation nudge: soft customer line (no backend word) + Kevin per-card tag
    nf = PricedRow(cert="n1", cert_id="n1", category="onepiece", decision="BUY",
                   offer=90, market_value=100, value=100, near_full=True, pct_full=88.0)
    qn = pricing.build_quote([nf])
    cmn = pricing.customer_message(qn)
    check("filling up" in cmn.lower() and "allocation" not in cmn.lower(),
          "customer near-full nudge (soft, no backend word)")
    check("88% full" in pricing.kevin_summary(qn, "x"), "kevin per-card near-full tag")
    brd = pricing.allocation_board(asyncio.run(portal_client.fetch_allocation()))
    check("Allocation board" in brd and "One Piece" in brd, "allocation board renders")


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

    # ops dashboard view: quote → ship → claim → paid lifecycle
    tracking.record_quote(20, {"username": "dashguy", "comp": 500, "payout": 455,
                               "cards": 9, "certs": {"a": 500}}, channel_name="ticket-99",
                          card_detail=[{"cert": "a", "value": 500, "offer": 455, "category": "baseball"}])
    row = next((r for r in tracking.ops_view({}) if r["channel_id"] == 20), None)
    check(row and row["status"] == "quoted" and row["payout"] == 455 and not row["shipped"]
          and "card_detail" not in row, "ops: quoted ticket shows (summary omits per-cert)")
    drow = next((r for r in tracking.ops_view({}, detail=True) if r["channel_id"] == 20), None)
    check(drow and drow.get("card_detail") and drow["card_detail"][0]["offer"] == 455,
          "ops: detail mode includes per-cert card_detail (value+offer for reconcile)")
    tracking.record_accepted_ship(20, {"username": "dashguy", "comp": 500, "payout": 455,
                                       "cards": 9, "certs": {"a": 500}})
    tracking.record_claim(20, 8, 9)
    row2 = next((r for r in tracking.ops_view({}) if r["channel_id"] == 20), None)
    check(row2["status"] == "accepted" and row2["claimed"]["locked"] == 8
          and row2["shipped"] and row2["paid"] is False,
          "ops: ship→accepted + claim recorded + unpaid owed")


# ── payout flow: payinfo store + packets + !owe + digest flag ───────────────────
def test_payout_flow():
    section("payout flow (payinfo + packets)")
    tracking.state.save("owed", [])       # deterministic stores for this section
    tracking.state.save("payinfo", {})

    # payinfo store via the command parser (multi-line, @-stripped, lowercased)
    r = tracking.payinfo_command("!payinfo set @Topguy Zelle: topguy@example.com\nWire: Chase 1234")
    check("saved" in r.lower() and "topguy" in r, "payinfo set stores + confirms")
    stored = tracking.get_payinfo("TOPGUY")
    check(bool(stored) and "Zelle" in stored["details"] and "Wire: Chase 1234" in stored["details"],
          "payinfo details incl. continuation line, key case-folded")
    check("Zelle" in tracking.payinfo_command("!payinfo topguy"), "payinfo view returns details")
    lst = tracking.payinfo_command("!payinfo list")
    check("topguy" in lst and "Zelle" not in lst, "payinfo list = names only (no details)")
    check("Usage" in tracking.payinfo_command("!payinfo set"), "payinfo set w/o args → usage")

    # owed entries drive the packets
    tracking.record_accepted_ship(301, {"username": "topguy", "comp": 300, "payout": 285, "cards": 2, "certs": {"a": 300}})
    tracking.record_accepted_ship(302, {"username": "nobody", "comp": 100, "payout": 90, "cards": 1, "certs": {"b": 100}})
    owed = tracking.state.load("owed", list)
    pk = tracking.build_payment_packet(next(o for o in owed if o["channel_id"] == 301))
    check("Payment packet" in pk and "$285.00" in pk and "topguy@example.com" in pk,
          "packet: amount + stored details")
    check("double-check before sending" in pk, "packet: flags details changed ≤3 days")
    check("not payment" in pk, "packet: carries bank-review disclaimer")
    pk2 = tracking.build_payment_packet(next(o for o in owed if o["channel_id"] == 302))
    check("No payment details on file" in pk2, "packet: flags missing payinfo")

    # !owe — whole queue, filtered, and empty
    allp = tracking.build_owe_packets()
    check("2 to pay" in allp and "topguy" in allp and "nobody" in allp, "!owe packets the whole unpaid queue")
    one = tracking.build_owe_packets("topguy")
    check("topguy" in one and "nobody" not in one, "!owe <user> filters to that seller")
    check("Nothing unpaid" in tracking.build_owe_packets("ghost"), "!owe <unknown> → nothing owed")

    # duplicate-amount flag: a 2nd unpaid entry, same seller + amount
    tracking.record_accepted_ship(303, {"username": "topguy", "comp": 300, "payout": 285, "cards": 2, "certs": {"c": 300}})
    dup = tracking.build_payment_packet(next(o for o in tracking.state.load("owed", list) if o["channel_id"] == 303))
    check("DUPLICATE" in dup, "packet: flags a duplicate unpaid amount")

    # digest flags the entry with no payment info + points at !owe
    dig = payout_digest.build_digest()
    check("no payment info" in dig and "missing payment info" in dig, "digest: flags missing payinfo")
    check("!owe" in dig, "digest: points at !owe for packets")

    # command plumbing — confidential handling
    class _Chan:
        def __init__(s, guild): s.guild = guild; s.sent = []
        async def send(s, t): s.sent.append(t)
    class _Author:
        def __init__(s, uid): s.id = uid; s.name = "x"; s.bot = False; s.dms = []
        async def send(s, t): s.dms.append(t)
    class _Msg:
        def __init__(s, content, uid=9, guild=None):
            s.content = content; s.author = _Author(uid); s.channel = _Chan(guild); s.deleted = False
        async def delete(s): s.deleted = True

    # non-Kevin !payinfo in a channel → swallowed AND the message is scrubbed
    m = _Msg("!payinfo set bob secret-wire-info", uid=9, guild=object())
    check(asyncio.run(commands.handle(None, m)) is True and m.deleted and not m.channel.sent,
          "cmd: !payinfo in a channel is deleted (details scrubbed), nothing posted")

    # Kevin happy-path: !owe DMs the packet, only a safe note hits the channel
    import botv2.config as _cfg
    _old = _cfg.KEVIN_USER_ID
    try:
        _cfg.KEVIN_USER_ID = 777
        km = _Msg("!owe topguy", uid=777, guild=object())
        check(asyncio.run(commands.handle(None, km)) is True, "cmd: Kevin !owe handled")
        check(bool(km.author.dms) and "topguy" in km.author.dms[0], "cmd: !owe packet → Kevin's DMs")
        check(km.deleted and all("topguy@example.com" not in s for s in km.channel.sent),
              "cmd: !owe leaks no details into the channel")
    finally:
        _cfg.KEVIN_USER_ID = _old

    tracking.state.save("owed", [])       # don't pollute later sections
    tracking.state.save("payinfo", {})


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

    cr = asyncio.run(portal_client.claim_certs(["12345678"], idempotency_key="t"))
    check(len(cr) == 1 and not cr[0].ok and "mock" in cr[0].skipped.lower(),
          "claim client mock (nothing claims by default)")
    ur = asyncio.run(portal_client.unclaim_certs(["12345678"]))
    check(len(ur) == 1, "unclaim client mock")


# ── PSA folder isolation guard ──────────────────────────────────────────────────
def test_psa_folder_guard():
    section("psa folder guard")
    import botv2.config as cfg
    import botv2.sheets as sheets_mod
    old = (cfg.PSA_FOLDER_ID, cfg.ALLOW_SHARED_PSA_FOLDER)
    try:
        cfg.PSA_FOLDER_ID, cfg.ALLOW_SHARED_PSA_FOLDER = cfg.V1_PSA_FOLDER_ID, False
        check(cfg.psa_folder_unsafe(), "v1 folder + no flag -> unsafe")
        # create_psa_sheet must refuse BEFORE any network/Google call
        try:
            sheets_mod.create_psa_sheet("x", ["12345678"])
            check(False, "create_psa_sheet should refuse v1 folder")
        except RuntimeError:
            check(True, "create_psa_sheet refuses v1 folder (no network)")
        except Exception as e:
            check(False, f"wrong error type from guard: {e!r}")
        cfg.ALLOW_SHARED_PSA_FOLDER = True
        check(not cfg.psa_folder_unsafe(), "explicit allow -> safe")
        cfg.PSA_FOLDER_ID, cfg.ALLOW_SHARED_PSA_FOLDER = "v2-own-folder-id", False
        check(not cfg.psa_folder_unsafe(), "v2's own folder -> safe")
    finally:
        cfg.PSA_FOLDER_ID, cfg.ALLOW_SHARED_PSA_FOLDER = old


def main():
    print("botv2 selftest:")
    for t in (test_certs, test_pricing, test_lots_shipping, test_tracking_digest,
              test_payout_flow, test_vip_commands_client, test_psa_folder_guard):
        t()
    if _fails:
        print(f"\nFAILED ({len(_fails)}):")
        for f in _fails:
            print("  ✗", f)
        raise SystemExit(1)
    print("\nALL TESTS PASS")


if __name__ == "__main__":
    main()
