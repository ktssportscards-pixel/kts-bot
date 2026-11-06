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

    fresh(1); lots.add_slab_values(1, {"a": 80}, {"a": "basketball"})
    _, gave, note = shipping.compose_shipping(1)
    check(gave and "Doral" in note and "HELD" not in note,
          "no minimum: a small sports-only lot still ships to Doral")

    fresh(2); lots.add_slab_values(2, {"a": 1200, "b": 900}, {"a": "baseball", "b": "football"})
    _, gave, note = shipping.compose_shipping(2)
    check(gave and "Doral" in note, "sports -> Doral")

    fresh(3); lots.add_slab_values(3, {"a": 500, "b": 80}, {"a": "pokemon", "b": "basketball"})
    _, gave, note = shipping.compose_shipping(3)
    check(gave and "VA" in note and "Doral" in note and "HELD" not in note,
          "mixed: both VA + Doral ship, no hold")

    fresh(4); lots.add_slab_values(4, {"a": 500}, {"a": "pokemon"})
    _, gave, note = shipping.compose_shipping(4)
    check(gave and "VA" in note, "pokemon only -> VA")

    fresh(5)
    _, gave, note = shipping.compose_shipping(5)
    check(gave and "default" in note, "empty -> fail open VA")

    fresh(6); lots.add_slab_values(6, {"a": 80}, {"a": "basketball"})
    check(lots.proceed_or_hold_tail(6) is None, "proceed tail clears a small lot (no minimum)")
    fresh(7); lots.add_slab_values(7, {"a": 2000}, {"a": "basketball"})
    check(lots.proceed_or_hold_tail(7) is None, "proceed tail clears when qualified")
    fresh(8)
    check(lots.proceed_or_hold_tail(8), "proceed tail: empty lot returns a 'not finalized' note")
    check(lots.has_lot(7) and not lots.has_lot(999), "has_lot")

    # SHIP_ALL_DORAL override: every ship consolidates to the single Doral address (no VA).
    from botv2 import config as _cfg
    _saved_all_doral = _cfg.SHIP_ALL_DORAL
    _cfg.SHIP_ALL_DORAL = True
    try:
        fresh(9); lots.add_slab_values(9, {"a": 500, "b": 800}, {"a": "pokemon", "b": "basketball"})
        msg, gave, _ = shipping.compose_shipping(9)
        check(gave and "Doral, FL" in msg and "Virginia" not in msg,
              "SHIP_ALL_DORAL: mixed lot → Doral only (no VA)")
    finally:
        _cfg.SHIP_ALL_DORAL = _saved_all_doral


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


# ── alerts (rate limiter) + /status counts ──────────────────────────────────────
def test_alerts_status():
    section("alerts + status")
    from botv2 import alerts
    alerts.reset()
    # A key fires once, is suppressed for the window, then fires again past it — so an
    # error storm DMs Kevin once, not hundreds of times.
    check(alerts.should_alert("k", now=0.0), "first alert for a key fires")
    check(not alerts.should_alert("k", now=10.0), "same key within window suppressed")
    check(alerts.should_alert("other", now=10.0), "a different key still fires")
    check(not alerts.should_alert("k", now=alerts.ALERT_WINDOW_S - 1), "still suppressed at window edge")
    check(alerts.should_alert("k", now=alerts.ALERT_WINDOW_S + 1), "same key fires again past the window")
    # last_error feeds /status and updates even when the DM itself was rate-limited.
    alerts.record_error("boom", "it broke in a big way")
    le = alerts.last_error()
    check(le and le["key"] == "boom" and "broke" in le["text"] and le.get("at"),
          "last_error recorded with a timestamp")
    alerts.reset()
    check(alerts.last_error() is None, "reset clears last_error")
    # counts() shape — what the heartbeat DM + /status read.
    c = tracking.counts()
    check({"tickets_total", "tickets_open", "owed_open", "owed_total"} <= set(c),
          "counts has the expected keys")
    check(isinstance(c["owed_total"], (int, float)) and c["tickets_open"] <= c["tickets_total"],
          "counts values sane (open ≤ total, owed_total numeric)")


# ── analytics (volume + payout only; NO margin) ──────────────────────────────────
def test_analytics():
    section("analytics")
    import json
    import botv2.analytics as analytics
    # Unique usernames/channels so we don't collide with other tests' seeded tickets.
    tracking.record_quote(30, {"username": "alice_a", "comp": 300, "payout": 270,
                               "cards": 2, "certs": {}}, channel_name="alice_a",
                          card_detail=[{"cert": "a", "value": 200, "offer": 180, "category": "baseball"},
                                       {"cert": "b", "value": 100, "offer": 90, "category": "basketball"}])
    tracking.record_quote(31, {"username": "bob_b", "comp": 500, "payout": 455,
                               "cards": 1, "certs": {}}, channel_name="bob_b",
                          card_detail=[{"cert": "c", "value": 500, "offer": 455, "category": "baseball"}])
    tracking.record_accepted_ship(31, {"username": "bob_b", "comp": 500, "payout": 455,
                                       "cards": 1, "certs": {}})
    tracking.mark_paid(31)

    s = analytics.summary()
    check(s["totals"]["tickets"] >= 2 and s["totals"]["cards"] >= 3 and s["totals"]["payout"] > 0,
          "analytics: totals aggregate")
    bob = next((c for c in s["top_customers_by_payout"] if c["user"] == "bob_b"), None)
    check(bob is not None and abs(bob["payout"] - 455) < 0.01 and bob["cards"] == 1 and bob["tickets"] == 1,
          "analytics: per-customer aggregate (bob_b)")
    check(s["status"]["paid"] >= 1 and s["status"]["shipped"] >= 1, "analytics: lifecycle status counts")
    check("baseball" in s["by_category"] and s["by_category"]["baseball"]["cards"] >= 2,
          "analytics: by-category from card_detail")

    # CONFIDENTIAL BOUNDARY: margin is portal-side; it must never appear in the bot's output.
    check("margin" not in json.dumps(s).lower(), "analytics: no confidential margin field")

    sw = analytics.summary(days=7)
    check(sw["window_days"] == 7 and "totals" in sw, "analytics: windowed summary")

    txt = analytics.summary_text()
    check("Stats" in txt and "payout" in txt, "analytics: !stats summary text")


# ── delivery tracking: record → mark_delivered → customer-safe note ─────────────
def test_delivery():
    section("delivery tracking")
    from botv2 import config
    # A quote opens a ticket; dropping a tracking number opens an in_transit delivery.
    tracking.record_quote(50, {"username": "ship_sam", "comp": 400, "payout": 360,
                               "cards": 1, "certs": {"a": 400}}, channel_name="ship_sam")
    check(tracking.record_tracking(50, "1Z888BB20999888777"), "delivery: tracking recorded")
    d = tracking.delivery_status(50)
    check(d.get("status") == "in_transit" and d.get("number") == "1Z888BB20999888777"
          and d.get("carrier") == "UPS", "delivery: in_transit sub-record + carrier inferred")
    check(tracking.find_channel_by_tracking("1Z888BB20999888777") == 50,
          "delivery: resolve channel by tracking number")
    check(tracking.find_channel_by_tracking("1z888bb20999888777") == 50,
          "delivery: tracking match is case-insensitive")

    # Mark delivered BY TRACKING NUMBER (what the webhook carries), then idempotency.
    res = tracking.mark_delivered(tracking_num="1Z888BB20999888777")
    check(res["found"] and res["channel_id"] == 50 and not res["already"]
          and res["user"] == "ship_sam", "delivery: mark_delivered resolves + flips")
    ds = tracking.delivery_status(50)
    check(ds["status"] == "delivered" and ds["delivered_at"], "delivery: status + timestamp set")
    again = tracking.mark_delivered(channel_id=50)
    check(again["found"] and again["already"], "delivery: idempotent (already delivered)")

    # Mark delivered BY CHANNEL_ID for a ticket whose number was never recorded.
    tracking.record_quote(51, {"username": "noeta", "comp": 200, "payout": 180,
                               "cards": 1, "certs": {"b": 200}}, channel_name="noeta")
    r2 = tracking.mark_delivered(channel_id=51)
    check(r2["found"] and not r2["already"], "delivery: mark_delivered by channel_id")

    # Unknown tracking number → graceful miss (no crash).
    miss = tracking.mark_delivered(tracking_num="1ZDOESNOTEXIST999")
    check(not miss["found"] and miss["channel_id"] is None, "delivery: unknown tracking → miss")

    # ops dashboard surfaces the delivered flag.
    drow = next((r for r in tracking.ops_view({}) if r["channel_id"] == 50), None)
    check(drow and drow["delivered"] is True and drow["delivered_at"],
          "delivery: ops row shows delivered")

    # CUSTOMER SAFETY: the 'arrived' note names no buyer and no forbidden word.
    msg = tracking.delivery_customer_message("ship_sam")
    low = msg.lower()
    check(msg and not any(w in low for w in config.CUSTOMER_FORBIDDEN_WORDS)
          and "payout" not in low, "delivery: customer note is scrubbed + buyer-safe")
    check(tracking.carrier_of("1Z999AA10123456784") == "UPS"
          and tracking.carrier_of("9400100000000000000000") == "USPS"
          and tracking.carrier_of("123456789012") == "FedEx", "delivery: carrier_of shapes")


# ── register-tracking: bot → portal watch + backfill list ────────────────────────
def test_register_tracking():
    section("register-tracking")
    # portal_client.register_tracking (mock) returns a started watch; empty is a no-op.
    r = asyncio.run(portal_client.register_tracking(60, "1Z777CC30111222333"))
    check(r.get("ok") and r.get("registered") and r.get("tracking") == "1Z777CC30111222333",
          "register_tracking: mock returns a started watch")
    check(asyncio.run(portal_client.register_tracking(60, "")).get("ok") is False,
          "register_tracking: empty tracking is a no-op")

    # tickets_with_open_tracking: only OPEN (not delivered) tickets that carry a number.
    tracking.record_quote(61, {"username": "backfill_bob", "comp": 300, "payout": 270,
                               "cards": 1, "certs": {}}, channel_name="backfill_bob")
    tracking.record_tracking(61, "1Z777CC30999000111")
    tracking.record_quote(62, {"username": "no_track", "comp": 100, "payout": 90,
                               "cards": 1, "certs": {}}, channel_name="no_track")   # no tracking #
    tracking.record_quote(63, {"username": "already_del", "comp": 200, "payout": 180,
                               "cards": 1, "certs": {}}, channel_name="already_del")
    tracking.record_tracking(63, "1Z777CC30222333444")
    tracking.mark_delivered(channel_id=63)             # delivered → excluded from backfill
    pend = tracking.tickets_with_open_tracking()
    chans = {p["channel_id"] for p in pend}
    check(61 in chans and 62 not in chans and 63 not in chans,
          "tickets_with_open_tracking: open+tracked only (skips untracked + delivered)")
    bob = next((p for p in pend if p["channel_id"] == 61), None)
    check(bob and bob["tracking"] == "1Z777CC30999000111" and bob["user"] == "backfill_bob",
          "tickets_with_open_tracking: carries tracking + user")


# ── live lanes board: customer-safe open/filling/full, NO numbers ────────────────
def test_lanes_board():
    section("lanes board")
    import re

    from botv2 import config
    mock = {"spendHidden": False, "reliability": "approximate", "categories": [
        {"category": "basketball", "categoryLabel": "Basketball", "ranges": [{"pctFull": 20.0}]},
        {"category": "baseball", "categoryLabel": "Baseball", "ranges": [{"pctFull": 92.0}]},
        {"category": "onepiece", "categoryLabel": "One Piece", "ranges": [{"pctFull": 100.0}]},
        {"category": "football", "categoryLabel": "Football", "ranges": [{"pctFull": None}]},
    ]}
    board = pricing.lanes_board(mock)

    def line_for(lbl):
        return next((ln for ln in board.splitlines() if lbl in ln), "")
    check("🟢" in line_for("Basketball"), "lanes: <85% full → open 🟢")
    check("🟡" in line_for("Baseball"), "lanes: 85-99% full → filling 🟡")
    check("🔴" in line_for("One Piece"), "lanes: >=100% full → full 🔴")
    check("🟢" in line_for("Football"), "lanes: unknown %-full → treated as open 🟢")

    # CONFIDENTIALITY: no buyer word (esp. 'allocation'), and no number / % / $ anywhere.
    low = board.lower()
    check(not any(w in low for w in config.CUSTOMER_FORBIDDEN_WORDS),
          "lanes: no forbidden word (no 'allocation'/buyer name)")
    check(re.search(r"[0-9%$]", board) is None, "lanes: no digit, %, or $ in the customer board")

    # A label that would leak a buyer name is dropped entirely; safe ones stay.
    leaky = pricing.lanes_board({"categories": [
        {"categoryLabel": "Triumph VIP", "ranges": [{"pctFull": 10.0}]},
        {"categoryLabel": "Baseball", "ranges": [{"pctFull": 10.0}]}]})
    check("triumph" not in leaky.lower() and "Baseball" in leaky,
          "lanes: a forbidden-word label is dropped, safe ones kept")

    # spendHidden → everything shows open, still no numbers.
    hidden = pricing.lanes_board({"spendHidden": True, "categories": [
        {"categoryLabel": "Basketball", "ranges": [{"pctFull": 99.0}]}]})
    check("🟢" in hidden and re.search(r"[0-9%$]", hidden) is None,
          "lanes: spendHidden → open + no numbers")

# ── stale-ticket nudges: pure stale_tickets(today) bucketing ─────────────────────
def test_stale_tickets():
    section("stale-ticket nudges")
    from datetime import date, timedelta
    from botv2 import config
    anchor = date(2026, 6, 15)

    def ago(n):
        return (anchor - timedelta(days=n)).isoformat()

    tks = tracking.state.load("tickets", list)
    owd = tracking.state.load("owed", list)

    def add_ticket(cid, user, qdays, delivered_ago=None, tracking_num=None):
        rec = {"id": f"tk_{cid}", "channel_id": cid, "user": user,
               "status": "accepted" if (delivered_ago is not None or tracking_num) else "quoted",
               "date": ago(qdays), "quoted_at": ago(qdays) + "T12:00:00"}
        if delivered_ago is not None:
            rec["delivery"] = {"number": tracking_num or "", "status": "delivered",
                               "delivered_at": ago(delivered_ago) + "T12:00:00"}
        elif tracking_num:
            rec["delivery"] = {"number": tracking_num, "status": "in_transit", "delivered_at": None}
        tks.append(rec)

    def add_owed(cid, user, shipdays, paid=False):
        owd.append({"id": f"owed_{cid}", "channel_id": cid, "user": user,
                    "date": ago(shipdays), "payout": 100.0, "paid": paid})

    # UNIQUE channel ids (970-976) + membership asserts, so other suites' seeded data
    # (dated ~today, which is in the FUTURE relative to the 2026-06-15 anchor) can't interfere.
    add_ticket(970, "qstale", 10)                                                       # quoted 10d, no owed
    add_ticket(971, "qfresh", 2)                                                        # quoted 2d
    add_ticket(972, "tstale", 8, tracking_num="1ZT972"); add_owed(972, "tstale", 8)     # shipped 8d, in transit
    add_ticket(973, "tfresh", 1, tracking_num="1ZT973"); add_owed(973, "tfresh", 1)     # shipped 1d, in transit
    add_ticket(974, "ustale", 9, delivered_ago=5, tracking_num="1ZU974"); add_owed(974, "ustale", 9)  # delivered 5d, unpaid
    add_ticket(975, "dpaid", 9, delivered_ago=5, tracking_num="1ZU975"); add_owed(975, "dpaid", 9, paid=True)  # delivered + PAID
    add_ticket(976, "dfresh", 9, delivered_ago=1, tracking_num="1ZU976"); add_owed(976, "dfresh", 9)  # delivered 1d, unpaid

    tracking.state.save("tickets", tks)
    tracking.state.save("owed", owd)

    res = tracking.stale_tickets(anchor)
    q = {x["channel_id"] for x in res["quoted_stale"]}
    tr = {x["channel_id"] for x in res["transit_stale"]}
    up = {x["channel_id"] for x in res["unpaid_stale"]}
    check(970 in q and 971 not in q, "stale: quoted-stale flagged, fresh quote excluded")
    check(972 in tr and 973 not in tr, "stale: transit-stale flagged, fresh transit excluded")
    check(974 in up and 975 not in up and 976 not in up,
          "stale: delivered-unpaid flagged; paid + fresh excluded")
    check(974 not in tr and 972 not in up, "stale: buckets mutually exclusive (delivered vs transit)")
    item = next((x for x in res["quoted_stale"] if x["channel_id"] == 970), None)
    check(item and item["days"] == 10 and item["user"] == "qstale",
          "stale: item carries user + days-stuck")
    _ = config.STALE_QUOTED_DAYS  # thresholds exist (defaults 5/5/3; the bucketing above exercises them)

# ── delivery: out-for-delivery intermediate stage ────────────────────────────────
def test_delivery_ofd():
    section("delivery out-for-delivery")
    from botv2 import config
    tracking.record_quote(55, {"username": "ofd_olivia", "comp": 300, "payout": 270,
                               "cards": 1, "certs": {}}, channel_name="ofd_olivia")
    tracking.record_tracking(55, "1Z555OFD00111222333")
    r = tracking.mark_out_for_delivery(tracking_num="1Z555OFD00111222333")
    check(r["found"] and r["channel_id"] == 55 and not r["already"],
          "ofd: marks out_for_delivery (by tracking #)")
    check(tracking.delivery_status(55)["status"] == "out_for_delivery", "ofd: status set")
    check(tracking.mark_out_for_delivery(channel_id=55)["already"],
          "ofd: idempotent (already out_for_delivery)")
    # a delivered event, then a late out-for-delivery event, must NOT regress the ticket.
    tracking.mark_delivered(channel_id=55)
    check(tracking.delivery_status(55)["status"] == "delivered", "ofd: delivered after OFD")
    noreg = tracking.mark_out_for_delivery(channel_id=55)
    check(noreg["already"] and tracking.delivery_status(55)["status"] == "delivered",
          "ofd: never regresses a delivered ticket")
    check(not tracking.mark_out_for_delivery(tracking_num="1ZNOPE00000")["found"],
          "ofd: unknown tracking → graceful miss")
    m = tracking.delivery_ofd_message("ofd_olivia")
    low = m.lower()
    check(m and not any(w in low for w in config.CUSTOMER_FORBIDDEN_WORDS)
          and "payout" not in low, "ofd: message is customer-safe")


# ── webserver endpoint auth (the HTTP blind spot that let the public-leak bug ship) ──
def test_webserver_auth():
    section("webserver auth")
    from botv2 import webserver
    from botv2 import config as cfg

    class _Req:
        def __init__(self, key=None, query=None, body=None):
            self.headers = {"x-bot-key": key} if key else {}
            self.query = query or {}
            self._body = body

        async def json(self):
            if self._body is None:
                raise ValueError("no body")
            return self._body

    loop = asyncio.new_event_loop()

    def st(coro):
        return loop.run_until_complete(coro).status

    saved = cfg.BOT_API_KEY
    cfg.BOT_API_KEY = "selftestkey"
    try:
        check(st(webserver._health(_Req())) == 200, "auth: /health stays public")
        gated = [("ops", webserver._ops), ("status", webserver._status),
                 ("analytics", webserver._analytics)]
        for name, h in gated:
            check(st(h(_Req())) == 401, f"auth: /{name} rejects a missing key")
            check(st(h(_Req(key="selftestkey"))) == 200, f"auth: /{name} allows a valid key")
        check(st(webserver._delivery(_Req(body={}))) == 401, "auth: /delivery rejects a missing key")
        import json as _j
        ops = _j.loads(loop.run_until_complete(webserver._ops(_Req(key="selftestkey"))).body.decode())
        check(all(isinstance(r["channel_id"], str) for r in ops.get("tickets", []) if "channel_id" in r),
              "auth: /ops emits channel_id as a JS-safe string")
        stat = _j.loads(loop.run_until_complete(webserver._status(_Req(key="selftestkey"))).body.decode())
        check(isinstance(stat.get("flags"), dict) and "delivery_tracking" in stat["flags"]
              and "ship_all_doral" in stat["flags"] and "lanes_schedule" in stat["flags"],
              "status: non-secret flags readout present")
        cfg.BOT_API_KEY = ""                          # fails closed when no key is configured
        check(st(webserver._ops(_Req(key="anything"))) == 401,
              "auth: fails closed when BOT_API_KEY is unset")
    finally:
        cfg.BOT_API_KEY = saved
        loop.close()


# ── audit fixes: paid-regression, cold-quote ship, numeric-tracking tolerance, re-drop ──
def test_audit_fixes():
    section("audit fixes")
    # (1) record_accepted_ship must NOT regress an already-PAID owed entry of the same amount.
    snap = {"username": "regress_ray", "comp": 300, "payout": 270, "cards": 2, "certs": {}}
    tracking.record_accepted_ship(880, snap)
    check(tracking.mark_paid(880), "fix: owed entry created + marked paid")
    tracking.record_accepted_ship(880, snap)          # a repeat "ship" of the same lot after !paid
    owed = next(r for r in tracking.state.load("owed", list) if r["id"] == "owed_880")
    check(owed.get("paid") is True, "fix: re-ship after paid does NOT regress to unpaid (no double-pay)")
    tracking.record_accepted_ship(880, {**snap, "payout": 500})   # a genuinely different lot
    owed2 = next(r for r in tracking.state.load("owed", list) if r["id"] == "owed_880")
    check(owed2.get("paid") is False, "fix: a different-amount lot re-opens as unpaid")

    # (1b) SAME payout but DIFFERENT certs = a new lot → must re-open as unpaid (Codex edge:
    # payout-total alone false-positived; the guard also compares the cert set).
    tracking.record_accepted_ship(883, {"username": "edge", "comp": 300, "payout": 270,
                                         "cards": 2, "certs": {"A": 150, "B": 150}})
    tracking.mark_paid(883)
    tracking.record_accepted_ship(883, {"username": "edge", "comp": 300, "payout": 270,
                                         "cards": 2, "certs": {"A": 150, "B": 150}})   # re-ship SAME lot
    check(next(r for r in tracking.state.load("owed", list) if r["id"] == "owed_883").get("paid") is True,
          "fix: re-ship of the same certs+total stays paid")
    tracking.record_accepted_ship(883, {"username": "edge", "comp": 300, "payout": 270,
                                         "cards": 2, "certs": {"C": 135, "D": 135}})   # new certs, same total
    check(next(r for r in tracking.state.load("owed", list) if r["id"] == "owed_883").get("paid") is False,
          "fix: new certs at the same total re-open as unpaid (no underpay)")

    # (2) quoted_snapshot rebuilds a ship snapshot from the persisted ticket (cold _last_quote).
    tracking.record_quote(881, {"username": "cold_cora", "comp": 100, "payout": 90,
                                "cards": 1, "certs": {}}, channel_name="cold_cora")
    qs = tracking.quoted_snapshot(881)
    check(qs and qs["payout"] == 90 and qs["username"] == "cold_cora",
          "fix: quoted_snapshot rebuilds from the persisted ticket")
    tracking.record_accepted_ship(881, qs)
    check(any(r["id"] == "owed_881" for r in tracking.state.load("owed", list)),
          "fix: cold-quote ship still books an owed entry")
    check(tracking.quoted_snapshot(99999) is None, "fix: quoted_snapshot is None for an unknown channel")

    # (3) a numeric (JSON int) tracking number must not crash carrier_of / find_channel_by_tracking.
    check(tracking.carrier_of(123456789012) == "FedEx", "fix: carrier_of tolerates an int")
    check(tracking.find_channel_by_tracking(999000111222) is None,
          "fix: find_channel_by_tracking tolerates an int")

    # (4) a re-drop of the SAME number must not regress an out_for_delivery sub-record.
    tracking.record_quote(882, {"username": "ofd_oona", "comp": 50, "payout": 45,
                                "cards": 1, "certs": {}}, channel_name="ofd_oona")
    tracking.record_tracking(882, "1Z882SAME00111")
    tracking.mark_out_for_delivery(channel_id=882)
    tracking.record_tracking(882, "1Z882SAME00111")   # same number re-dropped
    check(tracking.delivery_status(882)["status"] == "out_for_delivery",
          "fix: same-number re-drop keeps out_for_delivery (no regress)")

    # (5) Discord snowflakes must serialize as JSON STRINGS (JS safe-int); other ints untouched.
    from botv2 import webserver as ws
    s = ws._stringify_snowflakes({"channel_id": 1492170401608896533, "uptime": 42,
                                  "rows": [{"channel_id": 123, "message_id": 9, "offer_pct": 95}]})
    check(s["channel_id"] == "1492170401608896533" and s["uptime"] == 42
          and s["rows"][0]["channel_id"] == "123" and s["rows"][0]["message_id"] == "9"
          and s["rows"][0]["offer_pct"] == 95,
          "fix: snowflake ids -> strings, non-snowflake ints untouched")


# ── lanes board weekly schedule window (Sat 3 AM -> Mon 3 AM ET) ──────────────────
def test_lanes_schedule():
    section("lanes schedule window")
    from datetime import datetime, timedelta
    from botv2 import bot as b

    base = datetime(2026, 1, 5, 0, 0)                 # any anchor; derive weekdays via weekday()

    def at(target_wd, hr):
        d = base
        while d.weekday() != target_wd:
            d += timedelta(days=1)
        return d.replace(hour=hr)

    check(not b._lanes_in_window(at(2, 12)), "wed -> closed")
    check(not b._lanes_in_window(at(4, 23)), "fri night -> closed")
    check(not b._lanes_in_window(at(5, 2)), "sat 2am -> closed")
    check(b._lanes_in_window(at(5, 3)), "sat 3am -> open")
    check(b._lanes_in_window(at(5, 18)), "sat evening -> open")
    check(b._lanes_in_window(at(6, 12)), "sun -> open")
    check(b._lanes_in_window(at(0, 2)), "mon 2am -> still open")
    check(not b._lanes_in_window(at(0, 3)), "mon 3am -> closed")
    check(not b._lanes_in_window(at(0, 10)), "mon 10am -> closed")
    check(0 < b._seconds_until_lanes_open() <= 7 * 86400, "until-open is within a week")


# ── owed ACCUMULATION across unpaid lots (the Codex/portal money-ledger fix) ──────
def test_owed_accumulation():
    section("owed accumulation (per-cert ledger)")

    def snap(certs, offers):
        return {"username": "acc", "comp": round(sum(certs.values()), 2),
                "payout": round(sum(offers.values()), 2), "cards": len(offers),
                "certs": certs, "cert_offers": offers}

    def cd(certs, offers):   # card_detail as the bot passes it to record_quote
        return [{"cert": c, "value": certs[c], "offer": offers[c]} for c in offers]

    def quote(cid, certs, offers):    # one QUOTE message (accumulates the ledger)
        tracking.record_quote(cid, snap(certs, offers), card_detail=cd(certs, offers))

    def ship(cid, certs, offers):     # SHIP (snapshot = that one message, as in the bot)
        tracking.record_accepted_ship(cid, snap(certs, offers))

    def owed(cid):
        return next((r for r in tracking.state.load("owed", list) if r["id"] == f"owed_{cid}"), {})

    def seed_legacy(cid, row):        # a pre-ledger owed row (shipped before the ledger existed)
        data = [r for r in tracking.state.load("owed", list) if r["id"] != f"owed_{cid}"]
        data.append({"id": f"owed_{cid}", "channel_id": cid, "user": "old", "status": "accepted", **row})
        tracking.state.save("owed", data)
        tracking.state.save("tickets", [t for t in tracking.state.load("tickets", list)
                                        if t["id"] != f"tk_{cid}"])   # ensure NO ticket ledger

    # (A) Codex #3 — TWO quote messages then ONE ship books BOTH batches, even though the
    #     ship snapshot only carries the last message (the in-memory _last_quote is replaced).
    quote(912, {"A": 150, "B": 150}, {"A": 135, "B": 135})   # msg 1
    quote(912, {"C": 100, "D": 100}, {"C": 90, "D": 90})     # msg 2
    ship(912, {"C": 100, "D": 100}, {"C": 90, "D": 90})      # ship snapshot = msg 2 only
    check(owed(912).get("payout") == 450.0 and owed(912).get("cards") == 4,
          "two quote msgs then one ship book BOTH batches ($270+$180=$450, 4 cards)")

    # (B) two unpaid SHIPS on one ticket accumulate (no prior quote → direct ship path)
    ship(913, {"A": 150, "B": 150}, {"A": 135, "B": 135})    # $270
    ship(913, {"C": 100}, {"C": 90})                          # +$90 before paid
    check(owed(913).get("payout") == 360.0 and owed(913).get("cards") == 3,
          "two unpaid ships accumulate ($360, 3 cards)")

    # (C) re-quote a cert at a new price BEFORE ship → latest offer wins
    quote(914, {"A": 150}, {"A": 135})
    quote(914, {"A": 150}, {"A": 140})                        # same cert, re-quoted higher
    ship(914, {"A": 150}, {"A": 140})
    check(owed(914).get("payout") == 140.0, "re-quote before ship takes the latest offer ($140)")

    # (D) Codex #1a — pay a lot, then re-ship ONE of its paid cards → stays paid, NOT re-queued
    quote(915, {"A": 150, "B": 150}, {"A": 135, "B": 135})
    ship(915, {"A": 150, "B": 150}, {"A": 135, "B": 135})     # owed $270
    tracking.mark_paid(915)
    quote(915, {"B": 150}, {"B": 135}); ship(915, {"B": 150}, {"B": 135})   # retry B alone
    check(owed(915).get("payout") == 0.0 and owed(915).get("paid") is True,
          "Codex#1a: re-ship a paid sub-lot does NOT re-queue it (stays $0, paid)")

    # (E) Codex #1b — a partially-overlapping NEW lot after payment queues only the NEW card
    quote(915, {"B": 150, "G": 80}, {"B": 135, "G": 72})      # B already paid, G new
    ship(915, {"B": 150, "G": 80}, {"B": 135, "G": 72})
    check(owed(915).get("payout") == 72.0 and owed(915).get("paid") is False
          and owed(915).get("cards") == 1,
          "Codex#1b: overlapping new lot after payment queues only the NEW card ($72)")

    # (F) Codex #2a — legacy $100 across 3 certs: EXACT total preserved (no 99.99 split),
    #     and a new card that collides with that un-itemised set is flagged for review
    seed_legacy(916, {"certs": {"X": 40, "Y": 35, "Z": 25}, "payout": 100.0, "paid": False})
    ship(916, {"Y": 35, "W": 60}, {"Y": 30, "W": 55})        # Y overlaps legacy, W new
    check(owed(916).get("payout") == 155.0,
          "Codex#2a: legacy $100 preserved EXACTLY + new $55 = $155 (no invented split)")
    check("Y" in (owed(916).get("review_overlaps") or []),
          "Codex#2a: overlapping legacy cert flagged for review (not double-counted)")

    # (G) Codex #2b — legacy row with NO cert keys keeps its whole balance (was: lost)
    seed_legacy(917, {"certs": {}, "payout": 88.0, "paid": False})
    ship(917, {"Q": 100}, {"Q": 90})
    check(owed(917).get("payout") == 178.0,
          "Codex#2b: legacy row with no cert keys keeps its $88 (+$90) = $178")

    # (H) legacy PAID row: a new lot owes only the new card; the paid balance is not revived
    seed_legacy(918, {"certs": {"X": 100}, "payout": 90.0, "paid": True})
    ship(918, {"R": 50}, {"R": 45})
    check(owed(918).get("payout") == 45.0 and owed(918).get("paid") is False,
          "legacy PAID row: new lot owes only the new card ($45), paid not revived")

    # (I) identical retry does not double-count (dedup by cert)
    quote(919, {"A": 150, "B": 150}, {"A": 135, "B": 135}); ship(919, {"A": 150, "B": 150}, {"A": 135, "B": 135})
    ship(919, {"A": 150, "B": 150}, {"A": 135, "B": 135})     # identical retry
    check(owed(919).get("payout") == 270.0, "identical retry does not double-count ($270)")


def main():
    print("botv2 selftest:")
    for t in (test_certs, test_pricing, test_lots_shipping, test_tracking_digest,
              test_payout_flow, test_vip_commands_client, test_psa_folder_guard,
              test_alerts_status, test_analytics, test_delivery, test_register_tracking,
              test_lanes_board, test_stale_tickets, test_delivery_ofd,
              test_webserver_auth, test_audit_fixes, test_lanes_schedule,
              test_owed_accumulation):
        t()
    if _fails:
        print(f"\nFAILED ({len(_fails)}):")
        for f in _fails:
            print("  ✗", f)
        raise SystemExit(1)
    print("\nALL TESTS PASS")


if __name__ == "__main__":
    main()
