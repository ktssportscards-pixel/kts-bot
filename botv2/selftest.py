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
    tracking.record_accepted_ship(10, {"username": "topguy", "comp": 1500, "payout": 1350, "cards": 3, "certs": {"a": 1500}})  # idempotent re-ship (same lot)
    check(len(tracking.state.load("tickets", list)) == 2, "re-ship updates in place")
    check(next(r for r in tracking.state.load("owed", list) if r["id"] == "owed_10").get("payout") == 1350.0,
          "re-ship of the same un-itemised lot does not double the balance")
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
    from botv2 import tracking

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
        # /void (void-from-app) — auth + happy path + idempotency
        check(st(webserver._void(_Req(body={"user": "nobody"}))) == 401, "auth: /void rejects a missing key")
        tracking.state.save("tickets", []); tracking.state.save("owed", []); tracking.state.save("channel_sheet", {})
        tracking.record_quote(951, {"username": "void_web", "comp": 100, "payout": 90, "cards": 1,
                                    "certs": {"wv1": 100}, "cert_offers": {"wv1": 90}}, channel_name="void_web")
        vr = _j.loads(loop.run_until_complete(
            webserver._void(_Req(key="selftestkey", body={"user": "void_web"}))).body.decode())
        check(vr.get("voided") is True and vr.get("channel_id") == "951",
              "void: HTTP voids a quoted ticket by user (channel_id as JS-safe string)")
        check(not tracking.find_tickets(channel_id=951), "void: ticket row gone after HTTP void")
        nf = _j.loads(loop.run_until_complete(
            webserver._void(_Req(key="selftestkey", body={"user": "void_web"}))).body.decode())
        check(nf.get("voided") is False and nf.get("reason") == "not_found",
              "void: idempotent (second void → not_found)")
        cfg.BOT_API_KEY = ""                          # fails closed when no key is configured
        check(st(webserver._ops(_Req(key="anything"))) == 401,
              "auth: fails closed when BOT_API_KEY is unset")
    finally:
        cfg.BOT_API_KEY = saved
        loop.close()


# ── audit fixes: paid-regression, cold-quote ship, numeric-tracking tolerance, re-drop ──
def test_audit_fixes():
    section("audit fixes")
    # (1) record_accepted_ship must NOT re-queue an already-PAID card: a re-ship of the same
    #     cards stays paid even at a CORRECTED total (cert identity, not payout total — a paid
    #     card is settled); only a genuinely NEW card re-opens the row.
    snap = {"username": "regress_ray", "comp": 300, "payout": 270, "cards": 2,
            "certs": {"RR1": 150, "RR2": 150}, "cert_offers": {"RR1": 135, "RR2": 135}}
    tracking.record_accepted_ship(880, snap)
    check(tracking.mark_paid(880), "fix: owed entry created + marked paid")
    tracking.record_accepted_ship(880, snap)          # a repeat "ship" of the same lot after !paid
    owed = next(r for r in tracking.state.load("owed", list) if r["id"] == "owed_880")
    check(owed.get("paid") is True, "fix: re-ship of the same paid cards does NOT regress to unpaid")
    tracking.record_accepted_ship(880, {**snap, "payout": 500, "cert_offers": {"RR1": 250, "RR2": 250}})
    check(next(r for r in tracking.state.load("owed", list) if r["id"] == "owed_880").get("paid") is True,
          "fix: a corrected total on already-paid cards stays paid (no re-queue/overpay)")
    tracking.record_accepted_ship(880, {"username": "regress_ray", "comp": 100, "payout": 90,
                                        "cards": 1, "certs": {"RR3": 100}, "cert_offers": {"RR3": 90}})
    o880 = next(r for r in tracking.state.load("owed", list) if r["id"] == "owed_880")
    check(o880.get("paid") is False and o880.get("payout") == 90.0,
          "fix: a genuinely new card re-opens the row as unpaid ($90)")

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

    def uship(cid, certs, payout):    # un-itemised ship: cert ids known, NO per-cert offers, explicit total
        tracking.record_accepted_ship(cid, {"username": "acc", "comp": round(sum(certs.values()), 2),
                                             "payout": payout, "cards": len(certs),
                                             "certs": certs, "cert_offers": {}})

    def qpass(cid, certs, offers, passed):   # a QUOTE message that ALSO explicitly PASSes certs
        tracking.record_quote(cid, snap(certs, offers), card_detail=cd(certs, offers), passed=passed)

    def tkt(cid):
        return next((r for r in tracking.state.load("tickets", list) if r["id"] == f"tk_{cid}"), {})

    def owed(cid):
        return next((r for r in tracking.state.load("owed", list) if r["id"] == f"owed_{cid}"), {})

    def seed_legacy(cid, row):        # a pre-ledger owed row (shipped before the ledger existed)
        data = [r for r in tracking.state.load("owed", list) if r["id"] != f"owed_{cid}"]
        data.append({"id": f"owed_{cid}", "channel_id": cid, "user": "old", "status": "accepted", **row})
        tracking.state.save("owed", data)
        tracking.state.save("tickets", [t for t in tracking.state.load("tickets", list)
                                        if t["id"] != f"tk_{cid}"])   # ensure NO ticket ledger

    # (A) COMBINE-ALL-QUOTES: multiple quote messages, then one ship. EVERY BUY card across ALL
    #     messages is owed (each once). A card merely absent from the final ship message is a
    #     plain OMISSION, not a withdrawal (its slot was claimed at quote time) — so it is owed
    #     and NOT flagged. (Kevin's call; confirmed by the portal reconcile contract.)
    quote(912, {"A": 150, "B": 150}, {"A": 135, "B": 135})   # msg 1 (A,B)
    quote(912, {"C": 100, "D": 100}, {"C": 90, "D": 90})     # msg 2 (C,D)
    ship(912, {"C": 100, "D": 100}, {"C": 90, "D": 90})      # final message ships C,D
    check(owed(912).get("payout") == 450.0 and owed(912).get("cards") == 4,
          "combine-all: every BUY card across all quote messages is owed ($450, 4 cards)")
    check(not owed(912).get("review_overlaps"),
          "combine-all: a card omitted from the final message is owed, not flagged")
    check(tkt(912).get("payout") == 450.0 and tkt(912).get("cards") == 4,
          "combine-all: ticket totals reflect the whole lot union (ops + !stats volume correct)")

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

    # (J) Codex migration bug — a NEW quote lands on a legacy unpaid ticket BEFORE the ship.
    #     Migration must seed at quote time, else the ship projects only the new card.
    seed_legacy(920, {"certs": {"OLD1": 150, "OLD2": 150}, "payout": 270.0, "paid": False})
    quote(920, {"N": 200}, {"N": 180})                        # quote lands first
    ship(920, {"N": 200}, {"N": 180})
    check(owed(920).get("payout") == 450.0,
          "Codex: quote-before-ship still carries the legacy $270 (+$180=$450)")

    # (K) Codex promotion bug — a quote inserts a cert that OVERLAPS the carried set; it must
    #     not be itemised at quote time nor counted at promotion (no double-count).
    seed_legacy(921, {"certs": {"P": 100, "Q": 100}, "payout": 180.0, "paid": False})
    quote(921, {"P": 100}, {"P": 95})                         # P already inside the carried $180
    ship(921, {"P": 100}, {"P": 95})
    check(owed(921).get("payout") == 180.0,
          "Codex: overlapping cert via quote is not double-counted (stays $180)")
    check("P" in (owed(921).get("review_overlaps") or []),
          "Codex: the overlapping cert is flagged for review")

    # (L) legacy PAID ticket, then quote → ship a new card: only the new card owes
    seed_legacy(922, {"certs": {"S": 100}, "payout": 90.0, "paid": True})
    quote(922, {"T": 60}, {"T": 54}); ship(922, {"T": 60}, {"T": 54})
    check(owed(922).get("payout") == 54.0 and owed(922).get("paid") is False,
          "legacy PAID + quote-then-ship: only the new card owes ($54), paid not revived")

    # (M) a no-cert-list carried coexisting with an itemised card can't be overlap-checked →
    #     the balance is kept AND the ticket is flagged as unverifiable
    seed_legacy(923, {"certs": {}, "payout": 88.0, "paid": False})
    quote(923, {"U": 100}, {"U": 90}); ship(923, {"U": 100}, {"U": 90})
    check(owed(923).get("payout") == 178.0, "no-cert legacy + itemised card keeps $88 (+$90=$178)")
    check(tracking._UNKNOWN in (owed(923).get("review_overlaps") or []),
          "no-cert legacy beside an itemised card is flagged as unverifiable")

    # (N) ADVERSARIAL-AUDIT money-critical — once two un-itemised lots fold into the carried
    #     list, re-shipping ONE sub-lot must NOT double-count it (per-lot identity).
    uship(700, {"a": 100}, 100.0)                             # un-itemised lot (cert known, no offers)
    uship(700, {"b": 50}, 50.0)
    check(owed(700).get("payout") == 150.0, "audit: two distinct un-itemised lots accumulate ($150)")
    uship(700, {"b": 50}, 50.0)                               # re-ship the SAME sub-lot
    check(owed(700).get("payout") == 150.0,
          "audit: re-ship of one folded un-itemised sub-lot does NOT double-count (stays $150)")

    # (O) ADVERSARIAL-AUDIT — a bare-total (no-cert) re-ship on an already-itemised ticket must
    #     NOT open a carried lot on top of the itemised cards (was: $90 itemised + $90 bare = $180).
    quote(701, {"q": 100}, {"q": 90}); ship(701, {"q": 100}, {"q": 90})     # itemised, owed $90
    uship(701, {}, 90.0)                                      # bare total, no certs
    check(owed(701).get("payout") == 90.0,
          "audit: bare-total re-ship on an itemised ticket does NOT double (stays $90)")

    # (P) ADVERSARIAL-AUDIT (cosmetic) — tracking dropped in the quoted phase (before ship)
    #     still lands on the owed row, so the Friday digest shows it.
    quote(702, {"z": 200}, {"z": 180})
    tracking.record_tracking(702, "1Z702BEFORE0001")         # customer posts tracking before shipping
    ship(702, {"z": 200}, {"z": 180})
    check(owed(702).get("tracking") == "1Z702BEFORE0001",
          "audit: tracking dropped during the quoted phase still reaches the owed row")

    # (Q) RE-AUDIT money-critical — a PAID un-itemised card re-shipped at a CORRECTED total
    #     must stay paid (a settled card is never re-queued — was an overpay).
    uship(703, {"a": 100}, 100.0); tracking.mark_paid(703)
    uship(703, {"a": 100}, 110.0)
    check(owed(703).get("paid") is True and owed(703).get("payout") == 0.0,
          "re-audit: corrected-total re-ship of a PAID un-itemised card stays paid (no overpay)")

    # (R) RE-AUDIT money-critical — re-shipping a SUBSET of a PAID multi-cert carried lot must
    #     not re-queue the shared card.
    uship(704, {"a": 100, "b": 50}, 150.0); tracking.mark_paid(704)
    uship(704, {"a": 100}, 100.0)
    check(owed(704).get("paid") is True and owed(704).get("payout") == 0.0,
          "re-audit: sub-lot re-ship of a PAID carried lot does NOT re-queue the shared card")

    # (S) RE-AUDIT money-critical — a genuinely NEW bare-total lot must NOT be eaten by an
    #     equal-amount PAID bare-total lot (empty cert set has no identity → booked + flagged,
    #     never silently dropped / underpaid).
    seed_legacy(705, {"certs": {}, "payout": 88.0, "paid": True})
    uship(705, {}, 88.0)
    check(owed(705).get("payout") == 88.0 and owed(705).get("paid") is False,
          "re-audit: new bare-total lot not eaten by an equal paid lot (seller not underpaid)")
    check(tracking._UNKNOWN in (owed(705).get("review_overlaps") or []),
          "re-audit: an un-identifiable bare-total lot is flagged for review")

    # (T) RE-AUDIT money-critical — two un-itemised lots that PARTIALLY overlap must not
    #     double-count the shared card; the ambiguous overlap is flagged, balance left as-is.
    uship(706, {"a": 100, "b": 50}, 150.0)
    uship(706, {"b": 50, "c": 40}, 90.0)                     # shares b with the first lot
    check(owed(706).get("payout") == 150.0,
          "re-audit: partial-overlap un-itemised lots do not double-count the shared card")
    check(tracking._UNKNOWN in (owed(706).get("review_overlaps") or []),
          "re-audit: an ambiguous partial overlap is flagged for review")

    # (U) RE-AUDIT-3 money-critical — a SUPERSET un-itemised re-ship (shares a PAID card, adds a
    #     NEW one) must not re-queue the paid card NOR silently drop the new one → flag it.
    uship(707, {"a": 100}, 100.0); tracking.mark_paid(707)
    uship(707, {"a": 100, "b": 50}, 150.0)                   # 'a' paid, 'b' genuinely new
    check(owed(707).get("paid") is True and owed(707).get("payout") == 0.0,
          "re-audit-3: superset over a PAID card books nothing automatically (no overpay)")
    check(tracking._UNKNOWN in (owed(707).get("review_overlaps") or []),
          "re-audit-3: the new card riding on a paid card is FLAGGED, not silently dropped")

    # (V) RE-AUDIT-3 — a bare-total un-itemised ship that lists an already-ITEMISED cert plus a
    #     new one must not double the itemised card nor silently drop the new one → flag it.
    quote(708, {"a": 100}, {"a": 90}); ship(708, {"a": 100}, {"a": 90})   # itemised, 'a' accepted, owed $90
    uship(708, {"a": 100, "b": 50}, 150.0)                   # bare total lists itemised 'a' + new 'b'
    check(owed(708).get("payout") == 90.0,
          "re-audit-3: bare total overlapping an itemised cert does not double it (stays $90)")
    check(tracking._UNKNOWN in (owed(708).get("review_overlaps") or []),
          "re-audit-3: the new card on that bare lot is flagged, not silently dropped")

    # (W) COMBINE-ALL — a card quoted BUY then merely OMITTED from a later message is NOT a
    #     withdrawal; it stays owed. (Only an EXPLICIT re-quoted PASS drops it — see W2.)
    quote(709, {"A": 100, "B": 90}, {"A": 90, "B": 81})      # msg 1: A,B both BUY
    quote(709, {"A": 100}, {"A": 90})                        # msg 2: B merely omitted (NOT passed)
    ship(709, {"A": 100}, {"A": 90})                         # final message lists only A
    check(owed(709).get("payout") == 171.0 and owed(709).get("cards") == 2,
          "combine-all: a card omitted from a later message is still owed (A+B = $171)")
    check(not owed(709).get("review_overlaps"),
          "combine-all: a plain omission is not flagged")

    # (W2) EXPLICIT PASS — a card quoted BUY then explicitly PASSed on a re-quote (portal declined
    #     the re-listed card, e.g. its band filled) IS dropped from the lot, cleanly (not flagged).
    quote(7091, {"A": 100, "B": 90}, {"A": 90, "B": 81})     # msg 1: A,B both BUY
    qpass(7091, {"A": 100}, {"A": 90}, ["B"])                # msg 2: B re-listed and PASSed
    ship(7091, {"A": 100}, {"A": 90})
    check(owed(7091).get("payout") == 90.0 and owed(7091).get("cards") == 1,
          "explicit PASS: a re-quoted-PASS card is dropped (owes only A = $90)")
    check(not owed(7091).get("review_overlaps"),
          "explicit PASS: a cleanly-passed card is not flagged")

    # (X) PAYOUT-VISIBILITY — a flagged ticket must surface the flag in the !owe packet, the Friday
    #     digest, and /ops, and `!resolve` must clear it (money untouched). Under combine-all a flag
    #     now comes from genuine legacy/un-itemised ambiguity: here a quoted cert that sits inside a
    #     legacy un-itemised carried balance (can't auto-verify → flag, don't double-count).
    def _snapX(certs, offers):
        return {"username": "flagseller", "comp": round(sum(certs.values()), 2),
                "payout": round(sum(offers.values()), 2), "cards": len(offers),
                "certs": certs, "cert_offers": offers}
    seed_legacy(710, {"certs": {"FA": 120, "FZ": 80}, "payout": 200.0, "paid": False,
                      "user": "flagseller"})
    tracking.record_quote(710, _snapX({"FA": 120}, {"FA": 110}))   # FA sits inside the carried $200
    tracking.record_accepted_ship(710, _snapX({"FA": 120}, {"FA": 110}))
    check(owed(710).get("payout") == 200.0 and "FA" in (owed(710).get("review_overlaps") or []),
          "payout-visibility: overlapping legacy cert is flagged, exact balance preserved ($200)")
    pk = tracking.build_owe_packets("flagseller")
    check("NEEDS REVIEW" in pk and "FA" in pk,
          "payout-visibility: !owe packet surfaces the review flag + the flagged cert")
    check("REVIEW before paying" in payout_digest.build_digest(),
          "payout-visibility: the Friday digest flags ambiguous tickets")
    opsrow = next((r for r in tracking.ops_view({}) if r["channel_id"] == 710), None)
    check(opsrow and opsrow.get("needs_review") is True and "FA" in (opsrow.get("review_certs") or []),
          "payout-visibility: /ops exposes needs_review + the flagged cert for the admin dashboard")
    cleared = tracking.resolve_review(710)
    check("FA" in cleared and owed(710).get("payout") == 200.0 and not owed(710).get("review_overlaps"),
          "!resolve clears the flag, money unchanged ($200 still owed)")
    check("NEEDS REVIEW" not in tracking.build_owe_packets("flagseller"),
          "payout-visibility: after !resolve the packet no longer warns")

    # (Y) UNION, NO DOUBLE-COUNT — a cert that rides in TWO quote messages is owed ONCE, and the
    #     card_detail the portal reconciles 1:1 carries it ONCE (the portal has no dedup).
    quote(711, {"A": 150, "B": 150}, {"A": 135, "B": 135})   # msg 1: A,B
    quote(711, {"B": 150, "C": 100}, {"B": 135, "C": 90})    # msg 2: B re-listed, C new
    ship(711, {"B": 150, "C": 100}, {"B": 135, "C": 90})
    check(owed(711).get("payout") == 360.0 and owed(711).get("cards") == 3,
          "union: a cert quoted in two messages is owed once (A,B,C = $360, 3 cards)")
    cdY = [c for c in tkt(711).get("card_detail", []) if c.get("cert")]
    check({c["cert"] for c in cdY} == {"A", "B", "C"} and len(cdY) == 3,
          "union: card_detail carries each cert exactly once (no double-count for the portal)")
    cd_sum = round(sum(float(c.get("offer", 0) or 0) for c in cdY), 2)
    check(cd_sum == 360.0,
          "parity: sum(card_detail.offer) == owed union ($360) — the portal's 1:1 reconcile matches")

    # (Z) EXPLICIT PASS removes the card from card_detail too (the portal renders what's present,
    #     so a passed card must not leave behind a row reconcile would still count).
    quote(712, {"A": 100, "B": 90}, {"A": 90, "B": 81})
    qpass(712, {"A": 100}, {"A": 90}, ["B"])
    check({c["cert"] for c in tkt(712).get("card_detail", []) if c.get("cert")} == {"A"},
          "explicit PASS: the passed cert is removed from card_detail too")

    # (AA) OMISSION keeps the card in card_detail (a plain omission is not a withdrawal).
    quote(713, {"A": 100, "B": 90}, {"A": 90, "B": 81})
    quote(713, {"A": 100}, {"A": 90})                        # B omitted, NOT passed
    check({c["cert"] for c in tkt(713).get("card_detail", []) if c.get("cert")} == {"A", "B"},
          "omission: an omitted-but-not-passed cert stays in card_detail")

    # (AB) CARD_DETAIL LOCK — re-quoting an already-SHIPPED card at a new price must NOT re-price
    #      its card_detail row: the ledger locks accepted/paid cards, and card_detail must match so
    #      the portal's 1:1 reconcile and analytics by_category don't drift. Parity: Σ cd.offer==owed.
    quote(714, {"A": 150, "B": 150}, {"A": 135, "B": 135})
    ship(714, {"A": 150, "B": 150}, {"A": 135, "B": 135})    # A,B accepted at 135 each
    quote(714, {"A": 150}, {"A": 160})                       # re-quote the SHIPPED A at a higher price
    cdlock = {c["cert"]: c["offer"] for c in tkt(714).get("card_detail", []) if c.get("cert")}
    check(cdlock.get("A") == 135 and owed(714).get("payout") == 270.0,
          "card_detail lock: a re-quoted shipped card keeps its locked offer (owed stays $270)")
    cd_sum714 = round(sum(float(c.get("offer", 0) or 0) for c in tkt(714).get("card_detail", [])), 2)
    check(cd_sum714 == owed(714).get("payout"),
          "card_detail lock: Σ card_detail.offer == owed union — the portal's reconcile stays in parity")


def test_void():
    section("void command")
    tracking.state.save("tickets", [])
    tracking.state.save("owed", [])
    tracking.state.save("channel_sheet", {"900": "https://docs.google.com/spreadsheets/d/SHEET900/edit"})
    # a quoted (unshipped) ticket
    tracking.record_quote(900, {"username": "void_vic", "comp": 500, "payout": 455, "cards": 2,
                                "certs": {"z1": 300, "z2": 200},
                                "cert_offers": {"z1": 270, "z2": 185}}, channel_name="void_vic")
    check(any(r["id"] == "tk_900" for r in tracking.state.load("tickets", list)), "void: quoted ticket recorded")
    found = tracking.find_tickets(user="@Void_Vic", status="quoted")
    check(len(found) == 1 and found[0]["channel_id"] == 900, "void: find by username (case-insensitive)+status")
    check(tracking.find_tickets(user="void_vic", status="accepted") == [], "void: status filter excludes non-matching")
    res = tracking.void_ticket(900)
    check(res["found"] and res["user"] == "void_vic" and res["status"] == "quoted", "void: summary for quoted ticket")
    check(res["cards"] == 2 and res["payout"] == 455.0 and res["had_owed"] is False, "void: summary totals, no owed")
    check(res["sheet_url"].endswith("SHEET900/edit"), "void: sheet url handed back")
    check(not any(r["id"] == "tk_900" for r in tracking.state.load("tickets", list)), "void: tickets row removed")
    check("900" not in tracking.state.load("channel_sheet", dict), "void: channel_sheet entry removed")
    check(tracking.void_ticket(900)["found"] is False, "void: idempotent second call")
    # a SHIPPED ticket → owed row present → void removes owed too
    tracking.record_accepted_ship(901, {"username": "ship_sly", "comp": 300, "payout": 285, "cards": 1,
                                         "certs": {"s1": 300}, "cert_offers": {"s1": 285}})
    check(any(r["id"] == "owed_901" for r in tracking.state.load("owed", list)), "void: shipped ticket has owed row")
    res2 = tracking.void_ticket(901)
    check(res2["found"] and res2["status"] == "accepted" and res2["had_owed"] is True, "void: shipped summary + had_owed")
    check(not any(r["id"] == "owed_901" for r in tracking.state.load("owed", list)), "void: owed row removed")
    check(not any(r["id"] == "tk_901" for r in tracking.state.load("tickets", list)), "void: shipped tickets row removed")
    # lots.drop
    lots.add_slab_values(902, {"c1": 100.0}, {"c1": "pokemon"})
    check(lots.has_lot(902), "void: lot present before drop")
    check(lots.drop(902) and not lots.has_lot(902), "void: lots.drop clears the lot")
    check(lots.drop(902) is False, "void: lots.drop idempotent")
    # sheets.trash_sheet guard (empty id → False, no network)
    from botv2 import sheets as _sheets
    check(_sheets.trash_sheet("") is False, "void: trash_sheet('') fails soft, no network")
    # void_ticket ATOMICITY (Codex C5): an owed-save failure must NOT delete the ticket — bail
    # ok=False with everything intact so a retry recovers (never orphan owed behind a gone ticket).
    tracking.state.save("tickets", [{"id": "tk_970", "channel_id": 970, "user": "atom",
                                     "status": "accepted", "cards": 1, "payout": 50}])
    tracking.state.save("owed", [{"id": "owed_970", "channel_id": 970, "paid": False, "payout": 50}])
    _orig_save = tracking.state.save
    tracking.state.save = lambda name, data: False if name == "owed" else _orig_save(name, data)
    try:
        rfail = tracking.void_ticket(970)
    finally:
        tracking.state.save = _orig_save
    check(rfail.get("found") is True and rfail.get("ok") is False, "void: owed-save failure → found, ok=False")
    check(any(r["id"] == "tk_970" for r in tracking.state.load("tickets", list)),
          "void: ticket intact after failed owed save (recoverable)")
    check(any(r["id"] == "owed_970" for r in tracking.state.load("owed", list)),
          "void: owed intact after failed owed save (never orphaned behind a deleted ticket)")
    check(tracking.void_ticket(970).get("ok") is True, "void: retry completes cleanly after the transient failure")
    check(not any(r["id"] == "tk_970" for r in tracking.state.load("tickets", list)), "void: retry removed the ticket")
    # cache-write failure at the FINAL step: the quote still dies (ticket_removed) so the caller
    # clears memory + trashes — but ok=False and a retry completes the leftover cache cleanup.
    tracking.state.save("tickets", [{"id": "tk_971", "channel_id": 971, "user": "catom",
                                     "status": "quoted", "cards": 1, "payout": 40}])
    tracking.state.save("owed", [])
    tracking.state.save("channel_sheet", {"971": "https://docs.google.com/spreadsheets/d/SHEET971/edit"})
    _orig_save2 = tracking.state.save
    tracking.state.save = lambda name, data: False if name == "channel_sheet" else _orig_save2(name, data)
    try:
        cfail = tracking.void_ticket(971)
    finally:
        tracking.state.save = _orig_save2
    check(cfail.get("ticket_removed") is True and cfail.get("ok") is False,
          "void: cache-write failure → ticket_removed True (quote dead) + ok=False")
    check(cfail.get("sheet_url", "").endswith("SHEET971/edit"), "void: sheet_url returned despite cache-write failure")
    check(not any(r["id"] == "tk_971" for r in tracking.state.load("tickets", list)), "void: ticket gone after cache-fail")
    retry971 = tracking.void_ticket(971)
    check(retry971.get("ok") is True and "971" not in tracking.state.load("channel_sheet", dict),
          "void: retry after cache-fail completes the cache cleanup")
    # durable voided marker: set on void (blocks a ship-time Drive-sheet rebuild), cleared by a
    # fresh quote so a legitimate re-submit still rebuilds.
    tracking.state.save("tickets", [])
    tracking.state.save("owed", [])
    tracking.state.save("voided", [])
    tracking.record_quote(980, {"username": "vmark", "comp": 100, "payout": 90, "cards": 1,
                                "certs": {"vm1": 100}, "cert_offers": {"vm1": 90}}, channel_name="vmark")
    vmres = tracking.void_ticket(980)
    check(vmres.get("ticket_removed") and tracking.is_voided(980), "void: sets a durable voided marker")
    tracking.record_quote(980, {"username": "vmark", "comp": 100, "payout": 90, "cards": 1,
                                "certs": {"vm2": 100}, "cert_offers": {"vm2": 90}}, channel_name="vmark")
    check(not tracking.is_voided(980), "void: a fresh quote clears the voided marker (channel reactivated)")
    # marker save failure must bail (ok False) WITHOUT removing the ticket — never delete the
    # reconstruction anchor while the durable guard is missing (Codex marker-persistence boundary).
    tracking.state.save("tickets", [{"id": "tk_981", "channel_id": 981, "user": "mf",
                                     "status": "quoted", "cards": 1, "payout": 30}])
    tracking.state.save("owed", [])
    tracking.state.save("voided", [])
    _os3 = tracking.state.save
    tracking.state.save = lambda name, data: False if name == "voided" else _os3(name, data)
    try:
        mf = tracking.void_ticket(981)
    finally:
        tracking.state.save = _os3
    check(mf.get("ok") is False and not mf.get("ticket_removed"),
          "void: marker-save failure bails (ok=False, ticket NOT removed)")
    check(any(r["id"] == "tk_981" for r in tracking.state.load("tickets", list)),
          "void: ticket intact when the voided marker couldn't persist")
    tracking.state.save("tickets", [])
    tracking.state.save("owed", [])
    tracking.state.save("channel_sheet", {})
    tracking.state.save("voided", [])


def main():
    print("botv2 selftest:")
    for t in (test_certs, test_pricing, test_lots_shipping, test_tracking_digest,
              test_payout_flow, test_vip_commands_client, test_psa_folder_guard,
              test_alerts_status, test_analytics, test_delivery, test_register_tracking,
              test_lanes_board, test_stale_tickets, test_delivery_ofd,
              test_webserver_auth, test_audit_fixes, test_lanes_schedule,
              test_owed_accumulation, test_void):
        t()
    if _fails:
        print(f"\nFAILED ({len(_fails)}):")
        for f in _fails:
            print("  ✗", f)
        raise SystemExit(1)
    print("\nALL TESTS PASS")


if __name__ == "__main__":
    main()
