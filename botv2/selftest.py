"""
selftest — botv2's test suite. No pytest needed: run it directly.

    python -m botv2.selftest        # mock pricing + a temp DATA_DIR
    (exit 0 = all pass, 1 = a failure, printed with its section)

Covers every pure/offline path: cert + tracking parsing, quote building + messages,
VIP flag, lots/lanes/minimums, VA/Doral shipping, tracking record + payout digest,
commands, and the portal client's mock + param plumbing. The live Discord + Google
+ /bot/price paths are exercised separately (they need real creds/endpoint).

HERMETIC BY CONSTRUCTION — safe to run from any shell, including one that has the
production variables exported (a Railway shell, a dev box with a real .env sourced):
  * it ALWAYS uses a fresh temp DATA_DIR (an inherited DATA_DIR is ignored — the tests
    blank the tickets / owed / payinfo / void stores, which would wipe real data);
  * every config variable is scrubbed from the environment first (credentials, the
    auto-claim flag, the Drive folder ids, feature flags), so results never depend on
    the caller's shell and no real credential is ever loaded;
  * the network is disabled for the whole process, so no test can reach Discord, the
    portal (incl. /bot/claim), the Apps Script or Google Drive even if a guard regressed.
"""
import asyncio
import os
import socket
import tempfile

# ── hermetic harness: must run BEFORE any botv2 module reads config at import time ──
_INHERITED_DATA_DIR = os.environ.get("DATA_DIR")
_CONFIG_ENV = (
    "DISCORD_BOT_TOKEN", "DISCORD_TOKEN", "KEVIN_USER_ID", "PORTAL_BASE_URL", "BOT_API_KEY",
    "BOTV2_PRICE_MOCK", "BOTV2_AUTO_CLAIM", "NEAR_FULL_PCT", "BOTV2_DELIVERY_TRACKING",
    "BOTV2_REGISTER_TRACKING", "BOTV2_LANES_BOARD", "BOTV2_LANES_CHANNEL_ID",
    "BOTV2_LANES_REFRESH_MIN", "BOTV2_LANES_SCHEDULE", "BOTV2_STALE_NUDGES",
    "BOTV2_STALE_QUOTED_DAYS", "BOTV2_STALE_TRANSIT_DAYS", "BOTV2_STALE_UNPAID_DAYS",
    "GOOGLE_CREDENTIALS_JSON", "GOOGLE_CREDENTIALS_FILE", "APPS_SCRIPT_URL",
    "APPS_SCRIPT_SHARED_SECRET", "PSA_FOLDER_ID", "ALLOW_SHARED_PSA_FOLDER",
    "TEMPLATE_SHEET_ID", "DATA_DIR", "SPORTS_OP_MIN_LOT", "POKEMON_SINGLES_MIN_LOT",
    "BOTV2_SHIP_ALL_DORAL", "MIN_LOT_VALUE", "MIN_SLAB_COUNT", "POKEMON_LANE_OPEN", "PORT",
    "BOT_BASE_URL", "CANARY_CERT", "CANARY_ERROR_MAX_AGE_MIN",       # read by botv2.canary
)
for _k in _CONFIG_ENV:
    os.environ.pop(_k, None)
os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="botv2-selftest-")
os.environ["BOTV2_PRICE_MOCK"] = "1"
# Belt and braces under the network kill below: point every outbound URL at a dead
# loopback port and the service-account file at a path that cannot exist.
os.environ["PORTAL_BASE_URL"] = "http://127.0.0.1:9/selftest-network-disabled"
os.environ["APPS_SCRIPT_URL"] = "http://127.0.0.1:9/selftest-network-disabled"
os.environ["BOT_BASE_URL"] = "http://127.0.0.1:9/selftest-network-disabled"
os.environ["GOOGLE_CREDENTIALS_FILE"] = os.path.join(os.environ["DATA_DIR"], "no-credentials.json")

_net_attempts = []


def _no_network(*args, **_kwargs):
    """Every real connection attempt in this process lands here and fails loudly."""
    _net_attempts.append(repr(args[-1:])[:80])
    raise RuntimeError("botv2 selftest: network access is disabled")


socket.socket.connect = _no_network
socket.socket.connect_ex = _no_network
socket.create_connection = _no_network
socket.getaddrinfo = _no_network

from botv2 import (certs, commands, lots, payout_digest, portal_client,  # noqa: E402
                   pricing, shipping, textsplit, tracking, vip)
from botv2.portal_client import PricedRow                                # noqa: E402

_fails = []


def check(cond, msg):
    if not cond:
        _fails.append(msg)


def section(name):
    print(f"  · {name}")


# ── the harness itself: prove the safety properties the docstring promises ──────
def test_harness_is_hermetic():
    section("harness is hermetic")
    import urllib.request
    import botv2.config as cfg
    check(cfg.DATA_DIR == os.environ["DATA_DIR"] and "botv2-selftest-" in cfg.DATA_DIR
          and cfg.DATA_DIR != _INHERITED_DATA_DIR,
          "harness: DATA_DIR is a fresh temp dir (an inherited DATA_DIR is never used)")
    check(cfg.PRICE_MOCK is True, "harness: pricing is mocked")
    check(cfg.AUTO_CLAIM is False, "harness: auto-claim flag is off regardless of the caller's env")
    check(cfg.DISCORD_TOKEN == "" and cfg.BOT_API_KEY == "" and cfg.APPS_SCRIPT_SHARED_SECRET == ""
          and not cfg.GOOGLE_CREDENTIALS_JSON and not os.path.exists(cfg.GOOGLE_CREDENTIALS_FILE),
          "harness: no credential is loaded (tokens, bot key, sheet secret, service account)")
    # integer settings: unset or BLANK → the default (a blank Railway variable used to crash
    # the import with int('')); a typo still fails loudly instead of silently changing a value.
    os.environ.pop("ZZ_SELFTEST_INT", None)
    check(cfg._int_env("ZZ_SELFTEST_INT", 7) == 7, "config: unset int → default")
    for blank in ("", "   "):
        os.environ["ZZ_SELFTEST_INT"] = blank
        check(cfg._int_env("ZZ_SELFTEST_INT", 7) == 7, f"config: blank int {blank!r} → default (no crash)")
    os.environ["ZZ_SELFTEST_INT"] = " 12 "
    check(cfg._int_env("ZZ_SELFTEST_INT", 7) == 12, "config: a real value is used")
    os.environ["ZZ_SELFTEST_INT"] = "12x"
    try:
        cfg._int_env("ZZ_SELFTEST_INT", 7)
        check(False, "config: a garbage int must fail loudly")
    except ValueError:
        check(True, "config: a garbage int fails loudly")
    os.environ.pop("ZZ_SELFTEST_INT", None)
    check((cfg.MIN_LOT_VALUE, cfg.MIN_SLAB_COUNT, cfg.SPORTS_OP_MIN_LOT, cfg.POKEMON_SINGLES_MIN_LOT,
           cfg.NEAR_FULL_PCT, cfg.STALE_QUOTED_DAYS, cfg.STALE_TRANSIT_DAYS, cfg.STALE_UNPAID_DAYS,
           cfg.LANES_REFRESH_MIN, cfg.WEB_PORT, cfg.KEVIN_USER_ID, cfg.LANES_CHANNEL_ID)
          == (0, 0, 0, 0, 85, 5, 5, 3, 10, 8080, 0, 0),
          "config: every integer default is unchanged")
    check(cfg.PSA_FOLDER_ID == cfg.V1_PSA_FOLDER_ID and not cfg.ALLOW_SHARED_PSA_FOLDER
          and cfg.psa_folder_unsafe(),
          "harness: Drive folder left at the refuse-to-create tripwire default")
    check("127.0.0.1:9" in cfg.APPS_SCRIPT_URL and "127.0.0.1:9" in cfg.PRICE_ENDPOINT
          and "127.0.0.1:9" in cfg.CLAIM_ENDPOINT,
          "harness: every outbound URL points at a dead loopback port")
    before = len(_net_attempts)
    try:
        urllib.request.urlopen("http://127.0.0.1:9/selftest-probe", timeout=1)
        blocked = False
    except RuntimeError as e:
        blocked = "network access is disabled" in str(e)
    except Exception:
        blocked = False
    check(blocked and len(_net_attempts) == before + 1,
          "harness: a real connection attempt is refused in-process")
    del _net_attempts[before:]            # the deliberate probe doesn't count against the suite


# ── fake Discord plumbing: lets the REAL bot handlers run offline ───────────────
def _discord_len(text) -> int:
    """Message length the way Discord counts it (UTF-16 code units)."""
    return len(text) + sum(1 for ch in text if ord(ch) > 0xFFFF)


def _fake_send_check(text):
    """Mimic the two Discord API rules our senders must respect: no empty message, and
    nothing over 2,000 characters (the real API answers either with HTTP 400)."""
    if not isinstance(text, str) or not text.strip():
        raise RuntimeError("fake discord: cannot send an empty message (HTTP 400)")
    if _discord_len(text) > 2000:
        raise RuntimeError("fake discord: content must be 2000 or fewer in length (HTTP 400)")


class _FakeUser:
    """A Discord user (the customer, or Kevin when uid == config.KEVIN_USER_ID)."""
    def __init__(self, uid=9, name="cust", is_bot=False):
        self.id, self.name, self.bot, self.dms = uid, name, is_bot, []

    async def send(self, text):
        _fake_send_check(text)
        self.dms.append(text)


def _fake_channel(cid, name):
    """A stand-in ticket channel that passes the handlers' isinstance(TextChannel) gate.
    Records everything 'posted' in .sent; never touches the network."""
    import discord

    class _Typing:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    class _Chan(discord.TextChannel):
        category = None                       # no category → the plain name/known-id gate

        def __init__(self, cid_, name_):      # deliberately NOT calling the real __init__
            self.id, self.name, self.guild = cid_, name_, object()
            self.sent = []

        async def send(self, text):
            _fake_send_check(text)
            self.sent.append(text)

        def typing(self):
            return _Typing()

    return _Chan(cid, name)


class _FakeMessage:
    def __init__(self, channel, content, author=None, mid=1):
        self.channel, self.content, self.id = channel, content, mid
        self.author = author or _FakeUser()
        self.deleted = False

    async def delete(self):
        self.deleted = True


class _BotHarness:
    """Context manager: wires a fake Kevin + a fake ticket channel into the real bot
    module, with pricing / sheet calls replaceable per test. Restores everything on exit."""
    def __init__(self, cid, name="harness", rows=None, sheet_url=None):
        self.cid, self.name, self.rows, self.sheet_url = cid, name, rows, sheet_url
        self.sheet_calls, self.fill_calls, self.errors = [], [], []
        self.tracking_saves, self.trash_calls = [], []

    def __enter__(self):
        import botv2.config as cfg
        from botv2 import alerts, bot as botmod, sheets as sheets_mod, tickets
        self.cfg, self.botmod, self.sheets_mod, self.tickets = cfg, botmod, sheets_mod, tickets
        self.kevin = _FakeUser(uid=777, name="kevin")
        self.channel = _fake_channel(self.cid, self.name)
        self._saved = (cfg.KEVIN_USER_ID, portal_client.price_certs,
                       sheets_mod.create_psa_sheet, sheets_mod.fill_buying_sheet,
                       sheets_mod.save_tracking, sheets_mod.trash_sheet,
                       sheets_mod.get_drive_service, sheets_mod.get_gspread_client)
        cfg.KEVIN_USER_ID = 777
        # No Google client is ever built in here (keeps a test's own stub if it set one).
        if getattr(sheets_mod.get_drive_service, "__module__", "") == "botv2.sheets":
            def _no_google(*_a, **_k):
                raise RuntimeError("selftest: no Google client in the harness")
            sheets_mod.get_drive_service = sheets_mod.get_gspread_client = _no_google
        # The two Apps Script WRITE calls a handler can reach are always stubbed in here:
        # nothing driven through this harness may even attempt them (the process-wide network
        # kill would stop it, but then the "no network attempt" check rightly fails the run).
        sheets_mod.save_tracking = lambda url, tnum: self.tracking_saves.append((url, tnum))
        if getattr(sheets_mod.trash_sheet, "__module__", "") == "botv2.sheets":   # keep a test's own stub

            def _trash(url):
                self.trash_calls.append(url)
                return True
            sheets_mod.trash_sheet = _trash
        botmod.bot.get_user = lambda _uid: self.kevin          # instance attr shadows the method
        tickets._known.add(self.cid)
        botmod._welcomed.add(self.cid)
        alerts.reset()
        if self.rows is not None:
            async def _price(cert_list, customer=None, rate_override=None, session=None):
                self.price_args = {"certs": list(cert_list), "customer": customer,
                                   "rate_override": rate_override}
                rows = self.rows                      # read at call time: a test may swap it mid-way
                return rows(cert_list) if callable(rows) else rows
            portal_client.price_certs = _price
        if self.sheet_url is not None:
            def _create(username, cert_tokens, folder_id=None):
                self.sheet_calls.append((username, list(cert_tokens)))
                return self.sheet_url, f"{username} discord", {}

            def _fill(sheet_id, rows_, sheet_name=None):
                self.fill_calls.append(sheet_id)
                return []
            sheets_mod.create_psa_sheet, sheets_mod.fill_buying_sheet = _create, _fill
        return self

    def say(self, content, author=None):
        """Feed one message through the real on_message core. Like the live on_message
        wrapper, an exception is caught — it lands in .errors for the test to assert on."""
        msg = _FakeMessage(self.channel, content, author=author)
        try:
            asyncio.run(self.botmod._handle_message(msg))
        except Exception as e:
            self.errors.append(e)
        return msg

    def __exit__(self, *exc):
        cfg, botmod, sheets_mod = self.cfg, self.botmod, self.sheets_mod
        (cfg.KEVIN_USER_ID, portal_client.price_certs,
         sheets_mod.create_psa_sheet, sheets_mod.fill_buying_sheet,
         sheets_mod.save_tracking, sheets_mod.trash_sheet,
         sheets_mod.get_drive_service, sheets_mod.get_gspread_client) = self._saved
        botmod.bot.__dict__.pop("get_user", None)
        self.tickets._known.discard(self.cid)
        botmod._welcomed.discard(self.cid)
        botmod._last_quote.pop(self.cid, None)
        botmod._channel_sheet.pop(str(self.cid), None)
        lots.drop(self.cid)
        from botv2 import alerts
        alerts.reset()
        return False


# ── long messages: Discord's 2,000-character cap ───────────────────────────────
def test_textsplit():
    section("long messages (2,000-char cap)")
    sm, units = textsplit.split_message, _discord_len
    check(sm("hello") == ["hello"], "split: short text is one unchanged message")
    check(sm("") == [] and sm(None) == [] and sm("  \n ") == [], "split: nothing to send → no messages")
    # Anything Discord accepts as it is (≤ 2,000) is sent exactly as before — ONE message.
    # (The first version of this splitter cut at 1,900, turning a 1,901–2,000-character quote
    # that used to arrive whole into two posts.)
    for n in (1900, 1901, 1950, 2000):
        whole = "a" * n
        check(sm(whole) == [whole], f"split: a {n:,}-character message is still ONE unchanged message")
    check([len(x) for x in sm("a" * 2001)] == [textsplit.SAFE_LIMIT, 2001 - textsplit.SAFE_LIMIT],
          "split: one character over Discord's cap splits (packed to 1,900)")
    emoji_edge = "🔗" * 1000                                        # 1,000 characters = 2,000 units
    check(sm(emoji_edge) == [emoji_edge] and len(sm(emoji_edge + "🔗")) == 2,
          "split: the cap is counted the way Discord counts it (UTF-16 units)")
    check([len(x) for x in sm("a" * 50, 50)] == [50] and [len(x) for x in sm("a" * 51, 50)] == [50, 1],
          "split: an explicit limit applies to both the fits-check and the packing")
    lines = [f"• `{10000000 + i}` — **$100.00** → $90.00 — {'Some Card Name Here ' * 2}{i} (PSA 10)"
             for i in range(120)]
    parts = sm("header line\n\n" + "\n".join(lines))
    check(len(parts) > 1 and all(units(x) <= 2000 for x in parts), "split: a 120-card summary fits Discord in parts")
    check([ln for x in parts for ln in x.split("\n") if ln.startswith("•")] == lines,
          "split: every line survives intact and in order (never cut mid-line)")
    packet = lambda i: "\n".join([f"💸 **Payment packet — seller{i}**", "Amount: **$1,234.00**",
                                  "── details on file ──", "Wire: Example Bank 000111222\nName: Some Person",
                                  "_Review at the bank before sending — packets are prep, not payment._"])
    owe = "💸 **14 to pay** (oldest first):\n\n" + "\n\n".join(packet(i) for i in range(14))
    oparts = sm(owe)
    check(len(oparts) > 1 and all(sum(packet(i) in x for x in oparts) == 1 for i in range(14)),
          "split: prefers blank lines, so each payment packet stays in ONE message")
    wide = sm("x" * 5000 + "\n" + "🔗" * 1500)
    check(all(units(x) <= 2000 for x in wide) and "".join(wide).replace("\n", "") == "x" * 5000 + "🔗" * 1500,
          "split: an over-long single line (and 2-unit emoji) is hard-split without loss")
    # one very long comma-separated line (the "re-quote REMOVED n cards" DM): the cut falls
    # between two cert numbers, never inside one
    cert_line = "REMOVED: " + ", ".join(str(10000000 + i) for i in range(400))
    cparts = sm(cert_line)
    numbers = [tok for x in cparts for tok in x.replace("REMOVED:", "").replace(",", " ").split()]
    check(len(cparts) > 1 and numbers == [str(10000000 + i) for i in range(400)]
          and "".join(cparts) == cert_line,
          "split: a long comma-separated line is cut between numbers, not through one")

    # the parts of one message go out together — two long messages to the same place can't
    # interleave (one seller's cards under another seller's header), other places aren't held up
    class _SlowTarget:
        def __init__(self, tid, log):
            self.id, self.log = tid, log

        async def send(self, text):
            await asyncio.sleep(0)                                   # let the other sender run
            self.log.append((self.id, text.split("|")[0]))

    async def _two_at_once():
        log = []
        a, b = _SlowTarget(1, log), _SlowTarget(1, log)             # same destination id
        other = _SlowTarget(2, log)
        msg = lambda tag: "\n".join(f"{tag}| line {i} " + "x" * 80 for i in range(60))
        await asyncio.gather(textsplit.send_chunked(a, msg("A")), textsplit.send_chunked(b, msg("B")),
                             textsplit.send_chunked(other, msg("C")))
        return log
    log = asyncio.run(_two_at_once())
    same = [tag for tid, tag in log if tid == 1]
    check(len(same) >= 6 and same == sorted(same),
          "send: two multi-part messages to one destination arrive one after the other, not interleaved")
    first_c = next(i for i, (tid, _t) in enumerate(log) if tid == 2)
    last_ab = max(i for i, (tid, _t) in enumerate(log) if tid == 1)
    check(first_c < last_ab and not textsplit._send_locks,
          "send: another destination is not held up, and no lock is left behind afterwards")

    # a send that fails part-way says how far it got
    class _FailsAt:
        def __init__(self, n):
            self.id, self.n, self.got = 99, n, []

        async def send(self, text):
            if len(self.got) + 1 == self.n:
                raise RuntimeError("discord 503")
            self.got.append(text)

    async def _partial(n):
        t = _FailsAt(n)
        try:
            await textsplit.send_chunked(t, "\n".join("y" * 90 for _ in range(60)))
        except RuntimeError as e:
            return textsplit.sent_before_failure(e), len(t.got)
    (sent2, total2), got2 = asyncio.run(_partial(2))
    check((sent2, got2) == (1, 1) and total2 >= 3 and asyncio.run(_partial(1))[0][0] == 0
          and textsplit.sent_before_failure(ValueError("unrelated")) == (0, 0),
          "send: a failure on part 2 reports that part 1 was delivered; other errors report nothing sent")

    # ping_kevin: a long DM arrives in parts instead of being rejected + swallowed
    from botv2 import bot as botmod
    with _BotHarness(8801) as h:
        long_dm = "📋 summary\n" + "\n".join(lines)
        ok = asyncio.run(botmod.ping_kevin(long_dm))
        check(ok and len(h.kevin.dms) > 1 and "\n".join(h.kevin.dms).count("•") == 120,
              "ping_kevin: a long DM is delivered in parts (was: rejected by Discord, silently lost)")
        h.kevin.dms.clear()
        check(asyncio.run(botmod.ping_kevin("short")) and h.kevin.dms == ["short"],
              "ping_kevin: a normal DM is still exactly one message")

    # the Friday digest and !owe packets for a long unpaid queue
    tracking.state.save("owed", [{"id": f"owed_{9000 + i}", "channel_id": 9000 + i, "user": f"seller{i:02d}",
                                  "paid": False, "payout": 1000.0 + i, "cards": 5, "date": "2026-11-06",
                                  "tracking": "1Z999AA10123456784"} for i in range(40)])
    tracking.state.save("payinfo", {})
    try:
        digest = payout_digest.build_digest()
        check(units(digest) > 2000, "digest fixture is longer than one Discord message")
        with _BotHarness(8802) as h:
            check(asyncio.run(botmod.ping_kevin(digest)) and
                  sum(x.count("• **seller") for x in h.kevin.dms) == 40,
                  "payout digest: all 40 unpaid sellers reach Kevin across several DMs")
            km = _FakeMessage(_fake_channel(8803, "kevin-dm"), "!owe", author=h.kevin)
            km.channel.guild = None                                  # typed in a DM
            check(asyncio.run(commands.handle(None, km)) is True and
                  sum(x.count("Payment packet — seller") for x in h.kevin.dms) >= 40,
                  "!owe: every payment packet is delivered (was: one >2,000-char DM rejected)")

            # Discord refuses the 2nd message of a long DM: say PART arrived, not "nothing did"
            class _Flaky(_FakeUser):
                fail_at = 2

                async def send(self, text):
                    if len(self.dms) + 1 == self.fail_at:
                        raise RuntimeError("discord 503")
                    await super().send(text)
            flaky = _Flaky(uid=777, name="kevin")
            botmod.bot.get_user = lambda _uid: flaky
            alerts_mod = __import__("botv2.alerts", fromlist=["x"])
            alerts_mod.reset()
            ok_digest = asyncio.run(botmod._send_digest("payout", digest))
            botmod.bot.get_user = lambda _uid: h.kevin
            check(ok_digest is False and len(flaky.dms) == 1 and "Payout digest" in flaky.dms[0]
                  and "Only the first 1 of" in ((alerts_mod.last_error() or {}).get("text") or "")
                  and "could NOT be delivered to your DMs" not in ((alerts_mod.last_error() or {}).get("text") or ""),
                  "digest: when only part of it arrives the alert says so (it used to say none of it did)")
            flaky2 = _Flaky(uid=777, name="kevin")
            gm = _FakeMessage(_fake_channel(8805, "some-ticket"), "!owe", author=flaky2)
            asyncio.run(commands.handle(None, gm))
            check(gm.deleted and len(flaky2.dms) == 1
                  and gm.channel.sent == ["⚠️ Only part of the list reached your DMs — run `!owe` again."],
                  "!owe in a channel: the command is removed first, and a part-delivered list is called that")
            flaky3 = _Flaky(uid=777, name="kevin")
            flaky3.fail_at = 1
            gm3 = _FakeMessage(_fake_channel(8806, "some-ticket"), "!owe", author=flaky3)
            asyncio.run(commands.handle(None, gm3))
            check(gm3.deleted and gm3.channel.sent == ["⚠️ Couldn't DM you — open your DMs to the bot and retry."],
                  "!owe in a channel: nothing delivered still says to open DMs")
    finally:
        tracking.state.save("owed", [])

    # customer quote: many declined cards → a long message; it must still be posted, and
    # the rest of the flow (sheet step, Kevin's summary) must still run.
    def _rows(cert_list):
        out = []
        for i, c in enumerate(cert_list):
            if i % 2:
                out.append(PricedRow(cert=c, cert_id=c, category="pokemon", market_value=100, value=100,
                                     decision="BUY", offer=90, card_name="Charizard Holo " * 3, grader="PSA", grade="10"))
            else:
                out.append(PricedRow(cert=c, cert_id=c, category="basketball", market_value=450, value=450,
                                     decision="PASS", reason="outside the price range we are buying right now"))
        return out
    big = " ".join(str(20000000 + i) for i in range(90))
    with _BotHarness(8804, name="bigseller", rows=_rows) as h:
        h.say(big)
        posted = "\n".join(h.channel.sent)
        check(not h.errors and len(h.channel.sent) > 1 and all(units(x) <= 2000 for x in h.channel.sent),
              "quote: a long customer quote is posted in parts (was: rejected, handler aborted)")
        check(posted.count("outside the price range") == 45 and "Total payout" in posted
              and "proceed" in posted.lower(),
              "quote: totals, all 45 declined cards and the proceed line all reach the customer")
        kd = "\n".join(h.kevin.dms)
        check("bigseller" in kd and kd.count("Charizard") >= 45,
              "quote: Kevin's summary for the same lot arrives too (in parts)")
        check(any(r.get("id") == "tk_8804" for r in tracking.state.load("tickets", list)),
              "quote: ticket recorded")
    tracking.state.save("tickets", [])

    # Discord refuses the 2nd message of a long quote: the customer is left with a cut-off
    # quote, and Kevin must be told THAT (the generic handler error doesn't say it).
    with _BotHarness(8807, name="cutoff", rows=_rows, sheet_url="https://docs.google.com/spreadsheets/d/CUT8807/edit") as h:
        real_send, posted_n = h.channel.send, []

        async def _refuse_second(text):
            if len(posted_n) == 1:
                raise RuntimeError("discord 503")
            posted_n.append(1)
            await real_send(text)
        h.channel.send = _refuse_second
        h.say(big)
        kd = "\n".join(h.kevin.dms)
        check(len(h.errors) == 1 and len(h.channel.sent) == 1 and "PARTLY posted (1 of" in kd
              and "cut-off quote" in kd and not h.sheet_calls,
              "quote: a part-posted quote is reported to Kevin as cut off (handler still stops there, as before)")
    tracking.state.save("tickets", [])


# ── state: an unreadable store is preserved + reported, never silently replaced ──
def test_state_persistence():
    section("state persistence (corrupt / unreadable stores)")
    import glob
    import json
    from botv2 import alerts, state
    from botv2 import bot as botmod
    seen = []
    listener = lambda kind, name, detail: seen.append((kind, name, detail))
    state.add_listener(listener)
    path = lambda n: os.path.join(os.environ["DATA_DIR"], n + ".json")
    try:
        # missing store = the normal first run: default, no alarm
        check(state.load("zz_missing", list) == [] and not seen and not state.load_failures(),
              "state: a store that doesn't exist yet loads as the default, quietly")
        # healthy round-trip
        check(state.save("zz_ok", {"a": 1}) and state.load("zz_ok", dict) == {"a": 1} and not seen
              and not os.path.exists(path("zz_ok") + ".tmp"),
              "state: save + load round-trip, no temp file left behind")

        # CORRUPT store: default comes back (bot keeps running) but the bytes are kept + reported
        garbage = '{"owed": [ {"id": "owed_1", "payout": 900.0'            # truncated JSON
        with open(path("zz_corrupt"), "w") as f:
            f.write(garbage)
        check(state.load("zz_corrupt", list) == [], "state: corrupt store → default (never crashes)")
        copies = glob.glob(path("zz_corrupt") + ".corrupt-*")
        check(len(copies) == 1 and open(copies[0]).read() == garbage,
              "state: the corrupt file is copied aside immediately (evidence kept)")
        check(seen == [("load", "zz_corrupt", seen[0][2])] and "corrupt" in seen[0][2]
              and state.load_failures().get("zz_corrupt") == "corrupt",
              "state: listeners are told once, and the failure is remembered")
        check(f"copy kept: {os.path.basename(copies[0])}" in seen[0][2],
              "state: the alert names the copy that was actually kept")
        state.load("zz_corrupt", list); state.load("zz_corrupt", list)
        check(len(seen) == 1 and len(glob.glob(path("zz_corrupt") + ".corrupt-*")) == 1,
              "state: repeated loads of the same bad file don't re-alert or re-copy")
        # the caller's usual next move: save default+1 over it. Data must survive in the copy.
        check(state.save("zz_corrupt", [{"id": "new"}]) and state.load("zz_corrupt", list) == [{"id": "new"}],
              "state: the store is usable again after the next save")
        check(open(copies[0]).read() == garbage and "zz_corrupt" not in state.load_failures(),
              "state: the kept copy survives that save; the failure flag clears")

        # UNREADABLE store (can't even open it): simulated with a directory at the store's path.
        del seen[:]
        os.mkdir(path("zz_unreadable"))
        with open(os.path.join(path("zz_unreadable"), "real-data-marker"), "w") as f:
            f.write("precious")
        check(state.load("zz_unreadable", dict) == {} and seen and seen[0][0] == "load"
              and "unreadable" in seen[0][2], "state: unreadable store → default + reported")
        check(state.save("zz_unreadable", {"k": "v"}), "state: saving over an unreadable store succeeds")
        aside = glob.glob(path("zz_unreadable") + ".unreadable-*")
        check(len(aside) == 1 and open(os.path.join(aside[0], "real-data-marker")).read() == "precious",
              "state: …but the unreadable original was MOVED ASIDE first, not destroyed")
        check(state.load("zz_unreadable", dict) == {"k": "v"}, "state: new data readable afterwards")

        # ONE failed read poisons whatever was built from it (bot/tickets/vip cache a store at
        # import and never re-read it), so the next save must keep what is on disk even after
        # a later successful read — here, a file that became readable again. "Failing right
        # now" is a different fact and DOES end with that good read.
        del seen[:]
        with open(path("zz_flaky"), "w") as f:
            f.write("not json")
        cached = state.load("zz_flaky", list, cached=True)          # → [] (the "import-time cache")
        check(state.stuck().get("zz_flaky") == "corrupt" and state.load_failures().get("zz_flaky") == "corrupt",
              "state: a load-once cache built from a failed read is reported as stuck")
        with open(path("zz_flaky"), "w") as f:
            json.dump(["real", "history"], f)                       # the file is healthy again
        check(state.load("zz_flaky", list) == ["real", "history"] and "zz_flaky" not in state.load_failures(),
              "state: a later successful read ends the 'failing right now' state…")
        check(state.stuck().get("zz_flaky") == "corrupt" and "zz_flaky" in state.failures_since_start(),
              "state: …but the cache is still stuck on the default, and startup still reports it")
        check(state.save("zz_flaky", cached + ["one new row"]), "state: the stale cache is written back…")
        kept = glob.glob(path("zz_flaky") + ".unreadable-*")
        check(len(kept) == 1 and json.load(open(kept[0])) == ["real", "history"]
              and state.load("zz_flaky", list) == ["one new row"] and "zz_flaky" not in state.load_failures(),
              "state: …but the healthy file it replaced was kept (it used to be destroyed)")
        check([k for k, n, _d in seen if n == "zz_flaky"] == ["load"],
              "state: keeping the old file is not itself an alarm (one alert for the episode)")
        # a store that fails, recovers, and fails AGAIN is a new episode → a new alert
        del seen[:]
        with open(path("zz_again"), "w") as f:
            f.write("bad one")
        state.load("zz_again", list)
        with open(path("zz_again"), "w") as f:
            json.dump([1], f)
        state.load("zz_again", list)                                # healed, never saved
        with open(path("zz_again"), "w") as f:
            f.write("bad two")
        state.load("zz_again", list)
        check([k for k, n, _d in seen if n == "zz_again"] == ["load", "load"],
              "state: a second failure after a recovery alerts again (it used to be silent until a save)")

        # the same corrupt bytes are never kept twice — a restart re-reads the same bad file
        with open(path("zz_same"), "w") as f:
            f.write("still the same garbage")
        state.load("zz_same", list)
        del seen[:]
        with state._LOCK:                                           # "restart": the process forgets
            for d in (state._failing, state._must_keep, state._preserved):
                d.pop("zz_same", None)
        state.load("zz_same", list)
        first_copy = glob.glob(path("zz_same") + ".corrupt-*")
        check(len(first_copy) == 1 and len(seen) == 1
              and f"already kept as {os.path.basename(first_copy[0])}" in seen[0][2],
              "state: after a restart the same corrupt file is not copied again (and the alert says where it is)")

        # storm guard: at most 5 NEW copies per store per day; beyond that Kevin is told the
        # old bytes were NOT kept (he used to be told they were)
        del seen[:]
        for i in range(8):
            with open(path("zz_flap"), "w") as f:
                f.write("garbage %d" % i)
            state.load("zz_flap", list)
            state.save("zz_flap", [i])
        lost = [d for k, n, d in seen if (k, n) == ("lost", "zz_flap")]
        loads = [d for k, n, d in seen if (k, n) == ("load", "zz_flap")]
        check(len(glob.glob(path("zz_flap") + ".*-*")) == 5, "state: at most 5 kept copies per store per day")
        check(len(loads) == 8 and sum("(copy kept:" in d for d in loads) == 5
              and sum("(NO copy kept:" in d for d in loads) == 3,
              "state: each alert says truthfully whether a copy was kept")
        check(len(lost) == 3 and all("already kept in the last 24 h" in d for d in lost),
              "state: a store rewritten with nothing kept is reported as LOST, not passed over")
        # …and copies from an old incident don't block keeping new ones (no lifetime cap)
        for i in range(6):
            with open(path("zz_old") + f".corrupt-20250101T00000000000{i}Z-{i + 1}", "w") as f:
                f.write("an incident from long ago %d" % i)
        del seen[:]
        with open(path("zz_old"), "w") as f:
            f.write("fresh garbage")
        state.load("zz_old", list)
        state.save("zz_old", ["new"])
        check(len(glob.glob(path("zz_old") + ".corrupt-*")) == 7 and not any(k == "lost" for k, _n, _d in seen),
              "state: six kept copies from last year do not stop today's copy being kept")
        # the old bytes can't be kept for another reason (no hard link, no rename) → also LOST
        del seen[:]
        with open(path("zz_nolink"), "w") as f:
            f.write("{ nope")
        state.load("zz_nolink", list)
        for stale in glob.glob(path("zz_nolink") + ".corrupt-*"):
            os.unlink(stale)                                        # the copy made at load is gone
        with state._LOCK:
            state._preserved.pop("zz_nolink", None)
        real_link, real_replace = state.os.link, state.os.replace

        def _no_link(*_a, **_k):
            raise OSError("links not supported")

        def _replace_only_tmp(src, dst):
            if not str(src).endswith(".tmp"):
                raise OSError("rename refused")
            return real_replace(src, dst)
        state.os.link, state.os.replace = _no_link, _replace_only_tmp
        try:
            ok_save = state.save("zz_nolink", ["rebuilt"])
        finally:
            state.os.link, state.os.replace = real_link, real_replace
        check(ok_save and [(k, n) for k, n, _d in seen] == [("load", "zz_nolink"), ("lost", "zz_nolink")]
              and "rename refused" in seen[1][2],
              "state: when the old file can't be linked or moved aside, the save still works and Kevin is told it's LOST")

        # SAVE failure: reported, old contents untouched, no partial temp file
        del seen[:]
        state.save("zz_keep", ["old"])
        check(state.save("zz_keep", {"bad": object()}) is False, "state: an unserialisable save returns False")
        check(seen and seen[0][:2] == ("save", "zz_keep") and state.load("zz_keep", list) == ["old"]
              and not os.path.exists(path("zz_keep") + ".tmp"),
              "state: failed save is reported; previous contents intact")
        # …and a failure at the WRITE itself (the rename), which is where a partial temp file
        # would be left behind
        del seen[:]

        def _replace_fails(src, dst):
            raise OSError("No space left on device")
        state.os.replace = _replace_fails
        try:
            failed = state.save("zz_keep", ["new"])
        finally:
            state.os.replace = real_replace
        check(failed is False and [(k, n) for k, n, _d in seen] == [("save", "zz_keep")]
              and "No space left" in seen[0][2] and state.load("zz_keep", list) == ["old"]
              and not os.path.exists(path("zz_keep") + ".tmp"),
              "state: a save that fails at the rename is reported, leaves the old contents and no temp file")
        # a failed save must NOT forget that the old file still has to be kept
        with open(path("zz_keep2"), "w") as f:
            f.write("unparseable but precious")
        state.load("zz_keep2", list)
        for stale in glob.glob(path("zz_keep2") + ".corrupt-*"):
            os.unlink(stale)
        with state._LOCK:
            state._preserved.pop("zz_keep2", None)
        state.os.replace = _replace_fails
        try:
            state.save("zz_keep2", ["x"])
        finally:
            state.os.replace = real_replace
        check(state.save("zz_keep2", ["x"]) and any(
                  open(c).read() == "unparseable but precious" for c in glob.glob(path("zz_keep2") + ".*-*")),
              "state: after a failed save, the next save still keeps the unread file first")
    finally:
        state._listeners.remove(listener)

    # startup storage check: state kept inside the container / an unwritable data folder
    import botv2.config as cfg
    saved_default, saved_dir = cfg.DATA_DIR_IS_DEFAULT, cfg.DATA_DIR
    try:
        cfg.DATA_DIR_IS_DEFAULT = False
        check(state.writable() and botmod._startup_storage_problems() == []
              and not os.path.exists(os.path.join(cfg.DATA_DIR, ".write-probe")),
              "startup: a set, writable DATA_DIR raises nothing (and the probe leaves no file)")
        cfg.DATA_DIR_IS_DEFAULT = True
        check([k for k, _t in botmod._startup_storage_problems()] == ["data_dir_default"],
              "startup: DATA_DIR unset → Kevin is warned state will not survive a redeploy")
        saved_folder = cfg.PSA_FOLDER_ID
        try:
            cfg.DATA_DIR_IS_DEFAULT = False
            for blank in ("", "   "):
                cfg.PSA_FOLDER_ID = blank
                check(cfg.psa_folder_blank() and not cfg.psa_folder_unsafe()
                      and [k for k, _t in botmod._startup_storage_problems()] == ["psa_folder_blank"],
                      f"startup: PSA_FOLDER_ID={blank!r} (blank skips the v1-folder check) → Kevin is warned")
            cfg.PSA_FOLDER_ID = "a-real-folder-id"
            check(not cfg.psa_folder_blank() and botmod._startup_storage_problems() == [],
                  "startup: a real folder id raises nothing")
        finally:
            cfg.PSA_FOLDER_ID = saved_folder
        cfg.DATA_DIR_IS_DEFAULT = False
        blocker = os.path.join(saved_dir, "zz_not_a_dir")
        with open(blocker, "w") as f:
            f.write("x")
        cfg.DATA_DIR = os.path.join(blocker, "sub")            # can't be created: parent is a file
        check(state.writable() is False
              and [k for k, _t in botmod._startup_storage_problems()] == ["data_dir_write"],
              "startup: an unwritable data folder → Kevin is warned nothing is being saved")
    finally:
        cfg.DATA_DIR_IS_DEFAULT, cfg.DATA_DIR = saved_default, saved_dir

    # Google Sheets requests are bounded (gspread's default is no timeout at all)
    import sys
    import types
    from botv2 import sheets as sheets_mod
    seen_timeout = []
    fake_client = types.SimpleNamespace(set_timeout=lambda t: seen_timeout.append(t))
    fake_gspread = types.ModuleType("gspread")
    fake_gspread.authorize = lambda creds: fake_client
    real_gspread, real_creds = sys.modules.get("gspread"), sheets_mod.get_credentials
    sys.modules["gspread"] = fake_gspread
    sheets_mod.get_credentials = lambda: object()
    try:
        check(sheets_mod.get_gspread_client() is fake_client and seen_timeout == [cfg.SHEETS_TIMEOUT_S] == [120],
              "sheets: the gspread client is given a 120 s request timeout (it had none)")
        fake_gspread.authorize = lambda creds: object()         # an older client without set_timeout
        check(sheets_mod.get_gspread_client() is not None, "sheets: a client without set_timeout still works")
    finally:
        sheets_mod.get_credentials = real_creds
        if real_gspread is None:
            sys.modules.pop("gspread", None)
        else:
            sys.modules["gspread"] = real_gspread

    # bot wiring: the problem lands on /status, and (with a loop) in Kevin's DMs
    alerts.reset()
    botmod._on_state_problem("save", "owed", "No space left on device")
    le = alerts.last_error() or {}
    check(le.get("key") == "state_save:owed" and "owed" in le.get("text", ""),
          "state→bot: a save failure is recorded for /status even with no event loop")
    with _BotHarness(8811) as h:
        async def _go():
            botmod._on_state_problem("load", "tickets", "corrupt: Expecting value")
            for _ in range(5):
                await asyncio.sleep(0)
        asyncio.run(_go())
        check(len(h.kevin.dms) == 1 and "tickets.json" in h.kevin.dms[0] and "could not be read" in h.kevin.dms[0],
              "state→bot: Kevin is DMed when a data file can't be read")
        h.kevin.dms.clear()
        alerts.reset()

        async def _lost():
            botmod._on_state_problem("lost", "owed", "5 copies of it were already kept in the last 24 h")
            for _ in range(5):
                await asyncio.sleep(0)
        asyncio.run(_lost())
        check(len(h.kevin.dms) == 1 and "could NOT be kept" in h.kevin.dms[0] and "owed.json" in h.kevin.dms[0]
              and (alerts.last_error() or {}).get("key") == "state_lost:owed",
              "state→bot: Kevin is told when a store was rewritten and its old contents could not be kept")
    alerts.reset()

    # The real path end to end: a store goes bad while the bot is running and is read from a
    # WORKER THREAD (sheet work runs in threads) — the alert has to hop onto the bot's loop.
    saved_sets = {n: dict(getattr(state, n)) for n in ("_failing", "_must_keep", "_stuck", "_ever_failed", "_preserved")}
    with _BotHarness(8812) as h:
        with open(path("zz_thread"), "w") as f:
            f.write("{ broken")

        async def _threaded():
            botmod.bot.loop = asyncio.get_running_loop()            # what discord.py does at login
            try:
                got = await asyncio.to_thread(state.load, "zz_thread", dict)
                for _ in range(10):
                    await asyncio.sleep(0)
                return got
            finally:
                botmod.bot.__dict__.pop("loop", None)
        got = asyncio.run(_threaded())
        check(got == {} and len(h.kevin.dms) == 1 and "zz_thread.json" in h.kevin.dms[0]
              and "While it can't be read I treat `zz_thread` as EMPTY" in h.kevin.dms[0]
              and "copy kept: zz_thread.json.corrupt-" in h.kevin.dms[0],
              "state→bot: a read that fails on a worker thread reaches Kevin's DMs, saying what was kept")

    # Startup: failures from before the connection are reported ONCE, with wording that is
    # true at that moment; a reconnect repeats nothing.
    for n in ("_failing", "_must_keep", "_stuck", "_ever_failed", "_preserved"):
        getattr(state, n).clear()
    saved_default2, saved_folder2, saved_mock = cfg.DATA_DIR_IS_DEFAULT, cfg.PSA_FOLDER_ID, cfg.PRICE_MOCK
    saved_started, saved_runner = botmod._digest_started, botmod._web_runner
    saved_loops = (botmod._payout_digest_loop, botmod._heartbeat_loop, botmod.tickets.sweep_overflow)
    cfg.DATA_DIR_IS_DEFAULT, cfg.PSA_FOLDER_ID, cfg.PRICE_MOCK = False, "a-real-folder-id", False
    try:
        with open(path("zz_boot_cached"), "w") as f:
            f.write("<<garbage at boot>>")
        with open(path("zz_boot_reread"), "w") as f:
            f.write("<<garbage at boot>>")
        with open(path("zz_boot_healed"), "w") as f:
            f.write("<<garbage at boot>>")
        state.load("zz_boot_cached", dict, cached=True)             # e.g. channel_sheet / vip_rates
        state.load("zz_boot_reread", list)                          # e.g. owed, still bad at connect
        state.load("zz_boot_healed", list)
        with open(path("zz_boot_healed"), "w") as f:
            json.dump([], f)
        state.load("zz_boot_healed", list)                          # reads fine by the time we connect
        started = []

        async def _noop_loop():
            started.append(1)

        async def _noop_sweep(_bot):
            return None
        botmod._payout_digest_loop = botmod._heartbeat_loop = _noop_loop
        botmod.tickets.sweep_overflow = _noop_sweep
        with _BotHarness(8813) as h:
            botmod._web_runner, botmod._digest_started = object(), False     # web server "already up"

            async def _connect(times):
                botmod.bot.loop = asyncio.get_running_loop()
                try:
                    for _ in range(times):
                        await botmod.on_ready()
                    for _ in range(5):
                        await asyncio.sleep(0)
                finally:
                    botmod.bot.__dict__.pop("loop", None)
            asyncio.run(_connect(1))
            by = {n: next((d for d in h.kevin.dms if f"`{n}.json`" in d), "") for n in
                  ("zz_boot_cached", "zz_boot_reread", "zz_boot_healed")}
            check(len(h.kevin.dms) == 3 and all(by.values()),
                  "startup: every store that failed to read before the connection is reported at the first connect")
            check("then restart me" in by["zz_boot_cached"] and "loaded once" in by["zz_boot_cached"],
                  "startup: a load-once store says restore AND restart (a restore alone is overwritten)")
            check("While it can't be read I treat `zz_boot_reread` as EMPTY" in by["zz_boot_reread"],
                  "startup: a store that is still unreadable says so")
            check("reads again now" in by["zz_boot_healed"] and "EMPTY" not in by["zz_boot_healed"],
                  "startup: a store that has recovered is not described as empty")
            check(botmod._digest_started is True and len(started) == 2,
                  "startup: the scheduled loops are started once")
            h.kevin.dms.clear()
            alerts.reset()                                           # even with the rate limit out of the way…
            asyncio.run(_connect(3))
            check(h.kevin.dms == [] and len(started) == 2,
                  "reconnect: on_ready sends no storage alert and starts no second set of loops")
    finally:
        botmod._digest_started, botmod._web_runner = saved_started, saved_runner
        (botmod._payout_digest_loop, botmod._heartbeat_loop, botmod.tickets.sweep_overflow) = saved_loops
        cfg.DATA_DIR_IS_DEFAULT, cfg.PSA_FOLDER_ID, cfg.PRICE_MOCK = saved_default2, saved_folder2, saved_mock
        for n, old in saved_sets.items():
            getattr(state, n).clear()
            getattr(state, n).update(old)
    alerts.reset()


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
    # a customer who labels every slab: 4+ "PSA <number>" used to be dropped as chatter, silently
    check(ec("PSA 12345678 PSA 23456789 PSA 34567890 PSA 45678901")
          == ["12345678", "23456789", "34567890", "45678901"], "4 PSA-labelled certs are certs, not chatter")
    check(ec("PSA 12345678\nPSA: 23456789\nPSA #34567890\nPSA-45678901\nCGC 6193949247")
          == ["12345678", "23456789", "34567890", "45678901", "cgc:6193949247"],
          "a labelled mixed PSA/CGC list on separate lines is read in full")
    check(ec("psa psa psa psa is great 12345678") == [], "free-floating 'psa' words still count as chatter")
    check(ec("way too many random words here PSA 12345678") == [], "real chatter with one label is still ignored")
    # another grader's SHORT serial must not be quoted as a PSA cert
    check(ec("SGC 1234567") == [] and ec("BGS 12345678") == [] and ec("Beckett #12345678") == []
          and ec("TAG 1234567") == [], "7-9 digit BGS/SGC/TAG/Beckett serials are not PSA certs")
    check(ec("SGC 1234567 PSA 23456789 34567890") == ["23456789", "34567890"],
          "…while PSA-labelled and unlabelled numbers in the same message still are")
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
    # a BUY row with no usable offer is a malformed row → "re-check", never accepted at $0
    good = PricedRow(cert="5", cert_id="5", category="pokemon", market_value=100, value=100, decision="BUY", offer=90)
    for bad_offer in (None, 0, -5):
        bad = PricedRow(cert="6", cert_id="6", category="pokemon", market_value=100, value=100,
                        decision="BUY", offer=bad_offer)
        qb = pricing.build_quote([good, bad])
        check(qb.n_accepted == 1 and [r.cert for r in qb.unpriced] == ["6"] and abs(qb.payout_total - 90) < 0.01,
              f"BUY with offer={bad_offer!r} is not accepted at $0 (goes to re-check)")
        check("re-check" in pricing.customer_message(qb) and "no offer" in pricing.kevin_summary(qb, "x"),
              f"BUY with offer={bad_offer!r}: customer asked to resend; Kevin's summary says why")
    check(pricing.build_quote([good]).n_accepted == 1 and not good.error, "a well-formed BUY is untouched")
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

    # CLAIM SAFETY: with the bot-side flag off (the default, and forced off by this harness)
    # the claim client itself must short-circuit — never even reach the request layer.
    import botv2.config as _cfg
    reached = []
    real_claim_call = portal_client._claim_call

    async def _spy_claim_call(endpoint, *a, **k):
        reached.append(endpoint)
        return await real_claim_call(endpoint, *a, **k)
    portal_client._claim_call = _spy_claim_call
    try:
        check(_cfg.AUTO_CLAIM is False, "claim: BOTV2_AUTO_CLAIM is off in the selftest")
        cr = asyncio.run(portal_client.claim_certs(["12345678", "cgc:6193949247"], idempotency_key="t"))
        check(len(cr) == 2 and not any(r.ok or r.claimed for r in cr)
              and all("off" in r.skipped.lower() for r in cr),
              "claim client: flag off → every cert comes back skipped (nothing claims)")
        check(_cfg.CLAIM_ENDPOINT not in reached,
              "claim client: flag off → the /bot/claim request layer is never reached")
        ur = asyncio.run(portal_client.unclaim_certs(["12345678"]))
        check(len(ur) == 1 and reached == [_cfg.UNCLAIM_ENDPOINT], "unclaim client mock (the undo stays callable)")
    finally:
        portal_client._claim_call = real_claim_call


# ── PSA folder isolation guard ──────────────────────────────────────────────────
def test_psa_folder_guard():
    section("psa folder guard")
    import botv2.config as cfg
    import botv2.sheets as sheets_mod
    old = (cfg.PSA_FOLDER_ID, cfg.ALLOW_SHARED_PSA_FOLDER)
    try:
        cfg.PSA_FOLDER_ID, cfg.ALLOW_SHARED_PSA_FOLDER = cfg.V1_PSA_FOLDER_ID, False
        check(cfg.psa_folder_unsafe(), "v1 folder + no flag -> unsafe")
        # create_psa_sheet must refuse BEFORE any network/Google call. The Apps Script
        # call is stubbed with a counter, so a regressed tripwire shows up as a failed
        # check here instead of a real sheet in v1's live folder.
        calls = []
        orig_urlopen = sheets_mod.urllib.request.urlopen
        orig_sleep = sheets_mod.time.sleep

        def _spy(*a, **k):
            calls.append(a)
            raise RuntimeError("selftest: Apps Script call attempted")
        sheets_mod.urllib.request.urlopen = _spy
        sheets_mod.time.sleep = lambda *_a, **_k: None     # never sit in the retry/poll loop
        try:
            sheets_mod.create_psa_sheet("x", ["12345678"])
            check(False, "create_psa_sheet should refuse v1 folder")
        except RuntimeError as e:
            check("Refusing to create" in str(e), "create_psa_sheet refuses v1 folder (no network)")
        except Exception as e:
            check(False, f"wrong error type from guard: {e!r}")
        finally:
            sheets_mod.urllib.request.urlopen = orig_urlopen
            sheets_mod.time.sleep = orig_sleep
        check(not calls, "create_psa_sheet made NO Apps Script call for v1's folder")
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
def test_delivery_resolution():
    section("delivery event resolution")
    tracking.state.save("tickets", [])
    tracking.state.save("owed", [])
    tracking.record_quote(6101, {"username": "dlv", "comp": 100, "payout": 90, "cards": 1,
                                 "certs": {"d1": 100}, "cert_offers": {"d1": 90}}, channel_name="dlv")
    tracking.record_tracking(6101, "1Z999AA10123456784")
    # a channel_id that matches no ticket (e.g. rounded in transit) + the right tracking number
    r = tracking.mark_out_for_delivery(channel_id=6101000, tracking_num="1Z999AA10123456784")
    check(r["found"] is True and r["channel_id"] == 6101 and r["already"] is False,
          "delivery: a wrong channel_id falls back to the tracking number (the event used to be dropped)")
    r = tracking.mark_delivered(channel_id="not-a-ticket", tracking_num="1z999aa10123456784")
    check(r["found"] is True and r["channel_id"] == 6101 and tracking.delivery_status(6101).get("status") == "delivered",
          "delivery: same for 'delivered' (tracking match is case-insensitive)")
    check(tracking.mark_delivered(channel_id=6101, tracking_num="SOMETHING-ELSE")["already"] is True,
          "delivery: a channel_id that HAS a ticket still wins over the tracking number (idempotent)")
    check(tracking.mark_delivered(channel_id=999999, tracking_num="NOPE")["found"] is False
          and tracking.mark_delivered(channel_id=999999)["found"] is False,
          "delivery: nothing matches → still a clean miss")
    tracking.state.save("tickets", [])


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
        for wrong in ("selftestkeY", "selftestke", "selftestkey ", "sélftestkey", "\udcff"):
            check(st(webserver._ops(_Req(key=wrong))) == 401, f"auth: wrong key {wrong!r} rejected")
        cfg.BOT_API_KEY = ""                          # fails closed when no key is configured
        check(st(webserver._ops(_Req(key="anything"))) == 401,
              "auth: fails closed when BOT_API_KEY is unset")
        blank = _Req()
        blank.headers = {"x-bot-key": ""}             # unset key + EMPTY header must not "match"
        check(st(webserver._ops(_Req())) == 401 and st(webserver._ops(blank)) == 401
              and st(webserver._void(blank)) == 401,
              "auth: unset key + missing/empty header is still denied (no empty-equals-empty hole)")
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
    # strict trash-response parser (Codex blocker 3): only a JSON object that explicitly
    # confirms THIS sheet was trashed is success; every fail-open shape must fall through to
    # the (mocked-dead) SA fallback → False. No real network, no real Drive.
    _orig_urlopen = _sheets.urllib.request.urlopen
    _orig_drive = _sheets.get_drive_service
    def _no_sa():
        raise RuntimeError("no SA in test")
    def _mk_urlopen(body_str):
        class _R:
            def read(self, n=-1):
                return body_str.encode()
            def __enter__(self):
                return self
            def __exit__(self, *a):
                return False
        return lambda req, timeout=20: _R()
    _sheets.get_drive_service = _no_sa
    try:
        _TID = "SHEETZ123_-abc"
        _sheets.urllib.request.urlopen = _mk_urlopen(
            '{"success":true,"trashed":true,"sheet_id":"%s"}' % _TID)
        check(_sheets.trash_sheet(_TID) is True, "trash: strict parser accepts exact success")
        for _fb in ['{"success":"false","trashed":true,"sheet_id":"%s"}' % _TID,  # string 'false' is truthy
                    '{"success":1,"trashed":1,"sheet_id":"%s"}' % _TID,            # truthy non-bool
                    'the file was NOT trashed',                                   # denial contains "trashed"
                    '{"success":true,"trashed":true}',                            # missing sheet_id
                    '{"success":true,"trashed":true,"sheet_id":"OTHERID"}',       # wrong sheet_id
                    '{"success":true,"trashed":false,"sheet_id":"%s"}' % _TID,    # not actually trashed
                    '[true]', 'true', 'null', '{"success":true}', '<html>e</html>']:
            _sheets.urllib.request.urlopen = _mk_urlopen(_fb)
            check(_sheets.trash_sheet(_TID) is False,
                  "trash: strict parser rejects fail-open body %r" % _fb[:30])
    finally:
        _sheets.urllib.request.urlopen = _orig_urlopen
        _sheets.get_drive_service = _orig_drive
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
    # a fresh quote whose ticket-save FAILS must NOT clear the guard (no durable replacement quote)
    tracking.state.save("tickets", [])
    tracking.state.save("owed", [])
    tracking.state.save("voided", [])
    tracking.record_quote(982, {"username": "rq", "comp": 50, "payout": 45, "cards": 1,
                                "certs": {"rq1": 50}, "cert_offers": {"rq1": 45}}, channel_name="rq")
    tracking.void_ticket(982)
    check(tracking.is_voided(982), "void: guard set before the failed-requote case")
    _os4 = tracking.state.save
    tracking.state.save = lambda name, data: False if name == "tickets" else _os4(name, data)
    try:
        tracking.record_quote(982, {"username": "rq", "comp": 50, "payout": 45, "cards": 1,
                                    "certs": {"rq2": 50}, "cert_offers": {"rq2": 45}}, channel_name="rq")
    finally:
        tracking.state.save = _os4
    check(tracking.is_voided(982),
          "void: a fresh quote that fails to persist keeps the cancellation guard")
    # Fix B: void records a per-channel cancellation CUTOFF (permanent, survives a fresh quote) so
    # rebuild_from_sheets rebuilds only from sheets created strictly after it.
    tracking.state.save("tickets", [])
    tracking.state.save("voided", [])
    tracking.state.save("void_cutoff", {})
    tracking.state.save("channel_sheet", {"983": "https://docs.google.com/spreadsheets/d/SHEET983/edit"})
    tracking.record_quote(983, {"username": "fb", "comp": 50, "payout": 45, "cards": 1,
                                "certs": {"fb1": 50}, "cert_offers": {"fb1": 45}}, channel_name="fb")
    tracking.void_ticket(983)
    cutoff1 = tracking.void_cutoff_of(983)
    check(bool(cutoff1), "void: records a per-channel cancellation cutoff")
    tracking.record_quote(983, {"username": "fb", "comp": 50, "payout": 45, "cards": 1,
                                "certs": {"fb2": 50}, "cert_offers": {"fb2": 45}}, channel_name="fb")
    check(tracking.void_cutoff_of(983) == cutoff1,
          "void: cancellation cutoff is permanent — survives a fresh quote (reactivated channel)")
    tracking.state.save("tickets", [])
    tracking.state.save("owed", [])
    tracking.state.save("channel_sheet", {})
    tracking.state.save("voided", [])
    tracking.state.save("void_cutoff", {})


# ── replies and alerts that must tell the truth ─────────────────────────────────
def test_truthful_replies():
    section("truthful replies + alerts")
    import botv2.config as cfg
    from botv2 import bot as botmod, sheets as sheets_mod
    from botv2.portal_client import PortalUnavailable

    with _BotHarness(8950, name="truthy") as h:
        guild_chan = h.channel                          # a guild (ticket) channel: .guild is set

        # !payinfo: when the delete FAILS, don't say "I deleted the message".
        class _Undeletable(_FakeMessage):
            async def delete(self):
                raise RuntimeError("403 Missing Permissions")
        m = _Undeletable(guild_chan, "!payinfo list", author=h.kevin)
        check(asyncio.run(commands.handle(None, m)) is True and guild_chan.sent
              and "could NOT delete" in guild_chan.sent[-1] and "I deleted the message" not in guild_chan.sent[-1],
              "!payinfo: a failed delete is reported (it used to claim the message was deleted)")
        del guild_chan.sent[:]
        m2 = _FakeMessage(guild_chan, "!payinfo list", author=h.kevin)
        check(asyncio.run(commands.handle(None, m2)) is True and m2.deleted
              and "I deleted the message" in guild_chan.sent[-1], "!payinfo: normal path unchanged")

        # !paid: a failed write is not "no owed entry".
        del guild_chan.sent[:]
        tracking.state.save("owed", [{"id": "owed_8950", "channel_id": 8950, "user": "truthy",
                                      "paid": False, "payout": 100.0}])
        real = tracking.state.save
        tracking.state.save = lambda name, data: False if name == "owed" else real(name, data)
        try:
            asyncio.run(commands.handle(None, _FakeMessage(guild_chan, "!paid", author=h.kevin)))
        finally:
            tracking.state.save = real
        check("NOT marked paid" in guild_chan.sent[-1] and "No owed entry" not in guild_chan.sent[-1],
              "!paid: a failed save says so (it used to answer 'No owed entry')")
        asyncio.run(commands.handle(None, _FakeMessage(guild_chan, "!paid", author=h.kevin)))
        check(guild_chan.sent[-1] == "✅ Marked paid.", "!paid: works once the write succeeds")
        tracking.state.save("owed", [])
        asyncio.run(commands.handle(None, _FakeMessage(guild_chan, "!paid", author=h.kevin)))
        check("No owed entry" in guild_chan.sent[-1], "!paid: genuinely nothing owed still says so")

        # !bands: a raw network error gets the friendly reply too (only PortalError did).
        del h.kevin.dms[:]
        real_fetch = portal_client.fetch_allocation

        async def _boom():
            raise RuntimeError("Cannot connect to host")
        portal_client.fetch_allocation = _boom
        try:
            dm = _fake_channel(8951, "kevin-dm")
            dm.guild = None
            try:
                handled = asyncio.run(commands.handle(None, _FakeMessage(dm, "!bands", author=h.kevin)))
            except Exception:
                handled = False
        finally:
            portal_client.fetch_allocation = real_fetch
        check(handled and h.kevin.dms and "Couldn't load the allocation board" in h.kevin.dms[-1],
              "!bands: a network error is reported to Kevin instead of raising")

    # lone bare CGC-shaped number while pricing is DOWN: the alert must say pricing failed.
    async def _down(cert_list, customer=None, rate_override=None, session=None):
        raise PortalUnavailable("portal 503")
    with _BotHarness(8952, name="lonecgc") as h:
        real_price = portal_client.price_certs
        portal_client.price_certs = _down
        try:
            h.say("6193949247")
        finally:
            portal_client.price_certs = real_price
        kd = "\n".join(h.kevin.dms)
        check(not h.errors and "pricing error" in kd and "didn't resolve as a slab" not in kd and not h.channel.sent,
              "lone number during a pricing outage: Kevin is told pricing failed (not 'not a slab')")
        # …for EVERY ticket: the customer gets no reply here, so a DM suppressed by another
        # ticket's alert would leave nobody knowing to ask for a resend. Same alert window,
        # a normal quote failing first, then two more tickets with a lone number.
        del h.kevin.dms[:]
        portal_client.price_certs = _down
        try:
            h.say("12345678 23456789")                               # main path: the global pricing alert
            for cid, nm in ((8953, "lone-b"), (8954, "lone-c")):
                other = _fake_channel(cid, nm)
                h.tickets._known.add(cid)
                h.botmod._welcomed.add(cid)
                try:
                    asyncio.run(h.botmod._handle_message(_FakeMessage(other, "6193949247")))
                finally:
                    h.tickets._known.discard(cid)
                    h.botmod._welcomed.discard(cid)
                check(not other.sent, f"lone number in #{nm} during the outage: no reply in the channel (unchanged)")
        finally:
            portal_client.price_certs = real_price
        kd = "\n".join(h.kevin.dms)
        check("#lone-b while checking a lone number" in kd and "#lone-c while checking a lone number" in kd,
              "lone numbers during an outage: Kevin hears about each ticket (they were silenced by the first alert)")

    # ship with no in-memory lot and a failing Drive rebuild: the 'lot rebuild failed' alert fires.
    real_drive = sheets_mod.get_drive_service

    def _no_drive():
        raise RuntimeError("drive unavailable")
    sheets_mod.get_drive_service = _no_drive
    try:
        try:
            lots.rebuild_from_sheets(8953, "rebuilder")
            raised = False
        except RuntimeError:
            raised = True
        lots.drop(8953)
        check(raised and not lots.has_lot(8953), "rebuild_from_sheets: a Drive failure is raised, lot untouched")
        with _BotHarness(8953, name="rebuilder") as h:
            h.say("ship")
            kd = "\n".join(h.kevin.dms)
            check(not h.errors and "lot rebuild failed" in kd and "wrong hub" in kd,
                  "ship: Kevin is alerted when the lot rebuild fails (the alert was unreachable)")
            check(h.channel.sent and "Ship" in h.channel.sent[0],
                  "ship: the customer still gets the (fail-open) address")
    finally:
        sheets_mod.get_drive_service = real_drive


# ── the quote → sheet → ship → tracking handlers, driven end to end with fakes ──
def test_handlers_and_void_races():
    section("handlers + void races")
    import botv2.config as cfg
    from botv2 import bot as botmod, sheets as sheets_mod
    URL = "https://docs.google.com/spreadsheets/d/RACE%s/edit"

    def _rows(cert_list):
        return [PricedRow(cert=c, cert_id=c, category="pokemon", market_value=100, value=100,
                          decision="BUY", offer=90, card_name="Pikachu", grader="PSA", grade="10")
                for c in cert_list]

    # 1) normal quote: sheet cached + filled, Kevin gets the summary with the link, and the
    #    claim client is NEVER called while BOTV2_AUTO_CLAIM is off.
    claim_calls = []
    real_claim = portal_client.claim_certs

    async def _claim_spy(*a, **k):
        claim_calls.append(a)
        return []
    portal_client.claim_certs = _claim_spy
    try:
        with _BotHarness(9100, name="normalq", rows=_rows, sheet_url=URL % 9100) as h:
            h.say("12345678 23456789")
            check(not h.errors and len(h.channel.sent) == 1 and "Total payout" in h.channel.sent[0],
                  "quote: customer gets one quote message")
            check(cfg.AUTO_CLAIM is False and not claim_calls,
                  "quote: the claim client is not called while BOTV2_AUTO_CLAIM is off")
            check(h.sheet_calls == [("normalq", ["12345678", "23456789"])] and h.fill_calls == [URL % 9100]
                  and botmod._channel_sheet.get("9100") == URL % 9100
                  and tracking.state.load("channel_sheet", dict).get("9100") == URL % 9100,
                  "quote: buying sheet created, cached (memory + disk) and filled")
            check(any((URL % 9100) in d and "normalq" in d for d in h.kevin.dms),
                  "quote: Kevin's summary carries the sheet link")

            # 2) tracking in a normal (shipped) ticket: stored, Kevin told, no warning suffix
            h.say("ship")
            del h.kevin.dms[:], h.channel.sent[:]
            h.say("1Z999AA10123456784")
            check("noted" in h.channel.sent[-1] and any("Tracking from" in d and "⚠️" not in d for d in h.kevin.dms),
                  "tracking: normal ticket → ack + Kevin DM without a warning")
            check(h.tracking_saves == [(URL % 9100, "1Z999AA10123456784")]
                  and any("(saved to sheet)" in d for d in h.kevin.dms)
                  and tracking.find_tickets(channel_id=9100)[0].get("tracking") == "1Z999AA10123456784",
                  "tracking: number stored on the ticket and saved to the cached sheet")
            tracking.void_ticket(9100)
    finally:
        portal_client.claim_certs = real_claim

    # 3) a void that lands WHILE the buying sheet is building: the late sheet must not be
    #    cached, filled or summarised — Kevin gets its link to trash by hand.
    with _BotHarness(9101, name="racer", rows=_rows, sheet_url=URL % 9101) as h:
        voided = []

        def _slow_create(username, cert_tokens, folder_id=None):
            h.sheet_calls.append((username, list(cert_tokens)))
            voided.append(tracking.void_ticket(9101))        # Kevin's void runs mid-build
            return URL % 9101, f"{username} discord", {}
        sheets_mod.create_psa_sheet = _slow_create
        h.say("12345678")
        check(voided and voided[0].get("ticket_removed") is True, "race: the void removed the ticket mid-build")
        check(not h.errors and "9101" not in botmod._channel_sheet
              and "9101" not in tracking.state.load("channel_sheet", dict),
              "race: the late sheet is NOT cached for the dead quote (it used to be re-cached + persisted)")
        check(not h.fill_calls, "race: the late sheet is NOT filled with offers")
        kd = "\n".join(h.kevin.dms)
        check((URL % 9101) in kd and "VOIDED while its buying sheet" in kd and "📋" not in kd,
              "race: Kevin gets the sheet link to trash by hand, not a normal quote summary")
        check(tracking.is_voided(9101), "race: channel stays voided")

    # … and a void that lands BEFORE the build even starts (during the customer post): no sheet at all.
    with _BotHarness(9108, name="earlyvoid", rows=_rows, sheet_url=URL % 9108) as h:
        real_send = h.channel.send

        async def _send_then_void(text):
            await real_send(text)
            tracking.void_ticket(9108)                       # the void is processed during the post
        h.channel.send = _send_then_void
        h.say("12345678")
        check(not h.errors and not h.sheet_calls and not h.fill_calls and "9108" not in botmod._channel_sheet
              and any("VOIDED right after its quote" in d for d in h.kevin.dms),
              "race: a quote voided before its sheet build starts gets NO sheet (Kevin told)")

    # 3a) A void that BAILED (storage write failed, ticket still LIVE) while the sheet was
    #     building is not a void: the live quote keeps its sheet, its fill and its summary.
    #     (The first version keyed on the persisted cutoff, which a bailing void still moves.)
    for store in ("tickets", "owed"):
        with _BotHarness(9130, name="bailrace", rows=_rows, sheet_url=URL % 9130) as h:
            if store == "owed":                                  # an owed row must exist for that write to run
                h.say("11111111")
                h.say("ship")
                del h.sheet_calls[:], h.fill_calls[:], h.kevin.dms[:]
            results = []

            def _create_while_void_bails(username, cert_tokens, folder_id=None, _store=store):
                h.sheet_calls.append((username, list(cert_tokens)))
                real = tracking.state.save
                tracking.state.save = lambda name, data: False if name == _store else real(name, data)
                try:
                    results.append(tracking.void_ticket(9130))
                finally:
                    tracking.state.save = real
                return URL % 9130, f"{username} discord", {}
            sheets_mod.create_psa_sheet = _create_while_void_bails
            h.say("12345678")
            kd = "\n".join(h.kevin.dms)
            check(results and results[0].get("found") and not results[0].get("ticket_removed")
                  and tracking.find_tickets(channel_id=9130),
                  f"bail race ({store} write fails): the void bailed, the ticket is still live")
            check(not h.errors and h.fill_calls == [URL % 9130] and botmod._channel_sheet.get("9130") == URL % 9130
                  and "📋" in kd and "VOIDED" not in kd,
                  f"bail race ({store} write fails): the live quote's sheet is linked + filled and Kevin gets the summary")
            tracking.void_ticket(9130)
        lots.drop(9130)

    # 3b) A quote whose ticket row never reached disk, voided mid-build: there is no persisted
    #     marker for it at all, yet the void must still stop the late sheet.
    with _BotHarness(9131, name="memrace", rows=_rows, sheet_url=URL % 9131) as h:
        real = tracking.state.save
        tracking.state.save = lambda name, data: False if name == "tickets" else real(name, data)

        def _void_memory_only(username, cert_tokens, folder_id=None):
            h.sheet_calls.append((username, list(cert_tokens)))
            dmch = _fake_channel(9197, "kevin-dm")
            dmch.guild = None
            fut = asyncio.run_coroutine_threadsafe(
                commands.handle(None, _FakeMessage(dmch, "!void id:9131", author=h.kevin)), loop_box[0])
            fut.result(timeout=20)                              # Kevin's void runs on the loop, mid-build
            return URL % 9131, f"{username} discord", {}
        sheets_mod.create_psa_sheet = _void_memory_only
        loop_box = []

        async def _drive():
            loop_box.append(asyncio.get_running_loop())
            await botmod._handle_message(_FakeMessage(h.channel, "12345678"))
        try:
            asyncio.run(_drive())
        finally:
            tracking.state.save = real
        kd = "\n".join(h.kevin.dms)
        check(not tracking.find_tickets(channel_id=9131) and "in-memory quote" in kd,
              "memory-only race: the void (real command, mid-build) cleared the unsaved quote")
        check(not h.fill_calls and "9131" not in botmod._channel_sheet and 9131 not in botmod._last_quote
              and "VOIDED while its buying sheet was still building" in kd and (URL % 9131) in kd and "📋" not in kd,
              "memory-only race: the late sheet is not linked, filled or summarised (it used to be, after the void)")
    tracking.state.save("channel_sheets", {})

    # 3c) A void while the sheet build is FAILING (that can take minutes), and one while the
    #     sheet is being FILLED: neither may be followed by a normal quote summary.
    with _BotHarness(9132, name="failrace", rows=_rows, sheet_url=URL % 9132) as h:
        def _void_then_fail(username, cert_tokens, folder_id=None):
            tracking.void_ticket(9132)
            raise RuntimeError("Apps Script timeout")
        sheets_mod.create_psa_sheet = _void_then_fail
        h.say("12345678")
        kd = "\n".join(h.kevin.dms)
        check(not h.errors and "VOIDED while its buying sheet build was failing" in kd and "📋" not in kd
              and "summary below" not in kd,
              "void during a failing sheet build: one 'voided' note, no 'sheet failed — summary below', no summary")
    with _BotHarness(9133, name="fillrace", rows=_rows, sheet_url=URL % 9133) as h:
        def _void_during_fill(sheet_id, rows_, sheet_name=None):
            h.fill_calls.append(sheet_id)
            tracking.void_ticket(9133)
            return ["12345678"]                                  # would normally produce a "didn't land" DM
        sheets_mod.fill_buying_sheet = _void_during_fill
        h.say("12345678")
        kd = "\n".join(h.kevin.dms)
        check(not h.errors and "VOIDED while its buying sheet was being filled" in kd and "📋" not in kd
              and "didn't land" not in kd,
              "void during the fill: no quote summary and no 'didn't land on the sheet' follow the void")
    # a failing build with NO void still alerts and still sends the summary (unchanged)
    with _BotHarness(9134, name="plainfail", rows=_rows, sheet_url=URL % 9134) as h:
        def _just_fail(username, cert_tokens, folder_id=None):
            raise RuntimeError("Apps Script timeout")
        sheets_mod.create_psa_sheet = _just_fail
        h.say("12345678")
        kd = "\n".join(h.kevin.dms)
        check("buying sheet failed" in kd and "summary below" in kd and "📋" in kd and "VOIDED" not in kd,
              "sheet build fails with no void: alert + summary exactly as before")
        tracking.void_ticket(9134)
    lots.drop(9134)

    # 3d) The late sheet's link must survive a failed DM: it goes into the channel's sheet
    #     history, /status says so, and a later `!void id:` hands it over.
    with _BotHarness(9135, name="lostdm", rows=_rows, sheet_url=URL % 9135) as h:
        def _void_mid_build(username, cert_tokens, folder_id=None):
            tracking.void_ticket(9135)
            return URL % 9135, f"{username} discord", {}
        sheets_mod.create_psa_sheet = _void_mid_build
        real_dm = h.kevin.send

        async def _dm_down(text):
            raise RuntimeError("discord 503")
        h.kevin.send = _dm_down
        h.say("12345678")
        h.kevin.send = real_dm
        from botv2 import alerts as _al
        check(tracking.state.load("channel_sheets", dict).get("9135") == [URL % 9135]
              and (_al.last_error() or {}).get("key") == "late_sheet:9135"
              and (URL % 9135) not in ((_al.last_error() or {}).get("text") or ""),
              "late sheet, DM failed: the link is kept in the sheet history and /status flags it (without the link)")
        dmch = _fake_channel(9196, "kevin-dm")
        dmch.guild = None
        asyncio.run(commands.handle(None, _FakeMessage(dmch, "!void id:9135", author=h.kevin)))
        kd = "\n".join(h.kevin.dms)
        check((URL % 9135) in kd and "OTHER buying sheet(s) are on record" in kd and not h.trash_calls
              and "9135" not in tracking.state.load("channel_sheets", dict),
              "late sheet: a later `!void id:` hands Kevin the link (never auto-trashed) and clears the record")

    # 3e) A ticket that was already open when the sheet history was introduced: the one earlier
    #     sheet the bot still knows (its cache pointer) is recorded before it is replaced.
    with _BotHarness(9136, name="predeploy", rows=_rows, sheet_url=URL % "9136new") as h:
        botmod._channel_sheet["9136"] = URL % "9136old"
        tracking.state.save("channel_sheet", {"9136": URL % "9136old"})
        tracking.state.save("channel_sheets", {})
        h.say("12345678")
        check(tracking.state.load("channel_sheets", dict).get("9136") == [URL % "9136old", URL % "9136new"],
              "sheet history: the pointer being replaced is recorded first (it used to be dropped unrecorded)")
        dmch = _fake_channel(9195, "kevin-dm")
        dmch.guild = None
        del h.kevin.dms[:]
        asyncio.run(commands.handle(None, _FakeMessage(dmch, "!void id:9136", author=h.kevin)))
        check(h.trash_calls == [URL % "9136new"] and any((URL % "9136old") in d for d in h.kevin.dms),
              "…so the void trashes the latest and tells Kevin about the earlier one")

    # 4) ship in a voided channel: ignored (existing gate) …
    with _BotHarness(9102, name="shipvoid", rows=_rows, sheet_url=URL % 9102) as h:
        h.say("12345678")
        tracking.void_ticket(9102)
        lots.drop(9102)
        botmod._last_quote.pop(9102, None)
        del h.channel.sent[:], h.kevin.dms[:]
        h.say("ship")
        check(not h.channel.sent and any("VOIDED channel" in d for d in h.kevin.dms),
              "ship on a voided channel: no address posted, Kevin told")

    # … and a void that lands DURING the ship-time lot rebuild still wins (TOCTOU).
    with _BotHarness(9103, name="shiprace", rows=_rows, sheet_url=URL % 9103) as h:
        h.say("12345678")
        lots.drop(9103)                                       # as after a redeploy: no in-memory lot
        real_rebuild = lots.rebuild_from_sheets

        def _rebuild(cid, username, days=4, created_after=None):
            tracking.void_ticket(cid)                         # the void lands mid-rebuild …
            lots.add_slab_values(cid, {"x": 100.0}, {"x": "basketball"})   # … and the thread re-creates a lot
            return True
        lots.rebuild_from_sheets = _rebuild
        del h.channel.sent[:], h.kevin.dms[:]
        try:
            h.say("ship")
        finally:
            lots.rebuild_from_sheets = real_rebuild
        check(not h.errors and not h.channel.sent,
              "ship race: no shipping address is posted when the void landed during the rebuild")
        check(not lots.has_lot(9103) and any("VOIDED channel" in d for d in h.kevin.dms)
              and not any(r.get("id") == "owed_9103" for r in tracking.state.load("owed", list)),
              "ship race: the re-created lot is dropped, nothing booked, Kevin told")

    # 5) tracking number in a VOIDED channel / a channel with no ticket: customer ack unchanged,
    #    but Kevin is told the number was not stored.
    with _BotHarness(9104, name="trackvoid", rows=_rows, sheet_url=URL % 9104) as h:
        h.say("12345678")
        dm = _fake_channel(9199, "kevin-dm")
        dm.guild = None
        asyncio.run(commands.handle(None, _FakeMessage(dm, "!void id:9104", author=h.kevin)))   # a real void
        check(h.trash_calls == [URL % 9104] and "9104" not in botmod._channel_sheet,
              "void (through the command): sheet trashed once, cache cleared")
        del h.channel.sent[:], h.kevin.dms[:]
        h.say("1Z999AA10123456784")
        check(len(h.channel.sent) == 1 and "noted" in h.channel.sent[0] and not h.tracking_saves,
              "tracking on a voided channel: customer ack unchanged, nothing written to a sheet")
        check(any("VOIDED" in d and "NOT stored" in d for d in h.kevin.dms),
              "tracking on a voided channel: Kevin is told it was voided and the number isn't stored")
    with _BotHarness(9105, name="tracknone") as h:
        h.say("1Z999AA10123456784")
        check(any("no ticket on record" in d for d in h.kevin.dms),
              "tracking with no ticket on record: Kevin is told the number isn't stored")
    # "Shipped! <number>" on a VOIDED channel: a ship-shaped message there gets no reply, and
    # that must stay true when it also carries a tracking number — no ack, nothing registered
    # with the delivery watcher. Kevin still gets the number.
    registered = []
    real_register = portal_client.register_tracking

    async def _register_spy(cid, tnum):
        registered.append((cid, tnum))
    portal_client.register_tracking = _register_spy
    saved_reg = cfg.REGISTER_TRACKING
    cfg.REGISTER_TRACKING = True
    try:
        with _BotHarness(9113, name="shippedvoid", rows=_rows, sheet_url=URL % 9113) as h:
            h.say("12345678")
            tracking.void_ticket(9113)
            lots.drop(9113)
            botmod._last_quote.pop(9113, None)
            botmod._channel_sheet.pop("9113", None)
            del h.channel.sent[:], h.kevin.dms[:]
            h.say("Shipped! 1Z999AA10123456784")
            kd = "\n".join(h.kevin.dms)
            check(not h.errors and h.channel.sent == [] and not registered,
                  "'Shipped! <#>' on a voided channel: nothing is posted to the customer, nothing registered")
            check("ship attempt on a VOIDED channel" in kd and "1Z999AA10123456784" in kd
                  and "NOT stored" in kd and "No reply was posted to the customer" in kd,
                  "'Shipped! <#>' on a voided channel: Kevin gets the number and is told the customer saw no reply")
            h.say("1Z999AA10123456784")                           # a bare number is acked as it always was
            check(len(h.channel.sent) == 1 and "noted" in h.channel.sent[0] and registered == [(9113, "1Z999AA10123456784")],
                  "a bare tracking number on a voided channel: acked and registered exactly as before")
        # A void that BAILED leaves the cancel guard set on a LIVE ticket. A tracking number
        # there IS stored, and Kevin must not be told it wasn't (29211b1).
        with _BotHarness(9114, name="bailtrk", rows=_rows, sheet_url=URL % 9114) as h:
            h.say("12345678")
            real = tracking.state.save
            tracking.state.save = lambda name, data: False if name == "tickets" else real(name, data)
            try:
                bail = tracking.void_ticket(9114)
            finally:
                tracking.state.save = real
            del h.kevin.dms[:]
            h.say("1Z999AA10123456784")
            kd = "\n".join(h.kevin.dms)
            check(bail.get("found") and not bail.get("ticket_removed") and tracking.is_voided(9114)
                  and tracking.find_tickets(channel_id=9114)[0].get("tracking") == "1Z999AA10123456784"
                  and "Tracking from" in kd and "NOT stored" not in kd,
                  "tracking after a bailed void: the number is stored on the live ticket and Kevin isn't told otherwise")
            tracking.void_ticket(9114)
        lots.drop(9114)
    finally:
        portal_client.register_tracking, cfg.REGISTER_TRACKING = real_register, saved_reg

    # 5a) the tracking ack only PROMISES an arrival update when one can actually happen:
    #     customer updates on + the delivery watcher on + the number stored on a ticket.
    promise, neutral = "the moment it arrives", "Kevin will keep an eye out"
    saved_flags = (cfg.DELIVERY_TRACKING, cfg.REGISTER_TRACKING)
    try:
        for dt, rt, has_ticket, want_promise in ((False, False, True, False), (True, False, True, False),
                                                 (False, True, True, False), (True, True, True, True),
                                                 (True, True, False, False)):
            cfg.DELIVERY_TRACKING, cfg.REGISTER_TRACKING = dt, rt
            with _BotHarness(9120, name="ackcust", rows=_rows, sheet_url=URL % 9120) as h:
                if has_ticket:
                    h.say("12345678")
                    h.say("ship")
                del h.channel.sent[:]
                h.say("1Z999AA10123456784")
                ack = h.channel.sent[0] if h.channel.sent else ""
                check(not h.errors and len(h.channel.sent) == 1
                      and (promise in ack) == want_promise and (neutral in ack) == (not want_promise),
                      f"tracking ack: updates={dt} watcher={rt} ticket={has_ticket} → "
                      f"{'promise' if want_promise else 'neutral wording'}")
                tracking.void_ticket(9120)
            lots.drop(9120)
    finally:
        cfg.DELIVERY_TRACKING, cfg.REGISTER_TRACKING = saved_flags

    # 5b) "Shipped! <tracking #>" — agree-shaped AND a tracking drop: the number must not be lost.
    with _BotHarness(9109, name="shippedtrk", rows=_rows, sheet_url=URL % 9109) as h:
        h.say("12345678")
        del h.channel.sent[:], h.kevin.dms[:]
        h.say("Shipped! 1Z999AA10123456784")
        t = tracking.find_tickets(channel_id=9109)[0]
        check(not h.errors and t.get("status") == "accepted" and "Ship" in h.channel.sent[0],
              "'Shipped! <#>': still handled as a ship (address + lot booked — unchanged)")
        check(t.get("tracking") == "1Z999AA10123456784" and any("noted" in m for m in h.channel.sent)
              and h.tracking_saves == [(URL % 9109, "1Z999AA10123456784")]
              and any("Tracking from" in d for d in h.kevin.dms),
              "'Shipped! <#>': the tracking number is now acked, stored and saved (it used to be dropped)")
        owed = next(r for r in tracking.state.load("owed", list) if r["id"] == "owed_9109")
        check(owed.get("tracking") == "1Z999AA10123456784" and owed.get("paid") is False,
              "'Shipped! <#>': the number lands on the owed row the ship just booked")
        # plain agree and plain tracking messages are unchanged
        del h.channel.sent[:], h.tracking_saves[:]
        h.say("proceed")
        check(len(h.channel.sent) == 1 and "Ship" in h.channel.sent[0] and not h.tracking_saves,
              "plain 'proceed': one address message, no tracking handling")
        tracking.void_ticket(9109)
    lots.drop(9109)

    # 5c) a ticket quoted over TWO messages has TWO buying sheets. A void trashes only the
    #     latest (unchanged) — but it must tell Kevin the earlier one is still live. It used
    #     to say "Buying sheet moved to Drive trash" and nothing else.
    def _void_dm(h, cid):
        del h.kevin.dms[:]
        dm = _fake_channel(9198, "kevin-dm")
        dm.guild = None
        asyncio.run(commands.handle(None, _FakeMessage(dm, f"!void id:{cid}", author=h.kevin)))
        return "\n".join(h.kevin.dms)
    with _BotHarness(9110, name="twosheets", rows=_rows, sheet_url=URL % "9110A") as h:
        h.say("12345678")
        h.sheet_url = URL % "9110B"
        h.say("23456789")
        check(tracking.state.load("channel_sheets", dict).get("9110") == [URL % "9110A", URL % "9110B"]
              and botmod._channel_sheet.get("9110") == URL % "9110B",
              "sheets: every sheet built for a ticket is remembered (the cache still holds only the latest)")
        dm = _void_dm(h, 9110)
        check(h.trash_calls == [URL % "9110B"], "void: still auto-trashes ONLY the latest sheet (no new trash)")
        check("moved to Drive trash" in dm and (URL % "9110A") in dm and "OTHER buying sheet(s) are on record" in dm
              and "NOT auto-trashed" in dm,
              "void: Kevin is told the earlier sheet was not trashed, with its link")
        check("9110" not in tracking.state.load("channel_sheets", dict)
              and "9110" not in tracking.state.load("channel_sheet", dict),
              "void: the sheet history is cleared with the cache")
    with _BotHarness(9111, name="onesheet", rows=_rows, sheet_url=URL % "9111A") as h:
        h.say("12345678")
        dm = _void_dm(h, 9111)
        check(h.trash_calls == [URL % "9111A"] and "OTHER buying sheet" not in dm and "moved to Drive trash" in dm,
              "void: a single-sheet ticket reads exactly as before")
    # a bailed void keeps the history (nothing is cleaned for a live ticket)
    with _BotHarness(9112, name="bailsheets", rows=_rows, sheet_url=URL % "9112A") as h:
        h.say("12345678")
        h.sheet_url = URL % "9112B"
        h.say("23456789")
        real = tracking.state.save
        tracking.state.save = lambda name, data: False if name == "tickets" else real(name, data)
        try:
            dm = _void_dm(h, 9112)
        finally:
            tracking.state.save = real
        check("STILL LIVE" in dm and not h.trash_calls and "OTHER buying sheet" not in dm
              and len(tracking.state.load("channel_sheets", dict).get("9112") or []) == 2,
              "void bail: sheet history untouched, nothing reported as dead")
        _void_dm(h, 9112)

    # 6) save_tracking reads the Apps Script reply: an explicit failure is an error
    def _reply(body):
        class _R:
            def read(self, n=-1):
                return body.encode()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False
        return lambda req, timeout=15: _R()
    real_urlopen = sheets_mod.urllib.request.urlopen
    try:
        def saves(body):
            sheets_mod.urllib.request.urlopen = _reply(body)
            try:
                sheets_mod.save_tracking(URL % 1, "1Z999AA10123456784")
                return True
            except RuntimeError:
                return False
        check(saves('{"success": true}') and saves('{"ok": 1}') and saves("<html>ok</html>") and saves(""),
              "save_tracking: success / unknown / non-JSON replies are accepted as before")
        check(not saves('{"success": false, "error": "unauthorized"}') and not saves('{"error": "no such sheet"}'),
              "save_tracking: an explicit failure reply raises (Kevin gets 'add it by hand', not 'saved to sheet')")
    finally:
        sheets_mod.urllib.request.urlopen = real_urlopen

    # 7) record_claim is update-only: it can never re-create a row for a voided channel
    tracking.state.save("tickets", [{"id": "tk_9106", "channel_id": 9106, "user": "c", "status": "quoted"}])
    check(tracking.record_claim(9106, 1, 2) is True
          and tracking.find_tickets(channel_id=9106)[0]["claimed"]["locked"] == 1,
          "record_claim: updates an existing ticket")
    check(tracking.record_claim(9107, 1, 2) is False and not any(
        r.get("id") == "tk_9107" for r in tracking.state.load("tickets", list)),
          "record_claim: no ticket (voided mid-claim) → nothing is created")
    for st in ("tickets", "owed", "channel_sheet", "voided", "void_cutoff"):
        tracking.state.save(st, {} if st in ("channel_sheet", "void_cutoff") else [])


# ── the running lot (ship routing): value-only cards count; declined cards leave ──
def test_lot_routing():
    section("lot routing")
    from botv2 import bot as botmod
    URL = "https://docs.google.com/spreadsheets/d/LOT%s/edit"
    hoop = lambda c: PricedRow(cert=c, cert_id=c, category="basketball", market_value=300, value=300,
                               decision="BUY", offer=270, grader="PSA", grade="10")
    # a CGC priced off its last sale: the portal sends `value` but NO marketValue
    cgc = lambda c: PricedRow(cert=c, cert_id=c[4:], grader="CGC", grade="9.5", category="pokemon",
                              value=80, value_basis="lastSold", decision="BUY", offer=72)
    check(botmod._lot_values([hoop("1"), cgc("cgc:6193949247")]) == {"1": 300, "cgc:6193949247": 80},
          "lot values: marketValue when present (unchanged), else the value the offer was priced on")

    # alone: the quote must end with the normal proceed line, not "isn't finalized yet"
    with _BotHarness(9300, name="cgconly", rows=lambda cl: [cgc(c) for c in cl], sheet_url=URL % 9300) as h:
        h.say("CGC 6193949247")
        check(not h.errors and "isn't finalized" not in h.channel.sent[0] and "proceed" in h.channel.sent[0].lower(),
              "value-only CGC quoted alone: normal proceed line (it used to say 'your quote isn't finalized yet')")
        check(lots.has_lot(9300) and tracking.find_tickets(channel_id=9300)[0]["ledger"]["cgc:6193949247"]["comp"] == 80.0,
              "value-only CGC: in the running lot, and its ledger comp is its value (was 0)")

    # mixed with sports: BOTH addresses must be given
    def mixed(cl):
        return [cgc(c) if c.startswith("cgc:") else hoop(c) for c in cl]
    with _BotHarness(9301, name="mixedlot", rows=mixed, sheet_url=URL % 9301) as h:
        h.say("12345678 CGC 6193949247")
        del h.channel.sent[:]
        h.say("ship")
        ship = h.channel.sent[0]
        check("Virginia Beach" in ship and "Doral" in ship,
              "mixed lot with a value-only CGC Pokémon: BOTH hubs in the ship reply (the Pokémon lane was missing)")

    # a card declined on a re-quote leaves the lot, so its lane leaves the ship reply
    state = {"pass_hoop": False}

    def requote(cl):
        out = []
        for c in cl:
            if c.startswith("cgc:"):
                out.append(cgc(c))
            elif state["pass_hoop"]:
                out.append(PricedRow(cert=c, cert_id=c, category="basketball", market_value=300, value=300,
                                     decision="PASS", reason="out of band"))
            else:
                out.append(hoop(c))
        return out
    with _BotHarness(9302, name="declined", rows=requote, sheet_url=URL % 9302) as h:
        h.say("12345678 CGC 6193949247")
        check(lots.value_by_lane(9302) == (80.0, 300.0), "lot before the re-quote: Pokémon $80 + sports $300")
        state["pass_hoop"] = True
        h.say("12345678")                                   # the basketball card is now declined
        check(lots.value_by_lane(9302) == (80.0, 0.0), "re-quote PASS: the declined card leaves the running lot")
        del h.channel.sent[:]
        h.say("ship")
        ship = h.channel.sent[0]
        check("Virginia Beach" in ship and "Doral" not in ship,
              "ship after that: only the hub for cards we're still buying (Doral used to be listed too)")
        check(next(r for r in tracking.state.load("owed", list) if r["id"] == "owed_9302")["payout"] == 72.0,
              "…and only the still-accepted card is owed (ledger behaviour unchanged)")
    # …but a card that ALREADY SHIPPED is locked in the ledger (still owed), so a later
    # re-quote that declines it must NOT take its hub away. (The first version of this fix
    # removed every declined card: a sports lot's next ship reply fell back to the Pokémon
    # hub while the ledger still owed the sports cards.)
    state["pass_hoop"] = False
    with _BotHarness(9303, name="hoops", rows=requote, sheet_url=URL % 9303) as h:
        h.say("12345678 23456789")
        h.say("ship")
        check("Doral" in h.channel.sent[-1] and lots.value_by_lane(9303) == (0.0, 600.0),
              "shipped sports lot: Doral address, both cards in the lot")
        state["pass_hoop"] = True
        h.say("12345678 23456789")                          # re-listed; the portal now declines them
        check(lots.value_by_lane(9303) == (0.0, 600.0),
              "re-quote PASS on cards that already shipped: they stay in the running lot (ledger still owes them)")
        del h.channel.sent[:]
        h.say("still ship?")
        check(h.channel.sent and "Doral" in h.channel.sent[0] and "Virginia Beach" not in h.channel.sent[0],
              "ship after that: still the sports hub (it had fallen back to Virginia Beach)")
        check(next(r for r in tracking.state.load("owed", list) if r["id"] == "owed_9303")["payout"] == 540.0,
              "…and the shipped cards are still owed in full")
    # mixed: the shipped sports card is declined on a re-quote that adds a new Pokémon card
    state["pass_hoop"] = False
    with _BotHarness(9304, name="mixedshipped", rows=requote, sheet_url=URL % 9304) as h:
        h.say("12345678 CGC 6193949247")
        h.say("ship")
        state["pass_hoop"] = True
        h.say("12345678 CGC 6193949248")
        del h.channel.sent[:]
        h.say("ship")
        check("Virginia Beach" in h.channel.sent[0] and "Doral" in h.channel.sent[0],
              "mixed lot: a shipped sports card declined later keeps its hub next to the Pokémon one")
    state["pass_hoop"] = False
    lots.remove_certs(424242, ["x"])                        # unknown channel: no error
    for st in ("tickets", "owed", "channel_sheet", "channel_sheets", "voided", "void_cutoff"):
        tracking.state.save(st, {} if st in ("channel_sheet", "channel_sheets", "void_cutoff") else [])


# ── the ship booking step: say what was booked, never half-book, never book "nothing" ──
def test_ship_booking():
    section("ship booking")
    from botv2 import alerts, bot as botmod
    URL = "https://docs.google.com/spreadsheets/d/BOOK%s/edit"
    for st in ("tickets", "owed"):
        tracking.state.save(st, [])

    def buy(cert_list):
        return [PricedRow(cert=c, cert_id=c, category="pokemon", market_value=100, value=100,
                          decision="BUY", offer=90, card_name="Pikachu", grader="PSA", grade="10")
                for c in cert_list]

    def all_pass(cert_list):
        return [PricedRow(cert=c, cert_id=c, category="basketball", market_value=450, value=450,
                          decision="PASS", reason="out of band") for c in cert_list]

    def owed_row(cid):
        return next((r for r in tracking.state.load("owed", list) if r.get("id") == f"owed_{cid}"), None)

    # 1) normal ship: Kevin's DM now says what the ship booked
    with _BotHarness(9200, name="booker", rows=buy, sheet_url=URL % 9200) as h:
        h.say("12345678 23456789")
        del h.kevin.dms[:]
        h.say("ship")
        o = owed_row(9200)
        check(o and o["payout"] == 180.0 and o["paid"] is False, "ship: lot booked as owed (unchanged)")
        check(any("ship:" in d and "booked — owed on this ticket is now $180.00 (2 card(s))" in d for d in h.kevin.dms),
              "ship: Kevin's DM states what was booked")

    # 2) 'ship' after a quote with NOTHING accepted: no $0 'paid' row, ticket stays quoted
    with _BotHarness(9201, name="nobuys", rows=all_pass, sheet_url=URL % 9201) as h:
        h.say("12345678 23456789")
        del h.kevin.dms[:], h.channel.sent[:]
        h.say("ship anyway?")
        t = tracking.find_tickets(channel_id=9201)[0]
        check(owed_row(9201) is None and t.get("status") == "quoted",
              "ship with nothing accepted: no owed row, ticket stays 'quoted' (it used to become "
              "'accepted' with a $0 row marked paid)")
        check(any("NOTHING BOOKED" in d for d in h.kevin.dms), "ship with nothing accepted: Kevin is told nothing was booked")
        check(len(h.channel.sent) == 1 and "Ship" in h.channel.sent[0],
              "ship with nothing accepted: the customer reply is UNCHANGED here (flagged for Kevin)")
        check(len(tracking.find_tickets(user="nobuys", status="quoted")) == 1,
              "…so the ticket can still be voided by username")
        # a real quote afterwards books normally, with no phantom card
        h.rows = buy
        h.say("34567890")
        h.say("ship")
        t = tracking.find_tickets(channel_id=9201)[0]
        check(owed_row(9201)["payout"] == 90.0 and t["cards"] == 1 and "_carried" not in (t.get("ledger") or {}),
              "a later real lot on that ticket books exactly its own card (no phantom carried lot)")

    # 3) a $0 'paid' row already on disk from before this fix is not carried as a phantom lot
    tracking.state.save("tickets", [{"id": "tk_9202", "channel_id": 9202, "user": "oldzero",
                                     "status": "accepted", "cards": 0, "payout": 0, "ledger": {}}])
    tracking.state.save("owed", [{"id": "owed_9202", "channel_id": 9202, "user": "oldzero", "paid": True,
                                  "payout": 0.0, "certs": {}, "cert_payout": {}, "cards": 0}])
    tracking.record_quote(9202, {"username": "oldzero", "comp": 100, "payout": 90, "cards": 1,
                                 "certs": {"z9": 100}, "cert_offers": {"z9": 90}}, channel_name="oldzero")
    t = tracking.find_tickets(channel_id=9202)[0]
    check("_carried" not in t["ledger"] and t["cards"] == 1,
          "an old empty $0 owed row is not migrated into a phantom paid lot")
    # …while a real legacy balance still is (exact total carried)
    tracking.state.save("tickets", [])
    tracking.state.save("owed", [{"id": "owed_9203", "channel_id": 9203, "user": "legacy", "paid": False,
                                  "payout": 250.0, "comp": 300.0, "certs": {"L1": 300}, "cards": 1}])
    tracking.record_quote(9203, {"username": "legacy", "comp": 100, "payout": 90, "cards": 1,
                                 "certs": {"n1": 100}, "cert_offers": {"n1": 90}}, channel_name="legacy")
    led = tracking.find_tickets(channel_id=9203)[0]["ledger"]
    check(led.get("_carried") and led["_carried"][0]["offer"] == 250.0, "a real legacy balance is still carried verbatim")

    # 4) a failed TICKET write at ship: nothing is half-booked, and Kevin is told
    for st in ("tickets", "owed"):
        tracking.state.save(st, [])
    with _BotHarness(9204, name="diskfull", rows=buy, sheet_url=URL % 9204) as h:
        h.say("12345678")
        del h.kevin.dms[:]
        real = tracking.state.save
        tracking.state.save = lambda name, data: False if name == "tickets" else real(name, data)
        try:
            h.say("ship")
        finally:
            tracking.state.save = real
        check(owed_row(9204) is None and tracking.find_tickets(channel_id=9204)[0]["status"] == "quoted",
              "ship with a failed ticket write: NO owed row is written on top of a ledger that didn't save")
        check(any("did NOT save" in d for d in h.kevin.dms)
              and (alerts.last_error() or {}).get("key") == "ship_book:9204",
              "ship with a failed write: Kevin is told the lot did not book (and /status records it)")
        h.say("ship")                                       # storage healthy again → books
        check(owed_row(9204) and owed_row(9204)["payout"] == 90.0, "ship again once storage works: booked")
    # a failed OWED write is reported the same way
    with _BotHarness(9205, name="owedfail", rows=buy, sheet_url=URL % 9205) as h:
        h.say("12345678")
        del h.kevin.dms[:]
        real = tracking.state.save
        tracking.state.save = lambda name, data: False if name == "owed" else real(name, data)
        try:
            h.say("ship")
        finally:
            tracking.state.save = real
        check(owed_row(9205) is None and any("did NOT save" in d for d in h.kevin.dms),
              "ship with a failed owed write: reported, not silent")
    # ship in a ticket the bot never quoted: address posted (fail-open, unchanged), nothing booked, Kevin told
    with _BotHarness(9206, name="neverquoted") as h:
        h.say("ship")
        check(h.channel.sent and owed_row(9206) is None and any("NOTHING BOOKED" in d for d in h.kevin.dms),
              "ship with no quote on record: Kevin is told nothing was booked")
    # 5) a second lot on a ticket whose first lot was PAID must not inherit the first lot's
    #    ship date / tracking number (Kevin's digest and `!owe` showed last time's).
    for st in ("tickets", "owed"):
        tracking.state.save(st, [])
    lot = lambda cert: {"username": "repeat", "comp": 100, "payout": 90, "cards": 1,
                        "certs": {cert: 100}, "cert_offers": {cert: 90}}
    tracking.record_quote(9210, lot("r1"), channel_name="repeat")
    tracking.record_accepted_ship(9210, lot("r1"))
    tracking.record_tracking(9210, "1ZOLDOLDOLDOLDOLD1")
    rows = tracking.state.load("owed", list)
    rows[0]["date"] = "2026-10-01"                              # lot 1 shipped weeks ago
    tracking.state.save("owed", rows)
    check(tracking.mark_paid(9210) and owed_row(9210).get("paid_tracking") == "1ZOLDOLDOLDOLDOLD1",
          "paid: the tracking number on file is remembered as the paid lot's")
    tracking.record_quote(9210, lot("r2"), channel_name="repeat")
    tracking.record_accepted_ship(9210, lot("r2"))
    o = owed_row(9210)
    check(o["paid"] is False and o["payout"] == 90.0 and o["cert_payout"] == {"r2": 90.0},
          "second lot: only the new card is owed (unchanged)")
    check(o["date"] != "2026-10-01" and o["tracking"] is None,
          "second lot: fresh ship date and NO tracking yet (it used to show the paid lot's date + number)")
    dig = payout_digest.build_digest()
    check("no tracking yet" in dig and "1ZOLD" not in dig and "2026-10-01" not in dig,
          "second lot: the digest line no longer shows last time's tracking/date")
    tracking.record_accepted_ship(9210, lot("r2"))                  # idempotent re-ship
    check(owed_row(9210)["tracking"] is None and owed_row(9210)["date"] == o["date"],
          "second lot: a re-ship keeps the new date and still no stale number")
    tracking.record_tracking(9210, "1ZNEWNEWNEWNEWNEW2")
    tracking.record_accepted_ship(9210, lot("r2"))
    check(owed_row(9210)["tracking"] == "1ZNEWNEWNEWNEWNEW2", "second lot: its own tracking number sticks")
    # the SAME number posted again for the new, unpaid lot (two batches in one box) is the
    # new lot's number and must stick through later re-projections
    tracking.mark_paid(9210)
    tracking.record_quote(9210, lot("r3"), channel_name="repeat")
    tracking.record_accepted_ship(9210, lot("r3"))
    check(owed_row(9210)["tracking"] is None, "third lot: starts with no tracking (the paid lot's number is not inherited)")
    tracking.record_tracking(9210, "1ZNEWNEWNEWNEWNEW2")            # same number as the lot just paid
    tracking.record_accepted_ship(9210, lot("r3"))
    check(owed_row(9210)["tracking"] == "1ZNEWNEWNEWNEWNEW2",
          "third lot: the same number re-posted for the unpaid lot is kept")
    # a number dropped BEFORE the first ship still lands on the owed row (not a reopen)
    tracking.record_quote(9211, lot("p1"), channel_name="early")
    tracking.record_tracking(9211, "1ZEARLYEARLYEARLY3")
    tracking.record_accepted_ship(9211, lot("p1"))
    check(owed_row(9211)["tracking"] == "1ZEARLYEARLYEARLY3", "first lot: a number dropped before 'ship' is kept")

    # The NEXT lot's tracking arrives before Kevin pays the first lot (the customer mailed it
    # without typing ship — the case the new "NOT booked as owed" DM steers Kevin into): that
    # number is the new lot's, and must show once the lot is booked.
    tracking.record_quote(9212, lot("n1"), channel_name="nextlot")
    tracking.record_accepted_ship(9212, lot("n1"))
    tracking.record_tracking(9212, "1ZFIRSTBOXFIRSTBOX")
    tracking.record_quote(9212, lot("n2"), channel_name="nextlot")           # lot 2: quoted, not booked
    tracking.record_tracking(9212, "1ZSECONDBOXSECOND")                       # its package is moving
    check(tracking.mark_paid(9212) and "paid_tracking" in owed_row(9212)
          and owed_row(9212)["paid_tracking"] is None,
          "paid with the next lot's number on file: that number is NOT filed under the paid lot")
    tracking.record_accepted_ship(9212, lot("n2"))                            # Kevin books lot 2
    o2 = owed_row(9212)
    check(o2["paid"] is False and o2["payout"] == 90.0 and o2["tracking"] == "1ZSECONDBOXSECOND"
          and "1ZSECONDBOXSECOND" in payout_digest.build_digest(),
          "…so the booked second lot shows ITS tracking number (it showed 'no tracking yet')")
    # but the FIRST number on a ticket is the booked lot's, even if other cards are quoted
    tracking.record_quote(9213, lot("m1"), channel_name="firstnum")
    tracking.record_accepted_ship(9213, lot("m1"))
    tracking.record_quote(9213, lot("m2"), channel_name="firstnum")
    tracking.record_tracking(9213, "1ZONLYBOXONLYBOX11")
    tracking.mark_paid(9213)
    tracking.record_accepted_ship(9213, lot("m2"))
    check(owed_row(9213)["paid_tracking"] == "1ZONLYBOXONLYBOX11" and owed_row(9213)["tracking"] is None,
          "the first number on a ticket is the booked lot's: a lot booked after the payment doesn't inherit it")
    # a second number with nothing waiting to be booked is the same lot's second box
    tracking.record_quote(9214, lot("b1"), channel_name="twoboxes")
    tracking.record_accepted_ship(9214, lot("b1"))
    tracking.record_tracking(9214, "1ZBOXONEBOXONE1111")
    tracking.record_tracking(9214, "1ZBOXTWOBOXTWO2222")
    tracking.mark_paid(9214)
    check(owed_row(9214)["paid_tracking"] == "1ZBOXTWOBOXTWO2222",
          "a second number with no unbooked cards belongs to the lot being paid")
    # a number posted for the next lot AFTER the payment (row still paid) is kept on booking
    tracking.record_quote(9215, lot("a1"), channel_name="afterpaid")
    tracking.record_accepted_ship(9215, lot("a1"))
    tracking.record_tracking(9215, "1ZPAIDLOTPAIDLOT11")
    tracking.mark_paid(9215)
    tracking.record_quote(9215, lot("a2"), channel_name="afterpaid")
    tracking.record_tracking(9215, "1ZNEXTLOTNEXTLOT22")
    tracking.record_accepted_ship(9215, lot("a2"))
    check(owed_row(9215)["tracking"] == "1ZNEXTLOTNEXTLOT22", "a number posted after the payment is the new lot's")

    # Rows PAID BEFORE this change have no paid_tracking: the number they hold is the paid
    # lot's, so a new lot must not show it (it used to get today's date + last month's number).
    legacy_row = lambda cid, user: {"id": f"owed_{cid}", "channel_id": cid, "user": user, "date": "2026-10-01",
                                    "status": "accepted", "paid": True, "paid_date": "2026-10-09",
                                    "certs": {}, "cert_payout": {}, "payout": 0, "comp": 0, "cards": 0,
                                    "tracking": "1ZOLDOLDOLDOLDOLD1"}
    rows = tracking.state.load("owed", list)
    rows += [legacy_row(9216, "legacy"), legacy_row(9217, "legacy2")]
    tracking.state.save("owed", rows)
    tracking.record_quote(9216, lot("g2"), channel_name="legacy")
    tracking.record_accepted_ship(9216, lot("g2"))
    o6 = owed_row(9216)
    check(o6["payout"] == 90.0 and o6["tracking"] is None and o6["date"] != "2026-10-01"
          and o6.get("paid_tracking") == "1ZOLDOLDOLDOLDOLD1",
          "row paid before this change: a new lot gets a fresh date and NOT the old lot's number")
    tracking.record_accepted_ship(9216, lot("g2"))
    check(owed_row(9216)["tracking"] is None, "…and the old number doesn't come back on the next projection")
    tracking.record_quote(9217, lot("h2"), channel_name="legacy2")
    tracking.record_tracking(9217, "1ZNEWBOXNEWBOX2222")                      # posted while the row is still paid
    tracking.record_accepted_ship(9217, lot("h2"))
    check(owed_row(9217)["tracking"] == "1ZNEWBOXNEWBOX2222" and owed_row(9217)["paid_tracking"] == "1ZOLDOLDOLDOLDOLD1",
          "row paid before this change: a NEW number posted before booking is kept as the new lot's")

    # an UNPAID balance that grows keeps its original ship date (only a re-OPENED row gets a new one)
    tracking.record_quote(9218, lot("d1"), channel_name="grow")
    tracking.record_accepted_ship(9218, lot("d1"))
    rows = tracking.state.load("owed", list)
    next(r for r in rows if r["id"] == "owed_9218")["date"] = "2026-10-20"
    tracking.state.save("owed", rows)
    tracking.record_quote(9218, lot("d2"), channel_name="grow")           # a second message: one more card
    res = tracking.record_accepted_ship(9218, lot("d2"))                  # snapshot = that message only
    check(owed_row(9218)["date"] == "2026-10-20" and owed_row(9218)["payout"] == 180.0,
          "an unpaid balance that grows keeps its first ship date")
    check(res and (res.get("payout"), res.get("cards")) == (90, 1)
          and res.get("owed") == 180.0 and res.get("owed_cards") == 2,
          "ship: the reported booked balance is the ticket's whole unpaid balance, not the last message's total")
    tracking.mark_paid(9218)
    one_more = {"username": "repeat", "comp": 100, "payout": 90, "cards": 1,
                "certs": {"d3": 100}, "cert_offers": {"d3": 90}}
    tracking.record_quote(9218, one_more, channel_name="grow")
    res = tracking.record_accepted_ship(9218, one_more)
    check(res and res.get("owed") == 90.0 and res.get("owed_cards") == 1,
          "ship after a payment: the reported balance is only the new lot, not the paid one")

    # Legacy rows (shipped before the per-card ledger existed) are carried as they are:
    # a PAID one stays paid when its card is re-listed and "shipped" again (re-opening it
    # would put an already-paid card back in the payout queue), and a row with a card list
    # but no amount is still carried (its cards are known, so they are not booked twice).
    rows = tracking.state.load("owed", list)
    rows += [{"id": "owed_9219", "channel_id": 9219, "user": "oldpaid", "date": "2026-09-01", "status": "accepted",
              "paid": True, "paid_date": "2026-09-05", "certs": {"X1": 100}, "payout": 90.0, "comp": 100.0, "cards": 1},
             {"id": "owed_9221", "channel_id": 9221, "user": "certsonly", "date": "2026-09-01", "status": "accepted",
              "paid": False, "certs": {"Y1": 100}, "payout": 0, "comp": 0, "cards": 1}]
    tracking.state.save("owed", rows)
    for cid, user, cert in ((9219, "oldpaid", "X1"), (9221, "certsonly", "Y1")):
        again = {"username": user, "comp": 100, "payout": 90, "cards": 1, "certs": {cert: 100}, "cert_offers": {cert: 90}}
        tracking.record_quote(cid, again, channel_name=user)
        tracking.record_accepted_ship(cid, again)
    tk = lambda cid: next(r for r in tracking.state.load("tickets", list) if r["id"] == f"tk_{cid}")
    check(owed_row(9219)["paid"] is True and owed_row(9219)["payout"] == 0
          and tk(9219).get("review_overlaps") == ["X1"] and tk(9219)["ledger"]["_carried"][0]["status"] == "paid",
          "legacy PAID row: re-listing + shipping its card does not re-open it (flagged for review instead)")
    check(owed_row(9221)["payout"] == 0 and tk(9221).get("review_overlaps") == ["Y1"]
          and tk(9221)["ledger"]["_carried"][0]["certs"] == ["Y1"],
          "legacy row with a card list but no amount is still carried (its card is not booked a second time)")

    # 6) Kevin-only visibility (no booking, no payout change):
    for st in ("tickets", "owed"):
        tracking.state.save(st, [])
    # (a) tracking / delivery for cards that were quoted but never booked as owed
    with _BotHarness(9220, name="neversaidship", rows=buy, sheet_url=URL % 9220) as h:
        h.say("12345678 23456789")
        check(tracking.unbooked_quoted(9220) == (2, 180.0), "unbooked_quoted: 2 quoted cards, $180")
        del h.kevin.dms[:]
        h.say("1Z999AA10123456784")                      # tracking, but nobody said ship
        check(owed_row(9220) is None, "a tracking drop still books nothing (unchanged)")
        check(any("2 quoted card(s) ($180.00)" in d and "NOT booked" in d for d in h.kevin.dms),
              "tracking for an unbooked lot: Kevin is told it is not in the payout queue")
        del h.kevin.dms[:]
        asyncio.run(botmod.notify_delivered(9220, "neversaidship"))
        check(any("Delivered" in d and "NOT booked" in d for d in h.kevin.dms),
              "delivered for an unbooked lot: Kevin's delivery DM says so too")
        h.say("ship")
        del h.kevin.dms[:]
        check(tracking.unbooked_quoted(9220) == (0, 0.0), "after 'ship' nothing is left unbooked")
        asyncio.run(botmod.notify_delivered(9220, "neversaidship"))
        check(h.kevin.dms and "NOT booked" not in h.kevin.dms[-1], "a booked lot gets the plain delivery DM")
    # (b) a re-quote that retracts an earlier BUY tells Kevin which cards were removed
    flip = {"pass": False}

    def rq(cert_list):
        return all_pass(cert_list) if flip["pass"] else buy(cert_list)
    with _BotHarness(9221, name="retract", rows=rq, sheet_url=URL % 9221) as h:
        h.say("12345678 23456789")
        flip["pass"] = True
        del h.kevin.dms[:]
        h.say("12345678")
        check(any("REMOVED 1 previously quoted card(s)" in d and "12345678" in d for d in h.kevin.dms),
              "re-quote PASS of a quoted card: Kevin is told it was removed from the lot")
        check(tracking.unbooked_quoted(9221) == (1, 90.0), "…and only the other card is still quoted")
    check(tracking.record_quote(9222, {}) == [], "record_quote with no snapshot returns an empty list")
    # (c) `!owe`: the same card owed in two of one seller's tickets is flagged even at different totals
    tracking.state.save("owed", [
        {"id": "owed_9230", "channel_id": 9230, "user": "twotix", "paid": False, "payout": 90.0, "cards": 1,
         "date": "2026-11-06", "cert_payout": {"77777777": 90.0}},
        {"id": "owed_9231", "channel_id": 9231, "user": "TwoTix", "paid": False, "payout": 200.0, "cards": 2,
         "date": "2026-11-07", "cert_payout": {"77777777": 90.0, "88888888": 110.0}},
        {"id": "owed_9232", "channel_id": 9232, "user": "someoneelse", "paid": False, "payout": 90.0, "cards": 1,
         "date": "2026-11-07", "cert_payout": {"77777777": 90.0}}])
    packets = tracking.build_owe_packets()
    check(packets.count("SAME CARD in another unpaid entry") == 2 and "77777777" in packets,
          "!owe: the same cert in two unpaid entries of one seller is flagged on both (different totals)")
    check("SAME CARD" not in tracking.build_payment_packet(
        next(o for o in tracking.state.load("owed", list) if o["id"] == "owed_9232")),
          "!owe: another seller's entry is not flagged")

    for st in ("tickets", "owed", "channel_sheet", "channel_sheets", "voided", "void_cutoff"):
        tracking.state.save(st, {} if st in ("channel_sheet", "channel_sheets", "void_cutoff") else [])
    alerts.reset()


# ── guards that had no test at all: rebuild cutoff, /delivery, the real portal client ──
def test_untested_guards():
    section("previously untested guards")
    import json as _j
    import types
    from datetime import date, timedelta
    import botv2.config as cfg
    import botv2.analytics as analytics
    from botv2 import alerts, bot as botmod, sheets as sheets_mod, webserver
    from botv2.portal_client import PortalAuthError, PortalBadRequest, PortalUnavailable
    for st in ("tickets", "owed", "voided"):
        tracking.state.save(st, [])
    for st in ("channel_sheet", "channel_sheets", "void_cutoff"):
        tracking.state.save(st, {})

    # ── 1) Fix B (f2a2c60): after a void, the ship-time Drive rebuild must use ONLY sheets
    #       created after the cancellation — a cancelled sheet whose trash failed can't
    #       resurrect the lot. Nothing asserted that the filter is honoured.
    hdr = ["", "cert", "", "", "", "cat", "comp", "rate", "payout"]
    sheets_data = {
        "OLD": [hdr, ["PSA", "11111111", "", "", "PSA 10", "basketball", 300, 0.9, 270]],
        "NEW": [hdr, ["PSA", "22222222", "", "", "PSA 10", "pokemon", 100, 0.9, 90],
                ["CGC", "6193949247", "", "", "CGC 9.5", "", 80, 0.9, 72],
                ["PSA", "33333333", "", "", "PSA 9", "baseball", 50, 0, 0]],      # zeroed by hand → not in the lot
    }
    drive_files = [{"id": "OLD", "name": "cutoffuser discord 11/01", "createdTime": "2020-01-01T00:00:00.000Z"},
                   {"id": "NEW", "name": "cutoffuser discord 11/10", "createdTime": "2099-01-01T00:00:00.000Z"},
                   {"id": "OTHER", "name": "cutoffuser2 discord 11/10", "createdTime": "2099-01-01T00:00:00.000Z"}]
    queries = []

    class _Files:
        def list(self, **kw):
            queries.append(kw.get("q", ""))
            return self

        def execute(self):
            return {"files": list(drive_files)}
    fake_drive = types.SimpleNamespace(files=lambda: _Files())
    fake_gc = types.SimpleNamespace(open_by_key=lambda fid: types.SimpleNamespace(
        get_worksheet=lambda i: types.SimpleNamespace(
            get_all_values=lambda value_render_option=None: sheets_data[fid])))
    saved_g = (sheets_mod.get_drive_service, sheets_mod.get_gspread_client)
    sheets_mod.get_drive_service, sheets_mod.get_gspread_client = (lambda: fake_drive), (lambda: fake_gc)
    try:
        # no void on record → both of this customer's sheets count (and not the look-alike name)
        check(lots.rebuild_from_sheets(9400, "cutoffuser") is True
              and lots.value_by_lane(9400) == (180.0, 300.0),
              "rebuild: rebuilds from the customer's own sheets only (exact name), skipping zeroed rows")
        check("trashed=false" in queries[-1] and "cutoffuser discord" in queries[-1],
              "rebuild: asks Drive for un-trashed sheets of that customer")
        lots.drop(9400)
        # quote + VOID → cutoff recorded → only the sheet created AFTER it may be used
        tracking.record_quote(9400, {"username": "cutoffuser", "comp": 300, "payout": 270, "cards": 1,
                                     "certs": {"11111111": 300}, "cert_offers": {"11111111": 270}},
                              channel_name="cutoffuser")
        tracking.void_ticket(9400)
        check(bool(tracking.void_cutoff_of(9400)), "rebuild: the void recorded a cutoff")
        check(lots.rebuild_from_sheets(9400, "cutoffuser") is True and lots.value_by_lane(9400) == (180.0, 0.0),
              "rebuild after a void: the pre-cancellation sheet is IGNORED (its basketball card does not come back)")
        lots.drop(9400)
        # and the ship gate itself: a voided channel gets no address even though sheets exist —
        # and the Drive rebuild is not even attempted for it.
        with _BotHarness(9400, name="cutoffuser") as h:
            rebuilds = []
            real_rebuild = lots.rebuild_from_sheets

            def _spy_rebuild(*a, **k):
                rebuilds.append(a)
                return real_rebuild(*a, **k)
            lots.rebuild_from_sheets = _spy_rebuild
            try:
                h.say("ship")
                check(not h.channel.sent and not lots.has_lot(9400) and not rebuilds
                      and any("VOIDED channel" in d for d in h.kevin.dms),
                      "ship gate: a voided channel is refused BEFORE any rebuild (no Drive call at all)")
                # a stale in-memory lot + last quote must not get a voided channel shipped either
                lots.add_slab_values(9400, {"stale": 300.0}, {"stale": "basketball"})
                botmod._last_quote[9400] = {"username": "cutoffuser", "comp": 300, "payout": 270, "cards": 1,
                                            "certs": {"stale": 300}, "cert_offers": {"stale": 270}}
                del h.kevin.dms[:]
                h.say("ship")
                check(not h.channel.sent and not rebuilds
                      and not any(r.get("id") == "owed_9400" for r in tracking.state.load("owed", list))
                      and any("VOIDED channel" in d for d in h.kevin.dms),
                      "ship gate: even with a stale in-memory lot + quote, a voided channel gets no address and no booking")
            finally:
                lots.rebuild_from_sheets = real_rebuild
    finally:
        sheets_mod.get_drive_service, sheets_mod.get_gspread_client = saved_g
        lots.drop(9400)

    # ── 2) POST /delivery: the handler body and the ONE gate on the bot messaging a customer
    class _Req:
        def __init__(self, body, key="selftestkey"):
            self.headers, self.query, self._body = ({"x-bot-key": key} if key else {}), {}, body

        async def json(self):
            if isinstance(self._body, Exception):
                raise self._body
            return self._body
    loop = asyncio.new_event_loop()
    post = lambda h, body, key="selftestkey": loop.run_until_complete(h(_Req(body, key)))
    body_of = lambda resp: _j.loads(resp.body.decode())
    saved = (cfg.BOT_API_KEY, cfg.DELIVERY_TRACKING)
    cfg.BOT_API_KEY = "selftestkey"
    try:
        with _BotHarness(9410, name="dlvcust") as h:
            botmod.bot.get_channel = lambda _cid: h.channel
            lot = {"username": "dlvcust", "comp": 100, "payout": 90, "cards": 1,
                   "certs": {"d1": 100}, "cert_offers": {"d1": 90}}
            tracking.record_quote(9410, lot, channel_name="dlvcust")
            tracking.record_accepted_ship(9410, lot)
            tracking.record_tracking(9410, "1Z999AA10123456784")
            cfg.DELIVERY_TRACKING = False
            # a status-less / unknown event must never be treated as "delivered"
            for bad in ({"channel_id": "9410"}, {"channel_id": "9410", "status": ""},
                        {"channel_id": "9410", "status": "in_transit"}):
                r = post(webserver._delivery, bad)
                check(r.status == 200 and "ignored" in body_of(r)
                      and tracking.delivery_status(9410).get("status") == "in_transit",
                      f"/delivery {bad.get('status')!r}: acked + ignored, ticket still in transit")
            check(not h.channel.sent and not h.kevin.dms, "/delivery ignored events: nobody is messaged")
            check(post(webserver._delivery, {}).status == 400 and post(webserver._delivery, []).status == 400
                  and post(webserver._delivery, ValueError("bad json")).status == 400,
                  "/delivery: no target / non-object / bad JSON → 400")
            # flag OFF: an out-for-delivery event is recorded but posts NOTHING to the customer
            r = body_of(post(webserver._delivery, {"channel_id": "9410", "status": "out_for_delivery"}))
            check(r.get("found") is True and r.get("already") is False and r.get("posted") is False
                  and tracking.delivery_status(9410).get("status") == "out_for_delivery"
                  and not h.channel.sent and not h.kevin.dms,
                  "/delivery out-for-delivery (customer messaging OFF): recorded, nobody messaged")
            # flag OFF: delivered flips the ticket + DMs Kevin, and posts NOTHING to the customer
            r = body_of(post(webserver._delivery, {"channel_id": "9410", "status": "Delivered"}))
            check(r.get("found") is True and r.get("posted") is False and r.get("messaging_enabled") is False
                  and r.get("channel_id") == "9410"
                  and tracking.delivery_status(9410).get("status") == "delivered",
                  "/delivery delivered (customer messaging OFF): ticket flipped, posted:false")
            check(not h.channel.sent and len(h.kevin.dms) == 1 and "Delivered" in h.kevin.dms[0],
                  "/delivery flag OFF: the customer gets NOTHING; Kevin gets his DM")
            r = body_of(post(webserver._delivery, {"tracking": "1z999aa10123456784", "status": "delivered"}))
            check(r.get("already") is True and len(h.kevin.dms) == 1 and not h.channel.sent,
                  "/delivery replay: already delivered → no second DM, no post")
            r = body_of(post(webserver._delivery, {"channel_id": "9410", "status": "out_for_delivery"}))
            check(r.get("already") is True and not h.channel.sent, "/delivery OFD after delivered: no post")
            # flag ON: exactly one customer-safe post per stage
            cfg.DELIVERY_TRACKING = True
            tracking.record_quote(9411, {**lot, "username": "dlvcust2"}, channel_name="dlvcust2")
            tracking.record_tracking(9411, "1Z999AA10123456785")
            del h.kevin.dms[:]
            r = body_of(post(webserver._delivery, {"tracking": "1Z999AA10123456785", "status": "out-for-delivery"}))
            check(r.get("posted") is True and len(h.channel.sent) == 1 and "out for delivery" in h.channel.sent[0]
                  and not h.kevin.dms, "/delivery OFD (flag ON): one customer heads-up, no Kevin DM")
            r = body_of(post(webserver._delivery, {"tracking": "1Z999AA10123456785", "status": "delivered"}))
            check(r.get("posted") is True and len(h.channel.sent) == 2 and "arrived" in h.channel.sent[1],
                  "/delivery delivered (flag ON): exactly one 'arrived' post")
            check(not any(w in " ".join(h.channel.sent).lower() for w in cfg.CUSTOMER_FORBIDDEN_WORDS)
                  and "$" not in " ".join(h.channel.sent),
                  "/delivery customer posts: no forbidden word, no dollar figure")
            check(any("NOT booked as owed" in d for d in h.kevin.dms),
                  "/delivery delivered for a never-booked lot: Kevin's DM flags it")
            r = body_of(post(webserver._delivery, {"tracking": "1ZNOBODYNOBODY0000", "status": "delivered"}))
            check(r.get("found") is False and len(h.channel.sent) == 2, "/delivery unknown package: clean miss, no post")
            botmod.bot.__dict__.pop("get_channel", None)

        # ── 3) POST /void input rules (strict force, exactly one target)
        tracking.record_quote(9420, {"username": "strict", "comp": 100, "payout": 90, "cards": 1,
                                     "certs": {"s1": 100}, "cert_offers": {"s1": 90}}, channel_name="strict")
        for bad in ({"user": "strict", "force": "true"}, {"user": "strict", "force": 1},
                    {"user": "strict", "channel_id": "9420"}, {}, [], {"channel_id": 9420},
                    ValueError("bad json")):
            check(post(webserver._void, bad).status == 400 and tracking.find_tickets(channel_id=9420),
                  f"/void rejects {bad!r:.60} with 400 and voids nothing")
        check(post(webserver._void, {"user": "strict"}, key="wrong").status == 401
              and tracking.find_tickets(channel_id=9420), "/void: a wrong key is refused")
        tracking.void_ticket(9420)
    finally:
        cfg.BOT_API_KEY, cfg.DELIVERY_TRACKING = saved
        loop.close()

    # ── 4) the REAL portal request layer (mock mode never reaches it): what goes on the wire,
    #       and how failures are classified. Driven with a fake session — no network.
    class _Resp:
        def __init__(self, status=200, payload=None, text=""):
            self.status, self._payload, self._text = status, payload, text

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def text(self):
            return self._text

        async def json(self, content_type=None):
            if isinstance(self._payload, Exception):
                raise self._payload
            return self._payload

    class _Session:
        def __init__(self, *responses):
            self.responses, self.calls = list(responses), []

        def post(self, url, json=None, headers=None, timeout=None):
            self.calls.append({"url": url, "json": json, "headers": headers})
            return self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
    row = lambda cid, **kw: {"certId": cid, "category": "pokemon", "marketValue": 100, "value": 100,
                             "decision": "BUY", "offer": 90, "payoutCents": 9999, "memberMultiplier": 1.2, **kw}
    run = lambda coro: asyncio.run(coro)
    saved_cfg = (cfg.BOT_API_KEY, cfg.PRICE_RETRY_BACKOFF_S)
    cfg.BOT_API_KEY, cfg.PRICE_RETRY_BACKOFF_S = "wirekey", 0
    try:
        sess = _Session(_Resp(200, {"rows": [row("12345678"), row("6193949247", vip=True)], "customerMatched": True}))
        rows = run(portal_client._price_chunk(sess, ["12345678", "cgc:6193949247"], "j.smith", {"pokemon": 95}))
        call = sess.calls[0]
        check(call["url"] == cfg.PRICE_ENDPOINT and call["headers"].get("x-bot-key") == "wirekey"
              and call["json"] == {"certs": ["12345678", "cgc:6193949247"], "customer": "j.smith",
                                   "rateOverride": {"pokemon": 95}},
              "price client: certs + customer + rateOverride + x-bot-key go on the wire as documented")
        check([r.cert for r in rows] == ["12345678", "cgc:6193949247"] and rows[1].vip and rows[0].customer_matched
              and rows[0].offer == 90.0 and not hasattr(rows[0], "payoutCents"),
              "price client: rows mapped in input order; confidential fields never reach PricedRow")
        sess = _Session(_Resp(200, {"rows": [row("12345678")]}))
        rows = run(portal_client._price_chunk(sess, ["12345678"]))
        check("customer" not in sess.calls[0]["json"] and "rateOverride" not in sess.calls[0]["json"]
              and rows[0].customer_matched is False, "price client: no customer / override → neither key is sent")
        # a short reply must not shift offers onto the wrong cert
        sess = _Session(_Resp(200, {"rows": [row("23456789", offer=55)]}))
        rows = run(portal_client._price_chunk(sess, ["12345678", "23456789"]))
        check(rows[0].error and rows[0].offer is None and rows[1].offer == 55.0 and not rows[1].error,
              "price client: a short reply is matched by cert id (the missing cert errors; nothing shifts)")
        for status, exc in ((401, PortalAuthError), (400, PortalBadRequest)):
            sess = _Session(_Resp(status, text="nope"))
            try:
                run(portal_client._price_chunk(sess, ["12345678"]))
                got = None
            except Exception as e:
                got = type(e)
            check(got is exc and len(sess.calls) == 1, f"price client: HTTP {status} → {exc.__name__}, never retried")
        sess = _Session(_Resp(503), _Resp(200, ValueError("not json")), _Resp(200, {"rows": [row("12345678")]}))
        rows = run(portal_client._price_chunk(sess, ["12345678"]))
        check(len(sess.calls) == 3 and rows[0].offer == 90.0,
              "price client: a 5xx and a non-JSON 200 are retried, then the good reply is used")
        sess = _Session(_Resp(502))
        try:
            run(portal_client._price_chunk(sess, ["12345678"]))
            got = None
        except Exception as e:
            got = type(e)
        check(got is PortalUnavailable and len(sess.calls) == cfg.PRICE_MAX_RETRIES + 1,
              "price client: persistent 5xx → PortalUnavailable after the retry budget")
        # the claim request layer itself (never reached by the bot while the flag is off — see
        # test_vip_commands_client; here only its wire shape, against a fake session)
        sess = _Session(_Resp(200, {"results": [{"certId": "12345678", "skipped": "Auto-claim is turned off."}]}))
        res = run(portal_client._claim_chunk(sess, cfg.UNCLAIM_ENDPOINT, ["12345678"], "chan-1"))
        check(sess.calls[0]["json"] == {"certs": ["12345678"], "idempotencyKey": "chan-1"}
              and res[0].skipped and not res[0].ok, "claim wire shape (fake session, UNCLAIM url): certs + idempotencyKey")
        # register-tracking: what the bot sends today (channel_id as given — see the review notes)
        check(run(portal_client.register_tracking(1492170401608896533, "")) == {"ok": False, "error": "no tracking"},
              "register_tracking: an empty number is refused locally")
    finally:
        cfg.BOT_API_KEY, cfg.PRICE_RETRY_BACKOFF_S = saved_cfg

    # ── 5) counts() and the analytics window, on seeded stores (the old checks could not fail)
    today = date.today().isoformat()
    old = (date.today() - timedelta(days=30)).isoformat()
    tracking.state.save("tickets", [
        {"id": "tk_1", "channel_id": 1, "user": "a", "status": "accepted", "cards": 2, "comp": 200.0, "payout": 180.0, "date": today},
        {"id": "tk_2", "channel_id": 2, "user": "b", "status": "accepted", "cards": 1, "comp": 100.0, "payout": 90.0, "date": today},
        {"id": "tk_3", "channel_id": 3, "user": "c", "status": "quoted", "cards": 3, "comp": 300.0, "payout": 270.0, "date": old}])
    tracking.state.save("owed", [
        {"id": "owed_1", "channel_id": 1, "user": "a", "paid": False, "payout": 180.0},
        {"id": "owed_2", "channel_id": 2, "user": "b", "paid": True, "payout": 0.0}])
    check(tracking.counts() == {"tickets_total": 3, "tickets_open": 2, "owed_open": 1, "owed_total": 180.0},
          "counts: 3 tickets, 2 open (1 unpaid + 1 never shipped), $180 owed")
    allt, wk = analytics.summary(), analytics.summary(days=7)
    check(allt["totals"]["tickets"] == 3 and allt["totals"]["payout"] == 540.0
          and allt["status"] == {"open": 1, "shipped": 2, "paid": 1}, "analytics all-time: totals + status counts")
    check(wk["window_days"] == 7 and wk["totals"]["tickets"] == 2 and wk["totals"]["payout"] == 270.0,
          "analytics ?days=7: the 30-day-old ticket is excluded")
    for st in ("tickets", "owed", "voided"):
        tracking.state.save(st, [])
    for st in ("channel_sheet", "channel_sheets", "void_cutoff"):
        tracking.state.save(st, {})
    alerts.reset()


# ── alerts that name the ticket; the lanes board on Discord errors ─────────────
def test_alerts_and_lanes():
    section("alert routing + lanes board errors")
    import types
    import discord
    import botv2.config as cfg
    from botv2 import alerts, bot as botmod, webserver
    from botv2.portal_client import PortalAuthError

    # an alert whose DM could NOT be delivered must not use up the 15-minute quiet window
    alerts.reset()
    check(asyncio.run(botmod.alert_kevin("zz_undelivered", "x")) is False
          and alerts.should_alert("zz_undelivered", now=1.0),
          "alert_kevin: an undelivered alert (no Kevin id / DM failed) can fire again next time")
    alerts.reset()

    # a scheduled digest that can't be delivered raises an alert instead of vanishing
    with _BotHarness(8964) as h:
        check(asyncio.run(botmod._send_digest("payout", "")) is True and not h.kevin.dms,
              "digest: nothing owed → nothing sent, counts as success")
        check(asyncio.run(botmod._send_digest("payout", "💰 Payout digest — 1 unpaid")) is True
              and h.kevin.dms == ["💰 Payout digest — 1 unpaid"], "digest: delivered")
        del h.kevin.dms[:]
        fails = {"n": 0}
        real_send = h.kevin.send

        async def _flaky_send(text):                    # the digest DM fails, the short alert gets through
            if text.startswith("💰"):
                fails["n"] += 1
                raise RuntimeError("discord 500")
            await real_send(text)
        h.kevin.send = _flaky_send
        ok = asyncio.run(botmod._send_digest("payout", "💰 Payout digest — 30 unpaid"))
        check(ok is False and fails["n"] == 1 and any("could NOT be delivered" in d and "!owe" in d for d in h.kevin.dms)
              and (alerts.last_error() or {}).get("key") == "digest_send:payout",
              "digest: a failed send alerts Kevin and is recorded for /status (it used to vanish silently)")
    alerts.reset()

    # the on_message catch-all names the ticket and is rate-limited PER ticket
    with _BotHarness(8960, name="crashy") as h:
        real_handle = botmod._handle_message

        async def _boom(_message):
            raise RuntimeError("kaboom")
        botmod._handle_message = _boom
        try:
            asyncio.run(botmod.on_message(_FakeMessage(h.channel, "12345678")))
            other = _fake_channel(8961, "othercrash")
            asyncio.run(botmod.on_message(_FakeMessage(other, "12345678")))
        finally:
            botmod._handle_message = real_handle
        check(len(h.kevin.dms) == 2 and "#crashy" in h.kevin.dms[0] and "#othercrash" in h.kevin.dms[1],
              "on_message error: the alert names the ticket, and a 2nd ticket's error isn't suppressed")

    # pricing auth failure: Kevin's alert names the ticket; the customer text is unchanged
    async def _unauth(cert_list, customer=None, rate_override=None, session=None):
        raise PortalAuthError("portal rejected x-bot-key (401)")
    with _BotHarness(8962, name="authfail") as h:
        real_price = portal_client.price_certs
        portal_client.price_certs = _unauth
        try:
            h.say("12345678")
        finally:
            portal_client.price_certs = real_price
        check(not h.errors and any("#authfail" in d and "re-quote" in d for d in h.kevin.dms),
              "price auth failure: Kevin's alert names the ticket that needs a re-quote")
        check(h.channel.sent == ["⚠️ Pricing is briefly unavailable — hang tight, I'll get your quote shortly."],
              "price auth failure: customer-facing text unchanged")

    # !bands typed in a guild channel: the channel ack carries no forbidden customer word
    with _BotHarness(8963, name="bandsroom") as h:
        asyncio.run(commands.handle(None, _FakeMessage(h.channel, "!bands", author=h.kevin)))
        check(h.channel.sent and not any(w in h.channel.sent[-1].lower() for w in cfg.CUSTOMER_FORBIDDEN_WORDS)
              and h.kevin.dms and "Allocation board" in h.kevin.dms[0],
              "!bands: channel ack has no forbidden word (the DM'd board keeps its real title)")
        del h.channel.sent[:]
        asyncio.run(commands.handle(None, _FakeMessage(h.channel, "!help", author=h.kevin)))
        check(h.channel.sent and not any(w in "\n".join(h.channel.sent).lower() for w in cfg.CUSTOMER_FORBIDDEN_WORDS),
              "!help (Kevin, in a channel): no forbidden customer word in the admin block")

    # /status exposes the two 'is this bot misconfigured' booleans
    loop = asyncio.new_event_loop()
    saved_key = cfg.BOT_API_KEY
    cfg.BOT_API_KEY = "selftestkey"
    try:
        import json as _j
        req = types.SimpleNamespace(headers={"x-bot-key": "selftestkey"}, query={})
        flags = _j.loads(loop.run_until_complete(webserver._status(req)).body.decode())["flags"]
        check(flags.get("price_mock") is True and flags.get("kevin_dm") is False and flags.get("auto_claim") is False,
              "status: price_mock / kevin_dm flags reported (mock is ON only because this is the selftest)")
    finally:
        cfg.BOT_API_KEY = saved_key
        loop.close()

    # ── lanes board: only a real 404 means "the message is gone" ──
    def not_found():
        return discord.NotFound(types.SimpleNamespace(status=404, reason="Not Found"), "Unknown Message")

    class _BoardMsg:
        def __init__(self, mid, log):
            self.id, self.log = mid, log

        async def edit(self, content=None):
            self.log.append(("edit", self.id))

        async def delete(self):
            self.log.append(("delete", self.id))

        async def pin(self):
            self.log.append(("pin", self.id))

    class _LanesChan:
        def __init__(self):
            self.log, self.fetch_error, self.next_id = [], None, 500

        async def fetch_message(self, mid):
            if self.fetch_error is not None:
                raise self.fetch_error
            return _BoardMsg(mid, self.log)

        async def send(self, text):
            _fake_send_check(text)
            self.next_id += 1
            self.log.append(("send", self.next_id))
            return _BoardMsg(self.next_id, self.log)

    chan = _LanesChan()
    saved = (cfg.LANES_BOARD, cfg.LANES_CHANNEL_ID)
    cfg.LANES_BOARD, cfg.LANES_CHANNEL_ID = True, 4444
    botmod.bot.get_channel = lambda _cid: chan
    st = lambda: tracking.state.load("lanes_board", dict)

    def run(coro):
        try:
            asyncio.run(coro)
            return None
        except Exception as e:
            return e
    try:
        tracking.state.save("lanes_board", {"message_id": 111, "channel_id": 4444})
        check(run(botmod._render_lanes_board()) is None and chan.log == [("edit", 111)],
              "lanes: healthy refresh edits the pinned board in place")
        # transient fetch error → NO second board; the saved id is kept; the loop alerts + retries
        del chan.log[:]
        chan.fetch_error = RuntimeError("503 Service Unavailable")
        err = run(botmod._render_lanes_board())
        check(isinstance(err, RuntimeError) and chan.log == [] and st().get("message_id") == 111,
              "lanes: a transient fetch error does NOT post a duplicate board (it used to post + pin a second one)")
        # failed take-down → the saved id is kept so it is retried (it used to be forgotten)
        err = run(botmod._remove_lanes_board())
        check(isinstance(err, RuntimeError) and st().get("message_id") == 111,
              "lanes: a failed take-down keeps the saved id so the delete is retried")
        # a real 404 → re-create on refresh / just forget on take-down
        chan.fetch_error = not_found()
        check(run(botmod._render_lanes_board()) is None and chan.log == [("send", 501), ("pin", 501)]
              and st().get("message_id") == 501, "lanes: a deleted board (404) is re-created and re-pinned")
        check(run(botmod._remove_lanes_board()) is None and st() == {},
              "lanes: take-down of an already-deleted board just clears the saved id")
        # normal take-down
        del chan.log[:]
        chan.fetch_error = None
        tracking.state.save("lanes_board", {"message_id": 222, "channel_id": 4444})
        check(run(botmod._remove_lanes_board()) is None and chan.log == [("delete", 222)] and st() == {},
              "lanes: normal take-down deletes the board and clears the id")
    finally:
        cfg.LANES_BOARD, cfg.LANES_CHANNEL_ID = saved
        botmod.bot.__dict__.pop("get_channel", None)
        tracking.state.save("lanes_board", {})
        alerts.reset()


# ── canary: one old alert must not keep every hourly run red ────────────────────
def test_canary_error_age():
    section("canary last_error age")
    from datetime import datetime, timedelta, timezone
    import botv2.canary as canary
    now = datetime(2026, 11, 10, 12, 0, 0, tzinfo=timezone.utc)
    at = lambda mins: {"key": "sheet_fail:1", "text": "x",
                       "at": (now - timedelta(minutes=mins)).isoformat(timespec="seconds")}
    check(canary.is_fresh_error(None, now) is False, "canary: no last_error → pass")
    check(canary.is_fresh_error(at(5), now, 90) is True, "canary: an error 5 min ago fails the run")
    check(canary.is_fresh_error(at(89), now, 90) is True, "canary: still failing just inside the window")
    check(canary.is_fresh_error(at(180), now, 90) is False,
          "canary: a 3-hour-old error no longer fails every hourly run (it used to, until the next redeploy)")
    check(canary.is_fresh_error({"key": "k"}, now, 90) is True
          and canary.is_fresh_error({"key": "k", "at": "garbage"}, now, 90) is True,
          "canary: an undatable last_error fails closed")
    check(canary.is_fresh_error({"key": "k", "at": "2026-11-10T11:30:00Z"}, now, 90) is True,
          "canary: 'Z' timestamps are understood")
    # the default window covers TWO hourly runs: with 90 minutes an error recorded just after
    # a run was inside the window for exactly one later run, so a late or dropped run hid it
    check(canary.ERROR_MAX_AGE_MIN == 150 and canary.is_fresh_error(at(119), now) is True
          and canary.is_fresh_error(at(151), now) is False,
          "canary: by default an error stays red for two hourly runs (150 min), then clears")


# ── scheduled DMs across a daylight-saving change ───────────────────────────────
def test_schedule_dst():
    section("schedules across DST")
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from botv2 import bot as botmod
    et = ZoneInfo("America/New_York")
    daily, friday = botmod._seconds_until_daily_et, botmod._seconds_until_friday_9am_et
    H = 3600
    # ordinary day: 08:01:30 → next 08:00 is 23h58m30s away
    check(daily(8, now=datetime(2027, 6, 1, 8, 1, 30, tzinfo=et)) == 24 * H - 90, "daily: ordinary day")
    # the night clocks spring FORWARD (2027-03-14) is 23 hours long …
    check(daily(8, now=datetime(2027, 3, 13, 8, 1, 30, tzinfo=et)) == 23 * H - 90,
          "daily: spring-forward night is 23h (was computed as 24h → fired at 9 AM)")
    # … and the night they fall BACK (2027-11-07) is 25 hours long.
    check(daily(8, now=datetime(2027, 11, 6, 8, 1, 30, tzinfo=et)) == 25 * H - 90,
          "daily: fall-back night is 25h (was computed as 24h → fired at 7 AM, then again at 8)")
    # Friday digest: the week containing the fall-back is 7 days + 1 hour long.
    check(friday(now=datetime(2027, 11, 5, 9, 1, 30, tzinfo=et)) == 7 * 24 * H + H - 90,
          "friday digest: week across fall-back is 7d+1h (was 7d → fired at 8 AM, then again at 9)")
    check(friday(now=datetime(2027, 6, 3, 9, 0, 0, tzinfo=et)) == 24 * H, "friday digest: Thursday 9:00 → 24h")
    check(friday(now=datetime(2027, 6, 4, 9, 0, 0, tzinfo=et)) == 7 * 24 * H,
          "friday digest: exactly Friday 9:00 schedules NEXT week (no double fire)")
    check(0 < daily(8) <= 25 * H and 0 < friday() <= 7 * 24 * H + H, "schedules: live clock values are sane")


# ── void CALLERS: !void (commands.handle) and POST /void (webserver._void) ──────
def test_void_callers():
    section("void callers (!void + POST /void)")
    import json as _j
    import botv2.config as cfg
    from botv2 import bot as botmod, sheets as sheets_mod, webserver
    URL = "https://docs.google.com/spreadsheets/d/SHEET%s/edit"

    class _Req:
        def __init__(self, body):
            self.headers, self.query, self._body = {"x-bot-key": "selftestkey"}, {}, body

        async def json(self):
            return self._body

    trash_calls, trash_result = [], [True]

    def _trash(url):
        trash_calls.append(url)
        return trash_result[0]

    def seed(cid, user, status="quoted", sheet=True, mem=True, owed=False):
        """A ticket the way the live bot leaves one: store rows + the in-memory quote/lot/cache."""
        rows = [r for r in tracking.state.load("tickets", list) if r.get("id") != f"tk_{cid}"]
        rows.append({"id": f"tk_{cid}", "channel_id": cid, "user": user, "channel_name": user,
                     "status": status, "cards": 2, "payout": 455.0})
        tracking.state.save("tickets", rows)
        if owed:
            tracking.state.save("owed", tracking.state.load("owed", list)
                                + [{"id": f"owed_{cid}", "channel_id": cid, "user": user,
                                    "paid": False, "payout": 455.0}])
        if sheet:
            cs = tracking.state.load("channel_sheet", dict)
            cs[str(cid)] = URL % cid
            tracking.state.save("channel_sheet", cs)
            botmod._channel_sheet[str(cid)] = URL % cid
        if mem:
            lots.add_slab_values(cid, {"m1": 300.0}, {"m1": "pokemon"})
            botmod._last_quote[cid] = {"username": user, "comp": 500, "payout": 455, "cards": 2,
                                       "certs": {"m1": 300}, "cert_offers": {"m1": 270}}

    def has_ticket(cid):
        return any(r.get("id") == f"tk_{cid}" for r in tracking.state.load("tickets", list))

    def failing(store):
        real = tracking.state.save
        tracking.state.save = lambda name, data: False if name == store else real(name, data)
        return real

    for st in ("tickets", "owed", "channel_sheet", "voided", "void_cutoff"):
        tracking.state.save(st, {} if st in ("channel_sheet", "void_cutoff") else [])
    saved_key, saved_trash = cfg.BOT_API_KEY, sheets_mod.trash_sheet
    cfg.BOT_API_KEY, sheets_mod.trash_sheet = "selftestkey", _trash
    loop = asyncio.new_event_loop()
    def post(body):
        try:
            return loop.run_until_complete(webserver._void(_Req(body)))
        except Exception as e:                            # live: aiohttp would answer HTTP 500
            from aiohttp import web as _web
            return _web.json_response({"error": f"handler raised {e!r}"}, status=500)
    body_of = lambda resp: _j.loads(resp.body.decode())
    try:
        with _BotHarness(8900) as h:
            def kevin_says(text):
                """Kevin types a command in a DM with the bot; returns what he was DMed back."""
                del h.kevin.dms[:]
                dm = _fake_channel(8901, "kevin-dm")
                dm.guild = None
                try:
                    handled = asyncio.run(commands.handle(None, _FakeMessage(dm, text, author=h.kevin)))
                except Exception as e:                    # live: on_message would catch + alert
                    return False, f"<command raised {e!r}>"
                return handled, "\n".join(h.kevin.dms)

            # 1) A void that BAILS (tickets write fails → ticket still live) must clean up
            #    NOTHING and trash NOTHING. (It used to drop the lot and trash the real sheet,
            #    then report "nothing was removed".)
            seed(990, "bailbob")
            real = failing("tickets")
            try:
                ok, dm = kevin_says("!void id:990")
            finally:
                tracking.state.save = real
            check(ok and has_ticket(990) and "STILL LIVE" in dm and "`ship` is ignored" in dm,
                  "!void bail: ticket intact and Kevin is told so (incl. that ship is now blocked)")
            check(not trash_calls, "!void bail: the buying sheet is NOT trashed for a still-live ticket")
            check(lots.has_lot(990) and 990 in botmod._last_quote and botmod._channel_sheet.get("990"),
                  "!void bail: running lot, last quote and sheet cache are left alone")
            real = failing("tickets")
            try:
                r = body_of(post({"channel_id": "990"}))
            finally:
                tracking.state.save = real
            check(r.get("voided") is False and r.get("reason") == "persist_failed" and r.get("ticket_intact") is True
                  and r.get("ship_blocked") is True
                  and not trash_calls and lots.has_lot(990) and 990 in botmod._last_quote,
                  "/void bail: persist_failed, nothing trashed, nothing dropped")
            # …and the retry (storage healthy again) completes the void and trashes exactly once.
            ok, dm = kevin_says("!void id:990")
            check(not has_ticket(990) and trash_calls == [URL % 990] and "Voided" in dm
                  and "moved to Drive trash" in dm and not lots.has_lot(990) and 990 not in botmod._last_quote,
                  "!void retry: ticket gone, sheet trashed once, memory cleared")

            # an owed-write bail: the reply must not claim the owed balance was touched
            del trash_calls[:]
            seed(991, "owedbob", status="quoted", owed=True)
            real = failing("owed")
            try:
                ok, dm = kevin_says("!void id:991")
            finally:
                tracking.state.save = real
            check("STILL LIVE" in dm and "Nothing was removed" in dm and not trash_calls and has_ticket(991)
                  and any(r["id"] == "owed_991" for r in tracking.state.load("owed", list)),
                  "!void bail on the owed write: nothing removed, said plainly")
            real = failing("tickets")            # owed goes, then the ticket write fails
            try:
                ok, dm = kevin_says("!void id:991")
            finally:
                tracking.state.save = real
            check("STILL LIVE" in dm and "owed balance WAS already removed" in dm and not trash_calls,
                  "!void bail after the owed row went: Kevin is told the owed balance is already gone")
            kevin_says("!void id:991")
            del trash_calls[:]

            # 2) A blank username must never resolve to "every open quote".
            seed(992, "onlyopen", sheet=False, mem=False)
            ok, dm = kevin_says("!void @")
            check(ok and has_ticket(992) and "isn't a username" in dm,
                  "!void @ : rejected (used to void the only open quote)")
            ok, dm = kevin_says("!void @ onlyopen")
            check(has_ticket(992) and "isn't a username" in dm, "!void '@ name' (stray space): rejected, nothing voided")
            for blank in (" ", "@", "  @  "):
                resp = post({"user": blank})
                check(resp.status == 400 and has_ticket(992), f"/void user={blank!r}: 400, nothing voided")
            check(tracking.find_tickets(user=" ", status="quoted") == [] and tracking.find_tickets(user="@") == []
                  and len(tracking.find_tickets(status="quoted")) == 1,
                  "find_tickets: a blank username matches nothing (no user filter still lists all)")

            # 3) Channel ids: garbage is refused cleanly instead of raising.
            for bad in ("--5", "²", "12 34", "id:", ""):
                check(tracking.parse_channel_id(bad) is None, f"parse_channel_id rejects {bad!r}")
            check(tracking.parse_channel_id(" 1492170401608896533 ") == 1492170401608896533
                  and tracking.parse_channel_id("-5") == -5, "parse_channel_id accepts a real snowflake")
            ok, dm = kevin_says("!void id:--5")
            check(ok and "isn't a valid channel id" in dm, "!void id:--5 : clean error (used to raise ValueError)")
            ok, dm = kevin_says("!void --5")
            check(ok and "isn't a valid channel id" in dm, "!void --5 : clean error")
            for bad in ("--5", "²", 123):
                check(post({"channel_id": bad}).status == 400, f"/void channel_id={bad!r}: 400 (not a 500)")
            check(has_ticket(992), "bad ids voided nothing")

            # 4) /void when the sheet trash FAILS: the link must not be lost.
            trash_result[0] = False
            seed(993, "trashfail")
            del h.kevin.dms[:]
            r = body_of(post({"channel_id": "993"}))
            check(r.get("voided") is True and r.get("sheet_trashed") is False and r.get("had_sheet") is True
                  and r.get("sheet_url") == URL % 993,
                  "/void trash failure: response says a sheet exists, wasn't trashed, and carries its link")
            from botv2 import alerts as _alerts
            check(any((URL % 993) in d and "by hand" in d for d in h.kevin.dms) and _alerts.last_error() is None,
                  "/void trash failure: Kevin is DMed the link (it was silently dropped before); /status stays clean")
            trash_result[0] = True
            seed(994, "trashok")
            r = body_of(post({"channel_id": "994"}))
            check(r.get("voided") is True and r.get("sheet_trashed") is True and r.get("had_sheet") is True
                  and "sheet_url" not in r, "/void trash ok: had_sheet + sheet_trashed, no link needed")
            seed(995, "nosheet", sheet=False)
            r = body_of(post({"channel_id": "995"}))
            check(r.get("voided") is True and r.get("had_sheet") is False and r.get("sheet_trashed") is False,
                  "/void with no sheet: had_sheet False tells it apart from a failed trash")

            # 4b) /void of a ticket with an EARLIER sheet on record: reported, not trashed.
            del trash_calls[:]
            seed(1005, "multi")
            tracking.remember_sheet(1005, URL % "1005old")
            tracking.remember_sheet(1005, URL % 1005)
            del h.kevin.dms[:]
            r = body_of(post({"channel_id": "1005"}))
            check(r.get("voided") is True and r.get("sheet_trashed") is True
                  and r.get("other_sheet_urls") == [URL % "1005old"] and trash_calls == [URL % 1005],
                  "/void: earlier sheets are listed in other_sheet_urls; only the latest is trashed")
            check(any((URL % "1005old") in d and "OTHER buying sheet(s) are on record" in d for d in h.kevin.dms),
                  "/void: Kevin is DMed the earlier sheet's link")
            check(body_of(post({"channel_id": "994"})).get("other_sheet_urls") == [],
                  "/void: other_sheet_urls is an empty list when there are none")

            # 4c) Response shapes the contract documents. A `user` lookup that finds no open
            #     quote answers with the SAME keys as a channel_id not_found (a strict decoder
            #     written from the table choked on the bare {voided, reason}).
            NOT_FOUND_KEYS = {"voided", "reason", "residual_cleaned", "had_sheet", "sheet_trashed", "other_sheet_urls"}
            r = body_of(post({"user": "nobody-at-all"}))
            check(set(r) == NOT_FOUND_KEYS and r["reason"] == "not_found" and r["residual_cleaned"] is False
                  and r["other_sheet_urls"] == [], "/void user with no ticket: full not_found shape")
            seed(1006, "onlyshipped", status="accepted", sheet=False, mem=False)
            r = body_of(post({"user": "onlyshipped", "force": True}))
            check(set(r) == NOT_FOUND_KEYS and has_ticket(1006),
                  "/void user whose only ticket has shipped: same not_found shape, nothing voided (even with force)")
            check(set(body_of(post({"channel_id": "7999"}))) == NOT_FOUND_KEYS,
                  "/void channel_id with nothing there: the same keys")
            tracking.void_ticket(1006)
            # the latest sheet is trashed AND a different sheet lives only in memory: sheet_url is
            # the one still to do by hand, and it is not repeated in other_sheet_urls
            del trash_calls[:]
            seed(1007, "twoknown")
            tracking.remember_sheet(1007, URL % 1007)
            tracking.remember_sheet(1007, URL % "1007mem")
            botmod._channel_sheet["1007"] = URL % "1007mem"          # memory moved on; the file didn't
            r = body_of(post({"channel_id": "1007"}))
            check(r.get("voided") is True and trash_calls == [URL % 1007] and r.get("sheet_trashed") is True
                  and r.get("sheet_url") == URL % "1007mem" and r.get("other_sheet_urls") == [],
                  "/void: a memory-only sheet is given once (sheet_url), not again in other_sheet_urls")

            # 4d) The cache write fails AFTER the ticket is gone (ok=False, ticket_removed): the
            #     quote is dead, so both callers must still purge memory and trash at once.
            for caller in ("cmd", "http"):
                del trash_calls[:]
                seed(1008, "cachefail")
                real = failing("channel_sheet")
                try:
                    if caller == "cmd":
                        ok, dm = kevin_says("!void id:1008")
                        told = "Voided" in dm and "Minor: the sheet-cache entry didn't clear" in dm
                    else:
                        r = body_of(post({"channel_id": "1008"}))
                        told = r.get("voided") is True and r.get("cache_cleanup_pending") is True
                finally:
                    tracking.state.save = real
                check(told and not has_ticket(1008) and trash_calls == [URL % 1008]
                      and not lots.has_lot(1008) and 1008 not in botmod._last_quote
                      and "1008" not in botmod._channel_sheet,
                      f"void with a failed cache write ({caller}): quote dead, memory purged, sheet trashed once, said so")
                # the retry finds the leftover cache entry and finishes
                if caller == "cmd":
                    ok, dm = kevin_says("!void id:1008")
                    check("cleared leftover state" in dm and "1008" not in tracking.state.load("channel_sheet", dict),
                          "…and the suggested re-run tidies the leftover cache entry")
                else:
                    r = body_of(post({"channel_id": "1008"}))
                    check(r.get("reason") == "not_found" and r.get("residual_cleaned") is True
                          and "1008" not in tracking.state.load("channel_sheet", dict),
                          "…and the retry by channel_id answers residual_cleaned:true")
            # a leftover purge whose own write fails says so (and is safe to repeat)
            del trash_calls[:]
            tracking.state.save("channel_sheet", {"1009": URL % 1009})
            real = failing("channel_sheet")
            try:
                ok, dm = kevin_says("!void id:1009")
                r = body_of(post({"channel_id": "1009"}))
            finally:
                tracking.state.save = real
            check("A storage write failed during that cleanup" in dm and "fully clean" not in dm,
                  "!void leftover purge with a failed write: Kevin is told something is still on disk")
            check(r.get("reason") == "persist_failed" and r.get("ticket_intact") is False,
                  "/void leftover purge with a failed write: persist_failed, ticket_intact:false")
            kevin_says("!void id:1009")
            # a bail at the very first write (the cancel guard itself): ship is NOT blocked
            seed(1010, "guardfail", sheet=False, mem=False)
            real = failing("voided")
            try:
                ok, dm = kevin_says("!void id:1010")
                r = body_of(post({"channel_id": "1010"}))
            finally:
                tracking.state.save = real
            check("STILL LIVE" in dm and "`ship` is ignored" not in dm and has_ticket(1010)
                  and r.get("reason") == "persist_failed" and r.get("ship_blocked") is False,
                  "void bail at the cancel-guard write: ticket live and ship NOT reported as blocked")
            kevin_says("!void id:1010")
            del trash_calls[:]

            # 5) Residual cleanup (no ticket, leftover sheet cache): the reply is truthful.
            del trash_calls[:]
            trash_result[0] = False
            tracking.state.save("channel_sheet", {"996": URL % 996})
            ok, dm = kevin_says("!void id:996")
            check((URL % 996) in dm and "by hand" in dm and "fully clean" not in dm,
                  "!void residual + failed trash: gives the link, does NOT claim 'fully clean'")
            trash_result[0] = True
            tracking.state.save("channel_sheet", {"997": URL % 997})
            ok, dm = kevin_says("!void id:997")
            check("fully clean" in dm and trash_calls[-1] == URL % 997, "!void residual + trash ok: fully clean")
            tracking.state.save("channel_sheet", {"998": URL % 998})
            r = body_of(post({"channel_id": "998"}))
            check(r.get("reason") == "not_found" and r.get("residual_cleaned") is True and r.get("sheet_trashed") is True,
                  "/void residual: not_found + residual_cleaned (no longer looks like 'nothing happened')")
            r = body_of(post({"channel_id": "998"}))
            check(r.get("reason") == "not_found" and r.get("residual_cleaned") is False,
                  "/void with truly nothing left: residual_cleaned False")

            # 6) A quote that lives only in memory (its ticket row never persisted) must die too.
            lots.add_slab_values(999, {"q1": 100.0}, {"q1": "pokemon"})
            botmod._last_quote[999] = {"username": "memonly", "comp": 100, "payout": 90, "cards": 1,
                                       "certs": {"q1": 100}, "cert_offers": {"q1": 90}}
            ok, dm = kevin_says("!void id:999")
            check(999 not in botmod._last_quote and not lots.has_lot(999) and "in-memory quote" in dm,
                  "!void on a memory-only quote: the unsaved quote + lot are dropped (used to survive and stay shippable)")
            ok, dm = kevin_says("!void id:999")
            check("Nothing to void" in dm, "!void again: nothing left")

            # a sheet the bot only remembers in memory (its cache-file write failed) is NOT
            # auto-trashed (the trash acts on the persisted pointer only) — but its link is handed over.
            del trash_calls[:]
            seed(1000, "memsheet", sheet=False)
            botmod._channel_sheet["1000"] = URL % 1000
            ok, dm = kevin_says("!void id:1000")
            check(not trash_calls and (URL % 1000) in dm and "by hand" in dm and "1000" not in botmod._channel_sheet,
                  "!void: a sheet known only to the in-memory cache is reported for hand-trashing (link not lost)")
            seed(1003, "memsheet2", sheet=False)
            botmod._channel_sheet["1003"] = URL % 1003
            del h.kevin.dms[:]
            r = body_of(post({"channel_id": "1003"}))
            check(not trash_calls and r.get("voided") is True and r.get("sheet_url") == URL % 1003
                  and r.get("sheet_trashed") is False and any((URL % 1003) in d for d in h.kevin.dms),
                  "/void: same — memory-only sheet is returned + DMed, never auto-trashed")

            # typed in a guild channel: the command (it names a seller) is deleted BEFORE the slow
            # Drive trash, and nothing is posted in that channel.
            order = []
            seed(1004, "guildvoid")
            slow_trash = sheets_mod.trash_sheet

            def _ordered_trash(url):
                order.append("trash")
                return True
            sheets_mod.trash_sheet = _ordered_trash

            class _OrderedMsg(_FakeMessage):
                async def delete(self):
                    order.append("delete")
                    self.deleted = True
            gm = _OrderedMsg(h.channel, "!void id:1004", author=h.kevin)
            del h.channel.sent[:]
            try:
                asyncio.run(commands.handle(None, gm))
            finally:
                sheets_mod.trash_sheet = slow_trash
            check(order == ["delete", "trash"] and gm.deleted and not h.channel.sent and not has_ticket(1004),
                  "!void in a guild channel: command deleted first (once), then the trash; nothing posted there")

            # the 'accepted needs force' gate is unchanged
            seed(1001, "shipped", status="accepted", owed=True)
            ok, dm = kevin_says("!void id:1001")
            check(has_ticket(1001) and "force" in dm, "!void on a shipped ticket still needs force")
            check("$455.00 still owed (unpaid)" in dm, "!void gate: says what is actually still owed")
            tracking.mark_paid(1001)
            ok, dm = kevin_says("!void id:1001")
            check(has_ticket(1001) and "already PAID" in dm and "still owed" not in dm,
                  "!void gate: a fully paid ticket is not described as an unpaid balance")
            r = body_of(post({"channel_id": "1001"}))
            check(r.get("reason") == "needs_force" and has_ticket(1001), "/void on a shipped ticket still needs force")
            ok, dm = kevin_says("!void id:1001 force")
            check(not has_ticket(1001) and "owed balance" in dm, "!void … force voids it and reports the owed balance")
            # non-Kevin can't void
            seed(1002, "guarded", sheet=False, mem=False)
            dmch = _fake_channel(8902, "someone")
            check(asyncio.run(commands.handle(None, _FakeMessage(dmch, "!void id:1002", author=_FakeUser(uid=5)))) is True
                  and has_ticket(1002) and not dmch.sent, "!void from a non-Kevin user is swallowed, nothing voided")
    finally:
        cfg.BOT_API_KEY, sheets_mod.trash_sheet = saved_key, saved_trash
        loop.close()
        for st in ("tickets", "owed", "channel_sheet", "voided", "void_cutoff", "channel_sheets"):
            tracking.state.save(st, {} if st in ("channel_sheet", "void_cutoff", "channel_sheets") else [])


def main():
    print("botv2 selftest:")
    if _INHERITED_DATA_DIR:
        print("  (ignoring the inherited DATA_DIR — the selftest only ever writes to a temp dir)")
    for t in (test_harness_is_hermetic, test_textsplit, test_state_persistence, test_certs, test_pricing, test_lots_shipping, test_tracking_digest,
              test_payout_flow, test_vip_commands_client, test_psa_folder_guard,
              test_alerts_status, test_analytics, test_delivery, test_register_tracking,
              test_lanes_board, test_stale_tickets, test_delivery_resolution, test_delivery_ofd,
              test_webserver_auth, test_audit_fixes, test_lanes_schedule,
              test_owed_accumulation, test_void, test_void_callers, test_truthful_replies,
              test_handlers_and_void_races, test_lot_routing, test_ship_booking, test_untested_guards,
              test_alerts_and_lanes,
              test_canary_error_age,
              test_schedule_dst):
        t()
    check(not _net_attempts,
          f"no test attempted a real network connection (attempts: {_net_attempts[:5]})")
    if _fails:
        print(f"\nFAILED ({len(_fails)}):")
        for f in _fails:
            print("  ✗", f)
        raise SystemExit(1)
    print("\nALL TESTS PASS")


if __name__ == "__main__":
    main()
