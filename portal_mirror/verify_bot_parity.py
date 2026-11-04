"""
verify_bot_parity.py — does the LIVE bot's quote path (classify_psa_comp +
get_psa_payout_rate) reproduce the KTS Portal's decision/rate (portal_pricing.py,
itself verified == offer.js)?

Imports the REAL kts_collectr_bot with its heavy deps (discord/anthropic/gspread/
pandas/google) stubbed, so the actual bot functions are exercised — not a copy.
The Sat-3AM hold is monkeypatched open (we're verifying post-open behavior).

Scope: sports + One Piece at grade 8, with a recent sale and a benign name (so
the grade floor / sale gate / NBA bans don't fire — those are intentional
safe-stricter overlays, verified separately). Values from $1 (the bot's $1 floor
overlay; portal's $0.40-0.60 sub-$1 is a documented safe-stricter divergence).

Run:  python3 verify_bot_parity.py   ->  "BOT PARITY OK" or a list of mismatches.
"""
import sys, os, types
from unittest.mock import MagicMock

for _name in ['discord', 'discord.ext', 'discord.ext.commands', 'discord.ext.tasks',
              'anthropic', 'gspread', 'pandas',
              'google', 'google.oauth2', 'google.oauth2.service_account',
              'googleapiclient', 'googleapiclient.discovery',
              'aiohttp', 'aiohttp.web']:
    sys.modules[_name] = MagicMock()

HERE = os.path.dirname(os.path.abspath(__file__))
BOT_DIR = os.path.dirname(HERE)
sys.path.insert(0, BOT_DIR)
os.chdir(BOT_DIR)
import kts_collectr_bot as bot          # noqa: E402
sys.path.insert(0, HERE)
import portal_pricing as pp             # noqa: E402

# Verify post-open behavior (the Sat-3AM hold would reject everything right now).
bot.sports_op_live = lambda: True

# portal category -> bot CardLadder sport string
CAT_TO_BOTSPORT = {'onepiece': 'one piece', 'basketball': 'basketball',
                   'baseball': 'mlb', 'football': 'football'}
RECENT_SALE = '2026-11-01'   # inside any age window; bot requires a visible sale


def bot_eval(sport, value):
    comp = {'found': True, 'clValue': value, 'grade': '8', 'sport': sport,
            'cert': '90000001', 'lastSaleDate': RECENT_SALE,
            'cardName': 'Neutral Testcard', 'setName': 'Base Set'}
    status, reason = bot.classify_psa_comp(comp)
    accepted = status in ('accepted', 'flag')
    rate = bot.get_psa_payout_rate(sport, value, [value]) if accepted else None
    return accepted, rate, reason


mism = []
checked = 0
cents = 0
while cents <= 130000:               # $0.00 .. $1300.00 step $0.25
    value = round(cents / 100.0, 2)
    cents += 25
    if value < 1:                    # bot's documented $1 floor overlay
        continue
    for cat, bsport in CAT_TO_BOTSPORT.items():
        # oracle (app) decision/rate at grade 8, high confidence
        ev = pp.evaluate({'clValue': value, 'grade': 8, 'sport': bsport,
                          'cert': '90000001'})
        o_acc = (ev['decision'] == 'accept')
        o_rate = ev.get('rate')            # percent (e.g. 95) or None
        b_acc, b_rate, b_reason = bot_eval(bsport, value)
        checked += 1
        if b_acc != o_acc:
            mism.append((bsport, value, 'DECISION',
                         f'bot={"accept" if b_acc else "reject"}({b_reason})',
                         f'app={"accept" if o_acc else "reject"}'))
            continue
        if b_acc and o_acc:
            if o_rate is None or abs(b_rate * 100 - o_rate) > 0.001:
                mism.append((bsport, value, 'RATE',
                             f'bot={b_rate*100:.2f}%', f'app={o_rate}%'))

print(f"checked {checked} (sport,value) points, $1-$1300 step $0.25, grade 8, recent sale")
if mism:
    print(f"MISMATCHES: {len(mism)}")
    for sp, v, kind, b, a in mism[:60]:
        print(f"  {sp:11s} ${v:<8} {kind:8s} {b}  |  {a}")
    sys.exit(1)
print("BOT PARITY OK — classify_psa_comp + get_psa_payout_rate reproduce the portal on every point.")
