"""
verify_hformula.py — does the sheet payout formula (build_sheet_h_formula, the
number Kevin PAYS off the buying sheet) reproduce the KTS Portal's rate on every
value? Catches silent band-boundary typos in the Google-Sheets formula string.

It evaluates the REAL formula string with a small recursive-descent interpreter
for the Sheets subset the formula uses (IF/IFS/AND/OR/NOT/ISNUMBER/N/REGEXMATCH/
LOWER/DATEVALUE/TODAY/IFERROR/LEFT + comparisons/concat/arithmetic), then compares
H (a rate decimal, e.g. 0.95, or 0 for reject) to portal_pricing across the grid.
The interpreter self-tests on hand-computed cases before the grid runs.

Run:  python3 verify_hformula.py   ->  "H-FORMULA PARITY OK" or mismatches.
"""
import sys, os, re
from datetime import date
from unittest.mock import MagicMock

for _n in ['discord', 'discord.ext', 'discord.ext.commands', 'discord.ext.tasks',
           'anthropic', 'gspread', 'pandas', 'google', 'google.oauth2',
           'google.oauth2.service_account', 'googleapiclient',
           'googleapiclient.discovery', 'aiohttp', 'aiohttp.web']:
    sys.modules[_n] = MagicMock()

HERE = os.path.dirname(os.path.abspath(__file__))
BOT_DIR = os.path.dirname(HERE)
sys.path.insert(0, BOT_DIR); os.chdir(BOT_DIR)
import kts_collectr_bot as bot          # noqa: E402
sys.path.insert(0, HERE)
import portal_pricing as pp             # noqa: E402

# ── tiny Sheets-formula interpreter ──────────────────────────────────────────
TOK = re.compile(r'\s*(<=|>=|<>|[(),&=<>+\-]|"(?:[^"]*)"|[A-Za-z_][A-Za-z0-9_]*|\d+\.?\d*)')

def tokenize(s):
    toks, i = [], 0
    while i < len(s):
        m = TOK.match(s, i)
        if not m:
            if s[i].isspace():
                i += 1; continue
            raise SyntaxError(f"bad char {s[i]!r} at {i}")
        toks.append(m.group(1)); i = m.end()
    return toks

class P:
    def __init__(self, toks): self.t = toks; self.i = 0
    def peek(self): return self.t[self.i] if self.i < len(self.t) else None
    def nxt(self): tok = self.t[self.i]; self.i += 1; return tok

def truthy(v): return bool(v)

class Err(Exception): pass

def _n(v):   # Sheets N()
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else 0

TODAY = date(2026, 11, 5)

def _datevalue(s):
    return date.fromisoformat(str(s)).toordinal()

FUNCS = {
    'IF': lambda a: (a[1]() if truthy(a[0]()) else (a[2]() if len(a) > 2 else False)),
    'IFS': lambda a: _ifs(a),
    'AND': lambda a: all(truthy(x()) for x in a),
    'OR': lambda a: any(truthy(x()) for x in a),
    'NOT': lambda a: not truthy(a[0]()),
    'ISNUMBER': lambda a: isinstance(a[0](), (int, float)) and not isinstance(a[0](), bool),
    'N': lambda a: _n(a[0]()),
    'REGEXMATCH': lambda a: bool(re.search(a[1](), a[0]())),
    'LOWER': lambda a: str(a[0]()).lower(),
    'LEFT': lambda a: str(a[0]())[:int(a[1]())],
    'DATEVALUE': lambda a: _datevalue(a[0]()),
    'TODAY': lambda a: TODAY.toordinal(),
    'IFERROR': lambda a: _iferror(a),
}

def _ifs(a):
    for k in range(0, len(a), 2):
        if truthy(a[k]()):
            return a[k + 1]()
    raise Err("IFS no match")

def _iferror(a):
    try:
        return a[0]()
    except Exception:
        return a[1]()

def parse(toks):
    p = P(toks)
    node = p_expr(p)
    if p.peek() is not None:
        raise SyntaxError(f"trailing {p.peek()!r}")
    return node

def p_expr(p): return p_cmp(p)

def p_cmp(p):
    left = p_concat(p)
    if p.peek() in ('=', '<>', '<', '>', '<=', '>='):
        op = p.nxt(); right = p_concat(p)
        def cmp(l=left, r=right, o=op):
            lv, rv = l(), r()
            if o == '=':  return lv == rv
            if o == '<>': return lv != rv
            if isinstance(lv, str) or isinstance(rv, str):
                # Sheets compares text ordinally; our numeric bands never hit this
                lv, rv = str(lv), str(rv)
            if o == '<':  return lv < rv
            if o == '>':  return lv > rv
            if o == '<=': return lv <= rv
            if o == '>=': return lv >= rv
        return cmp
    return left

def p_concat(p):
    left = p_add(p)
    while p.peek() == '&':
        p.nxt(); right = p_add(p)
        left = (lambda l=left, r=right: (lambda: str(l()) + str(r())))()
    return left

def p_add(p):
    left = p_unary(p)
    while p.peek() in ('+', '-'):
        op = p.nxt(); right = p_unary(p)
        left = (lambda l=left, r=right, o=op: (lambda: l() + r() if o == '+' else l() - r()))()
    return left

def p_unary(p):
    if p.peek() == '-':
        p.nxt(); v = p_primary(p); return lambda v=v: -v()
    return p_primary(p)

def p_primary(p):
    tok = p.nxt()
    if tok == '(':
        node = p_expr(p); assert p.nxt() == ')'; return node
    if tok.startswith('"'):
        s = tok[1:-1]; return lambda s=s: s
    if re.fullmatch(r'\d+\.?\d*', tok):
        num = float(tok); num = int(num) if num == int(num) else num
        return lambda num=num: num
    if tok in ('TRUE', 'FALSE'):
        b = (tok == 'TRUE'); return lambda b=b: b
    if tok in FUNCS and p.peek() == '(':
        p.nxt()                       # consume '('
        args = []
        if p.peek() != ')':
            args.append(p_expr(p))
            while p.peek() == ',':
                p.nxt(); args.append(p_expr(p))
        assert p.nxt() == ')', "expected )"
        fn = FUNCS[tok]
        return lambda fn=fn, args=args: fn(args)
    # otherwise a cell ref like G2 / A2 ...
    return lambda ref=tok: CELLS[ref]

CELLS = {}

def eval_formula(formula, cells):
    global CELLS
    CELLS = cells
    body = formula[1:] if formula.startswith('=') else formula
    return parse(tokenize(body))()

# ── self-test the interpreter on hand-computed cases ─────────────────────────
F = bot.build_sheet_h_formula(2)   # row 2 formula from the REAL bot

def cell(sport, value, grade=8, name='Neutral Testcard', cert='90000001',
         sale='2026-11-01', a='PSA', l=''):
    return {'A2': a, 'B2': cert, 'D2': name, 'E2': grade,
            'F2': sport, 'G2': value, 'J2': sale, 'L2': l}

_self = [
    (('basketball', 500), 0.95), (('basketball', 850), 0), (('basketball', 1000), 0.95),
    (('basketball', 1001), 0), (('basketball', 245), 0),
    (('mlb', 30), 1.0), (('mlb', 35), 0.90), (('mlb', 32), 0), (('mlb', 500), 0.89),
    (('mlb', 450), 0.90), (('mlb', 470), 0), (('mlb', 600), 0.89), (('mlb', 650), 0),
    (('one piece', 100), 0.89), (('one piece', 245), 0), (('one piece', 1000), 0.84),
    (('one piece', 1100), 0.85), (('one piece', 1200), 0.85), (('one piece', 1201), 0),
    (('football', 30), 1.0), (('football', 500), 0.90), (('football', 501), 0.89),
    (('football', 800), 0.89), (('football', 801), 0),
]
bad = []
for (sp, v), exp in _self:
    got = eval_formula(F, cell(sp, v))
    if abs((got or 0) - exp) > 1e-9:
        bad.append((sp, v, got, exp))
if bad:
    print("INTERPRETER SELF-TEST FAILED:")
    for sp, v, g, e in bad:
        print(f"  {sp} ${v}: got {g} expected {e}")
    sys.exit(2)
# ban + blank-sale sanity
assert eval_formula(F, cell('basketball', 300, name='Ja Morant 2020')) == 0, "ban not applied"
assert eval_formula(F, cell('basketball', 150, name='Trae Young')) == 0.95, "under-$200 trae should clear"
assert eval_formula(F, cell('basketball', 300, name='Trae Young')) == 0, "over-$200 trae should ban"
assert eval_formula(F, cell('one piece', 500, sale='')) == 0, "blank sale should zero (must-show-sale overlay)"
print("interpreter self-test + ban/sale sanity: OK")

# ── full grid vs portal ──────────────────────────────────────────────────────
CAT_TO_BOTSPORT = {'onepiece': 'one piece', 'basketball': 'basketball',
                   'baseball': 'mlb', 'football': 'football'}
mism, checked = [], 0
cents = 100
while cents <= 130000:
    value = round(cents / 100.0, 2); cents += 25
    for cat, bsport in CAT_TO_BOTSPORT.items():
        ev = pp.evaluate({'clValue': value, 'grade': 8, 'sport': bsport, 'cert': '90000001'})
        exp = (ev['rate'] / 100.0) if ev['decision'] == 'accept' else 0
        got = eval_formula(F, cell(bsport, value)) or 0
        checked += 1
        if abs(got - exp) > 1e-9:
            mism.append((bsport, value, got, exp))

print(f"checked {checked} (sport,value) points, $1-$1300 step $0.25")
if mism:
    print(f"H-FORMULA MISMATCHES: {len(mism)}")
    for sp, v, g, e in mism[:60]:
        print(f"  {sp:11s} ${v:<8} sheet H={g}  portal={e}")
    sys.exit(1)
print("H-FORMULA PARITY OK — the sheet payout formula reproduces the portal rate on every point.")
