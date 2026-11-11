"""
certs — cert-token extraction (PSA + CGC), ported from v1.

PSA certs are bare 7-9 digit numbers. CGC certs come back as 'cgc:<digits>' tokens,
detected by a grader label in front, or a CGC-shaped bare number in a CGC context.
The context guards (tracking/phone/payment words, Discord timestamps, GIF links,
the 3-word chatter cap) are hard-won — getting extraction wrong is a broken bot,
so this is a faithful port, not an approximation. Pure/stdlib-only → fully testable.

Two DELIBERATE, NARROW deviations from v1 (2026-11-10) — do not "restore" v1 here:
  1. In a LIST of labelled slabs, the "PSA" labels are not counted as chatter (four or more
     "PSA 12345678" lines used to be dropped in silence). A list = four or more labelled
     numbers, or labelled numbers with at most one other word — and no tracking number or
     shipping/payment word in the message. Anything else is counted exactly as v1 did.
  2. A 7–9 digit number labelled BGS / SGC / Beckett / TAG (upper case) is another grader's
     serial, not a PSA cert, and is skipped (it used to be priced as the PSA card with that
     number) — only on a line that starts label-first; see other_grader_serials().
Everything else is v1's logic unchanged.
"""
import re

CGC_PREFIX = "cgc:"


# ── cert tokens ─────────────────────────────────────────────────────────────────
def is_cgc_cert(cert) -> bool:
    """True for a CGC cert token ('cgc:6193949247')."""
    return str(cert if cert is not None else "").strip().lower().startswith(CGC_PREFIX)


def cert_digits(cert) -> str:
    """The bare cert number (sheet column B / CardLadder link) of a PSA or CGC token."""
    s = str(cert if cert is not None else "").strip()
    return s[len(CGC_PREFIX):].strip() if is_cgc_cert(s) else s


def cgc_cert(digits) -> str:
    return CGC_PREFIX + str(digits).strip()


def cert_label(cert) -> str:
    """How a cert reads in Discord: '12345678' (PSA) or 'CGC 6193949247'."""
    return (f"CGC {cert_digits(cert)}" if is_cgc_cert(cert)
            else str(cert if cert is not None else "").strip())


# ── regexes / word sets ──────────────────────────────────────────────────────────
# One pass finds every 7-13 digit number, remembering which grader label (if any)
# sits right in front of it ON THE SAME LINE. The label may not cross a line break.
_CERT_NUMBER_RE = re.compile(
    r'(?:\b(cgc|psa|bgs|sgc|tag|beckett)[ \t]*[:#\-]?[ \t]*|\b)(\d{7,13})\b', re.I)
# Discord timestamps (<t:1729300000:R>) carry 10-digit numbers — never certs.
_DISCORD_TS_RE = re.compile(r'<t:\d+(?::[a-zA-Z])?>')
# Context that means a bare 10-13 digit number is NOT a CGC cert. Only gates BARE
# (unlabelled) 10-13 digit numbers — PSA 7-9 digit certs are never affected.
_NOT_A_CERT_WORDS = {
    'tracking', 'track', 'tracker', 'fedex', 'ups', 'usps', 'dhl', 'shipped',
    'shipping', 'label', 'phone', 'cell', 'mobile', 'call', 'text', 'tel',
    'whatsapp', 'zelle', 'venmo', 'paypal', 'cashapp', 'cash', 'apple',
    'payment', 'invoice', 'acct', 'account', 'routing', 'wire', 'ach', 'bank',
}


def _cgc_shaped(digits) -> bool:
    """The shape of every real CGC cert seen so far: 10 digits starting 6, or
    13 digits starting 14."""
    d = str(digits)
    return (len(d) == 10 and d.startswith('6')) or (len(d) == 13 and d.startswith('14'))


def _strip_links(text: str) -> str:
    # URLs first — GIF links end in long numeric IDs that look like certs.
    text = re.sub(r'https?://\S+', ' ', text)
    return re.sub(r'www\.\S+', ' ', text)


def _strip_cert_noise(text: str) -> str:
    return _DISCORD_TS_RE.sub(' ', _strip_links(text))


# ── extraction ────────────────────────────────────────────────────────────────
def extract_certs(text: str) -> list:
    """Cert tokens in a message, in order, deduped. (v1's logic, plus the two deviations
    listed in the module docstring.)"""
    return _scan(text)[0]


def other_grader_serials(text: str) -> list:
    """Numbers in a message that were NOT read as certs because they carry another grader's
    label, as display strings ('SGC 1234567'), in order. Only the 7–9 digit ones — the size
    that would otherwise be priced as a PSA cert. The bot tells Kevin about them: skipping a
    number without a word to anyone hides a card the seller thinks they submitted."""
    return _scan(text)[1]


def _scan(text: str) -> tuple:
    """(cert tokens, skipped other-grader serials) for one message."""
    if not text:
        return [], []
    text = _strip_links(text)
    scan = _DISCORD_TS_RE.sub(lambda m: ' ' * len(m.group(0)), text)   # same offsets
    found = [(m.group(2), (m.group(1) or '').lower(), m.start(2), m.group(1) or '')
             for m in _CERT_NUMBER_RE.finditer(scan)]
    if not found:
        return [], []
    # Chatter guard: count alphabetic words on the link-stripped text.
    stripped = re.sub(r'\d', '', text).strip()
    words = [w for w in stripped.split() if re.search(r'[a-zA-Z]', w)]
    plain = [re.sub(r'[^a-z]', '', w.lower()) for w in words]
    # "CGC" labelling a cert isn't chatter — "CGC 1… CGC 2…" is certs, not words. The same
    # goes for "PSA" labels in a LIST of labelled slabs: a customer who labels every slab
    # ("PSA 12345678" four or more times) used to be ignored in silence. Kept narrow on
    # purpose, so every other message is counted exactly as v1 counts it (label = a word):
    #   * a list = 4+ labelled numbers, or labelled numbers with at most ONE other word.
    #     "remove PSA 12345678 from lot" / "I already sold PSA 12345678" are sentences, not
    #     submissions — they stay ignored;
    #   * never when the message also carries a tracking number or a shipping / payment word:
    #     "Shipped PSA 12345678 tracking 1Z…" must stay a tracking message. Certs win over
    #     tracking in the handler, so reading it as certs re-quoted the card and dropped the number.
    # A free-floating "psa" (not attached to a number) always counts as a word.
    not_cert_ctx = any(p in _NOT_A_CERT_WORDS for p in plain)
    psa_labels = min(sum(1 for _d, lab, _p, _r in found if lab == 'psa'), plain.count('psa'))
    other_words = sum(1 for p in plain if p != 'cgc') - psa_labels
    label_list = ((psa_labels >= 4 or other_words <= 1)
                  and not not_cert_ctx and not extract_tracking(text))
    chatter = other_words if label_list else other_words + psa_labels
    if chatter > 3:
        return [], []
    says_cgc = bool(re.search(r'cgc', text, re.I))
    # Does each line start label-first ("SGC 1234567 …") or number-first ("12345678 PSA …")?
    # The regex binds a grader word to the number AFTER it, which is wrong for a number-first
    # line — so other graders' serials are only skipped on label-first lines; a number-first
    # line is read exactly as v1 reads it.
    label_first = {}
    for _d, lab, pos, _r in found:
        label_first.setdefault(scan.rfind('\n', 0, pos), bool(lab))
    seen = set()
    unique = []
    skipped = []
    for digits, label, pos, raw_label in found:
        n = len(digits)
        if n <= 9:
            # PSA: a standalone 7-9 digit number. A label glued on ("psa12345678")
            # was never a match before — keep it so.
            if pos > 0 and (scan[pos - 1].isalnum() or scan[pos - 1] == '_'):
                continue
            # Another grader's SHORT serial ("SGC 1234567") is not a PSA cert. "tag" is also an
            # ordinary word ("price tag 12345678"), so the grader is only TAG in capitals.
            if ((label in ('bgs', 'sgc', 'beckett') or raw_label == 'TAG')
                    and label_first[scan.rfind('\n', 0, pos)]):
                shown = f"{raw_label.upper()} {digits}"
                if shown not in skipped:
                    skipped.append(shown)
                continue
            tok = digits
        elif label == 'cgc':
            tok = cgc_cert(digits)
        elif label:
            continue          # another grader's serial (BGS 0012345678, …)
        elif n in (11, 12):
            continue          # never a bare CGC cert (12 = FedEx tracking)
        elif _cgc_shaped(digits) and (says_cgc or not not_cert_ctx):
            tok = cgc_cert(digits)
        elif says_cgc and not not_cert_ctx:
            tok = cgc_cert(digits)
        else:
            continue
        if tok not in seen:
            seen.add(tok)
            unique.append(tok)
    return unique, skipped


_TRACKING_RE = re.compile(r'\b([0-9]{20,22}|1Z[A-Z0-9]{16}|[0-9]{12,15})\b', re.I)


def extract_tracking(text: str) -> str:
    """The first tracking-number-shaped token (UPS 1Z…, USPS 20-22 digit,
    FedEx/other 12-15 digit), skipping a 14… 13-digit CGC cert. '' if none."""
    if not text:
        return ""
    for m in _TRACKING_RE.finditer(_strip_links(text)):
        tok = m.group(1)
        if not (len(tok) == 13 and tok.startswith("14")):   # not a CGC cert
            return tok
    return ""


def cgc_needs_confirm(text: str, certs: list) -> bool:
    """True when a message's ONLY cert is one bare 10-digit number and nothing in
    it says CGC — as likely a phone number as a slab. The bot should look it up on
    CardLadder/portal first and quote only if it resolves as a real CGC slab."""
    if len(certs or []) != 1 or not is_cgc_cert(certs[0]):
        return False
    if len(cert_digits(certs[0])) != 10:
        return False
    return not re.search(r'cgc', _strip_cert_noise(text or ''), re.I)
