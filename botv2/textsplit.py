"""
textsplit — split long text into Discord-sized messages.

Discord rejects any message whose content is over 2,000 characters (HTTP 400), and the
bot's senders treat a failed send as non-fatal — so an over-long quote, Kevin summary,
payout digest or `!owe` packet used to vanish without a trace. v1 chunked these
(_split_for_discord); this is the v2 equivalent. The splitting is pure/stdlib-only →
fully testable.
"""
import asyncio
import weakref

DISCORD_LIMIT = 2000        # Discord's hard cap on message content
SAFE_LIMIT = 1900           # what we pack to once a split IS needed (headroom under the cap)
_PARA_MIN_FRACTION = 0.25   # break at a blank line only if the chunk keeps at least this much
_WORD_MIN_FRACTION = 0.5    # break an over-long line at a space/comma only if the piece keeps this much


def _units(s: str) -> int:
    """Length the way Discord counts it: UTF-16 code units (an emoji outside the BMP is 2)."""
    return len(s) + sum(1 for ch in s if ord(ch) > 0xFFFF)


def _hard_split(line: str, limit: int) -> list:
    """Cut one over-long line into pieces of at most `limit` units, never mid-character.
    The cut goes after the last space or comma in the window when that keeps the piece at
    least half full, so a cert number or a link in a long comma-separated line is not broken
    in two; only a line with no such break (one very long token) is cut at an arbitrary point.
    Nothing is dropped: the pieces joined together are the line."""
    out, cur, cur_n = [], [], 0
    for ch in line:
        n = 2 if ord(ch) > 0xFFFF else 1
        if cur and cur_n + n > limit:
            cut = len(cur)
            floor_n, run_n = limit * _WORD_MIN_FRACTION, cur_n
            for i in range(len(cur) - 1, 0, -1):
                if cur[i] in " ,":
                    if run_n >= floor_n:
                        cut = i + 1
                    break
                run_n -= 2 if ord(cur[i]) > 0xFFFF else 1
            out.append("".join(cur[:cut]))
            cur = cur[cut:]
            cur_n = _units("".join(cur))
        cur.append(ch)
        cur_n += n
    if cur:
        out.append("".join(cur))
    return out


def split_message(text: str, limit: int = None) -> list:
    """Split `text` into chunks Discord will accept, in order.

    With no `limit` (every caller in the bot): text Discord takes as it is — up to its 2,000
    cap — comes back as ONE unchanged chunk, so any message that used to arrive whole still
    does; only longer text is split, and then packed into chunks of at most SAFE_LIMIT. An
    explicit `limit` is used for both (tests).

    Longer text is cut only at line breaks — preferring a blank line (so a payment packet /
    section stays in one message) when that doesn't leave the chunk mostly empty — and inside
    a line only when that single line is itself over the limit. Whitespace-only chunks are
    dropped (Discord rejects an empty message); '' → []."""
    text = "" if text is None else str(text)
    if limit is None:
        whole, limit = DISCORD_LIMIT, SAFE_LIMIT
    else:
        whole = limit = max(1, min(int(limit), DISCORD_LIMIT))
    if not text.strip():
        return []
    if _units(text) <= whole:
        return [text]

    lines = []
    for ln in text.split("\n"):
        lines.extend(_hard_split(ln, limit) if _units(ln) > limit else [ln])

    chunks, cur = [], []                 # cur = the lines of the chunk being built

    def size(ls):
        return sum(_units(x) for x in ls) + max(0, len(ls) - 1)      # + the joining newlines

    for ln in lines:
        while cur and size(cur + [ln]) > limit:
            # Prefer the LAST blank line as the cut, if what stays is a decent-sized message.
            cut = None
            for i in range(len(cur) - 1, 0, -1):
                if not cur[i].strip() and size(cur[:i]) >= limit * _PARA_MIN_FRACTION:
                    cut = i
                    break
            if cut is None:
                chunks.append("\n".join(cur))
                cur = []
            else:
                chunks.append("\n".join(cur[:cut]))
                cur = cur[cut + 1:]
        cur.append(ln)
    if cur:
        chunks.append("\n".join(cur))
    return [c for c in chunks if c.strip()]


# One lock per destination, alive only while something is sending to it (weak values: no
# growth, and never a lock left over from another event loop).
_send_locks = weakref.WeakValueDictionary()


def sent_before_failure(exc) -> tuple:
    """(parts delivered, parts in the message) for an exception that came out of
    send_chunked, or (0, 0) for any other exception. Lets a caller tell "nothing arrived"
    from "the first part arrived and the rest did not"."""
    return (int(getattr(exc, "kts_parts_sent", 0) or 0), int(getattr(exc, "kts_parts_total", 0) or 0))


async def send_chunked(target, text, limit: int = None) -> int:
    """`await target.send(...)` once per chunk of `text`, in order (a channel, a user, or
    anything else with an async .send). Returns the number of messages sent — 0 for empty
    text.

    The parts of one message go out together: sends through here to the same destination
    are serialised, so two long messages (two quotes in one ticket, two lot summaries in
    Kevin's DMs) can't interleave part-by-part and attribute one seller's cards to another.

    Errors propagate unchanged, so each caller keeps its own failure handling; the exception
    is tagged with how many parts had already been delivered (see sent_before_failure)."""
    parts = split_message(text, limit)
    if not parts:
        return 0
    key = getattr(target, "id", None)
    if key is None:
        key = id(target)
    lock = _send_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _send_locks[key] = lock
    sent = 0
    async with lock:
        for part in parts:
            try:
                await target.send(part)
            except Exception as e:
                try:
                    e.kts_parts_sent, e.kts_parts_total = sent, len(parts)
                except Exception:
                    pass                          # an exception type that refuses attributes
                raise
            sent += 1
    return sent
