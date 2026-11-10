"""
textsplit — split long text into Discord-sized messages.

Discord rejects any message whose content is over 2,000 characters (HTTP 400), and the
bot's senders treat a failed send as non-fatal — so an over-long quote, Kevin summary,
payout digest or `!owe` packet used to vanish without a trace. v1 chunked these
(_split_for_discord); this is the v2 equivalent. Pure/stdlib-only → fully testable.
"""

DISCORD_LIMIT = 2000        # Discord's hard cap on message content
SAFE_LIMIT = 1900           # what we actually pack to (headroom under the cap)
_PARA_MIN_FRACTION = 0.25   # break at a blank line only if the chunk keeps at least this much


def _units(s: str) -> int:
    """Length the way Discord counts it: UTF-16 code units (an emoji outside the BMP is 2)."""
    return len(s) + sum(1 for ch in s if ord(ch) > 0xFFFF)


def _hard_split(line: str, limit: int) -> list:
    """Cut one over-long line into pieces of at most `limit` units (never mid-character)."""
    out, cur, cur_n = [], [], 0
    for ch in line:
        n = 2 if ord(ch) > 0xFFFF else 1
        if cur and cur_n + n > limit:
            out.append("".join(cur))
            cur, cur_n = [], 0
        cur.append(ch)
        cur_n += n
    if cur:
        out.append("".join(cur))
    return out


def split_message(text: str, limit: int = SAFE_LIMIT) -> list:
    """Split `text` into chunks Discord will accept, in order. Text that already fits comes
    back as ONE unchanged chunk (so short messages behave exactly as before). Longer text
    is cut only at line breaks — preferring a blank line (so a payment packet / section
    stays in one message) when that doesn't leave the chunk mostly empty — and inside a
    line only when that single line is itself over the limit. Whitespace-only chunks are
    dropped (Discord rejects an empty message); '' → []."""
    text = "" if text is None else str(text)
    limit = max(1, min(int(limit), DISCORD_LIMIT))
    if not text.strip():
        return []
    if _units(text) <= limit:
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


async def send_chunked(target, text, limit: int = SAFE_LIMIT) -> int:
    """`await target.send(...)` once per chunk of `text`, in order (a channel, a user, or
    anything else with an async .send). Returns the number of messages sent — 0 for empty
    text. Errors propagate unchanged, so each caller keeps its own failure handling."""
    sent = 0
    for part in split_message(text, limit):
        await target.send(part)
        sent += 1
    return sent
