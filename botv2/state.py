"""
state — tiny, robust JSON persistence for botv2.

Every store is a named JSON file under DATA_DIR. Writes are atomic (temp file +
fsync + os.replace) and lock-guarded, so a crash or a concurrent write can never
leave a half-written store (v1 hit a corrupted vip_store once — this prevents that
class of bug). In prod, point DATA_DIR at a mounted volume so state survives
redeploys; without one, stores are ephemeral and callers must degrade gracefully
(v1's redeploy-recovery lessons are preserved in each consumer).

A store that EXISTS but can't be read is never silently replaced: load() still hands
back the default (the bot keeps running), but the old bytes are kept — a corrupt file
is copied aside at once (<name>.json.corrupt-<stamp>), and the next save() keeps
whatever is on disk (<name>.json.unreadable-<stamp>) before replacing it — and
listeners are told, so Kevin hears about it instead of quietly losing the owed /
tickets / payinfo history to the next write. At most 5 kept copies per store.
"""
import itertools
import json
import os
import shutil
import threading
from datetime import datetime, timezone

from . import config

_LOCK = threading.Lock()

# fn(kind, name, detail) callbacks for persistence problems (kind: "load" | "save").
# bot.py registers one that DMs Kevin (rate-limited): a store that can't be read or
# written is a money-tracking fault, and a line in the container log is not enough.
_listeners: list = []
# store name -> why a load of it failed ("corrupt" | "unreadable"). Cleared only by the next
# SAVE of that store; while set, that save keeps the old file instead of destroying it.
_load_failed: dict = {}
# store name -> (mtime_ns, size) of the corrupt file already copied aside (copy once).
_preserved: dict = {}


def _path(name: str) -> str:
    return os.path.join(config.DATA_DIR, f"{name}.json")


_seq = itertools.count(1)


def _stamp() -> str:
    """A sortable UTC stamp for kept-copy file names; the counter makes two names taken in
    the same microsecond (or after a clock step) distinct, so one copy never replaces another."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + f"-{next(_seq)}"


_KEEP_MAX = 5       # kept copies per store — the EARLIEST ones are closest to the last good state


def _sig(path: str):
    """(mtime_ns, size) of a file, or None if it can't be stat'ed."""
    try:
        st = os.stat(path)
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return None


def _kept_count(path: str) -> int:
    base = os.path.basename(path)
    try:
        return sum(1 for n in os.listdir(os.path.dirname(path))
                   if n.startswith(base + ".corrupt-") or n.startswith(base + ".unreadable-"))
    except OSError:
        return 0


def add_listener(fn) -> None:
    """Register fn(kind, name, detail) to be told about persistence problems."""
    if fn not in _listeners:
        _listeners.append(fn)


def load_failures() -> dict:
    """{store name: 'corrupt' | 'unreadable'} for stores that failed to load and have not
    been rewritten since (startup uses this to tell Kevin about a store that was already
    bad when the bot booted)."""
    with _LOCK:
        return dict(_load_failed)


def writable() -> bool:
    """True if a file can be created in DATA_DIR right now (a startup probe; leaves nothing
    behind). False means nothing the bot records is being saved."""
    try:
        os.makedirs(config.DATA_DIR, exist_ok=True)
        probe = os.path.join(config.DATA_DIR, ".write-probe")
        with open(probe, "w") as f:
            f.write("ok")
        os.unlink(probe)
        return True
    except Exception as e:
        print(f"[state] DATA_DIR write probe failed: {e}")
        return False


def _notify(kind: str, name: str, detail: str) -> None:
    for fn in list(_listeners):
        try:
            fn(kind, name, detail)
        except Exception as e:                      # a listener must never break persistence
            print(f"[state] listener failed (non-fatal): {e}")


def _default(default):
    return default() if callable(default) else default


def _load_problem(name: str, path: str, exc: Exception) -> None:
    """An existing store could not be read/parsed. Remember it, keep the evidence, tell
    the listeners — once per episode, not once per load (stores are re-read constantly)."""
    kind = "corrupt" if isinstance(exc, ValueError) else "unreadable"   # bad JSON / bad bytes vs I/O
    kept = ""
    with _LOCK:
        first = _load_failed.get(name) != kind
        _load_failed[name] = kind
        if kind == "corrupt":                       # readable bytes → copy them aside right now
            sig = _sig(path)
            if sig is not None and _preserved.get(name) != sig and _kept_count(path) < _KEEP_MAX:
                try:
                    kept = f"{path}.corrupt-{_stamp()}"
                    shutil.copy2(path, kept)
                    _preserved[name] = sig
                except Exception as e:
                    kept = ""
                    print(f"[state] couldn't keep a copy of corrupt {name!r}: {e}")
    if first or kept:
        print(f"[state] load {name!r} failed ({kind}): {exc}"
              + (f" — copy kept at {kept}" if kept else ""))
        _notify("load", name, f"{kind}: {exc}" + (f" (copy kept: {os.path.basename(kept)})" if kept else ""))


def load(name: str, default):
    """Load a store, or return `default` (a value, or a 0-arg factory) when it can't be
    read — a missing/corrupt store never crashes the bot. A store that simply doesn't
    exist yet is the normal first-run case; one that exists but can't be read is
    reported and preserved (see the module docstring).

    A failed read is remembered until the store is next SAVED, even if a later read
    succeeds: whatever was built from the empty default (the module-level caches in
    bot / tickets / vip are loaded once and never re-read) is still about to be written
    over the file, so that write must keep the old bytes."""
    path = _path(name)
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return _default(default)
    except Exception as e:
        _load_problem(name, path, e)
        return _default(default)


def _keep_aside(name: str, path: str) -> None:
    """(Called under _LOCK, just before the store is replaced.) This process failed to read
    the file at `path`, so the data about to be written was built WITHOUT it. Keep the old
    bytes next to it instead of destroying them. Never raises: if they can't be kept, the
    save still goes ahead (the pre-existing behaviour) — refusing to write would stall the
    whole bot on one bad file."""
    if not os.path.lexists(path):
        return
    if _load_failed.get(name) == "corrupt" and _preserved.get(name) == _sig(path):
        return                                      # load() already copied exactly these bytes
    if _kept_count(path) >= _KEEP_MAX:
        print(f"[state] {name!r}: already {_KEEP_MAX} kept copies — not keeping another")
        return
    aside = f"{path}.unreadable-{_stamp()}"
    try:
        try:
            os.link(path, aside)                    # hard link: the live name never disappears
        except OSError:
            os.replace(path, aside)                 # no hard links here → move it (needs no read access)
        print(f"[state] kept the previous {name!r} as {aside}")
    except OSError as e:
        print(f"[state] couldn't keep the previous {name!r} ({e}) — writing anyway")


def save(name: str, data) -> bool:
    """Atomically persist a store. Returns True on success; logs, tells the listeners and
    returns False on failure (persistence is best-effort, never fatal to a live quote)."""
    path = _path(name)
    tmp = path + ".tmp"
    try:
        # Serialise BEFORE touching any file: an unserialisable value fails here, cleanly,
        # and json.dumps uses the C encoder (several times faster than json.dump to a file —
        # these stores are rewritten on the event loop). Same bytes as before.
        payload = json.dumps(data)
        os.makedirs(config.DATA_DIR, exist_ok=True)
        with _LOCK:
            try:
                with open(tmp, "w") as f:
                    f.write(payload)
                    f.flush()
                    try:
                        os.fsync(f.fileno())        # the rename must not outrun the data
                    except OSError:
                        pass                        # best-effort (some filesystems refuse fsync)
                if name in _load_failed:
                    _keep_aside(name, path)
                os.replace(tmp, path)
                _load_failed.pop(name, None)
                _preserved.pop(name, None)
            except Exception:
                try:
                    os.unlink(tmp)                  # don't leave a partial temp file behind
                except OSError:
                    pass
                raise
        return True
    except Exception as e:
        print(f"[state] save {name!r} failed (non-fatal): {e}")
        _notify("save", name, str(e))
        return False


class Store:
    """Load-once, save-on-demand wrapper around one JSON store.

        tickets = Store("ticket_channels", list)   # default factory
        tickets.data.append(123); tickets.save()
    """

    def __init__(self, name: str, default):
        self.name = name
        self.data = load(name, default)

    def save(self) -> bool:
        return save(self.name, self.data)
