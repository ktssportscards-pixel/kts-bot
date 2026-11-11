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
tickets / payinfo history to the next write. Identical bytes are never kept twice, and
at most 5 NEW copies are kept per store per day (a storm guard, not a lifetime cap);
if the old bytes can't be kept, the listeners are told that too.
"""
import itertools
import json
import os
import shutil
import threading
from datetime import datetime, timedelta, timezone

from . import config

_LOCK = threading.Lock()

# fn(kind, name, detail) callbacks for persistence problems. kind is "load" (a read failed),
# "save" (a write failed) or "lost" (a store was rewritten after a failed read and its old
# bytes could NOT be kept). bot.py registers one that DMs Kevin (rate-limited): a store that
# can't be read or written is a money-tracking fault, and a container-log line is not enough.
_listeners: list = []

# What is known about failed reads. Four separate facts, because they end at different times
# (one flag used to do all four jobs, and each job then lied about the other three):
#
# name -> kind ("corrupt" | "unreadable"): the MOST RECENT read of this store failed. Cleared by
# the next read or save that works. One alert per episode; this is what load_failures() reports.
_failing: dict = {}
# name -> kind: a read failed since this store was last SAVED, so what the process holds for it
# may have been built from the empty default. Cleared only by that save, which keeps the old
# file first. (A later good read does not clear it: the load-once caches never re-read.)
_must_keep: dict = {}
# name -> kind: a load-ONCE cache (load(..., cached=True)) was built from a failed read. The
# process runs on the default for that store until it is restarted; nothing clears this.
_stuck: dict = {}
# name -> detail of the first failed read since the process started (never cleared). The bot
# reports these once when it first connects: a failure during import had nobody to tell.
_ever_failed: dict = {}
# name -> (mtime_ns, size) of the corrupt file this process already copied aside (fast path).
_preserved: dict = {}


def _path(name: str) -> str:
    return os.path.join(config.DATA_DIR, f"{name}.json")


_seq = itertools.count(1)


def _stamp() -> str:
    """A sortable UTC stamp for kept-copy file names; the counter makes two names taken in
    the same microsecond (or after a clock step) distinct, so one copy never replaces another."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + f"-{next(_seq)}"


_STAMP_FMT = "%Y%m%dT%H%M%S%fZ"
_KEEP_MAX = 5                 # NEW kept copies per store per _KEEP_WINDOW — a storm guard: a store
_KEEP_WINDOW = timedelta(hours=24)   # that flaps all day can't fill the volume, but there is no
                                     # lifetime cap (old incidents must not block keeping new ones)


def _sig(path: str):
    """(mtime_ns, size) of a file, or None if it can't be stat'ed."""
    try:
        st = os.stat(path)
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return None


def _kept_copies(path: str) -> list:
    """Paths of the copies kept for the store at `path` (any age, any order)."""
    base, folder = os.path.basename(path), os.path.dirname(path)
    try:
        return [os.path.join(folder, n) for n in os.listdir(folder)
                if n.startswith(base + ".corrupt-") or n.startswith(base + ".unreadable-")]
    except OSError:
        return []


def _recent_kept(path: str) -> int:
    """How many copies of this store were kept inside the storm-guard window. The time is
    read from the stamp in the name (a hard link / copy keeps the ORIGINAL file's mtime); a
    name without a readable stamp is not one of ours from this window and is not counted."""
    cutoff = datetime.now(timezone.utc) - _KEEP_WINDOW
    n = 0
    for other in _kept_copies(path):
        try:
            stamp = os.path.basename(other)[len(os.path.basename(path)) + 1:].split("-")[1]
            when = datetime.strptime(stamp, _STAMP_FMT).replace(tzinfo=timezone.utc)
        except (IndexError, ValueError):
            continue
        if when >= cutoff:
            n += 1
    return n


def _already_kept(path: str) -> str:
    """File name of a kept copy holding exactly the bytes now at `path`, or ''. A restart
    re-reads the same corrupt file and a passing I/O error leaves the same good file; neither
    needs (or should use up the allowance for) a second copy."""
    try:
        size = os.path.getsize(path)
        mine = None
        for other in _kept_copies(path):
            try:
                if os.path.isfile(other) and os.path.getsize(other) == size:
                    if mine is None:
                        with open(path, "rb") as f:
                            mine = f.read()
                    with open(other, "rb") as f:
                        if f.read() == mine:
                            return os.path.basename(other)
            except OSError:
                continue
    except OSError:
        pass
    return ""


def add_listener(fn) -> None:
    """Register fn(kind, name, detail) to be told about persistence problems."""
    if fn not in _listeners:
        _listeners.append(fn)


def load_failures() -> dict:
    """{store name: 'corrupt' | 'unreadable'} for stores whose MOST RECENT read failed, i.e.
    that are failing right now. An entry goes away as soon as the store is read or saved
    successfully."""
    with _LOCK:
        return dict(_failing)


def stuck() -> dict:
    """{store name: kind} for load-once caches that were built from a failed read: this
    process runs on the empty default for them until it is restarted (fixing the file is
    not enough, and the next save of the cache replaces the file)."""
    with _LOCK:
        return dict(_stuck)


def failures_since_start() -> dict:
    """{store name: detail} for every store a read of which has failed since the process
    started, whether or not it has recovered. Reported once at the first connect, because a
    failure during import (or before the Discord connection) had nobody to tell."""
    with _LOCK:
        return dict(_ever_failed)


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


def _guard_full() -> str:
    return f"{_KEEP_MAX} copies of it were already kept in the last 24 h"


def _load_problem(name: str, path: str, exc: Exception, cached: bool) -> None:
    """An existing store could not be read/parsed. Remember it, keep the evidence, tell
    the listeners — once per episode, not once per load (stores are re-read constantly).
    The detail says what actually happened to the bytes, so nobody is told a copy exists
    when it doesn't."""
    kind = "corrupt" if isinstance(exc, ValueError) else "unreadable"   # bad JSON / bad bytes vs I/O
    copied, fate = "", ""
    with _LOCK:
        first = _failing.get(name) != kind
        _failing[name] = kind
        _must_keep[name] = kind
        if cached:
            _stuck[name] = kind
        if kind == "corrupt":                       # readable bytes → copy them aside right now
            sig = _sig(path)
            if sig is not None and _preserved.get(name) == sig:
                fate = "these bytes are already kept"
            elif sig is not None:
                same = _already_kept(path)
                if same:
                    _preserved[name] = sig
                    fate = f"these bytes are already kept as {same}"
                elif _recent_kept(path) >= _KEEP_MAX:
                    fate = f"NO copy kept: {_guard_full()}"
                else:
                    try:
                        copied = f"{path}.corrupt-{_stamp()}"
                        shutil.copy2(path, copied)
                        _preserved[name] = sig
                        fate = f"copy kept: {os.path.basename(copied)}"
                    except Exception as e:
                        copied = ""
                        fate = f"NO copy kept: {e}"
        else:
            fate = ("it can't be copied while it can't be read; my next write to this store "
                    "moves the old file aside first")
        detail = f"{kind}: {exc}" + (f" ({fate})" if fate else "")
        _ever_failed.setdefault(name, detail)
    if first or copied:
        print(f"[state] load {name!r} failed: {detail}")
        _notify("load", name, detail)


def load(name: str, default, *, cached: bool = False):
    """Load a store, or return `default` (a value, or a 0-arg factory) when it can't be
    read — a missing/corrupt store never crashes the bot. A store that simply doesn't
    exist yet is the normal first-run case; one that exists but can't be read is
    reported and preserved (see the module docstring).

    Pass cached=True when the result is kept for the life of the process (the module-level
    caches in bot / tickets / vip): a failed read then can't heal by itself — see stuck().

    A failed read is remembered until the store is next SAVED, even if a later read
    succeeds: whatever was built from the empty default is still about to be written over
    the file, so that write must keep the old bytes. Only the "failing right now" state
    (load_failures) ends with the next good read."""
    path = _path(name)
    try:
        with open(path) as f:
            data = json.load(f)
    except FileNotFoundError:
        data = _default(default)
    except Exception as e:
        _load_problem(name, path, e, cached)
        return _default(default)
    if name in _failing:                            # it reads again (or is simply gone)
        with _LOCK:
            _failing.pop(name, None)
    return data


def _keep_aside(name: str, path: str) -> str:
    """(Called under _LOCK, just before the store is replaced.) This process failed to read
    the file at `path`, so the data about to be written may have been built WITHOUT it. Keep
    the old bytes next to it instead of destroying them. Returns '' when they are safe
    (kept now, kept already, or there was nothing there), else the reason they could not be
    kept. Never raises: the save goes ahead either way (the pre-existing behaviour) —
    refusing to write would stall the whole bot on one bad file — but the caller reports it."""
    if not os.path.lexists(path):
        return ""
    if _must_keep.get(name) == "corrupt" and _preserved.get(name) == _sig(path):
        return ""                                   # load() already copied exactly these bytes
    same = _already_kept(path)
    if same:
        print(f"[state] the previous {name!r} is already kept as {same}")
        return ""
    if _recent_kept(path) >= _KEEP_MAX:
        print(f"[state] {name!r}: {_guard_full()} — NOT keeping another")
        return _guard_full()
    aside = f"{path}.unreadable-{_stamp()}"
    try:
        try:
            os.link(path, aside)                    # hard link: the live name never disappears
        except OSError:
            os.replace(path, aside)                 # no hard links here → move it (needs no read access)
        print(f"[state] kept the previous {name!r} as {aside}")
        return ""
    except OSError as e:
        print(f"[state] couldn't keep the previous {name!r} ({e}) — writing anyway")
        return str(e) or e.__class__.__name__


def save(name: str, data) -> bool:
    """Atomically persist a store. Returns True on success; logs, tells the listeners and
    returns False on failure (persistence is best-effort, never fatal to a live quote)."""
    path = _path(name)
    tmp = path + ".tmp"
    not_kept = ""
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
                if name in _must_keep:
                    not_kept = _keep_aside(name, path)
                os.replace(tmp, path)
                _must_keep.pop(name, None)
                _failing.pop(name, None)
                _preserved.pop(name, None)
            except Exception:
                try:
                    os.unlink(tmp)                  # don't leave a partial temp file behind
                except OSError:
                    pass
                raise
    except Exception as e:
        print(f"[state] save {name!r} failed (non-fatal): {e}")
        _notify("save", name, str(e))
        return False
    if not_kept:                                    # the write worked, but what it replaced is gone
        _notify("lost", name, not_kept)
    return True


class Store:
    """Load-once, save-on-demand wrapper around one JSON store.

        tickets = Store("ticket_channels", list)   # default factory
        tickets.data.append(123); tickets.save()
    """

    def __init__(self, name: str, default):
        self.name = name
        self.data = load(name, default, cached=True)

    def save(self) -> bool:
        return save(self.name, self.data)
