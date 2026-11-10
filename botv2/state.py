"""
state — tiny, robust JSON persistence for botv2.

Every store is a named JSON file under DATA_DIR. Writes are atomic (temp file +
fsync + os.replace) and lock-guarded, so a crash or a concurrent write can never
leave a half-written store (v1 hit a corrupted vip_store once — this prevents that
class of bug). In prod, point DATA_DIR at a mounted volume so state survives
redeploys; without one, stores are ephemeral and callers must degrade gracefully
(v1's redeploy-recovery lessons are preserved in each consumer).

A store that EXISTS but can't be read is never silently replaced: load() still hands
back the default (the bot keeps running), but the unreadable file is kept — copied
aside at once if it is corrupt, moved aside by the next save() if it couldn't even be
opened — and listeners are told, so Kevin hears about it instead of quietly losing the
owed / tickets / payinfo history to the next write.
"""
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
# store name -> why its last load failed ("corrupt" | "unreadable"). Cleared by a later
# successful load or save. While set, save() keeps the old file instead of destroying it.
_load_failed: dict = {}
# store name -> (mtime_ns, size) of the corrupt file already copied aside (copy once).
_preserved: dict = {}


def _path(name: str) -> str:
    return os.path.join(config.DATA_DIR, f"{name}.json")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def add_listener(fn) -> None:
    """Register fn(kind, name, detail) to be told about persistence problems."""
    if fn not in _listeners:
        _listeners.append(fn)


def load_failures() -> dict:
    """{store name: 'corrupt' | 'unreadable'} for stores whose last load failed and that
    have not been read or rewritten successfully since (startup uses this to tell Kevin
    about a store that was already bad when the bot booted)."""
    with _LOCK:
        return dict(_load_failed)


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
            try:
                st = os.stat(path)
                sig = (st.st_mtime_ns, st.st_size)
                if _preserved.get(name) != sig:
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
    reported and preserved (see the module docstring)."""
    path = _path(name)
    try:
        with open(path) as f:
            data = json.load(f)
    except FileNotFoundError:
        return _default(default)
    except Exception as e:
        _load_problem(name, path, e)
        return _default(default)
    if _load_failed:                                # rare path: a store recovered on its own
        with _LOCK:
            _load_failed.pop(name, None)
    return data


def save(name: str, data) -> bool:
    """Atomically persist a store. Returns True on success; logs, tells the listeners and
    returns False on failure (persistence is best-effort, never fatal to a live quote)."""
    path = _path(name)
    tmp = path + ".tmp"
    try:
        os.makedirs(config.DATA_DIR, exist_ok=True)
        with _LOCK:
            try:
                with open(tmp, "w") as f:
                    json.dump(data, f)
                    f.flush()
                    try:
                        os.fsync(f.fileno())        # the rename must not outrun the data
                    except OSError:
                        pass                        # best-effort (some filesystems refuse fsync)
                if name in _load_failed:
                    # The file on disk could not be read, so `data` was built WITHOUT it.
                    # Never destroy it: a corrupt one was already copied aside by load();
                    # anything else is moved aside here (a rename needs no read access).
                    if _load_failed[name] != "corrupt" or name not in _preserved:
                        try:
                            if os.path.exists(path):
                                os.replace(path, f"{path}.unreadable-{_stamp()}")
                        except OSError as e:
                            print(f"[state] couldn't set aside unreadable {name!r}: {e}")
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
