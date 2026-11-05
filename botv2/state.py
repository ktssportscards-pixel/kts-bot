"""
state — tiny, robust JSON persistence for botv2.

Every store is a named JSON file under DATA_DIR. Writes are atomic (temp file +
os.replace) and lock-guarded, so a crash or a concurrent write can never leave a
half-written store (v1 hit a corrupted vip_store once — this prevents that class
of bug). In prod, point DATA_DIR at a mounted volume so state survives redeploys;
without one, stores are ephemeral and callers must degrade gracefully (v1's
redeploy-recovery lessons are preserved in each consumer).
"""
import json
import os
import threading

from . import config

_LOCK = threading.Lock()


def _path(name: str) -> str:
    return os.path.join(config.DATA_DIR, f"{name}.json")


def load(name: str, default):
    """Load a store, or return `default` (a value, or a 0-arg factory) on any
    error — a missing/corrupt store never crashes the bot."""
    try:
        with open(_path(name)) as f:
            return json.load(f)
    except Exception:
        return default() if callable(default) else default


def save(name: str, data) -> bool:
    """Atomically persist a store. Returns True on success; logs and returns
    False on failure (persistence is best-effort, never fatal to a live quote)."""
    try:
        os.makedirs(config.DATA_DIR, exist_ok=True)
        tmp = _path(name) + ".tmp"
        with _LOCK:
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, _path(name))
        return True
    except Exception as e:
        print(f"[state] save {name!r} failed (non-fatal): {e}")
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
