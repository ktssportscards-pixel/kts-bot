"""
Package entry point:  python -m botv2

IMPORTANT — do NOT start the bot with `python -m botv2.bot`. That loads bot.py as
the module `__main__`, while the rest of the package (webserver, commands, …) imports
it as `botv2.bot` — a SECOND, separate module object with its OWN, never-logged-in
discord Client. The gateway then runs on one client while the web server's delivery
posts call into the other, whose HTTP layer was never initialised
(`http._global_over` is MISSING → "'_MissingSentinel' object has no attribute
'is_set'"). Running `python -m botv2` loads bot.py exactly once, as `botv2.bot`, so
the gateway and the web server share a single client.
"""
from botv2.bot import run

run()
