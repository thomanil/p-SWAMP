"""The timeline app: a scrolling number sequence with playback controls.

An app package's public surface is exactly these three names, and src/server.py
uses nothing else:

  router      the endpoints, mounted by server.py under this app's /api/<app> prefix
  lifespan    optional; startup/shutdown work (here: the playback ticker task)
  WS_MESSAGE  optional; the model this app pushes down its socket

`WS_MESSAGE` is how a package joins the published api contract. api_contract.py
walks the same APPS registry server.py mounts and collects whatever each package
exports under that name, so the socket half of the api is described without a
second registry to keep in step — the same trick server.py already plays with
`lifespan`. An app with no socket simply omits it.

Keeping the surface that small is what lets server.py stay a plain registry —
copy this file when adding an app package.
"""

from .api import TimelineState, lifespan, router

WS_MESSAGE = TimelineState

__all__ = ["WS_MESSAGE", "TimelineState", "lifespan", "router"]
