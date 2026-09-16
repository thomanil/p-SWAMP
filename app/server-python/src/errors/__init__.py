"""The errors app: one socket per client carrying the operational errors of
every pipeline that client has, for the web layout's error tray.

Same public surface as every app package — src/server.py uses nothing else:

  router      GET /api/errors/ws, downstream only; no commands
  WS_MESSAGE  ErrorNotice, the model pushed down that socket

What makes it different from the other apps: it owns no pipeline. Every app
that builds a core pipeline appends an ``ErrorForwarderModule`` to its module
list (imported from ``shared``), which copies each ``ErrorEvent`` on that
pipeline's bus into ``HUB`` -- tagged with the app's slug -- and ``HUB`` fans
it out to whatever sockets that client has open here. So a failure in the
streamer's replay reaches the person while they are looking at any page.

``hub.py`` and ``forwarder.py`` import nothing from ``shared``, on purpose:
``shared`` re-exports them, and an app package that both feeds ``shared`` and
reads from it would be an import cycle. The socket in ``api.py`` therefore
reads the web-layer helpers from ``pswamp_web`` directly -- importing inward,
which is what ``shared`` does too.
"""

from .api import router
from .forwarder import ErrorForwarderModule
from .hub import HUB, ErrorHub
from .wire import ErrorNotice

WS_MESSAGE = ErrorNotice

__all__ = ["HUB", "WS_MESSAGE", "ErrorForwarderModule", "ErrorHub", "ErrorNotice", "router"]
