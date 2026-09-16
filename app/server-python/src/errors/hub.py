# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""``ErrorHub``: the process's one fan-out of error notices, per client id.

The core bus is per pipeline, and a client may have several pipelines (one per
app they have opened). The hub is what sits above them: any pipeline's
forwarder publishes here with its client id and app slug, and every layout
socket that client has open receives it. It also keeps the last few notices
per client, so a socket connecting *after* a failure (a page reload) still
shows it -- in memory, bounded, nothing persisted.

Every notice is also a log line, so the server log stays the source of truth
and this is the copy addressed to the person.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from collections.abc import Iterator
from uuid import uuid4

from pswamp_core.messages import ErrorEvent
from pswamp_web.log import get_logger
from pswamp_web.sessions import SessionRegistry

from .wire import ErrorNotice

__all__ = ["HUB", "ErrorHub"]

logger = get_logger("errors")

#: How many recent notices to keep per client, for replay on connect.
RECENT = 20


class ErrorHub:
    def __init__(self, recent: int = RECENT) -> None:
        self._recent: dict[str, deque[ErrorNotice]] = {}
        self._keep = recent
        # The open layout sockets, as the queues their push tasks wait on.
        self._watching: SessionRegistry[asyncio.Queue[ErrorNotice]] = SessionRegistry()

    def publish(self, client_id: str, app: str, event: ErrorEvent) -> ErrorNotice:
        """Turn ``event`` into a notice for ``client_id``, log it, keep it, fan it out."""
        notice = ErrorNotice(
            id=uuid4().hex,
            app=app,
            source=event.source,
            message=event.message,
            detail=event.detail,
            request_id=event.request_id,
            timestamp=event.timestamp,
        )
        logger.error(
            "client %s: %s/%s: %s%s%s",
            client_id, app, event.source, event.message,
            f" -- {event.detail}" if event.detail else "",
            f" (request {event.request_id})" if event.request_id else "",
        )
        ring = self._recent.get(client_id)
        if ring is None:
            ring = self._recent[client_id] = deque(maxlen=self._keep)
        ring.append(notice)
        for queue in self._watching.of(client_id):
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(notice)
        return notice

    def recent(self, client_id: str) -> list[ErrorNotice]:
        """What this client was told lately, oldest first."""
        return list(self._recent.get(client_id, ()))

    def forget(self, client_id: str) -> None:
        self._recent.pop(client_id, None)

    @contextlib.contextmanager
    def watching(self, client_id: str) -> Iterator[asyncio.Queue[ErrorNotice]]:
        """A queue fed with this client's notices for as long as the socket lives."""
        queue: asyncio.Queue[ErrorNotice] = asyncio.Queue(maxsize=64)
        with self._watching.registered(client_id, queue):
            yield queue

    def watchers(self, client_id: str) -> int:
        return len(self._watching.of(client_id))


#: The process's hub. Every forwarder publishes here; the errors socket reads here.
HUB = ErrorHub()
