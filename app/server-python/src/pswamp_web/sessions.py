# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Live per-connection view state, addressable by client id.

A socket handler naturally keeps its per-connection state in a local variable --
which channels this view has selected, what it last sent, the socket itself.
Commands arrive as HTTP requests, on no connection at all, so that state has to
be reachable from outside the handler that owns it. This is how.

**One structure, two uses.** A page package registers whatever a command needs to
reach: `time_window` its selection, `islanding` its wake-up queue, and the
scaffold apps under `src/` the WebSocket itself -- see `shared.SocketRegistry`,
which is this class with `T = WebSocket` and a `send_to_client` on top. Those were
two separate implementations of the same dict until they were merged; if you find
yourself writing a third, this is it.

Note what this is *not*: it is not where a client's data lives. That is the Hub
(see hub.py), which the registry there already addresses by client id. This holds
only what a particular open view is looking at.

Two properties worth being deliberate about:

* **A client may have several live sessions for one endpoint.** Two tabs, or a
  dashboard panel and its focused route overlapping for a moment during
  navigation. A command applies to all of them, which is also the right reading:
  one browser is one viewer, and its views should agree.
* **Registration is scoped to the connection**, via the context manager, so a
  session cannot outlive the socket it belongs to even on an exception path --
  and every endpoint here has one, since they all end in a bare ``except``.
"""

import contextlib
from collections.abc import Iterator
from typing import Generic, TypeVar

T = TypeVar("T")


class SessionRegistry(Generic[T]):
    """The open views of one endpoint, grouped by client id.

    Lives entirely on the event loop, like everything else on the socket side, so
    it needs no lock of its own.
    """

    def __init__(self) -> None:
        # A list rather than a set: the session objects are eq=True dataclasses,
        # which Python makes unhashable. Removal is therefore by identity.
        self._by_client: dict[str, list[T]] = {}

    @contextlib.contextmanager
    def registered(self, client_id: str, session: T) -> Iterator[T]:
        """Publish one connection's session for as long as that socket lives."""
        self._by_client.setdefault(client_id, []).append(session)
        try:
            yield session
        finally:
            sessions = self._by_client.get(client_id)
            if sessions is not None:
                for i, existing in enumerate(sessions):
                    if existing is session:
                        del sessions[i]
                        break
                if not sessions:
                    del self._by_client[client_id]

    def of(self, client_id: str) -> list[T]:
        """This client's open views, as a snapshot safe to iterate and mutate."""
        return list(self._by_client.get(client_id, ()))

    def clients(self) -> list[str]:
        """Every client with at least one view open. The *live* roster: a client
        whose sockets have all closed is gone from here, even where the app kept
        its state (which the scaffold apps deliberately do, so a reconnect
        resumes)."""
        return list(self._by_client)
