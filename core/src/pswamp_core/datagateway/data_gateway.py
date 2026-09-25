# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""
Entry point for reading and writing data across heterogeneous backends.

Lifted from the test_pswamp draft (``core/datagateway/data_gateway.py``). The
gateway owns a set of :class:`~pswamp_core.datagateway.data_client_model.DataClient`
instances and hides them behind two calls: ``consume``, which returns a single
stream stitched across whichever clients hold the requested window, and
``produce``, which fans a payload out to every writable client.

"Query a chunk" and "jump to a time" are the same call here --
``gateway.consume(PmuFrame, t0, t1)`` and ``gateway.consume(PmuFrame, t0, None)``
-- which is why STEP 1 A5's seek and range query are *provider* capabilities and
not bus features.

Adapted from the draft: ``coverage()`` (the union of what the clients hold,
filtered by declared capability -- the player reads the ``HISTORY_CONSUME`` union
for its seekable range and where a loop restarts) and ``supports()``; ``produce`` raises
:class:`ProduceError` after fanning out when any client failed, instead of only
logging -- an archive that silently stops is a history gap discovered weeks
later (STEP 2 §4); ``typing.Self`` became string annotations; loguru became
``logging``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from ..log import get_logger
from ..util.time import utcnow
from .data_client_model import Capability, DataClient, can_consume
from .planner import (
    DEFAULT_LIVE_HANDOFF_MARGIN,
    GapPolicy,
    SegmentPlanner,
)
from .stream import DataStream
from .time_range import Coverage, TimeRange

if TYPE_CHECKING:
    from types import TracebackType

    from ..messages.data_model import DataModel
    from .data_client_model import MRIDFilter
    from .enrich import Enricher

__all__ = ["DataGateway", "ProduceError"]

logger = get_logger("pswamp_core.datagateway.gateway")


class ProduceError(RuntimeError):
    """One or more clients failed to store a payload. ``failures`` names them."""

    def __init__(self, model: str, failures: dict[str, BaseException]) -> None:
        self.failures = failures
        detail = "; ".join(f"{name}: {error}" for name, error in failures.items())
        super().__init__(f"producing {model} failed on {len(failures)} client(s): {detail}")


class DataGateway:
    """
    Routes reads and writes across the registered data clients.

    Args:
        data_clients: Clients to register. ``None`` or empty disables the
            gateway, which then rejects any call.
        on_gap: How to handle stretches of the requested window that no client
            covers.
        live_handoff_margin: How close to ``now`` a replay must get before the
            gateway switches to a live-capable client.
        enrichers: Applied to every payload a stream yields, in order
            (:mod:`~pswamp_core.datagateway.enrich`: a ``cimReferenceId`` on
            PMU frames); opened and closed with the clients.

    Raises:
        ValueError: When two clients share the same name.
    """

    def __init__(
        self,
        data_clients: list[DataClient] | None = None,
        *,
        on_gap: GapPolicy = "skip",
        live_handoff_margin: timedelta = DEFAULT_LIVE_HANDOFF_MARGIN,
        enrichers: Sequence[Enricher] = (),
    ):
        self.clients: dict[str, DataClient] = {}
        self.enrichers: tuple[Enricher, ...] = tuple(enrichers)
        #: The last failure of each client's ``coverage`` call, by client name,
        #: cleared when it answers again. ``coverage`` below skips a failing
        #: client rather than raising, so this is where the *reason* survives
        #: for whoever has to tell a person (the player's error event).
        self.coverage_failures: dict[str, str] = {}
        self._planner: SegmentPlanner | None = None

        if not data_clients:
            logger.warning("no data clients were provided; the gateway is disabled")
            return

        for client in data_clients:
            if client.name in self.clients:
                raise ValueError(f"Duplicate data client name {client.name!r}")

            self.clients[client.name] = client

        self._planner = SegmentPlanner(
            list(self.clients.values()),
            on_gap=on_gap,
            live_handoff_margin=live_handoff_margin,
        )

        logger.info(
            "gateway initialised with %s client(s): %s",
            len(self.clients),
            ", ".join(self.clients),
        )

    @property
    def enabled(self) -> bool:
        """Whether any client is registered."""
        return self._planner is not None

    def supports(
        self,
        model: type[DataModel],
        capability: Capability | None = None,
    ) -> bool:
        """Whether any client handles ``model``, optionally for ``capability``.

        A synchronous question about what is *declared*, not what is held
        right now -- ``coverage`` answers the latter. A player asks this to
        know whether ``live`` is a control worth offering at all.
        """
        return any(client.supports(model, capability) for client in self.clients.values())

    def consume(
        self,
        model: type[DataModel],
        start: datetime | None = None,
        end: datetime | None = None,
        mRID: MRIDFilter = None,
    ) -> DataStream:
        """
        Open a stream over ``model`` between ``start`` and ``end``.

        Args:
            model: Model class to stream.
            start: Inclusive lower bound, or ``None`` to start at the earliest
                data any client holds.
            end: Exclusive upper bound. ``None`` tails live forever; a value in
                the future tails until that instant is reached.
            mRID: Optional identifier filter.

        Returns:
            An async iterable stream. Nothing is queried until it is iterated.

        Raises:
            RuntimeError: When the gateway has no clients.
        """
        if self._planner is None:
            raise RuntimeError("DataGateway is disabled: no data clients registered")

        return DataStream(self._planner, model, TimeRange(start, end), mRID, self.enrichers)

    async def coverage(
        self,
        model: type[DataModel],
        mRID: MRIDFilter = None,
        *,
        capability: Capability | None = None,
    ) -> Coverage | None:
        """
        The union of what every *consuming* client holds for ``model``, now.

        Earliest start, latest end (``None`` if any client is unbounded that
        way), and ``live`` if any client can follow live data. This is what a
        player reads to know where a loop restarts and what can be sought; a
        stream is still planned segment by segment.

        Args:
            model: The message class.
            mRID: Optional identifier filter.
            capability: Restrict the union to clients declaring this
                capability -- ``HISTORY_CONSUME`` gives the seekable range,
                ``LIVE_CONSUME`` the tailable one. ``None`` takes every client
                that can consume at all; a produce-only client never
                contributes coverage, since nothing can be read from it.

        Returns:
            The combined coverage, or ``None`` when no client holds anything.
        """
        candidates = [
            client
            for client in self.clients.values()
            if can_consume(client, model, capability)
        ]
        if not candidates:
            return None

        results = await asyncio.gather(
            *(client.coverage(model, mRID) for client in candidates),
            return_exceptions=True,
        )

        found: list[Coverage] = []
        for client, result in zip(candidates, results, strict=True):
            if isinstance(result, BaseException):
                logger.error(
                    "client %s failed to report coverage for %s: %s",
                    client.name,
                    model.__name__,
                    result,
                )
                self.coverage_failures[client.name] = f"{type(result).__name__}: {result}"
                continue
            self.coverage_failures.pop(client.name, None)
            if result is not None:
                found.append(result)

        if not found:
            return None

        starts = [coverage.range.start for coverage in found]
        ends = [coverage.range.end for coverage in found]
        return Coverage(
            range=TimeRange(
                None if any(start is None for start in starts) else min(starts),
                None if any(end is None for end in ends) else max(ends),
            ),
            live=any(coverage.live for coverage in found),
        )

    async def produce(self, data: DataModel) -> None:
        """
        Write a payload to every client able to store its model.

        Args:
            data: Payload to store. Its ``timestamp`` is stamped with the
                current time when left unset.

        Raises:
            RuntimeError: When the gateway has no clients.
            ProduceError: When any target client failed; the others still wrote.
        """
        if self._planner is None:
            raise RuntimeError("DataGateway is disabled: no data clients registered")

        if data.timestamp is None:
            data.timestamp = utcnow()

        model = type(data)
        targets = [
            client
            for client in self.clients.values()
            if client.supports(model, Capability.PRODUCE)
        ]

        if not targets:
            logger.warning("no data client can produce %s", model.__name__)
            return

        results = await asyncio.gather(
            *(client.produce(data) for client in targets),
            return_exceptions=True,
        )

        failures = {
            client.name: result
            for client, result in zip(targets, results, strict=True)
            if isinstance(result, BaseException)
        }
        if failures:
            for name, error in failures.items():
                logger.error("client %s failed to produce %s: %s", name, model.__name__, error)
            raise ProduceError(model.__name__, failures)

    async def open(self) -> None:
        """Open every registered client, then the enrichers."""
        await self._lifecycle("open")
        for enricher in self.enrichers:
            await enricher.open()

    async def close(self) -> None:
        """Close the enrichers, then every registered client."""
        for enricher in self.enrichers:
            try:
                await enricher.close()
            except Exception as error:
                logger.error("enricher %s failed to close: %s", enricher.name, error)
        await self._lifecycle("close")

    async def __aenter__(self) -> "DataGateway":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def _lifecycle(self, action: str) -> None:
        """Run a lifecycle hook on all clients, logging individual failures."""
        if not self.clients:
            return

        clients = list(self.clients.values())
        results = await asyncio.gather(
            *(getattr(client, action)() for client in clients),
            return_exceptions=True,
        )

        for client, result in zip(clients, results, strict=True):
            if isinstance(result, BaseException):
                logger.error("client %s failed to %s: %s", client.name, action, result)
