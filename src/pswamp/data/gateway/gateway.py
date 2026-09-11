# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.
# Lifted from Louis Pauchet's data-gateway draft (SINTEF, 2026), see STEP3 §13;
# only the logging changed (loguru -> stdlib).

"""Entry point for reading and writing data across heterogeneous backends.

The gateway owns a set of :class:`~pswamp.data.gateway.client.DataClient`
instances and hides them behind two calls: `consume`, which returns a single
stream stitched across whichever clients hold the requested window, and
`produce`, which fans a payload out to every writable client.

The one idea everything else is arranged around: **a consumer names a model and
a time range, never a source.** Which backend answers, and when the stream
switches from an archive to a live topic, is the planner's business.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Self

from ..time import utcnow
from .client import Capability, DataClient
from .planner import DEFAULT_LIVE_HANDOFF_MARGIN, GapPolicy, SegmentPlanner
from .stream import DataStream
from .time_range import TimeRange

if TYPE_CHECKING:
    from types import TracebackType

    from ..models.base import DataModel
    from .client import MRIDFilter

__all__ = ["DataGateway"]

logger = logging.getLogger("pswamp.data.gateway")


class DataGateway:
    """Routes reads and writes across the registered data clients.

    Args:
        data_clients: Clients to register. ``None`` or empty disables the
            gateway, which then rejects any call.
        on_gap: How to handle stretches of the requested window that no client
            covers.
        live_handoff_margin: How close to ``now`` a replay must get before the
            gateway switches to a live-capable client.

    Raises:
        ValueError: When two clients share the same name.
    """

    def __init__(
        self,
        data_clients: list[DataClient] | None = None,
        *,
        on_gap: GapPolicy = "skip",
        live_handoff_margin: timedelta = DEFAULT_LIVE_HANDOFF_MARGIN,
    ):
        self.clients: dict[str, DataClient] = {}
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

    def consume(
        self,
        model: type[DataModel],
        start: datetime | None = None,
        end: datetime | None = None,
        mRID: MRIDFilter = None,
    ) -> DataStream:
        """Open a stream over ``model`` between ``start`` and ``end``.

        ``start=None`` starts at the earliest data any client holds; ``end=None``
        tails live forever (a value in the future tails until it is reached).
        Nothing is queried until the stream is iterated.
        """
        if self._planner is None:
            raise RuntimeError("DataGateway is disabled: no data clients registered")

        return DataStream(self._planner, model, TimeRange(start, end), mRID)

    async def produce(self, data: DataModel) -> None:
        """Write a payload to every client able to store its model.

        Stamps ``timestamp`` with the current time when left unset.
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

        for client, result in zip(targets, results, strict=True):
            if isinstance(result, BaseException):
                logger.error(
                    "client %s failed to produce %s: %s",
                    client.name,
                    model.__name__,
                    result,
                )

    async def open(self) -> None:
        """Open every registered client."""
        await self._lifecycle("open")

    async def close(self) -> None:
        """Close every registered client."""
        await self._lifecycle("close")

    async def __aenter__(self) -> Self:
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
