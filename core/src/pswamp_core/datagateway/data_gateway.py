# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""
The providers of one pipeline, behind two reads.

A gateway holds **at most one history provider and one live provider** for a
class of data, told apart by their declared capabilities::

    gateway.consume(PmuFrame, t0, t1)   # history: exactly [t0, t1)
    gateway.consume(PmuFrame, t0)       # history: from t0 to the end of what it holds
    gateway.tail(PmuFrame)              # live: from now, open-ended

"Query a chunk" and "jump to a time" are the same call, which is why seek and
range query are *provider* capabilities, not bus features. Which read a stream
is, history or live, is always the caller's choice: nothing hands a replay
over to live on its own. ``coverage()`` is what the history provider holds.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING

from ..log import get_logger
from ..util.time import utcnow
from .data_client_model import Capability, DataClient
from .stream import DataStream
from .time_range import Coverage, TimeRange

if TYPE_CHECKING:
    from types import TracebackType

    from ..messages.data_model import DataModel
    from .data_client_model import MRIDFilter
    from .enrich import Enricher

__all__ = ["DataGateway"]

logger = get_logger("pswamp_core.datagateway.gateway")

#: The two roles a provider plays; a gateway has at most one of each per model.
_ROLES = (Capability.HISTORY_CONSUME, Capability.LIVE_CONSUME)


class DataGateway:
    """
    A pipeline's providers: at most one for history and one for live.

    Args:
        data_clients: The providers. ``None`` or empty disables the gateway,
            which then rejects any read.
        enrichers: Applied to every payload a stream yields, in order
            (:mod:`~pswamp_core.datagateway.enrich`: a ``cimReferenceId`` on
            PMU frames); opened and closed with the clients.

    Raises:
        ValueError: When two providers share a name, or declare the same role
            for an overlapping model: a read has exactly one provider.
    """

    def __init__(
        self,
        data_clients: list[DataClient] | None = None,
        *,
        enrichers: Sequence[Enricher] = (),
    ):
        self.clients: dict[str, DataClient] = {}
        self.enrichers: tuple[Enricher, ...] = tuple(enrichers)
        #: The last failure of the history provider's ``coverage`` call, by
        #: client name, cleared when it answers again. ``coverage`` returns
        #: ``None`` rather than raising, so this is where the *reason* survives
        #: for whoever has to tell a person (the player's error event).
        self.coverage_failures: dict[str, str] = {}

        if not data_clients:
            logger.warning("no data clients were provided; the gateway is disabled")
            return

        for client in data_clients:
            if client.name in self.clients:
                raise ValueError(f"Duplicate data client name {client.name!r}")
            for other in self.clients.values():
                _refuse_a_second_provider(other, client)
            self.clients[client.name] = client

        logger.info(
            "gateway initialised with %s client(s): %s",
            len(self.clients),
            ", ".join(self.clients),
        )

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
        Read history: ``[start, end)`` from the history provider.

        Args:
            model: Model class to stream.
            start: Inclusive lower bound, or ``None`` for the earliest it holds.
            end: Exclusive upper bound, or ``None`` for the latest it holds.
            mRID: Optional identifier filter.

        Returns:
            An async iterable stream. Nothing is read until it is iterated.

        Raises:
            RuntimeError: When no provider serves ``model``'s history.
        """
        client = self._provider(model, Capability.HISTORY_CONSUME)
        return DataStream(client, model, TimeRange(start, end), mRID, self.enrichers)

    def tail(self, model: type[DataModel], mRID: MRIDFilter = None) -> DataStream:
        """
        Read live: from now, open-ended, from the live provider.

        Raises:
            RuntimeError: When no provider serves ``model`` live.
        """
        client = self._provider(model, Capability.LIVE_CONSUME)
        return DataStream(client, model, TimeRange(utcnow(), None), mRID, self.enrichers, live=True)

    async def coverage(self, model: type[DataModel], mRID: MRIDFilter = None) -> Coverage | None:
        """
        What the history provider holds for ``model``, now: the range a replay
        seeks in and loops over.

        Returns:
            Its coverage, or ``None`` when there is no history provider, it
            holds nothing, or its ``coverage`` call failed (the reason is kept
            in ``coverage_failures``).
        """
        client = self._find(model, Capability.HISTORY_CONSUME)
        if client is None:
            return None
        try:
            coverage = await client.coverage(model, mRID)
        except Exception as error:
            logger.error(
                "client %s failed to report coverage for %s: %s", client.name, model.__name__, error
            )
            self.coverage_failures[client.name] = f"{type(error).__name__}: {error}"
            return None
        self.coverage_failures.pop(client.name, None)
        return coverage

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


    def _find(self, model: type[DataModel], capability: Capability) -> DataClient | None:
        """The provider of ``capability`` for ``model``; there is at most one."""
        return next((c for c in self.clients.values() if c.supports(model, capability)), None)

    def _provider(self, model: type[DataModel], capability: Capability) -> DataClient:
        client = self._find(model, capability)
        if client is None:
            raise RuntimeError(f"no provider serves {model.__name__} for {capability.name}")
        return client


def _refuse_a_second_provider(first: DataClient, second: DataClient) -> None:
    """Raise if both declare the same role for an overlapping model."""
    for role in _ROLES:
        if role not in first.capabilities or role not in second.capabilities:
            continue
        for a in first.supported_models:
            for b in second.supported_models:
                if issubclass(a, b) or issubclass(b, a):
                    raise ValueError(
                        f"{first.name} and {second.name} both provide {role.name} for {a.__name__}"
                    )
