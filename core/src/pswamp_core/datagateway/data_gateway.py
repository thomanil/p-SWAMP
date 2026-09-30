# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""
Entry point for reading and writing data across heterogeneous backends.

Lifted from the test_pswamp draft (``core/datagateway/data_gateway.py``). The
gateway owns a set of named :class:`~pswamp_core.datagateway.data_client_model.DataClient`
instances -- its **sources** -- and reads **one of them at a time**: the
*active* source. ``consume`` streams from it and ``coverage`` asks it what it
holds; ``switch(name)`` makes another source the active one. Nothing chooses
between sources on its own: a switch is always explicit (the player's
``SwitchSourceCommand``), so which provider a stream reads from is never a
question of coverage, priority or timing.

A source is either **history** (``HISTORY_CONSUME``: a recording, a store --
seekable, replayed) or **live** (``LIVE_CONSUME``: a feed, tailed from now);
``live`` says which the active one is. A client declaring both is refused when
the gateway is built. ``produce`` fans a payload out to every client that can
store it, independent of which source is active.

"Query a chunk" and "jump to a time" are the same call:
``gateway.consume(PmuFrame, t0, t1)`` and ``gateway.consume(PmuFrame, t0, None)``.

Adapted from the draft: its routing of one stream across several clients is gone
(no shipped app ever read one stream from two providers); ``produce`` raises
:class:`ProduceError` after fanning out when any client failed, instead of only
logging; loguru became ``logging``.
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

__all__ = ["DataGateway", "ProduceError"]

logger = get_logger("pswamp_core.datagateway.gateway")

#: The two ways a source can be read; a source declares exactly one.
_CONSUME = Capability.HISTORY_CONSUME | Capability.LIVE_CONSUME


class ProduceError(RuntimeError):
    """One or more clients failed to store a payload. ``failures`` names them."""

    def __init__(self, model: str, failures: dict[str, BaseException]) -> None:
        self.failures = failures
        detail = "; ".join(f"{name}: {error}" for name, error in failures.items())
        super().__init__(f"producing {model} failed on {len(failures)} client(s): {detail}")


class DataGateway:
    """
    A set of named sources, one of them active, and the writers beside them.

    Args:
        data_clients: Clients to register, in order. ``None`` or empty leaves
            the gateway without a source; ``consume`` then raises.
        active: The source read first. ``None`` takes the first client listed
            that can be read -- so a deployment's ``*_DATA_CLIENTS`` spec order
            decides the initial source.
        enrichers: Applied to every payload a stream yields, in order
            (:mod:`~pswamp_core.datagateway.enrich`: a ``cimReferenceId`` on
            PMU frames); opened and closed with the clients.

    Raises:
        ValueError: Two clients share a name; a client declares both consume
            capabilities; ``active`` names no source.
    """

    def __init__(
        self,
        data_clients: list[DataClient] | None = None,
        *,
        active: str | None = None,
        enrichers: Sequence[Enricher] = (),
    ):
        self.clients: dict[str, DataClient] = {}
        self.enrichers: tuple[Enricher, ...] = tuple(enrichers)
        #: The last failure of the active source's ``coverage`` call, cleared
        #: when it answers again. ``coverage`` returns ``None`` rather than
        #: raising, so this is where the *reason* survives for whoever has to
        #: tell a person (the player's error event).
        self.coverage_failure: str | None = None
        self._active: DataClient | None = None

        for client in data_clients or ():
            if client.name in self.clients:
                raise ValueError(f"Duplicate data client name {client.name!r}")
            if _CONSUME in client.capabilities:
                raise ValueError(
                    f"data client {client.name!r} declares both HISTORY_CONSUME and "
                    "LIVE_CONSUME; a source is one or the other"
                )
            self.clients[client.name] = client

        if active is not None:
            if active not in self.sources:
                raise ValueError(f"no source named {active!r}; the sources are {self.sources}")
            self._active = self.clients[active]
        elif self.sources:
            self._active = self.clients[self.sources[0]]

        if not self.clients:
            logger.warning("no data clients were provided; the gateway has no source")
        else:
            logger.info(
                "gateway over %s; active source: %s", ", ".join(self.clients), self.source
            )

    @property
    def sources(self) -> list[str]:
        """The names of the clients that can be read, in the order registered."""
        return [
            name for name, client in self.clients.items() if client.capabilities & _CONSUME
        ]

    @property
    def active(self) -> DataClient | None:
        """The source ``consume`` and ``coverage`` read."""
        return self._active

    @property
    def source(self) -> str | None:
        """The active source's name."""
        return None if self._active is None else self._active.name

    @property
    def live(self) -> bool:
        """Whether the active source is a live feed (else it is a history)."""
        return self._active is not None and Capability.LIVE_CONSUME in self._active.capabilities

    def switch(self, name: str) -> DataClient:
        """Make ``name`` the active source. An open stream is not touched: the
        caller (the player) closes it and opens a new one.

        Raises:
            ValueError: When ``name`` is not a source.
        """
        if name not in self.sources:
            raise ValueError(f"no source named {name!r}; the sources are {self.sources}")
        self._active = self.clients[name]
        self.coverage_failure = None
        logger.info("gateway switched to source %s", name)
        return self._active

    def consume(
        self,
        model: type[DataModel],
        start: datetime | None = None,
        end: datetime | None = None,
        mRID: MRIDFilter = None,
    ) -> DataStream:
        """
        Open a stream over ``model`` from the active source.

        Args:
            model: Model class to stream.
            start: Inclusive lower bound.
            end: Exclusive upper bound. ``None`` runs to the end of a history
                source's data, and tails a live one for ever.
            mRID: Optional identifier filter.

        Returns:
            An async iterable stream. Nothing is queried until it is iterated.

        Raises:
            RuntimeError: When the gateway has no source, or the active one
                does not serve ``model``.
        """
        client = self._require_active(model)
        return DataStream(client, model, TimeRange(start, end), mRID, self.enrichers)

    async def coverage(
        self,
        model: type[DataModel],
        mRID: MRIDFilter = None,
    ) -> Coverage | None:
        """
        What the active source holds for ``model``, asked afresh.

        A history's coverage is the seekable range and where a loop restarts;
        a live feed's starts about now and is open. A source that fails to
        answer is logged and recorded in ``coverage_failure``, and reads as
        holding nothing.

        Returns:
            The coverage, or ``None`` when the source holds nothing or failed.
        """
        client = self._active
        if client is None or not client.supports(model):
            return None
        try:
            coverage = await client.coverage(model, mRID)
        except Exception as error:
            logger.error(
                "source %s failed to report coverage for %s: %s", client.name, model.__name__, error
            )
            self.coverage_failure = f"{type(error).__name__}: {error}"
            return None
        self.coverage_failure = None
        return coverage

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
        if not self.clients:
            raise RuntimeError("DataGateway has no data clients")

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

    def _require_active(self, model: type[DataModel]) -> DataClient:
        if self._active is None:
            raise RuntimeError("DataGateway has no source to read")
        if not self._active.supports(model):
            raise RuntimeError(f"source {self._active.name!r} does not serve {model.__name__}")
        return self._active

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
