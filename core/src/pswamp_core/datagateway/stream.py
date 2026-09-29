# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""
One read from one provider, as an async stream.

A ``DataStream`` is what ``DataGateway.consume`` and ``DataGateway.tail``
return: one provider's iterator over one window. Nothing is read until it is
iterated, and closing the stream closes the provider's iterator.

A history read with an open bound is bounded by what the provider holds when
the stream starts: its coverage fills in the missing start or end. So a replay
ends where the recording ends, and only a live read runs on.

A stream never goes backwards, which is why "seek" and "loop" are always a
*new* stream.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING

from .time_range import TimeRange

if TYPE_CHECKING:
    from types import TracebackType

    from ..messages.data_model import DataModel
    from .data_client_model import DataClient, MRIDFilter
    from .enrich import Enricher

__all__ = ["DataStream"]


class DataStream:
    """
    Async iterator over one provider's payloads in one window.

    Args:
        client: The provider read.
        model: Model class being streamed.
        request: The window asked for.
        mRID: Optional identifier filter.
        enrichers: Applied to every payload, in order, just before it is
            yielded, so every reader sees the same.
        live: A live read (``tail``): its end stays open. A history read takes
            any open bound from the provider's coverage.

    Examples:
        >>> async with gateway.consume(PmuFrame, start=t0, end=t1) as stream:
        ...     async for frame in stream:
        ...         handle(frame)
    """

    def __init__(
        self,
        client: DataClient,
        model: type[DataModel],
        request: TimeRange,
        mRID: MRIDFilter = None,
        enrichers: Sequence[Enricher] = (),
        *,
        live: bool = False,
    ):
        self.client = client
        self.live = live
        self._model = model
        self._request = request
        self._mRID = mRID
        self._enrichers = tuple(enrichers)

        self._generator: AsyncIterator[DataModel] | None = None

    @property
    def request(self) -> TimeRange:
        """The window this stream was opened over."""
        return self._request

    def __aiter__(self) -> "DataStream":
        if self._generator is None:
            self._generator = self._iterate()

        return self

    async def __anext__(self) -> DataModel:
        if self._generator is None:
            self._generator = self._iterate()

        return await self._generator.__anext__()

    async def aclose(self) -> None:
        """Stop the stream and release the provider's iterator."""
        if self._generator is None:
            return

        generator, self._generator = self._generator, None
        await generator.aclose()

    async def __aenter__(self) -> "DataStream":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def _iterate(self) -> AsyncIterator[DataModel]:
        window = self._request if self.live else await self._bounded()
        if window is None:
            return

        iterator = self.client.consume(self._model, window, self._mRID)
        try:
            async for payload in iterator:
                for enricher in self._enrichers:
                    payload = enricher.enrich(payload)
                yield payload
        finally:
            await _aclose(iterator)

    async def _bounded(self) -> TimeRange | None:
        """The request, any open bound taken from the provider's coverage;
        ``None`` when there is nothing to read."""
        start, end = self._request.start, self._request.end
        if start is None or end is None:
            coverage = await self.client.coverage(self._model, self._mRID)
            if coverage is None:
                return None
            start = coverage.range.start if start is None else start
            end = coverage.range.end if end is None else end
        if start is not None and end is not None and end <= start:
            return None
        return TimeRange(start, end)


async def _aclose(iterator: AsyncIterator[DataModel]) -> None:
    """Close a provider iterator when it supports it."""
    closer = getattr(iterator, "aclose", None)

    if closer is None:
        return

    await closer()
