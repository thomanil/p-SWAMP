# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""
One async stream over one data client's window.

Lifted from the test_pswamp draft (``core/datagateway/stream.py``), down to
what is left once a stream reads a single client: the draft's loop that moved
a stream from one client to the next is gone. What the stream still
adds over the client's own iterator is the contract made safe for everyone
downstream -- a payload without a timestamp is dropped, nothing at or past the
requested end is yielded, every payload passes through the gateway's enrichers,
and closing the stream closes the client's iterator.

"Seek" and "loop" are a *new* stream (the player opens one); a stream only ever
moves forward.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING

from ..log import get_logger

if TYPE_CHECKING:
    from types import TracebackType

    from ..messages.data_model import DataModel
    from .data_client_model import DataClient, MRIDFilter
    from .enrich import Enricher
    from .time_range import TimeRange

__all__ = ["DataStream"]

logger = get_logger("pswamp_core.datagateway.stream")


class DataStream:
    """
    Async iterator over a model's payloads from one client, across a window.

    Args:
        client: The source read.
        model: Model class being streamed.
        request: Window requested by the caller. An open end runs to the end of
            a history's data, or tails a live source for ever.
        mRID: Optional identifier filter.
        enrichers: Applied to every payload, in order, just before it is
            yielded, so every reader sees the same.

    Examples:
        >>> async with gateway.consume(PmuFrame, start=yesterday) as stream:
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
    ):
        self.client = client
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
        """Stop the stream and release the client's iterator."""
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
        iterator = self.client.consume(self._model, self._request, self._mRID)
        try:
            async for payload in iterator:
                moment = payload.timestamp
                if moment is None:
                    logger.warning(
                        "dropping %s payload without timestamp from %s",
                        self._model.__name__,
                        self.client.name,
                    )
                    continue
                if self._request.end is not None and moment >= self._request.end:
                    return
                for enricher in self._enrichers:
                    payload = enricher.enrich(payload)
                yield payload
        finally:
            closer = getattr(iterator, "aclose", None)
            if closer is not None:
                await closer()
