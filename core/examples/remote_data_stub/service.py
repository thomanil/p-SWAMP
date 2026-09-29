# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""``QueryService``: a query becomes the run of NDJSON lines the contract
prescribes, as an async generator the REST route streams. No HTTP here -- that
is ``app.py``, the layer around it."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from pswamp_core.log import get_logger
from pswamp_core.messages import RemoteDataQuery, RemoteDataResult

from .recording import TiledRecording

__all__ = ["QueryService"]

logger = get_logger("remote-data-stub")

#: Hand the event loop back every this many records. Records come out of memory
#: here, and a write to an unpaused socket does not suspend either, so without
#: it one long query could run a whole range without letting another in.
YIELD_EVERY = 50


class QueryService:
    """Answers the two questions the REST routes ask, over one recording.

    ``stream(query)`` yields the answer line by line: ``record`` lines in time
    order, then one ``end`` carrying the count -- or one ``error`` if anything
    went wrong part way. It is only ever pulled as fast as the connection
    drains, so a client that reads slowly (a replay at 1x) paces the "store
    read" too. A client that goes away closes the generator where it stands,
    which is the whole of cancelling; it is logged, and nothing else follows.
    """

    def __init__(self, recording: TiledRecording) -> None:
        self.recording = recording
        #: Queries answered to their ``end`` line; the tests count them.
        self.completed = 0
        #: Answers being streamed right now.
        self.streaming = 0

    # -- the questions ---------------------------------------------------------

    def coverage(self, topic: str) -> dict[str, object] | None:
        """The coverage document for ``topic``, or ``None`` when unknown."""
        held = self.recording.coverage(topic)
        if held is None:
            return None
        start, end = held
        return {"model": topic, "start": start, "end": end}

    def knows(self, topic: str) -> bool:
        return self.recording.knows(topic)

    # -- answering -------------------------------------------------------------

    async def stream(self, query: RemoteDataQuery) -> AsyncIterator[bytes]:
        sent = 0
        finished = False
        self.streaming += 1
        logger.info(
            "query %s: %s [%s, %s)", query.query_id, query.model,
            query.start.isoformat() if query.start else "-", query.end.isoformat() if query.end else "-",
        )
        try:
            try:
                for record in self.recording.select(query.model, query.start, query.end, query.mrid):
                    yield RemoteDataResult.for_record(record).to_line()
                    sent += 1
                    if sent % YIELD_EVERY == 0:
                        await asyncio.sleep(0)
            except Exception as error:
                # Past the first byte the status code is spent, so a failure is
                # a line. (A disconnect is GeneratorExit or CancelledError, which
                # are not Exceptions, and land in the finally below instead.)
                reason = f"{type(error).__name__}: {error}"
                logger.warning("query %s: failed after %d record(s): %s", query.query_id, sent, reason)
                finished = True
                yield RemoteDataResult.failed(reason).to_line()
                return
            finished = True
            self.completed += 1
            logger.info("query %s: sent %d record(s)", query.query_id, sent)
            yield RemoteDataResult.ended(sent).to_line()
        finally:
            self.streaming -= 1
            if not finished:
                logger.info("query %s: client went away after %d record(s)", query.query_id, sent)
