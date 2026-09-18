# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""``QueryService``: an accepted query becomes the run of envelopes the contract
prescribes, published to a sink. No HTTP, no Kafka -- those are the layers
around it (``app.py``, ``kafka_sink.py``)."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Protocol

from pswamp_core.log import get_logger
from pswamp_core.messages import TimeSeriesQuery, TimeSeriesResult
from pswamp_core.util.time import utcnow

from .recording import TiledRecording

__all__ = ["QueryService", "Sink"]

logger = get_logger("time-series-stub")


class Sink(Protocol):
    """Where the envelopes go. ``KafkaSink`` in the container; a list, or the
    client's ``InMemoryResultFeed``, in a test."""

    async def publish(self, result: TimeSeriesResult) -> None: ...


class QueryService:
    """Answers the three questions the REST routes ask, over one recording.

    A query runs as its own task: ``record`` envelopes in time order with
    ``seq`` from 0, then one ``end`` carrying the count -- or one ``error`` if
    anything went wrong, including a model the recording does not hold. A
    cancelled query stops where it is and publishes nothing more, which is what
    the client asked for by cancelling. The record key on the topic is the
    ``query_id``, which the sink takes from the envelope.
    """

    def __init__(self, recording: TiledRecording, sink: Sink) -> None:
        self.recording = recording
        self.sink = sink
        self._running: dict[str, asyncio.Task] = {}
        self.completed = 0

    # -- the questions ---------------------------------------------------------

    def coverage(self, topic: str) -> dict[str, object] | None:
        """The coverage document for ``topic``, or ``None`` when unknown."""
        held = self.recording.coverage(topic)
        if held is None:
            return None
        start, end = held
        return {"model": topic, "start": start, "end": end, "live": False}

    def running(self, query_id: str) -> bool:
        task = self._running.get(query_id)
        return task is not None and not task.done()

    def start_query(self, query: TimeSeriesQuery) -> asyncio.Task:
        """Accept ``query`` and start answering it. ``KeyError`` if that id is running."""
        if self.running(query.query_id):
            raise KeyError(query.query_id)
        task = asyncio.create_task(self._answer(query), name=f"query.{query.query_id}")
        self._running[query.query_id] = task
        task.add_done_callback(lambda _t: self._running.pop(query.query_id, None))
        logger.info(
            "query %s: %s [%s, %s)", query.query_id, query.model,
            query.start.isoformat() if query.start else "-", query.end.isoformat() if query.end else "-",
        )
        return task

    def cancel(self, query_id: str) -> bool:
        """Stop a running query. ``True`` if there was one to stop."""
        # Popped here rather than left to the done-callback: the task is not
        # ``done()`` until the loop runs it, and a second cancel must find nothing.
        task = self._running.pop(query_id, None)
        if task is None or task.done():
            return False
        task.cancel()
        logger.info("query %s: cancelled", query_id)
        return True

    async def shutdown(self) -> None:
        tasks = list(self._running.values())
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    # -- answering -------------------------------------------------------------

    async def _answer(self, query: TimeSeriesQuery) -> None:
        seq = 0
        try:
            records = self.recording.select(query.model, query.start, query.end, query.mrid)
            for record in records:
                await self.sink.publish(
                    TimeSeriesResult.for_record(query.query_id, seq, record, timestamp=utcnow())
                )
                seq += 1
            await self.sink.publish(
                TimeSeriesResult.ended(query.query_id, seq, seq, timestamp=utcnow())
            )
            self.completed += 1
            logger.info("query %s: sent %d record(s)", query.query_id, seq)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            reason = f"{type(error).__name__}: {error}" if not isinstance(error, KeyError) else (
                f"unknown model {error.args[0]!r}"
            )
            logger.warning("query %s: failed: %s", query.query_id, reason)
            with contextlib.suppress(Exception):
                await self.sink.publish(
                    TimeSeriesResult.failed(query.query_id, seq, reason, timestamp=utcnow())
                )
