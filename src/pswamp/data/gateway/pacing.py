# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Replay pacing: play a history stream at wall-clock speed, with controls.

STEP3 §5.4. ``consume`` is unpaced — an archive yields as fast as it reads. A
monitor wants the recording at ×1 (or ×N), with pause, step and speed. Rather
than teach every client about clocks, pacing is one wrapper over *any* async
iterator of timestamped payloads, so a replay of a file, an archive and a broker
paces identically.

How it paces: on the first payload it emits, the pacer anchors that payload's
data time to the wall clock. Every later payload is released when
``(t - t_anchor) / speed`` of wall time has passed. Pause, resume, a speed change
and a step all re-anchor, so no control ever causes a catch-up burst — the
pre-fill burst the port document §4.4 fought is structurally impossible here.

What the pacer does *not* do is seek: a seek is a new ``consume`` (see
:mod:`.replay`). It does offer :meth:`~Pacer.interrupt`, which is how a seek
tells a pacer that is mid-wait to discard the payload it is holding. Note the
interrupt takes effect at the pacer's next wait; a pacer blocked inside the
underlying stream waiting for *live* data notices it when that payload arrives.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from datetime import datetime
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from ..models.base import DataModel

__all__ = ["Pacer", "PacerInterrupted"]


class PacerInterrupted(Exception):
    """Raised from ``__anext__`` when :meth:`Pacer.interrupt` discarded the pending payload."""


class Pacer:
    """Async iterator that releases each payload on its timestamp, scaled by speed.

    Args:
        stream: The unpaced source. Its payloads must carry a ``timestamp``.
        speed: Replay speed factor; ``2.0`` plays twice as fast as recorded.
        paused: Start held. A held pacer emits only on :meth:`step`.
        clock: Monotonic clock, injectable for tests.
    """

    def __init__(
        self,
        stream: AsyncIterator[DataModel],
        *,
        speed: float = 1.0,
        paused: bool = False,
        clock: Callable[[], float] = time.monotonic,
    ):
        if speed <= 0:
            raise ValueError("speed must be positive")
        self._stream = stream
        self._speed = speed
        self._paused = paused
        self._clock = clock

        # (data time, wall time) the schedule is measured from; None = re-anchor
        # on the next emission.
        self._anchor: tuple[datetime, float] | None = None
        self._credits = 0
        self._interrupted = False
        self._wake = asyncio.Event()
        self._pending: DataModel | None = None

        #: Timestamp of the last payload emitted.
        self.position: datetime | None = None

    # --- controls, all called on the event loop ---------------------------

    @property
    def speed(self) -> float:
        return self._speed

    @property
    def paused(self) -> bool:
        return self._paused

    def pause(self) -> None:
        self._paused = True
        self._wake.set()

    def resume(self) -> None:
        self._paused = False
        self._anchor = None
        self._wake.set()

    def step(self, n: int = 1) -> None:
        """Release the next ``n`` payloads immediately, paused or not."""
        self._credits += n
        self._wake.set()

    def set_speed(self, speed: float) -> None:
        if speed <= 0:
            raise ValueError("speed must be positive")
        self._speed = speed
        self._anchor = None
        self._wake.set()

    def interrupt(self) -> None:
        """Make the pending ``__anext__`` raise :class:`PacerInterrupted`."""
        self._interrupted = True
        self._wake.set()

    async def aclose(self) -> None:
        closer = getattr(self._stream, "aclose", None)
        if closer is not None:
            await closer()

    # --- iteration ----------------------------------------------------------

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> DataModel:
        if self._pending is None:
            self._pending = await self._stream.__anext__()
        payload = self._pending

        while True:
            if self._interrupted:
                self._interrupted = False
                self._pending = None
                raise PacerInterrupted

            if self._credits > 0:
                self._credits -= 1
                self._anchor = None
                break

            if self._paused:
                await self._wait(None)
                continue

            delay = self._delay_for(payload.timestamp)
            if delay <= 0:
                break
            await self._wait(delay)

        self._pending = None
        self.position = payload.timestamp
        if self._anchor is None and payload.timestamp is not None:
            self._anchor = (payload.timestamp, self._clock())
        return payload

    def _delay_for(self, moment: datetime | None) -> float:
        if self._anchor is None or moment is None:
            return 0.0
        t_anchor, wall_anchor = self._anchor
        due = wall_anchor + (moment - t_anchor).total_seconds() / self._speed
        return due - self._clock()

    async def _wait(self, timeout: float | None) -> None:
        """Sleep until ``timeout`` elapses or a control wakes us, whichever first.

        The event is cleared right before waiting, and no ``await`` separates the
        state checks above from this clear, so a control issued on the loop can
        never slip between the two unseen.
        """
        self._wake.clear()
        try:
            await asyncio.wait_for(self._wake.wait(), timeout)
        except TimeoutError:
            pass
