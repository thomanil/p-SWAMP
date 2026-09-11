# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A seekable, paceable cursor over ``gateway.consume``.

The piece STEP3 §7.3 and §8.3 call ``ReplayControl``, minus the module
re-priming (no module sits on this stream yet): one object that a per-client
replay owns and that its commands drive. The rules it embodies, decided once:

- **Pacing is a wrapper** (:class:`~.pacing.Pacer`) over whatever stream the
  gateway returns, so play / pause / step / speed never touch a client.
- **A seek is a new ``consume``.** The old stream is closed, a fresh one is opened
  at the target, and the pacer re-anchors — so there is never a burst of
  history and never a stale window. What a seek costs is one ``coverage``
  round-trip and one iterator; what it gives is that the same code seeks a
  file, an archive or a broker with retention.
- **Looping is a seek to the beginning.** A replay that is asked to loop
  reopens at the earliest data any client holds when the stream ends — and only
  if the pass produced something, so an empty source ends rather than spins.

Everything here runs on the event loop; the controls are plain methods a POST
handler calls, and the effect shows up on the next iteration.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import TYPE_CHECKING, Self

from .pacing import Pacer, PacerInterrupted

if TYPE_CHECKING:
    from ..models.base import DataModel
    from .client import MRIDFilter
    from .gateway import DataGateway

__all__ = ["Replay"]


class Replay:
    """One paced, seekable pass over ``model`` through ``gateway``.

    Args:
        gateway: Where the data comes from. Shared and read-only.
        model: The message type to replay.
        mRID: Optional identifier filter (a stream id).
        start: Where to begin; ``None`` is the earliest data any client holds.
        speed: Initial speed factor.
        paused: Start held; the first payload then arrives on the first step.
        loop: Reopen at the beginning when the source runs out.
    """

    def __init__(
        self,
        gateway: DataGateway,
        model: type[DataModel],
        *,
        mRID: MRIDFilter = None,
        start: datetime | None = None,
        speed: float = 1.0,
        paused: bool = False,
        loop: bool = False,
    ):
        self._gateway = gateway
        self._model = model
        self._mRID = mRID
        self._start = start
        self._speed = speed
        self._paused = paused
        self._loop = loop

        self._pacer: Pacer | None = None
        self._stale: list[Pacer] = []
        self._produced_in_pass = False

        #: Timestamp of the last payload emitted; ``None`` before the first.
        self.position: datetime | None = None
        #: How many times the source has been restarted from the beginning.
        self.passes = 0

    # --- state ----------------------------------------------------------------

    @property
    def speed(self) -> float:
        return self._speed

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def playing(self) -> bool:
        return not self._paused

    # --- controls, all synchronous and called on the loop ---------------------

    def play(self) -> None:
        self._paused = False
        if self._pacer is not None:
            self._pacer.resume()

    def pause(self) -> None:
        self._paused = True
        if self._pacer is not None:
            self._pacer.pause()

    def step(self, n: int = 1) -> None:
        """Emit the next ``n`` payloads now, whether playing or paused."""
        if self._pacer is None:
            self._pacer = self._open(self._start)
        self._pacer.step(n)

    def set_speed(self, speed: float) -> None:
        if speed <= 0:
            raise ValueError("speed must be positive")
        self._speed = speed
        if self._pacer is not None:
            self._pacer.set_speed(speed)

    def seek(self, moment: datetime | None) -> None:
        """Continue from ``moment`` (``None`` = the beginning) on the next iteration.

        Paused replays get one step credit, so the payload at the new position
        is shown rather than silently awaited.
        """
        old, self._pacer = self._pacer, self._open(moment)
        self._start = moment
        self._produced_in_pass = False
        if self._paused:
            self._pacer.step(1)
        if old is not None:
            old.interrupt()
            self._stale.append(old)

    # --- iteration ------------------------------------------------------------

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> DataModel:
        while True:
            await self._close_stale()

            if self._pacer is None:
                self._pacer = self._open(self._start)
            pacer = self._pacer

            try:
                payload = await pacer.__anext__()
            except PacerInterrupted:
                # A seek replaced the pacer while this one was waiting; the old
                # one is queued for closing, the new one is picked up next turn.
                continue
            except StopAsyncIteration:
                if pacer is not self._pacer:
                    continue
                await pacer.aclose()
                self._pacer = None
                if not self._loop or not self._produced_in_pass:
                    raise
                self._produced_in_pass = False
                self._start = None
                self.passes += 1
                continue

            self._produced_in_pass = True
            self.position = payload.timestamp
            return payload

    async def aclose(self) -> None:
        """Close whatever stream is open. The replay can be iterated again after."""
        if self._pacer is not None:
            self._stale.append(self._pacer)
            self._pacer = None
        await self._close_stale()

    # --- internals ------------------------------------------------------------

    def _open(self, start: datetime | None) -> Pacer:
        stream: AsyncIterator[DataModel] = self._gateway.consume(
            self._model, start=start, mRID=self._mRID
        )
        return Pacer(stream, speed=self._speed, paused=self._paused)

    async def _close_stale(self) -> None:
        while self._stale:
            await self._stale.pop().aclose()
