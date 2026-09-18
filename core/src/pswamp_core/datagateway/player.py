# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The player: paces a gateway stream and takes the replay commands.

This is the thing STEP 1 A5 actually commands. The gateway answers "give me the
frames from *t*" as fast as the provider can read them; the player turns that
into a stream at a chosen speed, pauses it, steps it, jumps it -- and knows when
none of that applies, so a client can render only the controls that are real
(port doc §10.2: "a mode, not a phase ... no dead buttons").

Three decisions, made here so they are made once:

1. **Seek is a new stream.** ``seek(t)`` closes the current ``DataStream``, opens
   ``gateway.consume(model, t, None)`` and publishes ``StreamChanged``. A stream's
   watermark rightly refuses to go backwards, so going backwards is a fresh one.
   A looping replay is the same mechanism once per pass. Consumers with a window
   re-prime themselves on ``StreamChanged``; the player does not know their
   window lengths and must not.
2. **The player is a coroutine in this slice.** Everything it drives is on the
   event loop, so pacing is an ``asyncio`` sleep and there is no thread to bridge.
   STEP 3 §4.3 argues for a thread with batched pulls once the desktop package's
   blocking modules are behind it; that variant is deferred, and the bus already
   has the seam it will need.
3. **Mode comes from the stream that is open, not from the gateway.** A
   *replay* is a bounded stream over the gateway's **history** coverage (what
   its ``HISTORY_CONSUME`` clients hold), paced, and reopened from the start
   when it runs out; *live* is an open-ended stream from now, which the planner
   routes to a ``LIVE_CONSUME`` client, delivered as it arrives. ``go_live()``
   and ``replay()`` switch between the two; nothing switches on its own, so a
   gateway of an archive plus a live feed replays the archive paced and seekable
   even though a later segment could tail. (The first cut derived the mode from
   the union of the coverages, which made such a gateway "live" from its first
   frame -- unpaced and unseekable.) ``can_seek`` is true only in replay mode;
   ``can_go_live`` says whether the switch is on offer. Live mode has no
   transport controls at all: pausing a live source would be view state, not
   source state, and this slice does not offer it rather than fake it.
4. **A replay may be bounded, and a failure ends it loudly.** ``replay(start,
   end)`` -- the ``replay`` verb with an ``end`` -- opens ``[start, end)`` and
   ends *paused* at ``end`` even on a looping player, which is what "play me
   this range" means; ``PlayerStatus.range_end`` says so while it runs. And a
   provider that raises mid-stream (a remote store that timed out) ends the
   stream the same way, paused with ``PlayerStatus.error`` set and an
   ``ErrorEvent`` on the bus, rather than killing the run task in silence and
   leaving a page that says "playing" for ever. The next play or seek clears it.

Pacing follows the rule the streamer's old ticker had: wait until the frame is
due on a monotonic clock, and if the loop has fallen more than one frame
interval behind, re-anchor and drop time rather than fire a catch-up burst.
Every control change re-anchors.

Commands arrive **on the bus**: the player subscribes to ``Command`` and applies
those addressed to it (``target`` ``None`` or its name). That is what lets a
``POST`` at the web edge, a test, or a future Qt widget all drive it the same
way, without holding a reference to it.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

from ..log import get_logger
from ..messages.control import Command, PlayerStatus, StreamChanged
from ..messages.errors import ErrorEvent
from ..messages.pmu import PmuFrame
from ..util.time import ensure_utc, utcnow
from .data_client_model import Capability
from .time_range import Coverage

if TYPE_CHECKING:
    from ..bus import Bus
    from ..messages.data_model import DataModel
    from .data_gateway import DataGateway
    from .stream import DataStream

__all__ = ["PLAYER_TARGET", "Player", "PlayerError"]

logger = get_logger("pswamp_core.datagateway.player")

#: The ``Command.target`` that addresses a stream's player. ``None`` also does.
PLAYER_TARGET = "player"

#: How much slower than real time the loop may fall before dropping time.
_BEHIND_TOLERANCE = 1.0  # in frame intervals


class PlayerError(RuntimeError):
    """A control was refused: seeking a live source, or a bad argument."""


class Player:
    """Paces one gateway stream and owns its replay controls.

    Args:
        gateway: Where the frames come from.
        bus: Where frames, ``PlayerStatus`` and ``StreamChanged`` go, and where
            ``Command`` messages are read from.
        model: The message class to stream; ``PmuFrame`` by default.
        start: Where to open the first replay stream; ``None`` is the earliest
            history. Ignored when the gateway has no history at all, in which
            case the player starts live.
        speed: Replay speed multiplier.
        loop: Restart from the coverage start when the stream runs out.
        paced: ``False`` delivers frames as fast as the provider yields them
            (tests; batch runs). Ignored in live mode, where arrival is the pace.
        autoplay: Start playing at ``start()`` rather than paused.
        name: The ``Command.target`` this player answers to.
    """

    def __init__(
        self,
        gateway: DataGateway,
        bus: Bus,
        *,
        model: type[DataModel] = PmuFrame,
        start: datetime | None = None,
        speed: float = 1.0,
        loop: bool = False,
        paced: bool = True,
        autoplay: bool = False,
        name: str = PLAYER_TARGET,
    ) -> None:
        if speed <= 0:
            raise PlayerError("speed must be positive")
        self._gateway = gateway
        self._bus = bus
        self.model = model
        self.name = name
        self.speed = speed
        self.loop = loop
        self.paced = paced
        self._start_at = ensure_utc(start) if start is not None else None
        self._autoplay = autoplay

        self.cursor: datetime | None = None
        self.last_frame: DataModel | None = None
        self.frame_interval: timedelta | None = None
        #: Why the stream stopped, when it stopped on a provider failure.
        self.error: str | None = None

        #: What the HISTORY_CONSUME clients hold: the seekable range.
        self._history: Coverage | None = None
        #: The explicit end of a bounded replay; ``None`` when it runs to the
        #: history end (and may loop).
        self._range_end: datetime | None = None
        #: Whether any client declares LIVE_CONSUME for the model.
        self._live_available = False
        #: Whether the open stream is the live one.
        self._live = False
        self._stream: DataStream | None = None
        self._generation = 0
        self._produced_since_switch = False
        self._previous_emitted: datetime | None = None
        # A frame read from the stream but not yet played: the run loop parks it
        # here while pacing or paused, and step() plays it first, so a pause
        # followed by a step never skips a frame or plays one out of order.
        self._pending: tuple[DataModel, int] | None = None
        # The one read in flight on the stream, if any. Reading is a task so
        # that nothing awaits the source while holding the lock: a live feed
        # that has gone quiet must not block a switch back to the replay.
        self._read: asyncio.Task | None = None
        self._paused = True
        self._ended = False
        self._anchor: tuple[datetime, float] | None = None
        self._wake = asyncio.Event()
        self._lock = asyncio.Lock()
        self._run_task: asyncio.Task | None = None
        self._command_task: asyncio.Task | None = None
        self._command_subscription = None

    # -- state -----------------------------------------------------------------

    @property
    def mode(self) -> Literal["live", "replay"]:
        return "live" if self._live else "replay"

    @property
    def can_seek(self) -> bool:
        return not self._live and self._history is not None

    @property
    def can_go_live(self) -> bool:
        return self._live_available

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def ended(self) -> bool:
        return self._ended

    @property
    def coverage(self) -> Coverage | None:
        """The seekable history coverage (``None`` without a history source)."""
        return self._history

    def status(self) -> PlayerStatus:
        """A snapshot of where the player is and what it can do."""
        coverage = self._history
        return PlayerStatus(
            timestamp=utcnow(),
            mode=self.mode,
            cursor=self.cursor,
            speed=self.speed,
            paused=self._paused,
            loop=self.loop,
            ended=self._ended,
            can_seek=self.can_seek,
            can_go_live=self.can_go_live,
            coverage_start=None if coverage is None else coverage.range.start,
            coverage_end=None if coverage is None else coverage.range.end,
            frame_interval_s=(
                None if self.frame_interval is None else self.frame_interval.total_seconds()
            ),
            range_end=self._range_end,
            error=self.error,
        )

    # -- lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        """Open the first stream and start the run and command tasks.

        A replay from ``start`` when the gateway has history; live when it has
        only a live source (there is nothing to replay, and a live stream that
        nobody has resumed would sit paused for ever).
        """
        unreachable = False
        async with self._lock:
            await self._read_gateway()
            if self._history is None and not self._live_available:
                if not self._gateway.supports(self.model):
                    raise PlayerError(f"no client can consume {self.model.__name__}")
                # A client is configured for the model but reports nothing --
                # typically a remote store that cannot be reached. Start anyway,
                # stopped with the error set and said on the bus, so the page
                # connects and shows *why*, and a refresh can find the store
                # when it is back; refusing would only close the socket.
                unreachable = True
                live = False
            else:
                live = self._history is None
                await self._switch_stream(None if live else self._start_at, live=live)
        self._paused = True if unreachable else (False if live else not self._autoplay)
        # Subscribe *here*, synchronously, rather than inside the task: a command
        # published right after start() returns must not be lost to a task that
        # has not had its first turn on the loop yet.
        from ..bus import Overflow

        self._command_subscription = self._bus.subscribe(Command, overflow=Overflow.GROW)
        self._run_task = asyncio.create_task(self._run(), name=f"{self.name}.run")
        self._command_task = asyncio.create_task(
            self._commands(), name=f"{self.name}.commands"
        )
        if unreachable:
            # One turn of the loop first: the pipeline's module tasks (the error
            # forwarder among them) were created just before this and have not
            # subscribed yet; an event published now would reach nobody.
            await asyncio.sleep(0)
            self._fail_on_coverage()
            return
        self._publish_status()

    async def stop(self) -> None:
        """Cancel the tasks and close the stream."""
        tasks = [
            task for task in (self._run_task, self._command_task, self._read) if task is not None
        ]
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                # A read that died of a provider failure holds its exception;
                # stopping is not the place to re-raise it.
                logger.exception("%s: task %s had failed", self.name, task.get_name())
        self._run_task = self._command_task = self._read = None
        if self._command_subscription is not None:
            self._command_subscription.close()
            self._command_subscription = None
        if self._stream is not None:
            await self._stream.aclose()
            self._stream = None

    # -- controls --------------------------------------------------------------

    def pause(self) -> None:
        self._refuse_in_live("pause")
        self._paused = True
        self._anchor = None
        self._wake.set()
        self._publish_status()

    def resume(self) -> None:
        self._refuse_in_live("play")
        self._paused = False
        self.error = None
        self._anchor = None
        self._wake.set()
        self._publish_status()

    def set_speed(self, speed: float) -> None:
        self._refuse_in_live("speed")
        if speed <= 0:
            raise PlayerError("speed must be positive")
        self.speed = float(speed)
        self._anchor = None
        self._wake.set()
        self._publish_status()

    async def seek(self, to: datetime) -> None:
        """Reposition the replay at ``to``: a new stream, announced on the bus."""
        self._refuse_in_live("seek")
        await self._require_history("seek")
        async with self._lock:
            await self._switch_stream(ensure_utc(to), live=False)
        self._wake.set()
        self._publish_status()

    async def go_live(self) -> None:
        """Switch to the live stream: an open-ended stream from now, delivered
        as the source produces it. Plays immediately; there is no pause."""
        if not self._live_available:
            raise PlayerError("cannot go live: no client can tail this stream")
        async with self._lock:
            await self._switch_stream(utcnow(), live=True)
        self._paused = False
        self._wake.set()
        self._publish_status()

    async def replay(self, start: datetime | None = None, end: datetime | None = None) -> None:
        """Switch to (or restart) the replay at ``start``, or the beginning of
        the history. Lands paused, the same state ``start()`` produces.

        With ``end`` the replay is bounded to ``[start, end)`` (clamped to the
        history end) and ends paused there rather than looping.
        """
        await self._require_history("replay")
        async with self._lock:
            target = self._history.range.start if start is None else ensure_utc(start)
            await self._switch_stream(target, live=False, end=end)
        self._paused = True
        self._wake.set()
        self._publish_status()

    async def refresh(self) -> None:
        """Ask the gateway again what it holds, and publish the status.

        The ``refresh`` verb. Coverage is otherwise re-read only when a stream is
        opened, so a page whose source went away (no coverage, every control
        disabled) needs one command that asks again without playing anything:
        if the source is back, the next status carries its coverage and the
        error is cleared; if not, the status says so still.
        """
        async with self._lock:
            await self._read_gateway()
            if self._history is not None and not self._live:
                self.error = None
        self._publish_status()

    async def step(self, n: int = 1) -> None:
        """Play ``n`` frames immediately, unpaced. Negative ``n`` seeks back
        ``|n|`` frame intervals from the cursor and plays the frame there."""
        self._refuse_in_live("step")
        if n == 0:
            return
        async with self._lock:
            if n < 0:
                if self.cursor is None or self.frame_interval is None:
                    logger.warning("cannot step back before the frame interval is known")
                    return
                if not self.can_seek:
                    raise PlayerError("cannot step back: the source has no history")
                target = self.cursor + self.frame_interval * n
                start = self._coverage_start()
                if start is not None and target < start:
                    target = start
                await self._switch_stream(target, live=False)
                n = 1
            for _ in range(n):
                frame = await self._take_frame()
                if frame is None:
                    break
                self._emit(frame)
            self._anchor = None
        self._wake.set()
        self._publish_status()

    async def apply(self, command: Command) -> None:
        """Apply one command addressed to this player."""
        verb = command.verb
        args = command.args
        if verb in ("play", "resume"):
            self.resume()
        elif verb in ("stop", "pause"):
            self.pause()
        elif verb == "step":
            await self.step(int(args.get("n", 1)))
        elif verb == "seek":
            await self.seek(self._seek_target(args))
        elif verb == "speed":
            self.set_speed(float(args["speed"]))
        elif verb == "live":
            await self.go_live()
        elif verb == "refresh":
            await self.refresh()
        elif verb == "replay":
            await self.replay(
                self._seek_target(args) if _names_an_instant(args) else None,
                self._range_end_target(args),
            )
            if args.get("play"):
                # One POST, one Command: "play this range" lands paused and is
                # resumed here, rather than needing a second command.
                self.resume()
        else:
            logger.warning("unknown player command %r (request %s)", verb, command.request_id)
            return
        logger.info("applied %s %s (request %s)", verb, args or "", command.request_id)

    # -- internals -------------------------------------------------------------

    def _seek_target(self, args: dict[str, Any]) -> datetime:
        if "to" in args:
            return _instant(args["to"])
        if "offset_s" in args:
            return self._from_coverage_start(float(args["offset_s"]), "seek by offset")
        raise PlayerError("seek needs 'to' (an instant) or 'offset_s' (seconds from start)")

    def _range_end_target(self, args: dict[str, Any]) -> datetime | None:
        """The optional exclusive end of a bounded replay: ``end`` (an instant)
        or ``end_offset_s`` (seconds from the coverage start); ``None`` if neither."""
        if args.get("end") is not None:
            return _instant(args["end"])
        if args.get("end_offset_s") is not None:
            return self._from_coverage_start(float(args["end_offset_s"]), "bound by offset")
        return None

    def _from_coverage_start(self, seconds: float, what: str) -> datetime:
        start = self._coverage_start()
        if start is None:
            raise PlayerError(f"cannot {what}: coverage start is unknown")
        return start + timedelta(seconds=seconds)

    def _coverage_start(self) -> datetime | None:
        return None if self._history is None else self._history.range.start

    async def _require_history(self, control: str) -> None:
        """Refuse ``control`` unless a history source answers -- asking the
        gateway again first, so a source that was down and is back is found."""
        if self._history is None:
            await self._read_gateway()
        if self._history is None:
            raise PlayerError(f"cannot {control}: the source has no history")

    def _refuse_in_live(self, control: str) -> None:
        if self._live:
            raise PlayerError(f"{control} does not apply in live mode")

    async def _read_gateway(self) -> None:
        """Refresh what the gateway offers: the seekable history, and whether a
        live source exists. Coverage is now-relative for most stores, so this
        is re-read on every stream switch."""
        self._history = await self._gateway.coverage(
            self.model, capability=Capability.HISTORY_CONSUME
        )
        self._live_available = self._gateway.supports(self.model, Capability.LIVE_CONSUME)

    async def _switch_stream(
        self, start: datetime | None, *, live: bool, end: datetime | None = None
    ) -> None:
        """Close the current stream and open one at ``start``. Caller holds the lock.

        A replay stream is **bounded** to the history's end, so it ends there
        (and loops) instead of handing off to a live client; an explicit ``end``
        bounds it earlier still, and such a range never loops. The live stream is
        open-ended from ``start``.
        """
        # Coverage first, and the range checked against it, so that a refused
        # range leaves the current stream exactly as it was.
        await self._read_gateway()
        if not live and self._history is None:
            # The history source stopped answering (the gateway logs the cause
            # and skips it). Close what was open and say so where the page can
            # see it, rather than opening a stream over nothing that would end
            # at once in silence. The next play or seek asks the gateway again.
            self._generation += 1
            await self._cancel_read()
            if self._stream is not None:
                await self._stream.aclose()
            self._stream = None
            self._pending = None
            self._fail_on_coverage()
            raise PlayerError(self.error or "no history coverage")
        history_end = None if self._history is None else self._history.range.end
        if live:
            end = None
        elif end is not None:
            end = ensure_utc(end)
            if history_end is not None and end > history_end:
                end = history_end
        else:
            end = history_end
        if start is not None and end is not None and end <= start:
            raise PlayerError("replay range is empty")
        # Bump the generation *before* cancelling the read, so the run loop can
        # tell a read cancelled by this switch from its own cancellation.
        self._generation += 1
        await self._cancel_read()
        if self._stream is not None:
            await self._stream.aclose()
        self._range_end = None if live or end is None or end == history_end else end
        self.error = None
        self._stream = self._gateway.consume(self.model, start, end)
        if live != self._live:
            # A measured interval belongs to the stream it was measured on: a
            # live feed's jitter must not become the replay's step size.
            self.frame_interval = None
        self._live = live
        self._produced_since_switch = False
        self._previous_emitted = None
        # The last frame belonged to the stream just closed; showing it against
        # the new one would mislead (a live frame under a "recorded, paused"
        # badge). Nothing has played on this stream yet.
        self.last_frame = None
        self._pending = None
        self._ended = False
        self._anchor = None
        self.cursor = start
        self._bus.publish(StreamChanged(timestamp=utcnow(), cursor=start))

    # The read in flight. One task at a time reads the stream; it parks what it
    # read in ``_pending`` and returns it (``None`` at the end of the stream).
    # The run loop awaits it *outside* the lock; ``step`` awaits it under the
    # lock, which is fine because step is refused in live mode and a history
    # read returns. A switch cancels it.

    def _ensure_read(self) -> tuple[asyncio.Task, int]:
        """The current read task, started if there is none. Caller holds the lock."""
        if self._read is None or self._read.done():
            self._read = asyncio.create_task(
                self._reader(self._stream, self._generation), name=f"{self.name}.read"
            )
        return self._read, self._generation

    async def _reader(self, stream: DataStream, generation: int) -> DataModel | None:
        try:
            frame = await stream.__anext__()
        except StopAsyncIteration:
            return None
        if generation == self._generation and self._pending is None:
            self._pending = (frame, generation)
            self._produced_since_switch = True
        return frame

    async def _cancel_read(self) -> None:
        task, self._read = self._read, None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _reopen_if_closed(self) -> None:
        """A resume after the end: replay from the start of the history, or,
        if live was what was open, tail live again. Caller holds the lock.
        Raises ``PlayerError`` when a replay's history source no longer answers."""
        if self._stream is None:
            live = self._live
            await self._switch_stream(None if live else self._coverage_start(), live=live)

    async def _on_stream_end(self) -> bool:
        """The stream ran out. Loop if so configured; else end, paused. Caller
        holds the lock. ``True`` if a new stream was opened."""
        if self._stream is not None:
            await self._stream.aclose()
        self._stream = None
        self._read = None
        # A bounded range ("play me [t0, t1)") ends where it was asked to, even
        # on a looping player; a live stream that ends means the source went
        # away, and there is nothing to loop back to.
        if self.loop and self._produced_since_switch and not self._live and self._range_end is None:
            await self._switch_stream(self._coverage_start(), live=False)
            return True
        self._ended = True
        self._paused = True
        self._publish_status()
        return False

    async def _on_stream_error(self, error: BaseException) -> None:
        """The provider failed mid-stream. End the stream paused, say why, and
        stay alive for the next command. Caller holds the lock."""
        if self._stream is not None:
            with contextlib.suppress(Exception):
                await self._stream.aclose()
        self._stream = None
        self._read = None
        self._pending = None
        self._fail(f"{type(error).__name__}: {error}")

    def _fail_on_coverage(self) -> None:
        """No history source answers. Name the client and its own error when the
        gateway recorded one (``coverage_failures``: a remote store that could
        not be reached says so, URL and all); a bare "no coverage" otherwise."""
        failures: dict[str, str] = getattr(self._gateway, "coverage_failures", {})
        if failures:
            source = next(iter(failures)) if len(failures) == 1 else self.name
            detail = "; ".join(f"{name}: {error}" for name, error in failures.items())
            self._fail(detail, source=source, message="the provider cannot be reached")
        else:
            self._fail("the history source reports no coverage; is it reachable?")

    def _fail(
        self,
        detail: str,
        *,
        source: str | None = None,
        message: str = "the stream stopped: its provider failed",
    ) -> None:
        """Record a provider failure: end paused, set ``error``, tell the bus.
        ``source`` names who failed on the event -- the client, when known."""
        self._ended = True
        self._paused = True
        self.error = detail
        logger.error("%s: stream stopped on a provider failure: %s", self.name, detail)
        self._bus.publish(
            ErrorEvent(
                timestamp=utcnow(),
                source=source or self.name,
                message=message,
                detail=detail,
            )
        )
        self._publish_status()

    async def _next_frame(self) -> DataModel | None:
        """The next frame of the current stream, reopening it on loop. Caller
        holds the lock. ``None`` means the replay ended (and is now paused)."""
        while True:
            await self._reopen_if_closed()
            task, _ = self._ensure_read()
            try:
                frame = await task
            except Exception as error:
                await self._on_stream_error(error)
                return None
            if frame is None:
                if await self._on_stream_end():
                    continue
                return None
            if self._pending is None:
                continue  # taken under us; read on
            frame, _ = self._pending
            self._pending = None
            return frame

    async def _take_frame(self) -> DataModel | None:
        """The parked frame if it is still current, else the next one from the
        stream. Caller holds the lock."""
        if self._pending is not None:
            frame, generation = self._pending
            self._pending = None
            if generation == self._generation:
                return frame
        return await self._next_frame()

    def _emit(self, frame: DataModel) -> None:
        moment = frame.timestamp
        if (
            moment is not None
            and self._previous_emitted is not None
            and moment > self._previous_emitted
        ):
            self.frame_interval = moment - self._previous_emitted
        self._previous_emitted = moment
        self.cursor = moment
        self.last_frame = frame
        self._bus.publish(frame)

    async def _pace(self, frame: DataModel) -> bool:
        """Wait until ``frame`` is due. ``False`` if a control woke the wait."""
        if not self.paced or self._live or frame.timestamp is None:
            return True
        now = time.monotonic()
        if self._anchor is None:
            self._anchor = (frame.timestamp, now)
            return True
        anchor_ts, anchor_mono = self._anchor
        due = anchor_mono + (frame.timestamp - anchor_ts).total_seconds() / self.speed
        delay = due - now
        if delay <= 0:
            interval = (
                self.frame_interval.total_seconds() / self.speed
                if self.frame_interval is not None
                else 0.0
            )
            if interval and -delay > interval * _BEHIND_TOLERANCE:
                # Behind by more than a frame: drop time rather than burst.
                self._anchor = (frame.timestamp, now)
            return True
        self._wake.clear()
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=delay)
        except TimeoutError:
            return True
        return False

    async def _run(self) -> None:
        while True:
            if self._paused:
                self._wake.clear()
                await self._wake.wait()
                continue
            if self._pending is None:
                async with self._lock:
                    try:
                        await self._reopen_if_closed()
                    except PlayerError:
                        continue  # the source is gone; _fail paused us and said so
                    task, generation = self._ensure_read()
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    me = asyncio.current_task()
                    if (me is not None and me.cancelling()) or not task.cancelled():
                        raise  # stop() cancelled us
                    if generation == self._generation:
                        raise  # the read died of something other than a switch
                    continue  # a switch cancelled the read; start over on the new stream
                except Exception as error:
                    async with self._lock:
                        if generation == self._generation:
                            await self._on_stream_error(error)
                    continue
                async with self._lock:
                    if generation != self._generation:
                        continue
                    if task.result() is None:
                        await self._on_stream_end()
                        continue
                if self._pending is None:
                    continue  # a step took the frame
            frame, generation = self._pending
            if generation != self._generation:
                # The stream was switched under us; this frame belongs to the old one.
                self._pending = None
                continue
            if not await self._pace(frame):
                continue  # a control woke us: re-check paused / generation, keep the frame parked
            if self._paused or self._pending is None or self._pending[1] != self._generation:
                continue
            self._pending = None
            self._emit(frame)

    async def _commands(self) -> None:
        commands = self._command_subscription
        if commands is None:
            return
        async for command in commands:
            if command.target not in (None, self.name):
                continue
            try:
                await self.apply(command)
            except PlayerError as error:
                logger.warning(
                    "refused %s (request %s): %s", command.verb, command.request_id, error
                )
            except Exception:
                logger.exception(
                    "command %s (request %s) failed", command.verb, command.request_id
                )

    def _publish_status(self) -> None:
        self._bus.publish(self.status())


def _instant(value: Any) -> datetime:
    """A command argument as a UTC instant: a datetime, or an ISO 8601 string."""
    return ensure_utc(value if isinstance(value, datetime) else datetime.fromisoformat(value))


def _names_an_instant(args: dict[str, Any]) -> bool:
    return "to" in args or "offset_s" in args
