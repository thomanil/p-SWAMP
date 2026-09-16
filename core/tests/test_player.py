# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The player: pacing, pause/resume, step, seek, loop, commands over the bus --
and, over a gateway holding both an archive and a live feed, that mode follows
the stream that is open rather than the union of what the clients hold."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta

import pytest
from support import HISTORY, Measurement, at, measurement, take

from pswamp_core.bus import InProcessBus, Overflow
from pswamp_core.datagateway import Capability, Coverage, DataGateway, Player, TimeRange
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.datagateway.player import PlayerError
from pswamp_core.messages import Command, ErrorEvent, PlayerStatus, StreamChanged
from pswamp_core.util.time import utcnow


def live_only_client(name: str = "live") -> InMemoryClient:
    """A client that can only tail: no records, a now-relative live window."""
    return InMemoryClient(
        name,
        Measurement,
        capabilities=Capability.LIVE_CONSUME,
        coverage_fn=lambda: Coverage(TimeRange(utcnow() - timedelta(seconds=1), None), live=True),
    )


@pytest.fixture
async def rig(history_client):
    """A bus, a gateway over ten one-second-apart frames, and an unpaced player.
    Yields the pieces and stops the player afterwards."""
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    gateway = DataGateway([history_client])
    player = Player(gateway, bus, model=Measurement, paced=False)
    yield bus, gateway, player
    await player.stop()


@pytest.fixture
async def mixed_rig(history_client):
    """An archive and a live-only feed in one gateway, an unpaced player over
    both. Yields (bus, live client, player)."""
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    live = live_only_client()
    gateway = DataGateway([history_client, live])
    player = Player(gateway, bus, model=Measurement, paced=False)
    yield bus, live, player
    await player.stop()


async def wait_status(sub, predicate, timeout: float = 2.0) -> PlayerStatus:
    async def _wait():
        while True:
            message = await sub.get()
            if isinstance(message, PlayerStatus) and predicate(message):
                return message

    return await asyncio.wait_for(_wait(), timeout)


async def test_starts_paused_and_reports_replay_mode(rig):
    bus, _, player = rig
    with bus.subscribe(PlayerStatus, StreamChanged) as sub:
        await player.start()
        changed, status = await take(sub, 2)
    assert isinstance(changed, StreamChanged)
    assert status.paused is True
    assert status.mode == "replay"
    assert status.can_seek is True
    assert status.can_go_live is False
    assert status.coverage_start == at(0)
    assert status.cursor is None
    assert status.ended is False


async def test_play_yields_every_frame_in_order_then_ends(rig):
    bus, _, player = rig
    await player.start()
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames, bus.subscribe(
        PlayerStatus
    ) as statuses:
        player.resume()
        got = await take(frames, 10)
        assert [m.mRID for m in got] == [f"m{i}" for i in range(10)]
        ended = await wait_status(statuses, lambda s: s.ended)
    assert ended.paused is True
    assert player.cursor == at(9)
    assert player.frame_interval.total_seconds() == 1.0


async def test_pause_holds_and_resume_continues(rig):
    bus, _, player = rig
    # Paced at 20x over 1 s frames: 50 ms a frame, so pause lands mid-stream.
    player.paced = True
    player.speed = 20
    await player.start()
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames:
        player.resume()
        first = await take(frames, 2)
        player.pause()
        await asyncio.sleep(0.2)
        leftover = []
        while (m := frames.get_nowait()) is not None:
            leftover.append(m)
        assert len(leftover) <= 1  # at most the frame already in flight
        player.resume()
        more = await take(frames, 2)
    ids = [m.mRID for m in first + leftover + more]
    assert ids == [f"m{i}" for i in range(len(ids))]


async def test_step_forward_plays_one_frame(rig):
    bus, _, player = rig
    await player.start()
    with bus.subscribe(Measurement) as frames:
        await player.step()
        await player.step()
        got = await take(frames, 2)
    assert [m.mRID for m in got] == ["m0", "m1"]
    assert player.paused is True
    assert player.cursor == at(1)


async def test_step_back_reopens_the_stream_one_frame_earlier(rig):
    bus, _, player = rig
    await player.start()
    with bus.subscribe(Measurement, StreamChanged) as sub:
        for _ in range(3):
            await player.step()
        await take(sub, 3)  # m0 m1 m2
        await player.step(-1)
        changed, frame = await take(sub, 2)
    assert isinstance(changed, StreamChanged)
    assert changed.cursor == at(1)
    assert frame.mRID == "m1"
    assert player.cursor == at(1)


async def test_step_back_clamps_at_coverage_start(rig):
    bus, _, player = rig
    await player.start()
    with bus.subscribe(Measurement) as frames:
        await player.step()
        await player.step()
        await take(frames, 2)
        await player.step(-5)
        (frame,) = await take(frames, 1)
    assert frame.mRID == "m0"


async def test_seek_opens_a_new_stream_at_the_target(rig):
    bus, _, player = rig
    await player.start()
    with bus.subscribe(Measurement, StreamChanged, PlayerStatus) as sub:
        await player.seek(at(5))
        changed, status = await take(sub, 2)
        assert isinstance(changed, StreamChanged) and changed.cursor == at(5)
        assert isinstance(status, PlayerStatus) and status.cursor == at(5)
        await player.step()
        _, frame = await take(sub, 2)  # the step's frame and its status, order: frame first
    assert frame.mRID == "m5" or _.mRID == "m5"


async def test_seek_by_offset_command(rig):
    bus, _, player = rig
    await player.start()
    with bus.subscribe(Measurement) as frames:
        bus.publish(Command(verb="seek", args={"offset_s": 7}))
        bus.publish(Command(verb="step"))
        (frame,) = await take(frames, 1)
    assert frame.mRID == "m7"


async def test_loop_restarts_from_coverage_start(history_client):
    bus = InProcessBus()
    gateway = DataGateway([history_client])
    player = Player(gateway, bus, model=Measurement, paced=False, loop=True, autoplay=True)
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames, bus.subscribe(
        StreamChanged, overflow=Overflow.GROW
    ) as changes:
        await player.start()
        got = await take(frames, 13)
        assert [m.mRID for m in got][9:13] == ["m9", "m0", "m1", "m2"]
        assert len(await take(changes, 2)) == 2  # the initial open and the loop restart
    await player.stop()
    assert player.ended is False


async def test_commands_over_the_bus_drive_the_player(rig):
    bus, _, player = rig
    await player.start()
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames, bus.subscribe(
        PlayerStatus
    ) as statuses:
        bus.publish(Command(verb="speed", args={"speed": 2.0}))
        status = await wait_status(statuses, lambda s: s.speed == 2.0)
        assert status.paused is True
        bus.publish(Command(verb="play"))
        await wait_status(statuses, lambda s: not s.paused)
        got = await take(frames, 3)
        assert [m.mRID for m in got] == ["m0", "m1", "m2"]
        bus.publish(Command(verb="stop"))
        await wait_status(statuses, lambda s: s.paused)
        bus.publish(Command(target="someone-else", verb="play"))
        bus.publish(Command(verb="teleport"))  # unknown verb: logged, ignored
        await asyncio.sleep(0.02)
        assert player.paused is True


async def test_pacing_spaces_frames_by_speed(history_client):
    bus = InProcessBus()
    gateway = DataGateway([history_client])
    # 1 s frames at 50x → 20 ms apart; five frames take at least 80 ms.
    player = Player(gateway, bus, model=Measurement, paced=True, speed=50, autoplay=True)
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames:
        started = time.monotonic()
        await player.start()
        await take(frames, 5)
        elapsed = time.monotonic() - started
    await player.stop()
    assert elapsed >= 0.075


async def test_a_live_only_gateway_starts_live_and_cannot_be_sought():
    """With nothing to replay the player starts live and playing. (A client
    that also holds history starts in replay: see the mixed cases below.)"""
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    live = live_only_client()
    gateway = DataGateway([live])
    player = Player(gateway, bus, model=Measurement, paced=False)
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames:
        await player.start()
        try:
            status = player.status()
            assert status.mode == "live"
            assert status.paused is False
            assert status.can_seek is False
            assert status.can_go_live is True
            assert status.coverage_start is None
            with pytest.raises(PlayerError):
                await player.seek(at(1))
            with pytest.raises(PlayerError):
                await player.replay()
            await asyncio.sleep(0)  # let the run task subscribe the live client
            live.publish(measurement(1, utcnow()))
            (frame,) = await take(frames, 1)
            assert frame.mRID == "m1"
        finally:
            await player.stop()


async def test_resume_after_end_restarts_from_the_beginning(rig):
    bus, _, player = rig
    await player.start()
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames, bus.subscribe(
        PlayerStatus
    ) as statuses:
        player.resume()
        await take(frames, 10)
        await wait_status(statuses, lambda s: s.ended)
        player.resume()
        got = await take(frames, 2)
    assert [m.mRID for m in got] == ["m0", "m1"]


def test_speed_must_be_positive():
    with pytest.raises(PlayerError):
        Player(DataGateway([InMemoryClient("x", Measurement, capabilities=HISTORY)]), InProcessBus(), speed=0)


async def test_pause_then_step_plays_the_parked_frame_in_order(rig):
    """A pause interrupts the run loop while it holds a frame it has read but not
    played; a step must play THAT frame, and a resume must not replay it."""
    bus, _, player = rig
    player.paced = True
    player.speed = 20  # 50 ms a frame: the loop is always holding the next one
    await player.start()
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames:
        player.resume()
        await take(frames, 2)  # m0 m1 played; m2 is read and being paced
        player.pause()
        await asyncio.sleep(0.12)
        while frames.get_nowait() is not None:
            pass
        held = player.cursor
        await player.step()
        (stepped,) = await take(frames, 1)
        assert stepped.timestamp == held + player.frame_interval
        player.resume()
        (next_one,) = await take(frames, 1)
        assert next_one.timestamp == stepped.timestamp + player.frame_interval


# --- an archive and a live feed in one gateway ---------------------------------


async def test_mixed_gateway_starts_in_replay_and_never_reads_the_live_client(mixed_rig):
    bus, live, player = mixed_rig
    live_reads = 0
    original = live.consume

    def counting(model, time_range, mRID=None):
        nonlocal live_reads
        live_reads += 1
        return original(model, time_range, mRID)

    live.consume = counting  # type: ignore[method-assign]
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames, bus.subscribe(
        PlayerStatus, overflow=Overflow.GROW
    ) as statuses:
        await player.start()
        status = player.status()
        assert status.mode == "replay"
        assert status.can_seek is True
        assert status.can_go_live is True
        assert status.coverage_start == at(0) and status.coverage_end is not None
        player.resume()
        got = await take(frames, 10)
        await wait_status(statuses, lambda s: s.ended)
    assert [m.mRID for m in got] == [f"m{i}" for i in range(10)]
    assert live_reads == 0
    assert player.mode == "replay"


async def test_replay_is_paced_although_a_live_client_exists(history_client):
    """The regression for deriving mode from the coverage union: an archive
    beside a live feed used to be "live" from its first frame, and unpaced."""
    bus = InProcessBus()
    gateway = DataGateway([history_client, live_only_client()])
    player = Player(gateway, bus, model=Measurement, paced=True, speed=50, autoplay=True)
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames:
        started = time.monotonic()
        await player.start()
        assert player.mode == "replay"
        await take(frames, 5)
        elapsed = time.monotonic() - started
    await player.stop()
    assert elapsed >= 0.075


async def test_replay_loops_at_the_end_of_history_over_a_mixed_gateway(history_client):
    bus = InProcessBus()
    gateway = DataGateway([history_client, live_only_client()])
    player = Player(gateway, bus, model=Measurement, paced=False, loop=True, autoplay=True)
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames:
        await player.start()
        got = await take(frames, 13)
    await player.stop()
    assert [m.mRID for m in got][9:13] == ["m9", "m0", "m1", "m2"]
    assert player.mode == "replay"


async def test_go_live_switches_mode_and_delivers_published_records(mixed_rig):
    bus, live, player = mixed_rig
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames, bus.subscribe(
        PlayerStatus, overflow=Overflow.GROW
    ) as statuses, bus.subscribe(StreamChanged, overflow=Overflow.GROW) as changes:
        await player.start()
        await take(changes, 1)
        bus.publish(Command(verb="live"))
        status = await wait_status(statuses, lambda s: s.mode == "live")
        assert status.paused is False
        assert status.can_seek is False
        assert status.can_go_live is True
        assert status.coverage_start == at(0)  # the seekable history is still reported
        (changed,) = await take(changes, 1)
        assert changed.cursor is not None and changed.cursor > at(10)
        await asyncio.sleep(0.01)  # the run task opens the live segment
        stamp = utcnow()
        live.publish(measurement(100, stamp))
        (frame,) = await take(frames, 1)
        assert frame.mRID == "m100"
        assert player.cursor == stamp
        assert player._stream is not None and player._stream.segments[-1].live is True


async def test_live_mode_refuses_the_transport_controls(mixed_rig):
    bus, live, player = mixed_rig
    with bus.subscribe(PlayerStatus, overflow=Overflow.GROW) as statuses:
        await player.start()
        await player.go_live()
        for refused in (player.pause, player.resume, lambda: player.set_speed(2.0)):
            with pytest.raises(PlayerError):
                refused()
        with pytest.raises(PlayerError):
            await player.step()
        with pytest.raises(PlayerError):
            await player.seek(at(1))
        # Over the bus a refusal is logged and changes nothing.
        cursor = player.cursor
        bus.publish(Command(verb="seek", args={"offset_s": 1.0}))
        bus.publish(Command(verb="stop"))
        await asyncio.sleep(0.02)
        assert player.mode == "live"
        assert player.paused is False
        assert player.cursor == cursor
        assert all(s.mode == "live" for s in _drain(statuses)[1:])


def _drain(subscription) -> list[PlayerStatus]:
    out = []
    while (item := subscription.get_nowait()) is not None:
        out.append(item)
    return out


async def test_replay_returns_to_the_history_start_paused(mixed_rig):
    bus, live, player = mixed_rig
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames, bus.subscribe(
        PlayerStatus, overflow=Overflow.GROW
    ) as statuses:
        await player.start()
        await player.go_live()
        await wait_status(statuses, lambda s: s.mode == "live")
        bus.publish(Command(verb="replay"))
        status = await wait_status(statuses, lambda s: s.mode == "replay")
        assert status.paused is True
        assert status.can_seek is True
        assert status.cursor == at(0)
        await player.step()
        (frame,) = await take(frames, 1)
        assert frame.mRID == "m0"
        bus.publish(Command(verb="replay", args={"offset_s": 5.0}))
        await wait_status(statuses, lambda s: s.cursor == at(5))
        await player.step()
        (frame,) = await take(frames, 1)
        assert frame.mRID == "m5"


# --- a bounded replay, and a provider that fails mid-stream ------------------------


async def test_replay_over_a_bounded_range_plays_exactly_the_range_then_ends_paused(rig):
    bus, _, player = rig
    player.loop = True  # a bounded range ends even on a looping player
    await player.start()
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames, bus.subscribe(
        PlayerStatus, overflow=Overflow.GROW
    ) as statuses:
        bus.publish(Command(verb="replay", args={"to": at(2), "end": at(5), "play": True}))
        running = await wait_status(statuses, lambda s: not s.paused and s.range_end == at(5))
        assert running.error is None
        got = await take(frames, 3)
        ended = await wait_status(statuses, lambda s: s.ended)
    assert [m.mRID for m in got] == ["m2", "m3", "m4"]
    assert ended.paused is True and ended.range_end == at(5)
    await asyncio.sleep(0.02)
    assert player.ended is True  # no loop restart
    assert player.cursor == at(4)


async def test_range_end_past_the_history_is_clamped_and_offsets_work(rig):
    bus, _, player = rig
    await player.start()
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames, bus.subscribe(
        PlayerStatus, overflow=Overflow.GROW
    ) as statuses:
        bus.publish(Command(verb="replay", args={"offset_s": 8, "end_offset_s": 50, "play": True}))
        status = await wait_status(statuses, lambda s: not s.paused)
        assert status.range_end is None  # clamped to the history end: an ordinary replay
        got = await take(frames, 2)
        await wait_status(statuses, lambda s: s.ended)
    assert [m.mRID for m in got] == ["m8", "m9"]
    # An empty range is refused and leaves the stream as it was.
    with pytest.raises(PlayerError):
        await player.replay(at(5), at(5))
    with pytest.raises(PlayerError):
        await player.replay(at(6), at(5))


async def test_a_seek_after_a_bounded_replay_clears_the_range(rig):
    bus, _, player = rig
    await player.start()
    await player.replay(at(1), at(3))
    assert player.status().range_end == at(3)
    await player.seek(at(2))
    assert player.status().range_end is None


class FailsAfterTwo(InMemoryClient):
    """A history client whose stream dies after two records, as a remote
    store that times out would."""

    async def consume(self, model, time_range, mRID=None):
        sent = 0
        async for record in super().consume(model, time_range, mRID):
            if sent == 2:
                raise TimeoutError("no result within 30s")
            sent += 1
            yield record


async def test_a_provider_failure_ends_the_stream_with_the_error_and_the_player_survives():
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    from support import measurements

    client = FailsAfterTwo("flaky", Measurement, measurements(10), capabilities=HISTORY)
    player = Player(DataGateway([client]), bus, model=Measurement, paced=False)
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames, bus.subscribe(
        PlayerStatus, overflow=Overflow.GROW
    ) as statuses, bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
        await player.start()
        try:
            player.resume()
            got = await take(frames, 2)
            failed = await wait_status(statuses, lambda s: s.error is not None)
            assert [m.mRID for m in got] == ["m0", "m1"]
            assert failed.paused is True and failed.ended is True
            assert failed.error == "TimeoutError: no result within 30s"
            (event,) = await take(errors, 1)
            assert event.source == "player" and event.detail == failed.error
            assert player._run_task is not None and not player._run_task.done()
            # A resume clears the error and reopens from the start.
            player.resume()
            assert player.status().error is None
            again = await take(frames, 2)
            assert [m.mRID for m in again] == ["m0", "m1"]
            await wait_status(statuses, lambda s: s.error is not None)
            # A step on a failed stream reports the same way and does not raise.
            await player.step()
            assert player.status().error is not None or player.cursor is not None
        finally:
            await player.stop()  # does not re-raise the stored failure


async def test_a_history_source_that_stops_answering_coverage_is_an_error_not_a_silent_end(rig):
    bus, gateway, player = rig
    await player.start()
    client = gateway.clients["history"]
    original = client.coverage

    async def unreachable(model, mRID=None):
        raise ConnectionError("refused")

    client.coverage = unreachable  # type: ignore[method-assign]
    with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors, bus.subscribe(
        Measurement, overflow=Overflow.GROW
    ) as frames:
        with pytest.raises(PlayerError):
            await player.seek(at(3))
        (event,) = await take(errors, 1)
        status = player.status()
        assert status.error == "history: ConnectionError: refused"  # the client's own error, named
        assert status.paused and status.ended and status.mode == "replay"
        assert event.detail == status.error and event.source == "history"
        # A play while it is still down fails the same way, and the run task lives.
        player.resume()
        (event,) = await take(errors, 1)
        await asyncio.sleep(0.02)
        assert player.paused and player.mode == "replay" and frames.get_nowait() is None
        assert player._run_task is not None and not player._run_task.done()
        # Still down: a refresh re-asks, finds nothing, and the error stands.
        bus.publish(Command(verb="refresh"))
        await asyncio.sleep(0.02)
        assert player.status().error is not None and player.status().coverage_start is None
        # Back: a refresh finds the coverage again and clears the error, playing nothing.
        client.coverage = original  # type: ignore[method-assign]
        await player.refresh()
        assert player.status().error is None and player.status().coverage_start == at(0)
        assert frames.get_nowait() is None
        await player.seek(at(3))
        assert player.status().error is None and player.status().coverage_start == at(0)
        await player.step()
        (frame,) = await take(frames, 1)
    assert frame.mRID == "m3"


async def test_a_player_over_an_unreachable_source_starts_stopped_with_its_error_and_recovers():
    """The store's URL cannot be reached when the page connects: the pipeline
    still starts, so the page sees *why* (the client's own error, named after
    the client) and a refresh finds the store once it is back."""
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    from support import measurements

    client = InMemoryClient("tsdb", Measurement, measurements(10), capabilities=HISTORY)
    original = client.coverage

    async def unreachable(model, mRID=None):
        raise ConnectionError("cannot reach http://tsdb:8100: ConnectError: refused")

    client.coverage = unreachable  # type: ignore[method-assign]
    gateway = DataGateway([client])
    player = Player(gateway, bus, model=Measurement, paced=False)
    with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors, bus.subscribe(
        Measurement, overflow=Overflow.GROW
    ) as frames:
        await player.start()  # does not raise
        try:
            (event,) = await take(errors, 1)
            status = player.status()
            assert event.source == "tsdb" and event.message == "the provider cannot be reached"
            assert event.detail == "tsdb: ConnectionError: cannot reach http://tsdb:8100: ConnectError: refused"
            assert status.error == event.detail and status.paused and status.ended
            assert status.coverage_start is None and status.mode == "replay" and not status.can_seek
            assert gateway.coverage_failures == {"tsdb": "ConnectionError: cannot reach http://tsdb:8100: ConnectError: refused"}
            client.coverage = original  # type: ignore[method-assign]
            await player.refresh()
            assert player.status().error is None and player.status().coverage_start == at(0)
            assert gateway.coverage_failures == {}
            player.resume()
            got = await take(frames, 2)
            assert [m.mRID for m in got] == ["m0", "m1"]
        finally:
            await player.stop()
    # A gateway with no client for the model at all is still a refusal to start.
    with pytest.raises(PlayerError):
        await Player(DataGateway([InMemoryClient("x", NumberOnly, capabilities=HISTORY)]), bus, model=Measurement).start()


class NumberOnly(Measurement):
    """A *sub*class of Measurement: a client declaring only it does not support
    the parent (``supports`` checks ``issubclass(model, declared)``), so a
    player for Measurement over such a gateway has no client at all."""


async def test_a_live_stream_that_ends_is_not_looped():
    class Finite(InMemoryClient):
        """A tail that returns as soon as its backlog is drained."""

        async def consume(self, model, time_range, mRID=None):
            for record in self.records:
                yield record

    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    live = Finite(
        "finite",
        Measurement,
        [measurement(1, utcnow())],
        capabilities=Capability.LIVE_CONSUME,
        coverage_fn=lambda: Coverage(TimeRange(utcnow() - timedelta(seconds=1), None), live=True),
    )
    player = Player(DataGateway([live]), bus, model=Measurement, paced=False, loop=True)
    with bus.subscribe(PlayerStatus, overflow=Overflow.GROW) as statuses, bus.subscribe(
        StreamChanged, overflow=Overflow.GROW
    ) as changes:
        await player.start()
        status = await wait_status(statuses, lambda s: s.ended)
        assert status.mode == "live"
        assert status.paused is True
        await asyncio.sleep(0.02)
        assert len(_drain(changes)) == 1  # the initial open, and no restart
    await player.stop()
