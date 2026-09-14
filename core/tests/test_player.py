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
from pswamp_core.messages import Command, PlayerStatus, StreamChanged
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
