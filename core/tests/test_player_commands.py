# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The player as a command receiver: commands over the bus drive it, and what
does not apply in its mode is refused -- at dispatch, or by its inbox with the
command's request id."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from support import Measurement, take

from pswamp_core.bus import InProcessBus, Overflow
from pswamp_core.command_routing import CommandInbox, CommandRefused
from pswamp_core.datagateway import Capability, Coverage, DataGateway, Player, TimeRange
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.messages import (
    ErrorEvent,
    GoLiveCommand,
    PauseCommand,
    PlayCommand,
    PlayerStatus,
    ReplayCommand,
    SeekCommand,
    SpeedCommand,
    StepCommand,
)
from pswamp_core.util.time import utcnow


@pytest.fixture
async def rig(history_client):
    """An unpaced player over ten one-second-apart frames, its commands arriving
    off the bus through its inbox, as in a pipeline."""
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    player = Player(DataGateway([history_client]), bus, model=Measurement, paced=False)
    inbox = CommandInbox(bus, player)
    inbox.start()
    yield bus, player
    await inbox.stop()
    await player.stop()


async def wait_status(sub, predicate, timeout: float = 2.0) -> PlayerStatus:
    async def _wait():
        while True:
            message = await sub.get()
            if isinstance(message, PlayerStatus) and predicate(message):
                return message

    return await asyncio.wait_for(_wait(), timeout)


async def test_seek_by_offset_command(rig):
    bus, player = rig
    await player.start()
    with bus.subscribe(Measurement) as frames:
        bus.publish(SeekCommand(offset_s=7))
        bus.publish(StepCommand())
        (frame,) = await take(frames, 1)
    assert frame.mRID == "m7"




async def test_commands_over_the_bus_drive_the_player(rig):
    bus, player = rig
    await player.start()
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames, bus.subscribe(
        PlayerStatus
    ) as statuses:
        bus.publish(SpeedCommand(speed=2.0))
        status = await wait_status(statuses, lambda s: s.speed == 2.0)
        assert status.paused is True
        bus.publish(PlayCommand())
        await wait_status(statuses, lambda s: not s.paused)
        got = await take(frames, 3)
        assert [m.mRID for m in got] == ["m0", "m1", "m2"]
        bus.publish(PauseCommand())
        await wait_status(statuses, lambda s: s.paused)





async def test_a_bounded_replay_by_offsets_in_one_command(rig):
    bus, player = rig
    await player.start()
    with bus.subscribe(Measurement, overflow=Overflow.GROW) as frames:
        bus.publish(ReplayCommand(offset_s=2, end_offset_s=4, play=True))
        got = await take(frames, 2)
    assert [m.mRID for m in got] == ["m2", "m3"]


async def test_validate_refuses_what_the_mode_does_not_allow(rig):
    _, player = rig
    await player.start()
    with pytest.raises(CommandRefused, match="no live source"):
        player.validate(GoLiveCommand())
    with pytest.raises(CommandRefused, match="outside the history"):
        player.validate(SeekCommand(offset_s=60))
    player.validate(SeekCommand(offset_s=3))  # fine


async def test_in_live_mode_the_inbox_refuses_transport_commands_with_their_request_id():
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    live = InMemoryClient(
        "live",
        Measurement,
        capabilities=Capability.LIVE_CONSUME,
        coverage_fn=lambda: Coverage(TimeRange(utcnow() - timedelta(seconds=1), None), live=True),
    )
    player = Player(DataGateway([live]), bus, model=Measurement, paced=False)
    inbox = CommandInbox(bus, player)
    inbox.start()
    await player.start()  # live only: starts live
    try:
        with pytest.raises(CommandRefused, match="live mode"):
            player.validate(PauseCommand())
        with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            seek, pause = SeekCommand(offset_s=0), PauseCommand()
            bus.publish(seek)
            bus.publish(pause)
            reports = await take(errors, 2)
        assert [e.request_id for e in reports] == [seek.request_id, pause.request_id]
        assert all(e.source == "player" for e in reports)
        assert player.mode == "live" and player.paused is False
    finally:
        await inbox.stop()
        await player.stop()
