# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Commands are routed by class, checked before they are published, and applied
by the receiver's inbox -- which reports what it refuses on the bus."""

from __future__ import annotations

import asyncio

import pytest
from support import Halver, HalveCommand, Measurement, NumberResult, at, take

from pswamp_core.bus import InProcessBus, Overflow
from pswamp_core.command_routing import (
    AmbiguousReceiver,
    CommandInbox,
    CommandRefused,
    NoReceiver,
    dispatch,
    resolve,
)
from pswamp_core.datagateway import DataGateway, Player
from pswamp_core.messages import (
    Command,
    ErrorEvent,
    GoLiveCommand,
    PlayCommand,
    PlayerCommand,
    PlayerStatus,
    SeekCommand,
)
from pswamp_core.pipeline import Pipeline


class Receiver:
    """A bare receiver: records what it is handed, refuses when told to."""

    def __init__(self, name: str, *commands: type[Command], refuse: str | None = None) -> None:
        self.name = name
        self.commands = commands
        self.refuse = refuse
        self.handled: list[Command] = []

    def validate(self, command: Command) -> None:
        if self.refuse:
            raise CommandRefused(self.refuse)

    async def handle(self, command: Command) -> None:
        self.handled.append(command)


def test_a_command_resolves_to_the_receiver_that_declared_its_class():
    player, halver = Receiver("player", PlayerCommand), Receiver("halver", HalveCommand)
    receivers = [player, halver]
    assert resolve(receivers, PlayCommand()) is player  # a base class takes every subclass
    assert resolve(receivers, HalveCommand(value=1)) is halver
    with pytest.raises(NoReceiver):
        resolve(receivers, Command())
    with pytest.raises(NoReceiver):
        resolve(receivers, HalveCommand(value=1, target="player"))


def test_two_receivers_of_one_class_need_a_target():
    a, b = Receiver("a", HalveCommand), Receiver("b", HalveCommand)
    with pytest.raises(AmbiguousReceiver):
        resolve([a, b], HalveCommand(value=1))
    assert resolve([a, b], HalveCommand(value=1, target="b")) is b


def test_a_refused_command_is_never_published():
    bus = InProcessBus()
    refuser = Receiver("halver", HalveCommand, refuse="not now")
    with bus.subscribe(Command) as seen:
        with pytest.raises(CommandRefused, match="not now"):
            dispatch(bus, [refuser], HalveCommand(value=1))
        with pytest.raises(NoReceiver):
            dispatch(bus, [refuser], PlayCommand())
        assert seen.get_nowait() is None
        refuser.refuse = None
        command = HalveCommand(value=1)
        assert dispatch(bus, [refuser], command) is refuser
        assert seen.get_nowait() is command


async def test_an_inbox_applies_only_what_is_for_it_and_hears_commands_published_before_it_runs():
    bus = InProcessBus()
    a, b = Receiver("a", HalveCommand), Receiver("b", HalveCommand)
    inbox_a, inbox_b = CommandInbox(bus, a), CommandInbox(bus, b)
    # Published before either task has run: the subscriptions already exist.
    first, second = HalveCommand(value=1, target="a"), HalveCommand(value=2)
    bus.publish(first)
    bus.publish(second)
    inbox_a.start()
    inbox_b.start()
    await asyncio.sleep(0.01)
    await inbox_a.stop()
    await inbox_b.stop()
    assert a.handled == [first, second]  # untargeted: every inbox of its class
    assert b.handled == [second]


@pytest.fixture
async def pipeline(history_client):
    """A started pipeline: an unpaced player over ten frames, and the Halver."""
    bus = InProcessBus()
    gateway = DataGateway([history_client])
    player = Player(gateway, bus, model=Measurement, paced=False)
    built = Pipeline("k", gateway, bus, player, [Halver()])
    await built.start()
    yield built
    await built.stop()


async def test_a_pipeline_dispatches_to_its_player_and_its_modules(pipeline):
    bus = pipeline.bus
    assert [r.name for r in pipeline.receivers] == ["player", "halver"]
    with bus.subscribe(PlayerStatus, NumberResult, overflow=Overflow.GROW) as updates:
        seek, halve = SeekCommand(offset_s=3), HalveCommand(value=8)
        pipeline.dispatch(seek)
        pipeline.dispatch(halve)
        got = await take(updates, 2)
    status = next(m for m in got if isinstance(m, PlayerStatus))
    result = next(m for m in got if isinstance(m, NumberResult))
    assert status.cursor == at(3)
    assert result.result.value == 4 and result.request_id == halve.request_id


async def test_a_pipeline_refuses_before_publishing_and_reports_what_its_inbox_refuses(pipeline):
    bus = pipeline.bus
    with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
        with pytest.raises(CommandRefused, match="no live source"):
            pipeline.dispatch(GoLiveCommand())
        with pytest.raises(CommandRefused, match="no negatives"):
            pipeline.dispatch(HalveCommand(value=-1))
        with pytest.raises(CommandRefused, match="outside the history"):
            pipeline.dispatch(SeekCommand(offset_s=60))
        assert errors.get_nowait() is None
        # Accepted at dispatch, failing when applied: an ErrorEvent with its request id.
        failing = HalveCommand(value=0)
        pipeline.dispatch(failing)
        (event,) = await take(errors, 1)
    assert (event.source, event.request_id) == ("halver", failing.request_id)
    assert event.detail == "RuntimeError: cannot halve zero"


def test_a_pipeline_refuses_two_receivers_of_one_name(history_client):
    bus = InProcessBus()
    gateway = DataGateway([history_client])
    player = Player(gateway, bus, model=Measurement)
    with pytest.raises(ValueError, match="share a name"):
        Pipeline("k", gateway, bus, player, [Halver(), Halver()])
