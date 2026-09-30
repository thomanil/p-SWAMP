# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A command's class is its address: one receiver per class in a family, every
declared class concrete, and an inbox that applies them in order and reports
what it refuses. (Dispatch through a pipeline is in ``test_pipeline.py``.)"""

from __future__ import annotations

import asyncio

import pytest
from support import Halver, HalveCommand, Measurement, NumberResult, Tap

from pswamp_core.command_routing import CommandInbox, CommandRefused, concrete_commands
from pswamp_core.datagateway import DataGateway
from pswamp_core.datagateway.player import PLAYER_COMMANDS
from pswamp_core.messages import Command, ErrorEvent, PlayerCommand, SeekCommand
from pswamp_core.modules import Module
from pswamp_core.pipeline import PipelineFamily
from pswamp_core.subscription import Overflow


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


def _concrete(base: type) -> set[type]:
    found = set()
    for sub in base.__subclasses__():
        found |= _concrete(sub) if sub.__subclasses__() else {sub}
    return found


def test_the_player_takes_every_concrete_player_command():
    assert set(PLAYER_COMMANDS) == _concrete(PlayerCommand)


def test_a_declared_command_must_be_concrete():
    assert concrete_commands("halver", (HalveCommand,)) == (HalveCommand,)
    with pytest.raises(ValueError, match="has subclasses"):
        concrete_commands("greedy", (PlayerCommand,))


class OtherHalver(Halver):
    name = "other-halver"


class SeekingModule(Module):
    name = "seeker"
    input_model = None
    output_model = NumberResult
    commands = (SeekCommand,)


def test_a_family_has_one_receiver_per_command_class():
    family = PipelineFamily("t", DataGateway, (Halver,))
    assert family.module_for(HalveCommand) is Halver
    assert family.module_for(SeekCommand) is None  # the player's
    with pytest.raises(ValueError, match="both take HalveCommand"):
        PipelineFamily("t", DataGateway, (Halver, OtherHalver))
    with pytest.raises(ValueError, match="both take SeekCommand"):
        PipelineFamily("t", DataGateway, (SeekingModule,))


async def test_an_inbox_applies_in_order_and_reports_a_refusal_with_its_request_id():
    commands, out = Tap(), Tap()
    receiver = Receiver("halver", HalveCommand)
    inbox = CommandInbox(commands.subscribe(HalveCommand, overflow=Overflow.GROW), receiver, out)
    # Published before the inbox runs: the queue already exists.
    first, second = HalveCommand(value=1), HalveCommand(value=2)
    commands.publish(first)
    commands.publish(second)
    inbox.start()
    await asyncio.sleep(0.01)
    receiver.refuse = "not now"
    refused = HalveCommand(value=3)
    commands.publish(refused)
    await asyncio.sleep(0.01)
    await inbox.stop()
    assert receiver.handled == [first, second]
    (report,) = [m for m in out.published if isinstance(m, ErrorEvent)]
    assert (report.source, report.request_id, report.detail) == ("halver", refused.request_id, "not now")


def test_an_inbox_needs_a_receiver_that_takes_commands():
    with pytest.raises(ValueError, match="handles no commands"):
        CommandInbox(Tap().subscribe(Measurement), Receiver("mute"), Tap())
