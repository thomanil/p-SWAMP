# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""How a command reaches its receiver: its class is its address.

A **receiver** is anything that takes commands -- the player, or a module. It
declares the concrete ``Command`` classes it handles (``commands``), checks one
synchronously against its current state (``validate``, raising
``CommandRefused``), and applies it (``handle``). A command class has exactly
one receiver in a pipeline family (checked when the family is declared), and
it travels on its own topic, so routing a command *is* publishing it::

    edge ── pipeline.dispatch(cmd) ── player command: player.validate(cmd)
                                        │ raises CommandRefused → the POST's 409
                                        ▼
                                   topic <app>.<command> ──▶ CommandInbox(receiver)
                                        validate again ─▶ await receiver.handle(cmd)
                                        │ refused or failed → ErrorEvent(request_id)

The player lives with the edge, so a player command is checked before it is
published and a refusal is the POST's 409. A module may run in another
process, so its commands are accepted and checked where it runs: a refusal
there is an ``ErrorEvent`` carrying the command's ``request_id``, which reaches
the person on the error tray.

**The inbox** applies one receiver's commands in order, checking each again
before applying, because the state may have moved between dispatch and
delivery (a seek dispatched while a switch to live is still queued ahead of
it).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterable, Callable, Sequence
from typing import TYPE_CHECKING, ClassVar, Protocol

from .log import get_logger
from .messages.errors import ErrorEvent
from .util.time import utcnow

if TYPE_CHECKING:
    from pydantic import BaseModel

    from .messages.commands import Command
    from .subscription import Sink

__all__ = [
    "CommandInbox",
    "CommandReceiver",
    "CommandRefused",
    "NoReceiver",
    "concrete_commands",
]

logger = get_logger("pswamp_core.command_routing")


class CommandRefused(Exception):
    """The command does not apply in the receiver's current state. The edge
    answers it with a 409; the message says why, for a person."""


class NoReceiver(LookupError):
    """Nothing in the pipeline takes the command's class: a wiring error."""


class CommandReceiver(Protocol):
    """What the inbox needs of whoever applies commands."""

    name: str
    commands: ClassVar[tuple[type[Command], ...]]

    def validate(self, command: Command) -> None: ...

    async def handle(self, command: Command) -> BaseModel | None: ...


def concrete_commands(owner: str, commands: Sequence[type[Command]]) -> tuple[type[Command], ...]:
    """``commands``, checked to be concrete: a topic carries one class, so a
    base class declared as a command would hear none of its subclasses."""
    for command in commands:
        if command.__subclasses__():
            raise ValueError(
                f"{owner} declares {command.__name__}, which has subclasses; "
                "list the concrete command classes, since a topic carries one class"
            )
    return tuple(commands)


#: Called with a command and what its receiver's ``handle`` returned, when not ``None``.
OnResult = Callable[["Command", "BaseModel"], None]


class CommandInbox:
    """One receiver's commands, applied in order.

    Args:
        commands: Where the commands arrive: an async iterable of them.
        receiver: Who applies them.
        out: Where a refusal or failure is reported, as an ``ErrorEvent``.
        on_result: What to do with a non-``None`` return from ``handle`` --
            a module wraps it in its result envelope and publishes it.
    """

    def __init__(
        self,
        commands: AsyncIterable[Command],
        receiver: CommandReceiver,
        out: Sink,
        *,
        on_result: OnResult | None = None,
    ) -> None:
        if not receiver.commands:
            raise ValueError(f"{receiver.name} handles no commands")
        self.receiver = receiver
        self._commands = commands
        self._out = out
        self._on_result = on_result
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._serve(), name=f"{self.receiver.name}.commands")

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _serve(self) -> None:
        async for command in self._commands:
            await self.apply(command)

    async def apply(self, command: Command) -> None:
        """Check and apply one command; report a refusal or failure."""
        name = self.receiver.name
        try:
            self.receiver.validate(command)
            result = await self.receiver.handle(command)
        except CommandRefused as refused:
            logger.warning("%s refused %s (request %s): %s", name, command.name, command.request_id, refused)
            self._report(command, f"{name} refused {command.name}", str(refused))
            return
        except Exception as error:
            logger.exception("%s failed to apply %s (request %s)", name, command.name, command.request_id)
            self._report(command, f"{name} failed to apply {command.name}", f"{type(error).__name__}: {error}")
            return
        logger.info("%s applied %s (request %s)", name, command.name, command.request_id)
        if result is not None and self._on_result is not None:
            self._on_result(command, result)

    def _report(self, command: Command, message: str, detail: str) -> None:
        self._out.publish(
            ErrorEvent(
                timestamp=utcnow(),
                source=self.receiver.name,
                message=message,
                detail=detail,
                request_id=command.request_id,
            )
        )
