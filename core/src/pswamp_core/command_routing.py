# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""How a command reaches its receiver: routed by class, checked, then applied.

A **receiver** is anything that takes commands -- the player, or a module. It
declares the ``Command`` subclasses it handles (``commands``), checks one
synchronously against its current state (``validate``, raising
``CommandRefused``), and applies it (``handle``). Nothing else about it
matters here.

A command travels in two steps, and they are deliberately split::

    edge ── dispatch(bus, receivers, cmd) ──▶ resolve by class ─▶ receiver.validate(cmd)
                                                                    │ raises CommandRefused → the POST's 409
                                                                    ▼
                                                              bus.publish(cmd)
    bus ──▶ CommandInbox(receiver) ── validate again ─▶ await receiver.handle(cmd)
                                        │ refused or failed → ErrorEvent(request_id) on the bus

**Dispatch** is synchronous: it finds the one receiver that declared the
command's class (narrowed by ``target`` if the command names one), asks it
whether the command applies *now*, and only then publishes. So a caller learns
"no" before anything happened, and "yes" means the command is queued for a
receiver that exists and accepted it.

**The inbox** applies it, one command at a time and in order. It checks again
before applying, because the state may have moved between the two (a seek
dispatched while a switch to live is still queued ahead of it); what it refuses
then, or what fails while applying, becomes an ``ErrorEvent`` carrying the
command's ``request_id`` -- so it reaches the person on the error tray, not
only the log. Its subscription is opened in ``__init__``, synchronously: a
command published right after the inbox exists is never lost to a task that
has not had its first turn yet.

A ``Pipeline`` builds one inbox per receiver and exposes ``dispatch``; a
worker's ``ModuleHost`` builds one per hosted module; a test driving a bare
player builds one itself.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, ClassVar, Protocol

from .bus import Overflow
from .log import get_logger
from .messages.errors import ErrorEvent
from .util.time import utcnow

if TYPE_CHECKING:
    from pydantic import BaseModel

    from .bus import Bus
    from .messages.commands import Command

__all__ = [
    "AmbiguousReceiver",
    "CommandInbox",
    "CommandReceiver",
    "CommandRefused",
    "NoReceiver",
    "RoutingError",
    "dispatch",
    "resolve",
]

logger = get_logger("pswamp_core.command_routing")


class CommandRefused(Exception):
    """The command does not apply in the receiver's current state. The edge
    answers it with a 409; the message says why, for a person."""


class RoutingError(LookupError):
    """The command has no single receiver in this pipeline: a wiring error."""


class NoReceiver(RoutingError):
    """No receiver declared the command's class (or none by its ``target``)."""


class AmbiguousReceiver(RoutingError):
    """Two receivers declared the command's class and it names no ``target``."""


class CommandReceiver(Protocol):
    """What takes commands: the player, a module."""

    #: What ``Command.target`` names it by, and the ``source`` of its errors.
    name: str
    #: The command classes it handles (a base class takes every subclass).
    commands: ClassVar[tuple[type[Command], ...]]

    def validate(self, command: Command) -> None:
        """Raise ``CommandRefused`` if ``command`` does not apply now. Reads
        only in-memory state: it runs inside a request, before publishing."""

    async def handle(self, command: Command) -> BaseModel | None:
        """Apply ``command``. What it returns goes to the inbox's ``on_result``."""


def resolve(receivers: Sequence[CommandReceiver], command: Command) -> CommandReceiver:
    """The one receiver ``command`` is for, by class and then by ``target``."""
    candidates = [r for r in receivers if r.commands and isinstance(command, r.commands)]
    if command.target is not None:
        candidates = [r for r in candidates if r.name == command.target]
    if not candidates:
        where = f" named {command.target!r}" if command.target is not None else ""
        raise NoReceiver(f"no receiver{where} handles {type(command).__name__}")
    if len(candidates) > 1:
        names = ", ".join(r.name for r in candidates)
        raise AmbiguousReceiver(
            f"{type(command).__name__} is handled by {names}; name one in 'target'"
        )
    return candidates[0]


def dispatch(bus: Bus, receivers: Sequence[CommandReceiver], command: Command) -> CommandReceiver:
    """Route, check and publish one command; return the receiver it went to.

    Raises ``RoutingError`` when no single receiver takes it, and
    ``CommandRefused`` when that receiver refuses it now -- in both cases
    nothing is published.
    """
    receiver = resolve(receivers, command)
    receiver.validate(command)
    bus.publish(command)
    return receiver


#: Called with a command and what its receiver's ``handle`` returned, when not ``None``.
OnResult = Callable[["Command", "BaseModel"], None]


class CommandInbox:
    """One receiver's commands, off one bus, applied in order.

    Args:
        bus: Where the commands arrive, and where refusals are reported.
        receiver: Who applies them.
        on_result: What to do with a non-``None`` return from ``handle`` --
            a module wraps it in its result envelope and publishes it.
    """

    def __init__(
        self, bus: Bus, receiver: CommandReceiver, *, on_result: OnResult | None = None
    ) -> None:
        if not receiver.commands:
            raise ValueError(f"{receiver.name} handles no commands")
        self.receiver = receiver
        self._bus = bus
        self._on_result = on_result
        # Subscribed here, synchronously, so nothing published after this
        # line is missed -- whether or not start() has run yet.
        self._subscription = bus.subscribe(*receiver.commands, overflow=Overflow.GROW)
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
        self._subscription.close()

    async def _serve(self) -> None:
        async for command in self._subscription:
            if command.target is not None and command.target != self.receiver.name:
                continue
            await self.apply(command)

    async def apply(self, command: Command) -> None:
        """Check and apply one command; report a refusal or failure on the bus."""
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
        self._bus.publish(
            ErrorEvent(
                timestamp=utcnow(),
                source=self.receiver.name,
                message=message,
                detail=detail,
                request_id=command.request_id,
            )
        )
