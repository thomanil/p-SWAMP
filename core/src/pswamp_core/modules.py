# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The module contract: consume one message class, publish another.

A module declares what it reads (``input_model``) and what it emits
(``output_model``, a ``ResultEnvelope`` subclass whose name is its topic), and
implements ``process``. ``run`` does the rest: subscribe, call, wrap, publish.
So a contributor's module is the analysis and two class attributes::

    class FrameStatsModule(Module):
        name = "frame-stats"
        input_model = PmuFrame
        output_model = FrameStatsResult

        async def process(self, frame: PmuFrame) -> FrameStats | None: ...

A module sees only the bus. Its input is all it needs (a ``PmuFrame`` carries
its layout), which is what lets the same class run in-process or as its own
service without a change.

``process`` runs on the event loop, so it must be cheap or hand heavy work to
a thread or process pool. A ``process`` that raises publishes an
``ErrorEvent``; the module carries on with the next message.

**A module may take commands, too.** It lists the ``Command`` subclasses it
answers in ``commands`` and implements ``handle`` (and ``validate``, if some
can be refused); the pipeline routes each to it by class
(:mod:`pswamp_core.command_routing`). What ``handle`` returns is published in
the module's ``output_model``, carrying the command's ``request_id``. A module
that *only* takes commands sets ``input_model = None``.

**A module may read the gateway itself** -- a batch query over a chunk, say,
answering a command rather than reading frames. It sets ``reads_gateway`` and
keeps the gateway the pipeline hands it in ``setup``. Such a module runs in the
pipeline's process only: a worker has no providers, and refuses to host it.
"""

from __future__ import annotations

from abc import ABC
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar
from uuid import uuid4

from pydantic import BaseModel

from .bus import Overflow
from .command_routing import CommandInbox
from .log import get_logger
from .messages.errors import ErrorEvent
from .messages.results import AppIdentity, AppStatus, ResultEnvelope
from .util.time import utcnow

if TYPE_CHECKING:
    from .bus import Bus
    from .datagateway.data_gateway import DataGateway
    from .messages.commands import Command
    from .messages.data_model import DataModel

__all__ = ["Module"]

logger = get_logger("pswamp_core.modules")


class Module(ABC):
    """Consume ``input_model`` from the bus; publish ``output_model`` results.

    Class attributes a subclass sets:

    * ``name`` -- how the module identifies itself in its results and errors.
    * ``input_model`` -- the message class to subscribe to; ``None`` for a
      module that only answers commands.
    * ``output_model`` -- the ``ResultEnvelope`` subclass to publish.
    * ``commands`` -- the ``Command`` subclasses it answers through ``handle``.
    * ``reads_gateway`` -- ``True`` for a module that reads the gateway itself
      (kept in ``setup``); such a module cannot run in a worker.
    * ``overflow`` / ``maxsize`` -- what to do when the module falls behind its
      input; ``DROP_OLDEST`` by default, so a module on a live stream analyses
      the newest frame rather than an ever-older backlog.
    """

    name: ClassVar[str] = "module"
    input_model: ClassVar[type[DataModel] | None]
    output_model: ClassVar[type[ResultEnvelope]]
    commands: ClassVar[tuple[type[Command], ...]] = ()
    reads_gateway: ClassVar[bool] = False
    overflow: ClassVar[Overflow] = Overflow.DROP_OLDEST
    maxsize: ClassVar[int] = 64

    def __init__(self) -> None:
        self.identity = AppIdentity(name=self.name, uuid=uuid4().hex)
        self.status = AppStatus.INITIALIZING
        self.parameters: dict[str, Any] = {}
        self.last_result: ResultEnvelope | None = None

    async def setup(self, gateway: DataGateway, bus: Bus) -> None:
        """Called by the pipeline before the module runs. A module that reads
        the gateway keeps it here; the default does nothing."""
        return

    async def process(self, message: DataModel) -> BaseModel | None:
        """Analyse one input message. Return the result body to publish, or
        ``None`` to publish nothing for this message."""
        raise NotImplementedError(f"{type(self).__name__} has no process()")

    def validate(self, command: Command) -> None:
        """Raise ``CommandRefused`` if ``command`` does not apply now. Runs inside
        the request that dispatched it, so it reads in-memory state only."""
        return

    async def handle(self, command: Command) -> BaseModel | None:
        """Answer one of ``commands``. Return the result body to publish (it
        carries the command's ``request_id``), or ``None`` for nothing."""
        raise NotImplementedError(f"{type(self).__name__} declares commands but no handle()")

    def wrap(self, body: BaseModel, *, timestamp: datetime, request_id: str | None = None) -> ResultEnvelope:
        """``body`` in this module's envelope, recorded as its latest result."""
        envelope = self.output_model(
            timestamp=timestamp,
            app=self.identity,
            parameters=self.parameters,
            request_id=request_id,
            result=body,
        )
        self.status = AppStatus.OK
        self.last_result = envelope
        return envelope

    def command_inbox(self, bus: Bus) -> CommandInbox:
        """This module's commands off ``bus``: each answer wrapped and published."""

        def answer(command: Command, body: BaseModel) -> None:
            bus.publish(self.wrap(body, timestamp=utcnow(), request_id=command.request_id))

        return CommandInbox(bus, self, on_result=answer)

    async def run(self, bus: Bus) -> None:
        """Subscribe and process until cancelled. What a pipeline runs as a task.
        A module with no ``input_model`` returns at once; its commands come
        through its inbox."""
        if self.input_model is None:
            return
        with bus.subscribe(self.input_model, overflow=self.overflow, maxsize=self.maxsize) as inputs:
            async for message in inputs:
                try:
                    result = await self.process(message)
                except Exception as error:
                    logger.exception("module %s failed on %s", self.name, type(message).__name__)
                    self.status = AppStatus.UNDEFINED
                    bus.publish(
                        ErrorEvent(
                            timestamp=utcnow(),
                            source=self.name,
                            message=f"module {self.name} failed on {type(message).__name__}",
                            detail=f"{type(error).__name__}: {error}",
                        )
                    )
                    continue
                if result is None:
                    continue
                bus.publish(self.wrap(result, timestamp=message.timestamp))
