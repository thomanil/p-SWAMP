# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The minimal module: consume one message class, produce another.

STEP 1 A3 and A6 in their smallest form. A module declares what it reads
(``input_model``) and what it emits (``output_model``, a ``ResultEnvelope``
subclass whose name is its topic), and implements ``process``. ``run`` does the
rest: read, call, wrap, publish. So a contributor's module is the analysis and
two class attributes.

**A module always runs in a host** (:class:`~pswamp_core.host.ModuleHost`),
which feeds it one pipeline key's input off the transport and publishes what
it emits back onto it. It never sees the transport: ``run`` reads an input
queue and publishes into an ``out`` sink, and that is all it knows. Whether the
host is in the server's process or in a worker is the deployment's choice
(which transport it configures), not the module's.

This is the *coroutine* module -- fine for anything cheap enough to run on the
event loop, which the frame statistics in the streamer are. A module whose
analysis blocks runs it in a pool (the mode estimation module does).

**A module that falls behind its input says so**, through its
:class:`~pswamp_core.keep_up.KeepUpMonitor`: dropped input, or input read too
long after it was sent, becomes an ``ErrorEvent`` on the error tray.

**A module may take commands, too.** It lists the concrete ``Command`` classes
it answers in ``commands`` and implements ``handle`` (and ``validate``, if some
of them can be refused). What ``handle`` returns is published the way
``process``'s result is, in the module's ``output_model``, stamped now and
carrying the command's ``request_id``; a refusal comes back as an
``ErrorEvent`` with it. A module that *only* takes commands sets
``input_model = None``.

**A module may read the gateway itself** -- a batch question over a range,
say. ``setup`` hands it the gateway its host built from the same configuration
the pipeline's player reads.
"""

from __future__ import annotations

from abc import ABC
from collections.abc import AsyncIterable
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar
from uuid import uuid4

from pydantic import BaseModel

from .command_routing import CommandInbox
from .keep_up import KeepUp, KeepUpMonitor
from .log import get_logger
from .messages.errors import ErrorEvent
from .messages.results import AppIdentity, AppStatus, ResultEnvelope
from .subscription import Overflow
from .util.time import utcnow

if TYPE_CHECKING:
    from .datagateway.data_gateway import DataGateway
    from .messages.commands import Command
    from .messages.data_model import DataModel
    from .subscription import Sink, Subscription

__all__ = ["KeepUp", "KeepUpMonitor", "Module"]

logger = get_logger("pswamp_core.modules")


class Module(ABC):
    """Consume ``input_model``; publish ``output_model`` results.

    Class attributes a subclass sets:

    * ``name`` -- how the module identifies itself in ``AppIdentity`` and on
      its error reports.
    * ``input_model`` -- the message class to read; ``None`` for a module that
      only answers commands.
    * ``output_model`` -- the ``ResultEnvelope`` subclass to publish.
    * ``commands`` -- the concrete ``Command`` classes it answers through
      ``handle``; empty for a module that takes none.
    * ``overflow`` / ``maxsize`` -- its input queue: what to do when this module
      falls behind; ``DROP_OLDEST`` by default, since a module reading a
      live-rate stream should analyse the newest frame rather than an
      ever-older backlog.
    * ``keep_up`` -- when falling behind is reported as an ``ErrorEvent``
      (:class:`~pswamp_core.keep_up.KeepUp`); ``None`` for a module that must
      never report itself.

    A module that derives something from the stream's layout reads it off the
    frame in ``process`` (``frame.header``) and re-derives it when the
    ``header_id`` changes. Its input is all it needs.
    """

    name: ClassVar[str] = "module"
    input_model: ClassVar[type[DataModel] | None]
    output_model: ClassVar[type[ResultEnvelope]]
    commands: ClassVar[tuple[type[Command], ...]] = ()
    overflow: ClassVar[Overflow] = Overflow.DROP_OLDEST
    maxsize: ClassVar[int] = 64
    keep_up: ClassVar[KeepUp | None] = KeepUp()

    def __init__(self) -> None:
        self.identity = AppIdentity(name=self.name, uuid=uuid4().hex)
        self.status = AppStatus.INITIALIZING
        self.parameters: dict[str, Any] = {}
        self.last_result: ResultEnvelope | None = None
        self.monitor = KeepUpMonitor(
            self.name,
            f"is not keeping up with {self.input_model.topic}" if self.input_model else "",
            self.keep_up if self.input_model else None,
        )

    async def setup(self, gateway: DataGateway, out: Sink) -> None:
        """Called once before ``run``: keep the gateway, keep ``out`` for
        reports made outside ``process``, prime a window, and so on."""
        return

    async def process(self, message: DataModel) -> BaseModel | None:
        """Analyse one input message. Return the result body to publish, or
        ``None`` to publish nothing for this message. Every module with an
        ``input_model`` implements it."""
        raise NotImplementedError(f"{type(self).__name__} has an input_model but no process()")

    def validate(self, command: Command) -> None:
        """Raise ``CommandRefused`` if ``command`` does not apply now; state only."""
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

    def command_inbox(self, commands: AsyncIterable[Command], out: Sink) -> CommandInbox:
        """This module's commands, applied in order: each answer wrapped and
        published into ``out``, each refusal an ``ErrorEvent`` there."""

        def answer(command: Command, body: BaseModel) -> None:
            out.publish(self.wrap(body, timestamp=utcnow(), request_id=command.request_id))

        return CommandInbox(commands, self, out, on_result=answer)

    async def run(self, inputs: Subscription, out: Sink) -> None:
        """Read and process until cancelled; what a host runs as a task. A
        module with no ``input_model`` has nothing to read and returns at once;
        its commands come through its inbox."""
        if self.input_model is None:
            return
        async for message in inputs:
            self.monitor.observe(inputs, message, out)
            try:
                result = await self.process(message)
            except Exception as error:
                logger.exception("module %s failed on %s", self.name, type(message).__name__)
                self.status = AppStatus.UNDEFINED
                # The same failure, addressed to the client whose pipeline this
                # is: the log line above is for the operator of the process.
                out.publish(
                    ErrorEvent(
                        timestamp=utcnow(),
                        source=self.name,
                        message=f"module {self.name} failed on {type(message).__name__}",
                        detail=f"{type(error).__name__}: {error}",
                        request_id=getattr(message, "request_id", None),
                    )
                )
                continue
            if result is None:
                continue
            out.publish(self.wrap(result, timestamp=message.timestamp))
