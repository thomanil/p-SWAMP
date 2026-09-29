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
"""

from __future__ import annotations

from abc import ABC
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar
from uuid import uuid4

from pydantic import BaseModel

from .bus import Overflow
from .log import get_logger
from .messages.errors import ErrorEvent
from .messages.results import AppIdentity, AppStatus, ResultEnvelope
from .util.time import utcnow

if TYPE_CHECKING:
    from .bus import Bus
    from .messages.data_model import DataModel

__all__ = ["Module"]

logger = get_logger("pswamp_core.modules")


class Module(ABC):
    """Consume ``input_model`` from the bus; publish ``output_model`` results.

    Class attributes a subclass sets:

    * ``name`` -- how the module identifies itself in its results.
    * ``input_model`` -- the message class to subscribe to.
    * ``output_model`` -- the ``ResultEnvelope`` subclass to publish.
    * ``overflow`` / ``maxsize`` -- what to do when the module falls behind its
      input; ``DROP_OLDEST`` by default, so a module on a live stream analyses
      the newest frame rather than an ever-older backlog.
    """

    name: ClassVar[str] = "module"
    input_model: ClassVar[type[DataModel]]
    output_model: ClassVar[type[ResultEnvelope]]
    overflow: ClassVar[Overflow] = Overflow.DROP_OLDEST
    maxsize: ClassVar[int] = 64

    def __init__(self) -> None:
        self.identity = AppIdentity(name=self.name, uuid=uuid4().hex)
        self.status = AppStatus.INITIALIZING
        self.parameters: dict[str, Any] = {}
        self.last_result: ResultEnvelope | None = None

    async def process(self, message: DataModel) -> BaseModel | None:
        """Analyse one input message. Return the result body to publish, or
        ``None`` to publish nothing for this message."""
        raise NotImplementedError(f"{type(self).__name__} has no process()")

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

    async def run(self, bus: Bus) -> None:
        """Subscribe and process until cancelled. What a pipeline runs as a task."""
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
