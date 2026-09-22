# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The minimal module: consume one message class, produce another.

STEP 1 A3 and A6 in their smallest form. A module declares what it reads
(``input_model``) and what it emits (``output_model``, a ``ResultEnvelope``
subclass whose name is its topic), and implements ``process``. ``run`` does the
rest: subscribe, call, wrap, publish. So a contributor's module is the analysis
and two class attributes.

This is the *coroutine* module -- fine for anything cheap enough to run on the
event loop, which the frame statistics in the streamer are. The desktop
package's ``SnapshotApp``/``TimeWindowApp`` run blocking loops on their own
threads and are bridged, not rewritten (STEP 3 §4.5, ``GatewayIO``); that bridge
is deferred from this slice. When it lands, both kinds publish the same
``ResultEnvelope`` on the same bus, which is what a page subscribes to.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
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
    from .datagateway.data_gateway import DataGateway
    from .messages.data_model import DataModel

__all__ = ["Module"]

logger = get_logger("pswamp_core.modules")


class Module(ABC):
    """Consume ``input_model`` from the bus; publish ``output_model`` results.

    Class attributes a subclass sets:

    * ``name`` -- how the module identifies itself in ``AppIdentity``.
    * ``input_model`` -- the message class to subscribe to.
    * ``output_model`` -- the ``ResultEnvelope`` subclass to publish.
    * ``overflow`` -- what to do when this module falls behind its input;
      ``DROP_OLDEST`` by default, since a module reading a live-rate stream
      should analyse the newest frame rather than an ever-older backlog.

    A module that derives something from the stream's layout reads it off the
    frame in ``process`` (``frame.header``) and re-derives it when the
    ``header_id`` changes. Its input is all it needs, which is what lets the
    same module run in another process (:mod:`pswamp_core.remote`).
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

    async def setup(self, gateway: DataGateway, bus: Bus) -> None:
        """Called once before ``run``: keep the gateway, prime a window, and so on."""
        return

    @abstractmethod
    async def process(self, message: DataModel) -> BaseModel | None:
        """Analyse one input message. Return the result body to publish, or
        ``None`` to publish nothing for this message."""

    async def run(self, bus: Bus) -> None:
        """Subscribe and process until cancelled. What a pipeline runs as a task."""
        with bus.subscribe(self.input_model, overflow=self.overflow, maxsize=self.maxsize) as inputs:
            async for message in inputs:
                try:
                    result = await self.process(message)
                except Exception as error:
                    logger.exception("module %s failed on %s", self.name, type(message).__name__)
                    self.status = AppStatus.UNDEFINED
                    # The same failure, addressed to the client whose pipeline this
                    # is: the log line above is for the operator of the process.
                    bus.publish(
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
                envelope = self.output_model(
                    timestamp=message.timestamp,
                    app=self.identity,
                    parameters=self.parameters,
                    result=result,
                )
                self.status = AppStatus.OK
                self.last_result = envelope
                bus.publish(envelope)
