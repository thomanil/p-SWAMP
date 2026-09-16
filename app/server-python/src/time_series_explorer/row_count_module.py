# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The row-count module: a *batch* module, driven by a command rather than by frames.

The other example modules (the streamer's stats, frequency peek) read every
``PmuFrame`` the player paces onto the bus. This one reads nothing until a
``Command`` addressed to it arrives -- ``target="row-count"``, ``verb="count"``,
``args={"start", "end"}`` -- and then asks the *gateway* for that range itself,
unpaced, counts what comes back, and publishes one ``RowCountResult`` carrying
the command's ``request_id``. That is the "query a chunk" case of STEP 1 A5:
a bounded read that never goes through the player, answered as a message.

Two things it shows that ``Module.run`` does not do for a module today:

* **A command as input.** ``Module.run`` wraps a result in an envelope stamped
  with the *input's* timestamp, and a ``Command`` has none -- so this module
  overrides ``run`` and stamps its result with the wall clock. It also filters
  on ``target``: every command on the bus reaches every subscriber, and the
  player's are not for us.
* **Failure as a result and as an error event.** A store that fails mid-count
  produces a result with ``error`` set (the page shows it beside the count) and
  an ``ErrorEvent`` on the bus (the layout shows it wherever the person is),
  both carrying the ``request_id``. The module itself stays up.

The count is the whole analysis, on purpose: what is being demonstrated is a
module that pulls a range on demand, not what it computes over it.

Imports only ``pswamp_core``.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, Field

from pswamp_core.bus import Bus, Overflow
from pswamp_core.datagateway import DataGateway
from pswamp_core.log import get_logger
from pswamp_core.messages import AppStatus, Command, ErrorEvent, PmuFrame, ResultEnvelope
from pswamp_core.modules import Module
from pswamp_core.util.time import ensure_utc, utcnow

__all__ = ["RowCount", "RowCountModule", "RowCountResult"]

logger = get_logger("time-series-explorer.row-count")


class RowCount(BaseModel):
    """How many frames the store returned for a range, and how long it took."""

    start: datetime = Field(description="Inclusive start of the range counted.")
    end: datetime = Field(description="Exclusive end of the range counted.")
    count: int = Field(ge=0, description="Frames received; partial when 'error' is set.")
    elapsed_s: float = Field(ge=0, description="Wall-clock seconds the query took.")
    error: str | None = Field(
        default=None, description="Why the count stopped early ('Type: text'); null when it completed."
    )


class RowCountResult(ResultEnvelope[RowCount]):
    """The module's envelope; its class name is its topic: ``row.count.result``."""

    version: Literal["v1"] = "v1"


class RowCountModule(Module):
    """Count the frames in ``[start, end)`` when told to, straight off the gateway."""

    name = "row-count"
    #: The ``Command.target`` that addresses this module. A fixed name, not the
    #: per-instance uuid: the edge that POSTs knows the name and nothing else.
    target: ClassVar[str] = "row-count"
    input_model = Command
    output_model = RowCountResult
    overflow = Overflow.GROW  # a command is never dropped for being late

    def __init__(self) -> None:
        super().__init__()
        self._gateway: DataGateway | None = None

    async def setup(self, gateway: DataGateway, bus: Bus) -> None:
        """Keep the gateway: the count reads the range from it, not from the bus."""
        self._gateway = gateway

    async def process(self, message: Command) -> BaseModel | None:
        """Unused: ``run`` is overridden below, because a command needs the result
        stamped with the wall clock and tagged with its ``request_id``."""
        return None

    async def run(self, bus: Bus) -> None:
        with bus.subscribe(Command, overflow=self.overflow) as commands:
            async for command in commands:
                if command.target != self.target or command.verb != "count":
                    continue
                result = await self.count_from_args(command.args)
                envelope = RowCountResult(
                    timestamp=utcnow(),
                    app=self.identity,
                    parameters=self.parameters,
                    request_id=command.request_id,
                    result=result,
                )
                if result.error is None:
                    self.status = AppStatus.OK
                    logger.info(
                        "request %s: %d frame(s) in [%s, %s) in %.3fs",
                        command.request_id, result.count, result.start.isoformat(),
                        result.end.isoformat(), result.elapsed_s,
                    )
                else:
                    self.status = AppStatus.UNDEFINED
                    logger.error("request %s: count failed: %s", command.request_id, result.error)
                    bus.publish(
                        ErrorEvent(
                            timestamp=utcnow(),
                            source=self.name,
                            message="the row count did not complete: its provider failed",
                            detail=result.error,
                            request_id=command.request_id,
                        )
                    )
                self.last_result = envelope
                bus.publish(envelope)

    async def count_from_args(self, args: dict[str, Any]) -> RowCount:
        """The count for a command's ``args``; bad arguments are a result with ``error``."""
        try:
            start = _instant(args["start"])
            end = _instant(args["end"])
        except (KeyError, TypeError, ValueError) as error:
            now = utcnow()
            return RowCount(start=now, end=now, count=0, elapsed_s=0.0, error=f"bad range: {error}")
        return await self.count(start, end)

    async def count(self, start: datetime, end: datetime) -> RowCount:
        """Read ``[start, end)`` from the gateway and count it. Never raises."""
        if self._gateway is None:
            raise RuntimeError("count before setup: the module has no gateway")
        began = time.monotonic()
        n = 0
        error: str | None = None
        try:
            # Coverage first: the gateway skips a provider whose coverage call
            # fails (logging the cause), and a range over no provider would
            # otherwise count to zero with nothing to say why.
            if await self._gateway.coverage(PmuFrame) is None:
                raise RuntimeError("the provider reports no coverage; is it reachable?")
            async for _frame in self._gateway.consume(PmuFrame, start, end):
                n += 1
        except Exception as failure:
            error = f"{type(failure).__name__}: {failure}"
        return RowCount(start=start, end=end, count=n, elapsed_s=time.monotonic() - began, error=error)


def _instant(value: Any) -> datetime:
    return ensure_utc(value if isinstance(value, datetime) else datetime.fromisoformat(str(value)))
