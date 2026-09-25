# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The row-count module: a *batch* module, driven by a command rather than by frames.

The other example modules (the streamer's stats, frequency peek) read every
``PmuFrame`` the player paces onto the bus. This one reads nothing off the bus
(``input_model = None``): it answers a ``CountRangeCommand`` -- defined here,
beside the module, as its result is -- by asking the *gateway* for that range
itself, unpaced, and counting what comes back. That is the "query a chunk"
case: a bounded read that never goes through the player, answered as a
message.

It is the worked example of a module that takes commands, and all of it is
declaration: ``commands`` names the class, ``handle`` returns the body, and the
pipeline routes the command here by its class and publishes the answer in a
``RowCountResult`` stamped now and carrying the command's ``request_id``
(``Module.command_inbox``). ``reads_gateway`` says it reads the gateway
itself, which is why it cannot run in a worker.

**Failure is a result and an error event.** A provider that fails mid-count
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
from typing import ClassVar, Literal

from pydantic import BaseModel, Field, model_validator

from pswamp_core.bus import Bus
from pswamp_core.datagateway import DataGateway
from pswamp_core.log import get_logger
from pswamp_core.messages import AppStatus, Command, ErrorEvent, PmuFrame, ResultEnvelope
from pswamp_core.modules import Module
from pswamp_core.util.time import ensure_utc, utcnow

__all__ = ["CountRangeCommand", "RowCount", "RowCountModule", "RowCountResult"]

logger = get_logger("time-series-explorer.row-count")


class CountRangeCommand(Command):
    """Count the frames in ``[start, end)``: the row-count module's one command."""

    start: datetime = Field(description="Inclusive start of the range to count.")
    end: datetime = Field(description="Exclusive end of the range to count.")

    @model_validator(mode="after")
    def _ordered(self) -> CountRangeCommand:
        self.start, self.end = ensure_utc(self.start), ensure_utc(self.end)
        if self.end <= self.start:
            raise ValueError("end must be after start")
        return self


class RowCount(BaseModel):
    """How many frames the provider returned for a range, and how long it took."""

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
    input_model = None
    output_model = RowCountResult
    commands: ClassVar[tuple[type[Command], ...]] = (CountRangeCommand,)
    reads_gateway = True

    def __init__(self) -> None:
        super().__init__()
        self._gateway: DataGateway | None = None
        self._bus: Bus | None = None

    async def setup(self, gateway: DataGateway, bus: Bus) -> None:
        """Keep the gateway, which the count reads, and the bus, for its errors."""
        self._gateway = gateway
        self._bus = bus

    async def handle(self, command: CountRangeCommand) -> RowCount:
        result = await self.count(command.start, command.end)
        if result.error is None:
            logger.info(
                "request %s: %d frame(s) in [%s, %s) in %.3fs",
                command.request_id, result.count, result.start.isoformat(),
                result.end.isoformat(), result.elapsed_s,
            )
        else:
            logger.error("request %s: count failed: %s", command.request_id, result.error)
            if self._bus is not None:
                self._bus.publish(
                    ErrorEvent(
                        timestamp=utcnow(),
                        source=self.name,
                        message="the row count did not complete: its provider failed",
                        detail=result.error,
                        request_id=command.request_id,
                    )
                )
        return result

    def wrap(self, body: BaseModel, *, timestamp: datetime, request_id: str | None = None) -> ResultEnvelope:
        """The base envelope, with the status saying whether the count completed."""
        envelope = super().wrap(body, timestamp=timestamp, request_id=request_id)
        if getattr(body, "error", None) is not None:
            self.status = AppStatus.UNDEFINED
        return envelope

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
