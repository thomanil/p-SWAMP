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

**A module that falls behind its input says so.** ``run`` watches its own input
queue: messages the queue dropped (``DROP_OLDEST`` makes room by discarding,
which is right for a live stream and silent unless someone counts), and, for
input that crossed a transport, how old each message is when it is read
(:func:`~pswamp_core.messages.sent_at`). Past the module's ``keep_up`` policy
it publishes an ``ErrorEvent`` -- once when it falls behind, again at most every
``report_every_s`` while it stays there, and once more when it has caught up --
so the person whose pipeline it is sees it on the error tray rather than only
in a log.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar
from uuid import uuid4

from pydantic import BaseModel

from .bus import Overflow
from .log import get_logger
from .messages.data_model import sent_at
from .messages.errors import ErrorEvent
from .messages.results import AppIdentity, AppStatus, ResultEnvelope
from .util.time import utcnow

if TYPE_CHECKING:
    from .bus import Bus, Subscription
    from .datagateway.data_gateway import DataGateway
    from .messages.data_model import DataModel

__all__ = ["KeepUp", "KeepUpMonitor", "Module"]

logger = get_logger("pswamp_core.modules")


@dataclass(frozen=True)
class KeepUp:
    """When a module counts as not keeping up with its input, and how often it
    says so.

    Attributes:
        max_input_age_s: An input older than this when the module reads it
            (measured only on input that crossed a transport) means the module
            is behind. Dropped input always does.
        report_every_s: While behind, at most one report per this interval;
            a whole interval with nothing dropped and nothing too old is what
            "caught up" means.
    """

    max_input_age_s: float = 2.0
    report_every_s: float = 5.0


class KeepUpMonitor:
    """Watches one input subscription and reports falling behind as ``ErrorEvent``.

    ``observe`` is called once per message read, before the module processes
    it. It keeps the readings a module may put into its own result
    (``input_dropped``, ``input_age_s``) and, under a ``KeepUp`` policy,
    publishes on the module's bus: on falling behind, every ``report_every_s``
    while behind (with that interval's counts), and on catching up.

    It judges only when a message arrives, so a stream that stops while behind
    reports its catching up on the next message rather than on a timer.

    ``note`` is the same judgement over counts the caller supplies, for
    falling behind that is not an input queue's: a module whose analysis
    skips evaluations it had no time for counts those, with ``unit`` naming
    them in the report.

    Args:
        source: The ``source`` the reports carry; the module's ``name``, which
            is also what a ``RemoteModule`` filters the error topic on.
        what: How the report reads after the label, e.g. ``is not keeping up
            with pmu.frame``.
        policy: The thresholds; ``None`` keeps the readings and reports nothing.
        label: Who the report names, when that is not just ``source`` -- the
            host's shared feed and the server-side publisher report under the
            module's ``source`` but are not the module.
        unit: What the dropped count counts, as the report words it.
    """

    def __init__(
        self,
        source: str,
        what: str,
        policy: KeepUp | None,
        *,
        label: str | None = None,
        unit: str = "input dropped",
    ) -> None:
        self.source = source
        self.label = label or source
        self.what = what
        self.policy = policy
        self.unit = unit
        #: Total input dropped by the subscription so far.
        self.input_dropped = 0
        #: Age of the last input read, in seconds; ``None`` if it never crossed a transport.
        self.input_age_s: float | None = None
        #: Reports published so far (falling behind, still behind, caught up).
        self.reports = 0
        self.behind = False
        self._behind_since = 0.0
        self._last_report = 0.0
        self._interval_dropped = 0
        self._interval_max_age: float | None = None
        self._interval_bad = False

    def observe(self, subscription: Subscription, message: DataModel, bus: Bus) -> None:
        dropped = subscription.dropped
        new_drops = dropped - self.input_dropped
        self.input_dropped = dropped
        stamped = sent_at(message)
        age = None if stamped is None else max(0.0, time.time() - stamped)
        self.input_age_s = age
        self.note(bus, new_drops, age)

    def note(self, bus: Bus, new_drops: int, age: float | None = None) -> None:
        """Judge ``new_drops`` (since the last call) and an ``age``, and report
        per the policy. What ``observe`` calls; callable directly for counts
        that are not a subscription's."""
        if self.policy is None:
            return

        too_old = age is not None and age > self.policy.max_input_age_s
        bad = new_drops > 0 or too_old
        self._interval_dropped += new_drops
        if age is not None:
            self._interval_max_age = age if self._interval_max_age is None else max(self._interval_max_age, age)
        self._interval_bad = self._interval_bad or bad
        now = time.monotonic()

        if not self.behind:
            if bad:
                self.behind = True
                self._behind_since = now
                self._report(bus, now, f"{self.label} {self.what}")
            else:
                self._reset_interval(now)
            return

        if now - self._last_report < self.policy.report_every_s:
            return
        if self._interval_bad:
            self._report(bus, now, f"{self.label} {self.what} (behind for {now - self._behind_since:.0f} s)")
        else:
            self.behind = False
            self._publish(
                bus,
                f"{self.label} caught up after {now - self._behind_since:.0f} s behind",
                self._detail(),
            )
            self._reset_interval(now)

    def _report(self, bus: Bus, now: float, message: str) -> None:
        detail = self._detail()
        logger.warning("%s: %s", message, detail)
        self._publish(bus, message, detail)
        self._reset_interval(now)

    def _detail(self) -> str:
        """This interval's counts: since the last report, or since it fell behind."""
        parts = [f"{self._interval_dropped} {self.unit}"]
        if self._interval_max_age is not None:
            parts.append(f"oldest input read {self._interval_max_age:.1f} s after it was sent")
        return ", ".join(parts)

    def _publish(self, bus: Bus, message: str, detail: str) -> None:
        self.reports += 1
        bus.publish(ErrorEvent(timestamp=utcnow(), source=self.source, message=message, detail=detail))

    def _reset_interval(self, now: float) -> None:
        self._last_report = now
        self._interval_dropped = 0
        self._interval_max_age = None
        self._interval_bad = False


class Module(ABC):
    """Consume ``input_model`` from the bus; publish ``output_model`` results.

    Class attributes a subclass sets:

    * ``name`` -- how the module identifies itself in ``AppIdentity``.
    * ``input_model`` -- the message class to subscribe to.
    * ``output_model`` -- the ``ResultEnvelope`` subclass to publish.
    * ``overflow`` -- what to do when this module falls behind its input;
      ``DROP_OLDEST`` by default, since a module reading a live-rate stream
      should analyse the newest frame rather than an ever-older backlog.
    * ``keep_up`` -- when falling behind is reported as an ``ErrorEvent``
      (:class:`KeepUp`); ``None`` for a module that must never report itself.

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
    keep_up: ClassVar[KeepUp | None] = KeepUp()

    def __init__(self) -> None:
        self.identity = AppIdentity(name=self.name, uuid=uuid4().hex)
        self.status = AppStatus.INITIALIZING
        self.parameters: dict[str, Any] = {}
        self.last_result: ResultEnvelope | None = None
        self.monitor = KeepUpMonitor(
            self.name, f"is not keeping up with {self.input_model.topic}", self.keep_up
        )

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
                self.monitor.observe(inputs, message, bus)
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
