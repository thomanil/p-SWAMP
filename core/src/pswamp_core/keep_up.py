# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Falling behind, noticed and said: ``KeepUp`` and ``KeepUpMonitor``.

A module reads its input from a bounded queue (``DROP_OLDEST``: right for a
live stream, and silent unless someone counts), and every input has crossed a
transport, which stamps when it was sent
(:func:`~pswamp_core.messages.sent_at`). The monitor watches both -- input the
queue dropped, and how old each input is when it is read -- and past a
``KeepUp`` policy publishes an ``ErrorEvent``: once on falling behind, again at
most every ``report_every_s`` while behind, and once on catching up. So the
person whose pipeline it is sees it on the error tray, not only in a log.

The same monitor watches the outbox in front of the transport, where a
publisher that cannot keep up drops the oldest data rather than grow.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .log import get_logger
from .messages.data_model import sent_at
from .messages.errors import ErrorEvent
from .util.time import utcnow

if TYPE_CHECKING:
    from .messages.data_model import DataModel
    from .subscription import Sink, Subscription

__all__ = ["KeepUp", "KeepUpMonitor"]

logger = get_logger("pswamp_core.keep_up")


@dataclass(frozen=True)
class KeepUp:
    """When a module counts as not keeping up with its input, and how often it
    says so.

    Attributes:
        max_input_age_s: An input older than this when the module reads it
            (from the moment the transport sent it) means the module is
            behind. Dropped input always does.
        report_every_s: While behind, at most one report per this interval;
            a whole interval with nothing dropped and nothing too old is what
            "caught up" means.
    """

    max_input_age_s: float = 2.0
    report_every_s: float = 5.0


class KeepUpMonitor:
    """Watches one input queue and reports falling behind as ``ErrorEvent``.

    ``observe`` is called once per message read, before the module processes
    it. It keeps the readings a module may put into its own result
    (``input_dropped``, ``input_age_s``) and, under a ``KeepUp`` policy,
    publishes into the module's outbox: on falling behind, every
    ``report_every_s`` while behind (with that interval's counts), and on
    catching up.

    It judges only when a message arrives, so a stream that stops while behind
    reports its catching up on the next message rather than on a timer.

    ``note`` is the same judgement over counts the caller supplies, for
    falling behind that is not an input queue's: a module whose analysis
    skips evaluations it had no time for counts those, with ``unit`` naming
    them in the report.

    Args:
        source: The ``source`` the reports carry; the module's ``name``.
        what: How the report reads after the label, e.g. ``is not keeping up
            with pmu.frame``.
        policy: The thresholds; ``None`` keeps the readings and reports nothing.
        label: Who the report names, when that is not just ``source`` -- the
            outbox in front of the transport reports under a ``source`` it is
            not.
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
        #: Age of the last input read, in seconds; ``None`` if nothing stamped it.
        self.input_age_s: float | None = None
        #: Reports published so far (falling behind, still behind, caught up).
        self.reports = 0
        self.behind = False
        self._behind_since = 0.0
        self._last_report = 0.0
        self._interval_dropped = 0
        self._interval_max_age: float | None = None
        self._interval_bad = False

    def observe(self, subscription: Subscription, message: DataModel, out: Sink) -> None:
        dropped = subscription.dropped
        new_drops = dropped - self.input_dropped
        self.input_dropped = dropped
        stamped = sent_at(message)
        age = None if stamped is None else max(0.0, time.time() - stamped)
        self.input_age_s = age
        self.note(out, new_drops, age)

    def note(self, out: Sink, new_drops: int, age: float | None = None) -> None:
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
                self._report(out, now, f"{self.label} {self.what}")
            else:
                self._reset_interval(now)
            return

        if now - self._last_report < self.policy.report_every_s:
            return
        if self._interval_bad:
            self._report(out, now, f"{self.label} {self.what} (behind for {now - self._behind_since:.0f} s)")
        else:
            self.behind = False
            self._publish(
                out,
                f"{self.label} caught up after {now - self._behind_since:.0f} s behind",
                self._detail(),
            )
            self._reset_interval(now)

    def _report(self, out: Sink, now: float, message: str) -> None:
        detail = self._detail()
        logger.warning("%s: %s", message, detail)
        self._publish(out, message, detail)
        self._reset_interval(now)

    def _detail(self) -> str:
        """This interval's counts: since the last report, or since it fell behind."""
        parts = [f"{self._interval_dropped} {self.unit}"]
        if self._interval_max_age is not None:
            parts.append(f"oldest input read {self._interval_max_age:.1f} s after it was sent")
        return ", ".join(parts)

    def _publish(self, out: Sink, message: str, detail: str) -> None:
        self.reports += 1
        out.publish(ErrorEvent(timestamp=utcnow(), source=self.source, message=message, detail=detail))

    def _reset_interval(self, now: float) -> None:
        self._last_report = now
        self._interval_dropped = 0
        self._interval_max_age = None
        self._interval_bad = False
