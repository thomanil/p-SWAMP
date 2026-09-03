"""Pure domain model for the PMU streamer's consumer side.

No I/O and no knowledge of WebSockets or brokers' network clients — api.py owns
all of that. This holds only what one client is looking at: a small scrolling
window of the most recent records, plus the live latency/throughput metrics for
whichever broker it currently has selected. It is deliberately unit-testable
without a broker or a socket (see tests/test_stream_model.py).

This replaces the file-cursor model the streamer had before the Kafka-vs-NATS
pub/sub layer: there is no longer a position in a file to seek within — the source
is a live topic — so what stays is the view of the live tail and the numbers that
make the two brokers comparable.

`sample_data.txt` still lives beside this package (the producer reads it by path),
but nothing here loads it any more.
"""

import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Literal

from brokers.envelope import Envelope

# The two pipes under evaluation. The default is NATS because it connects
# essentially instantly, so a freshly opened page shows data without waiting on
# Kafka's slower first-connect — a difference that is itself part of the story.
Broker = Literal["kafka", "nats"]
BROKERS: tuple[Broker, ...] = ("kafka", "nats")
DEFAULT_BROKER: Broker = "nats"

# Records kept for the scrolling view. Nine matches the old file-window height, so
# StreamWindow.tsx renders unchanged.
WINDOW_SIZE = 9

# Throughput is "records seen in the trailing second". A one-second trailing count
# reads directly as records/s and needs no rate estimation.
_THROUGHPUT_WINDOW_S = 1.0

# Weight of the newest sample in the latency EMA. Smooths the per-record jitter
# without lagging a real shift between brokers.
_LATENCY_EMA_ALPHA = 0.2


@dataclass
class Record:
    """One record in the scrolling view."""

    seq: int
    text: str


@dataclass(frozen=True)
class Stats:
    """A metric summarised four ways: its current value plus the min, max and mean
    seen since the last broker switch. `current` is the live reading the readout
    always had; the other three are what make jitter and drift visible over time."""

    current: float
    min: float
    max: float
    mean: float


class StreamMetrics:
    """Rolling end-to-end latency and throughput for the active broker.

    Both are exposed as a `Stats` (current / min / max / mean). Latency samples are
    natural — one per record — so its aggregates come straight off each arrival.
    Throughput has no per-record sample (it is a rate), so its aggregates are taken
    over *completed one-second buckets*: each finished second contributes one
    records/s reading to the min/max/mean, while `current` stays the trailing-second
    count the page always showed.

    Reset on a broker switch so the readout reflects the pipe now selected rather
    than a blend of both.
    """

    def __init__(self) -> None:
        self.received = 0
        # --- latency, sampled once per arriving record --------------------
        self.latency_ms = 0.0  # EMA of now - produced_at; the "current" reading
        self._lat_min = math.inf
        self._lat_max = 0.0
        self._lat_sum = 0.0  # running sum for the mean
        # --- throughput, current: receive times over the trailing second --
        self._stamps: deque[float] = deque()
        # --- throughput, aggregates: one records/s sample per whole second -
        self._last_mono: float | None = None
        self._bucket_start: float | None = None
        self._bucket_count = 0
        self._tput_min = math.inf
        self._tput_max = 0.0
        self._tput_sum = 0.0  # running sum of completed-second rates
        self._tput_n = 0  # number of completed-second samples

    def record(self, latency_ms: float, now_mono: float) -> None:
        self.received += 1
        # Latency: current (EMA) plus min/max/mean over every sample.
        self.latency_ms = (
            latency_ms
            if self.received == 1
            else _LATENCY_EMA_ALPHA * latency_ms
            + (1 - _LATENCY_EMA_ALPHA) * self.latency_ms
        )
        self._lat_min = min(self._lat_min, latency_ms)
        self._lat_max = max(self._lat_max, latency_ms)
        self._lat_sum += latency_ms
        # Throughput current: trailing-second count.
        self._stamps.append(now_mono)
        self._prune(now_mono)
        # Throughput aggregates: fold completed one-second buckets.
        self._advance_buckets(now_mono)
        self._bucket_count += 1
        self._last_mono = now_mono

    def _prune(self, now_mono: float) -> None:
        cutoff = now_mono - _THROUGHPUT_WINDOW_S
        while self._stamps and self._stamps[0] < cutoff:
            self._stamps.popleft()

    def _advance_buckets(self, now_mono: float) -> None:
        """Close out any whole seconds that have elapsed, each as one records/s
        sample. A gap longer than the window (a pause, or a stalled pipe) is a
        discontinuity, not a run of empty seconds, so the partial bucket is dropped
        rather than folded — otherwise resuming would inject spurious near-zero
        samples that drag min and mean down."""
        if self._bucket_start is None:
            self._bucket_start = now_mono
            return
        if self._last_mono is not None and now_mono - self._last_mono > _THROUGHPUT_WINDOW_S:
            self._bucket_start = now_mono
            self._bucket_count = 0
            return
        while now_mono - self._bucket_start >= _THROUGHPUT_WINDOW_S:
            rate = self._bucket_count / _THROUGHPUT_WINDOW_S
            self._tput_min = min(self._tput_min, rate)
            self._tput_max = max(self._tput_max, rate)
            self._tput_sum += rate
            self._tput_n += 1
            self._bucket_start += _THROUGHPUT_WINDOW_S
            self._bucket_count = 0

    def throughput_hz(self, now_mono: float | None = None) -> float:
        """Records/s over the trailing second. Pass the current time so an idle or
        paused stream decays to zero rather than reporting a stale count."""
        if now_mono is not None:
            self._prune(now_mono)
        return len(self._stamps) / _THROUGHPUT_WINDOW_S

    def latency_stats(self) -> Stats:
        """Current / min / max / mean end-to-end latency, ms."""
        if self.received == 0:
            return Stats(0.0, 0.0, 0.0, 0.0)
        return Stats(
            current=self.latency_ms,
            min=self._lat_min,
            max=self._lat_max,
            mean=self._lat_sum / self.received,
        )

    def throughput_stats(self, now_mono: float | None = None) -> Stats:
        """Current / min / max / mean throughput, records/s. `current` is the
        trailing-second count; the aggregates are over completed one-second
        buckets, and fall back to `current` until the first whole second has
        elapsed."""
        current = self.throughput_hz(now_mono)
        if self._tput_n == 0:
            return Stats(current, current, current, current)
        return Stats(
            current=current,
            min=self._tput_min,
            max=self._tput_max,
            mean=self._tput_sum / self._tput_n,
        )

    def reset(self) -> None:
        self.received = 0
        self.latency_ms = 0.0
        self._lat_min = math.inf
        self._lat_max = 0.0
        self._lat_sum = 0.0
        self._stamps.clear()
        self._last_mono = None
        self._bucket_start = None
        self._bucket_count = 0
        self._tput_min = math.inf
        self._tput_max = 0.0
        self._tput_sum = 0.0
        self._tput_n = 0


class StreamModel:
    """One client's view of the live stream: selected broker, play state, the
    scrolling window, and the metrics for the active pipe."""

    def __init__(self, broker: Broker = DEFAULT_BROKER) -> None:
        self.broker: Broker = broker
        self.playing = True  # auto-start: an opened page shows the live tail
        self.window: deque[Record] = deque(maxlen=WINDOW_SIZE)
        self.metrics = StreamMetrics()
        # A human-readable reason the current pipe is unavailable, or None when
        # healthy. Surfaced to the page so a down broker reads as a message rather
        # than a hang.
        self.error: str | None = None

    def ingest(self, envelope: Envelope, now_ms: int, now_mono: float) -> None:
        """Record one arrived envelope: update metrics and the scrolling window."""
        latency_ms = max(0.0, now_ms - envelope.produced_at_ms)
        self.metrics.record(latency_ms, now_mono)
        self.window.append(Record(seq=envelope.seq, text=envelope.text))

    def switch(self, broker: Broker) -> None:
        """Change the selected pipe and clear everything specific to the old one."""
        self.broker = broker
        self.window.clear()
        self.metrics.reset()
        self.error = None


def now_ms() -> int:
    """Wall clock in milliseconds — the same clock the producer stamps with, so the
    difference is a real end-to-end latency (see brokers/envelope.py)."""
    return int(time.time() * 1000)
