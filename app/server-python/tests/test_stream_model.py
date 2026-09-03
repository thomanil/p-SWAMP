# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Unit tests for the PMU streamer's pure domain: the envelope and StreamModel.

Hermetic by construction — the envelope is plain JSON and the model takes explicit
timestamps, so nothing here touches a broker, a socket, or the wall clock. These
pin the pieces of the Kafka-vs-NATS consumer that are worth being sure of: the
wire round-trip, the bounded scrolling window, the latency/throughput metrics, and
that switching brokers resets the per-pipe state.
"""

from brokers.envelope import Envelope, decode, encode
from pmu_test_streamer.model import (
    WINDOW_SIZE,
    StreamMetrics,
    StreamModel,
)


def test_envelope_round_trip():
    env = Envelope(seq=42, produced_at_ms=1_700_000_000_123, text="3000,50.01,418.6")
    back = decode(encode(env))
    assert back == env


def test_window_is_bounded_and_ordered():
    model = StreamModel()
    for seq in range(WINDOW_SIZE + 5):
        model.ingest(Envelope(seq=seq, produced_at_ms=0, text=f"r{seq}"), now_ms=0, now_mono=0.0)
    # Only the last WINDOW_SIZE records survive, oldest first.
    assert len(model.window) == WINDOW_SIZE
    seqs = [r.seq for r in model.window]
    assert seqs == list(range(5, WINDOW_SIZE + 5))
    assert model.metrics.received == WINDOW_SIZE + 5


def test_latency_is_now_minus_produced():
    model = StreamModel()
    # First sample: EMA seeds to the exact value.
    model.ingest(Envelope(seq=0, produced_at_ms=1000, text="x"), now_ms=1080, now_mono=0.0)
    assert model.metrics.latency_ms == 80.0
    # A negative delta (clock skew / same instant) is clamped to zero, not < 0.
    model.metrics = StreamMetrics()
    model2 = StreamModel()
    model2.ingest(Envelope(seq=0, produced_at_ms=2000, text="x"), now_ms=1000, now_mono=0.0)
    assert model2.metrics.latency_ms == 0.0


def test_throughput_counts_trailing_second_and_decays():
    m = StreamMetrics()
    # Five records within the same 1s window.
    for i in range(5):
        m.record(latency_ms=10.0, now_mono=0.0 + i * 0.1)  # t = 0.0..0.4
    assert m.throughput_hz(now_mono=0.4) == 5.0
    # Well past the trailing window: all have aged out.
    assert m.throughput_hz(now_mono=5.0) == 0.0


def test_switch_resets_metrics_and_window():
    model = StreamModel(broker="kafka")
    model.ingest(Envelope(seq=1, produced_at_ms=0, text="a"), now_ms=5, now_mono=0.0)
    model.error = "kafka pipe unavailable"
    model.switch("nats")
    assert model.broker == "nats"
    assert model.metrics.received == 0
    assert len(model.window) == 0
    assert model.error is None
