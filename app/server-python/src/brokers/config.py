"""Where the two brokers live and how fast the producer runs — all from the env.

Read once at import. Reading environment variables is side-effect-free (no
connection is opened), so this respects the "no I/O at import" rule in
`brokers/__init__.py`.

Defaults point at localhost so `python producer.py` and a locally-run server work
against brokers on the host. Docker Compose and the k8s manifests override
`KAFKA_BOOTSTRAP` / `NATS_URL` to the in-cluster service names.
"""

import os

# The Kafka broker(s), as aiokafka's bootstrap_servers string ("host:port").
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

# The NATS server URL.
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")

# The one topic/subject the producer publishes to and every consumer reads. The
# same string names a Kafka topic and a NATS subject; dots are legal in both.
TOPIC = os.environ.get("PMU_TOPIC", "pmu.records")

# Records per second the producer emits. 100 = real time: the sample is PMU frames
# at 20 Hz from five stations (see pmu_test_streamer/model.py), i.e. 100 lines/s.
# Crank it up to stress the brokers for the throughput half of the comparison.
PRODUCER_RATE_HZ = float(os.environ.get("PRODUCER_RATE_HZ", "100"))

# Per-message console traces, so the stream is visible flowing through each broker:
#   [KAFKA] message 123 published        (producer)
#   [NATS]  message 123 received         (consumer)
# On by default for this experiment. Tracing every message at 100 Hz is a firehose
# in the aggregated compose log, so STREAM_TRACE_EVERY samples it — default 20 is
# roughly every 20th record (~5/s per broker), readable while still clearly showing
# the stream. Set STREAM_TRACE_EVERY=1 to see every message, or STREAM_TRACE=0 to
# silence the per-message lines entirely.
STREAM_TRACE = os.environ.get("STREAM_TRACE", "1") not in ("0", "false", "False", "")
STREAM_TRACE_EVERY = max(1, int(os.environ.get("STREAM_TRACE_EVERY", "20")))


def should_trace(seq: int) -> bool:
    """Whether to emit a per-message trace line for this sequence number."""
    return STREAM_TRACE and seq % STREAM_TRACE_EVERY == 0
