"""The producer service: loop sample_data.txt into BOTH broker topics, forever.

This is the left end of the Kafka-vs-NATS experiment's chain:

    sample_data.txt -> [producer] -> Kafka topic  \\
                                  -> NATS subject  -}-> server consumer -> browser

It is a **separate deployable** (its own Compose service / k8s Deployment) so the
producer is a distinct moving part, the way it would be in a real system — the
state server is left as a pure consumer. It reuses the SAME image as the server
(`command: ["python", "producer.py"]`): the code and the sample data are already
baked in, so there is no second Dockerfile and no second dependency set.

It reads the committed sample file **directly by path** rather than
`import pmu_test_streamer`, whose package `__init__` pulls in the FastAPI app the
producer has no use for. Each line is wrapped in an `Envelope` (with a wall-clock
timestamp the consumer turns into end-to-end latency) and published to both
brokers at `PRODUCER_RATE_HZ`. Playback loops so the stream never runs dry.

Run locally with `PORT`-free direct invocation from the server source dir:

    (cd app/server-python/src && \\
     KAFKA_BOOTSTRAP=localhost:9092 NATS_URL=nats://localhost:4222 \\
     uv run --project .. python producer.py)
"""

import asyncio
import contextlib
import time
from pathlib import Path

from brokers import config
from brokers.envelope import Envelope
from brokers.log import get_logger
from brokers.producers import KafkaProducerClient, NatsProducerClient

logger = get_logger("producer")

# The sample file lives beside the streamer package's code (it is that app's
# fixture); resolve it relative to this module so the working directory does not
# matter. Read once at start — small, immutable, one record per line.
SAMPLE_FILE = Path(__file__).parent / "pmu_test_streamer" / "sample_data.txt"

# Bounds each individual publish so a wedged broker call is dropped rather than
# blocking its own publisher task forever.
PUBLISH_TIMEOUT = 0.5

# Per-broker hand-off queue depth. Each broker has its own publisher task draining
# its own queue, so the two pipes are fully independent: if one broker is down its
# queue backs up and drops oldest, while the other keeps publishing at full rate.
QUEUE_MAX = 2000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _offer(queue: "asyncio.Queue[Envelope]", envelope: Envelope) -> None:
    """Enqueue without ever blocking the pacing loop; drop the oldest if full.

    A full queue means that broker's publisher can't keep up (it is down or slow),
    which for a live replay is the right thing to shed — and it must not slow the
    pacing loop, which feeds the healthy broker too."""
    try:
        queue.put_nowait(envelope)
    except asyncio.QueueFull:
        with contextlib.suppress(asyncio.QueueEmpty):
            queue.get_nowait()
        queue.put_nowait(envelope)


async def _publisher(client, queue: "asyncio.Queue[Envelope]", name: str) -> None:
    """Drain one broker's queue, publishing each record. One task per broker, so a
    slow/down broker throttles only its own queue, never the other's."""
    while True:
        envelope = await queue.get()
        try:
            await asyncio.wait_for(client.publish(envelope), timeout=PUBLISH_TIMEOUT)
        except Exception as exc:  # timeout or a broker-specific error
            logger.warning("%s publish dropped a record (%s)", name, exc)
            continue
        if config.should_trace(envelope.seq):
            logger.info("[%s] message %d published", name.upper(), envelope.seq)


async def run() -> None:
    lines = SAMPLE_FILE.read_text().splitlines()
    logger.info(
        "producer: %d records, %.0f Hz, topic %r -> kafka %s / nats %s",
        len(lines),
        config.PRODUCER_RATE_HZ,
        config.TOPIC,
        config.KAFKA_BOOTSTRAP,
        config.NATS_URL,
    )

    kafka = KafkaProducerClient()
    nats = NatsProducerClient()
    # Connect both (each retries until its broker is up), concurrently.
    await asyncio.gather(kafka.connect(), nats.connect())

    # One queue + publisher task per broker: the two pipes run at their own pace.
    kafka_q: asyncio.Queue[Envelope] = asyncio.Queue(maxsize=QUEUE_MAX)
    nats_q: asyncio.Queue[Envelope] = asyncio.Queue(maxsize=QUEUE_MAX)
    tasks = [
        asyncio.create_task(_publisher(kafka, kafka_q, "kafka")),
        asyncio.create_task(_publisher(nats, nats_q, "nats")),
    ]

    interval = 1.0 / config.PRODUCER_RATE_HZ
    seq = 0
    index = 0
    # Monotonic-deadline pacing rather than sleep(interval): the latter would drift
    # by the per-record publish time, quietly making "real time" a lie over a long
    # run. Same approach as pmu_test_streamer's ticker. If a tick overruns by more
    # than one interval, reset to now instead of firing a catch-up burst. The loop
    # only paces and enqueues (never awaits a publish), so its cadence is immune to
    # either broker's health.
    next_tick = time.monotonic()
    try:
        while True:
            next_tick += interval
            now = time.monotonic()
            if now > next_tick + interval:
                next_tick = now
            await asyncio.sleep(max(0.0, next_tick - now))

            envelope = Envelope(seq=seq, produced_at_ms=_now_ms(), text=lines[index])
            # Same record to both pipes; each publisher drains its own queue.
            _offer(kafka_q, envelope)
            _offer(nats_q, envelope)
            seq += 1
            index = (index + 1) % len(lines)
            if seq % (int(config.PRODUCER_RATE_HZ) * 10 or 1) == 0:
                logger.info("producer: produced %d records", seq)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(kafka.close(), nats.close())


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
