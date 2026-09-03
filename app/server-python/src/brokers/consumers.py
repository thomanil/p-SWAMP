"""Subscribe-side clients for the two brokers, one class each, same interface.

Both expose the same three calls — `start` / `poll(timeout)` / `stop` — so the
streamer's consume loop treats Kafka and NATS identically and only the class it
instantiates differs. That symmetry is the experiment: the surrounding code is a
constant, so any difference the UI shows is the broker's.

`poll(timeout)` is deliberately timeout-bounded rather than an endless `async for`.
The consume loop needs to regain control periodically even when no records are
arriving — to notice a broker switch, to refresh the throughput readout, and to
detect that the selected broker has gone silent — so a blocking iterator would
strand it. Each client returns whatever has arrived within `timeout`, possibly an
empty list.

Both live-tail: a consumer sees records published after it subscribes, not the
backlog. Kafka: a unique group per consumer with `auto_offset_reset="latest"`.
NATS core pub/sub is live-tail by nature. So switching brokers, or reconnecting,
always resumes at "now" — correct for a live replay, where there is no meaningful
past position to seek to.

Nothing connects at import (see `brokers/__init__.py`); `start()` connects, and it
raises on failure so the caller can show the pipe as unavailable rather than hang.
"""

import asyncio

from aiokafka import AIOKafkaConsumer
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

from . import config
from .envelope import Envelope, decode
from .log import get_logger

logger = get_logger("consumer")

# Cap the NATS hand-off queue so a paused/slow consumer cannot grow it without
# bound; oldest are dropped past this, which for a live replay is the right loss.
_NATS_QUEUE_MAX = 10_000


class KafkaConsumerClient:
    """Live-tails the Kafka topic via aiokafka.

    `group_id` is made unique per consumer so each client is its own group and
    reads the live tail independently, rather than sharing partitions with other
    viewers.
    """

    def __init__(self, group_id: str) -> None:
        self._group_id = group_id
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        consumer = AIOKafkaConsumer(
            config.TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP,
            group_id=self._group_id,
            auto_offset_reset="latest",
            # Never commit offsets. This is a live tail with no notion of a past
            # position to resume, so committing would only let a later consumer
            # with the same group id resume a stale offset and drain the retained
            # backlog instead of tailing — which is exactly what it did. With no
            # commit, a fresh group always resolves to `latest` = the live tail.
            enable_auto_commit=False,
        )
        await consumer.start()
        self._consumer = consumer
        logger.info("kafka consumer started (group %s)", self._group_id)

    async def poll(self, timeout: float) -> list[Envelope]:
        assert self._consumer is not None, "start() before poll()"
        batches = await self._consumer.getmany(timeout_ms=int(timeout * 1000))
        out: list[Envelope] = []
        for records in batches.values():
            for record in records:
                out.append(decode(record.value))
        return out

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None


class NatsConsumerClient:
    """Live-tails the NATS subject via nats-py.

    The subscription callback feeds an `asyncio.Queue`; `poll` drains it. This
    turns nats-py's push delivery into the same pull/poll shape aiokafka gives,
    so the two clients present one interface to the consume loop.
    """

    def __init__(self) -> None:
        self._client: NatsClient | None = None
        self._sub = None
        self._queue: asyncio.Queue[Envelope] = asyncio.Queue(maxsize=_NATS_QUEUE_MAX)

    async def start(self) -> None:
        client = NatsClient()
        await client.connect(servers=[config.NATS_URL])

        async def _on_message(msg: Msg) -> None:
            try:
                envelope = decode(msg.data)
            except Exception as exc:  # a malformed frame must not kill the sub
                logger.warning("nats decode failed (%s)", exc)
                return
            try:
                self._queue.put_nowait(envelope)
            except asyncio.QueueFull:
                # Live replay: drop the oldest to make room for the newest.
                self._queue.get_nowait()
                self._queue.put_nowait(envelope)

        self._client = client
        self._sub = await client.subscribe(config.TOPIC, cb=_on_message)
        logger.info("nats consumer started")

    async def poll(self, timeout: float) -> list[Envelope]:
        out: list[Envelope] = []
        try:
            first = await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return out
        out.append(first)
        # Drain whatever else is already queued without waiting.
        while True:
            try:
                out.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return out

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.drain()
            self._client = None
            self._sub = None
