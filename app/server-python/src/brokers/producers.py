"""Publish-side clients for the two brokers, one class each, same interface.

Both are asyncio-native and both retry the initial connect, so the producer
service survives being started before its brokers are ready (the usual case in
Compose and k8s). Nothing connects at import — see `brokers/__init__.py`.

The interface is deliberately identical (`connect` / `publish` / `close`) so
`producer.py` can hold one list of producers and treat Kafka and NATS the same;
the whole point of the experiment is that only the class differs.
"""

import asyncio

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from nats.aio.client import Client as NatsClient

from . import config
from .envelope import Envelope, encode
from .log import get_logger

logger = get_logger("producer")

# How the connect-retry loop paces itself while a broker is still coming up.
_RETRY_SECONDS = 2.0


async def _retry_connect(name: str, connect) -> None:
    """Call an async `connect` until it succeeds, logging each failed attempt.

    Shared by both producers so the retry policy is defined once. `connect` is a
    zero-arg coroutine function that raises on failure.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            await connect()
            logger.info("%s producer connected", name)
            return
        except Exception as exc:  # noqa: BLE001 -- broker libs raise varied types
            logger.warning(
                "%s producer connect attempt %d failed (%s); retrying in %.0fs",
                name,
                attempt,
                exc,
                _RETRY_SECONDS,
            )
            await asyncio.sleep(_RETRY_SECONDS)


class KafkaProducerClient:
    """Publishes envelopes to the Kafka topic via aiokafka."""

    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def connect(self) -> None:
        async def _connect() -> None:
            producer = AIOKafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP)
            await producer.start()
            self._producer = producer

        await _retry_connect("kafka", _connect)

    async def publish(self, envelope: Envelope) -> None:
        assert self._producer is not None, "connect() before publish()"
        try:
            # send(), not send_and_wait(): enqueue to the buffer and return, rather
            # than blocking until the broker acks. This decouples the two brokers —
            # a down Kafka must not stall the shared publish loop and starve NATS
            # (it did, via send_and_wait's ~40 s request timeout). Awaiting send()
            # only waits for the record to be appended to the accumulator.
            await self._producer.send(config.TOPIC, encode(envelope))
        except KafkaConnectionError as exc:
            # A mid-run broker drop; log and move on rather than kill the producer.
            logger.warning("kafka publish failed (%s)", exc)

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None


class NatsProducerClient:
    """Publishes envelopes to the NATS subject via nats-py."""

    def __init__(self) -> None:
        self._client: NatsClient | None = None

    async def connect(self) -> None:
        async def _connect() -> None:
            client = NatsClient()
            await client.connect(servers=[config.NATS_URL])
            self._client = client

        await _retry_connect("nats", _connect)

    async def publish(self, envelope: Envelope) -> None:
        assert self._client is not None, "connect() before publish()"
        try:
            await self._client.publish(config.TOPIC, encode(envelope))
        except Exception as exc:  # noqa: BLE001 -- nats raises varied types
            logger.warning("nats publish failed (%s)", exc)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.drain()
            self._client = None
