# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A Kafka-API broker (Apache Kafka, or anything speaking its protocol) as a :class:`~pswamp_core.transport.Transport`.

One topic per message class -- ``pmu.frame``, ``frame.stats.result`` -- under an
optional namespace prefix that is configuration and never a message field
(STEP 3 ADR-005); the pipeline key is the **record key**. So a deployment with
eight per-client pipelines has three topics, not twenty-four, and a worker
consumes each topic once and tells the pipelines apart by key. Every topic is
read from its end, so a subscriber sees only what is said after it arrives;
a frame carries its own layout, so that is all a late worker needs.

The record value is the message's JSON (``model_dump_json``), decoded with
``model_validate_json`` on the way in; anything that does not validate is
logged and dropped. ``publish`` awaits the broker's acknowledgement per record,
deliberately: a broker that falls behind then backpressures into the caller's
drop-oldest outbox instead of growing an unbounded batch in the producer.

Topics are created on first use with one partition (the compose and k8s
brokers have auto-creation off, so a topic exists with the config this
transport chose and never with a default one; and one partition keeps a
topic totally ordered, which is what a replay wants). A deployment that partitions a topic keeps ordering
per key, which is all a per-key module needs. No consumer group and no
committed offsets: a feed is a tail, and a process that restarts wants *now*,
not its backlog.

Requires the ``kafka`` extra (``pswamp-core[kafka]``). ``aiokafka`` is imported
inside the methods that need it, so importing this module -- and naming the
class in a spec string -- costs nothing without it.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any

from ..datagateway.config import EnvSetting
from ..log import get_logger
from . import Transport

if TYPE_CHECKING:
    from ..messages.data_model import DataModel

__all__ = ["KafkaTransport", "create_topic"]

logger = get_logger("pswamp_core.transport.kafka")

#: A produce to a broker that is down fails in seconds, not aiokafka's 40.
_PRODUCER_OPTIONS: dict[str, Any] = {"request_timeout_ms": 10_000}

#: Kafka's error code for a topic that already exists, in a CreateTopics response.
_TOPIC_ALREADY_EXISTS = 36

#: How long a feed waits for its topic's partitions to be assigned.
_ASSIGNMENT_TIMEOUT_S = 15.0
_ASSIGNMENT_POLL_MS = 200


class KafkaTransport(Transport):
    """Keyed publish/subscribe over a Kafka-API broker.

    Args:
        name: The transport's name; also the ``{NAME}_`` prefix of its settings.
        bootstrap_servers: Broker addresses, as ``aiokafka`` takes them.
        topic_prefix: Namespace prepended to every topic (``no`` makes
            ``pmu.frame`` into ``no.pmu.frame``). Configuration, never a field.
        replication_factor: For the topics this transport creates.
    """

    env_settings = (
        EnvSetting(
            "BOOTSTRAP_SERVERS",
            "Comma-separated broker addresses, such as kafka:9092",
            required=True,
            kind="list",
        ),
        EnvSetting("TOPIC_PREFIX", "Namespace prepended to every topic name (optional)"),
        EnvSetting("REPLICATION_FACTOR", "For the topics this transport creates", default="1", kind="int"),
    )

    def __init__(
        self,
        name: str = "kafka",
        bootstrap_servers: str | Sequence[str] = "localhost:9092",
        *,
        topic_prefix: str | None = None,
        replication_factor: int = 1,
    ) -> None:
        super().__init__(name)
        self.bootstrap_servers = (
            bootstrap_servers if isinstance(bootstrap_servers, str) else list(bootstrap_servers)
        )
        self.topic_prefix = topic_prefix or None
        self.replication_factor = replication_factor
        self._producer: Any | None = None
        self._admin: Any | None = None
        self._known_topics: set[str] = set()
        self._open_lock = asyncio.Lock()
        self.published = 0

    # --- topics ---------------------------------------------------------------------

    def topic_for(self, model: type[DataModel]) -> str:
        """The topic carrying ``model``: its descriptor under the prefix, if any."""
        return f"{self.topic_prefix}.{model.topic}" if self.topic_prefix else model.topic

    async def ensure_topic(self, model: type[DataModel]) -> str:
        """Create ``model``'s topic if the broker lacks it; returns its name.

        "Already exists" is fine; any other failure is logged and left to the
        produce or consume that follows.
        """
        topic = self.topic_for(model)
        if topic in self._known_topics:
            return topic
        from aiokafka.admin import AIOKafkaAdminClient

        if self._admin is None:
            self._admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            await self._admin.start()
        if await create_topic(
            self._admin,
            topic,
            replication_factor=self.replication_factor,
            who=self.name,
        ):
            self._known_topics.add(topic)
        return topic

    # --- lifecycle --------------------------------------------------------------------

    async def open(self) -> None:
        """Start the shared producer. Idempotent, and safe to call concurrently."""
        async with self._open_lock:
            if self._producer is not None:
                return
            from aiokafka import AIOKafkaProducer

            producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers, **_PRODUCER_OPTIONS)
            await producer.start()
            self._producer = producer
            logger.info("%s: connected to %s", self.name, self.bootstrap_servers)

    async def close(self) -> None:
        await super().close()
        producer, self._producer = self._producer, None
        admin, self._admin = self._admin, None
        if producer is not None:
            await producer.stop()
        if admin is not None:
            await admin.close()

    # --- the contract -------------------------------------------------------------------

    async def publish(self, message: DataModel, key: str) -> None:
        if self._producer is None:
            await self.open()
        topic = await self.ensure_topic(type(message))
        await self._producer.send_and_wait(
            topic, value=message.model_dump_json().encode(), key=key.encode()
        )
        self.published += 1

    async def _feed(
        self, model: type[DataModel], ready: asyncio.Event
    ) -> AsyncIterator[tuple[str, DataModel]]:
        from aiokafka import AIOKafkaConsumer

        topic = await self.ensure_topic(model)
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="latest",  # a stream is joined at its end
        )
        await consumer.start()
        try:
            await self._await_assignment(consumer, topic)
            ready.set()
            async for record in consumer:
                key = record.key.decode() if record.key else ""
                try:
                    message = model.model_validate_json(record.value)
                except Exception as error:
                    logger.warning("%s: dropping undecodable record on %s: %s", self.name, topic, error)
                    continue
                yield key, message
        finally:
            await _stop_consumer(consumer)

    async def _await_assignment(self, consumer: Any, topic: str) -> None:
        """Poll until the subscription resolves to partitions, or give up loudly."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _ASSIGNMENT_TIMEOUT_S
        while not consumer.assignment():
            if loop.time() >= deadline:
                raise RuntimeError(f"no partitions assigned for topic {topic} within {_ASSIGNMENT_TIMEOUT_S:.0f}s")
            await consumer.getmany(timeout_ms=_ASSIGNMENT_POLL_MS)


async def create_topic(
    admin: Any,
    topic: str,
    *,
    replication_factor: int = 1,
    configs: dict[str, str] | None = None,
    who: str = "kafka",
) -> bool:
    """Create ``topic`` with one partition if the broker lacks it.

    ``True`` when the topic exists afterwards (created now, or already there);
    ``False`` when the broker refused, which is logged and left to the produce
    or consume that follows to fail loudly. Shared by the transport and by
    anything else in the core that owns a topic (the time-series provider's
    results feed), because the compose and k8s brokers have auto-creation off.
    """
    from aiokafka.admin import NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    try:
        response = await admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=1,
                    replication_factor=replication_factor,
                    topic_configs=configs,
                )
            ]
        )
    except TopicAlreadyExistsError:
        return True
    except Exception as exc:
        logger.warning("%s: could not create topic %s: %s", who, topic, exc)
        return False
    # aiokafka answers per topic in the response rather than raising:
    # (name, error code, message); 0 is created, 36 is "already exists".
    for name, code, message in getattr(response, "topic_errors", []):
        if code == 0:
            logger.info("%s: created topic %s%s", who, name, " (compacted)" if configs else "")
        elif code != _TOPIC_ALREADY_EXISTS:
            logger.warning("%s: could not create topic %s: %s", who, name, message)
            return False
    return True


async def _stop_consumer(consumer: Any) -> None:
    """Stop a consumer, absorbing the cancellation aiokafka can raise on shutdown."""
    try:
        await consumer.stop()
    except asyncio.CancelledError:
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise
