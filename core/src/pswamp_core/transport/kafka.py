# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A Kafka-API broker (Apache Kafka, or anything speaking its protocol) as a :class:`~pswamp_core.transport.Transport`.

One topic per message class per app -- ``pmu-test-streamer.pmu.frame``,
``pmu-test-streamer.frame.stats.result`` -- under an optional deployment-wide
prefix that is configuration and never a message field (STEP 3 ADR-005); the
pipeline key is the **record key**. So an app with eight per-client pipelines
has three topics, not twenty-four, and a worker
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
topic totally ordered, which is what a replay wants) and **bounded
retention** (:data:`LIVE_TOPIC_CONFIGS`): every topic here is a live hop that
nobody reads back, and the broker's default -- a week, no size cap -- let one
client replaying the N44 recording at speed fill a laptop's Docker disk in
minutes. A deployment that partitions a topic keeps ordering
per key, which is all a per-key module needs. No consumer group and no
committed offsets: a feed is a tail, and a process that restarts wants *now*,
not its backlog.

**One consumer per process**, however many topics it listens to: the server
listens to every app's command, result and error topics, and one consumer per
topic -- some fifty -- starved each other of the broker's attention. The
consumer is assigned each topic's one partition by hand and is re-assigned
when a new topic is first subscribed, keeping its position on every topic it
already held; a new topic is read from its end.

Requires the ``kafka`` extra (``pswamp-core[kafka]``). ``aiokafka`` is imported
inside the methods that need it, so importing this module -- and naming the
class in a spec string -- costs nothing without it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from ..datagateway.config import EnvSetting
from ..log import get_logger
from ..messages.data_model import stamp_sent_at
from . import Transport

if TYPE_CHECKING:
    from ..messages.data_model import DataModel

__all__ = ["KafkaTransport", "create_topic"]

logger = get_logger("pswamp_core.transport.kafka")

#: A produce to a broker that is down fails in seconds, not aiokafka's 40.
_PRODUCER_OPTIONS: dict[str, Any] = {"request_timeout_ms": 10_000}

#: What every topic this module creates is created with: a live hop, kept long
#: enough for a slow or restarting consumer to catch up and no longer. Retention
#: deletes whole closed segments, so the segment size and age are what make the
#: two limits bite: at most ~a minute, or ~256 MB plus one 32 MB segment, per
#: partition, and a deleted segment's files go after a second rather than a
#: minute. (A 700-channel frame is ~41 KB: 50 Hz is ~2 MB/s per client.)
#:
#: The broker enforces all of this only every ``log.retention.check.interval.ms``
#: -- five minutes by default, which is gigabytes at replay speed -- so the
#: compose and k8s brokers set that to 10 s. A broker this does not configure
#: needs the same.
LIVE_TOPIC_CONFIGS: dict[str, str] = {
    "retention.ms": "60000",
    "retention.bytes": str(256 * 1024 * 1024),
    "segment.ms": "10000",
    "segment.bytes": str(32 * 1024 * 1024),
    "file.delete.delay.ms": "1000",
}

#: Kafka's error code for a topic that already exists, in a CreateTopics response.
_TOPIC_ALREADY_EXISTS = 36

#: How long one fetch waits for records: also how soon a new topic is taken on.
_POLL_MS = 200

#: Backoff for a consumer that died (broker restart, network): first wait, and the cap.
_RECONNECT_DELAY = 1.0
_MAX_RECONNECT_DELAY = 30.0


class KafkaTransport(Transport):
    """Keyed publish/subscribe over a Kafka-API broker.

    Args:
        name: The transport's name; also the ``{NAME}_`` prefix of its settings.
        bootstrap_servers: Broker addresses, as ``aiokafka`` takes them.
        topic_prefix: A deployment-wide namespace prepended to every topic
            (``no`` makes ``pmu-test-streamer.pmu.frame`` into
            ``no.pmu-test-streamer.pmu.frame``). Configuration, never a field.
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
        #: Every topic this process listens to, and the class each carries.
        self._models: dict[str, type[DataModel]] = {}
        #: Topics subscribed but not yet assigned to the consumer.
        self._unassigned: set[str] = set()
        self._consumer_task: asyncio.Task | None = None

    # --- topics ---------------------------------------------------------------------

    def topic(self, model: type[DataModel], app: str) -> str:
        """``<app>.<model.topic>``, under the deployment's prefix, if any."""
        topic = super().topic(model, app)
        return f"{self.topic_prefix}.{topic}" if self.topic_prefix else topic

    async def ensure_topic(self, topic: str) -> str:
        """Create ``topic`` if the broker lacks it; returns its name.

        "Already exists" is fine; any other failure is logged and left to the
        produce or consume that follows.
        """
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
        task, self._consumer_task = self._consumer_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await super().close()
        producer, self._producer = self._producer, None
        admin, self._admin = self._admin, None
        if producer is not None:
            await producer.stop()
        if admin is not None:
            await admin.close()

    # --- the contract -------------------------------------------------------------------

    async def publish(self, message: DataModel, *, app: str, key: str) -> None:
        if self._producer is None:
            await self.open()
        topic = await self.ensure_topic(self.topic(type(message), app))
        await self._producer.send_and_wait(
            topic, value=message.model_dump_json().encode(), key=key.encode()
        )
        self.published += 1

    def _watch(self, topic: str, model: type[DataModel]) -> None:
        if topic in self._models:
            return
        self._models[topic] = model
        self._ready[topic] = asyncio.Event()
        self._unassigned.add(topic)
        if self._consumer_task is None or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(self._consume(), name=f"{self.name}.consumer")

    async def _consume(self) -> None:
        """The process's one consumer: every watched topic, until cancelled,
        reopened with backoff when it fails."""
        from aiokafka import AIOKafkaConsumer

        delay = _RECONNECT_DELAY
        while True:
            consumer = AIOKafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=None,
                enable_auto_commit=False,
                auto_offset_reset="latest",  # a stream is joined at its end
            )
            try:
                await consumer.start()
                self._unassigned |= set(self._models)  # a fresh consumer holds nothing
                while True:
                    if self._unassigned:
                        await self._assign(consumer)
                    batches = await consumer.getmany(timeout_ms=_POLL_MS)
                    for partition, records in batches.items():
                        self._decode(partition.topic, records)
                    delay = _RECONNECT_DELAY
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("%s: consumer failed: %s; reopening in %.0fs", self.name, exc, delay)
            finally:
                for event in self._ready.values():
                    event.clear()
                await _stop_consumer(consumer)
            await asyncio.sleep(delay)
            delay = min(delay * 2, _MAX_RECONNECT_DELAY)

    async def _assign(self, consumer: Any) -> None:
        """Add the unassigned topics to the consumer, keeping where it was on
        the ones it held; each new one is read from its end, and ready once
        its position is known."""
        from aiokafka.structs import TopicPartition

        new = sorted(self._unassigned)
        self._unassigned.clear()
        for topic in new:
            await self.ensure_topic(topic)
        held = {tp: await consumer.position(tp) for tp in consumer.assignment()}
        fresh = [TopicPartition(topic, 0) for topic in new if TopicPartition(topic, 0) not in held]
        consumer.assign([*held, *fresh])
        for tp, offset in held.items():
            consumer.seek(tp, offset)
        if fresh:
            await consumer.seek_to_end(*fresh)
            for tp in fresh:
                await consumer.position(tp)
        for topic in new:
            self._ready[topic].set()

    def _decode(self, topic: str, records: Sequence[Any]) -> None:
        model = self._models.get(topic)
        if model is None:
            return
        for record in records:
            key = record.key.decode() if record.key else ""
            try:
                message = model.model_validate_json(record.value)
            except Exception as error:
                logger.warning("%s: dropping undecodable record on %s: %s", self.name, topic, error)
                continue
            # The record's CreateTime, as the producer stamped it: how long a
            # message has been in flight is what a lagging consumer reports.
            if record.timestamp is not None and record.timestamp >= 0:
                stamp_sent_at(message, record.timestamp / 1000.0)
            self._deliver(topic, key, message)


async def create_topic(
    admin: Any,
    topic: str,
    *,
    replication_factor: int = 1,
    configs: dict[str, str] | None = None,
    who: str = "kafka",
) -> bool:
    """Create ``topic`` with one partition, and ``configs`` (by default the
    bounded :data:`LIVE_TOPIC_CONFIGS`), if the broker lacks it.

    ``True`` when the topic exists afterwards (created now, or already there);
    ``False`` when the broker refused, which is logged and left to the produce
    or consume that follows to fail loudly. Shared by the transport and by
    anything else in the core that owns a topic, because the compose and k8s
    brokers have auto-creation off.
    """
    from aiokafka.admin import NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    if configs is None:
        configs = LIVE_TOPIC_CONFIGS

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
            logger.info("%s: created topic %s (%s)", who, name, ", ".join(f"{k}={v}" for k, v in configs.items()))
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
