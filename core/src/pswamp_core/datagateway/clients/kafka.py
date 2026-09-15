# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A Kafka-API broker as a data client: one topic per model.

Lifted from the ``test_pswamp`` draft's ``kafka_bus.py`` and adapted to the
core's contract: ``from_env`` is the base class's, driven by ``env_settings``;
the client carries every ``DataModel`` unless told otherwise (a bus is handed
whatever a bridge forwards, and a provider spec cannot name classes); topics
take an optional namespace prefix from configuration, never from a message
(STEP 3 ADR-005); live coverage is open-ended; and topics are created on first
use, because Redpanda does not auto-create them by default.

Serves the recent window -- a topic's retention -- plus live data as it
arrives. Its first use is the other end of a :class:`~pswamp_core.bridge.TopicBridge`:
the web server produces frames, a worker tails them and produces results.
Paired with a colder client (a time-series store) the gateway replays the
cold store first and hands over here once the cursor is inside the retention.

Requires the ``kafka`` extra (``pswamp-core[kafka]``): ``aiokafka`` is
imported lazily inside the methods that need it, so importing this module --
and loading the class by dotted path -- costs nothing without it.

Ordering: Kafka orders records within a partition only, and the gateway's
stream expects non-decreasing timestamps, so every topic this client creates
has **one** partition. A retried produce after a broker hiccup can still put
two records out of order, in which case the stream's watermark drops the
earlier-stamped one; acceptable for live data, which is all that crosses.

Reads address by **payload** time: ``consume`` seeks the topic by the
record's timestamp (set from the payload on produce) and filters on it. A
payload stamped in the past therefore never passes a live tail; see the
priming note in :mod:`pswamp_core.bridge`.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import timedelta
from typing import Any

from ...log import get_logger
from ...messages.data_model import DataModel
from ...util.time import utcnow
from ..config import EnvSetting, format_capabilities
from ..data_client_model import (
    Capability,
    DataClient,
    ModelSelector,
    MRIDFilter,
    normalise_models,
    normalise_mrid_filter,
)
from ..time_range import Coverage, TimeRange


__all__ = ["DEFAULT_CAPABILITIES", "DEFAULT_RETENTION", "KafkaClient"]

logger = get_logger("pswamp_core.datagateway.kafka")

#: Default assumed topic retention when none is configured.
DEFAULT_RETENTION = timedelta(minutes=20)

#: Operations a Kafka bus exposes unless told otherwise.
DEFAULT_CAPABILITIES = Capability.LIVE_CONSUME | Capability.HISTORY_CONSUME | Capability.PRODUCE

#: How long a poll waits for records before the loop re-checks its exit conditions.
_POLL_TIMEOUT_MS = 500

#: How long a historical read waits for the subscription to resolve to partitions.
_ASSIGNMENT_TIMEOUT = timedelta(seconds=10)

#: Poll granularity while waiting for that assignment.
_ASSIGNMENT_POLL_MS = 200

#: A produce to a broker that is down fails in seconds, not aiokafka's 40.
_DEFAULT_PRODUCER_OPTIONS: dict[str, Any] = {"request_timeout_ms": 10_000}


class KafkaClient(DataClient):
    """
    A Kafka-API broker (Kafka, Redpanda) streaming models, one topic per model.

    Args:
        name: Unique client name; also the ``{NAME}_`` prefix of its settings.
        bootstrap_servers: Broker list, as accepted by ``aiokafka``.
        supported_models: Model class or classes carried; every ``DataModel``
            by default. A parent class covers its subclasses, each on its own topic.
        topic_prefix: Namespace prepended to every topic (``no`` makes
            ``pmu.frame`` into ``no.pmu.frame``). Configuration, never a
            message field.
        topics: Per-model topic overrides, taking precedence over the prefix.
        retention_seconds: Window the topics are expected to hold, reported as
            coverage; should match the broker's ``retention.ms``.
        priority: Preference against other clients covering the same instant.
        capabilities: Operations to expose.
        replication_factor: For the topics this client creates.
        consumer_options, producer_options: Extra keyword arguments for
            ``AIOKafkaConsumer`` / ``AIOKafkaProducer``.
    """

    env_settings = (
        EnvSetting(
            "BOOTSTRAP_SERVERS",
            "Comma-separated broker addresses, such as redpanda:9092",
            required=True,
            kind="list",
        ),
        EnvSetting("TOPIC_PREFIX", "Namespace prepended to every topic name (optional)"),
        EnvSetting(
            "RETENTION_SECONDS",
            "Window the topics hold, matching the broker's retention.ms",
            default=str(int(DEFAULT_RETENTION.total_seconds())),
            kind="seconds",
        ),
        EnvSetting("PRIORITY", "Preference when several clients cover the same instant", default="0", kind="int"),
        EnvSetting(
            "CAPABILITIES",
            "Comma-separated operations to expose",
            default=format_capabilities(DEFAULT_CAPABILITIES),
            kind="capabilities",
        ),
        EnvSetting("REPLICATION_FACTOR", "For the topics this client creates", default="1", kind="int"),
    )

    def __init__(
        self,
        name: str,
        bootstrap_servers: str | Sequence[str],
        *,
        supported_models: ModelSelector | None = None,
        topic_prefix: str | None = None,
        topics: dict[type[DataModel], str] | None = None,
        retention_seconds: timedelta = DEFAULT_RETENTION,
        priority: int = 0,
        capabilities: Capability = DEFAULT_CAPABILITIES,
        replication_factor: int = 1,
        consumer_options: dict[str, Any] | None = None,
        producer_options: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.supported_models = normalise_models(DataModel if supported_models is None else supported_models)
        self.priority = priority
        self.capabilities = capabilities
        self.bootstrap_servers = (
            bootstrap_servers if isinstance(bootstrap_servers, str) else list(bootstrap_servers)
        )
        self.topic_prefix = topic_prefix or None
        self.retention = retention_seconds
        self.replication_factor = replication_factor
        self._topics = dict(topics or {})
        self._consumer_options = consumer_options or {}
        self._producer_options = {**_DEFAULT_PRODUCER_OPTIONS, **(producer_options or {})}
        self._producer: Any | None = None
        self._admin: Any | None = None
        self._known_topics: set[str] = set()

    # --- topics ---------------------------------------------------------------------

    def topic_for(self, model: type[DataModel]) -> str:
        """The topic carrying ``model``: an override, else the prefixed descriptor."""
        override = self._topics.get(model)
        if override:
            return override
        return f"{self.topic_prefix}.{model.topic}" if self.topic_prefix else model.topic

    async def ensure_topic(self, model: type[DataModel]) -> None:
        """Create ``model``'s topic (one partition) if the broker lacks it.

        Failures other than "already exists" are logged and not raised: a
        broker that forbids creation may still auto-create, and the consume or
        produce that follows is what decides.
        """
        topic = self.topic_for(model)
        if topic in self._known_topics:
            return
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic
        from aiokafka.errors import TopicAlreadyExistsError

        if self._admin is None:
            self._admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            await self._admin.start()
        try:
            await self._admin.create_topics(
                [NewTopic(topic, num_partitions=1, replication_factor=self.replication_factor)]
            )
            logger.info("%s: created topic %s", self.name, topic)
        except TopicAlreadyExistsError:
            pass
        except Exception as exc:
            logger.warning("%s: could not create topic %s: %s", self.name, topic, exc)
            return
        self._known_topics.add(topic)

    # --- the contract -----------------------------------------------------------------

    async def coverage(
        self,
        model: type[DataModel],
        mRID: MRIDFilter = None,
    ) -> Coverage | None:
        """The rolling retention window; open-ended when this client tails."""
        if not self.supports(model):
            return None
        now = utcnow()
        live = Capability.LIVE_CONSUME in self.capabilities
        return Coverage(range=TimeRange(now - self.retention, None if live else now), live=live)

    async def consume(
        self,
        model: type[DataModel],
        time_range: TimeRange,
        mRID: MRIDFilter = None,
    ) -> AsyncIterator[DataModel]:
        """
        Stream records from ``time_range.start``, tailing when the range is open.

        A bounded range stops at the topic's high watermark or at the requested
        end, whichever comes first. An open range keeps polling indefinitely.
        One consumer per call, with no group and no committed offsets: a
        bridge opens one tail per class per process, which is what this is for.
        """
        if not self.supports(model):
            return

        from aiokafka import AIOKafkaConsumer

        wanted = normalise_mrid_filter(mRID)
        follow_live = time_range.end is None or time_range.end > utcnow()
        topic = self.topic_for(model)
        await self.ensure_topic(model)

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            enable_auto_commit=False,
            group_id=None,
            **self._consumer_options,
        )
        await consumer.start()
        try:
            assignment = await self._await_assignment(consumer, topic, time_range, follow_live)
            if not assignment:
                return
            await self._seek(consumer, assignment, time_range)
            end_offsets = None if follow_live else await consumer.end_offsets(assignment)

            while True:
                batches = await consumer.getmany(timeout_ms=_POLL_TIMEOUT_MS)
                for messages in batches.values():
                    for message in messages:
                        payload = self._decode(message.value, model, topic)
                        if payload is None:
                            continue
                        if time_range.end is not None and payload.timestamp >= time_range.end:
                            return
                        if not time_range.contains(payload.timestamp):
                            continue
                        if wanted is not None and payload.mRID not in wanted:
                            continue
                        yield payload

                if time_range.end is not None and utcnow() >= time_range.end:
                    return
                if end_offsets is not None and await _drained(consumer, end_offsets):
                    return
        finally:
            await _stop_consumer(consumer)

    async def produce(self, data: DataModel) -> None:
        """Publish a payload to its model's topic, keyed by its identifier."""
        model = type(data)
        if not self.supports(model, Capability.PRODUCE):
            raise TypeError(f"{self.name} cannot produce {model.__name__}")
        if data.timestamp is None:
            raise ValueError(f"{self.name}: a {model.__name__} needs a timestamp to be produced")

        if self._producer is None:
            await self.open()
        await self.ensure_topic(model)
        await self._producer.send_and_wait(
            self.topic_for(model),
            value=data.model_dump_json().encode(),
            key=None if data.mRID is None else str(data.mRID).encode(),
            timestamp_ms=int(data.timestamp.timestamp() * 1000),
        )

    async def open(self) -> None:
        """Start the shared producer, when this client produces."""
        if Capability.PRODUCE not in self.capabilities or self._producer is not None:
            return
        from aiokafka import AIOKafkaProducer

        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers, **self._producer_options)
        await producer.start()
        self._producer = producer

    async def close(self) -> None:
        """Stop the shared producer and the admin client."""
        producer, self._producer = self._producer, None
        admin, self._admin = self._admin, None
        if producer is not None:
            await producer.stop()
        if admin is not None:
            await admin.close()

    # --- internals ----------------------------------------------------------------------

    async def _await_assignment(
        self, consumer: Any, topic: str, time_range: TimeRange, follow_live: bool
    ) -> list[Any] | None:
        """Wait until the subscription resolves to concrete partitions.

        A live tail tolerates a topic that does not exist yet (the first
        producer creates it); a historical read waits briefly, then gives up.
        """
        deadline = utcnow() + _ASSIGNMENT_TIMEOUT
        while True:
            assignment = consumer.assignment()
            if assignment:
                return sorted(assignment)
            now = utcnow()
            if follow_live:
                if time_range.end is not None and now >= time_range.end:
                    return None
            elif now >= deadline:
                logger.warning("%s: no partitions for topic %s", self.name, topic)
                return None
            await consumer.getmany(timeout_ms=_ASSIGNMENT_POLL_MS)

    async def _seek(self, consumer: Any, assignment: list[Any], time_range: TimeRange) -> None:
        """Position every partition at the requested start instant."""
        if time_range.start is None:
            await consumer.seek_to_beginning(*assignment)
            return
        target = int(time_range.start.timestamp() * 1000)
        offsets = await consumer.offsets_for_times({partition: target for partition in assignment})
        for partition, offset in offsets.items():
            if offset is None:
                await consumer.seek_to_end(partition)
            else:
                consumer.seek(partition, offset.offset)

    def _decode(self, value: bytes | None, model: type[DataModel], topic: str) -> DataModel | None:
        """Deserialise a record, dropping anything that does not validate."""
        if value is None:
            return None
        try:
            payload = model.model_validate_json(value)
        except Exception as error:
            logger.warning("%s: skipping undecodable record on %s: %s", self.name, topic, error)
            return None
        if payload.timestamp is None:
            logger.warning("%s: skipping record without timestamp on %s", self.name, topic)
            return None
        return payload


async def _drained(consumer: Any, end_offsets: dict[Any, int]) -> bool:
    """Whether every assigned partition has reached its high watermark."""
    for partition, end in end_offsets.items():
        if await consumer.position(partition) < end:
            return False
    return True


async def _stop_consumer(consumer: Any) -> None:
    """Stop a consumer, absorbing the cancellation aiokafka raises on shutdown."""
    try:
        await consumer.stop()
    except asyncio.CancelledError:
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise
        logger.debug("ignoring a cancellation raised while stopping the Kafka consumer")
