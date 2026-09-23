# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The results topic as a ``Sink``: one aiokafka producer, key = ``query_id``.

Creates the topic with one partition if the broker lacks it (auto-creation is
off in compose and k8s) -- as does the client on its side, since neither knows
which starts first. aiokafka is imported inside ``open``, so importing this
module costs nothing without the extra.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pswamp_core.log import get_logger
from pswamp_core.messages import RemoteDataResult

__all__ = ["KafkaSink"]

logger = get_logger("remote-data-stub.kafka")


class KafkaSink:
    def __init__(
        self,
        bootstrap_servers: str | Sequence[str],
        topic: str = RemoteDataResult.topic,
        *,
        replication_factor: int = 1,
    ) -> None:
        self.bootstrap_servers = (
            bootstrap_servers if isinstance(bootstrap_servers, str) else list(bootstrap_servers)
        )
        self.topic = topic
        self.replication_factor = replication_factor
        self._producer: Any | None = None
        self.published = 0

    async def open(self) -> None:
        if self._producer is not None:
            return
        from aiokafka import AIOKafkaProducer
        from aiokafka.admin import AIOKafkaAdminClient

        from pswamp_core.transport.kafka import create_topic

        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            await create_topic(
                admin, self.topic, replication_factor=self.replication_factor, who="remote-data-stub"
            )
        finally:
            await admin.close()
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers, request_timeout_ms=10_000)
        await producer.start()
        self._producer = producer
        logger.info("publishing results on %s at %s", self.topic, self.bootstrap_servers)

    async def close(self) -> None:
        producer, self._producer = self._producer, None
        if producer is not None:
            await producer.stop()

    async def publish(self, result: RemoteDataResult) -> None:
        if self._producer is None:
            await self.open()
        await self._producer.send_and_wait(
            self.topic, value=result.model_dump_json().encode(), key=result.query_id.encode()
        )
        self.published += 1
