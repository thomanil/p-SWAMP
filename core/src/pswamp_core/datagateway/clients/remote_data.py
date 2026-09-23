# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Remote Data Client: ask a deployment's own data service for a range, over
REST up and a Kafka topic down.

The class docstring of :class:`RemoteDataClient` is the contract a
deployment implements against. This module docstring only says where it sits:
it is the first provider in the core that talks to something *outside the
process*, and the reference for the shape STEP 1 A5 called "query a chunk is
request/response" -- the request is an HTTP ``POST``, the response is a run of
records on a topic, and the two are tied by a ``query_id``.

The name is about the *decoupling*, not the storage. What answers the queries
is the deployment's business: a time-series database, a historian, an archive
of files, a cache in front of any of those. p-SWAMP only fixes the handful of
routes and the envelope on the topic, so a deployment can keep its store, and
swap it, without a change in this repo.

Requires the ``remote-data`` extra (``pswamp-core[remote-data]``: httpx and
aiokafka). Both are imported inside the methods that need them, so importing
this module -- and naming the class in a ``PSWAMP_DATA_CLIENTS`` spec -- costs
nothing without them, and the two seams (``http_client``, ``feed``) let the
tests run it with neither.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol
from uuid import uuid4

from ...log import get_logger
from ...messages.pmu import PmuFrame
from ...messages.remote_data import RemoteDataQuery, RemoteDataResult
from ...util.time import ensure_utc
from ..config import EnvSetting
from ..data_client_model import Capability, DataClient, MRIDFilter, normalise_mrid_filter
from ..time_range import Coverage, TimeRange

if TYPE_CHECKING:
    from ...messages.data_model import DataModel

__all__ = [
    "InMemoryResultFeed",
    "KafkaResultFeed",
    "ResultFeed",
    "ResultSubscription",
    "RemoteDataClient",
]

logger = get_logger("pswamp_core.datagateway.clients.remote_data")

#: How long one HTTP call to the service may take.
_HTTP_TIMEOUT_S = 10.0
#: How long a best-effort cancel may take; the stream is already being closed.
_CANCEL_TIMEOUT_S = 2.0
#: The results topic has one partition, by contract.
_PARTITION = 0


# --- the results feed: one topic, demultiplexed by query id ------------------------


class ResultSubscription:
    """The envelopes of one query, as they arrive.

    An ``async with`` block: entering registers the query's queue with the feed,
    leaving unregisters it, so an envelope for a query nobody waits on any more
    is dropped rather than held for ever.
    """

    def __init__(self, feed: _DemuxFeed, query_id: str) -> None:
        self.query_id = query_id
        self._feed = feed
        self.queue: asyncio.Queue[RemoteDataResult] = asyncio.Queue()

    async def __aenter__(self) -> ResultSubscription:
        self._feed._register(self.query_id, self.queue)
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._feed._unregister(self.query_id)

    async def next(self, timeout: float) -> RemoteDataResult:
        """The next envelope, or ``TimeoutError`` after ``timeout`` seconds."""
        return await asyncio.wait_for(self.queue.get(), timeout)


class ResultFeed(Protocol):
    """Where the client reads a query's envelopes from.

    ``KafkaResultFeed`` is the real one; ``InMemoryResultFeed`` is the test
    double, and doubles as the stub service's sink so a test can wire a service
    to a client with no broker between them.
    """

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def subscribe(self, query_id: str) -> ResultSubscription: ...

    def has(self, query_id: str) -> bool: ...


class _DemuxFeed:
    """The demultiplexing every feed shares: a queue per query id."""

    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[RemoteDataResult]] = {}
        #: Envelopes for a query nobody was waiting on (cancelled, or another
        #: process's), counted so a test can see the drop.
        self.dropped = 0

    def subscribe(self, query_id: str) -> ResultSubscription:
        return ResultSubscription(self, query_id)

    def has(self, query_id: str) -> bool:
        return query_id in self._queues

    def dispatch(self, result: RemoteDataResult) -> None:
        queue = self._queues.get(result.query_id)
        if queue is None:
            self.dropped += 1
            return
        queue.put_nowait(result)

    def _register(self, query_id: str, queue: asyncio.Queue[RemoteDataResult]) -> None:
        self._queues[query_id] = queue

    def _unregister(self, query_id: str) -> None:
        self._queues.pop(query_id, None)

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return


class InMemoryResultFeed(_DemuxFeed):
    """A feed fed by hand -- or by a service in the same process, which is why
    ``publish`` is a coroutine: it satisfies the stub service's sink protocol."""

    async def publish(self, result: RemoteDataResult) -> None:
        self.dispatch(result)


class KafkaResultFeed(_DemuxFeed):
    """The results topic, tailed from its end and demultiplexed by ``query_id``.

    One consumer per feed (so one per client instance, so one per pipeline):
    partition 0 assigned by hand -- no consumer group, no committed offsets, a
    restart wants *now* -- and positioned at the end when it starts, so nothing
    another pipeline asked for earlier is ever replayed into this one. The topic
    is created if the broker lacks it, because the compose and k8s brokers have
    auto-creation off and the server has no start-order dependency on the
    service that would otherwise create it.
    """

    def __init__(
        self,
        bootstrap_servers: str | Sequence[str],
        topic: str,
        *,
        replication_factor: int = 1,
        name: str = "remote_data",
    ) -> None:
        super().__init__()
        self.bootstrap_servers = (
            bootstrap_servers if isinstance(bootstrap_servers, str) else list(bootstrap_servers)
        )
        self.topic = topic
        self.replication_factor = replication_factor
        self.name = name
        self._task: asyncio.Task | None = None
        self._start_lock = asyncio.Lock()

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        async with self._start_lock:
            if self.running:
                return
            from aiokafka import AIOKafkaConsumer, TopicPartition
            from aiokafka.admin import AIOKafkaAdminClient

            from ...transport.kafka import create_topic

            admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            await admin.start()
            try:
                await create_topic(
                    admin, self.topic, replication_factor=self.replication_factor, who=self.name
                )
            finally:
                await admin.close()
            consumer = AIOKafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=None,
                enable_auto_commit=False,
                auto_offset_reset="latest",
            )
            await consumer.start()
            try:
                partition = TopicPartition(self.topic, _PARTITION)
                consumer.assign([partition])
                await consumer.seek_to_end(partition)
            except BaseException:
                await consumer.stop()
                raise
            self._task = asyncio.create_task(self._read(consumer), name=f"{self.name}.results")
            logger.info("%s: tailing %s on %s", self.name, self.topic, self.bootstrap_servers)

    async def _read(self, consumer: Any) -> None:
        from ...transport.kafka import _stop_consumer

        try:
            async for record in consumer:
                try:
                    result = RemoteDataResult.model_validate_json(record.value)
                except Exception as error:
                    logger.warning(
                        "%s: dropping undecodable record on %s: %s", self.name, self.topic, error
                    )
                    continue
                self.dispatch(result)
        finally:
            await _stop_consumer(consumer)

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


# --- the provider -----------------------------------------------------------------


class RemoteDataClient(DataClient):
    """A ``DataClient`` over a remote data service: range queries go up as REST
    calls, the answers come down on a Kafka topic.

    **What it is for: decoupling.** A deployment keeps its PMU history
    somewhere this repo neither ships nor knows -- a time-series database, a
    historian, an archive service -- and must stay free to choose, and change,
    that store. This client does not talk to any store. It talks to a small
    service the deployment's owner implements once in front of whatever they
    run, speaking the contract below. After that the whole stack (replay, seek,
    range queries, batch modules) works over their data with no code in this
    repo changed: name the client in ``PSWAMP_DATA_CLIENTS`` (or an app's own
    ``*_DATA_CLIENTS`` variable) and set its variables. What sits behind the
    service is an implementation detail on the deployment's side; the stub in
    ``app/server-python/src/remote_data_stub/`` happens to serve a recording
    as if it were a time series store.

    **Capabilities.** ``HISTORY_CONSUME`` only, for ``PmuFrame``. It never
    tails: the service answers about the past. Pair it with
    a live provider in the same gateway for a replay that hands over to live.

    **Configuration** -- the ``{NAME}_`` block, ``NAME`` being the client's name
    in the spec (``remote_data:...:RemoteDataClient`` reads ``REMOTE_DATA_*``)::

        REMOTE_DATA_URL                http://remote-data-stub:8100   (required) base URL of the service
        REMOTE_DATA_BOOTSTRAP_SERVERS  kafka:9092                     (required) brokers of the results topic
        REMOTE_DATA_TOPIC              remote.data.result             the results topic (this is the default)
        REMOTE_DATA_TIMEOUT            30                             seconds to wait for the next result
        REMOTE_DATA_PRIORITY           0                              preference against other clients

    ``show_config("remote_data")`` prints the same table.

    **The REST half** (what the service serves under ``URL``; JSON throughout):

    ``GET /v1/coverage?model=<topic>``
        What the service holds for the message class named by its topic string
        (``pmu.frame``). Answer ``200`` with
        ``{"start": <ISO 8601>, "end": <ISO 8601>}``; any other field (a
        ``model`` echo, say) is ignored. There is no liveness to report: the
        client is history-only by its own declaration. ``end`` is
        *exclusive* and must lie past the last record's timestamp (pad it by a
        microsecond), because the player bounds every replay to it and the
        gateway drops a record at or after it. ``{"start": null, ...}`` means
        "nothing", and the client reports no coverage. This is asked often --
        on every seek, at every segment boundary -- so keep it cheap.

    ``POST /v1/queries``
        Body: a ``RemoteDataQuery`` (``pswamp_core.messages.remote_data``) --
        ``{"version": "v1", "query_id": "...", "model": "pmu.frame", "start":
        <ISO|null>, "end": <ISO|null>, "mrid": [..]|null}``. The window is
        half-open, ``[start, end)``; a null bound is open. Answer ``202`` once
        the query is accepted; the records follow on the topic (below). Any
        other status makes the client raise, which ends the pipeline's stream
        with that error.

    ``DELETE /v1/queries/{query_id}``
        Best-effort cancel: the client sends it when a stream is closed before
        the query ended (a seek, a page closing), so a large query is not run to
        completion for nobody. Answer ``204`` (or ``404`` once it is gone);
        the client ignores the reply.

    **The Kafka half** (what the service publishes; ``TOPIC`` on
    ``BOOTSTRAP_SERVERS``): one ``RemoteDataResult`` envelope per record, value
    ``model_dump_json()``, **record key = ``query_id``**. ``kind`` is
    ``"record"`` (``model`` the topic string, ``record`` the message as JSON)
    for each record in ascending timestamp order, then exactly one ``"end"``
    (``count``) -- or one ``"error"`` (``error``) instead, at any point. ``seq``
    counts from 0 per query. **One partition**: Kafka orders within a partition
    only, and the gateway's watermark drops out-of-order records. The client
    creates the topic with one partition if the broker lacks it; so should the
    service, since neither knows who starts first.

    **What the client does with that**, in order, on every ``consume``:

    1. makes a ``query_id`` and registers a queue for it on the feed **before**
       anything is sent -- the consumer reads the topic from its end, so an
       envelope published before the queue exists would be lost;
    2. ``POST``s the query;
    3. yields each ``record`` envelope's record, validated against the model
       class named by its ``model`` (an unknown one is logged and skipped), and
       returns at ``end``; raises ``RuntimeError`` at ``error``; raises
       ``TimeoutError`` when ``TIMEOUT`` passes between two envelopes (a service
       that accepted and then went quiet). Either exception ends the pipeline's
       stream *paused with* ``PlayerStatus.error`` set (the player catches it),
       and the next play or seek retries from scratch;
    4. on any early exit -- the gateway closing the stream for a seek, the
       socket going away -- ``DELETE``s the query.

    **Limits, stated rather than hidden.** No paging and no backpressure: the
    service publishes the whole range at its own pace and the per-query queue
    grows until the player has paced it out, so a very large range costs memory
    on both sides. One Kafka consumer per client instance, and a gateway is
    built per pipeline, so one per pipeline: ``MAX_PIPELINES`` bounds it. The
    record is decoded twice (topic JSON → envelope, ``record`` dict → model),
    the price of an envelope that carries any model. Only PMU models are wired;
    ``supported_models`` is the place to widen that.

    **Testing it without a service or a broker.** Two seams: ``http_client`` (an
    ``httpx.AsyncClient``; give it ``httpx.MockTransport`` or, against a FastAPI
    stub, ``httpx.ASGITransport``) and ``feed`` (an ``InMemoryResultFeed``,
    which the stub service can publish straight into). The conformance suite
    runs the client over both in ``app/server-python/tests/``.
    """

    env_settings = (
        EnvSetting(
            "URL",
            "Base URL of the remote data service's REST api, e.g. http://remote-data:8100",
            required=True,
        ),
        EnvSetting(
            "BOOTSTRAP_SERVERS",
            "Comma-separated Kafka brokers carrying the results topic, e.g. kafka:9092",
            required=True,
            kind="list",
        ),
        EnvSetting(
            "TOPIC",
            "The Kafka topic the service publishes query results on",
            default=RemoteDataResult.topic,
        ),
        EnvSetting(
            "TIMEOUT",
            "Seconds to wait for the next result of a query before giving it up",
            default="30",
            kind="seconds",
        ),
        EnvSetting("PRIORITY", "Preference against other clients", default="0", kind="int"),
    )

    def __init__(
        self,
        name: str = "remote_data",
        url: str | None = None,
        bootstrap_servers: str | Sequence[str] = (),
        *,
        topic: str = RemoteDataResult.topic,
        timeout: timedelta = timedelta(seconds=30),
        priority: int = 0,
        http_client: Any | None = None,
        feed: ResultFeed | None = None,
    ) -> None:
        if not url and http_client is None:
            raise ValueError(f"{name}: a URL is required (or an http_client to use instead)")
        if feed is None and not bootstrap_servers:
            raise ValueError(f"{name}: BOOTSTRAP_SERVERS is required (or a feed to use instead)")
        self.name = name
        self.capabilities = Capability.HISTORY_CONSUME
        self.supported_models = {PmuFrame}
        self.priority = priority
        self.url = (url or "").rstrip("/")
        self.topic = topic
        self.timeout = timeout
        self._by_topic: dict[str, type[DataModel]] = {m.topic: m for m in self.supported_models}
        self._http = http_client
        self._owns_http = http_client is None
        self.feed: ResultFeed = (
            feed if feed is not None else KafkaResultFeed(bootstrap_servers, topic, name=name)
        )

    # -- lifecycle -------------------------------------------------------------

    async def open(self) -> None:
        self._ensure_http()
        await self.feed.start()

    async def close(self) -> None:
        await self.feed.stop()
        if self._owns_http and self._http is not None:
            http, self._http = self._http, None
            await http.aclose()

    def _ensure_http(self) -> Any:
        if self._http is None:
            import httpx

            self._http = httpx.AsyncClient(base_url=self.url, timeout=_HTTP_TIMEOUT_S)
        return self._http

    # -- the contract ----------------------------------------------------------

    async def coverage(self, model: type[DataModel], mRID: MRIDFilter = None) -> Coverage | None:
        if not self.supports(model):
            return None
        http = self._ensure_http()
        try:
            response = await http.get("/v1/coverage", params={"model": model.topic})
        except Exception as error:
            # Name the URL: this is the message a person sees when the service is
            # down (the player carries it into the error topic).
            raise ConnectionError(
                f"cannot reach {self.url or 'the service'}: {type(error).__name__}: {error}"
            ) from error
        response.raise_for_status()
        body = response.json() or {}
        start = body.get("start")
        if start is None:
            return None
        end = body.get("end")
        # History-only by declaration, so liveness is the client's to state, not
        # the service's: a "live" field in the answer is ignored.
        return Coverage(
            TimeRange(_parse_instant(start), None if end is None else _parse_instant(end)),
            live=False,
        )

    async def consume(
        self,
        model: type[DataModel],
        time_range: TimeRange,
        mRID: MRIDFilter = None,
    ) -> AsyncIterator[DataModel]:
        if not self.supports(model, Capability.HISTORY_CONSUME):
            return
        http = self._ensure_http()
        await self.feed.start()
        wanted = normalise_mrid_filter(mRID)
        query = RemoteDataQuery(
            query_id=uuid4().hex,
            model=model.topic,
            start=time_range.start,
            end=time_range.end,
            mrid=None if wanted is None else sorted(wanted),
        )
        timeout_s = self.timeout.total_seconds()
        finished = False
        # Subscribe first: the feed reads the topic from its end, so an answer
        # published before the queue exists would never be seen.
        async with self.feed.subscribe(query.query_id) as results:
            try:
                response = await http.post("/v1/queries", json=query.model_dump(mode="json"))
            except Exception as error:
                finished = True
                raise ConnectionError(
                    f"cannot reach {self.url or 'the service'}: {type(error).__name__}: {error}"
                ) from error
            if response.status_code != 202:
                finished = True
                raise RuntimeError(
                    f"{self.name}: {self.url or 'the service'} refused query {query.query_id}: "
                    f"HTTP {response.status_code} {response.text[:200]}"
                )
            try:
                while True:
                    try:
                        result = await results.next(timeout_s)
                    except TimeoutError:
                        raise TimeoutError(
                            f"{self.name}: no result for query {query.query_id} within "
                            f"{timeout_s:g}s"
                        ) from None
                    if result.kind == "end":
                        finished = True
                        return
                    if result.kind == "error":
                        finished = True
                        raise RuntimeError(
                            f"{self.name}: query {query.query_id} failed at the service: "
                            f"{result.error}"
                        )
                    cls = self._by_topic.get(result.model or "")
                    if cls is None:
                        logger.warning(
                            "%s: skipping a record of unknown model %r on query %s",
                            self.name,
                            result.model,
                            query.query_id,
                        )
                        continue
                    record = cls.model_validate(result.record)
                    if not time_range.contains(record.timestamp):
                        continue
                    yield record
            finally:
                if not finished:
                    await self._cancel(query.query_id)

    async def produce(self, data: DataModel) -> None:
        raise TypeError(f"{self.name} is read-only: it queries a remote data service, it does not write to one")

    # -- internals -------------------------------------------------------------

    async def _cancel(self, query_id: str) -> None:
        """Tell the service to stop a query the stream no longer wants. Best effort."""
        http = self._http
        if http is None:
            return
        try:
            await http.delete(f"/v1/queries/{query_id}", timeout=_CANCEL_TIMEOUT_S)
        except Exception as error:  # the stream is already closing; only note it
            logger.debug("%s: cancel of query %s did not reach the service: %s", self.name, query_id, error)


def _parse_instant(value: str) -> datetime:
    return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
