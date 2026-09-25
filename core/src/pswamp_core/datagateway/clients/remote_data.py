# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Remote Data Client: ask a deployment's own data service for a range, and
read the answer back as the streamed body of that same HTTP call.

The contract a deployment implements against is
``doc/remote-data-integration-contract.md``, written in HTTP terms alone so the
service can be built on any stack; the class docstring below summarises it and
says what this client does with it. This module docstring only says where it sits:
it is the first provider in the core that talks to something *outside the
process*, and the reference for the shape STEP 1 A5 called "query a chunk is
request/response" -- the request is an HTTP ``POST``, and the response is the
run of records, one NDJSON line each, arriving while the player consumes them.

The name is about the *decoupling*, not the storage. What answers the queries
is the deployment's business: a time-series database, a historian, an archive
of files, a cache in front of any of those. p-SWAMP only fixes the two routes
and the line format, so a deployment can keep its store, and swap it, without
a change in this repo.

Requires the ``remote-data`` extra (``pswamp-core[remote-data]``: httpx). It is
imported inside the methods that need it, so importing this module -- and
naming the class in a ``PSWAMP_DATA_CLIENTS`` spec -- costs nothing without it,
and the ``http_client`` seam lets the tests run it with no socket.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
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

__all__ = ["RemoteDataClient"]

logger = get_logger("pswamp_core.datagateway.clients.remote_data")

#: How long one ordinary HTTP call (coverage), or opening a connection, may take.
_HTTP_TIMEOUT_S = 10.0


class RemoteDataClient(DataClient):
    """A ``DataClient`` over a remote data service: a range query goes up as a
    REST call, and its answer streams back as that call's response body.

    **What it is for: decoupling.** A deployment keeps its PMU history
    somewhere this repo neither ships nor knows -- a time-series database, a
    historian, an archive service -- and must stay free to choose, and change,
    that store. This client does not talk to any store. It talks to a small
    service the deployment's owner implements once in front of whatever they
    run, speaking the contract summarised below (the normative text, in HTTP
    terms only, is ``doc/remote-data-integration-contract.md``, and
    ``scripts/check-remote-data-service.sh`` checks a service against it). After
    that the whole stack (replay, seek,
    range queries, batch modules) works over their data with no code in this
    repo changed: name the client in ``PSWAMP_DATA_CLIENTS`` (or an app's own
    ``*_DATA_CLIENTS`` variable) and set its variables. What sits behind the
    service is an implementation detail on the deployment's side; the stub in
    ``core/examples/remote_data_stub/`` happens to serve a recording
    as if it were a time series store.

    **Capabilities.** ``HISTORY_CONSUME`` only, for ``PmuFrame``. It never
    tails: the service answers about the past. Pair it with
    a live provider in the same gateway for a replay that hands over to live.

    **Configuration** -- the ``{NAME}_`` block, ``NAME`` being the client's name
    in the spec (``remote_data:...:RemoteDataClient`` reads ``REMOTE_DATA_*``)::

        REMOTE_DATA_URL       http://remote-data-stub:8100   (required) base URL of the service
        REMOTE_DATA_TIMEOUT   30                             seconds to wait for a response to start, and for each next line
        REMOTE_DATA_PRIORITY  0                              preference against other clients

    ``show_config("remote_data")`` prints the same table.

    **The contract, in short** (what the service serves under ``URL``):

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
        half-open, ``[start, end)``; a null bound is open; ``query_id`` is for
        the two sides' logs only. Answer ``200``, ``Content-Type:
        application/x-ndjson``, and **stream** the body: one
        ``RemoteDataResult`` per line -- ``{"kind": "record", "model":
        "pmu.frame", "record": {...}}`` for each record in ascending timestamp
        order, then exactly one ``{"kind": "end", "count": N}``, or one
        ``{"kind": "error", "error": "..."}`` instead, at any point. Anything
        known before the first byte (an unknown model, a bad body) is a status
        code instead: any status but ``200`` makes the client raise, which ends
        the pipeline's stream with that error.

        **Stream it; do not build it.** Write each line as the store yields it
        and let the socket's flow control pace you: the client reads a line
        only when the player wants the next frame, so a service that honours
        backpressure reads its store at replay speed and holds one record in
        memory, not the range. **A closed connection is a cancel**: stop the
        store read when the client goes away (a seek, a page closing) -- there
        is no cancel route, and none is needed.

    **What the client does with that**, in order, on every ``consume``:

    1. makes a ``query_id`` and ``POST``s the query, reading the response as a
       stream; ``TIMEOUT`` bounds the wait for it to start;
    2. yields each ``record`` line's record, validated against the model class
       named by its ``model`` (an unknown one is logged and skipped), reading
       the next line only when asked for the next record, and returns at
       ``end``;
    3. raises ``RuntimeError`` at ``error``, at a non-``200`` status, or when
       the body ends with no terminal line; ``ConnectionError`` when the
       connection breaks; ``TimeoutError`` when ``TIMEOUT`` passes while it is
       waiting for a line (a service that went quiet). Any of those ends the
       pipeline's stream *paused with* ``PlayerStatus.error`` set (the player
       catches it), and the next play or seek retries from scratch;
    4. on any early exit -- the gateway closing the stream for a seek, the
       socket going away -- closes the response, which closes the connection.

    **Limits, stated rather than hidden.** A replay holds its connection open
    for as long as it plays, and a *paused* replay holds it open idle: the
    client only times out while it is waiting for a line, never while the
    player is not asking for one. So a proxy between p-SWAMP and the service
    must neither buffer the response (nginx: ``proxy_buffering off``; the stub
    sends ``X-Accel-Buffering: no``) nor cut an idle one short, or a long pause
    ends in a ``ConnectionError`` on resume -- recovered by the next play, but
    visible. One connection per open stream, so ``MAX_PIPELINES`` bounds them.
    The record is decoded twice (line JSON → envelope, ``record`` dict → model),
    the price of an envelope that carries any model. Only PMU models are wired;
    ``supported_models`` is the place to widen that.

    **Testing it without a service.** One seam, ``http_client``: an
    ``httpx.AsyncClient`` over ``httpx.MockTransport`` or, against a FastAPI
    stub, ``httpx.ASGITransport`` (which buffers the whole body before
    returning it, so it proves the contract but not the streaming). The
    conformance suite runs the client over the stub in
    ``core/tests/test_remote_data_service.py``.
    """

    env_settings = (
        EnvSetting(
            "URL",
            "Base URL of the remote data service's REST api, e.g. http://remote-data:8100",
            required=True,
        ),
        EnvSetting(
            "TIMEOUT",
            "Seconds to wait for a query's response to start, and for each next line of it",
            default="30",
            kind="seconds",
        ),
        EnvSetting("PRIORITY", "Preference against other clients", default="0", kind="int"),
    )

    def __init__(
        self,
        name: str = "remote_data",
        url: str | None = None,
        *,
        timeout: timedelta = timedelta(seconds=30),
        priority: int = 0,
        http_client: Any | None = None,
    ) -> None:
        if not url and http_client is None:
            raise ValueError(f"{name}: a URL is required (or an http_client to use instead)")
        self.name = name
        self.capabilities = Capability.HISTORY_CONSUME
        self.supported_models = {PmuFrame}
        self.priority = priority
        self.url = (url or "").rstrip("/")
        self.timeout = timeout
        self._by_topic: dict[str, type[DataModel]] = {m.topic: m for m in self.supported_models}
        self._http = http_client
        self._owns_http = http_client is None

    # -- lifecycle -------------------------------------------------------------

    async def open(self) -> None:
        self._ensure_http()

    async def close(self) -> None:
        if self._owns_http and self._http is not None:
            http, self._http = self._http, None
            await http.aclose()

    def _ensure_http(self) -> Any:
        if self._http is None:
            import httpx

            self._http = httpx.AsyncClient(base_url=self.url, timeout=_HTTP_TIMEOUT_S)
        return self._http

    @property
    def _where(self) -> str:
        return self.url or "the service"

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
                f"cannot reach {self._where}: {type(error).__name__}: {error}"
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
        import httpx

        http = self._ensure_http()
        wanted = normalise_mrid_filter(mRID)
        query = RemoteDataQuery(
            query_id=uuid4().hex,
            model=model.topic,
            start=time_range.start,
            end=time_range.end,
            mrid=None if wanted is None else sorted(wanted),
        )
        timeout_s = self.timeout.total_seconds()
        qid = query.query_id
        # No httpx read timeout on the stream: TIMEOUT is ours to enforce, and
        # only while we are actually waiting -- a paused replay reads nothing
        # for as long as it is paused, and that is not the service's fault.
        request = http.build_request(
            "POST",
            "/v1/queries",
            json=query.model_dump(mode="json"),
            timeout=httpx.Timeout(_HTTP_TIMEOUT_S, read=None),
        )
        try:
            async with asyncio.timeout(timeout_s):
                response = await http.send(request, stream=True)
        except TimeoutError:
            raise TimeoutError(
                f"{self.name}: {self._where} did not answer query {qid} within {timeout_s:g}s"
            ) from None
        except Exception as error:
            raise ConnectionError(
                f"cannot reach {self._where}: {type(error).__name__}: {error}"
            ) from error
        try:
            if response.status_code != 200:
                await response.aread()
                raise RuntimeError(
                    f"{self.name}: {self._where} refused query {qid}: "
                    f"HTTP {response.status_code} {response.text[:200]}"
                )
            lines = response.aiter_lines()
            while True:
                try:
                    async with asyncio.timeout(timeout_s):
                        line = await anext(lines)
                except StopAsyncIteration:
                    raise RuntimeError(
                        f"{self.name}: the answer to query {qid} ended without an end line "
                        "(the service or the connection stopped part way)"
                    ) from None
                except TimeoutError:
                    raise TimeoutError(
                        f"{self.name}: no result for query {qid} within {timeout_s:g}s"
                    ) from None
                except httpx.TransportError as error:
                    raise ConnectionError(
                        f"{self.name}: lost {self._where} during query {qid}: "
                        f"{type(error).__name__}: {error}"
                    ) from error
                if not line.strip():
                    continue
                try:
                    result = RemoteDataResult.model_validate_json(line)
                except ValueError as error:
                    raise RuntimeError(
                        f"{self.name}: unreadable line in the answer to query {qid}: {error}"
                    ) from error
                if result.kind == "end":
                    return
                if result.kind == "error":
                    raise RuntimeError(
                        f"{self.name}: query {qid} failed at the service: {result.error}"
                    )
                cls = self._by_topic.get(result.model or "")
                if cls is None:
                    logger.warning(
                        "%s: skipping a record of unknown model %r on query %s",
                        self.name,
                        result.model,
                        qid,
                    )
                    continue
                record = cls.model_validate(result.record)
                if not time_range.contains(record.timestamp):
                    continue
                yield record
        finally:
            # Closing a response that was not read to its end closes the
            # connection, which is the service's cue to stop reading its store.
            await response.aclose()

    async def produce(self, data: DataModel) -> None:
        raise TypeError(f"{self.name} is read-only: it queries a remote data service, it does not write to one")


def _parse_instant(value: str) -> datetime:
    return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
