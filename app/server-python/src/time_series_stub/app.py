# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The REST surface of the contract, as a FastAPI app over a ``QueryService``.

Three routes and a probe -- exactly what
``TimeSeriesDatabaseClient``'s docstring asks a deployment to serve:

    GET    /v1/coverage?model=<topic>   → 200 {model, start, end, live} | 404 unknown model
    POST   /v1/queries  (TimeSeriesQuery) → 202 {query_id} | 409 already running
    DELETE /v1/queries/{query_id}       → 204 | 404 not running
    GET    /healthz                     → 200

``create_app`` takes the service rather than building it, so a test can run
the app in-process (``httpx.ASGITransport``) over a list sink.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Response

from pswamp_core.messages import TimeSeriesQuery

from .service import QueryService

__all__ = ["create_app"]


def create_app(
    service: QueryService,
    *,
    on_startup: Callable[[], Any] | None = None,
    on_shutdown: Callable[[], Any] | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        if on_startup is not None:
            await on_startup()
        try:
            yield
        finally:
            await service.shutdown()
            if on_shutdown is not None:
                await on_shutdown()

    app = FastAPI(
        title="p-SWAMP time-series stub",
        description=(
            "A dummy time-series store behind the REST + Kafka provider contract. "
            "Answers coverage over HTTP; publishes query results on a Kafka topic."
        ),
        lifespan=lifespan,
    )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/coverage")
    async def coverage(model: str) -> dict[str, Any]:
        document = service.coverage(model)
        if document is None:
            raise HTTPException(status_code=404, detail=f"no such model: {model!r}")
        return document

    @app.post("/v1/queries", status_code=202)
    async def start_query(query: TimeSeriesQuery) -> dict[str, str]:
        try:
            service.start_query(query)
        except KeyError:
            raise HTTPException(status_code=409, detail=f"query {query.query_id} is already running")
        return {"query_id": query.query_id}

    @app.delete("/v1/queries/{query_id}", status_code=204)
    async def cancel_query(query_id: str) -> Response:
        if not service.cancel(query_id):
            raise HTTPException(status_code=404, detail=f"query {query_id} is not running")
        return Response(status_code=204)

    return app
