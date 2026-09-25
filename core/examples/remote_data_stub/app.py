# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The REST surface of the contract, as a FastAPI app over a ``QueryService``.

Two routes and a probe -- exactly what ``RemoteDataClient``'s docstring asks a
deployment to serve:

    GET    /v1/coverage?model=<topic>     → 200 {model, start, end} | 404 unknown model
    POST   /v1/queries  (RemoteDataQuery) → 200 streamed NDJSON lines | 404 unknown model | 422 bad body
    GET    /healthz                       → 200

There is no cancel route: a client that closes the connection has cancelled,
and Starlette's ``StreamingResponse`` stops the generator when it does.

``create_app`` takes the service rather than building it, so a test can run
the app in-process (``httpx.ASGITransport``).
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

from pswamp_core.messages import RemoteDataQuery

from .service import QueryService

__all__ = ["create_app"]

#: The NDJSON media type the client reads, and the headers that keep a proxy
#: from buffering the stream into one blob (nginx honours X-Accel-Buffering).
MEDIA_TYPE = "application/x-ndjson"
STREAM_HEADERS = {"X-Accel-Buffering": "no", "Cache-Control": "no-cache"}


def create_app(service: QueryService) -> FastAPI:
    app = FastAPI(
        title="p-SWAMP remote data stub",
        description=(
            "A dummy remote data service behind the Remote Data Client's REST contract, serving a "
            "recording as a time series store would. Answers coverage as JSON, and each query "
            "as a streamed NDJSON response."
        ),
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

    @app.post("/v1/queries", response_class=StreamingResponse)
    async def query(query: RemoteDataQuery) -> StreamingResponse:
        # Everything that can be refused is refused here, while a status code
        # can still say so; after this the answer is lines.
        if not service.knows(query.model):
            raise HTTPException(status_code=404, detail=f"no such model: {query.model!r}")
        return StreamingResponse(service.stream(query), media_type=MEDIA_TYPE, headers=STREAM_HEADERS)

    return app
