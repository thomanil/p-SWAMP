# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A dummy remote data service behind the REST contract -- the service
side of ``pswamp_core.datagateway.clients.remote_data.RemoteDataClient``.

It lives under ``core/examples/`` beside everything else of the remote data
contract -- the client and line model in ``pswamp_core``, the black-box check
``core/examples/check_remote_data_service.py`` -- and outside the installed
``pswamp_core`` package, because it is a service rather than library code and
needs FastAPI and uvicorn (the core's ``examples`` dependency group). It
imports only ``pswamp_core`` and never anything under ``app/``. It runs as a
**separate process from the same image** -- ``python -m remote_data_stub`` with
``core/examples`` on ``PYTHONPATH`` -- standing where a deployment's own remote
data service would stand, so that the whole path (client → REST →
this service → streamed response → client → player → page) can be run, tested and
demonstrated with nothing outside this repo.

It is held to the contract exactly as a deployment's service is: over plain
HTTP, by ``scripts/check-remote-data-service.sh``, which CI runs against it on
every pull request. So it must never lean on being Python or on sharing the
client's process -- the Python-side tests may, the contract may not.

What a real service keeps behind the contract is its own business. This one
plays the part of a time series store: its "database" is a three-second
sample recording (a copy of the PMU test streamer's, as ``pmu.frame`` lines),
tiled ``REPEAT`` times back to back so there is a minute of timeline to query.

What is here, and what each piece is for:

* ``recording.py`` -- the tiled recording and the two questions a store answers:
  what do you hold (``coverage``), and give me this window (``select``).
* ``sample_frames.ndjson`` -- its data: sixty ``pmu.frame`` lines.
* ``service.py`` -- ``QueryService``: turns a ``RemoteDataQuery`` into the run
  of ``RemoteDataResult`` lines the contract prescribes, as an async generator
  pulled only as fast as the connection drains. Knows nothing of HTTP, so the
  tests iterate it directly.
* ``app.py`` -- the two REST routes plus ``/healthz``, as a FastAPI app over
  a ``QueryService``; a query's answer is a ``StreamingResponse``.
  ``httpx.ASGITransport`` drives it in-process in tests.
* ``__main__.py`` -- configuration from ``REMOTE_DATA_STUB_*`` and uvicorn.
"""
