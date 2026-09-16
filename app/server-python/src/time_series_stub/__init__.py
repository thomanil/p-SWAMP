# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A dummy time-series store behind the REST + Kafka contract -- the service side
of ``pswamp_core.datagateway.clients.time_series_database.TimeSeriesDatabaseClient``.

Not an app package: it exposes no ``router`` for ``server.py`` and is never in
``APPS``. It is a **separate process from the same image**, like the streamer's
``worker.py`` -- ``python -m time_series_stub`` -- standing where a deployment's
own api over its time-series database would stand, so that the whole path
(client → REST → this service → Kafka → client → player → page) can be run,
tested and demonstrated with nothing outside this repo. Its "database" is the
streamer's committed sample recording, tiled ``REPEAT`` times back to back so
there is a minute of timeline to query rather than three seconds.

What is here, and what each piece is for:

* ``recording.py`` -- the tiled recording and the two questions a store answers:
  what do you hold (``coverage``), and give me this window (``select``).
* ``service.py`` -- ``QueryService``: turns an accepted ``TimeSeriesQuery`` into
  the run of ``TimeSeriesResult`` envelopes the contract prescribes, published
  to a ``Sink``; tracks and cancels running queries. Knows nothing of HTTP or
  Kafka, so the tests drive it with a list.
* ``kafka_sink.py`` -- the ``Sink`` over aiokafka: the results topic, created
  if missing, key = ``query_id``.
* ``app.py`` -- the three REST routes plus ``/healthz``, as a FastAPI app over
  a ``QueryService``. ``httpx.ASGITransport`` drives it in-process in tests.
* ``__main__.py`` -- configuration from ``TIME_SERIES_STUB_*`` and uvicorn.

The one import from another app package is ``pmu_test_streamer.sample_client``
for its parser and its file: a tool borrowing a fixture, not an app depending
on an app.
"""
