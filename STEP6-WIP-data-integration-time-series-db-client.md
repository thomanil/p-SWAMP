# STEP 6 — A remote time-series store as a provider (REST up, Kafka down)

> **Addendum (2026-09-23): renamed to the Remote Data Client.** The body of
> this note is the record of the step as built, so it keeps the names used at
> the time. They were changed afterwards because they put the emphasis in the
> wrong place. The point of the client is not a time-series database. It is a
> *decoupled* way to query and get data back from a remote data service, where
> the deployment decides what store sits behind that service. A time-series
> database is the typical case, but it is an implementation detail on the
> deployment's side. The explorer page keeps its time-series name and nav label
> on purpose, since that is what is queried on the other end in the example.
>
> | At the time of this note | Now |
> |---|---|
> | `TimeSeriesDatabaseClient`, `datagateway/clients/time_series_database.py` | `RemoteDataClient`, `datagateway/clients/remote_data.py` |
> | `TimeSeriesQuery`, `TimeSeriesResult`, `messages/time_series.py` | `RemoteDataQuery`, `RemoteDataResult`, `messages/remote_data.py` |
> | results topic `time.series.result` | `remote.data.result` |
> | extra `pswamp-core[timeseries]` | `pswamp-core[remote-data]` |
> | spec name `tsdb`, env block `TSDB_*` | `remote_data`, `REMOTE_DATA_*` |
> | stub `time_series_stub`, `TIME_SERIES_STUB_*` | `remote_data_stub`, `REMOTE_DATA_STUB_*` |
> | compose `time-series-stub`, k8s `p-swamp-time-series-stub` | `remote-data-stub`, `p-swamp-remote-data-stub` |
> | `doc/time-series-database-integration-contract.md` | `doc/remote-data-integration-contract.md` |
> | tests `test_time_series_database.py`, `test_time_series_messages.py` (core) | `test_remote_data_client.py`, `test_remote_data_messages.py` |
> | tests `test_time_series_database_client.py`, `test_time_series_stub.py` (server) | `test_remote_data_service.py`, `test_remote_data_stub.py` |
> | unchanged: `/time-series-explorer`, `time_series_explorer/`, `TIME_SERIES_EXPLORER_DATA_CLIENTS`, nav label "Timeseries Db Explorer" | — |
>
> The integration contract doc was also brought up to date with the
> self-describing frame, which landed after it was written: the service now
> serves `pmu.frame` only, and each frame carries its own `header`.

Working note behind the sixth step of the data-integration track. STEP 1–5
built the provider contract, the gateway, the player, the bus, modules, the
pipeline registry, and a module running as its own service. Every one of them
names "a TSO's time-series database is a `DataClient`" as the real deployment
case; none built one. This step does, and adds the two things the case turned
out to need first.

## Goal

A `TimeSeriesDatabaseClient` that (1) takes the contract's time-range calls,
(2) sends them as REST requests to a configured URL, behind which a deployment
runs an api over its time-series database, and (3) reads the answers off a
configured Kafka topic that service fills. Plus a dummy implementation of that
service rigged in compose and the example k8s manifest, and an example
module + page that drives the client two ways: a paced *play from a timeframe*
and a batch *count the rows in a timeframe*, as separate commands.

## Decisions (asked and settled 2026-09-16)

| Question | Decision |
|---|---|
| Reply channel | Kafka topic; records wrapped in a correlated `TimeSeriesResult` envelope (`query_id`, `seq`, `kind: record\|end\|error`), key = `query_id`, one partition. An NDJSON HTTP response would have needed none of the correlation; kept Kafka because results are messages end to end and the service pushes at its own pace. |
| Where the client lives | The core, behind a `pswamp-core[timeseries]` extra (aiokafka + httpx, imported lazily). The contract a TSO implements against ships with the core. |
| Stub data | The streamer's `sample_data.txt` tiled `REPEAT` times (default 20 → a minute), so play-range and count have a timeline to work over. |
| Bounded playback | Extend the Player: `replay(start, end)`; ends paused at `end`, never loops, `PlayerStatus.range_end`. |
| Error topic (added mid-plan) | `ErrorEvent` on each pipeline's bus; an edge hub keyed by client id fed by a forwarder module per pipeline; one layout-level socket and a tray on every page. |

Resolved without asking: results topic defaults to the model's own topic name
(`time.series.result`); the client creates the topic too (auto-create is off and
nothing orders the server after the stub); the stub is its own package; the
explorer's state carries no separate error field (`player.error`,
`count.result.error` are state; the event goes to the tray).

## What the code forced

- **The Player could not bound a replay** (`_switch_stream` hard-coded the
  history end) and **a provider exception in its reader killed the run task in
  silence** (`_run` caught only `CancelledError`; `stop()` would have re-raised
  it out of `Pipeline.stop`). A remote store that times out is the normal
  failure of this provider, so both were prerequisites: `player.py` now clamps
  an explicit `end`, ends a bounded range paused even when looping, and turns a
  provider failure into `paused/ended/error` plus an `ErrorEvent`.
- **`Module.run` cannot carry a `Command`**: it stamps the envelope with the
  input's timestamp, `None` for a command, outside its `try`. The row-count
  module overrides `run`. Making `Module.run` do this natively is listed as not
  here yet.
- **`Command.target` is matched by name**, so a module target is a fixed
  `ClassVar` (`"row-count"`), not the per-instance uuid the docstring used to
  mention.
- **The conformance suite never calls `open()`**, so the client's feed starts
  lazily on first `consume`.
- **CI's e2e job is a bare `docker run`**: the explorer's default provider stays
  the sample recording; compose and k8s override it.

## What exists

Core: `messages/time_series.py` (`TimeSeriesQuery`, `TimeSeriesResult`),
`messages/errors.py` (`ErrorEvent`), `datagateway/clients/time_series_database.py`
(`TimeSeriesDatabaseClient`, `KafkaResultFeed`, `InMemoryResultFeed`; the
contract in the class docstring), `transport/kafka.py:create_topic` (shared),
the Player and `Module.run` changes above. Tests: `test_time_series_messages.py`,
`test_time_series_database.py`, the bounded/failure cases in `test_player.py`,
the error case in `test_module.py`.

Web backend: `time_series_stub/` (recording, service, Kafka sink, FastAPI app,
`__main__`), `time_series_explorer/` (`RowCountModule`, the api with three
POSTs and a coalescing socket), `errors/` (hub, forwarder, notice, socket),
`shared.py` re-exporting the forwarder and hub, the streamer and frequency
peek appending a forwarder. Tests: `test_time_series_stub.py`,
`test_time_series_database_client.py` (conformance over the stub in-process;
broker-gated round trip), `test_time_series_explorer.py`, `test_errors.py`.
Tool: `tools/smoketest_time_series_explorer.py`, step 8 of `e2e-smoke-test.sh`.

Web client: `pages/time-series-explorer/`, `hooks/useErrorFeed.ts`,
`components/ErrorTray.tsx` rendered by `AppLayout`, `ERRORS_WS_PATH`.

Deploy: `time-series-stub` in compose (healthcheck, the server depends on it),
`p-swamp-time-series-stub` Deployment + Service in `k8s/p-swamp-local.yaml`, the
server's `TIME_SERIES_EXPLORER_DATA_CLIENTS` / `TSDB_URL` /
`TSDB_BOOTSTRAP_SERVERS` in both, the minikube script rolling out four pods.

## Open points

1. **Paging / backpressure.** The service publishes a whole range at its own
   pace and the per-query queue grows until the player has paced it out. Fine
   for a minute of 20 Hz PMU data; a real store wants a `limit`/cursor in the
   query and a bounded queue with flow control.
2. **One Kafka consumer per pipeline.** `MAX_PIPELINES` bounds it; a shared
   consumer per process demultiplexing to pipelines is the next shape.
3. **Coverage is asked often** (every seek, every segment boundary). The stub
   answers from memory; a real service should cache it.
4. **The reply topic is configured on both sides** (`TSDB_TOPIC`,
   `TIME_SERIES_STUB_TOPIC`) rather than carried in the query. Carrying it
   (`reply_topic`) would remove one thing that can drift.
5. **`Module.run` and a `Command`**: default the envelope timestamp to now and
   copy `request_id` when the input has one, so a batch module needs no `run`
   override.
6. **The grid monitor's own hub** (`pswamp_web/`) is not wired to the error
   topic; it lands when the monitor is re-pointed at the core.
7. **Auth on `TSDB_URL`**: none; a bearer token setting is the obvious first
   addition when a real store sits behind it.
