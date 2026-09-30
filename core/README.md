# pswamp-core

The shared data-architecture building blocks of p-SWAMP, as a small Python
package whose only default dependency is pydantic; two optional extras add
`[kafka]` (aiokafka, for the Kafka transport) and `[remote-data]` (httpx, for
`RemoteDataClient`):

- `pswamp_core.messages` — every message that crosses a topic, a socket or a
  process boundary, as versioned pydantic models (`DataModel`, `PmuHeader`,
  `PmuFrame`, `ResultEnvelope`, `PlayerStatus`, `ErrorEvent`, …), and the typed
  commands (`Command`, `PlayerCommand` and its subclasses).
- `pswamp_core.datagateway` — the provider contract (`DataClient` with declared
  capabilities), the `DataGateway` that holds them as named sources and reads the
  active one as a time-addressed stream, the `Player` that paces such a stream and takes the replay commands, the
  enrichment hook (`enrich`: the stub CIM reference on each frame), the reference
  `InMemoryClient` and the `RemoteDataClient` over a deployment's remote data
  service, environment-driven configuration, and a conformance suite a provider
  author runs against their own client.
- `pswamp_core.subscription` — a consumer's queue and its overflow policy, and
  the `Sink` a player or a module publishes into.
- `pswamp_core.keep_up` — falling behind, noticed and reported as an `ErrorEvent`.
- `pswamp_core.transport` — the one publish/subscribe: one topic per message class
  per app, the pipeline key on every record; `InMemoryTransport` (one process) and
  `KafkaTransport` behind the `[kafka]` extra, and the `Outbox` in front of them.
- `pswamp_core.command_routing` — a command's class is its address; the inbox that
  checks and applies a receiver's commands in order.
- `pswamp_core.modules` — the minimal module: consume one message class and publish
  a result, and answer the commands it declares.
- `pswamp_core.pipeline` — `PipelineFamily` (what an app's pipelines are made of),
  `Pipeline` (one stream's gateway, player and topics, per key) and the registry
  that builds, caps and evicts them.
- `pswamp_core.host` — `ModuleHost`: one module instance per pipeline key, off the
  transport, in the server's process or a worker's.
- `pswamp_core.worker` — `python -m pswamp_core.worker`: a process that hosts the
  families it is told to.

`doc/server-data-architecture.md` at the repo root explains how the pieces fit
and how data flows from a source to a browser. The lineage of the gateway half
is the `test_pswamp` draft by Louis Pauchet; see
`STEP4-WIP-data-integration-impl-for-single-module.md` for what was lifted
verbatim, what was adapted, and what is still deferred.

Beside the package, `examples/` holds runnable examples that are not library
code and are never installed with it. Today that is the remote data contract's
reference service and its black-box check; see `examples/README.md`.

The web backend in `app/server-python/` consumes this package as an editable
path dependency (`pswamp-core = { path = "../../core", editable = true }`). Its tests under
`core/tests/` run in that backend's environment via
`./scripts/run-python-server-tests.sh`.
