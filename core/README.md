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
  capabilities), the `DataGateway` that stitches providers into one time-addressed
  stream, the `Player` that paces such a stream and takes the replay commands, the
  enrichment hook (`enrich`: the stub CIM reference on each frame), the reference
  `InMemoryClient` and the `RemoteDataClient` over a deployment's remote data
  service, environment-driven configuration, and a conformance suite a provider
  author runs against their own client.
- `pswamp_core.bus` — the in-process publish/subscribe bus, typed on message
  classes, with the one thread→loop crossing point.
- `pswamp_core.command_routing` — typed commands routed by their class to the one
  receiver (the player or a module) that declared them, checked, then applied.
- `pswamp_core.modules` — the minimal module: consume one message class and publish
  a result, and answer the commands it declares.
- `pswamp_core.pipeline` — one stream's player, bus and modules bound together,
  and the registry that builds, caps and evicts them per key.
- `pswamp_core.transport` — carries topics between processes (`InMemoryTransport`,
  and `KafkaTransport` behind the `[kafka]` extra).
- `pswamp_core.remote` — a module as its own service: `RemoteModule` in the
  pipeline, `ModuleHost` in the worker.

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
