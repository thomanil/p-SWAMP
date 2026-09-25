# pswamp-core

The shared data-architecture building blocks of p-SWAMP, as a small Python
package with pydantic as its only dependency:

- `pswamp_core.messages` — every message that crosses a topic, a socket or a
  process boundary, as versioned pydantic models (`DataModel`, `PmuHeader`,
  `PmuFrame`, `ResultEnvelope`, `Command`, `PlayerStatus`, …).
- `pswamp_core.datagateway` — the provider contract (`DataClient` with declared
  capabilities), the `DataGateway` that stitches providers into one time-addressed
  stream, the `Player` that paces such a stream and takes the replay commands, the
  reference `InMemoryClient`, environment-driven configuration, and a conformance
  suite a provider author runs against their own client.
- `pswamp_core.bus` — the in-process publish/subscribe bus, typed on message
  classes, with the one thread→loop crossing point.
- `pswamp_core.modules` — the minimal "consume one topic, produce another" module.
- `pswamp_core.pipeline` — one stream's player, bus and modules bound together,
  and the registry that builds, caps and evicts them per key.

`doc/server-data-architecture.md` at the repo root explains how the pieces fit
and how data flows from a source to a browser. The lineage of the gateway half
is the `test_pswamp` draft by Louis Pauchet; see
`STEP4-WIP-data-integration-impl-for-single-module.md` for what was lifted
verbatim, what was adapted, and what is still deferred.

Beside the package, `examples/` holds runnable examples that are not library
code and are never installed with it. Today that is the remote data contract's
reference service and its black-box check; see `examples/README.md`.

The web backend in `app/server-python/` consumes this package as an editable
path dependency (`pswamp-core = { path = "../../core" }`). Its tests under
`core/tests/` run in that backend's environment via
`./scripts/run-python-server-tests.sh`.
