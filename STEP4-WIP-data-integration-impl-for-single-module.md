# STEP 4 — The sample slice: the target architecture applied to the PMU test streamer

> **Status:** WIP implementation report, fourth step of the data-integration track.
> Companion to STEP 1 (requirements A1–A8, assessment of `main`), STEP 2 (evaluation of
> the `test_pswamp` draft at `draft:c6ce9a3`) and STEP 3 (the target architecture).
> This document describes **code that now exists on this branch**: a new repo-root
> `core/` package holding the shared building blocks, and the `pmu_test_streamer`
> subapp rebuilt on them as a thin slice — deliberately without touching the grid
> monitor (`pswamp_web/`), the reference subapp, `server.py`'s wiring, `shared.py` or
> the desktop package at root `src/pswamp/`. The durable description of the
> architecture is `doc/server-data-architecture.md`; this document is the audit:
> what was lifted from Louis's draft verbatim, what was adapted and why, what is
> deferred, and how far the slice covers A1–A8. §8 is the boundary of that
> coverage: what the slice does *not* prove, and the decisions the next step needs —
> read §6 with §8.2 beside it.
>
> **Decisions taken while building** (confirmed with the author of the requirements
> before starting): `core/` at the repo root as its own uv project rather than under
> `src/pswamp/`; `PmuHeader` + `PmuFrame` as the wire shape (STEP 3 §4.1) rather than
> the draft's per-PMU objects; the player as an asyncio task rather than STEP 3's
> thread-with-batched-pulls, since nothing in the slice blocks on a thread; and one
> tiny coroutine module in the slice to prove the consume-one-topic-produce-another
> chain without formalising `SnapshotApp` yet.

## 0. The one-paragraph version

The streamer used to read `sample_data.txt` into a list at import, keep a per-client
line index, tick it at 100 lines/s and push its own message. It now owns nothing but
the edge. Between the sources and its socket sit only pieces of `pswamp_core`: the file
is a **`DataClient`** with declared capabilities and exact coverage, and beside it a
second, **live-only** `DataClient` re-stamps the same rows on the wall clock; a
**`DataGateway`** holds both and turns them into time-addressed streams where seek and
range query are the same call, routing each segment only to a client that declared it
can serve it; a **`Player`** either paces a bounded replay of the history (play / stop
/ step / seek / speed as **`Command` messages off the bus**, looping at the end) or
tails the live feed with no transport, and switches between the two on command; an
**`InProcessBus`** typed on message classes fans frames and status out; a
**`FrameStatsModule`** consumes frames off the bus and publishes a `ResultEnvelope`
back onto it; a **`Pipeline`** binds these per client id and a **`PipelineRegistry`**
caps and evicts them; and the endpoint subscribes to message classes and sends one
`PmuStreamState` per change. The browser edge contract is unchanged in shape (POST up,
socket down, ack never carries state; a command the current mode cannot apply is a 409
rather than an ack) and its types are generated from the core's own models. The page
shows a Recorded | Live switch and a red LIVE badge with the transport disabled while
live. 147 tests pass (109 in `core/tests/`, the draft's routing suite among them, 38
server-side), `error_check.sh` gates `core/` fully, and the providers can be swapped by
one environment variable without touching the repo — which the local k8s manifest
demonstrates by re-pointing the live feed at a data file that is not in the image.

## 1. What the slice is

### 1.1 Before and after

```
BEFORE  sample_data.txt ──read at import──▶ LINES[] ──index per client──▶ ticker (100/s) ──▶ PmuStreamState{window of 9 lines}
        POST play/stop/forward/back ──▶ mutate index / flag directly

AFTER   sample_data.txt ──SampleRecordingClient (HISTORY)──┐
                                                            ├─ DataGateway ──Player──▶ InProcessBus ──▶ ws endpoint ──▶ PmuStreamState
        same rows, now ──LiveSyntheticClient (LIVE only)───┘  consume(PmuFrame,…)  replay: pace·loop      ├──FrameStatsModule──▶ bus
                                                                                   live: as it arrives
        POST play/stop/forward/back/seek/speed/live/replay ──▶ Command on the bus ──▶ Player.apply()   (409 if the mode refuses it)
```

Per client: one `Pipeline` (gateway + bus + player + module), built by the registry on
first connect and keyed by the browser's client id — the unit-of-isolation decision
for a replay (STEP 3 §4.6).

### 1.2 Where the code is

```
core/                                        NEW — the third Python project (pswamp-core, import pswamp_core)
├── pyproject.toml, README.md
├── src/pswamp_core/
│   ├── messages/   data_model.py  pmu.py  results.py  control.py
│   ├── datagateway/ time_range.py config.py data_client_model.py planner.py stream.py data_gateway.py
│   │                player.py  conformance.py  clients/in_memory.py
│   ├── bus/__init__.py    modules.py    pipeline.py    log.py    util/time.py
└── tests/          support.py conftest.py + 8 suites (109 tests)

app/server-python/src/pmu_test_streamer/     REBUILT
├── sample_client.py   the history provider (replaces model.py); serves the header
├── live_client.py     the live-only provider: the recording's rows on the wall clock
├── stats_module.py    the module
├── api.py             the edge: registry, socket, eight POSTs, the 409 refusal
└── __init__.py        same three exports

app/client-web/src/pages/pmu-test-streamer/  REBUILT — usePmuStreamSocket.ts, FrameTable.tsx (replaces StreamWindow.tsx), PmuTestStreamerPage.tsx
app/server-python/tests/test_pmu_test_streamer.py   NEW — parse, conformance (both providers), module, pipeline, the recorded/live switch, the 409, provider swap, the k8s example file
doc/server-data-architecture.md              NEW — the durable description

k8s/deployment_pmu_data_file_example.txt     NEW — the deployment example's data file (see §4, "the deployment example")
k8s/p-swamp-local.yaml                       env block names both providers and points LIVE_PATH at that file, mounted from a ConfigMap
scripts/start-pswamp-in-local-minikube-cluster.sh   builds the ConfigMap from the file before applying the manifest
```

Plumbing touched so the third project builds, ships and is gated:
`app/server-python/pyproject.toml` + `uv.lock` (second editable path dependency;
`core/tests` in `testpaths`), `Dockerfile` (manifest copied before the resolve,
`--no-emit-package pswamp-core`, `core/` copied and installed editable),
`docker-compose.yml` (watch sync + `--reload-dir`), `.dockerignore` (`core/tests/`),
`scripts/error_check.sh` (a "Python (core)" section: `py_compile` + the pinned ruff),
`api_contract.py` (`API_VERSION` 1.0.0 → 2.0.0: `PmuStreamState` changed shape),
`doc/api/openapi.json` + `schema.ts` regenerated.

## 2. Lifted verbatim from `test_pswamp`

Concept, names and — bar the mechanical changes in the last column — code, file for
file. Every lifted module keeps Louis's SPDX header and says in its docstring what
was adapted.

| draft (`src/p_swamp/…`) | here (`core/src/pswamp_core/…`) | what it is | mechanical changes only |
|---|---|---|---|
| `core/datagateway/time_range.py` | `datagateway/time_range.py` | `TimeRange` (half-open, `None` unbounded), `Coverage(range, live)` | import path |
| `core/datagateway/data_client_model.py` | `datagateway/data_client_model.py` | `Capability` flag, `DataClient` ABC with its three documented `consume` rules, `supports`, `show_config`, `normalise_*` | import path; `MRIDType` narrowed to `str` (no `uuid` on the wire); + `from_env`, `can_consume` (§3) |
| `core/datagateway/planner.py` | `datagateway/planner.py` | `SegmentPlanner`, `Segment`, `DataGapError`, `GapPolicy`, the 5 s live hand-off margin | import path; loguru → `logging`; + the capability rules (§3) |
| `core/datagateway/stream.py` | `datagateway/stream.py` | `DataStream`: plan–consume–replan, watermark de-dup, `segments` trace, lag warning | `typing.Self` → string annotations (3.11); loguru → `logging`; + `request` property |
| `core/datagateway/data_gateway.py` | `datagateway/data_gateway.py` | `DataGateway`: `consume`, `produce` fan-out, lifecycle, disabled-when-empty | `Self`; `logger.success` → `info`; + `coverage()`, `supports()`, `ProduceError` (§3) |
| `core/datagateway/config.py` | `datagateway/config.py` | `EnvSetting`, `env_key/str/required/list/int/seconds/capabilities`, `format_settings`, `MissingSettingError` | + `kind`, `read_setting`, `env_float`, `gateway_from_env` (§3) |
| `core/datagateway/clients/in_memory.py` | `datagateway/clients/in_memory.py` | `InMemoryClient`: the reference shape and test double, `coverage_fn` | `datetime.UTC` → `timezone.utc`; live fan-out (§3) |
| `core/models/data_model.py` | `messages/data_model.py` | `DataModel`: required `version`, `mRID`, UTC-coerced `timestamp`, class-derived `topic` descriptor | `branch` dropped, splitter fixed (§3) |
| `utils/time.py` | `util/time.py` | `ensure_utc`, `utcnow` | `datetime.UTC` → `timezone.utc` |
| `tests/conftest.py` | `tests/support.py` + `conftest.py` | `Measurement`, `T0`, `at`, `measurement`, `collect` | helpers moved to a plain module so two conftests cannot shadow each other |
| `tests/test_data_gateway.py` | `tests/test_data_gateway.py` | all 11 routing cases incl. the flagship live-hand-off test, `TrackingClient`, `ReplayingClient` | + 8 cases for the additions: coverage union, produce failures, live fan-out, the five capability rules |
| `tests/test_client_config.py` | `tests/test_client_config.py` | the 9 config cases | Csv/Kafka clients replaced by `support.EnvTestClient`; + `gateway_from_env` cases |
| the argument against pickle (demo notebook) | `messages/__init__.py` docstring | principle 3 of STEP 3 | — |

The draft's `Python >= 3.12` is the only real friction: `datetime.UTC` (three files)
and `typing.Self` (two) have 3.11 spellings; `zip(strict=True)` and
`except TimeoutError` were already fine.

## 3. Adapted: concept Louis's, shape changed, and why

| what | change | why |
|---|---|---|
| `DataModel.branch` | dropped, together with its splice into the topic at index 1 | STEP 3 §4.1: its one use is co-tenancy on a shared broker, which TSO deployments do not have (dev/test/prod are separate); the stream axis the multi-TSO example needs becomes a configured namespace, never a per-message field |
| topic derivation | `PmuFrame` → `pmu.frame`; acronyms and digits kept together (`MeasurementPMUVoltage` → `measurement.pmu.voltage`, not `p.m.u`); `topic: ClassVar[str]` overrides | the draft's `re.findall(r"[A-Z][a-z]*")` split `PMU` into three letters and dropped digits |
| measurement model | the draft's per-PMU `MeasurementPmu{Voltage,Current,Frequency}` **not** lifted; `PmuHeader` + `PmuFrame` (one instant of every channel) instead, `timestamp` required | STEP 2 A1: no channel identity table, no `data_rate`, optional timestamp, three unaligned streams — while every p-SWAMP application consumes one labelled row per instant. Per-PMU objects remain a valid *ingest* shape; the broker-side choice waits on the measurement STEP 3 §4.1 asks for |
| `from_env` | hoisted from each client onto the `DataClient` ABC, driven by a new `EnvSetting.kind` (`str/int/float/seconds/list/capabilities/path`) | the draft wrote one `from_env` per client with hand-typed parsing; one generic version means a provider declares its settings and gets configuration for free |
| gateway composition | new `gateway_from_env(default)` reading `PSWAMP_DATA_CLIENTS="name:module:Class,…"` | the draft configured *a* client from the environment but composed the gateway in code; a deployment must be able to name its provider without a fork |
| `DataGateway.coverage(model, capability=…)` + `supports` | new: the union over the clients that can *consume* — optionally only those with one capability | the player needs the `HISTORY_CONSUME` union for `can_seek` and where a loop restarts, and `supports(LIVE_CONSUME)` for `can_go_live`; a produce-only client contributes no coverage |
| `SegmentPlanner` capability rules | history segments only to `HISTORY_CONSUME` clients, the live hand-off only to `LIVE_CONSUME` ones, a live-only client offered only from the hand-off margin on | the draft's `DataClient` docstring promised "the core never asks for what was not declared" and its planner filtered on model alone; now the promise is enforced, and a lying `coverage(live=True)` from a history-only client is ignored |
| `DataGateway.produce` | raises `ProduceError` naming the failed clients after the fan-out completes (the healthy ones still wrote) | STEP 2 §4: an archive that silently stops is a history gap discovered weeks later |
| `InMemoryClient` live delivery | one queue per tailing consumer; a payload published while nobody tails is held for the next | the draft's single `asyncio.Queue` would have *split* messages between two consumers (STEP 2 A2); the backlog keeps the draft's own tests valid |
| the tests as a conformance suite | `DataClientConformance`: a class of test methods over three fixtures (`client_under_test`, `conformance_model`, `conformance_records`), the cases gated by the client's declared capabilities | STEP 2 A8: the draft's suite was written against `InMemoryClient`, not parameterised; a TSO had nothing to run against their client. `InMemoryClient` (as history and as tail-only) and both of the streamer's providers pass the same suite |
| loguru | `logging`, through `pswamp_core.log.get_logger` (a copy of `pswamp_web/log.py`) | no extra dependency; the rest of the repo logs this way |

Concepts of the draft's that shaped the *new* layers: capability gating is what lets
routing enforce a provider's declaration and what gives the player `can_seek` (a
history source exists) and `can_go_live` (a live one does); "history lives with the
provider" is what lets a seek be a `consume` from a time; and "seek = a new stream",
implicit in the draft, is explicit in the player -- and is also how the recorded/live
switch works, since each mode is simply a different stream opened.

## 4. New in this slice (nothing in the draft to lift)

| piece | file | what it is |
|---|---|---|
| `PmuHeader`, `PmuFrame`, `header_id_of` | `messages/pmu.py` | STEP 3 §4.1's sketch, with `columns(...)` as the `Indexer` query and `stations` |
| `ResultEnvelope[T]`, `AppIdentity`, `AppStatus`, `AppStatusMessage` | `messages/results.py` | the result layer STEP 1 A1 called missing; a subclass's name is its topic |
| `Command`, `PlayerStatus`, `StreamChanged` | `messages/control.py` | the control layer; `Command.request_id` is A7's correlation id |
| `Player` | `datagateway/player.py` | pacing (monotonic clock, re-anchor instead of burst), pause/resume, `step(±n)`, `seek`, `set_speed`, loop, `go_live` / `replay`, mode = which stream is open, `can_seek` / `can_go_live`, commands consumed off the bus; the next-frame read is a task awaited outside the lock so a quiet live feed never blocks a switch |
| `InProcessBus`, `Subscription`, `Overflow`, `Latest`, `Bus` protocol | `bus/__init__.py` | adapted from `pswamp_web/bus.py`: typed on classes (subclass matching), async-iterator subscriptions, explicit overflow policy, `publish_threadsafe` kept as the one thread seam |
| `Module` | `modules.py` | the coroutine module base |
| `Pipeline`, `PipelineRegistry`, `CapacityError` | `pipeline.py` | `HubRegistry` from `pswamp_web/hub.py` generalised: factory-built, fastapi-free, async start/stop; its seven regression tests re-targeted and passing, plus two for the async factory and for the pipeline running a player and a module on one bus |
| `DataClientConformance` | `datagateway/conformance.py` | see §3 |
| `SampleRecordingClient`, `load_sample` | `pmu_test_streamer/sample_client.py` | the history provider, outside the core on purpose; the one that serves the header |
| `LiveSyntheticClient` | `pmu_test_streamer/live_client.py` | the live-only provider: a 20 Hz ticker between `open` and `close`, the recording's rows re-stamped now, per-consumer queues, no backlog, no header |
| `FrameStatsModule`, `FrameStatsResult` | `pmu_test_streamer/stats_module.py` | the module |
| the edge | `pmu_test_streamer/api.py` | `build_pipeline` over both providers, `REGISTRY`, `connected_pipeline` (a local copy of `connected_hub` over the core registry), the page's subscription opened before the first send, the coalescing push loop, eight POSTs each `bus.publish(Command)` after `refusal()` has answered 409 for a verb the mode cannot apply. The state carries the *player's* last frame — forgotten on a stream switch — and only the stats computed for that frame, never the bus's newest: otherwise a page shows the other stream's values under the wrong badge, which is invisible while both streams carry the same rows and glaring the moment they do not |
| the deployment example | `k8s/p-swamp-local.yaml`, `k8s/deployment_pmu_data_file_example.txt`, `scripts/start-pswamp-in-local-minikube-cluster.sh` | A8's "no repo change" made concrete: the manifest's env block names both providers in `PSWAMP_DATA_CLIENTS` and sets `LIVE_PATH` to a file that is **not in the image**, mounted read-only from a ConfigMap the start script builds from it. The file keeps the recording's layout (the live client serves no header) and every value counts up by one per frame from 100 kV / 0° / 60 Hz, so Live in that deployment visibly shows the configured source moving while Recorded still replays the image's own recording. A test pins the file to the code; the ConfigMap is configuration, not storage |

## 5. Left out or deferred

| what | why, and where it is designed |
|---|---|
| `CsvClient`, `KafkaClient` (`draft:clients/`) | no use in the slice; Kafka is "after numbers" (STEP 3 §4.7, principle 7). Both lift the same way the in-memory client did when wanted |
| `DataHub`, the CIM profile, GraphDB, the three converters | a grid-model provider, a track of its own (STEP 2 §4) |
| `branch` | dropped (§3) |
| the `events/` stub, `fastapi`/`uvicorn`/`proton-driver` deps, `loguru` | unused / replaced |
| the thread-hosted `Player` with batched pulls (STEP 3 §4.3 decision 2) | nothing in the slice blocks on a thread; the coroutine player is measurably simpler. Revisit when a `SnapshotApp` is behind the bus |
| `AppIO` / `GatewayIO` / the formalised `SnapshotApp`–`TimeWindowApp` contract, `MODULES` registry, `generate-new-module.sh` (STEP 3 §4.5) | the bridge to the desktop package's thread modules; out of scope by the "don't touch `src/pswamp/`" rule. The coroutine `Module` publishes the same `ResultEnvelope` on the same bus, so the bridge is additive |
| `request_id` back to the browser (`CommandAck.request_id`), batch jobs (STEP 3 §4.6) | needs the shared `CommandAck` in `pswamp_web/wire.py` to change (a second breaking change); the id exists on `Command`, is logged, and is carried by `ResultEnvelope.request_id` |
| "pause is view state in live mode" | live mode offers no pause at all: the page disables the transport row and the edge answers 409. Faking a pause as a frozen view is a later choice, if wanted |
| `PmuFrameAssembler` (per-PMU objects → frames) | only when a deployment ingests per PMU; needs the §4.1 measurement |
| `GatewayBus` (a broker as a bus), `runmodule`, compose/k8s profiles | out-of-process hosting is gated on a load generator and an end-to-end timestamp |
| the web re-point (`Hub`/`Bus`/`HubRegistry` → core; `stores.py`, per-page `adapt.py`, `recorded_io.py`/`replay.py`) | STEP 3 §4.8, step 7 of the landing order; explicitly not this slice. `connected_pipeline` in the streamer duplicates ~40 lines of `connected_hub` until then |
| entry-point discovery (`[project.entry-points."pswamp.data_clients"]`) | `PSWAMP_DATA_CLIENTS` takes a dotted path, which is enough; an entry-point group is a convenience on top |

## 6. Coverage of the requirements

| req. | what the slice proves | what it does not |
|---|---|---|
| **A1** shared domain model | every message on the chain is a versioned `DataModel`; `PmuHeader` + `PmuFrame` as the measurement shape with units, `data_rate`, a required timestamp and `null` for NaN; the result envelope; the browser's types generated from these classes | the canonical broker-side shape (frame vs per-PMU) is still unmeasured; `quality` has a place but no producer |
| **A2** topics, publish/subscribe | `InProcessBus`: topic = class, subscribe to a base class, three overflow policies, `add_listener`, `publish_threadsafe`; the topic catalogue is the set of `DataModel` subclasses | a broker as an adapter of the same interface (`GatewayBus`) |
| **A3** module in → module out | `FrameStatsModule`: consumes `PmuFrame`, publishes `FrameStatsResult`; the page subscribes to the result class, never to the module; adding one is a subclass and a list entry | the thread-module bridge; a module registry and scaffold |
| **A4** in-process or separate service | all in-process; the bus keeps the thread seam and nothing here would change for a per-stream key | proven only in-process; out-of-process needs numbers first |
| **A5** upstream data commands | play/stop/step(±1)/seek/speed/live/replay as `Command`s on the bus; seek = a bounded `gateway.consume(model, t, history_end)`; live = `consume(model, now, None)`; a chunk = a bounded `consume` (exercised by the conformance suite and `step(-1)`); mode is which stream is open, so a gateway of an archive plus a live feed replays paced and seekable and switches on command, and the client renders no dead control (the transport row is disabled while live; the edge answers 409) | chunk queries by a server-side batch module; jobs |
| **A6** module contract | `Module` with `input_model`/`output_model`/`process`; enforced by `ABC` | `SnapshotApp`/`TimeWindowApp` formalised; `check_modules`; scaffold |
| **A7** multiple clients | per-client pipeline via `PipelineRegistry` (cap, idle eviction, LRU, refusal — seven regression tests); `client_id` on every `Command`; `request_id` generated, logged and carried by `ResultEnvelope` | `request_id` returned to the browser; results routed by request on a *shared* bus |
| **A8** provider contract, example, no infra | `DataClient` with capabilities, **enforced by routing**; two providers written outside the core importing only the contract — `SampleRecordingClient` (history) and `LiveSyntheticClient` (live only); `DataClientConformance` gated by capability and passed by all of them; `from_env` + `show_config`; `PSWAMP_DATA_CLIENTS` swaps the providers with no repo change (tested), and the local k8s manifest is the worked deployment example — both providers named, the live feed re-pointed by `LIVE_PATH` at a ConfigMap-mounted file outside the image (§4) | entry-point discovery; a broker-backed example; a live provider that describes its own layout; a deployment-provided provider *package* (the example re-points a shipped one) |

STEP 1 §5's tensions, as the slice answers them: (1) unit of isolation — per client
for a replay, and the class is indifferent to the key; (2) replay controls vs live —
a property of which stream is open, switched on command, never of the gateway; (3) stateless vs history — the recording is the
provider and the repo persists nothing; (4) pickle — gone from this chain; (5)
request/response — `request_id` exists on both ends of the bus, not yet at the edge;
(6) where shared Python lives — `core/` is a third place, chosen so it is a `git mv`
in either direction.

## 7. What was verified, and how

- `./scripts/error_check.sh` — green: tsc, eslint, `uv lock --check`, `py_compile`
  over `app/` and `core/`, ruff (`--select F`) over both, api contract in sync.
- `./scripts/run-python-server-tests.sh` — 147 passed, 23 skipped (the conformance
  suite's capability-gated cases: history cases on the tail-only clients, live cases
  on the history-only ones, and the identifier-filter case, which needs two `mRID`s):
  the draft's routing suite plus the capability rules, the registry regressions, the
  player over a single and over a mixed history+live gateway, the bus, the module,
  the pipeline, the sample parse, both providers' conformance, the live ticker, the
  recorded → live → recorded switch end to end (including that no frame or stats
  from the other stream survive a switch), the 409 refusal, the provider swap, and
  the k8s example file read through `LIVE_PATH`.
- Runtime, against the compose image (which builds `core/` in and hot-reloads it
  through `docker compose watch`):
  - **In Chrome**, the built page at `/pmu-test-streamer`, no console errors
    throughout. *Recorded:* opens paused at frame 1 of 60 with every control
    enabled; Play runs at real time (frame 39 at t = 1.90 s after ~1.9 s) and loops
    at frame 60; Stop holds the cursor; Step forward and back move exactly one
    frame; the seek slider set to 1.0 s lands on frame 21; the 2× speed select
    advances 0.8 s of recording per 0.4 s of wall clock. *Live:* the switch turns
    the badge into a red pulsing LIVE, disables the slider and all five transport
    controls, the table updates at ~20 Hz with a wall-clock readout advancing one
    second per second, and the stats line keeps updating on the live frames. A
    `POST …/playback/seek` while live answers 409 with the reason. *Recorded
    again:* back at frame 1 paused, controls enabled, seek works.
  - `./scripts/e2e-smoke-test.sh` against the same running image: green (the
    reference subapp and the HTTP surface are untouched).
  - The mixed history+live path and the switch are covered end to end by the
    server tests as well (`test_default_pipeline_replays_then_goes_live_then_returns`,
    `test_dispatch_answers_409_for_a_verb_the_mode_refuses`), so the click-through
    above confirms the page, not the pipeline.
- Runtime, **deployed in minikube** through `start-pswamp-in-local-minikube-cluster.sh`
  (image built into minikube, the ConfigMap created from the example file, the
  manifest applied, rollout complete):
  - Inside the pod: `PSWAMP_DATA_CLIENTS` and `LIVE_PATH` set as the manifest says,
    the mounted file present with its 300 records.
  - Over a port-forward, on the socket: `live` delivers frames whose values climb
    100 / 0 / 60, 101 / 1 / 61, … one step per frame at 20 Hz — the ConfigMap file,
    not the image's recording; `replay` then `play` delivers the recording's own
    values (419.95 kV, 49.9999 Hz). The same two-source check, driven in Chrome
    against the pod: Play at real time and looping; Live with the climbing values
    and the transport disabled; Recorded again showing an empty table at frame 1
    until a frame plays, then the recording's values on step and play; no console
    errors. That empty table is the point: before the state carried the player's
    own last frame, Recorded showed the live feed's values — invisible under
    compose, where both streams carry the same rows, and the reason the deployment
    example carries different ones.

## 8. Open points

§6 says what the slice covers. This section is the boundary of that: what the code
does not yet do, what the §6 table cannot be read as proving, the decisions the next
step needs before more code is written, and the order to take them in. Nothing here
is fixed on this branch.

### 8.1 Known gaps in the code

1. **Replay completeness is promised (STEP 3 §4.3: "in replay mode the player
   stalls") and not implemented.** *High for the contract, invisible in the slice.*
   `Module.overflow` defaults to `DROP_OLDEST` with `maxsize=64`; `Bus.publish` is
   synchronous and has no way to wait for a consumer. `Overflow.GROW` exists and a
   module can opt into it, but an unbounded queue is not backpressure — it moves the
   failure from a silent drop to memory. At 20 Hz with a sub-millisecond module the
   slice never fills 64 slots, which is why every check in §7 passed; at `speed=10`,
   or with `paced=False` (the player's own docstring offers it for "batch runs"), or
   with a module that costs more than a frame interval, frames drop with one warning
   per 50 and the replay still "ends normally". The two policies STEP 3 named need two
   *delivery paths*: an awaitable publish (or a per-subscription credit) for replay,
   the current fire-and-forget for live. This has to be settled **before** a real
   analysis module is put behind the bus, since the decision changes the `Bus`
   protocol. The live source sharpens the contrast rather than resolving it: for the
   live path `DROP_OLDEST` is the *right* policy, so the two paths really are two.

2. **The ack means "dispatched", but says `applied`, and `request_id` never reaches
   the caller.** *Medium.* A POST publishes a `Command` and returns
   `CommandAck(applied=verb)`; the player applies it on its next turn. The edge
   refuses with a 409, before publishing, any verb the player's current mode cannot
   apply (a transport verb while live, `live` without a live source, `replay`
   without history), so every refusal a page can provoke is honest. Three things
   are not: ADR-003's wording ("understood and applied") is stronger than a
   bus-routed command can promise; a refusal only the player can see (the race
   between the edge's check and the apply, or a bad argument) is logged and
   dropped; and `Command.request_id` — generated, logged, carried on
   `ResultEnvelope` — never reaches the caller who could correlate it. The
   decision is question 2 in §8.3; either answer is cheap.

3. **The `Bus` protocol is not yet transport-neutral.** *Medium; a design gap, not a
   bug.* `publish()` is synchronous and returns nothing; `subscribe()` returns the
   concrete queue-backed `Subscription`; `DataGateway.produce()` is `async` and raises
   `ProduceError`. A `GatewayBus` (STEP 3 §4.2) cannot present the current face without
   hiding a task, swallowing publication errors and inventing consumer identity,
   ordering and reconnect semantics on its own. §5 lists `GatewayBus` as deferred;
   the stronger statement is that **the protocol is not frozen until one broker
   adapter has been written against it** — and point 1 changes the same protocol from
   the other side. Do both in one pass.

4. **A provider that fails to open yields a running, inert pipeline; a failing stop
   leaks the rest.** *Medium.* `DataGateway._lifecycle` gathers `open()` with
   `return_exceptions=True` and only logs; `Pipeline.start` then starts modules and the
   player, the registry counts the pipeline as live and the socket is accepted.
   `Pipeline.stop` is sequential with no `finally`: an exception from `player.stop()`
   skips module cancellation, `Latest.detach`, `gateway.close` and `bus.bind(None)`.
   Neither provider in the slice shows it — the sample is lazy enough that a missing
   file fails later and loudly, and the synthetic feed cannot fail to open — but an
   external provider with a wrong broker address would. Startup failures should
   aggregate and raise (the registry's `_build` already handles a raising `start()`);
   shutdown should attempt every step and raise the collected failures afterwards.

5. **A header change mid-stream is documented and unsupported end to end, and the
   live provider sidesteps it by serving no header.** *Medium.* `PmuHeader` says a
   changed layout is resent. `FrameStatsModule.setup` reads the header once and
   `process` returns `None` for any other `header_id` — the right default for a
   layout the module was not re-primed for, and `StreamChanged` is on the bus for a
   consumer that wants to re-read, but the module does not listen to it. The socket
   subscribes to `PmuFrame, PlayerStatus, FrameStatsResult, StreamChanged` and
   **not** `PmuHeader`, and `state_message` sends the header on the first message
   only — so a client would render replacement frames against the old layout with no
   way to notice. The live provider makes the gap concrete from the other side: a
   client that can only tail is never asked for the past, and a header is a
   point-in-time record, so `LiveSyntheticClient` deliberately serves none and
   relies on the recording's header describing its frames (same `header_id`). A
   deployment naming only a live client therefore gets frames and no layout. Headers
   need to be events on the bus that re-prime modules and reach the client *before*
   the first frame that follows them; that is also what lets a live source describe
   itself.

6. **Frame width is never checked against the header.** *Low.* `PmuHeader` validates
   that its four rows align; nothing validates that `PmuFrame.values` has
   `header.n_columns` entries. A short frame raises `IndexError` inside `process`,
   which `Module.run` catches, logs and marks `UNDEFINED`; a long frame is silently
   misindexed. A cross-message invariant, so it belongs at the provider boundary —
   the conformance suite and `DataStream` — not in a pydantic validator that has only
   one message in hand.

7. **The live feed is per client, which is right for a synthetic source and wrong
   for a real one.** *Medium; the STEP 3 §4.6 design, still not built.* Each
   pipeline opens its own `LiveSyntheticClient` and its own ticker, so two browsers
   in live mode see two unrelated instants. A control room's live stream is one
   pipeline per *stream*, shared by every viewer, with only the replay cursors per
   client — and that is where per-client commands, view state and result routing
   have to be separated (§8.3, question 4). The slice proves the per-client half;
   the shared half is a design.

8. **`connected_pipeline` duplicates `connected_hub`.** Forty lines, kept local by
   the "don't touch `pswamp_web/`" rule. The re-point (STEP 3 §4.8, step 7) unifies
   them; until then this is the only duplication the slice introduced.

9. **The registry's start is on the loop.** `HubRegistry` ran `Hub.start` in a
   thread because it built three applications and prefilled windows (~40–50 ms of
   blocking work). `PipelineRegistry` awaits `pipeline.start()`, which is cheap here;
   when a blocking module joins, its `setup` is the place to `to_thread`.

10. **`core/tests` runs in the server's environment on purpose.** Core has no
    lockfile; one runner and one CI job cover both suites. If core ever needs a
    dependency the server does not have, it gets its own dev group and runner then.

### 8.2 What §6 can and cannot be read as proving

The decomposition holds — Louis's gateway as the data-access layer rather than the
architecture; `PmuHeader` + `PmuFrame` as the shape the analysis code actually
consumes; a provider contract that two providers outside the core really implement,
one of them live-only; a clean dependency direction — and the slice is the right
first slice. What needs narrowing is the **scope of the evidence**. Read §6 with
these in front of it:

| claimed or implied | actually shown | not shown, and where in §8.1 |
|---|---|---|
| A5 mode and the recorded/live switch | over a gateway of one history and one live client, per client; switch on command; a live feed that goes quiet does not block the switch | an automatic archive→live hand-off inside one open stream (the planner's story) is not what the player does: a replay is bounded on purpose; a *shared* live pipeline (7) |
| A2 "three overflow policies" | the policies exist per subscription | that a replay is lossless under *any* of them (1) |
| A7 "`request_id` … carried by `ResultEnvelope`" | on the bus; a refusal the mode decides is a 409 at the edge | `request_id` at the edge; refusals only the player can see (2) |
| A8 "the core never asks for what was not declared" | enforced in the planner and `coverage`, tested | a provider that lies about coverage in subtler ways than `live` |
| A2/A4 "a broker as an adapter of the same interface" | nothing | the interface itself is not yet adapter-shaped (3) |
| A7 lifecycle "cap, idle eviction, LRU, refusal" | all four | a failed provider start, a failing stop (4) |
| A1 "`PmuHeader` … resent when the layout changes" | the message shape | a consumer or the socket surviving a resend; a live source describing its own layout (5, 6) |
| A6 module contract | the coroutine `Module`, on recorded and on live frames | any bridge to `SnapshotApp` / `TimeWindowApp`, or any thread-hosted module |
| A4 in-process or separate | in-process only (§6 already says so) | out-of-process anything |
| STEP 3 §4.6 shared live pipeline + per-client commands | per-client pipelines only, each with its own live ticker | a shared pipeline with per-client view state and result routing (7) |

So: **A1–A3, A5 and A8 have implementation evidence, on the recorded and on the live
side. A4, the full A6 contract, and the shared-live / out-of-process parts of A7 and
A8 are still designs.** That is the honest line.

### 8.3 Decisions the next step needs, not code

Each of these is a choice the author of the requirements has to make; the code
follows from the answer, not the other way round.

1. **Which analyses need every replay frame**, and which may sample? That fixes the
   bus contract and the memory bound (8.1 #1).
2. **What does a 2xx on a command mean** — accepted for dispatch, or applied? If the
   former, the ack carries `request_id` and an outcome message exists; if the latter,
   every refusal is a 409 and the player validates synchronously (8.1 #2). Mode
   refusals are 409s either way; this decides the rest.
3. **Which delivery guarantees survive a module moving to a broker** — ordering,
   at-least-once, consumer identity, what a failed publish does (8.1 #3).
4. **Is a live pipeline shared per physical stream** with replay cursors per client?
   If so, where do per-client commands, view state and results split off (8.1 #7)?
5. **Where is the authoritative header registry**, and how are layout changes ordered
   against frames — and is a live source expected to describe its own layout (8.1 #5)?
6. **What happens when one provider in a composed gateway cannot start** — fail the
   pipeline, or degrade to the others (8.1 #4)?
7. **Which of `SnapshotApp` / `TimeWindowApp` is the contributor-facing module
   contract**, and which goes behind an adapter?
8. **Should a replay ever hand off to live on its own?** The planner can; the player
   deliberately does not (a bounded replay that loops). With a recording months
   older than the live feed the hand-off would be a gap-skip, so the slice chose the
   explicit switch. A deployment whose archive abuts its live feed may want the
   automatic continuation — which is one flag on the player, once someone asks.

### 8.4 Order of work

Arranged so the two changes that touch the `Bus` protocol (8.1 #1 and #3) happen
together and before any real module lands on it:

1. Replay backpressure *and* a first broker-backed `Bus` adapter, as one change to
   the protocol — 8.1 #1 and #3, decisions 1 and 3.
2. Dispatch vs applied: the ack, `request_id` at the edge, an outcome message —
   8.1 #2, decision 2. This is the second breaking change to `CommandAck` that §5
   deferred; do it once.
3. Bridge one real `SnapshotApp` or `TimeWindowApp` through the core — the missing
   A6 evidence, decision 7.
4. The shared live pipeline: one per stream, per-client cursors and view state,
   result routing by `request_id` — 8.1 #7, decision 4. The synthetic feed is enough
   to build it against.
5. Failure tests: a provider that cannot open, a stop that raises — 8.1 #4,
   decision 6.
6. Headers as bus events, a live source that describes its layout, and frame-width
   checks at the provider boundary — 8.1 #5 and #6, decision 5.
7. Rewrite §6 of this document and the coverage table in STEP 3 against what is then
   actually demonstrated.
