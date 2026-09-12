# STEP 2 — Evaluating the initial data-gateway draft (`test_pswamp`)

> **Status:** WIP, second step of the data-integration track. Evaluates the sibling
> repo `../test_pswamp` (25 commits, 2026-08-31 → 2026-09-07, tip `c10d9bb`, authored
> by Louis Pauchet, SINTEF) against the requirements A1–A8 and the added assumptions
> in `STEP1-WIP-data-integration-context.md`. Everything in that document is taken as
> given here — the requirement discussion, the "core first, web second" order, the
> keep/replace/learn-from sort of the web PoC — and is cited rather than repeated.
>
> **How it was evaluated.** Every source file in the draft was read (≈4,000 lines
> including tests and lockfile-free manifests), its test suite was run, its imports
> were exercised, and one microbenchmark was taken to put a number on its message
> shape at PMU rates. No code was changed in either repo. File references are
> relative to `../test_pswamp/` unless they start with `src/pswamp/` or `app/`, which
> are this repo.
>
> **What this is not.** Not a code review of the draft's style, and not a target
> architecture. It answers one question: *how much of what STEP1 says we need does
> this draft give us, and what do we do with it?*

## 0. The one-paragraph version

The draft is a well-built answer to the part of STEP1 that was flagged as *genuinely
new* — a capability-declared provider contract with history navigation — and it is
good enough to lift. Its core is a **data gateway**: a caller asks for one model class
over one time range and gets back a single async stream stitched across whichever
backends hold that window (an archive first, then a broker, then live), where each
backend is a `DataClient` that declares what it can do (`LIVE_CONSUME`,
`HISTORY_CONSUME`, `PRODUCE`) and reports what it holds (`Coverage`). Messages are
pydantic models that carry their own schema version, UTC timestamp, identifier and
**topic name**, so the topic catalogue is the model tree rather than a config table.
Clients are configured from namespaced environment variables and self-describe them.
Thirty-three hermetic tests pass in 40 ms with no broker. That covers most of A8, the
query half of A5, the wire-format assumption, and the envelope half of A1. It does not
attempt the other half of STEP1 at all: there is no module/app-template contract, no
in-process fan-out of results to several consumers, no command channel, no client or
session identity, no replay pacing, no measurement header, and it is async-only
against an analysis core that is thread-based and blocking. Beside the gateway sits a
CIM/GraphDB "DataHub" that belongs to a different track and should be parked. The
recommendation is to **adopt the gateway core as the provider contract of STEP1 steps
1–3, move it into `src/pswamp/` under the repo's own conventions, and build the
missing module, bus and command pieces around it** — treating its shape as the
starting vocabulary rather than importing it as a sealed package.

## 1. What the draft is

### 1.1 Inventory

| Module (under `src/p_swamp/`) | Lines | What it is | State |
|---|---|---|---|
| `core/datagateway/data_client_model.py` | 214 | `DataClient` ABC, `Capability` flags, `supports()`, `coverage()` / `consume()` / `produce()` / `open()` / `close()`; `MRIDFilter`, `ModelSelector` | Complete, documented |
| `core/datagateway/time_range.py` | 151 | `TimeRange` half-open `[start, end)` with `None` = unbounded; `Coverage(range, live)` | Complete |
| `core/datagateway/planner.py` | 274 | `SegmentPlanner.next_segment()`: picks the client for the cursor by priority, cuts at the next higher-priority client, skips or raises on gaps, hands over to a live client within `live_handoff_margin` (5 s) of now | Complete |
| `core/datagateway/stream.py` | 217 | `DataStream`: plan → drain one client → re-plan loop; watermark de-dup on `(timestamp, mRID)`; drops out-of-order and untimestamped payloads; lag warning when replay never catches live | Complete |
| `core/datagateway/data_gateway.py` | 200 | `DataGateway`: registry of named clients, `consume(model, start, end, mRID) -> DataStream`, `produce(payload)` fan-out to every `PRODUCE` client, async lifecycle | Complete |
| `core/datagateway/config.py` | 235 | `EnvSetting`, `env_key("archive","DIRECTORY") → ARCHIVE_DIRECTORY`, typed readers, `format_settings` for `show_config()` | Complete |
| `core/datagateway/clients/in_memory.py` | 189 | List-backed client with an `asyncio.Queue` live tail and an injectable `coverage_fn`; the reference shape and the test double | Complete |
| `core/datagateway/clients/csv_file.py` | 344 | One CSV per concrete model in a directory; batched reads on a worker thread; mtime-cached bounds scan for coverage | Complete |
| `core/datagateway/clients/kafka_bus.py` | 414 | `aiokafka` client, one topic per model, JSON payloads, `offsets_for_times` seek, drained-to-high-watermark for bounded ranges, coverage *assumed* as `[now − retention, now]` | Complete; lazy optional dependency |
| `core/models/data_model.py` | 147 | `DataModel(version, branch, mRID, timestamp)` pydantic base; `topic` descriptor derives `measurement.live.pmu.voltage` from the class name and a branch | Complete |
| `core/models/measurements/*.py` | 130 | `Measurement` (extra fields must be `mRID_<channel>`), `MeasurementPmuVoltage` / `Current` (complex extras) / `Frequency` | Sketch; **broken import on Linux** (§1.3) |
| `core/models/events/` | 23 | `EventPayloadBase(type)` plus a `pkgutil` auto-discovery of subclasses | Empty: discovers nothing |
| `core/datahub/datahub.py`, `core/protocole/cim_profile.py`, `converter/cim/*`, `cim/`, `utils/sparql2df.py` | ≈690 | A `DataHub` bundling a `cim-graph` GraphDB connection, a 400-line generated CIM profile `Protocol`, and SPARQL→DataFrame "converters" for TOPS and Matpower | Different track (§6.9); Matpower converter is stubs and references a non-existent attribute |
| `tests/` (4 files) | 582 | Gateway routing, CSV and Kafka client behaviour, env config | 33 passed, 0.04 s, no broker needed |
| `examples/data_gateway_demo2.ipynb` | — | A tutorial notebook: models → JSON contract → CSV gateway → Kafka gateway → DataHub | Needs two dockerised Kafkas and a GraphDB for the later cells |

Manifest: `pyproject.toml` declares `requires-python = ">=3.13"` and depends on
`pydantic`, `cim-graph`, `loguru`, `fastapi`, `uvicorn`, `proton-driver`; `aiokafka`
is an optional extra. The build backend is `uv_build`; the package is importable as
`p_swamp` (this repo's is `pswamp`). Nothing in it imports `numpy`, `synchrophasor`
or anything from this repo: it is a from-scratch sketch, not a refactor.

### 1.2 The one idea in it

Everything hangs off one design move: **the caller names a model class and a time
range, never a source.** Which backend answers, and when the stream switches from an
archive to a live topic, is the planner's business (`planner.py:96-152`). A client is
only three questions — what do you hold (`coverage`, recomputed on every call because
windows are now-relative, `data_client_model.py:160-178`), stream me that window
(`consume`, with three rules: timestamped, non-decreasing, stops at `end` unless live,
`:190-196`), and store this (`produce`). Priority resolves overlaps; a cold store
usually outranks the bus for history, the bus wins live. The consequence that matters
for us is that "history lives with the provider" (STEP1 §A5) is not a policy statement
in this design, it is the mechanism: the gateway *is* the thing that asks the archive
for `[t0, t1)` and the broker for `[t1, now)`.

### 1.3 Verified defects

Small and fixable; listed so nobody rediscovers them.

- **The PMU models cannot be imported on Linux.** `measurements/__init__.py:2`
  imports `.measurement_pmu`; the file is `measurement_PMU.py`. Passes on a
  case-insensitive filesystem (the notebook's own comment about Windows spawn
  semantics, cell 39, suggests where it was written). Every PMU cell of the demo
  notebook fails here. Rename the file.
- **`CIM2MatpowerConvertor` reads `hub.cim_network`** (`converter/cim/matpower.py:16`),
  which `DataHub` never defines (`datahub.py:16-22`), and `DataHub.__getattr__`
  raises for unknown names — so the converter cannot be constructed. Its methods are
  all `not_implemented()` anyway.
- **`events/` auto-discovery yields an empty list** (`ALL_CLASSES == []`): there is no
  event module beside `base.py`. Harmless, but it is machinery with nothing to find.
- `p_swamp:main` is the `uv init` hello-world; `README.md` is empty; `.idea/` is
  committed.

None of these touch the gateway core, which is the part worth lifting.

## 2. The draft's concepts, mapped onto what we have

| Draft concept | Desktop today (STEP1 §2.1, §2.3) | Web PoC today (STEP1 §2.2) | STEP1 requirement |
|---|---|---|---|
| `DataClient` ABC + `Capability` flags | the nine-method io duck type, implemented seven times, `pass` where a capability is missing | `RecordedIO` (same duck type) | A8 item 1, A2 item 1 |
| `coverage()` → `Coverage(range, live)` | nothing; retention is an implicit 120 s | `Recording.duration`, never asked | A5 "history with the provider", A8 capability model |
| `consume(model, TimeRange)` → `DataStream` | `get_next_data_frame()` blocking pull + one-shot `t_start` rewind | `RecordingPlayer` cursor + `RecordedIO` queue | A5 range query, A5 seek |
| `produce(payload)` fan-out to `PRODUCE` clients | `handle_result` / `handle_output` → one `output_topic` | `publish(topic, payload)` → per-pipeline `Bus` | A2 publishers, A3 module output |
| `InMemoryClient.publish` + live tail | NQKafka (`multiprocessing.Manager` lists) | `Bus.publish_threadsafe` | A2 in-process default |
| `KafkaClient` (`aiokafka`, JSON, seek by time) | `KafkaIO` (`kafka-python`, JSON-or-pickle, seek by relative offset) | — | A4 out-of-process, A8 second example |
| `CsvClient` | `OfflineTestingAdapter` (CSV playback) | — | A8 example provider |
| `DataModel(version, branch, mRID, timestamp)` | the `{time_stamp, info{app_name, uuid}, parameters, result}` dict convention | pydantic models at the browser edge only (`wire.py`) | A1 result envelope, "wire format is language-neutral and versioned" |
| `Model.topic` derived from the class name | `[topics]` logical→physical table in `config.toml` | four string constants in `hub.py` | A2 "topic catalogue as data" |
| `from_env` + `show_config` + `EnvSetting` | `config.toml` `[streaming]` block | `HOST` and `PORT` | A8 item 3 provider selection by configuration |
| `Measurement` with `mRID_<channel>` extras | `synchrophasor.DataFrame` (pickled) / `TimeWindowLabeled` + 3-row header | `Recording` + `(meta, t, row)` + `LabeledRowDecoder` | A1 measurement schema |
| Priority + planner + gap policy | — | — | (not asked for; useful) |
| `branch` in every topic name | multi-TSO `no.*`/`se.*` prefixing via config | — | (not asked for; see §6.8) |
| `DataHub` + CIM converters | `models/` + `grid_model.py` + the N44 sqlite | `pswamp_web/grid_model.py` | out of scope for this track |

The mapping shows what the draft is: the *provider* row of STEP1's plan done well, and
nothing from the *module*, *bus-as-fan-out*, *command* or *client* rows.

## 3. Coverage against the requirements

Verdicts use STEP1's scale. "Covered" means the draft gives us the abstraction and
we would adapt rather than design; "partial" means the shape is right but a piece is
missing; "missing" means the draft does not attempt it.

### A1 — Shared domain model: **partial** (envelope yes, measurements no)

*Good.* `DataModel` is a sound envelope: schema `version` pinned by `Literal` per
subclass so a `v2` payload fails validation loudly (the notebook demonstrates this,
cell 12); `timestamp` coerced to UTC-aware at validation (`data_model.py:115-122`);
`mRID` as the identifier; JSON in and out via pydantic. That is the "JSON-native,
versioned" assumption STEP1 added, done. It is the natural base for the *result*
envelope STEP1 §4 A1 item 3 asks for — an `IslandingResult(DataModel)` gets a topic
(`islanding.live.result`, verified), a version and a timestamp for free.

*Missing.* The measurement side is a sketch that does not yet fit PMU data:

- **No header / channel identity.** A `MeasurementPmuVoltage` row is
  `{timestamp, units, mRID_ch1: complex, mRID_ch2: complex, …}`. The station /
  channel / measurement-type triple that every p-SWAMP application selects channels
  by (`TimeWindowLabeled.generate_header`, `src/pswamp/utils/pmu_time_window.py:36-60`)
  lives nowhere; the only identity is the extra-field name. The C37.118 config frame
  has no model. A1 item 1 in STEP1 (header/channel identity, `data_rate`, time base,
  units, quality) is untouched apart from `units`.
- **Complex phasors instead of magnitude/angle.** Fine as an encoding (verified:
  pydantic serialises `complex` as `"1.5-2j"` and validates it back), but it takes a
  position STEP1 says has to be decided explicitly (FREQ semantics, POLAR assumption)
  and it is the opposite of how every existing application indexes
  (`*_Magnitude` / `*_Angle` columns).
- **One message per sample per quantity.** Three models × 50 Hz. Workable — see §9
  for the measured cost — but there is no *batch* / window shape, and every existing
  application consumes a window matrix, not a row.
- **Dynamic extras are a weak contract.** `Measurement` accepts any `mRID_*` key
  (`measurement.py:32-37`); the schema says nothing about *which* channels a stream
  carries, so a generated OpenAPI/JSON schema for it is `additionalProperties`. For
  the browser edge that is a regression from today's typed `TimeWindowSlice`.

### A2 — Pub/sub over topics: **partial** (topic naming yes, subscribers no)

*Good.* **The topic is a property of the model** (`data_model.py:124-148`). That turns
STEP1's "topic catalogue as data" into "the catalogue is the set of `DataModel`
subclasses", with the direction and payload schema implied by the class. It removes
the logical→physical table for the common case; `KafkaClient(topics={Model: "custom"})`
keeps the override for a deployment (`kafka_bus.py:181-183`).

*Missing.* There is no subscriber abstraction beyond "consume a model over a range",
and — the important gap — **no in-process fan-out**. `InMemoryClient` has one
`asyncio.Queue` (`in_memory.py:75`); two concurrent `consume` calls on it would
*split* the published payloads between them, not each receive all of them. So the
draft's in-process path is a single-consumer pipe, not the bus STEP1 A2 asks for as
the default transport, and not a replacement for the PoC `Bus` or NQKafka. A
fan-out (`InMemoryClient` with one queue per open stream, or a proper `Bus` behind a
`DataClient` face) is a contained addition, but it has to be added.

### A3 — Modules in → out, easy to plug in: **missing**

Nothing in the draft is a consumer that analyses. The gateway is the pipe; what sits
on both ends is left to the caller. STEP1 §4 A3/A6 (formalised `SnapshotApp`,
declared output model, module registry, scaffold, conformance test) is entirely
still to do. What the draft *does* settle is what a module's output should be — a
`DataModel` subclass `produce`d through the gateway — which is the input those items
need.

### A4 — In-process or separate service: **partial, with an execution-model clash**

*Good.* Because a module's input is `gateway.consume(Model, …)` and its output is
`gateway.produce(result)`, hosting really is a configuration choice: the same code
with an `InMemoryClient` runs in-process, with a `KafkaClient` runs as its own
process. The Kafka client is real (seek by timestamp, bounded drain, lazy import so
the core installs without it).

*Clash.* The whole draft is `async`: `consume` returns an `AsyncIterator`, `produce`
is a coroutine, the CSV client hops to a worker thread only for file I/O. p-SWAMP's
applications are **threads that block** on `io.get_next_data_frame()`
(`src/pswamp/app_templates/snapshot_app.py:199`), and STEP1 §6 lists "the analysis
applications keep their execution model" as settled. So an adapter is required in
one direction or the other — a thread-facing `io` object fed by an async
`DataStream` (the PoC already has this exact shape in `RecordedIO`'s queue and
drop-oldest policy, `app/server-python/src/pswamp_web/recorded_io.py:466-489`) or an
async wrapper that runs an application on a thread and bridges its results back
through `loop.call_soon_threadsafe` (the PoC's `Bus.publish_threadsafe`). Neither is
hard; the point is that the draft alone does not connect to a single existing
application, and the bridge is the first thing to build.

*Missing.* No compose/k8s definition for any broker (contributor-doc rule), no load
generator, no timing. Same state as before.

### A5 — Upstream data commands: **query covered; seek/pace missing**

*Good.* **"Query a chunk" is the draft's native operation.**
`gateway.consume(Model, start=t0, end=t1)` is the range query STEP1 said the
contract had no slot for, and it comes with the reconciliation STEP1 proposed
(history lives with the provider, the repo stays stateless): a deployment registers
a TimescaleDB/CSV/Kafka client and the *same* call answers from it. `Coverage` is the
capability declaration STEP1 §A8 asked for, expressed as data ("I hold `[a, b)`,
live/not") rather than as method presence. A live source that cannot seek simply
reports a narrow now-relative window and `live=True`.

*Missing.*

- **No seek on an open stream, no pause, no step, no speed.** Navigation in the
  draft is "close this stream, open another at `t`". That is actually a clean answer
  to STEP1 A5 item 2 (the seek-vs-windowed-application problem): a seek *is* a new
  `consume`, and the application is re-primed by definition. But it means the
  application must be restartable/re-primable cheaply, which the module contract has
  to provide — the desktop precedent is "construct a new app", and the draft doesn't
  change that.
- **No replay pacing.** A historical `consume` yields as fast as the backend can
  read. The web monitor needs the recording at wall-clock speed (or ×N), which is
  what `RecordingPlayer` does today. A paced iterator (sleep to the payload's
  timestamp delta) is a ten-line wrapper, but "replay at speed *s*" is a capability
  the contract should name, because it is what the §10.2 `live | replay` mode toggles
  the controls on.
- **No command *transport*.** Consistent with STEP1's reading: source commands are
  provider calls, and the browser-edge POSTs stay as they are. But "jump to t" from
  a browser becomes "tear down this client's stream and start another", which
  touches per-client state the draft has no notion of (A7).
- **Batch jobs / correlation ids**: nothing, as expected.

### A6 — App-template contract: **missing**

See A3. Nothing to evaluate.

### A7 — Multiple clients with ids: **missing**

The draft has no client, session or user concept. A `DataStream` is per call, so
"one stream per browser" is expressible (each browser's request is its own
`consume`), and that maps onto the PoC's per-client cursor cleanly — but the
registry, cap, eviction and id plumbing are all still the PoC's and still to be
re-decided per STEP1 §5.1.

One thing the draft *does* contribute to §5.1: with the gateway, a per-client
replay is a per-client `DataStream` over a **shared** archive client, not a per-client
copy of the source. That is exactly the "per-client cursor over shared data" split
STEP1 guessed at, and it costs one async generator per client rather than a thread
and 30 MB.

### A8 — Provider contract, example, no infra in repo: **covered** (the strongest area)

- **Declared contract**: an ABC with three abstract methods and two optional ones,
  a capability flag, a documented set of rules for `consume`, and the
  `InMemoryClient` explicitly written as "the reference shape for real backends"
  (`in_memory.py:1-8`). This is the `Protocol`-split-by-capability STEP1 A2 item 1
  asked for, done as data rather than as sub-interfaces.
- **Example implementations**: three, of which the in-memory one is dependency-free
  and the CSV one is a real cold store (batched, off-loop, mtime-cached).
- **Configuration from outside the repo**: `from_env("ARCHIVE", …)` reads
  `ARCHIVE_DIRECTORY` etc.; `show_config()` prints the table; several clients of one
  type coexist by name (`config.py:1-14`). This is STEP1 A8 item 3 as a mechanism,
  and it fits the k8s/compose world better than `config.toml`.
- **Out-of-repo providers**: a TSO writes a `DataClient` subclass and imports only
  `p_swamp.core.datagateway`. There is no entry-point discovery yet, but nothing
  prevents it.
- **Conformance**: the tests are the seed of one. `test_data_gateway.py:169-221`
  (archive replays, database coverage advances while replaying, live opened exactly
  once at hand-off) is precisely the kind of behavioural pin STEP1 asked for.
  What is missing is that they exercise the *gateway* over `InMemoryClient`;
  a suite parameterised over *every* client — "stream N, header matches, seek lands,
  range returns the right rows if declared" — is the next step.

### The added assumptions

- **Language-neutral, versioned wire format**: yes. JSON via pydantic, version
  field per model, Kafka payload is `model_dump_json()` (`kafka_bus.py:288`). Pickle
  gone.
- **Contracts are testable**: the gateway yes, hermetically; provider conformance
  partially (see A8).
- **Nothing is measured**: still true, except for §9 below.

### Coverage matrix

| | Verdict | What the draft gives | What is still missing |
|---|---|---|---|
| **A1** | partial | versioned JSON envelope, UTC timestamp, topic-from-class | measurement header/channel identity, config frame, batch shape, mag/angle vs complex decision, quality |
| **A2** | partial | topic = model property; `produce`/`consume` verbs | multi-consumer in-process fan-out; subscriber abstraction; the bus itself |
| **A3** | missing | output is a `DataModel` | everything else |
| **A4** | partial | same code in-process or via Kafka; real Kafka client | async↔thread bridge to the applications; compose/k8s; numbers |
| **A5** | partial | range query as the native operation; coverage as capability | pacing, seek/step/speed on a live stream, jobs/correlation |
| **A6** | missing | — | everything |
| **A7** | missing | per-call streams over shared clients (helps §5.1) | client id, sessions, registry, cap |
| **A8** | covered | ABC + capabilities + coverage, 3 clients, env config, hermetic tests | entry-point discovery, per-client conformance suite, `.npz` recording client |

## 4. What is good — lift these

In rough order of value, with the reason each survives contact with STEP1.

1. **`DataClient` + `Capability` + `Coverage` as the provider contract**
   (`data_client_model.py`, `time_range.py`). It replaces the nine-method duck type
   with three verbs and a declaration, it makes "can't seek" a reported fact instead
   of a `pass`, and it is what STEP1 step 1 was going to have to invent.
2. **Time-range consumption with planner-driven stitching** (`planner.py`,
   `stream.py`, `data_gateway.py`). This is the answer to "history lives with the
   provider" and to the range query, and the re-plan-at-every-boundary design
   (`planner.py:1-11`) is the right call for a database that keeps ingesting during
   a replay. Keep the priority rule, the gap policy and the live hand-off margin.
3. **`DataModel` as the versioned envelope with a derived topic**
   (`data_model.py`). Adopt it as the base of the result envelope and the status
   message; make the existing `wire.py` models subclasses of it where they are
   messages rather than views.
4. **Env-var configuration with self-description** (`config.py`, `from_env`,
   `show_config`). This is how a deployment names a provider without touching the
   repo, and the "several clients of one type, namespaced by name" trick is exactly
   the multi-TSO case (`KAFKA_NO_*`, `KAFKA_SE_*`).
5. **`InMemoryClient` as reference shape and test double**, and the **hermetic test
   style** (`conftest.py` fixed anchor in the past so live logic is never touched;
   `coverage_fn` injection to model moving windows). Adopt the style for the
   conformance suite.
6. **`KafkaClient`** as the second shipped example. Newer library (`aiokafka`),
   seek by timestamp, JSON only, the partition-ordering caveat written down
   (`kafka_bus.py:88-93`). It supersedes `KafkaIO` and its latent defects (STEP1 §7)
   once the bridge in §6.1 exists.
7. **`CsvClient`** as-is, for archives and for tests — but not as the in-repo
   *reference* provider (§6.5).

## 5. What is missing — the other half of STEP1

Not criticism of the draft; it did not set out to do these. Listed so the plan in §7
is honest about what remains design work.

- **Module contract and registry** (STEP1 §4 A3/A6): abstract `run_analysis`,
  declared input model(s) and output model, status enum, one way to run and stop,
  re-prime on seek, discovery by name, scaffold, conformance test.
- **In-process bus with fan-out** (A2): N consumers of one topic in one process,
  with the thread→loop seam preserved (STEP1 keeps "exactly two crossings").
- **Measurement schema** (A1): header/channel catalogue, config-frame model, batch
  shape, the decoded units/angle conventions, quality.
- **Replay controls** (A5): pace, pause, step, speed as capabilities of a replayable
  client; the `live | replay` mode surfaced to the client.
- **Client identity and session** (A7): who owns a `DataStream`, how many, when it
  dies. The PoC's registry reshaped to key on streams rather than pipelines.
- **Jobs and correlation** (A5/A7): still the ADR STEP1 §5.5 calls for.
- **Load generator and timing** (A4): still nothing.
- **Compose/k8s for any broker** (A4): still nothing.

## 6. What needs adapting — frictions to resolve before lifting

### 6.1 Async gateway vs. blocking thread applications (the big one)

Every existing application is a thread calling `io.get_next_data_frame()` and
returning results via `io.handle_result()`. The gateway is async end to end. STEP1
settles that the applications keep their execution model, so the gateway needs a
**thread-facing adapter**: an object satisfying the (now-declared) io contract whose
`get_next_data_frame()` pops from a bounded queue that an event-loop task fills from
a `DataStream`, and whose `handle_result()` schedules `gateway.produce()` back on the
loop. The PoC's `RecordedIO` is that adapter minus the gateway: queue, drop-oldest
for live, `_push_history` for the pre-fill burst (STEP1 §2.8 "learn from"). Build it
once, in the core, against the declared contract; it is the piece that lets the first
real application run over the draft, and it is where the "exactly two crossings"
rule is kept.

The alternative — rewriting applications as async consumers — is the tail wagging
the dog that STEP1 and the port document both rule out.

### 6.2 Python 3.13 vs. this repo's 3.11

The draft requires 3.13 (`pyproject.toml:7`). Both projects here pin 3.11
(`app/server-python/.python-version`, both `requires-python`). Reading the code, the
gateway uses nothing beyond 3.11 (`typing.Self`, `datetime.UTC`, `Task.cancelling()`
are all 3.11); the requirement is a `uv init` default, not a need. Lower it when
lifting, and run the tests on 3.11 to be sure.

### 6.3 Dependencies and logging

The draft's core pulls `fastapi`, `uvicorn`, `proton-driver`, `cim-graph` and
`loguru` into the *data* layer. This repo keeps FastAPI in the web manifest only,
uses stdlib `logging` through `get_logger` (`pswamp_web/log.py`), and has a written
rule that a headless server image carries nothing it does not import. Lifting means:
`pydantic` (already a root dependency) as the only core requirement; `aiokafka` as
an optional extra as the draft already does; `loguru` → `logging`; `proton-driver`
(a Timeplus/ClickHouse client, presumably for a future TimescaleDB-like client)
dropped until such a client exists; `cim-graph` parked with the CIM layer (§6.9).

### 6.4 Package name and placement

`p_swamp.core.datagateway` vs. this repo's `pswamp`. STEP1 §5.6 says the contract can
land under the core now without pre-empting where the shared Python finally lives.
Suggested home: `src/pswamp/data/` (`models/`, `gateway/`, `clients/`) beside
`streaming/` — with `streaming/` then being the *old* transport layer to retire, not
a sibling to grow. `KafkaIO`/`NQKafkaIO`/`MQTT_IO` become either adapters of the new
contract or deletions, decided per adapter in STEP1 step 7. Whatever the name, the
lifted package must obey the same movability rule `pswamp_web/` has: relative imports
inside, no imports of anything outside `pswamp`.

### 6.5 The reference provider should be the `.npz` recording, not CSV

The draft's in-repo example is CSV. This repo already owns a better one: the 4.9 MB
`n44_line_trip_50hz.npz` — 70 s of 700 labelled channels with a real disturbance,
plus a recorder tool that refuses to write a non-reproducing scenario (STEP1 §2.3,
§8.1 of the port document). The first client to write against the lifted contract is
a **`RecordingClient`** over `Recording`: `coverage()` = the file's span,
`consume(range)` = rows in range, paced or unpaced, `live=False`, `PRODUCE` absent.
That is STEP1 step 3 with the draft's contract as the target, and it is what the
grid monitor should run on. `CsvClient` stays as the archive example and test double.

### 6.6 Message shape: row-per-sample is affordable, but needs the header

Per §9, a 700-channel-equivalent row serialises and re-validates in about 0.14 ms —
under 1 % of a core at 50 Hz — so the draft's "one pydantic model per sample" is
*not* a performance problem in-process (and in-process no serialisation happens at
all; the model is just an object). What is missing is identity: the row must be
accompanied by a **header model** (station, channel, measurement type, unit,
`data_rate`, the source's config-frame identity) sent once and referenced by the
stream, which is what `Recording`'s three-row header and `LabeledRowDecoder` already
are. Then the `mRID_*` dynamic extras can become either a validated column list or,
more simply, a numeric array field (`list[float]` in JSON, `numpy` in memory) ordered
by the header — which is what every existing application consumes. Decide this in the
schema ADR (STEP1 step 1); the draft's PMU models are the placeholder, not the answer.

Two further sharp edges in the stream loop to carry into that decision:

- `DataStream` de-duplicates on `(timestamp, mRID)` and **silently drops** a payload
  whose timestamp is below the watermark (`stream.py:145-156`). Two PMU rows at the
  same instant with `mRID=None` would collapse to one. Either `mRID` = station on
  every measurement, or the de-dup key must include the model.
- `KafkaClient.coverage()` *assumes* `[now − retention, now]` (`kafka_bus.py:185-199`)
  rather than asking the broker for the earliest offset's timestamp. Fine as a first
  cut; note it, because a topic emptier than its retention will make the planner
  hand to Kafka for a window it cannot serve, which then drains immediately.

### 6.7 Replay pacing and controls

Add to the contract, as a capability of replayable clients: `PACED` consumption at a
speed factor, plus pause/resume/step on the paced iterator. Implement it once in the
core as a wrapper any history client can be given (`consume` is unpaced;
`paced(stream, speed)` sleeps to the timestamp deltas). The `live | replay` mode the
port document §10.2 wants is then `Coverage.live` for the stream a client is on.

### 6.8 The `branch` field

`DataModel.branch` (default `"live"`) is inserted into every topic name so a feature
branch can publish to isolated topics (`data_model.py:12-16`). Not in our
requirements, but it is a real answer to the multi-TSO topic prefixing that today
lives in config (`no.*`/`se.*`) and to "don't collide on a shared broker during
development". Keep it, rename it to something that does not suggest git
(`namespace`?), and make sure the in-process default ignores it.

### 6.9 The CIM / DataHub layer is a different track — park it

`DataHub` bundles a `cim-graph` GraphDB connection, a generated 400-line CIM profile
`Protocol`, and SPARQL "converters" that hand back pandas frames for TOPS/Matpower.
That is *grid model* work: a proper replacement for the N44 sqlite +
`models/`/`grid_model.py`, and eventually the route to a TSO's real network model. It
is valuable and it is not this track. Three reasons to keep it separate from the data
gateway lift:

- It needs a GraphDB (external infra) with no stub; the gateway needs nothing.
- `DataHub.__getattr__` instantiates converters by name on first access
  (`datahub.py:41-49`) — a service locator that would make the movability rule
  (§6.4) hard to hold.
- The Matpower converter is unimplemented and cannot currently be constructed
  (§1.3).

Pull `DataHub`'s *idea* — one object that hands a module both its measurements
(`gateway`) and its topology — into the module contract as two constructor arguments,
and leave the CIM adapter for a "grid model provider" step with its own STEP doc.

### 6.10 The demo notebook

Good as a tutorial of the *contract* (cells 3–16 are the clearest explanation of
"why pydantic, why not pickle" in either repo). As an *example* it needs two Kafka
containers and a GraphDB, and half its cells cannot run on Linux until §1.3 is fixed.
Turn cells 3–31 into a runnable `examples/` script against `InMemoryClient` and the
recording client; keep the Kafka/GraphDB cells behind a compose profile if at all.

## 7. How it slots into STEP1's plan

STEP1 §8 ordered eight steps, core first. With the draft, the first three change from
"design" to "adopt and adapt", and the rest gain a concrete vocabulary.

| STEP1 step | With the draft |
|---|---|
| **1. Contract + schemas as a document and ADR draft** | Write the ADR *around* `DataClient`/`Capability`/`Coverage`/`TimeRange` and `DataModel` as given. Decide the open points here: measurement header + array-vs-extras (§6.6), mag/angle vs complex, FREQ semantics, pacing as a capability (§6.7), `branch` naming (§6.8), de-dup key (§6.6). Unit of isolation (§5.1) is now "per-client `DataStream` over shared clients" unless someone objects. |
| **2. Schemas + JSON-native codec in the core** | `DataModel` *is* the codec: `model_dump_json` / `model_validate_json`. Land the measurement header model and the result envelope as `DataModel` subclasses; retire `streaming/utils.py`'s JSON-or-pickle. |
| **3. Move `recorded_io` into the core, generalise, conformance suite** | Becomes: lift the gateway package into `src/pswamp/data/` (§6.4, on 3.11, deps per §6.3), then write `RecordingClient` over `Recording` (§6.5) as the reference provider, then the thread-facing adapter (§6.1) so `IslandingApp` runs over it unchanged. Turn the draft's tests into a client-parameterised conformance suite. |
| **4. Module contract + registry** | Unchanged scope; the module's input is `gateway.consume(MeasurementModel, …)` through the adapter and its output is `gateway.produce(ResultModel)`. |
| **5. Core bus, in-process** | Generalise `InMemoryClient` to fan out (one queue per open stream) *or* wrap the PoC `Bus` in a `DataClient` face; either way it is the `LIVE_CONSUME | PRODUCE` client with no history, registered by default. |
| **6. Adapt the web layer** | `Hub` becomes: one shared `DataGateway` for the process; per client, a `DataStream` per open panel group plus the adapter and the application threads. `HubRegistry` keys on those. Page packages unchanged at the edge; `adapt.py` files go once results are `DataModel`s. Replay controls become "close and re-`consume`" POSTs. |
| **7. Broker adapter behind a compose profile** | `KafkaClient` as lifted, plus compose/k8s entries, *after* the load generator exists. |
| **8. Jobs / correlation ADR** | Unchanged; a batch module is `consume(Model, t0, t1)` → analysis → `produce(Report)`, with the request id carried in the `DataModel` envelope (add an optional `request_id` field there — one place). |

Nothing in the draft forces a change to the *order*; it shortens steps 1–3 and gives
5 a starting implementation.

## 8. Questions for the draft's author, and decisions to take together

1. Is the row-per-sample-per-quantity shape a considered position, or a placeholder?
   The applications consume windows indexed by a header; would you object to a
   header model plus array-valued rows (§6.6)?
2. Complex phasors vs. magnitude/angle: which does the analysis side want to own?
   (STEP1 A1: today everything is mag/angle; the decoder assumes POLAR.)
3. Was single-consumer `InMemoryClient` deliberate? We need N consumers of one topic
   in one process (§A2); is a per-stream queue inside `InMemoryClient` acceptable,
   or would you rather a separate bus client?
4. Pacing: agree that "replay at speed" is a capability of the client, with the
   paced iterator provided once by the core (§6.7)?
5. `branch`: keep, rename, or drop (§6.8)?
6. Python 3.13 and the `fastapi`/`uvicorn`/`proton-driver`/`loguru` dependencies:
   anything in there that the gateway actually needs, or all `uv init` defaults and
   future intentions (§6.2, §6.3)?
7. The CIM `DataHub`: agree to run it as its own track, with the gateway lifted
   first (§6.9)?
8. Kafka `coverage()` by assumed retention: fine as v1, or should it query the
   earliest offset (§6.6)?
9. Licence headers: the draft carries `SPDX-FileCopyrightText: 2026 Louis Pauchet`
   per file; this repo uses `Copyright Contributors to the p-SWAMP Project`. Align
   on lift.

## 9. Numbers

Measured on the draft's own environment (Python 3.13, pydantic 2.13), one row
modelled at Nordic 44 scale — 262 complex phasors, the equivalent of the PoC's 524
magnitude/angle current channels — or 524 floats:

| Row shape | JSON size | `model_dump_json` | `model_validate_json` | construct | at 50 Hz (dump + validate) |
|---|---|---|---|---|---|
| 262 `complex` extras | 13.9 KB | 0.06 ms | 0.08 ms | 0.02 ms | 0.7 % of one core, 0.70 MB/s on a wire |
| 524 `float` extras | 15.0 KB | 0.05 ms | 0.06 ms | — | 0.6 % of one core, 0.75 MB/s on a wire |

Reading: the pydantic envelope is not where the cost is. Across a broker, three such
models at 50 Hz are ≈2 MB/s of JSON per stream — not yet compared against the
pickled `synchrophasor` frames the desktop path sends today (which carry the config
frame on every message), and worth measuring before A4 splits anything out; in
process there is no serialisation at all. The draft's test suite runs in 0.04 s.

STEP1's "nothing is measured" remains true for latency end to end; this is one
component.

## 10. Side findings (housekeeping in the draft, not this track)

- `measurement_PMU.py` case mismatch (§1.3) — rename.
- `CIM2MatpowerConvertor` → `hub.cim_network` (§1.3) — fix or delete.
- `events/` auto-discovery finds nothing — delete until there is an event.
- `KafkaClient.produce()` calls `self.open()` lazily but `open()` is a no-op when
  `PRODUCE` is not in `capabilities` — the `TypeError` before it guards that, fine;
  but `close()` is only reached through `DataGateway.close()`, so a gateway that is
  never closed (the notebook's is not) leaks a producer.
- `DataStream.__aiter__` returns `self` while holding one generator: iterating the
  same stream twice continues, it does not restart. Document or guard.
- `DataGateway(None)` logs a warning and "disables" itself; a misconfigured
  deployment would rather fail at startup, as `check_apps` does here.
- `.idea/` committed; `README.md` empty; `p_swamp:main` is hello-world.
