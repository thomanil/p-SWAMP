# STEP 2 — Evaluating the `test_pswamp` data-gateway draft against the STEP 1 requirements

> **Status:** WIP evaluation, second step of the data-integration track. Companion to
> `STEP1-WIP-data-integration-context.md`, which restated the target requirements
> A1–A8, assessed `main`, and proposed an ordered list of next steps. This document
> takes one input to that list — the independent design draft in the sibling repo
> `~/versioncontrolled/statnett/test_pswamp` — and asks how much of A1–A8 it already
> covers, what is good, what is missing, and what is worth lifting into the p-SWAMP
> core. It changes no code in either repo.
>
> **What was evaluated.** `test_pswamp` at commit `c6ce9a3` on branch
> `feature/core-structure-utils` (69 commits, 2026-08-24 → 2026-09-11, single author,
> Louis Pauchet / SINTEF). That branch is 67 commits ahead of the draft's own
> `origin/main`, which holds only the initial scaffold, so the branch *is* the draft.
> A second branch, `feature/dynamic-lazy-cim-loading`, adds ~1.2k lines of CIM lazy
> loading on top and is not evaluated here. p-SWAMP is at `a68b199` (STEP 1 landed).
> File paths below are relative to each repo root; `draft:` marks the sibling repo.
> **The draft's test suite was read but not executed** — running it needs a fresh
> `uv sync` in that repo, which was outside this step.
>
> **How to read this.** STEP 1's scope decisions stand: the abstraction is
> transport-neutral with an in-process default, chunk queries are consumed by
> server-side modules, the core architecture lands first and the web layer is adapted
> to it afterwards. Where the draft agrees with a settled decision it is credited;
> where it pulls against one, that is said, not re-decided.

## 0. The one-paragraph version

The draft is a well-built **data-access layer** — a provider contract with declared
capabilities, a time-range query that stitches history into live, a JSON-native
versioned wire model, environment-driven provider configuration, and a test suite
that pins the routing behaviour — and, so far, *only* that. It covers the two things
STEP 1 called genuinely new (a capability-declared provider contract with history
navigation, A8 and A5) more completely than anything in p-SWAMP does, and it answers
the pickle-on-the-wire problem (A1) in the way STEP 1 recommended. It contains no
bus, no module or application contract, no multi-client model, no playback control on
an open stream, and no result envelope; and its measurement model is shaped for a
per-PMU JSON message, not for the per-instant row of labelled channels every p-SWAMP
application consumes. Everything that is there is asyncio, while every p-SWAMP
application is a blocking thread, and that bridge is unaddressed. Quality inside
`core/datagateway/` is high; the edges — the CIM/GraphDB `DataHub`, the simulation
example, packaging — are rough and drag heavy dependencies. **Recommendation:** lift
the gateway core more or less as it is, adapt the measurement model to p-SWAMP's row
shape, and build the missing layers (bus, module contract, pacing, sync bridge)
around it in the p-SWAMP core as STEP 1 §8 steps 4–6 — rather than either adopting
the draft wholesale or starting the provider contract over.

## 1. What the draft is

### 1.1 Shape and size

| | |
|---|---|
| source | ~4.0k lines under `draft:src/p_swamp/`; 582 lines of tests in four files; two notebooks; one compose example |
| Python | `>=3.12` (`draft:pyproject.toml:7`; uses `typing.Self`, `datetime.UTC`, `match`) — p-SWAMP pins **3.11** in both manifests |
| dependencies | pydantic 2, loguru, `cim-graph` 0.5.0a11, `tops`/`tops-rt`, `fastapi` + `uvicorn` + `proton-driver` (declared, **unused** anywhere in `src/`); optional extras `kafka` (aiokafka), `geo`, `visualization` |
| names | distribution `p-swamp` — **the same distribution name as the p-SWAMP root manifest** — import package `p_swamp` (p-SWAMP's is `pswamp`) |
| relation to p-SWAMP | none; imports nothing from it, and nothing in p-SWAMP knows about it |
| CI / lint | none; no lockfile check, no linter config (the repo has a `chore: lint` commit but no gate) |

### 1.2 The pieces

The part that matters is `draft:src/p_swamp/core/datagateway/` (~1.9k lines including
the three clients). Read top-down:

| piece | file | what it is |
|---|---|---|
| `DataModel` | `core/models/data_model.py:75` | pydantic base for every payload: `version` (required, subclasses pin a `Literal`), `branch` (default `"live"`), `mRID`, `timestamp` (UTC-coerced by a validator, `:115`, but **optional**). `topic` is a class-level descriptor derived from the class name — `MeasurementPmuVoltage` → `measurement.live.pmu.voltage`, the branch spliced in at index 1 (`:125-147`) |
| `Measurement` + PMU models | `core/models/measurements/measurement.py:10`, `measurement_pmu.py` | `units` as a per-model `Literal` (`"V"`, `"A"`, `"Hz"`); `extra="allow"` with a validator that every extra field is named `mRID_<channel>`; values typed `complex` for V and I. **One object per PMU, per quantity, per sample** |
| `DataClient` | `datagateway/data_client_model.py:89` | the provider contract, an ABC: `name`, `capabilities` (a `Flag`: `LIVE_CONSUME`, `HISTORY_CONSUME`, `PRODUCE`, `:81`), `supported_models` (subclass-matched, `:131`), `priority`; abstract `coverage(model, mRID) -> Coverage \| None` (must be recomputed on every call, `:161`), `consume(model, TimeRange, mRID) -> AsyncIterator[DataModel]` with three stated rules (`:190-196`: every payload timestamped, non-decreasing, stops at `end` unless open and live), `produce(data)`; `open`/`close` |
| env config | `datagateway/config.py`; `env_settings` + `from_env` + `show_config` on each client | `{CLIENTNAME}_{SETTING}` environment variables, declared per client class as `EnvSetting` tuples, rendered as a table by `show_config()`; models are never read from the environment, "they are types, so they stay in code" (`config.py:12`) |
| `TimeRange`, `Coverage` | `datagateway/time_range.py:48`, `:140` | half-open `[start, end)`, `None` unbounded; `Coverage(range, live)` |
| `SegmentPlanner` | `datagateway/planner.py:68` | incremental: re-asks every client's coverage at each segment boundary (`:154`), picks the highest-priority client covering the cursor (`:142`), cuts the segment where a higher-priority client's coverage begins (`:256`), gap policy `skip` (warn) or `raise` (`:205`), switches to a live-capable client only once the cursor is within `live_handoff_margin` (5 s) of now (`:238`) |
| `DataStream` | `datagateway/stream.py:37` | the plan → consume → replan loop; watermark de-duplication on `(timestamp, mRID)` across segments (`:145-156`); exactly one client iterator open at a time; `aclose()`; a `segments` trace for debugging; a lag warning when replay never gains on real time (`:177`) |
| `DataGateway` | `datagateway/data_gateway.py:40` | registry of clients by name; `consume(model, start, end, mRID) -> DataStream` (`:95`); `produce(data)` fans out to **every** client with `PRODUCE` (`:124`), stamping `utcnow()` on a missing timestamp (`:138`); async context manager for client lifecycle |
| `InMemoryClient` | `clients/in_memory.py:43` | list-backed; the reference shape and the test double; live data through one `asyncio.Queue` (`:75`) |
| `CsvClient` | `clients/csv_file.py:63` | a directory as a cold store, one file per concrete model; batched reads on a worker thread; coverage by scanning the file, cached on mtime + size (`:293`) |
| `KafkaClient` | `clients/kafka_bus.py:69` | aiokafka; one topic per model (`:181`); seeks by `offsets_for_times` (`:346`); JSON via `model_dump_json` (`:288`); coverage is the **configured retention, assumed** rather than measured (`:185-199`); one `AIOKafkaConsumer` per `consume` call, `group_id=None` |
| `DataHub` | `core/datahub/datahub.py:16` | a CIM graph connection (defaults to GraphDB through `cimgraph`, `:64-70`, failure logged and swallowed, `:82`) + CIM profile + a `DataGateway` + lazily-instantiated converters |
| converters | `converter/cim/{tops,matpower,geo}.py` | SPARQL over the CIM store to TOPS model tables, matpower tables, GeoJSON; `get_pmu()` reads PMU placement from CIM (`tops.py:189`) |
| events | `core/models/events/` | `EventPayloadBase(type: str)` — five lines — plus auto-discovery of subclasses. Nothing subclasses it |
| example | `examples/simulation_task/` | compose: GraphDB (needs a licence file) + Kafka + a job that uploads a small Nordic CIM graph + a TOPS real-time simulator that publishes `MeasurementPmu{Voltage,Current,Frequency}` per PMU every 20 ms through the gateway (`rtsim.py:231-244`). No consumer; the demo notebook reads back with `consume(..., start=now-10min)` |

The whole thing is asyncio end to end. There is no thread anywhere except the
CSV client's `to_thread` file reads.

## 2. Requirement by requirement

Each of A1–A8 as STEP 1 restated it, then: what the draft does, what is good, what
misses, and the lift/adapt/leave call.

### A1 — A shared domain model for the core PMU data

**What the draft does.** Every payload is a pydantic `DataModel` with a schema
version, an identifier, a UTC timestamp and a derived topic; the wire form is
`model_dump_json()` / `model_validate_json()` and nothing else. The demo notebook
makes the case explicitly against pickle ("the receiver don't completly know what
will be received and it present a security risk"). PMU data is three models, one
per quantity, each carrying `units` as a `Literal`, with the channels of one PMU as
dynamic `mRID_<channel>` fields.

**Good.**

- This *is* the "wire format is language-neutral and versioned" assumption STEP 1
  said was missing from the list — met, and met the way STEP 1 recommended (pydantic,
  the same thing the browser edge already uses under ADR-003).
- `version` as a required field that subclasses pin to a `Literal` is a cheap,
  enforced schema version; the notebook shows a `"v2"` payload failing validation
  against a `"v1"` model.
- Timestamps are UTC-aware by validator, not by convention. STEP 1 §1 A1 asked for
  the time base to be *stated*; here it is enforced.
- Units are a declared field, not a comment.
- Time-series, alarm and status payloads sharing one base with `mRID` + `timestamp`
  is the right generalisation: it is what lets the CSV client archive anything.

**Misses.**

- **No channel identity table.** p-SWAMP applications select their inputs by
  querying a three-row header — `station` / `channel` / `measurement` —
  (`src/pswamp/utils/pypmu.py:60-90`, `channel_indexer.get_col_idx(measurement='f')`),
  and `TimeWindowLabeled` (`src/pswamp/utils/time_window_labeled.py:93`) is that
  header over a 2-D array. The draft has `mRID_<channel>` field names inside a
  per-PMU object and nothing that says which station, which quantity, which
  nominal rate. Nothing in the draft can answer "give me every frequency channel".
- **No `data_rate`, no ROCOF, no quality.** STEP 1 A1 asked for all three to have
  a place. `MeasurementPmuFrequency` carries `f` only; the C37.118 `STAT` word has
  no field; the rate is nowhere.
- **FREQ semantics are still unstated.** The rtsim writes absolute Hz
  (`(freq_est + 1) * 50`, `rtsim.py:164`), which happens to match p-SWAMP's
  convention, but the model does not say so — STEP 1's open decision (absolute vs
  C37.118 deviation) is untouched.
- **`timestamp` is optional and defaulted at produce time.** `DataGateway.produce`
  stamps `utcnow()` on a missing timestamp (`data_gateway.py:138`). That is right for
  an alert and wrong for a measurement, whose time is the PMU's time and never
  "now"; the rtsim does pass one, but the model would silently accept its absence.
- **Shape and rate.** One JSON object per PMU per quantity per 20 ms. For the
  Nordic 44 recording's scale that is 44 stations × 3 models × 50 Hz ≈ **6,600
  messages per second** on a single-partition topic, each parsed and validated by
  pydantic on the consumer side, against p-SWAMP's **one** 700-float row per sample.
  The draft measures none of this. STEP 1 is explicit that a decision like this is
  *"supported by performance numbers and experiments"*; the number is the first
  thing to get.
- **Three streams, not one.** `consume` takes one model class ("One type at the
  time !!!!!" in `examples/simulation_task/demo.ipynb`), so voltage, current and
  frequency arrive as three unrelated async iterators. Every p-SWAMP application
  needs one row of all channels per instant; nothing in the draft time-aligns or
  merges streams.
- **No result envelope.** STEP 1 A1's second layer — `{time_stamp, app, parameters,
  result}` as a JSON-native model — has no counterpart. `events/base.py` is a
  `type: str` stub; `MessagesAlerts` exists only inside the notebook.

**Call.** *Lift* the base (`DataModel`: version, identifier, UTC timestamp, JSON
codec). *Adapt* the measurement model: a sample should be one instant with a header
(station / channel / measurement) and a vector — grow it from `Recording` +
`LabeledRowDecoder` as STEP 1 A1 item 1 already says — with `timestamp` required and
`data_rate` declared; keep `mRID` as the station identity. Add the result envelope
and status enum as `DataModel` subclasses. Measure the per-message cost before
choosing between "one object per PMU" and "one row per instant" on the broker.

### A2 — Abstractions for streaming data over topics: publishers, subscribers

**What the draft does.** The topic is a property of the model class: name derived
from the class name with a `branch` segment spliced in. A `KafkaClient` maps a
model to a topic (overridable per model); `produce` publishes to every writable
client; `consume` returns a pull-based async iterator.

**Good.**

- **Model = topic** gives one registry — the set of model classes — instead of
  p-SWAMP's `[topics]` logical→physical table kept by hand in every config
  (`src/pswamp/test_utils/default_config.toml:8`). It also makes the topic catalogue
  STEP 1 A2 asked for ("name, direction, payload schema") *derivable*: the schema is
  the class, the name is `Model.topic`.
- The `branch` segment (`live` vs anything else, `data_model.py:31,145`) is a
  namespace mechanism that could carry what the multi-TSO example does today with
  `no.*` / `se.*` prefixes (`examples/nordic44_rtsim_multi_tso/config_no.toml:29`),
  and what a developer needs to keep a feature stream off the live topic.
- Fan-out on produce (`data_gateway.py:141-155`) gives write-through archiving for
  free: a Kafka client and a CSV client registered together means every live
  message is also history.

**Misses.**

- **There is no bus.** `consume` is a cursor over stored data that a caller pulls;
  there is no subscribe-with-callback, no multi-subscriber delivery, no in-process
  topic. The only in-process "live" path is `InMemoryClient.live_queue`, a single
  `asyncio.Queue` (`in_memory.py:75`) drained by `get()` (`:142`) — so two consumers
  of the same in-memory client would **split** the messages between them, not both
  receive them. STEP 1's recommendation (confirmed) was an in-process bus as the
  default with brokers as adapters of the same interface; the draft has the broker
  and not the default.
- **Kafka is the only real transport**, and `retention` there is a configured
  assumption, not a measured coverage (`kafka_bus.py:185-199`): a topic younger than
  its retention reports coverage it does not have.
- **Everything is asyncio.** Every p-SWAMP application runs a blocking loop on its
  own thread (`src/pswamp/app_templates/snapshot_app.py:199`,
  `get_next_data_frame()` at `:130` blocks). The port document's rule of "exactly two
  thread→loop seams" (`app/server-python/src/pswamp_web/bus.py:119`) exists because
  that bridge is hard; the draft has not met it yet because it has no consumer that
  is not a coroutine.
- The topic *direction* and per-topic message contract — the `x-websocket-channels`
  idea applied one layer down — are not modelled; a Kafka topic is whatever the model
  class says.

**Call.** *Lift* model-derived topic names and the `branch` idea (revisit `branch`
against the multi-TSO `[topics]` mechanism before deciding it replaces it). *Add*,
not adapt: the in-process bus is missing outright and is STEP 1 §8 step 5. Fix
`InMemoryClient` to fan out to N subscribers when it becomes the reference client,
since that is the in-process default's first implementation.

### A3 — Modules consume on one topic and produce on another, and are simple to plug in

**What the draft does.** Nothing. There is no application, module, analysis or
handler concept. The rtsim example is a producer only; the notebook consumer prints.

**Misses.** All of it. STEP 1 §4 A3/A6 items — formalised `SnapshotApp` /
`TimeWindowApp`, a module registry, declared output models, a scaffold and a
conformance test — have no counterpart.

**Call.** Nothing to lift. The draft's `produce`/`consume` pair is the *io* a module
would use; the module contract is STEP 1 §8 step 4 and stays a p-SWAMP-side job.

### A4 — A module can run in the same process as the Qt/web app, or as a separate service

**What the draft does.** By construction: register a `KafkaClient` and a consumer in
another process sees the same topics; register an `InMemoryClient` and it stays in
one process. The compose example runs the simulator as its own container publishing
to Kafka.

**Good.**

- The example is the right *shape* under the contributor rule (a service exists in
  compose; `examples/simulation_task/compose.yml`), and a compose-only Kafka is what
  STEP 1 §8 step 7 asks for.
- A consumer per `consume` call with `group_id=None` (`kafka_bus.py:222-228`) means
  N independent readers of one topic cost N Kafka consumers, not N pipelines.

**Misses.**

- Without a multi-subscriber in-process bus (A2), "same process" is really "same
  process via a broker", so A4's *choice* does not yet exist.
- No k8s manifest — the rule is "both compose *and* k8s" — and the compose file
  hard-codes a LAN address (`172.25.41.77`, `compose.yml:32`, repeated in the demo
  notebook), so it runs on one machine.
- GraphDB in the local path needs a licence file (`.env.example:3`). The simple
  Nordic graph has an `InMemoryCimConnection` (`grid/simple_nordic.py:22`) that
  would let the example run without it, but `DataHub` defaults to GraphDB
  (`datahub.py:64`).
- No measurement of what crossing a process boundary costs, which STEP 1 A4 item 4
  makes a prerequisite.

**Call.** *Adapt* the compose shape as the second example STEP 1 step 7 describes,
behind a profile, once numbers exist. *Leave* GraphDB out of the default local path.

### A5 — Upstream commands to adjust what comes down: jump forward, back, query a chunk

**What the draft does.** `gateway.consume(model, start, end, mRID)` returns a stream
over any time window, stitched across whichever clients hold parts of it, and — when
`end` is `None` or in the future — hands over to a live-capable client once the
replay catches up to now. `Coverage` and `Capability` declare what each client can
do; `priority` says who wins where two overlap.

**Good — this is the draft's strongest contribution.**

- **"Query a chunk" and "jump to a time" are the same call.** A bounded `consume` is
  the range query STEP 1 A5 said the contract has no slot for; an open-ended
  `consume` from a past `start` is a seek. STEP 1 §2.5 noted that `Recording` is
  random-access in memory but *"exposes no `(t0, t1)` method"*; this is that method,
  as a contract rather than a method on one class.
- **History → live as one stream** is exactly the *"source is a mode (live | replay)"*
  of the port document §10.2 that STEP 1 A5 cited, done at the provider layer: the
  planner keeps a cold store carrying the stream as far as it can and subscribes to
  the live source only when close to now (`planner.py:1-11`). The test
  `test_live_source_is_opened_only_once_replay_catches_up`
  (`draft:tests/test_data_gateway.py:169`) pins it, including the cold store's
  coverage advancing *during* the replay.
- **Capability-declared providers** are STEP 1 A8's first refinement, verbatim: a
  CSV archive is `HISTORY_CONSUME | PRODUCE`, a Kafka topic adds `LIVE_CONSUME`,
  and the planner never asks a client for something it did not declare. This
  replaces the seven `seek_relative_input_offset` implementations of which four are
  `pass` (STEP 1 §2.3 table).
- **Seek by time, not by message count.** p-SWAMP's `t_start` converts a wall-clock
  target into a message count assuming constant rate (`snapshot_app.py:82-89`); the
  Kafka client uses `offsets_for_times` (`kafka_bus.py:355`), the CSV client filters
  on the timestamp.
- Gap policy (`skip` / `raise`) and the watermark de-duplication across overlapping
  sources (`stream.py:145-156`) are the two things a stitched replay gets wrong first,
  and both are handled and tested.
- **History lives with the provider**, which is STEP 1's reconciliation of A5 with
  the "no database in the repo" rule: the draft has a CSV archive as a *client*, and
  the repo holds no store.

**Misses.**

- **No control on an open stream.** Nothing pauses, resumes, steps or changes the
  speed of a running `DataStream`; "seek" means close it and open another. That is a
  defensible model (the desktop precedent is "construct a new application at
  `t_start`", STEP 1 §2.5) and a cheap one here, but it is a decision the draft makes
  implicitly — and it interacts with STEP 1's open question of what a windowed
  application does when the source jumps.
- **No pacing.** A history segment is yielded as fast as the client produces it
  (the CSV client reads 512-row batches). A replay of the recording at 1×, which is
  what the grid monitor does and what a human watching a disturbance needs, has no
  home in the draft; the PoC's `RecordingPlayer` (`recorded_io.py:273`, `speed`,
  `loop`) is that missing layer.
- **No request/correlation id**, so A7's "individual results back" for a batch
  query is not addressed.
- **Kafka coverage is assumed** (above); a seek before the true start of the topic
  lands at `seek_to_end` (`kafka_bus.py:359-363`) rather than at the beginning.

**Call.** *Lift* the time-range query as the provider interface. *Add* a pacing/
replay layer over a history stream (speed, pause, step) as the p-SWAMP-side
"player", and decide explicitly that seek = new stream. This is STEP 1 §8 step 3's
material, further along than the PoC's `recorded_io`.

### A6 — A standard "app template" contract so contributors can slot in modules

**What the draft does.** Nothing. See A3.

**Call.** Nothing to lift. Note, though, that the draft's *provider* contract is the
model of what STEP 1 said a contract should look like here — an ABC, a reference
implementation, a test suite, `show_config` self-documentation — and the module
contract should be built to the same standard.

### A7 — Multiple frontend clients at once, each with a client id, individual commands and results

**What the draft does.** Nothing explicit: no client id, no session, no per-client
state.

**Good, by accident of shape.** Each `consume` call is an independent stream with
its own cursor and its own client iterator, so "one stream per browser" is the
natural unit and costs one iterator (a Kafka consumer, a file handle, a list index)
rather than the PoC's four threads and ~30 MB per client
(`app/server-python/src/pswamp_web/hub.py:70-75`). That is a better fit for STEP 1
§5.1's likely answer — per-client *cursor and view*, shared analysis — than the
PoC's per-client pipeline.

**Misses.** Client identity, sessions, the correlation id, and the unit-of-isolation
decision itself are all untouched.

**Call.** Nothing to lift; nothing in the way. The PoC's `client_id` + `SessionRegistry`
stay the browser-edge answer (STEP 1 §2.8 "keep" column).

### A8 — Open source repo, no deployment infra; a provider contract, an example implementation

**What the draft does.** `DataClient` is the contract; `InMemoryClient`, `CsvClient`
and `KafkaClient` implement it; `from_env` + `env_settings` + `show_config` make a
deployment configure a client without touching code; the test suite exercises the
contract through the gateway.

**Good — the second strong area.**

- **The seam is declared.** STEP 1 §2.3 found the provider contract to be *"a
  nine-method duck type implemented seven times and declared nowhere"*; here it is
  an ABC with three abstract methods, each documenting the rules the gateway relies
  on (`data_client_model.py:160-214`).
- **Provider selection by configuration** (STEP 1 A8 item 3) exists and is
  self-documenting: `CsvClient.show_config("archive")` prints the exact variables a
  deployment must set, with defaults and required-ness (`config.py:202`). The
  decision to keep model *classes* out of the environment is right and stated.
- **The reference client** (`InMemoryClient`) is small, and the tests read as the
  start of a conformance suite: single-client history, window honoured, stitching,
  overlap de-duplication, gap skip and gap raise, live hand-off, iterator release on
  close, timestamp stamping, duplicate names rejected
  (`draft:tests/test_data_gateway.py`). `test_csv_client.py` runs the same shape
  against a real file, including the cold-then-live case.
- **Nothing sensitive ships.** Sample data is a tiny in-code Nordic graph; the
  example's Kafka and GraphDB are compose services; no auth, no TSO config. Matches
  the rig document's rule.
- The Kafka client's SPDX headers, docstrings and lazy `aiokafka` import
  (`kafka_bus.py:10-11`) are the right posture for an optional adapter.

**Misses.**

- **The conformance suite is not one yet.** It is written against `InMemoryClient`
  (plus a CSV twin), not parameterised over "any `DataClient`". A TSO writing a
  TimescaleDB client has nothing to run against it. Nothing tests the Kafka consume
  loop at all (`test_kafka_client.py` is offline by design).
- **No out-of-repo packaging story**: no entry-point group, no "a provider imports
  the contract and nothing else" boundary — `DataClient` sits in a package that
  also imports `cimgraph` at `DataHub` level.
- The env-var scheme is one flat namespace per client name; nothing selects *which
  client classes* to instantiate from configuration, so the composition of a gateway
  is still code (`rtsim.py:252-260`). That may be the right call — it is what the
  draft argues for models — but a deployment then needs a Python entry point of its
  own.
- Recorded-file provider: the draft has CSV, p-SWAMP has the `.npz` `Recording`.
  Neither is the other; the `.npz` one is the one with a real disturbance in it.

**Call.** *Lift* the contract, the env-config pattern and the tests. *Adapt* the
tests into a client-parameterised conformance suite (STEP 1 §8 step 3) and add the
`.npz` recording as a `DataClient`. *Add* the packaging boundary.

## 3. Coverage matrix

| | STEP 1 verdict on `main` | Draft verdict | The gap the draft leaves |
|---|---|---|---|
| **A1** domain model | Partial | **Partial, good base** | measurement shape (header + row, rate, quality), required timestamp, result envelope, FREQ decision, cost measured |
| **A2** pub/sub over topics | Partial | **Partial** | no bus at all; in-process fan-out; thread↔loop bridge |
| **A3** module in → out | Desktop covered, web partial | **Missing** | everything |
| **A4** in-process or service | Desktop covered, web missing | **Partial** | needs A2's bus to be a choice; k8s; numbers |
| **A5** upstream data commands | Mostly missing | **Half covered, well** | pacing/speed/pause, seek-vs-windows decision, correlation id |
| **A6** module contract | Partial | **Missing** | everything |
| **A7** multi-client | Web covered for replay | **Not addressed** | client id/sessions stay web-edge; unit of isolation still to decide |
| **A8** provider contract + example | Partial | **Strong** | conformance suite over any client, packaging boundary, `.npz` provider |

Against STEP 1 §8's ordered steps:

| STEP 1 step | what the draft supplies |
|---|---|
| 1. contract doc + schemas + ADR draft | most of the provider contract and the wire-model base, as code with docstrings; no doc, no ADR |
| 2. schemas + JSON codec in the core | the codec and the base model; not the measurement schema |
| 3. recorded provider + conformance suite | the contract and the test shape; not the `.npz` provider, not client-parameterised |
| 4. module contract + registry | nothing |
| 5. core bus, in-process | nothing |
| 6. adapt the web layer | nothing (correctly — it is later) |
| 7. broker adapter behind compose | `KafkaClient` + the compose example, ahead of the numbers STEP 1 wants first |
| 8. job/correlation id for batch | nothing, though the range query it needs exists |

## 4. Cross-cutting observations

**The sync bridge is the adoption path, and it is cheap.** Every p-SWAMP application
reads through the nine-method `io` duck type, and the port document's rule is that a
source swap is *"a constructor argument … Don't let it become a rewrite."* A
`GatewayIO` that wraps `gateway.consume(...)` in a sync facade — a queue fed from the
event loop, `get_next_data_frame()` blocking on it, `handle_result` → `produce()` —
would let `IslandingApp` and friends run **unchanged** over any `DataClient`. That is
the eighth implementation of the seam, and the one that retires the other seven. It
also keeps the draft's asyncio core intact rather than porting it to threads.

**Numbers before the measurement model.** The one design choice in the draft with a
real cost is per-PMU JSON objects at PMU rates (§A1). Nothing decides it today, and
STEP 1 already lists a load generator and an end-to-end timestamp as prerequisites.
Do that measurement with the draft's own rtsim (it already publishes at 50 Hz) before
lifting `MeasurementPmu*` as they are.

**Design fixes that go with a lift.** Required `timestamp` on measurements;
multi-subscriber in-memory delivery; a pacing layer; a time-aligned multi-model
consume (or a single-row measurement model that makes it unnecessary); Kafka coverage
from the broker's earliest offsets rather than configured retention; the `produce`
fan-out reporting failures rather than only logging them (`data_gateway.py:157-164`),
since an archive that silently stops is a history gap discovered weeks later.

**Repo friction, if lifted as files rather than as design.** Python 3.12 against
p-SWAMP's 3.11 pin (`app/server-python/.python-version`, both `requires-python`);
distribution name `p-swamp` colliding with the root manifest; `p_swamp` vs `pswamp`;
three unused dependencies (`fastapi`, `uvicorn`, `proton-driver`) plus
`cim-graph` at an alpha version; `loguru` where p-SWAMP uses `logging`; no lint
gate. None of it is hard; all of it says "lift the modules, not the repo".

**Beyond the brief: the CIM side.** `DataHub`, the CIM profile protocol, GraphDB and
the three converters are a *grid-model provider* — the topology analogue of the
measurement provider — and they answer a question A1–A8 do not ask. p-SWAMP has a
static Nordic 44 sqlite (`app/server-python/src/pswamp_web/grid_model.py:5-8`,
`src/pswamp/models/reader.py:21`); the draft can derive a TOPS simulation, a matpower
case, a GeoJSON layer and PMU placement from a CIM graph. That is valuable and it is
a separate decision: it brings a licensed triple store into the local path and an
alpha library into the dependency set, and it is the part of the draft still moving
(`feature/dynamic-lazy-cim-loading`). Recommend a track of its own, with the same
"stub it locally" rule; the `ConnectionInterface` seam the draft already uses is the
right one to keep.

## 5. Lift / adapt / leave

| Lift as-is (concept and mostly code) | Adapt | Leave or defer |
|---|---|---|
| `TimeRange`, `Coverage`, `Capability` (`time_range.py`, `data_client_model.py:81`) | measurement model → one instant, header + row + `data_rate`, required `timestamp` (grow from `Recording` / `LabeledRowDecoder`) | `DataHub` + CIM + GraphDB + converters — own track |
| the `DataClient` ABC and its three documented rules | `InMemoryClient` → N-subscriber fan-out; becomes the in-process bus's first implementation | `branch` topic namespacing — revisit against multi-TSO `[topics]` before adopting |
| `EnvSetting` / `from_env` / `show_config` | the tests → a client-parameterised conformance suite | `fastapi`, `uvicorn`, `proton-driver` deps; `loguru` |
| `SegmentPlanner` + `DataStream` + `test_data_gateway.py` | `KafkaClient` → the second example behind a compose profile, `aiokafka` replacing `kafka-python`, coverage measured; after numbers | the rtsim example as files (its *shape* — simulator publishes through the gateway — is what `examples/nordic44_rtsim` should become) |
| `DataModel` base: `version`, `mRID`, UTC `timestamp`, JSON codec | add: result envelope + status enum as `DataModel` subclasses (STEP 1 A1 items 3–4) | the `events/` stub |
| model-derived topic names | add: pacing/replay layer over a history stream (speed, pause, step); `GatewayIO` sync facade; `.npz` `Recording` as a `DataClient` | Python 3.12-only syntax (`Self`, `datetime.UTC`) until the pin moves |

Where it lands: under `src/pswamp/` in the core, beside `streaming/`, which is where
STEP 1 §5.6 says the contract can go without pre-empting the port document's §7
question. The draft's own layout (`core/datagateway/`, `core/models/`) is a sensible
sub-structure to keep.

## 6. What this adds to STEP 1's list

The draft does not change the order. It advances steps 1–3 (contract, codec,
provider + tests) far enough that those become "adapt and land" rather than "design",
and supplies step 7's adapter early. Steps 4, 5, 6 and 8 are untouched and remain
p-SWAMP-side work. One new decision it forces, worth an ADR of its own alongside
STEP 1 §5's list:

- **Pull versus push at the provider seam.** The draft's provider interface is a
  *pull* cursor (`consume` → async iterator over a time range); STEP 1's A2 assumed a
  *push* bus (publish/subscribe with callbacks). They are not in conflict — a bus can
  sit on top of a pull cursor, and a live client can back a bus — but which one is
  the contract a TSO implements decides what a provider has to write. The draft's
  answer (pull, with `live` as a capability of the same call) is simpler to implement
  and to test; it should be argued for explicitly rather than inherited.

And one measurement it makes possible now: the rtsim publishes at PMU rate through
the gateway to Kafka, so the per-object message cost (§4) can be measured in the
draft's own environment before any of this moves.

## 7. Housekeeping findings in the draft (not this track)

- `src/p_swamp/__init__.py` is the `uv init` hello-world; the `p-swamp` console
  script points at it.
- `rtsim.py:39-244` reads module globals (`ps`, `pmu_df`, `dh`) inside a method of
  `RealTimeSimulatorPublish`; the class only runs as `__main__`.
- Hard-coded host address `172.25.41.77` in `compose.yml:32` and in
  `examples/simulation_task/demo.ipynb`.
- `DataHub._init_cim_graph` catches every exception and continues with `cim_db =
  None` (`datahub.py:82-89`); every converter then fails on `None.execute` later
  and further away.
- `KafkaClient.produce` calls `open()` lazily (`kafka_bus.py:283`), so a client used
  outside the gateway's context manager leaks a producer.
- The two notebooks embed absolute Windows paths in outputs and one of them
  requires two Kafka brokers on different ports for a demo of a single feature.
- `test_kafka_client.py:44` builds an "unrelated" model as
  `Measurement.__bases__[0]`, i.e. straight off `DataModel`, which reads as a workaround
  for the conftest `Measurement` shadowing the package's `Measurement`.
- `requires-python >= 3.12` is broader than the code needs in only one place
  (`match` in `rtsim.py`); everything else is `Self` and `datetime.UTC`, which have
  3.11 spellings.
