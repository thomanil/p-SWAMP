# STEP 4 — The thin slice: the data-integration architecture, applied to the PMU test streamer

> **Status:** WIP, fourth step of the data-integration track. STEP1 stated the
> requirements (A1–A8) and where the repo stood; STEP2 evaluated Louis Pauchet's
> data-gateway draft; STEP3 synthesised a target architecture of six contracts.
> This step **implements a thin vertical slice of that architecture** — enough to
> run, click and measure — on the one subapp that was safe to rebuild: the PMU
> test streamer, which read a static text file and touched nothing else. The
> grid monitor, `pswamp_web/` and the existing desktop code under root
> `src/pswamp/` are **untouched**; the only new thing at the root is a new
> package, `src/pswamp/data/`, beside them.
>
> Everything described here is on branch
> `test-and-iterate-data-flow-and-integration-patterns`, runs locally with no
> broker, database or extra process, and passes `./scripts/error_check.sh` and
> `./scripts/run-python-server-tests.sh` (58 tests, ~0.7 s). It was also driven
> end to end over real sockets and in a browser (§7).

## 0. The one-paragraph version

The streamer used to be a ticker over a list of strings, one position per
client. It is now the smallest complete instance of STEP3: the sample file is a
**provider** (`DataClient`) that declares what it holds; the process has one
**gateway** through which a consumer names a model and a time range, never a
source; each browser owns a **paced, seekable replay** over that gateway (one
async generator, not a thread); play / stop / step / speed / seek are POST
commands on that replay; and a **batch report** — mean voltage over any range
of the same stream — is a job that queries the provider, analyses off the loop,
and `produce`s a typed `Report` onto the in-process **bus**, from where the
report page's own socket picks it up by `request_id`. All of that rests on a
new core package, `pswamp.data` (contracts 1–3 of STEP3, Louis's gateway lifted
largely as written), which is provable with no web server. What the slice does
**not** do is formalise the analysis-module contract (STEP3 contract 4) or move
the monitor onto any of this; those are the next steps, and the slice's job was
to find out what they will hit before they are attempted. §5 lists what it
found.

## 1. What was built

```
src/pswamp/data/                          NEW — contracts 1–3, core, movable (relative imports, no FastAPI)
├── __init__.py                           the public surface: `from pswamp.data import DataGateway, Replay, Sample, …`
├── time.py                               ensure_utc / utcnow                                       ← Louis, verbatim
├── models/
│   ├── base.py                           DataModel (branch→namespace), TopicDescriptor, catalogue()  ← Louis, adapted
│   ├── measurements.py                   StreamChannel, StreamHeader, Sample, Value (NaN→null)        new (STEP3 §4.2)
│   ├── results.py                        ModuleRef, Result, Report                                    new (STEP3 §4.3)
│   └── commands.py                       JobAck                                                       new (STEP3 §4.4)
├── gateway/
│   ├── client.py                         DataClient, Capability, + default from_env()                 ← Louis
│   ├── time_range.py                     TimeRange, Coverage                                          ← Louis, verbatim
│   ├── planner.py                        SegmentPlanner, GapPolicy                                    ← Louis (logging only)
│   ├── stream.py                         DataStream (de-dup key widened to include the model)         ← Louis, adapted
│   ├── gateway.py                        DataGateway                                                  ← Louis (logging only)
│   ├── config.py                         EnvSetting & env_* readers; + resolve_client_type,
│   │                                     build_gateway_from_env (PSWAMP_CLIENTS, <NAME>_TYPE)       ← Louis, extended
│   ├── pacing.py                         Pacer: speed / pause / step / interrupt over any stream      new (STEP3 §5.4)
│   ├── replay.py                         Replay: seekable cursor; seek = new consume; loop            new (STEP3 §7.3, §8.3)
│   └── jobs.py                           run_batch_job, new_job_id                                    new (STEP3 §8.4)
├── clients/in_memory.py                  InMemoryClient: multi-subscriber fan-out = the bus           ← Louis, adapted
└── conformance.py                        check_client(): the provider rules as one assertion set      new (STEP3 §5.6)

app/server-python/src/
├── pmu_data/                             NEW service package (no router; in server.SERVICES)
│   ├── service.py                        the process's one gateway, from env or defaults; coverage_of()
│   ├── sample_file.py                    SampleFileClient — the text file behind the contract
│   └── sample_data.txt                   moved here from pmu_test_streamer/
├── pmu_test_streamer/api.py              REWRITTEN: per-client Replay; 6 POSTs; new PmuStreamState
├── pmu_report/                           NEW app: model.py (mean_voltage → VoltageReport), api.py (job, bus consumer, socket)
├── server.py                             SERVICES = [pmu_data, pswamp_web]; APPS += pmu-report
├── shared.py                             re-exports serve_updates / offer / SessionRegistry / Event
└── api_contract.py                       + x-topics: the DataModel catalogue in the OpenAPI document

app/server-python/tests/                  9 new files: gateway (Louis's, ported), bus fan-out, pacer+replay,
                                          models, config, jobs+conformance, sample-file client, report module
app/client-web/src/pages/pmu-test-streamer/
├── usePmuStreamSocket.ts                 header kept from the opening message; window of played samples derived
├── StreamWindow.tsx                      recent samples per station + the current sample in full; kV/° only here
├── PmuTestStreamerPage.tsx               transport + seek slider + speed, shown only when mode == "replay"
└── pmu-report/{usePmuReportSocket.ts, ReportCard.tsx}   ↔ /api/pmu-report

doc/api/openapi.json, app/client-web/src/api/schema.ts   regenerated (+2 channels, +3 commands, +x-topics)
```

Not touched: `pswamp_web/`, `reference_subapp/`, the desktop package's existing
modules, both manifests and lockfiles (pydantic was already a root dependency),
the Dockerfile, compose, k8s, CI.

## 2. The chain, end to end

```
 sample_data.txt ──parse once──▶ SampleFileClient ("source", HISTORY_CONSUME, priority 10)   ┐
                                 InMemoryClient   ("bus",    LIVE|HISTORY|PRODUCE, [Report])  ├─ DataGateway (one per process, pmu_data.service)
                                 <a deployment's own client, by PSWAMP_CLIENTS / <NAME>_TYPE> ┘
                                                    │
        ┌───────────────────────────────────────────┴───────────────────────────────────────────┐
        │ streaming (per client)                                │ batch (per request)             │
        │                                                       │                                 │
        │ Replay(gateway, Sample, mRID=stream)                  │ POST /api/pmu-report/mean-voltage/run {start_s, end_s, request_id}
        │   = consume(Sample, start=position) → Pacer(speed)    │   → JobAck{job_id, request_id}          (no data in the reply)
        │   seek(t)   = close + consume(Sample, start=t)        │   → run_batch_job: consume(Sample, t0, t1) → [rows]
        │   play/pause/step/set_speed = pacer controls          │        → mean_voltage(header, rows)  [worker thread]
        │        │ async for sample                             │        → gateway.produce(VoltageReport{request_id})
        │        ▼                                              │              │ bus fans out to every consume(VoltageReport)
        │ pusher task → send_state to the client's sockets      │              ▼
        │                                                       │   pmu_report lifespan task: consume(VoltageReport, start=now)
        │ WS /api/pmu-test-streamer/ws → PmuStreamState         │      owners[request_id] → client → store → nudge sessions
        │ POST …/playback/{play,stop,forward,back,speed,seek}   │   WS /api/pmu-report/ws → PmuReportState{reports, pending, errors}
        └───────────────────────────────────────────────────────┴─────────────────────────────────┘
                                                    browser: one page, two sockets, seven POSTs
```

Two thread seams exist in the whole slice: none in the streaming path (the
replay is an async generator on the loop), and one in the batch path
(`asyncio.to_thread` around the analysis, because that is the execution model
every real p-SWAMP module has).

## 3. Contract by contract: what landed, and where it differs from STEP3

### Contract 1 — Messages (`pswamp.data.models`)

- `DataModel` is Louis's envelope with `branch` renamed to `namespace` (STEP3
  §15.4). `catalogue()` walks the subclasses that pin a `version` and sorts them
  by topic; `api_contract.inject_topics` publishes that as `x-topics` beside
  `x-websocket-channels`, so the generated document now says
  `sample.live`, `stream.live.header`, `voltage.live.report`.
- The measurement layer is STEP3 §4.2's *header once, array rows*:
  `StreamHeader{stream_id, data_rate, channels, source}`, `StreamChannel{station,
  channel, measurement, unit, mRID}`, `Sample{mRID=stream_id, values, quality}`.
  Units are SI on the wire (V, rad, Hz); the provider converts. NaN becomes
  `null` once, at the `Value` type, not per page.
- **Deviation:** `values` is `list[float | None]`, not the numpy-annotated
  `FloatArray` STEP3 proposes. Nothing in the slice consumes a matrix; the
  numpy type is the first thing the module contract (contract 4) will need.
- **Deviation:** the class is `StreamChannel`, not `Channel`. The monitor's
  `pswamp_web/wire.py` already publishes a `Channel` view model and the
  contract has one `components.schemas` namespace; §5.3.
- `Result{module, parameters, request_id}` and `Report(Result){range_start,
  range_end, n_samples}` are the envelope; `VoltageReport` is the first subclass.
- `JobAck{status, applied, job_id, request_id}` is new; `CommandAck` stays in
  `pswamp_web/wire.py` for now (moving it would touch the monitor).

### Contract 2 — Providers (`pswamp.data.gateway`, `pmu_data.sample_file`)

- `DataClient` / `Capability` / `Coverage` / `TimeRange` / `SegmentPlanner` /
  `DataStream` / `DataGateway` are lifted; only logging changed, plus the
  de-dup key in `DataStream` now includes the model class (STEP2 §6.6) — pinned
  by `test_two_models_at_one_instant_both_survive_dedup`.
- `DataClient.from_env` exists on the base and *refuses* by default, so a
  client that never thought about configuration cannot be built from an
  environment by accident.
- `SampleFileClient` is the deployment-side example: `HISTORY_CONSUME` only,
  serves `StreamHeader` and `Sample`, coverage = the file's span at a fixed
  epoch, `produce` raises. It imports `pswamp.data` and nothing else. It passes
  `check_client`.
- Configuration: `PSWAMP_CLIENTS=source,bus` + `SOURCE_TYPE=sample_file` /
  `BUS_TYPE=in_memory` / `X_TYPE=my_pkg.clients:MyClient`, each client then
  reading its own `<NAME>_*`. Unset, the process runs `default_clients()`.
  Pinned by `test_gateway_config.py`.
- `pmu_data.coverage_of(model, mRID)` merges every client's coverage. The
  gateway has no such call (it plans a segment at a time), and a scrub bar
  needs the whole span. Small, but it is an api the gateway will want.

### Contract 3 — The bus is a client (`pswamp.data.clients.in_memory`)

- `InMemoryClient` now fans out: each open-ended `consume` registers its own
  queue, `produce` stores *and* publishes, `publish` publishes only. Retention
  is bounded (`max_records`), queues are bounded (`queue_size`), and the overflow
  policy is per model (`drop_oldest` for sample-like payloads, drop-newest with a
  warning for everything else) — the two policies the port document §4.4 found
  live and history need.
- An **empty** live-capable client reports coverage `[now − 1 s, now)` with
  `live=True`, so the planner can route a subscriber to it before anything has
  been produced. Without that, nobody could ever subscribe to an idle bus (§5.1).
- The bus declares `[Report]` and deliberately **not** `Sample` (§5.2).

### Contract 4 — Modules: **not formalised**

`pmu_report.model.mean_voltage(header, samples) -> VoltageReport` is the
*shape* — a pure function, channel selection by the header's `measurement`
key, a declared output model, no I/O — but there is no `AnalysisModule` base,
no `ModuleIO`, no registry, no scaffold. Nothing in the slice needed one: the
streamer has no analysis, and the batch job is a single call. This is the
largest piece of STEP3 still untouched and the one the monitor migration
depends on.

### Contract 5 — Pipelines, sessions, commands, jobs

- **A replay per client** (STEP3 §8.2, the `("replay", client_id)` row) is a
  `Replay` object plus one pusher task, held on the client's `ClientState`. It
  is started by the first socket to connect and stopped by the last to leave;
  the *position, playing flag and speed* survive in the state dict, exactly as
  the streamer's index did before, so a reload resumes. No registry, cap or
  eviction yet — one async generator per viewer is cheap enough that the
  `MAX_PIPELINES` machinery of the monitor was not needed to prove the shape.
- **Source commands** (STEP3 §8.3): `play`, `stop`, `speed {speed}` change
  retained state and forward to the replay if one is running; `forward`,
  `back`, `seek {position_s}` require one and answer 404 otherwise — the same
  rule as the monitor's `live_hub`: a command never builds a pipeline. `back`
  is a seek to `position − 1/data_rate`; `seek` clamps to coverage.
- **Seek is a new `consume`** (STEP3 §5.4 / §7.3, decided once): `Replay.seek`
  opens a fresh stream at the target, interrupts the pacer holding the old
  one, and re-anchors — so there is never a burst and never a stale window.
  While paused, a seek grants one step so the landing sample is shown.
- **Jobs** (STEP3 §8.4): `run_batch_job` bounds an open end at *now*, collects
  the range, runs the analysis on a worker thread, stamps `request_id` and the
  covered range, and `produce`s. The app keeps `owners: request_id → client_id`
  and a lifespan task that consumes `VoltageReport` from the bus and files each
  report under whoever asked. The browser may supply its own `request_id`
  (the page sends `ui-<time>`); otherwise it equals the `job_id`.

### Contract 6 — The browser edge: unchanged, and exercised

ADR-003 as is. Two new socket channels and three new commands entered the
generated contract with no generator change; `check_apps` did its job on the
report package the moment it had a socket; `postCommand` typed the two new
request bodies. The page takes `Wire['PmuStreamState']` etc. straight from the
generated types and *derives* only what is its own (the kept header, the window
of played samples).

## 4. Coverage against the requirements

Verdicts are for **this slice**, not the repo; STEP1 §3 is the baseline.

| | STEP1 verdict (repo) | In the slice | What proved it |
|---|---|---|---|
| **A1** shared domain model | partial | **covered for measurements + results** | `StreamHeader`/`Sample`/`Report` are the only shapes on any topic; SI units; NaN→null at the type; `x-topics` in the contract |
| **A2** pub/sub over topics | partial | **covered, in-process** | `produce`/`consume` on `DataGateway`; N subscribers each get every report (`test_every_subscriber_receives_every_payload`); topic from the class |
| **A3** module in → module out, easy to plug in | desktop only | **partial** | `mean_voltage` is in→out with a declared model, but there is no module base/registry — adding a module is still "write a function and a job" |
| **A4** in-process or separate service | web missing | **partial** | the streamer and the report never name a client; `PSWAMP_CLIENTS` swaps backends; but no broker client, no compose/k8s, no numbers beyond §7 |
| **A5** upstream data commands | mostly missing | **covered for replay** | play/stop/step/speed/seek as POSTs on a per-client replay; `mode: live\|replay` on the wire; range query is `consume(Sample, t0, t1)`; report as request/response with `request_id` |
| **A6** app-template contract | partial | **not attempted** | — |
| **A7** multi-client, per-client commands and results | web covered for replay | **covered** | one replay per client id; reports filed per client; two browsers seeing different instants and different report lists is correct |
| **A8** provider contract + example, no infra | partial | **covered** | `DataClient` declared; `SampleFileClient` written against it from outside the core; `check_client` conformance; `<NAME>_TYPE=module:Class` |
| wire format language-neutral, versioned | — | **covered** | every topic payload is `model_dump_json`; `version` is a `Literal` per subclass; a `v2` payload fails validation loudly |
| contracts are testable without a server | — | **covered** | all 58 tests are hermetic; the core suite imports only `pswamp.data` |

## 5. What the slice taught

These are the things that were not in STEP3 and would have been discovered
again, later, on the monitor.

1. **An empty live client must cover "now".** Louis's `InMemoryClient` derived
   coverage from its records, so an idle bus reported *nothing* and the planner
   never routed a subscriber to it — the report collector could not subscribe
   until the first report existed, which is after it needed to. A live-capable
   client now reports `[now − 1 s, now + ε)` when empty (the Kafka client in the
   draft already assumes `[now − retention, now]`). The 1 s of slack is because
   the planner stamps its cursor *before* it awaits `coverage()`.

2. **Which models a live client declares decides where "the end of history" is.**
   If the bus had declared `Sample`, the planner would — at the end of the
   sample file — skip the gap to the bus's live window and *tail it forever*
   for samples that never come; the replay would never end its pass and never
   loop. That is the planner doing exactly what STEP3 §5.3 wants for a TSO
   (archive → broker → live) and exactly what a recording replay must not do.
   The answer in the slice is a declaration (`InMemoryClient("bus", [Report])`);
   the general answer, when a deployment does have live samples, is that a
   *replay* asks for a bounded range or a non-live plan, and that is a
   `Replay`/gateway option STEP3 should name.

3. **One `components.schemas` namespace.** The contract generator refused
   `Channel` twice (the monitor's view model vs. the core's channel row) — as
   designed, and the first time a core message met an edge view with the same
   name. Views and messages will collide again (`Alarm`, `AppStatus`,
   `IslandingResult` all exist in `wire.py` today under the names STEP3 §4.3
   gives the messages). STEP3 phase 1 needs a naming rule before those land:
   either views get a suffix, or the messages replace them.

4. **Pacing needs an interrupt, and it has a blind spot.** A seek cannot wait
   for the pacer's current sleep to end (seconds at ×0.25), so `Pacer.interrupt`
   discards the held payload. But a pacer blocked *inside the stream* waiting
   for live data cannot be interrupted until that payload arrives. Harmless on
   a recording; on a live source a seek would lag one sample. Noted in the
   docstring; the fix is racing the fetch against the wake event.

5. **The header rides once, the window is the client's.** Sending
   `StreamHeader` on the opening message and one `Sample` per tick, and letting
   the page keep the window, is the monitor's "deltas, not windows" rule
   applied from the start — and it turned the old 100 msg/s of raw lines into
   20 msg/s of typed rows. The page's `usePmuStreamSocket` has to *derive*
   (keep the header, accumulate rows) and that derivation is the only
   hand-written vocabulary on the client.

6. **Request/response fits inside commands-up, state-down with one dict.**
   `owners[request_id] = client_id` in the app, a `request_id` on the report,
   and a consumer on the bus. The envelope needed no client id, the module
   never learned a browser exists, and the same report produced by another
   process would take the same path. The cost is that the map lives in the
   web app; if reports must survive a restart, that map is the thing to persist.

7. **Commands need a running pipeline, or a rule for not having one.** Step /
   back / seek without an open socket answer 404, mirroring `live_hub`. Play /
   stop / speed apply to retained state so a reconnect honours them. The split
   fell out naturally: whatever a *reconnect* must restore is state; whatever
   only means something *on a stream* is a pipeline operation.

8. **Two small api gaps in the lifted gateway.** There is no "total coverage"
   call (added as `pmu_data.coverage_of`), and `DataGateway(None)` still
   *disables* rather than fails (kept verbatim; STEP2 §10 already flagged it).

9. **Tooling gotchas worth writing down.** `openapi-typescript` types an
   optional `requestBody` as `requestBody?:`, which the client's `CommandBody`
   conditional reads as *no body* — so a command with an all-defaults body must
   still be declared required (`body: RunMeanVoltage`). BSD `sed` has no `\b`.
   The server tests must run via `run-python-server-tests.sh` (or from
   `app/server-python/src`) for the app packages to import.

## 6. Numbers

From the socket drive in §7, on a laptop, one client:

| Measure | Value |
|---|---|
| samples pushed at ×1 | 20 / s (20 Hz source) |
| samples pushed at ×10 | 206 / s over 0.5 s |
| loop pass length at ×10 | 0.297 s, 0.594 s, 0.890 s for passes 1–3 (2.95 s of data) |
| step / back / seek while paused | exactly one sample each, at the expected index |
| reconnect | resumes at the retained position, paused, header re-sent |
| report over 60 samples | ack and report on the socket within the same 10 ms log line; mean 371 560 V |
| unit tests | 58 passed in 0.67 s |

Still unmeasured, as STEP1 §1 noted: end-to-end latency, and anything at the
monitor's 700 channels × 50 Hz. The slice's `Sample` is 15 floats.

## 7. How to run and verify

```
./scripts/run-python-server-tests.sh            # 58 tests, hermetic, no port
./scripts/error_check.sh                        # tsc, eslint, lockfile, py_compile, ruff, contract
./scripts/start-local-hotloaded-pswamp-server.sh
./scripts/start-local-hotloaded-pswamp-web-client.sh   # then open /pmu-test-streamer
```

What was checked by hand on this branch: `uv run src/server.py` on :8000 and
the Vite client on :5173; the page renders the header, the landing sample,
`1 of 60`, the seek slider, the speed select and the transport controls; Play
runs at ×1 with the wrap from 2.95 s to 0.00 s visible in the window; "Report
whole dataset" shows the request id as pending and then the report with per-
station means; no console errors. Over raw sockets (a script in the session's
scratch dir, not committed): every command, the 404 without a replay, the 422
on a bad range, and that no `values` array reaches the report page.

The shipped image was also checked: `./scripts/e2e-smoke-test.sh` builds it
through compose (the Dockerfile's `import server` smoke now imports `pmu_data`,
`pmu_report` and `pswamp.data` inside the image) and drives the reference
subapp against it — green. The smoke test does not yet drive the streamer or the
report; adding those steps is the natural next addition to it.

Configuration to try: `PSWAMP_CLIENTS=source,bus SOURCE_TYPE=sample_file
SOURCE_PATH=/some/other/records.txt BUS_TYPE=in_memory`, or a
`SOURCE_TYPE=my_pkg.clients:MyClient` on the path.

## 8. Deliberately not done here

- **No `pswamp.modules`** (contract 4): `AnalysisModule`, `ModuleIO`,
  `ModuleRunner`, registry, scaffold, conformance. §3.
- **No `RecordingClient`** over the `.npz`, no `CsvClient`, no `KafkaClient`
  lifted; no compose/k8s change. The sample file is the one provider.
- **The monitor is untouched**: `hub.py`, `bus.py`, `recorded_io.py`,
  `replay.py`, `stores.py` and the per-page adapters all still exist and still
  run the `/` dashboard exactly as before. `pmu_test_streamer` is the only app
  on the new path.
- **`CommandAck` not moved**, `wire.py` not split into views and messages,
  `FloatArray` not introduced, `PipelineRegistry` not generalised — each would
  touch `pswamp_web/`.
- **No ADRs written.** 005 (messages/topics) and 006 (provider contract) can
  now be drafted from running code rather than from a proposal; §5.2 and §5.3
  are decisions they should record.
- **`pmu_test_streamer` is still "slated for retirement"** in AGENTS.md. It is
  now the reference for the *data path*, the way `reference_subapp` is for the
  *page* path; whether it is renamed to `source/` per STEP3 §9 or kept is a
  naming call for when the monitor moves.

## 9. Next steps, in order

1. **Contract 4 on this slice first.** Wrap `mean_voltage` as the first
   `AnalysisModule` with a `ModuleIO` fed by `Replay` — a streaming module over
   the same 15-channel sample, in the same package, with the same tests. That
   is where `FloatArray`, `reset()`/re-prime on seek and the thread seam get
   decided, on 15 channels instead of 700.
2. **`RecordingClient` over `n44_line_trip_50hz.npz`** in `pswamp.data.clients`,
   passing `check_client`, and `SampleFileClient` demoted to a test double. The
   streamer then replays the real recording by changing one default.
3. **Naming rule for views vs. messages** (§5.3), then ADR-005/006.
4. **Replay over a gateway with a live client** (§5.2): a bounded or
   history-only plan option, tested with an `InMemoryClient` that declares
   `Sample` and `LIVE_CONSUME`.
5. **Then the monitor** — STEP3 phase 5 — with everything above already
   proven on the slice.
