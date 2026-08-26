# Porting p-SWAMP from Qt to a web front end

Capturing a bunch of verbose context here with LLM tooling along the way:
how the grid monitor got built, what it cost, what is left to do, and where the
Python should live once it is done.

`AGENTS.md` describes the system as it *is*. This file describes how it got
there, why, where the traps are, and what happens next. Read this one if you are
re-doing the work, extending it, finishing the port, or deciding where the code
lives; read `AGENTS.md` if you are changing the result.

**Sections at a glance.** §1–§2 are what exists and what was decided. §3–§6 are
the lessons, and §4 is the expensive part. §7 is the layout decision, with the
two plans in §8 and §9. §10 is the roadmap for finishing the port and retiring
Qt — read it before §7, because it changes the answer. §11–§12 are backlog.

---

## 0. Status: this happened in two moves

**The port was originally done in a separate repo.** The client-server harness
lived in `pswamp-client-server-poc`, beside a `../p-SWAMP` checkout, and the
central discipline of the work was that it modified that checkout *not at all* —
p-SWAMP was a library, reached through a uv path dependency.

**Those two are now one repo**, the one you are reading this in: the harness
under `app/`, the desktop package at the root. The port has been reapplied on top
of that. The port itself is unchanged; only the seam between the two halves
moved:

| Then (separate repos) | Now (one repo) |
|---|---|
| `p-swamp = { path = "../../../p-SWAMP" }` | `p-swamp = { path = "../../" }` — the repo root |
| `scripts/vendor-pswamp.sh` staged a filtered copy into `.vendor/` | **Deleted.** The source is already in the build context; `.dockerignore` does the filtering |
| Image mirrored a two-checkout `/workspace` | Image mirrors the one repo at `/workspace/p-SWAMP` |
| compose watched `../p-SWAMP/src` | compose watches root `src/` |
| CI path filters `app/**` | also `src/**` + root `pyproject.toml`, since root `src/` now ships |

The `/workspace` depth trick survives, and for the same reason as before
(§4.1): uv will not normalise a relative path above its base directory, so the
server cannot be flattened to `/app` while `../../` has to mean the same thing on
a laptop and in the image.

**The "changes nothing upstream" rule is now a design rule, not a fact of the
filesystem.** There is no separate upstream to protect. The discipline it
produced still holds in the code — `pswamp_web/` imports `pswamp.*`, and nothing
under root `src/` imports anything from `app/` — and it is worth keeping, because
it is what keeps *either* of §7's moves cheap. But nothing enforces it for you
now except review.

---

## 1. What exists

**Server** — `app/server-python/src/pswamp_web/` (~2200 LOC):
`wire.py` (the schema the Qt side never needed), `bus.py` (thread→loop),
`hub.py` (one pipeline per client, and the registry over them), `replay.py` (source + the counting-window
subclass), `recorded_io.py` (replay through p-SWAMP's `io` seam), `channels.py`,
`grid_model.py`, `stores.py`, `data/n44_line_trip_50hz.npz`, and five page
packages. Plus `tools/record_n44_dataset.py`, which regenerates the recording
under the `[full]` environment.

**Client** — `app/client-web/src/pages/grid-monitor/` (~1900 LOC): one app, five
routes — the dashboard at `/` plus four focused panel routes rendering the *same*
components at `variant="focused"`.

**Build** — the image is ~700 MB (scipy 119 + pandas 45 + numpy 56 dominate).

### The four things the harness needed that the analysis core did not provide

| Need | Where it lives |
|---|---|
| Replay a recorded, *labelled* PMU stream through the `io` seam | `pswamp_web/recorded_io.py` (~500 LOC) |
| A recorded Nordic 44 dataset with a real disturbance | `pswamp_web/data/n44_line_trip_50hz.npz` (4.9 MB, 700 channels), made by `tools/record_n44_dataset.py --channels all` |
| Know how many samples are new since the last read (for delta pushes) | `CountingTimeWindowLabeled` in `replay.py` — a ~30-line subclass |
| Disjoint island groups | `island_groups()` in `stores.py`, compensating for a core bug |

The last two are **compensations, not improvements**: the underlying gaps are
still in `src/pswamp/` and are listed in §11 as changes worth making on their own
merits. Don't "simplify" either away without fixing the gap first. The first two
are code that should arguably live in the core eventually, and is written so it
can simply move.

**`synchrophasor` is deliberately excluded from the image.** It is a declared
base dependency of the desktop package but only the live-PMU and playback paths
import it, and it is the one dependency fetched from git rather than an index.
Excluding it keeps git, network access and a hashless VCS pin out of the build.
The Dockerfile's `import server` smoke test is what makes that a checked decision
rather than a hopeful one — keep it. Note §10.2: restoring the live PMU path puts
it back.

### Cost to recreate, if this is ever thrown away

| | Cost |
|---|---|
| The recording `.npz` | **Cheap** — the generator runs in ~7 s. But it needs the `[full]` extra and a working `tops-rt`, so it is only cheap while that environment exists. |
| `recorded_io.py` | **Expensive.** Not the code — the knowledge in §4.3/§4.4 about what `TimeWindowApp` actually expects. |
| `pswamp_web/` | Moderate. The shapes in `wire.py` and the bridge in `bus.py` are the durable parts. |
| The React client | Moderate, and see §6 — build it as one dashboard this time. |
| **§4 of this document** | Not recreatable except by paying for it again. |

### The recording, and why it now carries currents

Regenerated with `--channels all`: **3501 samples × 700 channels**, 4.9 MB, up
from 176 channels and 1.0 MB. The extra 524 are `i_Magnitude`/`i_Angle`, and they
exist for exactly one reason — `LineOutageDetectionApp` reads `i_Magnitude` and
nothing else does. Without them it runs happily and detects nothing, forever,
which is a worse failure than not running it at all.

The simulation is otherwise identical, and the generator's own `verify()` proves
it: same median frequency, same median voltage, same islanded stations. Nothing
downstream needed changing — the time-window stream is bounded by the client's
channel selection, not by the recording's width, so the measured bandwidth below
is unchanged.

**Regenerating needs less than the `[full]` extra.** A Python 3.11 venv with the
root package + `tops-rt` + `synchrophasor` is enough; Qt is not required. One
wart: the tool also needs `fastapi` on the path, purely because it imports
`pswamp_web.recorded_io` and that package's `__init__` pulls in the whole web
stack. Worth fixing in the tool rather than working around again.

### Measured values, for sanity-checking a change

Re-verified against the running container after the repo merge:

- median frequency **50.0009 Hz**; median voltage **418.6 kV**
- islanded stations **6500, 6700, 6701**; island groups summing to exactly **44**
  stations with no overlap
- time-window steady state **5.85 KB/s** (vs ~1.4 MB/s naive); phasors 16.3 KB/s @ 5 Hz
- the dashboard opens exactly **4** WebSockets and renders **135** `<line>`
  elements (79 map branches + 12 dial spokes + 44 phasor arrows)

---

## 2. The framing decisions, and whether they held

The whole port hangs off five decisions taken before any code. Four held.

| Decision | Rationale | Verdict |
|---|---|---|
| **Depend on the analysis core; don't vendor or rewrite it** | The plan was always to fold the layers together, so forking the core would be exactly backwards | **Held, and then strengthened** — the original branch modified p-SWAMP not at all. Zero algorithm code copied. |
| **Write the server as if it already lives at `src/pswamp/web/`** | The fold should be a move, not a second port | **Held.** Verified: nothing in `pswamp_web/` imports from the rest of the web backend. See §7. |
| **Keep the core's threads; bridge to the event loop** | Rewriting the core's execution model to suit a web server is the tail wagging the dog | **Held.** Two crossing points, no locks added, no execution code touched. |
| **Recorded replay, no broker** | Reproducible disturbance, one process, and the `io` seam means a broker drops back in unchanged | **Held** — and §10.2 is where that bet gets called in. |
| **Four pages, one per analytic app** | Mirrors the four server packages | **Wrong.** See §6. They are views of *one* timeline and belong on one screen. Cost: a full client refactor. |

---

## 3. The order that actually worked

Roughly the order used, with the corrections from §6 folded in.

1. **The replay layer first, standalone.** `recorded_io.py` + the counting
   window, proven with a *synthetic* recording driving the real `IslandingApp`.
   This is the highest-risk step: it establishes that a decoder you wrote
   satisfies `TimeWindowApp`'s undocumented expectations. Do it before anything
   else and before any web code exists.
2. **Generate the recording.** Everything downstream is unblocked by this, and
   the generator self-asserts that the scenario actually fires.
3. **Dependency + Docker wiring, with no features.** So a build failure can't be
   confused with a code failure. Assert the negative here: `import PySide6` must
   fail inside the container. **Add CORS in this step** (§4.8).
4. **The bridge, proven with the cheapest possible payload.** `Hub` + `Bus` +
   the status endpoint. When two applications report status at 1 Hz through the
   bus into a browser, the architecture is done and everything after is content.
5. **The static endpoint** (`GET /api/grid/model`) — first non-WebSocket route.
6. **The dashboard, with panels added one at a time** — measurements, then
   islanding + alarms, then phasors, then status. *Not* four pages (§6).
7. **Documentation**, then §10, then §7.

---

## 4. Landmines

The expensive part. Each cost real time; none was predictable from the docs.

### 4.1 uv refuses to normalise a path above its base directory
The path dependency resolved fine on the host and failed in the image with
*"cannot normalize a relative path beyond the base directory"* — uv does **not**
clamp at `/` the way a shell does. The plan had assumed `/app/../../` → `/`.

**Fix:** mirror the developer's directory *depth* in the image. The repo sits at
`/workspace/p-SWAMP`, so the server is at `/workspace/p-SWAMP/app/server-python`
and `../../` is correct in both places, with no image-only variant of the
manifest. This is why the image does not flatten the server to `/app`, and it is
the single largest piece of machinery that §9 would delete.

### 4.2 `uv export` needs the path dependency to exist, even when excluding it
`--no-emit-package p-swamp` only suppresses it from the *output*; uv still
generates the package's metadata while resolving. Copying the whole source tree
before the dependency install would destroy layer caching.

**Fix:** copy only the root `pyproject.toml` + `README.md` first (uv reads the
README because the manifest's `readme =` points at it — excluding it from the
build context fails the build); copy the source after the wheel install, then
`uv pip install --no-deps -e`.

### 4.3 `topsrt`'s interfacer deliberately drops samples
`InterfacerQueues.interface_fun` calls `output_stream.get_nowait()` before every
put — it is built to keep a *live* consumer on the newest data. With the sim
running flat out, a recorder attached this way loses most of the stream and
produces a gap-riddled recording that still looks plausible.

**Fix:** bypass the threaded interfacer. Register a plain synchronous function in
`rts.interface_functions[...]` that calls `publisher.update(read_input_signal(rts))`
inline. Result: exactly 3501 samples for 70 s @ 50 Hz, no drops.

### 4.4 The time-window pre-fill overflows the reader queue
`TimeWindowApp.__init__` calls `seek_relative_input_offset(-n_samples)` — a burst
of 500 frames into a queue sized for streaming. The drop-oldest backpressure
policy (correct for a lagging live consumer) silently discarded 60% of it and
logged a misleading "consumer is behind".

**Fix:** separate the two paths. `_offer()` for live frames (drop oldest);
`_push_history()` for the deliberate burst (grow the queue, never drop). The two
overflow conditions mean opposite things.

Related: start the application threads **before** the player, or the first frames
are published into queues nobody is draining yet.

### 4.5 `detect_islands` returns overlapping groups (core bug, compensated here)
`island_idx` was masked with `& ~assigned` but the returned `islands` list was
not, so a channel close to two references appeared in both groups. Consumers that
recover the main system by subtraction were left with far fewer stations than
were actually connected — "main = 3 stations" while the grid was intact.

**Compensated in `island_groups()` (`stores.py`)**, by claiming each channel for
the first group that reports it. Verified: 0 overlapping groups across 104
assessments, groups summing to exactly 44 stations. The real fix is a two-line
change in `detect_islands`; see §11.

This was only visible because the port *printed real island membership early*.
Print domain data as soon as the pipeline runs.

### 4.6 NaN is not JSON, and it is the normal case
`json.dumps` emits a bare `NaN`, which `JSON.parse` rejects. A `TimeWindow` is
all-NaN until it fills and the PMU decoder NaNs missing frequencies — so this
breaks the **first message of every page**, not some edge case.

**Fix:** `Sample = float | None`, a `series()` helper, and one `send_state()` that
every page uses (`model_dump_json`, never `send_json`).

### 4.7 Re-sending the window is ~1.4 MB/s per client
30 s × 50 Hz × 8 channels at 10 Hz. **Fix:** send the window once, then only new
samples (`mode: "full" | "append"`). 5.85 KB/s measured — a ~240× reduction. This
is the difference between a page that works and one that does not.

### 4.8 CORS is invisible until the first HTTP fetch
WebSockets are exempt from the same-origin policy, so every socket worked in dev
across origins. The moment a panel used `fetch` (grid topology, channel
catalogue) the browser silently discarded the response and the component sat
waiting for data that had arrived. Symptom: "Loading grid model…" forever, in dev
only.

**Fix:** `CORSMiddleware` in `server.py`. Add it when the dependency wiring goes
in, not after — it costs nothing and saves a confusing hour. (Nothing depends on
it today, since dev goes through the Vite proxy; see `AGENTS.md`.)

### 4.9 A 50 Hz stream must not go through React state
Even holding the samples in a ref, using `setState` merely to *signal* a redraw
runs a render pass 10×/s that produces no DOM change.

**Fix:** `useServerSocket`'s `onMessage` option bypasses state entirely; the hook
writes to a ref and notifies subscribers, and uPlot's `setData` is called outside
React's cycle. Small SVG panels (phasors, islanding) use plain state, which is
correct for them.

### 4.10 This repo's eslint is stricter than it looks
`reactHooks.configs.flat.recommended` turns on compiler-backed rules as
**errors**. Three that will bite:
- `react-refresh/only-export-components` (exempt only for `components/ui/**`) —
  a module exporting a provider component *and* its hook/context **fails the
  build**. Put contexts in a separate `.ts`.
- `react-hooks/set-state-in-effect` — no `setState` in an effect body.
- `react-hooks/refs` — no ref writes during render (update in an effect).

### 4.11 `NavLink to="/"` matches every route
react-router matches descendant paths unless `end` is passed. Once the index
page's path actually *is* `/`, the old `isIndex` workaround becomes dead code and
every nav link renders active.

### 4.12 `--virtual-time-budget` lies about the network
This one cost the most time and produced a **falsely reported regression**.
Headless Chrome's virtual time fast-forwards timers but **not real network I/O**,
so the last WebSocket initiated gets cut off mid-handshake — non-deterministically,
depending on machine load. Raising the budget does not help, which is exactly what
makes it look like a logic bug rather than a measurement artefact.

**Only measure real network behaviour with real wall-clock time:** launch the
browser detached, `sleep`, then inspect. See §5.

---

## 5. Verification recipes that actually work

**Socket count (the one that matters for the dashboard):** fresh container so
logs are clean, load the page with *real* time, count uvicorn's accept lines.

```bash
docker run -d --name verify -p 8124:8000 p-swamp:latest && sleep 14
nohup chromium --headless=new --no-sandbox --user-data-dir=/tmp/x \
  http://127.0.0.1:8124/ >/dev/null 2>&1 &
sleep 10
docker logs verify | grep -oE 'WebSocket /api/[a-z-]+/ws' | sort | uniq -c
# expect exactly 4 — one per endpoint, one islanding shared by two panels
```

Do **not** use `--dump-dom --virtual-time-budget` for this (§4.12). It is fine for
static DOM checks where nothing is awaited.

**Live DOM after real time** — launch chromium with `--remote-debugging-port`,
sleep, then drive CDP `Runtime.evaluate` over the debugger WebSocket. Counting
SVG elements is a good proxy: 135 `<line>` (§1). On an ARM Linux VM the snap
wrapper does not work; point at the inner binary
(`/snap/chromium/current/usr/lib/chromium-browser/chrome`).

**Bandwidth:** connect a raw websocket client, read the first message, then time a
10 s window of the rest.

**Backend import graph, without Docker:** from `app/server-python/`,
`uv run --frozen python -c "import sys; sys.path.insert(0,'src'); import server"`.
Catches a broken dependency in seconds rather than at the end of an image build.
The image runs the same check as a build step; keep both.

**Don't stash to get a test baseline** — a `git stash` mid-session nearly lost the
work. Use `git worktree add --detach /tmp/baseline HEAD` and run there.

**Know the pre-existing test failures before touching the core.**
`tests/monitoring/test_islanding.py` (IndexError), `test_mock_case.py`,
`test_kafka.py`/`test_mqtt.py` (need brokers) already fail on a clean checkout,
and the suite *hangs on exit* after passing (a non-daemon PMU listener thread), so
`| tail` never flushes. Write pytest output to a file. Some test modules also
import things that no longer exist (`pswamp.monitoring.voltage_stability` has no
such module — see §10.1), so a collection error is not necessarily your fault.

---

## 6. What I would do differently

**Build the dashboard first.** The single real mistake. Four routes were chosen to
mirror the four server packages, but the panels are views of *one* server-side
timeline — one `RecordingPlayer`, one measurement window, one detector. Worse, it
was a *less* faithful port than a dashboard: the Qt UI (`gui/main_window.py`) is
one `QMainWindow` with a central grid view and docks around it. The client/server
symmetry is real on the *server* (one package per analytic app) and false on the
client. Cost: a whole refactor, plus two bugs that only appeared once the panels
were adjacent (island colours disagreeing at index 0; `NavLink` matching
everything).

**Add CORS with the Docker wiring** (§4.8).

**Decide panel sizes against a real viewport early.** The dashboard is still
~1200 px tall; it was sized by arithmetic, not by looking.

**Expect one adapter per analytic app, and budget for it.** Result dicts carry
numpy arrays, `uuid.UUID` and `datetime` — never hand one to pydantic. This
repeats for every app added (N4SID will need complex eigenvalues split into
`{re, im}`).

**What went right and should be repeated:** proving the decoder against the real
`IslandingApp` with synthetic data *before* generating a real recording; a
self-asserting generator; the "no feature code" Docker step; and building the
cheapest possible endpoint (status) to prove the thread→loop bridge.

---

## 7. Consolidating the Python into one place

The repo holds one Python codebase split across two projects that already depend
on each other in one direction (web → desktop). Two moves would collapse that
split. **They point in opposite directions and are mutually exclusive.**

| | **Option A — everything under `src/`** | **Option B — everything under `app/server-python/`** |
|---|---|---|
| Move | `app/server-python/src/pswamp_web/` → `src/pswamp/web/` | root `src/pswamp/` → `app/server-python/src/pswamp/` |
| Result | One package, `pswamp`, with `gui/`, `visualization/` and `web/` as presentation adapters over one Qt-free core | One project under `app/`; the root becomes repo furniture |
| Manifests | Two, plus a `web` extra on the root one | One |
| The web client (`app/client-web/`) | Moves too, to a sibling of `src/` | Stays exactly where it is |
| Effort | Small for the server (a `git mv` + ~13 imports); the client raises a real question | Moderate, and gated on a lint decision (§9.3) |
| Deletes the path-dependency machinery (§9.1)? | No — it inverts it | **Yes, all of it** |
| Plan | §8 | §9 |

**What is true either way**, and worth protecting whichever is chosen: the
`pswamp_web/` package is self-contained. Verified — no module inside it imports
anything from the rest of the web backend (only stdlib, fastapi, pydantic, numpy
and `pswamp.*`), and every intra-package import is relative. That is what makes
both moves cheap, and it is the one invariant a change should not break.

### The recommendation: decide §10 first

**Do not pick between A and B on tidiness grounds. The answer follows from
whether the Qt front end is being retired.**

- **If Qt stays** (indefinitely, or as a supported second client), **Option A is
  right.** `src/pswamp/` is then genuinely shared infrastructure with two
  presentation adapters over it — `gui/` and `web/` as siblings — and that is
  exactly the shape the port was written for (§2). Putting the shared core inside
  one of its two consumers, as B does, would be backwards.
- **If Qt is being removed** (§10.5), **Option B is right, and it gets much
  cheaper the moment Qt is gone.** With `gui/` and `visualization/` deleted,
  "the core" and "the web backend's dependency" are the same thing, so there is
  no reason for it to live in a separate project — and 78% of the lint debt that
  makes §9.3 hard disappears with them (measured: 672 of 860 errors are in those
  two subpackages).

So the sequencing is: **§10 first, then §7.** Consolidating before the
Qt decision risks doing the move twice, and doing it in the wrong direction is
the more expensive of the two mistakes.

**If a decision is needed now and §10 is unresolved**, take the cheap variant in
§9.2: move the desktop project *whole* into `app/pswamp-desktop/`, keeping it a
separate project. It gets `app/` to mean "all the code" without committing to
either direction, and costs one extra `git mv` whichever way §10 goes.

---

## 8. Plan A: consolidate under `src/pswamp/`

Fold the web layer into the core package, so `pswamp` has `gui/`,
`visualization/` and `web/` as siblings. **Right if Qt stays** (§7).

### 8.1 The server move

```bash
git mv app/server-python/src/pswamp_web src/pswamp/web
```

Then fix **13 references in `server.py`** (`import pswamp_web.grid` →
`from pswamp.web import grid`, and the `SERVICES`/`APPS` entries). Nothing inside
the package changes: its imports are all relative.

Three pieces should *not* stay under `web/`, because they are not web concerns:

- `web/recorded_io.py` → `pswamp/streaming/recorded_io.py`, beside `kafka_io.py`
  and `mqtt_io.py` where it belongs. Independently valuable — broker-free replay
  of real datasets for tests and CI, no web server involved — so it can land
  first and on its own. The only import to fix is the one in `web/hub.py`.
- `web/data/n44_line_trip_50hz.npz` → `test_utils/sample_datasets/n44/recordings/`,
  read via `importlib.resources` rather than the current `Path(__file__).parent`
  (`grid_model.py` already does exactly this for `grid_database.db`; copy it).
- `tools/record_n44_dataset.py` → beside that dataset.

At that point the two compensations in §1 should be **deleted, not moved**:
`CountingTimeWindowLabeled` becomes `n_appended` + `snapshot()` on
`utils/time_window.py`, and the disjoint-group loop in `island_groups()` becomes
the one-expression fix in `detect_islands`. Both are in §11 anyway.

### 8.2 Dependencies

The root `pyproject.toml` gains a **`web` extra** (`fastapi`,
`uvicorn[standard]`), not base dependencies: the analysis core must stay
importable without a web stack, for the same reason `synchrophasor` is kept out
of the image (§1). `app/server-python/pyproject.toml` and its lockfile go away;
the server is then installed as `p-swamp[web]`.

**Check the image does not gain Qt.** With one manifest, `[full]` and `[web]` are
siblings and the Dockerfile must install only the latter. The existing
`import server` smoke test plus an explicit `import PySide6` *failure* assertion
is what keeps that honest — do not drop either.

### 8.3 The web client

`app/client-web/` becomes a sibling of `src/` (e.g. `web-client/`), whose
`vite build` output is copied to `pswamp/web/static/`. The Dockerfile already
does the two-stage build and moves as-is.

The genuinely open question — the one new thing the port adds to the toolchain —
is **whether the package wants npm in its build at all**. Three answers, pick
deliberately:

- **Yes**: the Dockerfile stays as it is and a source checkout needs node to
  build the UI. Simplest, and what happens today.
- **No, ship built assets**: commit `static/`, or publish it as a release
  artifact. Removes node from the build at the cost of a generated-code-in-git
  argument (`AGENTS.md` currently says `static/` is never committed).
- **No, publish the client separately**: two artifacts, two release cadences.

### 8.4 What this does not fix

The path-dependency machinery in §9.1 does not go away — it inverts. The server
still points at a project it lives beside, the image still needs both trees, and
`error_check.sh`'s `app/`-scoped gate now covers *less* of the Python than it
does today, because `pswamp_web/` leaves `app/`. That last point is a real
regression and needs the gate rescoped in the same change.

---

## 9. Plan B: consolidate under `app/server-python/`

Fold the core into the server project. **Right if Qt is being retired** (§7).
A plan, not a decision — nothing here has been done.

### 9.1 Why it is on the table

`AGENTS.md` used to say the two Python projects shared nothing: separate
manifests, separate locks, no imports either way, an image copying only
`app/server-python/`. The grid monitor ended that. Today the web backend declares
`p-swamp` as an editable path dependency on `../../`, `pswamp_web/` imports
`pswamp.*`, root `src/` is installed **into the shipped image**, and
`app/server-python/uv.lock` already resolves the desktop package's whole closure
(numpy, scipy, pandas, and the `[full]` Qt extra) alongside FastAPI's.

So the projects are already coupled. What remains of the separation is machinery
that exists *only* to span a gap that no longer needs spanning:

| Machinery | Exists because |
|---|---|
| `[tool.uv.sources] p-swamp = { path = "../../" }` | the packages are in different projects |
| The `/workspace/p-SWAMP` depth trick in the Dockerfile (§4.1) | uv won't normalise `../../` above its base dir |
| `--no-emit-package p-swamp` in `uv export` (§4.2) | the path dependency must not be fetched from an index |
| A second `COPY` + `uv pip install --no-deps -e` layer | root `src/` has to be installed separately |
| A second compose `watch` entry for root `src/` | ditto, for hot reload |
| `uv lock --upgrade-package p-swamp` as a required extra step | a plain `uv lock` won't re-read the path dependency |
| Two lockfiles, of which only one is read by any script | historical |

Consolidating deletes all of it. That is the case *for*. The case *against* is one
specific, quantified cost, in §9.3.

### 9.2 Target shape

```
app/server-python/
  pyproject.toml          # one manifest: fastapi + uvicorn + numpy/scipy/pandas…
  uv.lock                 # one lockfile
  .python-version
  src/
    server.py             # entrypoint, unchanged
    shared.py
    timeline/             # scaffold demo
    pmu_test_streamer/    # scaffold demo
    pswamp_web/           # the web layer  ── imports pswamp.*
    pswamp/               # ◄── moved here from root src/pswamp/
  tests/                  # ◄── moved here from root tests/
```

`examples/` is an open question — see §9.6.

`import pswamp` then resolves exactly the way `import timeline` already does: off
the working directory, because `WORKDIR` is `src/`. No path dependency, no
editable install, no depth trick.

**The cheap variant, and the fence-sitting option (§7).** Move the desktop project
*whole* — `src/`, `tests/`, `examples/`, `pyproject.toml`, `uv.lock` — into
`app/pswamp-desktop/`, keeping it a separate project and keeping the path
dependency (now `../pswamp-desktop`). This gets `app/` to mean "all the code" and
the root to mean "repo furniture", at a fraction of the risk: no manifest merge,
no lint-gate question, no packaging change. It deletes almost none of the
machinery above — but it commits to neither §7 option and costs one extra `git mv`
later.

### 9.3 The thing that actually decides this

**`scripts/error_check.sh` scopes every Python check by the literal directory
`app`:**

```sh
find app -name '*.py' -not -path '*/__pycache__/*' -print0   # py_compile
ruff check --select E,F app
ruff format --check app
```

So the move does not merely relocate files — it silently drags the desktop code
under the quality gate. Measured on the tree as it stands:

| Check | Result on root `src/` |
|---|---|
| `py_compile` | **passes** (verified — no action needed) |
| `ruff check --select E,F` | **860 errors** |
| `ruff format --check` | **119 of 149 files would be reformatted** |

Where the 860 live — and this is the number that ties §9 to §10:

| Subpackage | Files | Errors | |
|---|---|---|---|
| `gui/` | 52 | 477 | **Qt front end** |
| `visualization/` | 30 | 195 | **Qt front end** |
| `test_utils/` | 26 | 79 | |
| `utils/` | 11 | 33 | |
| `monitoring/` | 6 | 22 | the analysis core proper |
| `models/` | 5 | 20 | |
| `streaming/` | 7 | 12 | |
| `coordination/` | 3 | 11 | |
| `app_templates/` | 4 | 7 | |
| `styles/`, `database/` | 4 | 3 | |

**672 of 860 errors — 78% — are in the two subpackages §10.5 deletes.** Retiring
Qt reduces this problem to 188 errors across 67 files, which is a weekend rather
than a project.

By rule:

| Count | Rule | |
|---|---|---|
| 508 | `E501` line-too-long | cosmetic; `ruff format` does not fix long strings/comments |
| 232 | `F401` unused-import | auto-fixable |
| 38 | `F841` unused-variable | auto-fixable |
| 27 + 22 | `F405`/`F403` star-import | needs real edits |
| 14 | `F811` redefined-while-unused | worth reading individually |
| 1 | **`F821` undefined-name** | **a genuine latent bug** — see below |
| 18 | assorted `E` | trivial |

That one `F821` is the argument that this exercise has value beyond tidiness:

```
src/pswamp/visualization/components/threshold_adjust.py:32
    self.range_slider = QRangeSlider(Qt.Horizontal)
                        ^^^^^^^^^^^^ undefined name
```

`QRangeSlider` is never imported in that module. `qtrangeslider` is a declared
`[full]` dependency, so this is a missing import, and the line raises `NameError`
whenever that widget is constructed. Nothing under `app/` reaches it, so the web
stack is unaffected — but it means the desktop code has never been run through
even the loosest static check.

**Three ways to handle it. Pick one before starting.**

1. **Fix everything first, move second.** Cleanest end state; worst diff. A
   `ruff format` pass over 119 files destroys `git blame` on research code and
   will conflict with anything in flight. Not recommended as a precondition — it
   turns a mechanical move into an open-ended cleanup, and 78% of the work is on
   code that may be deleted anyway.
2. **Exclude the moved tree, keep the gate exactly as strict as it is today.**
   One flag, in one place:
   ```sh
   ruff check  --select E,F app --exclude app/server-python/src/pswamp
   ruff format --check      app --exclude app/server-python/src/pswamp
   ```
   This is honest: root `src/` is ungated *right now*, and this preserves that
   rather than pretending otherwise. It does not drift the way an explicit
   include-list would. `py_compile` keeps covering everything, free, because it
   already passes.

   Verified that this works: `--exclude` applies to a subdirectory encountered
   while walking a passed directory, which is exactly this case. Note it does
   *not* apply to a path named explicitly on the command line — that needs
   `--force-exclude` — so don't restructure the invocation to pass the package
   directly and expect the exclude to hold.
3. **Exclude, then ratchet.** As (2), but the exclude is a list that shrinks:
   clean one subpackage, drop it from the exclude, and it can never regress. The
   natural first targets are the ones the web stack already depends on —
   `monitoring/` (22), `utils/` (33), `streaming/` (12) — because a regression
   there breaks the grid monitor, and together they are 67 errors, not 860.

**Recommendation: (3), starting as (2).** Do the move with a blanket exclude so
the move itself is reviewable as a pure rename, then ratchet in separate commits.
Fix the `F821` on its own, immediately, as a bug rather than as lint.

### 9.4 Steps

Each step should leave `./scripts/error_check.sh` green and the container
building.

**1. The `F821`, separately and first.** One-line import fix in
`threshold_adjust.py`. Independent of everything else, and better reviewed as a
bug fix than buried in a rename.

**2. Merge the manifests.** `app/server-python/pyproject.toml` absorbs the root
project's dependencies:

```toml
[project]
name = "p-swamp"                 # take the root project's identity
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.115.6,<0.116",
    "uvicorn[standard]>=0.34,<0.35",
    # from the root manifest:
    "numpy>=2.2.5", "scipy", "tomli", "pandas>=2.3.1",
    "numpy-dynamic-array", "pydantic>=2.12.5",
    "synchrophasor @ git+https://github.com/hallvar-h/pypmu",
]

[project.optional-dependencies]
full = [ ... ]                   # Qt/Kafka/tops-rt, verbatim from root
```

**The `full` extra must stay an extra.** It is the only thing keeping PySide6 out
of the image; the Dockerfile installs with no extras and its `import server`
smoke test is what proves it. Keep the smoke test. (If §10.5 has already
happened, most of `full` is deletable instead.)

Two flags come off the `uv export` line: `--no-emit-package p-swamp` (no longer a
separate package) and, depending on step 3, `--no-emit-project`.
`--no-emit-package synchrophasor` **stays**, unless §10.2 has restored the live
PMU path, in which case it must go.

**3. Decide packaging — `[tool.uv] package = false` stops being harmless.**
Today `app/server-python` is deliberately non-packaged. If `pswamp` moves in under
that flag, **`pswamp` stops being installed anywhere**. Consequences:

- The server is fine — `WORKDIR` is `src/`, so `import pswamp` resolves off the
  working directory exactly like `import timeline`.
- **`tests/` breaks.** It imports `pswamp.*` and currently relies on the root
  project's editable install; there is no `conftest.py` doing path setup.
- **`pip install -e .[full]`, the install command in `README.md`, breaks.**

So the project must become packaged, with the module root pointed at the moved
package:

```toml
[build-system]
requires = ["uv_build"]
build-backend = "uv_build"

[tool.uv.build-backend]
module-name = "pswamp"
# module-root defaults to "src"
```

This is a genuine hybrid worth documenting in `AGENTS.md`: `src/` becomes *both* a
packaging layout (for `pswamp`) *and* a plain source folder (for `server.py`,
`timeline/`, `pmu_test_streamer/`, `pswamp_web/`, which the wheel does not contain
and which resolve off the working directory). It works, but it reverses a
convention that file currently states flatly, so it has to be written down or the
next reader will "fix" it.

The image still need not install the project: `--no-emit-project` can stay and
`pswamp` resolves off `WORKDIR`.

**4. The move.**

```sh
git mv src/pswamp app/server-python/src/pswamp
git mv tests      app/server-python/tests
git rm  uv.lock                       # nothing reads it
# root pyproject.toml: delete, or reduce to repo-level tool config only
(cd app/server-python && uv lock)
```

Use `git mv` so `git log --follow` still works. Nothing inside the package needs
editing: every intra-package import is already absolute (`from pswamp.x import
y`), and the data files resolve two ways that both survive a move — the
shapefiles and `default_config.toml` via `Path(__file__).parent`, and
`grid_database.db` via `importlib.resources.files("pswamp.test_utils…")` from
`pswamp_web/grid_model.py`, which needs `pswamp` importable and nothing more.
Verified.

**5. Unwind the machinery.** Delete, in this order, checking the build after each:

- `[tool.uv.sources]` and the `p-swamp` dependency entry.
- The Dockerfile's `COPY pyproject.toml README.md ${REPO_DIR}/` layer, the
  `COPY src/ ${REPO_DIR}/src/` layer and its `uv pip install --no-deps -e` — all
  three collapse into the existing `COPY app/server-python/src/ ./src/`.
- The `REPO_DIR`/`SERVER_DIR` ARGs and the workspace-depth comment. `WORKDIR`
  goes back to `/app` and `/app/src`.
- The root-`src/` entry in `docker-compose.yml`'s `watch:` list.
- `.dockerignore`'s repo-root exclusion block, and the note that root
  `pyproject.toml` + `README.md` must not be excluded.
- CI path filters: `src/**` and root `pyproject.toml` come back out, leaving
  `app/**`; the build context can even narrow again.

Each of these has an explanatory comment naming the reason it exists; delete the
comment with the code, and grep for `workspace`, `path dependency` and
`--no-emit-package` afterwards to catch prose that outlived it.

**6. Update the gate.** Add the ruff excludes from §9.3, with a comment stating
plainly that the excluded tree is **not** checked, and why.

**7. Update the docs.**

- `AGENTS.md` — "Two Python projects in one repo" becomes a description of one
  project with two dependency tiers. The `src/` ambiguity note goes away, which is
  a real readability win. The image-mirrors-the-repo-root note reverts. The
  packaging hybrid from step 3 gets written down.
- `README.md` — the `pip install -e .[full]` line now runs from
  `app/server-python/`.
- This file — §8 becomes unreachable in its current form; restate it as
  "`pswamp_web/` → `src/pswamp/web/` *within* `app/server-python/src/`", which
  after the move is a short `git mv` and about a dozen import lines.
- `doc/client-server-rig.md` — check for stale layout claims.

### 9.5 What this plan does not touch

The wire protocol, the client, the k8s manifests, and the runtime behaviour of
either half. If any of those change, something has gone wrong: consolidation
should be observable only as a shorter Dockerfile and a smaller `AGENTS.md`.

### 9.6 Decisions still to make

- **Where `examples/` goes.** 13 MB of simulation cases, imported by nothing in
  `app/`, excluded from the image. Moving it under `app/server-python/` puts
  research material inside a deployable's directory for no benefit; leaving it at
  the root leaves the repo half-consolidated. A third option is to keep it at the
  root as *documentation* for the whole repo, which is arguably what it already
  is. No strong argument either way — decide it explicitly rather than by
  accident.
- **Whether the repo keeps a root `pyproject.toml` at all.** After the move it has
  no dependencies and builds nothing. It could still usefully hold repo-level tool
  config, or be deleted outright.
- **Wheel packaging of data files is still untested**, and this is true of Option A
  too. Every install so far has been editable, so nothing has ever verified that
  `uv_build` includes `grid_database.db`, the shapefiles, `sld.dxf` and
  `default_config.toml` as package data — nor `n44_line_trip_50hz.npz`. Build a
  real wheel and load one of them from it. Do this *before* the move if the answer
  would change the packaging decision in step 3.
- **Whether `tests/` should run in CI once it is under `app/`.** It is not run
  anywhere today. Moving it does not change that, but it makes the omission more
  conspicuous — and 4 of the 17 test modules are known-failing or need brokers
  (§5), so switching them on is its own piece of work.

---

## 10. Completing the port, and retiring the Qt front end

What exists today is a **vertical slice**: one data source, one analytic app, four
panels. It proves the architecture. It does not yet replace `gui/main_window.py`.

### 10.1 The gap, feature by feature

The Qt app is one `QMainWindow` with a grid view in the centre and docks around
it: Apps (launcher), Frequency, Status, Alarms, Alarm details. Against that:

| Qt | Web today | Gap |
|---|---|---|
| Grid view, 2D geo (`geo_plot_2d`, layers) | `IslandMap` — SVG nodes/edges | Partial. No layers, no heatmap, no interaction beyond selection |
| Grid view, 3D (`dim_3d`, `surface_plot`, deformation) | — | **Missing.** Needs WebGL |
| Single-line diagram (`single_line_diagram`, DXF) | — | **Missing.** Needs a server-side DXF→GeoJSON step |
| Frequency plot (`FreqPlot`) | Live Measurements chart | Done |
| App status (`AppStatusMonitoringWidget`) | `AppStatusPanel` | Done |
| Alarm overview (`AlarmOverview`) | `AlarmsPanel` + `AlarmTable` | **Done.** Click a row for the detail pane |
| Alarm details (`AlarmHandlingDialogue`) | `AlarmDetails` | **Done.** Info, event log, acknowledge/annotate/silence. The embedded per-app view is deliberately not repeated — it is already a panel on the same screen |
| Alarm view: islanding | `IslandMapPanel` | Done |
| Alarm view: oscillations | — | **Missing.** Needs `n4sid`/`fft` wired into the hub |
| Alarm view: voltage stability | — | **Missing**, and see below |
| Phasor plot 2D (`phasor_plot`) | `PhasorDial` | Done |
| Phasor plot 3D (`phasor_plot_3d`) | — | Missing |
| Frequency heatmap (grid-view layer + launcher) | — | Missing. **This is the live one** |
| Voltage heatmap | — | **Dead in Qt** — the launcher button is commented out. Do not port |
| Channel select/tree | `ChannelPicker` | Done |
| Line outage detection | `LineOutagePanel` | **Done**, once the recording gained current channels — see below |
| App launcher (start/stop apps) | — | **Missing.** Currently the hub decides |
| Multi-TSO alarm docks (`other_tso`) | — | Missing |

**`IslandingApp` and `LineOutageDetectionApp` are wired into the hub.**
`monitoring/` also holds `n4sid` and `fft`, neither reachable from the web yet.
Each is "one adapter plus one panel" (§6) — the shape is established, the work is
repetitive rather than hard. Two notes for whoever does them:

- **`N4SIDApp` slots straight in.** Its `__init__` is plain `TimeWindowApp` with
  no broker coupling, and `run_analysis` already returns eigenvalues and mode
  shapes. The cost is the dependency: `nfoursid` pulls **matplotlib** into the
  headless image.
- **`FFTOnline` does not.** Its `__init__` calls `get_last_message_from_topic`
  and `consumer_seek_relative_offset` — Kafka-only, before any window exists. The
  algorithm itself (`calculate_fft_spectrum`) is a pure function and is the part
  worth reusing.

**Only port what the Qt app actually runs.** Several things that exist in the
tree are not reachable from `main_window.py`: the voltage heatmap button is
commented out in `app_launcher.py`, and so are the SSI, Prony and "Time window
plot V1.5" buttons. `monitoring/fft_v2.py` is dead (§11). Check
`gui/main_window.py` and `gui/app_launcher.py` before assuming a widget is live.

Two traps in that table:

- **Voltage stability is half-missing from the tree.** The *presentation* half
  exists — `gui/alarms/views/voltage_stability.py`,
  `gui/grid_view/dim_3d/layers/voltage_stability.py`, and a
  `visualization/voltage_stability_viz/` containing one `dSdZ_plot.py`. The
  *analysis* half does not: tests import `pswamp.monitoring.voltage_stability`
  and `pswamp.monitoring.voltage_stability_indicators.corsi_taranto`, and an
  example imports `pswamp.visualization.voltage_stability_old` — **none of those
  three modules is in the tree.** So this is not "port a view", it is "find or
  rewrite the analysis first". Scope that discovery before promising the feature.
- **`monitoring/fft_v2.py` is dead** (§11) — port `fft.py`, not it.
- **Line outage detection was blocked on data, not code**, and is now unblocked:
  it reads `i_Magnitude`, which the original recording did not carry. See §1.

### 10.2 The blocker that is not a feature: live data

The web stack replays a committed recording. The Qt app reads **live** data
through `pswamp/streaming/` (`kafka_io`, `mqtt_io`, `nqkafka_io`,
`time_series_io`). Nothing can replace the Qt front end operationally until the
web server does too.

**The good news is that this was designed for.** `hub.py` passes
`io=self.player.subscribe(...)` into each application — a constructor argument.
Swapping in `kafka_io` is a wiring change at that one line, which is precisely
the bet §2 recorded. Do not let it become a rewrite.

**The bad news is the consequences around it**, none of which the replay path
exercises:

- **`synchrophasor` comes back into the image** (§1), bringing git and a hashless
  VCS pin with it. Decide whether to vendor, mirror, or pin it properly.
- **The replay controls stop making sense.** Play/stop/seek are meaningful for a
  recording and meaningless for a live feed. Either make the source a *mode*
  (live | replay) with the controls conditional, or split the deployments. Do not
  leave dead buttons on an operational screen.
- **Reconnect, gaps and backfill become real.** A recording never disconnects.
  A broker does, and `TimeWindowApp`'s queue behaviour under a reconnect burst is
  exactly the §4.4 landmine again.
- **`replicas: 1` stops being acceptable.** State is in-memory and per client
  (`AGENTS.md`), which is fine for a demo and not for something operators depend
  on — and the per-client pipeline makes it sharper, since a browser's five
  sockets landing on different pods would be five unrelated replays. Horizontal scaling needs an external live store — a real design change, not
  a manifest tweak. This is the single largest piece of unscoped work in this
  document.
- **Auth and CORS.** Both are wide open because every endpoint is a read of
  sample data. The moment it is real grid data, neither is acceptable.

**Keep the replay path when live lands.** It is the reproducible disturbance that
makes every panel testable and every one of §1's measured numbers meaningful.
It should become a mode, not a phase.

### 10.3 Suggested order

1. **Alarm details pane**, completing the alarm surface. Smallest useful step,
   and it uses machinery that already exists.
2. **One more analytic app end to end** — `line_outage_detection` or `fft`,
   whichever has the simpler result shape. This is what proves "one adapter plus
   one panel" is really the cost, before committing to the rest.
3. **The live `io` source** (§10.2), behind a mode flag, with replay retained.
   Do this before the expensive visualisations: it is the thing that decides
   whether any of this can be operational, and it is better to learn that early.
4. **The remaining analytic apps** — `n4sid`/oscillations, and voltage stability
   once its analysis half is located.
5. **The expensive visualisations** — single-line diagram (DXF→GeoJSON server
   side), then the 3D grid view (WebGL). `IslandMap` is deliberately behind a
   small nodes/edges/project seam so it can be swapped rather than extended.
6. **The operational work** — auth, and the external live store that lets
   `replicas` exceed 1.

Steps 1–2 are worth doing regardless of whether Qt is ever retired. Step 3 is the
decision point.

### 10.4 What has to be true before Qt can be removed

A checklist, not a plan. Nothing below is currently true.

- [ ] Every panel an operator actually uses has a web equivalent — confirmed with
      operators, not inferred from the widget tree in §10.1.
- [ ] The web server reads live data (§10.2), with reconnect behaviour tested
      against a real broker.
- [ ] The deployment is credible: auth, and either a defensible `replicas: 1` or
      the external store that removes it.
- [ ] The single-line diagram and 3D view are either ported or explicitly
      dropped, with that decision recorded.
- [ ] `examples/` and `tests/` no longer depend on `gui/` or `visualization/`,
      or those dependencies are moved. Measured today: **4 test modules and 18
      example files** import them, and most of `examples/nordic44_rtsim/apps/`
      *is* Qt front-end code. This is the item most likely to be underestimated —
      and it bears on §9.6's "where does `examples/` go" question, since a large
      part of `examples/` would be deleted alongside `gui/` rather than moved.
- [ ] Anything else importing `pswamp.gui` / `pswamp.visualization` is accounted
      for. Verified clean today: `monitoring/`, `coordination/` and
      `app_templates/` import neither, so the analysis core does not depend on
      the UI. Re-check before deleting.

### 10.5 Removing it, and what that buys

Once the checklist holds, the removal is mechanical:

```bash
git rm -r src/pswamp/gui src/pswamp/visualization
```

plus dropping `PySide6`, `pyqtgraph`, `pyopengl`, `qtrangeslider`, `ezdxf` and
`matplotlib` from the `full` extra — at which point `full` may not be worth
keeping as an extra at all, and `kafka-python`/`nqkafka`/`tops-rt` can be
reconsidered on their own merits.

What it buys, beyond deleting the maintenance burden of a UI nobody runs:

- **78% of the lint debt disappears** — 672 of 860 errors, 82 of 149 files
  (§9.3). This is what makes Plan B's hard decision easy.
- **The `[full]` install stops needing Qt**, so a headless checkout is the normal
  case rather than the special one.
- **`src/pswamp/` becomes unambiguously "the analysis core"**, which is exactly
  the condition under which §7 resolves to Option B.

**Do not delete it early.** The Qt app is currently the only thing that exercises
several analysis paths at all, and §10.1 was derived by reading it. It is the
specification for the remaining work; keep it until that work is done.

---

## 11. Changes to the analysis core deliberately not made

These are real gaps that the web layer works around locally. Each is small,
defensible on its own merits, and best proposed as its own change rather than
buried inside a port or a move.

- **`detect_islands` returns overlapping groups** (§4.5). Two lines. The most
  clearly a bug.
- **`TimeWindow` has no monotonic append counter.** ~26 additive lines
  (`n_appended` + `snapshot()`); nothing existing changes behaviour. Enables
  incremental updates — a delta over a socket here, an incremental Qt repaint
  later. Deletes `CountingTimeWindowLabeled`.
- **`QRangeSlider` is undefined** in `visualization/components/threshold_adjust.py`
  (§9.3). A missing import; raises `NameError` when the widget is constructed.
- **`synchrophasor` is a base dependency but only the live-PMU and playback paths
  import it.** Moving it to the `full` extra removes git, network and a hashless
  VCS pin from any headless install. (Worked around by excluding it from the
  image; see §1 — and revisit under §10.2.)
- **`LineOutageDetectionApp` never reports a status.** Its `set_status()` is
  `pass` with the whole body commented out, so the app keeps `SnapshotApp`'s
  initial `'Undefined'` for its entire life while every other application moves
  through `OK`/`Alert`/`Emergency`. Verified over 30 s: `IslandingApp` shows
  `['Emergency', 'OK']`, this one shows `['Undefined']`.

  The commented-out body is a copy-paste from the islanding app — it tests
  `return_value['result']['islands']`, a key line outage results do not contain —
  so it was evidently stubbed and never finished, not disabled deliberately.

  **Deliberately not implemented here.** Deciding when an outage is `Alert`
  rather than `Emergency` is a domain judgement (how many branches? which
  voltage level? radial or meshed?), and that belongs upstream, not in a
  presentation port. The Qt status dock shows exactly the same `Undefined` for
  this app, so the web client is being faithful rather than broken.
- **`monitoring/fft_v2.py` is dead** (no importers, superseded by `fft.py`, says
  so in its own TODO) and **two test modules carry a stray
  `from ensurepip import bootstrap`** that breaks collection. Unrelated to this
  work; noticed in passing.
- **Three voltage-stability modules are imported but absent** (§10.1):
  `monitoring.voltage_stability`, `monitoring.voltage_stability_indicators.corsi_taranto`
  and `visualization.voltage_stability_old`. Either they were lost or the callers
  were written against a plan. Resolve before the voltage-stability port is
  scoped — and note this makes the corresponding tests and example unrunnable
  today, independently of any of this work.

Further out:

- Split `coordination.AlarmMonitor` into a pure state machine + a Kafka feeder.
  `pswamp_web/stores.py` reimplements ~30 lines of it because the logic is trapped
  inside a `for msg in consumer:` loop. Shapes match; merging is mechanical — and
  §10.2 makes it necessary rather than nice.
- Make `AlarmHandler` emit JSON-native types (`str(uuid)`, epoch float) instead of
  `uuid.UUID`/`datetime`. This is the root cause of `streaming/utils.encoder`
  pickling, and fixing it removes an adapter. It is a wire-format change for
  existing Kafka consumers — do it deliberately.
- Move `scipy` to an `[analysis]` extra (−119 MB of image).

---

## 12. Not done

- **Everything in §10** — the port is a vertical slice, not a replacement.
- **Either move in §7.**
- **Wheel packaging of the data files** (§9.6).
- **A real look at the dashboard on a laptop viewport** (§6).
- **The desktop test suite has never run in CI**, here or before the merge.
