# AGENTS.md

The single source of truth for coding agents working in this repository —
regardless of model or harness. `CLAUDE.md` and `.github/copilot-instructions.md`
are thin pointers to this file; put new guidance **here**, never in those.

## What this is

The p-SWAMP repository. It holds **two implementations side by side**:

- the original **Python + Qt single-process application** — the research/desktop
  code at the repo root (`src/pswamp/`, `examples/`, `tests/`, and the root
  `pyproject.toml` + `uv.lock`), and
- the **client-server stack** — a FastAPI server plus a React web client under
  `app/`, with its dev tooling and deploy path (`Dockerfile`,
  `docker-compose.yml`, `k8s/`, `scripts/`, `.github/`, `doc/`).

The second started life as a separate proof-of-concept repo for working out the
client-server shape of P-SWAMP — tech stack, repo structure, local dev
experience, deploy path — and has since been **merged back into this repo**. It is
no longer a PoC of its own: it is the web half of this project, living *alongside*
the Qt implementation rather than replacing it. `doc/client-server-rig.md` holds
the driving goals and constraints behind it; `README.md` is the desktop package's.

The two are no longer disconnected. The web stack's **grid monitor** is a front
end over the desktop package's analysis core: `app/server-python/src/pswamp_web/`
imports `pswamp.*` and the web backend declares the root package as an editable
path dependency. See "Two Python projects in one repo" for what that does and
does not change, and "The p-SWAMP web layer" for the code itself.

**Everything below is about the client-server stack**, except where it says
otherwise. The desktop package at the repo root has its own manifest and is
covered by neither these conventions nor `error_check.sh` — see the next section.

The web stack ships three things. The **grid monitor** at `/` is the real one: a
dashboard of panels — measurements, islanding, alarms, phasors, application
status — over a recorded Nordic 44 PMU stream replayed through p-SWAMP's own
monitoring applications. Beside it sit two deliberately trivial scaffold demos, a
PMU record streamer and a scrolling-number timeline with playback controls (back /
play / stop / forward); they predate the monitor, exist to keep the "adding a
page" path honest, and are what `scripts/generate-new-subapp.sh` clones.

**The client-server stack is stateless on purpose.** There is no database and no
persistent volume anywhere under `app/` or `k8s/`. Don't reintroduce one without
an explicit ask. (The Nordic 44 grid model *is* a sqlite file, and the replayed
PMU stream *is* a committed `.npz` — but both are read-only sample data the
server opens, not storage it writes to.)

## Two Python projects in one repo

The root `pyproject.toml` + `uv.lock` belong to the **desktop `p-swamp` package**
(root `src/pswamp/`, imported as `pswamp`; PySide6, pyqtgraph, Kafka). The web
backend keeps its own `app/server-python/pyproject.toml` + `uv.lock`, and the two
still resolve **separately** — which is the point, since Qt + Kafka and FastAPI +
uvicorn have no business being solved as one dependency problem. Don't hoist
either manifest to the other's level.

They are not, however, independent. **The dependency runs one way, web →
desktop:**

```toml
# app/server-python/pyproject.toml
dependencies = ["fastapi…", "uvicorn…", "p-swamp"]

[tool.uv.sources]
p-swamp = { path = "../../", editable = true }   # the repo root
```

That single stanza is the whole seam. `pswamp_web/` imports `pswamp.*`; nothing
under root `src/pswamp/` imports anything from `app/`, and nothing may start.
Editable, so an edit to root `src/pswamp/` is live in the server with no
reinstall — locally through the venv, and in the container through the compose
watch that syncs root `src/` into it.

Consequences worth knowing before touching anything:

- **`src/` is ambiguous — always qualify it.** Root `src/` is the desktop package;
  `app/server-python/src/` is the server. This file always means the latter unless
  it says otherwise.
- **Root `src/` is now IN the image**, installed editable at
  `/workspace/p-SWAMP/src`. It follows that CI's path filters include `src/**` and
  the root `pyproject.toml`, and that `.dockerignore` has to keep `build/` (~1.6
  GB), `examples/` and `tests/` out of the build context by hand. It also follows
  that the image mirrors the *repo root*, not just the server dir — see the
  workspace note at the top of the Dockerfile's runtime stage for why the depth is
  required rather than a matter of taste.
- **`./scripts/error_check.sh` still only looks at `app/`.** ruff, `tsc` and the
  lockfile check are scoped there. A change under root `src/` passes no gate even
  though it now ships — widening it means first deciding what to do about the
  existing desktop code's lint state, which is a real piece of work and not a
  one-line scope change.
- **The web backend takes p-swamp with no extras.** `[full]` is what carries
  PySide6, pyqtgraph, kafka-python, nqkafka and tops-rt; none of that belongs in a
  headless server image. `synchrophasor` is a *base* root dependency but is
  excluded from the image too (`--no-emit-package` in the Dockerfile): only the
  live-PMU and playback paths import it, and it is the one dependency fetched from
  git rather than an index. The Dockerfile's `import server` smoke test is what
  makes both exclusions checked decisions rather than hopeful ones.
- **A dependency for the web backend goes in `app/server-python/pyproject.toml`**,
  never the root one — and vice versa. After editing either, re-lock **both**:
  `(cd app/server-python && uv lock --upgrade-package p-swamp)` is what refreshes
  the web backend's view of the root manifest. A plain `uv lock` will report
  "Resolved N packages" without re-reading the path dependency.

## Architecture

Two deployables, one wire protocol:

- **`app/server-python/`** — the authoritative state server. The code lives in
  `src/` (mirroring the web client's layout), with the manifests beside it.
  **`src/server.py` is the one entrypoint and holds no domain logic**: it mounts
  each app package's router under `/api/<app>`, composes their lifespans,
  serves `/healthz`, and serves the web client from `static/`. Each backend api
  is a package under `src/<app>/` exposing `router` + optional `lifespan`
  (the FastAPI mirror of the client's `src/pages/`). The real one is
  **`src/pswamp_web/`** — see "The p-SWAMP web layer" below; it is a package of
  packages, contributing five entries to `APPS` plus a process-wide service.
  Beside it sit the two scaffold demos:
  **`src/timeline/`** and **`src/pmu_test_streamer/`**, each with `api.py` (WS
  endpoint, per-client state, ticker, logging) and `model.py` (the pure domain
  model), which `api.py` imports **relatively** (`from .model import ...`).
  The streamer's `model.py` also owns its `sample_data.txt` (read once at import,
  one record per line): a **one-off sample committed for testing** — 300
  *simulated* PMU records extracted by hand from the Nordic 44 simulation that now
  lives in this same repo under `examples/nordic44_rtsim/` (voltage phasor +
  measured frequency, five stations at 20 Hz, spanning a line trip). Sharing a
  repo with that simulation buys the streamer nothing: it is still a static
  fixture and nothing generates it. (The *grid monitor* is the one that really
  runs the desktop package's code; the streamer does not, and should not start.)
  Don't add tooling or deps to regenerate it unless asked;
  replacing it is a file swap, since no code parses the contents. **`src/shared.py`** holds the
  domain-free helpers both packages use — `ConnectionManager` and
  `make_logger(name)`; it is *not* an app package and never appears in `APPS`.
  Note the spelling split: a package dir must be a Python identifier
  (`pmu_test_streamer`) while its URL prefix is hyphenated to match the page route
  (`/api/pmu-test-streamer`). The image mirrors the **repo root** —
  `<repo>/` → `/workspace/p-SWAMP`, so this directory lands at
  `/workspace/p-SWAMP/app/server-python` with `WORKDIR …/src`, and `server.py`,
  `server:app` and `import timeline` all resolve off the working directory. The
  depth is not cosmetic: it is what makes `../../` in `[tool.uv.sources]` mean the
  same thing on a laptop and in the image (uv refuses to normalise a relative path
  above its base directory). Everything under `src/` shares the one
  `pyproject.toml` + `uv.lock` one level up.
- **`app/client-web/`** — a thin React/TS/Vite renderer (shadcn/ui + Tailwind v4).
  Holds no state; sends commands, renders whatever the server pushes. In the
  shipped image it is **baked into the server image** and served from the same
  origin as `/api` (no second service, and no cross-origin requests). It is a
  **multi-page SPA**
  (react-router, declarative `<Routes>`): **one folder per app** at
  `src/pages/<app>/`, holding that app's components, its socket hooks and its
  views together — the client-side mirror of the server's `src/<app>/` packages,
  imported relatively within the folder. Most apps own a single route;
  **`grid-monitor/` owns five**. The route table is `src/App.tsx` and the
  nav + centering shell is `src/components/AppLayout.tsx`. Only genuinely
  cross-app code sits outside an app folder: shared UI in `src/components/`
  (`ui/` is vendored shadcn), shared logic in `src/hooks/` (`useServerSocket`) and
  `src/lib/` — `servers.ts` (each app's ws path + the serving-origin url),
  `basePath.ts` (the runtime-discovered mount prefix) and `utils.ts` (shadcn's `cn`).
  Note `src/lib/` was invisible to git until recently; see the `.gitignore` note
  under "Conventions".
  **`src/pages/grid-monitor/` is the p-SWAMP application**: `/` renders
  `GridMonitorPage`, a grid of panels over the four p-SWAMP sockets, and
  `/time-window`, `/phasors`, `/islanding`, `/app-status` render the *same* panel
  components full-size (`variant="focused"`) rather than copies of them. Its
  subfolders are named after those routes, so the client↔server mirror holds one
  level down: `grid-monitor/time-window/` ↔ `/api/time-window` ↔
  `pswamp_web/time_window/`. `/pmu-test-streamer` (`PmuTestStreamerPage`) and
  `/timeline` (`TimelinePage`) remain standalone scaffold demos — thin players
  over a WebSocket, where the connection half lives once in
  `src/hooks/useServerSocket.ts` and each app adds only its snake_case →
  camelCase mapping (`useTimelineSocket`, `usePmuStreamSocket`). See "Adding a
  p-SWAMP view" and "Adding a page" below.

Key invariants to preserve:

- **State is per-client and in-memory only.** Each client generates a random
  integer seed sent as `?client_id=` on the WebSocket URL; the server keeps one
  `TimelineModel` + play flag per seed in `states: dict[int, ClientState]` (never
  evicted — a bounded, acceptable leak here). That dict is the only store:
  nothing is persisted, so a process/pod restart resets every client to the start
  of the timeline. That reset is expected behavior, not a bug.
- **…but the grid data is process-wide.** The `pswamp_web/` pages invert the
  emphasis: there is one PMU replay, one measurement window and one islanding
  detector for the whole process, because there is one grid — two operators
  seeing different frequencies would be a bug, not a feature. `HUB` (in
  `pswamp_web/hub.py`) owns that shared, read-only data; each page package still
  keeps a `states: dict[int, ClientState]`, but what lives in it is *view* state
  (selected channels, last sequence sent). The rule is: **state is per client,
  data is process-wide.**
- **One event loop for all I/O, and exactly two thread seams.** The WS handlers
  and ticker tasks are cooperatively scheduled — never truly parallel — so nothing
  they touch needs a lock. The one exception is `pswamp_web/`: the p-SWAMP
  monitoring applications run in daemon threads exactly as they do in the desktop
  package, because rewriting their execution model to suit a web server would be
  the tail wagging the dog. Those threads cross into the loop at **two** places
  and nowhere else — `Bus.publish_threadsafe()` (via `loop.call_soon_threadsafe`)
  and the time window's own already-locked snapshot read. Don't add a third seam,
  and don't introduce threads outside that package.
- **This example does not scale past one replica.** Because state is in-process,
  both k8s manifests are `replicas: 1`. A second pod would keep its own
  independent state and the Service would split clients across them. Horizontal
  scaling would require an external live store — a real design change, not a
  manifest tweak. Only `…-local.yaml` also sets `strategy: Recreate`, so that the
  old pod is gone before the new one serves; `…-rndp.yaml` still defaults to
  `RollingUpdate` and briefly runs two pods with separate state during a rollout.
- **One message shape.** The server pushes `state_message()` on connect and every
  change; the client renders it. Changing the protocol means touching both sides.
- **CORS is enabled, but nothing depends on it any more.** The middleware in
  `server.py` dates from when the dev client called this process directly on
  another port. It is what made the first plain HTTP fetch (the grid topology)
  work: WebSockets are exempt from the same-origin policy but `fetch` is not, so
  the browser silently discarded the response and the panel sat waiting for data
  that had already arrived. Every request is now same-origin in both modes —
  served by this process in the image, proxied through Vite in dev. It is wide
  open (`allow_origins=["*"]`); keep it that way only while every endpoint is a
  read of replayed sample data.
- **One cheap probe.** `GET /healthz` is both the liveness and readiness probe (and
  the compose healthcheck). The server has no external dependencies, so "is the
  process serving?" is the whole health story; there is no `/readyz`. It stays at
  the root, not under `/api`: it is the process's health, not any one app's.
- **Every api lives under `/api/<app>/`.** The prefix comes from `APPS` in
  `server.py`, not from the package, so a router declares plain paths (`"/ws"`)
  and is reachable at `/api/timeline/ws`. Keep the prefix aligned with the web
  client's route for the same app. `/api` is also the only thing the Vite dev
  proxy forwards, so an endpoint outside it won't reach the backend in dev.

- **The client always talks to the origin it was served from.** There is no backend
  picker and no address table: `resolveServerUrl(wsPath)` in
  `app/client-web/src/lib/servers.ts` derives `ws(s)://<window.location.host>`,
  inserts `BASE_PATH`, and appends the app's `*_WS_PATH` const from that same file.
  In the shipped image that origin *is* the backend (container, minikube NodePort,
  any cluster); under Vite it is the dev server, whose `/api` proxy forwards to the
  local docker server on :8000 — so dev is made to *look* like production rather
  than be special-cased. Pointing dev at some other backend is a one-line
  `server.proxy` target change in `vite.config.ts`, not a runtime choice in the UI.

- **The mount prefix is discovered at runtime, never baked in.** Remotely the app
  sits behind a reverse proxy under `/p-swamp/`, which strips the prefix before
  forwarding — so the server sees plain `/api/...` and knows nothing about it, but
  the browser must put it back on the front of every url.
  `app/client-web/src/lib/basePath.ts` recovers it by reading its own
  `import.meta.url` and cutting at the `/assets/` marker; `App.tsx` feeds the
  result to react-router as `basename`, and `resolveServerUrl` prepends it to
  WebSocket urls. This is what lets **one** published image run at the origin root
  and under a prefix, which matters because CI publishes exactly one. Two
  invariants it rests on, both spelled out in that file: built chunks stay under
  `assets/` (Vite's default, and what `SPAStaticFiles` special-cases), and
  **routes stay one segment deep** — `base: './'` makes asset urls resolve against
  the current document's directory, so `/prefix/timeline/detail` would look for
  `/prefix/timeline/assets/…`. Nested routes would mean switching to a build-time
  `--base` or a server-injected `<base href>`.

## The p-SWAMP web layer

`app/server-python/src/pswamp_web/` is the web front end over the analysis core of
the **desktop `p-swamp` package** at the repo root. It is the only code in `app/`
that imports `pswamp.*`.

**It is written to move into that package.** The intended end state is
`src/pswamp/web/` — the sibling of the existing `gui/` (PySide6) and
`visualization/` (pyqtgraph) packages, a third presentation adapter over the same
Qt-free core. So the package obeys two rules that are otherwise unusual here:
*nothing inside it imports from the rest of the web backend* (not `shared.py`, not
`server.py`), and *every import between its own modules is relative*. Moving it is
then a `git mv` plus the handful of import lines in `server.py` that name it.
Don't add a dependency that breaks that. Note this is only **one of two**
possible directions: §7 of `doc/WIP-context-port-from-qt-to-web-frontend.md` sets
it against the opposite move — root `src/` under `app/server-python/` — and argues
the choice follows from whether the Qt front end is being retired, not from
tidiness. Either way, the invariant that keeps this package self-contained is what
makes both moves cheap.

Two things it compensates for rather than fixing in the core, both marked in the
code and both listed in §11 of
`doc/WIP-context-port-from-qt-to-web-frontend.md` as changes worth making on their
own merits: `CountingTimeWindowLabeled` in `replay.py` (an append counter, for the
delta protocol) and the disjoint-group loop in `stores.island_groups`
(`detect_islands` returns overlapping groups). Don't "simplify" either away
without fixing the underlying gap first.

What is in there:

- **`wire.py`** — every message shape, as pydantic models, plus `send_state()`.
  This is the schema the desktop side never needed: its Qt widgets read the very
  `TimeWindow` object the application thread writes into, so nothing was ever
  serialised. **Always send via `send_state()`**, never `ws.send_json`:
  `json.dumps` emits bare `NaN` tokens that `JSON.parse` rejects, and NaN is the
  normal case here (a `TimeWindow` is all-NaN until it fills), so this breaks on
  the first message of a page rather than in some edge case.
- **`bus.py`** — the thread→loop fan-out. See the concurrency invariant above.
- **`hub.py`** — `HUB`, the process-wide pipeline: the recording, the player, the
  application threads, the alarm and status stores. Started by this package's
  `lifespan`, which `server.py` enters via `SERVICES` *before* anything in `APPS`.
- **`replay.py`** + **`recorded_io.py`** — the PMU source: a recorded Nordic 44
  stream replayed at real time through p-SWAMP's own `io` seam, no broker and no
  second process. The recording is
  `pswamp_web/data/n44_line_trip_50hz.npz` (~4.9 MB, 3501 samples × 700 channels,
  70 s at 50 Hz), read once at import. It contains a **line trip and a
  reconnection**, so every panel has a real disturbance to show. It is a committed
  fixture; `tools/record_n44_dataset.py` regenerates it, but not from the server's
  own environment — it needs `tops-rt` and `synchrophasor`, neither of which is in
  the server's dependency set. A venv with the root package plus those two is
  enough (the full `[full]` extra, Qt included, is not required), and the tool
  additionally needs `fastapi` on the path purely because it imports
  `pswamp_web.recorded_io`, whose package `__init__` pulls in the whole web stack
  — a wart in the tool, not a real dependency. Regenerating takes ~8 s and the
  script refuses to write a file whose scenario does not reproduce.

  **It carries current channels (`i_Magnitude`/`i_Angle`, 524 of the 700).** That
  is what `line_outage/` reads, and it is why the file is ~4.9 MB rather than the
  ~1 MB a voltage-and-frequency-only recording takes. Regenerate with
  `--channels all`; the default drops them and would leave the outage detector
  running forever without ever finding anything.
- **`channels.py`**, **`grid_model.py`**, **`stores.py`** — channel catalogue,
  static topology, alarm/status state machines.
- Page packages: **`app_status/`**, **`grid/`** (HTTP only), **`time_window/`**,
  **`phasors/`**, **`islanding/`**, **`line_outage/`**.

Three things worth knowing before touching it:

- **Never hand a p-SWAMP result dict straight to pydantic.** They carry numpy
  arrays, `uuid.UUID` and `datetime` — that is exactly why the desktop streaming
  layer pickles. Each app gets an adapter; `islanding/adapt.py` is the worked
  example, and it also has to *reconstruct the main system*, because
  `detect_islands` returns only the separated groups.
- **Send deltas, not windows.** `time_window/api.py` sends the window once
  (`mode: "full"`) and then only new samples (`mode: "append"`). Measured: ~5.9
  KB/s, against ~1.4 MB/s for re-sending the window at the same rate. That is the
  difference between a page that works and one that does not.
- **Keep the sample path out of React.** The chart's data lives in a `useRef` and
  redraws come through the hook's own `subscribe`, never a `setState` — see
  `useServerSocket`'s `onMessage` option, which exists for exactly this. Putting a
  50 Hz stream through React state runs a render pass 10×/s that produces no DOM
  change. Panels whose payload is small and drawn as SVG (`phasors`, `islanding`)
  use plain state, which is correct for them — but on one screen those costs add
  up, which is why each panel owns its own hook rather than the dashboard owning
  all four.

Sanity values, for checking a change did not quietly break the pipeline: median
frequency **50.0009 Hz**, median voltage **418.6 kV**, islanded stations **6500,
6700, 6701**, island groups summing to exactly **44** stations with no overlap,
and the dashboard opening exactly **4** WebSockets.

## Adding a p-SWAMP view

**A new p-SWAMP view is a panel in the grid monitor, not a page.** Everything the
monitor shows is a view of one server-side timeline, so a new view belongs beside
the others rather than behind its own nav entry. (This is the one thing §6 of
`doc/WIP-context-port-from-qt-to-web-frontend.md` says was got wrong the first
time and had to be refactored — four routes were chosen to mirror the four server
packages, but the client/server symmetry is real on the server and false on the
client.)

Client side, four edits:

1. Add `src/pages/grid-monitor/<slug>/` with the socket hook, the drawing
   component, and a `<Name>Panel.tsx` that renders them inside `<Panel>`. Copy
   `app-status/` for a simple one. Give it a `variant?: PanelVariant` prop and use
   it for size and for `focusHref` (a focused route must not link to itself).
2. Place it in the grid in `GridMonitorPage.tsx`. Use `minmax(0,1fr)` columns,
   never `1fr` — a grid item's default `min-width:auto` lets a canvas or a table
   force its column wider than the viewport.
3. Add the focused `<Route>` in `App.tsx` rendering the same component with
   `variant="focused"`.
4. Add a `*_WS_PATH` const in `src/lib/servers.ts`.

The backend needs no wiring — every socket resolves against the serving origin, so
all panels on a screen are views of the same server by construction.
**Each panel owns its own hook** — a panel must never re-render because a
neighbour's socket ticked. If one socket feeds two panels, hoist it into a
folder-local context (`islanding/IslandingData.tsx`) rather than opening it twice;
note the context object and its hook must live in a separate `.ts` file, because
`react-refresh/only-export-components` is an error here.

Server side, the backend package under `src/pswamp_web/<app>/` exporting `router`
(and registered in `APPS`) is unchanged — see "Adding a backend api". Copy
`app_status/` for a ticker-driven api or `islanding/` for an event-driven one.
Decide which of the two delivery patterns fits:

- **Poll the shared window** (`time_window`, and any measurement plot) — read the
  snapshot on your own asyncio ticker. This is the direct translation of what the
  Qt widgets do, and it is what keeps the 50 Hz sample stream off the event loop.
- **Subscribe to the bus** (`islanding`, alarms, status) — for results and events,
  which arrive about once a second.

## Adding a page

**`./scripts/generate-new-subapp.sh grid-overview "Grid Overview"` does all of
this**, plus the backend section below: from the url-name it derives the other
spellings (`grid_overview`, `GridOverview`, `GRID_OVERVIEW`), renders
`scripts/templates/` into both folders, inserts into the four registries by
anchor, and runs `error_check.sh` on the result (`NO_CHECK=1` skips that). What it
leaves is a working per-client counter over a WebSocket — a placeholder to
replace, not a stub to fill in; change what that is by editing the templates, not
the script. Both arguments are required, and it writes nothing if the name is
taken. The steps it performs, which stay the reference for doing it by hand:

Three edits, no build config:

1. Add `app/client-web/src/pages/<slug>/` — the folder is the page. Put its
   component `<Name>Page.tsx` there plus anything only it uses (its socket hook,
   its views), importing them **relatively** (`./useMyThingSocket`). Copy
   `src/pages/pmu-test-streamer/` as a starting point.
2. Register it in `src/App.tsx`: `<Route path="<slug>" element={<NamePage />} />`,
   inside the `AppLayout` layout route. Keep the slug **one segment deep** — a
   nested route breaks relative asset resolution behind the remote path prefix
   (see the `basePath.ts` invariant above).
3. Add `{ to: '/<slug>', label: '…' }` to `NAV_ITEMS` in
   `src/components/AppLayout.tsx`.

Name the folder after the route (`pmu-test-streamer`), so a URL maps to a directory
on both sides of the wire — the server package is the same name with underscores.

A page that talks to a backend adds a fourth edit: a hook wrapping
`useServerSocket(<APP>_WS_PATH)` (copy `usePmuStreamSocket.ts`) with a
`*_WS_PATH` const in `src/lib/servers.ts`. Nothing to configure beyond that path —
the socket always points at the serving origin.

**Promote to a shared folder only on the second user.** A component or hook lives in
its page folder until another page needs it, at which point it moves to
`src/components/` or `src/hooks/` — that is how `useServerSocket` got there.

Notes that matter when touching routing:

- **Deep links depend on `SPAStaticFiles`** in `server.py`, which returns
  `index.html` for a 404 outside `assets/` and `api/` — that is what makes a hard
  refresh on `/pmu-test-streamer` work in the container, while a wrong asset or
  endpoint URL still fails loudly as a 404. It also sets the `Cache-Control` that
  `StaticFiles` omits: `no-cache` on the shell, a year + `immutable` under
  `assets/`. **Don't drop that** — without it a browser may serve the shell from
  cache unvalidated, and because asset names are content-hashed a cached shell
  pins the previous build's assets, so the whole old client comes back from cache
  with no requests made. It presents as "the deploy is stale" on an image that is
  perfectly current, and only ever in the shipped image — the Vite dev server
  sends `no-cache` itself, so dev never shows it. A consequence: any *new* HTTP route
  must be registered **above** the greedy `/` mount at the bottom of that file,
  which the `APPS` loop already is.
- Because that fallback answers every unknown path, `App.tsx` keeps a catch-all
  `*` route redirecting to `/`; without it a typo'd URL renders the nav over an
  empty outlet. Keep it pointed at whichever page `<Route index>` renders.
- **The `/` entry in `NAV_ITEMS` passes `end`.** `NavLink` matches descendant
  paths by default and `/` is a prefix of every route, so without it the grid
  monitor renders as the active link on every page. (This replaced an earlier
  `isIndex` flag, which solved the opposite problem back when the index page's
  path was not actually `/`.)
- WebSockets are torn down on navigation (the hook lives inside the page), so
  leaving and returning starts a fresh client_id — and, for the monitor, a fresh
  server-side channel selection. The grid monitor holds **four** sockets at once,
  one per panel, each at its own rate so a slow one degrades alone. The islanding
  socket is the exception: it feeds two panels, so it is hoisted into a provider
  (`grid-monitor/islanding/IslandingData.tsx`) rather than opened twice.

## Adding a backend api

The server-side mirror of "Adding a page", and likewise done for you by
`./scripts/generate-new-subapp.sh` — which generates both halves at once, since a
page and its api are added together in practice. Two edits, no manifest and no
Dockerfile/compose change (both copy `src/` wholesale and watch the directory):

1. Add `app/server-python/src/<app>/` with an `__init__.py` re-exporting the
   package's public surface, exactly two names — `router` (an `APIRouter`) and,
   only if it needs startup/shutdown work, `lifespan` (an `asynccontextmanager`).
   Copy `src/timeline/__init__.py`. Use **relative** imports inside the package
   (`from .model import ...`).
2. Add one `("/api/<app>", <app>)` entry to `APPS` in `src/server.py`, with the
   matching `import <app>`.

`server.py` does the rest: it includes the router under that prefix and enters the
package's `lifespan` (via `AsyncExitStack`) if present, so packages never need to
know about each other. Keep domain logic out of `server.py` — it is wiring only.

`src/pmu_test_streamer/` is the one to copy: it is the same player shape as the
timeline minus the sequence picker, and shows how a package ships a **data file**
beside its code (`Path(__file__).parent / "sample_data.txt"`, read once at import —
no Dockerfile change needed, since `COPY src/ ./src/` takes the whole tree). Put
anything a second app would otherwise duplicate in `src/shared.py`; the per-app
`states` dict, `state_message`, `ticker`, and command dispatch deliberately stay
per-package, so each api reads top-to-bottom.

## Common commands

Dev (each script is the stable interface and live-reloads; they hide the
underlying tech). Start the server first, then the client:

```
./scripts/start-local-hotloaded-pswamp-server.sh      # state server on 127.0.0.1:8000 (docker compose up --watch --build; streams logs, Ctrl-C stops it)
                                                     # also live-syncs root src/, so desktop-package edits hot-reload too
./scripts/start-local-hotloaded-pswamp-web-client.sh  # Vite/React web client w/ HMR on http://localhost:5173
```

The server script's own terminal is the local log view — it streams the
container's output already, so there is no separate Compose follow script (the
minikube path still has `logs-minikube.sh`, since `kubectl` logs aren't attached
to the deploy).

Scaffolding (see "Adding a page" above for what it writes):

```
./scripts/generate-new-subapp.sh grid-overview "Grid Overview"   # url-name + nav label, both required (NO_CHECK=1 skips the error_check.sh run)
```

Restart the server script afterwards rather than relying on compose watch: a
*new* Python package needs the rebuild.

**Don't drop `--build` from that script.** Compose builds only when the image is
missing, and `watch` syncs only what changes while it runs — so any edit made while
the stack was down (especially a new, renamed, or deleted module) would otherwise
run from a stale image. Don't drop the flag to save a few seconds of warm-cache
rebuild.

`(cd app/server-python && uv run src/server.py)` also boots the backend
directly (no web client served) for quick Python-only dev. It must run from that
directory — `uv` discovers `pyproject.toml` from the working directory, not from
the script path. `PORT=8123 uv run src/server.py` moves it off 8000 when the
compose container already holds that port. Dependency changes go through the same
manifest:

```
(cd app/server-python && uv lock)                          # re-resolve after editing app/server-python/pyproject.toml
(cd app/server-python && uv lock --upgrade-package p-swamp) # ALSO needed after editing the ROOT pyproject.toml
```

The second form is not optional and not interchangeable with the first. A plain
`uv lock` treats the already-locked path dependency as settled and reports
"Resolved N packages" without re-reading the root manifest — so a root dependency
change, or a change to where the path points, is silently ignored until
`--upgrade-package p-swamp` forces it.

Quality checks (cover both halves of the codebase):

```
./scripts/error_check.sh             # READ-ONLY: uv lock --check + py_compile + ruff check/format (python), tsc -b + eslint (web). Runs all checks even if one fails, exits non-zero on any failure.
./scripts/autofix_lint_formatting.sh # write counterpart: eslint --fix, ruff check --fix, ruff format
```

A **pre-push hook** (`.githooks/pre-push`) runs `error_check.sh`. Activate once
per clone with `git config core.hooksPath .githooks`. Bypass with
`git push --no-verify`. Run `error_check.sh` before finishing any change — CI
runs the same script (see "CI" below), so skipping it locally just moves the
failure to a slower place.

Deploy / test the real artifact:

```
./scripts/start-pswamp-in-local-minikube-cluster.sh   # build into minikube + apply k8s/p-swamp-local.yaml
./scripts/logs-minikube.sh     # follow server logs (kubectl logs -f, bound to one pod)
```

`start-pswamp-in-local-minikube-cluster.sh` is the single *local* k8s test path: it
builds the image straight from your working tree into minikube, opens the web
client once `/healthz` answers (`NO_BROWSER=1` skips that), then tails the pod's
logs until Ctrl-C (`NO_LOGS=1` skips that). NodePort is 30081.

**minikube keeps its own image store, separate from the host docker daemon.**
`minikube image build` writes there; `docker build` (and compose) writes to the
host. Both tag `p-swamp:latest`, and with `imagePullPolicy: Never` the pod only
ever sees minikube's copy — so `docker images` on the host tells you nothing about
what the cluster will run, and the two can hold different builds at the same time.
To check what a pod is actually running:

```
kubectl get pod -l app=p-swamp -o jsonpath='{.items[0].status.containerStatuses[0].imageID}'
```

If a deploy looks stale, check that before rebuilding anything — and check the
`Cache-Control` note under "Adding a page", which is the other cause and looks
identical from the browser.

**The NodePort is only reachable on Linux.** With the `docker` driver — the default
everywhere — the node is a container on a bridge network. Linux routes to that
bridge directly, so `http://$(minikube ip):30081` works; on **macOS and Windows**
docker runs the bridge inside its own Linux VM and the host has no route to it, so
that address hangs on connect while the pod is perfectly healthy. This is a
host-networking gap, not a deploy failure — `kubectl get pods` shows Running and an
in-pod `curl localhost:8000/healthz` answers.
The script handles it by **probing the NodePort for ~8s and, if nothing answers,
starting `kubectl port-forward service/p-swamp 30081:8000` and switching every URL
it prints to `http://127.0.0.1:30081`** — so the browser, the WebSocket and the
health check all behave the same on both platforms. Consequences worth keeping in
mind when editing that script:

- **`--connect-timeout 1` is what makes `wait_for_healthz` fail fast; `-m` does
  not.** An unreachable node IP has no route, so packets are *dropped* and nothing sends a
  TCP reset — each attempt runs to its full timeout instead of failing fast. With
  a bare `-m 2` over 20 attempts the probe sat **silent for ~50s** before falling
  back, which every macOS user reasonably read as a hang. Measured: 2s/attempt →
  1s/attempt, and the loop is 5 attempts rather than 20. It also prints a dot per
  attempt, because a silent wait is indistinguishable from a wedged one.
- **Five attempts is deliberate, not stingy.** `rollout status` has already waited
  for the pod to pass its readiness probe — which *is* `/healthz` — so a routable
  NodePort answers on the first try (verified on Linux: zero dots). The budget
  exists only to detect the no-route case quickly. The loopback probe after the
  fallback gets 30 attempts instead, since a not-yet-bound tunnel is refused
  instantly and those attempts cost nothing.

- **The tunnel is a child process and dies with the script**, cleaned up by an
  `EXIT`/`INT`/`TERM` trap. That is why the log tail at the end is a plain call and
  **not `exec`** — `exec` would replace the shell and orphan the port-forward.
- On the tunnel path the script must stay in the foreground to stay reachable;
  `NO_LOGS=1` returns to the prompt and takes the tunnel with it, so it prints the
  standalone `kubectl port-forward` command to run in another terminal instead.
- Don't "fix" the unreachable NodePort with `minikube tunnel` or by switching
  drivers. `minikube tunnel` is for `LoadBalancer` services (and wants sudo); the
  port-forward is driver-agnostic and needs no privileges.

There are two k8s manifests, one per target:
`k8s/p-swamp-local.yaml` (minikube, image built into the cluster)
and `k8s/p-swamp-rndp.yaml` (the remote cluster, pulling the
published image). `doc/pswamp-server-infra-ops.md` is the cheat sheet
for deploying and operating the remote one — a manual `kubectl` flow. **No script
in this repo touches a remote cluster**, and CI does not deploy either.

## Conventions

- **Central Python manifest.** `app/server-python/pyproject.toml` declares the
  web backend's direct dependencies (its single source of truth — the root
  `pyproject.toml` belongs to the desktop package, and reaches this one only as
  the `p-swamp` path dependency declared here) and
  `app/server-python/uv.lock` pins the whole resolved transitive closure with
  hashes — the Python mirror of `app/client-web/package.json` +
  `package-lock.json`. Both are committed; `.venv/` is derived and is not.
  Add/change a dep by editing `[project.dependencies]`, then run `uv lock` from
  that directory and commit both files. Ranges express intent, the lock pins
  reality: re-locking keeps existing versions, so upgrades need an explicit
  `uv lock --upgrade`. `error_check.sh` runs `uv lock --check` and the Dockerfile
  runs `uv export --locked`, so an unlocked manifest edit fails loudly in both.
  The project is **non-packaged** (`[tool.uv] package = false`): no wheel is
  built and `src/` is a plain source folder, not a packaging layout — `server.py`
  imports each app package by name because the working directory *is* `src/`, and
  a new backend api is just a new folder there (no new manifest, no new lockfile).
  `app/server-python/.python-version` pins local dev to Python 3.11, matching the
  image; without it `uv` would build the venv on whatever system Python is newest.
  Deliberately **no `[tool.ruff]` section** in `pyproject.toml`: ruff only treats
  the file as config when that section exists, so omitting it keeps the explicit
  `--select E,F` below authoritative (verified — lint/format output is unchanged
  by the file's presence).
- **`tsc -b` then `vite build`** is the web build (see `package.json` scripts);
  the Dockerfile's `web-build` stage runs `npm ci && vite build` and copies
  `dist/` to `static/` beside the server source —
  `/workspace/p-SWAMP/app/server-python/src/static` in the image,
  `app/server-python/src/static/` locally, both being what the server resolves as
  `Path(__file__).parent / "static"` and mounts at `/`. `static/` is generated,
  never committed (gitignored).
- **The root `.gitignore` is the stock Python template, and its rules are
  unanchored** — `lib/`, `public/`, `build/`, `dist/` match at *any* depth, so they
  reach into `app/client-web/` too. That silently kept `app/client-web/src/lib/`
  (`basePath.ts`, `servers.ts`, `utils.ts`) out of the repo entirely, even though
  every shadcn `ui/` component imports `@/lib/utils` — a clean checkout could not
  type-check or build. A `!app/client-web/src/lib/` + `!app/client-web/public/`
  block at the bottom of the file rescues them, and must stay **below** the Python
  rules since gitignore is last-match-wins. When adding a folder under `app/`, run
  `git check-ignore -v <path>` before assuming it is tracked, and `git status`
  is not enough — an ignored file simply never shows up.
- **`.dockerignore` now has to earn its keep.** The build context is the repo root
  and the image installs root `src/` (see "Two Python projects in one repo"), so
  everything else at the root would otherwise be uploaded to the daemon on every
  build — `build/` alone is ~1.6 GB. It is excluded there, along with `examples/`,
  `tests/` and the cache dirs. Two entries are deliberately *not* excluded and
  will break the build if added: root `pyproject.toml` and root `README.md`, which
  uv reads to generate the desktop package's metadata while resolving.
- **Dockerfile base images are digest-pinned**, with the readable tag kept as a
  comment and refresh instructions inline. Those digests are multi-arch indexes,
  so the Dockerfile builds natively on amd64 and arm64 alike — which is what lets
  an arm64 laptop build it locally. What **CI publishes is amd64 only**; see "CI".
- **The deployable is named `p-swamp` everywhere** — local image tag, k8s
  Deployment/Service/Ingress, `app:` labels and selectors, container name, and both
  manifest filenames. The published image follows from the repo name rather than
  being written down: CI sets `IMAGE_NAME: ghcr.io/${{ github.repository }}`, so
  `thomanil/p-SWAMP` publishes as `ghcr.io/thomanil/p-swamp` — which is exactly what
  `k8s/p-swamp-rndp.yaml` pins. Keep those two agreeing; they drifted apart once
  already, when the stack was merged in still carrying its old PoC-era name.
  The remote identifiers follow the same slug: namespace `rndp-p-swamp`, ingress
  path `/p-swamp`, external URL `https://rndpdevsvc.statnett.no/p-swamp/`.
  **The namespace is not created by this repo** — the rndp/auth chart makes it from
  the project entry in `environ/development/users.yaml`, which lives in *another*
  repo, so that entry has to say `rndp-p-swamp` before an apply here can succeed;
  applying into a namespace that does not exist is rejected outright.
- **The minikube NodePort is 30081**, not the usual 30080. It and the Deployment
  name have to stay distinct from an older `timeline-server` sandbox whose resources
  still live in the same local minikube cluster: a shared Deployment name means each
  deploy silently overwrites the other's, and a shared nodePort makes the second
  Service fail to allocate outright. Renaming a Deployment does **not** remove the
  old objects either — `kubectl apply` creates the new name alongside the old, and
  the stale Service keeps its nodePort claimed until deleted by name. (The Python
  module names — `server.py`, `timeline/` — are internal to the image and collide
  with nothing.)
- **Scripts must run on bash 3.2.** They all start `#!/usr/bin/env bash`, but
  macOS still ships **bash 3.2** as `/bin/bash` (GPLv3 licensing), so a bash-4+
  builtin is a portability bug that only shows up on a Mac. `mapfile`/`readarray`
  is the one that bit: `error_check.sh` used `mapfile -d ''` to gather the `.py`
  files, which on macOS printed `mapfile: command not found`, left `PY_FILES`
  unset, and **skipped `py_compile` + both ruff checks while still printing "All
  checks passed" and exiting 0** — a green gate that had checked no Python at all.
  Use `while IFS= read -r -d '' x; do arr+=("$x"); done < <(find … -print0)`
  instead. Same family of trap: associative arrays (`declare -A`), `${x^^}`/`${x,,}`
  case conversion, and `&>>`. Note that a check which *silently does nothing* is
  worse than one that fails, so prefer a construct that errors loudly to one that
  degrades — and test a script change against `/bin/bash`, not just the newer
  homebrew bash that `env` may pick up.
- **Lint is explicitly `--select E,F`** (pycodestyle errors + pyflakes) in both
  `error_check.sh` and `autofix_lint_formatting.sh`. Don't drop the flag: ruff's
  own defaults now include opinionated families that fail the build on style
  rather than bugs. Add a family deliberately if you want it. Ruff's own version
  is pinned in the `dev` dependency group of `app/server-python/pyproject.toml`
  and locked, so everyone runs the identical linter.
- **CI publishes; it never deploys.** The pipeline (below) checks, then builds and
  pushes the image. Nothing rolls anything out to a cluster. The **pre-push hook**
  is still the first gate and the fast one — run `error_check.sh` before finishing
  any change rather than discovering it in CI.

## CI

**One pipeline: `.github/workflows/build-and-publish-image.yml`**, `check` →
`build`, publishing to **GHCR** (`ghcr.io/<owner>/<repo>`). A failing check
produces no image, and a pull request builds the image but pushes nothing (a
smoke test). No secret to configure — the automatic `GITHUB_TOKEN` covers GHCR.

GHCR is the only registry. A GitLab pipeline publishing to a GitLab registry was
built and tested against the internal mirror's runners, then dropped: publishing
to one place is enough. It is recoverable rather than guessed at, if it is ever
revived:

```
git log --diff-filter=D -- .gitlab-ci.yml   # find the deleting commit
git show <sha>^:.gitlab-ci.yml
```

Two things that pipeline learned the hard way, and that any future one on those
runners will hit again:

- **Those runners have no general internet.** Registry pulls work, arbitrary HTTP
  does not, so `apt-get update` dies on `deb.debian.org` and a `curl … | sh` uv
  install would too. A job can only get tools from its image, and no public image
  carries node + npx + uv + python3 — which is what made running `error_check.sh`
  there awkward enough to skip.
- **Docker-in-Docker needs a privileged runner**; kaniko is the fallback.

What has to hold in the check job:

- **Call `scripts/error_check.sh`, don't reimplement it in YAML.** It is the
  single source of truth for "is the code sound?", shared with the pre-push hook.
  A CI job that duplicates the checks will drift from the local one. It also
  installs `node_modules` itself on a cold checkout, so there is no separate
  `npm ci` step to keep in sync.
- **Runner needs `node`, `npx`, `uv`, `python3`** — `error_check.sh` preflights
  exactly those and fails fast with a clear message otherwise. `python3` is
  already on `ubuntu-latest`. Node must be **22**, matching the Dockerfile's
  `web-build` stage. `uv` (not `uvx`) is required since ruff now comes from the
  locked `dev` dependency group.
- **A real Python 3.11 must be on the runner *before* the check runs.** Passing
  the preflight isn't enough: `uv lock --check` resolves against the project's
  `requires-python` and wants 3.11 (per `app/server-python/.python-version`), and
  it runs `--offline` so uv may not download one on demand. A runner with only a
  newer python3 fails with *"No interpreter found for Python 3.11 … uv is set to
  offline mode"* — `py_compile` still passes, which makes it look like a lockfile
  problem when it is an interpreter problem. The workflow therefore runs
  `uv python install 3.11` before calling the script. Fix it there, not by
  dropping `--offline` from `error_check.sh`: that flag keeps the pre-push hook
  off the network.
- **Cache keys:** npm on `app/client-web/package-lock.json`, uv on
  `app/server-python/uv.lock`. Both are committed, so both are valid keys.
- **Use directory globs in path filters, never per-file lists.** An older workflow
  listed the two `.py` files individually, which silently missed
  `pyproject.toml`/`uv.lock` — a dependency-only change would not have triggered
  a rebuild. `app/**` plus `Dockerfile`, and `scripts/error_check.sh` so a change
  to the gate re-runs the gate.
- **The filters also cover root `src/**` and root `pyproject.toml`.** They used to
  be `app/**` only, correctly, back when the image contained nothing from the repo
  root. It now installs the desktop package from root `src/`, so a change there
  changes what gets published and must rebuild. If the image's contents ever
  change again, check these filters — a stale filter fails silently, by publishing
  an image that does not match the commit.
- **Gate ordering:** the build job `needs:` the check job, or a failing check
  still produces an image.
- **The published image is `linux/amd64` only**, stated explicitly via
  `platforms:`. Dev boxes here are often arm64, but they build their own image via
  compose/minikube and never pull the published one, so an emulated arm64 leg
  would cost every push for an artifact nothing consumes. The *Dockerfile* is
  still multi-arch-capable (both base images are multi-arch index digests, and
  `uv.lock` carries wheel hashes for both) — adding arm64 back is a `platforms`
  edit plus a QEMU/binfmt step, not a rewrite. Don't drop lock hashes to "fix" a
  cross-arch failure.
- **Build context is the repo root**, as in compose and the minikube script — the
  `web-build` stage needs `app/client-web/` and the runtime stage needs root
  `src/` — so it can't be narrowed to `app/server-python/`.
- **A new GHCR package starts PRIVATE.** An unauthenticated pull (e.g. from
  minikube) fails until it is flipped to public once in the package settings —
  done once for this package, so it now pulls anonymously. The workflow sets
  `org.opencontainers.image.source`, so the package links back to the repo and
  inherits its visibility.
- **Pin deployments to the immutable `sha-<full sha>` tag**, not to `:latest`.
  `latest` and the branch tag both move, and neither triggers a k8s rollout on
  its own (the pod spec doesn't change) — the sha tag does.
- **k8s manifests:** `…-local.yaml` is local-only (`imagePullPolicy: Never`, image
  built into minikube); `…-rndp.yaml` runs the *published* image on the remote
  cluster and differs in little more than the registry image ref and the
  namespace/ingress. Keep the two in step when the pod spec changes.
- **CI publishes but never deploys.** Rolling the remote cluster forward is a
  manual `kubectl` step out of `doc/pswamp-server-infra-ops.md` —
  nothing here automates it, and no cluster credentials live in this repo.

## Workflow rules

- Never git commit or push anything, those are done by human so diff is always reviewed
