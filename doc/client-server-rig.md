# P-SWAMP client-server architecture



Driving goals, constraints and principles:
==

- The goal of the pswamp repo is to make it simple for project members to work with PMU/grid data locally, build UI prototype apps, and run those experiments
against real production data from inside Statnett infra.

- Should be simple to spin up and work on locally, with good doc. Should also be possible to test basic kubernetes running locally (via minikube) before deploying to remote cluster.

- Should be easy to iterate quickly on backend + frontend code locally. Hot reload of both layers so that changes are immediate.

- Should be easy to add new pages/"subapplications"/experiments: make it super simple to drop in new web pages and backend modules, with api between them

- Should do what we can to prevent people from tripping over each other and break others code/modules (some guardrails, such as basic syntax error checking before push). Probably also some automated testing
at least for a "reference app" to ensure the basic structure of the repo/project stays intact.

- Should have private github repo for partners to contribute to/pull from, without confidential data.
  With a public mirror that we publish to Linux Foundation Energy regularly.

- Consumed PMU/grid data need to be stubbed out, so that the private/cluster deployed Statnett instance fetches from full dataset, while the public
  repo consumes local non-sensitive test data during local development/testing.

- Apis that return sensitive prod data when deployed in SN infra must filter/shape/transform the data to only show what the UI needs to display/convey,
  not send entire dataset to client (production PMU data is K3 graded eg. should not be directly transmitted to a frontend in its raw form)

- Tech stack is as basic as possible, should be as easy as possible now but also make it easy to promote to prod system later if value is clear.
  Python is the research language, and also supported in prod by Statnett.
  React + Typescript is the lingua franca of web dev in 2026, and also main choice in Statnett.

- Start with only basic modularization (clear src folder structure separation between different pages/api endpoints in the repo).
  Defer jumping down rabbithole of microservices, separate packages, multi repo etc to begin with.
  Each such decoupling makes the project harder for partners to iterate in quickly!


# How to onboard when you are new to the project

- Get RND platform user/access
- Get org/repo access in Github
- Clone this repo
- Make sure you are able to run the two scripts that launches the project locally: 
`scripts/start-local-hotloaded-pswamp-server.sh` and `scripts/start-local-hotloaded-pswamp-web-client.sh`
- Make sure the error check script runs ok for you: `scripts/error_check.sh`
- Change something trivial about the project in a branch, create your first pull request to merge it into main

  
Folder structure for client-server specific bits
==

This project started as a single local python project. These folders are what gets *added* alongside the existing
desktop code:

```text
app/          the two deployables — everything that ends up in the container
  client-web/     React + TypeScript + Vite frontend (shadcn/ui, Tailwind).
                  One folder per page under src/pages/. Built to static assets and
                  baked into the server image; no separate frontend service.
  server-python/  FastAPI state server. src/server.py is the only entrypoint (wiring
                  only); one package per api under src/<app>/. 
doc/          markdown notes on how the rig works, and some notes on possible things to follow up later.
k8s/          Kubernetes manifests for the deployable. One per target: -local.yaml
              (minikube, image built into the cluster) and -rndp.yaml (the remote
              cluster).
scripts/      the stable developer interface — start the server, start the web client,
              run in minikube, etc. Call these rather than the underlying docker/npm/uv
              commands; they are what stays the same if the tooling changes.
.github/      CI: workflows/ci-pipeline.yml (static-errorcheck -> smoketest ->
              push the container image to GHCR)
.githooks/    pre-push hook running scripts/error_check.sh. Opt in per clone with
              `git config core.hooksPath .githooks`.
```

Loose files at the root that go with the above: `Dockerfile` and `docker-compose.yml`
(build/run the one container), `.dockerignore`, and `AGENTS.md` / `CLAUDE.md` /
`.github/copilot-instructions.md` (coding-agent guidance — `AGENTS.md` is the real one,
the other two are pointers to it).

Things to watch when merging into the Qt repo: keeping the web backend's `pyproject.toml` inside
`app/server-python/` rather than hoisting it to the root is what keeps those from
colliding.



How to run this locally
==

Fire up `start-local-hotloaded-pswamp-server.sh` and `start-local-hotloaded-pswamp-server.sh` to launch the project locally.

The server/backend runs in Docker, the web frontend is hosted in a Vite process. Both hot-reload: save a python or ts file under apps and the change is live.

You only need **two things installed**: Docker and Node.js. Note: You do *not* strictly need Python locally — the backend runs inside the container.




Prereqs, what to install
==

**1. Docker** (with the Compose v2 plugin)

The backend script uses `docker compose watch`, which needs Compose **2.22 or newer**.
Any current Docker Desktop or Docker Engine install has it.

- Linux (Ubuntu/Debian): follow https://docs.docker.com/engine/install/ubuntu/ and
  install `docker-ce` plus `docker-compose-plugin`.
- Mac/Windows: install Docker Desktop, https://docs.docker.com/desktop/

On Linux, add yourself to the `docker` group so you don't need `sudo` for every
command — **you must log out and back in for this to take effect**:

```
sudo usermod -aG docker $USER
```

**2. Node.js 22 or newer** (npm comes with it)

The container builds the frontend with Node 22, so match that or go higher.

- Any platform: https://nodejs.org/ (LTS), or use a version manager like `nvm`,
  `fnm` or `mise` if you juggle several Node versions.
- Ubuntu/Debian: the `nodejs` in apt is often too old — prefer nodesource or a
  version manager.

Check that everything is in place:

```
docker compose version      # 2.22+
docker compose watch --help # should print help, not "unknown command"
docker run hello-world      # proves the daemon is reachable without sudo
node --version              # v22 or newer
```


Running it locally in dev mode
==

Two terminals, backend first:

```
# terminal 1 — state server on http://127.0.0.1:8000
./scripts/start-local-hotloaded-pswamp-server.sh

# terminal 2 — web client on http://localhost:5173 (opens your browser)
./scripts/start-local-hotloaded-pswamp-web-client.sh
```

Open http://localhost:5173. The page talks to the backend through Vite, which
proxies `/api` to port 8000 — so it behaves like the deployed app.

**The first server start is slow** (a minute or two): Docker builds the server image and npm
downloads the frontend dependencies. Later runs start in seconds.

What hot-reloads:

- Any `.py` file in `app/server-python/src/**` → Docker Compose copies it into the running
  container and the server reloads itself in place.
- Any `.ts/.tsx` file in `app/client-web/**` → Vite patches the running page instantly (HMR).

What does **not**: the generated api contract. Change an endpoint or a socket
message and the server reloads with it, but `doc/api/openapi.json` and the web
client's generated types stay as they were until you run
`./scripts/generate-api-contract.sh`. Vite does not type-check, so a mismatch is
invisible in the browser — `scripts/error_check.sh` is what catches it.


Running it as a kubernetes service
==

In proper deployments, the app is deployed as a single container to kubernetes. The container serves up both the frontend and the api on the same port.
The frontend is served as static files/assets, which in turn talk to the /api paths via the same host/port.
You can test running in this "prod mode" in Minikube locally with `scripts/start-pswamp-in-local-minikuber-cluster.sh`

TODO more notes on the remote rndp infra deployment of the app


Build pipeline (CI)
==

One pipeline, `.github/workflows/ci-pipeline.yml`, publishing to
**GHCR** — `ghcr.io/<owner>/p-swamp`. 

Pull the current build with:

```
docker pull ghcr.io/<owner>/p-swamp:latest
```

This repo publishes a public container build, so this will in turn be directly mirrored in Harbor so RNDP can pull and deploy that container.

For a proper private pipeline for pswamp, we may need to push directly to harbor, which needs credentials secret in the gh actions pipeline.


Client-server architecture: many pages, one app
==

The project holds several small **pages/"subapplications"** rather than one app:
the web client is a single-page app routed client-side with react-router, and the
backend is one process that routes each api to its own package. One deployable
serves all of it from one origin, so there is nothing per-page to deploy or
configure.

One real application and two scaffold demos:

| Page URL | Api | What                                              |
|---|---|---------------------------------------------------|
| `/` (grid monitor) | `/api/time-window/ws`, `/api/islanding/ws`, `/api/phasors/ws`, `/api/app-status/ws`, `/api/grid/model` | Dashboard of panels over a recorded Nordic 44 PMU stream replayed through p-SWAMP's monitoring applications |
| `/time-window`, `/phasors`, `/islanding`, `/app-status` | as above | The same panel components, full-size — focused views, not copies |
| `/reference-subapp` | `/api/reference-subapp/ws` | The reference example subapp: a per-client counter, generated by `generate-new-subapp.sh`, and the end-to-end smoke test of the stack |
| `/pmu-test-streamer` | `/api/pmu-test-streamer/ws` | Older demo, slated for retirement: streams a canned sample of simulated PMU records line by line |

The Api column lists the *sockets* a page opens, which is where its state comes
from. A page with controls also POSTs commands to its app's prefix — see "The api
between client and backend" below.

The grid monitor is the real one; the last two are scaffold demos.
`/reference-subapp` is what `generate-new-subapp.sh` writes and the example every
doc points at — copy that one, and click it after a refactor or an upgrade to
check the whole seam still works. `/pmu-test-streamer` predates it, played the
same role, and is on its way out. A new *p-SWAMP* view is a panel in the monitor
rather than a new page — see "Adding a p-SWAMP view" in `AGENTS.md`.


### Adding a new page/subapp: just run the script

```
./scripts/generate-new-subapp.sh grid-overview "Grid Overview"   # url-name, nav label
```

It does every step in the two sections below — the page folder, the route, the
nav entry, the api package, the `APPS` entry — and leaves you a subapp that
already works: a nav entry, a page at `/grid-overview`, and a WebSocket to a new
backend package holding a per-client counter you can bump and reset. Replacing
that counter with the real thing is then the only work left.

The files it writes come from `scripts/templates/`; edit those to change what a
new subapp starts life as. Read on if you want to know what it wired up, or to do
it by hand.


### Frontend

Layout under `app/client-web/src/`:

```
pages/            one FOLDER per page — the thing you add
  reference-subapp/
    ReferenceSubappPage.tsx     the page itself
    useReferenceSubappSocket.ts its websocket hook + its commands
  grid-monitor/
    GridMonitorPage.tsx, plus one folder per panel
App.tsx           the route table: which URL renders which page
components/       shared across pages only
  AppLayout.tsx   nav bar + shell every page renders inside
  ui/             shared shadcn/ui building blocks
hooks/
  useServerSocket.ts  the websocket plumbing every page's hook builds on
lib/              servers.ts (each app's ws path + the serving-origin url), utils.ts
```

A page/subapp keeps its own parts in its own folder, named after the route. Things move out
to `components/` or `hooks/` only once a *second* page needs them.

**To add a page**, three small edits (all three are what
`scripts/generate-new-subapp.sh` does for you):

1. New folder `src/pages/my-thing/` with `MyThingPage.tsx` inside, plus whatever
   else only that page uses (copy `src/pages/reference-subapp/` as a starting
   point).
2. In `src/App.tsx`, add it to the route table:
   `<Route path="my-thing" element={<MyThingPage />} />`
3. In `src/components/AppLayout.tsx`, add `{ to: '/my-thing', label: 'My Thing' }`
   to `NAV_ITEMS` so it shows up in the nav bar.

Vite hot-reloads changes immediately, and deep links (`http://…/my-thing`, or a hard refresh on it) work in
both dev and the deployed container because the backend serves the SPA shell for
unknown paths.

### Backend

Same idea, one folder per api. Layout under `app/server-python/src/`:

```
server.py         the one entrypoint: which URL prefix goes to which package,
                  plus /healthz and serving the web client. No app logic here.
shared.py         helpers the packages share. Not an api itself.
reference_subapp/ one package per api — the thing you add
  __init__.py     what the package exposes: router (+ lifespan if it needs one)
  api.py          the endpoints: the /ws websocket and the POST commands
  model.py        the app's own domain logic
pmu_test_streamer/  the older demo, on its way out; it also ships a data file
  sample_data.txt  the streamed records — 300 lines of *simulated* PMU data
                   from the Nordic 44 sim, committed as a static test fixture
pswamp_web/       the p-SWAMP web layer: a package of page packages
```

**To add an api**, two small edits (again, both done by
`scripts/generate-new-subapp.sh`):

1. New folder `src/my_thing/` (copy `src/reference_subapp/`), whose `__init__.py` exposes a
   `router`, plus `lifespan` if it needs to run something in the background.
2. In `src/server.py`, add one line to `APPS`: `("/api/my-thing", my_thing)`.

Everything the package declares is then served under that prefix — the reference
subapp's `"/ws"` endpoint becomes `/api/reference-subapp/ws`. Endpoints all live under `/api/`,
which keeps them clear of the page URLs and is what the dev setup forwards to the
backend. Note the folder name is `my_thing` (underscores — Python has to import it)
while the URL is `my-thing`, matching the page route.

Data files live inside the package that reads them, like the streamer's
`sample_data.txt` above; they ship automatically, so nothing outside the folder
needs touching.


The api between client and backend
==

**The two directions use two transports: commands up over REST, state down over
the WebSocket.** `the-client-server-api.md`, beside this file, is the full
account — the contract, both call paths, and the failure semantics. This is the
one-screen version.

Each subapp exposes a WebSocket at `/api/<app>/ws`, which is downstream only: the
server pushes `{type: 'state', ...}` on connect and after every change — including
unprompted pushes from a server-side ticker — and the client renders it. Nothing is
sent up that socket; the receive loop on the server side exists purely to notice a
disconnect.

Everything a user can trigger is a `POST` under the same `/api/<app>` prefix, one
url per operation:

```
POST /api/reference-subapp/count/bump?client_id=…
POST /api/islanding/alarms/<uuid>/acknowledge?client_id=…
POST /api/time-window/selection?client_id=…        {"channels": [5, 9]}
```

The `client_id` is the same value the sockets carry — one per browser profile,
from `src/lib/clientId.ts` — which is what makes a command apply to the pipeline
the page is watching. The reply is a small `CommandAck`, deliberately *not* the
new state: state arrives on the socket, so there is exactly one path for it and
nothing to reconcile.

What the split buys, and why it is worth a round trip per click: every operation
is a row in the browser's Network tab and a line in the server's access log with a
status code, and a rejected command says *why* (422 for a bad sequence name, 404
for an unknown alarm) rather than failing quietly.

**Both halves are described by a generated contract** — `doc/api/openapi.json`,
committed, with the web client's TypeScript generated from it and
`scripts/error_check.sh` failing if the two drift. Socket messages are in there
too, which OpenAPI has no native notion of; how that works, and how to change the
api without breaking anyone, is in `the-client-server-api.md`.

Same origin in both dev and prod, for both transports: the shipped image serves
the frontend itself, and Vite proxies `/api` to the backend in dev.


Authentication
--
For the foreseeable future, the pswamp project itself does not implement its own 
authentication in the web->backend strata of the architecture. 

The project runs in the RND platform in Statnett infra, and its web/http ingresses are only available to autenticated rndp users. 
Therefore auth is implicit for pSwamp: when you are inside an oauth rndp session, you can reach the 
pswamp web frontend at f.ex https://rndpsvc.statnett.no/p-swamp/, otherwise it is not available.

The only client identifier/identity we implement at the moment is an integer clientId that frontend persists in localstorage in 
the browser. 


Performance/profiling
--

TODO: We try not to complicate the pswamp architecture until we need it, which means we need
good metrics on how its behaving. TBD!



Server state
--

**Every connected client gets its own PMU stream.** Not its own view of a shared
one — its own replay of the recording, its own copies of the monitoring
applications, its own alarms and its own detected islands. Open the grid monitor
and you start watching the disturbance from the beginning, whoever else is on the
server and however long it has been up.

This is the opposite of how it started, and the reasoning is worth keeping around.
The first version ran one pipeline for the whole process, on the grounds that
there is one grid and two operators seeing different frequencies would be a bug.
That is true of a control room. It is false of this, which is a rig for exploring
recorded data: a visitor wants the event from the start, not to join someone
else's replay half way through. So the useful unit is one timeline per viewer.

### Lifecycle

```
first socket connects  ->  pipeline built, replay starts at 0s
sockets come and go    ->  same pipeline, still replaying
last socket closes     ->  keeps replaying, idle timer starts
reconnect within 5 min ->  rejoins the same stream, mid-flight
5 min with nobody      ->  torn down; next connect starts fresh at 0s
```

The pieces that make that work:

- **`?client_id=` identifies the browser, not the tab or the socket.** The web
  client generates one random integer per browser profile and keeps it in
  `localStorage` (`app/client-web/src/lib/clientId.ts`), so every socket the page
  opens carries the same value. This matters more than it looks: the grid monitor
  opens **five** sockets at once, and they must all resolve to one pipeline.
  Because the id is persisted rather than rolled per mount, a reload, a
  navigation or a crashed tab all come back to the same stream.
- **`HubRegistry` owns the lifecycle** (`app/server-python/src/pswamp_web/hub.py`).
  It is the only thing that constructs a pipeline, and it holds a per-client lock
  so five simultaneous first-connects build one pipeline rather than five.
- **A pipeline outlives its sockets by design.** Closing the last one starts an
  idle timer instead of tearing down, which is what makes a reload cheap.
- **There is a hard cap** (`MAX_PIPELINES`, currently 8). At the cap a new client
  reclaims the least-recently-used pipeline that nobody is watching; if every one
  has a live socket the connection is refused with WebSocket close code 1013, and
  the client shows a banner instead of retrying forever.

### What it costs, and why the cap exists

A pipeline is **four OS threads** (three monitoring applications plus the replay
player) and roughly **30 MB** of resident memory, dominated by the 30-second
measurement window. Two consequences shape the design:

- **Memory is the binding constraint, not CPU.** A pipeline is about 1.4% of a
  core; eight of them are nothing. But freed memory is not returned to the OS, so
  **peak RSS follows the cap rather than the typical load** — which is why both
  k8s manifests size `resources.limits.memory` from `MAX_PIPELINES` and say so.
- **The GIL is the real ceiling.** Every one of those threads runs Python
  bytecode, contending with the event loop that serialises WebSocket messages.
  That, more than any single resource, is why the cap is single-digit.

The recording itself is the one thing still shared: ~10 MB of read-only arrays,
loaded once and handed to every pipeline (`load_recording()` is cached). It is
safe because `Recording` is frozen and the decoder copies each row before
touching it.

### What this still does not give you

- **One replica only.** Pipelines live in one process's memory, so the manifests
  are `replicas: 1`. A second pod would run its own unrelated replays and the
  Service would scatter clients between them — and, unlike before, a client whose
  sockets landed on different pods would see different instants on one screen.
- **Restarts reset everyone.** Nothing is persisted; every client starts over at
  0 s.
- **`client_id` is unauthenticated.** Supply someone else's and you share their
  stream. It is a routing key, not a credential.
- **Clearing site data is a new identity.** The id lives in `localStorage`, so
  clearing it (or opening a private window) gets a fresh pipeline — which is also
  the easy way to force a restart from 0 s.

TODO if the rndp ingresses have some identifiers (user id/email headers on incomring requests?), we could use that instead
of the currently client-generated clientid.




Adding a dependency
--

Both the client and server sides of work the same way: a manifest you edit, and a lockfile that is generated
and pins exact versions. Commit both; never commit the install directory.

| | Python (`app/server-python/`) | Web client (`app/client-web/`) |
|---|---|---|
| You edit | `pyproject.toml` | `package.json` |
| Generated lock | `uv.lock` | `package-lock.json` |
| Install dir (never committed) | `.venv/` | `node_modules/` |
| Re-resolve after editing | `uv lock` | `npm install` |

```
# Python: add the requirement to [project.dependencies], then
(cd app/server-python && uv lock)
```

The manifest states a compatible *range*; the lockfile pins the one exact version of
every package — direct and transitive — that everyone actually gets. Re-locking keeps
the versions already in the lock, so an upgrade is a deliberate `uv lock --upgrade`,
never a side effect of adding something unrelated. `./scripts/error_check.sh` fails if
the two drift apart, as does the Docker build.

Updating dependencies
--

It is good hygiene to update dependencies often, to stay ahead of supply chain attacks etc
(LLM tech is accelerating security risks). One script does the whole repo — both Python
projects and the web client, manifests and lockfiles:

```
./scripts/update-dependencies.sh          # TARGET=minor to skip major-version jumps
```

What it produces is a *candidate diff*: run it on a branch, read what it reports. Start the
app and click through it, then open a pull request so someone else reads the same diff. The
manifests have a `CODEOWNERS` entry so that lands in front of a reviewer.

Read its **"What actually moved"** report rather than the lockfile diff. A lockfile diff is a
bad summary of an upgrade — around 95% of its changed lines are per-wheel sha256 hashes, so
one numpy bump rewrites ~40 lines while moving one version. The report prints the versions
themselves, direct dependencies first and transitive ones as a count (`VERBOSE=1` lists
those too). The manifests are the decision and are short enough to read in full.

One thing to expect: on the npm side the script moves the version *ranges* themselves, on
the Python side it cannot — `uv lock --upgrade` respects the bounds in `pyproject.toml`, and
uv has no `npm-check-updates`. So a capped dependency needs a hand edit before it can move,
and the script ends by listing exactly which ones are held back and how far they could go.

The header comment in `scripts/update-dependencies.sh` explains the rest — what it runs in
what order, why, and how to recover from a half-finished run.

Optional extras
--

Needed only for the checks and the alternative run modes, not for the two scripts above:

- **uv** (https://docs.astral.sh/uv/) — runs the backend directly on your machine
  without Docker (`cd app/server-python && uv run src/server.py`), manages
  `app/server-python/pyproject.toml` + `uv.lock`, and provides the linter used by
  `./scripts/error_check.sh`. Install with
  `curl -LsSf https://astral.sh/uv/install.sh | sh`.
- **minikube + kubectl** — only for `./scripts/start-pswamp-in-local-minikube-cluster.sh`, which tests the
  real container image on a local Kubernetes cluster. That script also preflights
  the api contract, so it wants `uv` and `npx` too (`NO_CHECK=1` skips both the
  check and the requirement).


How to collab/merge changes between public partner repo of p-swamp and internal repo of it
==

*Note: these parts are not landed properly yet*

We will have partner researchers working against a private github repo, including some bits that should stay inside 
the Statnett system (ci pipelines and whatnot)

We will also have a public LFE mirror of the repo, which gets downstream updates regularly. Github pipelines etc should probably not
be mirrored, TBD.

We will in addition most likely have at least a separate private Statnett gitlab repo that holds some of the data 
integration/consumer/producer components of the project, TBD.


