# P-SWAMP client-server architecture



Classification
==

```text
K1 Internal code
```

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
.github/      CI: workflows/build-and-publish-image.yml (check -> build -> push the
              container image to GHCR)
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


Running it as a kubernetes service
==

In proper deployments, the app is deployed as a single container to kubernetes. The container serves up both the frontend and the api on the same port.
The frontend is served as static files/assets, which in turn talk to the /api paths via the same host/port.
You can test running in this "prod mode" in Minikube locally with `scripts/start-pswamp-in-local-minikuber-cluster.sh`

TODO more notes on the remote rndp infra deployment of the app


Build pipeline (CI)
==

One pipeline, `.github/workflows/build-and-publish-image.yml`, publishing to
**GHCR** — `ghcr.io/<owner>/pswamp-client-server-poc`. 

Pull the current build with:

```
docker pull ghcr.io/<owner>/pswamp-client-server-poc:latest
```

This poc publishes a public container build, so this will in turn be directly mirrored in Harbor so RNDP can pull and deploy that container.

For a proper private pipeline for pswamp, we may need to push directly to harbor, which needs credentials secret in the gh actions pipeline.


Client-server architecture: many pages, one app
==

The project holds several small **pages/"subapplications"** rather than one app:
the web client is a single-page app routed client-side with react-router, and the
backend is one process that routes each api to its own package. One deployable
serves all of it from one origin, so there is nothing per-page to deploy or
configure.

Currently there are two examples:

| Page URL | Api | What                                              |
|---|---|---------------------------------------------------|
| `/pmu-test-streamer` (also `/`) | `/api/pmu-test-streamer/ws` | Streams canned sample of PMU records line by line |
| `/timeline` | `/api/timeline/ws` | Scrolling-number timeline with playback controls  |


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
  timeline/
    TimelinePage.tsx       the page itself
    useTimelineSocket.ts   its websocket hook
    TimelineWindow.tsx     its view
  pmu-test-streamer/
    PmuTestStreamerPage.tsx, usePmuStreamSocket.ts, StreamWindow.tsx
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
   else only that page uses (copy `src/pages/pmu-test-streamer/` as a starting
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
timeline/         one package per api — the thing you add
  __init__.py     what the package exposes: router (+ lifespan if it needs one)
  api.py          the endpoints (here: the /ws websocket)
  model.py        the app's own domain logic
pmu_test_streamer/
  __init__.py, api.py, model.py
  sample_data.txt  the streamed records — real PMU data, see above
```

**To add an api**, two small edits (again, both done by
`scripts/generate-new-subapp.sh`):

1. New folder `src/my_thing/` (copy `src/pmu_test_streamer/`), whose `__init__.py` exposes a
   `router`, plus `lifespan` if it needs to run something in the background.
2. In `src/server.py`, add one line to `APPS`: `("/api/my-thing", my_thing)`.

Everything the package declares is then served under that prefix — the timeline's
`"/ws"` endpoint becomes `/api/timeline/ws`. Endpoints all live under `/api/`,
which keeps them clear of the page URLs and is what the dev setup forwards to the
backend. Note the folder name is `my_thing` (underscores — Python has to import it)
while the URL is `my-thing`, matching the page route.

Data files live inside the package that reads them, like `sample_data.txt` above;
they ship automatically, so nothing outside the folder needs touching.


The api between client and backend
==

There is no REST api yet: every subapp exposes exactly one WebSocket endpoint at
`/api/<app>/ws` (`/healthz` is the only plain HTTP route). The client sends
`{type: 'command', action: ...}` up, the server pushes `{type: 'state', ...}` down on
connect and after every change — including unprompted pushes from the playback
ticker. Same origin in both dev and prod: the shipped image serves the frontend
itself, and Vite proxies `/api` to the backend in dev.

Note that nothing validates the wire format — the TypeScript message types are
hand-written mirrors of Python dicts, so a renamed field fails at runtime, not in
any check. Rejected commands are logged server-side and silently ignored.

TODO harden wire format? Add REST api surface?



Session state management
--

Right now there are no sessions in the usual sense: no cookies, no login, no session
store. Each page mount rolls a random integer and sends it as `?client_id=` on the
WebSocket; the server keeps one in-memory `ClientState` per id. So every visitor does
get their own state — but:

- **One replica only.** State is in process memory, so the manifest is `replicas: 1`.
  Two pods = two `states` dicts. Scaling out needs an external store.
- **Restarts reset everyone.** Nothing is persisted.
- **A page reload is a new client.** Only a socket-level reconnect resumes.
- **`client_id` is unauthenticated.** Supply someone else's and you share their state.
- **State is never evicted.** Grows with cumulative page loads, not concurrent users.

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

Optional extras
--

Needed only for the checks and the alternative run modes, not for the two scripts above:

- **uv** (https://docs.astral.sh/uv/) — runs the backend directly on your machine
  without Docker (`cd app/server-python && uv run src/server.py`), manages
  `app/server-python/pyproject.toml` + `uv.lock`, and provides the linter used by
  `./scripts/error_check.sh`. Install with
  `curl -LsSf https://astral.sh/uv/install.sh | sh`.
- **minikube + kubectl** — only for `./scripts/start-pswamp-in-local-minikube-cluster.sh`, which tests the
  real container image on a local Kubernetes cluster.


How to collab/merge changes between public partner repo of p-swamp and internal repo of it
==

*Note: these parts are not landed properly yet*

We will have partner researchers working against a private github repo, including some bits that should stay inside 
the Statnett system (ci pipelines and whatnot)

We will also have a public LFE mirror of the repo, which gets downstream updates regularly. Github pipelines etc should probably not
be mirrored, TBD.

We will in addition most likely have at least a separate private Statnett gitlab repo that holds some of the data 
integration/consumer/producer components of the project, TBD.


