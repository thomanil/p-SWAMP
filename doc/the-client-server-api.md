# The client/server api

Everything about the seam between the web client and the state server: the rules it
follows, the contract that describes it, and the machinery underneath.

Read as much as you need:

| If you want to… | Read |
|---|---|
| add an endpoint, a field, or a page | [Common tasks](#common-tasks) |
| know which files are generated and which to edit | [Where the contract comes from](#where-the-contract-comes-from) |
| consume this api from another codebase | [What the contract is](#what-the-contract-is), [Consuming it from another codebase](#consuming-it-from-another-codebase) |
| understand how a click becomes a change | [Upstream](#upstream-how-a-command-reaches-the-server) |
| understand how state reaches the screen | [Downstream](#downstream-how-state-reaches-the-screen) |
| know what a failure looks like | [Error and refusal semantics](#error-and-refusal-semantics) |

`client-server-rig.md` is the shorter tour of the whole rig, of which this api is one part.

The shape in one paragraph
==

**Two directions, two transports.** Anything a user triggers goes up as an HTTP
`POST` under `/api/<app>/`. Everything the server has to say comes down a
WebSocket at `/api/<app>/ws`, which is downstream only. A command's reply is a
small acknowledgement, never state — so state has exactly one path and there is
no ordering to reconcile between two of them.

```
   BROWSER                                   SERVER (one process, one event loop)

   page component                            FastAPI app  (src/server.py)
        │  onClick                                │
        ▼                                         │  routers mounted per app
   page hook  (useTimelineSocket)                 │  under /api/<app>
        │                                         │
        ├──► postCommand() ──── POST ─────────────┼──► command endpoint
        │    lib/commands.ts   ?client_id=…       │      mutates that client's state
        │                      ◄── CommandAck ────┤      then pushes ↓
        │                                         │
        └──► useServerSocket() ◄══ WS frames ═════┴──◄ push task / ConnectionManager
             hooks/useServerSocket.ts   {type:"state",…}
```

Both resolve against **the origin the page was served from**. There is no backend
picker, and dev is made to look like production rather than special-cased.


Where the contract comes from
==

**The Python is the source; everything else is generated.** The api is defined by
ordinary server code — each app package's `router` (its paths, `operation_id`s,
body models and `CommandAck` replies) and the `WS_MESSAGE` model it pushes down
its socket. `api_contract.py` does *not* define the api: it describes and
assembles the document, and adds the socket half OpenAPI has no notion of.

```
   src/<app>/  and  src/pswamp_web/<app>/        ← THE DEFINITION. Edit here.
     router       POST paths, operation_id, body models, CommandAck
     WS_MESSAGE   the pydantic model this package pushes
        │
        ▼
   src/api_contract.py                            ← metadata + the socket half
     title, API_VERSION, description, tag text
     walks APPS for WS_MESSAGE → components.schemas + x-websocket-channels
     install() overrides app.openapi()
        │
        ▼
   tools/dump_openapi.py       imports `server`, asks it for its own document
        │
        ▼
   doc/api/openapi.json        generated, committed — and served live at
        │                      /openapi.json, /docs, /redoc
        │  openapi-typescript
        ▼
   app/client-web/src/api/schema.ts               generated, committed
        │
        ├──► src/api/wire.ts      →  Wire['TimelineState']  in each page hook
        └──► src/lib/commands.ts  →  postCommand, typed against the paths
```

Two things follow, and they are the whole reason it is arranged this way:

- **Don't edit the two generated files.** A change to the api goes into the
  Python — a route, a model, or the metadata in `api_contract.py` — then run
  `./scripts/generate-api-contract.sh`, then commit all of it together. Hand-edit
  `openapi.json` or `schema.ts` and the next regeneration silently discards you.
  See [Changing the api](#changing-the-api).
- **The committed contract cannot disagree with the running server**, because
  `dump_openapi.py` imports the app and asks for its document rather than
  describing the api a second time. `generate-api-contract.sh --check`
  regenerates into a temp dir and diffs; `error_check.sh` runs that on every push.

[Where the pieces live](#where-the-pieces-live) is the file-by-file table.


Common tasks
==

Recipes for the things people actually come here to do. Each ends the same way —
regenerate and commit — so that step is stated once, at the end.

### I want to add a command (a new POST) to an existing app

In that app's `api.py`. Commands are one url per operation, never one endpoint
taking an action name:

```python
@router.post("/playback/pause", operation_id="timeline_pause")
async def pause(client_id: ClientId) -> CommandAck:
    """One line saying what it does — this becomes the description in /docs."""
    ...
```

- **`operation_id` is required and is `<app>_<verb>`**, app in snake_case
  (`time_window_resync`, `pmu_test_streamer_play`). It is the name a generated
  client will call, so it is chosen, not derived — renaming the handler must not
  rename someone's generated method.
- **`ClientId` and `CommandAck`** come from `shared` in a standalone app, and from
  `..wire` inside `pswamp_web/` (which may not import the rest of the backend).
- **Taking a body?** Declare it as a pydantic model in the same file and add it as
  a parameter — `body: SequenceSelection`. That is what makes a malformed command
  a 422 instead of a handler crash.
- **Never return the new state.** `CommandAck` only; state goes down the socket.

Client side, one call — the path is checked against the contract, so a typo is a
`tsc` error:

```ts
const pause = useCallback(
  () => fire(postCommand(`${TIMELINE_API_PATH}/playback/pause`)),
  [],
)
```

### I want to add a field to a socket message

Add it to the message model — `pswamp_web/wire.py` for the p-SWAMP apps,
the app's own `api.py` for a standalone one — and fill it in wherever
`state_message()` (or the page's push task) builds it.

After regenerating, `Wire['<Model>']` in the client has the field. It does **not**
reach the page automatically: each hook maps the wire shape to its own camelCase
domain type, so add it there too if the page should see it. That mapping is
deliberately hand-written — the server's field names are the server's business.

### I want to add a whole new page and its api

```
./scripts/generate-new-subapp.sh flow-map "Flow Map"
```

That writes both halves, patches the four registries, regenerates the contract and
runs the checks. The new app is in the contract with **no registry entry to add** —
see "How a package joins the contract" below. Commit the two regenerated artifacts
with the rest.

### I want to add a plain GET (something static, not pushed)

Same as a command, but **declare a response model** — a bare `-> dict` publishes as
an untyped `object` and the client is left casting an implicitly-`any` body:

```python
@router.get("/channels", operation_id="time_window_channels")
async def list_channels() -> ChannelCatalogue:
    return ChannelCatalogue(channels=_channels())
```

Only for things that never change (the grid topology, the channel catalogue).
Anything that changes belongs on the socket.

### `error_check.sh` is failing on "api contract (spec matches code)"

You changed the api and the committed contract is stale. It prints the diff:

```
./scripts/generate-api-contract.sh      # regenerate both artifacts
```

then commit `doc/api/openapi.json` and `app/client-web/src/api/schema.ts` with your
change. Nothing to hand-edit — that is the whole fix.

### How do I see what the api looks like right now?

With a server running, `/docs` (Swagger UI, grouped by app) or `/redoc`. Without
one, the committed file answers most questions:

```
jq '.paths | keys' doc/api/openapi.json                  # every HTTP operation
jq '.["x-websocket-channels"]' doc/api/openapi.json      # every socket channel
jq '.components.schemas | keys' doc/api/openapi.json     # every model
```

### I renamed a field. Is that a breaking change?

Yes — for anyone generating against the contract, a rename is a removal plus an
addition. Bump `API_VERSION` and say so in the pull request. Adding an optional
field is not breaking and needs no bump. See "Versioning" below.

### Do I need to regenerate after changing only a docstring?

Yes. Docstrings become operation descriptions in the document, so they are part of
the generated artifacts and the check notices. It is the same one command.

------------------------------------------------------------------------------

*The rest is reference: what the contract guarantees, then how the two transports
actually work underneath it.*


What the contract is
==

**`doc/api/openapi.json`** — an OpenAPI 3.1 document, generated from the server
and committed. It describes:

- **every command**, as an HTTP `POST` operation with a stable `operationId`, its
  path and query parameters, its request body and its `CommandAck` reply;
- **the two plain GETs** — the grid topology and the channel catalogue;
- **`GET /healthz`**;
- **every WebSocket message**, as a schema under `components.schemas`, with the
  channels themselves listed under the `x-websocket-channels` extension.

**`app/client-web/src/api/schema.ts`** — TypeScript generated from that document
by `openapi-typescript`, also committed. The web client reads it through
`src/api/wire.ts`, which is the small hand-written layer over it.

The running server serves the identical document at **`/openapi.json`**, with
**`/docs`** (Swagger UI) and **`/redoc`** rendering it. Both come from the same
function that writes the file, so the served and committed copies cannot disagree
— and `scripts/error_check.sh` proves it on every push.

> Swagger UI at `/docs` fetches its own assets from a CDN, so it needs the browser
> to have internet. `/openapi.json` and `/redoc`'s data have no such dependency.


Which subapp a request belongs to
==

The contract is organised the same way the code is. Every operation is tagged
with its app's url name — `timeline`, `time-window`, `islanding` — which is the
`/api/<app>` segment, the server package name with underscores, and the web
client's page folder. So `/api/time-window/selection` is `time_window/` on the
server and `pages/grid-monitor/time-window/` in the client, and Swagger UI groups
it under `time-window`. One name, four places, no lookup table.

`operationId` follows the same rule: `<app>_<verb>`, e.g. `time_window_resync`,
`islanding_acknowledge`. Those are the names a generated client will use, so they
are chosen rather than derived — a renamed handler must not rename someone's
generated method.


How a package joins the contract
==

**By exporting a name.** There is no registry to add an entry to.

`server.py` already discovers optional package features with
`getattr(module, "lifespan", None)`. Socket messages work identically — a package
exports the model it pushes:

```python
# app/server-python/src/timeline/__init__.py
from .api import TimelineState, lifespan, router

WS_MESSAGE = TimelineState

__all__ = ["WS_MESSAGE", "TimelineState", "lifespan", "router"]
```

`api_contract.py` walks the same `APPS` list `server.py` mounts and collects
whatever each package exports under that name. An app with no socket
(`pswamp_web/grid/`, which is HTTP only) simply omits it.

That is why `scripts/generate-new-subapp.sh` needed no new anchor to patch: the
template exports `WS_MESSAGE`, so a scaffolded subapp is in the contract the
moment it is generated. Commit the two regenerated artifacts along with it — the
script regenerates them for you, before it runs the checks.

The one rule that keeps this working: **a socket payload must be a pydantic
model.** Push a bare dict and the app silently leaves the contract — the page
still works, and the type safety is gone with nothing to say so. `ConnectionManager.send_to_client`
and `wire.send_state` both take a `BaseModel` for this reason (and because
`json.dumps` emits bare `NaN`, which `JSON.parse` rejects).


Changing the api
==

1. Change the Python — an endpoint, a body model, or a message model.
2. Run `./scripts/generate-api-contract.sh`.
3. Commit `doc/api/openapi.json` and `app/client-web/src/api/schema.ts` with the
   change.

If you skip step 2, `./scripts/error_check.sh` fails — locally in the pre-push
hook and again in CI — with a diff of what moved. That is deliberate: the point
of committing the contract is that a change to it is **reviewable**, so it has to
be in the same pull request as the code that caused it.

Don't hand-edit either generated file. `schema.ts` says so at the top, and the
next regeneration would overwrite you.

### Versioning

`API_VERSION` in `app/server-python/src/api_contract.py` is bumped **by hand, on
a breaking change** — a removed or renamed field, a removed operation, a narrowed
type. Additive changes don't need one.

It is not automated on purpose. A version that moves on every commit tells a
reader nothing; one that moves only when compatibility breaks is the single thing
in the diff that says "this one needs coordinating". If you bump it, say so in the
pull request — see "The api contract" in `how-we-work-together.md`.


Why a WebSocket extension rather than AsyncAPI
==

OpenAPI describes HTTP operations. This api pushes **all** of its state over
WebSockets, so a plain OpenAPI document would describe the smaller half and leave
the part a client actually renders undescribed — which is exactly the state this
repo was in, with seven hand-written TypeScript mirrors of Python models and no
check that they matched.

AsyncAPI is the standard answer and was deliberately not chosen: a second spec
format means a second generator, a second drift check and a second thing for a
new team to learn, in order to describe seven one-way channels that all carry a
single message shape. Instead the message models are merged into
`components.schemas` — where every generator already looks — and the channels are
recorded in a vendor extension:

```jsonc
"x-websocket-channels": [
  {
    "path": "/api/time-window/ws",
    "app": "time-window",
    "direction": "server-to-client",
    "message": { "$ref": "#/components/schemas/TimeWindowSlice" }
  }
]
```

`x-` keys are legal OpenAPI and generators ignore what they do not recognise, so
the document stays valid everywhere and the schemas — the part codegen consumes —
come through as ordinary models. A consumer that wants the channel map reads the
extension; one that only wants types never notices it is there.

If the socket protocol ever grows a second message shape per channel, or an
upstream direction, revisit this. Today every channel is downstream-only and
carries one model, and the extension says exactly that.


Consuming it from another codebase
==

`doc/api/openapi.json` is a normal OpenAPI 3.1 document; point any generator at
it. For reference, this repo's own web client uses:

```
npx openapi-typescript doc/api/openapi.json -o schema.ts
```

Three things worth knowing before you generate against it:

- **Fields are snake_case**, as the server sends them. This repo's client maps to
  camelCase in each page hook rather than at the wire, so the server's field names
  stay the server's business.
- **`client_id` is required on everything** — every command and every socket — as
  a query parameter. It is a numeric string, one per browser profile, and all of
  one client's sockets must send the same value or they get separate server-side
  pipelines. It is **not** authentication.
- **A missing measurement is `null`, not `NaN`.** The models declare
  `float | None` and the server substitutes; a `TimeWindow` is all-`null` until it
  fills, which is normal and not an error.


Addressing: origin, prefix, client id
==

Three things decide where a request goes and whose state it touches. All three
are settled in `app/client-web/src/lib/`.

**Origin** — `servers.ts` builds every url from `window.location`:

| helper | produces | used by |
|---|---|---|
| `resolveServerUrl(wsPath)` | `ws(s)://<host><BASE_PATH><path>` | the one WebSocket, in `useServerSocket` |
| `resolveApiUrl(path)` | `http(s)://<host><BASE_PATH><path>` | `postCommand`, and the two plain GETs |

The protocol follows the page's (`https:` → `wss:`), so nothing has to know
whether it is running behind TLS.

**Prefix** — `basePath.ts` discovers the mount prefix at runtime by reading its
own `import.meta.url` and cutting at `/assets/`. Remotely the app sits under
`/p-swamp/` behind a reverse proxy that strips the prefix before forwarding, so
the *server* sees plain `/api/...` and the *browser* must put it back. This is
what lets one published image run both at a root and under a prefix. It rests on
routes staying one segment deep — see that file.

**Client id** — `clientId.ts` resolves one random integer per browser profile,
persisted in `localStorage`, and every socket and every command sends it:

- sockets as `?client_id=` on the WebSocket url (`useServerSocket`),
- commands as `?client_id=` on the POST (`postCommand` attaches it, so no caller
  passes it).

It is the sharding key for all server state. One id per browser is what makes the
grid monitor's five sockets views of **one** server-side pipeline rather than
five. It is **not authentication** and does not pretend to be: send someone
else's id and you drive their replay.

In dev the same urls work because Vite proxies the whole `/api` prefix —
including WebSocket upgrades — to the backend on `:8000` (`server.proxy` in
`vite.config.ts`). In the shipped image the server serves the client itself, so
every request is same-origin by construction.


Downstream: how state reaches the screen
==

Client side
--

`useServerSocket(wsPath, options)` (`src/hooks/useServerSocket.ts`) is the whole
connection half, shared by every app:

1. opens one WebSocket to `resolveServerUrl(wsPath) + '?client_id=' + CLIENT_ID`;
2. reconnects every 2 s while down — **except** after close codes `1008` (id
   rejected) and `1013` (server at pipeline capacity), which are refusals the
   server meant and which retrying would only make worse;
3. on each frame, `JSON.parse`, drop anything whose `type` is not `"state"`, then
   either store it (`message`) or hand it to `options.onMessage`;
4. tracks a `status` — `connecting` / `online` / `offline` — distinguishing "never
   reached the server" from "an established connection dropped".

It knows nothing about message *shape*. Each page's own hook maps the raw payload
to a typed object, which is also where snake_case becomes camelCase.

**The `onMessage` escape hatch matters.** Storing a message in React state costs
a render pass. That is right for a small payload drawn as SVG (phasors,
islanding) and wrong for a 50 Hz sample stream: `useTimeWindowSocket` keeps its
samples in a `useRef` and notifies the chart through its own `subscribe`, so ten
messages a second produce zero renders. React only re-renders when something it
owns changes — the channel list, or the connection status.

Server side
--

`src/server.py` is wiring only. It mounts each app package's `router` under its
prefix from `APPS`, tags it with the app's url name, composes every package's
`lifespan` into one, serves `/healthz`, and mounts the built client at `/` last
(a mount at `/` is greedy and would shadow anything registered after it).

From there the two families of app diverge.

**The scaffold demos** (`timeline/`, `pmu_test_streamer/`) keep it minimal:

- `states: dict[str, ClientState]` — the entire store, keyed by client id;
- `ConnectionManager` (`src/shared.py`) — client id → the set of live sockets,
  pure transport, one instance per app;
- one `ticker()` task started by the package's `lifespan`, which each tick
  advances only the clients that are playing and sends each its own
  `state_message()`.

A client id may briefly hold several sockets (a reconnect overlapping the dying
one), hence a set; `send_to_client` iterates a snapshot and drops any socket that
fails mid-send, so one dead connection cannot break delivery to the others.

**The p-SWAMP layer** (`src/pswamp_web/`) is the real one, and has a pipeline
behind it. Every endpoint opens the same way:

```python
async with connected_hub(ws) as hub:
    if hub is None:
        return
```

`connected_hub` (`pswamp_web/hub.py`) parses `?client_id=`, accepts the socket,
and acquires that client's `Hub` for the life of the connection. Note the
ordering of the two refusals, which is deliberate: **no usable id** is closed
*before* accepting, i.e. the handshake is rejected outright; **at capacity** is
accepted *first* and then closed with `1013`, because a close code only reaches
the browser on an established connection, and the client treats `1013` as
terminal.

A `Hub` is one client's pipeline: a `RecordingPlayer` over the committed Nordic
44 recording, three p-SWAMP monitoring applications each with their own reader off
that player, and the alarm/status/island stores. `HubRegistry` is the only thing
that constructs one, and it enforces three rules — one pipeline per client however
many sockets (a per-client lock, so the monitor's five simultaneous first-connects
build one rather than racing to build five), a pipeline outlives its sockets by
`IDLE_EVICT_SECONDS` so a reload rejoins the same stream, and never more than
`MAX_PIPELINES`.

Then each page picks one of **two delivery patterns**:

| pattern | apps | how it works |
|---|---|---|
| **Poll the window** | `time_window` (10 Hz), `phasors` (5 Hz), `app_status` (2 Hz) | an `asyncio` task reads the client's window or stores on its own timer and sends |
| **Subscribe to the bus** | `islanding`, `line_outage` | a listener on `hub.bus` offers into an `asyncio.Queue`; the push task blocks on that queue |

Polling is the direct translation of what the Qt widgets do, and it is what keeps
a 50 Hz sample stream from becoming 50 event-loop callbacks a second. Subscribing
suits results and events, which arrive about once a second or less.

Both bus-driven pages re-read the payload from the hub rather than carrying it on
the queue, so a dropped notification costs latency and never content.

Every page sends through **`send_state()`** (`pswamp_web/wire.py`), never
`ws.send_json`:

```python
await ws.send_text(message.model_dump_json())
```

`json.dumps` emits bare `NaN` and `Infinity` tokens, which `JSON.parse` rejects
outright — and NaN is the *normal* case here, since a `TimeWindow` is all-NaN
until it fills. Routing every page through one function means a new message type
cannot quietly reintroduce that. The pydantic models in `wire.py` are the schema
the Qt front end never needed, because its widgets read the very `TimeWindow`
object the application thread writes into.

The thread seam
--

p-SWAMP's monitoring applications are upstream code and run as plain daemon
threads looping on a blocking read. That is kept exactly as it is. They cross into
the event loop at **exactly two places**:

1. **`Bus.publish_threadsafe()`** (`pswamp_web/bus.py`) → `loop.call_soon_threadsafe`
   → `_deliver` on the loop → synchronous listeners (the stores) and queued
   subscriptions (the pages). The bus is **per pipeline**, so a listener only ever
   hears its own client's results.
2. **`CountingTimeWindowLabeled.snapshot()`** (`pswamp_web/replay.py`) — reads the
   append counter *and* the data under the window's own lock, so the two describe
   the same instant.

Everything else — WS handlers, push tasks, request handlers — is cooperatively
scheduled on the one loop and never truly parallel, so none of it needs a lock.
Adding a third seam is how that stops being reviewable.

The delta protocol
--

`time_window` is the one page where the naive thing does not work. A 30 s window
of 8 channels at 50 Hz is 12,000 numbers; re-sending it 10×/s is roughly a
megabyte per second per client. So it sends the window once (`mode: "full"`) and
after that only rows that are new (`mode: "append"`) — measured at ~5.9 KB/s
against ~1.4 MB/s.

The counter on the window is what makes that possible: `new_rows = appended -
state.last_appended`. Two cases force a full message instead — a fresh selection
(the client's traces are for different channels entirely), and a client that has
fallen behind by more than the window, where an append would splice unrelated
data onto what it already has.


Upstream: how a command reaches the server
==

Client side
--

A page hook exposes one named function per operation, each wrapping
`postCommand` from `src/lib/commands.ts`:

```ts
const play = useCallback(
  () => fire(postCommand(`${TIMELINE_API_PATH}/playback/play`)),
  [],
)
```

`postCommand(path, options?)` is the only place anything goes upstream. It fills in
any `{placeholder}` in the path from `options.path` (url-encoding each value),
resolves the url, appends `?client_id=`, sets `Content-Type` only when there is a
body, and throws a `CommandError` carrying the status and FastAPI's `detail` on any
non-2xx. Both the path and the body are typed against the generated contract, so a
typo is a `tsc` error rather than a 404 — see "Changing the api" above.

Callers log and carry on (`fire` in each hook). A command is fire-and-forget from
the UI's point of view: the resulting state arrives on the socket or not at all,
so a failed command produces no change — which is what the user already sees.
Controls stay `disabled={!connected}` for the same reason: the POST would reach
the server without a socket, but its result would have nowhere to arrive.

Server side
--

Every command endpoint declares its caller and its reply the same way:

```python
@router.post("/playback/play", operation_id="timeline_play")
async def play(client_id: ClientId) -> CommandAck:
    ...
```

- **`ClientId`** — an annotated query parameter, so FastAPI validates it before
  any handler runs (missing or non-numeric is a 422).
- **`CommandAck`** — `{status, applied}`. Deliberately not the new state.
- **`operation_id`** — an explicit, readable name per operation.
- Bodies are pydantic models (`SequenceSelection`, `ChannelSelection`,
  `AlarmNote`), so a malformed command is a 422 rather than a handler crash.

`shared.py` holds `ClientId` and `CommandAck` for the scaffold apps.
`pswamp_web/wire.py` keeps deliberate **twins** of both, because that package may
not import the rest of the web backend — it is written to move into the desktop
package as `pswamp/web/`. Change one pair and change the other.

Both `ClientId`s are a `str` matching `^\d{1,20}$` — the exact rule
`read_client_id` applies to a socket's query parameter, in both `shared.py` and
`pswamp_web/hub.py`. They must agree, or a page's commands would address
different state from its sockets. (They once did not: `shared.py` had an `int`
with `ge=1` while `wire.py` had this pattern, which published two schemas for one
identity and disagreed about `"0"`.)

Then the handler has to do two things: change the right state, and make sure the
change reaches the screen. There are three arrangements, because the state lives
in three different places.

**1. State in a module dict** — `timeline`, `pmu_test_streamer`, the scaffold
template. The simplest case, and it needs no new plumbing: `ConnectionManager`
already addresses a client id and already runs on this same event loop.

```
POST /api/timeline/playback/play?client_id=42
  → get_state("42").playing = True
  → log_event("play", "42")
  → manager.send_to_client("42", state_message(state))   ← the push, from an HTTP handler
  → 200 CommandAck(applied="play")
```

**2. State in the client's pipeline** — `islanding`. The hub is reachable by
client id, but the *push task* is not: it is blocked on its queue inside a socket
handler. So the endpoint mutates and then wakes it:

```
POST /api/islanding/alarms/<uuid>/acknowledge?client_id=42
  → live_hub("42")                       ← 404 if this client has no pipeline
  → hub.alarms.annotate(uuid, …)         ← False for an unknown alarm → 404
  → _nudge("42")  → _offer(queue, None) for each of this client's open views
  → push task wakes, re-reads the hub, send_state()
  → 200 CommandAck(applied="acknowledge")
```

An operator action changes the alarm list, which no application publishes an
event for; without the nudge the page would show it only when the islanding
detector next produced a result, up to a second later. Measured round trip with
it: ~1 ms.

**3. State on the connection itself** — `time_window`. Which channels a view has
selected is per-*connection*, held in a local variable inside the socket handler,
and a command arrives on no connection at all. `SessionRegistry`
(`pswamp_web/sessions.py`) is the fix: the handler publishes its `ClientState`
for the life of the socket, and the command finds it by client id.

```
POST /api/time-window/selection?client_id=42   {"channels": [5, 9]}
  → live_hub("42")
  → channels.sanitise(body.channels, tw.n_cols)   ← untrusted input, clamped
  → for each of this client's open views: selection = …, needs_full = True
  → 200 CommandAck(applied="select_channels (2)")
  → …no push here. The 10 Hz task sees needs_full on its next tick and sends
    the mode:"full" message by itself.
```

A client may hold several sessions for one endpoint (two tabs), so a command
applies to all of them — one browser is one viewer, and its views should agree.
The registry stores them in a **list**, not a set: these are `eq=True` dataclasses,
which Python makes unhashable.

Two rules that hold across all three
--

- **A command never builds a pipeline.** `live_hub(client_id)` peeks; it never
  calls `REGISTRY.acquire`. Acquiring would cost four threads and ~30 MB against
  `MAX_PIPELINES` for a replay nobody is watching, and the POST has no socket to
  deliver results to. No pipeline is a 404 — in practice, "you have no page open".
- **A socket that receives nothing still needs its receive loop.** Every WS
  handler ends in `while True: await ws.receive_text()`. Without a pending
  receive, a closed socket is only noticed on the next send, so an idle client
  lingers indefinitely — holding a pipeline slot, in the pswamp_web case.


The plain GETs
==

Two endpoints are neither commands nor pushed state, because what they return
never changes: the grid topology (`GET /api/grid/model`) and the channel
catalogue (`GET /api/time-window/channels`). A page fetches each once on mount
via `resolveApiUrl`, and the browser is free to cache it. Putting them on a
socket would mean every consumer's hook had to handle a second message shape to
receive something static.


Error and refusal semantics
==

| condition | how it surfaces |
|---|---|
| missing, empty, negative or non-numeric `client_id`, or more than 20 digits | `422`, from FastAPI validation, before the handler |
| malformed command body | `422`, from the pydantic model |
| unknown sequence name | `422` naming the valid ones |
| unknown alarm uuid | `404` |
| command for a client with no pipeline | `404` |
| command for a client with no open view (`time-window`) | `404` |
| socket with an unusable client id | close `1008`, **before** accept — client stops retrying |
| socket when every pipeline is in use | accept, then close `1013` — client stops retrying |
| unknown path under `/api/` | real `404`, never the SPA shell (`SPAStaticFiles._NO_FALLBACK`) |

`"0"` is *accepted*, incidentally — the rule is "numeric and at most 20 digits",
not "a positive integer". Nothing generates it (the browser picks a random
positive integer) and it is a valid key like any other; it is called out only
because an earlier `ge=1` on the scaffold apps rejected it, so the two halves used
to disagree.

Which is the practical difference the two transports make: a rejected command can
say *why*, with a status code, in the access log and the Network tab.


Where the pieces live
==

| Path | What |
|---|---|
| `app/server-python/src/api_contract.py` | Document metadata, `WS_MESSAGE` collection, the extension, schema-name collapsing |
| `app/server-python/tools/dump_openapi.py` | Imports the app, writes the document to a file |
| `scripts/generate-api-contract.sh` | Regenerates both artifacts; `--check` diffs instead |
| `doc/api/openapi.json` | The contract (generated, committed) |
| `app/client-web/src/api/schema.ts` | TypeScript (generated, committed, not linted) |
| `app/client-web/src/api/wire.ts` | Hand-written: `Wire[...]` and `ApiPaths` |
| `app/client-web/src/lib/commands.ts` | `postCommand`, typed against `ApiPaths` |
| `app/client-web/src/hooks/useServerSocket.ts` | The connection half, shared by every page |
| `app/client-web/src/lib/servers.ts` | `resolveApiUrl` / `resolveServerUrl` and the path consts |
| `app/server-python/src/server.py` | Wiring: mounts routers from `APPS`, composes lifespans |
| `app/server-python/src/shared.py` | `ClientId`, `CommandAck`, `ConnectionManager`, `read_client_id` |
| `app/server-python/src/pswamp_web/wire.py` | Every p-SWAMP message model, and `send_state` |
| `app/server-python/src/pswamp_web/hub.py` | One pipeline per client, and the registry over them |


Implementation notes
==

**Duplicate schema names are collapsed.** `CommandAck` is declared twice on
purpose — in `shared.py` and in `pswamp_web/wire.py` — and the reason is the
repo's central compromise: **one analysis core, two front ends.** The p-SWAMP web
layer is written to move into the desktop package as `pswamp/web/`, a third
presentation adapter beside `gui/` (PySide6) and `visualization/`, so it may not
import anything from the rest of the web backend — `shared.py` included, which is
where the scaffold apps keep their copy. Two packages that may not share a module
need two declarations of the same four-line model. The duplication is the price
of the Qt and web front ends living over one core, paid here rather than in the
core itself.

The cost lands in the published contract, not in the code. Pydantic
disambiguates same-named classes by *module path*, so the reply to all fourteen
commands would otherwise publish as `shared__CommandAck` and
`pswamp_web__wire__CommandAck` — two names for one concept, one of them baking in
a path that is documented to be moving, which would rename a schema in every
consumer's generated code the day that move happens. Both classes therefore set
`model_config = ConfigDict(title="CommandAck")`, and `collapse_titled_twins` folds
structurally identical twins back to that title. Twins that genuinely diverge keep
their separate names, which is the correct outcome for two different shapes.

**Don't expect this to expire on its own**, and note which half is the workaround.
The *duplication* is the standing compromise; `collapse_titled_twins` is what
keeps it out of the contract, and it is the piece consumers depend on. Whether the
duplication ever goes away is decided by §7 of
`WIP-context-port-from-qt-to-web-frontend.md`, and the answer is the opposite way
round from the intuition: if **Qt stays**, `pswamp_web/` moves *into* the desktop
package and still cannot import the web backend's `shared.py`, so the twin becomes
permanent. Only if **Qt is retired** and the two Python projects merge could one
`CommandAck` serve everything. Until that is settled, treat the twin as
load-bearing and keep the two in step.

**`openapi-typescript` declares a stale peer.** It wants `typescript@^5.x` while
this project is on 6; it drives the TS 6 compiler API without complaint (checked),
so `app/client-web/package.json` carries an `overrides` entry resolving that peer
to the project's own TypeScript. That keeps plain `npm install` and `npm ci`
working with no flags, rather than pinning everything to `--legacy-peer-deps`. The
visible cost: `scripts/update-dependencies.sh` runs `npm-check-updates --peer`,
which will decline to bump TypeScript past 5 while the stale range stands, and
says so in its report. Drop the `overrides` entry when upstream widens the range.
