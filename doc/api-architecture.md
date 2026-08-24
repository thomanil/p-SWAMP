# The api, end to end

How a click in the browser becomes a change on the server, and how server state
gets back to the screen. This traces the actual call path through both halves of
`app/`, naming the file at each hop.

`AGENTS.md` states the rules; this explains the machinery behind them.
`client-server-rig.md` is the shorter tour of the whole rig.


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

- `states: dict[int, ClientState]` — the entire store, keyed by client id;
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

`postCommand(path, body?)` is the only place anything goes upstream. It resolves
the url, appends `?client_id=`, sets `Content-Type` only when there is a body, and
throws a `CommandError` carrying the status and FastAPI's `detail` on any non-2xx.

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
package as `pswamp/web/`. Change one pair and change the other. (`ClientId` is
`int` in `shared.py` and `str` in `wire.py`: pipeline keys are strings, and the
pattern there is exactly the rule `read_client_id` applies to the socket's query
parameter. They must agree, or a page's commands would address a different
pipeline from its sockets.)

Then the handler has to do two things: change the right state, and make sure the
change reaches the screen. There are three arrangements, because the state lives
in three different places.

**1. State in a module dict** — `timeline`, `pmu_test_streamer`, the scaffold
template. The simplest case, and it needs no new plumbing: `ConnectionManager`
already addresses a client id and already runs on this same event loop.

```
POST /api/timeline/playback/play?client_id=42
  → get_state(42).playing = True
  → log_event("play", 42)
  → manager.send_to_client(42, state_message(state))   ← the push, from an HTTP handler
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
| missing / non-numeric / zero `client_id` | `422`, from FastAPI validation, before the handler |
| malformed command body | `422`, from the pydantic model |
| unknown sequence name | `422` naming the valid ones |
| unknown alarm uuid | `404` |
| command for a client with no pipeline | `404` |
| command for a client with no open view (`time-window`) | `404` |
| socket with an unusable client id | close `1008`, **before** accept — client stops retrying |
| socket when every pipeline is in use | accept, then close `1013` — client stops retrying |
| unknown path under `/api/` | real `404`, never the SPA shell (`SPAStaticFiles._NO_FALLBACK`) |

Which is the practical difference the two transports make: a rejected command can
say *why*, with a status code, in the access log and the Network tab.


Where OpenAPI comes in
==

FastAPI generates `/openapi.json` (and `/docs`, `/redoc`) from the endpoints
themselves — currently 17 operations, each with its own `operationId`, tagged by
app via the `include_router` loop in `server.py`. WebSocket endpoints are absent
by nature: OpenAPI describes HTTP operations, and the command surface is the half
that is HTTP.

Fuller OpenAPI/Swagger integration is a separate, later step. What exists today is
what the endpoints declare on their own; the pydantic bodies, the explicit
`operation_id`s and the per-app tags are the groundwork it will build on.

Note `/docs` fetches Swagger UI's own assets from a CDN, so it needs the browser
to have internet; `/openapi.json` has no such dependency.


Adding to either half
==

`AGENTS.md` has the checklists — "Adding a page", "Adding a backend api", "Adding
a p-SWAMP view" — and `./scripts/generate-new-subapp.sh <url-name> "<Nav Label>"`
performs the first two for you, leaving a working per-client counter with both
halves wired: `POST /api/<slug>/count/bump` to change it, the socket to see it.
