# Possible performance issues to follow up

Notes from a review of the streaming path, in the context of P-SWAMP's actual
target: streaming energy grid data from backend streams to the browser, where
**throughput and latency matter**. 

These are the things that may not survive contact with a real feed.

**Verdict on the stack itself: the framework choices seem sound.** FastAPI /
Starlette on `uvicorn[standard]` (uvloop + httptools + `websockets`) is a
legitimately fast Python WebSocket stack, and React/Vite is fine as a shell. The
risk is entirely in the *patterns* baked into the streaming path.

Items are roughly ordered by expected impact.

## Server side

### 1. One slow client stalls every client

`ticker()` awaits each send sequentially
(`app/server-python/src/pmu_test_streamer/api.py:141-144`). `await
ws.send_json(...)` awaits the transport, so a client on a bad link applies
backpressure to the **shared** ticker loop and every other client hitches with
it. This is the worst structural issue for a low-latency target.

Fix: per-client outbound queues with an explicit overflow policy. For telemetry
that policy should be **conflate / drop-latest**, not buffer — a stale PMU frame
delivered late is worse than one never delivered at all.

### 2. The whole window is re-serialized and re-sent every tick

`visible_window()` (`app/server-python/src/pmu_test_streamer/model.py:61`)
returns 9 records when 1 changed, and `send_json`
(`app/server-python/src/shared.py:74`) runs stdlib `json.dumps` per client per
tick. At 100 Hz that is ~900 records/s/client of redundant encode, transmit and
parse.

Fix: deltas plus periodic keyframes. Roughly a 9x cut on the wire and in the
browser's parse cost.

### 3. `json.dumps` is the wrong encoder at this rate

Swap for `orjson` or `msgspec` (5-10x on encode). For a real fan-out, encode
**once** and `send_bytes` / `send_text` the pre-encoded frame to every client,
rather than letting Starlette re-encode per socket.

### 4. permessage-deflate is on by default and unexamined

uvicorn's `websockets` implementation negotiates compression by default. For
small high-frequency frames that is CPU burned for little gain, and it adds
latency. Set `ws_per_message_deflate=False` deliberately — or deliberately leave
it on if measurement shows we are bandwidth-bound rather than CPU-bound. Today
it is neither decision, just a default.

### 5. GIL / single process is the real ceiling

Fine today: the work is I/O-bound with tiny payloads on one event loop. Once
real upstream PMU frames are decoded and transformed in Python, it is one core.

Order of mitigation: faster codec → numpy batch ops instead of per-record Python
→ move ingest off the event loop → a different runtime for the hot path. The
architecture here (thin client, one wire protocol, no shared state) makes that
last option a tractable swap rather than a rewrite — worth preserving that
property.

### 6. Two smaller ones

- `states` is never evicted (`app/server-python/src/pmu_test_streamer/api.py:45`).
  Documented as an acceptable leak for a demo; with a live feed and real client
  counts it stops being bounded.
- `except Exception: pass`
  (`app/server-python/src/pmu_test_streamer/api.py:209-210`) hides exactly the
  failures we will be chasing once we start pushing load.

## Client side

### 7. One React render per message

`setMessage` fires per message
(`app/client-web/src/hooks/useServerSocket.ts:58-62`), so at 100 Hz React is
asked to reconcile 100x/s against a 60 Hz display. This is the client-side
equivalent of issue 1.

Fix: accumulate into a `useRef` ring buffer and flush on `requestAnimationFrame`.
React / shadcn / Tailwind stay for the chrome; they should never be in the data
path.

### 8. `JSON.parse` runs on the main thread

`app/client-web/src/hooks/useServerSocket.ts:59`, competing with rendering. At
high rates, move the socket and the decode into a Web Worker and transfer typed
arrays across.

### 9. No charting library chosen yet — the biggest open decision

Pick a canvas/WebGL renderer (uPlot is the reference point for fast 2D time
series; regl / deck.gl if it needs to go bigger). Anything SVG-based — Recharts,
D3-SVG, Victory — creates a DOM node per point and will fall over on streaming
waveforms. Worth deciding before more UI accretes around the current DOM-row
rendering.

## Protocol

### 10. JSON text frames are the choice to revisit first

PMU data is fixed-schema and numeric — the ideal case for a binary frame decoded
straight into a `Float32Array` and handed to a canvas with no object allocation.
Large constant-factor win on both ends.

## What to do first

For a project whose stated priorities are throughput and latency, **nothing in
the repo currently measures either.**

Before optimizing anything above: add a synthetic load generator (N simulated
clients at realistic rates) and an end-to-end timestamp — server tick to browser
paint — so these choices become measured rather than argued.

The expectation is that items 1 and 7 dominate and the rest is noise until they
are fixed. That is a hypothesis to test, not a conclusion.
