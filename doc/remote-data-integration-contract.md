# Remote data integration contract

> **Status:** Preliminary contract implemented by the current branch and its
> local stub. This is a starting specification for developing a deployment-side
> remote data service in parallel with the rest of the p-SWAMP architecture.
> The production questions under "Not settled yet" remain open.
>
> **This document is the contract.** It is written in terms of HTTP alone, so a
> service can be built on any stack, and it is the reference for p-SWAMP's
> client and the stub in this repo alike. Where the client's docstring or the
> stub's code say more, they describe one implementation, not the contract.
> `scripts/check-remote-data-service.sh` checks a running service against it
> over plain HTTP (see "Checking a service").

## Summary

This contract lets p-SWAMP query deployment-owned historical PMU data without
depending on how or where that data is stored. A remote data service answers
two HTTP requests: a coverage request, and a range query whose response body
streams the matching `pmu.frame` records back one JSON line at a time, closed
by a terminal line. There is no second channel: the connection that asked is
the one that answers, and closing it cancels the query. The implemented
contract is suitable for parallel integration work, while production concerns
such as security, limits, and schema governance remain to be agreed.

## Purpose

The point of the contract is decoupling. p-SWAMP needs to ask for a range of
history and get it back; it should not need to know which database holds that
history, what its schema is, or whether the deployment changes either later.
So p-SWAMP does not talk to a store at all. It talks to a small remote data
service the deployment runs in front of its store, over plain HTTP.

The store behind the service is an implementation detail on the deployment's
side. A time-series database is the typical case, but a historian, an archive
of files, or a cache in front of any of those serves equally well. The service
is responsible for translating its store's native representation into the
message shapes below. It does not need to know about p-SWAMP players, modules,
pipelines, WebSockets, or frontend code.

```text
p-SWAMP                                   deployment environment
   |                                                |
   |  GET /v1/coverage                              |
   |  POST /v1/queries                              |
   +----------------------------------------------->| remote data service ---> any store
   |                                                |
   |  200, NDJSON body streamed as the store reads  |
   |<-----------------------------------------------+
```

The current reference implementation is
[`remote_data_stub`](../core/examples/remote_data_stub/), which plays
the part of a time-series store by serving a tiled sample recording. The
client is
[`RemoteDataClient`](../core/src/pswamp_core/datagateway/clients/remote_data.py),
and the shared request and response-line models are in
[`messages/remote_data.py`](../core/src/pswamp_core/messages/remote_data.py).
The web client's Time Series Explorer page is the worked example that queries
through it.

## Supported data

The preliminary client supports historical reads of one model name:

- `pmu.frame`: one timestamped, aligned set of channel values, carrying the
  channel layout those values follow.

There is no separate header model. Every frame is self-describing, so any
single record is enough to interpret.

The service may use any internal storage schema. The names and JSON shapes in
this document are the integration boundary, not a required storage schema.

All timestamps are ISO 8601 instants with a UTC offset. Examples use `Z`.
Requested and reported ranges are half-open: the start is inclusive and the end
is exclusive, $[start,end)$.

## HTTP API

### Health

```http
GET /healthz
```

Successful response:

```http
HTTP/1.1 200 OK
Content-Type: application/json
```

```json
{"status":"ok"}
```

The current stub reports process health only. Production readiness semantics
for the store dependency are not yet defined.

### Coverage

```http
GET /v1/coverage?model=pmu.frame
```

Successful response:

```json
{
  "model": "pmu.frame",
  "start": "2026-01-01T00:00:00Z",
  "end": "2026-01-01T01:00:00Z"
}
```

Rules:

- `model` is `pmu.frame`.
- `start` is the earliest available record timestamp.
- `end` is exclusive and must be later than the final available timestamp.
- `start: null` means that no records are available for the model.
- `end: null` represents unbounded coverage, although the current integration is
  history-only and normally reports a finite end.
- Other fields are ignored. The client is history-only, so a `live` flag
  changes nothing.
- An unknown model is `404`.
- The client may request coverage frequently, including around seeks and source
  boundaries. The operation should be cheap or backed by cached metadata.

Any non-success HTTP response is treated as a provider failure by the current
client.

### Query a range

```http
POST /v1/queries
Content-Type: application/json
```

Request body:

```json
{
  "version": "v1",
  "query_id": "4bb8c9757d834e2c80e658322aad51ee",
  "model": "pmu.frame",
  "start": "2026-01-01T00:10:00Z",
  "end": "2026-01-01T00:11:00Z",
  "mrid": ["nordic44"]
}
```

Fields:

| Field | Meaning |
|---|---|
| `version` | Request schema version. Currently exactly `v1`. |
| `query_id` | Opaque identifier generated by the caller, for correlating the two sides' logs. It is not echoed in the response. |
| `model` | Requested wire model: currently `pmu.frame`. |
| `start` | Inclusive lower bound; `null` means earliest available. |
| `end` | Exclusive upper bound; `null` means latest available. |
| `mrid` | Optional list of stream identities; `null` means all. |

Validate the request before sending anything. A request that can be refused is
refused with a status code, because once the body starts the status is spent:

- `404` if the model is unknown;
- `400` or `422` if the body is not JSON or does not match the schema.

Unknown extra fields in the body are ignored. The client treats every status
other than `200` as rejection of the query.

An accepted query answers `200` and streams its body. Over HTTP/1.1 that
looks like this:

```http
HTTP/1.1 200 OK
Content-Type: application/x-ndjson
Transfer-Encoding: chunked
```

The service does not know the body's length when it starts, so it sends no
`Content-Length`. Over HTTP/1.1 the body is therefore chunked, or ends when the
service closes the connection; over HTTP/2 the stream simply ends. Which one is
a property of the service's HTTP server, not of this contract, and the client
accepts all three. (The current client speaks HTTP/1.1.)

The body is described in the next section. There is no cancel route and no
query registry: the query lives exactly as long as its response.

## The streamed response

### Framing

The body is NDJSON: one JSON object per line, UTF-8, each line ending in `\n`
(`\r\n` is accepted). Lines are the only unit that means anything. How the
bytes are cut into HTTP/1.1 chunks, HTTP/2 frames or TCP segments carries no
meaning: a line may span several chunks and a chunk may hold several lines, and
proxies re-cut them freely. Each line is a result envelope with a `kind`:

| `kind` | Fields | Meaning |
|---|---|---|
| `record` | `model`, `record` | One record, as the JSON of the named model. |
| `end` | `count` | The query is complete; `count` is the number of `record` lines sent. |
| `error` | `error` | The query failed part way; `error` says why. |

Rules:

1. Zero or more `record` lines, in ascending record `timestamp` order.
2. Exactly one `end` or `error` line closes the body.
3. Nothing follows the terminal line.
4. A body that stops without a terminal line is a broken connection, not an
   empty answer, and the client reports it as a failure.

Absent fields may be omitted or `null`, and unknown fields are ignored.

### Record line

```json
{"kind":"record","model":"pmu.frame","record":{"version":"v1","mRID":"nordic44","timestamp":"2026-01-01T00:10:00Z","header":{"station":["6500","6500","6500"],"channel":["V","V",""],"measurement":["V_Magnitude","V_Angle","f"],"units":["kV","deg","Hz"],"data_rate":50.0,"freq_encoding":"absolute_hz","header_id":"fb4c60cd14cb"},"values":[418.2,-3.4,50.001],"quality":null}}
```

The nested `record` is a complete `pmu.frame`, described under
"PMU record schema" below.

### Successful completion

```json
{"kind":"end","count":1200}
```

### Failed query

```json
{"kind":"error","error":"StoreTimeout: query exceeded the store's timeout"}
```

The error text is currently free-form and intended for operational diagnosis.

### Stream it, and let the connection pace you

Write each line as the store yields it; do not build the whole answer first.
Two properties of the service's HTTP server matter, and every mainstream stack
has both:

1. **Flush.** Most servers buffer a response body before sending it. Flush
   after each line, or after each small batch, so lines leave while the query
   is still running. Without a flush a streamed answer arrives as one late blob.
2. **Backpressure.** The client reads a line only when p-SWAMP's player wants
   the next frame, which during a replay is at playback speed. When the client
   stops reading, TCP flow control fills the socket buffers, and the server's
   write then blocks, or reports that it must wait. A service that reads its
   next record only after the write returns reads its store at the client's
   pace, and holds a bounded amount in flight however large the range.

For example:

| Stack | Flush | Backpressure |
|---|---|---|
| Go `net/http` | `http.Flusher.Flush()` after writing | `Write` blocks |
| Java servlet | `ServletOutputStream.flush()` | `write` blocks |
| Spring WebFlux | return a `Flux` as `application/x-ndjson` | built in |
| Node.js | each `res.write()` is sent | `write()` returns `false`; wait for `'drain'` |
| ASP.NET Core | `await Response.Body.FlushAsync()` | `WriteAsync` waits |
| Python ASGI | a streaming response over a generator | the server awaits the socket |

The reference stub, asked for ten minutes and read five frames at a time,
stopped writing after about as many records as fit in the socket buffers.

### Cancellation

The client cancels a query by abandoning its response, for example on a seek
or when a page closes. Over HTTP/1.1 it closes the connection; over HTTP/2 it
resets the stream. The service must stop reading its store when that happens.
A server sees it as a write that fails, or as a notification that the request
was aborted: a cancelled request context in Go, an `IOException` on write in a
servlet, a `'close'` event in Node.js, `HttpContext.RequestAborted` in ASP.NET
Core. A service that does not write for a while, because its store is slow,
may only notice at its next write, which is acceptable.

### Long and idle responses

A replay holds its response open for as long as it plays, and a paused replay
holds it open with nothing flowing. Anything between p-SWAMP and the service
must therefore:

- **not buffer the response**, or the stream arrives as one late blob and
  backpressure is lost. For nginx, set `proxy_buffering off`, or have the
  service send `X-Accel-Buffering: no`, a hint nginx honours and every other
  hop ignores.
- **not cut an idle response short**, or a long pause ends in a connection
  error when playback resumes. The client recovers on the next play or seek,
  but the failure is visible to the user.

### Compression

Compression is optional. The client sends `Accept-Encoding: gzip, deflate`,
and a service may answer with a matching `Content-Encoding`. If it does, it
must flush the compressor with each flushed line, for example zlib's
`Z_SYNC_FLUSH`. A compressor that holds output until it has a full block turns
the stream back into a blob.

## PMU record schema

### `pmu.frame`

A frame is one instant of every channel in a stream, and it carries its own
channel layout in `header`. The record in the envelope example above is a
complete frame.

Frame rules:

- `mRID` identifies the stream or recording.
- `timestamp` is the measurement instant.
- `values` has exactly one entry per header column, in the header's column
  order. p-SWAMP rejects a frame whose width does not match its header.
- Missing or non-finite values are represented as JSON `null`.
- `quality`, when supplied, has one integer per column. Its detailed C37.118
  mapping remains provisional.

Header rules:

- `station`, `channel`, `measurement`, and `units` have equal lengths and
  describe columns by position.
- `measurement` entries are `f`, `df`, `<name>_Magnitude`, or `<name>_Angle`.
- `data_rate` is frames per second and is greater than zero.
- Only `absolute_hz` frequency encoding is currently supported.
- `header_id` is a content hash p-SWAMP computes from the layout. The service
  may send it or leave it out; p-SWAMP ignores it on read and recomputes it.

The header repeats in every frame. That costs bytes on every line; response
compression, where the deployment enables it, collapses most of the repetition.
In return, a layout change is simply the next frame's header, and the service
never has to correlate frames with a separately published layout.

## Client timing and failure behavior

The current client:

- uses a 10-second timeout for coverage requests and for opening a connection;
- waits 30 seconds by default for a query's response to start, and for each
  next line while it is waiting for one;
- never times out while the player is not asking for frames, so a paused
  replay is not an error on the client side;
- raises on a non-`200` query response;
- completes normally on `end`;
- fails the query on `error`, on a body that ends without a terminal line, and
  on a dropped connection;
- validates each nested record against the named PMU model;
- discards records outside the requested interval;
- logs and skips records naming an unsupported model;
- closes the connection on early exit, which is the cancellation; and
- depends on nothing but HTTP: a chunked, connection-close or fixed-length
  body, chunk boundaries anywhere, `\n` or `\r\n` line endings, and gzip or
  deflate that it asked for all read the same.

## Configuration correspondence

Current p-SWAMP client settings:

```text
REMOTE_DATA_URL=http://remote-data-service:8100
REMOTE_DATA_TIMEOUT=30
```

Current stub settings:

```text
REMOTE_DATA_STUB_PORT=8100
REMOTE_DATA_STUB_REPEAT=20
```

A production service may use different setting names. The resolved URL,
schemas, and protocol behavior must agree.

## Not settled/handled yet

The current implementation demonstrates the integration but does not yet define
production answers for:

- authentication, authorization, and TLS (the current impl assumes p-SWAMP and the remote service are in the same network and trust each other implicitly);
- proxy and load-balancer settings for long-lived and idle streamed responses;
- maximum range, record, and query sizes, and limits on concurrent queries;
- whether to compress responses in production;
- structured error codes versus free-form error text;
- formal schema publication outside the Python package;
- PMU quality, time quality, and configuration changes; and
- compatibility rules for future schema versions.

These are decisions to make with the deployment integration team. They should
not be inferred from the local stub, which is deliberately a small executable
example rather than a production service.

## Checking a service

`scripts/check-remote-data-service.sh` checks a running service against this
document, over plain HTTP:

```bash
scripts/check-remote-data-service.sh https://remote-data.example.internal
```

The check is `core/examples/check_remote_data_service.py`. It uses
Python's standard library and nothing from p-SWAMP, reads each response line
by line off the socket, and asks only for a few seconds at the start and end of
the coverage. Run with no URL, it starts the stub in this repo and checks that
instead, which is what CI does on every pull request, so the stub is held to
exactly the contract a deployment's service is.

It covers every acceptance case below that a client can observe. Whether the
service stopped reading its store after a hang-up (case 8) and what happens
behind the deployment's proxies (case 9) are for the service's owner to check
on their side.

## Acceptance tests for another implementation

A deployment's remote data service should be tested against at least these cases:

1. Coverage for `pmu.frame` has a correct exclusive end.
2. A bounded query returns exactly the records in $[start,end)$ in order.
3. `mrid` filtering returns only requested streams.
4. A successful query ends with one `end` line whose `count` matches the
   `record` lines before it.
5. A failed query ends with one `error` line and nothing after it.
6. An unknown model and a malformed body are refused with a status code before
   any body is sent.
7. Concurrent queries on separate connections are answered independently.
8. A client that stops reading stops the service's store reads (backpressure),
   and one that closes the connection stops them for good (cancellation).
9. The response reaches the client line by line through the deployment's own
   proxies, not buffered into one blob.
10. Every frame's `values` width matches its own `header`, and identities,
    timestamps, and versions are consistent.
