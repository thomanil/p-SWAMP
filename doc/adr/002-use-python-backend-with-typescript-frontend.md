# ADR-002: Use a Python backend with a TypeScript frontend

- Status: Accepted
- Date: 2026-08-11

> Written up retrospectively under
> [ADR-001](001-record-architecture-decisions.md), which post-dates it. The date
> is when the choice was made, not when this file was written.

## Context

P-SWAMP started as a single-process desktop application: Python throughout, with a
 PySide6/pyqtgraph UI reading straight from the objects the analysis threads write. 
 That analysis core is the project's real value, it is owned by the domain experts/researchers
, and it is not going to be rewritten. The desktop application is not being retired either.

We now want the same analysis reachable in a browser, without an install. That
means splitting the application across a network boundary and choosing a
language for each half — for a UI of live plots, phasor diagrams and a
multi-stream dashboard, co-built by the teams whose strength is user experience
rather than algorithms.

## Decision

We will build the web stack as a Python backend and a TypeScript frontend, split
at an HTTP/WebSocket boundary: FastAPI on uvicorn in `app/server-python/`, Vite and Typescript in `app/client-web/` (React currently as the SPA framework but that may change) The backend exists to put the *existing* core on a
socket — it imports `pswamp.*` and holds no domain logic of its own.

## Consequences

The core stays in one language with one set of owners, and the web
layer becomes a third presentation adapter beside the Qt `gui/` and
`visualization/` packages, so a fix reaches both front ends. The
network boundary doubles as the team boundary, which is the point. QT
GUI layer is still supported for a little while, but may get dropped
monce we really get going on the web based frontend.

We now run two toolchains (`uv` and `npm`, `ruff` and `tsc`);
`scripts/error_check.sh` covers both, and the built client is baked into the
server image so there is still one deployable.

The real cost is the seam. Domain types must now be *serialised*, which the
desktop never needed — numpy arrays, UUIDs and datetimes do not cross JSON on
their own, so every page needs an adapter and every message shape becomes
something two languages must agree on. That agreement is
[ADR-003](003-use-openapi-contract-for-client-server-api.md). The Python
execution model comes along too: the monitoring applications still run in daemon
threads, so the backend is an asyncio server with threads in it.

## Alternatives considered

- **Python end to end** (Streamlit, Dash, NiceGUI, PyScript). One toolchain, but
  a live multi-panel dashboard is where these stop being a shortcut — and it puts
  the interface work on the algorithm teams. Distribution is also harder than a classic web SPA project.
- **TypeScript end to end**, porting the core. Throws away the project's main
  asset and the expertise attached to it.
- **Serving the Qt app remotely** (VNC, Qt for WebAssembly). A delivery trick
  rather than an architecture: a remote desktop, scaling badly per user, leaving
  no api anything else could build on.
