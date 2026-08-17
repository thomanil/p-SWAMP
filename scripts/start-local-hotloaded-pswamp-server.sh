#!/usr/bin/env bash
# Start the p-SWAMP state server for local dev — the stable "run the server"
# entrypoint. Today: a containerized FastAPI/uvicorn server with hot reload
# (Compose syncs source in, uvicorn --reload picks it up).
#
# `up --watch` (not plain `watch`) also streams the container's logs into this
# terminal — the only local log view — and makes Ctrl-C actually STOP the server
# rather than detach and leave it bound to port 8000.
#
# NOT hot-reloaded: the generated api contract. An endpoint/message change reloads
# the server, but doc/api/openapi.json and the client's generated types move only
# when you run scripts/generate-api-contract.sh.
#
# `--build` is not optional: Compose only builds when the image is missing, and
# `watch` syncs only edits made while it runs — so any change made while the stack
# was DOWN (a new/renamed/deleted module) is absent from the stale image and the
# server runs code that no longer matches the repo. (Seen for real: an
# "Error loading ASGI app" after the entrypoint was renamed.) The no-op rebuild is
# a couple of seconds on a warm cache.
set -euo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.."

# Drop the noisy /healthz lines (Compose probes it every 10s); everything else
# passes through. --line-buffered keeps output flowing; grep's exit status is
# masked so an all-filtered chunk can't trip pipefail. Not exec'd — exec can't
# head a pipeline.
docker compose up --watch --build 2>&1 \
  | { grep -v --line-buffered "GET /healthz HTTP/1.1" || true; }
