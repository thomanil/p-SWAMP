#!/usr/bin/env bash
# Start the timeline state server for local dev.
#
# This is the stable entrypoint for "run the server" — the dev workflow stays
# the same even if the implementation changes. Today it's a containerized
# FastAPI/uvicorn server with hot reload (Compose syncs source into the running
# container and uvicorn --reload picks it up); swap the command below if that
# changes and callers don't have to care.
#
# `up --watch` rather than plain `watch`: it does the same file watching but also
# streams the container's logs into this terminal, so you see the server's roster
# tables and reload notices right where you started it — no second window needed.
# That's deliberately the only local log view: this terminal is it.
#
# It also means Ctrl-C actually STOPS the server here, instead of merely
# detaching and leaving the container bound to port 8000.
#
# `--build` is not optional: without it Compose reuses whatever
# p-swamp:latest already exists and only *builds* when the image
# is missing entirely. `watch` then syncs edits made while it runs — but every
# change made while the stack was DOWN (a new file, a rename, a deleted module) is
# absent from that stale image, and the server starts from code that no longer
# matches the repo. That failed loudly once the entrypoint was renamed to
# server.py ("Error loading ASGI app. Could not import module 'server'") and could
# fail silently in subtler ways. Docker's layer cache makes the no-op rebuild a
# couple of seconds; the price is that a client-web edit re-runs the image's
# web-build stage, which dev doesn't use (Vite serves the client) but which keeps
# the image honest.
set -euo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.."

# Drop the noisy healthcheck request lines (Compose probes /healthz every 10s);
# everything else — sync/rebuild events and all server output — passes through.
# --line-buffered keeps output flowing line-by-line rather than in blocks; grep's
# exit status is masked so an all-filtered chunk can't trip pipefail. Not exec'd,
# because exec can't head a pipeline.
docker compose up --watch --build 2>&1 \
  | { grep -v --line-buffered "GET /healthz HTTP/1.1" || true; }
