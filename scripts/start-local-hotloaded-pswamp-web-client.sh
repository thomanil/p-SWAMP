#!/usr/bin/env bash
# Start the Vite/React web client for local dev with HMR — the stable "run the web
# client" entrypoint. A save to any source under app/client-web patches the page
# instantly. Open the printed http://localhost:5173 URL.
#
# HMR does NOT cover the generated api types (src/api/schema.ts): those move only
# with scripts/generate-api-contract.sh, and Vite strips types rather than
# checking them, so a client built against a stale contract looks fine here and
# fails in scripts/error_check.sh.
#
# Vite proxies /api to the backend on :8000 (vite.config.ts), so run
# start-local-hotloaded-pswamp-server.sh in another terminal — same-origin, just
# as in the baked image where the server serves the production build at / instead.
#
# Dev-only. The shipped path is the static `vite build` baked into the server
# image and served by src/server.py.
set -euo pipefail

# Run from the web client dir regardless of where the script is invoked from.
cd "$(dirname "$0")/../app/client-web"

# Fresh checkout won't have node_modules — install from the lockfile first.
if [ ! -d node_modules ]; then
  echo "Installing web client dependencies (first run)…"
  npm ci
fi

# Open the browser once Vite is listening. Can't do it after `exec npm run dev`
# (exec replaces this process), so fork a waiter that polls the port then opens
# the URL with whatever the platform provides.
URL="http://localhost:5173/"
(
  for _ in $(seq 1 60); do
    if curl -sf -o /dev/null "$URL"; then
      if command -v xdg-open >/dev/null 2>&1; then
        xdg-open "$URL" >/dev/null 2>&1
      elif command -v open >/dev/null 2>&1; then
        open "$URL" >/dev/null 2>&1
      else
        echo "Could not find a browser launcher (xdg-open/open); open $URL manually."
      fi
      exit 0
    fi
    sleep 0.5
  done
  echo "Vite did not come up within 30s; open $URL manually."
) &

# The api doc is served by the BACKEND, not Vite: the dev proxy forwards only /api,
# so /docs on :5173 is a 404 that falls through to the SPA. Point at the server's
# own origin — it has to be running anyway.
echo
echo "API docs:      http://localhost:8000/docs  (ReDoc at /redoc, raw document at /openapi.json)"
echo "               served by start-local-hotloaded-pswamp-server.sh, not by Vite on :5173"
echo

exec npm run dev
