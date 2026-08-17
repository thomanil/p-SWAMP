#!/usr/bin/env bash
# Start the Vite/React web client for local dev with hot-module reload.
#
# This is the stable entrypoint for "run the web client" — the frontend half of
# the hot-reloading local dev stack. It launches Vite's dev server (HMR built in:
# a save to any source under app/client-web instantly patches the running page, no
# manual restart). Open the printed http://localhost:5173 URL in a browser.
#
# Vite proxies /api to the backend on :8000 (see app/client-web/vite.config.ts),
# so run start-local-hotloaded-pswamp-server.sh in another terminal and the web
# client talks to it on the same origin — exactly as it will in the baked image,
# where the server serves the production `vite build` at / instead.
#
# Note: this is dev-only. The shipped path is the static `vite build` baked into
# the server image (see Dockerfile) and served by src/server.py.
set -euo pipefail

# Run from the web client dir regardless of where the script is invoked from.
cd "$(dirname "$0")/../app/client-web"

# First run (or a fresh checkout) won't have node_modules — install from the
# lockfile so `npm run dev` doesn't fail on missing deps.
if [ ! -d node_modules ]; then
  echo "Installing web client dependencies (first run)…"
  npm ci
fi

# Open the app in the default browser once Vite is actually listening. We can't
# do this after `exec npm run dev` (exec replaces this process), so fork a waiter
# that polls the port, then opens the URL with whatever the platform provides.
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

exec npm run dev
