#!/usr/bin/env bash
# Deploy the project to a local minikube cluster.
# Use for local testing of k8s configs.
# For more rapid local dev with live reloads etc, use start-local-hotloaded-pswamp-server.sh instead (docker compose)
#
# This is the stable entrypoint for "run the server in kubernetes locally". It
# checks the committed api contract still matches the code, builds the image
# straight into minikube (no registry/push), applies the manifests in k8s/, forces
# a rollout so a rebuilt :latest image is actually picked up, waits for readiness,
# and prints the WebSocket URL the desktop client should use.
#
# Prerequisites: minikube and kubectl, plus uv and npx for the contract check
# (NO_CHECK=1 drops that requirement along with the check). If any is missing the
# script stops and tells you where to get it — it does not install anything for
# you.
#
# Environment flags: NO_CHECK=1 skips the api-contract preflight, NO_BROWSER=1
# skips opening the web client, NO_LOGS=1 returns to the prompt instead of
# tailing logs.
set -euo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.."

# --- Preflight: required tooling -------------------------------------------
if ! command -v minikube >/dev/null 2>&1; then
  echo "minikube not found. Install it: https://minikube.sigs.k8s.io/docs/start/" >&2
  exit 1
fi
if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found. Install it: https://kubernetes.io/docs/tasks/tools/" >&2
  exit 1
fi

# --- Preflight: the api contract is not stale -------------------------------
#
# Read-only, and deliberately BEFORE the cluster start and the image build: this
# is the one failure mode this path cannot otherwise catch, and failing here costs
# seconds instead of a minute of image build followed by a wrong deployment.
#
# Why it cannot be caught later: the image build does run `tsc -b`, so a client
# that disagrees with the committed schema.ts fails the build. But a *stale*
# schema.ts agrees with the client perfectly -- they are both simply behind the
# Python. tsc only ever compares TypeScript to TypeScript, so it sees nothing
# wrong, the image builds clean, and the pod serves a client expecting fields the
# server no longer sends. Regenerating and diffing is the only thing that notices.
#
# Unlike minikube/kubectl above, this needs uv and npx (it imports the app to ask
# for its own api description, then generates TypeScript from it). The script
# itself says so and exits non-zero if either is missing, so this stays a loud
# failure rather than a skipped check -- see the note in AGENTS.md about checks
# that silently do nothing being worse than checks that fail.
#
# NO_CHECK=1 skips it, matching generate-new-subapp.sh and update-dependencies.sh.
# Reach for that when you are knowingly deploying a work-in-progress contract, not
# to get past a red check.
if [ "${NO_CHECK:-0}" != 1 ]; then
  echo "Checking the api contract is up to date..."
  if ! scripts/generate-api-contract.sh --check; then
    echo >&2
    echo "Refusing to deploy: the committed api contract does not match the code." >&2
    echo "The image would build fine and serve a web client built from stale types." >&2
    echo >&2
    echo "  scripts/generate-api-contract.sh      # regenerate, then commit both files" >&2
    echo "  NO_CHECK=1 $0   # or deploy anyway" >&2
    exit 1
  fi
fi

# --- Ensure the cluster is running -----------------------------------------
if ! minikube status >/dev/null 2>&1; then
  echo "Starting minikube..."
  minikube start
fi

# --- Build the image into minikube -----------------------------------------
# Build inside minikube's own runtime so the image lands exactly where the
# kubelet looks for it (and the arch always matches the node — matters on arm64).
echo "Building p-swamp:latest into minikube..."
minikube image build -t p-swamp:latest .

# --- Apply manifests and roll out ------------------------------------------
echo "Applying manifests..."
kubectl apply -f k8s/p-swamp-local.yaml

# `kubectl apply` won't restart pods if the manifest text is unchanged, even
# though we just rebuilt the :latest image. Force a new pod so the fresh image
# is actually used.
echo "Rolling out..."
kubectl rollout restart deployment/p-swamp
kubectl rollout status deployment/p-swamp --timeout=120s

# --- Reach the Service ------------------------------------------------------
# The nodePort is fixed at 30080 and must stay in sync with k8s/p-swamp-local.yaml.
#
# `rollout status` above only waits for the pod; the Service's nodePort can take
# a moment longer to route, so poll /healthz before announcing a URL.
MINIKUBE_IP="$(minikube ip)"
NODE_PORT=30080
BASE_URL="http://${MINIKUBE_IP}:${NODE_PORT}"

# $1 = base url, $2 = attempts. Prints a dot per attempt and always closes the
# line, so a wait is visibly progressing rather than looking frozen.
#
# `--connect-timeout 1` is what makes this fail fast, not `-m`. An unroutable node IP
# *drops* packets rather than refusing them — nothing sends a TCP reset — so
# without it every attempt burns its full `-m` timeout and the probe sits silent
# for the better part of a minute before falling back, which reads as a hang.
# A refused or answered connection returns instantly either way.
wait_for_healthz() {
  local url="$1" attempts="$2" i
  for i in $(seq 1 "$attempts"); do
    if curl -sf --connect-timeout 1 -m 2 -o /dev/null "${url}/healthz"; then
      echo
      return 0
    fi
    printf '.'
    sleep 0.5
  done
  echo
  return 1
}

# Only a handful of attempts here: `rollout status` above already waited for the
# pod to pass its readiness probe (which *is* /healthz), so on a routable NodePort
# the first attempt answers. Anything longer is just the macOS no-route case
# waiting to be detected, and we want that detected fast.
printf '\nWaiting for %s/healthz ' "$BASE_URL"
if ! wait_for_healthz "$BASE_URL" 5; then
  # The nodePort is unreachable from here. On macOS (and Windows) the docker
  # driver runs the node inside a Linux VM whose bridge network the host has no
  # route to, so `minikube ip` simply never answers — the deployment is fine, the
  # path to it isn't. On Linux the bridge is routable and we never get here.
  # Tunnel to the Service instead and use localhost as the base URL; everything
  # downstream (browser, WebSocket, healthz) then works the same on both.
  echo "NodePort ${MINIKUBE_IP}:${NODE_PORT} is not routable from this host (expected on macOS/Windows"
  echo "with the docker driver) — forwarding a local port to the Service instead."
  kubectl port-forward "service/p-swamp" "${NODE_PORT}:8000" >/dev/null 2>&1 &
  PORT_FORWARD_PID=$!
  # Take the tunnel down with the script, however it exits.
  trap 'kill "$PORT_FORWARD_PID" 2>/dev/null || true' EXIT INT TERM
  BASE_URL="http://127.0.0.1:${NODE_PORT}"
  # More attempts than above, and they cost nothing: this is loopback, where a
  # not-yet-bound tunnel is refused instantly rather than timing out. The budget
  # is really for the tunnel to finish binding.
  printf 'Waiting for %s/healthz ' "$BASE_URL"
  if ! wait_for_healthz "$BASE_URL" 30; then
    echo "Port-forward did not come up either; the deployment may still be starting." >&2
    echo "Check with: kubectl get pods -l app=p-swamp" >&2
    exit 1
  fi
fi

WS_HOST="${BASE_URL#http://}"
echo
echo "p-swamp is up. Point the client's 'Local minikube' entry at:"
echo "    ws://${WS_HOST}/api/time-window/ws"
echo
echo "Web client:    ${BASE_URL}/"
echo "API docs:      ${BASE_URL}/docs  (ReDoc at /redoc, raw document at /openapi.json)"
echo "Health check:  curl -fsS ${BASE_URL}/healthz"

# --- Open the web client ----------------------------------------------------
# Hand the URL to whatever launcher the platform has (same idiom as
# start-local-hotloaded-pswamp-web-client.sh).
#
# Set NO_BROWSER=1 to skip this — e.g. when deploying over SSH or from a script.
if [ -z "${NO_BROWSER:-}" ]; then
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "${BASE_URL}/" >/dev/null 2>&1
  elif command -v open >/dev/null 2>&1; then
    open "${BASE_URL}/" >/dev/null 2>&1
  else
    echo "Could not find a browser launcher (xdg-open/open); open ${BASE_URL}/ manually."
  fi
fi

# --- Follow the server logs -------------------------------------------------
# End the same way start-local-hotloaded-pswamp-server.sh does: leave a live log
# stream in this terminal rather than dropping back to the prompt, so you can
# watch client connects and playback commands as you drive the web client.
#
# Ctrl-C stops only the tail — the deployment keeps running in the cluster.
# Re-attach any time with ./scripts/logs-minikube.sh.
#
# Set NO_LOGS=1 to return to the prompt immediately instead.
if [ -n "${NO_LOGS:-}" ]; then
  if [ -n "${PORT_FORWARD_PID:-}" ]; then
    echo
    echo "Note: the port-forward above dies with this script, so ${BASE_URL}/ goes away." >&2
    echo "Re-open it in its own terminal with:" >&2
    echo "    kubectl port-forward service/p-swamp ${NODE_PORT}:8000" >&2
  fi
  exit 0
fi

echo
if [ -n "${PORT_FORWARD_PID:-}" ]; then
  echo "Keeping the port-forward open for as long as this runs — Ctrl-C closes both"
  echo "the tail and the tunnel (the deployment keeps running either way)."
fi
echo "Following server logs (Ctrl-C stops the tail, not the deployment)..."
echo
# Not `exec`: that would replace this shell and orphan the port-forward instead
# of letting the EXIT trap clean it up.
#
# Backgrounded + `wait` rather than run in the foreground, because bash defers a
# trap until the running foreground command returns — and `kubectl logs -f` never
# returns on its own. Ctrl-C would still work (the terminal signals the whole
# process group), but `kill <script pid>` would hang forever with the tunnel still
# up. `wait` is interruptible, so the trap fires immediately either way.
# `set -m` so the log tail becomes its own process-group leader: logs-minikube.sh
# is a pipeline (kubectl | grep), and killing just its shell would orphan the
# `kubectl logs -f` behind it. With a group of its own we can signal the whole
# thing at once with `kill -- -PID`. It also makes Ctrl-C and `kill` behave
# identically: the tail no longer shares the terminal's foreground group, so both
# routes arrive as a signal to *this* shell and go through the same trap.
set -m
./scripts/logs-minikube.sh &
LOGS_PID=$!
set +m
cleanup() {
  kill -- "-${LOGS_PID}" 2>/dev/null || kill "$LOGS_PID" 2>/dev/null || true
  [ -n "${PORT_FORWARD_PID:-}" ] && kill "$PORT_FORWARD_PID" 2>/dev/null
  return 0
}
trap cleanup EXIT INT TERM
wait "$LOGS_PID"
