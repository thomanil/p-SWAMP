#!/usr/bin/env bash
# Deploy the project to a local minikube cluster.
# Use for local testing of k8s configs.
# For more rapid local dev with live reloads etc, use start-local-hotloaded-pswamp-server.sh instead (docker compose)
#
# This is the stable entrypoint for "run the server in kubernetes locally". It
# builds the image straight into minikube (no registry/push), applies the
# manifests in k8s/, forces a rollout so a rebuilt :latest image is actually
# picked up, waits for readiness, and prints the WebSocket URL the desktop
# client should use.
#
# Prerequisites: minikube and kubectl. If either is missing the script stops
# and tells you where to get it — it does not install anything for you.
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

# --- Ensure the cluster is running -----------------------------------------
if ! minikube status >/dev/null 2>&1; then
  echo "Starting minikube..."
  minikube start
fi

# --- Build the image into minikube -----------------------------------------
# Build inside minikube's own runtime so the image lands exactly where the
# kubelet looks for it (and the arch always matches the node — matters on arm64).
echo "Building pswamp-client-server-poc:latest into minikube..."
minikube image build -t pswamp-client-server-poc:latest .

# --- Apply manifests and roll out ------------------------------------------
echo "Applying manifests..."
kubectl apply -f k8s/pswamp-client-server-poc-local.yaml

# `kubectl apply` won't restart pods if the manifest text is unchanged, even
# though we just rebuilt the :latest image. Force a new pod so the fresh image
# is actually used.
echo "Rolling out..."
kubectl rollout restart deployment/pswamp-client-server-poc
kubectl rollout status deployment/pswamp-client-server-poc --timeout=120s

# --- Report the client URL -------------------------------------------------
# 30081, not the usual 30080: an older timeline-server sandbox still lives in the
# same local cluster, and two Services can't share a nodePort. Keep it in sync
# with k8s/pswamp-client-server-poc-local.yaml.
MINIKUBE_IP="$(minikube ip)"
NODE_PORT=30081
BASE_URL="http://${MINIKUBE_IP}:${NODE_PORT}"
echo
echo "pswamp-client-server-poc is up. Point the client's 'Local minikube' entry at:"
echo "    ws://${MINIKUBE_IP}:${NODE_PORT}/api/timeline/ws"
echo
echo "Web client:    ${BASE_URL}/"
echo "Health check:  curl -fsS ${BASE_URL}/healthz"

# --- Open the web client ----------------------------------------------------
# `rollout status` above only waits for the pod; the Service's nodePort can take
# a moment longer to route. Poll /healthz so we don't open a browser on a
# connection error, then hand the URL to whatever launcher the platform has
# (same idiom as start-local-hotloaded-pswamp-web-client.sh).
#
# Set NO_BROWSER=1 to skip this — e.g. when deploying over SSH or from a script.
if [ -z "${NO_BROWSER:-}" ]; then
  echo
  echo "Waiting for ${BASE_URL}/healthz ..."
  answered=""
  for _ in $(seq 1 60); do
    if curl -sf -o /dev/null "${BASE_URL}/healthz"; then
      answered=1
      if command -v xdg-open >/dev/null 2>&1; then
        xdg-open "${BASE_URL}/" >/dev/null 2>&1
      elif command -v open >/dev/null 2>&1; then
        open "${BASE_URL}/" >/dev/null 2>&1
      else
        echo "Could not find a browser launcher (xdg-open/open); open ${BASE_URL}/ manually."
      fi
      break
    fi
    sleep 0.5
  done
  [ -n "$answered" ] || echo "Service did not answer within 30s; open ${BASE_URL}/ manually." >&2
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
  exit 0
fi

echo
echo "Following server logs (Ctrl-C stops the tail, not the deployment)..."
echo
exec ./scripts/logs-minikube.sh
