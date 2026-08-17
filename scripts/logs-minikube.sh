#!/usr/bin/env bash
# Follow the p-SWAMP server's logs from the minikube deployment — the k8s
# equivalent of `docker compose logs -f`. The server logs client events (connects,
# playback commands) to stdout, so this streams the roster tables as they happen.
#
# A follow is bound to one pod. Re-running
# start-pswamp-in-local-minikube-cluster.sh replaces the pod (Recreate) and ends
# the stream — just run this again once the new pod is ready. The old pod's logs
# are gone with it; `kubectl logs deployment/p-swamp --previous` shows the last
# terminated pod's output.
set -euo pipefail

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")/.."

# Stream logs, dropping the noisy healthcheck lines. --line-buffered keeps output
# flowing; grep's exit status is masked so an all-filtered chunk can't trip
# pipefail and kill the tail.
kubectl logs -f deployment/p-swamp --timestamps \
  | { grep -v --line-buffered "GET /healthz HTTP/1.1" || true; }
