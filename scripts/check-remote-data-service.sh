#!/usr/bin/env bash
# Check a remote data service against doc/remote-data-integration-contract.md.
#
#   scripts/check-remote-data-service.sh                  start the stub in this repo, check it, stop it
#   scripts/check-remote-data-service.sh http://host:port  check a service already running (yours)
#
# The check is black-box HTTP (core/examples/check_remote_data_service.py,
# standard library only), so it neither knows nor cares what the service is
# written in: the stub here is held to exactly what a deployment's own service
# is held to. The second form touches nothing but that URL.
#
# Needs python3 for the check; the first form also needs uv (the stub runs in
# the web backend's environment) and curl (to wait for it).
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

prepend_path() { case ":$PATH:" in *":$1:"*) ;; *) [ -d "$1" ] && PATH="$1:$PATH";; esac; }
prepend_path "$HOME/.local/bin"
export PATH

CHECK=core/examples/check_remote_data_service.py
URL="${1:-}"
STUB_PID=""

cleanup() {
    if [ -n "$STUB_PID" ]; then
        kill "$STUB_PID" 2>/dev/null
        wait "$STUB_PID" 2>/dev/null
    fi
}
trap cleanup EXIT INT TERM

if [ -z "$URL" ]; then
    PORT="${REMOTE_DATA_CHECK_PORT:-8198}"
    URL="http://127.0.0.1:$PORT"
    if curl -s -m 1 -o /dev/null "$URL/healthz"; then
        echo "something already answers on $URL; set REMOTE_DATA_CHECK_PORT to a free port" >&2
        exit 1
    fi
    LOG="$(mktemp "${TMPDIR:-/tmp}/remote-data-stub.XXXXXX")"
    echo "starting the stub on $URL (log: $LOG)"
    # The stub lives in core/examples/, outside the installed pswamp_core; it
    # runs in the web backend's environment, which has pswamp_core, FastAPI
    # and uvicorn.
    PYTHONPATH="core/examples" REMOTE_DATA_STUB_PORT="$PORT" \
        uv run --project app/server-python python -m remote_data_stub >"$LOG" 2>&1 &
    STUB_PID=$!
    up=0
    for _ in $(seq 1 60); do
        if curl -s -m 1 -o /dev/null "$URL/healthz"; then up=1; break; fi
        if ! kill -0 "$STUB_PID" 2>/dev/null; then break; fi
        sleep 0.5
    done
    if [ "$up" != 1 ]; then
        echo "the stub did not come up; its log:" >&2
        cat "$LOG" >&2
        exit 1
    fi
fi

python3 "$CHECK" "$URL"
