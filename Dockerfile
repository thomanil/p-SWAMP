# State server image, with the web client baked in: the first stage below builds
# app/client-web to static assets that the server stage serves at / alongside /ws
# (same process, port, and origin) — so one image and one Service serve both.
#
# --- web client build stage -------------------------------------------------
#
# Build the Vite/React/TS web client to plain static files. A pinned Node image
# runs `npm ci` + `vite build`, emitting /web/dist (index.html + favicon/icons +
# hashed files under assets/). Only that dist/ is copied into the final image —
# Node and node_modules never ship. Pinned to a digest for reproducible builds
# (the tag is kept as documentation); this is the multi-arch index digest, so
# arm64/amd64 both resolve. Resolved 2026-06-06 from tag 22-bookworm-slim.
FROM node:22-bookworm-slim@sha256:7af03b14a13c8cdd38e45058fd957bf00a72bbe17feac43b1c15a689c029c732 AS web-build
WORKDIR /web
# Copy only the manifests first so `npm ci` is cached and re-runs only when the
# lockfile changes, not on every source edit.
COPY app/client-web/package.json app/client-web/package-lock.json ./
RUN npm ci
COPY app/client-web/ ./
RUN npm run build

# --- server stage -----------------------------------------------------------
#
# Base: Astral's official uv image (Python 3.11 on Debian 12 "bookworm" slim).
# Pinned to a digest for reproducible builds — the readable tag is kept as
# documentation, but Docker enforces the @sha256. This is the multi-arch OCI
# index digest, so arm64/amd64 still resolve automatically.
# Resolved 2026-06-06 from tag python3.11-bookworm-slim. To refresh:
#   curl -s "https://ghcr.io/token?scope=repository:astral-sh/uv:pull" | ...
#   curl -sI -H "Authorization: Bearer <token>" \
#     -H "Accept: application/vnd.oci.image.index.v1+json" \
#     https://ghcr.io/v2/astral-sh/uv/manifests/python3.11-bookworm-slim
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim@sha256:4f5d923c9dcea037f57bda425dd209f3ec643da2f0b74227f68d09dab0b3bb36

WORKDIR /app

# curl is used by the compose healthcheck to probe /healthz.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Dependency manifest first, on its own layer: the install below is then cached
# and re-runs only when pyproject.toml/uv.lock actually change, not on every
# source edit. Same trick as `npm ci` in the web-build stage above.
COPY app/server-python/pyproject.toml app/server-python/uv.lock ./

# pyproject.toml declares the direct dependencies; uv.lock pins the whole
# transitive closure resolved from it. Install system-wide at build time, so
# container startup needs no network and no runtime resolution.
#
# --locked asserts the lockfile exists and still matches pyproject.toml: change a
# dependency without re-running `uv lock`, or forget to COPY the lock, and the
# build fails loudly here instead of silently re-resolving to whatever the index
# serves today. --no-emit-project skips this non-packaged project itself, and
# --no-dev keeps the linter out of the image. Hashes are kept (no --no-hashes),
# so uv verifies every artifact it installs; the lock carries wheel hashes for
# both amd64 and arm64, so this stays multi-arch.
RUN uv export --locked --no-emit-project --no-dev -o /tmp/requirements.txt \
    && uv pip install --system -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# Source last, so editing it doesn't invalidate the dependency layer above. The
# image mirrors the repo: app/server-python/ → /app, so server.py and the app
# packages beside it land in /app/src exactly as they sit in the working tree.
# Copying the directory rather than naming files means a new module — or a whole
# new app package — needs no Dockerfile edit.
COPY app/server-python/src/ ./src/

# Web client assets: the Vite build output from the web-build stage above,
# dropped where server.py mounts it — static/ beside the source, i.e.
# Path(__file__).parent / "static". Content-hashed filenames, so it's safe to
# cache hard.
COPY --from=web-build /web/dist ./src/static

# Run as a non-root user (k8s-friendly).
RUN useradd --create-home --uid 10001 app
USER app

# Run from the source dir, so `server.py`, `server:app` (the form compose's
# uvicorn --reload override uses) and the app packages it imports (`import
# timeline`) all resolve off the working directory with no PYTHONPATH or --app-dir
# plumbing. Deps were installed system-wide above, so nothing here depends on
# being in /app.
WORKDIR /app/src

# 0.0.0.0 so the port is reachable outside the container; see server.py.
ENV HOST=0.0.0.0 \
    PORT=8000
EXPOSE 8000

# Plain python: deps are already installed, so the PEP 723 metadata is ignored
# and there's no uv resolution at start.
CMD ["python", "server.py"]
