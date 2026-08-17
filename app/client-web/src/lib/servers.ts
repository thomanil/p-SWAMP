// Where the web client's WebSockets point: always the origin the page was served
// from. There is no backend picker and no address table.
//
// That one rule covers both ways this app runs, because dev is made to look like
// production rather than special-cased:
//
//   - shipped build — the client is *served by* the backend (container, minikube
//     NodePort, any remote cluster), so its own origin already is the backend.
//   - `npm run dev` — the page's origin is the Vite dev server, whose proxy
//     forwards the whole /api prefix (WebSocket upgrades included) to the local
//     docker server on :8000. See `server.proxy` in vite.config.ts.
//
// So aiming dev at a different backend is a proxy-target edit in vite.config.ts,
// not a runtime choice in the UI.

import { BASE_PATH } from '@/lib/basePath'

// Each app's WebSocket endpoint. Every backend app package is mounted under its
// own /api/<app> prefix (see APPS in app/server-python/src/server.py), so these
// paths are the client-side half of that contract — one const per app, rather
// than a literal repeated across the hooks that connect.
export const TIMELINE_WS_PATH = '/api/timeline/ws'
export const PMU_STREAM_WS_PATH = '/api/pmu-test-streamer/ws'

/** The full ws:// (or wss://) URL for one app's endpoint on the serving origin,
 *  derived from the current page so it follows http→ws / https→wss automatically.
 *
 *  BASE_PATH is empty locally and `/p-swamp` behind the remote
 *  reverse proxy, which strips it again before the request reaches the server —
 *  so the paths above stay written the way the server actually mounts them.
 *
 *  `wsPath` is one of the *_WS_PATH consts above. */
export function resolveServerUrl(wsPath: string): string {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${proto}//${window.location.host}${BASE_PATH}${wsPath}`
}
