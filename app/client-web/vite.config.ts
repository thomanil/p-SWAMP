import path from 'node:path'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  // Emit *relative* asset urls ("./assets/index-a1b2c3.js") in index.html rather
  // than absolute ones rooted at "/". That is what lets one build run both at the
  // origin root (dev, docker, minikube) and under the remote reverse proxy's
  // /p-swamp/ prefix: the browser resolves them against whatever
  // path the page was served at. src/lib/basePath.ts reads the prefix back off
  // this module's own url and feeds it to the router and the WebSocket urls; see
  // that file for the invariants (notably: routes stay one segment deep).
  base: './',
  resolve: {
    // `@/…` → src/…, the import alias shadcn/ui components expect.
    alias: { '@': path.resolve(__dirname, './src') },
  },
  // Dev server only. In the shipped build the server serves these assets and the
  // api from one origin; here Vite is a separate origin (:5173), so proxy the
  // backend surfaces to :8000 to keep same-origin URLs working unchanged.
  // The whole /api prefix, not one endpoint: every backend app package is mounted
  // under it (see APPS in app/server-python/src/server.py), so a new api needs no
  // change here. An http:// target with `ws: true` carries both plain requests and
  // WebSocket upgrades — /api/phasors/ws is a socket, /api/grid/model is not.
  server: {
    proxy: {
      '/api': { target: 'http://localhost:8000', ws: true },
      '/docs': { target: 'http://localhost:8000' },
      '/openapi.json': { target: 'http://localhost:8000' },
    },
  },
})
