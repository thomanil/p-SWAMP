# p-SWAMP web client

Svelte 5 + TypeScript + Vite. Plain Svelte on Vite — **not SvelteKit**: no SSR and
no file-based routing; routing is client-side (`svelte-routing`). UI components are
shadcn-svelte (bits-ui) styled with Tailwind v4; charts are uPlot; icons are
`@lucide/svelte`.

This is a thin renderer: it holds no durable state, sends commands as POSTs, and
draws whatever the server pushes down a WebSocket. In the shipped image it is baked
into the FastAPI server and served from the same origin as `/api`.

**The authoritative guide for working in this repo is the root `AGENTS.md`** —
read it before changing anything here. It covers the client/server seam, the "one
folder per app under `src/pages/`" layout, the socket modules (`use…Socket.svelte.ts`
over `src/hooks/useServerSocket.svelte.ts`), the runtime-discovered mount prefix
(`src/lib/basePath.ts`), and how to add a page or a grid-monitor panel. The
generated api contract (`src/api/schema.ts`) is produced by
`scripts/generate-api-contract.sh` — don't hand-edit it.

## Commands

```
npm run dev      # Vite dev server + HMR on http://localhost:5173 (proxies /api → :8000)
npm run build    # svelte-check (type-check) then vite build → dist/
npm run check    # svelte-check only
npm run lint     # eslint (flat config, eslint-plugin-svelte)
```

Prefer the repo scripts for the usual flows:
`scripts/start-local-hotloaded-pswamp-web-client.sh` (dev) and
`scripts/error_check.sh` (the static gate CI and the pre-push hook run:
`svelte-check` + `eslint` here, plus the Python and api-contract checks).
