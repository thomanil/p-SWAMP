/// <reference types="vite/client" />

// Custom build-time env vars, declared so `import.meta.env.X` is typed rather
// than `any` (vite/client only carries an index signature for unknown keys).
interface ImportMetaEnv {
  // The git commit this bundle was built from, inlined by Vite at build time.
  // Set from the Dockerfile's GIT_SHA build arg (see the web-build stage); empty
  // under `npm run dev` and in a plain `docker build` with no --build-arg. The
  // footer surfaces it only off localhost — see AppLayout.tsx.
  readonly VITE_GIT_SHA?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
