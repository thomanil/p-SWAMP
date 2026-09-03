import { vitePreprocess } from '@sveltejs/vite-plugin-svelte'

// Plain Svelte + Vite (no SvelteKit): the only job here is to run the TypeScript
// (and PostCSS) preprocessor so `<script lang="ts">` and Tailwind work in
// components. Routing, the build and the dev proxy all live in vite.config.ts.
export default {
  preprocess: vitePreprocess(),
}
