import js from '@eslint/js'
import globals from 'globals'
import svelte from 'eslint-plugin-svelte'
import tseslint from 'typescript-eslint'
import svelteConfig from './svelte.config.js'
import { defineConfig, globalIgnores } from 'eslint/config'

export default defineConfig([
  // `dist` is the build output. `src/api/schema.ts` is generated from
  // doc/api/openapi.json by scripts/generate-api-contract.sh — it is checked by
  // svelte-check like everything else, but linting a machine-written file only
  // ever produces noise nobody may fix by hand. Regenerate it, don't edit it.
  globalIgnores(['dist', 'src/api/schema.ts']),
  {
    files: ['**/*.{ts,svelte}'],
    extends: [js.configs.recommended, tseslint.configs.recommended],
    languageOptions: {
      globals: globals.browser,
    },
    rules: {
      // `_` is the throwaway item in `{#each Array.from({length}) as _, i}`,
      // where only the index is wanted — the standard convention, so exempt it.
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_', caughtErrorsIgnorePattern: '^_' },
      ],
    },
  },
  // Svelte components and rune modules (*.svelte, *.svelte.ts): parse the markup
  // with svelte-eslint-parser, delegating each `<script lang="ts">` to the
  // TypeScript parser. `svelteConfig` gives the parser the same preprocessor the
  // compiler uses, so `lang="ts"` and Tailwind don't trip it.
  ...svelte.configs.recommended,
  {
    files: ['**/*.svelte', '**/*.svelte.ts'],
    languageOptions: {
      parserOptions: {
        parser: tseslint.parser,
        svelteConfig,
      },
    },
    rules: {
      // Every Map/Set in this client is either rebuilt inside a `$derived` (the
      // island/coord lookups, the channel-picker sets) or deliberately kept out
      // of the reactive graph (the chart's redraw listeners in
      // useTimeWindowSocket — the whole point of that file). None is mutated as
      // reactive state, so SvelteMap/SvelteSet would be the wrong tool here.
      'svelte/prefer-svelte-reactivity': 'off',
    },
  },
])
