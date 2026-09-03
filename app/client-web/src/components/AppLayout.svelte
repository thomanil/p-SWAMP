<script lang="ts">
  import type { Snippet } from 'svelte'
  import { ExternalLinkIcon } from '@lucide/svelte'

  import NavLink from '@/components/NavLink.svelte'
  import { CLIENT_ID } from '@/lib/clientId'
  import { BASE_PATH } from '@/lib/basePath'

  /**
   * The shell every page renders inside: a nav bar listing the apps, the outlet
   * the page itself fills, and a footer naming this browser. Wraps the route table
   * in App.svelte, so adding a page means adding a NAV_ITEMS entry plus a <Route>.
   *
   * Nothing here selects a backend: every socket resolves against the origin the
   * client was served from (see src/lib/servers.ts), so the several panels of the
   * grid monitor are necessarily views of one and the same server.
   */
  const NAV_ITEMS = [
    // `end` on the index entry: NavLink matches descendant paths by default, and
    // "/" is a prefix of every route — without it the monitor would render as the
    // active link on every page.
    { to: '/', label: 'Monitor', end: true },
    { to: '/pmu-test-streamer', label: 'PMU Test Streamer', end: false },
    { to: '/reference-subapp', label: 'Reference example', end: false },
  ]

  function isLocalhost(): boolean {
    const { hostname } = window.location
    return hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '[::1]'
  }

  // The git commit this frontend bundle was built from, inlined by Vite from the
  // Dockerfile's GIT_SHA build arg (CI passes github.sha). Empty in dev and in an
  // unstamped build — which is why the footer shows this line only off localhost,
  // where the value comes from a CI-built image and answers "which commit is this
  // deploy?".
  const GIT_SHA = import.meta.env.VITE_GIT_SHA ?? ''

  let { children }: { children: Snippet } = $props()
</script>

<div class="flex min-h-svh flex-col bg-background">
  <header class="flex items-center gap-6 border-b px-6 py-3">
    <span class="font-semibold tracking-tight">P-SWAMP</span>
    <nav class="flex items-center gap-4 text-sm">
      {#each NAV_ITEMS as item (item.to)}
        <NavLink to={item.to} end={item.end}>{item.label}</NavLink>
      {/each}
      {#if isLocalhost()}
        <a
          href={`${BASE_PATH}/docs`}
          target="_blank"
          rel="noreferrer"
          class="flex items-center gap-1 text-muted-foreground transition-colors hover:text-foreground"
        >
          API doc
          <ExternalLinkIcon class="size-3.5" aria-hidden="true" />
        </a>
      {/if}
    </nav>
  </header>

  <main class="flex flex-1 items-center justify-center p-6">
    {@render children()}
  </main>

  <!-- The client id is worth surfacing now that it decides which server-side
       PMU stream you are watching: two browsers showing different instants of
       the recording is correct behaviour, and this is what explains it. Also
       the thing to quote when a server log line names a client.

       Also show what git SHA/version we are running off of, so its easy to see what has been
       deployed to remote env at any time -->
  <footer class="px-6 py-2 text-right text-xs text-muted-foreground/60">
    client id: <span class="font-mono">{CLIENT_ID}</span>
    {#if !isLocalhost()}
      <br />
      built off git SHA:
      <span class="font-mono">{GIT_SHA || 'unknown'}</span>
    {/if}
  </footer>
</div>
