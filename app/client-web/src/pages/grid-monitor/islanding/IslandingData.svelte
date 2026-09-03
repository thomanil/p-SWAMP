<script lang="ts">
  import type { Snippet } from 'svelte'

  import { setIslandingData } from './islandingContext'
  import { useIslandingSocket } from './useIslandingSocket.svelte'

  /**
   * Opens the islanding socket once and shares it with the panels beneath.
   *
   * The context value's getters read the socket's reactive state, so only the
   * panels that actually read a given field re-render when it changes — the map
   * and the alarm table update independently off the one connection, rather than
   * both re-rendering at the socket's full rate.
   */
  let { children }: { children: Snippet } = $props()

  setIslandingData(useIslandingSocket())
</script>

{@render children()}
