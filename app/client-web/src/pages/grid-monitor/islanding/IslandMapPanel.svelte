<script lang="ts">
  import { AlertTriangleIcon, CheckCircle2Icon, WifiOffIcon } from '@lucide/svelte'

  import { Badge } from '@/components/ui/badge'

  import Panel from '../Panel.svelte'
  import { islandColor } from '../islands'
  import type { PanelVariant } from '../variant'
  import IslandMap from './IslandMap.svelte'
  import { useIslandingData } from './islandingContext'

  /**
   * Islanding detection over the grid map — the web counterpart of p-SWAMP's Qt
   * islanding alarm view.
   *
   * About twenty seconds into each pass of the recording four lines trip, the
   * northern stations separate onto their own frequency, and the map recolours.
   * Twenty seconds later they reconnect.
   *
   * Shares its socket with the alarms panel, so the two go offline together — they
   * are one connection, and pretending otherwise would be a lie about the state of
   * the system.
   */
  let { variant = 'dashboard' }: { variant?: PanelVariant } = $props()

  const data = useIslandingData()
  const state = $derived(data.state)
  const status = $derived(data.status)
  const connected = $derived(data.connected)

  const islands = $derived(state?.islanding?.islands ?? [])
  // Index 0 is the main system, so anything beyond it is a genuine split.
  const separated = $derived(islands.filter((i) => i.index > 0))
  const ready = $derived(connected && state !== null)
</script>

{#snippet badge()}
  {#if connected}
    {#if separated.length > 0}
      <Badge class="border-transparent bg-red-600/20 text-red-700 dark:text-red-400">
        <AlertTriangleIcon class="size-3" />
        {separated.length} island{separated.length > 1 ? 's' : ''}
      </Badge>
    {:else}
      <Badge class="border-transparent bg-emerald-600/15 text-emerald-700 dark:text-emerald-400">
        <CheckCircle2Icon class="size-3" />
        Intact
      </Badge>
    {/if}
  {:else}
    <Badge variant="outline" class="text-muted-foreground">
      <WifiOffIcon class="size-3" />
      Offline
    </Badge>
  {/if}
{/snippet}

{#snippet footerContent()}
  {`${state!.islanding!.parameters.window_length.toFixed(0)}s window · threshold ${state!.islanding!.parameters.mean_threshold}`}
{/snippet}

<Panel
  title="Islanding Detection"
  subtitle="Frequency-based island detection across the Nordic 44 grid"
  {status}
  {ready}
  drawsWithoutData
  focusHref="/islanding"
  {variant}
  minBodyClass="min-h-[300px]"
  {badge}
  footer={state?.islanding ? footerContent : undefined}
>
  <div class="space-y-3">
    <IslandMap {islands} height={variant === 'dashboard' ? 300 : 420} />
    <div class="space-y-1">
      {#each islands as island (island.index)}
        <div class="flex items-start gap-2 text-sm">
          <span
            class="mt-1 size-3 shrink-0 rounded-full"
            style={`background: ${islandColor(island.index)}`}
          ></span>
          <div class="min-w-0">
            <span class="font-medium">
              {island.index === 0 ? 'Main system' : `Island ${island.index}`}
            </span>
            <span class="ml-2 text-muted-foreground tabular-nums">
              {island.stations.length} stations
              {island.mean_freq !== null ? ` · ${island.mean_freq.toFixed(3)} Hz` : ''}
            </span>
            {#if island.index > 0}
              <div class="truncate font-mono text-xs text-muted-foreground">
                {island.stations.join(', ')}
              </div>
            {/if}
          </div>
        </div>
      {/each}
      {#if islands.length === 0}
        <p class="text-sm text-muted-foreground">
          {ready
            ? 'Waiting for the first assessment…'
            : 'Grid topology shown; stations are uncoloured until the detector reports.'}
        </p>
      {/if}
    </div>
  </div>
</Panel>
