<script lang="ts">
  import { BellIcon, WifiOffIcon } from '@lucide/svelte'

  import { Badge } from '@/components/ui/badge'

  import Panel from '../Panel.svelte'
  import type { PanelVariant } from '../variant'
  import AlarmDetails from './AlarmDetails.svelte'
  import AlarmTable from './AlarmTable.svelte'
  import { useIslandingData } from './islandingContext'

  /**
   * The alarm overview — p-SWAMP's Qt alarm dock.
   *
   * Reads the same socket as the islanding map: alarms are derived from the
   * detector's status, and the server sends both together so a client cannot
   * render them inconsistently.
   */
  let { variant = 'dashboard' }: { variant?: PanelVariant } = $props()

  const data = useIslandingData()
  // Named `snapshot`, not `state`: a variable named `state` would collide with
  // the `$state` rune (Svelte reads `$state` as subscribing to a `state` store).
  const snapshot = $derived(data.state)
  const status = $derived(data.status)
  const connected = $derived(data.connected)

  let selectedUuid = $state<string | null>(null)

  const alarms = $derived(snapshot?.alarms.alarms ?? [])
  // Resolved from the live list rather than held in local state, so an open pane
  // keeps updating as events land on that alarm. A selection that disappears (the
  // store is bounded) simply closes the pane.
  const selected = $derived(alarms.find((a) => a.uuid === selectedUuid) ?? null)
  const unseen = $derived(alarms.filter((a) => a.status === 'unseen').length)
  const ready = $derived(connected && snapshot !== null)
</script>

{#snippet badge()}
  {#if connected}
    <Badge
      variant={unseen > 0 ? 'default' : 'secondary'}
      class={unseen > 0
        ? 'border-transparent bg-red-600/20 text-red-700 dark:text-red-400'
        : undefined}
    >
      <BellIcon class="size-3" />
      {unseen > 0 ? `${unseen} unseen` : `${alarms.length}`}
    </Badge>
  {:else}
    <Badge variant="outline" class="text-muted-foreground">
      <WifiOffIcon class="size-3" />
      Offline
    </Badge>
  {/if}
{/snippet}

<Panel
  title="Alarms"
  subtitle="Raised by the monitoring applications"
  {status}
  {ready}
  focusHref="/islanding"
  {variant}
  minBodyClass="min-h-[160px]"
  contentClassName="px-0 pt-0"
  {badge}
>
  <!-- Capped and scrolled on the dashboard: alarms accumulate over a session,
       and a growing table would push every panel below it down the page. -->
  <div class={variant === 'dashboard' ? 'max-h-[220px] overflow-y-auto' : undefined}>
    <AlarmTable
      {alarms}
      {selectedUuid}
      onSelect={(uuid) => (selectedUuid = uuid)}
      onAcknowledge={data.acknowledge}
      onSilence={data.silence}
    />
  </div>

  {#if selected}
    <AlarmDetails
      alarm={selected}
      onAcknowledge={data.acknowledge}
      onSilence={data.silence}
      onAnnotate={data.annotate}
    />
  {/if}
</Panel>
