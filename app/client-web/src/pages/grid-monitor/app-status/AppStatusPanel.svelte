<script lang="ts">
  import { RadioIcon, WifiOffIcon } from '@lucide/svelte'

  import { Badge } from '@/components/ui/badge'

  import Panel from '../Panel.svelte'
  import type { PanelVariant } from '../variant'
  import AppStatusTable from './AppStatusTable.svelte'
  import { useAppStatusSocket } from './useAppStatusSocket.svelte'

  /**
   * Which monitoring applications are running and what they report — the web
   * counterpart of p-SWAMP's Qt status dock.
   *
   * The applications listed here are this browser's own: every client gets its own
   * pipeline, so two browsers watching at once see two independent sets of rows at
   * two points in the recording. The Qt version's Stop and Open-console buttons
   * are deliberately absent: both publish to a command topic that only exists with
   * a broker behind it.
   */
  let { variant = 'dashboard' }: { variant?: PanelVariant } = $props()

  const sock = useAppStatusSocket()
  const state = $derived(sock.state)
  const status = $derived(sock.status)
  const connected = $derived(sock.connected)
  const replay = $derived(state?.replay)
  const ready = $derived(connected && state !== null)
</script>

{#snippet badge()}
  {#if connected}
    <Badge variant={replay?.playing ? 'default' : 'secondary'}>
      <RadioIcon class="size-3" />
      {replay?.playing ? 'Streaming' : 'Stopped'}
    </Badge>
  {:else}
    <Badge variant="outline" class="text-muted-foreground">
      <WifiOffIcon class="size-3" />
      Offline
    </Badge>
  {/if}
{/snippet}

<!-- The only view of the replay position anywhere in the client — keep it. -->
{#snippet footerContent()}
  {`PMU replay · ${replay!.position.toFixed(1)}s / ${replay!.duration.toFixed(0)}s · ${replay!.n_channels} channels @ ${replay!.data_rate} Hz`}
{/snippet}

<Panel
  title="Application Status"
  subtitle="Monitoring applications running against the replayed PMU stream"
  {status}
  {ready}
  focusedClassName="w-full max-w-3xl"
  focusHref="/app-status"
  {variant}
  minBodyClass="min-h-[152px]"
  contentClassName="px-0 pt-0"
  {badge}
  footer={replay ? footerContent : undefined}
>
  <AppStatusTable apps={state?.apps ?? []} serverTime={state?.server_time ?? 0} />
</Panel>
