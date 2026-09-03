<script lang="ts">
  import { ActivityIcon, WifiOffIcon } from '@lucide/svelte'

  import { Badge } from '@/components/ui/badge'

  import Panel from '../Panel.svelte'
  import type { PanelVariant } from '../variant'
  import ChannelPicker from './ChannelPicker.svelte'
  import TimeWindowChart from './TimeWindowChart.svelte'
  import { useTimeWindowSocket } from './useTimeWindowSocket.svelte'

  /**
   * Live measurements — a moving window of the PMU stream, as the monitoring
   * applications see it. The web counterpart of p-SWAMP's Qt time window plot.
   *
   * With the default frequency channels selected, the recorded line trip is
   * visible directly: three northern stations separate onto their own frequency
   * about twenty seconds into each pass of the recording, and rejoin twenty
   * seconds later.
   *
   * The chart height is a constant per variant on purpose — it is a dependency of
   * the effect that builds the plot, so a measured value would destroy and rebuild
   * the plot on every reflow.
   */
  const CHART_HEIGHT = { dashboard: 260, focused: 380 }

  let { variant = 'dashboard' }: { variant?: PanelVariant } = $props()

  const sock = useTimeWindowSocket()
  const status = $derived(sock.status)
  const connected = $derived(sock.connected)
  const channels = $derived(sock.channels)
  const samplingRate = $derived(sock.samplingRate)

  const ready = $derived(connected && channels.length > 0)
</script>

{#snippet badge()}
  {#if connected}
    <Badge>
      <ActivityIcon class="size-3" />
      {samplingRate ? `${samplingRate} Hz` : 'Live'}
    </Badge>
  {:else}
    <Badge variant="outline" class="text-muted-foreground">
      <WifiOffIcon class="size-3" />
      Offline
    </Badge>
  {/if}
{/snippet}

{#snippet footerContent()}
  {`${channels.length} channels · ${samplingRate ?? '?'} Hz`}
{/snippet}

<Panel
  title="Live Measurements"
  subtitle="A moving window of the Nordic 44 PMU stream, as the monitoring applications see it"
  {status}
  {ready}
  focusedClassName="w-full max-w-5xl"
  focusHref="/time-window"
  {variant}
  minBodyClass="min-h-[260px]"
  {badge}
  footer={ready ? footerContent : undefined}
>
  <div class="space-y-4">
    <TimeWindowChart
      buffer={sock.buffer}
      subscribe={sock.subscribe}
      {channels}
      height={CHART_HEIGHT[variant]}
    />
    <ChannelPicker selected={channels} onChange={sock.selectChannels} />
  </div>
</Panel>
