<script lang="ts">
  import { onMount } from 'svelte'

  import type { Wire } from '@/api/wire'
  import { Badge } from '@/components/ui/badge'
  import { Button } from '@/components/ui/button'
  import { resolveApiUrl, TIME_WINDOW_API_PATH } from '@/lib/servers'
  import { cn } from '@/lib/utils'

  import type { ChannelInfo } from './useTimeWindowSocket.svelte'

  /** Cap on how many traces can be shown at once — past this they stop being
   *  readable, and the Qt plot draws the same line at 50. */
  const MAX_SELECTED = 12

  /** The catalogue endpoint is HTTP, on the same origin everything else resolves to. */
  function channelsUrl(): string {
    return resolveApiUrl(`${TIME_WINDOW_API_PATH}/channels`)
  }

  let { selected, onChange }: { selected: ChannelInfo[]; onChange: (indices: number[]) => void } =
    $props()

  let all = $state<ChannelInfo[]>([])
  let measurement = $state('f')
  let error = $state<string | null>(null)

  onMount(() => {
    const controller = new AbortController()
    fetch(channelsUrl(), { signal: controller.signal })
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(String(r.status)))))
      .then((body: Wire['ChannelCatalogue']) => {
        if (controller.signal.aborted) return
        all = body.channels
        error = null
      })
      .catch(() => {
        if (!controller.signal.aborted) error = 'Could not load the channel list.'
      })
    return () => controller.abort()
  })

  const measurements = $derived([...new Set(all.map((c) => c.measurement))])
  const visible = $derived(all.filter((c) => c.measurement === measurement))
  const selectedIdx = $derived(new Set(selected.map((c) => c.idx)))

  const toggle = (channel: ChannelInfo) => {
    const next = new Set(selectedIdx)
    if (next.has(channel.idx)) {
      // Never leave the chart with nothing to draw.
      if (next.size === 1) return
      next.delete(channel.idx)
    } else {
      if (next.size >= MAX_SELECTED) return
      next.add(channel.idx)
    }
    onChange([...next])
  }
</script>

{#if error}
  <p class="text-sm text-muted-foreground">{error}</p>
{:else if all.length === 0}
  <p class="text-sm text-muted-foreground">Loading channels…</p>
{:else}
  <div class="space-y-3">
    <div class="flex flex-wrap items-center gap-2">
      <span class="text-sm text-muted-foreground">Measurement</span>
      {#each measurements as m (m)}
        <Button
          size="sm"
          variant={m === measurement ? 'default' : 'outline'}
          onclick={() => (measurement = m)}
        >
          {m}
        </Button>
      {/each}
      <span class="ml-auto text-xs text-muted-foreground tabular-nums">
        {selectedIdx.size}/{MAX_SELECTED} shown
      </span>
    </div>

    <div class="flex max-h-32 flex-wrap gap-1.5 overflow-y-auto">
      {#each visible as channel (channel.idx)}
        {@const on = selectedIdx.has(channel.idx)}
        <Badge
          variant={on ? 'default' : 'outline'}
          class={cn('cursor-pointer select-none font-mono text-xs', !on && 'text-muted-foreground')}
          onclick={() => toggle(channel)}
        >
          {channel.station}
        </Badge>
      {/each}
    </div>
  </div>
{/if}
