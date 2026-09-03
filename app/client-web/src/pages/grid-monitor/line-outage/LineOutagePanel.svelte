<script lang="ts">
  import { PlugZapIcon, WifiOffIcon } from '@lucide/svelte'

  import { Badge } from '@/components/ui/badge'
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from '@/components/ui/table'

  import Panel from '../Panel.svelte'
  import type { PanelVariant } from '../variant'
  import { useLineOutageSocket } from './useLineOutageSocket.svelte'

  function clockTime(epochSeconds: number): string {
    return new Date(epochSeconds * 1000).toLocaleTimeString(undefined, {
      hour12: false,
    })
  }

  /**
   * Branch connect/disconnect events, from p-SWAMP's `LineOutageDetectionApp`.
   *
   * A log rather than a state view: the detector publishes nothing at all while
   * the grid is intact, so an empty table is the healthy case and says so.
   *
   * Colours follow the alarm table's convention — a disconnect is loud, a
   * reconnect is not — rather than inventing a second palette for the same idea.
   */
  let { variant = 'dashboard' }: { variant?: PanelVariant } = $props()

  const sock = useLineOutageSocket()
  const state = $derived(sock.state)
  const status = $derived(sock.status)
  const connected = $derived(sock.connected)
  const events = $derived(state?.events ?? [])
  const ready = $derived(connected && state !== null)
  const outages = $derived(events.filter((e) => e.kind === 'disconnect').length)
</script>

{#snippet badge()}
  {#if connected}
    <Badge variant={outages > 0 ? 'destructive' : 'secondary'}>
      <PlugZapIcon class="size-3" />
      {events.length === 0 ? 'No events' : `${events.length} events`}
    </Badge>
  {:else}
    <Badge variant="outline" class="text-muted-foreground">
      <WifiOffIcon class="size-3" />
      Offline
    </Badge>
  {/if}
{/snippet}

{#snippet footerContent()}
  {`${state!.app_name ?? 'LineOutageDetectionApp'} · ${state!.window_length!.toFixed(2)}s window`}
{/snippet}

<Panel
  title="Line Outages"
  subtitle="Branches whose current magnitude dropped to zero, and their recovery"
  {status}
  {ready}
  focusedClassName="w-full max-w-4xl"
  focusHref="/line-outage"
  {variant}
  minBodyClass="min-h-[152px]"
  contentClassName="px-0 pt-0"
  {badge}
  footer={state?.window_length ? footerContent : undefined}
>
  <Table>
    <TableHeader>
      <TableRow>
        <TableHead>Time</TableHead>
        <TableHead>Event</TableHead>
        <TableHead>Branches</TableHead>
        <TableHead class="text-right">Stations</TableHead>
      </TableRow>
    </TableHeader>
    <TableBody>
      {#each events as event, i (`${event.t}-${event.kind}-${i}`)}
        <TableRow>
          <TableCell class="tabular-nums">{clockTime(event.t)}</TableCell>
          <TableCell>
            <Badge
              class={event.kind === 'disconnect'
                ? 'border-transparent bg-red-600/20 text-red-700 dark:text-red-400'
                : 'border-transparent bg-emerald-600/15 text-emerald-700 dark:text-emerald-400'}
            >
              {event.kind === 'disconnect' ? 'Disconnected' : 'Reconnected'}
            </Badge>
          </TableCell>
          <TableCell class="font-mono text-xs">{event.branches.join(', ')}</TableCell>
          <TableCell class="text-right text-muted-foreground tabular-nums">
            {new Set(event.stations).size}
          </TableCell>
        </TableRow>
      {/each}
      {#if events.length === 0}
        <TableRow>
          <TableCell colspan={4} class="h-20 text-center text-muted-foreground">
            All branches carrying current.
          </TableCell>
        </TableRow>
      {/if}
    </TableBody>
  </Table>
</Panel>
