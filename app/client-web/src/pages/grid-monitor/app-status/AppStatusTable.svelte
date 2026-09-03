<script lang="ts">
  import { Badge } from '@/components/ui/badge'
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from '@/components/ui/table'
  import { cn } from '@/lib/utils'

  import type { AppStatusRow, AppStatusValue } from './useAppStatusSocket.svelte'

  /**
   * Status colours, carried over from p-SWAMP's Qt status table so the two read
   * the same at a glance: green healthy, yellow warning, red emergency, blue still
   * starting up.
   */
  const STATUS_STYLES: Record<AppStatusValue, string> = {
    OK: 'border-transparent bg-emerald-600/15 text-emerald-700 dark:text-emerald-400',
    Alert: 'border-transparent bg-amber-500/20 text-amber-700 dark:text-amber-400',
    Emergency: 'border-transparent bg-red-600/15 text-red-700 dark:text-red-400',
    'Initializing...':
      'border-transparent bg-blue-600/15 text-blue-700 dark:text-blue-400',
    Undefined: 'border-transparent bg-muted text-muted-foreground',
  }

  function timeOfDay(epochSeconds: number): string {
    return new Date(epochSeconds * 1000).toLocaleTimeString(undefined, {
      hour12: false,
    })
  }

  let { apps, serverTime }: { apps: AppStatusRow[]; serverTime: number } = $props()
</script>

<Table>
  <TableHeader>
    <TableRow>
      <TableHead>Application</TableHead>
      <TableHead>Status</TableHead>
      <TableHead class="text-right">Data time</TableHead>
      <TableHead class="text-right">Last heard</TableHead>
    </TableRow>
  </TableHeader>
  <TableBody>
    {#each apps as app (app.uuid)}
      <!-- A stale row is dimmed whole rather than recoloured: the status it is
           showing is the last one known, and the point is that it may no
           longer be true. -->
      <TableRow class={cn(app.stale && 'opacity-40')}>
        <TableCell class="font-medium">{app.app_name}</TableCell>
        <TableCell>
          <Badge class={STATUS_STYLES[app.status]}>{app.status}</Badge>
          {#if app.stale}
            <span class="ml-2 text-xs text-muted-foreground">stale</span>
          {/if}
        </TableCell>
        <TableCell class="text-right tabular-nums text-muted-foreground">
          {timeOfDay(app.t)}
        </TableCell>
        <TableCell class="text-right tabular-nums text-muted-foreground">
          {Math.max(0, serverTime - app.received_at).toFixed(1)}s ago
        </TableCell>
      </TableRow>
    {/each}
    {#if apps.length === 0}
      <TableRow>
        <TableCell colspan={4} class="h-24 text-center text-muted-foreground">
          No applications have reported yet.
        </TableCell>
      </TableRow>
    {/if}
  </TableBody>
</Table>
