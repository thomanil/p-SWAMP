<script lang="ts">
  import { ChevronDownIcon, ChevronUpIcon } from '@lucide/svelte'

  import { Badge } from '@/components/ui/badge'
  import { Button } from '@/components/ui/button'
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from '@/components/ui/table'
  import { cn } from '@/lib/utils'

  import type { Alarm, AlarmStatus } from './useIslandingSocket.svelte'

  /**
   * Alarm row colours, following p-SWAMP's Qt alarm overview: an unseen alarm is
   * loud, one that has been handled or has cleared is quiet, a silenced one is
   * greyed out.
   */
  const STATUS_STYLES: Record<AlarmStatus, string> = {
    unseen: 'border-transparent bg-red-600/20 text-red-700 dark:text-red-400',
    acknowledged:
      'border-transparent bg-amber-500/20 text-amber-700 dark:text-amber-400',
    not_critical:
      'border-transparent bg-emerald-600/15 text-emerald-700 dark:text-emerald-400',
    silenced: 'border-transparent bg-muted text-muted-foreground',
  }

  const STATUS_LABELS: Record<AlarmStatus, string> = {
    unseen: 'Unseen',
    acknowledged: 'Acknowledged',
    not_critical: 'Cleared',
    silenced: 'Silenced',
  }

  function clockTime(epochSeconds: number): string {
    return new Date(epochSeconds * 1000).toLocaleTimeString(undefined, {
      hour12: false,
    })
  }

  let {
    alarms,
    selectedUuid = null,
    onSelect,
    onAcknowledge,
    onSilence,
  }: {
    alarms: Alarm[]
    /** The alarm whose details are open, if any. */
    selectedUuid?: string | null
    /** Toggles the detail pane. Passing null closes it. */
    onSelect?: (uuid: string | null) => void
    onAcknowledge: (uuid: string) => void
    onSilence: (uuid: string) => void
  } = $props()
</script>

<Table>
  <TableHeader>
    <TableRow>
      {#if onSelect}
        <TableHead class="w-8" />
      {/if}
      <TableHead>Raised</TableHead>
      <TableHead>Application</TableHead>
      <TableHead>Status</TableHead>
      <TableHead class="text-right">Actions</TableHead>
    </TableRow>
  </TableHeader>
  <TableBody>
    {#each alarms as alarm (alarm.uuid)}
      <TableRow
        class={cn(onSelect && 'cursor-pointer', selectedUuid === alarm.uuid && 'bg-muted/60')}
        onclick={onSelect
          ? () => onSelect(selectedUuid === alarm.uuid ? null : alarm.uuid)
          : undefined}
      >
        {#if onSelect}
          <TableCell class="text-muted-foreground">
            {#if selectedUuid === alarm.uuid}
              <ChevronUpIcon class="size-4" />
            {:else}
              <ChevronDownIcon class="size-4" />
            {/if}
          </TableCell>
        {/if}
        <TableCell class="tabular-nums">
          {clockTime(alarm.t_start)}
          {#if alarm.t_end !== null}
            <span class="text-muted-foreground"> – {clockTime(alarm.t_end)}</span>
          {/if}
        </TableCell>
        <TableCell class="font-medium">{alarm.app_name}</TableCell>
        <TableCell>
          <Badge class={STATUS_STYLES[alarm.status]}>{STATUS_LABELS[alarm.status]}</Badge>
        </TableCell>
        <TableCell class="space-x-2 text-right" onclick={(e) => e.stopPropagation()}>
          <Button
            size="sm"
            variant="outline"
            disabled={alarm.status !== 'unseen'}
            onclick={() => onAcknowledge(alarm.uuid)}
          >
            Acknowledge
          </Button>
          <Button
            size="sm"
            variant="ghost"
            disabled={alarm.status === 'silenced'}
            onclick={() => onSilence(alarm.uuid)}
          >
            Silence
          </Button>
        </TableCell>
      </TableRow>
    {/each}
    {#if alarms.length === 0}
      <TableRow>
        <TableCell colspan={onSelect ? 5 : 4} class="h-20 text-center text-muted-foreground">
          No alarms raised.
        </TableCell>
      </TableRow>
    {/if}
  </TableBody>
</Table>
