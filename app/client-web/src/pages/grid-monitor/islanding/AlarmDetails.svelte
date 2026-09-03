<script lang="ts">
  import { Button } from '@/components/ui/button'
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from '@/components/ui/table'

  import type { Alarm } from './useIslandingSocket.svelte'

  /**
   * The detail view for one alarm — p-SWAMP's Qt "Alarm details" dock, which is a
   * separate dock below the overview there and an expanding section here.
   *
   * Same three parts as `AlarmHandlingDialogue`: who raised it and when, the full
   * event log, and the operator actions. The Qt dialogue additionally embeds an
   * app-specific view (`alarm_views[app_name]`, e.g. the islanding map for
   * `IslandingApp`) — not repeated here, because that view is already on this
   * screen as its own panel and would be a second copy of the same thing.
   *
   * Event row tints follow the Qt dialogue's `update_message_display`: the raising
   * event is loud, operator actions are muted, and a typed note is visually
   * distinct from both so it reads as human rather than machine.
   */
  const EVENT_STYLES: Record<string, string> = {
    init: 'bg-red-500/15',
    acknowledge: 'bg-red-500/5',
    not_critical: 'bg-red-500/5',
    user_message: 'bg-blue-500/10',
    silence: 'bg-muted',
  }

  function clockTime(epochSeconds: number): string {
    return new Date(epochSeconds * 1000).toLocaleTimeString(undefined, {
      hour12: false,
    })
  }

  let {
    alarm,
    onAcknowledge,
    onSilence,
    onAnnotate,
  }: {
    alarm: Alarm
    onAcknowledge: (uuid: string) => void
    onSilence: (uuid: string) => void
    onAnnotate: (uuid: string, message: string) => void
  } = $props()

  let note = $state('')

  const submitNote = () => {
    const text = note.trim()
    if (!text) return
    onAnnotate(alarm.uuid, text)
    note = ''
  }
</script>

<div class="space-y-4 border-t bg-muted/30 px-6 py-4">
  <dl class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
    <dt class="text-muted-foreground">Detected by</dt>
    <dd class="font-medium">{alarm.app_name}</dd>
    <dt class="text-muted-foreground">Raised</dt>
    <dd class="tabular-nums">{clockTime(alarm.t_start)}</dd>
    {#if alarm.t_end !== null}
      <dt class="text-muted-foreground">Cleared</dt>
      <dd class="tabular-nums">{clockTime(alarm.t_end)}</dd>
    {/if}
    <dt class="text-muted-foreground">Alarm ID</dt>
    <dd class="font-mono text-xs break-all">{alarm.uuid}</dd>
  </dl>

  <div class="overflow-hidden rounded-md border bg-background">
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead class="w-24">Time</TableHead>
          <TableHead class="w-36">Type</TableHead>
          <TableHead>Message</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {#each alarm.events as event, i (`${event.t}-${i}`)}
          <TableRow class={EVENT_STYLES[event.type] ?? undefined}>
            <TableCell class="tabular-nums">{clockTime(event.t)}</TableCell>
            <TableCell class="font-mono text-xs">{event.type}</TableCell>
            <TableCell>{event.message}</TableCell>
          </TableRow>
        {/each}
        {#if alarm.events.length === 0}
          <TableRow>
            <TableCell colspan={3} class="h-12 text-center text-muted-foreground">
              No events recorded.
            </TableCell>
          </TableRow>
        {/if}
      </TableBody>
    </Table>
  </div>

  <div class="flex flex-wrap items-center gap-2">
    <input
      bind:value={note}
      onkeydown={(e) => {
        if (e.key === 'Enter') submitNote()
      }}
      placeholder="Add a note…"
      aria-label="Annotation"
      class="h-9 min-w-0 flex-1 rounded-md border bg-background px-3 text-sm outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50"
    />
    <Button size="sm" variant="outline" disabled={!note.trim()} onclick={submitNote}>
      Annotate
    </Button>
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
  </div>
</div>
