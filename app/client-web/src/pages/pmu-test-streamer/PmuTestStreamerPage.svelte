<script lang="ts">
  import {
    PlayIcon,
    SquareIcon,
    SkipBackIcon,
    SkipForwardIcon,
    WifiOffIcon,
  } from '@lucide/svelte'

  // This page's own pieces, imported relatively so the folder stays self-contained.
  import { usePmuStreamSocket } from './usePmuStreamSocket.svelte'
  import StreamWindow from './StreamWindow.svelte'
  import { Alert, AlertTitle } from '@/components/ui/alert'
  import { Badge } from '@/components/ui/badge'
  import { Button } from '@/components/ui/button'
  import {
    Card,
    CardAction,
    CardContent,
    CardFooter,
    CardHeader,
    CardTitle,
  } from '@/components/ui/card'

  /**
   * The PMU test streamer page (route `/pmu-test-streamer`) — a scaffold demo
   * streaming sample grid records line by line. A thin renderer over
   * usePmuStreamSocket: it draws the server-pushed
   * window of records, offers the transport controls, and shows a status banner
   * (disabling controls) whenever it isn't connected.
   *
   * There is no sequence picker: one data file, one stream. The records are a one-off
   * PMU sample committed for testing — see
   * app/server-python/src/pmu_test_streamer/sample_data.txt.
   */
  const sock = usePmuStreamSocket()
  const state = $derived(sock.state)
  const status = $derived(sock.status)
  const connected = $derived(sock.connected)
</script>

<Card class="w-full max-w-xl gap-0">
  <CardHeader class="border-b">
    <CardTitle class="text-lg">PMU Test Streamer</CardTitle>
    <span class="text-gray-500">
      Example subapp. Raw dump of PMU data streamed from server (just a static textfile of sample data for now)
    </span>
    <CardAction class="self-center">
      {#if connected}
        <Badge variant={state?.playing ? 'default' : 'secondary'}>
          {state?.playing ? 'Playing' : 'Paused'}
        </Badge>
      {:else}
        <Badge variant="outline" class="text-muted-foreground">
          <WifiOffIcon class="size-3" />
          Offline
        </Badge>
      {/if}
    </CardAction>
  </CardHeader>

  <CardContent class="px-6 py-0">
    <!-- The record window, or the status banner in its place when not connected. -->
    {#if connected && state}
      <StreamWindow window={state.window} />
    {:else}
      <div class="flex min-h-[152px] items-center justify-center">
        <Alert
          variant={status.kind === 'offline' && status.isError ? 'destructive' : 'default'}
          class="w-auto"
        >
          <AlertTitle>
            {status.kind === 'online' ? 'Waiting for state…' : status.label}
          </AlertTitle>
        </Alert>
      </div>
    {/if}
  </CardContent>

  <CardFooter class="flex-col gap-4 border-t pt-6">
    <!-- Position readout — also how the wrap-around at the end of the file
         becomes visible: the count returns to 1 rather than stalling. -->
    <div class="grid w-full max-w-xs grid-cols-[auto_1fr] items-center gap-x-3 gap-y-2">
      <span class="text-right text-sm text-muted-foreground">Record</span>
      <span class="text-sm tabular-nums">
        {state ? `${state.index + 1} of ${state.total_lines}` : '—'}
      </span>
    </div>

    <!-- Transport controls — disabled until connected. -->
    <div class="flex items-center justify-center gap-2">
      <Button variant="outline" size="icon" aria-label="Step back" disabled={!connected} onclick={sock.back}>
        <SkipBackIcon />
      </Button>
      <Button
        variant={state?.playing ? 'outline' : 'default'}
        size="icon"
        aria-label="Play"
        disabled={!connected}
        onclick={sock.play}
      >
        <PlayIcon />
      </Button>
      <Button
        variant={state?.playing ? 'default' : 'outline'}
        size="icon"
        aria-label="Stop"
        disabled={!connected}
        onclick={sock.stop}
      >
        <SquareIcon />
      </Button>
      <Button
        variant="outline"
        size="icon"
        aria-label="Step forward"
        disabled={!connected}
        onclick={sock.forward}
      >
        <SkipForwardIcon />
      </Button>
    </div>
  </CardFooter>
</Card>
