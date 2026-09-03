<script lang="ts">
  import { WifiOffIcon } from '@lucide/svelte'

  // This page's own pieces, imported relatively so the folder stays self-contained.
  import { useReferenceSubappSocket } from './useReferenceSubappSocket.svelte'
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
   * The Reference example page (route `/reference-subapp`) — a thin renderer over
   * useReferenceSubappSocket: the buttons POST commands, the server owns the count and
   * pushes it back down the socket, and this component only renders what it is
   * handed.
   */
  const sock = useReferenceSubappSocket()
  // Reactive locals over the socket's getters: a plain identifier narrows the
  // `status` union (…kind === 'offline' && …isError), which a getter chain would not.
  const state = $derived(sock.state)
  const status = $derived(sock.status)
  const connected = $derived(sock.connected)
</script>

<Card class="w-full max-w-xl gap-0">
  <CardHeader class="border-b">
    <CardTitle class="text-lg">Reference example</CardTitle>
    <span class="text-gray-500">
      Used for end to end tests of our client-server stack. When we refactor or upgrade the project, we use this page as a smoketest to see if anything fundamental breaks.
      <em>
        Do not use this page to do p-SWAMP experiments; use <code>generate-new-subapp.sh</code> to generate a new
        page/subapp instead. 🙂</em>
    </span>
    <CardAction class="self-center">
      {#if connected}
        <Badge variant="default">Online</Badge>
      {:else}
        <Badge variant="outline" class="text-muted-foreground">
          <WifiOffIcon class="size-3" />
          Offline
        </Badge>
      {/if}
    </CardAction>
  </CardHeader>

  <CardContent class="px-6 py-0">
    <!-- The server-owned state, or the status banner in its place. -->
    <div class="flex min-h-[152px] items-center justify-center">
      {#if connected && state}
        <span class="text-6xl font-bold tabular-nums">{state.count}</span>
      {:else}
        <Alert
          variant={status.kind === 'offline' && status.isError ? 'destructive' : 'default'}
          class="w-auto"
        >
          <AlertTitle>
            {status.kind === 'online' ? 'Waiting for state…' : status.label}
          </AlertTitle>
        </Alert>
      {/if}
    </div>
  </CardContent>

  <CardFooter class="flex-col gap-4 border-t pt-6">
    <!-- Disabled until connected. The commands are POSTs and would in fact
         reach the server without a socket — but the result comes back *on*
         the socket, so a click with none open would appear to do nothing. -->
    <div class="flex items-center justify-center gap-2">
      <Button disabled={!connected} onclick={sock.bump}>Bump</Button>
      <Button variant="outline" disabled={!connected} onclick={sock.reset}>Reset</Button>
    </div>
  </CardFooter>
</Card>
