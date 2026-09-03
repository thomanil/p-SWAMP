<script lang="ts">
  import { CompassIcon, WifiOffIcon } from '@lucide/svelte'

  import { Badge } from '@/components/ui/badge'
  import { Button } from '@/components/ui/button'

  import Panel from '../Panel.svelte'
  import { ISLAND_COLORS } from '../islands'
  import type { PanelVariant } from '../variant'
  import PhasorDial from './PhasorDial.svelte'
  import { usePhasorsSocket } from './usePhasorsSocket.svelte'

  /**
   * Voltage phasors — the web counterpart of p-SWAMP's Qt voltage phasor plot.
   *
   * Reads the same measurement window the live measurements panel does — this
   * client's own, one per pipeline.
   * When the recorded line trip separates the northern stations, their phasors
   * drift away from the rest of the dial, coloured by the island the detector
   * assigned them.
   */
  let { variant = 'dashboard' }: { variant?: PanelVariant } = $props()

  let equalLengths = $state(true)
  let rotateToMean = $state(true)
  const sock = usePhasorsSocket()
  // Named `snapshot`, not `state`: a variable named `state` would collide with
  // the `$state` rune (Svelte reads `$state` as subscribing to a `state` store).
  const snapshot = $derived(sock.state)
  const status = $derived(sock.status)
  const connected = $derived(sock.connected)

  const ready = $derived(connected && snapshot !== null)
  const islandCount = $derived(
    snapshot ? new Set(snapshot.phasors.map((p) => p.island ?? 0)).size : 0,
  )
</script>

{#snippet badge()}
  {#if connected}
    <Badge>
      <CompassIcon class="size-3" />
      {snapshot?.phasors.length ?? 0}
    </Badge>
  {:else}
    <Badge variant="outline" class="text-muted-foreground">
      <WifiOffIcon class="size-3" />
      Offline
    </Badge>
  {/if}
{/snippet}

{#snippet footerContent()}
  {`max ${(snapshot!.mag_ref! / 1e3).toFixed(1)} kV` +
    (snapshot!.ang_ref !== null
      ? ` · mean angle ${((snapshot!.ang_ref * 180) / Math.PI).toFixed(1)}°`
      : '')}
{/snippet}

<Panel
  title="Voltage Phasors"
  subtitle="Bus voltage phasors across the Nordic 44 grid, coloured by island"
  {status}
  {ready}
  focusedClassName="w-full max-w-2xl"
  focusHref="/phasors"
  {variant}
  minBodyClass="min-h-[320px]"
  {badge}
  footer={snapshot?.mag_ref ? footerContent : undefined}
>
  <div class="space-y-3">
    <PhasorDial
      phasors={snapshot?.phasors ?? []}
      magRef={snapshot?.mag_ref ?? null}
      angRef={snapshot?.ang_ref ?? null}
      {equalLengths}
      {rotateToMean}
      size={variant === 'dashboard' ? 300 : 420}
    />
    <div class="flex flex-wrap items-center justify-center gap-2">
      <Button
        size="sm"
        variant={equalLengths ? 'default' : 'outline'}
        onclick={() => (equalLengths = !equalLengths)}
      >
        Equal lengths
      </Button>
      <Button
        size="sm"
        variant={rotateToMean ? 'default' : 'outline'}
        onclick={() => (rotateToMean = !rotateToMean)}
      >
        Rotate to mean
      </Button>
      {#if islandCount > 1}
        <span class="flex items-center gap-2 text-xs text-muted-foreground">
          {#each Array.from({ length: islandCount }) as _, i (i)}
            <span class="flex items-center gap-1">
              <span
                class="size-2 rounded-full"
                style={`background: ${ISLAND_COLORS[i % ISLAND_COLORS.length]}`}
              ></span>
              {i === 0 ? 'main' : `island ${i}`}
            </span>
          {/each}
        </span>
      {/if}
    </div>
  </div>
</Panel>
