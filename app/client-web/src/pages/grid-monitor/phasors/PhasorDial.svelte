<script lang="ts">
  import { ISLAND_COLORS } from '../islands'
  import type { Phasor } from './usePhasorsSocket.svelte'

  // The internal coordinate space. Fixed, because the viewBox scales it to
  // whatever CSS size the panel asks for — so a compact dashboard dial and a
  // full-size one share every bit of geometry below.
  const VIEW = 420
  const CENTER = VIEW / 2
  const RADIUS = VIEW / 2 - 28
  /** Label size in view units at full size; scaled up as the dial shrinks so text
   *  stays about as legible on screen either way. */
  const LABEL_PX = 9
  /** Concentric magnitude rings and radial angle spokes, as the Qt plot draws. */
  const RINGS = 5
  const SPOKE_DEGREES = 30

  /**
   * Voltage phasors on a polar dial.
   *
   * Hand-drawn SVG: no charting library draws a polar plot with arrowheads, and at
   * 44 elements updating ten times a second there is nothing to optimise — the
   * wrapper would be larger than the drawing code.
   */
  let {
    phasors,
    magRef,
    angRef,
    equalLengths,
    rotateToMean,
    size = VIEW,
  }: {
    phasors: Phasor[]
    magRef: number | null
    angRef: number | null
    /** Draw every phasor the same length, so only the angles differ — p-SWAMP's
     *  `normalize_length`. Magnitudes across the grid vary by a few percent, so
     *  scaling by them makes the angle spread hard to read. */
    equalLengths: boolean
    rotateToMean: boolean
    /** Rendered size in CSS pixels. Geometry is unaffected; the viewBox scales. */
    size?: number
  } = $props()

  const arrows = $derived.by(() => {
    const rotation = rotateToMean ? (angRef ?? 0) : 0

    return phasors
      .filter((p) => p.mag !== null && p.ang !== null && (equalLengths || magRef))
      .map((p) => {
        const r = equalLengths
          ? RADIUS
          : (Math.min(p.mag as number, magRef as number) / (magRef as number)) * RADIUS
        const theta = (p.ang as number) - rotation
        const island = (p.island ?? 0) % ISLAND_COLORS.length
        // SVG y grows downward, so the sine is negated to put 0 rad at the right
        // and positive angles anticlockwise, as a phasor diagram expects.
        return {
          station: p.station,
          island,
          color: ISLAND_COLORS[island],
          x: CENTER + r * Math.cos(theta),
          y: CENTER - r * Math.sin(theta),
        }
      })
  })

  // Labels last so they sit above the arrows. Only the outermost few are worth
  // naming; 44 labels on one dial is unreadable.
  const labels = $derived(
    arrows
      .slice()
      .sort(
        (a, b) =>
          Math.hypot(b.x - CENTER, b.y - CENTER) - Math.hypot(a.x - CENTER, a.y - CENTER),
      )
      .slice(0, 8),
  )
</script>

<svg
  viewBox={`0 0 ${VIEW} ${VIEW}`}
  style={`height: ${size}px; max-width: ${size}px`}
  class="mx-auto w-full"
  role="img"
  aria-label="Voltage phasors by station"
>
  <defs>
    <!-- One marker per colour: SVG markers cannot inherit the line's stroke
         in a way all browsers honour, so each island gets its own. -->
    {#each ISLAND_COLORS as color, i (i)}
      <marker
        id={`phasor-head-${i}`}
        viewBox="0 0 10 10"
        refX="9"
        refY="5"
        markerWidth="5"
        markerHeight="5"
        orient="auto-start-reverse"
      >
        <path d="M 0 0 L 10 5 L 0 10 z" fill={color} />
      </marker>
    {/each}
  </defs>

  {#each Array.from({ length: RINGS }) as _, i (i)}
    <circle
      cx={CENTER}
      cy={CENTER}
      r={(RADIUS * (i + 1)) / RINGS}
      fill="none"
      stroke="currentColor"
      stroke-opacity={0.15}
    />
  {/each}

  {#each Array.from({ length: 360 / SPOKE_DEGREES }) as _, i (i)}
    {@const theta = (i * SPOKE_DEGREES * Math.PI) / 180}
    <line
      x1={CENTER}
      y1={CENTER}
      x2={CENTER + RADIUS * Math.cos(theta)}
      y2={CENTER - RADIUS * Math.sin(theta)}
      stroke="currentColor"
      stroke-opacity={0.1}
    />
  {/each}

  {#each arrows as arrow (arrow.station)}
    <line
      x1={CENTER}
      y1={CENTER}
      x2={arrow.x}
      y2={arrow.y}
      stroke={arrow.color}
      stroke-width={1.5}
      stroke-opacity={0.85}
      marker-end={`url(#phasor-head-${arrow.island})`}
    />
  {/each}

  {#each labels as arrow (arrow.station)}
    <text
      x={arrow.x}
      y={arrow.y}
      dx={arrow.x >= CENTER ? 4 : -4}
      dy={arrow.y >= CENTER ? 10 : -4}
      text-anchor={arrow.x >= CENTER ? 'start' : 'end'}
      font-size={LABEL_PX * (VIEW / size)}
      class="fill-current opacity-60"
    >
      {arrow.station}
    </text>
  {/each}
</svg>
