<script lang="ts">
  import { onMount } from 'svelte'

  import type { Wire } from '@/api/wire'
  import { GRID_MODEL_PATH, resolveApiUrl } from '@/lib/servers'

  import { islandColor } from '../islands'
  import type { Island } from './useIslandingSocket.svelte'

  /** The static topology, straight from the contract (`GET /api/grid/model`).
   *  Note the fields stay snake_case here: unlike the socket payloads there is no
   *  mapping layer on this one, and the drawing code below reads them directly. */
  type GridModel = Wire['GridModel']

  const VIEW_W = 560
  const VIEW_H = 700

  /**
   * The grid, drawn geographically, recoloured by island.
   *
   * Hand-drawn SVG rather than a map or chart library: this is a few hundred
   * elements updating once a second, which SVG handles without help, and no
   * charting library draws a node-link diagram on geographic coordinates anyway.
   *
   * It is also the seed of the richer grid view p-SWAMP has in Qt — a 2D
   * single-line diagram and a 3D surface that deforms with frequency. Those will
   * need WebGL and a server-side conversion of the DXF diagrams, so the projection
   * and the node/edge lookup are kept behind a small seam here rather than spread
   * through the drawing code.
   */
  let {
    islands,
    height = 380,
  }: {
    islands: Island[]
    /** Rendered height in CSS pixels. The viewBox scales the geometry, so a
     *  compact dashboard map and a full-size one are identical drawings. */
    height?: number
  } = $props()

  let model = $state<GridModel | null>(null)

  onMount(() => {
    const controller = new AbortController()
    const url = resolveApiUrl(GRID_MODEL_PATH)
    fetch(url, { signal: controller.signal })
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(String(r.status)))))
      .then((body) => {
        if (!controller.signal.aborted) model = body as GridModel
      })
      .catch(() => {
        if (!controller.signal.aborted) model = null
      })
    return () => controller.abort()
  })

  // station -> island index, so a node or a branch can be coloured in O(1).
  const islandOf = $derived.by(() => {
    const map = new Map<string, number>()
    islands.forEach((island) => island.stations.forEach((s) => map.set(s, island.index)))
    return map
  })

  const project = $derived.by((): ((lon: number, lat: number) => [number, number]) | null => {
    if (!model) return null
    const [lonMin, latMin, lonMax, latMax] = model.bbox
    // Equirectangular, with longitude squeezed by cos(latitude): at Nordic
    // latitudes a degree of longitude is about half a degree of latitude, and
    // without the correction the map looks stretched sideways.
    const midLat = ((latMin + latMax) / 2) * (Math.PI / 180)
    const lonScale = Math.cos(midLat)
    const w = (lonMax - lonMin) * lonScale
    const h = latMax - latMin
    const scale = Math.min(VIEW_W / w, VIEW_H / h)
    const offsetX = (VIEW_W - w * scale) / 2
    const offsetY = (VIEW_H - h * scale) / 2
    return (lon: number, lat: number): [number, number] => [
      offsetX + (lon - lonMin) * lonScale * scale,
      // SVG y grows downward; latitude grows northward.
      VIEW_H - offsetY - (lat - latMin) * scale,
    ]
  })

  const coords = $derived.by(() => {
    const map = new Map<string, [number, number]>()
    if (model && project) {
      model.pmus.forEach((p) => map.set(p.name, project(p.lon, p.lat)))
    }
    return map
  })

  const colorFor = (station: string) => islandColor(islandOf.get(station))
</script>

{#if !model || !project}
  <div
    style={`height: ${height}px`}
    class="flex items-center justify-center text-sm text-muted-foreground"
  >
    Loading grid model…
  </div>
{:else}
  <svg
    viewBox={`0 0 ${VIEW_W} ${VIEW_H}`}
    style={`height: ${height}px`}
    class="w-full"
    role="img"
    aria-label="Nordic 44 grid, coloured by detected island"
  >
    {#each model.branches as branch (`${branch.kind}-${branch.name}`)}
      {@const a = coords.get(branch.from_bus)}
      {@const b = coords.get(branch.to_bus)}
      {#if a && b}
        {@const islandA = islandOf.get(branch.from_bus) ?? 0}
        {@const islandB = islandOf.get(branch.to_bus) ?? 0}
        <!-- A branch whose ends are in different islands is the boundary of the
             split — the same rule the Qt view uses to colour branches. -->
        {@const split = islandA !== islandB}
        <line
          x1={a[0]}
          y1={a[1]}
          x2={b[0]}
          y2={b[1]}
          stroke={split ? '#ef4444' : colorFor(branch.from_bus)}
          stroke-width={split ? 2 : 1}
          stroke-dasharray={split ? '4 3' : undefined}
          opacity={split ? 0.95 : 0.35}
        />
      {/if}
    {/each}

    {#each model.pmus as pmu (pmu.name)}
      {@const point = coords.get(pmu.name)}
      {#if point}
        {@const island = islandOf.get(pmu.name) ?? 0}
        <circle
          cx={point[0]}
          cy={point[1]}
          r={island === 0 ? 3 : 5}
          fill={colorFor(pmu.name)}
          stroke="white"
          stroke-width={island === 0 ? 0.5 : 1}
        />
      {/if}
    {/each}
  </svg>
{/if}
