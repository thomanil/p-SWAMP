<script lang="ts">
  import uPlot from 'uplot'
  import 'uplot/dist/uPlot.min.css'

  import type { WindowBuffer } from './useTimeWindowSocket.svelte'

  /**
   * Trace colours. Enough to keep a dozen simultaneous channels distinguishable,
   * and chosen to stay legible on both the light and dark page backgrounds.
   */
  const TRACE_COLORS = [
    '#2563eb',
    '#dc2626',
    '#059669',
    '#d97706',
    '#7c3aed',
    '#0891b2',
    '#db2777',
    '#65a30d',
    '#0284c7',
    '#b45309',
    '#4f46e5',
    '#be123c',
  ]

  /**
   * A streaming multi-channel chart.
   *
   * uPlot on a canvas, driven imperatively: the data arrives ten times a second
   * over a window of a few thousand points per channel, which is well past what
   * SVG or a reactive charting approach will do comfortably. Redraws come through
   * the socket module's `subscribe`, not through props, so **no re-render happens
   * on the data path at all** — this component rebuilds the plot only when the set
   * of channels changes.
   */
  let {
    buffer,
    subscribe,
    channels,
    height = 320,
  }: {
    buffer: WindowBuffer
    subscribe: (notify: () => void) => () => void
    /** Only used to detect a shape change; the values are read from `buffer`. */
    channels: { idx: number; label: string }[]
    height?: number
  } = $props()

  let container: HTMLDivElement
  let plot: uPlot | null = null

  // Build (or rebuild) the plot when the channel set changes.
  $effect(() => {
    // Track the shape (and height) so a selection change rebuilds the plot.
    const chans = channels
    const h = height
    const node = container
    if (!node || channels.length === 0) return

    plot?.destroy()
    plot = new uPlot(
      {
        width: node.clientWidth || 640,
        height: h,
        // Times are epoch seconds, uPlot's native x scale, so the axis formats
        // as wall-clock time with no extra configuration.
        series: [
          {},
          ...chans.map((channel, i) => ({
            label: channel.label,
            stroke: TRACE_COLORS[i % TRACE_COLORS.length],
            width: 1.5,
            // A missing sample is null; joining across it would draw a straight
            // line through data that was never measured.
            spanGaps: false,
          })),
        ],
        axes: [
          { stroke: 'currentColor', grid: { stroke: 'rgba(128,128,128,0.15)' } },
          { stroke: 'currentColor', grid: { stroke: 'rgba(128,128,128,0.15)' } },
        ],
        legend: { live: true },
        cursor: { drag: { x: true, y: false } },
      },
      [buffer.t, ...buffer.series] as unknown as uPlot.AlignedData,
      node,
    )

    return () => {
      plot?.destroy()
      plot = null
    }
  })

  // Redraw on each new batch of samples. Outside the reactive cycle entirely.
  $effect(() =>
    subscribe(() => {
      if (!plot) return
      // A message may arrive between the channel set changing and the plot
      // being rebuilt; drawing mismatched series would throw inside uPlot.
      if (buffer.series.length !== plot.series.length - 1) return
      plot.setData([buffer.t, ...buffer.series] as unknown as uPlot.AlignedData)
    }),
  )

  // Follow container width; the card this sits in is responsive.
  $effect(() => {
    const h = height
    const node = container
    if (!node) return
    const observer = new ResizeObserver(() => {
      if (plot && node.clientWidth > 0) {
        plot.setSize({ width: node.clientWidth, height: h })
      }
    })
    observer.observe(node)
    return () => observer.disconnect()
  })
</script>

<div bind:this={container} class="w-full overflow-x-auto"></div>
