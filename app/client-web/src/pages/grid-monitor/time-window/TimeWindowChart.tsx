import { useEffect, useRef } from 'react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'

import type { WindowBuffer } from './useTimeWindowSocket'

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
 * SVG or a React charting library will do comfortably. Redraws come through the
 * hook's `subscribe`, not through props, so **no React render happens on the
 * data path at all** — this component re-renders only when the set of channels
 * changes and the plot has to be rebuilt.
 */
export function TimeWindowChart({
  buffer,
  subscribe,
  channels,
  height = 320,
}: {
  buffer: React.RefObject<WindowBuffer>
  subscribe: (notify: () => void) => () => void
  /** Only used to detect a shape change; the values are read from `buffer`. */
  channels: { idx: number; label: string }[]
  height?: number
}) {
  const container = useRef<HTMLDivElement | null>(null)
  const plot = useRef<uPlot | null>(null)

  // Build (or rebuild) the plot when the channel set changes.
  useEffect(() => {
    const node = container.current
    const buf = buffer.current
    if (!node || !buf || channels.length === 0) return

    plot.current?.destroy()
    plot.current = new uPlot(
      {
        width: node.clientWidth || 640,
        height,
        // Times are epoch seconds, uPlot's native x scale, so the axis formats
        // as wall-clock time with no extra configuration.
        series: [
          {},
          ...channels.map((channel, i) => ({
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
      [buf.t, ...buf.series] as unknown as uPlot.AlignedData,
      node,
    )

    return () => {
      plot.current?.destroy()
      plot.current = null
    }
  }, [buffer, channels, height])

  // Redraw on each new batch of samples. Outside React's render cycle entirely.
  useEffect(
    () =>
      subscribe(() => {
        const buf = buffer.current
        if (!plot.current || !buf) return
        // A message may arrive between the channel set changing and the plot
        // being rebuilt; drawing mismatched series would throw inside uPlot.
        if (buf.series.length !== plot.current.series.length - 1) return
        plot.current.setData(
          [buf.t, ...buf.series] as unknown as uPlot.AlignedData,
        )
      }),
    [subscribe, buffer],
  )

  // Follow container width; the card this sits in is responsive.
  useEffect(() => {
    const node = container.current
    if (!node) return
    const observer = new ResizeObserver(() => {
      if (plot.current && node.clientWidth > 0) {
        plot.current.setSize({ width: node.clientWidth, height })
      }
    })
    observer.observe(node)
    return () => observer.disconnect()
  }, [height])

  return <div ref={container} className="w-full overflow-x-auto" />
}
