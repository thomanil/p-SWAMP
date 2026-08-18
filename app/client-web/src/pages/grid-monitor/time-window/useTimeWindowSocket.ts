import { useCallback, useRef, useState } from 'react'

import { useServerSocket } from '@/hooks/useServerSocket'
import { TIME_WINDOW_WS_PATH } from '@/lib/servers'

export type ChannelInfo = {
  idx: number
  station: string
  channel: string
  measurement: string
  label: string
}

/**
 * The chart's data, held as plain arrays outside React entirely.
 *
 * At 10 Hz over a 1500-sample window this is the difference between a page that
 * idles and one that doesn't. Two costs are avoided, and they are worth
 * separating:
 *
 *  - Putting the samples in state would rebuild arrays of tens of thousands of
 *    numbers on every message, purely to hand them back to a canvas.
 *  - Even keeping the arrays in a ref, a `setState` to *signal* the update would
 *    still run a render pass ten times a second that produces no DOM change.
 *
 * So the socket writes here directly (see `onMessage` below) and the chart is
 * told to redraw through a subscription. React renders only when something it
 * actually owns changes: the channel list, or the connection status.
 */
export type WindowBuffer = {
  /** Time stamps, epoch seconds. */
  t: number[]
  /** One array per channel, parallel to `channels`. */
  series: (number | null)[][]
  channels: ChannelInfo[]
  /** Capacity, so appends know when to start discarding from the front. */
  capacity: number
}

type TimeWindowMessage = {
  mode: 'full' | 'append'
  seq: number
  t: (number | null)[]
  series: (number | null)[][]
  channels: ChannelInfo[] | null
  n_samples: number | null
  sampling_rate: number | null
}

function emptyBuffer(): WindowBuffer {
  return { t: [], series: [], channels: [], capacity: 0 }
}

export function useTimeWindowSocket() {
  const buffer = useRef<WindowBuffer>(emptyBuffer())
  // Redraw subscribers, kept outside React so notifying them is not a render.
  const listeners = useRef(new Set<() => void>())

  // The chart's shape — not its data. Changing the selection genuinely does need
  // a render, because the series, legend and axes have to be rebuilt.
  const [shape, setShape] = useState<{
    channels: ChannelInfo[]
    samplingRate: number | null
  }>({ channels: [], samplingRate: null })

  const apply = useCallback((message: TimeWindowMessage) => {
    const buf = buffer.current

    if (message.mode === 'full') {
      buf.t = message.t.map((v) => v ?? NaN)
      buf.series = message.series.map((s) => [...s])
      buf.channels = message.channels ?? []
      buf.capacity = message.n_samples ?? message.t.length
      // Only on a full message, i.e. on connect and on a selection change.
      setShape({
        channels: buf.channels,
        samplingRate: message.sampling_rate,
      })
    } else {
      // An append with nothing to append to can only happen if a full message
      // and an append crossed on the wire; dropping it is harmless, since the
      // next full message re-establishes everything.
      if (buf.capacity === 0) return
      buf.t.push(...message.t.map((v) => v ?? NaN))
      message.series.forEach((incoming, i) => {
        buf.series[i]?.push(...incoming)
      })
      // Trim from the front in one splice with the whole overflow, rather than
      // shifting per sample.
      const overflow = buf.t.length - buf.capacity
      if (overflow > 0) {
        buf.t.splice(0, overflow)
        buf.series.forEach((s) => s.splice(0, overflow))
      }
    }

    listeners.current.forEach((notify) => notify())
  }, [])

  const { status, connected, send } = useServerSocket<TimeWindowMessage>(TIME_WINDOW_WS_PATH, {
    onMessage: apply,
  })

  /** Subscribe to redraws. Returns an unsubscribe function. */
  const subscribe = useCallback((notify: () => void) => {
    listeners.current.add(notify)
    return () => {
      listeners.current.delete(notify)
    }
  }, [])

  const selectChannels = useCallback(
    (indices: number[]) => send('select_channels', { channels: indices }),
    [send],
  )

  return {
    buffer,
    subscribe,
    channels: shape.channels,
    samplingRate: shape.samplingRate,
    status,
    connected,
    selectChannels,
  }
}
