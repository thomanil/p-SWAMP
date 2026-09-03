import { useServerSocket } from '@/hooks/useServerSocket.svelte'
import { fireCommand, postCommand } from '@/lib/commands'
import { TIME_WINDOW_API_PATH, TIME_WINDOW_WS_PATH } from '@/lib/servers'
import type { Wire } from '@/api/wire'

export type ChannelInfo = {
  idx: number
  station: string
  channel: string
  measurement: string
  label: string
}

/**
 * The chart's data, held as plain arrays outside the reactive graph entirely.
 *
 * At 10 Hz over a 1500-sample window this is the difference between a page that
 * idles and one that doesn't. Two costs are avoided, and they are worth
 * separating:
 *
 *  - Putting the samples in reactive state would rebuild arrays of tens of
 *    thousands of numbers on every message, purely to hand them back to a canvas.
 *  - Even keeping the arrays plain, a reactive signal to *notify* the update would
 *    still run a render pass ten times a second that produces no DOM change.
 *
 * So the socket writes here directly (see `apply` below) and the chart is told to
 * redraw through a subscription. The component re-renders only when something it
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

type TimeWindowMessage = Wire['TimeWindowSlice']

function emptyBuffer(): WindowBuffer {
  return { t: [], series: [], channels: [], capacity: 0 }
}

export function useTimeWindowSocket() {
  // Held as a plain object, not reactive state: it is the chart's data path and
  // is read imperatively, never through the template. Created once per component,
  // so the reference is stable for the chart to hold.
  const buffer = emptyBuffer()
  // Redraw subscribers, kept out of the reactive graph so notifying them is not a
  // render.
  const listeners = new Set<() => void>()

  // The chart's shape — not its data. Changing the selection genuinely does need
  // a render, because the series, legend and axes have to be rebuilt.
  const shape = $state<{ channels: ChannelInfo[]; samplingRate: number | null }>({
    channels: [],
    samplingRate: null,
  })

  const apply = (message: TimeWindowMessage) => {
    if (message.mode === 'full') {
      buffer.t = message.t.map((v) => v ?? NaN)
      buffer.series = message.series.map((s) => [...s])
      buffer.channels = message.channels ?? []
      buffer.capacity = message.n_samples ?? message.t.length
      // Only on a full message, i.e. on connect and on a selection change.
      shape.channels = buffer.channels
      shape.samplingRate = message.sampling_rate
    } else {
      // An append with nothing to append to can only happen if a full message
      // and an append crossed on the wire; dropping it is harmless, since the
      // next full message re-establishes everything.
      if (buffer.capacity === 0) return
      buffer.t.push(...message.t.map((v) => v ?? NaN))
      message.series.forEach((incoming, i) => {
        buffer.series[i]?.push(...incoming)
      })
      // Trim from the front in one splice with the whole overflow, rather than
      // shifting per sample.
      const overflow = buffer.t.length - buffer.capacity
      if (overflow > 0) {
        buffer.t.splice(0, overflow)
        buffer.series.forEach((s) => s.splice(0, overflow))
      }
    }

    listeners.forEach((notify) => notify())
  }

  const sock = useServerSocket<TimeWindowMessage>(TIME_WINDOW_WS_PATH, {
    onMessage: apply,
  })

  /** Subscribe to redraws. Returns an unsubscribe function. */
  const subscribe = (notify: () => void) => {
    listeners.add(notify)
    return () => {
      listeners.delete(notify)
    }
  }

  /** Change what this view plots. The POST only records the choice server-side;
   *  the new traces arrive on the socket as the next `mode: 'full'` message, at
   *  the pusher's next tick — so nothing here waits on the response. */
  const selectChannels = (indices: number[]) => {
    fireCommand(
      'time-window',
      postCommand(`${TIME_WINDOW_API_PATH}/selection`, { body: { channels: indices } }),
    )
  }

  return {
    buffer,
    subscribe,
    get channels() {
      return shape.channels
    },
    get samplingRate() {
      return shape.samplingRate
    },
    get status() {
      return sock.status
    },
    get connected() {
      return sock.connected
    },
    selectChannels,
  }
}
