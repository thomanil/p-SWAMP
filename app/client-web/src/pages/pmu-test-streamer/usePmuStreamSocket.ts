import { useMemo } from 'react'

import { PMU_STREAM_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'

/** One record in the visible window, or null where the window runs off either end
 *  of the data file. Line numbers are 1-based, as an editor would count them. */
export type StreamRecord = { lineNumber: number; text: string }

/** The single state message the server pushes on connect and every change
 *  (see `state_message` in app/server-python/src/pmu_test_streamer/api.py). */
export type PmuStreamState = {
  window: (StreamRecord | null)[]
  index: number
  totalLines: number
  playing: boolean
}

/** The raw wire shape, before snake_case → camelCase. */
type PmuStreamMessage = {
  window: ({ line_number: number; text: string } | null)[]
  index: number
  total_lines: number
  playing: boolean
}

/**
 * The PMU test streamer's socket — the twin of useTimelineSocket: connection
 * handling comes from useServerSocket, this adds only the mapping to
 * PmuStreamState.
 */
export function usePmuStreamSocket() {
  const { message, status, connected, send } = useServerSocket<PmuStreamMessage>(PMU_STREAM_WS_PATH)

  const state = useMemo<PmuStreamState | null>(
    () =>
      message === null
        ? null
        : {
            window: message.window.map((entry) =>
              entry === null ? null : { lineNumber: entry.line_number, text: entry.text },
            ),
            index: message.index,
            totalLines: message.total_lines,
            playing: message.playing,
          },
    [message],
  )

  return { state, status, connected, send }
}
