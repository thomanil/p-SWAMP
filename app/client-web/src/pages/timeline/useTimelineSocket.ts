import { useMemo } from 'react'

import { TIMELINE_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'

/** The single state message the server pushes on connect and every change
 *  (see `state_message` in app/server-python/src/timeline/api.py). `window`
 *  entries are ints, or null for positions before the start of the sequence. */
export type TimelineState = {
  window: (number | null)[]
  sequenceName: string
  sequences: string[]
  playing: boolean
}

/** The raw wire shape, before snake_case → camelCase. */
type TimelineMessage = {
  window: (number | null)[]
  sequence_name: string
  sequences: string[]
  playing: boolean
}

export type { ConnStatus } from '@/hooks/useServerSocket'

/**
 * The timeline's socket: connection handling comes from useServerSocket, this
 * adds only the mapping to TimelineState. Holds no authoritative state — it
 * renders whatever window the server pushes and sends commands on user actions.
 */
export function useTimelineSocket() {
  const { message, status, connected, send } = useServerSocket<TimelineMessage>(TIMELINE_WS_PATH)

  const state = useMemo<TimelineState | null>(
    () =>
      message === null
        ? null
        : {
            window: message.window,
            sequenceName: message.sequence_name,
            sequences: message.sequences,
            playing: message.playing,
          },
    [message],
  )

  return { state, status, connected, send }
}
