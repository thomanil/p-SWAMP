import { useCallback, useMemo } from 'react'

import { postCommand } from '@/lib/commands'
import { TIMELINE_API_PATH, TIMELINE_WS_PATH } from '@/lib/servers'
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

/** Fire a command and carry on. A failure is logged and nothing else happens:
 *  the page renders only what the server pushes, so a command that did not land
 *  simply produces no change — which is what the user already sees. */
function fire(promise: Promise<void>): void {
  promise.catch((error) => console.error('timeline command failed', error))
}

/**
 * The timeline's two halves: state down the socket, commands up over REST.
 *
 * Connection handling comes from useServerSocket and this adds the mapping to
 * TimelineState; the commands are POSTs to /api/timeline. Holds no authoritative
 * state — it renders whatever window the server pushes back after a command, the
 * same way it renders the ticker's unprompted pushes.
 */
export function useTimelineSocket() {
  const { message, status, connected } = useServerSocket<TimelineMessage>(TIMELINE_WS_PATH)

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

  const play = useCallback(
    () => fire(postCommand(`${TIMELINE_API_PATH}/playback/play`)),
    [],
  )
  const stop = useCallback(
    () => fire(postCommand(`${TIMELINE_API_PATH}/playback/stop`)),
    [],
  )
  const forward = useCallback(
    () => fire(postCommand(`${TIMELINE_API_PATH}/playback/forward`)),
    [],
  )
  const back = useCallback(
    () => fire(postCommand(`${TIMELINE_API_PATH}/playback/back`)),
    [],
  )
  const setSequence = useCallback(
    (name: string) => fire(postCommand(`${TIMELINE_API_PATH}/sequence`, { name })),
    [],
  )

  return { state, status, connected, play, stop, forward, back, setSequence }
}
