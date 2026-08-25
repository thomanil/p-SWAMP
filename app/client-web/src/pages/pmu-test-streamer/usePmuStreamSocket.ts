import { useCallback, useMemo } from 'react'

import { postCommand } from '@/lib/commands'
import { PMU_STREAM_API_PATH, PMU_STREAM_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'
import type { Wire } from '@/api/wire'

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

type PmuStreamMessage = Wire['PmuStreamState']

/** Fire a command and carry on; a failure is logged and nothing else happens.
 *  A command that did not land produces no state change, which is what the user
 *  already sees. */
function fire(promise: Promise<void>): void {
  promise.catch((error) => console.error('pmu-test-streamer command failed', error))
}

/**
 * The PMU test streamer: state arrives on the socket, commands go up as POSTs
 * to /api/pmu-test-streamer.
 */
export function usePmuStreamSocket() {
  const { message, status, connected } = useServerSocket<PmuStreamMessage>(PMU_STREAM_WS_PATH)

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

  const play = useCallback(
    () => fire(postCommand(`${PMU_STREAM_API_PATH}/playback/play`)),
    [],
  )
  const stop = useCallback(
    () => fire(postCommand(`${PMU_STREAM_API_PATH}/playback/stop`)),
    [],
  )
  const forward = useCallback(
    () => fire(postCommand(`${PMU_STREAM_API_PATH}/playback/forward`)),
    [],
  )
  const back = useCallback(
    () => fire(postCommand(`${PMU_STREAM_API_PATH}/playback/back`)),
    [],
  )

  return { state, status, connected, play, stop, forward, back }
}
