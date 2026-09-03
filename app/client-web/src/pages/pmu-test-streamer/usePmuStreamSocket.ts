import { useCallback } from 'react'

import { fireCommand, postCommand } from '@/lib/commands'
import { PMU_STREAM_API_PATH, PMU_STREAM_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'
import type { Wire } from '@/api/wire'

/** One record in the visible window, or null where the window runs off either
 *  end of the data file. Line numbers are 1-based, as an editor would count. */
export type StreamRecord = Wire['PmuRecord']

/** The single state message the server pushes on connect and every change
 *  (see `state_message` in app/server-python/src/pmu_test_streamer/api.py). */
export type PmuStreamState = Wire['PmuStreamState']

/**
 * The PMU test streamer: state arrives on the socket, commands go up as POSTs
 * to /api/pmu-test-streamer.
 */
export function usePmuStreamSocket() {
  const { message, status, connected } =
    useServerSocket<PmuStreamState>(PMU_STREAM_WS_PATH)

  const fire = (action: 'play' | 'stop' | 'forward' | 'back') =>
    fireCommand(
      'pmu-test-streamer',
      postCommand(`${PMU_STREAM_API_PATH}/playback/${action}`),
    )

  const play = useCallback(() => fire('play'), [])
  const stop = useCallback(() => fire('stop'), [])
  const forward = useCallback(() => fire('forward'), [])
  const back = useCallback(() => fire('back'), [])

  return { state: message, status, connected, play, stop, forward, back }
}
