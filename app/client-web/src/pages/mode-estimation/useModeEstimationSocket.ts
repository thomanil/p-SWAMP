import { useCallback } from 'react'

import type { Wire } from '@/api/wire'
import { MODE_ESTIMATION_API_PATH, MODE_ESTIMATION_WS_PATH } from '@/lib/servers'
import { fireCommand, postCommand } from '@/lib/commands'
import { useServerSocket } from '@/hooks/useServerSocket'

/** What the server pushes, generated from the api contract — the model declared
 *  in app/server-python/src/mode_estimation/api.py, not a hand-copy of it. The
 *  page reads its fields as the server names them. */
export type ModeEstimationState = Wire['ModeEstimationState']

/**
 * The Mode estimation app: the N4SID module's latest result and the
 * pipeline's throughput arrive on the socket (a few times a second, never the
 * frames themselves); play / stop / speed go up as POSTs to
 * /api/mode-estimation.
 */
export function useModeEstimationSocket() {
  const { message, status, connected } =
    useServerSocket<ModeEstimationState>(MODE_ESTIMATION_WS_PATH)

  const play = useCallback(
    () =>
      fireCommand('mode-estimation', postCommand(`${MODE_ESTIMATION_API_PATH}/playback/play`)),
    [],
  )
  const stop = useCallback(
    () =>
      fireCommand('mode-estimation', postCommand(`${MODE_ESTIMATION_API_PATH}/playback/stop`)),
    [],
  )
  const setSpeed = useCallback(
    (speed: number) =>
      fireCommand(
        'mode-estimation',
        postCommand(`${MODE_ESTIMATION_API_PATH}/playback/speed`, { body: { speed } }),
      ),
    [],
  )

  return { state: message, status, connected, play, stop, setSpeed }
}
