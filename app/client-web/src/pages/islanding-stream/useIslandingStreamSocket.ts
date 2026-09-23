import { useCallback } from 'react'

import type { Wire } from '@/api/wire'
import { ISLANDING_STREAM_API_PATH, ISLANDING_STREAM_WS_PATH } from '@/lib/servers'
import { fireCommand, postCommand } from '@/lib/commands'
import { useServerSocket } from '@/hooks/useServerSocket'

/** What the server pushes, generated from the api contract — the model declared
 *  in app/server-python/src/islanding_stream/api.py, not a hand-copy of it. The
 *  page reads its fields as the server names them. */
export type IslandingStreamState = Wire['IslandingStreamState']

/**
 * The Islanding stream app: the islanding module's latest result and the
 * pipeline's throughput arrive on the socket (a few times a second, never the
 * frames themselves); play / stop / speed go up as POSTs to
 * /api/islanding-stream.
 */
export function useIslandingStreamSocket() {
  const { message, status, connected } =
    useServerSocket<IslandingStreamState>(ISLANDING_STREAM_WS_PATH)

  const play = useCallback(
    () =>
      fireCommand('islanding-stream', postCommand(`${ISLANDING_STREAM_API_PATH}/playback/play`)),
    [],
  )
  const stop = useCallback(
    () =>
      fireCommand('islanding-stream', postCommand(`${ISLANDING_STREAM_API_PATH}/playback/stop`)),
    [],
  )
  const setSpeed = useCallback(
    (speed: number) =>
      fireCommand(
        'islanding-stream',
        postCommand(`${ISLANDING_STREAM_API_PATH}/playback/speed`, { body: { speed } }),
      ),
    [],
  )

  return { state: message, status, connected, play, stop, setSpeed }
}
