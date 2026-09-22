import { useCallback, useState } from 'react'

import type { Wire } from '@/api/wire'
import { TIME_SERIES_EXPLORER_API_PATH, TIME_SERIES_EXPLORER_WS_PATH } from '@/lib/servers'
import { CommandError, postCommand } from '@/lib/commands'
import { useServerSocket } from '@/hooks/useServerSocket'

/** What the server pushes, generated from the api contract — the model declared
 *  in app/server-python/src/time_series_explorer/api.py, not a hand-copy of it.
 *  The page reads these fields as the server names them, snake_case and all. */
export type TimeSeriesExplorerState = Wire['TimeSeriesExplorerState']
export type PlayerStatus = Wire['PlayerStatus']
export type RowCountResult = Wire['RowCountResult']

/**
 * The Timeseries Db Explorer: state arrives on the socket, and three commands go
 * up as POSTs to /api/time-series-explorer, each becoming a `Command` on this
 * client's bus — two for the player (play a range, stop) and one for the
 * row-count module (count a range). The reply is only an acknowledgement; the
 * effect comes back as the next state message.
 *
 * Unlike the streamer's hook this one keeps the server's *refusal*: a range
 * outside the coverage is a 409 whose detail says which, and a form wants to
 * show that rather than log it. `refusal` is null until a command is refused
 * and cleared by the next one sent.
 */
export function useTimeSeriesExplorerSocket() {
  const { message, status, connected } =
    useServerSocket<TimeSeriesExplorerState>(TIME_SERIES_EXPLORER_WS_PATH)

  const [refusal, setRefusal] = useState<string | null>(null)
  const send = useCallback(async (promise: Promise<void>) => {
    setRefusal(null)
    try {
      await promise
    } catch (error) {
      setRefusal(error instanceof CommandError ? error.detail : String(error))
      console.error('time-series-explorer command failed', error)
    }
  }, [])

  /** Replay exactly [start, end) at real time; ends paused at `end`. */
  const playRange = useCallback(
    (start: string, end: string) =>
      send(postCommand(`${TIME_SERIES_EXPLORER_API_PATH}/playback/play-range`, { body: { start, end } })),
    [send],
  )
  const stop = useCallback(
    () => send(postCommand(`${TIME_SERIES_EXPLORER_API_PATH}/playback/stop`)),
    [send],
  )
  /** Ask the row-count module for [start, end); its result arrives as `count`. */
  const count = useCallback(
    (start: string, end: string) =>
      send(postCommand(`${TIME_SERIES_EXPLORER_API_PATH}/count`, { body: { start, end } })),
    [send],
  )
  /** Ask the provider again what it holds — the way back after it stopped answering. */
  const refresh = useCallback(
    () => send(postCommand(`${TIME_SERIES_EXPLORER_API_PATH}/refresh`)),
    [send],
  )

  return { state: message, status, connected, refusal, playRange, stop, count, refresh }
}
