import { useCallback, useState } from 'react'

import { fireCommand, postCommand } from '@/lib/commands'
import { PMU_STREAM_API_PATH, PMU_STREAM_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'
import type { Wire } from '@/api/wire'

/** The one message the server pushes on connect and on every change (see
 *  `state_message` in app/server-python/src/pmu_test_streamer/api.py). Its
 *  parts are the core's own wire models, generated into these types as they
 *  are: nothing renames a field between the Python and this page. */
export type PmuStreamState = Wire['PmuStreamState']
export type PmuHeader = Wire['PmuHeader']
export type PmuFrame = Wire['PmuFrame']
export type PlayerStatus = Wire['PlayerStatus']
export type FrameStats = Wire['FrameStats']
export type Source = PmuStreamState['source']

/**
 * The PMU test streamer: state arrives on the socket, commands go up as POSTs
 * to /api/pmu-test-streamer, where each becomes a `Command` on this client's
 * bus for the player to apply. The reply is only an acknowledgement; the effect
 * comes back as the next state message. A command the player's current mode
 * cannot apply (a seek while live) is a 409, which `fireCommand` logs; the
 * page never offers one, since it renders its controls from `player.mode`.
 */
export function usePmuStreamSocket() {
  const { message, status, connected } =
    useServerSocket<PmuStreamState>(PMU_STREAM_WS_PATH)

  // The channel layout rides inside every frame (`frame.header`). The last one
  // seen is kept here so the table keeps its layout while no frame is at the
  // cursor -- a paused replay at its start, right after a stream switch.
  // Compared by `header_id` (a content hash), so a frame carrying the same
  // layout as the last one costs no state change. Derived-state-during-render
  // is React's own pattern for "remember something from an earlier render".
  const [header, setHeader] = useState<PmuHeader | null>(null)
  const seen = message?.frame?.header
  if (seen && seen.header_id !== header?.header_id) setHeader(seen)

  const fire = (action: 'play' | 'stop' | 'forward' | 'back' | 'refresh') =>
    fireCommand(
      'pmu-test-streamer',
      postCommand(`${PMU_STREAM_API_PATH}/playback/${action}`),
    )

  const play = useCallback(() => fire('play'), [])
  const stop = useCallback(() => fire('stop'), [])
  const forward = useCallback(() => fire('forward'), [])
  const back = useCallback(() => fire('back'), [])
  /** Retry a failed provider: ask the gateway again what it holds. */
  const retry = useCallback(() => fire('refresh'), [])
  /** Watch another source: a recording (this browser's own, restarted from its
   *  beginning) or the live stream every viewer shares. */
  const chooseSource = useCallback(
    (source: Source) =>
      fireCommand(
        'pmu-test-streamer',
        postCommand(`${PMU_STREAM_API_PATH}/source`, { body: { source } }),
      ),
    [],
  )
  /** A command to the stats *module*, not the player: zero its running count. */
  const resetStats = useCallback(
    () => fireCommand('pmu-test-streamer', postCommand(`${PMU_STREAM_API_PATH}/stats/reset`)),
    [],
  )
  /** Play one chunk of the recording, then stop at its end. */
  const playRange = useCallback(
    (startS: number, endS: number) =>
      fireCommand(
        'pmu-test-streamer',
        postCommand(`${PMU_STREAM_API_PATH}/playback/range`, {
          body: { start_offset_s: startS, end_offset_s: endS },
        }),
      ),
    [],
  )
  /** A batch query to a module: average the frequencies over one chunk. */
  const averageRange = useCallback(
    (startS: number, endS: number) =>
      fireCommand(
        'pmu-test-streamer',
        postCommand(`${PMU_STREAM_API_PATH}/stats/average`, {
          body: { start_offset_s: startS, end_offset_s: endS },
        }),
      ),
    [],
  )
  const seek = useCallback(
    (offsetS: number) =>
      fireCommand(
        'pmu-test-streamer',
        postCommand(`${PMU_STREAM_API_PATH}/playback/seek`, { body: { offset_s: offsetS } }),
      ),
    [],
  )
  const setSpeed = useCallback(
    (speed: number) =>
      fireCommand(
        'pmu-test-streamer',
        postCommand(`${PMU_STREAM_API_PATH}/playback/speed`, { body: { speed } }),
      ),
    [],
  )

  return {
    state: message,
    header,
    status,
    connected,
    play,
    stop,
    forward,
    back,
    seek,
    setSpeed,
    chooseSource,
    resetStats,
    playRange,
    averageRange,
    retry,
  }
}
