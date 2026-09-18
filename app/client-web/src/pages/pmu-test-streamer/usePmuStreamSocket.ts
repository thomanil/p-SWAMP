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

  // The channel layout arrives on the first message of a connection only (it
  // never changes for a recording, and it is ~1 KB nobody wants at 20 Hz), so
  // it is kept here. Derived-state-during-render is React's own pattern for
  // "remember something from an earlier render" without an effect.
  const [header, setHeader] = useState<PmuHeader | null>(null)
  if (message?.header && message.header !== header) setHeader(message.header)

  const fire = (action: 'play' | 'stop' | 'forward' | 'back' | 'live' | 'replay') =>
    fireCommand(
      'pmu-test-streamer',
      postCommand(`${PMU_STREAM_API_PATH}/playback/${action}`),
    )

  const play = useCallback(() => fire('play'), [])
  const stop = useCallback(() => fire('stop'), [])
  const forward = useCallback(() => fire('forward'), [])
  const back = useCallback(() => fire('back'), [])
  /** Switch to the live feed: frames from now, no transport controls. */
  const goLive = useCallback(() => fire('live'), [])
  /** Switch back to the recording, paused at its start. */
  const replay = useCallback(() => fire('replay'), [])
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
    goLive,
    replay,
  }
}
