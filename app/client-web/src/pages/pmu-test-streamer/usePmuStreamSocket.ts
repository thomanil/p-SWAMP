import { useCallback, useState } from 'react'

import { fireCommand, postCommand } from '@/lib/commands'
import { PMU_STREAM_API_PATH, PMU_STREAM_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'
import type { Wire } from '@/api/wire'

/** The single state message the server pushes: a control update, or one
 *  played sample (see `state_message` in app/server-python/src/pmu_test_streamer/api.py). */
export type PmuStreamState = Wire['PmuStreamState']
/** The stream's channel table — arrives on the opening message only. */
export type StreamHeader = Wire['StreamHeader']
/** One instant across every channel, in header order. Volts, radians, hertz. */
export type Sample = Wire['Sample']

/** A sample as it played, with where in the source it sits. */
export type PlayedSample = { position_s: number; sample: Sample }

/** How many played samples the page keeps for its window. */
const RECENT = 8

type StreamView = {
  header: StreamHeader | null
  recent: PlayedSample[]
  state: PmuStreamState | null
}

const EMPTY: StreamView = { header: null, recent: [], state: null }

/**
 * The PMU test streamer: samples arrive on the socket, commands go up as POSTs
 * to /api/pmu-test-streamer.
 *
 * Two things this hook *derives* rather than reads (the message is the
 * contract; this is the page's own vocabulary): the header is kept from the
 * opening message, since it does not ride on every sample; and the window of
 * recently played samples is accumulated here — the server sends each sample
 * once, not a window, which is the "deltas, not windows" rule from the monitor.
 */
export function usePmuStreamSocket() {
  const [view, setView] = useState<StreamView>(EMPTY)

  const { status, connected } = useServerSocket<PmuStreamState>(PMU_STREAM_WS_PATH, {
    onMessage: (msg) =>
      setView((prev) => ({
        header: msg.header ?? prev.header,
        recent:
          msg.sample && msg.position_s !== null
            ? [...prev.recent.slice(-(RECENT - 1)), { position_s: msg.position_s, sample: msg.sample }]
            : prev.recent,
        state: msg,
      })),
  })

  const fire = (label: string, promise: Promise<void>) => fireCommand(`pmu-test-streamer ${label}`, promise)

  const play = useCallback(() => fire('play', postCommand(`${PMU_STREAM_API_PATH}/playback/play`)), [])
  const stop = useCallback(() => fire('stop', postCommand(`${PMU_STREAM_API_PATH}/playback/stop`)), [])
  const forward = useCallback(
    () => fire('forward', postCommand(`${PMU_STREAM_API_PATH}/playback/forward`)),
    [],
  )
  const back = useCallback(() => fire('back', postCommand(`${PMU_STREAM_API_PATH}/playback/back`)), [])
  const setSpeed = useCallback(
    (speed: number) =>
      fire('speed', postCommand(`${PMU_STREAM_API_PATH}/playback/speed`, { body: { speed } })),
    [],
  )
  const seek = useCallback(
    (position_s: number) =>
      fire('seek', postCommand(`${PMU_STREAM_API_PATH}/playback/seek`, { body: { position_s } })),
    [],
  )

  return { ...view, status, connected, play, stop, forward, back, setSpeed, seek }
}
