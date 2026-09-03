import { useCallback } from 'react'

import { fireCommand, postCommand } from '@/lib/commands'
import { PMU_STREAM_API_PATH, PMU_STREAM_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'
import type { Wire } from '@/api/wire'

/** One record in the scrolling window (see `PmuRecord` in
 *  app/server-python/src/pmu_test_streamer/api.py). */
export type StreamRecord = Wire['PmuRecord']

/** The single state message the server pushes on connect and every change. */
export type PmuStreamState = Wire['PmuStreamState']

/** Which live pipe the socket retransmits from. Derived from the contract so it
 *  stays in step with the server's `Broker` literal. */
export type Broker = PmuStreamState['broker']

/**
 * The PMU streamer: state arrives on the socket, commands go up as POSTs to
 * /api/pmu-test-streamer. `selectBroker` switches which of the two live pipes
 * (Kafka or NATS) is retransmitted; `play`/`stop` pause and resume forwarding.
 */
export function usePmuStreamSocket() {
  const { message, status, connected } =
    useServerSocket<PmuStreamState>(PMU_STREAM_WS_PATH)

  const selectBroker = useCallback(
    (broker: Broker) =>
      fireCommand(
        'pmu-test-streamer',
        postCommand(`${PMU_STREAM_API_PATH}/broker/select`, { body: { broker } }),
      ),
    [],
  )

  const fire = (action: 'play' | 'stop') =>
    fireCommand(
      'pmu-test-streamer',
      postCommand(`${PMU_STREAM_API_PATH}/playback/${action}`),
    )

  const play = useCallback(() => fire('play'), [])
  const stop = useCallback(() => fire('stop'), [])

  return { state: message, status, connected, selectBroker, play, stop }
}
