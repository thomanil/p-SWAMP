import { useState } from 'react'

import type { Wire } from '@/api/wire'
import { FREQUENCY_PEEK_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'

/** What the server pushes, generated from the api contract — the model declared
 *  in app/server-python/src/frequency_peek/api.py, not a hand-copy of it. The
 *  page reads its fields as the server names them. */
export type FrequencyPeekState = Wire['FrequencyPeekState']
export type FrequencyResult = Wire['FrequencyResult']

/** One result, as the page keeps it: the instant and the per-station values. */
export type FrequencySample = {
  timestamp: string
  frequency_hz: Record<string, number | null>
}

/** How many results to keep: 10 s at the live feed's 20 Hz. */
export const HISTORY_LENGTH = 200

/**
 * The Frequency peek app: state arrives on the socket, and nothing goes up —
 * the page is live only and has no commands. The hook *derives* one thing, a
 * ring buffer of the last results, so the page can draw a stream rather than
 * a single value; it renames nothing.
 */
export function useFrequencyPeekSocket() {
  const { message, status, connected } =
    useServerSocket<FrequencyPeekState>(FREQUENCY_PEEK_WS_PATH)

  // Derived-state-during-render: append the newest result when it is a new
  // one, React's own pattern for remembering something across renders without
  // an effect (and without a second render pass per message).
  const [history, setHistory] = useState<FrequencySample[]>([])
  const result = message?.frequency
  const last = history[history.length - 1]
  if (result && result.timestamp !== last?.timestamp) {
    setHistory((prev) =>
      [...prev, { timestamp: result.timestamp, frequency_hz: result.result.frequency_hz }].slice(
        -HISTORY_LENGTH,
      ),
    )
  }

  return { state: message, history, status, connected }
}
