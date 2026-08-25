import { useMemo } from 'react'

import { useServerSocket } from '@/hooks/useServerSocket'
import { LINE_OUTAGE_WS_PATH } from '@/lib/servers'
import type { Wire } from '@/api/wire'

export type LineOutageEvent = {
  t: number
  kind: 'disconnect' | 'connect'
  /** One entry per channel that flipped, parallel to `measurements`. */
  stations: string[]
  measurements: string[]
  /** Branch names recovered from the channel labels — see `branchesOf`. */
  branches: string[]
}

export type LineOutageState = {
  appName: string | null
  windowLength: number | null
  /** Newest first, as the server sends them. */
  events: LineOutageEvent[]
} | null

type LineOutageMessage = Wire['LineOutageLog']

/**
 * The detector reports the *channels* that changed, e.g.
 * `I[L3244-6500]_Magnitude`. A single line trip shows up once per end, since the
 * current on that branch goes to zero as seen from both — so the raw list is
 * roughly twice as long as the number of physical branches involved, and reads
 * as noise.
 *
 * This recovers the branch name from the label and de-duplicates, which is what
 * an operator is actually looking at.
 *
 * It is the one place in this port that encodes anything about p-SWAMP's own
 * naming, so keep it *cosmetic*: the raw `stations` and `measurements` the
 * detector reported are carried through untouched and are the authoritative
 * fields. If the channel naming upstream ever changes, the label here degrades
 * to empty and nothing else breaks — no decision is made on the parsed value.
 */
function branchesOf(measurements: string[]): string[] {
  const names = measurements
    .map((m) => /\[([^\]]+)\]/.exec(m)?.[1])
    .filter((m): m is string => Boolean(m))
  return [...new Set(names)].sort()
}

export function useLineOutageSocket() {
  const { message, status, connected } =
    useServerSocket<LineOutageMessage>(LINE_OUTAGE_WS_PATH)

  const state = useMemo<LineOutageState>(
    () =>
      message === null
        ? null
        : {
            appName: message.app_name,
            windowLength: message.window_length,
            events: message.events.map((e) => ({
              t: e.t,
              kind: e.kind,
              stations: e.stations,
              measurements: e.measurements,
              branches: branchesOf(e.measurements),
            })),
          },
    [message],
  )

  return { state, status, connected }
}
