import { useMemo } from 'react'

import { useServerSocket } from '@/hooks/useServerSocket'
import { LINE_OUTAGE_WS_PATH } from '@/lib/servers'
import type { Wire } from '@/api/wire'

/** One event as the detector reported it, plus the branch names recovered from
 *  its channel labels — see `branchesOf`. The wire fields are spread in rather
 *  than copied out one by one, so a field added server-side arrives here without
 *  an edit. */
export type LineOutageEvent = Wire['LineOutageEvent'] & { branches: string[] }

export type LineOutageState =
  | (Omit<Wire['LineOutageLog'], 'events'> & { events: LineOutageEvent[] })
  | null

/**
 * The detector reports the *channels* that changed, e.g.
 * `I[L3244-6500]_Magnitude`. A single line trip shows up once per end, since the
 * current on that branch goes to zero as seen from both — so the raw list is
 * roughly twice as long as the number of physical branches involved, and reads
 * as noise.
 *
 * This recovers the branch name from the label and de-duplicates, which is what
 * an operator is actually looking at. It is also the reason this hook still has
 * a `useMemo` where the other panels' hooks pass the message straight through:
 * it *derives* something, rather than renaming what the server already sent.
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
    useServerSocket<Wire['LineOutageLog']>(LINE_OUTAGE_WS_PATH)

  const state = useMemo<LineOutageState>(
    () =>
      message === null
        ? null
        : {
            ...message,
            events: message.events.map((event) => ({
              ...event,
              branches: branchesOf(event.measurements),
            })),
          },
    [message],
  )

  return { state, status, connected }
}
