import { useMemo } from 'react'

import { useServerSocket } from '@/hooks/useServerSocket'
import { PHASORS_WS_PATH } from '@/lib/servers'

export type Phasor = {
  station: string
  channel: string
  /** Volts. Null when the measurement is missing. */
  mag: number | null
  /** Radians. */
  ang: number | null
  /** Island index from the detector; 0 is the main system, null if unknown. */
  island: number | null
}

export type PhasorsState = {
  t: number
  phasors: Phasor[]
  /** Largest magnitude in this snapshot, for a per-unit view. */
  magRef: number | null
  /** Circular mean angle, for a rotating-reference view. */
  angRef: number | null
}

type PhasorsMessage = {
  t: number
  phasors: {
    station: string
    channel: string
    mag: number | null
    ang: number | null
    island: number | null
  }[]
  mag_ref: number | null
  ang_ref: number | null
}

export function usePhasorsSocket() {
  const { message, status, connected, send } = useServerSocket<PhasorsMessage>(PHASORS_WS_PATH)

  const state = useMemo<PhasorsState | null>(
    () =>
      message === null
        ? null
        : {
            t: message.t,
            phasors: message.phasors,
            magRef: message.mag_ref,
            angRef: message.ang_ref,
          },
    [message],
  )

  return { state, status, connected, send }
}
