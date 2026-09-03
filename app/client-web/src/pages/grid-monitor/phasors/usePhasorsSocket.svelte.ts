import { useServerSocket } from '@/hooks/useServerSocket.svelte'
import { PHASORS_WS_PATH } from '@/lib/servers'
import type { Wire } from '@/api/wire'

/** One station's voltage phasor: `mag` in volts, `ang` in radians, `island` the
 *  detector's group (0 is the main system). Straight from the contract. */
export type Phasor = Wire['Phasor']

/** The snapshot the panel draws. `mag_ref` / `ang_ref` are sent rather than
 *  applied, so the dial can offer per-unit and rotating-reference views. */
export type PhasorsState = Wire['PhasorSnapshot']

export function usePhasorsSocket() {
  const sock = useServerSocket<PhasorsState>(PHASORS_WS_PATH)

  return {
    get state() {
      return sock.message
    },
    get status() {
      return sock.status
    },
    get connected() {
      return sock.connected
    },
  }
}
