import { getContext, setContext } from 'svelte'

import type { ConnStatus } from '@/hooks/useServerSocket.svelte'

import type { IslandingPageState } from './useIslandingSocket.svelte'

export type IslandingValue = {
  readonly state: IslandingPageState | null
  readonly status: ConnStatus
  readonly connected: boolean
  /** Operator actions. POSTs to /api/islanding — the resulting alarm list comes
   *  back down the shared socket, not from the call. */
  acknowledge: (uuid: string) => void
  silence: (uuid: string) => void
  annotate: (uuid: string, message: string) => void
}

/**
 * The islanding socket, shared by the two panels that read it.
 *
 * One connection carries both the detection result and the alarm list — they
 * change together and are read together, so the server sends them together. The
 * map and the alarm table are separate cards in the grid, though, so without
 * this they would open two sockets to the same endpoint and appear to the server
 * as two unrelated clients.
 *
 * A Svelte context, not a prop drilled down: <IslandingData> opens the socket
 * once and `setIslandingData`s the reactive value; the panels beneath read it
 * with `useIslandingData`. The value's getters read the socket's reactive state,
 * so a panel that reads them updates on its own when a message lands.
 */
const ISLANDING_KEY = Symbol('islanding')

export function setIslandingData(value: IslandingValue): void {
  setContext(ISLANDING_KEY, value)
}

export function useIslandingData(): IslandingValue {
  const value = getContext<IslandingValue | undefined>(ISLANDING_KEY)
  if (!value) {
    throw new Error('useIslandingData must be used inside <IslandingData>')
  }
  return value
}
