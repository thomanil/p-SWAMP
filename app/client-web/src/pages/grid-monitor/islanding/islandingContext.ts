import { createContext, use } from 'react'

import type { ConnStatus } from '@/hooks/useServerSocket'

import type { IslandingPageState } from './useIslandingSocket'

export type IslandingValue = {
  state: IslandingPageState | null
  status: ConnStatus
  connected: boolean
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
 * Split from the provider component so this module exports no component:
 * `react-refresh/only-export-components` is an error in this repo.
 */
export const IslandingContext = createContext<IslandingValue | null>(null)

export function useIslandingData(): IslandingValue {
  const value = use(IslandingContext)
  if (value === null) {
    throw new Error('useIslandingData must be used inside <IslandingData>')
  }
  return value
}
