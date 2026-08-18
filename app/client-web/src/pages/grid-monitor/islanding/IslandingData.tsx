import { useMemo, type ReactNode } from 'react'


import { IslandingContext } from './islandingContext'
import { useIslandingSocket } from './useIslandingSocket'

/**
 * Opens the islanding socket once and shares it with the panels beneath.
 *
 * Scoping matters here. When a message arrives this component re-renders, but
 * `children` is the same element object its parent created — and the parent did
 * not re-render — so React bails out of the whole subtree. Only the components
 * that actually call `useIslandingData()` update. Hoisting the hook up into the
 * dashboard instead would re-render every panel at the sum of all four sockets'
 * rates.
 */
export function IslandingData({ children }: { children: ReactNode }) {
  const { state, status, connected, send } = useIslandingSocket()
  const value = useMemo(
    () => ({ state, status, connected, send }),
    [state, status, connected, send],
  )

  return <IslandingContext value={value}>{children}</IslandingContext>
}
