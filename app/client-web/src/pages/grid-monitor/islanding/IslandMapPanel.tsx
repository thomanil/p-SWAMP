import { AlertTriangleIcon, CheckCircle2Icon, WifiOffIcon } from 'lucide-react'

import { Badge } from '@/components/ui/badge'

import { Panel } from '../Panel'
import { islandColor } from '../islands'
import type { PanelVariant } from '../variant'
import { IslandMap } from './IslandMap'
import { useIslandingData } from './islandingContext'

/**
 * Islanding detection over the grid map — the web counterpart of p-SWAMP's Qt
 * islanding alarm view.
 *
 * About twenty seconds into each pass of the recording four lines trip, the
 * northern stations separate onto their own frequency, and the map recolours.
 * Twenty seconds later they reconnect.
 *
 * Shares its socket with the alarms panel, so the two go offline together — they
 * are one connection, and pretending otherwise would be a lie about the state of
 * the system.
 */
export function IslandMapPanel({
  variant = 'dashboard',
}: {
  variant?: PanelVariant
}) {
  const { state, status, connected } = useIslandingData()

  const islands = state?.islanding?.islands ?? []
  // Index 0 is the main system, so anything beyond it is a genuine split.
  const separated = islands.filter((i) => i.index > 0)
  const ready = connected && state !== null

  return (
    <Panel
      title="Islanding Detection"
      subtitle="Frequency-based island detection across the Nordic 44 grid"
      status={status}
      ready={ready}
      // The topology is static and fetched over HTTP, independently of the
      // detector's socket — so the grid is drawn as soon as it loads, and the
      // socket only supplies the colouring.
      drawsWithoutData
      focusHref="/islanding"
      variant={variant}
      minBodyClass="min-h-[300px]"
      badge={
        connected ? (
          separated.length > 0 ? (
            <Badge className="border-transparent bg-red-600/20 text-red-700 dark:text-red-400">
              <AlertTriangleIcon className="size-3" />
              {separated.length} island{separated.length > 1 ? 's' : ''}
            </Badge>
          ) : (
            <Badge className="border-transparent bg-emerald-600/15 text-emerald-700 dark:text-emerald-400">
              <CheckCircle2Icon className="size-3" />
              Intact
            </Badge>
          )
        ) : (
          <Badge variant="outline" className="text-muted-foreground">
            <WifiOffIcon className="size-3" />
            Offline
          </Badge>
        )
      }
      footer={
        state?.islanding
          ? `${state.islanding.parameters.window_length.toFixed(0)}s window · threshold ${state.islanding.parameters.mean_threshold}`
          : undefined
      }
    >
      <div className="space-y-3">
        <IslandMap
          islands={islands}
          height={variant === 'dashboard' ? 300 : 420}
        />
        <div className="space-y-1">
          {islands.map((island) => (
            <div key={island.index} className="flex items-start gap-2 text-sm">
              <span
                className="mt-1 size-3 shrink-0 rounded-full"
                style={{ background: islandColor(island.index) }}
              />
              <div className="min-w-0">
                <span className="font-medium">
                  {island.index === 0 ? 'Main system' : `Island ${island.index}`}
                </span>
                <span className="ml-2 text-muted-foreground tabular-nums">
                  {island.stations.length} stations
                  {island.mean_freq !== null &&
                    ` · ${island.mean_freq.toFixed(3)} Hz`}
                </span>
                {island.index > 0 && (
                  <div className="truncate font-mono text-xs text-muted-foreground">
                    {island.stations.join(', ')}
                  </div>
                )}
              </div>
            </div>
          ))}
          {islands.length === 0 && (
            <p className="text-sm text-muted-foreground">
              {ready
                ? 'Waiting for the first assessment…'
                : 'Grid topology shown; stations are uncoloured until the detector reports.'}
            </p>
          )}
        </div>
      </div>
    </Panel>
  )
}
