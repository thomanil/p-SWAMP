import { useState } from 'react'
import { CompassIcon, WifiOffIcon } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'

import { Panel } from '../Panel'
import { ISLAND_COLORS } from '../islands'
import type { PanelVariant } from '../variant'
import { PhasorDial } from './PhasorDial'
import { usePhasorsSocket } from './usePhasorsSocket'

/**
 * Voltage phasors — the web counterpart of p-SWAMP's Qt voltage phasor plot.
 *
 * Reads the same measurement window the live measurements panel does — this
 * client's own, one per pipeline.
 * When the recorded line trip separates the northern stations, their phasors
 * drift away from the rest of the dial, coloured by the island the detector
 * assigned them.
 */
export function PhasorsPanel({
  variant = 'dashboard',
}: {
  variant?: PanelVariant
}) {
  const [equalLengths, setEqualLengths] = useState(true)
  const [rotateToMean, setRotateToMean] = useState(true)
  const { state, status, connected } = usePhasorsSocket()

  const ready = connected && state !== null
  const islandCount = state
    ? new Set(state.phasors.map((p) => p.island ?? 0)).size
    : 0

  return (
    <Panel
      title="Voltage Phasors"
      subtitle="Bus voltage phasors across the Nordic 44 grid, coloured by island"
      status={status}
      ready={ready}
      focusedClassName="w-full max-w-2xl"
      focusHref="/phasors"
      variant={variant}
      minBodyClass="min-h-[320px]"
      badge={
        connected ? (
          <Badge>
            <CompassIcon className="size-3" />
            {state?.phasors.length ?? 0}
          </Badge>
        ) : (
          <Badge variant="outline" className="text-muted-foreground">
            <WifiOffIcon className="size-3" />
            Offline
          </Badge>
        )
      }
      footer={
        state?.mag_ref
          ? `max ${(state.mag_ref / 1e3).toFixed(1)} kV` +
            (state.ang_ref !== null
              ? ` · mean angle ${((state.ang_ref * 180) / Math.PI).toFixed(1)}°`
              : '')
          : undefined
      }
    >
      <div className="space-y-3">
        <PhasorDial
          phasors={state?.phasors ?? []}
          magRef={state?.mag_ref ?? null}
          angRef={state?.ang_ref ?? null}
          equalLengths={equalLengths}
          rotateToMean={rotateToMean}
          size={variant === 'dashboard' ? 300 : 420}
        />
        <div className="flex flex-wrap items-center justify-center gap-2">
          <Button
            size="sm"
            variant={equalLengths ? 'default' : 'outline'}
            onClick={() => setEqualLengths((v) => !v)}
          >
            Equal lengths
          </Button>
          <Button
            size="sm"
            variant={rotateToMean ? 'default' : 'outline'}
            onClick={() => setRotateToMean((v) => !v)}
          >
            Rotate to mean
          </Button>
          {islandCount > 1 && (
            <span className="flex items-center gap-2 text-xs text-muted-foreground">
              {Array.from({ length: islandCount }, (_, i) => (
                <span key={i} className="flex items-center gap-1">
                  <span
                    className="size-2 rounded-full"
                    style={{ background: ISLAND_COLORS[i % ISLAND_COLORS.length] }}
                  />
                  {i === 0 ? 'main' : `island ${i}`}
                </span>
              ))}
            </span>
          )}
        </div>
      </div>
    </Panel>
  )
}
