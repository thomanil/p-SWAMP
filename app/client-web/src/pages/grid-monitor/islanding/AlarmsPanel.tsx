import { useState } from 'react'
import { BellIcon, WifiOffIcon } from 'lucide-react'

import { Badge } from '@/components/ui/badge'

import { Panel } from '../Panel'
import type { PanelVariant } from '../variant'
import { AlarmDetails } from './AlarmDetails'
import { AlarmTable } from './AlarmTable'
import { useIslandingData } from './islandingContext'

/**
 * The alarm overview — p-SWAMP's Qt alarm dock.
 *
 * Reads the same socket as the islanding map: alarms are derived from the
 * detector's status, and the server sends both together so a client cannot
 * render them inconsistently.
 */
export function AlarmsPanel({
  variant = 'dashboard',
}: {
  variant?: PanelVariant
}) {
  const { state, status, connected, send } = useIslandingData()
  const [selectedUuid, setSelectedUuid] = useState<string | null>(null)

  const alarms = state?.alarms ?? []
  // Resolved from the live list rather than held in state, so an open pane keeps
  // updating as events land on that alarm. A selection that disappears (the
  // store is bounded) simply closes the pane.
  const selected = alarms.find((a) => a.uuid === selectedUuid) ?? null
  const unseen = alarms.filter((a) => a.status === 'unseen').length
  const ready = connected && state !== null

  return (
    <Panel
      title="Alarms"
      subtitle={variant === 'focused' ? 'Raised by the monitoring applications' : undefined}
      status={status}
      ready={ready}
      focusHref={variant === 'dashboard' ? '/islanding' : undefined}
      minBodyClass="min-h-[160px]"
      contentClassName="px-0 pt-0"
      badge={
        connected ? (
          <Badge
            variant={unseen > 0 ? 'default' : 'secondary'}
            className={
              unseen > 0
                ? 'border-transparent bg-red-600/20 text-red-700 dark:text-red-400'
                : undefined
            }
          >
            <BellIcon className="size-3" />
            {unseen > 0 ? `${unseen} unseen` : `${alarms.length}`}
          </Badge>
        ) : (
          <Badge variant="outline" className="text-muted-foreground">
            <WifiOffIcon className="size-3" />
            Offline
          </Badge>
        )
      }
    >
      {/* Capped and scrolled on the dashboard: alarms accumulate over a session,
          and a growing table would push every panel below it down the page. */}
      <div
        className={
          variant === 'dashboard' ? 'max-h-[220px] overflow-y-auto' : undefined
        }
      >
        <AlarmTable
          alarms={alarms}
          selectedUuid={selectedUuid}
          onSelect={setSelectedUuid}
          onAcknowledge={(uuid) => send('acknowledge', { alarm_uuid: uuid })}
          onSilence={(uuid) => send('silence', { alarm_uuid: uuid })}
        />
      </div>

      {selected && (
        <AlarmDetails
          alarm={selected}
          onAcknowledge={(uuid) => send('acknowledge', { alarm_uuid: uuid })}
          onSilence={(uuid) => send('silence', { alarm_uuid: uuid })}
          onAnnotate={(uuid, message) =>
            send('annotate', { alarm_uuid: uuid, message })
          }
        />
      )}
    </Panel>
  )
}
