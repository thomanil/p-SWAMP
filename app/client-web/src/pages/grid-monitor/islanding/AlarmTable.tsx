import { ChevronDownIcon, ChevronUpIcon } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { cn } from '@/lib/utils'

import type { Alarm, AlarmStatus } from './useIslandingSocket'

/**
 * Alarm row colours, following p-SWAMP's Qt alarm overview: an unseen alarm is
 * loud, one that has been handled or has cleared is quiet, a silenced one is
 * greyed out.
 */
const STATUS_STYLES: Record<AlarmStatus, string> = {
  unseen: 'border-transparent bg-red-600/20 text-red-700 dark:text-red-400',
  acknowledged:
    'border-transparent bg-amber-500/20 text-amber-700 dark:text-amber-400',
  not_critical:
    'border-transparent bg-emerald-600/15 text-emerald-700 dark:text-emerald-400',
  silenced: 'border-transparent bg-muted text-muted-foreground',
}

const STATUS_LABELS: Record<AlarmStatus, string> = {
  unseen: 'Unseen',
  acknowledged: 'Acknowledged',
  not_critical: 'Cleared',
  silenced: 'Silenced',
}

function clockTime(epochSeconds: number): string {
  return new Date(epochSeconds * 1000).toLocaleTimeString(undefined, {
    hour12: false,
  })
}

export function AlarmTable({
  alarms,
  selectedUuid,
  onSelect,
  onAcknowledge,
  onSilence,
}: {
  alarms: Alarm[]
  /** The alarm whose details are open, if any. */
  selectedUuid?: string | null
  /** Toggles the detail pane. Passing null closes it. */
  onSelect?: (uuid: string | null) => void
  onAcknowledge: (uuid: string) => void
  onSilence: (uuid: string) => void
}) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          {onSelect && <TableHead className="w-8" />}
          <TableHead>Raised</TableHead>
          <TableHead>Application</TableHead>
          <TableHead>Status</TableHead>
          <TableHead className="text-right">Actions</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {alarms.map((alarm) => (
          <TableRow
            key={alarm.uuid}
            className={cn(
              onSelect && 'cursor-pointer',
              selectedUuid === alarm.uuid && 'bg-muted/60',
            )}
            onClick={
              onSelect
                ? () =>
                    onSelect(selectedUuid === alarm.uuid ? null : alarm.uuid)
                : undefined
            }
          >
            {onSelect && (
              <TableCell className="text-muted-foreground">
                {selectedUuid === alarm.uuid ? (
                  <ChevronUpIcon className="size-4" />
                ) : (
                  <ChevronDownIcon className="size-4" />
                )}
              </TableCell>
            )}
            <TableCell className="tabular-nums">
              {clockTime(alarm.t_start)}
              {alarm.t_end !== null && (
                <span className="text-muted-foreground">
                  {' '}
                  – {clockTime(alarm.t_end)}
                </span>
              )}
            </TableCell>
            <TableCell className="font-medium">{alarm.app_name}</TableCell>
            <TableCell>
              <Badge className={STATUS_STYLES[alarm.status]}>
                {STATUS_LABELS[alarm.status]}
              </Badge>
            </TableCell>
            <TableCell
              className="space-x-2 text-right"
              onClick={(e) => e.stopPropagation()}
            >
              <Button
                size="sm"
                variant="outline"
                disabled={alarm.status !== 'unseen'}
                onClick={() => onAcknowledge(alarm.uuid)}
              >
                Acknowledge
              </Button>
              <Button
                size="sm"
                variant="ghost"
                disabled={alarm.status === 'silenced'}
                onClick={() => onSilence(alarm.uuid)}
              >
                Silence
              </Button>
            </TableCell>
          </TableRow>
        ))}
        {alarms.length === 0 && (
          <TableRow>
            <TableCell
              colSpan={onSelect ? 5 : 4}
              className="h-20 text-center text-muted-foreground"
            >
              No alarms raised.
            </TableCell>
          </TableRow>
        )}
      </TableBody>
    </Table>
  )
}
