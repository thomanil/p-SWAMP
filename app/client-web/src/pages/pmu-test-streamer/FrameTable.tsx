import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

import type { PmuFrame, PmuHeader } from './usePmuStreamSocket'

/** Which column of a frame holds `measurement` for `station`, or -1. The
 *  header's three label rows are the same ones p-SWAMP's Indexer queries. */
function column(header: PmuHeader, station: string, measurement: string): number {
  return header.station.findIndex(
    (s, i) => s === station && header.measurement[i] === measurement,
  )
}

function cell(frame: PmuFrame | null, index: number, digits: number): string {
  if (frame === null || index < 0) return '—'
  const value = frame.values[index]
  return value === null || value === undefined ? '—' : value.toFixed(digits)
}

/**
 * One frame of the recording as a table: a row per station, the three
 * measurements the sample carries as columns. `header` is the layout of the
 * last frame seen (every frame carries its own); with no frame at the cursor
 * -- a replay paused at its start after a stream switch -- the rows show
 * dashes, so the block never changes size.
 */
export function FrameTable({ header, frame }: { header: PmuHeader; frame: PmuFrame | null }) {
  const stations = Array.from(new Set(header.station))
  return (
    <Table className="font-mono text-xs">
      <TableHeader>
        <TableRow>
          <TableHead>Station</TableHead>
          <TableHead className="text-right">V (kV)</TableHead>
          <TableHead className="text-right">angle (deg)</TableHead>
          <TableHead className="text-right">f (Hz)</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {stations.map((station) => (
          <TableRow key={station}>
            <TableCell className="font-medium">{station}</TableCell>
            <TableCell className="text-right tabular-nums">
              {cell(frame, column(header, station, 'V_Magnitude'), 2)}
            </TableCell>
            <TableCell className="text-right tabular-nums">
              {cell(frame, column(header, station, 'V_Angle'), 2)}
            </TableCell>
            <TableCell className="text-right tabular-nums">
              {cell(frame, column(header, station, 'f'), 4)}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}
