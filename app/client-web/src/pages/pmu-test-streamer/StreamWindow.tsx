import { cn } from '@/lib/utils'

import type { PlayedSample, StreamHeader } from './usePmuStreamSocket'

/** Column indices of one station's three channels, or -1 where absent. */
type StationColumns = { station: string; v: number; ang: number; f: number }

/** Group the header's flat channel table by station, in header order. The
 *  selection is by the `measurement` key, the same way a server-side module
 *  picks its channels. */
function stationColumns(header: StreamHeader): StationColumns[] {
  const byStation = new Map<string, StationColumns>()
  header.channels.forEach((channel, index) => {
    let entry = byStation.get(channel.station)
    if (!entry) {
      entry = { station: channel.station, v: -1, ang: -1, f: -1 }
      byStation.set(channel.station, entry)
    }
    if (channel.measurement === 'v_Magnitude') entry.v = index
    else if (channel.measurement === 'v_Angle') entry.ang = index
    else if (channel.measurement === 'f') entry.f = index
  })
  return [...byStation.values()]
}

const at = (values: (number | null)[], index: number): number | null =>
  index < 0 ? null : (values[index] ?? null)

const kV = (v: number | null) => (v === null ? '—' : (v / 1000).toFixed(2))
const deg = (rad: number | null) => (rad === null ? '—' : ((rad * 180) / Math.PI).toFixed(2))
const hz = (f: number | null) => (f === null ? '—' : f.toFixed(4))

/**
 * The window of recently played samples, then the current one in detail.
 *
 * The window is one row per sample — its position in the source and the
 * voltage magnitude at every station — newest at the bottom and highlighted,
 * older rows fading with age. It is padded to a fixed height so the block does
 * not grow while the first rows arrive. The wire carries volts and radians;
 * this is the one place they become kV and degrees.
 */
export function StreamWindow({
  header,
  recent,
  rows,
}: {
  header: StreamHeader
  recent: PlayedSample[]
  rows: number
}) {
  const stations = stationColumns(header)
  const padded: (PlayedSample | null)[] = [
    ...Array<null>(Math.max(0, rows - recent.length)).fill(null),
    ...recent.slice(-rows),
  ]
  const current = recent[recent.length - 1] ?? null

  return (
    <div className="py-4 font-mono text-xs select-none">
      <table className="w-full tabular-nums">
        <thead className="text-muted-foreground">
          <tr>
            <th className="w-16 text-left font-normal">t (s)</th>
            {stations.map(({ station }) => (
              <th key={station} className="text-right font-normal">
                {station} kV
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {padded.map((played, i) => {
            const isCurrent = i === padded.length - 1 && played !== null
            const age = (padded.length - 1 - i) / Math.max(1, padded.length - 1)
            return (
              <tr
                key={i}
                className={cn(
                  'h-6',
                  isCurrent ? 'font-bold text-blue-600 dark:text-blue-400' : 'text-muted-foreground',
                )}
                style={isCurrent ? undefined : { opacity: 1 - 0.75 * age }}
              >
                <td className="text-left">
                  {isCurrent ? '▸ ' : ''}
                  {played === null ? '' : played.position_s.toFixed(2)}
                </td>
                {stations.map(({ station, v }) => (
                  <td key={station} className="text-right">
                    {played === null ? '' : kV(at(played.sample.values, v))}
                  </td>
                ))}
              </tr>
            )
          })}
        </tbody>
      </table>

      {/* The current sample, every channel. */}
      <table className="mt-4 w-full border-t pt-2 tabular-nums">
        <thead className="text-muted-foreground">
          <tr>
            <th className="pt-2 text-left font-normal">station</th>
            <th className="pt-2 text-right font-normal">V (kV)</th>
            <th className="pt-2 text-right font-normal">angle (°)</th>
            <th className="pt-2 text-right font-normal">f (Hz)</th>
          </tr>
        </thead>
        <tbody>
          {stations.map(({ station, v, ang, f }) => (
            <tr key={station} className="h-5">
              <td>{station}</td>
              <td className="text-right">{current ? kV(at(current.sample.values, v)) : '—'}</td>
              <td className="text-right">{current ? deg(at(current.sample.values, ang)) : '—'}</td>
              <td className="text-right">{current ? hz(at(current.sample.values, f)) : '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
