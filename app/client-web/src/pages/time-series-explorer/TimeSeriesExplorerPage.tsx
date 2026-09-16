import { useState } from 'react'
import { PlayIcon, RefreshCwIcon, SigmaIcon, SquareIcon, WifiOffIcon } from 'lucide-react'

// This page's own pieces, imported relatively so the folder stays self-contained.
import { useTimeSeriesExplorerSocket } from './useTimeSeriesExplorerSocket'
import type { PlayerStatus } from './useTimeSeriesExplorerSocket'
import { Alert, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardAction,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'

/** Seconds between two instants the server sent as ISO strings. */
function secondsBetween(from: string | null | undefined, to: string | null | undefined): number | null {
  if (!from || !to) return null
  return (new Date(to).getTime() - new Date(from).getTime()) / 1000
}

/** The instant `offsetS` seconds after `start`, as the ISO string a command takes. */
function instantAt(start: string, offsetS: number): string {
  return new Date(new Date(start).getTime() + offsetS * 1000).toISOString()
}

/** An instant as `HH:MM:SS.mmm`, the coverage start being the day's context. */
function clock(iso: string | null | undefined): string {
  return iso ? new Date(iso).toISOString().slice(11, 23) : '—'
}

function statusLine(player: PlayerStatus | undefined): string {
  if (!player) return '—'
  if (player.error) return `stopped: ${player.error}`
  if (player.ended) return player.range_end ? 'ended at the end of the range' : 'ended'
  return player.paused ? 'paused' : `playing ×${player.speed}`
}

/**
 * The Timeseries Db Explorer page (route `/time-series-explorer`): one range,
 * two ways to ask the provider for it. "Play range" streams it through the
 * player at real time and stops at the end; "Count rows" has the row-count
 * module pull it unpaced and answer with one number. Both are POSTs; both
 * results arrive on the socket. The range is entered as seconds from the
 * start of the provider's coverage, which the page shows.
 */
export function TimeSeriesExplorerPage() {
  const { state, header, status, connected, refusal, playRange, stop, count, refresh } =
    useTimeSeriesExplorerSocket()

  const [fromS, setFromS] = useState(0)
  const [toS, setToS] = useState(3)

  const player = state?.player
  const coverageStart = player?.coverage_start ?? null
  const duration = secondsBetween(player?.coverage_start, player?.coverage_end)
  const hasCoverage = coverageStart !== null && duration !== null
  const rangeOk = hasCoverage && toS > fromS && fromS >= 0 && toS <= duration
  const canSend = connected && rangeOk
  const playing = player !== undefined && !player.paused
  const cursorS = secondsBetween(coverageStart, player?.cursor)
  const rangeEndS = secondsBetween(coverageStart, player?.range_end)
  const frame = state?.frame ?? null
  const result = state?.count?.result ?? null

  // The frequency column of each station, for a one-line summary of the frame
  // at the cursor; the header says which columns those are.
  const frequencies =
    header && frame
      ? header.measurement
          .map((m, i) => (m === 'f' ? `${header.station[i]} ${frame.values[i]?.toFixed(3) ?? '—'} Hz` : null))
          .filter((s): s is string => s !== null)
      : []

  const send = (action: (start: string, end: string) => void) => {
    if (coverageStart === null) return
    action(instantAt(coverageStart, fromS), instantAt(coverageStart, toS))
  }

  return (
    <Card className="w-full max-w-xl gap-0">
      <CardHeader className="border-b">
        <CardTitle className="text-lg">Timeseries Db Explorer</CardTitle>
        <span className="text-gray-500">
          One range, two questions to the provider: stream it through the player, or have a
          module count it. Same client, same gateway, two commands.
        </span>
        <CardAction className="self-center">
          {connected ? (
            <Badge variant={playing ? 'default' : 'secondary'}>
              {player?.ended ? 'Ended' : playing ? 'Playing' : 'Paused'}
            </Badge>
          ) : (
            <Badge variant="outline" className="text-muted-foreground">
              <WifiOffIcon className="size-3" />
              Offline
            </Badge>
          )}
        </CardAction>
      </CardHeader>

      <CardContent className="px-6 py-4">
        {connected && state ? (
          <div className="grid grid-cols-[auto_1fr] items-baseline gap-x-3 gap-y-1 text-sm">
            <span className="text-right text-muted-foreground">Coverage</span>
            <span className="tabular-nums">
              {hasCoverage
                ? `${clock(coverageStart)} → ${clock(player?.coverage_end)} · ${duration.toFixed(2)} s`
                : 'none reported by the provider'}
            </span>
            <span className="text-right text-muted-foreground">Player</span>
            <span className="tabular-nums">
              {statusLine(player)}
              {cursorS !== null ? ` · cursor t = ${cursorS.toFixed(2)} s` : ''}
              {rangeEndS !== null ? ` · until t = ${rangeEndS.toFixed(2)} s` : ''}
            </span>
            <span className="text-right text-muted-foreground">Frame</span>
            <span className="tabular-nums">
              {frame ? `${clock(frame.timestamp)} · ${frequencies.join(' · ') || `${frame.values.length} values`}` : '—'}
            </span>
            <span className="text-right text-muted-foreground">Count</span>
            <span className="tabular-nums">
              {result
                ? `${result.count} rows in [${clock(result.start)}, ${clock(result.end)}) · ${result.elapsed_s.toFixed(3)} s` +
                  (result.error ? ` · stopped: ${result.error}` : '') +
                  (state.count?.request_id ? ` · request ${state.count.request_id.slice(0, 8)}` : '')
                : '—'}
            </span>
          </div>
        ) : (
          <div className="flex min-h-[152px] items-center justify-center">
            <Alert
              variant={status.kind === 'offline' && status.isError ? 'destructive' : 'default'}
              className="w-auto"
            >
              <AlertTitle>
                {status.kind === 'online' ? 'Waiting for state…' : status.label}
              </AlertTitle>
            </Alert>
          </div>
        )}
      </CardContent>

      <CardFooter className="flex-col gap-4 border-t pt-6">
        {/* The range, as seconds from the coverage start. Both commands take it. */}
        <div className="flex items-center gap-3 text-sm">
          <label className="flex items-center gap-2">
            <span className="text-muted-foreground">from</span>
            <input
              type="number"
              aria-label="Range start, seconds from coverage start"
              className="w-24 rounded-md border bg-background px-2 py-1 tabular-nums"
              min={0}
              max={duration ?? undefined}
              step={0.05}
              value={fromS}
              disabled={!hasCoverage}
              onChange={(event) => setFromS(Number(event.target.value))}
            />
            <span className="text-muted-foreground">s</span>
          </label>
          <label className="flex items-center gap-2">
            <span className="text-muted-foreground">to</span>
            <input
              type="number"
              aria-label="Range end, seconds from coverage start"
              className="w-24 rounded-md border bg-background px-2 py-1 tabular-nums"
              min={0}
              max={duration ?? undefined}
              step={0.05}
              value={toS}
              disabled={!hasCoverage}
              onChange={(event) => setToS(Number(event.target.value))}
            />
            <span className="text-muted-foreground">s</span>
          </label>
        </div>

        {/* Disabled until connected and the range is inside the coverage: the
            server would refuse it with a 409 anyway, whose detail is shown below. */}
        <div className="flex items-center justify-center gap-2">
          <Button disabled={!canSend} onClick={() => send(playRange)}>
            <PlayIcon />
            Play range
          </Button>
          <Button variant="outline" disabled={!connected || !playing} onClick={stop}>
            <SquareIcon />
            Stop
          </Button>
          <Button variant="secondary" disabled={!canSend} onClick={() => send(count)}>
            <SigmaIcon />
            Count rows
          </Button>
          {/* Only while the provider reports nothing: the range controls are
              disabled then, and nothing else asks the provider again. */}
          {connected && state && !hasCoverage && (
            <Button variant="outline" onClick={refresh}>
              <RefreshCwIcon />
              Retry provider
            </Button>
          )}
        </div>
        {refusal && <div className="text-sm text-destructive">{refusal}</div>}
      </CardFooter>
    </Card>
  )
}
