import { LoaderCircleIcon, WifiOffIcon } from 'lucide-react'

import { usePmuReportSocket, type VoltageReport } from './usePmuReportSocket'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardAction, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

const kV = (v: number | null | undefined) => (v === null || v === undefined ? '—' : (v / 1000).toFixed(2))

/** The span a report covered, in seconds, from its own range bounds. */
function spanSeconds(report: VoltageReport): string {
  if (!report.range_start || !report.range_end) return '—'
  const seconds = (Date.parse(report.range_end) - Date.parse(report.range_start)) / 1000
  return seconds.toFixed(2)
}

/**
 * The batch half of the slice: ask for a mean-voltage report over the whole
 * source or a slice of it, and watch it arrive. Nothing here sees a sample —
 * only the derived report the module produced onto the bus.
 */
export function ReportCard() {
  const { state, status, connected, run } = usePmuReportSocket()

  return (
    <Card className="w-full gap-0">
      <CardHeader className="border-b">
        <CardTitle className="text-lg">Mean voltage report</CardTitle>
        <span className="text-gray-500">
          Batch query/response over the same stream: a POST starts the job, the report comes down this
          card&apos;s own socket with the request id it was asked under.
        </span>
        <CardAction className="self-center">
          {connected ? (
            <Badge variant="secondary">{state?.pending.length ? 'Running' : 'Idle'}</Badge>
          ) : (
            <Badge variant="outline" className="text-muted-foreground">
              <WifiOffIcon className="size-3" />
              {status.kind === 'connecting' ? 'Connecting' : 'Offline'}
            </Badge>
          )}
        </CardAction>
      </CardHeader>

      <CardContent className="flex flex-col gap-4 px-6 py-4">
        <div className="flex flex-wrap gap-2">
          <Button size="sm" disabled={!connected} onClick={() => run()}>
            Report whole dataset
          </Button>
          <Button size="sm" variant="outline" disabled={!connected} onClick={() => run({ start_s: 0, end_s: 1 })}>
            Report first second
          </Button>
          <Button size="sm" variant="outline" disabled={!connected} onClick={() => run({ start_s: 2 })}>
            Report from 2 s
          </Button>
        </div>

        {state?.pending.map((requestId) => (
          <div key={requestId} className="flex items-center gap-2 text-sm text-muted-foreground">
            <LoaderCircleIcon className="size-4 animate-spin" />
            <span className="font-mono">{requestId}</span> running…
          </div>
        ))}

        {state?.errors.map((error) => (
          <div key={error.request_id} className="text-sm text-destructive">
            <span className="font-mono">{error.request_id}</span> failed: {error.message}
          </div>
        ))}

        {state && state.reports.length === 0 && state.pending.length === 0 && (
          <p className="text-sm text-muted-foreground">No reports yet.</p>
        )}

        <ul className="flex flex-col gap-3">
          {state?.reports.map((report) => (
            <li key={report.request_id ?? report.timestamp ?? ''} className="rounded-md border p-3 text-sm">
              <div className="flex flex-wrap items-baseline justify-between gap-2">
                <span className="font-medium tabular-nums">mean {kV(report.mean_voltage)} kV</span>
                <span className="font-mono text-xs text-muted-foreground">{report.request_id}</span>
              </div>
              <div className="text-xs text-muted-foreground">
                {report.n_samples} samples over {spanSeconds(report)} s · {report.module.name} · topic{' '}
                <span className="font-mono">voltage.live.report</span>
              </div>
              <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 font-mono text-xs tabular-nums">
                {report.stations.map((station) => (
                  <span key={station.station}>
                    {station.station}: {kV(station.mean_voltage)} kV
                  </span>
                ))}
              </div>
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  )
}
